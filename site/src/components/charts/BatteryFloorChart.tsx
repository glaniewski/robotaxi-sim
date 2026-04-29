import {
  LineChart,
  Line,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  ReferenceLine,
  ResponsiveContainer,
} from "recharts";
import experiments from "../../content/experiments.json";
import { colors, tickStyle, tooltipStyle, legendStyle } from "./chartTheme";

type SeriesRow = {
  label: string;
  battery_kwh: number;
  sla_adherence_pct: { mean: number | null; min: number | null; max: number | null };
};

type ChartRow = {
  kwh: number;
  label: string;
  n77: number | null;
  n77min: number | null;
  n77max: number | null;
  n2: number | null;
  n2min: number | null;
  n2max: number | null;
};

export default function BatteryFloorChart({ height = 320 }: { height?: number }) {
  const bf = (experiments as any).battery_floor as
    | { n77: SeriesRow[]; n2: SeriesRow[] }
    | undefined;

  if (!bf) {
    return (
      <div
        style={{ height, background: colors.bgSoft }}
        className="flex items-center justify-center rounded-md border border-slate-200 text-sm text-slate-500"
      >
        Battery floor data not yet extracted — run scripts/extract_battery_floor.py
      </div>
    );
  }

  // Build a unified keyed map by kWh
  const allKwh = Array.from(
    new Set([...bf.n77.map((r) => r.battery_kwh), ...bf.n2.map((r) => r.battery_kwh)])
  ).sort((a, b) => a - b);

  const n77Map = Object.fromEntries(bf.n77.map((r) => [r.battery_kwh, r]));
  const n2Map = Object.fromEntries(bf.n2.map((r) => [r.battery_kwh, r]));

  const rows: ChartRow[] = allKwh.map((kwh) => {
    const a = n77Map[kwh];
    const b = n2Map[kwh];
    return {
      kwh,
      label: `${kwh} kWh`,
      n77: a?.sla_adherence_pct?.mean ?? null,
      n77min: a?.sla_adherence_pct?.min ?? null,
      n77max: a?.sla_adherence_pct?.max ?? null,
      n2: b?.sla_adherence_pct?.mean ?? null,
      n2min: b?.sla_adherence_pct?.min ?? null,
      n2max: b?.sla_adherence_pct?.max ?? null,
    };
  });

  const yMin = 85;
  const yMax = 100;

  return (
    <div style={{ width: "100%", height }}>
      <ResponsiveContainer>
        <LineChart data={rows} margin={{ top: 20, right: 24, left: 15, bottom: 48 }}>
          <CartesianGrid stroke={colors.grid} strokeDasharray="3 3" vertical={false} />
          <XAxis
            dataKey="label"
            tick={tickStyle}
            axisLine={{ stroke: colors.axis }}
            tickLine={{ stroke: colors.axis }}
            label={{
              value: "Vehicle battery capacity",
              position: "insideBottom",
              offset: -6,
              style: { ...tickStyle, fill: colors.inkSoft },
            }}
          />
          <YAxis
            domain={[yMin, yMax]}
            tickFormatter={(v) => `${v}%`}
            tick={tickStyle}
            axisLine={{ stroke: colors.axis }}
            tickLine={{ stroke: colors.axis }}
            label={{
              value: "Share of requests",
              angle: -90,
              position: "insideLeft",
              dx: 10,
              dy: 54,
              style: { ...tickStyle, fill: colors.inkSoft },
            }}
          />
          <Tooltip
            contentStyle={tooltipStyle}
            formatter={(value: number | null, name: string) => [
              value !== null ? `${value.toFixed(1)}%` : "—",
              name,
            ]}
            labelFormatter={(label) => `Battery: ${label}`}
          />
          <Legend wrapperStyle={legendStyle} />
          <ReferenceLine
            y={95}
            stroke={colors.muted}
            strokeDasharray="4 4"
            label={{
              value: "95% target",
              position: "insideTopRight",
              style: { ...tickStyle, fill: colors.muted, fontSize: 10 },
            }}
          />
          <Line
            type="monotone"
            dataKey="n77"
            name="77 distributed depots"
            stroke={colors.accent}
            strokeWidth={2.5}
            dot={{ r: 5, fill: colors.accent, strokeWidth: 0 }}
            activeDot={{ r: 7 }}
            connectNulls={false}
            isAnimationActive={false}
          />
          <Line
            type="monotone"
            dataKey="n2"
            name="2 mega-depots"
            stroke={colors.accent2}
            strokeWidth={2.5}
            dot={{ r: 5, fill: colors.accent2, strokeWidth: 0 }}
            activeDot={{ r: 7 }}
            connectNulls={false}
            isAnimationActive={false}
            strokeDasharray="6 3"
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
