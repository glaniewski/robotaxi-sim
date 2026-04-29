import {
  BarChart,
  Bar,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  ResponsiveContainer,
  ErrorBar,
  Cell,
} from "recharts";
import experiments from "../../content/experiments.json";
import { colors, tickStyle, tooltipStyle, legendStyle } from "./chartTheme";

type Anchor = {
  label: string;
  battery_kwh: number;
  seeds: number[];
  cost_per_trip: { mean: number | null; min: number | null; max: number | null };
  served_pct: { mean: number | null; min: number | null; max: number | null };
  sla_adherence_pct: { mean: number | null; min: number | null; max: number | null };
  p90_wait_min: { mean: number | null; min: number | null; max: number | null };
};

type Metric = "cost_per_trip" | "served_pct" | "sla_adherence_pct" | "p90_wait_min";
type Props = { metric?: Metric; height?: number };

export default function BatterySizePareto({ metric = "cost_per_trip", height = 280 }: Props) {
  const anchors = (experiments as any).battery?.anchors as Anchor[] | undefined;
  if (!anchors || anchors.length === 0) {
    return (
      <div
        style={{ height, background: colors.bgSoft }}
        className="flex items-center justify-center rounded-md border border-slate-200 text-sm text-slate-500"
      >
        Battery anchors still running — chart will appear once data is extracted.
      </div>
    );
  }

  const rows = anchors
    .map((a) => ({
      label: `${a.battery_kwh.toFixed(0)} kWh battery`,
      battery_kwh: a.battery_kwh,
      value: (a[metric]?.mean ?? 0) as number,
      err: [
        ((a[metric]?.mean ?? 0) as number) - ((a[metric]?.min ?? a[metric]?.mean ?? 0) as number),
        ((a[metric]?.max ?? a[metric]?.mean ?? 0) as number) - ((a[metric]?.mean ?? 0) as number),
      ] as [number, number],
    }))
    .sort((a, b) => a.battery_kwh - b.battery_kwh);

  const metricCfg: Record<
    Metric,
    { yTitle: string; format: (v: number) => string; axisDomain: [number | string, number | string] }
  > = {
    cost_per_trip: {
      yTitle: "Cost per trip (USD)",
      format: (v) => `$${v.toFixed(2)}`,
      axisDomain: [0, "dataMax + 0.2"],
    },
    served_pct: {
      yTitle: "Served % of requests",
      format: (v) => `${v.toFixed(1)}%`,
      axisDomain: [0, 100],
    },
    sla_adherence_pct: {
      yTitle: "SLA adherence (% within 10 min)",
      format: (v) => `${v.toFixed(1)}%`,
      axisDomain: [0, 100],
    },
    p90_wait_min: {
      yTitle: "p90 wait time (min)",
      format: (v) => `${v.toFixed(2)} min`,
      axisDomain: [0, "dataMax + 1"],
    },
  };

  const cfg = metricCfg[metric];

  return (
    <div style={{ width: "100%", height }}>
      <ResponsiveContainer>
        <BarChart data={rows} margin={{ top: 20, right: 20, left: 15, bottom: 10 }}>
          <CartesianGrid stroke={colors.grid} strokeDasharray="3 3" vertical={false} />
          <XAxis
            dataKey="label"
            tick={tickStyle}
            axisLine={{ stroke: colors.axis }}
            tickLine={{ stroke: colors.axis }}
          />
          <YAxis
            domain={cfg.axisDomain}
            tickFormatter={cfg.format}
            tick={tickStyle}
            axisLine={{ stroke: colors.axis }}
            tickLine={{ stroke: colors.axis }}
            label={{
              value: cfg.yTitle,
              angle: -90,
              position: "insideLeft",
              dx: 10,
              dy: cfg.yTitle.length > 18 ? 54 : 32,
              style: { ...tickStyle, fill: colors.inkSoft },
            }}
          />
          <Tooltip
            contentStyle={tooltipStyle}
            formatter={(v: number) => cfg.format(v)}
          />
          <Bar
            dataKey="value"
            name={cfg.yTitle}
            radius={[6, 6, 0, 0]}
            isAnimationActive={false}
          >
            {rows.map((r, i) => (
              <Cell key={i} fill={r.battery_kwh <= 50 ? colors.accent3 : colors.accent} />
            ))}
            <ErrorBar
              dataKey="err"
              width={8}
              stroke={colors.inkSoft}
              strokeWidth={1.2}
              direction="y"
            />
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
