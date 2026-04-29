import {
  ComposedChart,
  Line,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  ResponsiveContainer,
  ReferenceLine,
} from "recharts";
import experiments from "../../content/experiments.json";
import { colors, tickStyle, tooltipStyle, legendStyle } from "./chartTheme";

type CongPoint = {
  osrm_time_multiplier: number;
  served_pct: number;
  p90_wait_min: number;
  deadhead_pct: number;
  charger_utilization_pct: number;
};

type Props = { height?: number };

export default function CongestionCapacityTax({ height = 320 }: Props) {
  const points = (experiments as any).congestion?.points as CongPoint[] | undefined;
  if (!points || points.length === 0) {
    return (
      <div
        style={{ height, background: colors.bgSoft }}
        className="flex items-center justify-center rounded-md border border-slate-200 text-sm text-slate-500"
      >
        Congestion sweep unavailable — check data/sweep_osrm_time_multiplier_exp71_3d.json.
      </div>
    );
  }

  const rows = [...points]
    .sort((a, b) => a.osrm_time_multiplier - b.osrm_time_multiplier)
    .map((p) => ({
      label: `${p.osrm_time_multiplier.toFixed(1)}×`,
      multiplier: p.osrm_time_multiplier,
      served_pct: p.served_pct,
      deadhead_pct: p.deadhead_pct,
    }));

  return (
    <div style={{ width: "100%", height }}>
      <ResponsiveContainer>
        <ComposedChart data={rows} margin={{ top: 20, right: 20, left: 15, bottom: 48 }}>
          <CartesianGrid stroke={colors.grid} strokeDasharray="3 3" vertical={false} />
          <XAxis
            dataKey="label"
            tick={tickStyle}
            axisLine={{ stroke: colors.axis }}
            tickLine={{ stroke: colors.axis }}
            label={{
              value: "Travel-time multiplier vs. free-flow OSRM",
              position: "insideBottom",
              offset: -6,
              style: { ...tickStyle, fill: colors.inkSoft },
            }}
          />
          <YAxis
            yAxisId="pct"
            domain={[0, 100]}
            tickFormatter={(v: number) => `${v}%`}
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
            formatter={(v: number, name: string) => [`${v?.toFixed(1)}%`, name]}
            labelFormatter={(label: string) => `Travel time ${label} slower than free-flow`}
          />
          <Legend wrapperStyle={legendStyle} />
          <ReferenceLine
            yAxisId="pct"
            y={95}
            stroke={colors.muted}
            strokeDasharray="4 4"
            label={{ value: "95% target", position: "insideTopRight", fill: colors.muted, fontSize: 11 }}
          />
          <Line
            yAxisId="pct"
            type="monotone"
            dataKey="served_pct"
            name="Served %"
            stroke={colors.accent}
            strokeWidth={2.5}
            dot={{ fill: colors.accent, r: 4 }}
            isAnimationActive={false}
          />
          <Line
            yAxisId="pct"
            type="monotone"
            dataKey="deadhead_pct"
            name="Deadhead %"
            stroke={colors.accent2}
            strokeWidth={2}
            strokeDasharray="2 3"
            dot={{ fill: colors.accent2, r: 3 }}
            isAnimationActive={false}
          />
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  );
}
