import {
  ScatterChart,
  Scatter,
  CartesianGrid,
  XAxis,
  YAxis,
  ZAxis,
  Tooltip,
  Legend,
  ResponsiveContainer,
  ErrorBar,
} from "recharts";
import experiments from "../../content/experiments.json";
import { colors, axisStyle, tickStyle, tooltipStyle, legendStyle } from "./chartTheme";

type Anchor = {
  label: string;
  n_sites: number;
  plugs_per_site: number;
  charger_kw: number;
  fleet_size: number;
  battery_kwh: number;
  seeds: number[];
  cost_per_trip: { mean: number | null; min: number | null; max: number | null };
  sla_adherence_pct: { mean: number | null; min: number | null; max: number | null };
  p90_wait_min: { mean: number | null; min: number | null; max: number | null };
  served_pct: { mean: number | null; min: number | null; max: number | null };
};

type Props = { height?: number };

export default function ChargerTierFrontier({ height = 340 }: Props) {
  const anchors = (experiments as any).charger?.anchors as Anchor[] | undefined;
  if (!anchors || anchors.length === 0) {
    return (
      <div
        style={{ height, background: colors.bgSoft }}
        className="flex items-center justify-center rounded-md border border-slate-200 text-sm text-slate-500"
      >
        Charger-tier replicates still running — chart will appear once data is extracted.
      </div>
    );
  }

  const rows = anchors.map((a) => ({
    label: `${a.plugs_per_site} plugs/site × ${a.charger_kw.toFixed(1)} kW`,
    kind: a.charger_kw <= 25 ? "slow" : "fast",
    charger_kw: a.charger_kw,
    n_sites: a.n_sites,
    fleet_size: a.fleet_size,
    cost_mean: a.cost_per_trip.mean ?? 0,
    cost_err: [
      (a.cost_per_trip.mean ?? 0) - (a.cost_per_trip.min ?? a.cost_per_trip.mean ?? 0),
      (a.cost_per_trip.max ?? a.cost_per_trip.mean ?? 0) - (a.cost_per_trip.mean ?? 0),
    ] as [number, number],
    sla_mean: a.sla_adherence_pct.mean ?? 0,
    sla_err: [
      (a.sla_adherence_pct.mean ?? 0) - (a.sla_adherence_pct.min ?? a.sla_adherence_pct.mean ?? 0),
      (a.sla_adherence_pct.max ?? a.sla_adherence_pct.mean ?? 0) - (a.sla_adherence_pct.mean ?? 0),
    ] as [number, number],
    served_mean: a.served_pct.mean ?? 0,
    p90_mean: a.p90_wait_min.mean ?? 0,
  }));

  const slow = rows.filter((r) => r.kind === "slow");
  const fast = rows.filter((r) => r.kind === "fast");

  return (
    <div style={{ width: "100%", height }}>
      <ResponsiveContainer>
          <ScatterChart margin={{ top: 20, right: 24, left: 15, bottom: 52 }}>
          <CartesianGrid stroke={colors.grid} strokeDasharray="3 3" />
          <XAxis
            type="number"
            dataKey="cost_mean"
            name="Cost per trip"
            domain={["dataMin - 0.2", "dataMax + 0.2"]}
            tickFormatter={(v: number) => `$${v.toFixed(2)}`}
            tick={tickStyle}
            axisLine={{ stroke: colors.axis }}
            tickLine={{ stroke: colors.axis }}
            label={{
              value: "Cost per trip (USD)",
              position: "insideBottom",
              offset: -8,
              style: { ...tickStyle, fill: colors.inkSoft },
            }}
          />
          <YAxis
            type="number"
            dataKey="sla_mean"
            name="SLA %"
            domain={[Math.floor(Math.min(...rows.map((r) => r.sla_mean)) - 2), 100]}
            tickFormatter={(v: number) => `${v.toFixed(0)}%`}
            tick={tickStyle}
            axisLine={{ stroke: colors.axis }}
            tickLine={{ stroke: colors.axis }}
            label={{
              value: "SLA adherence (% within 10 min)",
              angle: -90,
              position: "insideLeft",
              dx: 10,
              dy: 54,
              style: { ...tickStyle, fill: colors.inkSoft },
            }}
          />
          <ZAxis range={[220, 220]} />
          <Tooltip
            contentStyle={tooltipStyle}
            cursor={{ strokeDasharray: "3 3", stroke: colors.muted }}
            formatter={(value: number, name: string) => {
              if (name === "Cost per trip") return [`$${value.toFixed(2)}`, name];
              if (name === "SLA %") return [`${value.toFixed(1)}%`, name];
              return [value, name];
            }}
          />
          <Legend wrapperStyle={legendStyle} />
          <Scatter
            name="10 plugs/site × 11.5 kW"
            data={slow}
            fill={colors.accent3}
            shape="circle"
            legendType="circle"
            isAnimationActive={false}
          >
            <ErrorBar dataKey="cost_err" direction="x" width={6} stroke={colors.inkSoft} />
            <ErrorBar dataKey="sla_err" direction="y" width={6} stroke={colors.inkSoft} />
          </Scatter>
          <Scatter
            name="2 plugs/site × 57.5 kW"
            data={fast}
            fill={colors.accent2}
            shape="square"
            legendType="square"
            isAnimationActive={false}
          >
            <ErrorBar dataKey="cost_err" direction="x" width={6} stroke={colors.inkSoft} />
            <ErrorBar dataKey="sla_err" direction="y" width={6} stroke={colors.inkSoft} />
          </Scatter>
        </ScatterChart>
      </ResponsiveContainer>
    </div>
  );
}
