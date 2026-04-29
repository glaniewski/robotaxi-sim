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
  ReferenceLine,
} from "recharts";
import experiments from "../../content/experiments.json";
import { colors, axisStyle, tickStyle, tooltipStyle, legendStyle } from "./chartTheme";

type GeoPoint = {
  n_sites: number;
  plugs_per_site: number;
  charger_kw: number;
  fleet_size: number;
  label: string;
  seeds: number[];
  served_pct: { mean: number | null; min: number | null; max: number | null };
  sla_adherence_pct: { mean: number | null; min: number | null; max: number | null };
  p90_wait_min: { mean: number | null; min: number | null; max: number | null };
};

type Mode = "served" | "sla";
type Props = { mode?: Mode; height?: number };

const niceLabels: Record<number, string> = {
  2: "2 mega depots",
  5: "5 large depots",
  20: "20 medium depots",
  77: "77 small depots",
};

export default function GeoCeilingBars({ mode = "served", height = 320 }: Props) {
  const points = (experiments as any).geographic?.points as GeoPoint[] | undefined;
  if (!points || points.length === 0) {
    return (
      <div
        style={{ height, background: colors.bgSoft }}
        className="flex items-center justify-center rounded-md border border-slate-200 text-sm text-slate-500"
      >
        Geographic-ceiling replicates still running — chart will appear once data is extracted.
      </div>
    );
  }

  const rows = points.map((p) => ({
    n_sites: p.n_sites,
    label: niceLabels[p.n_sites] ?? `N=${p.n_sites}`,
    servedMean: p.served_pct.mean ?? 0,
    servedRange: [
      (p.served_pct.mean ?? 0) - (p.served_pct.min ?? p.served_pct.mean ?? 0),
      (p.served_pct.max ?? p.served_pct.mean ?? 0) - (p.served_pct.mean ?? 0),
    ] as [number, number],
    slaMean: p.sla_adherence_pct.mean ?? 0,
    slaRange: [
      (p.sla_adherence_pct.mean ?? 0) - (p.sla_adherence_pct.min ?? p.sla_adherence_pct.mean ?? 0),
      (p.sla_adherence_pct.max ?? p.sla_adherence_pct.mean ?? 0) - (p.sla_adherence_pct.mean ?? 0),
    ] as [number, number],
    p90Mean: p.p90_wait_min.mean ?? 0,
    fleet_size: p.fleet_size,
    charger_kw: p.charger_kw,
    plugs_per_site: p.plugs_per_site,
  })).sort((a, b) => a.n_sites - b.n_sites);

  const key = mode === "served" ? "servedMean" : "slaMean";
  const errKey = mode === "served" ? "servedRange" : "slaRange";
  const yLabel =
    mode === "served"
      ? "Requests served (% of demand)"
      : "SLA adherence (% within 10 min)";

  return (
    <div style={{ width: "100%", height }}>
      <ResponsiveContainer>
          <BarChart data={rows} margin={{ top: 20, right: 20, left: 15, bottom: 20 }}>
          <CartesianGrid stroke={colors.grid} strokeDasharray="3 3" vertical={false} />
          <XAxis
            dataKey="label"
            tick={tickStyle}
            axisLine={{ stroke: colors.axis }}
            tickLine={{ stroke: colors.axis }}
            interval={0}
          />
          <YAxis
            domain={[0, 100]}
            tick={tickStyle}
            tickFormatter={(v: number) => `${v}%`}
            axisLine={{ stroke: colors.axis }}
            tickLine={{ stroke: colors.axis }}
            label={{
              value: yLabel,
              angle: -90,
              position: "insideLeft",
              dx: 10,
              dy: 54,
              style: { ...tickStyle, fill: colors.inkSoft },
            }}
          />
          <Tooltip
            contentStyle={tooltipStyle}
            formatter={(value: number) => `${value?.toFixed(2)}%`}
            labelFormatter={(label: string) => label}
          />
          <ReferenceLine
            y={95}
            stroke={colors.muted}
            strokeDasharray="4 4"
            label={{ value: "95%", position: "right", fill: colors.muted, fontSize: 11 }}
          />
          <Bar
            dataKey={key}
            name={mode === "served" ? "Served %" : "SLA adherence %"}
            radius={[6, 6, 0, 0]}
            isAnimationActive={false}
          >
            {rows.map((r, i) => (
              <Cell key={i} fill={r.n_sites === 77 ? colors.accent3 : colors.accent} />
            ))}
            <ErrorBar
              dataKey={errKey}
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
