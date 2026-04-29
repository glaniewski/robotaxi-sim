import {
  LineChart,
  Line,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  ErrorBar,
  ReferenceLine,
} from "recharts";
import experiments from "../../content/experiments.json";
import { colors, tickStyle, tooltipStyle } from "./chartTheme";

type PlugPoint = {
  plugs_per_site: number;
  charger_kw: number;
  total_plugs: number;
  total_mw: number;
  seeds: number[];
  served_pct: { mean: number | null; min: number | null; max: number | null };
  p90_wait_min: { mean: number | null; min: number | null; max: number | null };
  charger_utilization_pct: { mean: number | null; min: number | null; max: number | null };
};

type Props = { height?: number };

export default function ChargerPlugSweep({ height = 360 }: Props) {
  const raw = (experiments as any).charger?.plug_sweep as PlugPoint[] | undefined;

  if (!raw || raw.length === 0) {
    return (
      <div
        style={{ height, background: colors.bgSoft }}
        className="flex items-center justify-center rounded-md border border-slate-200 text-sm text-slate-500"
      >
        Iso-power sweep still running — chart will appear once extract completes.
      </div>
    );
  }

  const points = raw
    .filter((p) => p.served_pct.mean !== null)
    .map((p) => ({
      plugs: p.plugs_per_site,
      label: `${p.plugs_per_site}p × ${p.charger_kw.toFixed(2).replace(/\.?0+$/, "")} kW`,
      served: p.served_pct.mean!,
      served_err: [
        p.served_pct.mean! - (p.served_pct.min ?? p.served_pct.mean!),
        (p.served_pct.max ?? p.served_pct.mean!) - p.served_pct.mean!,
      ] as [number, number],
      p90: p.p90_wait_min.mean,
      chgUtil: p.charger_utilization_pct.mean,
      total_mw: p.total_mw,
    }));

  const servedMin = Math.min(...points.map((p) => p.served));
  const yMin = Math.floor(servedMin - 2);

  const CustomDot = (props: any) => {
    const { cx, cy } = props;
    return <circle cx={cx} cy={cy} r={5} fill={colors.accent} stroke="#fff" strokeWidth={1.5} />;
  };

  const CustomTooltip = ({ active, payload }: any) => {
    if (!active || !payload?.length) return null;
    const d = payload[0].payload;
    return (
      <div style={tooltipStyle}>
        <div style={{ fontWeight: 600, marginBottom: 4, color: colors.ink }}>{d.label}</div>
        <div style={{ color: colors.inkSoft }}>Served: <b>{d.served.toFixed(1)}%</b></div>
        {d.p90 !== null && (
          <div style={{ color: colors.inkSoft }}>p90 wait: <b>{d.p90.toFixed(1)} min</b></div>
        )}
        {d.chgUtil !== null && (
          <div style={{ color: colors.inkSoft }}>Charger util: <b>{d.chgUtil.toFixed(0)}%</b></div>
        )}
        <div style={{ color: colors.muted, fontSize: 10, marginTop: 4 }}>
          {d.total_mw.toFixed(2)} MW installed
        </div>
      </div>
    );
  };

  return (
    <div style={{ width: "100%", height }}>
      <ResponsiveContainer>
        <LineChart data={points} margin={{ top: 16, right: 24, left: 15, bottom: 52 }}>
          <CartesianGrid stroke={colors.grid} strokeDasharray="3 3" />
          <XAxis
            dataKey="plugs"
            type="number"
            scale="log"
            domain={["dataMin", "dataMax"]}
            ticks={points.map((p) => p.plugs)}
            tickFormatter={(v: number) => `${v}p`}
            tick={tickStyle}
            axisLine={{ stroke: colors.axis }}
            tickLine={{ stroke: colors.axis }}
            label={{
              value: "Plugs per site  (← fewer, faster  |  more, slower →)",
              position: "insideBottom",
              offset: -10,
              style: { ...tickStyle, fill: colors.inkSoft },
            }}
          />
          <YAxis
            dataKey="served"
            domain={[yMin, 100]}
            tickFormatter={(v: number) => `${v.toFixed(0)}%`}
            tick={tickStyle}
            axisLine={{ stroke: colors.axis }}
            tickLine={{ stroke: colors.axis }}
            label={{
              value: "Served demand (%)",
              angle: -90,
              position: "insideLeft",
              dx: 10,
              dy: 54,
              style: { ...tickStyle, fill: colors.inkSoft },
            }}
          />
          <Tooltip content={<CustomTooltip />} cursor={{ stroke: colors.muted, strokeDasharray: "3 3" }} />
          <ReferenceLine
            y={95}
            stroke={colors.accent3}
            strokeDasharray="4 3"
            label={{ value: "95% SLA target", position: "insideTopRight", fill: colors.accent3, fontSize: 10 }}
          />
          <Line
            type="monotone"
            dataKey="served"
            stroke={colors.accent}
            strokeWidth={2.5}
            dot={<CustomDot />}
            activeDot={{ r: 6 }}
            isAnimationActive={false}
          >
            <ErrorBar dataKey="served_err" direction="y" width={5} stroke={colors.inkSoft} opacity={0.5} />
          </Line>
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
