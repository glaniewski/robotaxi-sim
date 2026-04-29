/**
 * Horizontal bar chart showing cost-per-trip sensitivity across all tested
 * operational parameters, sorted by magnitude of effect.
 *
 * Each bar = (high-setting cost) − (low-setting cost) for that parameter,
 * holding everything else fixed. Labels show the SLA change that came with it.
 */
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Cell,
  LabelList,
  ReferenceLine,
} from "recharts";
import experiments from "../../content/experiments.json";
import { colors, tickStyle, tooltipStyle } from "./chartTheme";

type Props = { height?: number };

function buildLevers() {
  const d = experiments as any;

  // 1. Vehicle consumption tier — compare tesla vs waymo at ~95% SLA configs
  //    from the Pareto frontier. Take the average cost of each preset at the
  //    configs where SLA ≥ 94%.
  const pareto: any[] = d.pareto?.points ?? [];
  const teslaHigh = pareto.filter(
    (p) => p.preset === "tesla" && p.sla_adherence_pct >= 94
  );
  const waymoHigh = pareto.filter(
    (p) => p.preset === "waymo" && p.sla_adherence_pct >= 94
  );
  const teslaAvg =
    teslaHigh.length > 0
      ? teslaHigh.reduce((s: number, p: any) => s + p.cost_per_trip, 0) /
        teslaHigh.length
      : null;
  const waymoAvg =
    waymoHigh.length > 0
      ? waymoHigh.reduce((s: number, p: any) => s + p.cost_per_trip, 0) /
        waymoHigh.length
      : null;

  // 2. Fleet size above the knee — cost at fleet=4,250 vs fleet=5,500
  const pts: any[] = d.fleet_sweep?.points ?? [];
  const at4250 = pts.find((p) => p.fleet === 4250);
  const at5500 = pts.find((p) => p.fleet === 5500);

  // 3. Battery capacity — 40 vs 75 kWh (same depot/charger config)
  const batAnchors: any[] = d.battery?.anchors ?? [];
  const bat40 = batAnchors.find((a) => a.battery_kwh === 40);
  const bat75 = batAnchors.find((a) => a.battery_kwh === 75);

  // 4. Charger power tier — same total installed power (8.9 MW), fewer faster
  //    plugs vs many slower ones
  const chAnchors: any[] = d.charger?.anchors ?? [];
  const chSlow = chAnchors.find((a) => a.label === "charger_slow_N77_10p11kW");
  const chFast = chAnchors.find(
    (a) => a.label === "charger_matched_N77_2p57kW"
  );

  const levers = [
    teslaAvg !== null && waymoAvg !== null
      ? {
          name: "Consumption rate\n(low → high preset)",
          delta: +(waymoAvg - teslaAvg).toFixed(3),
          note: "Procurement choice; effect partly\ncompounded by charging policy",
          caveat: true,
        }
      : null,
    at4250 && at5500
      ? {
          name: "Fleet above knee\n(4,250 → 5,500 vehicles)",
          delta: +(
            at5500.cost_per_trip.mean - at4250.cost_per_trip.mean
          ).toFixed(3),
          note: "+2 pts SLA adherence",
          caveat: false,
        }
      : null,
    bat40 && bat75
      ? {
          name: "Battery capacity\n(40 → 75 kWh)",
          delta: +(
            bat75.cost_per_trip.mean - bat40.cost_per_trip.mean
          ).toFixed(3),
          note: "+0.6 pts SLA adherence",
          caveat: false,
        }
      : null,
    chSlow && chFast
      ? {
          name: "Charger power tier\n(2×57.5kW → 10×11.5kW)",
          delta: +(
            chSlow.cost_per_trip.mean - chFast.cost_per_trip.mean
          ).toFixed(3),
          note: "+0.5 pts SLA adherence",
          caveat: false,
        }
      : null,
  ]
    .filter(Boolean)
    .sort((a: any, b: any) => b.delta - a.delta) as {
    name: string;
    delta: number;
    note: string;
    caveat: boolean;
  }[];

  return levers;
}

const CustomTooltip = ({ active, payload }: any) => {
  if (!active || !payload?.length) return null;
  const d = payload[0].payload;
  return (
    <div style={{ ...tooltipStyle, maxWidth: 240 }}>
      <div style={{ fontWeight: 600, marginBottom: 4 }}>
        {d.name.replace(/\n/g, " ")}
      </div>
      <div>Cost delta: +${d.delta.toFixed(2)}/trip</div>
      <div style={{ color: colors.muted, marginTop: 4, fontSize: 11 }}>
        {d.note}
      </div>
    </div>
  );
};

export default function CostLeverBars({ height = 320 }: Props) {
  const levers = buildLevers();

  if (levers.length === 0) {
    return (
      <div
        style={{ height, background: colors.bgSoft }}
        className="flex items-center justify-center rounded-md border border-slate-200 text-sm text-slate-500"
      >
        Cost lever data not available.
      </div>
    );
  }

  return (
    <div style={{ width: "100%", height }}>
      <ResponsiveContainer>
        <BarChart
          layout="vertical"
          data={levers}
          margin={{ top: 16, right: 80, left: 8, bottom: 8 }}
        >
          <CartesianGrid
            stroke={colors.grid}
            strokeDasharray="3 3"
            horizontal={false}
          />
          <XAxis
            type="number"
            tickFormatter={(v) => `+$${v.toFixed(2)}`}
            tick={tickStyle}
            axisLine={{ stroke: colors.axis }}
            tickLine={{ stroke: colors.axis }}
            domain={[0, "dataMax + 0.1"]}
            label={{
              value: "Cost per trip increase (USD)",
              position: "insideBottom",
              offset: -2,
              style: { ...tickStyle, fill: colors.inkSoft },
            }}
          />
          <YAxis
            type="category"
            dataKey="name"
            width={170}
            tick={{ ...tickStyle, fontSize: 11 }}
            axisLine={false}
            tickLine={false}
            tickFormatter={(v: string) => v.split("\n")[0]}
          />
          <Tooltip content={<CustomTooltip />} cursor={{ fill: colors.bgSoft }} />
          <ReferenceLine x={0} stroke={colors.axis} />
          <Bar dataKey="delta" radius={[0, 4, 4, 0]} isAnimationActive={false}>
            {levers.map((l, i) => (
              <Cell
                key={i}
                fill={l.caveat ? colors.accent2 : colors.accent}
                fillOpacity={l.caveat ? 0.6 : 0.85}
              />
            ))}
            <LabelList
              dataKey="delta"
              position="right"
              formatter={(v: number) => `+$${v.toFixed(2)}`}
              style={{ ...tickStyle, fontSize: 12, fontWeight: 600 }}
            />
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
