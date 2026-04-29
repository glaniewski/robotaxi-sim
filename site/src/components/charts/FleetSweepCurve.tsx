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
  ReferenceArea,
} from "recharts";
import experiments from "../../content/experiments.json";
import { colors, axisStyle, tickStyle, tooltipStyle, legendStyle } from "./chartTheme";

type FleetPoint = {
  fleet: number;
  seeds: number[];
  served_pct: { mean: number | null; min: number | null; max: number | null; values: number[] };
  sla_adherence_pct: { mean: number | null; min: number | null; max: number | null; values: number[] };
  p90_wait_min: { mean: number | null; min: number | null; max: number | null; values: number[] };
  median_wait_min: { mean: number | null; min: number | null; max: number | null; values: number[] };
  cost_per_trip: { mean: number | null; values: number[] };
  trips_per_vehicle_per_day?: { mean: number | null; values: number[] };
  utilization_pct?: { mean: number | null; values: number[] };
  active_time_pct?: { mean: number | null; values: number[] };
};

type Mode = "service" | "wait";

type Props = {
  mode?: Mode;
  height?: number;
  slaTarget?: number;
};

function toRows(points: FleetPoint[]) {
  return points
    .filter((p) => p.served_pct.mean !== null)
    .map((p) => ({
      fleet: p.fleet,
      servedMean: p.served_pct.mean!,
      servedRange: [p.served_pct.min, p.served_pct.max],
      slaMean: p.sla_adherence_pct.mean,
      p50Mean: p.median_wait_min.mean,
      p90Mean: p.p90_wait_min.mean,
      p90Range: [p.p90_wait_min.min, p.p90_wait_min.max],
      costMean: p.cost_per_trip.mean,
      tripsPerVehicle: p.trips_per_vehicle_per_day?.mean,
      utilizationPct: p.utilization_pct?.mean,
      activeTimePct: p.active_time_pct?.mean,
      n_seeds: p.seeds.length,
    }));
}

export default function FleetSweepCurve({
  mode = "service",
  height = 340,
  slaTarget = 95,
}: Props) {
  const points = (experiments as any).fleet_sweep?.points as FleetPoint[] | undefined;
  if (!points || points.length === 0) {
    return (
      <div
        style={{ height, background: colors.bgSoft }}
        className="flex items-center justify-center rounded-md border border-slate-200 text-sm text-slate-500"
      >
        Fleet-sizing sweep still running — chart will appear once data is extracted.
      </div>
    );
  }
  const rows = toRows(points);

  if (mode === "service") {
    return (
      <div style={{ width: "100%", height }}>
        <ResponsiveContainer>
          <ComposedChart
            data={rows}
            margin={{ top: 20, right: 20, left: 15, bottom: 48 }}
          >
            <CartesianGrid stroke={colors.grid} strokeDasharray="3 3" vertical={false} />
            <XAxis
              dataKey="fleet"
              type="number"
              domain={["dataMin", "dataMax"]}
              ticks={rows.map((r) => r.fleet)}
              tick={tickStyle}
              tickFormatter={(v: number) => v.toLocaleString()}
              axisLine={{ stroke: colors.axis }}
              tickLine={{ stroke: colors.axis }}
              label={{
                value: "Fleet size (vehicles)",
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
            <YAxis
              yAxisId="trips"
              orientation="right"
              domain={[0, "dataMax + 5"]}
              tickFormatter={(v: number) => `${v.toFixed(0)}`}
              tick={tickStyle}
              axisLine={{ stroke: colors.axis }}
              tickLine={{ stroke: colors.axis }}
              label={{
                value: "Trips / vehicle / day",
                angle: 90,
                position: "insideRight",
                dx: -8,
                dy: 45,
                style: { ...tickStyle, fill: colors.inkSoft },
              }}
            />
            <Tooltip
              contentStyle={tooltipStyle}
              formatter={(value: number | [number, number], name: string) => {
                if (name === "Trips / vehicle / day") {
                  return [`${(value as number)?.toFixed(1)}`, name];
                }
                return [`${(value as number)?.toFixed(2)}%`, name];
              }}
              labelFormatter={(fleet: number) => `Fleet = ${fleet.toLocaleString()} vehicles`}
            />
            <Legend wrapperStyle={legendStyle} />
            <ReferenceLine
              yAxisId="pct"
              y={slaTarget}
              stroke={colors.muted}
              strokeDasharray="4 4"
              label={{
                value: `${slaTarget}% target`,
                position: "insideTopRight",
                fill: colors.muted,
                fontSize: 11,
              }}
            />
            <Line
              yAxisId="pct"
              type="monotone"
              dataKey="servedMean"
              name="Served % of requests (mean of 3 seeds)"
              stroke={colors.accent}
              strokeWidth={2.4}
              dot={{ fill: colors.accent, r: 3.2 }}
              activeDot={{ r: 5 }}
              isAnimationActive={false}
            />
            <Line
              yAxisId="trips"
              type="monotone"
              dataKey="tripsPerVehicle"
              name="Trips / vehicle / day"
              stroke={colors.accent2}
              strokeWidth={2}
              strokeDasharray="5 4"
              dot={{ fill: colors.accent2, r: 3 }}
              isAnimationActive={false}
            />
            {rows.some((r) => r.activeTimePct != null) && (
              <Line
                yAxisId="pct"
                type="monotone"
                dataKey="activeTimePct"
                name="Active time % (non-idle)"
                stroke={colors.accent3}
                strokeWidth={2}
                strokeDasharray="3 3"
                dot={{ fill: colors.accent3, r: 3 }}
                isAnimationActive={false}
              />
            )}
          </ComposedChart>
        </ResponsiveContainer>
      </div>
    );
  }

  return (
    <div style={{ width: "100%", height }}>
      <ResponsiveContainer>
        <ComposedChart data={rows} margin={{ top: 20, right: 20, left: 15, bottom: 48 }}>
          <CartesianGrid stroke={colors.grid} strokeDasharray="3 3" vertical={false} />
          <XAxis
            dataKey="fleet"
            type="number"
            domain={["dataMin", "dataMax"]}
            ticks={rows.map((r) => r.fleet)}
            tick={tickStyle}
            tickFormatter={(v: number) => v.toLocaleString()}
            axisLine={{ stroke: colors.axis }}
            tickLine={{ stroke: colors.axis }}
            label={{
              value: "Fleet size (vehicles)",
              position: "insideBottom",
              offset: -6,
              style: { ...tickStyle, fill: colors.inkSoft },
            }}
          />
          <YAxis
            domain={[0, "auto"]}
            tickFormatter={(v: number) => `${v.toFixed(0)}m`}
            tick={tickStyle}
            axisLine={{ stroke: colors.axis }}
            tickLine={{ stroke: colors.axis }}
            label={{
              value: "Wait time (minutes)",
              angle: -90,
              position: "insideLeft",
              dx: 10,
              dy: 38,
              style: { ...tickStyle, fill: colors.inkSoft },
            }}
          />
          <Tooltip
            contentStyle={tooltipStyle}
            formatter={(value: number, name: string) => [`${value?.toFixed(2)} min`, name]}
            labelFormatter={(fleet: number) => `Fleet = ${fleet.toLocaleString()} vehicles`}
          />
          <Legend wrapperStyle={legendStyle} />
          <ReferenceArea
            y1={0}
            y2={10}
            fill={colors.band}
            stroke="none"
            ifOverflow="hidden"
          />
          <ReferenceLine
            y={10}
            stroke={colors.accent}
            strokeDasharray="4 3"
            strokeOpacity={0.45}
            label={{
              value: "10 min wait limit",
              position: "insideTopRight",
              style: { ...tickStyle, fill: colors.accent, fontSize: 10 },
            }}
          />
          <Line
            type="monotone"
            dataKey="p50Mean"
            name="Median wait (p50)"
            stroke={colors.accent2}
            strokeWidth={2}
            dot={{ fill: colors.accent2, r: 2.5 }}
            isAnimationActive={false}
          />
          <Line
            type="monotone"
            dataKey="p90Mean"
            name="p90 wait"
            stroke={colors.accent}
            strokeWidth={2}
            dot={{ fill: colors.accent, r: 2.5 }}
            isAnimationActive={false}
          />
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  );
}
