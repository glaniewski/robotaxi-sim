import {
  ComposedChart,
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

type FleetPoint = {
  fleet: number;
  contribution_margin_per_trip?: { mean: number | null };
  trips_per_vehicle_per_day?: { mean: number | null };
};

export default function FleetEconomicsCurve({ height = 320 }: { height?: number }) {
  const points = (experiments as any).fleet_sweep?.points as FleetPoint[] | undefined;

  if (!points || points.length === 0) {
    return (
      <div
        style={{ height, background: colors.bgSoft }}
        className="flex items-center justify-center rounded-md border border-slate-200 text-sm text-slate-500"
      >
        Fleet sweep data not available.
      </div>
    );
  }

  const rows = points
    .filter((p) => p.contribution_margin_per_trip?.mean != null && p.trips_per_vehicle_per_day?.mean != null)
    .map((p) => {
      const cm = p.contribution_margin_per_trip!.mean!;
      const tvd = p.trips_per_vehicle_per_day!.mean!;
      const perVehicle = Math.round(cm * tvd);
      const totalK = Math.round((perVehicle * p.fleet) / 1000);
      return { fleet: p.fleet, perVehicle, totalK };
    })
    .sort((a, b) => a.fleet - b.fleet);

  // Find peak total contribution
  const peakRow = rows.reduce((best, r) => (r.totalK > best.totalK ? r : best), rows[0]);

  return (
    <div style={{ width: "100%", height }}>
      <ResponsiveContainer>
        <ComposedChart data={rows} margin={{ top: 20, right: 30, left: 25, bottom: 48 }}>
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
            yAxisId="perVehicle"
            domain={[0, 1200]}
            tickFormatter={(v: number) => `$${v}`}
            tick={tickStyle}
            axisLine={{ stroke: colors.axis }}
            tickLine={{ stroke: colors.axis }}
            label={{
              value: "Contribution / vehicle / day ($)",
              angle: -90,
              position: "insideLeft",
              dx: 10,
              dy: 80,
              style: { ...tickStyle, fill: colors.inkSoft },
            }}
          />
          <YAxis
            yAxisId="total"
            orientation="right"
            domain={[0, 2400]}
            tickFormatter={(v: number) => `$${v}k`}
            tick={tickStyle}
            axisLine={{ stroke: colors.axis }}
            tickLine={{ stroke: colors.axis }}
            label={{
              value: "Total system contribution / day ($k)",
              angle: 90,
              position: "insideRight",
              dx: 18,
              dy: 90,
              style: { ...tickStyle, fill: colors.inkSoft },
            }}
          />
          <Tooltip
            contentStyle={tooltipStyle}
            formatter={(value: number, name: string) => {
              if (name.startsWith("Total")) return [`$${value}k`, name];
              return [`$${value}`, name];
            }}
            labelFormatter={(v: number) => `Fleet = ${v.toLocaleString()} vehicles`}
          />
          <Legend wrapperStyle={legendStyle} />
          <ReferenceLine
            yAxisId="total"
            x={peakRow.fleet}
            stroke={colors.muted}
            strokeDasharray="4 4"
            label={{
              value: `Peak total (fleet=${peakRow.fleet.toLocaleString()})`,
              position: "insideTopRight",
              style: { ...tickStyle, fill: colors.muted, fontSize: 10 },
            }}
          />
          <Line
            yAxisId="perVehicle"
            type="monotone"
            dataKey="perVehicle"
            name="Contribution / vehicle / day"
            stroke={colors.accent}
            strokeWidth={2.4}
            dot={{ fill: colors.accent, r: 3.2 }}
            activeDot={{ r: 5 }}
            isAnimationActive={false}
          />
          <Line
            yAxisId="total"
            type="monotone"
            dataKey="totalK"
            name="Total system contribution / day"
            stroke={colors.accent2}
            strokeWidth={2.4}
            strokeDasharray="6 3"
            dot={{ fill: colors.accent2, r: 3.2 }}
            activeDot={{ r: 5 }}
            isAnimationActive={false}
          />
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  );
}
