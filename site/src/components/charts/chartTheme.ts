// Shared chart styling so all 5 blog charts feel like one family.

export const colors = {
  ink: "#0f172a",
  inkSoft: "#334155",
  muted: "#64748b",
  rule: "#e2e8f0",
  axis: "#94a3b8",
  grid: "#e2e8f0",
  bg: "#ffffff",
  bgSoft: "#f8fafc",
  accent: "#3b82f6",
  accent2: "#f97316",
  accent3: "#10b981",
  accent4: "#a855f7",
  badService: "#ef4444",
  okService: "#10b981",
  band: "rgba(59, 130, 246, 0.10)",
};

export const axisStyle = {
  stroke: colors.axis,
  fontSize: 12,
  fontFamily: "Inter, system-ui, sans-serif",
};

export const tickStyle = {
  fill: colors.muted,
  fontSize: 11,
  fontFamily: "Inter, system-ui, sans-serif",
};

export const tooltipStyle = {
  backgroundColor: "#fff",
  border: `1px solid ${colors.rule}`,
  borderRadius: 6,
  padding: "8px 10px",
  fontSize: 12,
  fontFamily: "Inter, system-ui, sans-serif",
  color: colors.ink,
  boxShadow: "0 4px 12px rgba(15, 23, 42, 0.06)",
};

export const legendStyle = {
  fontSize: 12,
  fontFamily: "Inter, system-ui, sans-serif",
  color: colors.inkSoft,
  paddingTop: 14,
};
