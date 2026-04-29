"""
Generate the blog map showing depot locations for the four equal-power geography
configs: N=2, N=5, N=20, and N=77.

Run from repo root:
    python3 scripts/map_blog_depot_configs.py
"""
from __future__ import annotations

from pathlib import Path

import folium
import h3
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
REQUESTS_PATH = ROOT / "data" / "requests_austin_h3_r8.parquet"
OUTPUT_MAP = ROOT / "site" / "public" / "sim" / "blog_depot_configs_map.html"

CONFIGS = (
    (2,  "2 mega depots",    "#111827", 6, True),   # black,  on by default
    (5,  "5 large depots",   "#2563EB", 6, False),  # blue
    (20, "20 medium depots", "#8B5CF6", 6, False),  # purple
    (77, "77 small depots",  "#14B8A6", 6, False),  # teal
)


def _h3_hex_boundary(cell: str) -> list[tuple[float, float]]:
    return [(lat, lng) for lat, lng in h3.cell_to_boundary(cell)]


def main() -> None:
    df = pd.read_parquet(REQUESTS_PATH, columns=["origin_h3"])
    origin_counts = df["origin_h3"].astype(str).value_counts()
    ranked_cells = origin_counts.index.tolist()

    lats_lngs = [h3.cell_to_latlng(c) for c in ranked_cells]
    center_lat = sum(lat for lat, _ in lats_lngs) / len(lats_lngs)
    center_lng = sum(lng for _, lng in lats_lngs) / len(lats_lngs)

    m = folium.Map(
        location=[center_lat, center_lng],
        zoom_start=11,
        min_zoom=10,
        tiles="CartoDB positron",
    )

    HEATMAP_COLOR = "#F97316"

    # 5 quantile tiers — discrete steps make hotspots immediately obvious
    # instead of the log-scale compression that makes all cells look similar.
    counts_arr = origin_counts.values.astype(float)
    breakpoints = np.percentile(counts_arr, [50, 80, 95, 99])
    TIERS: list[tuple[float, float, str]] = [
        (breakpoints[3], 0.82, "top 1%"),
        (breakpoints[2], 0.60, "top 5%"),
        (breakpoints[1], 0.38, "top 20%"),
        (breakpoints[0], 0.18, "top 50%"),
        (0,              0.07, "bottom 50%"),
    ]

    def _opacity(count: int) -> float:
        for threshold, op, _ in TIERS:
            if count >= threshold:
                return op
        return TIERS[-1][1]

    demand_layer = folium.FeatureGroup(name="Trip-origin density", show=True)
    for rank, (cell, count) in enumerate(origin_counts.items(), start=1):
        folium.Polygon(
            locations=_h3_hex_boundary(cell),
            color=None,
            fill=True,
            fill_color=HEATMAP_COLOR,
            fill_opacity=_opacity(int(count)),
            weight=0,
            tooltip=f"Origin H3: {cell}<br>Trips: {int(count):,}<br>Rank: #{rank}",
        ).add_to(demand_layer)
    demand_layer.add_to(m)

    selected: set[str] = set()
    for n_sites, label, color, radius, show_default in CONFIGS:
        selected.update(ranked_cells[:n_sites])
        layer = folium.FeatureGroup(name=label, show=show_default)
        for rank, cell in enumerate(ranked_cells[:n_sites], start=1):
            lat, lng = h3.cell_to_latlng(cell)
            count = int(origin_counts[cell])
            folium.Polygon(
                locations=_h3_hex_boundary(cell),
                color=color,
                fill=False,
                weight=2.5,
                opacity=0.9,
                tooltip=(
                    f"<b>{label}</b><br>"
                    f"Depot rank: #{rank} of {n_sites}<br>"
                    f"Origin trips in cell: {count:,}<br>"
                    f"H3: {cell}"
                ),
            ).add_to(layer)
            folium.CircleMarker(
                location=[lat, lng],
                radius=radius,
                color="#111827",
                weight=1,
                fill=True,
                fill_color=color,
                fill_opacity=0.92,
                tooltip=f"{label}: depot #{rank}<br>{count:,} historical origins",
            ).add_to(layer)
        layer.add_to(m)

    # Build tier rows with actual trip-count thresholds for the legend.
    bp = [int(b) for b in breakpoints]  # [p50, p80, p95, p99]
    tier_rows = [
        (TIERS[0][1], f"≥ {bp[3]:,} trips &nbsp;(top 1%)"),
        (TIERS[1][1], f"{bp[2]:,} – {bp[3]:,} trips &nbsp;(top 5%)"),
        (TIERS[2][1], f"{bp[1]:,} – {bp[2]:,} trips &nbsp;(top 20%)"),
        (TIERS[3][1], f"{bp[0]:,} – {bp[1]:,} trips &nbsp;(top 50%)"),
        (TIERS[4][1], f"< {bp[0]:,} trips &nbsp;(bottom 50%)"),
    ]
    tier_html = "".join(
        f'<div style="display:flex;align-items:center;gap:7px;margin:3px 0;">'
        f'<span style="display:inline-block;width:14px;height:14px;border-radius:2px;'
        f'background:rgba(249,115,22,{op});flex-shrink:0;"></span>'
        f'<span>{label}</span></div>'
        for op, label in tier_rows
    )
    legend_html = f"""
    <div style="position:fixed; bottom:26px; left:26px; z-index:9999;
                background:rgba(255,255,255,0.94); padding:12px 14px;
                border-radius:10px; color:#111827; font-size:12px;
                border:1px solid #D1D5DB; max-width:340px;
                box-shadow:0 8px 30px rgba(15,23,42,0.14);">
      <b>Depot geography configs</b><br>
      Equal total installed power, different spatial distribution.<br><br>
      <span style="color:#111827;">●</span> 2 mega depots<br>
      <span style="color:#2563EB;">●</span> 5 large depots<br>
      <span style="color:#8B5CF6;">●</span> 20 medium depots<br>
      <span style="color:#14B8A6;">●</span> 77 small depots<br><br>
      <b style="font-size:11px;text-transform:uppercase;letter-spacing:0.04em;color:#6B7280;">
        Trip-origin density
      </b><br>
      {tier_html}
      <div style="margin-top:5px;color:#6B7280;font-size:11px;">
        Depot cells ranked by historical trip-origin volume.<br>
        Use the layer control to toggle configs.
      </div>
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))
    m.get_root().html.add_child(folium.Element(
        "<style>.leaflet-tile-pane { filter: grayscale(100%); }</style>"
    ))
    folium.LayerControl(collapsed=False).add_to(m)

    OUTPUT_MAP.parent.mkdir(parents=True, exist_ok=True)
    m.save(OUTPUT_MAP)
    print(f"Map saved -> {OUTPUT_MAP}")


if __name__ == "__main__":
    main()
