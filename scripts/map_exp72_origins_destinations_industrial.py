"""
Map **top origin vs top destination** H3 cells (Exp72) with **OSM industrial** context.

Answers geographically:
- Where the **top-2 destination** cells sit vs downtown (reference point + km).
- Which demand cells have **high origin or destination counts** and **cell center ∈ OSM
  landuse=industrial** (same rule as ``map_exp70_depots_central_vs_peripheral.py``).
  These are drawn in a dedicated **★ High O or D ∩ industrial** layer (top ``N`` by O+D
  among industrial centers, or best single-leg rank ≤ ``R``; see constants below).

Output: ``scripts/exp72_origins_destinations_industrial_map.html``

Requires: osmnx, geopandas, folium (see ``backend/requirements.txt``).

Run from repo root:
    python3 scripts/map_exp72_origins_destinations_industrial.py
"""
from __future__ import annotations

import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(ROOT, "backend"))
sys.path.insert(0, SCRIPT_DIR)

import h3
import numpy as np
import folium
from branca.colormap import linear

from industrial_high_od_cells import (
    DOWNTOWN_REF,
    HIGH_INDUSTRIAL_TOP_N,
    HIGH_OD_RANK_CUTOFF,
    haversine_m,
    load_industrial_high_od_context,
)

OUTPUT_MAP = os.path.join(ROOT, "scripts", "exp72_origins_destinations_industrial_map.html")


def _h3_hex_boundary(h3_cell: str) -> list[tuple[float, float]]:
    boundary = h3.cell_to_boundary(h3_cell)
    return [(lat, lng) for lat, lng in boundary]


def main() -> None:
    print("Loading requests, travel cache, OSM industrial …")
    ctx = load_industrial_high_od_context()
    origin_counts = ctx.origin_counts
    dest_counts = ctx.dest_counts
    origin_rank = ctx.origin_rank
    dest_rank = ctx.dest_rank
    industrial_cells = set(ctx.industrial_cells)
    high_industrial = set(ctx.high_industrial)
    other_industrial = industrial_cells - high_industrial
    ind_gdf = ctx.ind_gdf
    top2_o = list(ctx.top2_origin)
    top2_d = list(ctx.top2_destination)

    print(f"  {len(ind_gdf)} industrial polygons")
    print(f"  {len(industrial_cells)} feasible H3 cells with center inside industrial OSM")

    dt_lat, dt_lng = DOWNTOWN_REF
    print("\nTop-2 **destination** cells vs downtown ref (lat, lng, distance):")
    for i, c in enumerate(top2_d, start=1):
        lat, lng = h3.cell_to_latlng(c)
        d_m = haversine_m(dt_lat, dt_lng, lat, lng)
        print(
            f"    dest #{i}: {c}  n={dest_counts[c]:,}  "
            f"origin_rank #{origin_rank.get(c, '—')}  "
            f"{d_m/1000:.2f} km from downtown"
        )

    industrial_list = []
    for c in industrial_cells:
        o = int(origin_counts.get(c, 0))
        d = int(dest_counts.get(c, 0))
        industrial_list.append(
            {
                "cell": c,
                "o": o,
                "d": d,
                "od": o + d,
                "ro": origin_rank.get(c) if o > 0 else None,
                "rd": dest_rank.get(c) if d > 0 else None,
            }
        )
    industrial_list.sort(key=lambda x: -x["od"])

    def _best_od_rank(row: dict) -> int:
        ro = row.get("ro")
        rd = row.get("rd") if row.get("d", 0) > 0 else None
        parts = [x for x in (ro, rd) if x is not None]
        return min(parts) if parts else 999_999

    print("\n**High O/D ∩ industrial** (map emphasis): top by O+D and/or best rank ≤500:")
    for row in sorted(
        (r for r in industrial_list if r["cell"] in high_industrial),
        key=lambda r: -r["od"],
    ):
        rd = row["rd"]
        rd_s = f"#{rd}" if rd is not None else "—"
        ro_s = f"#{row['ro']}" if row["ro"] is not None else "—"
        br = _best_od_rank(row)
        print(
            f"  {row['cell']}  O={row['o']:,} ({ro_s})  "
            f"D={row['d']:,} ({rd_s})  O+D={row['od']:,}  best_rank={br}"
        )

    print("\nAll industrial-tagged cells by O+D (reference):")
    for row in industrial_list[:15]:
        tag = " ★ high" if row["cell"] in high_industrial else ""
        rd = row["rd"]
        rd_s = f"#{rd}" if rd is not None else "—"
        ro_s = f"#{row['ro']}" if row["ro"] is not None else "—"
        print(
            f"  {row['cell']}  O={row['o']:,} ({ro_s})  "
            f"D={row['d']:,} ({rd_s})  O+D={row['od']:,}{tag}"
        )

    demand_cell_set = set(origin_counts.keys())
    max_count = max(origin_counts.values())
    lats = [h3.cell_to_latlng(c)[0] for c in demand_cell_set]
    lngs = [h3.cell_to_latlng(c)[1] for c in demand_cell_set]
    center_lat = float(np.mean(lats))
    center_lng = float(np.mean(lngs))

    # Leave low-traffic industrial on the origin heatmap; pull out high overlap explicitly.
    highlight = set(top2_o + top2_d + list(high_industrial))

    m = folium.Map(location=[center_lat, center_lng], zoom_start=11, tiles="CartoDB dark_matter")
    colormap = linear.YlOrRd_09.scale(0, max_count)
    colormap.caption = "Trip origin count (historical)"
    colormap.add_to(m)

    demand_layer = folium.FeatureGroup(name="Demand heatmap (origins)", show=True)
    for cell, count in origin_counts.items():
        if cell in highlight:
            continue
        boundary = _h3_hex_boundary(cell)
        opacity = 0.15 + 0.55 * (count / max_count)
        folium.Polygon(
            locations=boundary,
            color=None,
            fill=True,
            fill_color=colormap(count),
            fill_opacity=opacity,
            weight=0,
            tooltip=(
                f"Cell: {cell}<br>Origins: {count:,} (#{origin_rank[cell]})<br>"
                f"Destinations: {dest_counts.get(cell, 0):,}"
            ),
        ).add_to(demand_layer)
    demand_layer.add_to(m)

    ind_layer = folium.FeatureGroup(name="OSM landuse=industrial", show=True)
    if len(ind_gdf) > 0:
        gj = json.loads(ind_gdf.to_json())
        folium.GeoJson(
            gj,
            style_function=lambda _f: {
                "fillColor": "#2E7D32",
                "color": "#81C784",
                "weight": 1,
                "fillOpacity": 0.32,
            },
            highlight_function=lambda _f: {"weight": 2, "fillOpacity": 0.5},
        ).add_to(ind_layer)
    ind_layer.add_to(m)

    ind_other_layer = folium.FeatureGroup(
        name="Industrial-tagged: other (lower O+D)", show=False
    )
    for cell in sorted(other_industrial):
        boundary = _h3_hex_boundary(cell)
        lat, lng = h3.cell_to_latlng(cell)
        o = origin_counts.get(cell, 0)
        d = dest_counts.get(cell, 0)
        ro = origin_rank.get(cell, "?")
        rd = dest_rank.get(cell, "?")
        folium.Polygon(
            locations=boundary,
            color="#AED581",
            fill=True,
            fill_color="#33691E",
            fill_opacity=0.12,
            weight=1,
            dash_array="4 3",
            tooltip=(
                f"<b>Industrial (lower O+D)</b><br>H3: {cell}<br>"
                f"Origins: {o:,} (rank #{ro})<br>"
                f"Destinations: {d:,} (rank #{rd})<br>"
                f"O+D: {o+d:,}"
            ),
        ).add_to(ind_other_layer)
    ind_other_layer.add_to(m)

    orig_layer = folium.FeatureGroup(name="Top-2 origin cells (Exp72)", show=True)
    for cell in top2_o:
        boundary = _h3_hex_boundary(cell)
        lat, lng = h3.cell_to_latlng(cell)
        o = origin_counts[cell]
        d = dest_counts.get(cell, 0)
        folium.Polygon(
            locations=boundary,
            color="#E0FFFF",
            fill=True,
            fill_color="#00CED1",
            fill_opacity=0.8,
            weight=3,
            tooltip=(
                f"<b>Top origin #{origin_rank[cell]}</b><br>{cell}<br>"
                f"Origins: {o:,}<br>Destinations: {d:,} (#{dest_rank.get(cell, '?')})"
            ),
        ).add_to(orig_layer)
        folium.CircleMarker(
            location=[lat, lng],
            radius=8,
            color="#FFFFFF",
            fill=True,
            fill_color="#00CED1",
            fill_opacity=1.0,
            weight=2,
        ).add_to(orig_layer)
    orig_layer.add_to(m)

    dest_layer = folium.FeatureGroup(name="Top-2 destination cells (Exp72)", show=True)
    for cell in top2_d:
        boundary = _h3_hex_boundary(cell)
        lat, lng = h3.cell_to_latlng(cell)
        o = origin_counts.get(cell, 0)
        d = dest_counts[cell]
        d_km = haversine_m(dt_lat, dt_lng, lat, lng) / 1000.0
        folium.Polygon(
            locations=boundary,
            color="#FFECB3",
            fill=True,
            fill_color="#FF7043",
            fill_opacity=0.78,
            weight=3,
            tooltip=(
                f"<b>Top destination #{dest_rank[cell]}</b><br>{cell}<br>"
                f"Destinations: {d:,}<br>Origins: {o:,} (#{origin_rank.get(cell, '?')})<br>"
                f"~{d_km:.2f} km from downtown ref"
            ),
        ).add_to(dest_layer)
        folium.CircleMarker(
            location=[lat, lng],
            radius=8,
            color="#FFECB3",
            fill=True,
            fill_color="#FF7043",
            fill_opacity=1.0,
            weight=2,
        ).add_to(dest_layer)
    dest_layer.add_to(m)

    # Draw last so high-traffic industrial hexes sit above top-O/D overlays when nearby.
    ind_high_layer = folium.FeatureGroup(
        name="★ High O or D ∩ industrial (emphasis)", show=True
    )
    for cell in sorted(high_industrial):
        boundary = _h3_hex_boundary(cell)
        lat, lng = h3.cell_to_latlng(cell)
        o = origin_counts.get(cell, 0)
        d = dest_counts.get(cell, 0)
        ro_i = origin_rank.get(cell) if o > 0 else None
        rd_i = dest_rank.get(cell) if d > 0 else None
        ro_s = f"#{ro_i}" if ro_i is not None else "—"
        rd_s = f"#{rd_i}" if rd_i is not None else "—"
        br = _best_od_rank({"o": o, "d": d, "ro": ro_i, "rd": rd_i})
        folium.Polygon(
            locations=boundary,
            color="#FFEA00",
            fill=True,
            fill_color="#E040FB",
            fill_opacity=0.52,
            weight=4,
            tooltip=(
                f"<b>High demand ∩ industrial OSM</b><br>H3: {cell}<br>"
                f"<b>Origins:</b> {o:,} (rank {ro_s})<br>"
                f"<b>Destinations:</b> {d:,} (rank {rd_s})<br>"
                f"O+D: {o+d:,}<br>"
                f"<i>Best single-leg rank: #{br}</i><br>"
                f"<small>Center ∈ OSM landuse=industrial</small>"
            ),
        ).add_to(ind_high_layer)
        folium.CircleMarker(
            location=[lat, lng],
            radius=11,
            color="#FFEA00",
            fill=True,
            fill_color="#AB47BC",
            fill_opacity=0.95,
            weight=3,
            tooltip=(
                f"<b>{cell}</b><br>O+D {o+d:,} · best rank #{br}"
            ),
        ).add_to(ind_high_layer)
    ind_high_layer.add_to(m)

    folium.CircleMarker(
        location=list(DOWNTOWN_REF),
        radius=6,
        color="#EEEEEE",
        fill=True,
        fill_color="#212121",
        fill_opacity=0.95,
        weight=2,
        tooltip="Downtown Austin ref (~City Hall / core)",
    ).add_to(m)

    o_lines = "<br>".join(
        f"&nbsp;&nbsp;<code>{c}</code> origin #{origin_rank[c]} "
        f"({origin_counts[c]:,} O, {dest_counts.get(c, 0):,} D)"
        for c in top2_o
    )
    d_lines = "<br>".join(
        f"&nbsp;&nbsp;<code>{c}</code> dest #{dest_rank[c]} "
        f"({dest_counts[c]:,} D, {origin_counts.get(c, 0):,} O)"
        for c in top2_d
    )
    legend_html = f"""
    <div style="position:fixed; bottom:26px; left:26px; z-index:9999;
                background:rgba(28,28,28,0.94); padding:12px 16px;
                border-radius:8px; color:#f5f5f5; font-size:11px;
                border:1px solid #555; max-width:360px;">
      <b>Exp72 — origins vs destinations + industrial (OSM)</b><br><br>
      <span style="color:#00CED1;">■</span> <b>Top-2 origins</b> (trip starts)<br>{o_lines}<br><br>
      <span style="color:#FF7043;">■</span> <b>Top-2 destinations</b> (trip ends)<br>{d_lines}<br><br>
      Both pairs sit in the <b>dense central hex cluster</b> (typically
      <b>&lt; ~2–3 km</b> from the downtown ref — see hex tooltips for km).<br><br>
      <span style="color:#81C784;">■</span> OSM <code>landuse=industrial</code> polygons<br>
      <span style="color:#E040FB;">■</span> <b>High O or D ∩ industrial</b> — top {HIGH_INDUSTRIAL_TOP_N} by O+D among
      industrial centers, <i>or</i> best O/D rank ≤{HIGH_OD_RANK_CUTOFF} (bright hex + purple fill).<br>
      <span style="color:#AED581;">▭</span> Other industrial-tagged cells (dashed outline, off by default in layer control).<br><br>
      <span style="color:#bdbdbd;">●</span> Downtown reference
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))
    folium.LayerControl(collapsed=False).add_to(m)
    m.save(OUTPUT_MAP)
    print(f"\nMap saved → {OUTPUT_MAP}")


if __name__ == "__main__":
    main()
