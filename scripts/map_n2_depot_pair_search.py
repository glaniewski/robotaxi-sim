"""
Folium map: top **N=2 depot pairs** from ``compute_ranked_depot_pairs`` (origin-access proxy).

Draws the **Exp71 central** pair (top-2 by origin count) plus the **top K** pairs by
trip-weighted mean origin→nearest-depot time. Each pair: two H3 hex outlines, a line
between centroids, and popups with proxy stats.

Also overlays **OSM** polygons (via OSMnx): ``landuse=industrial`` (green) and
``landuse=commercial`` plus ``landuse=retail`` (slate blue, one layer).

Run from repo root:
    python3 scripts/map_n2_depot_pair_search.py

Output: ``scripts/map_n2_depot_pair_search.html`` (then open in a browser).
"""
from __future__ import annotations

import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(ROOT, "backend"))
sys.path.insert(0, SCRIPT_DIR)

import folium
import geopandas as gpd
import h3
import numpy as np
import osmnx as ox
import pandas as pd
from branca.colormap import linear

from industrial_high_od_cells import OSM_BBOX
from search_n2_depot_pairs_origin_access import compute_ranked_depot_pairs

REQUESTS_PATH = os.path.join(ROOT, "data", "requests_austin_h3_r8.parquet")
OUTPUT_HTML = os.path.join(ROOT, "scripts", "map_n2_depot_pair_search.html")

DOWNTOWN_REF = (30.2672, -97.7431)
TOP_MAP_PAIRS = 8  # proxy ranks #1 … #8 as separate layers


def _hex_boundary(cell: str) -> list[tuple[float, float]]:
    return [(lat, lng) for lat, lng in h3.cell_to_boundary(cell)]


def _landuse_polygons(landuse: str) -> gpd.GeoDataFrame:
    """OSM polygons for a single ``landuse`` tag; simplified like Exp70/72."""
    gdf = ox.features_from_bbox(OSM_BBOX, {"landuse": landuse})
    if len(gdf) == 0:
        return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
    gdf = gdf[gdf.geometry.notna()].copy()
    gdf = gdf[gdf.geometry.geom_type.isin(["Polygon", "MultiPolygon"])].copy()
    gdf["geometry"] = gdf.geometry.simplify(0.00012, preserve_topology=True)
    return gdf


def main() -> None:
    res = compute_ranked_depot_pairs(top_m=40, top_k_dest=12)
    baseline = res.baseline
    base_sorted = tuple(sorted(baseline))

    df_o = pd.read_parquet(REQUESTS_PATH, columns=["origin_h3"])
    vc = df_o["origin_h3"].value_counts()
    max_c = int(vc.max())
    demand_cells = set(vc.index.astype(str))

    lats = [h3.cell_to_latlng(c)[0] for c in demand_cells]
    lngs = [h3.cell_to_latlng(c)[1] for c in demand_cells]
    center_lat = float(np.mean(lats))
    center_lng = float(np.mean(lngs))

    m = folium.Map(location=[center_lat, center_lng], zoom_start=11, tiles="CartoDB dark_matter")

    print("Loading OSM landuse (industrial + commercial + retail) …")
    ind_osm = _landuse_polygons("industrial")
    com_osm = _landuse_polygons("commercial")
    ret_osm = _landuse_polygons("retail")
    print(f"  industrial: {len(ind_osm)} polys · commercial: {len(com_osm)} · retail: {len(ret_osm)}")
    com_parts = [g for g in (com_osm, ret_osm) if len(g) > 0]
    if not com_parts:
        commercial_all = gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
    else:
        commercial_all = gpd.GeoDataFrame(
            pd.concat(com_parts, ignore_index=True),
            crs="EPSG:4326",
        )

    ind_layer = folium.FeatureGroup(name="OSM landuse=industrial", show=True)
    if len(ind_osm) > 0:
        folium.GeoJson(
            json.loads(ind_osm.to_json()),
            style_function=lambda _f: {
                "fillColor": "#2E7D32",
                "color": "#81C784",
                "weight": 1,
                "fillOpacity": 0.32,
            },
            highlight_function=lambda _f: {"weight": 2, "fillOpacity": 0.48},
        ).add_to(ind_layer)
    ind_layer.add_to(m)

    com_layer = folium.FeatureGroup(name="OSM landuse=commercial+retail", show=True)
    if len(commercial_all) > 0:
        gj_com = json.loads(commercial_all.to_json())
        folium.GeoJson(
            gj_com,
            style_function=lambda _f: {
                "fillColor": "#3949AB",
                "color": "#9FA8DA",
                "weight": 1,
                "fillOpacity": 0.28,
            },
            highlight_function=lambda _f: {"weight": 2, "fillOpacity": 0.45},
        ).add_to(com_layer)
    com_layer.add_to(m)

    colormap = linear.YlOrRd_09.scale(0, max_c)
    colormap.caption = "Trip origin count"
    colormap.add_to(m)

    heat_fg = folium.FeatureGroup(name="Origin demand (all cells)", show=True)
    for cell, cnt in vc.items():
        cell = str(cell)
        if cnt < max_c * 0.02:
            continue
        folium.Polygon(
            locations=_hex_boundary(cell),
            color=None,
            fill=True,
            fill_color=colormap(int(cnt)),
            fill_opacity=0.12 + 0.35 * (cnt / max_c),
            weight=0,
            tooltip=f"{cell}<br>Origins: {int(cnt):,}",
        ).add_to(heat_fg)
    heat_fg.add_to(m)

    # Baseline central (gold) — always visible
    base_fg = folium.FeatureGroup(name="Exp71 central (top-2 origin count)", show=True)
    for cell in baseline:
        lat, lng = h3.cell_to_latlng(cell)
        folium.Polygon(
            locations=_hex_boundary(cell),
            color="#FFEB3B",
            fill=True,
            fill_color="#FFC107",
            fill_opacity=0.75,
            weight=3,
            tooltip=(
                f"<b>Central depot</b><br>{cell}<br>"
                f"Top-2 by origin volume<br>"
                f"Proxy: mean {res.baseline_mean_min:.2f} min, p90 {res.baseline_p90_min:.2f} min"
            ),
        ).add_to(base_fg)
        folium.CircleMarker(
            location=[lat, lng],
            radius=8,
            color="#FFEB3B",
            fill=True,
            fill_color="#FF8F00",
            fill_opacity=1.0,
            weight=2,
        ).add_to(base_fg)
    la, lb = h3.cell_to_latlng(baseline[0]), h3.cell_to_latlng(baseline[1])
    folium.PolyLine(
        locations=[(la[0], la[1]), (lb[0], lb[1])],
        color="#FFEB3B",
        weight=2,
        dash_array="6 4",
        opacity=0.85,
    ).add_to(base_fg)
    base_fg.add_to(m)

    colors = ["#00BCD4", "#26C6DA", "#4DD0E1", "#80DEEA", "#AB47BC", "#BA68C8", "#CE93D8", "#E1BEE7"]
    mapped = 0
    for rank0, (mean_m, p90_m, a, b) in enumerate(res.results):
        key = (a, b)
        if key == base_sorted:
            continue
        color = colors[mapped % len(colors)]
        fg = folium.FeatureGroup(
            name=f"Proxy #{mapped + 1}  mean={mean_m:.2f}m",
            show=(mapped < 3),
        )
        for cell in (a, b):
            lat, lng = h3.cell_to_latlng(cell)
            folium.Polygon(
                locations=_hex_boundary(cell),
                color="#E0F7FA",
                fill=True,
                fill_color=color,
                fill_opacity=0.55,
                weight=2,
                tooltip=(
                    f"<b>Proxy list #{mapped + 1}</b> (search rank #{rank0 + 1})<br>{cell}<br>"
                    f"Pair: <code>{a}</code><br><code>{b}</code><br>"
                    f"Mean origin→nearest: {mean_m:.3f} min<br>p90: {p90_m:.2f} min"
                ),
            ).add_to(fg)
            folium.CircleMarker(
                location=[lat, lng],
                radius=6,
                color="#FFFFFF",
                fill=True,
                fill_color=color,
                fill_opacity=0.95,
                weight=1,
            ).add_to(fg)
        la, lb = h3.cell_to_latlng(a), h3.cell_to_latlng(b)
        folium.PolyLine(
            locations=[(la[0], la[1]), (lb[0], lb[1])],
            color=color,
            weight=2,
            opacity=0.75,
        ).add_to(fg)
        fg.add_to(m)
        mapped += 1
        if mapped >= TOP_MAP_PAIRS:
            break

    folium.CircleMarker(
        location=list(DOWNTOWN_REF),
        radius=5,
        color="#EEE",
        fill=True,
        fill_color="#333",
        fill_opacity=0.9,
        weight=1,
        tooltip="Downtown ref",
    ).add_to(m)

    best = res.results[0]
    legend = f"""
    <div style="position:fixed; bottom:24px; left:24px; z-index:9999;
         background:rgba(20,20,20,0.93); padding:12px 14px; border-radius:8px;
         color:#eee; font-size:11px; border:1px solid #555; max-width:340px;">
      <b>N=2 depot pairs (origin-access proxy)</b><br><br>
      <span style="color:#FFC107;">■</span> <b>Exp71 central</b> — top-2 by <i>origin count</i><br>
      &nbsp;&nbsp;<code>{baseline[0]}</code><br>
      &nbsp;&nbsp;<code>{baseline[1]}</code><br>
      &nbsp;&nbsp;Proxy rank #{res.baseline_proxy_rank} of {len(res.results)} in top-40×40 search<br><br>
      <span style="color:#4DD0E1;">■</span> <b>Top {TOP_MAP_PAIRS} proxy pairs</b> (toggle layers)<br>
      Best: <code>{best[2]}</code> + <code>{best[3]}</code><br>
      mean {best[0]:.3f} min · p90 {best[1]:.2f} min<br><br>
      <span style="color:#81C784;">■</span> OSM <code>landuse=industrial</code><br>
      <span style="color:#9FA8DA;">■</span> OSM <code>commercial</code> + <code>retail</code><br><br>
      <span style="color:#aaa;">Same candidate rules as
      <code>search_n2_depot_pairs_origin_access.py</code></span>
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend))
    folium.LayerControl(collapsed=False).add_to(m)
    m.save(OUTPUT_HTML)
    print(f"Saved → {OUTPUT_HTML}")


if __name__ == "__main__":
    main()
