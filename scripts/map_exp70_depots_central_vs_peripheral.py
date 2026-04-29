"""
Map Exp70 N=2 centralized depot cells vs proposed peripheral pair.

Industrial context from **OpenStreetMap** via OSMnx: `landuse=industrial` polygons
in an Austin metro bbox, drawn on the map (simplified). Demand H3 (res 8) cells
whose **centers** fall inside any industrial polygon are outlined as
**industrial-tagged demand cells**.

Proposed peripheral depots (feasible = demand ∩ travel cache, center in industrial):
  - **East** of downtown: among industrial∩feasible within **15 km** of downtown,
    pick highest origins with **lng > downtown_lng + 0.01°** (excludes SW bulk).
  - **West**: among same set with **lng < downtown_lng - 0.01°**, pick highest origins.

Style matches `analyze_isolated_cells.py` → `isolated_cells_map.html` (CartoDB dark,
YlOrRd demand hexes).

Requires: `osmnx`, `geopandas`, `folium` (see `backend/requirements.txt` + folium).

Run from repo root:
    python3 scripts/map_exp70_depots_central_vs_peripheral.py
"""
from __future__ import annotations

import json
import math
import os
import sys
from typing import Any

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "backend"))

import geopandas as gpd
import h3
import numpy as np
import osmnx as ox
import pandas as pd
import folium
from branca.colormap import linear
from shapely.geometry import Point

REQUESTS_PATH = os.path.join(ROOT, "data", "requests_austin_h3_r8.parquet")
TRAVEL_CACHE = os.path.join(ROOT, "data", "h3_travel_cache.parquet")
OUTPUT_MAP = os.path.join(ROOT, "scripts", "exp70_depots_central_vs_peripheral_map.html")

# Austin metro bbox for OSM pull (west, south, east, north) — OSMnx 2.x tuple order
OSM_BBOX = (-97.92, 30.18, -97.62, 30.42)

DOWNTOWN_REF = (30.2672, -97.7431)
DOWNTOWN_LNG = DOWNTOWN_REF[1]
MERIDIAN_EPS = 0.01  # degrees (~1 km) — separates E vs W of downtown for depot picks
# Prefer industrial-feasible cells **near** downtown (SE / NW pockets); widen if empty.
MAX_DIST_DEPOT_PICK_M = 10_000.0
MAX_DIST_DEPOT_FALLBACK_M = 15_000.0


def _h3_hex_boundary(h3_cell: str) -> list[tuple[float, float]]:
    boundary = h3.cell_to_boundary(h3_cell)
    return [(lat, lng) for lat, lng in boundary]


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(min(1.0, math.sqrt(a)))


def _load_industrial_polygons() -> gpd.GeoDataFrame:
    """OSM landuse=industrial multipolygons/polygons (WGS84)."""
    gdf = ox.features_from_bbox(OSM_BBOX, {"landuse": "industrial"})
    if len(gdf) == 0:
        return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
    gdf = gdf[gdf.geometry.notna()].copy()
    # Drop rows with invalid/non-polygon geometries
    gdf = gdf[gdf.geometry.geom_type.isin(["Polygon", "MultiPolygon"])].copy()
    gdf["geometry"] = gdf.geometry.simplify(0.00012, preserve_topology=True)
    return gdf


def _industrial_feasible_cells(
    origin_counts: dict[str, int],
    feasible: set[str],
    ind_gdf: gpd.GeoDataFrame,
) -> tuple[gpd.GeoDataFrame, set[str]]:
    """Demand cells (centers) inside industrial OSM polygons; return joined gdf + cell set."""
    if len(ind_gdf) == 0:
        return gpd.GeoDataFrame(), set()

    rows = []
    for c in feasible:
        lat, lng = h3.cell_to_latlng(c)
        rows.append({"cell": c, "lat": lat, "lng": lng, "n": origin_counts.get(c, 0)})
    pg = gpd.GeoDataFrame(
        rows,
        geometry=[Point(r["lng"], r["lat"]) for r in rows],
        crs="EPSG:4326",
    )
    ind = ind_gdf[["geometry"]].copy()
    joined = pg.sjoin(ind, how="inner", predicate="within")
    joined = joined.drop_duplicates(subset=["cell"], keep="first")
    cells = set(joined["cell"].astype(str))
    return joined, cells


def _pick_peripheral_osm_based(
    joined: gpd.GeoDataFrame,
) -> tuple[tuple[str, float, float, float, int], tuple[str, float, float, float, int]]:
    """Return ((east_cell, d_m, lat, lng, n), (west_cell, ...)) from industrial∩feasible."""
    if len(joined) == 0:
        raise RuntimeError("No demand cells with centers inside OSM industrial landuse.")

    dt_lat, dt_lng = DOWNTOWN_REF
    j = joined.copy()
    j["d_m"] = j.apply(
        lambda r: _haversine_m(dt_lat, dt_lng, float(r["lat"]), float(r["lng"])),
        axis=1,
    )

    def _pool(max_m: float) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
        sub = j[j["d_m"] <= max_m]
        east_p = sub[sub["lng"] > DOWNTOWN_LNG + MERIDIAN_EPS]
        west_p = sub[sub["lng"] < DOWNTOWN_LNG - MERIDIAN_EPS]
        return east_p, west_p

    east_pool, west_pool = _pool(MAX_DIST_DEPOT_PICK_M)
    if len(east_pool) == 0 or len(west_pool) == 0:
        east_pool, west_pool = _pool(MAX_DIST_DEPOT_FALLBACK_M)
    if len(east_pool) == 0 or len(west_pool) == 0:
        raise RuntimeError(
            f"Need both E and W industrial-feasible cells (tried "
            f"{MAX_DIST_DEPOT_PICK_M/1000:.0f} then {MAX_DIST_DEPOT_FALLBACK_M/1000:.0f} km); "
            f"east={len(east_pool)} west={len(west_pool)}"
        )

    e_row = east_pool.loc[east_pool["n"].idxmax()]
    w_row = west_pool.loc[west_pool["n"].idxmax()]
    if str(e_row["cell"]) == str(w_row["cell"]):
        w_row = west_pool.nlargest(2, "n").iloc[1]

    def pack(row: Any) -> tuple[str, float, float, float, int]:
        c = str(row["cell"])
        lat, lng = float(row["lat"]), float(row["lng"])
        d = float(row["d_m"])
        n = int(row["n"])
        return c, d, lat, lng, n

    return pack(e_row), pack(w_row)


def main() -> None:
    print("Loading OSM industrial (landuse=industrial) …")
    ind_gdf = _load_industrial_polygons()
    print(f"  {len(ind_gdf)} industrial polygons (simplified)")

    df_req = pd.read_parquet(REQUESTS_PATH, columns=["origin_h3"])
    origin_counts = df_req["origin_h3"].value_counts().to_dict()
    demand_cell_set = set(origin_counts.keys())
    cell_ranks = {c: i + 1 for i, (c, _) in enumerate(sorted(origin_counts.items(), key=lambda x: -x[1]))}

    df_cache = pd.read_parquet(
        TRAVEL_CACHE, columns=["origin_h3", "destination_h3"]
    )
    in_cache = set(df_cache["origin_h3"].astype(str).unique()) | set(
        df_cache["destination_h3"].astype(str).unique()
    )
    feasible = demand_cell_set & in_cache

    joined_ind, industrial_cells = _industrial_feasible_cells(
        origin_counts, feasible, ind_gdf
    )
    print(f"  {len(industrial_cells)} demand cells with center inside industrial OSM")

    top2 = df_req["origin_h3"].value_counts().head(2).index.tolist()
    central_cells = [str(x) for x in top2]

    east_tup, west_tup = _pick_peripheral_osm_based(joined_ind)
    east_cell, east_d, east_lat, east_lng, east_n = east_tup
    west_cell, west_d, west_lat, west_lng, west_n = west_tup

    lats = [h3.cell_to_latlng(c)[0] for c in demand_cell_set]
    lngs = [h3.cell_to_latlng(c)[1] for c in demand_cell_set]
    center_lat = float(np.mean(lats))
    center_lng = float(np.mean(lngs))

    m = folium.Map(location=[center_lat, center_lng], zoom_start=11, tiles="CartoDB dark_matter")

    max_count = max(origin_counts.values())
    colormap = linear.YlOrRd_09.scale(0, max_count)
    colormap.caption = "Trip origin count (historical)"
    colormap.add_to(m)

    highlight = set(central_cells + [east_cell, west_cell])

    demand_layer = folium.FeatureGroup(name="Demand heatmap", show=True)
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
                f"Cell: {cell}<br>Origins: {count:,}<br>Rank: #{cell_ranks.get(cell, '?')}"
            ),
        ).add_to(demand_layer)
    demand_layer.add_to(m)

    # OSM industrial polygons (GeoJSON)
    ind_layer = folium.FeatureGroup(name="OSM landuse=industrial", show=True)
    if len(ind_gdf) > 0:
        gj = json.loads(ind_gdf.to_json())
        folium.GeoJson(
            gj,
            style_function=lambda _f: {
                "fillColor": "#2E7D32",
                "color": "#81C784",
                "weight": 1,
                "fillOpacity": 0.35,
            },
            highlight_function=lambda _f: {"weight": 2, "fillOpacity": 0.55},
            name="Industrial OSM",
        ).add_to(ind_layer)
    ind_layer.add_to(m)

    # H3 cells whose centers OSM classifies as industrial (subset of demand)
    ind_hex_layer = folium.FeatureGroup(
        name="Demand H3 in industrial (center ∈ OSM polygon)", show=True
    )
    for cell in sorted(industrial_cells):
        boundary = _h3_hex_boundary(cell)
        lat, lng = h3.cell_to_latlng(cell)
        n = origin_counts.get(cell, 0)
        rk = cell_ranks.get(cell, "?")
        folium.Polygon(
            locations=boundary,
            color="#B2FF59",
            fill=True,
            fill_color="#1B5E20",
            fill_opacity=0.25,
            weight=2,
            tooltip=(
                f"<b>Industrial-tagged demand cell</b><br>H3: {cell}<br>"
                f"Center inside OSM <code>landuse=industrial</code><br>"
                f"Origins: {n:,} (rank #{rk})"
            ),
        ).add_to(ind_hex_layer)
        folium.CircleMarker(
            location=[lat, lng],
            radius=4,
            color="#B2FF59",
            fill=True,
            fill_color="#1B5E20",
            fill_opacity=0.9,
            weight=1,
        ).add_to(ind_hex_layer)
    ind_hex_layer.add_to(m)

    central_layer = folium.FeatureGroup(name="Exp70 N=2 depots (top origins)", show=True)
    for cell in central_cells:
        boundary = _h3_hex_boundary(cell)
        lat, lng = h3.cell_to_latlng(cell)
        n = origin_counts[cell]
        rk = cell_ranks[cell]
        folium.Polygon(
            locations=boundary,
            color="#FFFFFF",
            fill=True,
            fill_color="#00CED1",
            fill_opacity=0.82,
            weight=3,
            tooltip=(
                f"<b>Current centralized depot</b><br>Cell: {cell}<br>"
                f"Origin rank #{rk}<br>Origins: {n:,}<br>"
                f"(top_demand_cells(2) — same as Exp70)"
            ),
        ).add_to(central_layer)
        folium.CircleMarker(
            location=[lat, lng],
            radius=7,
            color="#FFFFFF",
            fill=True,
            fill_color="#00CED1",
            fill_opacity=1.0,
            weight=2,
        ).add_to(central_layer)
    central_layer.add_to(m)

    prop_layer = folium.FeatureGroup(
        name="Proposed peripheral (OSM-industrial E / W)", show=True
    )
    for cell, lat, lng, n, d_m, side, color in [
        (east_cell, east_lat, east_lng, east_n, east_d, "East", "#FF8C00"),
        (west_cell, west_lat, west_lng, west_n, west_d, "West", "#BA68C8"),
    ]:
        boundary = _h3_hex_boundary(cell)
        rk = cell_ranks.get(cell, "?")
        folium.Polygon(
            locations=boundary,
            color="#FFEB3B",
            fill=True,
            fill_color=color,
            fill_opacity=0.78,
            weight=3,
            tooltip=(
                f"<b>Proposed {side} depot (OSM-based)</b><br>Cell: {cell}<br>"
                f"Origins: {n:,} (rank #{rk})<br>"
                f"Center in OSM industrial; {d_m/1000:.1f} km from downtown ref<br>"
                f"Feasible (demand ∩ travel cache)"
            ),
        ).add_to(prop_layer)
        folium.CircleMarker(
            location=[lat, lng],
            radius=7,
            color="#FFEB3B",
            fill=True,
            fill_color=color,
            fill_opacity=1.0,
            weight=2,
        ).add_to(prop_layer)
    prop_layer.add_to(m)

    folium.CircleMarker(
        location=list(DOWNTOWN_REF),
        radius=5,
        color="#EEEEEE",
        fill=True,
        fill_color="#333333",
        fill_opacity=0.95,
        weight=1,
        tooltip="Downtown reference point (approx.)",
    ).add_to(m)

    legend_html = f"""
    <div style="position:fixed; bottom:30px; left:30px; z-index:9999;
                background:rgba(30,30,30,0.92); padding:12px 16px;
                border-radius:8px; color:white; font-size:11px;
                border:1px solid #555; max-width:340px;">
      <b>Exp70 depot geography + OSM industrial</b><br><br>
      <span style="color:#81C784;">■</span> <b>OSM</b> <code>landuse=industrial</code> (polygons)<br>
      <span style="color:#B2FF59;">■</span> <b>Demand H3</b> whose <b>center</b> lies in those polygons<br><br>
      <span style="color:#00CED1;">■</span> <b>Current N=2</b> top origin cells<br>
      &nbsp;&nbsp;<code>{central_cells[0]}</code> (#1)<br>
      &nbsp;&nbsp;<code>{central_cells[1]}</code> (#2)<br><br>
      <span style="color:#FF8C00;">■</span> <b>Proposed east</b> <code>{east_cell}</code><br>
      &nbsp;&nbsp;max origins east of downtown (±{MERIDIAN_EPS}°), ≤{MAX_DIST_DEPOT_PICK_M/1000:.0f} km
      (fallback {MAX_DIST_DEPOT_FALLBACK_M/1000:.0f} km)<br><br>
      <span style="color:#BA68C8;">■</span> <b>Proposed west</b> <code>{west_cell}</code><br>
      &nbsp;&nbsp;max origins west of downtown (±{MERIDIAN_EPS}°), same distance rule<br><br>
      <span style="color:#eee;">●</span> Downtown ref<br><br>
      <span style="color:#aaa;">OSM via OSMnx bbox {OSM_BBOX}; style like
      <code>isolated_cells_map.html</code></span>
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))

    folium.LayerControl(collapsed=False).add_to(m)
    m.save(OUTPUT_MAP)

    print("\nExp70 N=2 centralized (top_demand_cells(2)):")
    for c in central_cells:
        lat, lng = h3.cell_to_latlng(c)
        print(f"  {c}  rank #{cell_ranks[c]}  origins={origin_counts[c]:,}  ({lat:.5f}, {lng:.5f})")
    print("\nProposed peripheral (OSM industrial ∩ feasible, E/W of downtown):")
    print(f"  EAST  {east_cell}  {east_n:,} origins  {east_d/1000:.1f} km from downtown")
    print(f"  WEST  {west_cell}  {west_n:,} origins  {west_d/1000:.1f} km from downtown")
    print(f"\nMap saved → {OUTPUT_MAP}")


if __name__ == "__main__":
    main()
