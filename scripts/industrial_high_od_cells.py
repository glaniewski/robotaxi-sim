"""
Shared logic: H3 cells whose **centers** lie in OSM ``landuse=industrial``, with a **high
traffic** subset (same rule as ``map_exp72_origins_destinations_industrial.py``).

Used by mapping scripts and ``run_exp71_n2_central_vs_peripheral_depots.py`` (periphery #2).
"""
from __future__ import annotations

import math
import os
from dataclasses import dataclass

import geopandas as gpd
import h3
import osmnx as ox
import pandas as pd
from shapely.geometry import Point

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REQUESTS_PATH = os.path.join(ROOT, "data", "requests_austin_h3_r8.parquet")
TRAVEL_CACHE = os.path.join(ROOT, "data", "h3_travel_cache.parquet")

OSM_BBOX = (-97.92, 30.18, -97.62, 30.42)
DOWNTOWN_REF = (30.2672, -97.7431)

HIGH_INDUSTRIAL_TOP_N = 8
HIGH_OD_RANK_CUTOFF = 500


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(min(1.0, math.sqrt(a)))


def h3_centroid_distance_m(c1: str, c2: str) -> float:
    a = h3.cell_to_latlng(c1)
    b = h3.cell_to_latlng(c2)
    return haversine_m(a[0], a[1], b[0], b[1])


def _rank_map(counts: dict[str, int]) -> dict[str, int]:
    return {c: i + 1 for i, (c, _) in enumerate(sorted(counts.items(), key=lambda x: -x[1]))}


def _load_industrial_polygons() -> gpd.GeoDataFrame:
    gdf = ox.features_from_bbox(OSM_BBOX, {"landuse": "industrial"})
    if len(gdf) == 0:
        return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
    gdf = gdf[gdf.geometry.notna()].copy()
    gdf = gdf[gdf.geometry.geom_type.isin(["Polygon", "MultiPolygon"])].copy()
    gdf["geometry"] = gdf.geometry.simplify(0.00012, preserve_topology=True)
    return gdf


def _industrial_cells_for_points(cells: set[str], ind_gdf: gpd.GeoDataFrame) -> set[str]:
    if len(ind_gdf) == 0 or not cells:
        return set()
    rows = []
    for c in cells:
        lat, lng = h3.cell_to_latlng(c)
        rows.append({"cell": c, "lat": lat, "lng": lng})
    pg = gpd.GeoDataFrame(
        rows,
        geometry=[Point(r["lng"], r["lat"]) for r in rows],
        crs="EPSG:4326",
    )
    ind = ind_gdf[["geometry"]].copy()
    joined = pg.sjoin(ind, how="inner", predicate="within")
    joined = joined.drop_duplicates(subset=["cell"], keep="first")
    return set(joined["cell"].astype(str))


def _best_od_rank(o: int, d: int, ro: int | None, rd: int | None) -> int:
    rd_eff = rd if d > 0 else None
    parts = [x for x in (ro, rd_eff) if x is not None]
    return min(parts) if parts else 999_999


@dataclass(frozen=True)
class IndustrialHighOdContext:
    origin_counts: dict[str, int]
    dest_counts: dict[str, int]
    origin_rank: dict[str, int]
    dest_rank: dict[str, int]
    industrial_cells: frozenset[str]
    high_industrial: frozenset[str]
    top2_origin: tuple[str, str]
    top2_destination: tuple[str, str]
    ind_gdf: gpd.GeoDataFrame


def load_industrial_high_od_context() -> IndustrialHighOdContext:
    """Load parquet, travel cache, OSM industrial polygons, and derived high-traffic subset."""
    df = pd.read_parquet(REQUESTS_PATH, columns=["origin_h3", "destination_h3"])
    origin_counts = df["origin_h3"].value_counts().to_dict()
    dest_counts = df["destination_h3"].value_counts().to_dict()
    origin_rank = _rank_map(origin_counts)
    dest_rank = _rank_map(dest_counts)

    df_cache = pd.read_parquet(TRAVEL_CACHE, columns=["origin_h3", "destination_h3"])
    in_cache = set(df_cache["origin_h3"].astype(str).unique()) | set(
        df_cache["destination_h3"].astype(str).unique()
    )
    feasible = (set(origin_counts) | set(dest_counts)) & in_cache

    ind_gdf = _load_industrial_polygons()
    industrial_cells = _industrial_cells_for_points(feasible, ind_gdf)

    industrial_list: list[dict] = []
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

    high: set[str] = set()
    for row in industrial_list[:HIGH_INDUSTRIAL_TOP_N]:
        high.add(row["cell"])
    for row in industrial_list:
        if _best_od_rank(row["o"], row["d"], row["ro"], row["rd"]) <= HIGH_OD_RANK_CUTOFF:
            high.add(row["cell"])

    t2o = tuple(str(x) for x in df["origin_h3"].value_counts().head(2).index.tolist())
    t2d = tuple(str(x) for x in df["destination_h3"].value_counts().head(2).index.tolist())
    assert len(t2o) == len(t2d) == 2

    return IndustrialHighOdContext(
        origin_counts=origin_counts,
        dest_counts=dest_counts,
        origin_rank=origin_rank,
        dest_rank=dest_rank,
        industrial_cells=frozenset(industrial_cells),
        high_industrial=frozenset(high),
        top2_origin=t2o,
        top2_destination=t2d,
        ind_gdf=ind_gdf,
    )


def closest_centroid_pair_m(cells: frozenset[str] | set[str]) -> tuple[tuple[str, str], float]:
    """Return ((h3_a, h3_b), distance_m) for the pair with minimum centroid separation."""
    cl = sorted(cells)
    if len(cl) < 2:
        raise ValueError("need at least two H3 cells")
    best_d = float("inf")
    best_pair = (cl[0], cl[1])
    for i, a in enumerate(cl):
        for b in cl[i + 1 :]:
            d = h3_centroid_distance_m(a, b)
            pair = (a, b) if a < b else (b, a)
            if d < best_d - 1e-6 or (abs(d - best_d) <= 1e-6 and pair < best_pair):
                best_d = d
                best_pair = pair
    return best_pair, best_d
