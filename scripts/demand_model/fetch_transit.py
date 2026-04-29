"""
Compute a transit access score per H3 cell using transit station locations
from Overture Maps (via DuckDB / S3 Parquet).

Score = normalized weighted station count within k=1 H3 ring.
Weights: train/light_rail > bus_station > generic transit.
Cells with no nearby transit get score 0. Max score is 1.0.

Cached artifact: data/transit_access_h3.parquet
"""
from __future__ import annotations

import logging
from pathlib import Path

import h3
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

H3_RES = 8
_ROOT = Path(__file__).resolve().parents[2]
CACHE_PATH = _ROOT / "data" / "transit_access_h3.parquet"

LAT_MIN, LAT_MAX = 30.0, 30.7
LNG_MIN, LNG_MAX = -98.2, -97.4

_OVERTURE_RELEASE = "2026-03-18.0"
_OVERTURE_S3 = f"s3://overturemaps-us-west-2/release/{_OVERTURE_RELEASE}/theme=places/*/*"

# Overture place categories that represent transit infrastructure,
# with weights reflecting typical service frequency.
_TRANSIT_CATEGORIES: dict[str, float] = {
    "train_station": 3.0,
    "light_rail_and_subway_stations": 3.0,
    "bus_station": 2.0,
    "bus_service": 1.5,
    "transportation": 1.0,
    "public_transportation": 1.5,
}


def _fetch_overture_transit() -> pd.DataFrame:
    """Query Overture Maps for transit stations in the Austin bbox."""
    import duckdb

    cats_sql = ", ".join(f"'{c}'" for c in _TRANSIT_CATEGORIES)

    logger.info("Querying Overture Maps for transit stations ...")
    con = duckdb.connect()
    con.execute("INSTALL spatial; INSTALL httpfs;")
    con.execute("LOAD spatial; LOAD httpfs;")
    con.execute("SET s3_region='us-west-2'")

    df = con.execute(f"""
        SELECT
            ST_Y(geometry) AS lat,
            ST_X(geometry) AS lng,
            categories.primary AS category
        FROM read_parquet('{_OVERTURE_S3}')
        WHERE bbox.xmin BETWEEN {LNG_MIN} AND {LNG_MAX}
          AND bbox.ymin BETWEEN {LAT_MIN} AND {LAT_MAX}
          AND categories.primary IN ({cats_sql})
    """).fetchdf()
    con.close()

    logger.info("Overture transit stations: %d", len(df))
    return df


def _build_scores_from_stations(stations_df: pd.DataFrame) -> pd.DataFrame:
    """
    Map transit stations to H3 cells and compute access scores.
    Each station contributes its type weight to its cell and k=1 neighbors
    (H3-r8 edge ~460m, so k=1 covers the ~400m walking shed).
    """
    stations_df = stations_df.copy()
    stations_df["weight"] = stations_df["category"].map(_TRANSIT_CATEGORIES).fillna(1.0)
    stations_df["h3_cell"] = [
        h3.latlng_to_cell(lat, lng, H3_RES)
        for lat, lng in zip(stations_df["lat"], stations_df["lng"])
    ]

    # Accumulate weighted scores per cell, spreading to k=1 ring with decay
    from collections import defaultdict
    cell_score: dict[str, float] = defaultdict(float)
    cell_count: dict[str, int] = defaultdict(int)

    for _, row in stations_df.iterrows():
        center = row["h3_cell"]
        w = row["weight"]

        cell_score[center] += w
        cell_count[center] += 1

        for neighbor in h3.grid_ring(center, 1):
            cell_score[neighbor] += w * 0.5
            cell_count[neighbor] += 1

    records = [
        {"h3_cell": cell, "raw_score": score, "stop_count": cell_count[cell]}
        for cell, score in cell_score.items()
        if score > 0
    ]

    if not records:
        return pd.DataFrame(columns=["h3_cell", "transit_access_score", "stop_count"])

    df = pd.DataFrame(records)
    max_score = df["raw_score"].max()
    df["transit_access_score"] = df["raw_score"] / max_score if max_score > 0 else 0.0
    return df[["h3_cell", "transit_access_score", "stop_count"]]


def _fallback_transit_scores() -> pd.DataFrame:
    """
    Last-resort fallback if Overture Maps query fails entirely.
    Uses known Capital Metro high-frequency corridor locations.
    """
    logger.warning("Using fallback transit scores (hardcoded corridor locations)")

    corridors = [
        (30.2672, -97.7431, 3.0),  # Downtown station (MetroRail)
        (30.2840, -97.7390, 2.5),  # MLK station
        (30.3225, -97.7265, 2.0),  # Highland station
        (30.3570, -97.7200, 1.5),  # Crestview station
        (30.2950, -97.7420, 2.0),  # N Lamar / Guadalupe (frequent bus)
        (30.2500, -97.7500, 2.5),  # S Congress / downtown
        (30.2672, -97.7200, 1.5),  # E Riverside
        (30.2850, -97.7060, 1.0),  # Airport Blvd
        (30.2849, -97.7341, 2.5),  # UT campus
    ]

    from collections import defaultdict
    cell_score: dict[str, float] = defaultdict(float)
    cell_count: dict[str, int] = defaultdict(int)

    for lat, lng, w in corridors:
        center = h3.latlng_to_cell(lat, lng, H3_RES)
        cell_score[center] += w
        cell_count[center] += 1
        for neighbor in h3.grid_ring(center, 1):
            cell_score[neighbor] += w * 0.5
            cell_count[neighbor] += 1

    records = [
        {"h3_cell": c, "raw_score": s, "stop_count": cell_count[c]}
        for c, s in cell_score.items()
    ]
    df = pd.DataFrame(records)
    max_s = df["raw_score"].max()
    df["transit_access_score"] = df["raw_score"] / max_s if max_s > 0 else 0.0
    return df[["h3_cell", "transit_access_score", "stop_count"]]


def fetch_transit_scores() -> pd.DataFrame:
    """
    Fetch and cache transit access scores per H3 cell.
    Primary: Overture Maps. Fallback: hardcoded corridors.
    Returns DataFrame with columns: h3_cell, transit_access_score, stop_count.
    """
    if CACHE_PATH.exists():
        logger.info("Loading cached transit scores from %s", CACHE_PATH)
        return pd.read_parquet(CACHE_PATH)

    try:
        stations_df = _fetch_overture_transit()
        if len(stations_df) >= 5:
            result = _build_scores_from_stations(stations_df)
        else:
            logger.warning("Too few Overture transit stations (%d); using fallback", len(stations_df))
            result = _fallback_transit_scores()
    except Exception as e:
        logger.warning("Overture transit query failed (%s); using fallback", e)
        result = _fallback_transit_scores()

    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    result.to_parquet(CACHE_PATH, index=False)
    logger.info("Cached transit scores: %d cells", len(result))
    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    df = fetch_transit_scores()
    print(f"Transit cells: {len(df)}")
    print(f"Score range: {df['transit_access_score'].min():.3f} - {df['transit_access_score'].max():.3f}")
    print(df.describe())
