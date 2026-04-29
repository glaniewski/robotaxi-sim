"""
Download and cache Census LODES OD data and ACS population/vehicle/income/age
data for Travis County TX, mapped to H3 resolution 8.

Cached artifacts:
  data/census/lodes_travis_h3.parquet   — home-work OD flows at H3 level
  data/census/acs_tract_h3.parquet      — tract-level demographics at H3 level
  data/census/employment_h3.parquet     — employment counts per H3 cell
"""
from __future__ import annotations

import io
import logging
import zipfile
from pathlib import Path

import h3
import numpy as np
import pandas as pd
import requests

logger = logging.getLogger(__name__)

H3_RES = 8
TRAVIS_FIPS = "48453"
_ROOT = Path(__file__).resolve().parents[2]  # project root
CACHE_DIR = _ROOT / "data" / "census"

# Austin ODD bounding box (matches preprocess_rideaustin_requests.py)
LAT_MIN, LAT_MAX = 30.0, 30.7
LNG_MIN, LNG_MAX = -98.2, -97.4

LODES_URL = (
    "https://lehd.ces.census.gov/data/lodes/LODES8/tx/od/tx_od_main_JT00_2021.csv.gz"
)
# ACS 5-year 2022 variables via Census API
ACS_YEAR = 2022
ACS_VARIABLES = [
    "B01003_001E",   # total population
    "B08201_001E",   # total households
    "B08201_002E",   # households with 0 vehicles
    "B19013_001E",   # median household income
    "B01002_001E",   # median age
]


def _census_block_to_latlng(geoid: str) -> tuple[float, float] | None:
    """
    Approximate lat/lng for a census block FIPS code using its embedded
    tract+block geography. Falls back to tract centroid lookup.
    For LODES we batch-geocode via the Census geocoder is too slow,
    so we use the block-group level mapping instead.
    """
    # We'll use a simpler approach: aggregate to tract level and use
    # tract centroids from the Census TIGERweb API
    return None


def _tract_fips_to_h3(tract_fips: str, tract_centroids: dict[str, tuple[float, float]]) -> list[str]:
    """Map a tract FIPS to its H3 cells using precomputed centroid."""
    coords = tract_centroids.get(tract_fips)
    if coords is None:
        return []
    lat, lng = coords
    if not (LAT_MIN <= lat <= LAT_MAX and LNG_MIN <= lng <= LNG_MAX):
        return []
    return [h3.latlng_to_cell(lat, lng, H3_RES)]


def fetch_tract_centroids() -> dict[str, tuple[float, float]]:
    """
    Fetch Travis County census tract centroids from TIGERweb.
    Returns {tract_geoid: (lat, lng)}.
    """
    url = (
        "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/"
        "Tracts_Blocks/MapServer/8/query"
    )
    params = {
        "where": f"STATE='48' AND COUNTY='453'",
        "outFields": "GEOID,CENTLAT,CENTLON",
        "returnGeometry": "false",
        "f": "json",
        "resultRecordCount": 5000,
    }
    resp = requests.get(url, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    centroids: dict[str, tuple[float, float]] = {}
    for feat in data.get("features", []):
        attrs = feat["attributes"]
        raw_geoid = str(attrs["GEOID"])
        # TIGERweb may return block-group GEOIDs (12 chars) — truncate to
        # 11-char tract GEOID (state 2 + county 3 + tract 6) to match LODES/ACS
        geoid = raw_geoid[:11]
        lat = float(attrs["CENTLAT"])
        lng = float(attrs["CENTLON"])
        centroids[geoid] = (lat, lng)

    logger.info("Fetched %d Travis County tract centroids", len(centroids))
    return centroids


def fetch_lodes(tract_centroids: dict[str, tuple[float, float]]) -> pd.DataFrame:
    """
    Download LODES OD data, filter to Travis County, aggregate to H3 cells.
    Returns DataFrame with columns: home_h3, work_h3, total_jobs.
    """
    cache_path = CACHE_DIR / "lodes_travis_h3.parquet"
    if cache_path.exists():
        logger.info("Loading cached LODES from %s", cache_path)
        return pd.read_parquet(cache_path)

    logger.info("Downloading LODES from %s ...", LODES_URL)
    df = pd.read_csv(
        LODES_URL,
        dtype={"w_geocode": str, "h_geocode": str},
        usecols=["w_geocode", "h_geocode", "S000"],
    )

    # Filter: both home and work blocks must be in Travis County
    df = df[
        df["w_geocode"].str[:5].eq(TRAVIS_FIPS)
        & df["h_geocode"].str[:5].eq(TRAVIS_FIPS)
    ].copy()
    logger.info("LODES rows after Travis County filter: %d", len(df))

    # Extract tract FIPS (first 11 chars of block GEOID)
    df["home_tract"] = df["h_geocode"].str[:11]
    df["work_tract"] = df["w_geocode"].str[:11]

    # Map tracts to H3 cells
    tract_to_h3: dict[str, str] = {}
    for tract_id, (lat, lng) in tract_centroids.items():
        if LAT_MIN <= lat <= LAT_MAX and LNG_MIN <= lng <= LNG_MAX:
            tract_to_h3[tract_id] = h3.latlng_to_cell(lat, lng, H3_RES)

    df["home_h3"] = df["home_tract"].map(tract_to_h3)
    df["work_h3"] = df["work_tract"].map(tract_to_h3)
    df = df.dropna(subset=["home_h3", "work_h3"]).copy()

    # Aggregate by H3 OD pair
    result = (
        df.groupby(["home_h3", "work_h3"], as_index=False)["S000"]
        .sum()
        .rename(columns={"S000": "total_jobs"})
    )

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    result.to_parquet(cache_path, index=False)
    logger.info("Cached LODES H3 OD: %d pairs, %d total jobs", len(result), result["total_jobs"].sum())
    return result


def fetch_acs(tract_centroids: dict[str, tuple[float, float]]) -> pd.DataFrame:
    """
    Fetch ACS 5-year data for Travis County tracts via Census API.
    Returns DataFrame with H3 cell and demographic columns.
    """
    cache_path = CACHE_DIR / "acs_tract_h3.parquet"
    if cache_path.exists():
        logger.info("Loading cached ACS from %s", cache_path)
        return pd.read_parquet(cache_path)

    variables = ",".join(ACS_VARIABLES)
    url = (
        f"https://api.census.gov/data/{ACS_YEAR}/acs/acs5"
        f"?get={variables}&for=tract:*&in=state:48&in=county:453"
    )
    logger.info("Fetching ACS data from Census API ...")
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    header = data[0]
    rows = data[1:]
    df = pd.DataFrame(rows, columns=header)

    # Build tract GEOID
    df["tract_geoid"] = df["state"] + df["county"] + df["tract"]

    # Convert numeric columns
    for var in ACS_VARIABLES:
        df[var] = pd.to_numeric(df[var], errors="coerce")

    # Rename for clarity
    df = df.rename(columns={
        "B01003_001E": "population",
        "B08201_001E": "total_households",
        "B08201_002E": "zero_vehicle_households",
        "B19013_001E": "median_income",
        "B01002_001E": "median_age",
    })

    # Compute car-free fraction
    df["carfree_pct"] = np.where(
        df["total_households"] > 0,
        df["zero_vehicle_households"] / df["total_households"],
        0.0,
    )

    # Map tracts to H3
    tract_to_h3: dict[str, str] = {}
    for tract_id, (lat, lng) in tract_centroids.items():
        if LAT_MIN <= lat <= LAT_MAX and LNG_MIN <= lng <= LNG_MAX:
            tract_to_h3[tract_id] = h3.latlng_to_cell(lat, lng, H3_RES)

    df["h3_cell"] = df["tract_geoid"].map(tract_to_h3)
    df = df.dropna(subset=["h3_cell"]).copy()

    # Multiple tracts may map to the same H3 cell — aggregate
    result = (
        df.groupby("h3_cell", as_index=False)
        .agg({
            "population": "sum",
            "total_households": "sum",
            "zero_vehicle_households": "sum",
            "median_income": "mean",
            "median_age": "mean",
        })
    )
    result["carfree_pct"] = np.where(
        result["total_households"] > 0,
        result["zero_vehicle_households"] / result["total_households"],
        0.0,
    )

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    result.to_parquet(cache_path, index=False)
    logger.info(
        "Cached ACS H3: %d cells, total pop %d",
        len(result), result["population"].sum(),
    )
    return result


def build_employment_by_h3(lodes_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate LODES to get total employment per H3 cell (workplace side).
    Returns DataFrame with columns: h3_cell, employment.
    """
    cache_path = CACHE_DIR / "employment_h3.parquet"
    if cache_path.exists():
        return pd.read_parquet(cache_path)

    result = (
        lodes_df.groupby("work_h3", as_index=False)["total_jobs"]
        .sum()
        .rename(columns={"work_h3": "h3_cell", "total_jobs": "employment"})
    )

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    result.to_parquet(cache_path, index=False)
    logger.info("Employment H3: %d cells, total jobs %d", len(result), result["employment"].sum())
    return result


def fetch_all() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Fetch and cache all census data.
    Returns (lodes_od_df, acs_df, employment_df).
    """
    tract_centroids = fetch_tract_centroids()
    lodes_df = fetch_lodes(tract_centroids)
    acs_df = fetch_acs(tract_centroids)
    employment_df = build_employment_by_h3(lodes_df)
    return lodes_df, acs_df, employment_df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    lodes, acs, emp = fetch_all()
    print(f"LODES OD pairs: {len(lodes)}")
    print(f"ACS cells: {len(acs)}, total population: {acs['population'].sum()}")
    print(f"Employment cells: {len(emp)}, total jobs: {emp['employment'].sum()}")
