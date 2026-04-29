"""
Pull Austin POIs from Overture Maps via DuckDB (S3 Parquet), categorize into
demand-model categories, map to H3 resolution 8, and cache.

Uses Overture Maps Foundation data (S3, no API key needed). DuckDB filters
server-side by bbox so only Austin-area records are transferred (~few MB).

Categories:
  entertainment  — bar, nightclub, restaurant, cafe, pub, food_court
  medical        — hospital, clinic, doctors, dentist, pharmacy
  shopping       — mall, supermarket, marketplace, department_store
  university     — university, college, school
  hotel          — hotel, motel, guest_house, hostel
  airport        — aeroway=terminal (single ABIA cell)
  leisure        — park, recreation_ground, stadium, sports_centre
"""
from __future__ import annotations

import logging
from pathlib import Path

import h3
import pandas as pd

logger = logging.getLogger(__name__)

H3_RES = 8
_ROOT = Path(__file__).resolve().parents[2]
CACHE_PATH = _ROOT / "data" / "pois_austin_h3_r8.parquet"

LAT_MIN, LAT_MAX = 30.0, 30.7
LNG_MIN, LNG_MAX = -98.2, -97.4

# Overture Maps release to use (update when newer releases are available)
_OVERTURE_RELEASE = "2026-03-18.0"
_OVERTURE_S3 = f"s3://overturemaps-us-west-2/release/{_OVERTURE_RELEASE}/theme=places/*/*"

# Overture `categories.primary` → our demand-model category.
# Overture uses granular names like "mexican_restaurant", "pizza_restaurant" etc.
_OVERTURE_TO_CATEGORY: dict[str, str] = {
    # entertainment
    "bar": "entertainment",
    "pub": "entertainment",
    "nightclub": "entertainment",
    "lounge": "entertainment",
    "wine_bar": "entertainment",
    "brewery": "entertainment",
    "restaurant": "entertainment",
    "mexican_restaurant": "entertainment",
    "pizza_restaurant": "entertainment",
    "fast_food_restaurant": "entertainment",
    "asian_restaurant": "entertainment",
    "italian_restaurant": "entertainment",
    "american_restaurant": "entertainment",
    "chinese_restaurant": "entertainment",
    "japanese_restaurant": "entertainment",
    "thai_restaurant": "entertainment",
    "indian_restaurant": "entertainment",
    "seafood_restaurant": "entertainment",
    "sushi_restaurant": "entertainment",
    "vietnamese_restaurant": "entertainment",
    "korean_restaurant": "entertainment",
    "mediterranean_restaurant": "entertainment",
    "french_restaurant": "entertainment",
    "steakhouse": "entertainment",
    "bbq_restaurant": "entertainment",
    "burger_restaurant": "entertainment",
    "sandwich_shop": "entertainment",
    "breakfast_restaurant": "entertainment",
    "cafe": "entertainment",
    "coffee_shop": "entertainment",
    "bakery": "entertainment",
    "ice_cream_shop": "entertainment",
    "dessert_shop": "entertainment",
    "food_court": "entertainment",
    "food_truck": "entertainment",
    # medical
    "hospital": "medical",
    "urgent_care": "medical",
    "doctor": "medical",
    "dentist": "medical",
    "pharmacy": "medical",
    "chiropractor": "medical",
    "optometrist": "medical",
    "physical_therapist": "medical",
    "veterinarian": "medical",
    "counseling_and_mental_health": "medical",
    "medical_center": "medical",
    # shopping
    "shopping_mall": "medical",  # will fix below
    "supermarket": "shopping",
    "grocery_store": "shopping",
    "department_store": "shopping",
    "convenience_store": "shopping",
    "clothing_store": "shopping",
    "discount_store": "shopping",
    "outlet_store": "shopping",
    "marketplace": "shopping",
    # university / education
    "university": "university",
    "college_university": "university",
    "school": "university",
    "high_school": "university",
    "preschool": "university",
    "elementary_school": "university",
    "middle_school": "university",
    "trade_school": "university",
    # hotel / lodging
    "hotel": "hotel",
    "motel": "hotel",
    "hostel": "hotel",
    "bed_and_breakfast": "hotel",
    "resort": "hotel",
    "inn": "hotel",
    "guest_house": "hotel",
    "vacation_rental": "hotel",
    # leisure
    "park": "leisure",
    "playground": "leisure",
    "recreation_center": "leisure",
    "stadium": "leisure",
    "sports_complex": "leisure",
    "gym": "leisure",
    "swimming_pool": "leisure",
    "golf_course": "leisure",
    "bowling_alley": "leisure",
    "movie_theater": "leisure",
    "performing_arts_theater": "leisure",
    "museum": "leisure",
    "amusement_park": "leisure",
    "zoo": "leisure",
    "spas": "leisure",
}

# Fix the typo above
_OVERTURE_TO_CATEGORY["shopping_mall"] = "shopping"

# ABIA airport location (single hardcoded cell — Overture won't have "aeroway")
_ABIA_LAT, _ABIA_LNG = 30.1975, -97.6664


def _fetch_overture_places() -> pd.DataFrame:
    """Query Overture Maps places via DuckDB for the Austin bbox."""
    import duckdb

    logger.info("Querying Overture Maps (%s) via DuckDB ...", _OVERTURE_RELEASE)

    # Build SQL IN clause for the categories we care about
    cats_sql = ", ".join(f"'{c}'" for c in _OVERTURE_TO_CATEGORY)

    con = duckdb.connect()
    con.execute("INSTALL spatial; INSTALL httpfs;")
    con.execute("LOAD spatial; LOAD httpfs;")
    con.execute("SET s3_region='us-west-2'")

    query = f"""
        SELECT
            ST_Y(geometry) AS lat,
            ST_X(geometry) AS lng,
            categories.primary AS category
        FROM read_parquet('{_OVERTURE_S3}')
        WHERE bbox.xmin BETWEEN {LNG_MIN} AND {LNG_MAX}
          AND bbox.ymin BETWEEN {LAT_MIN} AND {LAT_MAX}
          AND categories.primary IN ({cats_sql})
    """

    df = con.execute(query).fetchdf()
    con.close()

    logger.info("Overture returned %d matching places", len(df))
    return df


def fetch_pois() -> pd.DataFrame:
    """
    Pull POIs from Overture Maps, classify, map to H3, and cache.
    Returns DataFrame with columns: h3_cell, category, poi_count.
    """
    if CACHE_PATH.exists():
        logger.info("Loading cached POIs from %s", CACHE_PATH)
        return pd.read_parquet(CACHE_PATH)

    raw_df = _fetch_overture_places()

    # Map Overture categories to our demand-model categories
    raw_df["demand_category"] = raw_df["category"].map(_OVERTURE_TO_CATEGORY)
    raw_df = raw_df.dropna(subset=["demand_category"]).copy()

    # Filter to bbox (Overture bbox filter is slightly loose)
    mask = (
        (raw_df["lat"] >= LAT_MIN) & (raw_df["lat"] <= LAT_MAX)
        & (raw_df["lng"] >= LNG_MIN) & (raw_df["lng"] <= LNG_MAX)
    )
    raw_df = raw_df[mask].copy()

    # Map to H3
    raw_df["h3_cell"] = [
        h3.latlng_to_cell(lat, lng, H3_RES)
        for lat, lng in zip(raw_df["lat"], raw_df["lng"])
    ]

    # Aggregate
    result = (
        raw_df.groupby(["h3_cell", "demand_category"])
        .size()
        .reset_index(name="poi_count")
    )
    result = result.rename(columns={"demand_category": "category"})

    # Inject airport cell (Overture doesn't tag aeroway)
    airport_cell = h3.latlng_to_cell(_ABIA_LAT, _ABIA_LNG, H3_RES)
    airport_row = pd.DataFrame([{
        "h3_cell": airport_cell, "category": "airport", "poi_count": 1,
    }])
    result = pd.concat([result, airport_row], ignore_index=True)

    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    result.to_parquet(CACHE_PATH, index=False)
    logger.info("Cached POIs: %d cell-category pairs", len(result))

    for cat in sorted(result["category"].unique()):
        n = result[result["category"] == cat]["poi_count"].sum()
        logger.info(
            "  %s: %d POIs across %d cells", cat, n,
            len(result[result["category"] == cat]),
        )

    return result


def build_poi_scores(poi_df: pd.DataFrame) -> pd.DataFrame:
    """
    Pivot POI counts into per-cell scores by category.
    Returns DataFrame with h3_cell as index and one column per category.
    Missing categories get 0.
    """
    if poi_df.empty:
        return pd.DataFrame(columns=["h3_cell", "entertainment", "medical",
                                      "shopping", "university", "hotel",
                                      "airport", "leisure"])

    pivot = poi_df.pivot_table(
        index="h3_cell",
        columns="category",
        values="poi_count",
        aggfunc="sum",
        fill_value=0,
    )
    for cat in ["entertainment", "medical", "shopping", "university",
                "hotel", "airport", "leisure"]:
        if cat not in pivot.columns:
            pivot[cat] = 0
    return pivot.reset_index()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    df = fetch_pois()
    scores = build_poi_scores(df)
    print(f"\nPOI cells: {len(scores)}")
    print(scores.drop(columns=["h3_cell"]).sum().to_string())
