"""
Main demand generation entry point.

Orchestrates all pipeline steps:
  1. Fetch/cache census, POI, transit, and travel time data
  2. Build gravity model with purpose-specific OD distributions
  3. Compute per-cell production rates (population × demographics × transit)
  4. Poisson-sample trips per cell per hour per purpose
  5. Inject event demand if configured
  6. Generate airport trips via special generator
  7. Write parquet in sim-compatible schema

Usage:
    python -m demand_model.generate [--config config.json] [--output path.parquet]
    python scripts/demand_model/generate.py --help
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

from .airport import generate_airport_trips
from .config import DemandModelConfig
from .fetch_census import fetch_all as fetch_census
from .fetch_pois import build_poi_scores, fetch_pois
from .fetch_transit import fetch_transit_scores
from .gravity import GravityModel, build_gravity_model, load_travel_times_from_cache
from .temporal_profiles import get_temporal_profile

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parents[2]  # project root
DEFAULT_OUTPUT = _ROOT / "data" / "synthetic_demand.parquet"


def _compute_production_rates(
    cells: np.ndarray,
    config: DemandModelConfig,
    acs_df: pd.DataFrame,
    transit_df: pd.DataFrame,
) -> dict[str, np.ndarray]:
    """
    Compute per-cell trip production rates for each purpose.
    Returns {purpose: array[n_cells] of trips/hour at peak}.

    Actual rates per hour = production_rate[i] * temporal_profile[h].
    """
    n = len(cells)
    cell_to_idx = {c: i for i, c in enumerate(cells)}

    # Population and car-free data per cell
    pop = np.zeros(n, dtype=np.float64)
    carfree_pct = np.zeros(n, dtype=np.float64)
    for _, row in acs_df.iterrows():
        idx = cell_to_idx.get(row["h3_cell"])
        if idx is not None:
            pop[idx] = row["population"]
            carfree_pct[idx] = row.get("carfree_pct", 0.0)

    # Transit suppression per cell
    transit_score = np.zeros(n, dtype=np.float64)
    for _, row in transit_df.iterrows():
        idx = cell_to_idx.get(row["h3_cell"])
        if idx is not None:
            transit_score[idx] = row["transit_access_score"]

    transit_modifier = 1.0 - config.transit_suppression * transit_score

    # Car-free boost: households without cars generate more demand
    # effective_pop = pop * (1 + (carfree_boost - 1) * carfree_pct)
    effective_pop = pop * (1.0 + (config.carfree_boost - 1.0) * carfree_pct)

    # Base production scaled by demand_intensity
    # Calibration: ~15k trips/day for a city of ~1M at intensity=1.0
    # That's ~0.015 trips/person/day or ~0.000625 trips/person/hour
    BASE_RATE_PER_PERSON_HOUR = 0.000625

    base_production = config.demand_intensity * BASE_RATE_PER_PERSON_HOUR * effective_pop * transit_modifier

    # Split production among purposes
    commute_prod = base_production * config.commute_weight
    remaining = base_production * (1.0 - config.commute_weight)
    social_prod = remaining * 0.40
    errands_prod = remaining * 0.40
    tourism_prod = remaining * 0.20 * config.tourism_intensity

    return {
        "commute": commute_prod,
        "social": social_prod,
        "errands": errands_prod,
        "tourism": tourism_prod,
    }


def _generate_purpose_trips(
    rng: np.random.Generator,
    gravity: GravityModel,
    production: np.ndarray,
    purpose: str,
    config: DemandModelConfig,
) -> list[tuple[float, str, str]]:
    """
    Generate trips for a single purpose across all cells and hours.
    Returns list of (request_time_seconds, origin_h3, destination_h3).
    """
    profile = get_temporal_profile(purpose, config.day_type, config.peak_sharpness)
    cells = gravity.cells
    n_cells = len(cells)
    duration_s = config.duration_hours * 3600

    trips: list[tuple[float, str, str]] = []

    # For commute, use AM/PM gravity distinction
    is_commute = purpose == "commute"

    hours_to_generate = config.duration_hours
    for abs_hour in range(hours_to_generate):
        hour_of_day = abs_hour % 24
        hour_start_s = abs_hour * 3600.0
        if hour_start_s >= duration_s:
            break

        hourly_fraction = profile[hour_of_day]

        # Determine gravity purpose for this hour
        if is_commute:
            grav_purpose = "commute_am" if hour_of_day < 12 else "commute_pm"
        elif purpose == "tourism":
            grav_purpose = "tourism"
        elif purpose == "errands":
            grav_purpose = "errands"
        else:
            grav_purpose = "social"

        for cell_idx in range(n_cells):
            rate = production[cell_idx] * hourly_fraction * 24.0  # scale fraction to rate
            if rate < 1e-8:
                continue

            n_trips = rng.poisson(rate)
            if n_trips == 0:
                continue

            # Sample destinations
            dest_indices = gravity.sample_destinations(rng, cell_idx, grav_purpose, n_trips)

            # Assign times uniformly within the hour
            times = hour_start_s + rng.uniform(0, 3600, size=n_trips)

            for t, d_idx in zip(times, dest_indices):
                if t < duration_s and cell_idx != d_idx:
                    trips.append((float(t), cells[cell_idx], cells[d_idx]))

    return trips


def _inject_event_trips(
    rng: np.random.Generator,
    gravity: GravityModel,
    config: DemandModelConfig,
    base_production: dict[str, np.ndarray],
) -> list[tuple[float, str, str]]:
    """
    Generate extra trips for the event cell during the event window.
    First half: inbound (city -> event). Second half: outbound (event -> city).
    """
    if not config.has_event:
        return []

    event_cell = config.event_h3
    event_idx = gravity.cell_to_idx.get(event_cell)
    if event_idx is None:
        logger.warning("Event cell %s not in active cells — skipping event injection", event_cell)
        return []

    start_s = config.event_start_hour * 3600.0
    duration_s = config.event_duration_hours * 3600.0
    midpoint_s = start_s + duration_s / 2.0
    end_s = start_s + duration_s

    # Event rate: multiply the cell's base production by the event multiplier
    total_base = sum(p[event_idx] for p in base_production.values())
    event_rate = total_base * config.event_multiplier  # trips/hour

    trips: list[tuple[float, str, str]] = []
    cells = gravity.cells
    n_cells = len(cells)

    # Build destination weights for event trips (use social attraction)
    social_probs = gravity._od_social[event_idx]
    social_probs = np.maximum(social_probs, 0.0)
    total = social_probs.sum()
    if total > 0:
        social_probs = social_probs / total
    else:
        social_probs = np.ones(n_cells) / n_cells

    # Generate in 15-minute bins
    bin_minutes = 15
    bin_seconds = bin_minutes * 60.0
    t = start_s
    while t < end_s:
        bin_end = min(t + bin_seconds, end_s)
        n_trips = rng.poisson(event_rate * (bin_end - t) / 3600.0)

        if n_trips > 0:
            times = rng.uniform(t, bin_end, size=n_trips)
            other_cells = rng.choice(n_cells, size=n_trips, p=social_probs)

            for trip_t, other_idx in zip(times, other_cells):
                other_cell = cells[other_idx]
                if trip_t < midpoint_s:
                    # Inbound: city -> event
                    trips.append((float(trip_t), other_cell, event_cell))
                else:
                    # Outbound: event -> city
                    trips.append((float(trip_t), event_cell, other_cell))

        t = bin_end

    logger.info("Event trips: %d (cell=%s, multiplier=%.1f)", len(trips), event_cell, config.event_multiplier)
    return trips


def generate_demand(
    config: DemandModelConfig,
    output_path: Path | str = DEFAULT_OUTPUT,
) -> pd.DataFrame:
    """
    Full demand generation pipeline. Fetches data, builds model, samples trips.
    Returns DataFrame and writes parquet.
    """
    config.validate()
    output_path = Path(output_path)
    t0 = time.time()

    logger.info("Generating demand: %s", config.to_json())

    # 1. Fetch all data sources
    logger.info("Step 1/6: Fetching census data ...")
    lodes_df, acs_df, employment_df = fetch_census()

    logger.info("Step 2/6: Fetching POI data ...")
    poi_df = fetch_pois()
    poi_scores_df = build_poi_scores(poi_df)

    logger.info("Step 3/6: Fetching transit scores ...")
    transit_df = fetch_transit_scores()

    # 2. Load travel times (optional, from sim cache)
    logger.info("Step 4/6: Loading travel times ...")
    travel_times = load_travel_times_from_cache()

    # 3. Build gravity model
    logger.info("Step 5/6: Building gravity model ...")
    gravity = build_gravity_model(
        config=config,
        acs_df=acs_df,
        employment_df=employment_df,
        poi_scores_df=poi_scores_df,
        travel_times=travel_times if travel_times else None,
    )

    # 4. Compute production rates
    production = _compute_production_rates(
        cells=gravity.cells,
        config=config,
        acs_df=acs_df,
        transit_df=transit_df,
    )

    # 5. Generate trips by purpose
    logger.info("Step 6/6: Sampling trips ...")
    rng = np.random.default_rng(config.seed)
    all_trips: list[tuple[float, str, str]] = []

    for purpose in ["commute", "social", "errands", "tourism"]:
        trips = _generate_purpose_trips(
            rng=rng,
            gravity=gravity,
            production=production[purpose],
            purpose=purpose,
            config=config,
        )
        logger.info("  %s: %d trips", purpose, len(trips))
        all_trips.extend(trips)

    # 6. Airport trips
    airport_cells, airport_weights = gravity.get_airport_destination_weights()
    airport_trips = generate_airport_trips(
        rng=rng,
        destination_cells=airport_cells,
        destination_weights=airport_weights,
        airport_boost=config.airport_boost,
        demand_intensity=config.demand_intensity,
        duration_hours=config.duration_hours,
        peak_sharpness=config.peak_sharpness,
    )
    all_trips.extend(airport_trips)

    # 7. Event injection
    event_trips = _inject_event_trips(rng, gravity, config, production)
    all_trips.extend(event_trips)

    # 8. Build DataFrame and write parquet
    if not all_trips:
        logger.warning("No trips generated — check parameters")
        df = pd.DataFrame(columns=["request_time_seconds", "origin_h3", "destination_h3"])
    else:
        df = pd.DataFrame(all_trips, columns=["request_time_seconds", "origin_h3", "destination_h3"])
        df = df.sort_values("request_time_seconds").reset_index(drop=True)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)

    elapsed = time.time() - t0
    logger.info(
        "Demand generation complete: %d trips, %.1f sec, output: %s",
        len(df), elapsed, output_path,
    )
    logger.info(
        "  Trips/hour: %.0f avg, duration: %d hours",
        len(df) / max(config.duration_hours, 1),
        config.duration_hours,
    )

    return df


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate synthetic demand for the Austin robotaxi sim",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--config", type=str, default=None, help="Path to JSON config file")
    parser.add_argument("--output", type=str, default=str(DEFAULT_OUTPUT), help="Output parquet path")

    # Allow overriding individual parameters via CLI
    parser.add_argument("--demand-intensity", type=float, default=None)
    parser.add_argument("--duration-hours", type=int, default=None)
    parser.add_argument("--day-type", type=str, default=None, choices=["weekday", "saturday", "sunday"])
    parser.add_argument("--peak-sharpness", type=float, default=None)
    parser.add_argument("--beta", type=float, default=None)
    parser.add_argument("--commute-weight", type=float, default=None)
    parser.add_argument("--transit-suppression", type=float, default=None)
    parser.add_argument("--tourism-intensity", type=float, default=None)
    parser.add_argument("--airport-boost", type=float, default=None)
    parser.add_argument("--entertainment-weight", type=float, default=None)
    parser.add_argument("--employment-weight", type=float, default=None)
    parser.add_argument("--medical-weight", type=float, default=None)
    parser.add_argument("--carfree-boost", type=float, default=None)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--event-h3", type=str, default=None)
    parser.add_argument("--event-start-hour", type=float, default=None)
    parser.add_argument("--event-duration-hours", type=float, default=None)
    parser.add_argument("--event-multiplier", type=float, default=None)

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )

    # Build config: start from file or defaults, then override with CLI args
    if args.config:
        config = DemandModelConfig.from_json(Path(args.config).read_text())
    else:
        config = DemandModelConfig()

    cli_overrides = {
        "demand_intensity": args.demand_intensity,
        "duration_hours": args.duration_hours,
        "day_type": args.day_type,
        "peak_sharpness": args.peak_sharpness,
        "beta": args.beta,
        "commute_weight": args.commute_weight,
        "transit_suppression": args.transit_suppression,
        "tourism_intensity": args.tourism_intensity,
        "airport_boost": args.airport_boost,
        "entertainment_weight": args.entertainment_weight,
        "employment_weight": args.employment_weight,
        "medical_weight": args.medical_weight,
        "carfree_boost": args.carfree_boost,
        "seed": args.seed,
        "event_h3": args.event_h3,
        "event_start_hour": args.event_start_hour,
        "event_duration_hours": args.event_duration_hours,
        "event_multiplier": args.event_multiplier,
    }
    for key, val in cli_overrides.items():
        if val is not None:
            setattr(config, key, val)

    df = generate_demand(config, output_path=Path(args.output))
    print(f"\nGenerated {len(df)} trips → {args.output}")
    print(f"Config fingerprint: {config.fingerprint()}")


if __name__ == "__main__":
    main()
