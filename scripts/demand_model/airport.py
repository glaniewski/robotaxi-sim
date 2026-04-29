"""
ABIA (Austin-Bergstrom International Airport) special demand generator.

Models airport trips separately from the gravity model because:
1. Airport temporal profile follows flight schedules, not commute patterns
2. Airport is a single cell with disproportionate trip volume
3. Inbound/outbound flows have distinct temporal asymmetry

Calibration: ~18M passengers/year at ABIA (2023 data) ≈ ~49k/day.
Of those, ~5-10% might use a robotaxi service → ~2,500-5,000 trips/day baseline.
The airport_boost parameter scales this.
"""
from __future__ import annotations

import logging

import h3
import numpy as np

logger = logging.getLogger(__name__)

H3_RES = 8

# ABIA terminal approximate location
ABIA_LAT = 30.1975
ABIA_LNG = -97.6664
ABIA_H3 = h3.latlng_to_cell(ABIA_LAT, ABIA_LNG, H3_RES)

# Baseline daily airport rideshare trips (before airport_boost scaling)
# ~49k passengers/day, ~6% single-operator mode share ≈ 2,940 trips/day
BASELINE_DAILY_AIRPORT_TRIPS = 2940

# Hourly profile aligned to flight schedules (24 values, sums to 1.0)
# Early AM departures, afternoon/evening arrivals, red-eye lull
_DEPARTURE_PROFILE = np.array([
    0.02, 0.03, 0.04, 0.06, 0.08, 0.09,  # 0-5: pre-dawn departure rush
    0.08, 0.07, 0.06, 0.05, 0.04, 0.04,  # 6-11: morning departures taper
    0.03, 0.03, 0.03, 0.03, 0.03, 0.03,  # 12-17: midday steady
    0.03, 0.03, 0.02, 0.02, 0.02, 0.02,  # 18-23: evening taper
], dtype=np.float64)
_DEPARTURE_PROFILE /= _DEPARTURE_PROFILE.sum()

# Arrivals peak in afternoon/evening (passengers needing rides FROM airport)
_ARRIVAL_PROFILE = np.array([
    0.01, 0.01, 0.01, 0.01, 0.02, 0.02,  # 0-5: red-eye arrivals
    0.03, 0.04, 0.05, 0.05, 0.06, 0.06,  # 6-11: morning arrivals
    0.06, 0.06, 0.06, 0.07, 0.07, 0.07,  # 12-17: afternoon peak
    0.06, 0.05, 0.04, 0.03, 0.02, 0.02,  # 18-23: evening taper
], dtype=np.float64)
_ARRIVAL_PROFILE /= _ARRIVAL_PROFILE.sum()


def get_airport_h3() -> str:
    """Return the H3 cell containing ABIA terminal."""
    return ABIA_H3


def get_airport_hourly_rates(
    airport_boost: float = 1.0,
    demand_intensity: float = 1.0,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Return (departure_rates, arrival_rates) as 24-element arrays.
    departure = trips TO airport (rider going to catch flight)
    arrival = trips FROM airport (rider arriving, needs ride to destination)

    Rates are in trips/hour.
    """
    daily_total = BASELINE_DAILY_AIRPORT_TRIPS * airport_boost * demand_intensity
    # Split roughly 50/50 between departures and arrivals
    half = daily_total / 2.0

    departure_rates = _DEPARTURE_PROFILE * half  # trips/hour going TO airport
    arrival_rates = _ARRIVAL_PROFILE * half       # trips/hour leaving FROM airport

    return departure_rates, arrival_rates


def generate_airport_trips(
    rng: np.random.Generator,
    destination_cells: np.ndarray,
    destination_weights: np.ndarray,
    airport_boost: float = 1.0,
    demand_intensity: float = 1.0,
    duration_hours: int = 24,
    peak_sharpness: float = 1.0,
) -> list[tuple[float, str, str]]:
    """
    Generate airport trip tuples: (request_time_seconds, origin_h3, destination_h3).

    destination_cells: array of H3 cell IDs that airport trips can go to/from
    destination_weights: probability weights for those cells (e.g., hotel + residential density)
    """
    if len(destination_cells) == 0:
        logger.warning("No destination cells for airport trips — skipping")
        return []

    # Normalize weights
    w = destination_weights.astype(np.float64)
    w_sum = w.sum()
    if w_sum <= 0:
        return []
    w = w / w_sum

    dep_rates, arr_rates = get_airport_hourly_rates(airport_boost, demand_intensity)

    # Apply peak_sharpness
    if peak_sharpness != 1.0:
        dep_rates = dep_rates ** peak_sharpness
        dep_rates *= (BASELINE_DAILY_AIRPORT_TRIPS * airport_boost * demand_intensity / 2.0) / dep_rates.sum()
        arr_rates = arr_rates ** peak_sharpness
        arr_rates *= (BASELINE_DAILY_AIRPORT_TRIPS * airport_boost * demand_intensity / 2.0) / arr_rates.sum()

    trips: list[tuple[float, str, str]] = []
    airport = ABIA_H3

    for hour in range(min(duration_hours, 24)):
        for extra_day in range(duration_hours // 24 + (1 if hour < duration_hours % 24 else 0)):
            day_offset = extra_day * 86400.0
            if day_offset + hour * 3600 >= duration_hours * 3600:
                break

            # Departure trips: city -> airport
            n_dep = rng.poisson(dep_rates[hour])
            if n_dep > 0:
                origins = rng.choice(destination_cells, size=n_dep, p=w)
                times = day_offset + hour * 3600.0 + rng.uniform(0, 3600, size=n_dep)
                for t, o in zip(times, origins):
                    if t < duration_hours * 3600:
                        trips.append((float(t), str(o), airport))

            # Arrival trips: airport -> city
            n_arr = rng.poisson(arr_rates[hour])
            if n_arr > 0:
                destinations = rng.choice(destination_cells, size=n_arr, p=w)
                times = day_offset + hour * 3600.0 + rng.uniform(0, 3600, size=n_arr)
                for t, d in zip(times, destinations):
                    if t < duration_hours * 3600:
                        trips.append((float(t), airport, str(d)))

    logger.info(
        "Airport trips generated: %d (boost=%.1f, intensity=%.2f)",
        len(trips), airport_boost, demand_intensity,
    )
    return trips
