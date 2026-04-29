from __future__ import annotations

import random
from typing import Optional

import numpy as np
import pandas as pd

from .entities import Request, RequestStatus


def _h3_cells_in_polygon(polygon_coords: list[list[float]], resolution: int = 8) -> set[str]:
    """
    Return the set of H3 cells at `resolution` that are fully contained in or
    intersect the given polygon.

    polygon_coords: list of [lng, lat] pairs (GeoJSON coordinate order),
    e.g. [[-97.75, 30.25], [-97.70, 30.25], [-97.70, 30.30], [-97.75, 30.30], [-97.75, 30.25]]
    """
    import h3
    # Build GeoJSON dict; h3.geo_to_cells expects a dict with 'type' and 'coordinates'
    geojson = {
        "type": "Polygon",
        "coordinates": [polygon_coords],
    }
    return set(h3.geo_to_cells(geojson, resolution))


def load_requests(
    parquet_path: str,
    duration_minutes: float,
    day_offset_seconds: float = 0.0,
    max_wait_time_seconds: float = 600.0,
    demand_scale: float = 1.0,
    demand_flatten: float = 0.0,
    seed: int = 0,
    coverage_polygon: Optional[list[list[float]]] = None,
    h3_resolution: int = 8,
) -> list[Request]:
    """
    Load requests from the collapsed synthetic-day parquet.

    Selects rows where request_time_seconds falls within
    [day_offset_seconds, day_offset_seconds + duration_minutes * 60],
    then subsamples by demand_scale (1.0 = all trips, 0.5 = half, 2.0 = double
    via sampling with replacement).  Times are rebased to start at 0.

    demand_flatten (0.0–1.0): linearly blends each trip's original timestamp
    toward a uniform draw across the full simulation window.  0.0 preserves
    the raw historical peak pattern; 1.0 produces perfectly uniform arrivals.
    Same trips and O-D pairs — only *when* they arrive changes.
    """
    df = pd.read_parquet(
        parquet_path,
        columns=["request_time_seconds", "origin_h3", "destination_h3"],
    )

    end_s = day_offset_seconds + duration_minutes * 60.0
    mask = (df["request_time_seconds"] >= day_offset_seconds) & (
        df["request_time_seconds"] < end_s
    )
    df = df[mask].copy()

    # Optional geographic filter: keep only trips whose origin AND destination
    # fall inside the coverage polygon.
    if coverage_polygon is not None and len(coverage_polygon) >= 3:
        zone_cells = _h3_cells_in_polygon(coverage_polygon, h3_resolution)
        df = df[
            df["origin_h3"].isin(zone_cells) & df["destination_h3"].isin(zone_cells)
        ].copy()

    if demand_scale != 1.0:
        n_target = int(round(len(df) * demand_scale))
        replace = demand_scale > 1.0
        df = df.sample(n=n_target, replace=replace, random_state=seed).copy()

    df["request_time_seconds"] = df["request_time_seconds"] - day_offset_seconds

    duration_s = duration_minutes * 60.0
    if demand_flatten > 0.0:
        rng = np.random.default_rng(seed + 1)  # +1 keeps flatten independent of scale seed
        uniform_times = rng.uniform(0.0, duration_s, size=len(df))
        df["request_time_seconds"] = (
            (1.0 - demand_flatten) * df["request_time_seconds"].values
            + demand_flatten * uniform_times
        )

    df = df.sort_values("request_time_seconds").reset_index(drop=True)

    requests: list[Request] = []
    for i, row in enumerate(df.itertuples(index=False)):
        requests.append(
            Request(
                id=f"req_{i}",
                request_time=float(row.request_time_seconds),
                origin_h3=str(row.origin_h3),
                destination_h3=str(row.destination_h3),
                max_wait_time_seconds=max_wait_time_seconds,
            )
        )
    return requests


def load_requests_repeated_days(
    parquet_path: str,
    duration_minutes_per_day: float,
    num_days: int,
    day_offset_seconds: float = 0.0,
    max_wait_time_seconds: float = 600.0,
    demand_scale: float = 1.0,
    demand_flatten: float = 0.0,
    seed: int = 0,
    coverage_polygon: Optional[list[list[float]]] = None,
    h3_resolution: int = 8,
) -> list[Request]:
    """
    Build a single continuous request stream by repeating the same synthetic day
    ``num_days`` times on one clock.

    Loads one day via ``load_requests`` (times rebased to [0, duration_per_day)),
    then for day ``d`` offsets each request by ``d * duration_minutes_per_day * 60`` seconds.
    Request ids are prefixed so they stay unique across days.
    """
    if num_days < 1:
        return []
    base = load_requests(
        parquet_path,
        duration_minutes=duration_minutes_per_day,
        day_offset_seconds=day_offset_seconds,
        max_wait_time_seconds=max_wait_time_seconds,
        demand_scale=demand_scale,
        demand_flatten=demand_flatten,
        seed=seed,
        coverage_polygon=coverage_polygon,
        h3_resolution=h3_resolution,
    )
    day_s = duration_minutes_per_day * 60.0
    out: list[Request] = []
    for d in range(num_days):
        off = d * day_s
        for r in base:
            out.append(
                Request(
                    id=f"req_d{d}_{r.id}",
                    request_time=float(r.request_time) + off,
                    origin_h3=r.origin_h3,
                    destination_h3=r.destination_h3,
                    max_wait_time_seconds=r.max_wait_time_seconds,
                )
            )
    out.sort(key=lambda x: x.request_time)
    return out


def apply_demand_control(
    requests: list[Request],
    flex_pct: float = 0.0,
    flex_minutes: float = 10.0,
    pool_pct: float = 0.0,
    max_detour_pct: float = 0.15,
    prebook_pct: float = 0.0,
    eta_threshold_minutes: float = 10.0,
    prebook_shift_minutes: float = 10.0,
    offpeak_shift_pct: float = 0.0,
    peak_start_s: float = 7 * 3600,
    peak_end_s: float = 9 * 3600,
    shoulder_offset_s: float = 3600,
    seed: int = 0,
) -> list[Request]:
    """
    Apply demand-control transformations in place (modifies request list).

    Order: offpeak shift → prebooking → flex → pooling.
    Returns the (possibly reordered) modified list.
    """
    rng = random.Random(seed)

    # --- Off-peak shift: move a fraction of peak requests into shoulder ---
    if offpeak_shift_pct > 0:
        peak_reqs = [
            r for r in requests if peak_start_s <= r.request_time <= peak_end_s
        ]
        n_shift = int(len(peak_reqs) * offpeak_shift_pct)
        to_shift = rng.sample(peak_reqs, min(n_shift, len(peak_reqs)))
        for r in to_shift:
            direction = rng.choice([-1, 1])
            r.request_time = max(0.0, r.request_time + direction * shoulder_offset_s)

    # --- Prebooking: shift requests that would have long waits earlier ---
    if prebook_pct > 0:
        n_prebook = int(len(requests) * prebook_pct)
        for r in rng.sample(requests, min(n_prebook, len(requests))):
            r.request_time = max(0.0, r.request_time - prebook_shift_minutes * 60.0)

    # --- Flex window: assign a latest_departure_time ---
    if flex_pct > 0:
        n_flex = int(len(requests) * flex_pct)
        for r in rng.sample(requests, min(n_flex, len(requests))):
            r.latest_departure_time = r.request_time + flex_minutes * 60.0

    # --- Pooling: mark eligible requests ---
    if pool_pct > 0:
        n_pool = int(len(requests) * pool_pct)
        for r in rng.sample(requests, min(n_pool, len(requests))):
            r.pooled_allowed = True

    # Re-sort by request_time after any time shifts
    requests.sort(key=lambda r: r.request_time)
    return requests
