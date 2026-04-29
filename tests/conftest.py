"""
Shared fixtures for robotaxi-sim tests.

All fixtures use synthetic data so no OSRM service or parquet files
are required for the test suite.
"""
from __future__ import annotations

import pytest

from app.sim.engine import SimConfig, SimulationEngine, build_vehicles
from app.sim.entities import Depot, Request, RequestStatus
from app.sim.routing import RoutingCache

# ---------------------------------------------------------------------------
# Austin H3 cells at resolution 8 used in tests
# (real cells within Austin city limits)
# ---------------------------------------------------------------------------
AUSTIN_CELLS = [
    "8844c0a2a5fffff",  # downtown
    "8844c0a2a1fffff",  # east austin
    "8844c0a2b3fffff",  # south congress
    "8844c0a2b1fffff",  # zilker
    "8844c0a297fffff",  # north loop
    "8844c0a295fffff",  # mueller
    "8844c0a2b5fffff",  # bouldin
    "8844c0a293fffff",  # highland
]

# ---------------------------------------------------------------------------
# Mock routing: fixed 5-minute / 3-mile travel between any distinct cell pair
# ---------------------------------------------------------------------------
_MOCK_TIME_S = 300.0        # 5 minutes
_MOCK_DIST_M = 4828.0       # 3 miles


def _build_mock_cache() -> dict[tuple[str, str], tuple[float, float]]:
    cache: dict[tuple[str, str], tuple[float, float]] = {}
    for o in AUSTIN_CELLS:
        for d in AUSTIN_CELLS:
            if o != d:
                cache[(o, d)] = (_MOCK_TIME_S, _MOCK_DIST_M)
    return cache


@pytest.fixture
def mock_routing() -> RoutingCache:
    return RoutingCache(cache=_build_mock_cache())


@pytest.fixture
def mini_depot() -> Depot:
    return Depot(
        id="test_depot",
        h3_cell=AUSTIN_CELLS[0],
        chargers_count=5,
        charger_kw=150.0,
        site_power_kw=750.0,
    )


@pytest.fixture
def mini_config() -> SimConfig:
    return SimConfig(
        duration_minutes=30.0,
        seed=42,
        fleet_size=5,
        battery_kwh=75.0,
        kwh_per_mile=0.30,
        soc_initial=0.75,
        soc_min=0.20,
        soc_target=0.80,
        soc_buffer=0.05,
        max_wait_time_seconds=600.0,
        reposition_enabled=False,  # off by default; individual tests opt in
        timeseries_bucket_minutes=5.0,
    )


def make_requests(n: int, seed: int = 0) -> list[Request]:
    """
    Generate n synthetic requests spread across the first 20 minutes.
    Origins and destinations alternate through AUSTIN_CELLS.
    """
    import random
    rng = random.Random(seed)
    requests = []
    for i in range(n):
        t = rng.uniform(0, 1200)  # 0–20 minutes
        origin = AUSTIN_CELLS[i % len(AUSTIN_CELLS)]
        dest = AUSTIN_CELLS[(i + 1) % len(AUSTIN_CELLS)]
        requests.append(
            Request(
                id=f"req_{i}",
                request_time=t,
                origin_h3=origin,
                destination_h3=dest,
                max_wait_time_seconds=600.0,
            )
        )
    requests.sort(key=lambda r: r.request_time)
    return requests


def run_mini_sim(
    n_requests: int = 10,
    fleet_size: int = 5,
    seed: int = 42,
    reposition_enabled: bool = False,
    duration_minutes: float = 30.0,
) -> dict:
    config = SimConfig(
        duration_minutes=duration_minutes,
        seed=seed,
        fleet_size=fleet_size,
        reposition_enabled=reposition_enabled,
        timeseries_bucket_minutes=5.0,
    )
    routing = RoutingCache(cache=_build_mock_cache())
    depot = Depot(
        id="depot_0",
        h3_cell=AUSTIN_CELLS[0],
        chargers_count=5,
        charger_kw=150.0,
        site_power_kw=750.0,
    )
    vehicles = build_vehicles(config, [AUSTIN_CELLS[0]], seed=seed)
    requests = make_requests(n_requests, seed=seed)

    engine = SimulationEngine(
        config=config,
        vehicles=vehicles,
        requests=requests,
        depots=[depot],
        routing=routing,
    )
    return engine.run()
