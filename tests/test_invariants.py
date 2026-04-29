"""
Simulation invariants per SPEC §18.
"""
from __future__ import annotations

import pytest

from app.sim.engine import SimConfig, SimulationEngine, build_vehicles
from app.sim.entities import Depot, RequestStatus, VehicleState
from app.sim.routing import RoutingCache

from .conftest import AUSTIN_CELLS, _build_mock_cache, make_requests


@pytest.fixture
def full_result():
    """Run a moderately-sized sim and return the engine with final state."""
    config = SimConfig(
        duration_minutes=60.0,
        seed=42,
        fleet_size=10,
        reposition_enabled=False,
        timeseries_bucket_minutes=5.0,
    )
    routing = RoutingCache(cache=_build_mock_cache())
    depot = Depot(
        id="depot_0",
        h3_cell=AUSTIN_CELLS[0],
        chargers_count=4,
        charger_kw=150.0,
        site_power_kw=600.0,
    )
    vehicles = build_vehicles(config, [AUSTIN_CELLS[0]], seed=42)
    requests = make_requests(30, seed=42)

    engine = SimulationEngine(
        config=config,
        vehicles=vehicles,
        requests=requests,
        depots=[depot],
        routing=routing,
    )
    result = engine.run()
    return engine, result


def test_served_count_le_request_count(full_result):
    engine, result = full_result
    served = result["metrics"]["served_count"]
    total = len(engine.requests)
    assert served <= total, f"served={served} > total={total}"


def test_soc_in_bounds_after_run(full_result):
    engine, _ = full_result
    for v in engine.vehicles.values():
        assert 0.0 <= v.soc <= 1.0, f"Vehicle {v.id} SOC={v.soc} out of bounds"


def test_active_chargers_le_capacity(full_result):
    engine, _ = full_result
    for depot in engine.depots:
        assert depot.active_chargers <= depot.chargers_count, (
            f"Depot {depot.id}: active_chargers={depot.active_chargers} "
            f"> chargers_count={depot.chargers_count}"
        )


def test_no_negative_travel_times():
    routing = RoutingCache(cache=_build_mock_cache())
    for o in AUSTIN_CELLS:
        for d in AUSTIN_CELLS:
            time_s, dist_m = routing.get(o, d)
            assert time_s >= 0.0, f"Negative travel time: {o} → {d}: {time_s}s"
            assert dist_m >= 0.0, f"Negative distance: {o} → {d}: {dist_m}m"


def test_served_plus_unserved_le_total(full_result):
    engine, result = full_result
    m = result["metrics"]
    assert m["served_count"] + m["unserved_count"] <= len(engine.requests)


def test_served_pct_consistent(full_result):
    engine, result = full_result
    m = result["metrics"]
    total = len(engine.requests)
    if total > 0:
        expected_pct = m["served_count"] / total * 100.0
        assert abs(m["served_pct"] - expected_pct) < 0.01, (
            f"served_pct mismatch: {m['served_pct']} vs computed {expected_pct}"
        )


def test_idle_vehicle_can_reach_soc_target_with_depot_no_requests():
    config = SimConfig(
        duration_minutes=30.0,
        seed=7,
        fleet_size=1,
        soc_initial=0.40,
        soc_target=0.80,
        reposition_enabled=False,
        timeseries_bucket_minutes=5.0,
    )
    routing = RoutingCache(cache=_build_mock_cache())
    depot = Depot(
        id="depot_0",
        h3_cell=AUSTIN_CELLS[0],
        chargers_count=1,
        charger_kw=150.0,
        site_power_kw=150.0,
    )
    vehicles = build_vehicles(config, [AUSTIN_CELLS[0]], seed=7)
    engine = SimulationEngine(
        config=config,
        vehicles=vehicles,
        requests=[],
        depots=[depot],
        routing=routing,
    )
    engine.run()
    vehicle = next(iter(engine.vehicles.values()))
    assert vehicle.soc >= config.soc_target - 1e-9
    assert vehicle.state == VehicleState.IDLE


def test_no_depot_vehicle_remains_below_soc_target_at_horizon():
    config = SimConfig(
        duration_minutes=30.0,
        seed=8,
        fleet_size=1,
        soc_initial=0.40,
        soc_target=0.80,
        reposition_enabled=False,
        timeseries_bucket_minutes=5.0,
    )
    routing = RoutingCache(cache=_build_mock_cache())
    vehicles = build_vehicles(config, [AUSTIN_CELLS[0]], seed=8)
    engine = SimulationEngine(
        config=config,
        vehicles=vehicles,
        requests=[],
        depots=[],
        routing=routing,
    )
    engine.run()
    vehicle = next(iter(engine.vehicles.values()))
    assert vehicle.soc < config.soc_target
