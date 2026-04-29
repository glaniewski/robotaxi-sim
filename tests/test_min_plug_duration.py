"""Minimum plug dwell (min_plug_duration_minutes) and SOC reporting metrics."""
from __future__ import annotations

import pytest

from app.sim.engine import SimConfig, SimulationEngine, build_vehicles
from app.sim.entities import Depot, Request, Vehicle, VehicleState
from app.sim.metrics import compute_metrics
from app.sim.routing import RoutingCache

from .conftest import AUSTIN_CELLS, _build_mock_cache, make_requests


def test_negative_min_plug_raises() -> None:
    cfg = SimConfig(min_plug_duration_minutes=-1.0)
    with pytest.raises(ValueError, match="min_plug_duration_minutes"):
        SimulationEngine(
            cfg,
            build_vehicles(SimConfig(fleet_size=1, seed=0), [AUSTIN_CELLS[0]], seed=0),
            [],
            [
                Depot(
                    id="d0",
                    h3_cell=AUSTIN_CELLS[0],
                    chargers_count=1,
                    charger_kw=50.0,
                    site_power_kw=50.0,
                )
            ],
            RoutingCache(cache=_build_mock_cache()),
        )


def test_effective_soc_charge_start_tightens_with_min_plug() -> None:
    """Voluntary charging threshold lowers when min dwell implies a meaningful SOC gap."""
    config = SimConfig(
        fleet_size=1,
        seed=0,
        duration_minutes=60.0,
        battery_kwh=75.0,
        soc_target=0.80,
        soc_charge_start=0.80,
        soc_min=0.20,
        min_plug_duration_minutes=30.0,
        reposition_enabled=False,
        timeseries_bucket_minutes=30.0,
    )
    routing = RoutingCache(cache=_build_mock_cache())
    v = build_vehicles(config, [AUSTIN_CELLS[0]], seed=0)[0]
    depot = Depot(
        id="d0",
        h3_cell=AUSTIN_CELLS[0],
        chargers_count=4,
        charger_kw=20.0,
        site_power_kw=80.0,
    )
    eng = SimulationEngine(config, [v], [], [depot], routing)
    # 30 min @ 20 kW = 10 kWh → 10/75 ≈ 0.1333 SOC → floor ≈ 0.667
    eff = eng._effective_soc_charge_start()
    assert abs(eff - (0.80 - 10.0 / 75.0)) < 1e-6

    config0 = SimConfig(
        fleet_size=1,
        seed=0,
        duration_minutes=60.0,
        soc_target=0.80,
        soc_charge_start=0.80,
        min_plug_duration_minutes=0.0,
        reposition_enabled=False,
        timeseries_bucket_minutes=30.0,
    )
    v0 = build_vehicles(config0, [AUSTIN_CELLS[0]], seed=0)[0]
    eng0 = SimulationEngine(
        config0, [v0], [], [depot], RoutingCache(cache=_build_mock_cache())
    )
    assert eng0._effective_soc_charge_start() == 0.80


def test_charge_duration_respects_min_plug() -> None:
    config = SimConfig(
        fleet_size=1,
        seed=0,
        duration_minutes=60.0,
        soc_target=0.80,
        min_plug_duration_minutes=30.0,
        reposition_enabled=False,
        timeseries_bucket_minutes=30.0,
    )
    routing = RoutingCache(cache=_build_mock_cache())
    v = build_vehicles(config, [AUSTIN_CELLS[0]], seed=0)[0]
    v.soc = 0.79  # tiny energy need
    depot = Depot(
        id="d0",
        h3_cell=AUSTIN_CELLS[0],
        chargers_count=4,
        charger_kw=50.0,
        site_power_kw=200.0,
    )
    requests = make_requests(5, seed=0)
    eng = SimulationEngine(config, [v], requests, [depot], routing)
    d = eng._charge_duration(v, depot)
    assert d >= 30.0 * 60.0 - 1e-6


def test_metrics_exempts_charging_above_charge_start_when_min_plug_on() -> None:
    v_ok = Vehicle(
        id="a",
        current_h3=AUSTIN_CELLS[0],
        state=VehicleState.CHARGING,
        soc=0.78,
        battery_kwh=75.0,
        kwh_per_mile=0.2,
    )
    v_low = Vehicle(
        id="b",
        current_h3=AUSTIN_CELLS[0],
        state=VehicleState.CHARGING,
        soc=0.50,
        battery_kwh=75.0,
        kwh_per_mile=0.2,
    )
    v_idle = Vehicle(
        id="c",
        current_h3=AUSTIN_CELLS[0],
        state=VehicleState.IDLE,
        soc=0.78,
        battery_kwh=75.0,
        kwh_per_mile=0.2,
    )
    req = Request(
        id="r0",
        request_time=0.0,
        origin_h3=AUSTIN_CELLS[0],
        destination_h3=AUSTIN_CELLS[1],
        max_wait_time_seconds=600.0,
    )
    m = compute_metrics(
        {"a": v_ok, "b": v_low, "c": v_idle},
        {"r0": req},
        [],
        duration_s=3600.0,
        soc_target=0.80,
        soc_charge_start=0.75,
        min_plug_duration_minutes=30.0,
    )
    # CHARGING @ 0.78 >= charge_start 0.75 → not below (ops-adjusted)
    # CHARGING @ 0.50 < charge_start → still below
    # IDLE @ 0.78 < target → below
    assert m["vehicles_below_soc_target_strict_count"] == 3
    assert m["vehicles_below_soc_target_count"] == 2

    m0 = compute_metrics(
        {"a": v_ok, "b": v_low, "c": v_idle},
        {"r0": req},
        [],
        duration_s=3600.0,
        soc_target=0.80,
        soc_charge_start=0.75,
        min_plug_duration_minutes=0.0,
    )
    assert m0["vehicles_below_soc_target_count"] == m0["vehicles_below_soc_target_strict_count"] == 3
