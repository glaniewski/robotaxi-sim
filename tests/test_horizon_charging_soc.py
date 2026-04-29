"""End-of-horizon linear SOC for vehicles still in CHARGING (metrics snapshot)."""
from __future__ import annotations

from app.sim.engine import SimConfig, SimulationEngine, build_vehicles
from app.sim.entities import Depot, VehicleState
from app.sim.routing import RoutingCache

from .conftest import AUSTIN_CELLS, _build_mock_cache


def test_horizon_charging_soc_interpolation_caps_at_target() -> None:
    config = SimConfig(
        fleet_size=1,
        seed=0,
        duration_minutes=60.0,
        soc_target=0.80,
        reposition_enabled=False,
        timeseries_bucket_minutes=60.0,
        battery_kwh=75.0,
    )
    v = build_vehicles(config, [AUSTIN_CELLS[0]], seed=0)[0]
    v.state = VehicleState.CHARGING
    v.soc = 0.50
    v.charging_session_start_time = 0.0
    v.charging_session_kw = 75.0  # 1 hour full pack at this rate; would overshoot
    depot = Depot(
        id="d0",
        h3_cell=AUSTIN_CELLS[0],
        chargers_count=1,
        charger_kw=75.0,
        site_power_kw=75.0,
    )
    eng = SimulationEngine(config, [v], [], [depot], RoutingCache(cache=_build_mock_cache()))
    eng._current_time = 3600.0
    eng._active_charge_end_by_vehicle[v.id] = 10_000.0
    eng._apply_horizon_charging_soc_for_metrics()
    assert abs(v.soc - 0.80) < 1e-9


def test_horizon_charging_soc_respects_scheduled_session_end() -> None:
    config = SimConfig(
        fleet_size=1,
        seed=0,
        duration_minutes=60.0,
        soc_target=0.80,
        reposition_enabled=False,
        timeseries_bucket_minutes=60.0,
        battery_kwh=75.0,
    )
    v = build_vehicles(config, [AUSTIN_CELLS[0]], seed=0)[0]
    v.state = VehicleState.CHARGING
    v.soc = 0.50
    v.charging_session_start_time = 0.0
    v.charging_session_kw = 20.0
    depot = Depot(
        id="d0",
        h3_cell=AUSTIN_CELLS[0],
        chargers_count=1,
        charger_kw=20.0,
        site_power_kw=20.0,
    )
    eng = SimulationEngine(config, [v], [], [depot], RoutingCache(cache=_build_mock_cache()))
    # Session was only 30 minutes long; horizon at 1 h must not add energy past that
    eng._current_time = 3600.0
    eng._active_charge_end_by_vehicle[v.id] = 1800.0
    eng._apply_horizon_charging_soc_for_metrics()
    # 30 min @ 20 kW on 75 kWh → 10 kWh → 10/75 SOC
    expected = 0.50 + 10.0 / 75.0
    assert abs(v.soc - expected) < 1e-6


def test_begin_charging_sets_kw_and_start_time() -> None:
    config = SimConfig(
        fleet_size=1,
        seed=0,
        duration_minutes=120.0,
        soc_target=0.80,
        soc_initial=0.50,
        reposition_enabled=False,
        timeseries_bucket_minutes=60.0,
    )
    v = build_vehicles(config, [AUSTIN_CELLS[0]], seed=0)[0]
    v.current_h3 = depot_cell = AUSTIN_CELLS[0]
    depot = Depot(
        id="d0",
        h3_cell=depot_cell,
        chargers_count=2,
        charger_kw=50.0,
        site_power_kw=80.0,
    )
    routing = RoutingCache(cache=_build_mock_cache())
    eng = SimulationEngine(config, [v], [], [depot], routing)
    t0 = 100.0
    eng._begin_charging_session(v.id, depot, t0)
    assert v.charging_session_start_time == t0
    assert v.charging_session_kw == depot.effective_charger_kw()
    assert v.state == VehicleState.CHARGING
