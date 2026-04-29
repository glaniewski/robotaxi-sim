"""Charging depot selection: fastest vs fastest_balanced (SPEC §11.1)."""
from __future__ import annotations

import pytest

from app.sim.engine import SimConfig, SimulationEngine, build_vehicles
from app.sim.entities import Depot, VehicleState
from app.sim.routing import RoutingCache

from .conftest import AUSTIN_CELLS, _build_mock_cache


def _two_depots_same_cell() -> list[Depot]:
    cell = AUSTIN_CELLS[0]
    return [
        Depot(
            id="depot_aaa",
            h3_cell=cell,
            chargers_count=4,
            charger_kw=150.0,
            site_power_kw=600.0,
        ),
        Depot(
            id="depot_zzz",
            h3_cell=cell,
            chargers_count=4,
            charger_kw=150.0,
            site_power_kw=600.0,
        ),
    ]


def test_invalid_charging_depot_selection_raises() -> None:
    config = SimConfig(charging_depot_selection="round_robin")
    routing = RoutingCache(cache=_build_mock_cache())
    depots = _two_depots_same_cell()
    vehicles = build_vehicles(config, [AUSTIN_CELLS[0]], seed=0)
    with pytest.raises(ValueError, match="charging_depot_selection"):
        SimulationEngine(
            config=config,
            vehicles=vehicles,
            requests=[],
            depots=depots,
            routing=routing,
        )


def test_negative_balance_slack_raises() -> None:
    config = SimConfig(charging_depot_balance_slack_minutes=-1.0)
    routing = RoutingCache(cache=_build_mock_cache())
    depots = _two_depots_same_cell()
    vehicles = build_vehicles(config, [AUSTIN_CELLS[0]], seed=0)
    with pytest.raises(ValueError, match="charging_depot_balance_slack_minutes"):
        SimulationEngine(
            config=config,
            vehicles=vehicles,
            requests=[],
            depots=depots,
            routing=routing,
        )


def test_fastest_tie_breaks_by_depot_id() -> None:
    """Same cell + identical plugs → equal depart_time; fastest uses lexicographic depot id."""
    config = SimConfig(
        reposition_enabled=False,
        charging_depot_selection="fastest",
    )
    routing = RoutingCache(cache=_build_mock_cache())
    depots = _two_depots_same_cell()
    depots[0].queue.extend(["q1", "q2", "q3"])
    vehicles = build_vehicles(config, [AUSTIN_CELLS[0]], seed=0)
    v = vehicles[0]
    engine = SimulationEngine(config, vehicles, [], depots, routing)
    assert v.state == VehicleState.IDLE
    ok = engine._reserve_charge_departure(v, 0.0, target_soc=config.soc_target)
    assert ok
    assert engine._charge_reservation_by_vehicle[v.id]["depot_id"] == "depot_aaa"


def test_fastest_balanced_prefers_lower_pressure() -> None:
    """Within slack of best depart_time, pick depot with fewer waiters / less pressure."""
    config = SimConfig(
        reposition_enabled=False,
        charging_depot_selection="fastest_balanced",
        charging_depot_balance_slack_minutes=3.0,
    )
    routing = RoutingCache(cache=_build_mock_cache())
    depots = _two_depots_same_cell()
    depots[0].queue.extend(["q1", "q2", "q3"])
    vehicles = build_vehicles(config, [AUSTIN_CELLS[0]], seed=0)
    v = vehicles[0]
    engine = SimulationEngine(config, vehicles, [], depots, routing)
    ok = engine._reserve_charge_departure(v, 0.0, target_soc=config.soc_target)
    assert ok
    assert engine._charge_reservation_by_vehicle[v.id]["depot_id"] == "depot_zzz"
