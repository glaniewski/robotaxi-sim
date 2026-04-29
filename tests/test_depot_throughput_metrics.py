"""Depot throughput metrics: arrivals, completions, peak sim-hour rates, session dwell."""
from __future__ import annotations

import pytest

from app.sim.engine import SimConfig, SimulationEngine, build_vehicles
from app.sim.entities import Depot
from app.sim.routing import RoutingCache

from .conftest import AUSTIN_CELLS, _build_mock_cache, make_requests


def test_depot_throughput_metrics_present_and_consistent() -> None:
    config = SimConfig(
        duration_minutes=45.0,
        seed=42,
        fleet_size=8,
        reposition_enabled=False,
        timeseries_bucket_minutes=5.0,
        charging_queue_policy="fifo",
    )
    routing = RoutingCache(cache=_build_mock_cache())
    depot = Depot(
        id="depot_0",
        h3_cell=AUSTIN_CELLS[0],
        chargers_count=3,
        charger_kw=150.0,
        site_power_kw=450.0,
    )
    vehicles = build_vehicles(config, [AUSTIN_CELLS[0]], seed=42)
    requests = make_requests(40, seed=42)
    engine = SimulationEngine(
        config=config,
        vehicles=vehicles,
        requests=requests,
        depots=[depot],
        routing=routing,
    )
    m = engine.run()["metrics"]

    assert m["depot_arrivals_total"] == m["depot_arrivals_by_depot_id"][depot.id]
    assert m["depot_charge_completions_total"] == m["depot_charge_completions_by_depot_id"][depot.id]
    assert m["depot_jit_plug_full_total"] == m["depot_jit_plug_full_by_depot_id"][depot.id]
    assert m["depot_arrivals_total"] >= m["depot_charge_completions_total"]
    assert m["depot_charge_completions_total"] == len(engine._charging_session_durations_s)
    assert m["depot_arrivals_peak_fleet_per_hour"] >= m["depot_arrivals_peak_max_site_per_hour"]
    assert m["depot_charge_completions_peak_fleet_per_hour"] >= m[
        "depot_charge_completions_peak_max_site_per_hour"
    ]
    if m["depot_charge_completions_total"] > 0:
        assert m["charging_session_duration_median_min"] > 0
        assert m["charging_session_duration_p90_min"] >= m["charging_session_duration_median_min"]


def test_depot_throughput_metrics_under_jit_policy() -> None:
    """JIT policy still emits throughput keys (JIT-bounce count may be zero if reservations avoid overlap)."""
    config = SimConfig(
        duration_minutes=30.0,
        seed=1,
        fleet_size=4,
        reposition_enabled=False,
        timeseries_bucket_minutes=5.0,
        charging_queue_policy="jit",
    )
    routing = RoutingCache(cache=_build_mock_cache())
    depot = Depot(
        id="depot_j",
        h3_cell=AUSTIN_CELLS[0],
        chargers_count=2,
        charger_kw=150.0,
        site_power_kw=300.0,
    )
    vehicles = build_vehicles(config, [AUSTIN_CELLS[0]], seed=1)
    requests = make_requests(20, seed=1)
    m = SimulationEngine(
        config=config,
        vehicles=vehicles,
        requests=requests,
        depots=[depot],
        routing=routing,
    ).run()["metrics"]
    assert m["depot_jit_plug_full_total"] == m["depot_jit_plug_full_by_depot_id"][depot.id]
    assert m["depot_jit_plug_full_total"] >= 0
