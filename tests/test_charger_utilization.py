"""
Why `charger_utilization_pct` can look "abysmal" with many microsites / FIFO queues.

The metric is fleet-wide: sum of charge-session durations divided by
(total physical plugs × simulation length). Hot depots with long FIFO waits do
not increase the numerator until vehicles actually plug in; cold depots with
idle plugs shrink the percentage. This is not per-site utilization.
"""
from __future__ import annotations

import pytest

from app.sim.engine import SimConfig, SimulationEngine, build_vehicles
from app.sim.entities import Depot
from app.sim.metrics import summarize_charger_util_by_depot
from app.sim.routing import RoutingCache

from .conftest import AUSTIN_CELLS, _build_mock_cache, make_requests


def test_summarize_charger_util_by_depot() -> None:
    s = summarize_charger_util_by_depot({"a": 0.0, "b": 10.0, "c": 20.0, "d": 5.0})
    assert s["depot_charger_util_max_pct"] == 20.0
    assert s["depot_charger_util_nonzero_count"] == 3  # 0.0 not counted as > 0.01
    assert s["depot_charger_util_mean_pct"] == round(35.0 / 4, 2)
    assert summarize_charger_util_by_depot({})["depot_charger_util_nonzero_count"] == 0


def test_charger_utilization_matches_busy_over_plug_capacity() -> None:
    """Published % equals internal busy_seconds / (n_plugs * horizon_seconds)."""
    config = SimConfig(
        duration_minutes=30.0,
        seed=42,
        fleet_size=5,
        reposition_enabled=False,
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
    m = result["metrics"]
    cap_s = engine._total_charger_capacity_s
    busy_s = engine._charger_busy_seconds
    expected_pct = busy_s / cap_s * 100.0 if cap_s > 0 else 0.0
    assert abs(m["charger_utilization_pct"] - expected_pct) < 0.02
    assert set(m["charger_utilization_by_depot_pct"].keys()) == {depot.id}
    assert abs(m["charger_utilization_by_depot_pct"][depot.id] - expected_pct) < 0.02
    assert abs(sum(engine._charger_busy_seconds_by_depot.values()) - busy_s) < 1e-6


def test_unused_plugs_across_depots_dilute_utilization() -> None:
    """
    Same charging workload, but denominator scales with every depot's plug count.
    Only the nearest depot to the vehicle should see sessions; the rest are idle
    diluting the fleet-wide %.
    """
    n_depots = len(AUSTIN_CELLS)
    chargers_per = 10
    duration_min = 60.0
    horizon_s = duration_min * 60.0
    config = SimConfig(
        duration_minutes=duration_min,
        seed=0,
        fleet_size=1,
        soc_initial=0.25,
        soc_target=0.80,
        soc_charge_start=0.80,
        reposition_enabled=False,
        timeseries_bucket_minutes=30.0,
        charging_queue_policy="fifo",
    )
    depots = [
        Depot(
            id=f"d{i}",
            h3_cell=AUSTIN_CELLS[i],
            chargers_count=chargers_per,
            charger_kw=150.0,
            site_power_kw=1500.0,
        )
        for i in range(n_depots)
    ]
    routing = RoutingCache(cache=_build_mock_cache())
    vehicles = build_vehicles(config, [AUSTIN_CELLS[0]], seed=0)
    engine = SimulationEngine(
        config=config,
        vehicles=vehicles,
        requests=[],
        depots=depots,
        routing=routing,
    )
    m = engine.run()["metrics"]

    total_plugs = n_depots * chargers_per
    busy_s = engine._charger_busy_seconds
    util = busy_s / (total_plugs * horizon_s) * 100.0
    # One vehicle, one (primary) charge cycle — nowhere near using all plugs all day
    assert util < 2.0, f"expected heavy dilution, got {util:.3f}%"
    assert busy_s > 60.0, "should have at least ~1 min of scheduled charge time"
    avg_plugs_in_use = busy_s / horizon_s
    assert avg_plugs_in_use < total_plugs * 0.05, (
        f"time-average fleet-wide plugs delivering power ~{avg_plugs_in_use:.3f} "
        f"vs {total_plugs} installed — denominator still multiplies all plugs by full horizon"
    )
    # Charging should hit only the depot at the vehicle's cell (travel 0)
    by_depot = m["charger_utilization_by_depot_pct"]
    assert by_depot["d0"] > 1.0
    assert sum(by_depot[f"d{i}"] for i in range(1, n_depots)) < 0.05
    n_plugs = sum(d.chargers_count for d in depots)
    weighted = sum(by_depot[d.id] * d.chargers_count for d in depots) / n_plugs
    assert abs(weighted - m["charger_utilization_pct"]) < 0.15


def test_fifo_and_jit_same_util_when_no_depot_queue_forms() -> None:
    """If arrivals never stack, both policies run identical charging sessions."""
    def run(policy: str) -> tuple[float, float]:
        config = SimConfig(
            duration_minutes=45.0,
            seed=3,
            fleet_size=2,
            soc_initial=0.35,
            soc_target=0.80,
            soc_charge_start=0.80,
            reposition_enabled=False,
            timeseries_bucket_minutes=15.0,
            charging_queue_policy=policy,
        )
        depot = Depot(
            id="depot_0",
            h3_cell=AUSTIN_CELLS[0],
            chargers_count=4,
            charger_kw=150.0,
            site_power_kw=600.0,
        )
        routing = RoutingCache(cache=_build_mock_cache())
        vehicles = build_vehicles(config, [AUSTIN_CELLS[0]], seed=3)
        engine = SimulationEngine(
            config=config,
            vehicles=vehicles,
            requests=[],
            depots=[depot],
            routing=routing,
        )
        result = engine.run()
        return result["metrics"]["charger_utilization_pct"], engine._charger_busy_seconds

    u_jit, b_jit = run("jit")
    u_fifo, b_fifo = run("fifo")
    assert abs(b_jit - b_fifo) < 1.0
    assert abs(u_jit - u_fifo) < 0.05


def test_vehicle_charging_time_sum_near_busy_seconds() -> None:
    """Scheduled busy_seconds should match flushed CHARGING state time when runs finish cleanly."""
    config = SimConfig(
        duration_minutes=90.0,
        seed=5,
        fleet_size=1,
        soc_initial=0.20,
        soc_target=0.80,
        soc_charge_start=0.80,
        reposition_enabled=False,
        timeseries_bucket_minutes=30.0,
        charging_queue_policy="fifo",
    )
    depot = Depot(
        id="depot_0",
        h3_cell=AUSTIN_CELLS[0],
        chargers_count=2,
        charger_kw=150.0,
        site_power_kw=300.0,
    )
    routing = RoutingCache(cache=_build_mock_cache())
    vehicles = build_vehicles(config, [AUSTIN_CELLS[0]], seed=5)
    engine = SimulationEngine(
        config=config,
        vehicles=vehicles,
        requests=[],
        depots=[depot],
        routing=routing,
    )
    engine.run()
    total_v_chg = sum(v.time_charging_s for v in engine.vehicles.values())
    assert abs(total_v_chg - engine._charger_busy_seconds) < 2.0
