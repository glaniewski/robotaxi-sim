"""
Sanity checks per SPEC §18:
- Larger fleet should not worsen p90 wait (monotonic with tolerance).
- Repositioning on/off comparison should produce meaningful differences.
- Timeseries structure is correct.
"""
from __future__ import annotations

import pytest

from .conftest import run_mini_sim


def test_larger_fleet_does_not_worsen_p90_wait():
    """
    With the same requests and seed, a larger fleet should yield equal or
    better p90 wait time.  We allow a 1-minute tolerance.
    """
    small = run_mini_sim(n_requests=20, fleet_size=3, seed=42, duration_minutes=60.0)
    large = run_mini_sim(n_requests=20, fleet_size=10, seed=42, duration_minutes=60.0)

    p90_small = small["metrics"]["p90_wait_min"]
    p90_large = large["metrics"]["p90_wait_min"]

    assert p90_large <= p90_small + 1.0, (
        f"p90 worsened with larger fleet: small={p90_small:.2f} large={p90_large:.2f}"
    )


def test_timeseries_has_correct_structure():
    result = run_mini_sim(n_requests=10, fleet_size=5, seed=42, duration_minutes=30.0)
    ts = result["timeseries"]
    assert isinstance(ts, list)
    assert len(ts) > 0

    required_keys = {
        "t_minutes",
        "idle_count",
        "to_pickup_count",
        "in_trip_count",
        "charging_count",
        "repositioning_count",
        "pending_requests",
        "served_cumulative",
        "unserved_cumulative",
    }
    for bucket in ts:
        assert required_keys.issubset(bucket.keys()), (
            f"Missing keys in timeseries bucket: {required_keys - bucket.keys()}"
        )
        assert bucket["t_minutes"] >= 0.0


def test_timeseries_fleet_counts_partition_fleet_size():
    """idle + to_pickup + in_trip + charging + repositioning == fleet_size at each bucket."""
    result = run_mini_sim(n_requests=10, fleet_size=5, seed=42, duration_minutes=30.0)
    fleet = 5
    for bucket in result["timeseries"]:
        s = (
            bucket["idle_count"]
            + bucket["to_pickup_count"]
            + bucket["in_trip_count"]
            + bucket["charging_count"]
            + bucket["repositioning_count"]
        )
        assert s == fleet, (
            f"Fleet partition sum {s} != {fleet} at t={bucket['t_minutes']}"
        )


def test_timeseries_served_cumulative_nondecreasing():
    result = run_mini_sim(n_requests=15, fleet_size=5, seed=42, duration_minutes=30.0)
    ts = result["timeseries"]
    for i in range(1, len(ts)):
        assert ts[i]["served_cumulative"] >= ts[i - 1]["served_cumulative"], (
            f"served_cumulative decreased at t={ts[i]['t_minutes']}"
        )


def test_metrics_keys_present():
    result = run_mini_sim(n_requests=10, fleet_size=5, seed=1)
    m = result["metrics"]
    expected_keys = {
        "p10_wait_min", "median_wait_min", "p90_wait_min", "served_pct", "unserved_count",
        "served_count", "sla_adherence_pct",
        "trips_per_vehicle_per_day", "utilization_pct", "deadhead_pct",
        "repositioning_pct", "avg_dispatch_distance",
        "depot_queue_p90_min", "depot_queue_max_concurrent", "depot_queue_max_at_site", "charger_utilization_pct",
        "charger_utilization_by_depot_pct",
        "depot_arrivals_total",
        "depot_arrivals_by_depot_id",
        "depot_jit_plug_full_total",
        "depot_jit_plug_full_by_depot_id",
        "depot_charge_completions_total",
        "depot_charge_completions_by_depot_id",
        "depot_arrivals_peak_fleet_per_hour",
        "depot_arrivals_peak_max_site_per_hour",
        "depot_charge_completions_peak_fleet_per_hour",
        "depot_charge_completions_peak_max_site_per_hour",
        "charging_session_duration_median_min",
        "charging_session_duration_p90_min",
        "fleet_battery_pct", "fleet_soc_median_pct",
        "vehicles_below_soc_target_count", "vehicles_below_soc_target_strict_count", "total_charge_sessions",
        "energy_cost", "demand_cost", "maintenance_cost", "fleet_fixed_cost",
        "infra_cost", "total_system_cost", "total_system_cost_per_trip",
        "system_margin_per_trip", "depreciation_per_vehicle_day",
        "cost_per_trip", "cost_per_mile", "fixed_cost_total", "avg_revenue_per_trip",
        "revenue_total", "total_margin", "pool_match_pct",
        "contribution_margin_per_trip",
    }
    missing = expected_keys - m.keys()
    assert not missing, f"Missing metrics keys: {missing}"


def test_zero_requests_does_not_crash():
    result = run_mini_sim(n_requests=0, fleet_size=5, seed=0)
    m = result["metrics"]
    assert m["served_count"] == 0
    assert m["unserved_count"] == 0
