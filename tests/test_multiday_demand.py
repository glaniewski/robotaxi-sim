"""Repeated-day demand stream for one continuous simulation clock."""
from __future__ import annotations

from app.schemas import DemandConfig, ScenarioConfig
from app.sim.demand import load_requests, load_requests_repeated_days


def test_scenario_effective_duration_minutes():
    single = ScenarioConfig(duration_minutes=1440.0, demand=DemandConfig())
    assert single.effective_duration_minutes() == 1440.0
    tiled = ScenarioConfig(
        duration_minutes=999.0,
        demand=DemandConfig(repeat_num_days=3, duration_minutes_per_day=120.0),
    )
    assert tiled.effective_duration_minutes() == 360.0
    default_per_day = ScenarioConfig(
        duration_minutes=4320.0,
        demand=DemandConfig(repeat_num_days=3),
    )
    assert default_per_day.effective_duration_minutes() == 4320.0


def test_load_requests_repeated_days_count_and_times(tmp_path_factory):
    # Use tiny in-memory path: project data file
    import os

    pq = os.path.join(
        os.path.dirname(__file__),
        "..",
        "data",
        "requests_austin_h3_r8.parquet",
    )
    pq = os.path.normpath(pq)
    if not os.path.isfile(pq):
        import pytest

        pytest.skip("requests parquet not present")

    one = load_requests(
        pq,
        duration_minutes=120.0,
        max_wait_time_seconds=600.0,
        demand_scale=0.01,
        seed=99,
    )
    n1 = len(one)
    assert n1 > 0
    three = load_requests_repeated_days(
        pq,
        duration_minutes_per_day=120.0,
        num_days=3,
        max_wait_time_seconds=600.0,
        demand_scale=0.01,
        seed=99,
    )
    assert len(three) == 3 * n1
    day_s = 120.0 * 60.0
    assert three[0].request_time < day_s
    assert three[-1].request_time < 3 * day_s
    # First request of day 2 ≈ first of day 1 + day_s
    day2_first = next(r for r in three if r.id.startswith("req_d1_"))
    assert abs((day2_first.request_time - one[0].request_time) - day_s) < 1e-6
    ids = {r.id for r in three}
    assert len(ids) == len(three)
