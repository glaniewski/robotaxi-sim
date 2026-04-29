"""
Determinism: same seed → identical metrics and timeseries.
"""
from __future__ import annotations

import pytest

from .conftest import run_mini_sim


def test_same_seed_same_metrics():
    result_a = run_mini_sim(n_requests=20, fleet_size=5, seed=7)
    result_b = run_mini_sim(n_requests=20, fleet_size=5, seed=7)
    assert result_a["metrics"] == result_b["metrics"]


def test_same_seed_same_timeseries():
    result_a = run_mini_sim(n_requests=20, fleet_size=5, seed=99)
    result_b = run_mini_sim(n_requests=20, fleet_size=5, seed=99)
    assert result_a["timeseries"] == result_b["timeseries"]


def test_different_seeds_may_differ():
    result_a = run_mini_sim(n_requests=20, fleet_size=5, seed=1)
    result_b = run_mini_sim(n_requests=20, fleet_size=5, seed=2)
    # With synthetic fixed routing the requests themselves differ by seed
    # so served counts can differ.  Just ensure it doesn't crash.
    assert "metrics" in result_a
    assert "metrics" in result_b
