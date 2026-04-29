"""
Swappable repositioning policies.

Available policies
------------------
demand_score      DemandScorePolicy   blended reactive + forecast (default)
coverage_floor    CoverageFloorPolicy road-network coverage-deficit-first,
                                      then demand score

Factory
-------
    from app.sim.reposition_policies import build_policy

    policy = build_policy(
        name="coverage_floor",
        config=sim_config,
        forecast_table=forecast_table,
        demand_cells=demand_cells,
        covered_by=covered_by,   # from build_covered_by()
        min_coverage=1,
    )
"""
from typing import Optional

from .base import BaseRepositioningPolicy
from .demand_score import DemandScorePolicy
from .coverage_floor import CoverageFloorPolicy, build_covered_by

__all__ = [
    "BaseRepositioningPolicy",
    "DemandScorePolicy",
    "CoverageFloorPolicy",
    "build_covered_by",
    "build_policy",
]


def build_policy(
    name: str,
    *,
    alpha: float,
    half_life_minutes: float,
    forecast_horizon_minutes: float,
    max_reposition_travel_minutes: float,
    max_vehicles_targeting_cell: int,
    min_idle_minutes: float,
    top_k_cells: int,
    reposition_lambda: float,
    forecast_table: dict[str, float],
    demand_cells: Optional[set[str]] = None,
    covered_by: Optional[dict[str, frozenset[str]]] = None,
    max_wait_time_seconds: float = 600.0,
    min_coverage: int = 1,
    coverage_reposition_travel_minutes: Optional[float] = None,
    timed_forecast_table: Optional[dict[str, dict[int, float]]] = None,
    forecast_bucket_minutes: float = 15.0,
    coverage_lookahead_minutes: Optional[float] = None,
) -> BaseRepositioningPolicy:
    """
    Instantiate a repositioning policy by name.

    Parameters
    ----------
    name : "demand_score" | "coverage_floor"
    demand_cells : required when name == "coverage_floor"
    covered_by : precomputed road-network reachability dict; required for
                 coverage_floor.  Build via build_covered_by().
    max_wait_time_seconds : dispatch time budget; should match the threshold
                 used in build_covered_by() so the policy is self-consistent.
    min_coverage : minimum road-reachable eligible vehicles per demand cell
                   (coverage_floor only, default 1).
    coverage_reposition_travel_minutes : max travel time for coverage-deficit
                   repositioning moves (default = max_wait_time_seconds/60).
                   Kept separate from demand-score's max_reposition_travel_minutes
                   so deficit fills can reach farther cells.
    """
    ds = DemandScorePolicy(
        alpha=alpha,
        half_life_minutes=half_life_minutes,
        forecast_horizon_minutes=forecast_horizon_minutes,
        max_reposition_travel_minutes=max_reposition_travel_minutes,
        max_vehicles_targeting_cell=max_vehicles_targeting_cell,
        min_idle_minutes=min_idle_minutes,
        top_k_cells=top_k_cells,
        reposition_lambda=reposition_lambda,
        forecast_table=forecast_table,
    )

    if name == "demand_score":
        return ds

    if name == "coverage_floor":
        if demand_cells is None:
            raise ValueError("demand_cells is required for coverage_floor policy")
        if covered_by is None:
            raise ValueError(
                "covered_by is required for coverage_floor policy. "
                "Build it with build_covered_by(travel_cache_path, demand_cell_set, max_wait_time_seconds)."
            )
        return CoverageFloorPolicy(
            demand_score_policy=ds,
            demand_cells=demand_cells,
            covered_by=covered_by,
            max_wait_time_seconds=max_wait_time_seconds,
            min_coverage=min_coverage,
            coverage_reposition_travel_minutes=coverage_reposition_travel_minutes,
            timed_forecast_table=timed_forecast_table,
            forecast_bucket_minutes=forecast_bucket_minutes,
            coverage_lookahead_minutes=coverage_lookahead_minutes,
        )

    raise ValueError(
        f"Unknown repositioning policy: {name!r}. Choose 'demand_score' or 'coverage_floor'."
    )
