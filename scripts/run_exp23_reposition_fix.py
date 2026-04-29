"""
Experiment 23 — Accurate repositioning position tracking

Fixes an inaccuracy in the sim where repositioning vehicles were treated as
sitting at their ORIGIN cell A throughout the transit to B:
  - Coverage tracking now shifts from A to B at reposition START
  - Dispatch ETA for repositioning vehicles = routing(B, pickup) + remaining_s
    (conservative: never underestimates actual customer wait)
  - VehicleIndex also shifted to B at start so spatial pre-filter is accurate

This ensures:
  1. Coverage correctly reflects where vehicles are going, not where they were
  2. A mid-reposition vehicle is never dispatched for a trip it cannot reach
     within max_wait_time_seconds from its actual in-transit position

Variants (fleet=3000, scale=0.01, seed=123, 24h, max_wait=600s, demand_init):
  A  demand_score                    (baseline)
  B  coverage_floor+optB look=30 travel=30  (best from Exp 22, now with fix)

We expect:
  - served% may change slightly (some previously optimistic dispatches now correctly
    rejected, replaced by nearer IDLE vehicles or serve requests slightly later)
  - p90/p99 wait times should be more accurate reflections of real customer experience
  - Problem cell unserved count should remain near 1 from Exp 22

Run from repo root:
    python3 scripts/run_exp23_reposition_fix.py
"""
from __future__ import annotations

import os, sys, time
from collections import defaultdict
from dataclasses import dataclass
from typing import Optional

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "backend"))

import numpy as np
import pandas as pd

from app.sim.demand import load_requests
from app.sim.engine import SimConfig, SimulationEngine, build_vehicles
from app.sim.entities import Request, RequestStatus
from app.sim.reposition_policies import CoverageFloorPolicy, build_covered_by, build_policy
from app.sim.routing import RoutingCache

REQUESTS_PATH = os.path.join(ROOT, "data", "requests_austin_h3_r8.parquet")
TRAVEL_CACHE  = os.path.join(ROOT, "data", "h3_travel_cache.parquet")
OSRM_URL      = os.environ.get("OSRM_URL", "http://localhost:5001")

FLEET    = 3_000
SCALE    = 0.01
SEED     = 123
DURATION = 1440
MAX_WAIT = 600.0
BUCKET_MIN = 15.0

DEPOT_CELL   = "88489e3467fffff"
PROBLEM_CELL = "88489e30cbfffff"


@dataclass
class Variant:
    label: str
    policy_name: str
    use_timed: bool = False
    lookahead_min: Optional[float] = None
    travel_min: float = 20.0


VARIANTS = [
    Variant("A  demand_score              (baseline)", "demand_score",   False, None,  20.0),
    Variant("B  coverage_floor+optB look=30 travel=30","coverage_floor", True,  30.0,  30.0),
]


def _fmt(s: float) -> str:
    if not (0 <= s < 36000):
        return "--:--"
    return f"{int(s // 60):02d}:{int(s % 60):02d}"


def _build_forecast_table(requests, duration_min: float) -> dict[str, float]:
    counts: dict[str, int] = defaultdict(int)
    for r in requests:
        counts[r.origin_h3] += 1
    dur_s = duration_min * 60.0
    return {cell: cnt / dur_s for cell, cnt in counts.items()}


def _build_timed_forecast_table(requests, bucket_min: float = 15.0) -> dict[str, dict[int, float]]:
    bucket_s = bucket_min * 60.0
    n_buckets = int(round(1440.0 / bucket_min))
    counts: dict[str, dict[int, int]] = {}
    for r in requests:
        cell = r.origin_h3
        b = int(r.request_time / bucket_s) % n_buckets
        if cell not in counts:
            counts[cell] = {}
        counts[cell][b] = counts[cell].get(b, 0) + 1
    return {
        cell: {b: c / bucket_s for b, c in bmap.items()}
        for cell, bmap in counts.items()
    }


def run_variant(
    v: Variant,
    base_requests,
    routing: RoutingCache,
    demand_cells_weights: dict[str, float],
    demand_cell_set: set[str],
    covered_by: Optional[dict[str, frozenset[str]]],
    timed_forecast: Optional[dict[str, dict[int, float]]],
) -> dict:
    requests = [
        Request(
            id=r.id, request_time=r.request_time,
            origin_h3=r.origin_h3, destination_h3=r.destination_h3,
            max_wait_time_seconds=MAX_WAIT,
        )
        for r in base_requests
    ]

    sim_config = SimConfig(
        duration_minutes=DURATION, seed=SEED, fleet_size=FLEET,
        max_wait_time_seconds=MAX_WAIT, reposition_enabled=True,
        reposition_alpha=0.6, reposition_top_k_cells=50,
        max_vehicles_targeting_cell=3,
    )

    vehicles = build_vehicles(
        sim_config, depot_h3_cells=[DEPOT_CELL],
        seed=SEED, demand_cells=demand_cells_weights,
    )

    forecast_table = _build_forecast_table(requests, DURATION)
    policy = build_policy(
        name=v.policy_name,
        alpha=sim_config.reposition_alpha,
        half_life_minutes=sim_config.reposition_half_life_minutes,
        forecast_horizon_minutes=sim_config.reposition_forecast_horizon_minutes,
        max_reposition_travel_minutes=sim_config.max_reposition_travel_minutes,
        max_vehicles_targeting_cell=sim_config.max_vehicles_targeting_cell,
        min_idle_minutes=sim_config.reposition_min_idle_minutes,
        top_k_cells=sim_config.reposition_top_k_cells,
        reposition_lambda=sim_config.reposition_lambda,
        forecast_table=forecast_table,
        demand_cells=demand_cell_set,
        covered_by=covered_by,
        max_wait_time_seconds=MAX_WAIT,
        min_coverage=1,
        coverage_reposition_travel_minutes=v.travel_min,
        timed_forecast_table=timed_forecast if v.use_timed else None,
        forecast_bucket_minutes=BUCKET_MIN,
        coverage_lookahead_minutes=v.lookahead_min,
    )

    t0 = time.time()

    def _progress(n_done, n_total):
        pct = n_done / n_total * 100 if n_total else 0
        elapsed = time.time() - t0
        rate = n_done / elapsed if elapsed > 0 else 0
        eta = (n_total - n_done) / rate if rate > 0 else float("inf")
        bar_w = 30
        filled = int(pct / 100 * bar_w)
        bar = "█" * filled + "░" * (bar_w - filled)
        print(f"\r    [{bar}] {pct:5.1f}%  {_fmt(elapsed)}<{_fmt(eta)}  {rate:,.0f} t/s",
              end="", flush=True)

    engine = SimulationEngine(
        config=sim_config, vehicles=vehicles, requests=requests,
        depots=[], routing=routing, reposition_policy=policy,
        progress_callback=_progress,
    )
    result = engine.run()
    elapsed = time.time() - t0
    print(f"\r    done {_fmt(elapsed)}" + " " * 60)

    m  = result["metrics"]
    ec = result["event_counts"]

    req_list  = list(engine.requests.values())
    served    = [r for r in req_list if r.status == RequestStatus.SERVED]
    unserved  = [r for r in req_list if r.status == RequestStatus.UNSERVED]
    wait_s    = [r.actual_wait_seconds for r in served if r.actual_wait_seconds is not None]
    p50 = float(np.percentile(wait_s, 50)) / 60 if wait_s else 0.0
    p90 = float(np.percentile(wait_s, 90)) / 60 if wait_s else 0.0
    p99 = float(np.percentile(wait_s, 99)) / 60 if wait_s else 0.0

    prob_unserved  = sum(1 for r in unserved if r.origin_h3 == PROBLEM_CELL)
    other_unserved = len(unserved) - prob_unserved

    if isinstance(policy, CoverageFloorPolicy):
        t_end = DURATION * 60.0
        mc = policy._min_cov_for(PROBLEM_CELL, t_end)
        cov = policy._coverage.get(PROBLEM_CELL, -1)
        print(f"    min_cov(problem@end)={mc}  coverage(problem@end)={cov}  deficit_cells={policy.deficit_count}")

    print(f"    served={m['served_pct']:.3f}%  p50={p50:.1f}m  p90={p90:.1f}m  p99={p99:.1f}m  wall={elapsed:.0f}s")
    print(f"    unserved: {PROBLEM_CELL[:8]}={prob_unserved}  other={other_unserved}")
    print(f"    REPOSITION_COMPLETE={ec.get('REPOSITION_COMPLETE',0):,}  DISPATCH={ec.get('DISPATCH',0):,}")

    return {
        "served_pct":       m["served_pct"],
        "p50":              round(p50, 2),
        "p90":              round(p90, 2),
        "p99":              round(p99, 2),
        "wall_s":           round(elapsed),
        "repo":             ec.get("REPOSITION_COMPLETE", 0),
        "unserved_problem": prob_unserved,
        "unserved_other":   other_unserved,
    }


def main():
    print("Experiment 23: accurate repositioning position (current_h3 = target at start)")
    print(f"  fleet={FLEET}, scale={SCALE}, seed={SEED}, max_wait={MAX_WAIT}s")
    print(f"  Compare to Exp 22: A=98.260% (58 unserved), B=98.890% (1 unserved)\n")

    base_requests = load_requests(
        REQUESTS_PATH, duration_minutes=DURATION,
        max_wait_time_seconds=MAX_WAIT, demand_scale=SCALE, seed=SEED,
    )
    print(f"  {len(base_requests):,} requests\n")

    routing = RoutingCache(parquet_path=TRAVEL_CACHE, osrm_url=OSRM_URL)

    _df = pd.read_parquet(REQUESTS_PATH, columns=["origin_h3"])
    demand_cells_weights = _df["origin_h3"].value_counts().to_dict()
    demand_cell_set = set(demand_cells_weights.keys())

    timed_forecast = _build_timed_forecast_table(base_requests, bucket_min=BUCKET_MIN)
    covered_by = build_covered_by(TRAVEL_CACHE, demand_cell_set, MAX_WAIT)

    results = []
    for v in VARIANTS:
        cb = covered_by if v.policy_name == "coverage_floor" else None
        print(f"  ── Variant {v.label}")
        r = run_variant(v, base_requests, routing, demand_cells_weights,
                        demand_cell_set, cb, timed_forecast)
        results.append((v, r))
        print()

    print("=" * 110)
    print(f"  {'variant':<45}  {'served%':>8}  {'p50':>5}  {'p90':>5}  {'p99':>5}  "
          f"{'REPO':>8}  {'wall':>5}  {'Δserved':>8}  {'@prob':>6}  {'@other':>6}")
    print("-" * 110)
    base_pct = results[0][1]["served_pct"]
    for v, r in results:
        delta = r["served_pct"] - base_pct
        sign  = "+" if delta >= 0 else ""
        print(
            f"  {v.label:<45}  {r['served_pct']:>7.3f}%  {r['p50']:>4.1f}m  {r['p90']:>4.1f}m  {r['p99']:>4.1f}m  "
            f"{r['repo']:>8,}  {r['wall_s']:>4}s  {sign}{delta:>+.3f}%  "
            f"{r['unserved_problem']:>6}  {r['unserved_other']:>6}"
        )
    print("=" * 110)
    print(f"\n  Exp 22 reference: A=98.260%/p90=2.1m  B=98.890%/p90=2.5m  (old inaccurate model)")


if __name__ == "__main__":
    main()
