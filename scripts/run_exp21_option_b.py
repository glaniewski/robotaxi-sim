"""
Experiment 21 — Option B: time-varying min_cov via timed forecast table

Key idea: instead of a static min_coverage floor, CoverageFloorPolicy now
computes min_cov(cell, t) by looking ahead `lookahead_s` in a 15-minute-bucket
historical demand forecast:

    min_cov(cell, t) = max(global_floor,
                           ceil(rate_at(cell, t + lookahead_s) * lookahead_s))

This pre-positions vehicles BEFORE demand bursts.  The problem cell
(88489e30cbfffff) bursts 4-9 AM at ~14.8 trips/hr.  At 3:30 AM, the policy
looks ahead 20 min, sees the 4 AM bucket rate = 14.8/hr, computes
min_cov = ceil(14.8/hr * 1200s) = 5, and starts pre-positioning vehicles.
They arrive by 4 AM, before the first trips expire (max_wait=600s).

Variants (fleet=3000, scale=0.01, seed=123, 24h, max_wait=600s, demand_init):
  A  demand_score        (baseline, no coverage floor)
  B  coverage_floor      static min_cov=1  (Exp 20 best)
  C  coverage_floor      Option B timed forecast, global_floor=1, lookahead=20min
  D  coverage_floor      Option B timed forecast, global_floor=1, lookahead=30min

Run from repo root:
    python3 scripts/run_exp21_option_b.py
"""
from __future__ import annotations

import os, sys, time
from collections import defaultdict
from dataclasses import dataclass, field
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

DEPOT_CELL   = "88489e3467fffff"
PROBLEM_CELL = "88489e30cbfffff"   # rank #4, source of all unserved trips

BUCKET_MINUTES = 15.0   # 15-min forecast buckets (96 per day)


@dataclass
class Variant:
    label: str
    policy_name: str
    min_coverage: int = 1
    use_timed_forecast: bool = False
    lookahead_minutes: Optional[float] = None   # None = default (=coverage_reposition_travel_min)


VARIANTS = [
    Variant("A  demand_score         (baseline)",     "demand_score",   1, False, None),
    Variant("B  coverage_floor       static min=1",   "coverage_floor", 1, False, None),
    Variant("C  coverage_floor+optB  floor=1 look=20","coverage_floor", 1, True,  20.0),
    Variant("D  coverage_floor+optB  floor=1 look=30","coverage_floor", 1, True,  30.0),
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


def _build_timed_forecast_table(
    requests, bucket_minutes: float = 15.0
) -> dict[str, dict[int, float]]:
    bucket_s = bucket_minutes * 60.0
    n_buckets = int(round(1440.0 / bucket_minutes))
    counts: dict[str, dict[int, int]] = {}
    for r in requests:
        cell = r.origin_h3
        bucket = int(r.request_time / bucket_s) % n_buckets
        if cell not in counts:
            counts[cell] = {}
        counts[cell][bucket] = counts[cell].get(bucket, 0) + 1
    return {
        cell: {b: c / bucket_s for b, c in bmap.items()}
        for cell, bmap in counts.items()
    }


def _problem_cell_bucket_rates(timed_table: dict[str, dict[int, float]]) -> None:
    """Print the per-bucket arrival rate for the problem cell."""
    bmap = timed_table.get(PROBLEM_CELL, {})
    print(f"  Problem cell {PROBLEM_CELL[:8]} bucket rates (trips/hr) — top 10 buckets:")
    sorted_buckets = sorted(bmap.items(), key=lambda x: -x[1])
    for b, rate in sorted_buckets[:10]:
        hour_start = b * BUCKET_MINUTES / 60
        rate_hr = rate * 3600
        print(f"    bucket {b:3d} ({hour_start:4.1f}h): {rate_hr:.2f} trips/hr  →  "
              f"min_cov(lookahead=20m)={max(1, int(rate * 1200) + (1 if (rate * 1200) % 1 else 0))}")
    print()


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
            id=r.id,
            request_time=r.request_time,
            origin_h3=r.origin_h3,
            destination_h3=r.destination_h3,
            max_wait_time_seconds=MAX_WAIT,
        )
        for r in base_requests
    ]

    sim_config = SimConfig(
        duration_minutes=DURATION,
        seed=SEED,
        fleet_size=FLEET,
        max_wait_time_seconds=MAX_WAIT,
        reposition_enabled=True,
        reposition_alpha=0.6,
        reposition_top_k_cells=50,
        max_vehicles_targeting_cell=3,
    )

    vehicles = build_vehicles(
        sim_config,
        depot_h3_cells=[DEPOT_CELL],
        seed=SEED,
        demand_cells=demand_cells_weights,
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
        min_coverage=v.min_coverage,
        coverage_reposition_travel_minutes=20.0,  # same as Exp 20
        timed_forecast_table=timed_forecast if v.use_timed_forecast else None,
        forecast_bucket_minutes=BUCKET_MINUTES,
        coverage_lookahead_minutes=v.lookahead_minutes,
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

    deficit_final  = None
    cov_problem    = None
    if isinstance(policy, CoverageFloorPolicy):
        deficit_final = policy.deficit_count
        cov_problem   = policy._coverage.get(PROBLEM_CELL, -1)
        # What did the policy think min_cov was for problem cell at end of day?
        final_min_cov = policy._min_cov_for(PROBLEM_CELL, DURATION * 60.0)
        print(f"    min_cov(problem_cell, end)={final_min_cov}  "
              f"coverage[problem_cell]@end={cov_problem}  deficit_cells={deficit_final}")

    print(f"    served={m['served_pct']:.3f}%  p50={p50:.1f}m  p90={p90:.1f}m  p99={p99:.1f}m  wall={elapsed:.0f}s")
    print(f"    unserved from {PROBLEM_CELL[:8]}: {prob_unserved}  other cells: {other_unserved}")
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
        "deficit_final":    deficit_final,
        "cov_problem":      cov_problem,
    }


def main():
    print("Experiment 21: Option B — time-varying min_cov from timed forecast table")
    print(f"  fleet={FLEET}, scale={SCALE}, seed={SEED}, duration={DURATION}min, max_wait={MAX_WAIT}s")
    print(f"  bucket_size={BUCKET_MINUTES}min, demand_init=True\n")

    base_requests = load_requests(
        REQUESTS_PATH, duration_minutes=DURATION,
        max_wait_time_seconds=MAX_WAIT, demand_scale=SCALE, seed=SEED,
    )
    print(f"  {len(base_requests):,} requests\n")

    routing = RoutingCache(parquet_path=TRAVEL_CACHE, osrm_url=OSRM_URL)

    _df = pd.read_parquet(REQUESTS_PATH, columns=["origin_h3"])
    demand_cells_weights = _df["origin_h3"].value_counts().to_dict()
    demand_cell_set = set(demand_cells_weights.keys())

    # Build timed forecast and show problem-cell profile
    timed_forecast = _build_timed_forecast_table(base_requests, bucket_minutes=BUCKET_MINUTES)
    _problem_cell_bucket_rates(timed_forecast)

    covered_by = build_covered_by(TRAVEL_CACHE, demand_cell_set, MAX_WAIT)
    inbound_count = sum(1 for cb in covered_by.values() if PROBLEM_CELL in cb)
    print(f"  covered_by built: {len(covered_by):,} origins")
    print(f"  Gateway cells (can reach problem cell ≤600s): {inbound_count}\n")

    results = []
    for v in VARIANTS:
        cb = covered_by if v.policy_name == "coverage_floor" else None
        print(f"  ── Variant {v.label}")
        r = run_variant(
            v, base_requests, routing, demand_cells_weights,
            demand_cell_set, cb, timed_forecast,
        )
        results.append((v, r))
        print()

    print("=" * 115)
    print(f"  {'variant':<45}  {'served%':>8}  {'p50':>5}  {'p90':>5}  {'p99':>5}  "
          f"{'REPO':>8}  {'wall':>5}  {'Δserved':>8}  {'@prob':>6}  {'@other':>7}")
    print("-" * 115)
    base_pct = results[0][1]["served_pct"]
    for v, r in results:
        delta = r["served_pct"] - base_pct
        sign  = "+" if delta >= 0 else ""
        print(
            f"  {v.label:<45}  {r['served_pct']:>7.3f}%  {r['p50']:>4.1f}m  {r['p90']:>4.1f}m  {r['p99']:>4.1f}m  "
            f"{r['repo']:>8,}  {r['wall_s']:>4}s  {sign}{delta:>+.3f}%  {r['unserved_problem']:>6}  {r['unserved_other']:>7}"
        )
    print("=" * 115)


if __name__ == "__main__":
    main()
