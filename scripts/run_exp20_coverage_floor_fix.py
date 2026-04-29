"""
Experiment 20 — Coverage floor with self-coverage fix + wider reposition radius

Two bugs fixed since Exp 18:
  1. build_covered_by now includes self-coverage: a vehicle AT cell X counts
     toward _coverage[X].  Previously vehicles parked at a cell were invisible
     to their own coverage counter.
  2. CoverageFloorPolicy now uses coverage_reposition_travel_minutes
     (default = max_wait_time_seconds/60 = 10min at 600s) instead of
     demand_score's max_reposition_travel_minutes (12min).  This is the SAME
     here, but the parameter is now independent — set it higher if needed.

Root cause (from diagnostic): ALL 61 unserved trips come from a SINGLE cell,
  88489e30cbfffff (rank #4, ~33k historical trips).
  min_inbound = 257s from 4 gateway cells → reachable in time, but gateway
  cells are emptied by 4 AM as demand_score repositions vehicles downtown.
  Coverage_floor should detect and refill this cell.

Variants (fleet=3000, scale=0.01, seed=123, 24h, max_wait=600s, demand_init):
  A  demand_score   min_cov=—   (baseline)
  B  coverage_floor min_cov=1
  C  coverage_floor min_cov=2
  D  coverage_floor min_cov=3
  E  coverage_floor min_cov=4

Run from repo root:
    python3 scripts/run_exp20_coverage_floor_fix.py
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

DEPOT_CELL    = "88489e3467fffff"
PROBLEM_CELL  = "88489e30cbfffff"   # rank #4, all unserved trips at 600s


@dataclass
class Variant:
    label: str
    policy_name: str
    min_coverage: int = 1


VARIANTS = [
    Variant("A  demand_score   min_cov=—", "demand_score",   1),
    Variant("B  coverage_floor min_cov=1", "coverage_floor", 1),
    Variant("C  coverage_floor min_cov=2", "coverage_floor", 2),
    Variant("D  coverage_floor min_cov=3", "coverage_floor", 3),
    Variant("E  coverage_floor min_cov=4", "coverage_floor", 4),
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


def run_variant(
    v: Variant,
    base_requests,
    routing,
    demand_cells_weights: dict[str, float],
    demand_cell_set: set[str],
    covered_by: Optional[dict[str, frozenset[str]]],
):
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
        # coverage_reposition_travel_minutes defaults to max_wait_time_seconds/60 = 10min
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

    # Problem-cell diagnostics
    prob_unserved = sum(1 for r in unserved if r.origin_h3 == PROBLEM_CELL)
    other_unserved = len(unserved) - prob_unserved

    deficit_final  = None
    zero_cov_final = None
    cov_problem    = None
    cov_moves      = None
    if isinstance(policy, CoverageFloorPolicy):
        deficit_final  = policy.deficit_count
        zero_cov_final = policy.zero_coverage_count
        cov_problem    = policy._coverage.get(PROBLEM_CELL, -1)
        cov_moves      = ec.get("REPOSITION_COMPLETE", 0) - (0 if v.policy_name == "demand_score" else 0)

    print(f"    served={m['served_pct']:.3f}%  p50={p50:.1f}m  p90={p90:.1f}m  wall={elapsed:.0f}s")
    print(f"    unserved from {PROBLEM_CELL[:8]}: {prob_unserved}  other cells: {other_unserved}")
    print(f"    REPOSITION_COMPLETE={ec.get('REPOSITION_COMPLETE',0):,}  DISPATCH={ec.get('DISPATCH',0):,}")
    if isinstance(policy, CoverageFloorPolicy):
        print(f"    coverage[problem_cell]@end={cov_problem}  deficit_cells={deficit_final}  zero_cov={zero_cov_final}")

    return {
        "served_pct": m["served_pct"],
        "p50": round(p50, 2),
        "p90": round(p90, 2),
        "wall_s": round(elapsed),
        "repo": ec.get("REPOSITION_COMPLETE", 0),
        "unserved_problem": prob_unserved,
        "unserved_other": other_unserved,
        "deficit_final": deficit_final,
        "zero_cov_final": zero_cov_final,
    }


def main():
    print(f"Experiment 20: coverage_floor self-coverage fix + wider reposition radius")
    print(f"  Problem cell: {PROBLEM_CELL} (rank #4, all unserved trips at 600s)")
    print(f"  max_wait=600s, fleet=3000, demand_init\n")

    base_requests = load_requests(REQUESTS_PATH, duration_minutes=DURATION,
        max_wait_time_seconds=MAX_WAIT, demand_scale=SCALE, seed=SEED)
    print(f"  {len(base_requests):,} requests\n")

    routing = RoutingCache(parquet_path=TRAVEL_CACHE, osrm_url=OSRM_URL)

    _df = pd.read_parquet(REQUESTS_PATH, columns=["origin_h3"])
    demand_cells_weights = _df["origin_h3"].value_counts().to_dict()
    demand_cell_set = set(demand_cells_weights.keys())

    covered_by = build_covered_by(TRAVEL_CACHE, demand_cell_set, MAX_WAIT)
    # Verify self-coverage fix
    prob_self = PROBLEM_CELL in covered_by.get(PROBLEM_CELL, frozenset())
    # How many cells can road-reach the problem cell in ≤600s?
    inbound_count = sum(1 for cb in covered_by.values() if PROBLEM_CELL in cb)
    print(f"  covered_by built: {len(covered_by):,} origins")
    print(f"  Problem cell self-covered: {prob_self}")
    print(f"  Demand cells that can road-reach problem cell in ≤600s: {inbound_count}")
    print(f"  (these are the 'gateway cells' + problem cell itself)\n")

    results = []
    for v in VARIANTS:
        cb = covered_by if v.policy_name == "coverage_floor" else None
        print(f"  ── Variant {v.label}")
        r = run_variant(v, base_requests, routing, demand_cells_weights, demand_cell_set, cb)
        results.append((v, r))
        print()

    print("=" * 105)
    print(f"  {'variant':<38}  {'served%':>8}  {'p50':>5}  {'p90':>5}  "
          f"{'REPO':>8}  {'wall':>5}  {'Δserved':>8}  {'unserved@prob':>14}  {'unserved@other':>14}")
    print("-" * 105)
    base_pct = results[0][1]["served_pct"]
    for v, r in results:
        delta = r["served_pct"] - base_pct
        sign  = "+" if delta >= 0 else ""
        print(
            f"  {v.label:<38}  {r['served_pct']:>7.3f}%  {r['p50']:>4.1f}m  {r['p90']:>4.1f}m  "
            f"{r['repo']:>8,}  {r['wall_s']:>4}s  {sign}{delta:>+.3f}%  {r['unserved_problem']:>14}  {r['unserved_other']:>14}"
        )
    print("=" * 105)


if __name__ == "__main__":
    main()
