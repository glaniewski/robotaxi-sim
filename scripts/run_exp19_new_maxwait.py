"""
Experiment 19 — min_coverage sweep with max_wait_time_seconds = 1740s

Same structure as Experiment 18 but with the dispatch budget raised to 1740s,
the minimum required to road-reach all 15 inbound-isolated demand cells.

Note: max_wait_time_seconds affects both the coverage tracking (covered_by is
rebuilt at the new threshold) AND the dispatch radius (vehicles up to 29min
away are eligible for assignment). This will change p50/p90 wait times.

Variants (fleet=3000, scale=0.01, seed=123, 24h, demand_init):
  A  demand_score   min_cov=—    (baseline at new max_wait)
  B  coverage_floor min_cov=1
  C  coverage_floor min_cov=2
  D  coverage_floor min_cov=3

Run from repo root:
    python3 scripts/run_exp19_new_maxwait.py
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

# max_wait_time_seconds computed by analyze_isolated_cells.py:
# minimum value to make every isolated demand cell inbound-reachable from
# at least one other demand cell via the road network.
MAX_WAIT = 1740.0   # seconds (~29 min)

DEPOT_CELL = "88489e3467fffff"   # downtown Austin


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
    )

    t0 = time.time()

    def _progress(n_done, n_total):
        pct = n_done / n_total * 100 if n_total else 0
        elapsed = time.time() - t0
        rate = n_done / elapsed if elapsed > 0 else 0
        eta = (n_total - n_done) / rate if rate > 0 else float("inf")
        bar_w = 34
        filled = int(pct / 100 * bar_w)
        bar = "█" * filled + "░" * (bar_w - filled)
        print(
            f"\r    [{bar}] {pct:5.1f}%  {_fmt(elapsed)}<{_fmt(eta)}  {rate:,.0f} t/s",
            end="",
            flush=True,
        )

    engine = SimulationEngine(
        config=sim_config,
        vehicles=vehicles,
        requests=requests,
        depots=[],
        routing=routing,
        reposition_policy=policy,
        progress_callback=_progress,
    )
    result = engine.run()
    elapsed = time.time() - t0
    print(f"\r    done {_fmt(elapsed)}" + " " * 60)

    m  = result["metrics"]
    ec = result["event_counts"]

    req_list = list(engine.requests.values())
    served   = [r for r in req_list if r.status == RequestStatus.SERVED]
    wait_s   = [r.actual_wait_seconds for r in served if r.actual_wait_seconds is not None]
    p50 = float(np.percentile(wait_s, 50)) / 60 if wait_s else 0.0
    p90 = float(np.percentile(wait_s, 90)) / 60 if wait_s else 0.0
    p99 = float(np.percentile(wait_s, 99)) / 60 if wait_s else 0.0

    deficit_final    = None
    zero_cov_final   = None
    if isinstance(policy, CoverageFloorPolicy):
        deficit_final  = policy.deficit_count
        zero_cov_final = policy.zero_coverage_count

    print(f"    served={m['served_pct']:.3f}%  p50={p50:.1f}m  p90={p90:.1f}m  p99={p99:.1f}m  wall={elapsed:.0f}s")
    print(f"    REPOSITION_COMPLETE={ec.get('REPOSITION_COMPLETE', 0):,}  "
          f"DISPATCH={ec.get('DISPATCH', 0):,}  VEHICLE_IDLE={ec.get('VEHICLE_IDLE', 0):,}")
    if deficit_final is not None:
        print(f"    coverage deficit cells at end: {deficit_final}  "
              f"zero-coverage cells at end: {zero_cov_final}")

    return {
        "served_pct": m["served_pct"],
        "p50": round(p50, 2),
        "p90": round(p90, 2),
        "p99": round(p99, 2),
        "wall_s": round(elapsed),
        "repo": ec.get("REPOSITION_COMPLETE", 0),
        "dispatch": ec.get("DISPATCH", 0),
        "unserved": m["unserved_count"],
        "deficit_final": deficit_final,
        "zero_cov_final": zero_cov_final,
    }


def main():
    print(f"Experiment 19: min_coverage sweep at max_wait={MAX_WAIT:.0f}s ({MAX_WAIT/60:.1f}min)")
    print(f"Loading requests (scale={SCALE}) …")
    base_requests = load_requests(
        parquet_path=REQUESTS_PATH,
        duration_minutes=DURATION,
        max_wait_time_seconds=MAX_WAIT,
        demand_scale=SCALE,
        seed=SEED,
    )
    print(f"  {len(base_requests):,} requests  fleet={FLEET:,}\n")

    routing = RoutingCache(parquet_path=TRAVEL_CACHE, osrm_url=OSRM_URL)

    print("Loading demand cells …")
    _df = pd.read_parquet(REQUESTS_PATH, columns=["origin_h3"])
    _counts = _df["origin_h3"].value_counts()
    demand_cells_weights = _counts.to_dict()
    demand_cell_set = set(demand_cells_weights.keys())
    print(f"  {len(demand_cell_set):,} unique demand cells\n")

    print(f"Building covered_by dict at max_wait={MAX_WAIT:.0f}s …")
    t0 = time.time()
    covered_by = build_covered_by(
        travel_cache_path=TRAVEL_CACHE,
        demand_cell_set=demand_cell_set,
        max_wait_time_seconds=MAX_WAIT,
    )
    reachable_counts = [len(v) for v in covered_by.values()]
    print(
        f"  Built in {time.time()-t0:.1f}s  origins with ≥1 reachable dest: {len(covered_by):,}"
    )
    if reachable_counts:
        print(
            f"  Reachable dests per origin: "
            f"p50={int(np.percentile(reachable_counts, 50))}  "
            f"p90={int(np.percentile(reachable_counts, 90))}  "
            f"max={max(reachable_counts)}"
        )
    cells_zero_reachable = demand_cell_set - set(covered_by.keys())
    print(f"  Demand cells still with 0 reachable at {MAX_WAIT:.0f}s: {len(cells_zero_reachable)}\n")

    results = []
    for v in VARIANTS:
        cb = covered_by if v.policy_name == "coverage_floor" else None
        print(f"  ── Variant {v.label}")
        r = run_variant(v, base_requests, routing, demand_cells_weights, demand_cell_set, cb)
        results.append((v, r))
        print()

    print("=" * 108)
    print(
        f"  {'variant':<38}  {'served%':>8}  {'p50':>5}  {'p90':>5}  {'p99':>5}  "
        f"{'REPO':>8}  {'wall':>6}  {'Δserved':>8}  {'zero-cov':>9}"
    )
    print("-" * 108)
    base_pct = results[0][1]["served_pct"]
    for v, r in results:
        delta = r["served_pct"] - base_pct
        sign  = "+" if delta >= 0 else ""
        z_s   = str(r["zero_cov_final"]) if r["zero_cov_final"] is not None else "—"
        print(
            f"  {v.label:<38}  {r['served_pct']:>7.3f}%  {r['p50']:>4.1f}m  {r['p90']:>4.1f}m  {r['p99']:>4.1f}m  "
            f"{r['repo']:>8,}  {r['wall_s']:>5}s  {sign}{delta:>+.3f}%  {z_s:>9}"
        )
    print("=" * 108)
    print(f"\n  Exp 18 baseline (max_wait=600s): 98.210% served  p90=2.2m")
    print(f"  Exp 19 baseline (max_wait={MAX_WAIT:.0f}s): {results[0][1]['served_pct']:.3f}% served  p90={results[0][1]['p90']}m")


if __name__ == "__main__":
    main()
