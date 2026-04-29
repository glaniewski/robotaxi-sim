"""
Experiment 16 — Coverage ceiling sweep
Tests whether increasing dispatch radius (max_wait_time_seconds) and
repositioning top_k_cells can break through the served_pct ceiling.

Drain fix: engine now continues past duration_s until _pending is empty,
so late-arriving requests are no longer stranded.

Run from repo root:
    python3 scripts/run_coverage_sweep.py
"""
from __future__ import annotations
import os, sys, time
from dataclasses import dataclass

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "backend"))

import numpy as np
from app.sim.demand import load_requests
from app.sim.engine import SimConfig, SimulationEngine, build_vehicles
from app.sim.entities import RequestStatus
from app.sim.repositioning import RepositioningPolicy
from app.sim.routing import RoutingCache

REQUESTS_PATH = os.path.join(ROOT, "data", "requests_austin_h3_r8.parquet")
TRAVEL_CACHE  = os.path.join(ROOT, "data", "h3_travel_cache.parquet")
OSRM_URL      = os.environ.get("OSRM_URL", "http://localhost:5001")

FLEET    = 3_000
SCALE    = 0.01       # ~8,700 trips/day — fast iterations
SEED     = 123
DURATION = 1440       # 24 h of requests


@dataclass
class Variant:
    name: str
    max_wait_s: float = 600.0
    top_k: int = 50


VARIANTS = [
    Variant("baseline"),
    Variant("radius_900s",  max_wait_s=900.0),
    Variant("radius_1200s", max_wait_s=1200.0),
    Variant("top_k_100",    top_k=100),
    Variant("top_k_200",    top_k=200),
    Variant("combined",     max_wait_s=900.0, top_k=100),
]

# Events to highlight in per-variant summary (skip SNAPSHOT noise)
SHOW_EVENTS = [
    "REQUEST_ARRIVAL", "DISPATCH", "TRIP_START", "TRIP_COMPLETE",
    "VEHICLE_IDLE", "REPOSITION_COMPLETE",
]


def build_forecast_table(requests, duration_min: float) -> dict[str, float]:
    from collections import defaultdict
    counts: dict[str, int] = defaultdict(int)
    for r in requests:
        counts[r.origin_h3] += 1
    duration_s = duration_min * 60.0
    return {cell: cnt / duration_s for cell, cnt in counts.items()}


def _fmt(s: float) -> str:
    if not (0 <= s < 36000):
        return "--:--"
    return f"{int(s//60):02d}:{int(s%60):02d}"


def run_variant(v: Variant, all_requests, routing) -> dict:
    """Re-create request objects with updated max_wait_time_seconds."""
    from app.sim.entities import Request
    requests = [
        Request(
            id=r.id,
            request_time=r.request_time,
            origin_h3=r.origin_h3,
            destination_h3=r.destination_h3,
            max_wait_time_seconds=v.max_wait_s,
        )
        for r in all_requests
    ]

    sim_config = SimConfig(
        duration_minutes=DURATION,
        seed=SEED,
        fleet_size=FLEET,
        max_wait_time_seconds=v.max_wait_s,
        reposition_enabled=True,
        reposition_alpha=0.6,
        reposition_top_k_cells=v.top_k,
        max_vehicles_targeting_cell=3,
    )
    vehicles = build_vehicles(sim_config, depot_h3_cells=["88489e3467fffff"], seed=SEED)

    forecast_table = build_forecast_table(requests, DURATION)
    repo_policy = RepositioningPolicy(
        alpha=sim_config.reposition_alpha,
        half_life_minutes=sim_config.reposition_half_life_minutes,
        forecast_horizon_minutes=sim_config.reposition_forecast_horizon_minutes,
        max_reposition_travel_minutes=sim_config.max_reposition_travel_minutes,
        max_vehicles_targeting_cell=sim_config.max_vehicles_targeting_cell,
        min_idle_minutes=sim_config.reposition_min_idle_minutes,
        top_k_cells=sim_config.reposition_top_k_cells,
        reposition_lambda=sim_config.reposition_lambda,
        forecast_table=forecast_table,
    )

    t0 = time.time()
    total = len(requests)

    def _progress(n_done, n_total):
        pct = n_done / n_total * 100 if n_total else 0
        elapsed = time.time() - t0
        rate = n_done / elapsed if elapsed > 0 else 0
        eta = (n_total - n_done) / rate if rate > 0 else float("inf")
        bar_w = 36
        filled = int(pct / 100 * bar_w)
        bar = "█" * filled + "░" * (bar_w - filled)
        print(
            f"\r    [{bar}] {pct:5.1f}%  {_fmt(elapsed)}<{_fmt(eta)}  {rate:,.0f} t/s",
            end="", flush=True,
        )

    engine = SimulationEngine(
        config=sim_config,
        vehicles=vehicles,
        requests=requests,
        depots=[],
        routing=routing,
        reposition_policy=repo_policy,
        progress_callback=_progress,
    )
    result = engine.run()
    elapsed = time.time() - t0
    print(f"\r    done in {elapsed:.0f}s" + " " * 60)

    # ── per-run summary ───────────────────────────────────────────────
    m  = result["metrics"]
    ec = result["event_counts"]
    rs = result["routing_stats"]

    req_list = list(engine.requests.values())
    served   = [r for r in req_list if r.status == RequestStatus.SERVED]
    wait_s   = [r.actual_wait_seconds for r in served if r.actual_wait_seconds is not None]
    p50 = float(np.percentile(wait_s, 50)) / 60 if wait_s else 0.0
    p90 = float(np.percentile(wait_s, 90)) / 60 if wait_s else 0.0

    print(f"    served={m['served_pct']:.2f}%  "
          f"wait p50={p50:.1f}min  p90={p90:.1f}min  "
          f"wall={elapsed:.0f}s")
    print(f"    routing: hits={rs['cache_hits']:,}  misses={rs['cache_misses']:,}  "
          f"hit_rate={rs['hit_rate_pct']:.1f}%")
    print(f"    events: " + "  ".join(
        f"{e}={ec.get(e, 0):,}" for e in SHOW_EVENTS
    ))

    return {
        "served_pct": m["served_pct"],
        "wait_p50":   round(p50, 2),
        "wait_p90":   round(p90, 2),
        "wall_s":     round(elapsed),
        "event_counts": ec,
        "routing_misses": rs["cache_misses"],
        "served":   m["served_count"],
        "unserved": m["unserved_count"],
        "pending":  sum(1 for r in req_list if r.status == RequestStatus.PENDING),
    }


def main():
    print(f"Loading requests (scale={SCALE}) …")
    base_requests = load_requests(
        parquet_path=REQUESTS_PATH,
        duration_minutes=DURATION,
        max_wait_time_seconds=600.0,
        demand_scale=SCALE,
        seed=SEED,
    )
    print(f"  {len(base_requests):,} requests  (fleet={FLEET:,})\n")

    routing = RoutingCache(parquet_path=TRAVEL_CACHE, osrm_url=OSRM_URL)

    results = []
    for v in VARIANTS:
        print(f"  ── {v.name:<22}  max_wait={v.max_wait_s:.0f}s  top_k={v.top_k}")
        r = run_variant(v, base_requests, routing)
        results.append((v, r))
        print()

    # ── final comparison table ────────────────────────────────────────────────
    print("=" * 95)
    print(f"  {'variant':<22}  {'max_wait':>9}  {'top_k':>6}  "
          f"{'served%':>8}  {'p50':>6}  {'p90':>6}  {'misses':>8}  "
          f"{'REPOSITION':>11}  {'wall':>6}  {'Δserved%':>9}")
    print("-" * 95)
    base_pct = results[0][1]["served_pct"]
    for v, r in results:
        delta = r["served_pct"] - base_pct
        sign = "+" if delta >= 0 else ""
        print(
            f"  {v.name:<22}  {v.max_wait_s:>9.0f}  {v.top_k:>6}  "
            f"{r['served_pct']:>7.2f}%  {r['wait_p50']:>5.1f}m  {r['wait_p90']:>5.1f}m  "
            f"{r['routing_misses']:>8,}  "
            f"{r['event_counts'].get('REPOSITION_COMPLETE', 0):>11,}  "
            f"{r['wall_s']:>5}s  {sign}{delta:.2f}%"
        )
    print("=" * 95)


if __name__ == "__main__":
    main()
