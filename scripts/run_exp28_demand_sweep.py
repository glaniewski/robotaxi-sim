"""
Experiment 28 — Demand-scale sweep, fixed fleet=3000
=====================================================
Sweeps demand_scale ∈ {0.01, 0.02, 0.05, 0.1, 0.2} for two policies:
  • demand_score       (travel=30m, floor=1)
  • cov_floor+optB     (travel=30m, look=30m, floor=2)

Progress bar per run via engine progress_callback + tqdm.
Results printed as a table and appended to RESULTS.md.

Usage:
    python scripts/run_exp28_demand_sweep.py
"""
from __future__ import annotations

import sys, time
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd
from tqdm import tqdm

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "backend"))

from app.sim.demand import load_requests
from app.sim.engine import SimConfig, SimulationEngine, build_vehicles
from app.sim.entities import Request, RequestStatus
from app.sim.reposition_policies import build_covered_by, build_policy
from app.sim.routing import RoutingCache

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
REQUESTS_PATH = str(ROOT / "data" / "requests_austin_h3_r8.parquet")
TRAVEL_CACHE  = str(ROOT / "data" / "h3_travel_cache.parquet")
DEPOT_CELL    = "88489e3467fffff"
FLEET         = 3000
SEED          = 123
DURATION      = 1440   # minutes
MAX_WAIT      = 600.0  # seconds
BUCKET_MIN    = 15.0

SCALES   = [0.01, 0.02, 0.05, 0.1, 0.2]
POLICIES = [
    ("demand_score",   "demand_score",   1),
    ("cov_floor+optB", "coverage_floor", 2),
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def build_timed(reqs, bm: float = 15.0) -> dict:
    bs = bm * 60; nb = int(round(1440 / bm)); c: dict = {}
    for r in reqs:
        b = int(r.request_time / bs) % nb
        c.setdefault(r.origin_h3, {})
        c[r.origin_h3][b] = c[r.origin_h3].get(b, 0) + 1
    return {cell: {b: v / bs for b, v in bm2.items()} for cell, bm2 in c.items()}


def build_flat(reqs, dur: float) -> dict:
    c: dict = defaultdict(int)
    for r in reqs:
        c[r.origin_h3] += 1
    return {cell: cnt / (dur * 60) for cell, cnt in c.items()}


# ---------------------------------------------------------------------------
# One run
# ---------------------------------------------------------------------------

def run_variant(
    base_reqs, scale, plabel, pname, floor,
    dcw, dcs, covered_by, routing,
) -> dict:
    timed = build_timed(base_reqs)
    flat  = build_flat(base_reqs, DURATION)

    requests = [
        Request(id=r.id, request_time=r.request_time, origin_h3=r.origin_h3,
                destination_h3=r.destination_h3, max_wait_time_seconds=MAX_WAIT)
        for r in base_reqs
    ]
    sc = SimConfig(
        duration_minutes=DURATION, seed=SEED, fleet_size=FLEET,
        max_wait_time_seconds=MAX_WAIT, reposition_enabled=True,
        reposition_alpha=0.6, reposition_top_k_cells=50,
        max_vehicles_targeting_cell=3,
    )
    vehicles = build_vehicles(sc, depot_h3_cells=[DEPOT_CELL], seed=SEED, demand_cells=dcw)

    kw = dict(
        alpha=0.6, half_life_minutes=45, forecast_horizon_minutes=30,
        max_reposition_travel_minutes=30.0, max_vehicles_targeting_cell=3,
        min_idle_minutes=2, top_k_cells=50, reposition_lambda=0.05,
        forecast_table=flat, demand_cells=dcs, covered_by=covered_by,
        max_wait_time_seconds=MAX_WAIT, min_coverage=floor,
    )
    if pname == "coverage_floor":
        kw.update(
            coverage_reposition_travel_minutes=30.0,
            timed_forecast_table=timed,
            forecast_bucket_minutes=BUCKET_MIN,
            coverage_lookahead_minutes=30.0,
        )
    policy = build_policy(name=pname, **kw)

    total_reqs = len(requests)
    bar = tqdm(
        total=total_reqs,
        desc=f"  scale={scale:.2f}  {plabel:<18}",
        unit="trips", ncols=90, leave=False,
        bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]",
    )
    last_resolved = [0]

    def _progress(resolved: int, total: int) -> None:
        delta = resolved - last_resolved[0]
        if delta > 0:
            bar.update(delta)
            last_resolved[0] = resolved

    t0 = time.time()
    eng = SimulationEngine(
        config=sc, vehicles=vehicles, requests=requests,
        depots=[], routing=routing, reposition_policy=policy,
        progress_callback=_progress,
    )
    res = eng.run()
    wall = time.time() - t0
    bar.update(total_reqs - last_resolved[0])  # fill remainder (in-flight at end)
    bar.close()

    rlist = list(eng.requests.values())
    served  = [r for r in rlist if r.status == RequestStatus.SERVED]
    expired = [r for r in rlist if r.status == RequestStatus.UNSERVED]
    wait_s  = [r.actual_wait_seconds for r in served if r.actual_wait_seconds is not None]

    p50 = float(np.percentile(wait_s, 50)) / 60 if wait_s else 0.0
    p90 = float(np.percentile(wait_s, 90)) / 60 if wait_s else 0.0
    p99 = float(np.percentile(wait_s, 99)) / 60 if wait_s else 0.0

    fleet_s = FLEET * DURATION * 60

    # util% = time spent moving throughout the full simulated day.
    st = res.get("state_time_s") or {}
    move_time_s = (
        st.get("to_pickup", 0)
        + st.get("in_trip", 0)
        + st.get("repositioning", 0)
        + st.get("to_depot", 0)
    )
    util_pct = move_time_s / fleet_s * 100
    trips_per_veh = len(served) / FLEET
    served_pct    = len(served) / len(rlist) * 100

    ec = res["event_counts"]
    repo = ec.get("REPOSITION_COMPLETE", 0)
    total_repo_mi = sum(v.reposition_miles for v in eng.vehicles.values())
    mi_per_repo = (total_repo_mi / repo) if repo > 0 else 0.0
    return dict(
        scale=scale, policy=plabel, trips=len(rlist),
        served_pct=served_pct, expired=len(expired),
        p50=p50, p90=p90, p99=p99,
        util_pct=util_pct, trips_per_veh=trips_per_veh,
        repo=repo,
        mi_per_repo=mi_per_repo,
        wall=wall,
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print("Loading shared data…")
    _df  = pd.read_parquet(REQUESTS_PATH, columns=["origin_h3"])
    dcw  = _df["origin_h3"].value_counts().to_dict()
    dcs  = set(dcw.keys())
    routing    = RoutingCache(parquet_path=TRAVEL_CACHE, osrm_url="http://localhost:5001")
    covered_by = build_covered_by(TRAVEL_CACHE, dcs, MAX_WAIT)
    print(f"Routing cache: {routing.size():,} entries  |  demand cells: {len(dcs):,}\n")

    HDR = (f"{'scale':>6}  {'policy':<18}  {'trips':>7}  {'served%':>8}  "
           f"{'expired':>7}  {'p50':>5}  {'p90':>5}  {'p99':>5}  "
           f"{'util%':>6}  {'t/veh':>5}  {'REPO':>7}  {'mi/REPO':>8}  {'wall':>5}")
    SEP = "-" * len(HDR)
    print(f"Exp 28 — Demand scale sweep  (fleet={FLEET}, max_wait={int(MAX_WAIT)}s)\n")
    print(HDR); print(SEP)

    rows: list[dict] = []
    prev_scale = None

    for scale in SCALES:
        base_reqs = load_requests(
            REQUESTS_PATH, duration_minutes=DURATION,
            max_wait_time_seconds=MAX_WAIT, demand_scale=scale, seed=SEED,
        )
        if prev_scale is not None:
            print()
        prev_scale = scale

        for plabel, pname, floor in POLICIES:
            r = run_variant(base_reqs, scale, plabel, pname, floor,
                            dcw, dcs, covered_by, routing)
            rows.append(r)
            print(
                f"  {r['scale']:>6.2f}  {r['policy']:<18}  {r['trips']:>7,}  "
                f"{r['served_pct']:>8.3f}%  {r['expired']:>7}  "
                f"{r['p50']:>4.1f}m  {r['p90']:>4.1f}m  {r['p99']:>4.1f}m  "
                f"{r['util_pct']:>5.1f}%  {r['trips_per_veh']:>4.1f}  "
                f"{r['repo']:>7,}  {r['mi_per_repo']:>8.1f}  {r['wall']:>4.0f}s"
            )

    print(f"\n{SEP}")
    print("Done.")

    # ------------------------------------------------------------------
    # Append summary table to RESULTS.md
    # ------------------------------------------------------------------
    results_path = ROOT / "RESULTS.md"
    md_rows = "\n".join(
        f"| {r['scale']:.2f} | {r['policy']} | {r['trips']:,} | "
        f"{r['served_pct']:.3f}% | {r['expired']} | "
        f"{r['p50']:.1f}m | {r['p90']:.1f}m | {r['p99']:.1f}m | "
        f"{r['util_pct']:.1f}% | {r['trips_per_veh']:.1f} | {r['repo']:,} | {r['mi_per_repo']:.1f} |"
        for r in rows
    )
    md_block = f"""
---

## Experiment 28 — Demand scale sweep (fleet=3,000 fixed, max_wait=600s)

Config: `seed=123`, `fleet=3000`, `duration=1440min`, `max_wait=600s`, `travel=30m`, `look=30m`.  
Policies: **demand_score** (floor=1, travel=30m) vs **cov_floor+optB** (floor=2, travel=30m, look=30m).

| scale | policy | trips | served% | expired | p50 | p90 | p99 | util% | t/veh | REPO | mi/REPO |
|---|---|---|---|---|---|---|---|---|---|---|---|
{md_rows}

"""
    with open(results_path, "a") as f:
        f.write(md_block)
    print(f"\nResults appended to {results_path}")


if __name__ == "__main__":
    main()
