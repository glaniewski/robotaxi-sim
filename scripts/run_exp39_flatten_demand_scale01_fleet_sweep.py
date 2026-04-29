"""
Experiment 39 — Fully flattened demand (constant throughout day), scale=0.1, fleet 3k/4k/5k
==========================================================================================

Uses demand_flatten=1.0 so trip arrivals are uniformly distributed across the day
(same O-D pairs as scale=0.1, only timestamps changed). No OSRM multiplier or buffer.

Metrics: same set as Exp 30 + Exp 38 — served%, expired, p50/p90/p99, util%, move mi/v,
revenue% (dead%), time move% (dead%), charging N & time per veh, t/veh, REPO, mi/REPO, wall.

Usage (do not run until ready):
    PYTHONHASHSEED=0 python3 scripts/run_exp39_flatten_demand_scale01_fleet_sweep.py
"""
from __future__ import annotations

import os
import sys
import time
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

REQUESTS_PATH = str(ROOT / "data" / "requests_austin_h3_r8.parquet")
TRAVEL_CACHE = str(ROOT / "data" / "h3_travel_cache.parquet")
DEPOT_CELL = "88489e3467fffff"

SEED = 123
DURATION = 1440
MAX_WAIT = 600.0
BUCKET_MIN = 15.0

SCALE = 0.1
DEMAND_FLATTEN = 1.0  # Fully uniform arrivals (constant demand throughout day)
FLEET_SIZES = [3000, 4000, 5000]


def build_timed(reqs, bm: float = 15.0) -> dict:
    bs = bm * 60.0
    nb = int(round(DURATION * 60.0 / bs))
    c: dict = {}
    for r in reqs:
        b = int(r.request_time / bs) % nb
        c.setdefault(r.origin_h3, {})
        c[r.origin_h3][b] = c[r.origin_h3].get(b, 0) + 1
    return {cell: {b: v / bs for b, v in bm2.items()} for cell, bm2 in c.items()}


def build_flat(reqs, dur: float) -> dict:
    c: dict = {}
    for r in reqs:
        c[r.origin_h3] = c.get(r.origin_h3, 0) + 1
    return {cell: cnt / (dur * 60) for cell, cnt in c.items()}


def run_one(
    scale: float,
    fleet_size: int,
    demand_flatten: float,
    dcw,
    dcs,
    covered_by,
    routing: RoutingCache,
) -> dict:
    base_reqs = load_requests(
        REQUESTS_PATH,
        duration_minutes=DURATION,
        max_wait_time_seconds=MAX_WAIT,
        demand_scale=scale,
        demand_flatten=demand_flatten,
        seed=SEED,
    )
    timed = build_timed(base_reqs, bm=BUCKET_MIN)
    flat = build_flat(base_reqs, DURATION)

    requests = [
        Request(
            id=r.id,
            request_time=r.request_time,
            origin_h3=r.origin_h3,
            destination_h3=r.destination_h3,
            max_wait_time_seconds=MAX_WAIT,
        )
        for r in base_reqs
    ]

    sc = SimConfig(
        duration_minutes=DURATION,
        seed=SEED,
        fleet_size=fleet_size,
        max_wait_time_seconds=MAX_WAIT,
        reposition_enabled=True,
        reposition_alpha=0.6,
        reposition_top_k_cells=50,
        max_vehicles_targeting_cell=3,
    )

    vehicles = build_vehicles(
        sc,
        depot_h3_cells=[DEPOT_CELL],
        seed=SEED,
        demand_cells=dcw,
    )

    policy = build_policy(
        name="coverage_floor",
        alpha=0.6,
        half_life_minutes=45,
        forecast_horizon_minutes=30,
        max_reposition_travel_minutes=30.0,
        max_vehicles_targeting_cell=3,
        min_idle_minutes=2,
        top_k_cells=50,
        reposition_lambda=0.05,
        forecast_table=flat,
        demand_cells=dcs,
        covered_by=covered_by,
        max_wait_time_seconds=MAX_WAIT,
        min_coverage=2,
        coverage_reposition_travel_minutes=60.0,
        timed_forecast_table=timed,
        forecast_bucket_minutes=BUCKET_MIN,
        coverage_lookahead_minutes=60.0,
    )

    total_reqs = len(requests)
    bar = tqdm(
        total=total_reqs,
        desc=f"fleet={fleet_size} flat={demand_flatten}",
        unit="trips",
        ncols=100,
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
        config=sc,
        vehicles=vehicles,
        requests=requests,
        depots=[],
        routing=routing,
        reposition_policy=policy,
        progress_callback=_progress,
    )
    res = eng.run()
    wall = time.time() - t0
    bar.update(total_reqs - last_resolved[0])
    bar.close()

    rlist = list(eng.requests.values())
    served = [r for r in rlist if r.status == RequestStatus.SERVED]
    expired = [r for r in rlist if r.status == RequestStatus.UNSERVED]
    wait_s = [r.actual_wait_seconds for r in served if r.actual_wait_seconds is not None]

    p50 = float(np.percentile(wait_s, 50)) / 60 if wait_s else 0.0
    p90 = float(np.percentile(wait_s, 90)) / 60 if wait_s else 0.0
    p99 = float(np.percentile(wait_s, 99)) / 60 if wait_s else 0.0

    trips_per_veh = len(served) / fleet_size if fleet_size else 0.0
    served_pct = len(served) / len(rlist) * 100 if rlist else 0.0

    # Movement miles: revenue + deadhead (Exp 30 + Exp 38)
    vlist = list(eng.vehicles.values())
    total_trip_mi = sum(v.trip_miles for v in vlist)
    total_pickup_mi = sum(v.pickup_miles for v in vlist)
    total_repo_mi = sum(v.reposition_miles for v in vlist)
    total_miles = total_trip_mi + total_pickup_mi + total_repo_mi
    move_mi_veh = total_miles / fleet_size if fleet_size else 0.0
    deadhead_pct = (
        (total_pickup_mi + total_repo_mi) / total_miles * 100.0
        if total_miles > 0
        else 0.0
    )
    revenue_mi_pct = total_trip_mi / total_miles * 100.0 if total_miles > 0 else 0.0

    # util% = time spent moving (all non-idle); time move% and dead% (Exp 30 style)
    st = res.get("state_time_s") or {}
    fleet_s = fleet_size * DURATION * 60.0
    move_time_s = (
        st.get("to_pickup", 0)
        + st.get("in_trip", 0)
        + st.get("repositioning", 0)
        + st.get("to_depot", 0)
        + st.get("charging", 0)
    )
    deadhead_time_s = (
        st.get("to_pickup", 0)
        + st.get("repositioning", 0)
        + st.get("to_depot", 0)
    )
    util_pct = move_time_s / fleet_s * 100 if fleet_s else 0.0
    move_time_pct = util_pct  # same as util% (time move%)
    deadhead_time_pct = (
        deadhead_time_s / move_time_s * 100.0 if move_time_s > 0 else 0.0
    )

    # Charging (Exp 30) — depots=[] so these will be 0
    charge_events_total = sum(v.charge_sessions for v in vlist)
    charge_events_per_veh = charge_events_total / fleet_size if fleet_size else 0.0
    charge_time_total_s = st.get("charging", 0)
    charge_time_per_veh_s = charge_time_total_s / fleet_size if fleet_size else 0.0

    # REPO and mi/REPO (Exp 38)
    ec = res["event_counts"]
    repo = ec.get("REPOSITION_COMPLETE", 0)
    mi_per_repo = (total_repo_mi / repo) if repo > 0 else 0.0

    return dict(
        scale=scale,
        fleet=fleet_size,
        trips=len(rlist),
        served_pct=served_pct,
        expired=len(expired),
        p50=p50,
        p90=p90,
        p99=p99,
        util_pct=util_pct,
        move_time_pct=move_time_pct,
        deadhead_time_pct=deadhead_time_pct,
        trips_per_veh=trips_per_veh,
        move_mi_veh=move_mi_veh,
        revenue_mi_pct=revenue_mi_pct,
        deadhead_pct=deadhead_pct,
        charge_events_total=charge_events_total,
        charge_events_per_veh=charge_events_per_veh,
        charge_time_total_s=charge_time_total_s,
        charge_time_per_veh_s=charge_time_per_veh_s,
        repo=repo,
        mi_per_repo=mi_per_repo,
        wall=wall,
    )


def main() -> None:
    # Defaults: no OSRM multiplier or pickup/drop buffer (same as Exp 30/37)
    os.environ.setdefault("OSRM_TIME_MULTIPLIER", "1.0")
    os.environ.setdefault("OSRM_PICKUP_DROPOFF_BUFFER_MINUTES", "0.0")

    print("Loading shared data…")
    _df = pd.read_parquet(REQUESTS_PATH, columns=["origin_h3"])
    dcw = _df["origin_h3"].value_counts().to_dict()
    dcs = set(dcw.keys())

    routing = RoutingCache(parquet_path=TRAVEL_CACHE, osrm_url="http://localhost:5001")
    covered_by = build_covered_by(TRAVEL_CACHE, dcs, MAX_WAIT)
    print(
        f"Routing cache: {routing.size():,} entries | demand cells: {len(dcs):,}\n"
    )
    print(
        f"Exp 39 — demand_flatten={DEMAND_FLATTEN} (constant demand), scale={SCALE}, fleet={FLEET_SIZES}\n"
    )

    # Console header (Exp 30 style + REPO, mi/REPO)
    HDR = (
        f"{'fleet':>6}  {'trips':>7}  {'served%':>8}  {'expired':>7}  "
        f"{'p50':>5}  {'p90':>5}  {'p99':>5}  {'move mi/v':>9}  {'revenue% (dead%)':>18}  "
        f"{'util% (dead%)':>16}  {'chg N/v':>7}  {'chg min/v':>9}  {'t/veh':>5}  {'REPO':>7}  {'mi/REPO':>7}  {'wall':>5}"
    )
    SEP = "-" * 160
    print(HDR)
    print(SEP)

    rows: list[dict] = []
    for fleet_size in FLEET_SIZES:
        r = run_one(
            SCALE, fleet_size, DEMAND_FLATTEN, dcw, dcs, covered_by, routing
        )
        rows.append(r)
        chg_min_v = r["charge_time_per_veh_s"] / 60.0
        print(
            f"  {r['fleet']:>6,}  {r['trips']:>7,}  {r['served_pct']:>7.3f}%  "
            f"{r['expired']:>7,}  {r['p50']:>4.1f}m  {r['p90']:>4.1f}m  {r['p99']:>4.1f}m  "
            f"{r['move_mi_veh']:>8.1f}  "
            f"{r['revenue_mi_pct']:>5.1f}% ({r['deadhead_pct']:>5.1f}%)  "
            f"{r['util_pct']:>5.1f}% ({r['deadhead_time_pct']:>5.1f}%)  "
            f"{r['charge_events_per_veh']:>6.2f}  {chg_min_v:>8.1f}  "
            f"{r['trips_per_veh']:>4.1f}  {r['repo']:>7,}  {r['mi_per_repo']:>6.1f}  {r['wall']:>4.0f}s"
        )

    print(f"\n{SEP}\nDone.")

    # Append markdown table to RESULTS.md (Exp 30 + Exp 38 columns)
    results_path = ROOT / "RESULTS.md"
    md_rows = "\n".join(
        f"| {r['scale']:.1f}× | {r['fleet']:,} | cov_floor+optB | {r['trips']:,} | "
        f"{r['served_pct']:.3f}% | {r['expired']:,} | "
        f"{r['p50']:.1f}m | {r['p90']:.1f}m | {r['p99']:.1f}m | "
        f"{r['move_mi_veh']:.1f} | {r['revenue_mi_pct']:.1f}% ({r['deadhead_pct']:.1f}%) | "
        f"{r['util_pct']:.1f}% ({r['deadhead_time_pct']:.1f}%) | "
        f"{r['charge_events_total']:,} / {r['charge_events_per_veh']:.2f} | {r['charge_time_total_s']/60:.0f}m / {r['charge_time_per_veh_s']/60:.1f}m | "
        f"{r['trips_per_veh']:.1f} | {r['repo']:,} | {r['mi_per_repo']:.1f} | {r['wall']:.0f}s |"
        for r in rows
    )
    md_block = f"""
---
## Experiment 39 — Fully flattened demand (constant), scale=0.1, fleet 3k/4k/5k

Config: `demand_flatten=1.0` (uniform arrivals), `scale=0.1`, `seed=123`, `duration=1440min`, `max_wait=600s`, `reposition=cov_floor+optB`. depots=[].
OSRM defaults (no time multiplier or pickup/drop buffer).

| scale | fleet | policy | trips | served% | expired | p50 | p90 | p99 | move mi/v | revenue% (dead%) | util% (dead%) | chg N (per v) | chg time (per v) | t/veh | REPO | mi/REPO | wall |
|-------|-------|--------|-------|---------|---------|-----|-----|-----|-----------|------------------|---------------|---------------|------------------|-------|------|---------|------|
{md_rows}
"""
    with open(results_path, "a") as f:
        f.write(md_block)
    print(f"\nResults appended to {results_path}")


if __name__ == "__main__":
    main()
