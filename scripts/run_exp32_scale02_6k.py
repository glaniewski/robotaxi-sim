"""
Experiment 32 — 2× Exp 30: scale=0.2, fleet=6k
==============================================
Same policy as Exp 30 (cov_floor+optB, travel=60m, look=60m, floor=2).
Double demand (scale=0.2 → ~173.6k trips) and 6k vehicles.

Usage:
    PYTHONHASHSEED=0 python scripts/run_exp32_scale02_6k.py
"""
from __future__ import annotations

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

SCALE = 0.2
FLEET_SIZE = 6000


def build_timed(reqs, bm: float = 15.0) -> dict:
    bs = bm * 60.0
    nb = int(round(1440.0 / bm))
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


def run_one(scale: float, fleet_size: int, dcw, dcs, covered_by, routing) -> dict:
    base_reqs = load_requests(
        REQUESTS_PATH,
        duration_minutes=DURATION,
        max_wait_time_seconds=MAX_WAIT,
        demand_scale=scale,
        seed=SEED,
    )
    timed = build_timed(base_reqs)
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
    vehicles = build_vehicles(sc, depot_h3_cells=[DEPOT_CELL], seed=SEED, demand_cells=dcw)
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
        desc=f"fleet={fleet_size}  cov_floor+optB scale={scale}",
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

    fleet_s = fleet_size * DURATION * 60
    trip_s_total = sum(r.trip_duration_seconds for r in served if r.trip_duration_seconds)
    util_pct = trip_s_total / fleet_s * 100
    trips_per_veh = len(served) / fleet_size
    served_pct = len(served) / len(rlist) * 100

    vlist = list(eng.vehicles.values())
    total_trip_mi = sum(v.trip_miles for v in vlist)
    total_pickup_mi = sum(v.pickup_miles for v in vlist)
    total_repo_mi = sum(v.reposition_miles for v in vlist)
    total_miles = total_trip_mi + total_pickup_mi + total_repo_mi
    move_mi_veh = total_miles / fleet_size if fleet_size else 0.0
    deadhead_pct = (total_pickup_mi + total_repo_mi) / total_miles * 100.0 if total_miles > 0 else 0.0
    revenue_mi_pct = total_trip_mi / total_miles * 100.0 if total_miles > 0 else 0.0

    st = res.get("state_time_s") or {}
    duration_s = DURATION * 60.0
    fleet_sec = fleet_size * duration_s
    move_time_s = (
        st.get("to_pickup", 0)
        + st.get("in_trip", 0)
        + st.get("repositioning", 0)
        + st.get("to_depot", 0)
        + st.get("charging", 0)
    )
    deadhead_time_s = st.get("to_pickup", 0) + st.get("repositioning", 0) + st.get("to_depot", 0)
    move_time_pct = move_time_s / fleet_sec * 100.0 if fleet_sec else 0.0
    deadhead_time_pct = deadhead_time_s / move_time_s * 100.0 if move_time_s > 0 else 0.0

    charge_events_total = sum(v.charge_sessions for v in vlist)
    charge_events_per_veh = charge_events_total / fleet_size if fleet_size else 0.0
    charge_time_total_s = st.get("charging", 0)
    charge_time_per_veh_s = charge_time_total_s / fleet_size if fleet_size else 0.0

    ec = res["event_counts"]
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
        trips_per_veh=trips_per_veh,
        move_mi_veh=move_mi_veh,
        revenue_mi_pct=revenue_mi_pct,
        deadhead_pct=deadhead_pct,
        move_time_pct=move_time_pct,
        deadhead_time_pct=deadhead_time_pct,
        charge_events_total=charge_events_total,
        charge_events_per_veh=charge_events_per_veh,
        charge_time_total_s=charge_time_total_s,
        charge_time_per_veh_s=charge_time_per_veh_s,
        repo=ec.get("REPOSITION_COMPLETE", 0),
        wall=wall,
    )


def main() -> None:
    print("Loading shared data…")
    _df = pd.read_parquet(REQUESTS_PATH, columns=["origin_h3"])
    dcw = _df["origin_h3"].value_counts().to_dict()
    dcs = set(dcw.keys())
    routing = RoutingCache(parquet_path=TRAVEL_CACHE, osrm_url="http://localhost:5001")
    covered_by = build_covered_by(TRAVEL_CACHE, dcs, MAX_WAIT)
    print(f"Routing cache: {routing.size():,} entries  |  demand cells: {len(dcs):,}\n")

    HDR = (
        f"{'fleet':>7}  {'trips':>9}  {'served%':>8}  {'expired':>7}  "
        f"{'p50':>5}  {'p90':>5}  {'p99':>5}  {'move mi/v':>9}  {'revenue% (dead%)':>16}  "
        f"{'time move% (dead%)':>20}  {'t/veh':>5}  {'REPO':>8}  {'wall':>8}"
    )
    SEP = "-" * 150
    print(f"Exp 32 — 2× Exp 30: scale={SCALE} fleet={FLEET_SIZE:,}  (max_wait={int(MAX_WAIT)}s)\n")
    print(HDR)
    print(SEP)

    r = run_one(SCALE, FLEET_SIZE, dcw, dcs, covered_by, routing)
    print(
        f"  {r['fleet']:>7,}  {r['trips']:>9,}  {r['served_pct']:>8.3f}%  "
        f"{r['expired']:>7}  {r['p50']:>4.1f}m  {r['p90']:>4.1f}m  {r['p99']:>4.1f}m  "
        f"{r['move_mi_veh']:>8.1f}  {r['revenue_mi_pct']:>5.1f}% ({r['deadhead_pct']:>5.1f}%)  "
        f"{r['move_time_pct']:>5.1f}% ({r['deadhead_time_pct']:>5.1f}%)  "
        f"{r['trips_per_veh']:>4.1f}  {r['repo']:>8,}  {r['wall']:>7.1f}s"
    )

    print(f"\n{SEP}\nDone.")

    results_path = ROOT / "RESULTS.md"
    md_block = f"""
---

## Experiment 32 — 2× Exp 30: scale=0.2, fleet=6k

Double demand (scale=0.2, ~173.6k trips) and 6k vehicles; same policy as Exp 30 (cov_floor+optB, travel=60m, look=60m, floor=2).  
Config: `seed=123`, `duration=1440min`, `max_wait=600s`. depots=[].

| fleet | policy | trips | served% | expired | p50 | p90 | p99 | move mi/v | revenue% (dead%) | time move% (dead%) | t/veh | REPO | wall |
|-------|--------|-------|---------|---------|-----|-----|-----|-----------|------------------|--------------------|-------|------|------|
| {r['fleet']:,} | cov_floor+optB | {r['trips']:,} | {r['served_pct']:.3f}% | {r['expired']:,} | {r['p50']:.1f}m | {r['p90']:.1f}m | {r['p99']:.1f}m | {r['move_mi_veh']:.1f} | {r['revenue_mi_pct']:.1f}% ({r['deadhead_pct']:.1f}%) | {r['move_time_pct']:.1f}% ({r['deadhead_time_pct']:.1f}%) | {r['trips_per_veh']:.1f} | {r['repo']:,} | {r['wall']:.0f}s |

"""
    with open(results_path, "a") as f:
        f.write(md_block)
    print(f"\nResults appended to {results_path}")


if __name__ == "__main__":
    main()
