"""
Sweep: scale=0.1, no reposition — fleet size × max_wait_time (minutes).
======================================================================
Goal: (1) See how high util% can go as we vary fleet size.
      (2) See if changing max_wait_time (minutes) alters util%.

Config: flat demand (demand_flatten=1.0), scale=0.1, reposition_enabled=False,
no reposition policy. depots=[].

Metrics: same as Exp 40 — scale | fleet | policy | trips | served% | expired |
p50 | p90 | p99 | move mi/v | revenue% (dead%) | util% (dead%) | chg N (per v) |
chg time (per v) | t/veh | REPO | mi/REPO | wall.

Usage:
    PYTHONHASHSEED=0 python3 scripts/run_sweep_util_maxwait_norepo.py
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
from app.sim.routing import RoutingCache

REQUESTS_PATH = str(ROOT / "data" / "requests_austin_h3_r8.parquet")
TRAVEL_CACHE = str(ROOT / "data" / "h3_travel_cache.parquet")
DEPOT_CELL = "88489e3467fffff"

SEED = 123
DURATION = 1440
BUCKET_MIN = 15.0
SCALE = 0.1
DEMAND_FLATTEN = 1.0

# Sweep dimensions
FLEET_SIZES = [2000, 3000, 4000, 5000, 6000, 8000]
MAX_WAIT_MINUTES = [5, 10, 15, 20]


def build_flat(reqs, dur: float) -> dict:
    c: dict = {}
    for r in reqs:
        c[r.origin_h3] = c.get(r.origin_h3, 0) + 1
    return {cell: cnt / (dur * 60) for cell, cnt in c.items()}


def run_one(
    scale: float,
    fleet_size: int,
    max_wait_seconds: float,
    dcw: dict,
    routing: RoutingCache,
) -> dict:
    base_reqs = load_requests(
        REQUESTS_PATH,
        duration_minutes=DURATION,
        max_wait_time_seconds=max_wait_seconds,
        demand_scale=scale,
        demand_flatten=DEMAND_FLATTEN,
        seed=SEED,
    )
    flat = build_flat(base_reqs, DURATION)

    requests = [
        Request(
            id=r.id,
            request_time=r.request_time,
            origin_h3=r.origin_h3,
            destination_h3=r.destination_h3,
            max_wait_time_seconds=max_wait_seconds,
        )
        for r in base_reqs
    ]

    sc = SimConfig(
        duration_minutes=DURATION,
        seed=SEED,
        fleet_size=fleet_size,
        max_wait_time_seconds=max_wait_seconds,
        reposition_enabled=False,
    )

    vehicles = build_vehicles(
        sc,
        depot_h3_cells=[DEPOT_CELL],
        seed=SEED,
        demand_cells=dcw,
    )

    total_reqs = len(requests)
    bar = tqdm(
        total=total_reqs,
        desc=f"fleet={fleet_size} max_wait={max_wait_seconds/60:.0f}m",
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
        reposition_policy=None,
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
    deadhead_time_pct = (
        deadhead_time_s / move_time_s * 100.0 if move_time_s > 0 else 0.0
    )

    charge_events_total = sum(v.charge_sessions for v in vlist)
    charge_events_per_veh = charge_events_total / fleet_size if fleet_size else 0.0
    charge_time_total_s = st.get("charging", 0)
    charge_time_per_veh_s = charge_time_total_s / fleet_size if fleet_size else 0.0

    ec = res["event_counts"]
    repo = ec.get("REPOSITION_COMPLETE", 0)
    mi_per_repo = (total_repo_mi / repo) if repo > 0 else 0.0

    return dict(
        scale=scale,
        fleet=fleet_size,
        max_wait_min=max_wait_seconds / 60.0,
        trips=len(rlist),
        served_pct=served_pct,
        expired=len(expired),
        p50=p50,
        p90=p90,
        p99=p99,
        util_pct=util_pct,
        move_mi_veh=move_mi_veh,
        revenue_mi_pct=revenue_mi_pct,
        deadhead_pct=deadhead_pct,
        deadhead_time_pct=deadhead_time_pct,
        charge_events_total=charge_events_total,
        charge_events_per_veh=charge_events_per_veh,
        charge_time_total_s=charge_time_total_s,
        charge_time_per_veh_s=charge_time_per_veh_s,
        trips_per_veh=trips_per_veh,
        repo=repo,
        mi_per_repo=mi_per_repo,
        wall=wall,
    )


def main() -> None:
    os.environ.setdefault("OSRM_TIME_MULTIPLIER", "1.0")
    os.environ.setdefault("OSRM_PICKUP_DROPOFF_BUFFER_MINUTES", "0.0")

    print("Loading shared data…")
    _df = pd.read_parquet(REQUESTS_PATH, columns=["origin_h3"])
    dcw = _df["origin_h3"].value_counts().to_dict()

    routing = RoutingCache(parquet_path=TRAVEL_CACHE, osrm_url="http://localhost:5001")
    print(f"Routing cache: {routing.size():,} entries | demand cells: {len(dcw):,}\n")
    print(
        "Sweep — scale=0.1, flat demand, NO reposition | fleet × max_wait_min\n"
        f"  fleet: {FLEET_SIZES}\n  max_wait_min: {MAX_WAIT_MINUTES}\n"
    )

    # Exp 40–style header
    HDR = (
        f"{'max_w':>5}  {'fleet':>6}  {'served%':>8}  {'expired':>7}  "
        f"{'p50':>5}  {'p90':>5}  {'p99':>5}  {'move mi/v':>9}  {'revenue% (dead%)':>18}  "
        f"{'util% (dead%)':>16}  {'chg N/v':>7}  {'chg min/v':>9}  {'t/veh':>5}  {'REPO':>7}  {'mi/REPO':>7}  {'wall':>5}"
    )
    SEP = "-" * 165
    print(HDR)
    print(SEP)

    rows: list[dict] = []
    for max_wait_min in MAX_WAIT_MINUTES:
        max_wait_sec = max_wait_min * 60.0
        for fleet_size in FLEET_SIZES:
            r = run_one(SCALE, fleet_size, max_wait_sec, dcw, routing)
            r["max_wait_min"] = max_wait_min
            rows.append(r)
            chg_min_v = r["charge_time_per_veh_s"] / 60.0
            print(
                f"  {r['max_wait_min']:>4.0f}m  {r['fleet']:>6,}  {r['served_pct']:>7.3f}%  "
                f"{r['expired']:>7,}  {r['p50']:>4.1f}m  {r['p90']:>4.1f}m  {r['p99']:>4.1f}m  "
                f"{r['move_mi_veh']:>8.1f}  "
                f"{r['revenue_mi_pct']:>5.1f}% ({r['deadhead_pct']:>5.1f}%)  "
                f"{r['util_pct']:>5.1f}% ({r['deadhead_time_pct']:>5.1f}%)  "
                f"{r['charge_events_per_veh']:>6.2f}  {chg_min_v:>8.1f}  "
                f"{r['trips_per_veh']:>4.1f}  {r['repo']:>7,}  {r['mi_per_repo']:>6.1f}  {r['wall']:>4.0f}s"
            )

    print(f"\n{SEP}\nDone.")

    # Summary by fleet (for "how high can util go") and by max_wait (for "does max_wait alter util%")
    print("\n--- util% by fleet (across all max_wait) ---")
    by_fleet: dict[int, list[float]] = {}
    for r in rows:
        by_fleet.setdefault(r["fleet"], []).append(r["util_pct"])
    for f in sorted(by_fleet.keys()):
        vals = by_fleet[f]
        print(f"  fleet {f:,}: util% min={min(vals):.1f} max={max(vals):.1f} avg={np.mean(vals):.1f}")

    print("\n--- util% by max_wait_min (across all fleet sizes) ---")
    by_mw: dict[float, list[float]] = {}
    for r in rows:
        by_mw.setdefault(r["max_wait_min"], []).append(r["util_pct"])
    for m in sorted(by_mw.keys()):
        vals = by_mw[m]
        print(f"  max_wait {m:.0f}m: util% min={min(vals):.1f} max={max(vals):.1f} avg={np.mean(vals):.1f}")

    # Markdown table (Exp 40 columns) — one table with max_wait as extra column
    results_path = ROOT / "RESULTS.md"
    md_rows = "\n".join(
        f"| 0.1× | {r['fleet']:,} | no reposition | {r['max_wait_min']:.0f}m | {r['trips']:,} | "
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
## Sweep — util% vs fleet size and max_wait (scale=0.1, no reposition)

Flat demand, scale=0.1, seed=123, duration=1440min. `reposition_enabled=False`, depots=[].
Sweep: fleet ∈ {FLEET_SIZES}, max_wait_min ∈ {MAX_WAIT_MINUTES}. Same metrics as Exp 40.

| scale | fleet | policy | max_wait | trips | served% | expired | p50 | p90 | p99 | move mi/v | revenue% (dead%) | util% (dead%) | chg N (per v) | chg time (per v) | t/veh | REPO | mi/REPO | wall |
|-------|-------|--------|----------|-------|---------|---------|-----|-----|-----|-----------|------------------|---------------|---------------|------------------|-------|------|---------|------|
{md_rows}
"""
    with open(results_path, "a") as f:
        f.write(md_block)
    print(f"\nResults appended to {results_path}")


if __name__ == "__main__":
    main()
