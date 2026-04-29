"""
Experiment 42 — smoke test: charging with one depot (Exp 41 stack + depot).

fleet=2000, scale=0.1, flat demand, reposition off, one depot
(default scenario–style chargers). Default max_wait=20 min; override with --max-wait-min.

Usage:
  PYTHONHASHSEED=0 python3 scripts/run_exp42_single_depot.py
  PYTHONHASHSEED=0 python3 scripts/run_exp42_single_depot.py --max-wait-min 10
"""
from __future__ import annotations

import argparse
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
from app.sim.entities import Depot, Request, RequestStatus
from app.sim.routing import RoutingCache

REQUESTS_PATH = str(ROOT / "data" / "requests_austin_h3_r8.parquet")
TRAVEL_CACHE = str(ROOT / "data" / "h3_travel_cache.parquet")
DEPOT_CELL = "88489e3467fffff"

SEED = 123
DURATION = 1440
SCALE = 0.1
DEMAND_FLATTEN = 1.0
FLEET = 2000


def main() -> None:
    p = argparse.ArgumentParser(description="Exp 42: single-depot charging smoke test")
    p.add_argument(
        "--max-wait-min",
        type=float,
        default=20.0,
        help="Passenger max wait time in minutes (default 20)",
    )
    args = p.parse_args()
    max_wait_min = float(args.max_wait_min)
    max_wait_s = max_wait_min * 60.0

    os.environ.setdefault("OSRM_TIME_MULTIPLIER", "1.0")
    os.environ.setdefault("OSRM_PICKUP_DROPOFF_BUFFER_MINUTES", "0.0")

    base_reqs = load_requests(
        REQUESTS_PATH,
        duration_minutes=DURATION,
        max_wait_time_seconds=max_wait_s,
        demand_scale=SCALE,
        demand_flatten=DEMAND_FLATTEN,
        seed=SEED,
    )
    _df = pd.read_parquet(REQUESTS_PATH, columns=["origin_h3"])
    dcw = _df["origin_h3"].value_counts().to_dict()

    requests = [
        Request(
            id=r.id,
            request_time=r.request_time,
            origin_h3=r.origin_h3,
            destination_h3=r.destination_h3,
            max_wait_time_seconds=max_wait_s,
        )
        for r in base_reqs
    ]

    sc = SimConfig(
        duration_minutes=DURATION,
        seed=SEED,
        fleet_size=FLEET,
        max_wait_time_seconds=max_wait_s,
        reposition_enabled=False,
    )
    vehicles = build_vehicles(
        sc, depot_h3_cells=[DEPOT_CELL], seed=SEED, demand_cells=dcw
    )

    depot = Depot(
        id="depot_1",
        h3_cell=DEPOT_CELL,
        chargers_count=20,
        charger_kw=150.0,
        site_power_kw=1500.0,
    )

    routing = RoutingCache(parquet_path=TRAVEL_CACHE, osrm_url="http://localhost:5001")

    total_reqs = len(requests)
    mw_label = f"{max_wait_min:g}m" if max_wait_min == int(max_wait_min) else f"{max_wait_min}m"
    print(
        f"Exp 42: fleet={FLEET} max_wait={mw_label} scale={SCALE} depots=1 "
        f"| trips={total_reqs:,} | routing cache {routing.size():,} rows\n"
    )

    bar = tqdm(
        total=total_reqs,
        desc="exp42 1 depot",
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
        depots=[depot],
        routing=routing,
        reposition_policy=None,
        progress_callback=_progress,
    )
    res = eng.run()
    bar.update(total_reqs - last_resolved[0])
    bar.close()
    wall = time.time() - t0

    rlist = list(eng.requests.values())
    served = [r for r in rlist if r.status == RequestStatus.SERVED]
    expired = [r for r in rlist if r.status == RequestStatus.UNSERVED]
    wait_s = [r.actual_wait_seconds for r in served if r.actual_wait_seconds is not None]

    p50 = float(np.percentile(wait_s, 50)) / 60 if wait_s else 0.0
    p90 = float(np.percentile(wait_s, 90)) / 60 if wait_s else 0.0
    p99 = float(np.percentile(wait_s, 99)) / 60 if wait_s else 0.0

    trips_per_veh = len(served) / FLEET if FLEET else 0.0
    served_pct = len(served) / len(rlist) * 100 if rlist else 0.0

    vlist = list(eng.vehicles.values())
    total_trip_mi = sum(v.trip_miles for v in vlist)
    total_pickup_mi = sum(v.pickup_miles for v in vlist)
    total_repo_mi = sum(v.reposition_miles for v in vlist)
    total_miles = total_trip_mi + total_pickup_mi + total_repo_mi
    move_mi_veh = total_miles / FLEET if FLEET else 0.0
    deadhead_pct = (
        (total_pickup_mi + total_repo_mi) / total_miles * 100.0 if total_miles > 0 else 0.0
    )
    revenue_mi_pct = total_trip_mi / total_miles * 100.0 if total_miles > 0 else 0.0

    st = res.get("state_time_s") or {}
    fleet_s = FLEET * DURATION * 60.0
    move_time_s = (
        st.get("to_pickup", 0)
        + st.get("in_trip", 0)
        + st.get("repositioning", 0)
        + st.get("to_depot", 0)
        + st.get("charging", 0)
    )
    deadhead_time_s = (
        st.get("to_pickup", 0) + st.get("repositioning", 0) + st.get("to_depot", 0)
    )
    util_pct = move_time_s / fleet_s * 100 if fleet_s else 0.0
    deadhead_time_pct = deadhead_time_s / move_time_s * 100.0 if move_time_s > 0 else 0.0

    charge_events_total = sum(v.charge_sessions for v in vlist)
    charge_events_per_veh = charge_events_total / FLEET if FLEET else 0.0
    charge_time_total_s = st.get("charging", 0)
    charge_time_per_veh_s = charge_time_total_s / FLEET if FLEET else 0.0

    ec = res["event_counts"]
    repo = ec.get("REPOSITION_COMPLETE", 0)
    mi_per_repo = (total_repo_mi / repo) if repo > 0 else 0.0

    m = res.get("metrics") or {}
    print("--- diagnostics ---")
    print(
        f"chg N total: {charge_events_total:,} | per veh: {charge_events_per_veh:.2f} | "
        f"chg time veh: {charge_time_per_veh_s/60:.2f} min | "
        f"to_depot s (fleet): {st.get('to_depot', 0):.0f} | charging s (fleet): {st.get('charging', 0):.0f}"
    )
    print(
        f"charger_util_pct: {m.get('charger_utilization_pct', 'n/a')} | "
        f"depot_queue_p90_min: {m.get('depot_queue_p90_min', 'n/a')} | "
        f"ARRIVE_DEPOT events: {ec.get('ARRIVE_DEPOT', 0)}"
    )
    print(
        f"depot_queue_max_concurrent (peak sum of wait queues): {m.get('depot_queue_max_concurrent', 'n/a')} | "
        f"depot_queue_max_at_site (peak at one depot): {m.get('depot_queue_max_at_site', 'n/a')}"
    )
    print(f"wall_s: {wall:.1f}")

    chg_per_v = f'"{charge_events_per_veh:.2f}"'
    chg_time_per_v = f'"{charge_time_per_veh_s/60:.1f}m"'
    chg_time_fleet = f"{charge_time_total_s/60:.0f}m"
    row_mw = f"{int(max_wait_min)}m" if max_wait_min == int(max_wait_min) else f"{max_wait_min}m"
    print("\n--- RESULTS.md row ---")
    print(
        f"| 0.1×  | {FLEET:,} | no reposition | 1 depot        | {row_mw:<8} | {len(rlist):,} | "
        f"{served_pct:.3f}%  | {len(expired):,}  | {p50:.1f}m | {p90:.1f}m  | {p99:.1f}m  | "
        f"{move_mi_veh:.1f}     | {revenue_mi_pct:.1f}% ({deadhead_pct:.1f}%)    | "
        f"{util_pct:.1f}% ({deadhead_time_pct:.1f}%)  | {charge_events_total:,} | {chg_per_v}      | "
        f"{chg_time_fleet}   | {chg_time_per_v}          | "
        f"{trips_per_veh:.1f}  | {repo}    | {mi_per_repo:.1f}     | {wall:.0f}s  |"
    )


if __name__ == "__main__":
    main()
