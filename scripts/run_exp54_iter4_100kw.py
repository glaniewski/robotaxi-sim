"""
Experiment 54 iter4 — Single run with 100kW chargers to test if charging speed
closes the remaining -0.34% gap, or if the residual is purely vehicles mid-trip at horizon.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import pandas as pd
from tqdm import tqdm

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "backend"))

from app.sim.demand import load_requests
from app.sim.engine import SimConfig, SimulationEngine, build_vehicles
from app.sim.entities import Depot, Request, VehicleState
from app.sim.metrics import summarize_charger_util_by_depot
from app.sim.reposition_policies import build_covered_by, build_policy
from app.sim.routing import RoutingCache

REQUESTS_PATH = str(ROOT / "data" / "requests_austin_h3_r8.parquet")
TRAVEL_CACHE = str(ROOT / "data" / "h3_travel_cache.parquet")

SEED = 123
DURATION = 1440
MAX_WAIT = 600.0
BUCKET_MIN = 15.0
SCALE = 0.1
FLEET = 3000
N_SITES = 50
PLUGS_PER_SITE = 8
CHARGER_KW = 100.0
SITE_POWER_KW = 800.0


def build_timed(reqs, bm=15.0):
    bs = bm * 60.0
    nb = int(round(1440.0 / bm))
    counts = {}
    for r in reqs:
        b = int(r.request_time / bs) % nb
        counts.setdefault(r.origin_h3, {})
        counts[r.origin_h3][b] = counts[r.origin_h3].get(b, 0) + 1
    return {cell: {b: v / bs for b, v in bk.items()} for cell, bk in counts.items()}


def build_flat(reqs, dur):
    counts = {}
    for r in reqs:
        counts[r.origin_h3] = counts.get(r.origin_h3, 0) + 1
    return {c: n / (dur * 60.0) for c, n in counts.items()}


def top_demand_cells(n):
    df = pd.read_parquet(REQUESTS_PATH, columns=["origin_h3"])
    return df["origin_h3"].value_counts().head(n).index.tolist()


def main():
    df = pd.read_parquet(REQUESTS_PATH, columns=["origin_h3"])
    dcw = df["origin_h3"].value_counts().to_dict()
    dcs = set(dcw.keys())
    covered_by = build_covered_by(TRAVEL_CACHE, dcs, MAX_WAIT)

    routing = RoutingCache(parquet_path=TRAVEL_CACHE, osrm_url="http://localhost:5001")
    base_reqs = load_requests(REQUESTS_PATH, duration_minutes=DURATION,
                              max_wait_time_seconds=MAX_WAIT, demand_scale=SCALE, seed=SEED)
    timed = build_timed(base_reqs, BUCKET_MIN)
    flat = build_flat(base_reqs, DURATION)
    requests = [
        Request(id=r.id, request_time=r.request_time, origin_h3=r.origin_h3,
                destination_h3=r.destination_h3, max_wait_time_seconds=MAX_WAIT)
        for r in base_reqs
    ]
    sc = SimConfig(
        duration_minutes=DURATION, seed=SEED, fleet_size=FLEET,
        max_wait_time_seconds=MAX_WAIT, reposition_enabled=True,
        reposition_alpha=0.6, reposition_top_k_cells=50,
        max_vehicles_targeting_cell=3, kwh_per_mile=0.20,
        charging_queue_policy="jit", charging_depot_selection="fastest",
        charge_supply_ratio=2.0, max_concurrent_charging_pct=0.10,
    )
    vehicles = build_vehicles(sc, depot_h3_cells=top_demand_cells(N_SITES), seed=SEED, demand_cells=dcw)
    policy = build_policy(
        name="coverage_floor", alpha=0.6, half_life_minutes=45,
        forecast_horizon_minutes=30, max_reposition_travel_minutes=30.0,
        max_vehicles_targeting_cell=3, min_idle_minutes=2, top_k_cells=50,
        reposition_lambda=0.05, forecast_table=flat, demand_cells=dcs,
        covered_by=covered_by, max_wait_time_seconds=MAX_WAIT, min_coverage=2,
        coverage_reposition_travel_minutes=60.0, timed_forecast_table=timed,
        forecast_bucket_minutes=BUCKET_MIN, coverage_lookahead_minutes=60.0,
    )
    site_cells = top_demand_cells(N_SITES)
    depots = [
        Depot(id=f"micro_{i+1:03d}", h3_cell=cell, chargers_count=PLUGS_PER_SITE,
              charger_kw=CHARGER_KW, site_power_kw=SITE_POWER_KW)
        for i, cell in enumerate(site_cells)
    ]

    bar = tqdm(total=len(requests), desc="exp54i4 100kW", unit="trips", ncols=110,
               bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]")
    last_resolved = [0]
    def _progress(resolved, total):
        delta = resolved - last_resolved[0]
        if delta > 0:
            bar.update(delta)
            last_resolved[0] = resolved

    t0 = time.time()
    eng = SimulationEngine(config=sc, vehicles=vehicles, requests=requests, depots=depots,
                           routing=routing, reposition_policy=policy, progress_callback=_progress)
    res = eng.run()
    bar.update(len(requests) - last_resolved[0])
    bar.close()
    wall = time.time() - t0

    m = res["metrics"]
    depot_u = summarize_charger_util_by_depot(m["charger_utilization_by_depot_pct"])
    net = round(m["fleet_battery_pct"] - sc.soc_initial * 100, 2)

    # Count vehicles NOT idle at end of sim (mid-trip / mid-charge / etc.)
    busy_at_end = sum(1 for v in eng.vehicles.values() if v.state != VehicleState.IDLE)
    below_tgt_and_busy = sum(1 for v in eng.vehicles.values()
                             if v.soc < sc.soc_target and v.state != VehicleState.IDLE)

    row = {
        "served_pct": m["served_pct"],
        "p90_wait_min": m["p90_wait_min"],
        "median_wait_min": m["median_wait_min"],
        "fleet_battery_pct": m["fleet_battery_pct"],
        "fleet_soc_median_pct": m["fleet_soc_median_pct"],
        "vehicles_below_soc_target_count": m["vehicles_below_soc_target_count"],
        "net_energy_pct": net,
        "charger_utilization_pct": m["charger_utilization_pct"],
        **depot_u,
        "total_charge_sessions": m["total_charge_sessions"],
        "contribution_margin_per_trip": m["contribution_margin_per_trip"],
        "busy_at_end": busy_at_end,
        "below_target_and_busy": below_tgt_and_busy,
        "wall_s": round(wall, 1),
    }
    print(json.dumps(row, sort_keys=True))


if __name__ == "__main__":
    main()
