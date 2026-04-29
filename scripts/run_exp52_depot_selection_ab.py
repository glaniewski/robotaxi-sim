"""
Experiment 52 — charging_depot_selection: fastest vs fastest_balanced (JIT microsites)

Same stack as Exp51 JIT @ 50 microsites (4×20 kW, 80 kW site, fleet=3000, scale=0.1).
A/B only `charging_depot_selection` + default slack (3 min). Requires OSRM + parquet data.
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
from app.sim.entities import Depot, Request
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
PLUGS_PER_SITE = 4
CHARGER_KW = 20.0
SITE_POWER_KW = 80.0

RUNS: list[str] = ["fastest", "fastest_balanced"]


def build_timed(reqs: list[Request], bm: float = 15.0) -> dict[str, dict[int, float]]:
    bs = bm * 60.0
    nb = int(round(1440.0 / bm))
    counts: dict[str, dict[int, int]] = {}
    for r in reqs:
        b = int(r.request_time / bs) % nb
        counts.setdefault(r.origin_h3, {})
        counts[r.origin_h3][b] = counts[r.origin_h3].get(b, 0) + 1
    return {cell: {b: v / bs for b, v in by_bucket.items()} for cell, by_bucket in counts.items()}


def build_flat(reqs: list[Request], dur: float) -> dict[str, float]:
    counts: dict[str, int] = {}
    for r in reqs:
        counts[r.origin_h3] = counts.get(r.origin_h3, 0) + 1
    return {cell: c / (dur * 60.0) for cell, c in counts.items()}


def top_demand_cells(n: int) -> list[str]:
    df = pd.read_parquet(REQUESTS_PATH, columns=["origin_h3"])
    vc = df["origin_h3"].value_counts()
    return vc.head(n).index.tolist()


def run_one(
    depot_selection: str,
    dcw: dict[str, int],
    dcs: set[str],
    covered_by: dict,
) -> dict:
    routing = RoutingCache(parquet_path=TRAVEL_CACHE, osrm_url="http://localhost:5001")
    base_reqs = load_requests(
        REQUESTS_PATH,
        duration_minutes=DURATION,
        max_wait_time_seconds=MAX_WAIT,
        demand_scale=SCALE,
        seed=SEED,
    )
    timed = build_timed(base_reqs, BUCKET_MIN)
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
        fleet_size=FLEET,
        max_wait_time_seconds=MAX_WAIT,
        reposition_enabled=True,
        reposition_alpha=0.6,
        reposition_top_k_cells=50,
        max_vehicles_targeting_cell=3,
        kwh_per_mile=0.20,
        charging_queue_policy="jit",
        charging_depot_selection=depot_selection,
        charging_depot_balance_slack_minutes=3.0,
    )
    vehicles = build_vehicles(sc, depot_h3_cells=top_demand_cells(max(N_SITES, 1)), seed=SEED, demand_cells=dcw)
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

    site_cells = top_demand_cells(N_SITES)
    depots = [
        Depot(
            id=f"micro_{i+1:03d}",
            h3_cell=cell,
            chargers_count=PLUGS_PER_SITE,
            charger_kw=CHARGER_KW,
            site_power_kw=SITE_POWER_KW,
        )
        for i, cell in enumerate(site_cells)
    ]

    total_reqs = len(requests)
    desc = f"exp52 {depot_selection}"
    bar = tqdm(
        total=total_reqs,
        desc=desc,
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
        depots=depots,
        routing=routing,
        reposition_policy=policy,
        progress_callback=_progress,
    )
    res = eng.run()
    bar.update(total_reqs - last_resolved[0])
    bar.close()
    wall = time.time() - t0

    m = res["metrics"]
    depot_u = summarize_charger_util_by_depot(m["charger_utilization_by_depot_pct"])
    return {
        "microsites": N_SITES,
        "charging_queue_policy": "jit",
        "charging_depot_selection": depot_selection,
        "charging_depot_balance_slack_minutes": 3.0,
        "plugs_total": N_SITES * PLUGS_PER_SITE,
        "served_pct": m["served_pct"],
        "p90_wait_min": m["p90_wait_min"],
        "median_wait_min": m["median_wait_min"],
        "sla_adherence_pct": m["sla_adherence_pct"],
        "repositioning_pct": m["repositioning_pct"],
        "contribution_margin_per_trip": m["contribution_margin_per_trip"],
        "pool_match_pct": m["pool_match_pct"],
        "depot_queue_p90_min": m["depot_queue_p90_min"],
        "depot_queue_max_concurrent": m["depot_queue_max_concurrent"],
        "depot_queue_max_at_site": m["depot_queue_max_at_site"],
        "charger_utilization_pct": m["charger_utilization_pct"],
        **depot_u,
        "fleet_battery_pct": m["fleet_battery_pct"],
        "fleet_soc_median_pct": m["fleet_soc_median_pct"],
        "vehicles_below_soc_target_count": m["vehicles_below_soc_target_count"],
        "total_charge_sessions": m["total_charge_sessions"],
        "wall_s": round(wall, 1),
    }


def main() -> None:
    df = pd.read_parquet(REQUESTS_PATH, columns=["origin_h3"])
    dcw = df["origin_h3"].value_counts().to_dict()
    dcs = set(dcw.keys())
    covered_by = build_covered_by(TRAVEL_CACHE, dcs, MAX_WAIT)

    rows: list[dict] = []
    for sel in tqdm(RUNS, desc="exp52 depot selection", unit="run"):
        row = run_one(sel, dcw, dcs, covered_by)
        rows.append(row)
        print(json.dumps(row, sort_keys=True))
    print("FINAL_ROWS=" + json.dumps(rows, sort_keys=True))


if __name__ == "__main__":
    main()
