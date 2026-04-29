"""
Experiment 44 — Microsite sweep (3k fleet, scale=0.1, 4x20kW sites)
=====================================================================

Runs Exp30-like policy stack with:
- fleet=3000
- demand_scale=0.1
- max_wait=600s
- cov_floor+optB policy settings

Sweeps number of charging microsites, where each site is:
- 4 plugs
- 20 kW per plug
- 80 kW site power
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

MICROSITE_COUNTS = [50, 60, 70, 80]
PLUGS_PER_SITE = 4
CHARGER_KW = 20.0
SITE_POWER_KW = 80.0


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


def run_one(n_sites: int, dcw: dict[str, int], dcs: set[str], covered_by: dict) -> dict:
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
    )
    vehicles = build_vehicles(sc, depot_h3_cells=top_demand_cells(max(n_sites, 1)), seed=SEED, demand_cells=dcw)
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

    site_cells = top_demand_cells(n_sites)
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
    bar = tqdm(
        total=total_reqs,
        desc=f"exp44 microsites={n_sites}",
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
    st = res.get("state_time_s") or {}
    vlist = list(eng.vehicles.values())
    chg_n = sum(v.charge_sessions for v in vlist)
    chg_per_v = chg_n / FLEET if FLEET else 0.0
    chg_time_min = st.get("charging", 0.0) / 60.0
    chg_time_per_v_min = chg_time_min / FLEET if FLEET else 0.0

    return {
        "microsites": n_sites,
        "plugs_total": n_sites * PLUGS_PER_SITE,
        "served_pct": m["served_pct"],
        "median_wait_min": m["median_wait_min"],
        "p90_wait_min": m["p90_wait_min"],
        "sla_adherence_pct": m["sla_adherence_pct"],
        "repositioning_pct": m["repositioning_pct"],
        "contribution_margin_per_trip": m["contribution_margin_per_trip"],
        "depot_queue_p90_min": m["depot_queue_p90_min"],
        "depot_queue_max_concurrent": m["depot_queue_max_concurrent"],
        "depot_queue_max_at_site": m["depot_queue_max_at_site"],
        "charger_utilization_pct": m["charger_utilization_pct"],
        "chg_n": chg_n,
        "chg_per_v": chg_per_v,
        "chg_time_min": chg_time_min,
        "chg_time_per_v_min": chg_time_per_v_min,
        "wall_s": wall,
    }


def main() -> None:
    df = pd.read_parquet(REQUESTS_PATH, columns=["origin_h3"])
    dcw = df["origin_h3"].value_counts().to_dict()
    dcs = set(dcw.keys())
    covered_by = build_covered_by(TRAVEL_CACHE, dcs, MAX_WAIT)

    rows: list[dict] = []
    for n in tqdm(MICROSITE_COUNTS, desc="exp44 sweep", unit="config"):
        row = run_one(n, dcw, dcs, covered_by)
        rows.append(row)
        print(json.dumps(row, sort_keys=True))
    print("FINAL_ROWS=" + json.dumps(rows, sort_keys=True))


if __name__ == "__main__":
    main()

