"""
Experiment 54 iter2 — Hold best policy (cap=10%, ratio=2.0), sweep charging infra.

The binding constraint is now charger infrastructure, not policy.
Test more plugs, higher power, and more sites.
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

# Best policy from iter1
CHARGE_SUPPLY_RATIO = 2.0
MAX_CONCURRENT_CHARGING_PCT = 0.10

SWEEP = [
    {"label": "50s_4p_20kW",  "n_sites": 50,  "plugs": 4, "kw": 20.0,  "site_kw": 80.0},
    {"label": "50s_8p_20kW",  "n_sites": 50,  "plugs": 8, "kw": 20.0,  "site_kw": 160.0},
    {"label": "50s_4p_50kW",  "n_sites": 50,  "plugs": 4, "kw": 50.0,  "site_kw": 200.0},
    {"label": "50s_8p_50kW",  "n_sites": 50,  "plugs": 8, "kw": 50.0,  "site_kw": 400.0},
    {"label": "100s_4p_20kW", "n_sites": 100, "plugs": 4, "kw": 20.0,  "site_kw": 80.0},
    {"label": "100s_8p_50kW", "n_sites": 100, "plugs": 8, "kw": 50.0,  "site_kw": 400.0},
]


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


def run_one(cfg: dict, dcw: dict[str, int], dcs: set[str], covered_by: dict) -> dict:
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
            id=r.id, request_time=r.request_time,
            origin_h3=r.origin_h3, destination_h3=r.destination_h3,
            max_wait_time_seconds=MAX_WAIT,
        )
        for r in base_reqs
    ]

    n_sites = cfg["n_sites"]
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
        charging_depot_selection="fastest",
        charge_supply_ratio=CHARGE_SUPPLY_RATIO,
        max_concurrent_charging_pct=MAX_CONCURRENT_CHARGING_PCT,
    )
    vehicles = build_vehicles(sc, depot_h3_cells=top_demand_cells(max(n_sites, 1)), seed=SEED, demand_cells=dcw)
    policy = build_policy(
        name="coverage_floor",
        alpha=0.6, half_life_minutes=45, forecast_horizon_minutes=30,
        max_reposition_travel_minutes=30.0, max_vehicles_targeting_cell=3,
        min_idle_minutes=2, top_k_cells=50, reposition_lambda=0.05,
        forecast_table=flat, demand_cells=dcs, covered_by=covered_by,
        max_wait_time_seconds=MAX_WAIT, min_coverage=2,
        coverage_reposition_travel_minutes=60.0,
        timed_forecast_table=timed, forecast_bucket_minutes=BUCKET_MIN,
        coverage_lookahead_minutes=60.0,
    )

    site_cells = top_demand_cells(n_sites)
    depots = [
        Depot(
            id=f"micro_{i+1:03d}", h3_cell=cell,
            chargers_count=cfg["plugs"], charger_kw=cfg["kw"],
            site_power_kw=cfg["site_kw"],
        )
        for i, cell in enumerate(site_cells)
    ]

    total_reqs = len(requests)
    label = cfg["label"]
    bar = tqdm(
        total=total_reqs, desc=f"exp54i2 {label}", unit="trips", ncols=110,
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
        config=sc, vehicles=vehicles, requests=requests, depots=depots,
        routing=routing, reposition_policy=policy, progress_callback=_progress,
    )
    res = eng.run()
    bar.update(total_reqs - last_resolved[0])
    bar.close()
    wall = time.time() - t0

    m = res["metrics"]
    depot_u = summarize_charger_util_by_depot(m["charger_utilization_by_depot_pct"])
    net_energy = round(m["fleet_battery_pct"] - sc.soc_initial * 100, 2)
    total_plugs = n_sites * cfg["plugs"]
    nameplate_kw = total_plugs * cfg["kw"]
    return {
        "label": label,
        "n_sites": n_sites,
        "plugs_per_site": cfg["plugs"],
        "kw_per_plug": cfg["kw"],
        "total_plugs": total_plugs,
        "nameplate_kw": nameplate_kw,
        "served_pct": m["served_pct"],
        "p90_wait_min": m["p90_wait_min"],
        "median_wait_min": m["median_wait_min"],
        "sla_adherence_pct": m["sla_adherence_pct"],
        "repositioning_pct": m["repositioning_pct"],
        "contribution_margin_per_trip": m["contribution_margin_per_trip"],
        "charger_utilization_pct": m["charger_utilization_pct"],
        **depot_u,
        "fleet_battery_pct": m["fleet_battery_pct"],
        "fleet_soc_median_pct": m["fleet_soc_median_pct"],
        "vehicles_below_soc_target_count": m["vehicles_below_soc_target_count"],
        "total_charge_sessions": m["total_charge_sessions"],
        "net_energy_pct": net_energy,
        "wall_s": round(wall, 1),
    }


def main() -> None:
    df = pd.read_parquet(REQUESTS_PATH, columns=["origin_h3"])
    dcw = df["origin_h3"].value_counts().to_dict()
    dcs = set(dcw.keys())
    covered_by = build_covered_by(TRAVEL_CACHE, dcs, MAX_WAIT)

    rows: list[dict] = []
    for cfg in tqdm(SWEEP, desc="exp54i2 infra sweep", unit="run"):
        row = run_one(cfg, dcw, dcs, covered_by)
        rows.append(row)
        print(json.dumps(row, sort_keys=True))
    print("\nFINAL_ROWS=" + json.dumps(rows, sort_keys=True))


if __name__ == "__main__":
    main()
