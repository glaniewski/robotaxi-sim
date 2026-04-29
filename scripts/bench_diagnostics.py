"""
Run the same Exp 27 config twice: with and without unserved diagnostics.
Reports wall time for each to compare diagnostic overhead.
"""
from __future__ import annotations

import sys
import time
from collections import defaultdict
from pathlib import Path

import pandas as pd

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
FLEET, SCALE, SEED = 3000, 0.01, 123
DURATION, MAX_WAIT = 1440, 600.0
BUCKET_MIN = 15.0


def build_timed(reqs, bm: float = 15.0) -> dict:
    bs = bm * 60
    nb = int(round(1440 / bm))
    c: dict = {}
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


def run_once(collect_diagnostics: bool) -> float:
    base_reqs = load_requests(REQUESTS_PATH, duration_minutes=DURATION, demand_scale=SCALE, seed=SEED)
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
    _df = pd.read_parquet(REQUESTS_PATH, columns=["origin_h3"])
    dcw = _df["origin_h3"].value_counts().to_dict()
    dcs = set(dcw.keys())
    timed = build_timed(base_reqs)
    flat = build_flat(base_reqs, DURATION)
    routing = RoutingCache(parquet_path=TRAVEL_CACHE, osrm_url="http://localhost:5001")
    covered_by = build_covered_by(TRAVEL_CACHE, dcs, MAX_WAIT)
    sc = SimConfig(
        duration_minutes=DURATION,
        seed=SEED,
        fleet_size=FLEET,
        max_wait_time_seconds=MAX_WAIT,
        reposition_enabled=True,
        reposition_alpha=0.6,
        reposition_top_k_cells=50,
        max_vehicles_targeting_cell=3,
        collect_unserved_diagnostics=collect_diagnostics,
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
        coverage_reposition_travel_minutes=30.0,
        timed_forecast_table=timed,
        forecast_bucket_minutes=BUCKET_MIN,
        coverage_lookahead_minutes=30.0,
    )
    eng = SimulationEngine(
        config=sc,
        vehicles=vehicles,
        requests=requests,
        depots=[],
        routing=routing,
        reposition_policy=policy,
    )
    t0 = time.perf_counter()
    eng.run()
    return time.perf_counter() - t0


def main() -> None:
    print("Exp 27 config (scale=0.01, fleet=3000). Each run is a fresh sim.\n")
    print("Running without diagnostics...")
    t_no = run_once(collect_diagnostics=False)
    print(f"  Wall time: {t_no:.2f}s\n")
    print("Running with diagnostics...")
    t_yes = run_once(collect_diagnostics=True)
    print(f"  Wall time: {t_yes:.2f}s\n")
    print("Summary:")
    print(f"  Without diagnostics: {t_no:.2f}s")
    print(f"  With diagnostics:    {t_yes:.2f}s")
    print(f"  Overhead:           {t_yes - t_no:.2f}s ({100 * (t_yes - t_no) / t_no:.1f}%)")


if __name__ == "__main__":
    main()
