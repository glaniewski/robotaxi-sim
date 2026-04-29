"""
Profile the sim from PROFILE_START until the last trip.
Starts cProfile when resolved >= PROFILE_START, dumps when run completes.

Usage:
    python3 scripts/profile_drain.py

Then inspect profile_drain.prof (e.g. python3 -c "import pstats; p=pstats.Stats('profile_drain.prof'); p.sort_stats('cumulative'); p.print_stats(40)").
"""
from __future__ import annotations

import cProfile
import pstats
import sys
import time
from io import StringIO
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
FLEET_SIZE = 3000

PROFILE_START = 85_000  # start profiling when resolved >= this
# Profile until end of run (no PROFILE_END; dump when run completes)
PROFILE_OUT = ROOT / "profile_drain.prof"


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


def main() -> None:
    print("Loading shared data…")
    _df = pd.read_parquet(REQUESTS_PATH, columns=["origin_h3"])
    dcw = _df["origin_h3"].value_counts().to_dict()
    dcs = set(dcw.keys())
    routing = RoutingCache(parquet_path=TRAVEL_CACHE, osrm_url="http://localhost:5001")
    covered_by = build_covered_by(TRAVEL_CACHE, dcs, MAX_WAIT)

    base_reqs = load_requests(
        REQUESTS_PATH, duration_minutes=DURATION,
        max_wait_time_seconds=MAX_WAIT, demand_scale=SCALE, seed=SEED,
    )
    timed = build_timed(base_reqs)
    flat = build_flat(base_reqs, DURATION)
    requests = [
        Request(id=r.id, request_time=r.request_time, origin_h3=r.origin_h3,
                destination_h3=r.destination_h3, max_wait_time_seconds=MAX_WAIT)
        for r in base_reqs
    ]

    sc = SimConfig(
        duration_minutes=DURATION, seed=SEED, fleet_size=FLEET_SIZE,
        max_wait_time_seconds=MAX_WAIT, reposition_enabled=True,
        reposition_alpha=0.6, reposition_top_k_cells=50,
        max_vehicles_targeting_cell=3,
    )
    vehicles = build_vehicles(sc, depot_h3_cells=[DEPOT_CELL], seed=SEED, demand_cells=dcw)
    policy = build_policy(
        name="coverage_floor",
        alpha=0.6, half_life_minutes=45, forecast_horizon_minutes=30,
        max_reposition_travel_minutes=30.0, max_vehicles_targeting_cell=3,
        min_idle_minutes=2, top_k_cells=50, reposition_lambda=0.05,
        forecast_table=flat, demand_cells=dcs, covered_by=covered_by,
        max_wait_time_seconds=MAX_WAIT, min_coverage=2,
        coverage_reposition_travel_minutes=60.0,
        timed_forecast_table=timed,
        forecast_bucket_minutes=BUCKET_MIN,
        coverage_lookahead_minutes=60.0,
    )

    total_reqs = len(requests)
    prof = cProfile.Profile()
    state = {"profiling": False}

    def _progress(resolved: int, total: int) -> None:
        if not state["profiling"] and resolved >= PROFILE_START:
            state["profiling"] = True
            prof.enable()
            print(f"\n[profile] started at resolved={resolved}, profiling until end of run", flush=True)

    bar = tqdm(total=total_reqs, desc="profile_drain", unit="trips", ncols=90)
    last_resolved = [0]

    def _progress_with_bar(resolved: int, total: int) -> None:
        delta = resolved - last_resolved[0]
        if delta > 0:
            bar.update(delta)
            last_resolved[0] = resolved
        _progress(resolved, total)

    t0 = time.time()
    eng = SimulationEngine(
        config=sc, vehicles=vehicles, requests=requests,
        depots=[], routing=routing, reposition_policy=policy,
        progress_callback=_progress_with_bar,
    )
    eng.run()
    wall = time.time() - t0
    bar.close()

    if state["profiling"]:
        prof.disable()
        prof.dump_stats(str(PROFILE_OUT))
        print(f"[profile] stopped at end of run, wrote {PROFILE_OUT}", flush=True)

    print(f"\nWall time: {wall:.1f}s")

    # Print top 50 by cumulative time
    if PROFILE_OUT.exists():
        s = StringIO()
        p = pstats.Stats(str(PROFILE_OUT), stream=s)
        p.sort_stats("cumulative")
        p.print_stats(50)
        print("\n--- Top 50 by cumulative time (profile segment only) ---\n")
        print(s.getvalue())


if __name__ == "__main__":
    main()
