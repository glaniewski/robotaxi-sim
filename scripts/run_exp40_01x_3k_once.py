"""Single run: Exp 40 config @ 0.1× @ 3k (no reposition). Used to check wall time after code changes."""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

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
MAX_WAIT = 600.0
SCALE = 0.1
DEMAND_FLATTEN = 1.0
FLEET = 3000


def main() -> None:
    os.environ.setdefault("OSRM_TIME_MULTIPLIER", "1.0")
    os.environ.setdefault("OSRM_PICKUP_DROPOFF_BUFFER_MINUTES", "0.0")

    print("Loading data…")
    _df = pd.read_parquet(REQUESTS_PATH, columns=["origin_h3"])
    dcw = _df["origin_h3"].value_counts().to_dict()
    routing = RoutingCache(parquet_path=TRAVEL_CACHE, osrm_url="http://localhost:5001")

    base_reqs = load_requests(
        REQUESTS_PATH,
        duration_minutes=DURATION,
        max_wait_time_seconds=MAX_WAIT,
        demand_scale=SCALE,
        demand_flatten=DEMAND_FLATTEN,
        seed=SEED,
    )
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
        reposition_enabled=False,
    )
    vehicles = build_vehicles(
        sc, depot_h3_cells=[DEPOT_CELL], seed=SEED, demand_cells=dcw
    )

    print(f"Exp 40 @ 0.1× @ {FLEET} — running…")
    t0 = time.time()
    eng = SimulationEngine(
        config=sc,
        vehicles=vehicles,
        requests=requests,
        depots=[],
        routing=routing,
        reposition_policy=None,
    )
    res = eng.run()
    wall = time.time() - t0

    rlist = list(eng.requests.values())
    served = [r for r in rlist if r.status == RequestStatus.SERVED]
    served_pct = len(served) / len(rlist) * 100 if rlist else 0.0
    st = res.get("state_time_s") or {}
    fleet_s = FLEET * DURATION * 60.0
    move_time_s = (
        st.get("to_pickup", 0) + st.get("in_trip", 0) + st.get("repositioning", 0)
        + st.get("to_depot", 0) + st.get("charging", 0)
    )
    util_pct = move_time_s / fleet_s * 100 if fleet_s else 0.0

    print(f"  served%: {served_pct:.3f}%  util%: {util_pct:.1f}%  wall: {wall:.1f}s")
    print(f"  (Exp 40 in RESULTS.md: 86.043% served, 20.6% util, 64s wall)")


if __name__ == "__main__":
    main()
