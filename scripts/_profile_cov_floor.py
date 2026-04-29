"""Profile cov_floor+optB policy at scale=0.05 to find walltime hotspots."""
import sys; sys.path.insert(0, "backend")
import time, pandas as pd
from tqdm import tqdm
from app.sim.demand import load_requests
from app.sim.engine import SimConfig, SimulationEngine, build_vehicles
from app.sim.entities import Request
from app.sim.reposition_policies import build_covered_by, build_policy
from app.sim.routing import RoutingCache

REQUESTS_PATH = "data/requests_austin_h3_r8.parquet"
TRAVEL_CACHE  = "data/h3_travel_cache.parquet"
FLEET = 3000; SCALE = 0.05; SEED = 123; DURATION = 1440; MAX_WAIT = 600.0
BUCKET_MIN = 15.0; DEPOT_CELL = "88489e3467fffff"

base_reqs = load_requests(REQUESTS_PATH, duration_minutes=DURATION,
                          max_wait_time_seconds=MAX_WAIT, demand_scale=SCALE, seed=SEED)
routing = RoutingCache(parquet_path=TRAVEL_CACHE, osrm_url="http://localhost:5001")
_df = pd.read_parquet(REQUESTS_PATH, columns=["origin_h3"])
dcw = _df["origin_h3"].value_counts().to_dict()
dcs = set(dcw.keys())

def _flat(reqs, dur):
    c: dict = {}
    for r in reqs: c[r.origin_h3] = c.get(r.origin_h3, 0) + 1
    return {cell: cnt / (dur * 60) for cell, cnt in c.items()}

def _timed(reqs, bm=15.0):
    bs = bm * 60.0; nb = int(round(1440.0 / bm)); c: dict = {}
    for r in reqs:
        b = int(r.request_time / bs) % nb
        c.setdefault(r.origin_h3, {})
        c[r.origin_h3][b] = c[r.origin_h3].get(b, 0) + 1
    return {cell: {b: v / bs for b, v in bm2.items()} for cell, bm2 in c.items()}

flat     = _flat(base_reqs, DURATION)
timed    = _timed(base_reqs)
covered_by = build_covered_by(TRAVEL_CACHE, dcs, MAX_WAIT)
requests = [Request(id=r.id, request_time=r.request_time, origin_h3=r.origin_h3,
                    destination_h3=r.destination_h3, max_wait_time_seconds=MAX_WAIT)
            for r in base_reqs]
sc = SimConfig(duration_minutes=DURATION, seed=SEED, fleet_size=FLEET,
               max_wait_time_seconds=MAX_WAIT, reposition_enabled=True,
               reposition_alpha=0.6, reposition_top_k_cells=50, max_vehicles_targeting_cell=3)
vehicles = build_vehicles(sc, depot_h3_cells=[DEPOT_CELL], seed=SEED, demand_cells=dcw)
policy = build_policy(
    name="coverage_floor", alpha=0.6, half_life_minutes=45, forecast_horizon_minutes=30,
    max_reposition_travel_minutes=30.0, max_vehicles_targeting_cell=3, min_idle_minutes=2,
    top_k_cells=50, reposition_lambda=0.05, forecast_table=flat, demand_cells=dcs,
    covered_by=covered_by, max_wait_time_seconds=MAX_WAIT, min_coverage=2,
    coverage_reposition_travel_minutes=30.0,
    timed_forecast_table=timed, forecast_bucket_minutes=BUCKET_MIN,
    coverage_lookahead_minutes=30.0,
)

bar = tqdm(total=len(requests), desc="cov_floor+optB scale=0.05", unit="trips", ncols=90)
last_resolved = [0]
def _progress(resolved, total):
    delta = resolved - last_resolved[0]
    if delta > 0:
        bar.update(delta); last_resolved[0] = resolved

t0 = time.time()
eng = SimulationEngine(config=sc, vehicles=vehicles, requests=requests, depots=[],
                       routing=routing, reposition_policy=policy, progress_callback=_progress)
eng.run()
bar.update(len(requests) - last_resolved[0]); bar.close()
print(f"done in {time.time()-t0:.1f}s")
