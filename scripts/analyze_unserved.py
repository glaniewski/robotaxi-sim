"""
Analyze why we have unserved requests: time-of-day and origin cell distribution.
Uses same Exp 27 config (coverage_floor+optB floor=2, scale=0.01, fleet=3000).
"""
from __future__ import annotations

import sys
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


def main() -> None:
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
        collect_unserved_diagnostics=False,
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
        coverage_reposition_travel_minutes=60.0,
        timed_forecast_table=timed,
        forecast_bucket_minutes=BUCKET_MIN,
        coverage_lookahead_minutes=60.0,
    )
    eng = SimulationEngine(
        config=sc,
        vehicles=vehicles,
        requests=requests,
        depots=[],
        routing=routing,
        reposition_policy=policy,
    )
    res = eng.run()

    unserved = [r for r in eng.requests.values() if r.status == RequestStatus.UNSERVED]
    diag = res.get("unserved_diagnostics", [])
    if len(diag) != len(unserved):
        print(f"WARNING: unserved_diagnostics has {len(diag)} entries but {len(unserved)} unserved requests.")
    # Time of day: request_time is seconds from sim start (0 .. 86400 for 24h)
    by_hour: dict[int, int] = defaultdict(int)
    for r in unserved:
        hour = int(r.request_time // 3600)
        by_hour[hour] += 1
    by_cell: dict[str, int] = defaultdict(int)
    for r in unserved:
        by_cell[r.origin_h3] += 1

    print("Unserved request analysis (Exp 27 config, scale=0.01, fleet=3000)")
    print(f"Total unserved: {len(unserved)}")
    print("(After REQUEST_EXPIRE guard fix, only requests still in _pending at expiry are marked UNSERVED;")
    print(" assigned-but-not-yet-completed are no longer wrongly expired. Run-to-run variance is typical.)\n")
    print("By hour (request arrival time):")
    for h in range(24):
        n = by_hour.get(h, 0)
        if n:
            bar = "█" * n + "░" * (max(0, 20 - n))
            print(f"  {h:02d}:00  {n:3d}  {bar}")
    print("\nBy origin H3 cell (top 20):")
    for cell, count in sorted(by_cell.items(), key=lambda x: -x[1])[:20]:
        print(f"  {cell}  {count}")
    # Travel time from depot to each unserved origin (explain "far" cells)
    depot_tt: list[float] = []
    for r in unserved:
        s, _ = routing.get(DEPOT_CELL, r.origin_h3)
        depot_tt.append(s)
    if depot_tt:
        print(f"\nTravel time from depot to unserved origins: min={min(depot_tt)/60:.1f}m  max={max(depot_tt)/60:.1f}m  mean={sum(depot_tt)/len(depot_tt)/60:.1f}m")
    # Request time in last hour (sim end = 86400s)
    last_hour = [r for r in unserved if r.request_time >= 82800]  # 23:00
    if last_hour:
        req_times = sorted(r.request_time for r in last_hour)
        print(f"\nLast-hour unserved: {len(last_hour)}  request_time range: {req_times[0]/3600:.2f}h – {req_times[-1]/3600:.2f}h (sim end = 24.0h)")

    # Diagnostics: eligible_count at expiry
    if diag:
        print("\n--- Unserved diagnostics (at time of expiry) ---")
        by_eligible: dict[int, int] = defaultdict(int)
        by_source: dict[str, int] = defaultdict(int)
        for d in diag:
            by_eligible[d["eligible_count"]] += 1
            by_source[d["expiry_source"]] += 1
        print("By eligible_count (vehicles available when request expired):")
        for k in sorted(by_eligible.keys()):
            print(f"  eligible_count={k}:  {by_eligible[k]} unserved")
        print("By expiry_source:")
        for k, v in sorted(by_source.items()):
            print(f"  {k}:  {v}")
        # Sample: show a few with eligible_count > 0 (min_eta vs remaining_wait)
        mystery = [d for d in diag if d["eligible_count"] > 0]
        if mystery:
            print(f"\nRequests that expired with eligible_count > 0: {len(mystery)}")
            has_eta = [d for d in mystery if "min_eta_seconds" in d and "remaining_wait_seconds" in d]
            if has_eta:
                print("  (min_eta_at_arrival = nearest vehicle when request first arrived; min_eta/remaining_wait = at expiry)")
            for d in mystery[:10]:
                eta_arrival_s = d.get("min_eta_at_arrival_seconds")
                eta_s = d.get("min_eta_seconds")
                rem_s = d.get("remaining_wait_seconds")
                arrival_str = f"  min_eta_at_arrival={eta_arrival_s/60:.1f}m" if eta_arrival_s is not None else ""
                eta_str = f"  min_eta={eta_s/60:.1f}m" if eta_s is not None else ""
                rem_str = f"  remaining_wait={rem_s/60:.1f}m" if rem_s is not None else ""
                over = "  (min_eta > remaining_wait)" if (eta_s is not None and rem_s is not None and eta_s > rem_s) else ""
                print(f"  req {d['request_id'][:12]}...  req_t={d['request_time']/3600:.2f}h  origin={d['origin_h3']}  eligible={d['eligible_count']}{arrival_str}{eta_str}{rem_str}{over}  {d['expiry_source']}")
            if len(mystery) > 10:
                print(f"  ... and {len(mystery) - 10} more")
            if has_eta:
                over_count = sum(1 for d in has_eta if d["min_eta_seconds"] > d["remaining_wait_seconds"])
                print(f"  Summary: {over_count}/{len(has_eta)} had min_eta > remaining_wait (nearest vehicle too far to make the window)")
        zero_eligible = [d for d in diag if d["eligible_count"] == 0]
        if zero_eligible:
            print(f"\nRequests that expired with eligible_count=0 (fleet saturated): {len(zero_eligible)}")
            pending_vals = sorted(set(d["pending_count"] for d in zero_eligible), reverse=True)
            print(f"  pending_count when they expired: min={min(d['pending_count'] for d in zero_eligible)}, max={max(d['pending_count'] for d in zero_eligible)}")
        # Coverage at origin cell: what was happening before the request arrived / at expiry
        with_coverage = [d for d in diag if "coverage_at_arrival" in d]
        if with_coverage:
            print("\n--- Coverage at unserved origin cells (before request arrived / at expiry) ---")
            print("  coverage = eligible vehicles that can reach cell within max_wait")
            print("  targeting = vehicles currently repositioning toward this cell")
            print("  targeting_repositioning_count = of those, how many are in REPOSITIONING state")
            print("  in_deficit = coverage < min_cov (policy would send vehicles here)")
            for d in with_coverage:
                arr = d["coverage_at_arrival"]
                exp = d.get("coverage_at_expiry", {})
                req_t_h = d["request_time"] / 3600
                tr = arr.get("targeting_repositioning_count", "?")
                print(f"  req {d['request_id'][:14]}...  origin={d['origin_h3']}  req_t={req_t_h:.2f}h")
                print(f"    at arrival:  coverage={arr.get('coverage', '?')}  targeting={arr.get('targeting', '?')}  targeting_repositioning={tr}  min_cov={arr.get('min_cov', '?')}  in_deficit={arr.get('in_deficit', '?')}")
                print(f"    at expiry:   coverage={exp.get('coverage', '?')}  targeting={exp.get('targeting', '?')}  min_cov={exp.get('min_cov', '?')}  in_deficit={exp.get('in_deficit', '?')}")
    print("\nInterpretation: If unserved cluster in certain hours → capacity. If in certain cells → coverage/deficit.")


if __name__ == "__main__":
    main()
