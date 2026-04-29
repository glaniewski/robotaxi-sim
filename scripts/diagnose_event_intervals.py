"""
Diagnostics: event counts per sim-time interval (e.g. per hour).
Run with fleet=5000, scale=0.1 to see why wall-clock slows after ~trip 85600.

Usage:
    python3 scripts/diagnose_event_intervals.py

Output: event counts per 60-min bucket, and sim time of the 85600th TRIP_COMPLETE
so we can see which intervals have event explosion.
"""
from __future__ import annotations

import sys
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd
from tqdm import tqdm

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "backend"))

from app.sim.demand import load_requests
from app.sim.engine import SimConfig, SimulationEngine, build_vehicles
from app.sim.entities import Request
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
FLEET_SIZE = 5000
BUCKET_MINUTES = 60
TRIP_MILESTONE = 85600  # investigate slowdown after this many trips (TRIP_COMPLETE)


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
        collect_event_log=True,
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
    bar = tqdm(total=total_reqs, desc="diagnose events", unit="trips", ncols=90)
    last_resolved = [0]

    def _progress(resolved: int, total: int) -> None:
        delta = resolved - last_resolved[0]
        if delta > 0:
            bar.update(delta)
            last_resolved[0] = resolved

    eng = SimulationEngine(
        config=sc, vehicles=vehicles, requests=requests,
        depots=[], routing=routing, reposition_policy=policy,
        progress_callback=_progress,
    )
    res = eng.run()
    bar.close()

    event_log = res.get("event_log")
    if not event_log:
        print("No event_log in result (collect_event_log=True?)")
        return

    # Evidence: pending vs eligible in the drain
    drain_log = res.get("drain_dispatch_log", [])
    if drain_log:
        print("\n--- Evidence: pending vs eligible in drain (each DISPATCH when sim time > 24h) ---")
        pending_vals = [p for (_, p, _) in drain_log]
        eligible_vals = [e for (_, _, e) in drain_log]
        print(f"  DISPATCH events in drain: {len(drain_log):,}")
        print(f"  pending: min={min(pending_vals):,}  max={max(pending_vals):,}  mean={sum(pending_vals)/len(pending_vals):.0f}")
        print(f"  eligible: min={min(eligible_vals):,}  max={max(eligible_vals):,}  mean={sum(eligible_vals)/len(eligible_vals):.0f}")
        print("  First 5 (t_s, pending, eligible):", drain_log[:5])
        print("  Last 5 (t_s, pending, eligible):", drain_log[-5:])
    else:
        print("\n--- No drain_dispatch_log (no DISPATCH ran after 24h or log not collected) ---")

    # Last snapshots (within 24h) show trend of pending and eligible
    timeseries = res.get("timeseries", [])
    if timeseries and "eligible_count" in timeseries[-1]:
        print("\n--- Last 6 snapshots (within 24h): pending_requests, eligible_count ---")
        for s in timeseries[-6:]:
            t_min = s.get("t_minutes", 0)
            pr = s.get("pending_requests", 0)
            ec = s.get("eligible_count", "N/A")
            print(f"  t={t_min:.0f} min  pending={pr:,}  eligible={ec:,}")
    elif timeseries:
        print("\n--- Last 6 snapshots (eligible_count not in snapshot; upgrade engine) ---")
        for s in timeseries[-6:]:
            print(f"  {s}")

    # Drain reposition debug: coverage table bucket and coverage-target decisions when t > 24h
    drain_repo_debug = res.get("drain_reposition_debug", [])
    if drain_repo_debug:
        # (current_time, lookahead_bucket, chose_coverage_target)
        n_drain_calls = len(drain_repo_debug)
        n_coverage = sum(1 for (_, _, chose) in drain_repo_debug if chose)
        bucket_counts: dict[int, int] = defaultdict(int)
        for (_, b, _) in drain_repo_debug:
            if b >= 0:
                bucket_counts[b] += 1
        print("\n--- Reposition in drain: coverage table (next-day bucket?) ---")
        print(f"  select_target calls in drain (t>24h): {n_drain_calls:,}")
        print(f"  Chose coverage (deficit) target: {n_coverage:,} ({100*n_coverage/n_drain_calls:.1f}%)" if n_drain_calls else "  (none)")
        if bucket_counts:
            print("  Lookahead bucket distribution (bucket = (t+lookahead)/bucket_s % n_buckets; 0=midnight, 4=1am, ...):")
            for b in sorted(bucket_counts.keys()):
                print(f"    bucket {b}: {bucket_counts[b]:,} calls")
    else:
        print("\n--- No drain_reposition_debug (policy not CoverageFloor or not collected) ---")

    bucket_s = BUCKET_MINUTES * 60.0
    # bucket index by sim time (seconds)
    buckets: dict[int, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for t, etype in event_log:
        b = int(t / bucket_s)
        buckets[b][etype] += 1

    # 85600th TRIP_COMPLETE (1-based trip count)
    trip_complete_count = 0
    t_at_85600: float | None = None
    bucket_at_85600: int | None = None
    for t, etype in event_log:
        if etype == "TRIP_COMPLETE":
            trip_complete_count += 1
            if trip_complete_count == TRIP_MILESTONE:
                t_at_85600 = t
                bucket_at_85600 = int(t / bucket_s)
                break

    # Report
    all_buckets = sorted(buckets.keys())
    event_types = sorted(set(k for b in buckets.values() for k in b))
    print(f"\nEvent counts per {BUCKET_MINUTES}-min sim-time bucket (fleet={FLEET_SIZE}, scale={SCALE})")
    print(f"Total events logged: {len(event_log):,}")
    if t_at_85600 is not None:
        print(f"Sim time at {TRIP_MILESTONE}th TRIP_COMPLETE: {t_at_85600:.0f}s = {t_at_85600/3600:.2f}h (bucket {bucket_at_85600})")
    print()

    # Header
    hdr = f"{'bucket':>6} {'hr':>5} {'total':>8}"
    for e in event_types:
        hdr += f" {e:>10}"
    print(hdr)
    print("-" * (20 + 10 * len(event_types)))

    for b in all_buckets:
        hr = b * BUCKET_MINUTES / 60.0
        row = buckets[b]
        total = sum(row.values())
        marker = "  <-- 85600th trip here" if b == bucket_at_85600 else ""
        if t_at_85600 is not None and b >= bucket_at_85600 and b > (bucket_at_85600 or 0):
            marker = "  (post-slowdown)" if not marker else marker
        line = f"{b:>6} {hr:>5.1f} {total:>8,}"
        for e in event_types:
            line += f" {row.get(e, 0):>10,}"
        print(line + marker)

    # Summary: events in the bucket containing 85600 and the next few buckets
    if bucket_at_85600 is not None:
        print("\n--- Events in slowdown region (bucket of 85600th trip and following) ---")
        for b in all_buckets:
            if b >= bucket_at_85600:
                row = buckets[b]
                total = sum(row.values())
                disp = row.get("DISPATCH", 0)
                exp = row.get("REQUEST_EXPIRE", 0)
                arr = row.get("REQUEST_ARRIVAL", 0)
                trip = row.get("TRIP_COMPLETE", 0)
                idle = row.get("VEHICLE_IDLE", 0)
                print(f"  bucket {b} ({b*BUCKET_MINUTES/60:.1f}h): total={total:,}  DISPATCH={disp:,}  REQUEST_EXPIRE={exp:,}  REQUEST_ARRIVAL={arr:,}  TRIP_COMPLETE={trip:,}  VEHICLE_IDLE={idle:,}")

    # Reposition spike: last in-window hours vs drain (coverage table next-day?)
    if all_buckets:
        b_start = max(0, (bucket_at_85600 or 24) - 3)
        b_end = max(all_buckets) + 1
        print("\n--- Reposition spike (REPOSITION_COMPLETE by bucket) ---")
        for b in range(b_start, min(b_start + 6, b_end)):
            repo = buckets.get(b, {}).get("REPOSITION_COMPLETE", 0)
            label = " (drain)" if b >= 24 else ""
            print(f"  bucket {b} ({b*BUCKET_MINUTES/60:.1f}h): REPOSITION_COMPLETE={repo:,}{label}")
        if bucket_at_85600 is not None and bucket_at_85600 >= 24:
            in_window_repo = sum(buckets.get(b, {}).get("REPOSITION_COMPLETE", 0) for b in all_buckets if b < 24)
            drain_repo = sum(buckets.get(b, {}).get("REPOSITION_COMPLETE", 0) for b in all_buckets if b >= 24)
            repo_23 = buckets.get(23, {}).get("REPOSITION_COMPLETE", 0)
            print(f"  In-window (b<24) total REPOSITION_COMPLETE: {in_window_repo:,}; drain (b>=24): {drain_repo:,}")
            if drain_repo > 0 and repo_23 == 0 and drain_repo > 50:
                print("  -> Spike in drain (0 in b23 -> many in b24): consistent with coverage table using next-day demand (wrapped bucket).")

    # Conclusion
    print("\n--- Conclusion ---")
    print("Slowdown after trip 85600: the 85600th TRIP_COMPLETE is in the DRAIN phase (sim time > 24h).")
    if drain_log:
        pending_vals = [p for (_, p, _) in drain_log]
        eligible_vals = [e for (_, _, e) in drain_log]
        if max(pending_vals) < 100 and min(eligible_vals) > 1000:
            print("LIMITING FACTOR: NOT 'vehicles finishing trips'.")
            print("  In the drain we have FEW pending (max %s) and MANY eligible (min %s, mean %s)."
                  % (max(pending_vals), min(eligible_vals), sum(eligible_vals)//len(eligible_vals)))
            print("  So the bottleneck is NOT few eligible vehicles.")
            print("  Likely cause: COST PER EVENT in the drain (e.g. VEHICLE_IDLE -> select_target with")
            print("  wrapped min_cov bucket, or many DISPATCH/VEHICLE_IDLE events each doing work).")
        else:
            print("  In the drain: pending max=%s, eligible min=%s (interpret above)." % (max(pending_vals), min(eligible_vals)))
    print("Coverage table edge case: at t>24h the policy uses (t+lookahead)%%n_buckets, so it wraps to")
    print("'next day' buckets; min_cov targets may be wrong and select_target may do extra work.")


if __name__ == "__main__":
    main()
