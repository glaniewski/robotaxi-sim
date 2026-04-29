"""
Plot Exp54 best config time-series: vehicle state breakdown + request arrival rate.

Stacked area = vehicle states (IDLE, TO_PICKUP, IN_TRIP, CHARGING, REPOSITIONING)
Bar overlay  = request arrivals per 15-min bucket
"""
from __future__ import annotations

import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
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
N_SITES = 50
PLUGS = 8
KW = 50.0
SITE_KW = 400.0


def top_demand_cells(n):
    df = pd.read_parquet(REQUESTS_PATH, columns=["origin_h3"])
    return df["origin_h3"].value_counts().head(n).index.tolist()


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


def main():
    print("Loading data...")
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
        timeseries_bucket_minutes=BUCKET_MIN,
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
        Depot(id=f"micro_{i+1:03d}", h3_cell=cell, chargers_count=PLUGS,
              charger_kw=KW, site_power_kw=SITE_KW)
        for i, cell in enumerate(site_cells)
    ]

    bar = tqdm(total=len(requests), desc="sim", unit="trips", ncols=100,
               bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]")
    last = [0]
    def _cb(resolved, total):
        d = resolved - last[0]
        if d > 0:
            bar.update(d)
            last[0] = resolved

    print("Running simulation...")
    eng = SimulationEngine(config=sc, vehicles=vehicles, requests=requests,
                           depots=depots, routing=routing, reposition_policy=policy,
                           progress_callback=_cb)
    res = eng.run()
    bar.update(len(requests) - last[0])
    bar.close()

    # --- Build dataframes ---
    ts = pd.DataFrame(res["timeseries"])
    ts["to_pickup_count"] = FLEET - ts["idle_count"] - ts["in_trip_count"] - ts["charging_count"] - ts["repositioning_count"]
    ts["to_pickup_count"] = ts["to_pickup_count"].clip(lower=0)

    # Request arrivals per bucket
    bucket_s = BUCKET_MIN * 60.0
    req_times = np.array([r.request_time for r in requests])
    n_buckets = int(DURATION / BUCKET_MIN)
    bucket_edges = np.arange(0, (n_buckets + 1) * bucket_s, bucket_s)
    req_counts, _ = np.histogram(req_times, bins=bucket_edges)
    req_df = pd.DataFrame({
        "t_minutes": np.arange(0, n_buckets) * BUCKET_MIN,
        "request_arrivals": req_counts,
    })

    ts_merged = ts.merge(req_df, on="t_minutes", how="left").fillna(0)
    # Compute served delta per bucket (new trips served in this bucket)
    ts_merged["served_delta"] = ts_merged["served_cumulative"].diff().fillna(0).clip(lower=0)
    ts_merged["unserved_delta"] = ts_merged["unserved_cumulative"].diff().fillna(0).clip(lower=0)

    # --- Plot ---
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 10), sharex=True,
                                    gridspec_kw={"height_ratios": [3, 1], "hspace": 0.08})
    t = ts_merged["t_minutes"].values
    hours = t / 60.0

    idle = ts_merged["idle_count"].values
    pickup = ts_merged["to_pickup_count"].values
    trip = ts_merged["in_trip_count"].values
    charging = ts_merged["charging_count"].values
    repo = ts_merged["repositioning_count"].values

    colors = {
        "IDLE": "#b0bec5",
        "TO_PICKUP": "#ffb74d",
        "IN_TRIP": "#4caf50",
        "CHARGING": "#42a5f5",
        "REPOSITIONING": "#ab47bc",
    }

    ax1.stackplot(hours, idle, pickup, trip, charging, repo,
                  labels=["IDLE", "TO_PICKUP", "IN_TRIP", "CHARGING", "REPOSITIONING"],
                  colors=[colors["IDLE"], colors["TO_PICKUP"], colors["IN_TRIP"],
                          colors["CHARGING"], colors["REPOSITIONING"]],
                  alpha=0.85)

    ax1.set_ylabel("Vehicle count", fontsize=12)
    ax1.set_ylim(0, FLEET * 1.05)
    ax1.axhline(FLEET, color="black", linewidth=0.5, linestyle="--", alpha=0.3)
    ax1.legend(loc="upper left", fontsize=9, ncol=5, framealpha=0.9)
    ax1.set_title(
        f"Exp54 Best Config — Fleet State & Demand (fleet={FLEET}, scale={SCALE}, "
        f"{N_SITES}×{PLUGS}p×{int(KW)}kW, cap=10%)",
        fontsize=13, fontweight="bold", pad=10,
    )
    ax1.grid(axis="y", alpha=0.3)

    # Pending requests on secondary y-axis
    ax1b = ax1.twinx()
    ax1b.plot(hours, ts_merged["pending_requests"].values, color="#e53935",
              linewidth=1.2, alpha=0.8, label="Pending requests")
    ax1b.set_ylabel("Pending requests", fontsize=11, color="#e53935")
    ax1b.tick_params(axis="y", labelcolor="#e53935")
    max_pending = ts_merged["pending_requests"].max()
    ax1b.set_ylim(0, max(max_pending * 1.5, 50))
    ax1b.legend(loc="upper right", fontsize=9, framealpha=0.9)

    # Bottom panel: request arrival rate + served/unserved per bucket
    bar_width = (BUCKET_MIN / 60.0) * 0.8
    arrivals = ts_merged["request_arrivals"].values
    served_d = ts_merged["served_delta"].values
    unserved_d = ts_merged["unserved_delta"].values

    ax2.bar(hours, arrivals, width=bar_width, color="#78909c", alpha=0.5, label="Requests arriving")
    ax2.bar(hours, served_d, width=bar_width, color="#4caf50", alpha=0.7, label="Served (this bucket)")
    ax2.bar(hours, unserved_d, width=bar_width, bottom=served_d, color="#e53935", alpha=0.7, label="Expired (this bucket)")

    ax2.set_xlabel("Time of day (hours)", fontsize=12)
    ax2.set_ylabel("Trips / bucket", fontsize=12)
    ax2.legend(loc="upper left", fontsize=9, ncol=3, framealpha=0.9)
    ax2.grid(axis="y", alpha=0.3)

    # X-axis formatting
    ax2.set_xlim(0, 24)
    ax2.xaxis.set_major_locator(mticker.MultipleLocator(2))
    ax2.xaxis.set_minor_locator(mticker.MultipleLocator(0.5))
    ax2.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x)}h"))

    out_path = ROOT / "plots" / "exp54_timeseries.png"
    out_path.parent.mkdir(exist_ok=True)
    fig.savefig(out_path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"\nSaved → {out_path}")

    # Print some summary stats
    peak_charging = ts_merged["charging_count"].max()
    peak_trip = ts_merged["in_trip_count"].max()
    peak_idle = ts_merged["idle_count"].max()
    peak_pending = ts_merged["pending_requests"].max()
    print(f"Peak charging: {peak_charging}  Peak in_trip: {peak_trip}  "
          f"Peak idle: {peak_idle}  Peak pending: {peak_pending}")
    print(f"Request arrivals: min={arrivals.min():.0f}  max={arrivals.max():.0f}  "
          f"mean={arrivals.mean():.0f}/bucket")


if __name__ == "__main__":
    main()
