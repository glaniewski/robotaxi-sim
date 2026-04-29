"""
Side-by-side comparison: fixed cap (10%) vs dynamic cap on the Exp54 best config.
Two-panel time-series + summary stats.
"""
from __future__ import annotations

import json
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
from app.sim.metrics import summarize_charger_util_by_depot
from app.sim.reposition_policies import build_covered_by, build_policy
from app.sim.routing import RoutingCache

REQUESTS_PATH = str(ROOT / "data" / "requests_austin_h3_r8.parquet")
TRAVEL_CACHE = str(ROOT / "data" / "h3_travel_cache.parquet")

SEED = 123; DURATION = 1440; MAX_WAIT = 600.0; BUCKET_MIN = 15.0
SCALE = 0.1; FLEET = 3000; N_SITES = 50; PLUGS = 8; KW = 50.0; SITE_KW = 400.0


def top_demand_cells(n):
    df = pd.read_parquet(REQUESTS_PATH, columns=["origin_h3"])
    return df["origin_h3"].value_counts().head(n).index.tolist()


def build_timed(reqs, bm=15.0):
    bs = bm * 60.0; nb = int(round(1440.0 / bm))
    counts = {}
    for r in reqs:
        b = int(r.request_time / bs) % nb
        counts.setdefault(r.origin_h3, {}); counts[r.origin_h3][b] = counts[r.origin_h3].get(b, 0) + 1
    return {cell: {b: v / bs for b, v in bk.items()} for cell, bk in counts.items()}


def build_flat(reqs, dur):
    counts = {}
    for r in reqs:
        counts[r.origin_h3] = counts.get(r.origin_h3, 0) + 1
    return {c: n / (dur * 60.0) for c, n in counts.items()}


def run_sim(label, use_dynamic_cap, dcw, dcs, covered_by):
    """Run sim and return (timeseries_df, metrics_dict, requests)."""
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

    # Dynamic cap: set base cap very low (1%) so the headroom formula dominates.
    # Fixed cap: set base cap at 10% (the old behavior, since headroom >= base_cap
    # during lulls anyway, but base_cap = 300 during peaks).
    # With dynamic cap change, max_concurrent_charging_pct is now just the FLOOR.
    # To simulate old fixed behavior: set it to 100% (no headroom expansion).
    # Actually, the old code used: charging_cap = int(max_concurrent_charging_pct * fleet)
    # The new code uses: charging_cap = max(base_cap, headroom)
    # To get the OLD behavior with new code: we can't perfectly replicate it because
    # the new code always takes max(base_cap, headroom). But we can approximate by
    # noting that during peaks, headroom ~ eligible - pending*2 which IS larger than
    # base_cap when idle >> pending. So the dynamic cap is always >= fixed cap.
    # For a clean comparison, let's just run both and see.
    
    cap = 0.10  # base floor is 10% for both; dynamic expansion happens automatically

    sc = SimConfig(
        duration_minutes=DURATION, seed=SEED, fleet_size=FLEET,
        max_wait_time_seconds=MAX_WAIT, reposition_enabled=True,
        reposition_alpha=0.6, reposition_top_k_cells=50,
        max_vehicles_targeting_cell=3, kwh_per_mile=0.20,
        charging_queue_policy="jit", charging_depot_selection="fastest",
        charge_supply_ratio=2.0, max_concurrent_charging_pct=cap,
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
    depots = [
        Depot(id=f"micro_{i+1:03d}", h3_cell=cell, chargers_count=PLUGS,
              charger_kw=KW, site_power_kw=SITE_KW)
        for i, cell in enumerate(top_demand_cells(N_SITES))
    ]

    bar = tqdm(total=len(requests), desc=label, unit="trips", ncols=100,
               bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]")
    last = [0]
    def _cb(r, t):
        d = r - last[0]
        if d > 0: bar.update(d); last[0] = r

    eng = SimulationEngine(config=sc, vehicles=vehicles, requests=requests,
                           depots=depots, routing=routing, reposition_policy=policy,
                           progress_callback=_cb)
    res = eng.run()
    bar.update(len(requests) - last[0]); bar.close()

    ts = pd.DataFrame(res["timeseries"])
    ts["to_pickup_count"] = FLEET - ts["idle_count"] - ts["in_trip_count"] - ts["charging_count"] - ts["repositioning_count"]
    ts["to_pickup_count"] = ts["to_pickup_count"].clip(lower=0)

    bucket_s = BUCKET_MIN * 60.0
    req_times = np.array([r.request_time for r in requests])
    n_buckets = int(DURATION / BUCKET_MIN)
    edges = np.arange(0, (n_buckets + 1) * bucket_s, bucket_s)
    req_counts, _ = np.histogram(req_times, bins=edges)
    req_df = pd.DataFrame({"t_minutes": np.arange(0, n_buckets) * BUCKET_MIN, "request_arrivals": req_counts})
    ts = ts.merge(req_df, on="t_minutes", how="left").fillna(0)
    ts["served_delta"] = ts["served_cumulative"].diff().fillna(0).clip(lower=0)

    m = res["metrics"]
    depot_u = summarize_charger_util_by_depot(m["charger_utilization_by_depot_pct"])
    net = round(m["fleet_battery_pct"] - sc.soc_initial * 100, 2)
    summary = {
        "served_pct": m["served_pct"], "p90_wait_min": m["p90_wait_min"],
        "fleet_battery_pct": m["fleet_battery_pct"], "fleet_soc_median_pct": m["fleet_soc_median_pct"],
        "vehicles_below_soc_target_count": m["vehicles_below_soc_target_count"],
        "net_energy_pct": net, "charger_utilization_pct": m["charger_utilization_pct"],
        "total_charge_sessions": m["total_charge_sessions"],
        "contribution_margin_per_trip": m["contribution_margin_per_trip"],
        **depot_u,
    }
    return ts, summary, requests


def plot_panel(ax, ax_right, ts, title, fleet):
    hours = ts["t_minutes"].values / 60.0
    idle = ts["idle_count"].values
    pickup = ts["to_pickup_count"].values
    trip = ts["in_trip_count"].values
    charging = ts["charging_count"].values
    repo = ts["repositioning_count"].values

    ax.stackplot(hours, idle, pickup, trip, charging, repo,
                 labels=["IDLE", "TO_PICKUP", "IN_TRIP", "CHARGING", "REPOSITIONING"],
                 colors=["#b0bec5", "#ffb74d", "#4caf50", "#42a5f5", "#ab47bc"], alpha=0.85)
    ax.set_ylabel("Vehicle count", fontsize=10)
    ax.set_ylim(0, fleet * 1.05)
    ax.set_title(title, fontsize=11, fontweight="bold")
    ax.grid(axis="y", alpha=0.3)

    ax_right.plot(hours, ts["pending_requests"].values, color="#e53935", linewidth=1, alpha=0.8)
    ax_right.set_ylabel("Pending", fontsize=9, color="#e53935")
    ax_right.tick_params(axis="y", labelcolor="#e53935")
    ax_right.set_ylim(0, max(ts["pending_requests"].max() * 1.5, 50))


def main():
    df = pd.read_parquet(REQUESTS_PATH, columns=["origin_h3"])
    dcw = df["origin_h3"].value_counts().to_dict()
    dcs = set(dcw.keys())
    covered_by = build_covered_by(TRAVEL_CACHE, dcs, MAX_WAIT)

    print("Running dynamic-cap sim...")
    ts_dyn, sum_dyn, reqs = run_sim("dynamic_cap", True, dcw, dcs, covered_by)

    print("\nDynamic cap results:")
    print(json.dumps(sum_dyn, indent=2, sort_keys=True))

    # Also load the iter2 50s_8p_50kW timeseries for reference by re-running with fixed cap.
    # Since the engine code now always uses dynamic cap, we can compare to the Exp54 iter2
    # fixed cap results we already have in RESULTS.md.

    # --- Plot ---
    fig, axes = plt.subplots(2, 1, figsize=(16, 10), sharex=True,
                              gridspec_kw={"height_ratios": [3, 1], "hspace": 0.08})
    ax1 = axes[0]
    ax1b = ax1.twinx()
    plot_panel(ax1, ax1b, ts_dyn,
               f"Dynamic Cap — Fleet State & Demand (fleet={FLEET}, {N_SITES}×{PLUGS}p×{int(KW)}kW)",
               FLEET)
    ax1.legend(loc="upper left", fontsize=8, ncol=5, framealpha=0.9)
    ax1b.legend(["Pending requests"], loc="upper right", fontsize=8, framealpha=0.9)

    # Bottom: demand flow
    ax2 = axes[1]
    hours = ts_dyn["t_minutes"].values / 60.0
    bar_w = (BUCKET_MIN / 60.0) * 0.8
    ax2.bar(hours, ts_dyn["request_arrivals"].values, width=bar_w, color="#78909c", alpha=0.5, label="Requests arriving")
    ax2.bar(hours, ts_dyn["served_delta"].values, width=bar_w, color="#4caf50", alpha=0.7, label="Served")
    ax2.set_xlabel("Time of day (hours)", fontsize=11)
    ax2.set_ylabel("Trips / bucket", fontsize=10)
    ax2.legend(loc="upper left", fontsize=8, ncol=2)
    ax2.grid(axis="y", alpha=0.3)
    ax2.set_xlim(0, 24)
    ax2.xaxis.set_major_locator(mticker.MultipleLocator(2))
    ax2.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x)}h"))

    # Add summary stats as text box
    txt = (f"served={sum_dyn['served_pct']:.1f}%  p90={sum_dyn['p90_wait_min']:.2f}min  "
           f"SOC_mean={sum_dyn['fleet_battery_pct']:.1f}%  SOC_med={sum_dyn['fleet_soc_median_pct']:.1f}%  "
           f"below_tgt={sum_dyn['vehicles_below_soc_target_count']}  "
           f"net={sum_dyn['net_energy_pct']:+.2f}%  "
           f"chg_util={sum_dyn['charger_utilization_pct']:.1f}%  "
           f"sessions={sum_dyn['total_charge_sessions']:,}")
    fig.text(0.5, 0.01, txt, ha="center", fontsize=9, family="monospace",
             bbox=dict(boxstyle="round,pad=0.4", facecolor="lightyellow", alpha=0.9))

    out = ROOT / "plots" / "exp54_dynamic_cap.png"
    out.parent.mkdir(exist_ok=True)
    fig.savefig(out, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"\nSaved → {out}")

    # Print comparison vs fixed cap (from iter2)
    print("\n--- Comparison vs fixed cap (iter2 50s_8p_50kW) ---")
    fixed = {"served_pct": 93.67, "p90_wait_min": 6.38, "fleet_battery_pct": 79.66,
             "fleet_soc_median_pct": 80.00, "vehicles_below_soc_target_count": 339,
             "net_energy_pct": -0.34, "charger_utilization_pct": 20.25,
             "total_charge_sessions": 78992, "contribution_margin_per_trip": 9.5276}
    for k in sorted(fixed):
        f_val = fixed[k]
        d_val = sum_dyn.get(k, "N/A")
        delta = ""
        if isinstance(f_val, (int, float)) and isinstance(d_val, (int, float)):
            delta = f"  Δ={d_val - f_val:+.2f}" if isinstance(f_val, float) else f"  Δ={d_val - f_val:+d}"
        print(f"  {k:40s}  fixed={f_val:>10}  dynamic={d_val:>10}{delta}")


if __name__ == "__main__":
    main()
