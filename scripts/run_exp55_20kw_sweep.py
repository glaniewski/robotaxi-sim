"""
Experiment 55 — 20kW charger sweep.

Goal: find a viable 20kW config with low site power while maintaining SLA.
Sweep: sites × plugs with kW=20, site_power = plugs × kW (no throttle).
Reference: 50×8p×50kW (Exp54 best).

Generates:
  - Metrics comparison table (printed)
  - Time-series plot for the best 20kW config (with expired requests in histogram)
"""
from __future__ import annotations

import json, sys, textwrap
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
TRAVEL_CACHE  = str(ROOT / "data" / "h3_travel_cache.parquet")

SEED = 123; DURATION = 1440; MAX_WAIT = 600.0; BUCKET_MIN = 15.0
SCALE = 0.1; FLEET = 3000

# ---- Sweep grid ----
CONFIGS = [
    # (label, n_sites, plugs, kw)
    ("50s_4p_20kW",   50,  4, 20.0),
    ("50s_8p_20kW",   50,  8, 20.0),
    ("75s_4p_20kW",   75,  4, 20.0),
    ("75s_8p_20kW",   75,  8, 20.0),
    ("100s_4p_20kW", 100,  4, 20.0),
    ("100s_8p_20kW", 100,  8, 20.0),
    ("125s_4p_20kW", 125,  4, 20.0),
    ("125s_8p_20kW", 125,  8, 20.0),
    # Reference: 50kW config
    ("50s_8p_50kW",   50,  8, 50.0),
]

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


def run_one(label, n_sites, plugs, kw, routing, base_reqs, timed, flat, dcw, dcs, covered_by):
    site_power = plugs * kw
    total_plugs = n_sites * plugs
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
    depot_cells = top_demand_cells(n_sites)
    vehicles = build_vehicles(sc, depot_h3_cells=depot_cells, seed=SEED, demand_cells=dcw)
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
        Depot(id=f"depot_{i+1:03d}", h3_cell=cell, chargers_count=plugs,
              charger_kw=kw, site_power_kw=site_power)
        for i, cell in enumerate(depot_cells)
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

    # Build timeseries with expired data
    ts = pd.DataFrame(res["timeseries"])
    ts["to_pickup_count"] = (FLEET - ts["idle_count"] - ts["in_trip_count"]
                             - ts["charging_count"] - ts["repositioning_count"]).clip(lower=0)

    bucket_s = BUCKET_MIN * 60.0
    req_times = np.array([r.request_time for r in requests])
    n_buckets = int(DURATION / BUCKET_MIN)
    edges = np.arange(0, (n_buckets + 1) * bucket_s, bucket_s)
    req_counts, _ = np.histogram(req_times, bins=edges)
    req_df = pd.DataFrame({"t_minutes": np.arange(0, n_buckets) * BUCKET_MIN,
                            "request_arrivals": req_counts})
    ts = ts.merge(req_df, on="t_minutes", how="left").fillna(0)
    ts["served_delta"] = ts["served_cumulative"].diff().fillna(0).clip(lower=0)
    ts["unserved_delta"] = ts["unserved_cumulative"].diff().fillna(0).clip(lower=0)

    m = res["metrics"]
    depot_u = summarize_charger_util_by_depot(m["charger_utilization_by_depot_pct"])
    net = round(m["fleet_battery_pct"] - sc.soc_initial * 100, 2)
    summary = {
        "label": label,
        "sites": n_sites, "plugs": plugs, "kw": kw,
        "site_kw": site_power, "total_plugs": total_plugs,
        "served_pct": m["served_pct"], "p90_wait_min": m["p90_wait_min"],
        "median_wait_min": m["median_wait_min"],
        "fleet_battery_pct": m["fleet_battery_pct"],
        "fleet_soc_median_pct": m["fleet_soc_median_pct"],
        "vehicles_below_soc_target_count": m["vehicles_below_soc_target_count"],
        "net_energy_pct": net,
        "charger_utilization_pct": m["charger_utilization_pct"],
        "total_charge_sessions": m["total_charge_sessions"],
        "contribution_margin_per_trip": m["contribution_margin_per_trip"],
        **depot_u,
    }
    return ts, summary


def plot_timeseries(ts, summary, filename):
    """Two-panel plot: stacked vehicle states + demand flow with expired bars."""
    fig, axes = plt.subplots(2, 1, figsize=(16, 10), sharex=True,
                              gridspec_kw={"height_ratios": [3, 1], "hspace": 0.08})
    hours = ts["t_minutes"].values / 60.0
    bar_w = (BUCKET_MIN / 60.0) * 0.8
    lbl = summary["label"]

    # Top: stacked vehicle states
    ax1 = axes[0]
    ax1.stackplot(
        hours,
        ts["idle_count"].values,
        ts["to_pickup_count"].values,
        ts["in_trip_count"].values,
        ts["charging_count"].values,
        ts["repositioning_count"].values,
        labels=["IDLE", "TO_PICKUP", "IN_TRIP", "CHARGING", "REPOSITIONING"],
        colors=["#b0bec5", "#ffb74d", "#4caf50", "#42a5f5", "#ab47bc"],
        alpha=0.85,
    )
    ax1.set_ylabel("Vehicle count", fontsize=11)
    ax1.set_ylim(0, FLEET * 1.05)
    ax1.set_title(f"{lbl} — Fleet State & Demand (fleet={FLEET})", fontsize=12, fontweight="bold")
    ax1.legend(loc="upper left", fontsize=8, ncol=5, framealpha=0.9)
    ax1.grid(axis="y", alpha=0.3)

    ax1b = ax1.twinx()
    ax1b.plot(hours, ts["pending_requests"].values, color="#e53935", linewidth=1, alpha=0.8)
    ax1b.set_ylabel("Pending", fontsize=9, color="#e53935")
    ax1b.tick_params(axis="y", labelcolor="#e53935")
    ax1b.set_ylim(0, max(ts["pending_requests"].max() * 1.5, 50))
    ax1b.legend(["Pending requests"], loc="upper right", fontsize=8, framealpha=0.9)

    # Bottom: demand flow with EXPIRED bars
    ax2 = axes[1]
    arrivals = ts["request_arrivals"].values
    served_d = ts["served_delta"].values
    unserved_d = ts["unserved_delta"].values

    ax2.bar(hours, arrivals, width=bar_w, color="#78909c", alpha=0.4, label="Requests arriving")
    ax2.bar(hours, served_d, width=bar_w, color="#4caf50", alpha=0.7, label="Served")
    ax2.bar(hours, unserved_d, width=bar_w, bottom=served_d, color="#e53935", alpha=0.7, label="Expired")

    ax2.set_xlabel("Time of day (hours)", fontsize=11)
    ax2.set_ylabel("Trips / bucket", fontsize=10)
    ax2.legend(loc="upper left", fontsize=8, ncol=3, framealpha=0.9)
    ax2.grid(axis="y", alpha=0.3)
    ax2.set_xlim(0, 24)
    ax2.xaxis.set_major_locator(mticker.MultipleLocator(2))
    ax2.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x)}h"))

    s = summary
    txt = (f"served={s['served_pct']:.1f}%  p90={s['p90_wait_min']:.2f}min  "
           f"SOC={s['fleet_battery_pct']:.1f}%  net={s['net_energy_pct']:+.2f}%  "
           f"below_tgt={s['vehicles_below_soc_target_count']}  "
           f"chg_util={s['charger_utilization_pct']:.1f}%  "
           f"sessions={s['total_charge_sessions']:,}  "
           f"site_kw={s['site_kw']:.0f}")
    fig.text(0.5, 0.01, txt, ha="center", fontsize=9, family="monospace",
             bbox=dict(boxstyle="round,pad=0.4", facecolor="lightyellow", alpha=0.9))

    out = ROOT / "plots" / filename
    out.parent.mkdir(exist_ok=True)
    fig.savefig(out, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"  Plot → {out}")


def main():
    df = pd.read_parquet(REQUESTS_PATH, columns=["origin_h3"])
    dcw = df["origin_h3"].value_counts().to_dict()
    dcs = set(dcw.keys())
    covered_by = build_covered_by(TRAVEL_CACHE, dcs, MAX_WAIT)

    routing = RoutingCache(parquet_path=TRAVEL_CACHE, osrm_url="http://localhost:5001")
    base_reqs = load_requests(REQUESTS_PATH, duration_minutes=DURATION,
                              max_wait_time_seconds=MAX_WAIT, demand_scale=SCALE, seed=SEED)
    timed = build_timed(base_reqs, BUCKET_MIN)
    flat = build_flat(base_reqs, DURATION)

    results = []
    ts_store = {}

    for label, n_sites, plugs, kw in CONFIGS:
        print(f"\n{'='*60}")
        print(f"  Running: {label}  ({n_sites} sites × {plugs} plugs × {kw}kW, "
              f"site_power={plugs*kw}kW, total_plugs={n_sites*plugs})")
        print(f"{'='*60}")
        ts, summary = run_one(label, n_sites, plugs, kw,
                              routing, base_reqs, timed, flat, dcw, dcs, covered_by)
        results.append(summary)
        ts_store[label] = ts
        print(f"  served={summary['served_pct']:.1f}%  p90={summary['p90_wait_min']:.2f}min  "
              f"net={summary['net_energy_pct']:+.2f}%  below_tgt={summary['vehicles_below_soc_target_count']}  "
              f"chg_util={summary['charger_utilization_pct']:.1f}%  sessions={summary['total_charge_sessions']:,}")

    # ---- Print comparison table ----
    print(f"\n\n{'='*120}")
    print("EXPERIMENT 55 — 20kW Charger Sweep Results")
    print(f"{'='*120}")
    header = (f"{'Config':<18s} {'Sites':>5s} {'Plugs':>5s} {'kW':>4s} {'SiteKW':>6s} "
              f"{'TotPlg':>6s} {'Served%':>8s} {'p90Wait':>8s} {'MedWait':>8s} "
              f"{'SOC%':>6s} {'Net%':>7s} {'<Tgt':>5s} {'ChgUtil':>8s} "
              f"{'Sessions':>9s} {'Margin':>7s}")
    print(header)
    print("-" * len(header))
    for r in results:
        print(f"{r['label']:<18s} {r['sites']:5d} {r['plugs']:5d} {r['kw']:4.0f} {r['site_kw']:6.0f} "
              f"{r['total_plugs']:6d} {r['served_pct']:8.2f} {r['p90_wait_min']:8.2f} {r['median_wait_min']:8.2f} "
              f"{r['fleet_battery_pct']:6.2f} {r['net_energy_pct']:+7.2f} "
              f"{r['vehicles_below_soc_target_count']:5d} {r['charger_utilization_pct']:8.2f} "
              f"{r['total_charge_sessions']:9,d} {r['contribution_margin_per_trip']:7.2f}")

    # ---- Plot the best 20kW config and the 50kW reference ----
    kw20_results = [r for r in results if r["kw"] == 20.0]
    # Best = highest served_pct among those with net_energy >= -1.0
    viable = [r for r in kw20_results if r["net_energy_pct"] >= -1.0]
    if not viable:
        viable = kw20_results
    best_20 = max(viable, key=lambda r: r["served_pct"])
    print(f"\nBest 20kW config: {best_20['label']}")
    plot_timeseries(ts_store[best_20["label"]], best_20, f"exp55_{best_20['label']}.png")

    ref = next(r for r in results if r["label"] == "50s_8p_50kW")
    plot_timeseries(ts_store["50s_8p_50kW"], ref, "exp55_50s_8p_50kW_ref.png")

    # ---- Delta table: best 20kW vs 50kW reference ----
    print(f"\n--- Best 20kW ({best_20['label']}) vs 50kW reference (50s_8p_50kW) ---")
    for k in ["served_pct", "p90_wait_min", "median_wait_min", "fleet_battery_pct",
              "net_energy_pct", "vehicles_below_soc_target_count",
              "charger_utilization_pct", "total_charge_sessions",
              "contribution_margin_per_trip", "site_kw", "total_plugs"]:
        b = best_20[k]; r_ = ref[k]
        if isinstance(b, float):
            print(f"  {k:40s}  20kW={b:>10.2f}  50kW={r_:>10.2f}  Δ={b - r_:+.2f}")
        else:
            print(f"  {k:40s}  20kW={b:>10}  50kW={r_:>10}  Δ={b - r_:+d}")


if __name__ == "__main__":
    main()
