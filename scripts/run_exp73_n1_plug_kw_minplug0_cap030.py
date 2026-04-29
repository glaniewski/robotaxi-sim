"""
Experiment 73 — N=1 mega-depot: 154p×20 / 28p×110 / 10p×308 kW @ 3080 kW/site each,
with ``min_plug_duration_minutes=0`` and ``max_concurrent_charging_pct=0.30``.

Mirrors the Exp63/70 continuous pipeline but builds ``SimConfig`` in-script (does not
change ``run_continuous_experiment``). Depot = ``top_demand_cells(1)``.

Run: PYTHONHASHSEED=0 python3 scripts/run_exp73_n1_plug_kw_minplug0_cap030.py
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import pandas as pd
from tqdm import tqdm

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(ROOT / "backend"))
sys.path.insert(0, str(SCRIPT_DIR))

import run_exp63_continuous_multiday_steady_state as e63  # noqa: E402

from app.sim.demand import load_requests, load_requests_repeated_days  # noqa: E402
from app.sim.engine import SimConfig, SimulationEngine, build_vehicles  # noqa: E402
from app.sim.entities import Depot  # noqa: E402
from app.sim.reposition_policies import build_covered_by, build_policy  # noqa: E402
from app.sim.routing import RoutingCache  # noqa: E402

N_DEPOTS = 1
NUM_DAYS = 3
FLEET = 4000
SCALE = 0.2
MIN_PLUG = 0.0
MAX_CHG_PCT = 0.30

CONFIGS: tuple[tuple[str, int, float], ...] = (
    ("154p×20 kW (site 3080)", 154, 20.0),
    ("28p×110 kW (site 3080)", 28, 110.0),
    ("10p×308 kW (site 3080)", 10, 308.0),
)


def _run_continuous_variant(
    n_sites: int,
    num_days: int,
    *,
    demand_scale: float,
    fleet_size: int,
    plugs_per_site: int,
    charger_kw: float,
    trip_bar_desc: str,
) -> dict:
    """Same as ``run_exp63.run_continuous_experiment`` but fixed min_plug=0 and cap=0.30."""
    scale = float(demand_scale)
    fs = int(fleet_size)
    pp = int(plugs_per_site)
    ckw = float(charger_kw)
    site_kw = float(pp * ckw)
    df = pd.read_parquet(e63.REQUESTS_PATH, columns=["origin_h3"])
    dcw = df["origin_h3"].value_counts().to_dict()
    dcs = set(dcw.keys())
    covered_by = build_covered_by(e63.TRAVEL_CACHE, dcs, e63.MAX_WAIT)
    routing = RoutingCache(parquet_path=e63.TRAVEL_CACHE, osrm_url="http://localhost:5001")

    template = load_requests(
        e63.REQUESTS_PATH,
        duration_minutes=e63.DAY_MINUTES,
        max_wait_time_seconds=e63.MAX_WAIT,
        demand_scale=scale,
        seed=e63.SEED,
    )
    timed = e63.build_timed(template, e63.BUCKET_MIN)
    flat = e63.build_flat(template, e63.DAY_MINUTES)

    requests = load_requests_repeated_days(
        e63.REQUESTS_PATH,
        duration_minutes_per_day=e63.DAY_MINUTES,
        num_days=num_days,
        max_wait_time_seconds=e63.MAX_WAIT,
        demand_scale=scale,
        seed=e63.SEED,
    )

    sc = SimConfig(
        duration_minutes=float(e63.DAY_MINUTES * num_days),
        seed=e63.SEED,
        fleet_size=fs,
        soc_initial=e63.SOC_INITIAL,
        soc_target=0.80,
        soc_charge_start=0.80,
        soc_min=0.20,
        max_wait_time_seconds=e63.MAX_WAIT,
        reposition_enabled=True,
        reposition_alpha=0.6,
        reposition_top_k_cells=50,
        max_vehicles_targeting_cell=3,
        kwh_per_mile=0.20,
        charging_queue_policy="fifo",
        charging_depot_selection="fastest_balanced",
        charging_depot_balance_slack_minutes=3.0,
        charge_supply_ratio=2.0,
        max_concurrent_charging_pct=MAX_CHG_PCT,
        timeseries_bucket_minutes=e63.BUCKET_MIN,
        min_plug_duration_minutes=MIN_PLUG,
    )

    depot_cells = e63.top_demand_cells(n_sites)
    vehicles = build_vehicles(sc, depot_h3_cells=depot_cells, seed=e63.SEED, demand_cells=dcw)
    depots = [
        Depot(
            id=f"depot_{i+1:03d}",
            h3_cell=c,
            chargers_count=pp,
            charger_kw=ckw,
            site_power_kw=site_kw,
        )
        for i, c in enumerate(depot_cells)
    ]
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
        max_wait_time_seconds=e63.MAX_WAIT,
        min_coverage=2,
        coverage_reposition_travel_minutes=60.0,
        timed_forecast_table=timed,
        forecast_bucket_minutes=e63.BUCKET_MIN,
        coverage_lookahead_minutes=60.0,
    )

    bar = tqdm(
        total=len(requests),
        desc=trip_bar_desc,
        unit="trips",
        ncols=100,
        bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
    )
    last = [0]

    def _cb(r, _t):
        d = r - last[0]
        if d > 0:
            bar.update(d)
            last[0] = r

    eng = SimulationEngine(
        config=sc,
        vehicles=vehicles,
        requests=requests,
        depots=depots,
        routing=routing,
        reposition_policy=policy,
        progress_callback=_cb,
    )
    res = eng.run()
    bar.update(len(requests) - last[0])
    bar.close()

    m = res["metrics"]
    ts = pd.DataFrame(res["timeseries"])
    daily = e63.daily_stats_from_timeseries(ts, requests, num_days)
    return {
        "metrics": m,
        "daily": daily,
        "result": res,
        "n_sites": n_sites,
        "num_days": num_days,
        "depot_h3_cells": list(depot_cells),
    }


def main() -> None:
    t0 = time.perf_counter()
    depot_cell = e63.top_demand_cells(N_DEPOTS)[0]
    rows: list[dict] = []
    for label, plugs, ckw in tqdm(CONFIGS, desc="config", unit="run", ncols=100):
        site_kw = float(plugs * ckw)
        out = _run_continuous_variant(
            N_DEPOTS,
            NUM_DAYS,
            demand_scale=SCALE,
            fleet_size=FLEET,
            plugs_per_site=plugs,
            charger_kw=ckw,
            trip_bar_desc=f"exp73_N1_{plugs}p_{int(ckw) if ckw == int(ckw) else ckw}kW_m0_c30",
        )
        m = out["metrics"]
        daily = out["daily"]
        sp = [x["served_pct"] for x in daily]
        trips = sum(x["arrivals"] for x in daily)
        rows.append(
            {
                "label": label,
                "n_sites": N_DEPOTS,
                "plugs": plugs,
                "charger_kw": ckw,
                "site_kw": site_kw,
                "trips": trips,
                "served_pct": m["served_pct"],
                "median_wait_min": m["median_wait_min"],
                "p90_wait": m["p90_wait_min"],
                "sla_adherence_pct": m["sla_adherence_pct"],
                "repositioning_pct": m["repositioning_pct"],
                "chg_util": m["charger_utilization_pct"],
                "fleet_battery_pct": m["fleet_battery_pct"],
                "contrib_margin_per_trip": m["contribution_margin_per_trip"],
                "served_pct_d1_d3": sp,
            }
        )

    wall_s = time.perf_counter() - t0
    lbl_w = max(36, max(len(r["label"]) for r in rows) + 2)
    print("\n" + "=" * 120)
    print(
        f"Exp73: N={N_DEPOTS} depot @ {depot_cell} | demand_scale={SCALE}, fleet={FLEET}, {NUM_DAYS}d | "
        f"min_plug={MIN_PLUG}, max_concurrent_charging_pct={MAX_CHG_PCT}"
    )
    print(f"Wall clock: {wall_s:.1f} s ({wall_s/60:.2f} min)")
    hdr = (
        f"{'config':<{lbl_w}} {'site_kW':>8} {'trips':>8} {'served%':>8} {'med_w':>7} {'p90_w':>7} "
        f"{'sla%':>6} {'repo%':>6} {'chgU%':>7} {'fleetSOC%':>9} {'cm$/trip':>9}  d1,d2,d3 served%"
    )
    print(hdr)
    print("-" * len(hdr))
    for r in rows:
        sd = ", ".join(f"{x:.1f}" for x in r["served_pct_d1_d3"])
        print(
            f"{r['label']:<{lbl_w}} {r['site_kw']:8.0f} {r['trips']:8d} {r['served_pct']:8.2f} "
            f"{r['median_wait_min']:7.2f} {r['p90_wait']:7.2f} {r['sla_adherence_pct']:6.1f} "
            f"{r['repositioning_pct']:6.1f} {r['chg_util']:7.1f} {r['fleet_battery_pct']:9.2f} "
            f"{r['contrib_margin_per_trip']:9.4f}  {sd}"
        )


if __name__ == "__main__":
    main()
