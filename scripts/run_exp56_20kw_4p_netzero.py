"""
Experiment 56 — 20kW × 4 plugs/site: net energy + charger util (meaningful util).

Key finding: under plug contention, JIT inflates session counts (bounce replans) and
starves the fleet; FIFO + fastest_balanced delivers real charge sessions and ~net -0.8%
with ~20–25% fleet charger util (not plug-saturated 90%+).

Constraints: charger_kw=20, chargers_count=4, site_power_kw=80.

Usage:
  PYTHONHASHSEED=0 python3 scripts/run_exp56_20kw_4p_netzero.py           # quick (2 runs)
  PYTHONHASHSEED=0 python3 scripts/run_exp56_20kw_4p_netzero.py --sweep   # full grid (~11 runs)
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from tqdm import tqdm

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "backend"))

import pandas as pd

from app.sim.demand import load_requests
from app.sim.engine import SimConfig, SimulationEngine, build_vehicles
from app.sim.entities import Depot, Request
from app.sim.metrics import summarize_charger_util_by_depot
from app.sim.reposition_policies import build_covered_by, build_policy
from app.sim.routing import RoutingCache

REQUESTS_PATH = str(ROOT / "data" / "requests_austin_h3_r8.parquet")
TRAVEL_CACHE = str(ROOT / "data" / "h3_travel_cache.parquet")

SEED = 123
DURATION = 1440
MAX_WAIT = 600.0
SCALE = 0.1
FLEET = 3000
PLUGS = 4
KW = 20.0
SITE_KW = PLUGS * KW

QUICK_CASES = [
    ("275s_fifo_bal_c20", 275, "fifo", "fastest_balanced", 0.20),
    ("250s_jit_fast_c10", 250, "jit", "fastest", 0.10),
]

SWEEP_CASES = [
    ("225s_fifo_bal_c20", 225, "fifo", "fastest_balanced", 0.20),
    ("250s_fifo_bal_c20", 250, "fifo", "fastest_balanced", 0.20),
    ("275s_fifo_bal_c20", 275, "fifo", "fastest_balanced", 0.20),
    ("300s_fifo_bal_c20", 300, "fifo", "fastest_balanced", 0.20),
    ("325s_fifo_bal_c20", 325, "fifo", "fastest_balanced", 0.20),
    ("250s_jit_fast_c10", 250, "jit", "fastest", 0.10),
    ("300s_fifo_bal_c30", 300, "fifo", "fastest_balanced", 0.30),
    ("350s_fifo_bal_c20", 350, "fifo", "fastest_balanced", 0.20),
    ("400s_fifo_bal_c20", 400, "fifo", "fastest_balanced", 0.20),
    ("275s_fifo_bal_c40", 275, "fifo", "fastest_balanced", 0.40),
]


def top_demand_cells(n: int):
    df = pd.read_parquet(REQUESTS_PATH, columns=["origin_h3"])
    return df["origin_h3"].value_counts().head(n).index.tolist()


def build_timed(reqs, bm=15.0):
    bs = bm * 60.0
    nb = int(round(1440.0 / bm))
    counts: dict = {}
    for r in reqs:
        b = int(r.request_time / bs) % nb
        counts.setdefault(r.origin_h3, {})
        counts[r.origin_h3][b] = counts[r.origin_h3].get(b, 0) + 1
    return {cell: {bb: v / bs for bb, v in bk.items()} for cell, bk in counts.items()}


def build_flat(reqs, dur):
    counts: dict[str, int] = {}
    for r in reqs:
        counts[r.origin_h3] = counts.get(r.origin_h3, 0) + 1
    return {c: n / (dur * 60.0) for c, n in counts.items()}


def run_case(
    label: str,
    n_sites: int,
    queue: str,
    depot_sel: str,
    max_cap: float,
    routing: RoutingCache,
    base_reqs,
    timed,
    flat,
    dcw,
    dcs,
    covered_by,
):
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
        reposition_enabled=True,
        reposition_alpha=0.6,
        reposition_top_k_cells=50,
        max_vehicles_targeting_cell=3,
        kwh_per_mile=0.20,
        charging_queue_policy=queue,
        charging_depot_selection=depot_sel,
        charging_depot_balance_slack_minutes=3.0,
        charge_supply_ratio=2.0,
        max_concurrent_charging_pct=max_cap,
        timeseries_bucket_minutes=15.0,
    )
    depot_cells = top_demand_cells(n_sites)
    vehicles = build_vehicles(sc, depot_h3_cells=depot_cells, seed=SEED, demand_cells=dcw)
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
        forecast_bucket_minutes=15.0,
        coverage_lookahead_minutes=60.0,
    )
    depots = [
        Depot(
            id=f"depot_{i+1:03d}",
            h3_cell=cell,
            chargers_count=PLUGS,
            charger_kw=KW,
            site_power_kw=SITE_KW,
        )
        for i, cell in enumerate(depot_cells)
    ]
    bar = tqdm(
        total=len(requests),
        desc=label,
        unit="trips",
        ncols=100,
        bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
    )
    last = [0]

    def _cb(r, t):
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
    du = summarize_charger_util_by_depot(m["charger_utilization_by_depot_pct"])
    net = round(m["fleet_battery_pct"] - sc.soc_initial * 100, 2)
    return {
        "label": label,
        "sites": n_sites,
        "total_plugs": n_sites * PLUGS,
        "queue": queue,
        "depot_sel": depot_sel,
        "max_cap": max_cap,
        "served_pct": m["served_pct"],
        "p90_wait_min": m["p90_wait_min"],
        "fleet_battery_pct": m["fleet_battery_pct"],
        "net_energy_pct": net,
        "vehicles_below_soc_target_count": m["vehicles_below_soc_target_count"],
        "charger_utilization_pct": m["charger_utilization_pct"],
        "total_charge_sessions": m["total_charge_sessions"],
        "depot_queue_p90_min": m["depot_queue_p90_min"],
        "depot_queue_max_concurrent": m["depot_queue_max_concurrent"],
        "contribution_margin_per_trip": m["contribution_margin_per_trip"],
        **du,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sweep", action="store_true", help="Run full site/policy grid (~11 sims)")
    args = ap.parse_args()
    cases = SWEEP_CASES if args.sweep else QUICK_CASES

    df = pd.read_parquet(REQUESTS_PATH, columns=["origin_h3"])
    dcw = df["origin_h3"].value_counts().to_dict()
    dcs = set(dcw.keys())
    covered_by = build_covered_by(TRAVEL_CACHE, dcs, MAX_WAIT)

    routing = RoutingCache(parquet_path=TRAVEL_CACHE, osrm_url="http://localhost:5001")
    base_reqs = load_requests(
        REQUESTS_PATH,
        duration_minutes=DURATION,
        max_wait_time_seconds=MAX_WAIT,
        demand_scale=SCALE,
        seed=SEED,
    )
    timed = build_timed(base_reqs, 15.0)
    flat = build_flat(base_reqs, DURATION)

    rows = []
    for label, ns, q, ds, cap in cases:
        print(f"\n=== {label} ===")
        rows.append(
            run_case(
                label, ns, q, ds, cap, routing, base_reqs, timed, flat, dcw, dcs, covered_by
            )
        )
        r = rows[-1]
        print(
            f"  served={r['served_pct']:.2f}% p90={r['p90_wait_min']:.2f} "
            f"net={r['net_energy_pct']:+.2f}% below_tgt={r['vehicles_below_soc_target_count']} "
            f"chg_util={r['charger_utilization_pct']:.1f}% "
            f"sessions={r['total_charge_sessions']:,} q_p90={r['depot_queue_p90_min']:.2f}m"
        )

    print("\n" + "=" * 130)
    print(
        f"{'label':<22} {'sites':>5} {'plugs':>6} {'queue':>5} {'depot':>12} {'cap':>5} "
        f"{'served':>7} {'p90':>6} {'net%':>7} {'<tgt':>5} {'chgU':>6} {'sess':>8} {'q90m':>6}"
    )
    print("-" * 130)
    for r in rows:
        print(
            f"{r['label']:<22} {r['sites']:5d} {r['total_plugs']:6d} {r['queue']:>5} "
            f"{r['depot_sel']:>12} {r['max_cap']*100:4.0f}% "
            f"{r['served_pct']:7.2f} {r['p90_wait_min']:6.2f} {r['net_energy_pct']:+7.2f} "
            f"{r['vehicles_below_soc_target_count']:5d} {r['charger_utilization_pct']:6.1f} "
            f"{r['total_charge_sessions']:8,d} {r['depot_queue_p90_min']:6.2f}"
        )

    best = max(rows, key=lambda x: x["net_energy_pct"])
    print(f"\nBest net_energy: {best['label']} → {best['net_energy_pct']:+.2f}%")


if __name__ == "__main__":
    main()
