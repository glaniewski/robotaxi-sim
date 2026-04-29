"""
Experiment 57 — Minimum plug dwell (min_plug_duration_minutes) sweep.

Holds Exp56-style infra fixed: 275 sites × 4 plugs × 20 kW, FIFO, fastest_balanced,
charge_supply_ratio=2, max_concurrent_charging_pct=0.20.

Sweeps min_plug_duration_minutes ∈ {0, 5, 10, 15, 20, 30, 45} and reports SLA,
energy, charger util, below-target (adjusted vs strict).

Run: PYTHONHASHSEED=0 python3 scripts/run_exp57_min_plug_sweep.py
"""
from __future__ import annotations

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
N_SITES = 275
PLUGS = 4
KW = 20.0
SITE_KW = PLUGS * KW

MIN_PLUG_MINUTES = [0.0, 5.0, 10.0, 15.0, 20.0, 30.0, 45.0]


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
    return {c: {bb: v / bs for bb, v in bk.items()} for c, bk in counts.items()}


def build_flat(reqs, dur):
    counts: dict[str, int] = {}
    for r in reqs:
        counts[r.origin_h3] = counts.get(r.origin_h3, 0) + 1
    return {c: n / (dur * 60.0) for c, n in counts.items()}


def main() -> None:
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
    for min_m in MIN_PLUG_MINUTES:
        label = f"minplug_{int(min_m)}m" if min_m == int(min_m) else f"minplug_{min_m}m"
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
            charging_queue_policy="fifo",
            charging_depot_selection="fastest_balanced",
            charging_depot_balance_slack_minutes=3.0,
            charge_supply_ratio=2.0,
            max_concurrent_charging_pct=0.20,
            timeseries_bucket_minutes=15.0,
            min_plug_duration_minutes=min_m,
        )
        depot_cells = top_demand_cells(N_SITES)
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
                h3_cell=c,
                chargers_count=PLUGS,
                charger_kw=KW,
                site_power_kw=SITE_KW,
            )
            for i, c in enumerate(depot_cells)
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
        rows.append(
            {
                "min_plug_min": min_m,
                "served_pct": m["served_pct"],
                "p90_wait_min": m["p90_wait_min"],
                "fleet_battery_pct": m["fleet_battery_pct"],
                "net_energy_pct": net,
                "below_tgt": m["vehicles_below_soc_target_count"],
                "below_tgt_strict": m["vehicles_below_soc_target_strict_count"],
                "chg_util": m["charger_utilization_pct"],
                "sessions": m["total_charge_sessions"],
                "q_p90": m["depot_queue_p90_min"],
                "margin": m["contribution_margin_per_trip"],
                **du,
            }
        )
        print(
            f"  min={min_m:g}m → served={m['served_pct']:.2f}% p90={m['p90_wait_min']:.2f} "
            f"net={net:+.2f}% below={m['vehicles_below_soc_target_count']} "
            f"strict={m['vehicles_below_soc_target_strict_count']} chgU={m['charger_utilization_pct']:.1f}%"
        )

    print("\n" + "=" * 120)
    hdr = (
        f"{'min_m':>6} {'served':>7} {'p90':>6} {'net%':>7} {'<tgt':>5} {'strict':>6} "
        f"{'chgU':>6} {'sess':>8} {'q90m':>6} {'margin':>7}"
    )
    print(hdr)
    print("-" * len(hdr))
    for r in rows:
        print(
            f"{r['min_plug_min']:6.0f} {r['served_pct']:7.2f} {r['p90_wait_min']:6.2f} {r['net_energy_pct']:+7.2f} "
            f"{r['below_tgt']:5d} {r['below_tgt_strict']:6d} {r['chg_util']:6.1f} {r['sessions']:8,d} "
            f"{r['q_p90']:6.2f} {r['margin']:7.2f}"
        )


if __name__ == "__main__":
    main()
