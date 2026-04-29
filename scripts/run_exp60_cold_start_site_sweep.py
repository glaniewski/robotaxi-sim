"""
Experiment 60 — Cold start (soc_initial=0.50), min_plug=10m: microsite count sweep.

Fixed: 4×20 kW plugs, 80 kW/site, FIFO, fastest_balanced, charge_supply_ratio=2,
max_concurrent_charging_pct=0.20, seed=123, demand_scale=0.1, fleet=3000, 24h.

Goal: bracket site count N such that net_energy_pct ∈ [0, 5]
(net = fleet_battery_pct − soc_initial×100). **Refinement:** coarse grid ended at 30 sites;
runs for N ∈ {29,28,27} showed **minimum N = 28** still in-band (+0.06%), **N = 27** below 0%.

Run: PYTHONHASHSEED=0 python3 scripts/run_exp60_cold_start_site_sweep.py
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
PLUGS = 4
KW = 20.0
SITE_KW = PLUGS * KW
MIN_PLUG = 10.0
SOC_INITIAL = 0.50

# Descending coarse grid; extend low if net still > 5%
SITE_COUNTS_COARSE = [220, 180, 150, 130, 110, 95, 80, 70, 60, 55, 50, 45, 40, 35, 30]


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


def run_one(
    n_sites: int,
    *,
    base_reqs,
    timed,
    flat,
    dcw,
    dcs,
    covered_by,
    routing,
) -> dict:
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
        soc_initial=SOC_INITIAL,
        soc_target=0.80,
        soc_charge_start=0.80,
        soc_min=0.20,
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
        min_plug_duration_minutes=MIN_PLUG,
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
            h3_cell=c,
            chargers_count=PLUGS,
            charger_kw=KW,
            site_power_kw=SITE_KW,
        )
        for i, c in enumerate(depot_cells)
    ]
    label = f"sites{n_sites}"
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
        "n_sites": n_sites,
        "plugs": n_sites * PLUGS,
        "served_pct": m["served_pct"],
        "p90_wait_min": m["p90_wait_min"],
        "fleet_battery_pct": m["fleet_battery_pct"],
        "fleet_soc_median_pct": m["fleet_soc_median_pct"],
        "net_energy_pct": net,
        "below_tgt": m["vehicles_below_soc_target_count"],
        "below_tgt_strict": m["vehicles_below_soc_target_strict_count"],
        "chg_util": m["charger_utilization_pct"],
        "sessions": m["total_charge_sessions"],
        "q_p90": m["depot_queue_p90_min"],
        "margin": m["contribution_margin_per_trip"],
        **du,
    }


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

    rows: list[dict] = []
    for n_sites in SITE_COUNTS_COARSE:
        r = run_one(
            n_sites,
            base_reqs=base_reqs,
            timed=timed,
            flat=flat,
            dcw=dcw,
            dcs=dcs,
            covered_by=covered_by,
            routing=routing,
        )
        rows.append(r)
        net = r["net_energy_pct"]
        print(
            f"  sites={n_sites:3d} → served={r['served_pct']:.2f}% p90={r['p90_wait_min']:.2f} "
            f"net={net:+.2f}% median_soc={r['fleet_soc_median_pct']:.1f}% "
            f"chgU={r['chg_util']:.1f}%"
        )

    in_band = [r for r in rows if 0.0 <= r["net_energy_pct"] <= 5.0]
    min_in_band = min((r["n_sites"] for r in in_band), default=None)

    print("\n" + "=" * 100)
    hdr = f"{'sites':>5} {'plugs':>6} {'served':>7} {'p90':>6} {'net%':>7} {'med%':>6} {'<tgt':>5} {'chgU':>6} {'q90m':>6}"
    print(hdr)
    print("-" * len(hdr))
    for r in rows:
        print(
            f"{r['n_sites']:5d} {r['plugs']:6d} {r['served_pct']:7.2f} {r['p90_wait_min']:6.2f} "
            f"{r['net_energy_pct']:+7.2f} {r['fleet_soc_median_pct']:6.1f} {r['below_tgt']:5d} "
            f"{r['chg_util']:6.1f} {r['q_p90']:6.2f}"
        )

    print()
    if min_in_band is not None:
        print(
            f"Minimum microsites in [0%, 5%] on this coarse grid: **{min_in_band}** "
            f"(refine downward from there; Exp60 refinement found **28** as global min, **27** out of band)."
        )
    else:
        print("No coarse grid point landed in [0%, 5%]. See table to bracket; refine with extra N.")


if __name__ == "__main__":
    main()
