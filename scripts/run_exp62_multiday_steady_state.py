"""
Experiment 62 — Multi-day steady state (220 microsites, 80 kW/site, 4×20 kW).

Day 1: soc_initial=0.8, same Austin demand slice as Exp60 (seed=123, scale=0.1, 24h).
Days 2–10: identical request stream each day; each vehicle starts at its previous
day's end-of-run SOC (after engine horizon charging interpolation), IDLE at last cell.

Run: PYTHONHASHSEED=0 python3 scripts/run_exp62_multiday_steady_state.py
"""
from __future__ import annotations

import statistics
import sys
from pathlib import Path

import pandas as pd
from tqdm import tqdm

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "backend"))

from app.sim.demand import load_requests
from app.sim.engine import SimConfig, SimulationEngine, build_vehicles
from app.sim.entities import Depot, Request, Vehicle, VehicleState
from app.sim.reposition_policies import build_covered_by, build_policy
from app.sim.routing import RoutingCache

REQUESTS_PATH = str(ROOT / "data" / "requests_austin_h3_r8.parquet")
TRAVEL_CACHE = str(ROOT / "data" / "h3_travel_cache.parquet")

SEED = 123
DURATION = 1440
MAX_WAIT = 600.0
SCALE = 0.1
FLEET = 3000
N_SITES = 220
PLUGS = 4
KW = 20.0
SITE_KW = PLUGS * KW
DAYS = 10
SOC_DAY1 = 0.80
MIN_PLUG = 10.0


def top_demand_cells(n: int) -> list[str]:
    df = pd.read_parquet(REQUESTS_PATH, columns=["origin_h3"])
    return df["origin_h3"].value_counts().head(n).index.tolist()


def build_timed(reqs, bm: float = 15.0):
    bs = bm * 60.0
    nb = int(round(1440.0 / bm))
    counts: dict = {}
    for r in reqs:
        b = int(r.request_time / bs) % nb
        counts.setdefault(r.origin_h3, {})
        counts[r.origin_h3][b] = counts[r.origin_h3].get(b, 0) + 1
    return {c: {bb: v / bs for bb, v in bk.items()} for c, bk in counts.items()}


def build_flat(reqs, dur: int):
    counts: dict[str, int] = {}
    for r in reqs:
        counts[r.origin_h3] = counts.get(r.origin_h3, 0) + 1
    return {c: n / (dur * 60.0) for c, n in counts.items()}


def clone_requests_from_template(template: list[Request]) -> list[Request]:
    return [
        Request(
            id=r.id,
            request_time=r.request_time,
            origin_h3=r.origin_h3,
            destination_h3=r.destination_h3,
            max_wait_time_seconds=MAX_WAIT,
        )
        for r in template
    ]


def vehicles_for_next_day(
    vehicles: dict[str, Vehicle], *, soc_target: float, battery_kwh: float, kpm: float
) -> list[Vehicle]:
    """Carry SOC and location; reset to IDLE for a new 24h window."""
    out: list[Vehicle] = []
    for v in sorted(vehicles.values(), key=lambda x: x.id):
        soc = max(0.0, min(1.0, float(v.soc)))
        out.append(
            Vehicle(
                id=v.id,
                current_h3=v.current_h3,
                state=VehicleState.IDLE,
                soc=soc,
                battery_kwh=battery_kwh,
                kwh_per_mile=kpm,
                charge_target_soc=soc_target,
            )
        )
    return out


def run_one_day(
    day_idx: int,
    vehicles: list[Vehicle],
    requests: list[Request],
    *,
    sc: SimConfig,
    depots: list[Depot],
    routing: RoutingCache,
    policy,
    show_trip_bar: bool,
) -> tuple[dict, SimulationEngine]:
    label = f"day{day_idx}"
    bar = tqdm(
        total=len(requests),
        desc=label,
        unit="trips",
        ncols=100,
        disable=not show_trip_bar,
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
    if show_trip_bar:
        bar.update(len(requests) - last[0])
        bar.close()
    return res, eng


def main() -> None:
    df = pd.read_parquet(REQUESTS_PATH, columns=["origin_h3"])
    dcw = df["origin_h3"].value_counts().to_dict()
    dcs = set(dcw.keys())
    covered_by = build_covered_by(TRAVEL_CACHE, dcs, MAX_WAIT)
    routing = RoutingCache(parquet_path=TRAVEL_CACHE, osrm_url="http://localhost:5001")

    template_reqs = load_requests(
        REQUESTS_PATH,
        duration_minutes=DURATION,
        max_wait_time_seconds=MAX_WAIT,
        demand_scale=SCALE,
        seed=SEED,
    )
    timed = build_timed(template_reqs, 15.0)
    flat = build_flat(template_reqs, DURATION)

    sc = SimConfig(
        duration_minutes=DURATION,
        seed=SEED,
        fleet_size=FLEET,
        soc_initial=SOC_DAY1,
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

    depot_cells = top_demand_cells(N_SITES)
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

    vehicles = build_vehicles(sc, depot_h3_cells=depot_cells, seed=SEED, demand_cells=dcw)
    rows: list[dict] = []
    prev_mean: float | None = None

    day_bar = tqdm(range(1, DAYS + 1), desc="days", unit="day", ncols=80)
    for day in day_bar:
        reqs = clone_requests_from_template(template_reqs)
        res, eng = run_one_day(
            day,
            vehicles,
            reqs,
            sc=sc,
            depots=depots,
            routing=routing,
            policy=policy,
            show_trip_bar=True,
        )
        m = res["metrics"]
        mean_soc = float(m["fleet_battery_pct"])
        d_mean = mean_soc - prev_mean if prev_mean is not None else float("nan")
        prev_mean = mean_soc
        net_vs_80 = mean_soc - SOC_DAY1 * 100.0
        socs = [float(v.soc) for v in eng.vehicles.values()]

        rows.append(
            {
                "day": day,
                "fleet_battery_pct": mean_soc,
                "fleet_soc_median_pct": m["fleet_soc_median_pct"],
                "soc_std_pp": statistics.pstdev(socs) * 100.0 if len(socs) > 1 else 0.0,
                "delta_mean_soc_pp": d_mean,
                "net_vs_day1_anchor_pct": net_vs_80,
                "served_pct": m["served_pct"],
                "p90_wait_min": m["p90_wait_min"],
                "charger_utilization_pct": m["charger_utilization_pct"],
                "vehicles_below_soc_target_count": m["vehicles_below_soc_target_count"],
            }
        )
        vehicles = vehicles_for_next_day(
            eng.vehicles,
            soc_target=float(sc.soc_target),
            battery_kwh=float(sc.battery_kwh),
            kpm=float(sc.kwh_per_mile),
        )

    print("\n" + "=" * 100)
    hdr = (
        f"{'day':>4} {'mean%':>8} {'med%':>8} {'std(pp)':>9} {'Δmean':>8} "
        f"{'net@80':>8} {'served%':>8} {'p90':>6} {'chgU':>7} {'<tgt':>6}"
    )
    print(hdr)
    print("-" * len(hdr))
    for r in rows:
        print(
            f"{r['day']:4d} {r['fleet_battery_pct']:8.2f} {r['fleet_soc_median_pct']:8.2f} "
            f"{r['soc_std_pp']:9.3f} {r['delta_mean_soc_pp']:+8.3f} "
            f"{r['net_vs_day1_anchor_pct']:+8.2f} {r['served_pct']:8.2f} {r['p90_wait_min']:6.2f} "
            f"{r['charger_utilization_pct']:7.1f} {int(r['vehicles_below_soc_target_count']):6d}"
        )
    d1, d10 = rows[0], rows[-1]
    print(
        f"\nMean SOC day1→day10: {d1['fleet_battery_pct']:.2f}% → {d10['fleet_battery_pct']:.2f}% "
        f"(Δ {d10['fleet_battery_pct'] - d1['fleet_battery_pct']:+.3f} pp)"
    )
    late = rows[-3:]
    dlate = [late[i]["delta_mean_soc_pp"] for i in range(len(late)) if late[i]["day"] > 1]
    if dlate:
        print(f"Last 3 day-over-day Δmean SOC (pp): {dlate}")


if __name__ == "__main__":
    main()
