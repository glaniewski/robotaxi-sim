"""
Experiment 63 — One continuous multi-day clock (no midnight reset).

Repeats the same 24h demand slice for NUM_DAYS on a single SimConfig.duration_minutes
horizon via load_requests_repeated_days. 220×(4×20 kW @ 80 kW/site), soc_initial=0.8,
same charging/reposition settings as Exp60/62.

Aggregates per-calendar-day served% and mean fleet SOC from timeseries buckets.

Run: PYTHONHASHSEED=0 python3 scripts/run_exp63_continuous_multiday_steady_state.py
      PYTHONHASHSEED=0 python3 scripts/run_exp63_continuous_multiday_steady_state.py --sites 77 --days 10
      PYTHONHASHSEED=0 python3 scripts/run_exp63_continuous_multiday_steady_state.py --days 3 --fleet 4000 --demand-scale 0.1
      PYTHONHASHSEED=0 python3 scripts/run_exp63_continuous_multiday_steady_state.py --days 3 --sites 77 --fleet 4000 --demand-scale 0.2 --plugs 4 --charger-kw 40
      PYTHONHASHSEED=0 python3 scripts/run_exp63_continuous_multiday_steady_state.py --sites 2 --depot-cells 88489e3569fffff,88489e341bfffff --plugs 308 --charger-kw 20 --fleet 4000 --demand-scale 0.2 --days 3
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd
from tqdm import tqdm

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "backend"))

from app.schemas import (
    DepotConfig, DemandConfig, EconomicsConfig, FleetConfig,
    RepositioningConfig, ScenarioConfig,
)
from app.sim.demand import load_requests, load_requests_repeated_days
from app.sim.engine import SimConfig, SimulationEngine, build_vehicles
from app.sim.entities import Depot, Request
from app.sim.reposition_policies import build_covered_by, build_policy
from app.sim.routing import RoutingCache

REQUESTS_PATH = str(ROOT / "data" / "requests_austin_h3_r8.parquet")
TRAVEL_CACHE = str(ROOT / "data" / "h3_travel_cache.parquet")

SEED = 123
DAY_MINUTES = 1440
NUM_DAYS = 10
MAX_WAIT = 600.0
SCALE = 0.1
FLEET = 3000
N_SITES = 220
PLUGS = 4
KW = 20.0
SITE_KW = PLUGS * KW
BUCKET_MIN = 15.0
MIN_PLUG = 10.0
SOC_INITIAL = 0.80


def top_demand_cells(n: int) -> list[str]:
    """Top ``n`` H3 cells by trip **origin** count (same as historical demand ranking)."""
    df = pd.read_parquet(REQUESTS_PATH, columns=["origin_h3"])
    return df["origin_h3"].value_counts().head(n).index.tolist()


def top_destination_cells(n: int) -> list[str]:
    """Top ``n`` H3 cells by trip **destination** count."""
    df = pd.read_parquet(REQUESTS_PATH, columns=["destination_h3"])
    return df["destination_h3"].value_counts().head(n).index.tolist()


def top_origin_plus_destination_cells(n: int) -> list[str]:
    """Top ``n`` H3 cells by **origin count + destination count** (per cell)."""
    df = pd.read_parquet(REQUESTS_PATH, columns=["origin_h3", "destination_h3"])
    o = df["origin_h3"].value_counts()
    d = df["destination_h3"].value_counts()
    combined = o.add(d, fill_value=0).sort_values(ascending=False)
    return combined.head(n).index.tolist()


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


def arrivals_per_day(requests: list[Request], day: int) -> int:
    lo = day * DAY_MINUTES * 60.0
    hi = (day + 1) * DAY_MINUTES * 60.0
    return sum(1 for r in requests if lo <= r.request_time < hi)


def _served_cumulative_upto(ts: pd.DataFrame, t_minutes: float) -> int:
    s = ts[ts["t_minutes"] <= t_minutes]
    return int(s["served_cumulative"].iloc[-1]) if len(s) else 0


def run_continuous_experiment(
    n_sites: int,
    num_days: int,
    *,
    demand_scale: float | None = None,
    fleet_size: int | None = None,
    plugs_per_site: int | None = None,
    charger_kw: float | None = None,
    battery_kwh: float | None = None,
    depot_h3_cells: list[str] | None = None,
    min_plug_duration_minutes: float | None = None,
    charging_queue_policy: str | None = None,
    vehicle_preset: str | None = None,
    reposition_alpha: float | None = None,
    show_trip_progress: bool = True,
    trip_bar_desc: str | None = None,
) -> dict:
    """
    One continuous multi-day run (same config as Exp63). Returns metrics, daily rows, engine result.

    vehicle_preset: "tesla" or "waymo" — overrides kwh_per_mile, vehicle_cost_usd,
        and maintenance_cost_per_mile from VEHICLE_PRESETS dict.
    reposition_alpha: 0.0–1.0 blend weight for repositioning; None → 0.6.
    """
    from app.schemas import VEHICLE_PRESETS

    scale = SCALE if demand_scale is None else float(demand_scale)
    fs = FLEET if fleet_size is None else int(fleet_size)
    pp = PLUGS if plugs_per_site is None else int(plugs_per_site)
    ckw = float(KW if charger_kw is None else charger_kw)
    site_kw = float(pp * ckw)

    preset_name = (vehicle_preset or "tesla").lower()
    preset = VEHICLE_PRESETS.get(preset_name, VEHICLE_PRESETS["tesla"])
    _battery = float(battery_kwh) if battery_kwh is not None else 75.0
    _base_cost = preset["base_vehicle_cost_usd"]
    _battery_cost_per_kwh = 100.0
    _vehicle_cost = _base_cost + _battery * _battery_cost_per_kwh
    df = pd.read_parquet(REQUESTS_PATH, columns=["origin_h3"])
    dcw = df["origin_h3"].value_counts().to_dict()
    dcs = set(dcw.keys())
    covered_by = build_covered_by(TRAVEL_CACHE, dcs, MAX_WAIT)
    routing = RoutingCache(parquet_path=TRAVEL_CACHE, osrm_url="http://localhost:5001")

    template = load_requests(
        REQUESTS_PATH,
        duration_minutes=DAY_MINUTES,
        max_wait_time_seconds=MAX_WAIT,
        demand_scale=scale,
        seed=SEED,
    )
    timed = build_timed(template, BUCKET_MIN)
    flat = build_flat(template, DAY_MINUTES)

    requests = load_requests_repeated_days(
        REQUESTS_PATH,
        duration_minutes_per_day=DAY_MINUTES,
        num_days=num_days,
        max_wait_time_seconds=MAX_WAIT,
        demand_scale=scale,
        seed=SEED,
    )

    sc_kwargs: dict = dict(
        duration_minutes=float(DAY_MINUTES * num_days),
        seed=SEED,
        fleet_size=fs,
        soc_initial=SOC_INITIAL,
        soc_target=0.80,
        soc_charge_start=0.80,
        soc_min=0.20,
        max_wait_time_seconds=MAX_WAIT,
        reposition_enabled=True,
        reposition_alpha=float(reposition_alpha) if reposition_alpha is not None else 0.6,
        reposition_top_k_cells=50,
        max_vehicles_targeting_cell=3,
        kwh_per_mile=preset["kwh_per_mile"],
        # Itemized cost params
        electricity_cost_per_kwh=0.068,
        demand_charge_per_kw_month=13.56,
        maintenance_cost_per_mile=preset["maintenance_cost_per_mile"],
        insurance_cost_per_vehicle_day=4.00,
        teleops_cost_per_vehicle_day=3.50,
        cleaning_cost_per_vehicle_day=6.00,
        base_vehicle_cost_usd=_base_cost,
        battery_cost_per_kwh=_battery_cost_per_kwh,
        vehicle_cost_usd=_vehicle_cost,
        vehicle_lifespan_years=5.0,
        cost_per_site_day=250.0,
        # Charging
        charging_queue_policy=(
            "fifo" if charging_queue_policy is None else str(charging_queue_policy).strip().lower()
        ),
        charging_depot_selection="fastest_balanced",
        charging_depot_balance_slack_minutes=3.0,
        charge_supply_ratio=2.0,
        max_concurrent_charging_pct=0.20,
        timeseries_bucket_minutes=BUCKET_MIN,
        min_plug_duration_minutes=MIN_PLUG
        if min_plug_duration_minutes is None
        else float(min_plug_duration_minutes),
    )
    if battery_kwh is not None:
        sc_kwargs["battery_kwh"] = float(battery_kwh)
    _qpol = sc_kwargs["charging_queue_policy"]
    if _qpol not in ("fifo", "jit"):
        raise ValueError(f"charging_queue_policy must be 'fifo' or 'jit', got {_qpol!r}")
    sc = SimConfig(**sc_kwargs)

    if depot_h3_cells is not None:
        depot_cells = [str(c).strip() for c in depot_h3_cells if str(c).strip()]
        if len(depot_cells) == 0:
            raise ValueError("depot_h3_cells must contain at least one non-empty H3 id")
        if len(depot_cells) != n_sites:
            raise ValueError(
                f"n_sites ({n_sites}) must equal len(depot_h3_cells) ({len(depot_cells)})"
            )
    else:
        depot_cells = top_demand_cells(n_sites)
    vehicles = build_vehicles(sc, depot_h3_cells=depot_cells, seed=SEED, demand_cells=dcw)
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
    _alpha = float(reposition_alpha) if reposition_alpha is not None else 0.6
    policy = build_policy(
        name="coverage_floor",
        alpha=_alpha,
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

    desc = trip_bar_desc or f"exp63_N{n_sites}_d{num_days}"
    bar = tqdm(
        total=len(requests),
        desc=desc,
        unit="trips",
        ncols=100,
        disable=not show_trip_progress,
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
    if show_trip_progress:
        bar.update(len(requests) - last[0])
        bar.close()

    m = res["metrics"]
    ts = pd.DataFrame(res["timeseries"])
    daily = daily_stats_from_timeseries(ts, requests, num_days)

    scenario_config = ScenarioConfig(
        seed=SEED,
        duration_minutes=float(DAY_MINUTES * num_days),
        fleet=FleetConfig(
            size=fs,
            battery_kwh=_battery,
            kwh_per_mile=preset["kwh_per_mile"],
            soc_initial=SOC_INITIAL,
            soc_min=0.20,
            soc_charge_start=0.80,
            soc_target=0.80,
        ),
        depots=[
            DepotConfig(id=d.id, h3_cell=d.h3_cell,
                        chargers_count=pp, charger_kw=ckw, site_power_kw=site_kw)
            for d in depots
        ],
        demand=DemandConfig(
            max_wait_time_seconds=MAX_WAIT,
            demand_scale=scale,
        ),
        repositioning=RepositioningConfig(
            reposition_enabled=True,
            reposition_alpha=_alpha,
        ),
        economics=EconomicsConfig(
            electricity_cost_per_kwh=0.068,
            demand_charge_per_kw_month=13.56,
            maintenance_cost_per_mile=preset["maintenance_cost_per_mile"],
            insurance_cost_per_vehicle_day=4.00,
            teleops_cost_per_vehicle_day=3.50,
            cleaning_cost_per_vehicle_day=6.00,
            base_vehicle_cost_usd=_base_cost,
            battery_cost_per_kwh=_battery_cost_per_kwh,
            vehicle_cost_usd=_vehicle_cost,
            vehicle_lifespan_years=5.0,
            cost_per_site_day=250.0,
        ),
        timeseries_bucket_minutes=BUCKET_MIN,
        charging_queue_policy=sc_kwargs["charging_queue_policy"],
        charging_depot_selection="fastest_balanced",
        charging_depot_balance_slack_minutes=3.0,
        min_plug_duration_minutes=sc_kwargs.get("min_plug_duration_minutes", 0.0),
    )

    return {
        "metrics": m,
        "daily": daily,
        "result": res,
        "scenario_config": scenario_config,
        "n_sites": n_sites,
        "num_days": num_days,
        "depot_h3_cells": list(depot_cells),
    }


def daily_stats_from_timeseries(
    ts: pd.DataFrame, requests: list[Request], num_days: int
) -> list[dict]:
    rows: list[dict] = []
    for d in range(num_days):
        lo_m = d * DAY_MINUTES
        hi_m = (d + 1) * DAY_MINUTES
        # Half-open [lo_m, hi_m) for bucket averages; boundary snapshots at hi_m
        # belong to cumulative served via <= hi_m above.
        sub = ts[(ts["t_minutes"] >= lo_m) & (ts["t_minutes"] < hi_m)]
        tail = ts[ts["t_minutes"] < hi_m]
        served_lo = _served_cumulative_upto(ts, lo_m)
        served_hi = _served_cumulative_upto(ts, hi_m)
        served_day = served_hi - served_lo
        arr = arrivals_per_day(requests, d)
        served_pct = 100.0 * served_day / arr if arr > 0 else 0.0
        rows.append(
            {
                "day": d + 1,
                "served_pct": round(served_pct, 2),
                "mean_fleet_soc_pct": round(sub["fleet_mean_soc_pct"].mean(), 2)
                if len(sub)
                else 0.0,
                "last_bucket_soc_pct": float(tail["fleet_mean_soc_pct"].iloc[-1])
                if len(tail)
                else 0.0,
                "arrivals": arr,
            }
        )
    return rows


def main() -> None:
    ap = argparse.ArgumentParser(description="Continuous multi-day sim (Exp63 config).")
    ap.add_argument("--sites", type=int, default=N_SITES, help=f"microsite count (default {N_SITES})")
    ap.add_argument("--days", type=int, default=NUM_DAYS, help=f"calendar days on one clock (default {NUM_DAYS})")
    ap.add_argument(
        "--fleet",
        type=int,
        default=None,
        help=f"fleet size (default module FLEET={FLEET})",
    )
    ap.add_argument(
        "--demand-scale",
        type=float,
        default=None,
        dest="demand_scale",
        help=f"demand scale (default module SCALE={SCALE})",
    )
    ap.add_argument(
        "--plugs",
        type=int,
        default=None,
        dest="plugs_per_site",
        help=f"chargers per microsite (default module PLUGS={PLUGS})",
    )
    ap.add_argument(
        "--charger-kw",
        type=float,
        default=None,
        dest="charger_kw",
        help=f"per-plug kW (default module KW={KW}); site_power_kw = plugs × charger_kw",
    )
    ap.add_argument(
        "--battery-kwh",
        type=float,
        default=None,
        dest="battery_kwh",
        help="vehicle battery pack size kWh (default: SimConfig 75)",
    )
    ap.add_argument(
        "--depot-cells",
        type=str,
        default=None,
        dest="depot_cells",
        help="comma-separated H3 res-8 depot cells (overrides top_demand_cells); count must match --sites",
    )
    ap.add_argument(
        "--charging-queue-policy",
        type=str,
        choices=("fifo", "jit"),
        default="fifo",
        dest="charging_queue_policy",
        help="depot wait policy: fifo (wait in queue) or jit (retry later if plugs full)",
    )
    args = ap.parse_args()
    n_sites, num_days = args.sites, args.days

    fs = args.fleet
    sc = args.demand_scale
    pp = args.plugs_per_site
    ckw_arg = args.charger_kw
    trip_lbl = f"exp63_N{n_sites}_d{num_days}"
    if fs is not None:
        trip_lbl += f"_f{fs}"
    if sc is not None:
        trip_lbl += f"_s{sc}"
    if pp is not None:
        trip_lbl += f"_p{pp}"
    if ckw_arg is not None:
        trip_lbl += f"_k{int(ckw_arg) if ckw_arg == int(ckw_arg) else ckw_arg}"
    bk_arg = args.battery_kwh
    if bk_arg is not None:
        trip_lbl += f"_bat{int(bk_arg) if bk_arg == int(bk_arg) else bk_arg}"

    depot_list: list[str] | None = None
    if args.depot_cells:
        depot_list = [x.strip() for x in args.depot_cells.split(",") if x.strip()]
        if len(depot_list) != n_sites:
            ap.error(f"--depot-cells must have {n_sites} entries (got {len(depot_list)})")
        trip_lbl += "_customDepots"

    out = run_continuous_experiment(
        n_sites,
        num_days,
        demand_scale=sc,
        fleet_size=fs,
        plugs_per_site=pp,
        charger_kw=ckw_arg,
        battery_kwh=bk_arg,
        depot_h3_cells=depot_list,
        charging_queue_policy=args.charging_queue_policy,
        show_trip_progress=True,
        trip_bar_desc=trip_lbl,
    )
    m = out["metrics"]
    daily = out["daily"]

    fleet_print = fs if fs is not None else FLEET
    scale_print = sc if sc is not None else SCALE
    plugs_print = pp if pp is not None else PLUGS
    ckw_print = float(KW if ckw_arg is None else ckw_arg)
    site_kw_print = plugs_print * ckw_print
    bat_print = bk_arg if bk_arg is not None else 75.0
    print("\n" + "=" * 90)
    print(
        f"Exp63 continuous: {num_days}×{DAY_MINUTES}m, N={n_sites} sites, "
        f"fleet={fleet_print}, demand_scale={scale_print}, "
        f"{plugs_print}p×{ckw_print:g}kW (site {site_kw_print:g} kW), "
        f"battery_kwh={bat_print:g}, soc_initial={SOC_INITIAL}"
    )
    print(
        f"Overall: served_pct={m['served_pct']:.2f} fleet_battery_pct={m['fleet_battery_pct']:.2f} "
        f"p90={m['p90_wait_min']:.2f} chgU={m['charger_utilization_pct']:.1f}%"
    )
    hdr = f"{'day':>4} {'served%':>8} {'meanSOC':>8} {'lastSOC':>8} {'arrivals':>8}"
    print(hdr)
    print("-" * len(hdr))
    for r in daily:
        print(
            f"{r['day']:4d} {r['served_pct']:8.2f} {r['mean_fleet_soc_pct']:8.2f} "
            f"{r['last_bucket_soc_pct']:8.2f} {r['arrivals']:8d}"
        )
    late = daily[-3:]
    sp = [x["served_pct"] for x in daily]
    soc = [x["mean_fleet_soc_pct"] for x in daily]
    print(f"\nServed% range (all days): {min(sp):.2f}–{max(sp):.2f}")
    print(f"Mean fleet SOC range (intraday avg): {min(soc):.2f}–{max(soc):.2f}")
    print(f"Last 3 days served%: {[x['served_pct'] for x in late]}")
    print(
        "Last 3 days mean SOC: "
        f"{[float(x['mean_fleet_soc_pct']) for x in late]}"
    )


if __name__ == "__main__":
    main()
