"""
Experiment 68 — demand_scale=0.2, fleet=4000, N=77, 3-day continuous: plug count vs plug kW.

A) More plugs @ 20 kW, site_kw = plugs×20 (no throttle at full occupancy).
B) 4 plugs @ 40 kW and 60 kW, site_kw = 4×kW (matched peak vs 8×20 and 12×20).
C) 16p×20 kW vs 4p×80 kW @ 320 kW/site (fair peak-power pair).
D) **More microsites**, same **4p×20 kW/site** (baseline per-site layout): N=**154** (2×77), N=**231** (3×77).

Run: PYTHONHASHSEED=0 python3 scripts/run_exp68_scale02_plugs_vs_kw_sweep.py
      PYTHONHASHSEED=0 python3 scripts/run_exp68_scale02_plugs_vs_kw_sweep.py --only-extra
      PYTHONHASHSEED=0 python3 scripts/run_exp68_scale02_plugs_vs_kw_sweep.py --only-site-mults
"""
from __future__ import annotations

import sys
from pathlib import Path

import argparse

from tqdm import tqdm

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(ROOT / "backend"))
sys.path.insert(0, str(SCRIPT_DIR))

import run_exp63_continuous_multiday_steady_state as e63  # noqa: E402

N_SITES_BASE = 77
NUM_DAYS = 3
FLEET = 4000
SCALE = 0.2
PLUGS_BASE = 4
KW_BASE = 20.0

# More sites, same 4×20 kW per site as Exp67 baseline (N=77).
SITE_MULT_CONFIGS: tuple[tuple[str, int], ...] = (
    (f"N={N_SITES_BASE * 2} (2×sites), 4p×20", N_SITES_BASE * 2),
    (f"N={N_SITES_BASE * 3} (3×sites), 4p×20", N_SITES_BASE * 3),
)

# (label, plugs, charger_kw) — site_kw = plugs * charger_kw always
CONFIGS_BASE: tuple[tuple[str, int, float], ...] = (
    ("8p×20kW (site 160)", 8, 20.0),
    ("12p×20kW (site 240)", 12, 20.0),
    ("4p×40kW (site 160)", 4, 40.0),
    ("4p×60kW (site 240)", 4, 60.0),
)
CONFIGS_EXTRA: tuple[tuple[str, int, float], ...] = (
    ("16p×20kW (site 320)", 16, 20.0),
    ("4p×80kW (site 320)", 4, 80.0),
)
CONFIGS: tuple[tuple[str, int, float], ...] = CONFIGS_BASE + CONFIGS_EXTRA


def main() -> None:
    ap = argparse.ArgumentParser(description="Exp68 plug vs kW sweep at demand_scale=0.2.")
    ap.add_argument(
        "--only-extra",
        action="store_true",
        help="run only 16p×20 and 4p×80 (320 kW/site pair); skip first four configs",
    )
    ap.add_argument(
        "--only-site-mults",
        action="store_true",
        help="run only N=154 and N=231 with baseline 4p×20 kW/site (2× and 3× sites vs 77)",
    )
    args = ap.parse_args()
    if args.only_extra and args.only_site_mults:
        ap.error("use only one of --only-extra and --only-site-mults")

    rows: list[dict] = []

    if args.only_site_mults:
        for label, n_sites in tqdm(SITE_MULT_CONFIGS, desc="config", unit="run", ncols=100):
            site_kw = PLUGS_BASE * KW_BASE
            out = e63.run_continuous_experiment(
                n_sites,
                NUM_DAYS,
                demand_scale=SCALE,
                fleet_size=FLEET,
                plugs_per_site=PLUGS_BASE,
                charger_kw=KW_BASE,
                show_trip_progress=True,
                trip_bar_desc=f"exp68_s{SCALE}_f{FLEET}_N{n_sites}_4p_20kW",
            )
            m = out["metrics"]
            daily = out["daily"]
            sp = [x["served_pct"] for x in daily]
            trips = sum(x["arrivals"] for x in daily)
            rows.append(
                {
                    "label": label,
                    "n_sites": n_sites,
                    "plugs": PLUGS_BASE,
                    "charger_kw": KW_BASE,
                    "site_kw": site_kw,
                    "trips": trips,
                    "served_pct": m["served_pct"],
                    "p90_wait": m["p90_wait_min"],
                    "chg_util": m["charger_utilization_pct"],
                    "fleet_battery_pct": m["fleet_battery_pct"],
                    "served_pct_d1_d3": sp,
                }
            )
    else:
        configs = CONFIGS_EXTRA if args.only_extra else CONFIGS
        for label, plugs, ckw in tqdm(configs, desc="config", unit="run", ncols=100):
            site_kw = plugs * ckw
            out = e63.run_continuous_experiment(
                N_SITES_BASE,
                NUM_DAYS,
                demand_scale=SCALE,
                fleet_size=FLEET,
                plugs_per_site=plugs,
                charger_kw=ckw,
                show_trip_progress=True,
                trip_bar_desc=f"exp68_s{SCALE}_f{FLEET}_{plugs}p_{int(ckw)}kW",
            )
            m = out["metrics"]
            daily = out["daily"]
            sp = [x["served_pct"] for x in daily]
            trips = sum(x["arrivals"] for x in daily)
            rows.append(
                {
                    "label": label,
                    "n_sites": N_SITES_BASE,
                    "plugs": plugs,
                    "charger_kw": ckw,
                    "site_kw": site_kw,
                    "trips": trips,
                    "served_pct": m["served_pct"],
                    "p90_wait": m["p90_wait_min"],
                    "chg_util": m["charger_utilization_pct"],
                    "fleet_battery_pct": m["fleet_battery_pct"],
                    "served_pct_d1_d3": sp,
                }
            )

    lbl_w = max(36, max(len(r["label"]) for r in rows) + 2) if rows else 36
    print("\n" + "=" * 110)
    if args.only_site_mults:
        print(
            f"Exp68 (site count): demand_scale={SCALE}, fleet={FLEET}, 4p×20 kW/site, {NUM_DAYS}d | "
            "ref N=77 4p×20 (Exp67): served~76.2%, chgU~93.1%"
        )
    else:
        print(
            f"Exp68: demand_scale={SCALE}, fleet={FLEET}, N={N_SITES_BASE}, {NUM_DAYS}d continuous | "
            "baseline 4p×20 (Exp67): served~76.2%, chgU~93.1%"
        )
    hdr = (
        f"{'config':<{lbl_w}} {'site_kW':>8} {'trips':>8} {'served%':>8} {'p90_w':>7} "
        f"{'chgU%':>7} {'fleetSOC%':>9}  d1,d2,d3 served%"
    )
    print(hdr)
    print("-" * len(hdr))
    for r in rows:
        sd = ", ".join(f"{x:.1f}" for x in r["served_pct_d1_d3"])
        print(
            f"{r['label']:<{lbl_w}} {r['site_kw']:8.0f} {r['trips']:8d} {r['served_pct']:8.2f} "
            f"{r['p90_wait']:7.2f} {r['chg_util']:7.1f} {r['fleet_battery_pct']:9.2f}  {sd}"
        )


if __name__ == "__main__":
    main()
