"""
Experiment 69 — Repeat all Exp68 configs at demand_scale=0.2, fleet=4000, with battery_kwh=40.

Exp68 used SimConfig default battery_kwh=75 (see engine.SimConfig). This sweep isolates pack size.

Same eight configs as RESULTS Exp68 table (order preserved).

Run: PYTHONHASHSEED=0 python3 scripts/run_exp69_scale02_repeat_exp68_battery40.py
"""
from __future__ import annotations

import sys
from pathlib import Path

from tqdm import tqdm

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(ROOT / "backend"))
sys.path.insert(0, str(SCRIPT_DIR))

import run_exp63_continuous_multiday_steady_state as e63  # noqa: E402

NUM_DAYS = 3
FLEET = 4000
SCALE = 0.2
BATTERY_KWH = 40.0
N77 = 77

# Exp68 served_pct (battery_kwh=75) for Δ column — from RESULTS.md 2026-04-04
EXP68_SERVED_PCT: tuple[float, ...] = (
    90.15,
    93.38,
    91.11,
    91.85,
    93.92,
    90.26,
    90.39,
    94.62,
)

# (label, n_sites, plugs, charger_kw)
RUNS: tuple[tuple[str, int, int, float], ...] = (
    ("8p×20 kW (N=77)", N77, 8, 20.0),
    ("12p×20 kW (N=77)", N77, 12, 20.0),
    ("4p×40 kW (N=77)", N77, 4, 40.0),
    ("4p×60 kW (N=77)", N77, 4, 60.0),
    ("16p×20 kW (N=77)", N77, 16, 20.0),
    ("4p×80 kW (N=77)", N77, 4, 80.0),
    ("N=154 (2×sites), 4p×20", 154, 4, 20.0),
    ("N=231 (3×sites), 4p×20", 231, 4, 20.0),
)


def main() -> None:
    rows: list[dict] = []
    for i, (label, n_sites, plugs, ckw) in enumerate(
        tqdm(RUNS, desc="exp69_config", unit="run", ncols=100)
    ):
        site_kw = plugs * ckw
        slug = f"exp69_bat{int(BATTERY_KWH)}_N{n_sites}_{plugs}p_{int(ckw)}kW"
        out = e63.run_continuous_experiment(
            n_sites,
            NUM_DAYS,
            demand_scale=SCALE,
            fleet_size=FLEET,
            plugs_per_site=plugs,
            charger_kw=ckw,
            battery_kwh=BATTERY_KWH,
            show_trip_progress=True,
            trip_bar_desc=slug,
        )
        m = out["metrics"]
        daily = out["daily"]
        sp = [x["served_pct"] for x in daily]
        trips = sum(x["arrivals"] for x in daily)
        served = m["served_pct"]
        rows.append(
            {
                "label": label,
                "site_kw": site_kw,
                "trips": trips,
                "served_pct": served,
                "exp68_served": EXP68_SERVED_PCT[i],
                "delta_served": served - EXP68_SERVED_PCT[i],
                "p90_wait": m["p90_wait_min"],
                "chg_util": m["charger_utilization_pct"],
                "fleet_battery_pct": m["fleet_battery_pct"],
                "served_pct_d1_d3": sp,
            }
        )

    lbl_w = max(28, max(len(r["label"]) for r in rows) + 2)
    print("\n" + "=" * 125)
    print(
        f"Exp69: same eight configs as Exp68 | demand_scale={SCALE}, fleet={FLEET}, "
        f"battery_kwh={BATTERY_KWH:g} (Exp68 used SimConfig default 75 kWh)"
    )
    hdr = (
        f"{'config':<{lbl_w}} {'site_kW':>7} {'trips':>8} {'served%':>8} "
        f"{'exp68%':>8} {'Δsrv':>7} {'p90_w':>7} {'chgU%':>7} {'fleetSOC%':>9}  d1,d2,d3"
    )
    print(hdr)
    print("-" * len(hdr))
    for r in rows:
        sd = ", ".join(f"{x:.1f}" for x in r["served_pct_d1_d3"])
        print(
            f"{r['label']:<{lbl_w}} {r['site_kw']:7.0f} {r['trips']:8d} {r['served_pct']:8.2f} "
            f"{r['exp68_served']:8.2f} {r['delta_served']:+7.2f} {r['p90_wait']:7.2f} "
            f"{r['chg_util']:7.1f} {r['fleet_battery_pct']:9.2f}  {sd}"
        )


if __name__ == "__main__":
    main()
