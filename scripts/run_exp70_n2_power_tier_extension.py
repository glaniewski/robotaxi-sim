"""
Experiment 70 extension — seven new N=2 depot rows (skip 154×20 and 28×110 @ 1× already in Exp70).

20 / 110 / 308 kW classes; 308 kW replaces non-integer 330 at 1× and 2×; 3× uses 28×330.

Run: PYTHONHASHSEED=0 python3 scripts/run_exp70_n2_power_tier_extension.py
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

N_DEPOTS = 2
NUM_DAYS = 3
FLEET = 4000
SCALE = 0.2

# (label, plugs, charger_kw) — site_kw = plugs * charger_kw per depot
CONFIGS: tuple[tuple[str, int, float], ...] = (
    ("1× fleet 6160 kW: 10p×308 kW (site 3080)", 10, 308.0),
    ("2× fleet 12,320 kW: 308p×20 kW (site 6160)", 308, 20.0),
    ("2× fleet 12,320 kW: 56p×110 kW (site 6160)", 56, 110.0),
    ("2× fleet 12,320 kW: 20p×308 kW (site 6160)", 20, 308.0),
    ("3× fleet 18,480 kW: 462p×20 kW (site 9240)", 462, 20.0),
    ("3× fleet 18,480 kW: 84p×110 kW (site 9240)", 84, 110.0),
    ("3× fleet 18,480 kW: 28p×330 kW (site 9240)", 28, 330.0),
)


def main() -> None:
    rows: list[dict] = []
    for label, plugs, ckw in tqdm(CONFIGS, desc="exp70_ext", unit="run", ncols=100):
        site_kw = float(plugs * ckw)
        slug_kw = int(ckw) if ckw == int(ckw) else ckw
        out = e63.run_continuous_experiment(
            N_DEPOTS,
            NUM_DAYS,
            demand_scale=SCALE,
            fleet_size=FLEET,
            plugs_per_site=plugs,
            charger_kw=ckw,
            show_trip_progress=True,
            trip_bar_desc=f"exp70ext_N2_{plugs}p_{slug_kw}kW",
        )
        m = out["metrics"]
        daily = out["daily"]
        sp = [x["served_pct"] for x in daily]
        trips = sum(x["arrivals"] for x in daily)
        rows.append(
            {
                "label": label,
                "site_kw": site_kw,
                "fleet_kw": 2.0 * site_kw,
                "trips": trips,
                "served_pct": m["served_pct"],
                "p90_wait": m["p90_wait_min"],
                "chg_util": m["charger_utilization_pct"],
                "fleet_battery_pct": m["fleet_battery_pct"],
                "served_pct_d1_d3": sp,
            }
        )

    lbl_w = max(44, max(len(r["label"]) for r in rows) + 2)
    print("\n" + "=" * 120)
    print(
        f"Exp70 extension: demand_scale={SCALE}, fleet={FLEET}, N={N_DEPOTS} depots, {NUM_DAYS}d | "
        "seven new power-tier rows (merge into RESULTS Exp70 table)"
    )
    hdr = (
        f"{'config':<{lbl_w}} {'site_kW':>8} {'fleet_kW':>9} {'trips':>8} {'served%':>8} {'p90_w':>7} "
        f"{'chgU%':>7} {'fleetSOC%':>9}  d1,d2,d3 served%"
    )
    print(hdr)
    print("-" * len(hdr))
    for r in rows:
        sd = ", ".join(f"{x:.1f}" for x in r["served_pct_d1_d3"])
        print(
            f"{r['label']:<{lbl_w}} {r['site_kw']:8.0f} {r['fleet_kw']:9.0f} {r['trips']:8d} "
            f"{r['served_pct']:8.2f} {r['p90_wait']:7.2f} {r['chg_util']:7.1f} {r['fleet_battery_pct']:9.2f}  {sd}"
        )


if __name__ == "__main__":
    main()
