"""
Experiment 70 add-on — **154p×20** and **308p×20** with **proxy-best** N=2 depot cells.

Depots = best pair from ``search_n2_depot_pairs_origin_access`` (trip-weighted mean
origin→nearest-depot over top-40×40 candidates): ``88489e3461fffff``, ``88489e35e1fffff``.

Same continuous harness as Exp70 (``demand_scale=0.2``, fleet **4000**, **3-day**, **N=2**).

Run: PYTHONHASHSEED=0 python3 scripts/run_exp70_proxy_best_depots_154_308.py
"""
from __future__ import annotations

import sys
import time
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

DEPOT_CELLS = ["88489e3461fffff", "88489e35e1fffff"]

CONFIGS: tuple[tuple[str, int, float], ...] = (
    ("154p×20 kW (proxy-best depots)", 154, 20.0),
    ("308p×20 kW (2× fleet 12,320, proxy-best depots)", 308, 20.0),
)


def main() -> None:
    t0 = time.perf_counter()
    rows: list[dict] = []
    for label, plugs, ckw in tqdm(CONFIGS, desc="exp70_proxybest", unit="run", ncols=100):
        site_kw = float(plugs * ckw)
        slug_kw = int(ckw) if ckw == int(ckw) else ckw
        out = e63.run_continuous_experiment(
            N_DEPOTS,
            NUM_DAYS,
            demand_scale=SCALE,
            fleet_size=FLEET,
            plugs_per_site=plugs,
            charger_kw=ckw,
            depot_h3_cells=DEPOT_CELLS,
            show_trip_progress=True,
            trip_bar_desc=f"exp70_proxybest_N2_{plugs}p_{slug_kw}kW",
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

    wall_s = time.perf_counter() - t0
    lbl_w = max(48, max(len(r["label"]) for r in rows) + 2)
    print("\n" + "=" * 118)
    print(
        f"Exp70 proxy-best depots: {','.join(DEPOT_CELLS)} | "
        f"demand_scale={SCALE}, fleet={FLEET}, N={N_DEPOTS}, {NUM_DAYS}d | wall {wall_s:.1f}s ({wall_s/60:.2f} min)"
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
    print(f"\ndepots: {','.join(DEPOT_CELLS)}")


if __name__ == "__main__":
    main()
