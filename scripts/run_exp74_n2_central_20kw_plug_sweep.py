"""
Experiment 74 — **N=2** central depots: sweep **20 kW** plug count per site (**200–400**).

Same continuous harness as **Exp71**: ``demand_scale=0.2``, fleet **4000**, **3-day**,
**N=2**, default **`top_demand_cells(2)`** depot placement, **20 kW** per plug.
Only **plugs_per_site** varies (**200, 250, 300, 350, 400**).

Run: PYTHONHASHSEED=0 python3 scripts/run_exp74_n2_central_20kw_plug_sweep.py
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
CHARGER_KW = 20.0

PLUG_COUNTS: tuple[int, ...] = (200, 250, 300, 350, 400)


def main() -> None:
    central = e63.top_demand_cells(N_DEPOTS)
    print(f"Central depots (default): {central}")
    t0 = time.perf_counter()
    rows: list[dict] = []
    for plugs in tqdm(PLUG_COUNTS, desc="exp74_plugs", unit="run", ncols=100):
        out = e63.run_continuous_experiment(
            N_DEPOTS,
            NUM_DAYS,
            demand_scale=SCALE,
            fleet_size=FLEET,
            plugs_per_site=plugs,
            charger_kw=CHARGER_KW,
            show_trip_progress=True,
            trip_bar_desc=f"exp74_N2_{plugs}p20_central",
        )
        m = out["metrics"]
        daily = out["daily"]
        sp = [x["served_pct"] for x in daily]
        trips = sum(x["arrivals"] for x in daily)
        site_kw = plugs * CHARGER_KW
        rows.append(
            {
                "plugs": plugs,
                "site_kw": site_kw,
                "fleet_kw": 2 * site_kw,
                "trips": trips,
                "served_pct": m["served_pct"],
                "p90_wait": m["p90_wait_min"],
                "chg_util": m["charger_utilization_pct"],
                "fleet_battery_pct": m["fleet_battery_pct"],
                "served_pct_d1_d3": sp,
                "depots": ",".join(out["depot_h3_cells"]),
            }
        )

    wall_s = time.perf_counter() - t0
    lbl_w = 28
    print("\n" + "=" * 118)
    print(
        f"Exp74: demand_scale={SCALE}, fleet={FLEET}, N={N_DEPOTS}, {NUM_DAYS}d | "
        f"20 kW/plug, default central depots | {len(rows)} runs | wall {wall_s:.1f}s ({wall_s/60:.2f} min)"
    )
    hdr = (
        f"{'plugs@20kW':<{lbl_w}} {'site_kW':>9} {'fleet_kW':>9} {'trips':>8} {'served%':>8} {'p90_w':>7} "
        f"{'chgU%':>7} {'fleetSOC%':>9}  d1,d2,d3 served%"
    )
    print(hdr)
    print("-" * len(hdr))
    for r in rows:
        sd = ", ".join(f"{x:.1f}" for x in r["served_pct_d1_d3"])
        label = f"{r['plugs']}p×20"
        print(
            f"{label:<{lbl_w}} {r['site_kw']:9.0f} {r['fleet_kw']:9.0f} {r['trips']:8d} "
            f"{r['served_pct']:8.2f} {r['p90_wait']:7.2f} {r['chg_util']:7.1f} {r['fleet_battery_pct']:9.2f}  {sd}"
        )
        print(f"{'':<{lbl_w}} depots: {r['depots']}")


if __name__ == "__main__":
    main()
