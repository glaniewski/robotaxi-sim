"""
Experiment 75 — **N=1** vs **N=2** depots at **identical fleet charging nameplate** (20 kW plugs only).

Matches **Exp71** central harness: ``demand_scale=0.2``, fleet **4000**, **3-day** continuous,
**20 kW** per plug, ``site_power_kw = plugs × charger_kw``, default **FIFO** + ``fastest_balanced``.

- **N=2 (baseline):** ``308p×20`` per site → **12,320 kW** fleet, ``top_demand_cells(2)``.
- **N=1:** **616p×20** on **one** site → **12,320 kW** fleet, ``top_demand_cells(1)`` (densest origin cell).

Run: ``PYTHONHASHSEED=0 python3 scripts/run_exp75_n1_vs_n2_same_fleet_kw.py``
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

NUM_DAYS = 3
FLEET = 4000
SCALE = 0.2
CHARGER_KW = 20.0
FLEET_KW = 12_320.0
PLUGS_N2 = 308
PLUGS_N1 = int(FLEET_KW / CHARGER_KW)  # 616 — all power at one mega-site


def main() -> None:
    arms = (
        ("N=1", 1, PLUGS_N1, "exp75_N1_616p20_top1"),
        ("N=2 central", 2, PLUGS_N2, "exp75_N2_308p20_top2"),
    )
    print(f"Fleet nameplate target: {FLEET_KW:.0f} kW (20 kW plugs only)")
    print(f"  N=1: {PLUGS_N1} plugs × {CHARGER_KW:.0f} kW @ top_demand_cells(1)")
    print(f"  N=2: {PLUGS_N2} plugs × {CHARGER_KW:.0f} kW × 2 @ top_demand_cells(2)")
    print(f"Central N=2 cells (reference): {e63.top_demand_cells(2)}")
    print(f"Top-1 cell: {e63.top_demand_cells(1)}")

    t0 = time.perf_counter()
    rows: list[dict] = []
    for label, n_sites, plugs, bar_desc in tqdm(arms, desc="exp75_arms", unit="run", ncols=100):
        out = e63.run_continuous_experiment(
            n_sites,
            NUM_DAYS,
            demand_scale=SCALE,
            fleet_size=FLEET,
            plugs_per_site=plugs,
            charger_kw=CHARGER_KW,
            show_trip_progress=True,
            trip_bar_desc=bar_desc,
        )
        m = out["metrics"]
        daily = out["daily"]
        sp = [x["served_pct"] for x in daily]
        trips = sum(x["arrivals"] for x in daily)
        site_kw = plugs * CHARGER_KW
        fleet_kw = n_sites * site_kw
        rows.append(
            {
                "label": label,
                "n_sites": n_sites,
                "plugs_per_site": plugs,
                "site_kw": site_kw,
                "fleet_kw": fleet_kw,
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
    print("\n" + "=" * 118)
    print(
        f"Exp75: demand_scale={SCALE}, fleet={FLEET}, {NUM_DAYS}d | "
        f"matched {FLEET_KW:.0f} kW fleet (20 kW plugs) | {len(rows)} runs | wall {wall_s:.1f}s ({wall_s/60:.2f} min)"
    )
    hdr = (
        f"{'arm':<14} {'N':>3} {'plugs/site':>11} {'site_kW':>9} {'fleet_kW':>9} {'trips':>8} "
        f"{'served%':>8} {'p90_w':>7} {'chgU%':>7} {'fleetSOC%':>9}  d1,d2,d3"
    )
    print(hdr)
    print("-" * len(hdr))
    for r in rows:
        sd = ", ".join(f"{x:.1f}" for x in r["served_pct_d1_d3"])
        print(
            f"{r['label']:<14} {r['n_sites']:3d} {r['plugs_per_site']:11d} {r['site_kw']:9.0f} {r['fleet_kw']:9.0f} "
            f"{r['trips']:8d} {r['served_pct']:8.2f} {r['p90_wait']:7.2f} {r['chg_util']:7.1f} "
            f"{r['fleet_battery_pct']:9.2f}  {sd}"
        )
        print(f"{'':14} depots: {r['depots']}")


if __name__ == "__main__":
    main()
