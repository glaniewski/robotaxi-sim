"""
Experiment 72 — N=2 depots, 308p×20 kW: depot sites ranked by **origin** vs **destination** counts.

Uses ``run_continuous_experiment(..., depot_h3_cells=...)`` with cells from
``top_demand_cells``, ``top_destination_cells``. For Austin slice, top-2 by
origin+destination combined usually matches top-2 by destination only; script
skips a redundant third run when pairs are identical.

Run: PYTHONHASHSEED=0 python3 scripts/run_exp72_n2_depot_rank_origin_vs_destination.py
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
PLUGS = 308
CHARGER_KW = 20.0


def main() -> None:
    origin_cells = e63.top_demand_cells(N_DEPOTS)
    dest_cells = e63.top_destination_cells(N_DEPOTS)
    od_cells = e63.top_origin_plus_destination_cells(N_DEPOTS)

    configs: list[tuple[str, list[str]]] = [
        ("Top-2 by origin count", list(origin_cells)),
        ("Top-2 by destination count", list(dest_cells)),
    ]
    if tuple(od_cells) != tuple(dest_cells):
        configs.append(("Top-2 by origin+destination", list(od_cells)))

    print("Depot H3 sets:")
    print(f"  origin:      {origin_cells}")
    print(f"  destination: {dest_cells}")
    print(f"  O+D:         {od_cells}")
    if tuple(od_cells) == tuple(dest_cells):
        print("  (O+D top-2 equals destination top-2 — one arm skipped.)\n")

    rows: list[dict] = []
    for label, depot_cells in tqdm(configs, desc="exp72_arm", unit="run", ncols=100):
        if label.startswith("Top-2 by origin"):
            slug = "orig"
        elif label.startswith("Top-2 by destination"):
            slug = "dest"
        else:
            slug = "od"
        out = e63.run_continuous_experiment(
            N_DEPOTS,
            NUM_DAYS,
            demand_scale=SCALE,
            fleet_size=FLEET,
            plugs_per_site=PLUGS,
            charger_kw=CHARGER_KW,
            depot_h3_cells=depot_cells,
            show_trip_progress=True,
            trip_bar_desc=f"exp72_N2_308p20_{slug}",
        )
        m = out["metrics"]
        daily = out["daily"]
        sp = [x["served_pct"] for x in daily]
        trips = sum(x["arrivals"] for x in daily)
        rows.append(
            {
                "label": label,
                "depots": ",".join(out["depot_h3_cells"]),
                "fleet_kw": 2 * PLUGS * CHARGER_KW,
                "trips": trips,
                "served_pct": m["served_pct"],
                "p90_wait": m["p90_wait_min"],
                "chg_util": m["charger_utilization_pct"],
                "fleet_battery_pct": m["fleet_battery_pct"],
                "served_pct_d1_d3": sp,
            }
        )

    lbl_w = max(36, max(len(r["label"]) for r in rows) + 2)
    print("\n" + "=" * 118)
    print(
        f"Exp72: demand_scale={SCALE}, fleet={FLEET}, N={N_DEPOTS}, {NUM_DAYS}d | "
        f"{PLUGS}p×{CHARGER_KW:g} kW/depot, fleet {2 * PLUGS * CHARGER_KW:g} kW"
    )
    hdr = (
        f"{'ranking':<{lbl_w}} {'fleet_kW':>9} {'trips':>8} {'served%':>8} {'p90_w':>7} "
        f"{'chgU%':>7} {'fleetSOC%':>9}  d1,d2,d3 served%"
    )
    print(hdr)
    print("-" * len(hdr))
    for r in rows:
        sd = ", ".join(f"{x:.1f}" for x in r["served_pct_d1_d3"])
        print(
            f"{r['label']:<{lbl_w}} {r['fleet_kw']:9.0f} {r['trips']:8d} {r['served_pct']:8.2f} "
            f"{r['p90_wait']:7.2f} {r['chg_util']:7.1f} {r['fleet_battery_pct']:9.2f}  {sd}"
        )
        print(f"{'':<{lbl_w}} depots: {r['depots']}")


if __name__ == "__main__":
    main()
