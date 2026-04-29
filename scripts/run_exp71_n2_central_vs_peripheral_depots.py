"""
Experiment 71 — N=2 depots, 308p×20 kW/site (12,320 kW fleet): central vs peripheral placements.

- **Central:** ``top_demand_cells(2)`` (same as Exp70).
- **Peripheral (E/W):** OSM-industrial east/west pair from ``map_exp70_depots_central_vs_peripheral.py``
  (``88489e3569fffff``, ``88489e341bfffff``).
- **Peripheral #2:** among the **high O/D ∩ industrial** subset (see ``industrial_high_od_cells.py``),
  the **two H3 cells whose centroids are closest** (~1 km apart on current data).

**Default:** runs **only peripheral #2** (one sim). Arms 1–2 metrics are **frozen** from the
logged Exp71 run in ``RESULTS.md`` (same config — deterministic, no need to re-burn ~34 min).

**Full re-run (all three arms):** ``PYTHONHASHSEED=0 python3 scripts/run_exp71_n2_central_vs_peripheral_depots.py --all``
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from tqdm import tqdm

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(ROOT / "backend"))
sys.path.insert(0, str(SCRIPT_DIR))

import run_exp63_continuous_multiday_steady_state as e63  # noqa: E402
from industrial_high_od_cells import closest_centroid_pair_m, load_industrial_high_od_context  # noqa: E402

N_DEPOTS = 2
NUM_DAYS = 3
FLEET = 4000
SCALE = 0.2
PLUGS = 308
CHARGER_KW = 20.0

# OSM-industrial peripheral pair (see map_exp70_depots_central_vs_peripheral.py)
PERIPHERAL_DEPOTS = ("88489e3569fffff", "88489e341bfffff")

# Frozen from RESULTS.md Experiment 71 (2026-04-09); same workload as Exp72 central → same trip count.
_EXP71_BASELINE_TRIPS = 520_674
EXP71_BASELINE_ARMS: tuple[dict, ...] = (
    {
        "label": "Central (top 2 origin cells)",
        "depots": "88489e3467fffff,88489e3463fffff",
        "fleet_kw": 2 * PLUGS * CHARGER_KW,
        "trips": _EXP71_BASELINE_TRIPS,
        "served_pct": 92.75,
        "p90_wait": 7.55,
        "chg_util": 63.0,
        "fleet_battery_pct": 76.81,
        "served_pct_d1_d3": [92.3, 93.0, 92.9],
        "_frozen": True,
    },
    {
        "label": "Peripheral (OSM-industrial E/W)",
        "depots": "88489e3569fffff,88489e341bfffff",
        "fleet_kw": 2 * PLUGS * CHARGER_KW,
        "trips": _EXP71_BASELINE_TRIPS,
        "served_pct": 64.36,
        "p90_wait": 8.92,
        "chg_util": 40.7,
        "fleet_battery_pct": 75.06,
        "served_pct_d1_d3": [67.6, 62.8, 62.7],
        "_frozen": True,
    },
)


def _run_one_arm(label: str, depot_cells: list[str], slug: str) -> dict:
    out = e63.run_continuous_experiment(
        N_DEPOTS,
        NUM_DAYS,
        demand_scale=SCALE,
        fleet_size=FLEET,
        plugs_per_site=PLUGS,
        charger_kw=CHARGER_KW,
        depot_h3_cells=depot_cells,
        show_trip_progress=True,
        trip_bar_desc=f"exp71_N2_308p20_{slug}",
    )
    m = out["metrics"]
    daily = out["daily"]
    sp = [x["served_pct"] for x in daily]
    trips = sum(x["arrivals"] for x in daily)
    return {
        "label": label,
        "depots": ",".join(out["depot_h3_cells"]),
        "site_kw": PLUGS * CHARGER_KW,
        "fleet_kw": 2 * PLUGS * CHARGER_KW,
        "trips": trips,
        "served_pct": m["served_pct"],
        "p90_wait": m["p90_wait_min"],
        "chg_util": m["charger_utilization_pct"],
        "fleet_battery_pct": m["fleet_battery_pct"],
        "served_pct_d1_d3": sp,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Exp71 N=2 depot placement sweeps")
    ap.add_argument(
        "--all",
        action="store_true",
        help="Run all three arms (long). Default: peripheral #2 only + frozen baselines for arms 1–2.",
    )
    args = ap.parse_args()

    central = e63.top_demand_cells(N_DEPOTS)
    ctx = load_industrial_high_od_context()
    (c1, c2), sep_m = closest_centroid_pair_m(ctx.high_industrial)
    peripheral2 = [c1, c2]
    print(
        f"Peripheral #2 depots (closest pair in high O/D ∩ industrial, {sep_m:.0f} m apart): "
        f"{c1}, {c2}"
    )

    rows: list[dict] = []

    if args.all:
        configs: tuple[tuple[str, list[str], str], ...] = (
            ("Central (top 2 origin cells)", list(central), "central"),
            ("Peripheral (OSM-industrial E/W)", list(PERIPHERAL_DEPOTS), "peripheral"),
            ("Peripheral #2 (closest high-industrial pair)", peripheral2, "periph2"),
        )
        for label, depot_cells, slug in tqdm(
            configs,
            desc="exp71_arm",
            unit="run",
            ncols=100,
        ):
            rows.append(_run_one_arm(label, depot_cells, slug))
    else:
        print("(Arms 1–2 from RESULTS.md baseline; use --all to re-sim everything.)")
        rows.extend({k: v for k, v in r.items() if k != "_frozen"} for r in EXP71_BASELINE_ARMS)
        rows.append(
            _run_one_arm(
                "Peripheral #2 (closest high-industrial pair)",
                peripheral2,
                "periph2",
            )
        )

    lbl_w = max(42, max(len(r["label"]) for r in rows) + 2)
    print("\n" + "=" * 118)
    print(
        f"Exp71: demand_scale={SCALE}, fleet={FLEET}, N={N_DEPOTS}, {NUM_DAYS}d | "
        f"{PLUGS}p×{CHARGER_KW:g} kW/site → {PLUGS * CHARGER_KW:g} kW/depot, fleet {2 * PLUGS * CHARGER_KW:g} kW"
    )
    hdr = (
        f"{'arm':<{lbl_w}} {'fleet_kW':>9} {'trips':>8} {'served%':>8} {'p90_w':>7} "
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
