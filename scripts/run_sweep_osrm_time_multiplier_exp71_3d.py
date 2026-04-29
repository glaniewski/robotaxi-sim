"""
Sweep OSRM_TIME_MULTIPLIER (travel-time inflation) on the Exp71-class 3-day config.

Same harness as Exp71 Central: N=2 ``top_demand_cells(2)``, demand_scale=0.2, fleet 4000,
308×20 kW per depot, FIFO, fastest_balanced, continuous 3-day clock.

Run: PYTHONHASHSEED=0 python3 scripts/run_sweep_osrm_time_multiplier_exp71_3d.py
Extra multipliers merged into JSON: … --mult 2.5,3 --merge-json
"""
from __future__ import annotations

import argparse
import json
import os
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
PLUGS = 308
CHARGER_KW = 20.0

# Systematic congestion / traffic buffer on top of free-flow OSRM times
DEFAULT_MULTIPLIERS: tuple[float, ...] = (1.0, 1.5, 2.0)

JSON_OUT = ROOT / "data" / "sweep_osrm_time_multiplier_exp71_3d.json"


def _parse_mults(s: str | None) -> tuple[float, ...]:
    if not s or not str(s).strip():
        return DEFAULT_MULTIPLIERS
    parts = [p.strip() for p in str(s).split(",") if p.strip()]
    return tuple(float(p) for p in parts)


def main() -> None:
    ap = argparse.ArgumentParser(description="OSRM time multiplier sweep (Exp71-class 3d).")
    ap.add_argument(
        "--mult",
        type=str,
        default=None,
        metavar="LIST",
        help="Comma-separated multipliers (default: 1,1.5,2)",
    )
    ap.add_argument(
        "--merge-json",
        action="store_true",
        help=f"Merge runs into existing {JSON_OUT.name} (replace row if multiplier exists).",
    )
    args = ap.parse_args()
    multipliers = _parse_mults(args.mult)

    central = e63.top_demand_cells(N_DEPOTS)
    print(f"OSRM_TIME_MULTIPLIER sweep | Exp71-class | depots: {central} | mults={multipliers}")
    t0 = time.perf_counter()
    rows: list[dict] = []

    for mult in tqdm(multipliers, desc="osrm_time_mult", unit="run", ncols=100):
        os.environ["OSRM_TIME_MULTIPLIER"] = str(mult)
        out = e63.run_continuous_experiment(
            N_DEPOTS,
            NUM_DAYS,
            demand_scale=SCALE,
            fleet_size=FLEET,
            plugs_per_site=PLUGS,
            charger_kw=CHARGER_KW,
            depot_h3_cells=list(central),
            charging_queue_policy="fifo",
            show_trip_progress=True,
            trip_bar_desc=f"exp71_3d_osrm_x{mult:g}",
        )
        m = out["metrics"]
        daily = out["daily"]
        sp = [x["served_pct"] for x in daily]
        trips = sum(x["arrivals"] for x in daily)
        rows.append(
            {
                "osrm_time_multiplier": mult,
                "trips": trips,
                "served_pct": m["served_pct"],
                "median_wait_min": m["median_wait_min"],
                "p90_wait_min": m["p90_wait_min"],
                "sla_adherence_pct": m["sla_adherence_pct"],
                "repositioning_pct": m["repositioning_pct"],
                "contribution_margin_per_trip": m["contribution_margin_per_trip"],
                "charger_utilization_pct": m["charger_utilization_pct"],
                "fleet_battery_pct": m["fleet_battery_pct"],
                "deadhead_pct": m["deadhead_pct"],
                "served_pct_d1_d3": sp,
                "depots": ",".join(out["depot_h3_cells"]),
            }
        )

    wall_s = time.perf_counter() - t0
    JSON_OUT.parent.mkdir(parents=True, exist_ok=True)

    prev_wall = None
    if args.merge_json and JSON_OUT.exists():
        old = json.loads(JSON_OUT.read_text(encoding="utf-8"))
        prev_wall = old.get("wall_clock_s")
        by_m = {float(r["osrm_time_multiplier"]): r for r in old.get("runs", [])}
        for r in rows:
            by_m[float(r["osrm_time_multiplier"])] = r
        rows = [by_m[k] for k in sorted(by_m.keys())]
        multipliers_list = [float(r["osrm_time_multiplier"]) for r in rows]
    else:
        multipliers_list = [float(r["osrm_time_multiplier"]) for r in rows]

    total_wall = round(wall_s, 1)
    if args.merge_json and prev_wall is not None:
        total_wall = round(float(prev_wall) + wall_s, 1)

    payload = {
        "config": {
            "n_depots": N_DEPOTS,
            "num_days": NUM_DAYS,
            "demand_scale": SCALE,
            "fleet": FLEET,
            "plugs_per_site": PLUGS,
            "charger_kw": CHARGER_KW,
            "charging_queue_policy": "fifo",
            "multipliers": multipliers_list,
        },
        "wall_clock_s": total_wall,
        "runs": rows,
    }
    if args.merge_json and prev_wall is not None:
        payload["baseline_wall_clock_s"] = prev_wall
        payload["supplement_wall_clock_s"] = round(wall_s, 1)

    JSON_OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"\nWrote {JSON_OUT}")

    lbl_w = 8
    print("\n" + "=" * 110)
    print(
        f"OSRM_TIME_MULTIPLIER sweep | scale={SCALE}, fleet={FLEET}, N={N_DEPOTS}, {NUM_DAYS}d | "
        f"308×{CHARGER_KW:.0f} kW | wall {wall_s:.1f}s ({wall_s/60:.2f} min)"
    )
    hdr = (
        f"{'mult':>{lbl_w}} {'trips':>8} {'served%':>8} {'med_w':>7} {'p90_w':>7} "
        f"{'sla%':>7} {'repo%':>7} {'$margin':>9} {'chgU%':>7} {'d1–d3':>22}"
    )
    print(hdr)
    print("-" * len(hdr))
    for r in rows:
        sd = ", ".join(f"{x:.1f}" for x in r["served_pct_d1_d3"])
        mlt = float(r["osrm_time_multiplier"])
        print(
            f"{mlt:>{lbl_w}.1f} {r['trips']:8d} {r['served_pct']:8.2f} "
            f"{r['median_wait_min']:7.2f} {r['p90_wait_min']:7.2f} {r['sla_adherence_pct']:7.1f} "
            f"{r['repositioning_pct']:7.1f} {r['contribution_margin_per_trip']:9.2f} "
            f"{r['charger_utilization_pct']:7.1f}  {sd}"
        )


if __name__ == "__main__":
    main()
