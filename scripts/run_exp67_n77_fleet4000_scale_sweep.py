"""
Experiment 67 — N=77, fleet=4000, 3-day continuous: demand_scale sweep (with charging).

Extends the Exp67 probe (scale 0.1): same infra class as Exp63/66 unless --plugs overrides.
Use fixed plugs to stress charging; or scale --plugs with scale to hold util ~flat (heuristic).

Run: PYTHONHASHSEED=0 python3 scripts/run_exp67_n77_fleet4000_scale_sweep.py
      PYTHONHASHSEED=0 python3 scripts/run_exp67_n77_fleet4000_scale_sweep.py --plugs 8
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

N_SITES = 77
NUM_DAYS = 3
FLEET = 4000
# 0.8 omitted by default (long runtime); add via --scales 0.1,0.2,0.4,0.8
DEFAULT_SCALES = (0.2, 0.4)


def main() -> None:
    ap = argparse.ArgumentParser(description="Exp67: fleet 4000, multiscale continuous 3d.")
    ap.add_argument(
        "--scales",
        type=str,
        default=None,
        help=f"comma-separated demand_scale values (default {'/'.join(str(s) for s in DEFAULT_SCALES)})",
    )
    ap.add_argument(
        "--plugs",
        type=int,
        default=None,
        help="chargers per site for all runs (default: exp63 module PLUGS=4)",
    )
    args = ap.parse_args()
    if args.scales:
        scales = tuple(float(x.strip()) for x in args.scales.split(",") if x.strip())
    else:
        scales = DEFAULT_SCALES

    rows: list[dict] = []
    pp = args.plugs
    for scale in tqdm(scales, desc="demand_scale", unit="scale", ncols=100):
        out = e63.run_continuous_experiment(
            N_SITES,
            NUM_DAYS,
            demand_scale=scale,
            fleet_size=FLEET,
            plugs_per_site=pp,
            show_trip_progress=True,
            trip_bar_desc=f"exp67_N{N_SITES}_f{FLEET}_d{NUM_DAYS}_s{scale}"
            + (f"_p{pp}" if pp is not None else ""),
        )
        m = out["metrics"]
        daily = out["daily"]
        sp = [x["served_pct"] for x in daily]
        trips = sum(x["arrivals"] for x in daily)
        rows.append(
            {
                "scale": scale,
                "trips": trips,
                "served_pct": m["served_pct"],
                "p50_wait": m.get("median_wait_min"),
                "p90_wait": m["p90_wait_min"],
                "chg_util": m["charger_utilization_pct"],
                "fleet_battery_pct": m["fleet_battery_pct"],
                "repositioning_pct": m.get("repositioning_pct"),
                "sla_adherence_pct": m.get("sla_adherence_pct"),
                "served_pct_d1_d3": sp,
            }
        )

    plugs_s = pp if pp is not None else e63.PLUGS
    print("\n" + "=" * 100)
    print(
        f"Exp67: N={N_SITES}, fleet={FLEET}, {NUM_DAYS}×{e63.DAY_MINUTES}m continuous, "
        f"{plugs_s}p×{e63.KW}kW/site, demand_scale ∈ {scales}"
    )
    hdr = (
        f"{'scale':>6} {'trips':>8} {'served%':>8} {'p50_w':>7} {'p90_w':>7} "
        f"{'chgU%':>7} {'fleetSOC%':>9} {'repo%':>7} {'sla%':>7}  served% by day"
    )
    print(hdr)
    print("-" * len(hdr))
    for r in rows:
        trips = r["trips"]
        trips_s = f"{trips:8d}" if trips is not None else f"{'n/a':>8}"
        p50 = r["p50_wait"]
        p50_s = f"{p50:7.2f}" if p50 is not None else f"{'n/a':>7}"
        repo = r["repositioning_pct"]
        repo_s = f"{repo:7.2f}" if repo is not None else f"{'n/a':>7}"
        sla = r["sla_adherence_pct"]
        sla_s = f"{sla:7.2f}" if sla is not None else f"{'n/a':>7}"
        sd = r["served_pct_d1_d3"]
        sd_s = ", ".join(f"{x:.2f}" for x in sd)
        print(
            f"{r['scale']:6.2f} {trips_s} {r['served_pct']:8.2f} {p50_s} {r['p90_wait']:7.2f} "
            f"{r['chg_util']:7.1f} {r['fleet_battery_pct']:9.2f} {repo_s} {sla_s}  {sd_s}"
        )


if __name__ == "__main__":
    main()
