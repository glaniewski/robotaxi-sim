"""
Experiment 70 — N=2 centralized depots: plug count vs per-plug kW at matched 3080 kW/depot.

Total fleet charging nameplate: 2 × 3080 = 6160 kW (same as 77 microsites × 4p × 20 kW).
Uses default SimConfig battery_kwh (75). Compare axis to **Experiment 69** (Exp68 ladder at
battery_kwh=40): `run_exp69_scale02_repeat_exp68_battery40.py`.

Run: PYTHONHASHSEED=0 python3 scripts/run_exp70_n2_depot_plug_kw_sweep.py
      PYTHONHASHSEED=0 python3 scripts/run_exp70_n2_depot_plug_kw_sweep.py --limit 2
      PYTHONHASHSEED=0 python3 scripts/run_exp70_n2_depot_plug_kw_sweep.py --limit 2 --min-plug 5
      PYTHONHASHSEED=0 python3 scripts/run_exp70_n2_depot_plug_kw_sweep.py --configs 28:110,10:308 --min-plug 5
      PYTHONHASHSEED=0 python3 scripts/run_exp70_n2_depot_plug_kw_sweep.py --configs 154:20,308:20 --depot-cells 88489e3461fffff,88489e35e1fffff

`--configs` takes comma-separated `plugs:kw` pairs; overrides `--limit`.
Optional **`--depot-cells`** (comma-separated H3 res-8, length **N**) overrides default ``top_demand_cells(N)`` depot placement.

Power-tier extension (seven rows, merge into RESULTS Exp70): `run_exp70_n2_power_tier_extension.py`.
"""
from __future__ import annotations

import argparse
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

# plugs × charger_kw = 3080 per depot
CONFIGS: tuple[tuple[str, int, float], ...] = (
    ("154p×20 kW (site 3080)", 154, 20.0),
    ("77p×40 kW (site 3080)", 77, 40.0),
    ("40p×77 kW (site 3080)", 40, 77.0),
    ("28p×110 kW (site 3080)", 28, 110.0),
)


def _parse_configs(spec: str) -> list[tuple[str, int, float]]:
    """``plugs:kw,plugs:kw`` → labeled rows (3080 kW per depot)."""
    out: list[tuple[str, int, float]] = []
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        if ":" not in part:
            raise ValueError(f"bad config token {part!r} (want plugs:kw)")
        a, b = part.split(":", 1)
        plugs, ckw = int(a.strip()), float(b.strip())
        site = int(plugs * ckw)
        kw_s = int(ckw) if ckw == int(ckw) else ckw
        if plugs == 10 and abs(ckw - 308.0) < 1e-6:
            label = "10p×308 kW (1× fleet 6160)"
        elif plugs == 154 and abs(ckw - 20.0) < 1e-6:
            label = "154p×20 kW"
        elif plugs == 308 and abs(ckw - 20.0) < 1e-6:
            label = "308p×20 kW (2× fleet 12,320)"
        else:
            label = f"{plugs}p×{kw_s} kW (site {site})"
        out.append((label, plugs, ckw))
    if not out:
        raise ValueError("empty --configs")
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Exp70 N=2 plug×kW sweep (3080 kW/depot).")
    ap.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="run only the first N configs (e.g. 2 for a quick wall-time check)",
    )
    ap.add_argument(
        "--min-plug",
        type=float,
        default=None,
        dest="min_plug",
        help="min_plug_duration_minutes for SimConfig (default: Exp63 module MIN_PLUG, usually 10)",
    )
    ap.add_argument(
        "--configs",
        type=str,
        default=None,
        metavar="PLUGS:KW,...",
        help='run only these pairs, e.g. "28:110,10:308" (comma-separated plugs:kw); ignores --limit',
    )
    ap.add_argument(
        "--depot-cells",
        type=str,
        default=None,
        metavar="H3,H3",
        help="override depot H3 cells (comma-separated, must be N_DEPOTS entries)",
    )
    args = ap.parse_args()
    if args.configs:
        configs = _parse_configs(args.configs)
    elif args.limit is None:
        configs = list(CONFIGS)
    else:
        configs = list(CONFIGS[: max(0, args.limit)])

    depot_list: list[str] | None = None
    if args.depot_cells:
        depot_list = [x.strip() for x in args.depot_cells.split(",") if x.strip()]
        if len(depot_list) != N_DEPOTS:
            raise SystemExit(f"--depot-cells must have exactly {N_DEPOTS} H3 ids (got {len(depot_list)})")

    t0 = time.perf_counter()
    rows: list[dict] = []
    for label, plugs, ckw in tqdm(configs, desc="config", unit="run", ncols=100):
        site_kw = float(plugs * ckw)
        mp = args.min_plug
        trip_lbl = f"exp70_s{SCALE}_f{FLEET}_N2_{plugs}p_{int(ckw)}kW"
        if mp is not None:
            trip_lbl += f"_minplug{int(mp) if mp == int(mp) else mp}"
        if depot_list:
            trip_lbl += "_proxybest"
        out = e63.run_continuous_experiment(
            N_DEPOTS,
            NUM_DAYS,
            demand_scale=SCALE,
            fleet_size=FLEET,
            plugs_per_site=plugs,
            charger_kw=ckw,
            min_plug_duration_minutes=mp,
            depot_h3_cells=depot_list,
            show_trip_progress=True,
            trip_bar_desc=trip_lbl,
        )
        m = out["metrics"]
        daily = out["daily"]
        sp = [x["served_pct"] for x in daily]
        trips = sum(x["arrivals"] for x in daily)
        rows.append(
            {
                "label": label,
                "n_sites": N_DEPOTS,
                "plugs": plugs,
                "charger_kw": ckw,
                "site_kw": site_kw,
                "fleet_kw": 2.0 * site_kw,
                "trips": trips,
                "served_pct": m["served_pct"],
                "median_wait": m["median_wait_min"],
                "p90_wait": m["p90_wait_min"],
                "sla_adherence_pct": m["sla_adherence_pct"],
                "repositioning_pct": m["repositioning_pct"],
                "chg_util": m["charger_utilization_pct"],
                "fleet_battery_pct": m["fleet_battery_pct"],
                "contrib_margin_per_trip": m["contribution_margin_per_trip"],
                "served_pct_d1_d3": sp,
            }
        )

    wall_s = time.perf_counter() - t0
    lbl_w = max(36, max(len(r["label"]) for r in rows) + 2)
    print("\n" + "=" * 110)
    mp_note = f"min_plug={args.min_plug}m" if args.min_plug is not None else f"min_plug=default({e63.MIN_PLUG}m)"
    dep_note = f"depot_cells={','.join(depot_list)}" if depot_list else "depots=top_demand_cells(2)"
    print(
        f"Exp70: demand_scale={SCALE}, fleet={FLEET}, N={N_DEPOTS} depots, {NUM_DAYS}d continuous | "
        f"{len(rows)} config(s) | {mp_note} | {dep_note} | wall {wall_s:.1f}s ({wall_s/60:.2f} min) | "
        "ref microsites Exp67/68 N=77 4p×20: served~76.2%, chgU~93.1%"
    )
    if args.configs or args.min_plug is not None:
        hdr = (
            f"{'config':<{lbl_w}} {'site_kW':>8} {'fleet_kW':>9} {'trips':>8} {'served%':>8} {'med_w':>7} "
            f"{'p90_w':>7} {'sla%':>6} {'repo%':>6} {'chgU%':>7} {'fleetSOC%':>9} {'cm$/tr':>8}  d1,d2,d3"
        )
        print(hdr)
        print("-" * len(hdr))
        for r in rows:
            sd = ", ".join(f"{x:.1f}" for x in r["served_pct_d1_d3"])
            print(
                f"{r['label']:<{lbl_w}} {r['site_kw']:8.0f} {r['fleet_kw']:9.0f} {r['trips']:8d} "
                f"{r['served_pct']:8.2f} {r['median_wait']:7.2f} {r['p90_wait']:7.2f} "
                f"{r['sla_adherence_pct']:6.1f} {r['repositioning_pct']:6.1f} {r['chg_util']:7.1f} "
                f"{r['fleet_battery_pct']:9.2f} {r['contrib_margin_per_trip']:8.4f}  {sd}"
            )
    else:
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
