"""
Blog — Pareto frontier sweep (cost-per-trip vs service level).

Runs 10 carefully selected {n_sites, plugs_per_site, charger_kw, fleet_size,
battery_kwh} "corners" under both Tesla and Waymo presets = 20 runs total.
Used for the cost-vs-service scatter that closes Insight 3/4 in the blog post.

All runs share: demand_scale=0.2, 3-day continuous, jit queue, seed=42,
max_wait_time_seconds=600, reposition_alpha=0.6. Depots at top_demand_cells(n).

Idempotent: re-running skips corners whose JSON already exists.

Run: PYTHONHASHSEED=0 python3.11 scripts/run_blog_pareto_sweep.py
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from tqdm import tqdm

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(ROOT / "backend"))
sys.path.insert(0, str(SCRIPT_DIR))

import run_exp63_continuous_multiday_steady_state as e63  # noqa: E402

# ------------------------------------------------------------------
# 10 corners (label, n_sites, plugs, charger_kw, fleet, battery)
# Spans: cheap-dispersed  →  fast-expensive-mega
# ------------------------------------------------------------------
CORNERS: tuple[tuple[str, int, int, float, int, float], ...] = (
    ("mega_N2_308p20kW_f4500_b75",          2,   308, 20.0,  4500, 75.0),
    ("midgeo_N20_31p20kW_f4500_b75",        20,   31, 20.0,  4500, 75.0),
    ("disp_slow_N77_10p11kW_f4500_b75",     77,   10, 11.5,  4500, 75.0),
    ("disp_med_N77_10p20kW_f4500_b75",      77,   10, 20.0,  4500, 75.0),
    ("disp_cheap_N77_8p11kW_f4500_b40",     77,    8, 11.5,  4500, 40.0),
    ("mid_fast_N50_10p75kW_f5000_b75",      50,   10, 75.0,  5000, 75.0),
    ("agg_fast_N20_10p150kW_f5000_b75",     20,   10, 150.0, 5000, 75.0),
    ("oversup_slow_N77_10p11kW_f5500_b75",  77,   10, 11.5,  5500, 75.0),
    ("undersup_N20_20p20kW_f3000_b75",      20,   20, 20.0,  3000, 75.0),
    ("ultracheap_N77_5p11kW_f4500_b75",     77,    5, 11.5,  4500, 75.0),
)
PRESETS: tuple[str, ...] = ("tesla", "waymo")
SEED = 42
NUM_DAYS = 3
DEMAND_SCALE = 0.2
CHARGING_QUEUE_POLICY = "jit"

OUT_DIR = ROOT / "data" / "blog_pareto"
SCRIPT_NAME = Path(__file__).name


def _git_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=str(ROOT), stderr=subprocess.DEVNULL
        ).decode().strip()
    except Exception:
        return "unknown"


def _run_cell(corner: tuple, preset: str, out_path: Path) -> dict:
    label, n_sites, plugs, ckw, fleet, bat = corner
    e63.SEED = SEED
    t0 = time.perf_counter()
    out = e63.run_continuous_experiment(
        n_sites,
        NUM_DAYS,
        demand_scale=DEMAND_SCALE,
        fleet_size=fleet,
        plugs_per_site=plugs,
        charger_kw=ckw,
        battery_kwh=bat,
        charging_queue_policy=CHARGING_QUEUE_POLICY,
        vehicle_preset=preset,
        reposition_alpha=0.6,
        show_trip_progress=True,
        trip_bar_desc=f"{preset}_{label}",
    )
    wall_s = time.perf_counter() - t0

    record = {
        "corner_label": label,
        "preset": preset,
        "config": out["scenario_config"].model_dump(),
        "seed": SEED,
        "metrics": out["metrics"],
        "time_series": out["result"]["timeseries"],
        "daily": out["daily"],
        "depot_h3_cells": list(out["depot_h3_cells"]),
        "metadata": {
            "wall_time_s": round(wall_s, 2),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "git_sha": _git_sha(),
            "script": SCRIPT_NAME,
            "sweep_axis": {
                "label": label,
                "preset": preset,
                "n_sites": n_sites,
                "plugs_per_site": plugs,
                "charger_kw": ckw,
                "fleet_size": fleet,
                "battery_kwh": bat,
            },
        },
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(record, default=str, indent=2), encoding="utf-8")

    m = out["metrics"]
    return {
        "filename": out_path.name,
        "label": label,
        "preset": preset,
        "n_sites": n_sites,
        "plugs_per_site": plugs,
        "charger_kw": ckw,
        "fleet_size": fleet,
        "battery_kwh": bat,
        "wall_time_s": round(wall_s, 1),
        "served_pct": m["served_pct"],
        "sla_adherence_pct": m["sla_adherence_pct"],
        "p10_wait_min": m["p10_wait_min"],
        "median_wait_min": m["median_wait_min"],
        "p90_wait_min": m["p90_wait_min"],
        "cost_per_trip": m["cost_per_trip"],
        "cost_per_mile": m["cost_per_mile"],
        "contribution_margin_per_trip": m["contribution_margin_per_trip"],
        "system_margin_per_trip": m["system_margin_per_trip"],
        "avg_revenue_per_trip": m["avg_revenue_per_trip"],
        "deadhead_pct": m["deadhead_pct"],
        "utilization_pct": m["utilization_pct"],
        "charger_utilization_pct": m["charger_utilization_pct"],
        "fleet_battery_pct": m["fleet_battery_pct"],
        "daily_served_pct": [d["served_pct"] for d in out["daily"]],
    }


def _rebuild_index() -> None:
    rows: list[dict] = []
    for p in sorted(OUT_DIR.glob("*.json")):
        if p.name == "index.json":
            continue
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        m = data.get("metrics", {})
        ax = data.get("metadata", {}).get("sweep_axis", {})
        rows.append({
            "filename": p.name,
            "label": data.get("corner_label") or ax.get("label"),
            "preset": data.get("preset") or ax.get("preset"),
            "n_sites": ax.get("n_sites"),
            "plugs_per_site": ax.get("plugs_per_site"),
            "charger_kw": ax.get("charger_kw"),
            "fleet_size": ax.get("fleet_size"),
            "battery_kwh": ax.get("battery_kwh"),
            "wall_time_s": data.get("metadata", {}).get("wall_time_s"),
            "served_pct": m.get("served_pct"),
            "sla_adherence_pct": m.get("sla_adherence_pct"),
            "p10_wait_min": m.get("p10_wait_min"),
            "median_wait_min": m.get("median_wait_min"),
            "p90_wait_min": m.get("p90_wait_min"),
            "cost_per_trip": m.get("cost_per_trip"),
            "cost_per_mile": m.get("cost_per_mile"),
            "contribution_margin_per_trip": m.get("contribution_margin_per_trip"),
            "system_margin_per_trip": m.get("system_margin_per_trip"),
            "avg_revenue_per_trip": m.get("avg_revenue_per_trip"),
            "deadhead_pct": m.get("deadhead_pct"),
            "utilization_pct": m.get("utilization_pct"),
            "charger_utilization_pct": m.get("charger_utilization_pct"),
            "fleet_battery_pct": m.get("fleet_battery_pct"),
            "daily_served_pct": [d["served_pct"] for d in data.get("daily", [])],
        })
    rows.sort(key=lambda r: (r.get("preset") or "", r.get("label") or ""))

    index = {
        "sweep": "blog_pareto",
        "fixed": {
            "demand_scale": DEMAND_SCALE,
            "num_days": NUM_DAYS,
            "seed": SEED,
            "charging_queue_policy": CHARGING_QUEUE_POLICY,
            "max_wait_time_seconds": 600,
        },
        "corners": [
            {
                "label": c[0], "n_sites": c[1], "plugs_per_site": c[2],
                "charger_kw": c[3], "fleet_size": c[4], "battery_kwh": c[5],
            }
            for c in CORNERS
        ],
        "presets": list(PRESETS),
        "n_expected": len(CORNERS) * len(PRESETS),
        "n_completed": len(rows),
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "runs": rows,
    }
    (OUT_DIR / "index.json").write_text(json.dumps(index, indent=2), encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser(description="Blog Pareto sweep (cost vs service).")
    ap.add_argument("--corners", type=str, default=None,
                    help="Comma-separated corner labels to run (default: all)")
    ap.add_argument("--presets", type=str, default=None,
                    help=f"Comma-separated presets (default: {PRESETS})")
    ap.add_argument("--force", action="store_true",
                    help="Rerun cells whose JSON already exists")
    args = ap.parse_args()

    active_corners = None
    if args.corners:
        active_corners = set(x.strip() for x in args.corners.split(",") if x.strip())
    active_presets = PRESETS if not args.presets else tuple(
        x.strip().lower() for x in args.presets.split(",") if x.strip()
    )

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    cells: list[tuple] = []
    skipped = 0
    for corner in CORNERS:
        if active_corners is not None and corner[0] not in active_corners:
            continue
        for preset in active_presets:
            out_path = OUT_DIR / f"{preset}_{corner[0]}.json"
            if out_path.exists() and not args.force:
                skipped += 1
                continue
            cells.append((corner, preset, out_path))

    print(f"[blog_pareto] out_dir={OUT_DIR}")
    print(f"[blog_pareto] to_run={len(cells)} skipped={skipped} force={args.force}")

    t0 = time.perf_counter()
    rows: list[dict] = []
    for corner, preset, out_path in tqdm(cells, desc="pareto", unit="run", ncols=110):
        row = _run_cell(corner, preset, out_path)
        rows.append(row)
        _rebuild_index()
    total_wall = time.perf_counter() - t0

    _rebuild_index()

    print("\n" + "=" * 120)
    print(f"Blog Pareto sweep | scale={DEMAND_SCALE}, {NUM_DAYS}d, seed={SEED} | "
          f"wall {total_wall:.1f}s ({total_wall/60:.1f} min)")

    if rows:
        hdr = (f"{'preset':<7} {'corner':<36} {'served%':>8} {'sla%':>7} "
               f"{'p50_w':>6} {'p90_w':>6} {'$/trip':>7} {'$marg':>7} {'chgU%':>6} {'wall(s)':>7}")
        print(hdr)
        print("-" * len(hdr))
        for r in sorted(rows, key=lambda x: (x["preset"], x["cost_per_trip"])):
            print(f"{r['preset']:<7} {r['label']:<36} {r['served_pct']:>8.2f} "
                  f"{r['sla_adherence_pct']:>7.2f} {r['median_wait_min']:>6.2f} "
                  f"{r['p90_wait_min']:>6.2f} {r['cost_per_trip']:>7.2f} "
                  f"{r['contribution_margin_per_trip']:>7.2f} "
                  f"{r['charger_utilization_pct']:>6.1f} {r['wall_time_s']:>7.1f}")

    print(f"\n[blog_pareto] wrote {len(rows)} new rows; index at {OUT_DIR / 'index.json'}")


if __name__ == "__main__":
    main()
