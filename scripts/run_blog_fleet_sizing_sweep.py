"""
Blog — Insight 1 fleet-sizing sweep.

Sweeps ``fleet_size`` across {500..5500} × seeds {42, 123, 7} at the Insight-1 harness
(Tesla preset, demand_scale=0.2, N=20 depots at 20p×20kW, 3-day continuous,
max_wait_time_seconds=600, charging_queue_policy=jit). Produces the data that powers
the "picking an SLA picks a fleet size" chart on the personal-site blog post.

Each run writes one full-fidelity JSON file containing:
  - config          : full ScenarioConfig used
  - seed            : the seed
  - metrics         : full compute_metrics() dict (all 50+ fields)
  - time_series     : full per-bucket state vector
  - daily           : per-day served% / mean SOC rows
  - metadata        : wall time, timestamp, git SHA, script name

Idempotent: re-running skips cells whose JSON already exists.

Run: PYTHONHASHSEED=0 python3.11 scripts/run_blog_fleet_sizing_sweep.py
     PYTHONHASHSEED=0 python3.11 scripts/run_blog_fleet_sizing_sweep.py --fleets 4500 --seeds 42
"""
from __future__ import annotations

import argparse
import json
import os
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
# Sweep spec (see plan Phase 1, Insight 1)
# ------------------------------------------------------------------
FLEETS: tuple[int, ...] = (500, 1000, 1500, 2000, 2500, 3000, 3500, 4000, 4250, 4500, 4750, 5000, 5500)
SEEDS: tuple[int, ...] = (42, 123, 7)
NUM_DAYS = 3
DEMAND_SCALE = 0.2
N_SITES = 20
PLUGS_PER_SITE = 20
CHARGER_KW = 20.0
BATTERY_KWH = 75.0
VEHICLE_PRESET = "tesla"
CHARGING_QUEUE_POLICY = "jit"

OUT_DIR = ROOT / "data" / "blog_fleet_sweep"
SCRIPT_NAME = Path(__file__).name


def _git_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=str(ROOT), stderr=subprocess.DEVNULL
        ).decode().strip()
    except Exception:
        return "unknown"


def _run_cell(fleet: int, seed: int, out_path: Path) -> dict:
    """Run one (fleet, seed) cell and write its JSON. Returns the summary row."""
    e63.SEED = seed  # monkey-patch the module-level SEED read inside run_continuous_experiment
    t0 = time.perf_counter()
    out = e63.run_continuous_experiment(
        N_SITES,
        NUM_DAYS,
        demand_scale=DEMAND_SCALE,
        fleet_size=fleet,
        plugs_per_site=PLUGS_PER_SITE,
        charger_kw=CHARGER_KW,
        battery_kwh=BATTERY_KWH,
        charging_queue_policy=CHARGING_QUEUE_POLICY,
        vehicle_preset=VEHICLE_PRESET,
        show_trip_progress=True,
        trip_bar_desc=f"fleet{fleet:04d}_seed{seed}",
    )
    wall_s = time.perf_counter() - t0

    record = {
        "config": out["scenario_config"].model_dump(),
        "seed": seed,
        "metrics": out["metrics"],
        "time_series": out["result"]["timeseries"],
        "daily": out["daily"],
        "depot_h3_cells": list(out["depot_h3_cells"]),
        "metadata": {
            "wall_time_s": round(wall_s, 2),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "git_sha": _git_sha(),
            "script": SCRIPT_NAME,
            "sweep_axis": {"fleet": fleet, "seed": seed},
        },
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(record, default=str, indent=2), encoding="utf-8")

    m = out["metrics"]
    return {
        "filename": out_path.name,
        "fleet": fleet,
        "seed": seed,
        "wall_time_s": round(wall_s, 1),
        "served_pct": m["served_pct"],
        "sla_adherence_pct": m["sla_adherence_pct"],
        "p10_wait_min": m["p10_wait_min"],
        "median_wait_min": m["median_wait_min"],
        "p90_wait_min": m["p90_wait_min"],
        "cost_per_trip": m["cost_per_trip"],
        "contribution_margin_per_trip": m["contribution_margin_per_trip"],
        "deadhead_pct": m["deadhead_pct"],
        "utilization_pct": m["utilization_pct"],
        "charger_utilization_pct": m["charger_utilization_pct"],
        "fleet_battery_pct": m["fleet_battery_pct"],
        "daily_served_pct": [d["served_pct"] for d in out["daily"]],
    }


def _rebuild_index() -> None:
    """Scan OUT_DIR and rewrite index.json from whatever runs exist on disk."""
    rows: list[dict] = []
    for p in sorted(OUT_DIR.glob("fleet*_seed*.json")):
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        m = data.get("metrics", {})
        rows.append({
            "filename": p.name,
            "fleet": data.get("metadata", {}).get("sweep_axis", {}).get("fleet"),
            "seed": data.get("seed"),
            "wall_time_s": data.get("metadata", {}).get("wall_time_s"),
            "served_pct": m.get("served_pct"),
            "sla_adherence_pct": m.get("sla_adherence_pct"),
            "p10_wait_min": m.get("p10_wait_min"),
            "median_wait_min": m.get("median_wait_min"),
            "p90_wait_min": m.get("p90_wait_min"),
            "cost_per_trip": m.get("cost_per_trip"),
            "contribution_margin_per_trip": m.get("contribution_margin_per_trip"),
            "deadhead_pct": m.get("deadhead_pct"),
            "utilization_pct": m.get("utilization_pct"),
            "charger_utilization_pct": m.get("charger_utilization_pct"),
            "fleet_battery_pct": m.get("fleet_battery_pct"),
            "daily_served_pct": [d["served_pct"] for d in data.get("daily", [])],
        })
    rows.sort(key=lambda r: (r.get("fleet") or 0, r.get("seed") or 0))

    index = {
        "sweep": "blog_fleet_sweep",
        "fixed": {
            "demand_scale": DEMAND_SCALE,
            "n_sites": N_SITES,
            "plugs_per_site": PLUGS_PER_SITE,
            "charger_kw": CHARGER_KW,
            "battery_kwh": BATTERY_KWH,
            "num_days": NUM_DAYS,
            "vehicle_preset": VEHICLE_PRESET,
            "charging_queue_policy": CHARGING_QUEUE_POLICY,
            "max_wait_time_seconds": 600,
        },
        "sweep_axes": {
            "fleets": list(FLEETS),
            "seeds": list(SEEDS),
        },
        "n_expected": len(FLEETS) * len(SEEDS),
        "n_completed": len(rows),
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "runs": rows,
    }
    (OUT_DIR / "index.json").write_text(json.dumps(index, indent=2), encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser(description="Blog fleet-sizing sweep (Insight 1).")
    ap.add_argument("--fleets", type=str, default=None,
                    help=f"Comma-separated fleet sizes to run (default: all {FLEETS})")
    ap.add_argument("--seeds", type=str, default=None,
                    help=f"Comma-separated seeds to run (default: all {SEEDS})")
    ap.add_argument("--force", action="store_true",
                    help="Rerun cells whose JSON already exists (default: skip)")
    args = ap.parse_args()

    fleets = FLEETS if not args.fleets else tuple(int(x) for x in args.fleets.split(",") if x.strip())
    seeds = SEEDS if not args.seeds else tuple(int(x) for x in args.seeds.split(",") if x.strip())

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    cells = [(f, s) for f in fleets for s in seeds]
    to_run = []
    skipped = 0
    for fleet, seed in cells:
        out_path = OUT_DIR / f"fleet{fleet:04d}_seed{seed}.json"
        if out_path.exists() and not args.force:
            skipped += 1
            continue
        to_run.append((fleet, seed, out_path))

    print(f"[blog_fleet_sweep] out_dir={OUT_DIR}")
    print(f"[blog_fleet_sweep] cells total={len(cells)} to_run={len(to_run)} skipped={skipped} force={args.force}")
    print(f"[blog_fleet_sweep] fixed: scale={DEMAND_SCALE}, N={N_SITES}, "
          f"{PLUGS_PER_SITE}p×{CHARGER_KW:g}kW, {NUM_DAYS}d, preset={VEHICLE_PRESET}, "
          f"queue_policy={CHARGING_QUEUE_POLICY}, battery={BATTERY_KWH:g}")

    t0 = time.perf_counter()
    rows: list[dict] = []
    for fleet, seed, out_path in tqdm(to_run, desc="fleet-sweep", unit="run", ncols=110):
        row = _run_cell(fleet, seed, out_path)
        rows.append(row)
        _rebuild_index()
    total_wall = time.perf_counter() - t0

    _rebuild_index()

    print("\n" + "=" * 120)
    print(f"Blog fleet-sizing sweep | scale={DEMAND_SCALE}, N={N_SITES}, "
          f"{PLUGS_PER_SITE}p×{CHARGER_KW:g}kW, {NUM_DAYS}d | wall {total_wall:.1f}s ({total_wall/60:.1f} min)")

    if rows:
        hdr = (f"{'fleet':>6} {'seed':>4} {'wall(s)':>7} {'served%':>8} {'sla%':>7} "
               f"{'p50_w':>6} {'p90_w':>6} {'$/trip':>7} {'$marg':>7} {'deadhd%':>8} "
               f"{'util%':>6} {'chgU%':>6} {'daily_d1-d3':>22}")
        print(hdr)
        print("-" * len(hdr))
        for r in rows:
            sd = ", ".join(f"{x:.1f}" for x in r["daily_served_pct"])
            print(f"{r['fleet']:>6d} {r['seed']:>4d} {r['wall_time_s']:>7.1f} "
                  f"{r['served_pct']:>8.2f} {r['sla_adherence_pct']:>7.2f} "
                  f"{r['median_wait_min']:>6.2f} {r['p90_wait_min']:>6.2f} "
                  f"{r['cost_per_trip']:>7.2f} {r['contribution_margin_per_trip']:>7.2f} "
                  f"{r['deadhead_pct']:>8.1f} {r['utilization_pct']:>6.1f} "
                  f"{r['charger_utilization_pct']:>6.1f}   {sd}")

    print(f"\n[blog_fleet_sweep] wrote {len(rows)} new rows; index at {OUT_DIR / 'index.json'}")


if __name__ == "__main__":
    main()
