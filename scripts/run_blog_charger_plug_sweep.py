"""
Blog — charger iso-power plug-count sweep (Insight 3).

Fixes total installed power at ~8.9 MW across 77 sites and varies only
how that capacity is split between plug count and per-plug speed.

4 new configs × 3 seeds = 12 runs.  The two configs that already exist in
blog_anchor_replicates (2p×57.5kW and 10p×11.5kW) are NOT re-run here;
extract_blog_data.py stitches them in from that directory automatically.

New configs (N=77, fleet=4500, bat=75kWh, Tesla, alpha=0.6, jit, scale=0.2, 3d):
  charger_iso_1p115kW  : 1p  × 115.0  kW  (77 plugs,   8.86 MW)
  charger_iso_4p29kW   : 4p  ×  28.75 kW  (308 plugs,  8.86 MW)
  charger_iso_7p16kW   : 7p  ×  16.5  kW  (539 plugs,  8.89 MW)
  charger_iso_20p6kW   : 20p ×   5.75 kW  (1540 plugs, 8.86 MW)

Idempotent: re-running skips cells whose JSON already exists.

Run: PYTHONHASHSEED=0 python3.11 scripts/run_blog_charger_plug_sweep.py
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
# Sweep config spec
# Each tuple: (label, plugs_per_site, charger_kw)
# All share: n_sites=77, fleet=4500, bat=75kWh, Tesla, alpha=0.6, jit
# ------------------------------------------------------------------
SWEEP_CONFIGS: tuple[tuple[str, int, float], ...] = (
    ("charger_iso_1p115kW",  1,  115.0),
    ("charger_iso_4p29kW",   4,   28.75),
    ("charger_iso_7p16kW",   7,   16.5),
    ("charger_iso_20p6kW",  20,    5.75),
)

N_SITES = 77
FLEET_SIZE = 4500
BATTERY_KWH = 75.0
VEHICLE_PRESET = "tesla"
REPOSITION_ALPHA = 0.6
CHARGING_QUEUE_POLICY = "jit"
NUM_DAYS = 3
DEMAND_SCALE = 0.2
SEEDS: tuple[int, ...] = (7, 42, 123)

OUT_DIR = ROOT / "data" / "blog_charger_plug_sweep"
SCRIPT_NAME = Path(__file__).name


def _git_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=str(ROOT), stderr=subprocess.DEVNULL
        ).decode().strip()
    except Exception:
        return "unknown"


def _run_cell(label: str, plugs: int, ckw: float, seed: int, out_path: Path) -> dict:
    e63.SEED = seed
    t0 = time.perf_counter()
    out = e63.run_continuous_experiment(
        N_SITES,
        NUM_DAYS,
        demand_scale=DEMAND_SCALE,
        fleet_size=FLEET_SIZE,
        plugs_per_site=plugs,
        charger_kw=ckw,
        battery_kwh=BATTERY_KWH,
        charging_queue_policy=CHARGING_QUEUE_POLICY,
        vehicle_preset=VEHICLE_PRESET,
        reposition_alpha=REPOSITION_ALPHA,
        show_trip_progress=True,
        trip_bar_desc=f"{label}_seed{seed}",
    )
    wall_s = time.perf_counter() - t0

    record = {
        "anchor_label": label,
        "block": "charger_plug_sweep",
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
            "sweep_axis": {
                "label": label,
                "n_sites": N_SITES,
                "plugs_per_site": plugs,
                "charger_kw": ckw,
                "fleet_size": FLEET_SIZE,
                "battery_kwh": BATTERY_KWH,
                "vehicle_preset": VEHICLE_PRESET,
                "reposition_alpha": REPOSITION_ALPHA,
                "seed": seed,
            },
        },
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(record, default=str, indent=2), encoding="utf-8")

    m = out["metrics"]
    return {
        "label": label,
        "plugs_per_site": plugs,
        "charger_kw": ckw,
        "seed": seed,
        "wall_time_s": round(wall_s, 1),
        "served_pct": m["served_pct"],
        "sla_adherence_pct": m["sla_adherence_pct"],
        "p90_wait_min": m["p90_wait_min"],
        "cost_per_trip": m["cost_per_trip"],
        "charger_utilization_pct": m["charger_utilization_pct"],
        "fleet_battery_pct": m["fleet_battery_pct"],
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Charger iso-power plug-count sweep.")
    ap.add_argument("--configs", type=str, default=None,
                    help="Comma-separated config labels to run (default: all 4)")
    ap.add_argument("--seeds", type=str, default=None,
                    help=f"Comma-separated seeds (default: {SEEDS})")
    ap.add_argument("--force", action="store_true",
                    help="Rerun cells whose JSON already exists")
    args = ap.parse_args()

    active_labels = None
    if args.configs:
        active_labels = set(x.strip() for x in args.configs.split(",") if x.strip())
    active_seeds = SEEDS if not args.seeds else tuple(int(x) for x in args.seeds.split(","))

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    cells: list[tuple] = []
    skipped = 0
    for label, plugs, ckw in SWEEP_CONFIGS:
        if active_labels is not None and label not in active_labels:
            continue
        for seed in active_seeds:
            out_path = OUT_DIR / f"{label}_seed{seed}.json"
            if out_path.exists() and not args.force:
                skipped += 1
                continue
            cells.append((label, plugs, ckw, seed, out_path))

    print(f"[charger_plug_sweep] out_dir={OUT_DIR}")
    print(f"[charger_plug_sweep] to_run={len(cells)}  skipped={skipped}  force={args.force}")
    print(f"[charger_plug_sweep] configs: {[c[0] for c in SWEEP_CONFIGS]}")

    t0 = time.perf_counter()
    rows: list[dict] = []
    for label, plugs, ckw, seed, out_path in tqdm(cells, desc="plug-sweep", unit="run", ncols=110):
        row = _run_cell(label, plugs, ckw, seed, out_path)
        rows.append(row)
    total_wall = time.perf_counter() - t0

    print("\n" + "=" * 110)
    print(f"Charger plug sweep | N={N_SITES}, fleet={FLEET_SIZE}, scale={DEMAND_SCALE}, "
          f"{NUM_DAYS}d | wall {total_wall:.1f}s ({total_wall/60:.1f} min)")
    if rows:
        hdr = (f"{'label':<28} {'plugs':>5} {'kW':>7} {'seed':>4} "
               f"{'wall(s)':>7} {'served%':>8} {'p90_w':>6} {'$/trip':>7} {'chgU%':>6}")
        print(hdr)
        print("-" * len(hdr))
        for r in rows:
            print(f"{r['label']:<28} {r['plugs_per_site']:>5d} {r['charger_kw']:>7.2f} "
                  f"{r['seed']:>4d} {r['wall_time_s']:>7.1f} "
                  f"{r['served_pct']:>8.2f} {r['p90_wait_min']:>6.2f} "
                  f"{r['cost_per_trip']:>7.3f} {r['charger_utilization_pct']:>6.1f}")
    print(f"\n[charger_plug_sweep] done — {len(rows)} new cells in {OUT_DIR}")
    print("Next: PYTHONHASHSEED=0 python3.11 scripts/extract_blog_data.py")


if __name__ == "__main__":
    main()
