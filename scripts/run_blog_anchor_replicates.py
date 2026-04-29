"""
Blog — anchor replicates across Insights 2, 3, 4.

Runs each blog "anchor" config for seeds {42, 123, 7} so the post can quote a mean
and a min–max range instead of a single point estimate.

Anchors (7 unique cells × 3 seeds = 21 runs):

  Geographic-ceiling block (Insight 2) — equal total installed kW ≈ 12.3 MW:
    - geo_N2_308p20kW    : N=2,  308 p × 20 kW, fleet 4500, bat 75
    - geo_N5_124p20kW    : N=5,  124 p × 20 kW, fleet 4500, bat 75
    - geo_N20_31p20kW    : N=20,  31 p × 20 kW, fleet 4500, bat 75
    - geo_N77_8p20kW     : N=77,   8 p × 20 kW, fleet 4500, bat 75

  Charger-tier block (Insight 3):
    - charger_slow_N77_10p11kW : N=77, 10 p × 11.5 kW, fleet 4500, bat 75
    - charger_fast_N50_10p75kW : N=50, 10 p × 75 kW,   fleet 5000, bat 75
    - charger_matched_N77_2p57kW : N=77, 2 p × 57.5 kW, fleet 4500, bat 75

  Battery block (Insight 4):
    - battery_small_N77_10p11kW_bat40 : N=77, 10 p × 11.5 kW, fleet 4500, bat 40

All shared: demand_scale=0.2, 3-day continuous, Tesla preset, jit queue,
max_wait_time_seconds=600, reposition_alpha=0.6. Depots at top_demand_cells(n).

Idempotent: re-running skips cells whose JSON already exists.

Run: PYTHONHASHSEED=0 python3.11 scripts/run_blog_anchor_replicates.py
     PYTHONHASHSEED=0 python3.11 scripts/run_blog_anchor_replicates.py --anchors geo_N2_308p20kW --seeds 42
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
# Anchor config spec
# Each tuple: (label, n_sites, plugs_per_site, charger_kw,
#              fleet_size, battery_kwh, vehicle_preset, reposition_alpha, block)
# ------------------------------------------------------------------
ANCHORS: tuple[tuple[str, int, int, float, int, float, str, float, str], ...] = (
    ("geo_N2_308p20kW",                  2,   308, 20.0, 4500, 75.0, "tesla", 0.6, "geographic"),
    ("geo_N5_124p20kW",                  5,   124, 20.0, 4500, 75.0, "tesla", 0.6, "geographic"),
    ("geo_N20_31p20kW",                  20,   31, 20.0, 4500, 75.0, "tesla", 0.6, "geographic"),
    ("geo_N77_8p20kW",                   77,    8, 20.0, 4500, 75.0, "tesla", 0.6, "geographic"),
    ("charger_slow_N77_10p11kW",         77,   10, 11.5, 4500, 75.0, "tesla", 0.6, "charger"),
    ("charger_fast_N50_10p75kW",         50,   10, 75.0, 5000, 75.0, "tesla", 0.6, "charger"),
    ("charger_matched_N77_2p57kW",        77,    2, 57.5, 4500, 75.0, "tesla", 0.6, "charger"),
    ("battery_small_N77_10p11kW_bat40",  77,   10, 11.5, 4500, 40.0, "tesla", 0.6, "battery"),
)
SEEDS: tuple[int, ...] = (42, 123, 7)
NUM_DAYS = 3
DEMAND_SCALE = 0.2
CHARGING_QUEUE_POLICY = "jit"

OUT_DIR = ROOT / "data" / "blog_anchor_replicates"
SCRIPT_NAME = Path(__file__).name


def _git_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=str(ROOT), stderr=subprocess.DEVNULL
        ).decode().strip()
    except Exception:
        return "unknown"


def _run_cell(spec: tuple, seed: int, out_path: Path) -> dict:
    label, n_sites, plugs, ckw, fleet, bat, preset, alpha, block = spec
    e63.SEED = seed
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
        reposition_alpha=alpha,
        show_trip_progress=True,
        trip_bar_desc=f"{label}_seed{seed}",
    )
    wall_s = time.perf_counter() - t0

    record = {
        "anchor_label": label,
        "block": block,
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
                "n_sites": n_sites,
                "plugs_per_site": plugs,
                "charger_kw": ckw,
                "fleet_size": fleet,
                "battery_kwh": bat,
                "vehicle_preset": preset,
                "reposition_alpha": alpha,
                "seed": seed,
            },
        },
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(record, default=str, indent=2), encoding="utf-8")

    m = out["metrics"]
    return {
        "filename": out_path.name,
        "label": label,
        "block": block,
        "seed": seed,
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
        "contribution_margin_per_trip": m["contribution_margin_per_trip"],
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
            "label": data.get("anchor_label") or ax.get("label"),
            "block": data.get("block"),
            "seed": data.get("seed"),
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
            "contribution_margin_per_trip": m.get("contribution_margin_per_trip"),
            "deadhead_pct": m.get("deadhead_pct"),
            "utilization_pct": m.get("utilization_pct"),
            "charger_utilization_pct": m.get("charger_utilization_pct"),
            "fleet_battery_pct": m.get("fleet_battery_pct"),
            "daily_served_pct": [d["served_pct"] for d in data.get("daily", [])],
        })
    rows.sort(key=lambda r: (r.get("label") or "", r.get("seed") or 0))

    index = {
        "sweep": "blog_anchor_replicates",
        "fixed": {
            "demand_scale": DEMAND_SCALE,
            "num_days": NUM_DAYS,
            "charging_queue_policy": CHARGING_QUEUE_POLICY,
            "max_wait_time_seconds": 600,
        },
        "anchors": [
            {
                "label": a[0], "n_sites": a[1], "plugs_per_site": a[2], "charger_kw": a[3],
                "fleet_size": a[4], "battery_kwh": a[5], "vehicle_preset": a[6],
                "reposition_alpha": a[7], "block": a[8],
            }
            for a in ANCHORS
        ],
        "seeds": list(SEEDS),
        "n_expected": len(ANCHORS) * len(SEEDS),
        "n_completed": len(rows),
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "runs": rows,
    }
    (OUT_DIR / "index.json").write_text(json.dumps(index, indent=2), encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser(description="Blog anchor replicates (Insights 2/3/4).")
    ap.add_argument("--anchors", type=str, default=None,
                    help="Comma-separated anchor labels to run (default: all 7)")
    ap.add_argument("--seeds", type=str, default=None,
                    help=f"Comma-separated seeds (default: all {SEEDS})")
    ap.add_argument("--force", action="store_true",
                    help="Rerun cells whose JSON already exists")
    args = ap.parse_args()

    active_labels = None
    if args.anchors:
        active_labels = set(x.strip() for x in args.anchors.split(",") if x.strip())

    active_seeds = SEEDS if not args.seeds else tuple(int(x) for x in args.seeds.split(",") if x.strip())

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    cells: list[tuple] = []
    skipped = 0
    for spec in ANCHORS:
        label = spec[0]
        if active_labels is not None and label not in active_labels:
            continue
        for seed in active_seeds:
            out_path = OUT_DIR / f"{label}_seed{seed}.json"
            if out_path.exists() and not args.force:
                skipped += 1
                continue
            cells.append((spec, seed, out_path))

    print(f"[blog_anchor_replicates] out_dir={OUT_DIR}")
    print(f"[blog_anchor_replicates] to_run={len(cells)} skipped={skipped} force={args.force}")

    t0 = time.perf_counter()
    rows: list[dict] = []
    for spec, seed, out_path in tqdm(cells, desc="anchor-reps", unit="run", ncols=110):
        row = _run_cell(spec, seed, out_path)
        rows.append(row)
        _rebuild_index()
    total_wall = time.perf_counter() - t0

    _rebuild_index()

    print("\n" + "=" * 120)
    print(f"Blog anchor replicates | scale={DEMAND_SCALE}, {NUM_DAYS}d | "
          f"wall {total_wall:.1f}s ({total_wall/60:.1f} min)")

    if rows:
        hdr = (f"{'label':<38} {'seed':>4} {'wall(s)':>7} {'served%':>8} {'sla%':>7} "
               f"{'p50_w':>6} {'p90_w':>6} {'$/trip':>7} {'$marg':>7} {'chgU%':>6}")
        print(hdr)
        print("-" * len(hdr))
        for r in rows:
            print(f"{r['label']:<38} {r['seed']:>4d} {r['wall_time_s']:>7.1f} "
                  f"{r['served_pct']:>8.2f} {r['sla_adherence_pct']:>7.2f} "
                  f"{r['median_wait_min']:>6.2f} {r['p90_wait_min']:>6.2f} "
                  f"{r['cost_per_trip']:>7.2f} {r['contribution_margin_per_trip']:>7.2f} "
                  f"{r['charger_utilization_pct']:>6.1f}")

    print(f"\n[blog_anchor_replicates] wrote {len(rows)} new rows; index at {OUT_DIR / 'index.json'}")


if __name__ == "__main__":
    main()
