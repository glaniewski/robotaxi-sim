"""
Battery floor sweep for Section 4 of the blog post.

Question: how low can battery capacity go before SLA degrades?
Tests two charging networks side-by-side:
  - N=77 distributed microsites (10 plugs × 11.5 kW)
  - N=2 mega-depots (308 plugs × 20 kW)

Existing data already covers:
  - N=77 @ 75 kWh → charger_slow_N77_10p11kW (95.8% SLA)
  - N=77 @ 40 kWh → battery_small_N77_10p11kW_bat40 (95.2% SLA)
  - N=2  @ 75 kWh → geo_N2_308p20kW (93.9% SLA)

New runs:
  N=77: 30, 20, 15 kWh  × seeds [7, 42]   = 6 runs
  N=2:  40, 30, 20 kWh  × seeds [7, 42]   = 6 runs
  Total: 12 runs ≈ 2.5 h

Output: data/blog_battery_floor/<label>_seed<N>.json
        data/blog_battery_floor/index.json

Idempotent — skips files that already exist.

Run from repo root:
    PYTHONHASHSEED=0 python3 scripts/run_blog_battery_floor.py
"""
from __future__ import annotations

import json
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

# -----------------------------------------------------------------------
# (label, n_sites, plugs_per_site, charger_kw, fleet_size, battery_kwh)
# -----------------------------------------------------------------------
CONFIGS: tuple[tuple[str, int, int, float, int, float], ...] = (
    # N=77 distributed — 10 plugs × 11.5 kW
    # (75kWh and 40kWh already exist in blog_anchor_replicates)
    ("bat77_30kWh",  77, 10, 11.5, 4500, 30.0),
    ("bat77_20kWh",  77, 10, 11.5, 4500, 20.0),
    ("bat77_15kWh",  77, 10, 11.5, 4500, 15.0),
    # N=2 mega-depots — 308 plugs × 20 kW (matches geo_N2_308p20kW baseline)
    # (75kWh already exists as geo_N2_308p20kW in blog_anchor_replicates)
    ("bat2_40kWh",    2, 308, 20.0, 4500, 40.0),
    ("bat2_30kWh",    2, 308, 20.0, 4500, 30.0),
    ("bat2_20kWh",    2, 308, 20.0, 4500, 20.0),
    ("bat2_15kWh",    2, 308, 20.0, 4500, 15.0),
)

SEEDS = (7, 42)
NUM_DAYS = 3
DEMAND_SCALE = 0.2
VEHICLE_PRESET = "tesla"
REPOSITION_ALPHA = 0.6
CHARGING_QUEUE_POLICY = "jit"

OUT_DIR = ROOT / "data" / "blog_battery_floor"
SCRIPT_NAME = Path(__file__).name


def _run_cell(label: str, n_sites: int, plugs: int, ckw: float,
              fleet: int, bat: float, seed: int, out_path: Path) -> dict:
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
        vehicle_preset=VEHICLE_PRESET,
        reposition_alpha=REPOSITION_ALPHA,
        show_trip_progress=True,
        trip_bar_desc=f"{label}_s{seed}",
    )
    wall_s = time.perf_counter() - t0

    record = {
        "label": label,
        "n_sites": n_sites,
        "plugs_per_site": plugs,
        "charger_kw": ckw,
        "fleet_size": fleet,
        "battery_kwh": bat,
        "seed": seed,
        "metrics": out["metrics"],
        "metadata": {
            "wall_time_s": round(wall_s, 2),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "script": SCRIPT_NAME,
        },
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(record, default=str, indent=2), encoding="utf-8")
    return record


def main(group: str | None = None) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    configs = CONFIGS
    if group == "n77":
        configs = tuple(c for c in CONFIGS if c[0].startswith("bat77"))
    elif group == "n2":
        configs = tuple(c for c in CONFIGS if c[0].startswith("bat2"))

    work = [
        (cfg, seed)
        for cfg in configs
        for seed in SEEDS
        if not (OUT_DIR / f"{cfg[0]}_seed{seed}.json").exists()
    ]

    print(f"Battery floor sweep [{group or 'all'}]: {len(work)} runs to go "
          f"(skipping {len(configs)*len(SEEDS)-len(work)} already done)")

    results = []
    for cfg, seed in tqdm(work, unit="run", desc="battery_floor"):
        label, n_sites, plugs, ckw, fleet, bat = cfg
        out_path = OUT_DIR / f"{label}_seed{seed}.json"
        print(f"\n→ {label} seed={seed}  bat={bat}kWh  N={n_sites}  {plugs}p×{ckw}kW")
        rec = _run_cell(label, n_sites, plugs, ckw, fleet, bat, seed, out_path)
        m = rec["metrics"]
        print(f"  served={m['served_pct']:.1f}%  sla={m['sla_adherence_pct']:.1f}%  "
              f"p90={m['p90_wait_min']:.1f}min  fleet_soc={m['fleet_battery_pct']:.1f}%  "
              f"wall={rec['metadata']['wall_time_s']:.0f}s")
        results.append(rec)

    # Rebuild full index from all files on disk
    all_records = []
    for cfg in CONFIGS:
        label = cfg[0]
        for seed in SEEDS:
            p = OUT_DIR / f"{label}_seed{seed}.json"
            if p.exists():
                all_records.append(json.loads(p.read_text()))

    index = {
        "generated": datetime.now(timezone.utc).isoformat(),
        "script": SCRIPT_NAME,
        "configs": [
            {
                "label": cfg[0],
                "n_sites": cfg[1],
                "plugs_per_site": cfg[2],
                "charger_kw": cfg[3],
                "fleet_size": cfg[4],
                "battery_kwh": cfg[5],
                "seeds": [
                    r["seed"] for r in all_records if r["label"] == cfg[0]
                ],
                **_aggregate(
                    [r["metrics"] for r in all_records if r["label"] == cfg[0]]
                ),
            }
            for cfg in CONFIGS
        ],
    }
    (OUT_DIR / "index.json").write_text(
        json.dumps(index, default=str, indent=2), encoding="utf-8"
    )
    print(f"\nIndex written → {OUT_DIR / 'index.json'}")


def _aggregate(metrics_list: list[dict]) -> dict:
    if not metrics_list:
        return {}
    keys = [
        "served_pct", "sla_adherence_pct", "p90_wait_min",
        "median_wait_min", "cost_per_trip", "fleet_battery_pct",
        "charger_utilization_pct", "utilization_pct", "deadhead_pct",
    ]
    out = {}
    for k in keys:
        vals = [m[k] for m in metrics_list if m.get(k) is not None]
        if vals:
            out[k] = {
                "mean": round(sum(vals) / len(vals), 4),
                "min": round(min(vals), 4),
                "max": round(max(vals), 4),
                "values": [round(v, 4) for v in vals],
            }
    return out


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--group", choices=["n77", "n2"], default=None,
                    help="Run only n77 or n2 configs (for parallel execution)")
    args = ap.parse_args()
    main(group=args.group)
