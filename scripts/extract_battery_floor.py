"""
Merge battery-floor sweep results into site/src/content/experiments.json
under the key "battery_floor".

Structure added:
  experiments.battery_floor = {
    "n77": [  # N=77 distributed depots, sorted by battery_kwh
      { label, battery_kwh, n_sites, plugs_per_site, charger_kw, seeds,
        served_pct: {mean,min,max}, sla_adherence_pct, p90_wait_min,
        fleet_battery_pct, charger_utilization_pct, cost_per_trip }
    ],
    "n2": [   # N=2 mega-depots, sorted by battery_kwh
      { ...same fields... }
    ]
  }

Existing anchor data (75kWh baseline for both networks) is pulled from the
blog_anchor_replicates block so the chart has a continuous series.

Run from repo root:
    python3 scripts/extract_battery_floor.py
"""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FLOOR_DIR = ROOT / "data" / "blog_battery_floor"
ANCHOR_DIR = ROOT / "data" / "blog_anchor_replicates"
EXPERIMENTS_JSON = ROOT / "site" / "src" / "content" / "experiments.json"


def _agg(vals: list[float]) -> dict:
    if not vals:
        return {"mean": None, "min": None, "max": None, "values": []}
    return {
        "mean": round(sum(vals) / len(vals), 4),
        "min": round(min(vals), 4),
        "max": round(max(vals), 4),
        "values": [round(v, 4) for v in vals],
    }


METRIC_KEYS = [
    "served_pct", "sla_adherence_pct", "p90_wait_min",
    "median_wait_min", "fleet_battery_pct", "charger_utilization_pct",
    "cost_per_trip", "utilization_pct", "deadhead_pct",
]


def _load_floor_files(label_prefix: str) -> list[dict]:
    """Load all seed files for configs starting with label_prefix."""
    records = []
    if not FLOOR_DIR.exists():
        return records
    for f in sorted(FLOOR_DIR.glob(f"{label_prefix}*_seed*.json")):
        try:
            records.append(json.loads(f.read_text()))
        except Exception as e:
            print(f"  WARN skip {f.name}: {e}")
    return records


def _build_series(label: str, battery_kwh: float, n_sites: int,
                  plugs: int, ckw: float, records: list[dict]) -> dict:
    seeds = [r["seed"] for r in records]
    entry: dict = {
        "label": label,
        "battery_kwh": battery_kwh,
        "n_sites": n_sites,
        "plugs_per_site": plugs,
        "charger_kw": ckw,
        "seeds": seeds,
    }
    for k in METRIC_KEYS:
        vals = [r["metrics"][k] for r in records if r.get("metrics", {}).get(k) is not None]
        entry[k] = _agg(vals)
    return entry


def _baseline_from_anchor(anchor_label: str, battery_kwh: float,
                           n_sites: int, plugs: int, ckw: float) -> dict | None:
    """Pull existing anchor replicate files for the baseline row."""
    files = sorted(ANCHOR_DIR.glob(f"{anchor_label}_seed*.json"))
    if not files:
        return None
    records = [json.loads(f.read_text()) for f in files]
    seeds = [r["seed"] for r in records]
    entry: dict = {
        "label": anchor_label,
        "battery_kwh": battery_kwh,
        "n_sites": n_sites,
        "plugs_per_site": plugs,
        "charger_kw": ckw,
        "seeds": seeds,
    }
    for k in METRIC_KEYS:
        vals = [r["metrics"][k] for r in records if r.get("metrics", {}).get(k) is not None]
        entry[k] = _agg(vals)
    return entry


def main() -> None:
    exp = json.loads(EXPERIMENTS_JSON.read_text())

    # ---- N=77 series -------------------------------------------------------
    # Baselines from anchor replicates (already have 3 seeds each)
    n77_75 = _baseline_from_anchor(
        "charger_slow_N77_10p11kW", 75.0, 77, 10, 11.5
    )
    n77_40 = _baseline_from_anchor(
        "battery_small_N77_10p11kW_bat40", 40.0, 77, 10, 11.5
    )

    # New sweep files
    n77_series = []
    if n77_75:
        n77_series.append(n77_75)
    if n77_40:
        n77_series.append(n77_40)

    for label, bat in [("bat77_30kWh", 30.0), ("bat77_20kWh", 20.0), ("bat77_15kWh", 15.0)]:
        recs = _load_floor_files(label)
        if recs:
            n77_series.append(_build_series(label, bat, 77, 10, 11.5, recs))
        else:
            print(f"  MISSING: {label}")

    n77_series.sort(key=lambda x: x["battery_kwh"])

    # ---- N=2 series --------------------------------------------------------
    n2_75 = _baseline_from_anchor("geo_N2_308p20kW", 75.0, 2, 308, 20.0)

    n2_series = []
    if n2_75:
        n2_series.append(n2_75)

    for label, bat in [("bat2_40kWh", 40.0), ("bat2_30kWh", 30.0), ("bat2_20kWh", 20.0), ("bat2_15kWh", 15.0)]:
        recs = _load_floor_files(label)
        if recs:
            n2_series.append(_build_series(label, bat, 2, 308, 20.0, recs))
        else:
            print(f"  MISSING: {label}")

    n2_series.sort(key=lambda x: x["battery_kwh"])

    # ---- Merge and save ----------------------------------------------------
    exp["battery_floor"] = {"n77": n77_series, "n2": n2_series}
    EXPERIMENTS_JSON.write_text(json.dumps(exp, indent=2, default=str))
    print(f"Updated {EXPERIMENTS_JSON}")

    print("\n=== N=77 summary ===")
    for row in n77_series:
        sla = row.get("sla_adherence_pct", {}).get("mean")
        soc = row.get("fleet_battery_pct", {}).get("mean")
        seeds = row.get("seeds", [])
        print(f"  {row['battery_kwh']:5.0f} kWh | sla={sla:.1f}% | fleet_soc={soc:.1f}% | seeds={seeds}")

    print("\n=== N=2 summary ===")
    for row in n2_series:
        sla = row.get("sla_adherence_pct", {}).get("mean")
        soc = row.get("fleet_battery_pct", {}).get("mean")
        seeds = row.get("seeds", [])
        print(f"  {row['battery_kwh']:5.0f} kWh | sla={sla:.1f}% | fleet_soc={soc:.1f}% | seeds={seeds}")


if __name__ == "__main__":
    main()
