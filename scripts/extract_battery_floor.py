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
EQP_DIR = ROOT / "data" / "blog_battery_floor_equal_power"
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


def _load_files(directory: Path, label_prefix: str) -> list[dict]:
    """Load all seed files for configs starting with label_prefix."""
    records = []
    if not directory.exists():
        return records
    for f in sorted(directory.glob(f"{label_prefix}*_seed*.json")):
        try:
            records.append(json.loads(f.read_text()))
        except Exception as e:
            print(f"  WARN skip {f.name}: {e}")
    return records


def _load_floor_files(label_prefix: str) -> list[dict]:
    return _load_files(FLOOR_DIR, label_prefix)


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

    # ---- Equal-power N=77 series (12.32 MW @ 8p × 20 kW) ------------------
    # Baseline 75 kWh from geo sweep; sweep from new equal-power dir
    n77_eqp_75 = _baseline_from_anchor("geo_N77_8p20kW", 75.0, 77, 8, 20.0)
    n77_eqp_series: list[dict] = []
    if n77_eqp_75:
        n77_eqp_series.append(n77_eqp_75)

    for label, bat in [
        ("eqp_bat77_40kWh", 40.0),
        ("eqp_bat77_30kWh", 30.0),
        ("eqp_bat77_20kWh", 20.0),
        ("eqp_bat77_15kWh", 15.0),
        ("eqp_bat77_10kWh", 10.0),
    ]:
        recs = _load_files(EQP_DIR, label)
        if recs:
            n77_eqp_series.append(_build_series(label, bat, 77, 8, 20.0, recs))
        else:
            print(f"  MISSING (eqp): {label}")
    n77_eqp_series.sort(key=lambda x: x["battery_kwh"])

    # ---- Equal-power N=2 series (12.32 MW @ 308p × 20 kW) -----------------
    # 75-15 kWh already exist in original FLOOR_DIR (same N=2 config); add 10 kWh from EQP_DIR
    n2_eqp_series = list(n2_series)  # reuse — same architecture, already at 12.3 MW
    eqp_bat2_10 = _load_files(EQP_DIR, "eqp_bat2_10kWh")
    if eqp_bat2_10:
        n2_eqp_series.append(_build_series("eqp_bat2_10kWh", 10.0, 2, 308, 20.0, eqp_bat2_10))
    n2_eqp_series.sort(key=lambda x: x["battery_kwh"])

    # ---- Merge and save ----------------------------------------------------
    exp["battery_floor"] = {
        "n77": n77_series,
        "n2": n2_series,
        # Equalized at ~12.3 MW total power across both architectures
        # (N=77 at 8p × 20 kW; N=2 at 308p × 20 kW)
        "n77_equal_power": n77_eqp_series,
        "n2_equal_power": n2_eqp_series,
    }
    EXPERIMENTS_JSON.write_text(json.dumps(exp, indent=2, default=str))
    print(f"Updated {EXPERIMENTS_JSON}")

    def _summary(label: str, series: list[dict]) -> None:
        print(f"\n=== {label} ===")
        for row in series:
            sla = row.get("sla_adherence_pct", {}).get("mean")
            soc = row.get("fleet_battery_pct", {}).get("mean")
            seeds = row.get("seeds", [])
            print(
                f"  {row['battery_kwh']:5.0f} kWh | sla={sla:.2f}% | "
                f"fleet_soc={soc:.1f}% | seeds={seeds}"
            )

    _summary("N=77 (original 8.86 MW, 10p × 11.5 kW)", n77_series)
    _summary("N=2  (original 12.3 MW, 308p × 20 kW)", n2_series)
    _summary("N=77 equalized (12.3 MW, 8p × 20 kW)", n77_eqp_series)
    _summary("N=2  equalized (12.3 MW, 308p × 20 kW + new 10 kWh)", n2_eqp_series)


if __name__ == "__main__":
    main()
