"""
Extract blog-post chart data from the three sweep folders into a single JSON
consumed by Recharts components on the personal-site blog.

Inputs (read-only):
  - data/blog_fleet_sweep/*.json          (13 fleets × 3 seeds)
  - data/blog_anchor_replicates/*.json    (blog anchors × 3 seeds)
  - data/blog_pareto/*.json               (10 corners × 2 presets)
  - data/blog_battery_floor/*.json        (battery floor sweep, N=77 + N=2)
  - data/blog_charger_plug_sweep/*.json   (iso-power plug count sweep)
  - data/sweep_osrm_time_multiplier_exp71_3d.json  (congestion tax, single seed)

Output:
  - site/src/content/experiments.json

Pure file I/O — no sim calls. Safe to re-run any time.

Run: python3.11 scripts/extract_blog_data.py
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
FLEET_DIR = ROOT / "data" / "blog_fleet_sweep"
ANCHOR_DIR = ROOT / "data" / "blog_anchor_replicates"
PARETO_DIR = ROOT / "data" / "blog_pareto"
CHARGER_PLUG_DIR = ROOT / "data" / "blog_charger_plug_sweep"
BATTERY_FLOOR_DIR = ROOT / "data" / "blog_battery_floor"
CONGESTION_JSON = ROOT / "data" / "sweep_osrm_time_multiplier_exp71_3d.json"

OUT_PATH = ROOT / "site" / "src" / "content" / "experiments.json"

# Metrics to carry through for each sweep
FLEET_METRICS: tuple[str, ...] = (
    "served_pct", "sla_adherence_pct", "p10_wait_min", "median_wait_min",
    "p90_wait_min", "cost_per_trip", "contribution_margin_per_trip",
    "utilization_pct", "active_time_pct", "deadhead_pct", "charger_utilization_pct",
    "fleet_battery_pct", "trips_per_vehicle_per_day",
)
ANCHOR_METRICS = FLEET_METRICS
PARETO_METRICS: tuple[str, ...] = (
    "served_pct", "sla_adherence_pct", "p10_wait_min", "median_wait_min",
    "p90_wait_min", "cost_per_trip", "cost_per_mile",
    "contribution_margin_per_trip", "system_margin_per_trip",
    "avg_revenue_per_trip", "utilization_pct", "deadhead_pct",
    "charger_utilization_pct", "fleet_battery_pct",
)


def _load_json(p: Path) -> dict | None:
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def _agg(vals: list[float]) -> dict[str, Any]:
    if not vals:
        return {"mean": None, "min": None, "max": None, "values": []}
    return {
        "mean": round(mean(vals), 4),
        "min": round(min(vals), 4),
        "max": round(max(vals), 4),
        "values": [round(float(v), 4) for v in vals],
    }


def _aggregate_by(cells: list[dict], key_fn, metrics: tuple[str, ...]) -> list[dict]:
    """Group ``cells`` by ``key_fn`` and aggregate each ``metric`` across the group."""
    groups: dict[Any, list[dict]] = {}
    order: list[Any] = []
    for c in cells:
        k = key_fn(c)
        if k not in groups:
            order.append(k)
            groups[k] = []
        groups[k].append(c)
    out: list[dict] = []
    for k in order:
        g = groups[k]
        row: dict[str, Any] = {"_key": k, "seeds": sorted({c["seed"] for c in g})}
        for m in metrics:
            vals = [float(c["metrics"][m]) for c in g if c["metrics"].get(m) is not None]
            row[m] = _agg(vals)
        out.append(row)
    return out


def _load_fleet_sweep() -> list[dict]:
    cells: list[dict] = []
    for p in sorted(FLEET_DIR.glob("fleet*_seed*.json")):
        d = _load_json(p)
        if not d:
            continue
        cells.append({
            "filename": p.name,
            "fleet": d["metadata"]["sweep_axis"]["fleet"],
            "seed": d["seed"],
            "metrics": d["metrics"],
        })
    return cells


def _load_anchors() -> list[dict]:
    cells: list[dict] = []
    for p in sorted(ANCHOR_DIR.glob("*.json")):
        if p.name == "index.json":
            continue
        d = _load_json(p)
        if not d:
            continue
        ax = d["metadata"]["sweep_axis"]
        cells.append({
            "filename": p.name,
            "label": d["anchor_label"],
            "block": d.get("block"),
            "seed": d["seed"],
            "n_sites": ax["n_sites"],
            "plugs_per_site": ax["plugs_per_site"],
            "charger_kw": ax["charger_kw"],
            "fleet_size": ax["fleet_size"],
            "battery_kwh": ax["battery_kwh"],
            "metrics": d["metrics"],
        })
    return cells


def _load_pareto() -> list[dict]:
    cells: list[dict] = []
    for p in sorted(PARETO_DIR.glob("*.json")):
        if p.name == "index.json":
            continue
        d = _load_json(p)
        if not d:
            continue
        ax = d["metadata"]["sweep_axis"]
        cells.append({
            "filename": p.name,
            "label": d["corner_label"],
            "preset": d["preset"],
            "n_sites": ax["n_sites"],
            "plugs_per_site": ax["plugs_per_site"],
            "charger_kw": ax["charger_kw"],
            "fleet_size": ax["fleet_size"],
            "battery_kwh": ax["battery_kwh"],
            "seed": d.get("seed"),
            "metrics": d["metrics"],
        })
    return cells


_BATTERY_METRIC_KEYS: tuple[str, ...] = (
    "served_pct", "sla_adherence_pct", "p90_wait_min", "median_wait_min",
    "fleet_battery_pct", "charger_utilization_pct", "cost_per_trip",
    "utilization_pct", "deadhead_pct",
)


def _bat_agg(records: list[dict], key: str) -> dict[str, Any]:
    vals = [float(r["metrics"][key]) for r in records if r.get("metrics", {}).get(key) is not None]
    return _agg(vals)


def _bat_entry_from_files(label: str, battery_kwh: float, n_sites: int,
                           plugs: int, ckw: float, files: list[Path]) -> dict[str, Any]:
    records = []
    for f in files:
        try:
            records.append(json.loads(f.read_text(encoding="utf-8")))
        except Exception:
            pass
    entry: dict[str, Any] = {
        "label": label,
        "battery_kwh": battery_kwh,
        "n_sites": n_sites,
        "plugs_per_site": plugs,
        "charger_kw": ckw,
        "seeds": [r["seed"] for r in records],
    }
    for k in _BATTERY_METRIC_KEYS:
        entry[k] = _bat_agg(records, k)
    return entry


def _load_battery_floor() -> dict[str, list[dict[str, Any]]]:
    """Load battery floor sweep (N=77 and N=2 series) from blog_battery_floor/
    plus baseline 75 kWh rows from blog_anchor_replicates/."""
    n77_series: list[dict[str, Any]] = []
    n2_series: list[dict[str, Any]] = []

    # 75 kWh baselines come from existing anchor replicates
    n77_75_files = sorted(ANCHOR_DIR.glob("charger_slow_N77_10p11kW_seed*.json"))
    if n77_75_files:
        n77_series.append(_bat_entry_from_files(
            "charger_slow_N77_10p11kW", 75.0, 77, 10, 11.5, n77_75_files))

    n2_75_files = sorted(ANCHOR_DIR.glob("geo_N2_308p20kW_seed*.json"))
    if n2_75_files:
        n2_series.append(_bat_entry_from_files(
            "geo_N2_308p20kW", 75.0, 2, 308, 20.0, n2_75_files))

    if BATTERY_FLOOR_DIR.exists():
        # N=77 sweep points
        for label, bat in [
            ("battery_small_N77_10p11kW_bat40", 40.0),
            ("bat77_30kWh", 30.0),
            ("bat77_20kWh", 20.0),
            ("bat77_15kWh", 15.0),
        ]:
            files = sorted(BATTERY_FLOOR_DIR.glob(f"{label}_seed*.json"))
            # bat40 may live in anchor replicates instead
            if not files:
                files = sorted(ANCHOR_DIR.glob(f"{label}_seed*.json"))
            if files:
                n77_series.append(_bat_entry_from_files(label, bat, 77, 10, 11.5, files))

        # N=2 sweep points
        for label, bat in [
            ("bat2_40kWh", 40.0),
            ("bat2_30kWh", 30.0),
            ("bat2_20kWh", 20.0),
            ("bat2_15kWh", 15.0),
        ]:
            files = sorted(BATTERY_FLOOR_DIR.glob(f"{label}_seed*.json"))
            if files:
                n2_series.append(_bat_entry_from_files(label, bat, 2, 308, 20.0, files))

    n77_series.sort(key=lambda x: x["battery_kwh"])
    n2_series.sort(key=lambda x: x["battery_kwh"])
    return {"n77": n77_series, "n2": n2_series}


def _load_charger_plug_sweep() -> list[dict]:
    """Load iso-power plug-count sweep from blog_charger_plug_sweep/ plus the
    two matching anchors that already exist in blog_anchor_replicates/."""
    ANCHOR_LABELS_TO_INCLUDE = {
        "charger_slow_N77_10p11kW",
        "charger_matched_N77_2p57kW",
    }
    cells: list[dict] = []
    # new sweep dir
    if CHARGER_PLUG_DIR.exists():
        for p in sorted(CHARGER_PLUG_DIR.glob("*.json")):
            d = _load_json(p)
            if not d:
                continue
            ax = d["metadata"]["sweep_axis"]
            cells.append({
                "label": d["anchor_label"],
                "seed": d["seed"],
                "plugs_per_site": ax["plugs_per_site"],
                "charger_kw": ax["charger_kw"],
                "metrics": d["metrics"],
            })
    # stitch in existing anchor replicates for 2p×57.5kW and 10p×11.5kW
    for p in sorted(ANCHOR_DIR.glob("*.json")):
        if p.name == "index.json":
            continue
        d = _load_json(p)
        if not d:
            continue
        label = d.get("anchor_label") or ""
        if label not in ANCHOR_LABELS_TO_INCLUDE:
            continue
        ax = d.get("metadata", {}).get("sweep_axis", {})
        cells.append({
            "label": label,
            "seed": d["seed"],
            "plugs_per_site": ax.get("plugs_per_site"),
            "charger_kw": ax.get("charger_kw"),
            "metrics": d["metrics"],
        })
    return cells


def _load_congestion() -> dict:
    if not CONGESTION_JSON.exists():
        return {"points": []}
    d = _load_json(CONGESTION_JSON) or {}
    points = [
        {
            "osrm_time_multiplier": float(r["osrm_time_multiplier"]),
            "trips": r.get("trips"),
            "served_pct": r["served_pct"],
            "sla_adherence_pct": r["sla_adherence_pct"],
            "median_wait_min": r["median_wait_min"],
            "p90_wait_min": r["p90_wait_min"],
            "contribution_margin_per_trip": r["contribution_margin_per_trip"],
            "charger_utilization_pct": r["charger_utilization_pct"],
            "deadhead_pct": r["deadhead_pct"],
            "repositioning_pct": r["repositioning_pct"],
            "fleet_battery_pct": r["fleet_battery_pct"],
            "served_pct_d1_d3": r.get("served_pct_d1_d3", []),
        }
        for r in d.get("runs", [])
    ]
    return {
        "config": d.get("config", {}),
        "points": points,
    }


def main() -> None:
    fleet_cells = _load_fleet_sweep()
    anchor_cells = _load_anchors()
    pareto_cells = _load_pareto()
    congestion = _load_congestion()
    plug_sweep_cells = _load_charger_plug_sweep()
    battery_floor = _load_battery_floor()

    # -------- Fleet sweep: aggregate across seeds per fleet --------
    fleet_by = _aggregate_by(fleet_cells, key_fn=lambda c: c["fleet"], metrics=FLEET_METRICS)
    fleet_points = []
    for row in sorted(fleet_by, key=lambda r: r["_key"]):
        fleet_points.append({
            "fleet": row["_key"],
            "seeds": row["seeds"],
            **{m: row[m] for m in FLEET_METRICS},
        })

    # -------- Geographic block: filter anchors with block=="geographic" --------
    geo_cells = [c for c in anchor_cells if c["block"] == "geographic"]
    geo_by = _aggregate_by(
        geo_cells,
        key_fn=lambda c: (c["n_sites"], c["label"]),
        metrics=ANCHOR_METRICS,
    )
    geo_points = []
    for row in sorted(geo_by, key=lambda r: r["_key"][0]):
        n_sites, label = row["_key"]
        # pick a representative cell for config metadata
        rep = next(c for c in geo_cells if c["n_sites"] == n_sites and c["label"] == label)
        geo_points.append({
            "n_sites": n_sites,
            "label": label,
            "plugs_per_site": rep["plugs_per_site"],
            "charger_kw": rep["charger_kw"],
            "fleet_size": rep["fleet_size"],
            "battery_kwh": rep["battery_kwh"],
            "seeds": row["seeds"],
            **{m: row[m] for m in ANCHOR_METRICS},
        })

    # -------- Charger tier block --------
    clean_charger_labels = {
        "charger_slow_N77_10p11kW",
        "charger_matched_N77_2p57kW",
    }
    charger_cells = [
        c for c in anchor_cells
        if c["block"] == "charger" and c["label"] in clean_charger_labels
    ]
    charger_by = _aggregate_by(
        charger_cells,
        key_fn=lambda c: c["label"],
        metrics=ANCHOR_METRICS,
    )
    charger_anchors = []
    for row in sorted(charger_by, key=lambda r: r["_key"]):
        label = row["_key"]
        rep = next(c for c in charger_cells if c["label"] == label)
        charger_anchors.append({
            "label": label,
            "n_sites": rep["n_sites"],
            "plugs_per_site": rep["plugs_per_site"],
            "charger_kw": rep["charger_kw"],
            "fleet_size": rep["fleet_size"],
            "battery_kwh": rep["battery_kwh"],
            "seeds": row["seeds"],
            **{m: row[m] for m in ANCHOR_METRICS},
        })

    # -------- Battery block: small (bat=40) vs 75 (reuse charger_slow anchor) --------
    bat_cells = [c for c in anchor_cells if c["block"] == "battery"] + [
        c for c in anchor_cells
        if c["label"] == "charger_slow_N77_10p11kW"
    ]
    bat_by = _aggregate_by(
        bat_cells,
        key_fn=lambda c: c["label"],
        metrics=ANCHOR_METRICS,
    )
    battery_anchors = []
    for row in sorted(bat_by, key=lambda r: r["_key"]):
        label = row["_key"]
        rep = next(c for c in bat_cells if c["label"] == label)
        battery_anchors.append({
            "label": label,
            "n_sites": rep["n_sites"],
            "plugs_per_site": rep["plugs_per_site"],
            "charger_kw": rep["charger_kw"],
            "fleet_size": rep["fleet_size"],
            "battery_kwh": rep["battery_kwh"],
            "seeds": row["seeds"],
            **{m: row[m] for m in ANCHOR_METRICS},
        })

    # -------- Pareto: single seed, carry per-run metrics --------
    pareto_points = []
    for c in sorted(pareto_cells, key=lambda x: (x["preset"], x["label"])):
        pareto_points.append({
            "label": c["label"],
            "preset": c["preset"],
            "n_sites": c["n_sites"],
            "plugs_per_site": c["plugs_per_site"],
            "charger_kw": c["charger_kw"],
            "fleet_size": c["fleet_size"],
            "battery_kwh": c["battery_kwh"],
            "seed": c["seed"],
            **{m: float(c["metrics"][m]) if c["metrics"].get(m) is not None else None for m in PARETO_METRICS},
        })

    # Sort Pareto by cost_per_trip ascending per preset for chart convenience
    pareto_points.sort(key=lambda r: (r["preset"], r.get("cost_per_trip") or 0.0))

    # -------- Charger plug sweep: iso-power curve --------
    PLUG_METRICS: tuple[str, ...] = (
        "served_pct", "sla_adherence_pct", "p90_wait_min", "cost_per_trip",
        "charger_utilization_pct", "fleet_battery_pct", "deadhead_pct",
    )
    plug_by = _aggregate_by(
        plug_sweep_cells,
        key_fn=lambda c: c["plugs_per_site"],
        metrics=PLUG_METRICS,
    )
    # Sort by ascending plug count for line chart
    plug_sweep_points = []
    for row in sorted(plug_by, key=lambda r: r["_key"]):
        plugs = row["_key"]
        rep = next(c for c in plug_sweep_cells if c["plugs_per_site"] == plugs)
        plug_sweep_points.append({
            "plugs_per_site": plugs,
            "charger_kw": rep["charger_kw"],
            "total_plugs": 77 * plugs,
            "total_mw": round(77 * plugs * rep["charger_kw"] / 1000, 3),
            "seeds": row["seeds"],
            **{m: row[m] for m in PLUG_METRICS},
        })

    out = {
        "meta": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "fleet_sweep": {
                "n_cells": len(fleet_cells),
                "fleets": sorted({c["fleet"] for c in fleet_cells}),
                "seeds": sorted({c["seed"] for c in fleet_cells}),
            },
            "anchors": {
                "n_cells": len(anchor_cells),
                "labels": sorted({c["label"] for c in anchor_cells}),
                "seeds": sorted({c["seed"] for c in anchor_cells}),
            },
            "pareto": {
                "n_cells": len(pareto_cells),
                "presets": sorted({c["preset"] for c in pareto_cells}),
            },
        },
        "fleet_sweep": {"points": fleet_points},
        "geographic": {"points": geo_points},
        "charger": {"anchors": charger_anchors, "plug_sweep": plug_sweep_points},
        "battery": {"anchors": battery_anchors},
        "battery_floor": battery_floor,
        "pareto": {"points": pareto_points},
        "congestion": congestion,
    }

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(out, indent=2), encoding="utf-8")

    print(f"Wrote {OUT_PATH}")
    print(f"  fleet_sweep: {len(fleet_points)} fleet points "
          f"(expected 13 when complete; have {len(fleet_cells)}/39 cells)")
    print(f"  geographic : {len(geo_points)} N values "
          f"(expected 4; have {len(geo_cells)}/12 cells)")
    print(f"  charger    : {len(charger_anchors)} anchors "
          f"(expected 2; have {len(charger_cells)}/6 cells)")
    print(f"  plug_sweep : {len(plug_sweep_points)} plug configs "
          f"(expected 6 when complete; have {len(plug_sweep_cells)} cells)")
    print(f"  batt_floor : n77={len(battery_floor['n77'])} pts, "
          f"n2={len(battery_floor['n2'])} pts (expected 5 each when complete)")
    print(f"  battery    : {len(battery_anchors)} anchors "
          f"(expected 2; have {sum(1 for c in bat_cells)} /9 cells incl. reuse)")
    print(f"  pareto     : {len(pareto_points)} corners "
          f"(expected 20; have {len(pareto_cells)}/20 cells)")
    print(f"  congestion : {len(congestion['points'])} multipliers")


if __name__ == "__main__":
    main()
