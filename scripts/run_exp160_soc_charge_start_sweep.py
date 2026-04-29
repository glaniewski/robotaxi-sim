"""
Experiment 160 — soc_charge_start sweep on 3-day repeating demand.

Varies the SOC threshold at which vehicles are sent to the depot to recharge,
on the default_scenario-style 200-vehicle / 1-depot / demand_scale=0.02 /
3-day repeating demand harness (see backend/app/scenario_repeat_3d.json).

We hold soc_initial=0.90, soc_min=0.15, soc_target=0.85 constant so only the
charge-trigger threshold moves. This isolates the depot-load-vs-fulfillment
trade-off: low soc_charge_start -> few, long charge cycles -> low depot flow
but longer time-off-road; high soc_charge_start -> many, short top-ups ->
high depot flow but more time-on-road per vehicle.

Run:
    PYTHONHASHSEED=0 python3 scripts/run_exp160_soc_charge_start_sweep.py
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

from tqdm import tqdm

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(ROOT / "backend"))

from app.main import _run_scenario  # noqa: E402
from app.schemas import ScenarioConfig  # noqa: E402

BASE_SCENARIO = ROOT / "backend" / "app" / "scenario_repeat_3d.json"

# soc_charge_start sweep values. 0.35 = "drive till nearly empty";
# 0.80 = "top up after every trip" (original baseline policy).
THRESHOLDS: tuple[float, ...] = (0.35, 0.50, 0.60, 0.70, 0.80)

# Compare JIT replanning (base scenario default) with FIFO queueing so we can
# see whether actually queueing at plugs changes the fulfillment-vs-depot-flow
# trade-off (under JIT, a vehicle that finds all plugs busy returns to service
# and tries later; under FIFO it waits idle at the depot in a queue).
QUEUE_POLICIES: tuple[str, ...] = ("jit", "fifo")

# Fixed SOC policy parameters (held constant across the sweep)
SOC_INITIAL = 0.90
SOC_MIN = 0.15
SOC_TARGET = 0.85

JSON_OUT = ROOT / "data" / "exp160_soc_charge_start_sweep.json"


def _depot_throughput(timeseries: list[dict]) -> dict:
    """Extract depot-wide throughput summary from timeseries depot_snapshots.

    arrivals is cumulative per depot across the sim; we take the final bucket's
    value. Peak concurrent charging and peak queue are rolling maxima. We also
    integrate queue depth to derive a mean (vehicle-minutes-in-queue / duration).
    """
    total_arrivals = 0
    peak_charging = 0
    peak_queue = 0
    minutes_with_queue = 0
    queue_vehicle_minutes = 0.0
    final = timeseries[-1].get("depot_snapshots", {})
    for _dep_id, snap in final.items():
        total_arrivals += int(snap.get("arrivals", 0))
    for bucket in timeseries:
        for snap in bucket.get("depot_snapshots", {}).values():
            peak_charging = max(peak_charging, int(snap.get("charging", 0)))
            q = int(snap.get("queue", 0))
            peak_queue = max(peak_queue, q)
            queue_vehicle_minutes += q
            if q > 0:
                minutes_with_queue += 1
    duration_min = max(1.0, float(timeseries[-1]["t_minutes"] - timeseries[0]["t_minutes"]))
    mean_queue = queue_vehicle_minutes / duration_min
    return {
        "total_arrivals_3d": total_arrivals,
        "arrivals_per_day": round(total_arrivals / 3.0, 1),
        "peak_concurrent_charging": peak_charging,
        "peak_queue": peak_queue,
        "mean_queue": round(mean_queue, 3),
        "minutes_with_queue": minutes_with_queue,
    }


def main() -> None:
    with open(BASE_SCENARIO) as f:
        base_raw = json.load(f)

    print(f"Exp 160 | soc_charge_start × queue_policy sweep | base scenario: {BASE_SCENARIO.name}")
    print(f"  sweeping soc_charge_start = {list(THRESHOLDS)}")
    print(f"  queue policies = {list(QUEUE_POLICIES)}")
    print(f"  fixed: soc_initial={SOC_INITIAL}, soc_min={SOC_MIN}, soc_target={SOC_TARGET}")

    t0 = time.perf_counter()
    rows: list[dict] = []

    combos = [(p, t) for p in QUEUE_POLICIES for t in THRESHOLDS]
    for policy, thr in tqdm(combos, desc="policy,thr", unit="run", ncols=100):
        cfg_dict = json.loads(json.dumps(base_raw))
        cfg_dict["fleet"]["soc_initial"] = SOC_INITIAL
        cfg_dict["fleet"]["soc_min"] = SOC_MIN
        cfg_dict["fleet"]["soc_charge_start"] = thr
        cfg_dict["fleet"]["soc_target"] = SOC_TARGET
        cfg_dict["charging_queue_policy"] = policy
        cfg = ScenarioConfig.model_validate(cfg_dict)

        run_t0 = time.perf_counter()
        out = _run_scenario(cfg)
        run_wall = time.perf_counter() - run_t0

        m = out["metrics"]
        ts = out["timeseries"]
        depot = _depot_throughput(ts)

        rows.append({
            "queue_policy": policy,
            "soc_charge_start": thr,
            "wall_s": round(run_wall, 1),
            "served_pct": m.get("served_pct"),
            "served_count": m.get("served_count"),
            "unserved_count": m.get("unserved_count"),
            "median_wait_min": m.get("median_wait_min"),
            "p90_wait_min": m.get("p90_wait_min"),
            "sla_adherence_pct": m.get("sla_adherence_pct"),
            "repositioning_pct": m.get("repositioning_pct"),
            "charger_utilization_pct": m.get("charger_utilization_pct"),
            "deadhead_pct": m.get("deadhead_pct"),
            "contribution_margin_per_trip": m.get("contribution_margin_per_trip"),
            "cost_per_trip": m.get("cost_per_trip"),
            "fleet_battery_pct": m.get("fleet_battery_pct"),
            "depot_queue_p90_min": m.get("depot_queue_p90_min"),
            **depot,
        })

    wall = time.perf_counter() - t0

    JSON_OUT.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "experiment": 160,
        "config": {
            "base_scenario": BASE_SCENARIO.name,
            "fleet_size": base_raw["fleet"]["size"],
            "demand_scale": base_raw["demand"]["demand_scale"],
            "repeat_num_days": base_raw["demand"]["repeat_num_days"],
            "chargers_count": base_raw["depots"][0]["chargers_count"],
            "charger_kw": base_raw["depots"][0]["charger_kw"],
            "soc_initial": SOC_INITIAL,
            "soc_min": SOC_MIN,
            "soc_target": SOC_TARGET,
            "soc_charge_start_values": list(THRESHOLDS),
            "queue_policies": list(QUEUE_POLICIES),
        },
        "wall_clock_s": round(wall, 1),
        "runs": rows,
    }
    JSON_OUT.write_text(json.dumps(payload, indent=2))
    print(f"\nWrote {JSON_OUT}")

    print("\n" + "=" * 135)
    print(f"Exp 160 soc_charge_start × queue_policy sweep | wall {wall:.1f}s ({wall/60:.2f} min)")
    hdr = (
        f"{'policy':>6} {'thr':>5} {'served%':>8} {'unserv':>7} {'med_w':>6} {'p90_w':>6} "
        f"{'sla%':>6} {'chgU%':>6} {'deadh%':>7} {'$marg':>6} "
        f"| {'arr/day':>8} {'peakChg':>7} {'peakQ':>6} {'meanQ':>6} {'qMin':>5} {'qP90':>6}"
    )
    print(hdr)
    print("-" * len(hdr))
    for r in rows:
        q90 = r.get("depot_queue_p90_min") or 0.0
        print(
            f"{r['queue_policy']:>6} {r['soc_charge_start']:>5.2f} {r['served_pct']:8.2f} {r['unserved_count']:>7} "
            f"{r['median_wait_min']:6.2f} {r['p90_wait_min']:6.2f} "
            f"{r['sla_adherence_pct']:6.2f} {r['charger_utilization_pct']:6.2f} "
            f"{r['deadhead_pct']:7.2f} {r['contribution_margin_per_trip']:6.2f} "
            f"| {r['arrivals_per_day']:>8.0f} {r['peak_concurrent_charging']:>7} "
            f"{r['peak_queue']:>6} {r['mean_queue']:>6.2f} {r['minutes_with_queue']:>5} {q90:>6.2f}"
        )


if __name__ == "__main__":
    main()
