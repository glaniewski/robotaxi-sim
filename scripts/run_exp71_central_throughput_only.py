"""Exp71 Central arm only: dump metrics + event_counts + state_time to JSON (FIFO or JIT)."""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "backend"))
sys.path.insert(0, str(ROOT / "scripts"))

import run_exp63_continuous_multiday_steady_state as e63  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser(description="Exp71 Central throughput JSON (FIFO vs JIT)")
    ap.add_argument(
        "--charging-queue-policy",
        choices=("fifo", "jit"),
        default="fifo",
        help="SimConfig charging_queue_policy (default fifo, same as Exp63 continuous harness)",
    )
    args = ap.parse_args()
    policy = args.charging_queue_policy

    t0 = time.perf_counter()
    cells = e63.top_demand_cells(2)
    out = e63.run_continuous_experiment(
        2,
        3,
        demand_scale=0.2,
        fleet_size=4000,
        plugs_per_site=308,
        charger_kw=20.0,
        depot_h3_cells=list(cells),
        charging_queue_policy=policy,
        show_trip_progress=True,
        trip_bar_desc=f"exp71_central_throughput_{policy}",
    )
    res = out["result"]
    wall = time.perf_counter() - t0
    report = {
        "charging_queue_policy": policy,
        "wall_clock_s": round(wall, 1),
        "depot_h3_cells": out["depot_h3_cells"],
        "metrics": res["metrics"],
        "event_counts": res.get("event_counts"),
        "state_time_s": res.get("state_time_s"),
        "daily": out["daily"],
    }
    if policy == "fifo":
        outp = ROOT / "data" / "exp71_central_throughput_report.json"
    else:
        outp = ROOT / "data" / f"exp71_central_throughput_report_{policy}.json"
    outp.parent.mkdir(parents=True, exist_ok=True)
    outp.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"Wrote {outp} (policy={policy}, wall {wall:.1f}s)")


if __name__ == "__main__":
    main()
