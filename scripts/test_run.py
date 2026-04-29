"""
Quick test run with live streaming progress bar.
Usage: python3 scripts/test_run.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from sim_runner import run_stream

DEMAND_SCALE = 0.03
FLEET = 1000
TOTAL_IN_DATASET = 867_791

print(f"Test run: demand_scale={DEMAND_SCALE}, fleet={FLEET}, 24h day")
print(f"  Expected trips: ~{int(TOTAL_IN_DATASET * DEMAND_SCALE):,}")
print()

m = run_stream(
    label=f"scale={DEMAND_SCALE}  fleet={FLEET}",
    payload={
        "seed": 123,
        "duration_minutes": 1440,
        "demand": {"demand_scale": DEMAND_SCALE},
        "fleet": {"size": FLEET},
        "economics": {"fixed_cost_per_vehicle_day": 0.0},
    },
)

if m:
    print()
    print(f"  served:        {m['served_pct']:.1f}%  ({m['served_count']:,} trips)")
    print(f"  wait p50/p90:  {m['median_wait_min']:.1f} / {m['p90_wait_min']:.1f} min")
    print(f"  utilization:   {m['utilization_pct']:.1f}%")
    print(f"  repositioning: {m['repositioning_pct']:.1f}%")
    print(f"  revenue:       ${m['revenue_total']:,.0f}")
    print(f"  total_margin:  ${m['total_margin']:,.0f}  (no fixed cost)")
    print(f"  cm/trip:       ${m['contribution_margin_per_trip']:.2f}")
