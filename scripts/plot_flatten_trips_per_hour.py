"""
Load the exact trip sample used when demand_flatten=1.0 (Exp 39) and plot
trip count per hour to verify demand is flat across the day.

Uses same params: scale=0.1, seed=123, duration=1440, max_wait=600.
Output: table of count per hour + bar chart (PNG if matplotlib available, else ASCII).
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "backend"))

from app.sim.demand import load_requests

REQUESTS_PATH = str(ROOT / "data" / "requests_austin_h3_r8.parquet")
SEED = 123
DURATION = 1440  # minutes
MAX_WAIT = 600.0
SCALE = 0.1
DEMAND_FLATTEN = 1.0

# Same as Exp 39
requests = load_requests(
    REQUESTS_PATH,
    duration_minutes=DURATION,
    max_wait_time_seconds=MAX_WAIT,
    demand_scale=SCALE,
    demand_flatten=DEMAND_FLATTEN,
    seed=SEED,
)

# request_time is seconds from 0 to 86400 (24h)
times_s = np.array([r.request_time for r in requests])
hour_bin = (times_s / 3600.0).astype(int).clip(0, 23)
counts_per_hour = np.bincount(hour_bin, minlength=24)

total = len(requests)
mean_per_hour = total / 24.0
print(f"Total trips: {total:,}  (scale={SCALE}, demand_flatten={DEMAND_FLATTEN}, seed={SEED})")
print(f"Mean per hour: {mean_per_hour:,.1f}")
print(f"Min hour count: {counts_per_hour.min():,}  Max: {counts_per_hour.max():,}")
print()

# Table
print("Hour    Count    Pct     Bar")
print("----    -----    ---     ---")
max_ct = max(counts_per_hour.max(), 1)
for h in range(24):
    ct = counts_per_hour[h]
    pct = ct / total * 100.0 if total else 0
    bar_len = int(round(ct / max_ct * 50)) if max_ct else 0
    bar = "#" * bar_len
    print(f"  {h:2d}    {ct:>5,}   {pct:>5.2f}%   {bar}")

# Try matplotlib for a real plot
try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.bar(range(24), counts_per_hour, color="steelblue", edgecolor="white")
    ax.axhline(mean_per_hour, color="orangered", linestyle="--", label=f"Mean = {mean_per_hour:,.0f}")
    ax.set_xlabel("Hour of day")
    ax.set_ylabel("Trip count")
    ax.set_title(f"Trips per hour (demand_flatten={DEMAND_FLATTEN}, scale={SCALE}, n={total:,})")
    ax.set_xticks(range(24))
    ax.legend()
    fig.tight_layout()
    out_path = ROOT / "scripts" / "exp39_flatten_trips_per_hour.png"
    fig.savefig(out_path, dpi=120)
    plt.close()
    print(f"\nPlot saved to {out_path}")
except ImportError:
    print("\n(Install matplotlib to save a PNG: pip install matplotlib)")
