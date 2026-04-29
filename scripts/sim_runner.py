"""
Shared streaming run helper used by all experiment scripts.

Usage:
    from sim_runner import run_stream, apply_fc, FIXED_COSTS, FC_LABELS

    m = run_stream("scale=0.05 fleet=1000", payload)
"""
from __future__ import annotations
import json, time, urllib.request
from typing import Optional

API = "http://localhost:8000"
BAR_WIDTH = 38

FIXED_COSTS = [27.40, 56.00, 100.00]
FC_LABELS = {27.40: "A ($27.40/veh/day)", 56.00: "B ($56/veh/day)", 100.00: "C ($100/veh/day)"}


def _fmt_time(seconds: float) -> str:
    if seconds != seconds or seconds < 0 or seconds == float("inf"):  # NaN, negative, or inf
        return "--:--"
    m, s = divmod(int(seconds), 60)
    if m >= 100:
        return f"{m}:{s:02d}"
    return f"{m:02d}:{s:02d}"


def run_stream(label: str, payload: dict, timeout: int = 3600) -> dict:
    """
    POST to /run/stream and render a live progress bar:
      [████████░░░░░] 45.0%  11,000/26,034  [02:05<03:12, 88 trips/s]
    Returns the metrics dict on success, {} on failure.
    """
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        f"{API}/run/stream", data=data,
        headers={"Content-Type": "application/json"}, method="POST",
    )
    print(f"  {label}")
    t_start = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            while True:
                raw = resp.readline()
                if not raw:
                    break
                msg = json.loads(raw.strip())

                if msg["type"] == "progress":
                    done, total = msg["done"], msg["total"]
                    elapsed = time.time() - t_start
                    pct = done / total if total else 0
                    filled = int(pct * BAR_WIDTH)
                    bar = "█" * filled + "░" * (BAR_WIDTH - filled)
                    rate = done / elapsed if elapsed > 0 else 0
                    eta = (total - done) / rate if rate > 0 else float("inf")
                    rate_str = f"{rate:,.0f} trips/s"
                    time_str = f"{_fmt_time(elapsed)}<{_fmt_time(eta)}"
                    print(
                        f"\033[2K\r  [{bar}] {pct*100:5.1f}%  {done:,}/{total:,}"
                        f"  [{time_str}, {rate_str}]",
                        end="", flush=True,
                    )

                elif msg["type"] == "result":
                    elapsed = time.time() - t_start
                    m = msg["metrics"]
                    total_trips = m.get("served_count", 0) + m.get("unserved_count", 0)
                    print(
                        f"\033[2K\r  [{'█'*BAR_WIDTH}] 100.0%  {total_trips:,}/{total_trips:,}"
                        f"  [{_fmt_time(elapsed)}<00:00] ✓"
                    )
                    return m

                elif msg["type"] == "error":
                    print(f"\r  ✗  Error: {msg['message']}")
                    return {}

    except Exception as e:
        print(f"\r  ✗  {e}")
        return {}
    return {}


def apply_fc(m: dict, fleet: int, fc: float) -> dict:
    """Analytically recompute economics for a different fixed-cost assumption."""
    if not m:
        return {}
    fixed = fleet * fc
    variable = m["revenue_total"] - m["total_margin"] - m.get("fixed_cost_total", 0.0)
    margin = m["revenue_total"] - variable - fixed
    served = m["served_count"] or 1
    return {
        **m,
        "fixed_cost_total": round(fixed, 2),
        "total_margin": round(margin, 2),
        "contribution_margin_per_trip": round(margin / served, 4),
    }
