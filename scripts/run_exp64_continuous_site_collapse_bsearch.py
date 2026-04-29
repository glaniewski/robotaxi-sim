"""
Experiment 64 — Find site count where continuous multi-day served% collapses.

Uses **3-day** continuous runs (same config as Exp63) to save wall time (~0.3× a 10-day run),
then binary search on microsite count N.

**Collapsed** (rapid decay / stress): last-day served% < 88 OR (day1 − last-day) served% > 12 pp.
**Stable**: neither condition (for screening).

Binary search: smallest N in [SITE_LO, SITE_HI] that is **stable** (monotone: more sites → better).

Run: PYTHONHASHSEED=0 python3 scripts/run_exp64_continuous_site_collapse_bsearch.py
"""
from __future__ import annotations

import sys
from pathlib import Path

from tqdm import tqdm

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "backend"))
sys.path.insert(0, str(ROOT / "scripts"))

from run_exp63_continuous_multiday_steady_state import run_continuous_experiment

SCREEN_DAYS = 3
SITE_LO = 30
SITE_HI = 220
# Thresholds for "collapse" on a SCREEN_DAYS window
LAST_DAY_MIN_OK = 88.0
MAX_DROP_OK = 12.0


def is_collapsed(daily: list[dict]) -> bool:
    sp = [float(r["served_pct"]) for r in daily]
    if not sp:
        return True
    drop = sp[0] - sp[-1]
    return sp[-1] < LAST_DAY_MIN_OK or drop > MAX_DROP_OK


def main() -> None:
    print(
        f"Exp64: binary search N ∈ [{SITE_LO},{SITE_HI}], {SCREEN_DAYS}-day continuous runs, "
        f"collapse if last_served<{LAST_DAY_MIN_OK}% or day1−last>{MAX_DROP_OK}pp\n"
    )

    trials: list[tuple[int, bool, list[float]]] = []
    cache: dict[int, bool] = {}

    def run_n(n: int) -> bool:
        if n in cache:
            return cache[n]
        out = run_continuous_experiment(
            n,
            SCREEN_DAYS,
            show_trip_progress=True,
            trip_bar_desc=f"exp64_N{n}",
        )
        daily = out["daily"]
        sp = [float(r["served_pct"]) for r in daily]
        collapsed = is_collapsed(daily)
        trials.append((n, collapsed, sp))
        cache[n] = collapsed
        print(
            f"  N={n:3d}  collapsed={collapsed}  served% by day: {[round(x, 2) for x in sp]}  "
            f"overall_served={out['metrics']['served_pct']:.2f}"
        )
        return collapsed

    # Need: high site count stable, low count collapsed (monotone in N).
    if run_n(SITE_HI):
        print(f"\nSITE_HI={SITE_HI} unexpectedly collapsed — raise ceiling or relax thresholds.")
        return
    if not run_n(SITE_LO):
        print(f"\nSITE_LO={SITE_LO} unexpectedly stable — lower floor or tighten thresholds.")
        return

    lo, hi = SITE_LO, SITE_HI
    while lo < hi:
        mid = (lo + hi) // 2
        if run_n(mid):
            lo = mid + 1
        else:
            hi = mid

    print(
        f"\n**Smallest stable N ({SCREEN_DAYS}-day screen, "
        f"last≥{LAST_DAY_MIN_OK}% & drop≤{MAX_DROP_OK}pp): {lo}**  ({len(trials)} sims)"
    )
    print("Trials:", trials)


if __name__ == "__main__":
    main()
