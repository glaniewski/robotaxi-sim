"""
Search for **better N=2 depot sites** under a **fast proxy**: trip-weighted mean drive time
from each trip **origin** to the **nearest** depot (same H3 cache as the sim).

This is **not** a full discrete-event search (no fleet size, queues, or dispatch). It finds
pairs that **minimize geographic access** from where rides start — a useful screen before
you burn wall time on ``run_continuous_experiment``.

Candidate pool: top ``M`` origin cells plus top ``K`` destination cells (deduped).

Run from repo root:
    python3 scripts/search_n2_depot_pairs_origin_access.py
    python3 scripts/search_n2_depot_pairs_origin_access.py --top-m 50 --list 25

Import ``compute_ranked_depot_pairs`` for ``map_n2_depot_pair_search.py``.
"""
from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from itertools import combinations
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

import run_exp63_continuous_multiday_steady_state as e63  # noqa: E402

REQUESTS_PATH = ROOT / "data" / "requests_austin_h3_r8.parquet"
TRAVEL_CACHE = ROOT / "data" / "h3_travel_cache.parquet"

_MISSING_SEC = 3_600.0


def _weighted_p90_minutes(t_sec: np.ndarray, w: np.ndarray) -> float:
    order = np.argsort(t_sec)
    ts = t_sec[order]
    ws = w[order]
    cw = np.cumsum(ws) / np.sum(ws)
    idx = int(np.searchsorted(cw, 0.90))
    return float(ts[min(idx, len(ts) - 1)] / 60.0)


@dataclass(frozen=True)
class DepotPairSearchResult:
    """Sorted ``results``: (mean_min, p90_min, cell_a, cell_b) with a < b lexicographically."""

    results: list[tuple[float, float, str, str]]
    baseline: tuple[str, str]  # order from ``top_demand_cells(2)``
    candidates: tuple[str, ...]
    top_m: int
    top_k_dest: int
    baseline_mean_min: float
    baseline_p90_min: float
    baseline_proxy_rank: int | None  # 1-based rank among ``results`` by mean then p90


def compute_ranked_depot_pairs(top_m: int = 40, top_k_dest: int = 12) -> DepotPairSearchResult:
    """Enumerate all pairs among top-M origins (+ top-K dest extras); rank by proxy mean then p90."""
    df = pd.read_parquet(REQUESTS_PATH, columns=["origin_h3", "destination_h3"])
    vc_o = df["origin_h3"].value_counts()
    vc_d = df["destination_h3"].value_counts()

    top_o = vc_o.head(top_m).index.astype(str).tolist()
    top_d_extra = [c for c in vc_d.head(top_k_dest).index.astype(str) if c not in top_o]
    candidates = tuple(top_o + top_d_extra)
    cand_idx = {c: i for i, c in enumerate(candidates)}
    n_c = len(candidates)

    origins = vc_o.index.astype(str).tolist()
    w = vc_o.values.astype(np.float64)
    n_o = len(origins)
    origin_row = {o: i for i, o in enumerate(origins)}

    cset = set(candidates)
    oset = set(origins)
    cache_df = pd.read_parquet(
        TRAVEL_CACHE, columns=["origin_h3", "destination_h3", "time_seconds"]
    )
    sub = cache_df[
        cache_df["destination_h3"].isin(cset) & cache_df["origin_h3"].isin(oset)
    ]
    sub = sub.groupby(["origin_h3", "destination_h3"], sort=False)["time_seconds"].min().reset_index()

    T = np.full((n_o, n_c), _MISSING_SEC, dtype=np.float64)
    for row in sub.itertuples(index=False):
        ri = origin_row.get(str(row.origin_h3))
        ci = cand_idx.get(str(row.destination_h3))
        if ri is not None and ci is not None:
            T[ri, ci] = float(row.time_seconds)

    baseline_list = e63.top_demand_cells(2)
    baseline = (str(baseline_list[0]), str(baseline_list[1]))
    if baseline[0] not in cand_idx or baseline[1] not in cand_idx:
        raise ValueError(
            f"Top-2 origin depots {baseline} not both in top-{top_m} candidates; increase top_m."
        )

    results: list[tuple[float, float, str, str]] = []
    for i, j in combinations(range(n_c), 2):
        tmin = np.minimum(T[:, i], T[:, j])
        mean_min = float(np.sum(tmin * w) / np.sum(w) / 60.0)
        p90 = _weighted_p90_minutes(tmin, w)
        a, b = candidates[i], candidates[j]
        if a > b:
            a, b = b, a
        results.append((mean_min, p90, a, b))

    results.sort(key=lambda x: (x[0], x[1], x[2]))
    base_key = tuple(sorted(baseline))
    rank_baseline = next(
        (r for r, x in enumerate(results, start=1) if (x[2], x[3]) == base_key),
        None,
    )
    bi, bj = cand_idx[baseline[0]], cand_idx[baseline[1]]
    if bi > bj:
        bi, bj = bj, bi
    t_b = np.minimum(T[:, bi], T[:, bj])
    baseline_mean = float(np.sum(t_b * w) / np.sum(w) / 60.0)
    baseline_p90 = _weighted_p90_minutes(t_b, w)

    return DepotPairSearchResult(
        results=results,
        baseline=baseline,
        candidates=candidates,
        top_m=top_m,
        top_k_dest=top_k_dest,
        baseline_mean_min=baseline_mean,
        baseline_p90_min=baseline_p90,
        baseline_proxy_rank=rank_baseline,
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--top-m", type=int, default=40, help="Top M origin cells as candidates")
    ap.add_argument("--top-k-dest", type=int, default=12, help="Top K destination cells merged in")
    ap.add_argument("--list", type=int, default=20, help="How many best pairs to print")
    args = ap.parse_args()

    print("Loading origins + destinations …")
    res = compute_ranked_depot_pairs(top_m=args.top_m, top_k_dest=args.top_k_dest)
    results = res.results
    baseline = res.baseline
    base_key = tuple(sorted(baseline))

    df = pd.read_parquet(REQUESTS_PATH, columns=["origin_h3"])
    vc_o = df["origin_h3"].value_counts()
    print(f"Candidates: {len(res.candidates)} cells (top-{args.top_m} origins + dest extras).")
    print(f"Origins: {len(vc_o)} cells, {int(vc_o.sum()):,} trips.")

    print("\n--- Current Exp71 / Exp72 central (top-2 by origin) ---")
    print(
        f"  {baseline[0]}, {baseline[1]}  |  "
        f"mean origin→nearest {res.baseline_mean_min:.3f} min  |  "
        f"p90 {res.baseline_p90_min:.2f} min"
    )

    print(f"\n--- Top {args.list} pairs by trip-weighted mean (then p90) ---")
    for rank, (mean_m, p90_m, a, b) in enumerate(results[: args.list], start=1):
        tag = "  ← current central" if (a, b) == base_key else ""
        print(f"  {rank:2d}.  mean {mean_m:.3f} min  p90 {p90_m:.2f} min   {a}, {b}{tag}")

    best = results[0]
    print("\n--- Conclusion (proxy only) ---")
    if res.baseline_proxy_rank is not None:
        print(
            f"Current central pair ranks **#{res.baseline_proxy_rank}** / {len(results)} "
            f"in this candidate pool by trip-weighted mean access."
        )
    else:
        print("Current central pair was not found in enumerated results (unexpected).")
    if (best[2], best[3]) == base_key:
        print("Best pair under this proxy is **exactly** top-2 origin cells — no better site in the candidate pool.")
    else:
        delta = res.baseline_mean_min - best[0]
        print(
            f"Best proxy pair: **{best[2]}**, **{best[3]}** (mean {best[0]:.3f} min vs central "
            f"{res.baseline_mean_min:.3f} min, **{delta:.3f} min** better on average). "
            "Run ``run_continuous_experiment`` with ``depot_h3_cells`` to validate served%."
        )


if __name__ == "__main__":
    main()
