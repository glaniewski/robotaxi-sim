"""
Pre-compute all demand_cell × demand_cell routing pairs via OSRM's table API
and flush them to the travel cache parquet.

This is a one-time setup step. After running, all simulation variants
(including demand_init and coverage_floor) have 100% cache hit rates and
run at full speed.

Strategy
--------
OSRM's /table endpoint computes an N×M duration+distance matrix in one HTTP
call. We chunk 1,660 sources × 1,660 destinations into batches small enough
that OSRM doesn't time out (typically 500×500 works fine), then write all
new pairs to the parquet.

Expected runtime: ~2–5 minutes (dominated by OSRM compute, not network).

Usage:
    python3 scripts/warm_routing_cache.py
    python3 scripts/warm_routing_cache.py --batch-size 300  # for slower machines
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from itertools import product

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "backend"))

import httpx
import h3
import numpy as np
import pandas as pd

REQUESTS_PATH = os.path.join(ROOT, "data", "requests_austin_h3_r8.parquet")
TRAVEL_CACHE  = os.path.join(ROOT, "data", "h3_travel_cache.parquet")
OSRM_URL      = os.environ.get("OSRM_URL", "http://localhost:5001")

# H3 → (lng, lat) — OSRM expects lng,lat order
def _cell_lnglat(cell: str) -> tuple[float, float]:
    lat, lng = h3.cell_to_latlng(cell)
    return lng, lat


def load_existing_cache(path: str) -> set[tuple[str, str]]:
    if not os.path.exists(path):
        return set()
    df = pd.read_parquet(path, columns=["origin_h3", "destination_h3"])
    return set(zip(df["origin_h3"], df["destination_h3"]))


def osrm_table_batch(
    sources: list[str],
    destinations: list[str],
    osrm_url: str,
    timeout: float = 120.0,
) -> dict[tuple[str, str], tuple[float, float]]:
    """
    Query OSRM /table for a sources×destinations matrix.
    Returns {(origin_h3, dest_h3): (time_s, dist_m)}.
    Missing pairs (OSRM returned null) are excluded.
    """
    all_cells = sources + destinations
    coords_str = ";".join(f"{lng:.6f},{lat:.6f}" for lng, lat in (_cell_lnglat(c) for c in all_cells))
    # OSRM table API uses semicolons to separate index values
    src_indices = ";".join(str(i) for i in range(len(sources)))
    dst_indices = ";".join(str(i) for i in range(len(sources), len(sources) + len(destinations)))

    url = (
        f"{osrm_url}/table/v1/driving/{coords_str}"
        f"?sources={src_indices}&destinations={dst_indices}"
        f"&annotations=duration,distance"
    )

    resp = httpx.get(url, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()

    durations = data["durations"]
    distances = data["distances"]

    result: dict[tuple[str, str], tuple[float, float]] = {}
    for i, src in enumerate(sources):
        for j, dst in enumerate(destinations):
            t = durations[i][j]
            d = distances[i][j]
            if t is not None and d is not None and src != dst:
                result[(src, dst)] = (float(t), float(d))
    return result


def main():
    parser = argparse.ArgumentParser(description="Pre-warm H3 travel cache.")
    parser.add_argument("--batch-size", type=int, default=400,
                        help="Max sources or destinations per OSRM table call (default 400)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print plan without making OSRM calls")
    args = parser.parse_args()

    t0 = time.time()

    # ── Load demand cells ────────────────────────────────────────────────
    print("Loading demand cells …")
    req_df = pd.read_parquet(REQUESTS_PATH, columns=["origin_h3", "destination_h3"])
    demand_cells = sorted(
        set(req_df["origin_h3"].tolist()) | set(req_df["destination_h3"].tolist())
    )
    print(f"  {len(demand_cells):,} unique demand cells (origins + destinations)")

    # ── Load existing cache ──────────────────────────────────────────────
    print("Loading existing travel cache …")
    existing = load_existing_cache(TRAVEL_CACHE)
    print(f"  {len(existing):,} pairs already cached")

    # ── Compute missing pairs ─────────────────────────────────────────────
    all_pairs = set(product(demand_cells, demand_cells))
    # Same-cell pairs are trivially 0 — no need to cache
    all_pairs -= {(c, c) for c in demand_cells}
    missing = all_pairs - existing

    print(f"  {len(all_pairs):,} total pairs needed")
    print(f"  {len(missing):,} pairs to fetch from OSRM")

    if not missing:
        print("Cache already complete — nothing to do.")
        return

    if args.dry_run:
        print("[dry-run] Exiting before OSRM calls.")
        return

    # ── Batch OSRM calls ─────────────────────────────────────────────────
    B = args.batch_size
    # We need (origin, dest) pairs. Group by origin chunks × dest chunks.
    src_chunks = [demand_cells[i:i + B] for i in range(0, len(demand_cells), B)]
    dst_chunks = [demand_cells[i:i + B] for i in range(0, len(demand_cells), B)]

    total_batches = len(src_chunks) * len(dst_chunks)
    print(f"\nBatching into {total_batches} OSRM calls ({B}×{B} max per call) …")

    new_rows: list[dict] = []
    n_fetched = 0
    n_null    = 0
    batch_i   = 0

    for src_chunk in src_chunks:
        for dst_chunk in dst_chunks:
            batch_i += 1
            # Only request pairs we actually need
            pairs_needed = {(s, d) for s in src_chunk for d in dst_chunk} - existing
            if not pairs_needed:
                continue

            t_batch = time.time()
            try:
                results = osrm_table_batch(src_chunk, dst_chunk, OSRM_URL)
            except Exception as exc:
                print(f"\n  [WARN] batch {batch_i}/{total_batches} failed: {exc}")
                continue

            for (src, dst), (t_s, d_m) in results.items():
                if (src, dst) in pairs_needed:
                    new_rows.append({
                        "origin_h3": src,
                        "destination_h3": dst,
                        "time_seconds": t_s,
                        "distance_meters": d_m,
                    })
                    n_fetched += 1

            n_null += len(pairs_needed) - len({k for k in results if k in pairs_needed})
            elapsed = time.time() - t0
            rate = n_fetched / elapsed if elapsed > 0 else 0
            eta  = (len(missing) - n_fetched) / rate if rate > 0 else float("inf")
            print(
                f"\r  batch {batch_i}/{total_batches}  "
                f"fetched={n_fetched:,}  null={n_null:,}  "
                f"elapsed={elapsed:.0f}s  eta={eta:.0f}s",
                end="", flush=True,
            )

    print(f"\n\nFetched {n_fetched:,} new pairs ({n_null:,} OSRM-null, i.e. unreachable)")

    if not new_rows:
        print("No new pairs to write.")
        return

    # ── Append to parquet ────────────────────────────────────────────────
    print("Appending to travel cache parquet …")
    new_df = pd.DataFrame(new_rows, columns=["origin_h3", "destination_h3", "time_seconds", "distance_meters"])

    if os.path.exists(TRAVEL_CACHE):
        existing_df = pd.read_parquet(TRAVEL_CACHE)
        combined = pd.concat([existing_df, new_df], ignore_index=True)
        # Deduplicate (keep last = new OSRM values preferred)
        combined = combined.drop_duplicates(subset=["origin_h3", "destination_h3"], keep="last")
    else:
        combined = new_df

    combined.to_parquet(TRAVEL_CACHE, index=False)
    print(f"  Wrote {len(combined):,} total rows to {TRAVEL_CACHE}")
    print(f"  Added {len(new_df):,} new rows  ({n_null:,} unreachable pairs skipped)")
    print(f"\nDone in {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
