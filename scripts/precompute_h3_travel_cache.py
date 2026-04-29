#!/usr/bin/env python3
"""
Precompute H3-to-H3 travel time/distance cache for Austin ODD.

Uses a three-layer hybrid strategy based on analysis of the actual request dataset:

  Layer 1 — Data-driven pairs (83,208 pairs)
    All actual O-D pairs that appear in data/requests_austin_h3_r8.parquet.
    These are exactly what the sim needs for trip dispatch lookups.

  Layer 2 — Active-cell k-NN (≈20K pairs)
    k nearest neighbours (by H3 grid distance) for each of the 1,992 active
    cells. Covers repositioning moves to nearby hot cells not in historical O-D.

  Layer 3 — Depot routing (≈2K × n_depots)
    All active cells ↔ each depot cell. Covers TO_DEPOT charging decisions.

Total: ~105K pairs vs the naive k-NN-on-all-Austin-cells approach that would
miss 42K actual trip pairs and query 5,114 cells the sim never uses.

Requires OSRM to be running:
  docker compose up osrm

Usage:
  python scripts/precompute_h3_travel_cache.py [--k 10] [--osrm http://localhost:5000]
"""
from __future__ import annotations

import argparse
import os
import sys
from collections import defaultdict
from pathlib import Path

import h3
import httpx
import pandas as pd
from tqdm import tqdm

OUTPUT_PATH = Path("data/h3_travel_cache.parquet")
REQUESTS_PATH = Path("data/requests_austin_h3_r8.parquet")
H3_RESOLUTION = 8

# Default depot cell (Austin downtown/6th St — matches default_scenario.json)
DEFAULT_DEPOT_CELLS = ["88489e3467fffff"]

# Austin bounding box for fallback cell enumeration
AUSTIN_BBOX = {
    "min_lat": 30.00, "max_lat": 30.70,
    "min_lng": -98.20, "max_lng": -97.40,
}


# ---------------------------------------------------------------------------
# OSRM helpers
# ---------------------------------------------------------------------------

def h3_to_lnglat(cell: str) -> tuple[float, float]:
    lat, lng = h3.cell_to_latlng(cell)
    return lng, lat


def osrm_table_batch(
    origins: list[str],
    destinations: list[str],
    osrm_url: str,
) -> list[dict]:
    """
    Query OSRM table API for origins × destinations.
    Returns list of {origin_h3, destination_h3, time_seconds, distance_meters}.
    Skips same-cell pairs and null results.
    """
    all_cells = list(dict.fromkeys(origins + destinations))
    coords = ";".join(
        f"{h3_to_lnglat(c)[0]},{h3_to_lnglat(c)[1]}" for c in all_cells
    )
    origin_idxs = ";".join(str(all_cells.index(c)) for c in origins)
    dest_idxs = ";".join(str(all_cells.index(c)) for c in destinations)

    url = (
        f"{osrm_url}/table/v1/driving/{coords}"
        f"?sources={origin_idxs}&destinations={dest_idxs}"
        f"&annotations=duration,distance"
    )
    resp = httpx.get(url, timeout=120.0)
    resp.raise_for_status()
    data = resp.json()

    durations = data.get("durations", [])
    distances = data.get("distances", [])
    rows = []
    for i, orig in enumerate(origins):
        for j, dest in enumerate(destinations):
            if orig == dest:
                continue
            t = durations[i][j] if durations else None
            d = distances[i][j] if distances else None
            if t is not None and d is not None and t > 0:
                rows.append({
                    "origin_h3": orig,
                    "destination_h3": dest,
                    "time_seconds": float(t),
                    "distance_meters": float(d),
                })
    return rows


def query_pairs_batched(
    pairs: list[tuple[str, str]],
    osrm_url: str,
    batch_size: int = 50,
    label: str = "",
) -> list[dict]:
    """
    Query arbitrary (origin, dest) pairs by grouping into origin batches.
    Returns deduplicated rows.
    """
    # Group destinations by origin for efficient table queries
    by_origin: dict[str, list[str]] = defaultdict(list)
    for o, d in pairs:
        by_origin[o].append(d)

    all_rows: list[dict] = []
    origins_list = list(by_origin.keys())
    total = len(origins_list)
    failed = 0

    with tqdm(total=total, desc=label, unit="origins", ncols=80) as bar:
        for i in range(0, total, batch_size):
            batch_origins = origins_list[i: i + batch_size]
            # Collect all destinations needed for this origin batch
            batch_dests = list({d for o in batch_origins for d in by_origin[o]})
            try:
                rows = osrm_table_batch(batch_origins, batch_dests, osrm_url)
                # Keep only the requested pairs (table returns all combinations)
                needed = {(o, d) for o in batch_origins for d in by_origin[o]}
                rows = [r for r in rows if (r["origin_h3"], r["destination_h3"]) in needed]
                all_rows.extend(rows)
            except Exception as exc:
                failed += 1
                tqdm.write(f"  WARNING: batch {i//batch_size + 1} failed: {exc}")

            bar.update(len(batch_origins))
            bar.set_postfix(pairs=f"{len(all_rows):,}", failed=failed)

    return all_rows


# ---------------------------------------------------------------------------
# Pair generation layers
# ---------------------------------------------------------------------------

def layer1_data_driven(df: pd.DataFrame) -> list[tuple[str, str]]:
    """All actual O-D pairs from the request parquet."""
    pairs = df.groupby(["origin_h3", "destination_h3"]).size().index.tolist()
    return [(o, d) for o, d in pairs]


def layer2_active_knn(active_cells: list[str], k: int) -> list[tuple[str, str]]:
    """
    k nearest H3 grid neighbours for each active cell.
    Uses h3.grid_disk to get concentric rings until k unique active neighbours
    are found, capped at ring radius 5.
    """
    active_set = set(active_cells)
    pairs: set[tuple[str, str]] = set()

    for cell in active_cells:
        found = []
        for radius in range(1, 6):
            ring = h3.grid_ring(cell, radius)
            # Prefer neighbours that are themselves active cells
            active_neighbours = [c for c in ring if c in active_set and c != cell]
            for n in active_neighbours:
                if n not in [p[1] for p in found]:
                    found.append((cell, n))
            if len(found) >= k:
                break
        for pair in found[:k]:
            pairs.add(pair)

    return list(pairs)


def layer3_depot_routing(
    active_cells: list[str], depot_cells: list[str]
) -> list[tuple[str, str]]:
    """All active cells → each depot, and each depot → all active cells."""
    pairs: set[tuple[str, str]] = set()
    for depot in depot_cells:
        for cell in active_cells:
            if cell != depot:
                pairs.add((cell, depot))
                pairs.add((depot, cell))
    return list(pairs)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--k", type=int, default=10,
                        help="k nearest active-cell neighbours per cell (layer 2)")
    parser.add_argument("--osrm", default=os.environ.get("OSRM_URL", "http://localhost:5001"),
                        help="OSRM base URL")
    parser.add_argument("--batch", type=int, default=50,
                        help="Origins per OSRM table call")
    parser.add_argument("--depots", nargs="*", default=DEFAULT_DEPOT_CELLS,
                        help="Depot H3 cells (space-separated)")
    args = parser.parse_args()

    # Verify OSRM — any HTTP response (including 400) means it's running
    try:
        httpx.get(f"{args.osrm}/", timeout=5.0)
        print(f"OSRM reachable at {args.osrm}")
    except httpx.ConnectError:
        print(f"ERROR: OSRM not reachable at {args.osrm} (connection refused)", file=sys.stderr)
        print("Run: docker compose up", file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        # Non-connection errors (e.g. HTTP 400) still mean OSRM is up
        print(f"OSRM reachable at {args.osrm}")

    # Load parquet
    if not REQUESTS_PATH.exists():
        print(f"ERROR: {REQUESTS_PATH} not found.", file=sys.stderr)
        print("Run: python scripts/preprocess_rideaustin_requests.py", file=sys.stderr)
        sys.exit(1)

    print(f"\nLoading request parquet...")
    df = pd.read_parquet(REQUESTS_PATH, columns=["origin_h3", "destination_h3"])
    print(f"  {len(df):,} trips, computing pair layers...")

    active_cells = list(set(df["origin_h3"]) | set(df["destination_h3"]))
    print(f"  Active cells: {len(active_cells):,}")

    # --- Layer 1: data-driven O-D pairs ---
    l1 = layer1_data_driven(df)
    print(f"\nLayer 1 (data-driven O-D pairs):   {len(l1):,}")

    # --- Layer 2: active-cell k-NN ---
    l2 = layer2_active_knn(active_cells, k=args.k)
    print(f"Layer 2 (active k={args.k} NN):           {len(l2):,}")

    # --- Layer 3: depot routing ---
    l3 = layer3_depot_routing(active_cells, args.depots)
    print(f"Layer 3 (depot routing):            {len(l3):,}")

    # Deduplicate across all layers
    all_pairs: set[tuple[str, str]] = set(l1) | set(l2) | set(l3)
    print(f"\nTotal unique pairs to query:        {len(all_pairs):,}")

    # --- Query OSRM ---
    pairs_list = list(all_pairs)
    print(f"\nQuerying OSRM ({len(pairs_list):,} pairs, batch={args.batch})...")
    rows = query_pairs_batched(pairs_list, args.osrm, args.batch, label="total")

    if not rows:
        print("ERROR: No rows returned from OSRM.", file=sys.stderr)
        sys.exit(1)

    df_out = pd.DataFrame(rows).drop_duplicates(subset=["origin_h3", "destination_h3"])
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_parquet(OUTPUT_PATH, index=False)

    coverage = len(df_out) / len(all_pairs) * 100
    l1_covered = sum(
        1 for o, d in l1
        if len(df_out[(df_out["origin_h3"] == o) & (df_out["destination_h3"] == d)]) > 0
    )
    print(f"\nWrote {len(df_out):,} pairs to {OUTPUT_PATH}")
    print(f"Coverage: {coverage:.1f}% of requested pairs returned by OSRM")
    print(f"Layer 1 (trip pairs) covered: {l1_covered:,} / {len(l1):,} ({l1_covered/len(l1)*100:.1f}%)")
    print(f"Avg time_seconds: {df_out['time_seconds'].mean():.1f}s")
    print(f"Avg distance_meters: {df_out['distance_meters'].mean():.0f}m ({df_out['distance_meters'].mean()/1609:.2f} mi)")


if __name__ == "__main__":
    main()
