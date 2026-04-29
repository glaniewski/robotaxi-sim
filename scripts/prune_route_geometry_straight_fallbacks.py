#!/usr/bin/env python3
"""
Remove straight-line (≤2-point) polylines from data/route_geometry_cache.json.

When OSRM is down or returns non-Ok, scripts/dashboard.py stores a two-point segment
(cell centroid → centroid). Pruning those entries forces the next dashboard generation
to call OSRM /route again for those H3 pairs (run with OSRM up: docker compose up -d osrm).

Usage:
  python3 scripts/prune_route_geometry_straight_fallbacks.py
  python3 scripts/prune_route_geometry_straight_fallbacks.py --dry-run
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_PATH = ROOT / "data" / "route_geometry_cache.json"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--path",
        type=Path,
        default=DEFAULT_PATH,
        help=f"Route geometry JSON (default: {DEFAULT_PATH})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print counts only; do not write",
    )
    args = parser.parse_args()
    path: Path = args.path

    if not path.exists():
        print(f"ERROR: {path} not found.", file=sys.stderr)
        sys.exit(1)

    with open(path) as f:
        raw: dict[str, list] = json.load(f)

    pruned = 0
    kept: dict[str, list] = {}
    for key, coords in raw.items():
        if not isinstance(coords, list):
            pruned += 1
            continue
        if len(coords) <= 2:
            pruned += 1
            continue
        kept[key] = coords

    print(f"File: {path}")
    print(f"  Entries before: {len(raw):,}")
    print(f"  Straight / invalid removed: {pruned:,}")
    print(f"  Entries after: {len(kept):,}")

    if args.dry_run:
        print("(dry-run — no file written)")
        return

    fd, tmp = tempfile.mkstemp(
        dir=path.parent, prefix=".route_geometry_", suffix=".json.tmp"
    )
    try:
        with os.fdopen(fd, "w") as out:
            json.dump(kept, out, separators=(",", ":"))
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    print(f"Wrote {path}")


if __name__ == "__main__":
    main()
