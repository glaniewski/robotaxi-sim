#!/usr/bin/env python3
"""
Download the Texas OpenStreetMap PBF for OSRM preprocessing.

Output: osrm/data/texas-latest.osm.pbf (~1 GB)

Run this once before `docker compose up`:
  python3.11 scripts/download_osrm_pbf.py
"""
from __future__ import annotations

import sys
import urllib.request
from pathlib import Path

URL = "https://download.geofabrik.de/north-america/us/texas-latest.osm.pbf"
OUTPUT = Path("osrm/data/texas-latest.osm.pbf")


def _progress(count: int, block_size: int, total: int) -> None:
    if total <= 0:
        return
    pct = min(count * block_size / total * 100, 100)
    mb_done = count * block_size / 1_048_576
    mb_total = total / 1_048_576
    bar = "█" * int(pct / 2)
    print(f"\r  {pct:5.1f}%  {mb_done:6.0f} / {mb_total:.0f} MB  {bar:<50}", end="", flush=True)


def main() -> None:
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)

    if OUTPUT.exists():
        size_mb = OUTPUT.stat().st_size / 1_048_576
        print(f"PBF already exists at {OUTPUT} ({size_mb:.0f} MB). Skipping download.")
        print("Delete it and re-run to force re-download.")
        return

    print(f"Downloading Texas PBF (~1 GB) from Geofabrik...")
    print(f"Output: {OUTPUT}")
    print()

    try:
        urllib.request.urlretrieve(URL, OUTPUT, reporthook=_progress)
        print()  # newline after progress bar
        size_mb = OUTPUT.stat().st_size / 1_048_576
        print(f"\nDone. {size_mb:.0f} MB written to {OUTPUT}")
        print("\nNext step: docker compose up --build")
    except KeyboardInterrupt:
        print("\nInterrupted. Removing partial file...")
        OUTPUT.unlink(missing_ok=True)
        sys.exit(1)
    except Exception as exc:
        print(f"\nERROR: {exc}", file=sys.stderr)
        OUTPUT.unlink(missing_ok=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
