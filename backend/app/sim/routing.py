from __future__ import annotations

import os
from typing import Optional

import h3
import httpx
import pandas as pd


_METERS_PER_MILE = 1609.344


def _h3_to_lnglat(cell: str) -> tuple[float, float]:
    """Returns (lng, lat) for the centroid of an H3 cell."""
    lat, lng = h3.cell_to_latlng(cell)
    return lng, lat


class RoutingCache:
    """
    H3-to-H3 travel time + distance lookup.

    Priority:
      1. In-memory dict (pre-loaded from parquet or injected directly in tests).
      2. Synchronous OSRM /route call on cache miss (result stored in memory).

    New entries discovered via OSRM can be flushed back to the parquet with
    flush_new_entries(path) so subsequent runs benefit from them.
    """

    def __init__(
        self,
        cache: Optional[dict[tuple[str, str], tuple[float, float]]] = None,
        parquet_path: Optional[str] = None,
        osrm_url: Optional[str] = None,
        time_multiplier: Optional[float] = None,
    ) -> None:
        self._cache: dict[tuple[str, str], tuple[float, float]] = {}
        self._new_entries: dict[tuple[str, str], tuple[float, float]] = {}
        self._hits: int = 0
        self._misses: int = 0
        self._osrm_url = osrm_url or os.environ.get("OSRM_URL", "http://localhost:5001")
        # Multiplier applied to OSRM-reported travel times to model systematic
        # duration underestimation (e.g., congestion).
        if time_multiplier is None:
            time_multiplier = float(os.environ.get("OSRM_TIME_MULTIPLIER", "1.0"))
        self._time_multiplier: float = float(time_multiplier)

        if cache is not None:
            mul = self._time_multiplier
            for k, (t, d) in cache.items():
                self._cache[k] = (t * mul, d)
        elif parquet_path and os.path.exists(parquet_path):
            self._load_parquet(parquet_path)

    def _load_parquet(self, path: str) -> None:
        mul = self._time_multiplier
        df = pd.read_parquet(path, columns=["origin_h3", "destination_h3", "time_seconds", "distance_meters"])
        for row in df.itertuples(index=False):
            key = (row.origin_h3, row.destination_h3)
            self._cache[key] = (float(row.time_seconds) * mul, float(row.distance_meters))

    def get(self, origin_h3: str, dest_h3: str) -> tuple[float, float]:
        """
        Returns (time_seconds, distance_meters).
        Times are pre-multiplied by time_multiplier at cache load.
        Falls back to OSRM on cache miss.
        """
        if origin_h3 == dest_h3:
            return (0.0, 0.0)

        cached = self._cache.get((origin_h3, dest_h3))
        if cached is not None:
            self._hits += 1
            return cached

        self._misses += 1
        key = (origin_h3, dest_h3)
        raw = self._osrm_lookup(origin_h3, dest_h3)
        adjusted = (raw[0] * self._time_multiplier, raw[1])
        self._cache[key] = adjusted
        self._new_entries[key] = raw
        return adjusted

    def get_miles(self, origin_h3: str, dest_h3: str) -> tuple[float, float]:
        """Returns (time_seconds, distance_miles)."""
        time_s, dist_m = self.get(origin_h3, dest_h3)
        return time_s, dist_m / _METERS_PER_MILE

    def cache_stats(self) -> dict:
        """Returns hit/miss counters and hit rate."""
        total = self._hits + self._misses
        return {
            "cache_hits": self._hits,
            "cache_misses": self._misses,
            "hit_rate_pct": round(self._hits / total * 100.0, 2) if total > 0 else 0.0,
            "new_entries": len(self._new_entries),
            "cache_size": len(self._cache),
        }

    def flush_new_entries(self, parquet_path: str) -> int:
        """
        Append newly-fetched OSRM entries to the parquet file on disk.
        Deduplicates against existing rows before writing.
        Returns the number of rows appended.
        """
        if not self._new_entries:
            return 0

        rows = [
            {
                "origin_h3": o,
                "destination_h3": d,
                "time_seconds": t,
                "distance_meters": dist,
            }
            for (o, d), (t, dist) in self._new_entries.items()
        ]
        df_new = pd.DataFrame(rows)

        if os.path.exists(parquet_path):
            df_existing = pd.read_parquet(
                parquet_path, columns=["origin_h3", "destination_h3"]
            )
            existing_keys = set(
                zip(df_existing["origin_h3"], df_existing["destination_h3"])
            )
            df_new = df_new[
                ~df_new.apply(
                    lambda r: (r["origin_h3"], r["destination_h3"]) in existing_keys,
                    axis=1,
                )
            ]

        if df_new.empty:
            return 0

        if os.path.exists(parquet_path):
            df_base = pd.read_parquet(parquet_path)
            df_combined = pd.concat([df_base, df_new], ignore_index=True)
        else:
            df_combined = df_new

        df_combined.to_parquet(parquet_path, index=False)
        self._new_entries.clear()
        return len(df_new)

    def _osrm_lookup(self, origin_h3: str, dest_h3: str) -> tuple[float, float]:
        """
        Single-pair lookup using the OSRM table API (same endpoint as the
        precompute script) so cached and live-fetched values are consistent.
        """
        orig_lng, orig_lat = _h3_to_lnglat(origin_h3)
        dest_lng, dest_lat = _h3_to_lnglat(dest_h3)
        url = (
            f"{self._osrm_url}/table/v1/driving/"
            f"{orig_lng},{orig_lat};{dest_lng},{dest_lat}"
            f"?sources=0&destinations=1&annotations=duration,distance"
        )
        try:
            resp = httpx.get(url, timeout=10.0)
            resp.raise_for_status()
            data = resp.json()
            duration = data["durations"][0][0]
            distance = data["distances"][0][0]
            if duration is None or distance is None:
                raise RuntimeError("OSRM table returned null for this pair")
            return float(duration), float(distance)
        except Exception as exc:
            raise RuntimeError(
                f"OSRM table lookup failed for ({origin_h3} → {dest_h3}): {exc}"
            ) from exc

    def size(self) -> int:
        return len(self._cache)
