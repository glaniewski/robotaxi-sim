"""
CoverageFloorPolicy — road-network-aware coverage guarantee.

Wraps DemandScorePolicy and ensures every demand cell has at least
`min_coverage` eligible vehicles that can reach it within
`max_wait_time_seconds` by actual road network.

Coverage model
--------------
  covered_by[origin_cell] = frozenset of demand cells reachable from
  origin_cell in ≤ max_wait_time_seconds, precomputed from the travel-cache
  parquet (passed in at init time).

  _coverage[cell] = number of currently-eligible vehicles V such that
  routing.get(V.current_h3, cell) ≤ max_wait_time_seconds.

  This is incrementally maintained:
    on_vehicle_eligible(h3)   → _coverage[y] += 1  for y in covered_by[h3]
    on_vehicle_ineligible(h3) → _coverage[y] -= 1  for y in covered_by[h3]
    on_vehicle_move(old, new) → swap old→new contributions (repositioning)

  Deficit: _coverage[cell] < _min_cov_for(cell, t).

Coverage-deficit target selection
-----------------------------------
  1. At each time-bucket boundary, refresh _deficit_set using current
     time-varying min_cov thresholds.
  2. Iterate _deficit_set; for each cell verify travel_s ≤ _coverage_travel_s.
  3. Pick highest demand score among reachable deficit cells.
  4. If none reachable → fall through to DemandScorePolicy.

min_coverage (static global floor)
-----------------------------------
  Configurable (default 1).  Time-varying min_cov can only exceed this floor,
  never go below it.

Option B: time-varying min_cov
-------------------------------
  When timed_forecast_table is supplied, min_cov is computed dynamically:

    min_cov(cell, t) = max(min_coverage,
                           ceil(rate_at(cell, t + lookahead_s) * lookahead_s))

  where rate_at(cell, t+lookahead) is the historical arrival rate for the
  forecast bucket containing t+lookahead_s.

  This pre-positions vehicles BEFORE a demand burst: at 3:30 AM the policy
  "sees" the 4 AM burst coming (via the lookahead window), raises min_cov,
  and starts repositioning with enough lead time so vehicles arrive before
  the first trips expire.

  The deficit_set is refreshed fully at each bucket-boundary crossing, and
  incrementally on vehicle state changes between boundaries.
"""
from __future__ import annotations

import math
from typing import TYPE_CHECKING, Optional

from ..entities import Vehicle
from .base import BaseRepositioningPolicy
from .demand_score import DemandScorePolicy

if TYPE_CHECKING:
    from ..dispatch import VehicleIndex
    from ..routing import RoutingCache


class CoverageFloorPolicy(BaseRepositioningPolicy):

    def __init__(
        self,
        demand_score_policy: DemandScorePolicy,
        demand_cells: set[str],
        covered_by: dict[str, frozenset[str]],
        max_wait_time_seconds: float = 600.0,
        min_coverage: int = 1,
        coverage_reposition_travel_minutes: Optional[float] = None,
        coverage_max_vehicles_targeting: int = 1,
        timed_forecast_table: Optional[dict[str, dict[int, float]]] = None,
        forecast_bucket_minutes: float = 15.0,
        coverage_lookahead_minutes: Optional[float] = None,
    ) -> None:
        """
        Parameters
        ----------
        demand_score_policy:
            Fully constructed DemandScorePolicy instance.
        demand_cells:
            All H3 cells that have any historical demand (origins + dests).
        covered_by:
            Precomputed road-network reachability: covered_by[origin] is the
            frozenset of demand cells reachable from origin within
            max_wait_time_seconds by road.
        max_wait_time_seconds:
            The dispatch time budget used when building covered_by.
        min_coverage:
            Global minimum floor: min_cov(cell, t) is never below this value.
            1 = zero-deficit guarantee.  2+ = redundancy buffer.
        coverage_reposition_travel_minutes:
            Maximum travel time for coverage-deficit repositioning moves.
            Default: 20 minutes (wider than demand_score's 12-minute limit so
            isolated cells reachable only via highway detours can be filled).
        coverage_max_vehicles_targeting:
            How many vehicles may simultaneously be heading to the same deficit
            cell for coverage.
        timed_forecast_table:
            Option B — {h3_cell: {bucket_idx: arrivals_per_second}}.
            When provided, min_cov is computed as:
                max(min_coverage, ceil(rate_at(cell, t+lookahead) * lookahead_s))
            enabling pre-emptive pre-positioning before demand bursts.
            When None, falls back to static min_coverage for all cells/times.
        forecast_bucket_minutes:
            Duration of each forecast bucket in minutes (default 15).
            n_buckets = 1440 / forecast_bucket_minutes.
        coverage_lookahead_minutes:
            How far ahead (in minutes) to look in the forecast when computing
            min_cov(cell, t).  Should match the reposition latency so vehicles
            arrive just in time.  Defaults to coverage_reposition_travel_minutes
            (or 20 min if that is also None).
        """
        self._inner = demand_score_policy
        self._demand_cells: frozenset[str] = frozenset(demand_cells)
        self._covered_by = covered_by
        self.max_wait_time_seconds = max_wait_time_seconds
        self.min_coverage = min_coverage

        # Coverage-specific reposition limit (seconds).
        if coverage_reposition_travel_minutes is None:
            self._coverage_travel_s: float = 20.0 * 60.0
        else:
            self._coverage_travel_s = coverage_reposition_travel_minutes * 60.0

        # Option B: time-varying forecast for min_cov computation.
        self._timed_forecast = timed_forecast_table  # may be None
        self._bucket_s = forecast_bucket_minutes * 60.0
        self._n_buckets = int(round(1440.0 / forecast_bucket_minutes))

        # Lookahead for min_cov (seconds).  Aligned to reposition latency so
        # pre-positioning starts early enough for vehicles to arrive in time.
        if coverage_lookahead_minutes is not None:
            self._lookahead_s: float = coverage_lookahead_minutes * 60.0
        else:
            self._lookahead_s = self._coverage_travel_s  # default = reposition limit

        # Precomputed min_cov table: _min_cov_table[bucket_idx][cell] = int.
        # Avoids repeated dict lookups inside hot callbacks (10M+ calls/run).
        # Shape: n_buckets × |demand_cells|, computed once at init.
        if timed_forecast_table is not None:
            self._min_cov_table: list[dict[str, int]] = []
            for b in range(self._n_buckets):
                bucket_cov: dict[str, int] = {}
                for cell in demand_cells:
                    rate = timed_forecast_table.get(cell, {}).get(b, 0.0)
                    bucket_cov[cell] = max(min_coverage, math.ceil(rate * self._lookahead_s))
                self._min_cov_table.append(bucket_cov)
        else:
            self._min_cov_table = []

        # Road-network coverage counter per demand cell.
        self._coverage: dict[str, int] = {cell: 0 for cell in demand_cells}

        # _coverage_targeting[cell] = number of vehicles currently en route
        # to that cell for coverage-deficit repositioning.
        # The cap per cell is the deficit size (_min_cov_for - _coverage), so
        # we never dispatch more vehicles than needed to fill the gap.
        # The coverage_max_vehicles_targeting arg is kept for API compatibility
        # but is no longer the primary limit; deficit size takes precedence.
        self._coverage_max_targeting = coverage_max_vehicles_targeting
        self._coverage_targeting: dict[str, int] = {}

        # Last known sim time — used by callbacks (which don't receive time).
        self._current_time: float = 0.0

        # Track the last forecast bucket for which we did a full deficit refresh.
        # -1 forces a refresh on the first select_target call.
        self._last_refresh_bucket: int = -1

        # Cached per-cell threshold dict for the *current* bucket.
        # Refreshed at each bucket boundary (96×/day) so callbacks don't
        # recompute `int((t+lookahead)/bucket_s) % n_buckets` per cell call.
        if self._min_cov_table:
            self._cur_thresholds: dict[str, int] = self._min_cov_table[0]
        else:
            self._cur_thresholds = {}

        # Live set of cells in coverage deficit.
        # All cells start in deficit (coverage=0 < any min_cov ≥ 1).
        self._deficit_set: set[str] = set(demand_cells)

        # Optional drain debug: when set, append (current_time, lookahead_bucket, chose_coverage)
        # for every select_target call when current_time > drain_duration_s.
        self._drain_debug_list: Optional[list] = None
        self._drain_duration_s: float = 86400.0

    def set_drain_debug(self, L: Optional[list], duration_s: float = 86400.0) -> None:
        """When L is not None, log (current_time, lookahead_bucket, chose_coverage_target)
        for every select_target call with current_time > duration_s (drain phase)."""
        self._drain_debug_list = L
        self._drain_duration_s = duration_s

    # ------------------------------------------------------------------
    # Time-varying min_cov helpers (Option B)
    # ------------------------------------------------------------------

    def _min_cov_for(self, cell: str, current_time: float) -> int:
        """Return the required min coverage for cell at current_time.

        Uses _cur_thresholds (refreshed at bucket boundaries) — pure dict.get,
        no arithmetic.  Falls back to static min_coverage when Option B is off.
        """
        return self._cur_thresholds.get(cell, self.min_coverage)

    def get_cell_state(self, cell: str, current_time: float) -> dict:
        """Return coverage state for a demand cell (for unserved diagnostics).

        Returns dict: coverage (int), targeting (int), min_cov (int), in_deficit (bool).
        """
        coverage = self._coverage.get(cell, 0)
        targeting = self._coverage_targeting.get(cell, 0)
        if self._min_cov_table:
            lookahead_bucket = int(
                (current_time + self._lookahead_s) / self._bucket_s
            ) % self._n_buckets
            min_cov = self._min_cov_table[lookahead_bucket].get(cell, self.min_coverage)
        else:
            min_cov = self.min_coverage
        return {
            "coverage": coverage,
            "targeting": targeting,
            "min_cov": min_cov,
            "in_deficit": coverage < min_cov,
        }

    def _refresh_deficit_set(self, current_time: float) -> None:
        """Full recompute of _deficit_set using current time-varying min_cov.

        Called at each forecast-bucket boundary (96 times per 24h day) to
        re-classify cells whose threshold has changed.  Incremental updates
        in callbacks handle changes between boundaries.
        """
        self._current_time = current_time
        lookahead_bucket = int(
            (current_time + self._lookahead_s) / self._bucket_s
        ) % self._n_buckets
        # Update cached threshold dict before computing deficit set.
        if self._min_cov_table:
            self._cur_thresholds = self._min_cov_table[lookahead_bucket]
        new_deficit: set[str] = set()
        for cell, cov in self._coverage.items():
            if cov < self._cur_thresholds.get(cell, self.min_coverage):
                new_deficit.add(cell)
        self._deficit_set = new_deficit
        self._last_refresh_bucket = lookahead_bucket

    # ------------------------------------------------------------------
    # Coverage tracking callbacks (called by engine — no current_time arg)
    # ------------------------------------------------------------------

    def on_vehicle_eligible(self, h3_cell: str) -> None:
        """Vehicle at h3_cell enters eligible pool (IDLE or REPOSITIONING)."""
        # Inline _min_cov_for to avoid per-cell Python function call overhead
        # (this loop runs O(covered_cells) ≈ 200 times per call).
        thresholds = self._cur_thresholds
        mc = self.min_coverage
        coverage = self._coverage
        deficit = self._deficit_set
        for dest in self._covered_by.get(h3_cell, frozenset()):
            if dest in coverage:
                coverage[dest] += 1
                if coverage[dest] >= thresholds.get(dest, mc):
                    deficit.discard(dest)

    def on_vehicle_ineligible(self, h3_cell: str) -> None:
        """Vehicle at h3_cell leaves eligible pool (dispatched / charging)."""
        thresholds = self._cur_thresholds
        mc = self.min_coverage
        coverage = self._coverage
        deficit = self._deficit_set
        for dest in self._covered_by.get(h3_cell, frozenset()):
            if dest in coverage:
                coverage[dest] = max(0, coverage[dest] - 1)
                if coverage[dest] < thresholds.get(dest, mc):
                    deficit.add(dest)

    def on_vehicle_move(self, old_h3: str, new_h3: str) -> None:
        """Called when a REPOSITIONING vehicle arrives at new_h3.

        Updates coverage counters without changing eligible count (the vehicle
        remains eligible throughout repositioning).
        """
        thresholds = self._cur_thresholds
        mc = self.min_coverage
        coverage = self._coverage
        deficit = self._deficit_set
        for dest in self._covered_by.get(old_h3, frozenset()):
            if dest in coverage:
                coverage[dest] = max(0, coverage[dest] - 1)
                if coverage[dest] < thresholds.get(dest, mc):
                    deficit.add(dest)
        for dest in self._covered_by.get(new_h3, frozenset()):
            if dest in coverage:
                coverage[dest] += 1
                if coverage[dest] >= thresholds.get(dest, mc):
                    deficit.discard(dest)

    # ------------------------------------------------------------------
    # BaseRepositioningPolicy interface
    # ------------------------------------------------------------------

    def on_request_arrival(self, h3_cell: str, current_time: float) -> None:
        self._inner.on_request_arrival(h3_cell, current_time)

    def release_target(self, h3_cell: str) -> None:
        self._inner.release_target(h3_cell)
        if h3_cell in self._coverage_targeting:
            self._coverage_targeting[h3_cell] = max(
                0, self._coverage_targeting[h3_cell] - 1
            )

    def select_target(
        self,
        vehicle: Vehicle,
        current_time: float,
        routing: "RoutingCache",
        vehicle_index: Optional["VehicleIndex"] = None,
    ) -> Optional[str]:
        if current_time - vehicle.last_became_idle_time < self._inner.min_idle_s:
            return None

        # Refresh deficit set at forecast-bucket boundaries so time-varying
        # min_cov thresholds stay current between vehicle state callbacks.
        lookahead_bucket: int = -1
        if self._timed_forecast is not None:
            lookahead_bucket = int(
                (current_time + self._lookahead_s) / self._bucket_s
            ) % self._n_buckets
            if lookahead_bucket != self._last_refresh_bucket:
                self._refresh_deficit_set(current_time)
        else:
            self._current_time = current_time

        # Phase 1: fill road-network coverage deficits
        deficit_target = self._select_coverage_target(vehicle, current_time, routing)
        if self._drain_debug_list is not None and current_time > self._drain_duration_s:
            self._drain_debug_list.append((current_time, lookahead_bucket, deficit_target is not None))
        if deficit_target is not None:
            return deficit_target

        # Phase 2: fall through to demand-score policy
        return self._inner.select_target(vehicle, current_time, routing, vehicle_index)

    # ------------------------------------------------------------------
    # Internal: choose the best reachable deficit cell
    # ------------------------------------------------------------------

    def _select_coverage_target(
        self,
        vehicle: Vehicle,
        current_time: float,
        routing: "RoutingCache",
    ) -> Optional[str]:
        if not self._deficit_set:
            return None

        best_cell: Optional[str] = None
        best_score: float = float("-inf")

        for cell in sorted(self._deficit_set):
            if cell == vehicle.current_h3:
                continue

            # Cap in-flight vehicles at the actual deficit size: don't send more
            # vehicles than needed to close the gap.  This replaces the old
            # fixed coverage_max_vehicles_targeting=1 cap which prevented
            # pre-positioning multiple vehicles before a burst.
            needed = self._min_cov_for(cell, current_time) - self._coverage[cell]
            if needed <= 0:
                continue
            if self._coverage_targeting.get(cell, 0) >= needed:
                continue

            travel_s, _ = routing.get(vehicle.current_h3, cell)
            if travel_s > self._coverage_travel_s:
                continue

            score = self._inner.blended_score(cell, current_time)
            if score > best_score:
                best_score = score
                best_cell = cell

        if best_cell is not None:
            self._coverage_targeting[best_cell] = (
                self._coverage_targeting.get(best_cell, 0) + 1
            )
            self._inner.vehicles_targeting[best_cell] = (
                self._inner.vehicles_targeting.get(best_cell, 0) + 1
            )

        return best_cell

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------

    @property
    def deficit_count(self) -> int:
        """Number of demand cells currently below their min_cov threshold."""
        t = self._current_time
        return sum(
            1 for cell, cov in self._coverage.items()
            if cov < self._min_cov_for(cell, t)
        )

    @property
    def zero_coverage_count(self) -> int:
        """Number of demand cells with zero road-reachable eligible vehicles."""
        return sum(1 for v in self._coverage.values() if v == 0)


# ---------------------------------------------------------------------------
# Helper: build covered_by from travel-cache parquet
# ---------------------------------------------------------------------------

def build_covered_by(
    travel_cache_path: str,
    demand_cell_set: set[str],
    max_wait_time_seconds: float,
) -> dict[str, frozenset[str]]:
    """
    Precompute road-network reachability from the travel-cache parquet.

    Returns
    -------
    covered_by : dict[origin_h3 -> frozenset[dest_h3]]
        For each demand-cell origin, the set of demand-cell destinations
        reachable within max_wait_time_seconds by road.

        Self-coverage is always included: a vehicle already at cell X can
        serve a trip at X in 0 seconds, so X ∈ covered_by[X] for every
        demand cell.  Without this, a vehicle parked at X would be invisible
        to the coverage counter for X itself.
    """
    import pandas as pd

    df = pd.read_parquet(
        travel_cache_path,
        columns=["origin_h3", "destination_h3", "time_seconds"],
    )
    df = df[
        (df["time_seconds"] <= max_wait_time_seconds)
        & df["origin_h3"].isin(demand_cell_set)
        & df["destination_h3"].isin(demand_cell_set)
    ]

    covered_by: dict[str, frozenset[str]] = {}
    for origin, group in df.groupby("origin_h3"):
        covered_by[str(origin)] = frozenset(group["destination_h3"].tolist())

    # Self-coverage: vehicle at cell X always covers cell X (0s travel).
    # The travel-cache parquet omits self-routes; add them explicitly.
    for cell in demand_cell_set:
        cell_str = str(cell)
        if cell_str in covered_by:
            covered_by[cell_str] = covered_by[cell_str] | frozenset([cell_str])
        else:
            covered_by[cell_str] = frozenset([cell_str])

    return covered_by
