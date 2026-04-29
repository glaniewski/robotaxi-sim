"""
DemandScorePolicy — blended reactive + forecast repositioning.

Reactive score: exponential decay per H3 cell, incremented on each
REQUEST_ARRIVAL at that cell.

Forecast score: expected arrivals in the next forecast_horizon_minutes,
derived from a historical frequency table {h3_cell: arrivals_per_second}
built from the loaded request dataset at startup.

Blended: target_score[h3] = alpha * reactive + (1 - alpha) * forecast

Target selection ranks all demand cells by blended score, takes the top-K,
then chooses the one maximising:
    utility = blended_score - lambda * travel_time_minutes
subject to max_vehicles_targeting_cell and max_reposition_travel_minutes.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from ..entities import Vehicle
from .base import BaseRepositioningPolicy

if TYPE_CHECKING:
    from ..dispatch import VehicleIndex
    from ..routing import RoutingCache


class DemandScorePolicy(BaseRepositioningPolicy):

    def __init__(
        self,
        alpha: float,
        half_life_minutes: float,
        forecast_horizon_minutes: float,
        max_reposition_travel_minutes: float,
        max_vehicles_targeting_cell: int,
        min_idle_minutes: float,
        top_k_cells: int,
        reposition_lambda: float = 0.05,
        forecast_table: Optional[dict[str, float]] = None,
    ) -> None:
        self.alpha = alpha
        self.half_life_s = half_life_minutes * 60.0
        self.forecast_horizon_s = forecast_horizon_minutes * 60.0
        self.max_travel_s = max_reposition_travel_minutes * 60.0
        self.max_vehicles_targeting = max_vehicles_targeting_cell
        self.min_idle_s = min_idle_minutes * 60.0
        self.top_k = top_k_cells
        self.lambda_ = reposition_lambda

        # {h3_cell: (score, last_update_time)}
        self._reactive: dict[str, tuple[float, float]] = {}
        # {h3_cell: arrivals_per_second}
        self._forecast_table: dict[str, float] = forecast_table or {}
        # {h3_cell: count of vehicles currently heading there}
        self.vehicles_targeting: dict[str, int] = {}

        # Cache for sorted cell ranking — recomputed at most every _RANK_TTL sim-seconds.
        # Reactive scores decay slowly (half_life=45min), so 30s TTL is safe.
        self._RANK_TTL: float = 30.0
        self._rank_cache_time: float = -9999.0
        self._rank_top_k: list[str] = []
        self._rank_scores: dict[str, float] = {}
        # Fixed set of cells that appear in the forecast table (never changes post-init)
        self._forecast_cells: frozenset[str] = frozenset(self._forecast_table.keys())

    # ------------------------------------------------------------------
    # Reactive score maintenance
    # ------------------------------------------------------------------

    def on_request_arrival(self, h3_cell: str, current_time: float) -> None:
        score, last_t = self._reactive.get(h3_cell, (0.0, current_time))
        delta_minutes = (current_time - last_t) / 60.0
        decay = 0.5 ** (delta_minutes / (self.half_life_s / 60.0))
        self._reactive[h3_cell] = (score * decay + 1.0, current_time)

    def _reactive_score(self, h3_cell: str, current_time: float) -> float:
        if h3_cell not in self._reactive:
            return 0.0
        score, last_t = self._reactive[h3_cell]
        delta_minutes = (current_time - last_t) / 60.0
        decay = 0.5 ** (delta_minutes / (self.half_life_s / 60.0))
        return score * decay

    def _forecast_score(self, h3_cell: str) -> float:
        return self._forecast_table.get(h3_cell, 0.0) * self.forecast_horizon_s

    def blended_score(self, h3_cell: str, current_time: float) -> float:
        r = self._reactive_score(h3_cell, current_time)
        f = self._forecast_score(h3_cell)
        return self.alpha * r + (1.0 - self.alpha) * f

    # ------------------------------------------------------------------
    # Target selection
    # ------------------------------------------------------------------

    def _refresh_rank_cache(self, current_time: float) -> None:
        """Recompute the sorted cell ranking.  Called at most every _RANK_TTL seconds."""
        all_cells = self._forecast_cells | set(self._reactive.keys())
        scores = {c: self.blended_score(c, current_time) for c in all_cells}
        scores = {c: s for c, s in scores.items() if s > 0}
        self._rank_scores = scores
        self._rank_top_k = sorted(scores, key=lambda c: (-scores[c], c))[: self.top_k]
        self._rank_cache_time = current_time

    def select_target(
        self,
        vehicle: Vehicle,
        current_time: float,
        routing: "RoutingCache",
        vehicle_index: Optional["VehicleIndex"] = None,
    ) -> Optional[str]:
        if current_time - vehicle.last_became_idle_time < self.min_idle_s:
            return None

        # Use cached ranking if fresh enough; otherwise recompute.
        if current_time - self._rank_cache_time > self._RANK_TTL:
            self._refresh_rank_cache(current_time)

        if not self._rank_top_k:
            return None

        top_cells = self._rank_top_k
        candidates = self._rank_scores

        best_cell: Optional[str] = None
        best_utility: float = float("-inf")

        for cell in top_cells:
            if cell == vehicle.current_h3:
                continue
            if self.vehicles_targeting.get(cell, 0) >= self.max_vehicles_targeting:
                continue
            travel_s, _ = routing.get(vehicle.current_h3, cell)
            if travel_s > self.max_travel_s:
                continue
            utility = candidates[cell] - self.lambda_ * (travel_s / 60.0)
            if utility > best_utility:
                best_utility = utility
                best_cell = cell

        if best_cell is not None:
            self.vehicles_targeting[best_cell] = self.vehicles_targeting.get(best_cell, 0) + 1

        return best_cell

    def release_target(self, h3_cell: str) -> None:
        if h3_cell and h3_cell in self.vehicles_targeting:
            self.vehicles_targeting[h3_cell] = max(0, self.vehicles_targeting[h3_cell] - 1)
