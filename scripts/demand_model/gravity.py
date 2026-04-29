"""
Gravity model for trip distribution with purpose-specific attraction vectors
and AM/PM commute symmetry.

P(j|i, purpose) = A_j(purpose) * exp(-beta * t_ij) / sum_k(A_k(purpose) * exp(-beta * t_ik))

For commute trips:
  AM hours (before noon): attraction = employment (people go to work)
  PM hours (noon onward): attraction = population (people go home)
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional

import h3
import httpx
import numpy as np
import pandas as pd

from .config import DemandModelConfig

logger = logging.getLogger(__name__)

H3_RES = 8
_METERS_PER_MILE = 1609.344

# When travel time data is unavailable, use H3 grid distance * this factor
_FALLBACK_SPEED_MPH = 25.0
_H3_EDGE_KM = 0.46  # approximate edge length at res 8


def _h3_to_lnglat(cell: str) -> tuple[float, float]:
    lat, lng = h3.cell_to_latlng(cell)
    return lng, lat


class GravityModel:
    """
    Purpose-specific gravity model for trip distribution.

    Builds OD probability matrices for each trip purpose:
      - commute_am: employment-weighted destinations (morning → work)
      - commute_pm: population-weighted destinations (evening → home)
      - social: entertainment + medical + shopping weighted
      - errands: shopping + medical weighted
      - tourism: entertainment + airport weighted
    """

    def __init__(
        self,
        cells: np.ndarray,
        config: DemandModelConfig,
        employment: dict[str, float],
        population: dict[str, float],
        poi_scores: dict[str, dict[str, float]],
        travel_times: Optional[dict[tuple[str, str], float]] = None,
    ):
        """
        cells: array of H3 cell IDs (the "active" cells in the model)
        employment: {h3_cell: job_count}
        population: {h3_cell: pop_count}
        poi_scores: {h3_cell: {category: score}} (entertainment, medical, shopping, hotel, airport, etc.)
        travel_times: {(origin_h3, dest_h3): seconds} — if None, estimated from H3 distance
        """
        self.cells = cells
        self.n_cells = len(cells)
        self.cell_to_idx = {c: i for i, c in enumerate(cells)}
        self.config = config

        # Build attraction vectors
        self._employment = np.array([employment.get(c, 0.0) for c in cells])
        self._population = np.array([population.get(c, 0.0) for c in cells])

        self._entertainment = np.array([
            poi_scores.get(c, {}).get("entertainment", 0.0) for c in cells
        ])
        self._medical = np.array([
            poi_scores.get(c, {}).get("medical", 0.0) for c in cells
        ])
        self._shopping = np.array([
            poi_scores.get(c, {}).get("shopping", 0.0) for c in cells
        ])
        self._hotel = np.array([
            poi_scores.get(c, {}).get("hotel", 0.0) for c in cells
        ])
        self._airport = np.array([
            poi_scores.get(c, {}).get("airport", 0.0) for c in cells
        ])
        self._leisure = np.array([
            poi_scores.get(c, {}).get("leisure", 0.0) for c in cells
        ])

        # Build impedance matrix
        self._impedance = self._build_impedance(travel_times)

        # Pre-compute OD probability matrices
        self._od_commute_am = self._build_od("commute_am")
        self._od_commute_pm = self._build_od("commute_pm")
        self._od_social = self._build_od("social")
        self._od_errands = self._build_od("errands")
        self._od_tourism = self._build_od("tourism")

        logger.info(
            "Gravity model built: %d cells, beta=%.3f",
            self.n_cells, config.beta,
        )

    def _build_impedance(
        self, travel_times: Optional[dict[tuple[str, str], float]]
    ) -> np.ndarray:
        """Build exp(-beta * t_ij) impedance matrix (n_cells × n_cells)."""
        n = self.n_cells
        impedance = np.zeros((n, n), dtype=np.float64)
        beta = self.config.beta

        if travel_times:
            for (o, d), t_sec in travel_times.items():
                i = self.cell_to_idx.get(o)
                j = self.cell_to_idx.get(d)
                if i is not None and j is not None:
                    impedance[i, j] = np.exp(-beta * t_sec / 60.0)

        # Fill missing pairs with H3 grid distance estimate
        for i in range(n):
            for j in range(n):
                if impedance[i, j] == 0.0 and i != j:
                    grid_dist = h3.grid_distance(self.cells[i], self.cells[j])
                    est_km = grid_dist * _H3_EDGE_KM * 1.4  # manhattan factor
                    est_min = (est_km / (_FALLBACK_SPEED_MPH * 1.609)) * 60.0
                    impedance[i, j] = np.exp(-beta * est_min)
                elif i == j:
                    # Self-loops get minimal impedance (very short intra-cell trips)
                    impedance[i, j] = np.exp(-beta * 2.0)

        return impedance

    def _build_attraction(self, purpose: str) -> np.ndarray:
        """Build attraction vector for a given purpose."""
        cfg = self.config

        if purpose == "commute_am":
            # Morning: go to work
            a = cfg.employment_weight * self._employment
        elif purpose == "commute_pm":
            # Evening: go home
            a = self._population
        elif purpose == "social":
            a = (
                cfg.entertainment_weight * self._entertainment
                + cfg.medical_weight * self._medical
                + self._shopping
                + self._leisure
            )
        elif purpose == "errands":
            a = (
                self._shopping * 2.0
                + cfg.medical_weight * self._medical
                + self._entertainment * 0.3
            )
        elif purpose == "tourism":
            a = (
                cfg.entertainment_weight * self._entertainment
                + cfg.airport_boost * self._airport
                + self._leisure
                + self._hotel * 0.5
            )
        else:
            raise ValueError(f"Unknown purpose: {purpose}")

        # Ensure minimum attraction so no cell is completely unreachable
        a = np.maximum(a, 1e-6)
        return a

    def _build_od(self, purpose: str) -> np.ndarray:
        """
        Build OD probability matrix for purpose.
        Result[i, j] = P(destination=j | origin=i, purpose).
        Each row sums to 1.
        """
        attraction = self._build_attraction(purpose)
        # weighted_impedance[i, j] = A_j * f(t_ij)
        weighted = self._impedance * attraction[np.newaxis, :]
        row_sums = weighted.sum(axis=1, keepdims=True)
        row_sums = np.maximum(row_sums, 1e-12)
        od = weighted / row_sums
        return od

    def sample_destinations(
        self,
        rng: np.random.Generator,
        origin_idx: int,
        purpose: str,
        n: int,
    ) -> np.ndarray:
        """
        Sample n destination cell indices for a given origin and purpose.
        Returns array of cell indices into self.cells.
        """
        if purpose == "commute_am":
            probs = self._od_commute_am[origin_idx]
        elif purpose == "commute_pm":
            probs = self._od_commute_pm[origin_idx]
        elif purpose == "social":
            probs = self._od_social[origin_idx]
        elif purpose == "errands":
            probs = self._od_errands[origin_idx]
        elif purpose == "tourism":
            probs = self._od_tourism[origin_idx]
        else:
            raise ValueError(f"Unknown purpose: {purpose}")

        # Guard against numerical issues
        probs = np.maximum(probs, 0.0)
        total = probs.sum()
        if total <= 0:
            return rng.integers(0, self.n_cells, size=n)
        probs = probs / total

        return rng.choice(self.n_cells, size=n, p=probs)

    def get_airport_destination_weights(self) -> tuple[np.ndarray, np.ndarray]:
        """
        Return (cell_array, weight_array) for airport trip destinations.
        Weights combine hotel density + residential population.
        """
        weights = self._hotel * 2.0 + self._population / max(self._population.max(), 1.0) * 5.0
        weights = np.maximum(weights, 1e-6)
        return self.cells, weights


def build_gravity_model(
    config: DemandModelConfig,
    acs_df: pd.DataFrame,
    employment_df: pd.DataFrame,
    poi_scores_df: pd.DataFrame,
    travel_times: Optional[dict[tuple[str, str], float]] = None,
) -> GravityModel:
    """
    Factory: build a GravityModel from the cached data artifacts.

    acs_df: from fetch_census — columns: h3_cell, population, ...
    employment_df: from fetch_census — columns: h3_cell, employment
    poi_scores_df: from fetch_pois.build_poi_scores — columns: h3_cell + category columns
    travel_times: optional precomputed {(o,d): seconds}
    """
    # Collect all active cells
    all_cells = set()
    all_cells.update(acs_df["h3_cell"].tolist())
    all_cells.update(employment_df["h3_cell"].tolist())
    all_cells.update(poi_scores_df["h3_cell"].tolist())
    cells = np.array(sorted(all_cells))

    # Build lookup dicts
    population = dict(zip(acs_df["h3_cell"], acs_df["population"]))
    employment = dict(zip(employment_df["h3_cell"], employment_df["employment"]))

    poi_scores: dict[str, dict[str, float]] = {}
    category_cols = [c for c in poi_scores_df.columns if c != "h3_cell"]
    for _, row in poi_scores_df.iterrows():
        cell = row["h3_cell"]
        poi_scores[cell] = {cat: float(row[cat]) for cat in category_cols}

    return GravityModel(
        cells=cells,
        config=config,
        employment=employment,
        population=population,
        poi_scores=poi_scores,
        travel_times=travel_times,
    )


def load_travel_times_from_cache(
    cache_path: str | None = None,
) -> dict[tuple[str, str], float]:
    """Load precomputed H3 travel times from the sim's routing cache."""
    if cache_path is None:
        _root = Path(__file__).resolve().parents[2]
        cache_path = str(_root / "data" / "h3_travel_cache.parquet")
    if not os.path.exists(cache_path):
        logger.warning("Travel cache not found at %s — using H3 distance estimates", cache_path)
        return {}

    df = pd.read_parquet(
        cache_path,
        columns=["origin_h3", "destination_h3", "time_seconds"],
    )
    result = {}
    for row in df.itertuples(index=False):
        result[(row.origin_h3, row.destination_h3)] = float(row.time_seconds)

    logger.info("Loaded %d travel time pairs from cache", len(result))
    return result
