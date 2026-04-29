"""
DemandModelConfig — all 18 tunable parameters for the demand generation model.

Every parameter is designed for the AI experimenter agent to form hypotheses about
and sweep alongside fleet/infrastructure parameters.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field, asdict
from typing import Literal, Optional


DayType = Literal["weekday", "saturday", "sunday"]


@dataclass
class DemandModelConfig:
    # --- Intensity and duration ---
    demand_intensity: float = 1.0       # master volume: trips/person/hour multiplier (0.1-5.0)
    duration_hours: int = 24            # hours of demand to generate (1-168)

    # --- Temporal shape ---
    day_type: DayType = "weekday"       # selects NHTS temporal profile
    peak_sharpness: float = 1.0         # exponent on temporal profile (0.1-3.0)

    # --- Spatial / gravity model ---
    beta: float = 0.08                  # distance decay in gravity model (0.01-0.30)
    commute_weight: float = 0.40        # fraction of demand from LODES commute pairs (0.0-1.0)

    # --- Transit competition ---
    transit_suppression: float = 0.3    # reduction from good transit access (0.0-1.0)

    # --- Tourism and special attractors ---
    tourism_intensity: float = 1.0      # visitor trip multiplier (0.0-5.0)
    airport_boost: float = 1.0          # ABIA mode-share multiplier (0.5-10.0); 1.0 ≈ 6% of pax

    # --- Attraction weights by category ---
    entertainment_weight: float = 1.5   # nightlife + restaurants (0.0-5.0)
    employment_weight: float = 1.0      # jobs from LODES (0.0-5.0)
    medical_weight: float = 0.8         # hospitals/clinics (0.0-3.0)

    # --- Demographics ---
    carfree_boost: float = 2.0          # trip multiplier for car-free households (1.0-5.0)

    # --- Event injection (all null = no event) ---
    event_h3: Optional[str] = None
    event_start_hour: Optional[float] = None
    event_duration_hours: Optional[float] = None
    event_multiplier: Optional[float] = None

    # --- Reproducibility ---
    seed: int = 42

    def validate(self) -> None:
        """Raise ValueError if any parameter is out of range."""
        checks = [
            (0.1 <= self.demand_intensity <= 5.0, "demand_intensity must be 0.1-5.0"),
            (1 <= self.duration_hours <= 168, "duration_hours must be 1-168"),
            (self.day_type in ("weekday", "saturday", "sunday"), "day_type must be weekday/saturday/sunday"),
            (0.1 <= self.peak_sharpness <= 3.0, "peak_sharpness must be 0.1-3.0"),
            (0.01 <= self.beta <= 0.30, "beta must be 0.01-0.30"),
            (0.0 <= self.commute_weight <= 1.0, "commute_weight must be 0.0-1.0"),
            (0.0 <= self.transit_suppression <= 1.0, "transit_suppression must be 0.0-1.0"),
            (0.0 <= self.tourism_intensity <= 5.0, "tourism_intensity must be 0.0-5.0"),
            (0.5 <= self.airport_boost <= 10.0, "airport_boost must be 0.5-10.0"),
            (0.0 <= self.entertainment_weight <= 5.0, "entertainment_weight must be 0.0-5.0"),
            (0.0 <= self.employment_weight <= 5.0, "employment_weight must be 0.0-5.0"),
            (0.0 <= self.medical_weight <= 3.0, "medical_weight must be 0.0-3.0"),
            (1.0 <= self.carfree_boost <= 5.0, "carfree_boost must be 1.0-5.0"),
        ]
        if self.event_h3 is not None:
            checks.extend([
                (self.event_start_hour is not None, "event_start_hour required when event_h3 is set"),
                (self.event_duration_hours is not None, "event_duration_hours required when event_h3 is set"),
                (self.event_multiplier is not None, "event_multiplier required when event_h3 is set"),
            ])
            if self.event_start_hour is not None:
                checks.append((0 <= self.event_start_hour <= 23, "event_start_hour must be 0-23"))
            if self.event_duration_hours is not None:
                checks.append((1 <= self.event_duration_hours <= 8, "event_duration_hours must be 1-8"))
            if self.event_multiplier is not None:
                checks.append((2.0 <= self.event_multiplier <= 20.0, "event_multiplier must be 2.0-20.0"))

        for ok, msg in checks:
            if not ok:
                raise ValueError(msg)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> DemandModelConfig:
        known_fields = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in d.items() if k in known_fields}
        return cls(**filtered)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)

    @classmethod
    def from_json(cls, s: str) -> DemandModelConfig:
        return cls.from_dict(json.loads(s))

    def fingerprint(self) -> str:
        """Deterministic hash of all parameters — used for parquet caching."""
        canonical = json.dumps(self.to_dict(), sort_keys=True)
        return hashlib.sha256(canonical.encode()).hexdigest()[:12]

    @property
    def has_event(self) -> bool:
        return self.event_h3 is not None
