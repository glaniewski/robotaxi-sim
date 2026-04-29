from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class EventType(str, Enum):
    REQUEST_ARRIVAL = "REQUEST_ARRIVAL"
    REQUEST_EXPIRE = "REQUEST_EXPIRE"
    DISPATCH = "DISPATCH"
    TRIP_START = "TRIP_START"
    POOL_PICKUP = "POOL_PICKUP"         # vehicle detours to pick up second pooled rider
    TRIP_COMPLETE = "TRIP_COMPLETE"
    ARRIVE_DEPOT = "ARRIVE_DEPOT"
    CHARGE_DEPARTURE = "CHARGE_DEPARTURE"
    CHARGING_COMPLETE = "CHARGING_COMPLETE"
    REPOSITION_COMPLETE = "REPOSITION_COMPLETE"
    REPOSITION_ELIGIBLE = "REPOSITION_ELIGIBLE"
    VEHICLE_IDLE = "VEHICLE_IDLE"
    SNAPSHOT = "SNAPSHOT"               # internal: time-series bucket


@dataclass
class Event:
    time: float
    seq: int                            # monotonic insertion counter for stable heap ordering
    type: EventType
    payload: dict[str, Any] = field(default_factory=dict)

    def __lt__(self, other: "Event") -> bool:
        return self.time < other.time or (self.time == other.time and self.seq < other.seq)

    def __le__(self, other: "Event") -> bool:
        return self.time < other.time or (self.time == other.time and self.seq <= other.seq)
