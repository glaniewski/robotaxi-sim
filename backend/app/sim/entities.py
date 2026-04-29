from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class VehicleState(str, Enum):
    IDLE = "IDLE"
    TO_PICKUP = "TO_PICKUP"
    IN_TRIP = "IN_TRIP"
    TO_DEPOT = "TO_DEPOT"
    CHARGING = "CHARGING"
    REPOSITIONING = "REPOSITIONING"


class RequestStatus(str, Enum):
    PENDING = "PENDING"
    SERVED = "SERVED"
    UNSERVED = "UNSERVED"


@dataclass
class Vehicle:
    id: str
    current_h3: str
    state: VehicleState
    soc: float                          # [0, 1]
    battery_kwh: float                  # e.g. 75
    kwh_per_mile: float                 # e.g. 0.30
    charge_target_soc: float = 0.80
    assigned_request_id: Optional[str] = None
    reposition_target_h3: Optional[str] = None
    last_became_idle_time: float = 0.0
    state_entered_time: float = 0.0       # sim time when current state started (for state-time accumulation)
    reposition_start_time: Optional[float] = None  # sim time when REPOSITIONING started
    total_reposition_s: float = 0.0      # scheduled duration of current reposition leg (seconds)

    # State-time accumulation (seconds in each state)
    time_idle_s: float = 0.0
    time_to_pickup_s: float = 0.0
    time_in_trip_s: float = 0.0
    time_repositioning_s: float = 0.0
    time_to_depot_s: float = 0.0
    time_charging_s: float = 0.0

    # Pool state
    pooled_passenger_id: Optional[str] = None  # second rider's request ID while in pool trip

    # Accumulated mileage counters for metrics
    trip_miles: float = 0.0             # revenue miles (IN_TRIP legs)
    pickup_miles: float = 0.0           # deadhead TO_PICKUP miles
    reposition_miles: float = 0.0       # deadhead REPOSITIONING miles
    shared_miles: float = 0.0           # miles driven with 2 passengers aboard
    charge_sessions: int = 0
    # Session start (sim seconds) and effective kW at plug-in; used only for end-of-run SOC interpolation
    charging_session_start_time: Optional[float] = None
    charging_session_kw: Optional[float] = None

    def __post_init__(self) -> None:
        self._soc_per_mile: float = self.kwh_per_mile / self.battery_kwh

    @property
    def remaining_range_miles(self) -> float:
        return self.soc * self.battery_kwh / self.kwh_per_mile

    def energy_for_miles(self, miles: float) -> float:
        """SOC consumed for a given distance."""
        return miles * self._soc_per_mile


@dataclass
class Request:
    id: str
    request_time: float                 # seconds from sim start
    origin_h3: str
    destination_h3: str
    max_wait_time_seconds: float
    status: RequestStatus = RequestStatus.PENDING
    latest_departure_time: Optional[float] = None
    pooled_allowed: bool = False

    # Filled in at dispatch / serve time
    dispatched_at: Optional[float] = None
    served_at: Optional[float] = None
    assigned_vehicle_id: Optional[str] = None
    pool_matched: bool = False          # True if this request was served as a pool partner

    # Trip characteristics (filled at TRIP_START, always direct O-D regardless of pool detour)
    trip_duration_seconds: Optional[float] = None   # OSRM travel time for direct O-D route
    trip_miles_direct: Optional[float] = None       # OSRM distance for direct O-D route (miles)

    @property
    def actual_wait_seconds(self) -> Optional[float]:
        if self.served_at is not None:
            return self.served_at - self.request_time
        return None


@dataclass
class Depot:
    id: str
    h3_cell: str
    chargers_count: int
    charger_kw: float
    site_power_kw: float
    active_chargers: int = 0
    queue: list[str] = field(default_factory=list)  # vehicle ids FIFO

    def effective_charger_kw(self) -> float:
        if self.active_chargers == 0:
            return self.charger_kw
        return min(self.charger_kw, self.site_power_kw / self.active_chargers)
