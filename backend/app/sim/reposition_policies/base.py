"""
Abstract base class for all repositioning policies.

Every policy must implement three methods:
  select_target   — called on VEHICLE_IDLE; returns target H3 cell or None
  on_request_arrival — called on REQUEST_ARRIVAL; update demand signal
  release_target  — called on REPOSITION_COMPLETE or dispatch preemption

The engine holds a reference typed as BaseRepositioningPolicy and never
inspects the concrete class — swap policies by passing a different instance.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Optional

from ..entities import Vehicle

if TYPE_CHECKING:
    from ..dispatch import VehicleIndex
    from ..routing import RoutingCache


class BaseRepositioningPolicy(ABC):

    @abstractmethod
    def select_target(
        self,
        vehicle: Vehicle,
        current_time: float,
        routing: "RoutingCache",
        vehicle_index: Optional["VehicleIndex"] = None,
    ) -> Optional[str]:
        """
        Choose the best H3 cell to reposition this vehicle toward.
        Returns cell string or None (stay idle).
        """

    @abstractmethod
    def on_request_arrival(self, h3_cell: str, current_time: float) -> None:
        """Update internal demand signal when a request arrives at h3_cell."""

    @abstractmethod
    def release_target(self, h3_cell: str) -> None:
        """
        Decrement the targeting count for h3_cell.
        Called when a vehicle finishes repositioning there or is preempted.
        """
