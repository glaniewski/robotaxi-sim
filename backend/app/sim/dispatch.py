from __future__ import annotations

import h3 as _h3
from typing import TYPE_CHECKING, Optional

from .entities import Request, RequestStatus, Vehicle, VehicleState

if TYPE_CHECKING:
    from .routing import RoutingCache

# Approximate seconds of travel per H3 resolution-8 ring step.
# Each ring ≈ 0.92 km apart; assuming ~40 km/h average urban speed → ~83 s/ring.
_H3_SECS_PER_RING: float = 83.0
_DISPATCH_ELIGIBLE_STATES = (VehicleState.IDLE, VehicleState.REPOSITIONING)


class VehicleIndex:
    """
    Spatial index: H3 cell → set of eligible vehicle IDs (IDLE or REPOSITIONING).

    Reduces dispatch candidate search from O(V) to O(k) where k is the number
    of vehicles within the reachable H3 radius, typically 5–20× smaller than V.

    Callers are responsible for keeping the index in sync:
      add(v_id, cell)          when vehicle becomes IDLE/REPOSITIONING
      remove(v_id, cell)       when vehicle leaves eligible state (dispatched / charging)
      move(v_id, old, new)     when vehicle finishes repositioning (cell change)
    """

    def __init__(self) -> None:
        self._cells: dict[str, set[str]] = {}
        # Pre-computed grid_disk results keyed by (origin_h3, radius).
        # At H3-r8, a given origin cell always produces the same disk set, so we
        # cache the list once and reuse it every subsequent call — eliminating the
        # repeated h3.grid_disk() cost which profiling showed at ~51 s / 673k calls.
        self._disk_cache: dict[tuple[str, int], list[str]] = {}

    def add(self, vehicle_id: str, h3_cell: str) -> None:
        if h3_cell not in self._cells:
            self._cells[h3_cell] = set()
        self._cells[h3_cell].add(vehicle_id)

    def contains(self, vehicle_id: str, h3_cell: str) -> bool:
        bucket = self._cells.get(h3_cell)
        return bucket is not None and vehicle_id in bucket

    def remove(self, vehicle_id: str, h3_cell: str) -> None:
        bucket = self._cells.get(h3_cell)
        if bucket:
            bucket.discard(vehicle_id)

    def move(self, vehicle_id: str, from_cell: str, to_cell: str) -> None:
        self.remove(vehicle_id, from_cell)
        self.add(vehicle_id, to_cell)

    def ring_cells(self, origin_h3: str, ring: int) -> list[str]:
        """H3 cells in ring k around origin_h3 (cached, no vehicle lookup).

        Useful for the vehicle-first dispatch loop where we need the cell list
        to look up pending requests, not vehicles.
        """
        key = (origin_h3, ring)
        shell = self._disk_cache.get(key)
        if shell is None:
            shell = [origin_h3] if ring == 0 else list(_h3.grid_ring(origin_h3, ring))
            self._disk_cache[key] = shell
        return shell

    def ring_shell(self, origin_h3: str, ring: int) -> list[str]:
        """Vehicle IDs in exactly ring k (the shell only, not a filled disk).

        Uses h3.grid_ring for ring > 0 and the cell itself for ring == 0.
        Results are cached by (origin_h3, ring) since H3 topology is fixed.
        """
        result: list[str] = []
        for cell in self.ring_cells(origin_h3, ring):
            bucket = self._cells.get(cell)
            if bucket:
                result.extend(bucket)
        return result

    def all_eligible_ids(self) -> list[str]:
        """All eligible vehicle IDs (order undefined; set PYTHONHASHSEED=0 for deterministic runs)."""
        result: list[str] = []
        for bucket in self._cells.values():
            if bucket:
                result.extend(bucket)
        return result

    def candidates(self, origin_h3: str, max_radius: int) -> list[str]:
        """All eligible vehicle IDs within max_radius rings of origin_h3.

        Kept for callers outside dispatch (e.g. coverage_floor) that still
        need a flat candidate list without ring-by-ring expansion.
        """
        result: list[str] = []
        for ring in range(max_radius + 1):
            result.extend(self.ring_shell(origin_h3, ring))
        return result


def find_best_vehicle(
    request: Request,
    vehicles: dict[str, Vehicle],
    routing: "RoutingCache",
    current_time: float,
    soc_buffer: float,
    strategy: str = "nearest",
    first_feasible_threshold_seconds: float = 300.0,
    vehicle_index: Optional[VehicleIndex] = None,
    soc_min: float = 0.0,
) -> Optional[Vehicle]:
    """
    Select a vehicle to dispatch for a request.

    Strategies
    ----------
    nearest
        Return the eligible vehicle with the minimum pickup ETA within max_wait.
    first_feasible
        Return the first eligible vehicle with ETA ≤ first_feasible_threshold_seconds.
        Falls back to nearest-within-max-wait if no vehicle meets the threshold.

    Spatial index (vehicle_index)
    -----------------------------
    When provided, vehicles are evaluated ring-by-ring (shell k before shell k+1)
    using h3.grid_ring.  All shells are cached after the first call (same H3 topology
    every time), eliminating repeated h3.grid_ring/grid_disk computation.
    Expansion stops as soon as best_eta ≤ (ring+1)*_H3_SECS_PER_RING — no outer shell
    can improve on a vehicle whose ETA is already ≤ the next ring's lower-bound travel
    time.  In the common case a vehicle is found within 1-3 rings, reducing routing.get
    calls from ~500 (filled disk) to ~10-30 per dispatch.
    """
    best_vehicle: Optional[Vehicle] = None
    best_eta: float = float("inf")

    if vehicle_index is not None:
        # Ring-by-ring expansion: evaluate the shell at ring k before expanding to k+1.
        # Termination: once best_eta ≤ (ring+1)*_H3_SECS_PER_RING, no outer shell can
        # improve on it (road ETA ≥ H3 ring lower-bound), so return early.
        # This preserves nearest-vehicle semantics while avoiding routing.get calls for
        # the ~400 far-away candidates that the old filled-disk approach evaluated.
        #
        # Use REMAINING wait time (not static max_wait) to bound the search.
        # A request with 60s left only needs a vehicle reachable in 60s; using
        # the full 600s window would search ring-10 (379 vehicles) for every
        # last-chance retry — O(379) wasted routing.get calls per failed search.
        remaining_wait = request.max_wait_time_seconds - (current_time - request.request_time)
        max_radius = int(remaining_wait / _H3_SECS_PER_RING) + 2

        # Hoist request-constant values out of the inner loop.
        _, trip_dist_m = routing.get(request.origin_h3, request.destination_h3)
        trip_miles = trip_dist_m / 1609.344

        for ring in range(max_radius + 1):
            shell_ids = vehicle_index.ring_shell(request.origin_h3, ring)
            for vid in shell_ids:
                vehicle = vehicles.get(vid)
                if vehicle is None:
                    continue
                if vehicle.state not in _DISPATCH_ELIGIBLE_STATES:
                    continue
                if vehicle.soc < soc_min:
                    continue
                if vehicle.state == VehicleState.REPOSITIONING:
                    remaining_s = max(
                        0.0,
                        vehicle.total_reposition_s - (current_time - vehicle.reposition_start_time),
                    )
                    # Skip routing.get entirely if reposition alone exceeds remaining wait.
                    if remaining_s >= remaining_wait:
                        continue
                else:
                    remaining_s = 0.0
                pickup_time_s, pickup_dist_m = routing.get(vehicle.current_h3, request.origin_h3)
                pickup_time_s += remaining_s
                if pickup_time_s > remaining_wait:
                    continue
                pickup_miles = pickup_dist_m / 1609.344
                soc_needed = vehicle.energy_for_miles(pickup_miles + trip_miles) + soc_buffer
                if vehicle.soc < soc_needed:
                    continue
                if strategy == "first_feasible" and pickup_time_s <= first_feasible_threshold_seconds:
                    return vehicle
                if pickup_time_s < best_eta:
                    best_eta = pickup_time_s
                    best_vehicle = vehicle

            # Early-exit: best found so far can't be beaten by any outer ring.
            if best_vehicle is not None and best_eta <= (ring + 1) * _H3_SECS_PER_RING:
                break

        return best_vehicle

    # Fallback: no spatial index — scan all vehicles (O(V), only used in tests).
    _, trip_dist_m = routing.get(request.origin_h3, request.destination_h3)
    trip_miles = trip_dist_m / 1609.344
    for vehicle in vehicles.values():
        if vehicle.state not in _DISPATCH_ELIGIBLE_STATES:
            continue
        if vehicle.soc < soc_min:
            continue
        pickup_time_s, pickup_dist_m = routing.get(vehicle.current_h3, request.origin_h3)
        if vehicle.state == VehicleState.REPOSITIONING:
            remaining_s = max(
                0.0,
                vehicle.total_reposition_s - (current_time - vehicle.reposition_start_time),
            )
            pickup_time_s += remaining_s
        if pickup_time_s > request.max_wait_time_seconds:
            continue
        pickup_miles = pickup_dist_m / 1609.344
        if vehicle.soc < vehicle.energy_for_miles(pickup_miles + trip_miles) + soc_buffer:
            continue
        if strategy == "first_feasible" and pickup_time_s <= first_feasible_threshold_seconds:
            return vehicle
        if pickup_time_s < best_eta:
            best_eta = pickup_time_s
            best_vehicle = vehicle
    return best_vehicle


def find_pool_match(
    origin_h3: str,
    primary_dest_h3: str,
    pending_requests: dict[str, Request],
    routing: "RoutingCache",
    max_detour_pct: float,
    current_time: float,
) -> Optional[Request]:
    """
    Find the best pooled_allowed pending request B that a vehicle at origin_h3
    (heading to primary_dest_h3) can pick up within the detour budget.

    Detour is measured as the triangle overhead:
        overhead = dist(origin → B.origin) + dist(B.origin → primary_dest) - dist(origin → primary_dest)
        detour_pct = overhead / dist(origin → primary_dest)

    Also checks that B can still be served within its max_wait_time_seconds.
    Returns the lowest-detour match, or None.
    """
    _, direct_dist_m = routing.get(origin_h3, primary_dest_h3)
    if direct_dist_m <= 0:
        return None

    best_req: Optional[Request] = None
    best_detour: float = float("inf")

    for req in pending_requests.values():
        if not req.pooled_allowed or req.status != RequestStatus.PENDING:
            continue

        time_to_b_s, dist_to_b_m = routing.get(origin_h3, req.origin_h3)

        # B must still be reachable within its wait window
        if current_time + time_to_b_s - req.request_time > req.max_wait_time_seconds:
            continue

        # Triangle overhead check
        _, dist_b_to_primary_m = routing.get(req.origin_h3, primary_dest_h3)
        overhead_m = dist_to_b_m + dist_b_to_primary_m - direct_dist_m
        detour_pct = overhead_m / direct_dist_m

        if detour_pct <= max_detour_pct and detour_pct < best_detour:
            best_detour = detour_pct
            best_req = req

    return best_req
