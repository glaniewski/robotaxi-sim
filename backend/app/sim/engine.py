from __future__ import annotations

import heapq
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

import numpy as np

from .dispatch import _H3_SECS_PER_RING, VehicleIndex, find_best_vehicle, find_pool_match
from .entities import Depot, Request, RequestStatus, Vehicle, VehicleState
from .events import Event, EventType
from .metrics import compute_metrics, compute_timeseries
from .reposition_policies import BaseRepositioningPolicy, CoverageFloorPolicy
from .routing import RoutingCache


@dataclass
class SimConfig:
    duration_minutes: float = 360.0
    seed: int = 0
    # Fleet
    fleet_size: int = 50
    battery_kwh: float = 75.0
    kwh_per_mile: float = 0.20
    soc_initial: float = 0.80
    soc_min: float = 0.20
    soc_charge_start: float = 0.80
    soc_target: float = 0.80
    soc_buffer: float = 0.05
    # Demand
    max_wait_time_seconds: float = 600.0
    # Economics — itemized cost model
    electricity_cost_per_kwh: float = 0.068
    demand_charge_per_kw_month: float = 13.56
    maintenance_cost_per_mile: float = 0.03
    insurance_cost_per_vehicle_day: float = 4.00
    teleops_cost_per_vehicle_day: float = 3.50
    cleaning_cost_per_vehicle_day: float = 6.00
    base_vehicle_cost_usd: float = 22_500.0
    battery_cost_per_kwh: float = 100.0
    vehicle_cost_usd: float = 30_000.0
    vehicle_lifespan_years: float = 5.0
    cost_per_site_day: float = 250.0
    # Revenue
    revenue_base: float = 2.50
    revenue_per_mile: float = 1.50
    revenue_per_minute: float = 0.35
    revenue_min_fare: float = 5.00
    pool_discount_pct: float = 0.25
    # Repositioning
    reposition_enabled: bool = True
    reposition_alpha: float = 0.6
    reposition_half_life_minutes: float = 45.0
    reposition_forecast_horizon_minutes: float = 30.0
    max_reposition_travel_minutes: float = 12.0
    max_vehicles_targeting_cell: int = 3
    reposition_min_idle_minutes: float = 2.0
    reposition_top_k_cells: int = 50
    reposition_lambda: float = 0.05     # travel cost weight in utility = score - lambda * travel_min
    # Dispatch strategy
    dispatch_strategy: str = "nearest"
    first_feasible_threshold_seconds: float = 300.0
    # Pooling
    max_detour_pct: float = 0.0         # 0 = pooling disabled; >0 enables pool matching
    # Time series
    timeseries_bucket_minutes: float = 1.0
    # Unserved diagnostics (min_eta at arrival/expiry, etc.) — costs extra time when True
    collect_unserved_diagnostics: bool = False
    # Event log for diagnostics: list of (event.time, event.type.value) for every event
    collect_event_log: bool = False
    # Depot plug contention: "jit" = no FIFO queue (re-plan via VEHICLE_IDLE if busy);
    # "fifo" = wait in depot.queue (TO_DEPOT) until CHARGING_COMPLETE frees a plug.
    charging_queue_policy: str = "jit"
    # Depot choice for charging reservation: "fastest" = min depart_time only;
    # "fastest_balanced" = among depots within slack of best depart_time, min pressure
    # (queue + active_chargers + reservation count), then depart_time, travel, id.
    charging_depot_selection: str = "fastest"
    charging_depot_balance_slack_minutes: float = 3.0
    # Supply-aware charging: when eligible/pending ratio exceeds this, let
    # low-SOC idle vehicles peel off to charge even while requests are pending.
    charge_supply_ratio: float = 2.0
    # Hard cap: at most this fraction of the fleet may be in TO_DEPOT or
    # CHARGING state simultaneously (prevents mass charging during lulls).
    max_concurrent_charging_pct: float = 0.15
    # Minimum plug dwell time per charging session (minutes). 0 = no minimum.
    # Session length is max(energy-to-target time, this floor). SOC still snaps
    # to soc_target at CHARGING_COMPLETE (no partial-SOC during live events; see
    # _apply_horizon_charging_soc_for_metrics for end-of-run metric interpolation).
    min_plug_duration_minutes: float = 0.0


class SimulationEngine:
    """
    Discrete-event simulation engine.

    Usage:
        engine = SimulationEngine(config, vehicles, requests, depots, routing)
        result = engine.run()
    """

    def __init__(
        self,
        config: SimConfig,
        vehicles: list[Vehicle],
        requests: list[Request],
        depots: list[Depot],
        routing: RoutingCache,
        reposition_policy: Optional[BaseRepositioningPolicy] = None,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> None:
        self.config = config
        self.vehicles = {v.id: v for v in vehicles}
        self.requests = {r.id: r for r in requests}
        self.depots = depots
        self.routing = routing

        self._rng = np.random.default_rng(config.seed)
        self._seq = 0
        self._heap: list[Event] = []
        self._current_time: float = 0.0
        self._event_log: list[tuple[float, str]] = []
        self._drain_dispatch_log: list[tuple[float, int, int]] = []

        # Dispatch guard: timestamp at which a DISPATCH is already scheduled
        self._dispatch_scheduled_at: Optional[float] = None

        # Pending requests set (request ids)
        self._pending: set[str] = set()

        # Depot queue wait tracking for p90 metric
        self._depot_queue_waits: list[float] = []  # minutes waiting in queue
        self._depot_enqueue_time: dict[str, float] = {}  # vehicle_id -> enqueue_time
        # Peak FIFO queue depth (vehicles waiting for a plug, not charging)
        self._depot_queue_max_concurrent: int = 0  # max sum of len(queue) across depots
        self._depot_queue_max_at_site: int = 0  # max len(queue) at any single depot
        # Charger utilization tracking
        self._charger_busy_seconds: float = 0.0
        self._charger_busy_seconds_by_depot: dict[str, float] = {d.id: 0.0 for d in depots}
        self._total_charger_capacity_s: float = sum(d.chargers_count for d in depots) * config.duration_minutes * 60.0
        # Active charging end times and future reservations (no-queue policy)
        self._active_charge_end_by_vehicle: dict[str, float] = {}
        self._active_charge_end_by_depot: dict[str, dict[str, float]] = {d.id: {} for d in depots}
        self._charge_reservation_by_vehicle: dict[str, dict[str, float | str | int]] = {}
        self._charge_reservations_by_depot: dict[str, dict[str, dict[str, float | str | int]]] = {d.id: {} for d in depots}
        self._charge_resv_gen: int = 0
        # Count of vehicles currently in TO_DEPOT or CHARGING state (updated via _enter_state)
        self._charging_or_enroute_count: int = 0
        # Depot throughput diagnostics (ARRIVE_DEPOT / CHARGING_COMPLETE; hour = floor(t/3600s))
        self._depot_arrival_count_by_depot: dict[str, int] = {d.id: 0 for d in depots}
        self._depot_jit_plug_full_by_depot: dict[str, int] = {d.id: 0 for d in depots}
        self._depot_charge_completion_count_by_depot: dict[str, int] = {d.id: 0 for d in depots}
        self._depot_arrivals_by_hour: dict[str, defaultdict[int, int]] = {
            d.id: defaultdict(int) for d in depots
        }
        self._depot_completions_by_hour: dict[str, defaultdict[int, int]] = {
            d.id: defaultdict(int) for d in depots
        }
        self._charging_session_durations_s: list[float] = []

        if config.charging_queue_policy not in ("jit", "fifo"):
            raise ValueError("charging_queue_policy must be 'jit' or 'fifo'")
        if config.min_plug_duration_minutes < 0:
            raise ValueError("min_plug_duration_minutes must be >= 0")
        if config.charging_depot_selection not in ("fastest", "fastest_balanced"):
            raise ValueError("charging_depot_selection must be 'fastest' or 'fastest_balanced'")
        if config.charging_depot_balance_slack_minutes < 0:
            raise ValueError("charging_depot_balance_slack_minutes must be >= 0")

        # Repositioning policy
        if reposition_policy is not None:
            self._repo: Optional[BaseRepositioningPolicy] = reposition_policy
        elif config.reposition_enabled:
            from .reposition_policies import DemandScorePolicy
            self._repo = DemandScorePolicy(
                alpha=config.reposition_alpha,
                half_life_minutes=config.reposition_half_life_minutes,
                forecast_horizon_minutes=config.reposition_forecast_horizon_minutes,
                max_reposition_travel_minutes=config.max_reposition_travel_minutes,
                max_vehicles_targeting_cell=config.max_vehicles_targeting_cell,
                min_idle_minutes=config.reposition_min_idle_minutes,
                top_k_cells=config.reposition_top_k_cells,
                reposition_lambda=config.reposition_lambda,
            )
        else:
            self._repo = None

        # Dashboard: vehicle state-transition log [(time_s, vid, h3, state_str), ...]
        self._vehicle_transitions: list[tuple[float, str, str, str]] = []

        # Time-series snapshots
        self._snapshots: list[dict[str, Any]] = []
        self._served_cumulative = 0
        self._unserved_cumulative = 0
        # Diagnostic: one entry per UNSERVED (eligible_count, pending_count, etc.)
        self._unserved_diagnostics: list[dict[str, Any]] = []
        # Min ETA from any eligible vehicle to request origin at arrival time (for diagnostics)
        self._min_eta_at_arrival: dict[str, float] = {}
        # Coverage state at request arrival (for unserved diagnostics; CoverageFloorPolicy only)
        self._coverage_state_at_arrival: dict[str, dict] = {}

        self._duration_s = config.duration_minutes * 60.0
        self._progress_callback = progress_callback

        # Spatial index for dispatch candidate pre-filtering
        self._vehicle_index = VehicleIndex()
        # Count of currently eligible vehicles (IDLE or REPOSITIONING).
        # Maintained in sync with _vehicle_index; allows O(1) "any eligible?" check.
        self._eligible_count: int = 0
        # Count of currently repositioning vehicles — used to skip select_target
        # when all repositioning slots are already filled (oversupply guard).
        self._repositioning_count: int = 0

        # Pending-skip cache: requests that failed find_best_vehicle and are
        # backed off.  They are NOT retried on every dispatch tick.  Instead,
        # when a vehicle becomes eligible, we do ONE targeted ring-expansion
        # from the vehicle's cell, find the nearest backed-off request, and
        # assign directly — no thundering herd.
        # Key: req_id  Value: earliest sim-time for time-based last-chance retry
        self._pending_skip: dict[str, float] = {}
        # Reverse index for O(1) ring lookup: origin_h3 → set of backed-off req_ids
        self._skip_by_origin: dict[str, set[str]] = {}

        # Event counters — incremented on every processed event; returned in results
        # so callers can see how algo changes shift event frequency.
        from .events import EventType as _ET
        self._event_counts: dict[str, int] = {e.value: 0 for e in _ET}

        # Config + depots are fixed for the run — avoid scanning all depots on every idle/charge event.
        self._effective_soc_charge_start_cached: float = (
            self._compute_effective_soc_charge_start_value()
        )

        self._handlers = {
            EventType.REQUEST_ARRIVAL: self._handle_request_arrival,
            EventType.REQUEST_EXPIRE: self._handle_request_expire,
            EventType.DISPATCH: self._handle_dispatch,
            EventType.TRIP_START: self._handle_trip_start,
            EventType.POOL_PICKUP: self._handle_pool_pickup,
            EventType.TRIP_COMPLETE: self._handle_trip_complete,
            EventType.ARRIVE_DEPOT: self._handle_arrive_depot,
            EventType.CHARGE_DEPARTURE: self._handle_charge_departure,
            EventType.CHARGING_COMPLETE: self._handle_charging_complete,
            EventType.REPOSITION_COMPLETE: self._handle_reposition_complete,
            EventType.REPOSITION_ELIGIBLE: self._handle_reposition_eligible,
            EventType.VEHICLE_IDLE: self._handle_vehicle_idle,
            EventType.SNAPSHOT: self._handle_snapshot,
        }

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self) -> dict[str, Any]:
        self._initialize()
        self._drain(until_s=self._duration_s)
        return self._collect_results()

    # ------------------------------------------------------------------
    # Initialization
    # ------------------------------------------------------------------

    def _initialize(self) -> None:
        # Latest sim time at which any request arrives (for "no more incoming requests" check).
        self._max_request_time_s = max(
            (r.request_time for r in self.requests.values()),
            default=0.0,
        )
        # Schedule all REQUEST_ARRIVAL events
        for req in self.requests.values():
            if req.request_time <= self._duration_s:
                self._push(req.request_time, EventType.REQUEST_ARRIVAL, {"request_id": req.id})

        # Bootstrap: run VEHICLE_IDLE once at t=0 for each initially-IDLE vehicle so
        # charging / reposition logic applies from the start (not only after a trip).
        for vehicle_id in sorted(self.vehicles.keys()):
            v = self.vehicles[vehicle_id]
            if v.state == VehicleState.IDLE:
                self._push(0.0, EventType.VEHICLE_IDLE, {"vehicle_id": vehicle_id})

        # Schedule time-series snapshot events
        bucket_s = self.config.timeseries_bucket_minutes * 60.0
        t = 0.0
        while t <= self._duration_s:
            self._push(t, EventType.SNAPSHOT, {"t": t})
            t += bucket_s

        # Seed spatial index with all initially-IDLE vehicles (no current_time → no skip assignment)
        for vehicle in self.vehicles.values():
            vehicle.state_entered_time = 0.0  # so first flush counts time from sim start
            self._vehicle_transitions.append(
                (0.0, vehicle.id, vehicle.current_h3, vehicle.state.value)
            )
            if vehicle.state in (VehicleState.IDLE, VehicleState.REPOSITIONING):
                self._mark_eligible(vehicle.id, vehicle.current_h3, current_time=None)

        # Drain debug: log coverage-table bucket and coverage-target decisions when t > 24h
        if self.config.collect_event_log and isinstance(self._repo, CoverageFloorPolicy):
            self._drain_reposition_debug: list = []
            self._repo.set_drain_debug(self._drain_reposition_debug, self._duration_s)

    # ------------------------------------------------------------------
    # Core loop
    # ------------------------------------------------------------------

    def _drain(self, until_s: float | None = None) -> None:
        """Process events until heap empty or optional time cutoff."""
        total_requests = sum(
            1 for r in self.requests.values() if r.request_time <= self._duration_s
        )
        cb = self._progress_callback
        if cb:
            cb(0, total_requests)

        REPORT_EVERY = 200  # emit a progress update every N resolved trips
        resolved_last = 0

        while self._heap:
            if until_s is not None and self._heap[0][0] > until_s:
                break
            _, _, event = heapq.heappop(self._heap)
            self._current_time = event.time
            self._dispatch_event(event)

            if cb:
                resolved = self._served_cumulative + self._unserved_cumulative
                if resolved - resolved_last >= REPORT_EVERY:
                    cb(resolved, total_requests)
                    resolved_last = resolved

        if cb:
            cb(self._served_cumulative + self._unserved_cumulative, total_requests)

    def _dispatch_event(self, event: Event) -> None:
        self._event_counts[event.type.value] += 1
        if self.config.collect_event_log:
            self._event_log.append((event.time, event.type.value))
        self._handlers[event.type](event)

    # ------------------------------------------------------------------
    # Event helpers
    # ------------------------------------------------------------------

    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq

    def _push(self, time: float, etype: EventType, payload: dict[str, Any] | None = None) -> None:
        if etype == EventType.REQUEST_ARRIVAL and time > self._duration_s:
            return
        seq = self._next_seq()
        event = Event(time=time, seq=seq, type=etype, payload=payload or {})
        heapq.heappush(self._heap, (time, seq, event))

    def _schedule_dispatch(self, t: float) -> None:
        if self._dispatch_scheduled_at != t:
            self._dispatch_scheduled_at = t
            self._push(t, EventType.DISPATCH)

    # Seconds before expiry at which we always retry via normal dispatch.
    _DISPATCH_FINAL_RETRY_S: float = 60.0

    def _mark_eligible(self, vehicle_id: str, h3_cell: str,
                       current_time: Optional[float] = None) -> None:
        """Add vehicle to spatial index + notify coverage policy.

        If current_time is provided (i.e. during live simulation, not
        initialization), immediately try to serve the nearest backed-off
        request from h3_cell.  This replaces thundering-herd wake-ups:
        one vehicle → one routing.get → one assignment (or nothing).
        """
        self._vehicle_index.add(vehicle_id, h3_cell)
        self._eligible_count += 1
        if isinstance(self._repo, CoverageFloorPolicy):
            self._repo.on_vehicle_eligible(h3_cell)
        if current_time is not None and self._skip_by_origin:
            self._try_assign_skipped(vehicle_id, h3_cell, current_time)

    def _mark_ineligible(self, vehicle_id: str, h3_cell: str) -> None:
        """Remove vehicle from spatial index + notify coverage policy (idempotent)."""
        if not self._vehicle_index.contains(vehicle_id, h3_cell):
            return
        self._vehicle_index.remove(vehicle_id, h3_cell)
        self._eligible_count -= 1
        if isinstance(self._repo, CoverageFloorPolicy):
            self._repo.on_vehicle_ineligible(h3_cell)

    def _flush_state_time(self, vehicle: Vehicle, now: float) -> None:
        """Accumulate time spent in current state; set state_entered_time = now for next period."""
        dur = now - vehicle.state_entered_time
        if dur > 0:
            if vehicle.state == VehicleState.IDLE:
                vehicle.time_idle_s += dur
            elif vehicle.state == VehicleState.TO_PICKUP:
                vehicle.time_to_pickup_s += dur
            elif vehicle.state == VehicleState.IN_TRIP:
                vehicle.time_in_trip_s += dur
            elif vehicle.state == VehicleState.REPOSITIONING:
                vehicle.time_repositioning_s += dur
            elif vehicle.state == VehicleState.TO_DEPOT:
                vehicle.time_to_depot_s += dur
            elif vehicle.state == VehicleState.CHARGING:
                vehicle.time_charging_s += dur
        vehicle.state_entered_time = now

    def _enter_state(self, vehicle: Vehicle, new_state: VehicleState, now: float) -> None:
        """Flush current state time, then set vehicle to new_state."""
        _CHARGING_STATES = (VehicleState.TO_DEPOT, VehicleState.CHARGING)
        old_state = vehicle.state
        self._flush_state_time(vehicle, now)
        vehicle.state = new_state
        self._vehicle_transitions.append(
            (now, vehicle.id, vehicle.current_h3, new_state.value)
        )
        if old_state in _CHARGING_STATES and new_state not in _CHARGING_STATES:
            self._charging_or_enroute_count -= 1
        elif new_state in _CHARGING_STATES and old_state not in _CHARGING_STATES:
            self._charging_or_enroute_count += 1

    def _flush_all_state_times(self, now: float) -> None:
        """Flush every vehicle's current state duration (call at sim end)."""
        for vehicle in self.vehicles.values():
            self._flush_state_time(vehicle, now)

    def _try_assign_skipped(self, vehicle_id: str, h3_cell: str,
                            current_time: float) -> None:
        """Ring-expand from h3_cell to find the nearest backed-off request.

        Performs a single routing.get feasibility check and, if the vehicle
        can reach the request within its remaining wait window, assigns
        directly.  At most ONE assignment per call — no thundering herd.
        Requests that are found to be expired are cleaned up along the way.
        """
        vehicle = self.vehicles.get(vehicle_id)
        if vehicle is None or vehicle.state not in (
                VehicleState.IDLE, VehicleState.REPOSITIONING):
            return
        if vehicle.soc < self.config.soc_min:
            return

        # Check only the vehicle's immediate neighborhood (ring 0+1 = 7 cells).
        # This catches requests that backed off right when vehicles were busy nearby.
        # Requests further away are handled by the jittered 90s time-based retry.
        # Ring-1 disk = 7 cells vs ring-10 = 271 — 39× cheaper per call.
        max_r = 1
        for ring in range(max_r + 1):
            for cell in self._vehicle_index.ring_cells(h3_cell, ring):
                bucket = self._skip_by_origin.get(cell)
                if not bucket:
                    continue
                for req_id in list(bucket):
                    req = self.requests.get(req_id)
                    if req is None:
                        bucket.discard(req_id)
                        self._pending_skip.pop(req_id, None)
                        continue
                    time_waiting = current_time - req.request_time
                    if time_waiting > req.max_wait_time_seconds:
                        # Expire in-place rather than waiting for next dispatch tick
                        remaining_wait = (req.max_wait_time_seconds - time_waiting) if self.config.collect_unserved_diagnostics else None
                        min_eta = self._min_eta_to_origin(req.origin_h3, current_time) if self.config.collect_unserved_diagnostics and self._eligible_count > 0 else None
                        self._record_unserved(
                            req_id, req, current_time,
                            eligible_count=self._eligible_count,
                            pending_count=len(self._pending),
                            expiry_source="in_place_skip",
                            min_eta_seconds=min_eta,
                            remaining_wait_seconds=remaining_wait,
                        )
                        req.status = RequestStatus.UNSERVED
                        self._unserved_cumulative += 1
                        self._pending.discard(req_id)
                        bucket.discard(req_id)
                        self._pending_skip.pop(req_id, None)
                        continue
                    # Feasibility check: one routing.get
                    pickup_s, pickup_dist_m = self.routing.get(h3_cell, req.origin_h3)
                    if vehicle.state == VehicleState.REPOSITIONING:
                        remaining_s = max(
                            0.0,
                            vehicle.total_reposition_s
                            - (current_time - vehicle.reposition_start_time),
                        )
                        pickup_s += remaining_s
                    remaining_wait = req.max_wait_time_seconds - time_waiting
                    if pickup_s <= remaining_wait:
                        # Assign — vehicle serves this request directly
                        bucket.discard(req_id)
                        if not bucket:
                            self._skip_by_origin.pop(cell, None)
                        self._pending_skip.pop(req_id, None)
                        self._assign_vehicle_to_request(
                            vehicle, req, req_id, current_time,
                            pickup_s=pickup_s, pickup_dist_m=pickup_dist_m,
                        )
                        self._pending.discard(req_id)
                        return  # vehicle is now dispatched — stop
                if not bucket:
                    self._skip_by_origin.pop(cell, None)

    def _nearest_depot(self, h3_cell: str) -> Optional[Depot]:
        if not self.depots:
            return None
        best = min(self.depots, key=lambda d: self.routing.get(h3_cell, d.h3_cell)[0])
        return best

    def _clear_charge_reservation(self, vehicle_id: str) -> None:
        resv = self._charge_reservation_by_vehicle.pop(vehicle_id, None)
        if not resv:
            return
        depot_id = str(resv["depot_id"])
        by_depot = self._charge_reservations_by_depot.get(depot_id)
        if by_depot is not None:
            by_depot.pop(vehicle_id, None)

    def _charging_jobs_for_depot(self, depot: Depot, now: float) -> list[tuple[float, float]]:
        jobs: list[tuple[float, float]] = []
        active = self._active_charge_end_by_depot.get(depot.id, {})
        for end_t in active.values():
            if end_t > now:
                jobs.append((now, end_t))
        for r in self._charge_reservations_by_depot.get(depot.id, {}).values():
            start_t = float(r["slot_time"])
            end_t = float(r["slot_end_time"])
            if end_t > now:
                jobs.append((max(now, start_t), end_t))
        return jobs

    @staticmethod
    def _earliest_slot_start(
        arrival_earliest: float, duration_s: float, jobs: list[tuple[float, float]], capacity: int
    ) -> float:
        if capacity <= 0:
            return float("inf")
        if duration_s <= 0:
            return arrival_earliest

        # Sweep-line approach: build sorted event list of occupancy changes,
        # then scan for the first window of length duration_s where
        # occupancy stays below capacity.  O(J log J) vs the old O(J²).
        events: list[tuple[float, int]] = []
        for s, e in jobs:
            events.append((s, 1))
            events.append((e, -1))
        events.sort()

        # Compute initial occupancy at arrival_earliest.
        occ = 0
        for s, e in jobs:
            if s <= arrival_earliest < e:
                occ += 1

        # Collect sorted boundary times >= arrival_earliest where occ changes.
        future: list[tuple[float, int]] = [
            (t, d) for t, d in events if t > arrival_earliest
        ]

        if occ < capacity:
            # Currently below capacity; check if it stays below for duration_s.
            end_needed = arrival_earliest + duration_s
            ok = True
            for t, d in future:
                if t >= end_needed:
                    break
                occ += d
                if occ >= capacity:
                    ok = False
                    break
            if ok:
                return arrival_earliest

        # Scan forward through boundaries to find a feasible window.
        # Reset occ to value at arrival_earliest.
        occ = 0
        for s, e in jobs:
            if s <= arrival_earliest < e:
                occ += 1

        for i, (t, d) in enumerate(future):
            occ += d
            if occ < capacity and t >= arrival_earliest:
                # Potential window starts at t.  Check it holds for duration_s.
                end_needed = t + duration_s
                test_occ = occ
                ok = True
                for j in range(i + 1, len(future)):
                    tj, dj = future[j]
                    if tj >= end_needed:
                        break
                    test_occ += dj
                    if test_occ >= capacity:
                        ok = False
                        break
                if ok:
                    return t

        # Fallback: start after all jobs end.
        if future:
            return future[-1][0] + 1.0
        return arrival_earliest

    def _depot_charging_pressure(self, depot: Depot) -> int:
        """Rough load: FIFO waiters + active plugs + reserved slots (excludes this vehicle's resv — cleared)."""
        resv_n = len(self._charge_reservations_by_depot.get(depot.id, {}))
        return len(depot.queue) + depot.active_chargers + resv_n

    def _reserve_charge_departure(self, vehicle: Vehicle, now: float, *, target_soc: float) -> bool:
        if not self.depots:
            return False
        self._clear_charge_reservation(vehicle.id)
        cands: list[tuple[float, Depot, float, float, float]] = []
        # (depart_time, depot, slot_start, slot_end, travel_s)
        for depot in self.depots:
            travel_s, travel_dist_m = self.routing.get(vehicle.current_h3, depot.h3_cell)
            travel_miles = travel_dist_m / 1609.344
            soc_arrive = max(0.0, vehicle.soc - vehicle.energy_for_miles(travel_miles))
            kwh_needed = max(0.0, (target_soc - soc_arrive) * vehicle.battery_kwh)
            kw = depot.effective_charger_kw()
            if kw <= 0:
                continue
            dur_s = (kwh_needed / kw) * 3600.0
            dur_s = max(dur_s, self._min_plug_duration_s())
            arrival_t = now + travel_s
            # Fast path L1: if total load (active + reserved) < capacity, the
            # vehicle can plug in on arrival — skip the O(n²) boundary scan.
            resv_map = self._charge_reservations_by_depot.get(depot.id, {})
            active_map = self._active_charge_end_by_depot.get(depot.id, {})
            if len(active_map) + len(resv_map) < depot.chargers_count:
                slot_start = arrival_t
            else:
                # Fast path L2: count chargers actually occupied at arrival_t.
                # Many active sessions end before arrival; many reservations
                # have slot_time after arrival.  O(n) scan avoids the O(n²)
                # boundary algorithm in the common case.
                occ = sum(1 for et in active_map.values() if et > arrival_t)
                occ += sum(
                    1 for r in resv_map.values()
                    if float(r["slot_time"]) <= arrival_t < float(r["slot_end_time"])
                )
                if occ < depot.chargers_count:
                    slot_start = arrival_t
                else:
                    jobs = self._charging_jobs_for_depot(depot, now)
                    slot_start = self._earliest_slot_start(arrival_t, dur_s, jobs, depot.chargers_count)
            slot_end = slot_start + dur_s
            depart_t = max(now, slot_start - travel_s)
            cands.append((depart_t, depot, slot_start, slot_end, travel_s))
        if not cands:
            return False

        best_depart = min(c[0] for c in cands)
        if self.config.charging_depot_selection == "fastest_balanced":
            slack_s = self.config.charging_depot_balance_slack_minutes * 60.0
            near = [c for c in cands if c[0] <= best_depart + slack_s]
            pool = near if near else cands
            chosen = min(
                pool,
                key=lambda c: (
                    self._depot_charging_pressure(c[1]),
                    c[0],
                    c[4],
                    c[1].id,
                ),
            )
        else:
            chosen = min(cands, key=lambda c: (c[0], c[4], c[1].id))

        depart_t, depot, slot_start, slot_end, _ = chosen
        self._charge_resv_gen += 1
        gen = self._charge_resv_gen
        resv = {
            "depot_id": depot.id,
            "slot_time": slot_start,
            "slot_end_time": slot_end,
            "depart_time": depart_t,
            "gen": gen,
        }
        self._charge_reservation_by_vehicle[vehicle.id] = resv
        self._charge_reservations_by_depot[depot.id][vehicle.id] = resv
        self._push(depart_t, EventType.CHARGE_DEPARTURE, {"vehicle_id": vehicle.id, "gen": gen})
        return True

    def _snapshot_queue_depth_peaks(self) -> None:
        """Update peak queue depth after enqueue/dequeue changes depot.queue."""
        if not self.depots:
            return
        total = sum(len(d.queue) for d in self.depots)
        self._depot_queue_max_concurrent = max(self._depot_queue_max_concurrent, total)
        for d in self.depots:
            self._depot_queue_max_at_site = max(self._depot_queue_max_at_site, len(d.queue))

    def _begin_charging_session(self, vehicle_id: str, depot: Depot, event_time: float) -> None:
        """Occupy a plug and schedule CHARGING_COMPLETE (vehicle must be at depot cell)."""
        vehicle = self.vehicles[vehicle_id]
        vehicle.charge_sessions += 1
        depot.active_chargers += 1
        self._enter_state(vehicle, VehicleState.CHARGING, event_time)
        vehicle.charging_session_start_time = event_time
        vehicle.charging_session_kw = depot.effective_charger_kw()
        charge_duration_s = self._charge_duration(vehicle, depot)
        self._charger_busy_seconds += charge_duration_s
        self._charger_busy_seconds_by_depot[depot.id] = (
            self._charger_busy_seconds_by_depot.get(depot.id, 0.0) + charge_duration_s
        )
        end_t = event_time + charge_duration_s
        self._active_charge_end_by_vehicle[vehicle_id] = end_t
        self._active_charge_end_by_depot[depot.id][vehicle_id] = end_t
        self._push(
            end_t,
            EventType.CHARGING_COMPLETE,
            {
                "vehicle_id": vehicle_id,
                "depot_id": depot.id,
                "charge_start": event_time,
            },
        )

    def _min_eta_to_origin(self, origin_h3: str, current_time: float) -> Optional[float]:
        """Minimum ETA (seconds) from any eligible vehicle to origin_h3. None if no eligible."""
        if self._eligible_count <= 0:
            return None
        best_s: float = float("inf")
        for vehicle_id in self._vehicle_index.all_eligible_ids():
            vehicle = self.vehicles.get(vehicle_id)
            if vehicle is None or vehicle.state not in (
                VehicleState.IDLE,
                VehicleState.REPOSITIONING,
            ):
                continue
            pickup_s, _ = self.routing.get(vehicle.current_h3, origin_h3)
            if vehicle.state == VehicleState.REPOSITIONING and vehicle.reposition_start_time is not None:
                remaining_repo_s = max(
                    0.0,
                    vehicle.total_reposition_s - (current_time - vehicle.reposition_start_time),
                )
                pickup_s += remaining_repo_s
            if pickup_s < best_s:
                best_s = pickup_s
        return best_s if best_s != float("inf") else None

    def _record_unserved(
        self,
        req_id: str,
        req: "Request",
        current_time: float,
        *,
        eligible_count: int,
        pending_count: int,
        expiry_source: str,
        min_eta_seconds: Optional[float] = None,
        remaining_wait_seconds: Optional[float] = None,
    ) -> None:
        """Append one row to unserved diagnostics for debugging why requests expired."""
        entry: dict[str, Any] = {
            "request_id": req_id,
            "request_time": req.request_time,
            "current_time": current_time,
            "origin_h3": req.origin_h3,
            "eligible_count": eligible_count,
            "pending_count": pending_count,
            "expiry_source": expiry_source,
        }
        if min_eta_seconds is not None:
            entry["min_eta_seconds"] = min_eta_seconds
        if remaining_wait_seconds is not None:
            entry["remaining_wait_seconds"] = remaining_wait_seconds
        if not self.config.collect_unserved_diagnostics:
            return
        min_eta_arrival = self._min_eta_at_arrival.get(req_id)
        if min_eta_arrival is not None:
            entry["min_eta_at_arrival_seconds"] = min_eta_arrival
        coverage_at_arrival = self._coverage_state_at_arrival.get(req_id)
        if coverage_at_arrival is not None:
            entry["coverage_at_arrival"] = coverage_at_arrival
        if isinstance(self._repo, CoverageFloorPolicy):
            entry["coverage_at_expiry"] = self._repo.get_cell_state(
                req.origin_h3, current_time
            )
        self._unserved_diagnostics.append(entry)

    # ------------------------------------------------------------------
    # Event handlers
    # ------------------------------------------------------------------

    def _handle_request_arrival(self, event: Event) -> None:
        req_id = event.payload["request_id"]
        req = self.requests[req_id]
        req.status = RequestStatus.PENDING
        self._pending.add(req_id)

        if self._repo:
            self._repo.on_request_arrival(req.origin_h3, event.time)

        # Schedule guaranteed expiry — fires at exactly arrival + max_wait.
        # This replaces O(pending) polling: expiry is O(1) per request with
        # no dispatch ticks needed when the fleet is fully saturated.
        expire_at = event.time + req.max_wait_time_seconds
        if req.latest_departure_time is not None:
            expire_at = min(expire_at, req.latest_departure_time)
        self._push(expire_at, EventType.REQUEST_EXPIRE, {"request_id": req_id})

        # Only trigger dispatch when there are eligible vehicles.
        # If the fleet is saturated (eligible_count==0), dispatch cannot
        # assign anyone; it will fire naturally when the next vehicle frees up.
        if self._eligible_count > 0:
            self._schedule_dispatch(event.time)
            if self.config.collect_unserved_diagnostics:
                min_eta_arrival = self._min_eta_to_origin(req.origin_h3, event.time)
                if min_eta_arrival is not None:
                    self._min_eta_at_arrival[req_id] = min_eta_arrival
                if isinstance(self._repo, CoverageFloorPolicy):
                    state = self._repo.get_cell_state(req.origin_h3, event.time)
                    # How many of the vehicles "targeting" this cell are actually repositioning
                    state["targeting_repositioning_count"] = sum(
                        1
                        for v in self.vehicles.values()
                        if v.state == VehicleState.REPOSITIONING
                        and v.reposition_target_h3 == req.origin_h3
                    )
                    self._coverage_state_at_arrival[req_id] = state

    def _handle_request_expire(self, event: Event) -> None:
        """Fires at arrival_time + max_wait_seconds.

        If the request is still PENDING and still in _pending (not yet assigned)
        at this moment it has timed out — mark UNSERVED and remove from all
        tracking structures.  If it was already served, assigned, or previously
        expired, this is a no-op.
        """
        req_id = event.payload["request_id"]
        req = self.requests[req_id]
        if req.status != RequestStatus.PENDING:
            return  # already served or double-expire guard
        if req_id not in self._pending:
            return  # already assigned (vehicle en route); wait for TRIP_COMPLETE
        remaining_wait = req.max_wait_time_seconds - (event.time - req.request_time) if self.config.collect_unserved_diagnostics else None
        min_eta = self._min_eta_to_origin(req.origin_h3, event.time) if self.config.collect_unserved_diagnostics and self._eligible_count > 0 else None
        self._record_unserved(
            req_id, req, event.time,
            eligible_count=self._eligible_count,
            pending_count=len(self._pending),
            expiry_source="REQUEST_EXPIRE",
            min_eta_seconds=min_eta,
            remaining_wait_seconds=remaining_wait,
        )
        req.status = RequestStatus.UNSERVED
        self._unserved_cumulative += 1
        self._pending.discard(req_id)
        self._pending_skip.pop(req_id, None)
        origin_bucket = self._skip_by_origin.get(req.origin_h3)
        if origin_bucket:
            origin_bucket.discard(req_id)
            if not origin_bucket:
                self._skip_by_origin.pop(req.origin_h3, None)

    def _handle_dispatch(self, event: Event) -> None:
        if self.config.collect_event_log and event.time > self._duration_s:
            self._drain_dispatch_log.append((event.time, len(self._pending), self._eligible_count))
        if self._eligible_count == 0:
            return  # nothing to assign; expiry is handled by REQUEST_EXPIRE events

        self._dispatch_request_first(event)

    def _dispatch_request_first(self, event: Event) -> None:
        to_remove: list[str] = []

        for req_id in self._pending:
            req = self.requests[req_id]

            # Skip if backed off and not yet near expiry.
            remaining = req.max_wait_time_seconds - (event.time - req.request_time)
            if (self._pending_skip.get(req_id, 0) > event.time
                    and remaining > self._DISPATCH_FINAL_RETRY_S):
                continue

            if self._eligible_count <= 0:
                break

            vehicle = find_best_vehicle(
                request=req,
                vehicles=self.vehicles,
                routing=self.routing,
                current_time=event.time,
                soc_buffer=self.config.soc_buffer,
                strategy=self.config.dispatch_strategy,
                first_feasible_threshold_seconds=self.config.first_feasible_threshold_seconds,
                vehicle_index=self._vehicle_index,
                soc_min=self.config.soc_min,
            )

            if vehicle is None:
                if remaining > self._DISPATCH_FINAL_RETRY_S:
                    # Back off with jitter to desynchronize wave wake-ups.
                    # Base 90s ± 30s random spread prevents thundering-herd
                    # when many requests fail simultaneously (PM rush).
                    jitter = float(self._rng.uniform(-30.0, 30.0))
                    backoff_s = min(90.0 + jitter, remaining - self._DISPATCH_FINAL_RETRY_S)
                    backoff_until = event.time + max(backoff_s, 10.0)
                    self._pending_skip[req_id] = backoff_until
                    self._skip_by_origin.setdefault(req.origin_h3, set()).add(req_id)
                continue

            # Successful match — clear any backoff entry.
            self._pending_skip.pop(req_id, None)
            self._skip_by_origin.get(req.origin_h3, set()).discard(req_id)
            self._assign_vehicle_to_request(vehicle, req, req_id, event.time)
            to_remove.append(req_id)

        for req_id in to_remove:
            self._pending.discard(req_id)

    def _dispatch_vehicle_first(self, event: Event) -> None:
        """Vehicle-first dispatch: used when pending >= eligible (rush hours).

        Step 1: build origin_h3 → [req_id] spatial index for pending requests
                (no routing.get calls — purely dict ops). Expire overdue requests.
        Step 2: iterate eligible vehicles (sorted for determinism), ring-expand
                outward to find the nearest unclaimed request. Stop iterating
                vehicles once all requests are claimed or eligible_count hits 0.
        """
        _H3R = _H3_SECS_PER_RING
        max_radius = int(self.config.max_wait_time_seconds / _H3R) + 3

        # Step 1 — build spatial index.  Expiry is handled by REQUEST_EXPIRE events;
        # any request still in _pending here is within its wait window.
        h3_to_reqs: dict[str, list[str]] = {}
        for req_id in self._pending:
            req = self.requests[req_id]
            h3_to_reqs.setdefault(req.origin_h3, []).append(req_id)

        if not h3_to_reqs or self._eligible_count <= 0:
            return

        # Step 2 — vehicle-first ring expansion.
        # Iterate eligible vehicles in VehicleIndex cell order (H3 cells are
        # sorted by address) for stable-enough ordering without an O(n log n)
        # global sort.  Breaks immediately once h3_to_reqs empties — so even
        # with 3000 eligible vehicles and only 5 pending requests, we stop after
        # ~5 vehicles claim all requests.  No per-tick sort over full fleet.
        claimed: set[str] = set()

        for vehicle_id in self._vehicle_index.all_eligible_ids():
            if not h3_to_reqs or self._eligible_count <= 0:
                break
            vehicle = self.vehicles.get(vehicle_id)
            if vehicle is None or vehicle.state not in (VehicleState.IDLE, VehicleState.REPOSITIONING):
                continue

            best_req_id: Optional[str] = None
            best_pickup_s: float = float("inf")
            best_pickup_dist_m: float = 0.0

            for ring in range(max_radius + 1):
                for cell in self._vehicle_index.ring_cells(vehicle.current_h3, ring):
                    req_list = h3_to_reqs.get(cell)
                    if not req_list:
                        continue
                    for req_id in req_list:
                        if req_id in claimed:
                            continue
                        req = self.requests[req_id]
                        pickup_s, pickup_dist_m = self.routing.get(
                            vehicle.current_h3, req.origin_h3
                        )
                        if vehicle.state == VehicleState.REPOSITIONING:
                            remaining_s = max(
                                0.0,
                                vehicle.total_reposition_s
                                - (event.time - vehicle.reposition_start_time),
                            )
                            pickup_s += remaining_s
                        if pickup_s > req.max_wait_time_seconds:
                            continue
                        if pickup_s < best_pickup_s:
                            best_pickup_s = pickup_s
                            best_pickup_dist_m = pickup_dist_m
                            best_req_id = req_id

                if best_req_id is not None and best_pickup_s <= (ring + 1) * _H3R:
                    break  # no outer ring can improve on current best

            if best_req_id is None:
                continue

            req = self.requests[best_req_id]
            claimed.add(best_req_id)

            # Remove from spatial index so later vehicles skip it.
            origin = req.origin_h3
            cell_list = h3_to_reqs[origin]
            cell_list.remove(best_req_id)
            if not cell_list:
                del h3_to_reqs[origin]

            self._assign_vehicle_to_request(vehicle, req, best_req_id, event.time,
                                            pickup_s=best_pickup_s,
                                            pickup_dist_m=best_pickup_dist_m)
            self._pending.discard(best_req_id)

    def _assign_vehicle_to_request(
        self,
        vehicle: "Vehicle",
        req: "Request",
        req_id: str,
        now: float,
        pickup_s: Optional[float] = None,
        pickup_dist_m: Optional[float] = None,
    ) -> None:
        """Shared assignment logic for both dispatch paths."""
        self._clear_charge_reservation(vehicle.id)
        if vehicle.state == VehicleState.REPOSITIONING and vehicle.reposition_target_h3:
            if self._repo:
                self._repo.release_target(vehicle.reposition_target_h3)
            vehicle.reposition_target_h3 = None
            self._repositioning_count = max(0, self._repositioning_count - 1)

        if pickup_s is None or pickup_dist_m is None:
            pickup_s, pickup_dist_m = self.routing.get(vehicle.current_h3, req.origin_h3)
            if vehicle.state == VehicleState.REPOSITIONING:
                remaining_s = max(
                    0.0,
                    vehicle.total_reposition_s - (now - vehicle.reposition_start_time),
                )
                pickup_s += remaining_s

        pickup_miles = pickup_dist_m / 1609.344
        self._mark_ineligible(vehicle.id, vehicle.current_h3)
        self._enter_state(vehicle, VehicleState.TO_PICKUP, now)
        vehicle.assigned_request_id = req_id
        vehicle.pickup_miles += pickup_miles

        req.status = RequestStatus.PENDING
        req.dispatched_at = now
        req.assigned_vehicle_id = vehicle.id

        self._push(
            now + pickup_s,
            EventType.TRIP_START,
            {"vehicle_id": vehicle.id, "request_id": req_id},
        )

    def _handle_trip_start(self, event: Event) -> None:
        vehicle_id = event.payload["vehicle_id"]
        req_id = event.payload["request_id"]
        vehicle = self.vehicles[vehicle_id]
        req = self.requests[req_id]

        self._enter_state(vehicle, VehicleState.IN_TRIP, event.time)
        vehicle.current_h3 = req.origin_h3
        req.served_at = event.time  # wait time ends when vehicle arrives at pickup

        # --- Pool matching: try to pick up a second rider on the way ---
        if self.config.max_detour_pct > 0.0 and self._pending and not vehicle.pooled_passenger_id:
            pending_reqs = {r_id: self.requests[r_id] for r_id in self._pending}
            pool_partner = find_pool_match(
                origin_h3=req.origin_h3,
                primary_dest_h3=req.destination_h3,
                pending_requests=pending_reqs,
                routing=self.routing,
                max_detour_pct=self.config.max_detour_pct,
                current_time=event.time,
            )
        else:
            pool_partner = None

        # Lookup A's direct O-D once — used for fare, vehicle miles, and scheduling
        trip_time_s, trip_dist_m = self.routing.get(req.origin_h3, req.destination_h3)
        trip_miles = trip_dist_m / 1609.344

        # Record direct O-D stats on the request for per-trip fare computation
        req.trip_duration_seconds = float(trip_time_s)
        req.trip_miles_direct = trip_miles

        if pool_partner is not None:
            # Remove B from pending and mark as dispatched (pool leg)
            self._pending.discard(pool_partner.id)
            pool_partner.dispatched_at = event.time
            pool_partner.assigned_vehicle_id = vehicle_id
            pool_partner.pool_matched = True
            vehicle.pooled_passenger_id = pool_partner.id

            # Miles for detour to B's pickup (counted as pickup/deadhead)
            pool_pickup_time_s, pool_pickup_dist_m = self.routing.get(
                req.origin_h3, pool_partner.origin_h3
            )
            pool_pickup_miles = pool_pickup_dist_m / 1609.344
            vehicle.pickup_miles += pool_pickup_miles

            self._push(
                event.time + pool_pickup_time_s,
                EventType.POOL_PICKUP,
                {
                    "vehicle_id": vehicle_id,
                    "first_req_id": req_id,
                    "second_req_id": pool_partner.id,
                    "pool_pickup_miles": pool_pickup_miles,
                },
            )
        else:
            # Normal single-rider trip — vehicle drives A's direct route
            vehicle.trip_miles += trip_miles

            self._push(
                event.time + trip_time_s,
                EventType.TRIP_COMPLETE,
                {"vehicle_id": vehicle_id, "request_id": req_id, "trip_miles": trip_miles},
            )

    def _handle_pool_pickup(self, event: Event) -> None:
        """Vehicle has arrived at B's pickup location — both riders now on board."""
        vehicle_id = event.payload["vehicle_id"]
        first_req_id = event.payload["first_req_id"]
        second_req_id = event.payload["second_req_id"]
        pool_pickup_miles = event.payload.get("pool_pickup_miles", 0.0)

        vehicle = self.vehicles[vehicle_id]
        req_a = self.requests[first_req_id]
        req_b = self.requests[second_req_id]

        # Vehicle is now at B's origin; deduct SOC for the detour leg
        vehicle.current_h3 = req_b.origin_h3
        vehicle.soc = max(0.0, vehicle.soc - vehicle.energy_for_miles(pool_pickup_miles))
        req_b.served_at = event.time  # B's wait time ends at pickup

        # Record B's direct O-D route stats for fare computation (always direct, not detour)
        b_direct_time_s, b_direct_dist_m = self.routing.get(req_b.origin_h3, req_b.destination_h3)
        req_b.trip_duration_seconds = float(b_direct_time_s)
        req_b.trip_miles_direct = b_direct_dist_m / 1609.344

        # Determine drop-off order: serve the nearer destination first
        _, dist_to_a_dest_m = self.routing.get(req_b.origin_h3, req_a.destination_h3)
        dist_to_b_dest_m = b_direct_dist_m  # reuse — B's direct dist = B.origin → B.dest

        if dist_to_a_dest_m <= dist_to_b_dest_m:
            first_drop_req_id, first_drop_dest = first_req_id, req_a.destination_h3
            second_drop_req_id, second_drop_dest = second_req_id, req_b.destination_h3
        else:
            first_drop_req_id, first_drop_dest = second_req_id, req_b.destination_h3
            second_drop_req_id, second_drop_dest = first_req_id, req_a.destination_h3

        # Leg 1: current pos → first dropoff
        leg1_time_s, leg1_dist_m = self.routing.get(req_b.origin_h3, first_drop_dest)
        leg1_miles = leg1_dist_m / 1609.344

        # Leg 2: first dropoff → second dropoff
        leg2_time_s, leg2_dist_m = self.routing.get(first_drop_dest, second_drop_dest)
        leg2_miles = leg2_dist_m / 1609.344

        # Both legs are revenue miles; leg 1 is shared (two riders aboard)
        vehicle.trip_miles += leg1_miles + leg2_miles
        vehicle.shared_miles += leg1_miles

        self._push(
            event.time + leg1_time_s,
            EventType.TRIP_COMPLETE,
            {
                "vehicle_id": vehicle_id,
                "request_id": first_drop_req_id,
                "trip_miles": leg1_miles,
                "pool_continuation_req_id": second_drop_req_id,
                "pool_continuation_dest": second_drop_dest,
                "pool_continuation_miles": leg2_miles,
                "pool_continuation_time_s": leg2_time_s,
            },
        )

    def _handle_trip_complete(self, event: Event) -> None:
        vehicle_id = event.payload["vehicle_id"]
        req_id = event.payload["request_id"]
        trip_miles = event.payload.get("trip_miles", 0.0)
        pool_continuation_req_id = event.payload.get("pool_continuation_req_id")
        vehicle = self.vehicles[vehicle_id]
        req = self.requests[req_id]

        vehicle.current_h3 = req.destination_h3
        vehicle.soc = max(0.0, vehicle.soc - vehicle.energy_for_miles(trip_miles))
        req.status = RequestStatus.SERVED
        self._served_cumulative += 1

        if pool_continuation_req_id:
            # Pool trip: continue to drop off the second rider
            continuation_req = self.requests[pool_continuation_req_id]
            continuation_miles = event.payload.get("pool_continuation_miles", 0.0)
            continuation_time_s = event.payload.get("pool_continuation_time_s", 0.0)

            self._push(
                event.time + continuation_time_s,
                EventType.TRIP_COMPLETE,
                {
                    "vehicle_id": vehicle_id,
                    "request_id": pool_continuation_req_id,
                    "trip_miles": continuation_miles,
                    # no pool_continuation here — final dropoff
                },
            )
        else:
            # Final dropoff (single trip or last leg of pool)
            vehicle.assigned_request_id = None
            vehicle.pooled_passenger_id = None
            self._push(event.time, EventType.VEHICLE_IDLE, {"vehicle_id": vehicle_id})

    def _handle_arrive_depot(self, event: Event) -> None:
        vehicle_id = event.payload["vehicle_id"]
        depot_id = event.payload["depot_id"]
        depot = next(d for d in self.depots if d.id == depot_id)
        vehicle = self.vehicles[vehicle_id]

        did = depot.id
        self._depot_arrival_count_by_depot[did] += 1
        h = int(event.time // 3600.0)
        if h < 0:
            h = 0
        self._depot_arrivals_by_hour[did][h] += 1

        # Release the hard-lock reservation now that we have arrived.
        had_reservation = vehicle_id in self._charge_reservation_by_vehicle
        self._clear_charge_reservation(vehicle_id)

        # Deduct SOC for travel to depot
        travel_miles = event.payload.get("travel_miles", 0.0)
        vehicle.soc = max(0.0, vehicle.soc - vehicle.energy_for_miles(travel_miles))
        vehicle.current_h3 = depot.h3_cell

        # Enforce the hard plug cap at arrival. The slot planner's reservation
        # is a *plan*, not a license to oversubscribe: effective_charger_kw
        # varies with active_chargers so planned end times drift, and multiple
        # reservations with overlapping slots can arrive concurrently. We must
        # never exceed depot.chargers_count concurrent sessions.
        _ = had_reservation  # reservation is cleared above; retained name for clarity
        if depot.active_chargers < depot.chargers_count:
            self._begin_charging_session(vehicle_id, depot, event.time)
            return
        if self.config.charging_queue_policy == "fifo":
            depot.queue.append(vehicle_id)
            self._depot_enqueue_time[vehicle_id] = event.time
            self._snapshot_queue_depth_peaks()
            return
        # JIT: plugs full — bounce to IDLE and re-plan charging
        self._depot_jit_plug_full_by_depot[did] += 1
        vehicle.charge_sessions += 1
        self._push(event.time, EventType.VEHICLE_IDLE, {"vehicle_id": vehicle_id})

    def _handle_charge_departure(self, event: Event) -> None:
        vehicle_id = event.payload["vehicle_id"]
        vehicle = self.vehicles[vehicle_id]
        resv = self._charge_reservation_by_vehicle.get(vehicle_id)
        if not resv:
            return
        if resv.get("gen") != event.payload.get("gen"):
            return  # stale event from a superseded reservation
        if vehicle.state != VehicleState.IDLE:
            self._clear_charge_reservation(vehicle_id)
            return
        ecs = self._effective_soc_charge_start()
        if not (vehicle.soc < self.config.soc_min or vehicle.soc < ecs):
            self._clear_charge_reservation(vehicle_id)
            return
        depot_id = str(resv["depot_id"])
        depot = next(d for d in self.depots if d.id == depot_id)
        # Hard lock: reservation stays in _charge_reservation_by_vehicle and
        # _charge_reservations_by_depot through travel so the slot is held.
        # Released on arrival (_handle_arrive_depot) or dispatch preempt
        # (_assign_vehicle_to_request).
        travel_s, travel_dist_m = self.routing.get(vehicle.current_h3, depot.h3_cell)
        travel_miles = travel_dist_m / 1609.344
        self._mark_ineligible(vehicle.id, vehicle.current_h3)
        self._enter_state(vehicle, VehicleState.TO_DEPOT, event.time)
        vehicle.pickup_miles += travel_miles
        self._push(
            event.time + travel_s,
            EventType.ARRIVE_DEPOT,
            {"vehicle_id": vehicle_id, "depot_id": depot.id, "travel_miles": travel_miles},
        )

    def _handle_charging_complete(self, event: Event) -> None:
        vehicle_id = event.payload["vehicle_id"]
        depot_id = event.payload["depot_id"]
        depot = next(d for d in self.depots if d.id == depot_id)
        vehicle = self.vehicles[vehicle_id]

        t0 = vehicle.charging_session_start_time
        if t0 is not None and event.time >= t0:
            self._charging_session_durations_s.append(event.time - t0)
        self._depot_charge_completion_count_by_depot[depot_id] += 1
        hc = int(event.time // 3600.0)
        if hc < 0:
            hc = 0
        self._depot_completions_by_hour[depot_id][hc] += 1

        vehicle.soc = self.config.soc_target
        vehicle.charging_session_start_time = None
        vehicle.charging_session_kw = None
        depot.active_chargers -= 1
        self._active_charge_end_by_vehicle.pop(vehicle_id, None)
        self._active_charge_end_by_depot.get(depot_id, {}).pop(vehicle_id, None)

        self._push(event.time, EventType.VEHICLE_IDLE, {"vehicle_id": vehicle_id})

        if self.config.charging_queue_policy == "fifo" and depot.queue:
            next_id = depot.queue.pop(0)
            enq_t = self._depot_enqueue_time.pop(next_id, event.time)
            self._depot_queue_waits.append((event.time - enq_t) / 60.0)
            self._snapshot_queue_depth_peaks()
            self._begin_charging_session(next_id, depot, event.time)

    def _handle_reposition_complete(self, event: Event) -> None:
        vehicle_id = event.payload["vehicle_id"]
        target_h3 = event.payload["target_h3"]
        travel_miles = event.payload.get("travel_miles", 0.0)
        vehicle = self.vehicles[vehicle_id]

        # Guard: dispatch may have preempted the repositioning move
        if vehicle.state != VehicleState.REPOSITIONING:
            return

        # current_h3, coverage policy, and vehicle_index were all updated to
        # target_h3 at reposition START, so no position/coverage shifts here.
        vehicle.soc = max(0.0, vehicle.soc - vehicle.energy_for_miles(travel_miles))
        vehicle.reposition_miles += travel_miles
        self._enter_state(vehicle, VehicleState.IDLE, event.time)
        vehicle.reposition_target_h3 = None
        self._repositioning_count = max(0, self._repositioning_count - 1)

        if self._repo:
            self._repo.release_target(target_h3)

        # already_eligible=True: vehicle is already in the spatial index at target_h3
        # (either via _vehicle_index.move at short-reposition start, or via
        # REPOSITION_ELIGIBLE for long repositions).  Skip re-adding.
        self._push(event.time, EventType.VEHICLE_IDLE, {"vehicle_id": vehicle_id, "already_eligible": True})

    def _handle_reposition_eligible(self, event: Event) -> None:
        """Add a long-reposition vehicle to the dispatch index once it enters
        the actionable window (remaining travel ≤ max_wait_time_seconds).

        For repositions shorter than max_wait_s this event is never scheduled;
        those vehicles stay in the index throughout via _vehicle_index.move.
        """
        vehicle_id = event.payload["vehicle_id"]
        target_h3 = event.payload["target_h3"]
        vehicle = self.vehicles[vehicle_id]

        # Guard: vehicle was preempted (dispatched) or otherwise interrupted
        # before reaching this window — no index update needed.
        if vehicle.state != VehicleState.REPOSITIONING:
            return
        if vehicle.current_h3 != target_h3:
            # Shouldn't happen in practice, but be defensive.
            return

        # Enter the dispatch window: add to spatial index.
        # Coverage was already updated by on_vehicle_move at reposition start,
        # so on_vehicle_eligible must NOT be called here (would double-count).
        self._vehicle_index.add(vehicle_id, target_h3)
        self._eligible_count += 1

        # Trigger dispatch so pending requests can now consider this vehicle.
        if self._pending:
            self._schedule_dispatch(event.time)
        if self._skip_by_origin:
            self._try_assign_skipped(vehicle_id, target_h3, event.time)

    def _handle_vehicle_idle(self, event: Event) -> None:
        vehicle_id = event.payload["vehicle_id"]
        vehicle = self.vehicles[vehicle_id]
        is_recheck = event.payload.get("recheck", False)

        if is_recheck:
            if vehicle.state != VehicleState.IDLE:
                return
        else:
            self._enter_state(vehicle, VehicleState.IDLE, event.time)
            vehicle.last_became_idle_time = event.time
            if not event.payload.get("already_eligible"):
                self._mark_eligible(vehicle.id, vehicle.current_h3, current_time=event.time)
            elif self._skip_by_origin:
                self._try_assign_skipped(vehicle.id, vehicle.current_h3, event.time)

        reposition_enabled = self._repo and self.config.reposition_enabled
        _recheck_delay = self.config.reposition_min_idle_minutes * 60.0

        # Priority 0: mandatory charge — SOC below soc_min means vehicle MUST charge.
        # Removes from dispatch pool so it cannot be poached.
        if vehicle.soc < self.config.soc_min and self.depots:
            reserved = self._reserve_charge_departure(vehicle, event.time, target_soc=self.config.soc_target)
            if reserved:
                self._mark_ineligible(vehicle.id, vehicle.current_h3)
                return

        # Priority 1: dispatch if pending requests
        if self._pending:
            # Supply-aware charging: if enough headroom (eligible >> pending),
            # let low-SOC vehicles charge.  Cap is dynamic: the larger of the
            # fixed floor (max_concurrent_charging_pct) and the actual surplus
            # (eligible minus the headroom needed for dispatch).  This lets the
            # fleet charge aggressively during lulls while protecting peak SLA.
            needs_charge = vehicle.soc < self._effective_soc_charge_start()
            if needs_charge and self.depots:
                pending_n = max(len(self._pending), 1)
                ratio = self._eligible_count / pending_n
                base_cap = int(self.config.max_concurrent_charging_pct * len(self.vehicles))
                headroom = self._eligible_count - int(pending_n * self.config.charge_supply_ratio)
                charging_cap = max(base_cap, headroom)
                if ratio >= self.config.charge_supply_ratio and self._charging_or_enroute_count < charging_cap:
                    reserved = self._reserve_charge_departure(vehicle, event.time, target_soc=self.config.soc_target)
                    if reserved:
                        self._schedule_dispatch(event.time)
                        return

            self._clear_charge_reservation(vehicle_id)
            self._schedule_dispatch(event.time)
            if not is_recheck and reposition_enabled:
                self._push(
                    event.time + _recheck_delay,
                    EventType.VEHICLE_IDLE,
                    {"vehicle_id": vehicle_id, "recheck": True},
                )
            return

        # Priority 2: charge if SOC is below safety floor or hysteresis threshold
        if vehicle.soc < self.config.soc_min or vehicle.soc < self._effective_soc_charge_start():
            self._reserve_charge_departure(vehicle, event.time, target_soc=self.config.soc_target)
            return

        # Priority 3: reposition (skip once no more incoming requests — avoids coverage wrap in drain)
        if reposition_enabled and event.time < self._max_request_time_s:
            # Oversupply guard: skip expensive select_target when all slots are full.
            max_repo_slots = self.config.reposition_top_k_cells * self.config.max_vehicles_targeting_cell
            slots_available = self._repositioning_count < max_repo_slots
            target = self._repo.select_target(vehicle, event.time, self.routing, self._vehicle_index) if slots_available else None
            if target:
                origin_h3 = vehicle.current_h3
                travel_s, travel_dist_m = self.routing.get(origin_h3, target)
                travel_miles = travel_dist_m / 1609.344

                # Coverage tracking: shift vehicle from origin → target immediately
                # so deficit sets stay current.  Dispatch ETA is always conservative:
                # routing(target, pickup) + remaining_s (see dispatch.py).
                if isinstance(self._repo, CoverageFloorPolicy):
                    self._repo.on_vehicle_move(origin_h3, target)

                max_wait_s = self.config.max_wait_time_seconds
                if travel_s > max_wait_s:
                    # Delayed eligibility: for the first (travel_s - max_wait_s)
                    # seconds the vehicle's ETA to any pickup always exceeds
                    # max_wait, so it would never win a dispatch race anyway.
                    # Remove from the spatial index now; re-add via
                    # REPOSITION_ELIGIBLE once it enters the actionable window.
                    # This eliminates O(repositioning_vehicles) wasted routing.get
                    # calls per dispatch tick during the "reposition storm".
                    self._vehicle_index.remove(vehicle.id, origin_h3)
                    self._eligible_count -= 1
                    self._push(
                        event.time + travel_s - max_wait_s,
                        EventType.REPOSITION_ELIGIBLE,
                        {"vehicle_id": vehicle_id, "target_h3": target},
                    )
                else:
                    # Short reposition: vehicle remains in the dispatch index
                    # throughout (current behaviour).
                    self._vehicle_index.move(vehicle.id, origin_h3, target)

                vehicle.current_h3 = target
                self._enter_state(vehicle, VehicleState.REPOSITIONING, event.time)
                vehicle.reposition_target_h3 = target
                vehicle.reposition_start_time = event.time
                vehicle.total_reposition_s = travel_s
                self._repositioning_count += 1
                self._push(
                    event.time + travel_s,
                    EventType.REPOSITION_COMPLETE,
                    {"vehicle_id": vehicle_id, "target_h3": target, "travel_miles": travel_miles},
                )
            elif not is_recheck:
                # Not ready yet (min_idle not elapsed) — schedule one recheck
                self._push(
                    event.time + _recheck_delay,
                    EventType.VEHICLE_IDLE,
                    {"vehicle_id": vehicle_id, "recheck": True},
                )

        # Priority 4: remain IDLE (no event scheduled)

    def _handle_snapshot(self, event: Event) -> None:
        t_min = event.payload["t"] / 60.0
        counts = {s: 0 for s in VehicleState}
        for v in self.vehicles.values():
            counts[v.state] += 1

        n_v = len(self.vehicles)
        fleet_mean_soc_pct = (
            round(sum(v.soc for v in self.vehicles.values()) / n_v * 100.0, 2)
            if n_v > 0
            else 0.0
        )
        depot_snapshots: dict[str, dict[str, int]] = {}
        for d in self.depots:
            active = self._active_charge_end_by_depot.get(d.id, {})
            charging = sum(1 for et in active.values() if et > event.payload["t"])
            depot_snapshots[d.id] = {
                "charging": charging,
                "queue": len(d.queue),
                "arrivals": self._depot_arrival_count_by_depot.get(d.id, 0),
            }

        self._snapshots.append({
            "t_minutes": round(t_min, 1),
            "idle_count": counts[VehicleState.IDLE],
            "to_pickup_count": counts[VehicleState.TO_PICKUP],
            "in_trip_count": counts[VehicleState.IN_TRIP],
            "charging_count": counts[VehicleState.CHARGING] + counts[VehicleState.TO_DEPOT],
            "repositioning_count": counts[VehicleState.REPOSITIONING],
            "pending_requests": len(self._pending),
            "eligible_count": self._eligible_count,
            "served_cumulative": self._served_cumulative,
            "unserved_cumulative": self._unserved_cumulative,
            "fleet_mean_soc_pct": fleet_mean_soc_pct,
            "depot_snapshots": depot_snapshots,
        })

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _min_plug_duration_s(self) -> float:
        return max(0.0, float(self.config.min_plug_duration_minutes)) * 60.0

    def _compute_effective_soc_charge_start_value(self) -> float:
        """
        One-time value for the run: min(configured soc_charge_start, floor from min plug dwell).
        See _effective_soc_charge_start().
        """
        scs = float(self.config.soc_charge_start)
        m = float(self.config.min_plug_duration_minutes)
        if m <= 1e-12 or not self.depots:
            return scs
        kws = [d.effective_charger_kw() for d in self.depots if d.effective_charger_kw() > 0]
        if not kws:
            return scs
        kw = min(kws)
        bat = float(self.config.battery_kwh)
        tgt = float(self.config.soc_target)
        smin = float(self.config.soc_min)
        kwh_m = kw * (m / 60.0)
        delta_soc = kwh_m / bat
        delta_soc = min(delta_soc, tgt - smin)
        floor_soc = tgt - delta_soc
        floor_soc = max(floor_soc, smin)
        return min(scs, floor_soc)

    def _effective_soc_charge_start(self) -> float:
        """
        Hysteresis threshold for voluntary charging (VEHICLE_IDLE, CHARGE_DEPARTURE guard).

        When min_plug_duration_minutes > 0, do not send vehicles for top-ups smaller than
        one minimum dwell: tighten to at most (soc_target - kWh delivered in min dwell / battery),
        using the slowest depot kW (conservative). Capped so floor stays >= soc_min.
        Result is min(configured soc_charge_start, that floor).

        Cached at engine construction (O(n_depots) once per run, not per event).
        """
        return self._effective_soc_charge_start_cached

    def _charge_duration(self, vehicle: Vehicle, depot: Depot) -> float:
        """Seconds to charge from current SOC to soc_target (with optional min dwell)."""
        kwh_needed = (self.config.soc_target - vehicle.soc) * vehicle.battery_kwh
        kwh_needed = max(0.0, kwh_needed)
        effective_kw = depot.effective_charger_kw()
        if effective_kw <= 0:
            return 0.0
        base_s = (kwh_needed / effective_kw) * 3600.0
        return max(base_s, self._min_plug_duration_s())

    def _apply_horizon_charging_soc_for_metrics(self) -> None:
        """
        For vehicles still in CHARGING at horizon, replace plug-in SOC with a linear
        estimate so fleet SOC metrics reflect energy delivered so far. Uses kW frozen
        at session start (matches _charge_duration). Capped at soc_target and at the
        scheduled CHARGING_COMPLETE time. Does not change discrete event behavior.
        """
        end_t = self._current_time
        tgt = float(self.config.soc_target)
        for vehicle in self.vehicles.values():
            if vehicle.state != VehicleState.CHARGING:
                continue
            t0 = vehicle.charging_session_start_time
            kw = vehicle.charging_session_kw
            if t0 is None or kw is None or kw <= 0 or vehicle.battery_kwh <= 0:
                continue
            elapsed = max(0.0, end_t - t0)
            end_scheduled = self._active_charge_end_by_vehicle.get(vehicle.id)
            if end_scheduled is not None:
                max_elapsed = max(0.0, end_scheduled - t0)
                elapsed = min(elapsed, max_elapsed)
            delta_soc = kw * elapsed / (3600.0 * vehicle.battery_kwh)
            vehicle.soc = min(tgt, max(0.0, vehicle.soc + delta_soc))

    def _collect_results(self) -> dict[str, Any]:
        # Flush remaining time in current state for every vehicle
        self._flush_all_state_times(self._current_time)
        self._apply_horizon_charging_soc_for_metrics()

        metrics = compute_metrics(
            vehicles=self.vehicles,
            requests=self.requests,
            depots=self.depots,
            duration_s=self._duration_s,
            electricity_cost_per_kwh=self.config.electricity_cost_per_kwh,
            demand_charge_per_kw_month=self.config.demand_charge_per_kw_month,
            maintenance_cost_per_mile=self.config.maintenance_cost_per_mile,
            insurance_cost_per_vehicle_day=self.config.insurance_cost_per_vehicle_day,
            teleops_cost_per_vehicle_day=self.config.teleops_cost_per_vehicle_day,
            cleaning_cost_per_vehicle_day=self.config.cleaning_cost_per_vehicle_day,
            vehicle_cost_usd=self.config.vehicle_cost_usd,
            vehicle_lifespan_years=self.config.vehicle_lifespan_years,
            cost_per_site_day=self.config.cost_per_site_day,
            kwh_per_mile=self.config.kwh_per_mile,
            revenue_base=self.config.revenue_base,
            revenue_per_mile=self.config.revenue_per_mile,
            revenue_per_minute=self.config.revenue_per_minute,
            revenue_min_fare=self.config.revenue_min_fare,
            pool_discount_pct=self.config.pool_discount_pct,
            soc_target=self.config.soc_target,
            soc_charge_start=self._effective_soc_charge_start(),
            min_plug_duration_minutes=self.config.min_plug_duration_minutes,
        )

        # Patch in depot queue p90
        if self._depot_queue_waits:
            metrics["depot_queue_p90_min"] = round(
                float(np.percentile(self._depot_queue_waits, 90)), 3
            )

        metrics["depot_queue_max_concurrent"] = float(self._depot_queue_max_concurrent)
        metrics["depot_queue_max_at_site"] = float(self._depot_queue_max_at_site)

        # Patch in charger utilization (fleet-wide and per depot)
        dur_s = self._duration_s
        by_depot_pct: dict[str, float] = {}
        for d in self.depots:
            cap_d = float(d.chargers_count) * dur_s
            busy_d = float(self._charger_busy_seconds_by_depot.get(d.id, 0.0))
            by_depot_pct[d.id] = round(100.0 * busy_d / cap_d, 2) if cap_d > 0 else 0.0
        metrics["charger_utilization_by_depot_pct"] = by_depot_pct

        if self._total_charger_capacity_s > 0:
            metrics["charger_utilization_pct"] = round(
                self._charger_busy_seconds / self._total_charger_capacity_s * 100.0, 2
            )

        # Depot throughput (ARRIVE_DEPOT / CHARGING_COMPLETE; sim clock hour buckets)
        metrics["depot_arrivals_total"] = int(sum(self._depot_arrival_count_by_depot.values()))
        metrics["depot_arrivals_by_depot_id"] = {
            k: int(v) for k, v in self._depot_arrival_count_by_depot.items()
        }
        metrics["depot_jit_plug_full_total"] = int(sum(self._depot_jit_plug_full_by_depot.values()))
        metrics["depot_jit_plug_full_by_depot_id"] = {
            k: int(v) for k, v in self._depot_jit_plug_full_by_depot.items()
        }
        metrics["depot_charge_completions_total"] = int(
            sum(self._depot_charge_completion_count_by_depot.values())
        )
        metrics["depot_charge_completions_by_depot_id"] = {
            k: int(v) for k, v in self._depot_charge_completion_count_by_depot.items()
        }
        arr_hours: set[int] = set()
        for d in self.depots:
            arr_hours |= self._depot_arrivals_by_hour[d.id].keys()
        peak_arr_fleet = 0
        for h in arr_hours:
            peak_arr_fleet = max(
                peak_arr_fleet,
                sum(self._depot_arrivals_by_hour[d.id][h] for d in self.depots),
            )
        peak_arr_site = 0
        for d in self.depots:
            bh = self._depot_arrivals_by_hour[d.id]
            if bh:
                peak_arr_site = max(peak_arr_site, max(bh.values()))
        metrics["depot_arrivals_peak_fleet_per_hour"] = int(peak_arr_fleet)
        metrics["depot_arrivals_peak_max_site_per_hour"] = int(peak_arr_site)

        comp_hours: set[int] = set()
        for d in self.depots:
            comp_hours |= self._depot_completions_by_hour[d.id].keys()
        peak_comp_fleet = 0
        for h in comp_hours:
            peak_comp_fleet = max(
                peak_comp_fleet,
                sum(self._depot_completions_by_hour[d.id][h] for d in self.depots),
            )
        peak_comp_site = 0
        for d in self.depots:
            bh = self._depot_completions_by_hour[d.id]
            if bh:
                peak_comp_site = max(peak_comp_site, max(bh.values()))
        metrics["depot_charge_completions_peak_fleet_per_hour"] = int(peak_comp_fleet)
        metrics["depot_charge_completions_peak_max_site_per_hour"] = int(peak_comp_site)

        durs = self._charging_session_durations_s
        if durs:
            arr_d = np.array(durs, dtype=float)
            metrics["charging_session_duration_median_min"] = round(float(np.median(arr_d)) / 60.0, 3)
            metrics["charging_session_duration_p90_min"] = round(float(np.percentile(arr_d, 90)) / 60.0, 3)
        else:
            metrics["charging_session_duration_median_min"] = 0.0
            metrics["charging_session_duration_p90_min"] = 0.0

        timeseries = compute_timeseries(self._snapshots)
        # Aggregate time-per-state across all vehicles (seconds)
        state_time_s = {
            "idle": sum(v.time_idle_s for v in self.vehicles.values()),
            "to_pickup": sum(v.time_to_pickup_s for v in self.vehicles.values()),
            "in_trip": sum(v.time_in_trip_s for v in self.vehicles.values()),
            "repositioning": sum(v.time_repositioning_s for v in self.vehicles.values()),
            "to_depot": sum(v.time_to_depot_s for v in self.vehicles.values()),
            "charging": sum(v.time_charging_s for v in self.vehicles.values()),
        }
        trip_log = [
            {
                "rt": r.request_time,
                "o": r.origin_h3,
                "d": r.destination_h3,
                "st": r.status.value,
                "sa": r.served_at,
                "dur": r.trip_duration_seconds,
            }
            for r in self.requests.values()
        ]

        out: dict[str, Any] = {
            "metrics": metrics,
            "timeseries": timeseries,
            "event_counts": dict(self._event_counts),
            "routing_stats": self.routing.cache_stats(),
            "state_time_s": state_time_s,
            "vehicle_transitions": self._vehicle_transitions,
            "trip_log": trip_log,
        }
        if self._unserved_diagnostics:
            out["unserved_diagnostics"] = list(self._unserved_diagnostics)
        if self.config.collect_event_log and self._event_log:
            out["event_log"] = list(self._event_log)
        if self.config.collect_event_log and self._drain_dispatch_log:
            out["drain_dispatch_log"] = list(self._drain_dispatch_log)
        if self.config.collect_event_log and getattr(self, "_drain_reposition_debug", None):
            out["drain_reposition_debug"] = list(self._drain_reposition_debug)
        return out


# ------------------------------------------------------------------
# Factory helpers
# ------------------------------------------------------------------

def build_vehicles(
    config: SimConfig,
    depot_h3_cells: list[str],
    seed: int = 0,
    demand_cells: Optional[dict[str, float]] = None,
) -> list[Vehicle]:
    """
    Distribute fleet_size vehicles across H3 cells.

    Two modes
    ---------
    demand_cells is None (default — legacy):
        Vehicles are spread evenly across depot_h3_cells (round-robin).

    demand_cells provided (dict[h3_cell, weight]):
        Floor + proportional allocation:
          1. Floor pass — every cell with any demand gets exactly 1 vehicle.
             If fleet_size < len(demand_cells), only the top cells by weight
             receive a floor vehicle (budget-constrained floor).
          2. Proportional pass — remaining vehicles are allocated to cells
             proportional to demand weight, so high-demand cells are dense.
        This ensures sparse suburban cells start with at least one vehicle
        (Option A seeding) while the hot core remains well-covered.
    """
    import numpy as np

    rng = np.random.default_rng(seed)
    n = config.fleet_size

    if demand_cells is None or not demand_cells:
        # Legacy: round-robin across depot cells
        cells_seq = [depot_h3_cells[i % max(len(depot_h3_cells), 1)] for i in range(n)]
    else:
        cells_by_weight = sorted(demand_cells.items(), key=lambda x: x[1], reverse=True)
        all_cells = [c for c, _ in cells_by_weight]
        weights = np.array([w for _, w in cells_by_weight], dtype=float)

        # Floor pass: 1 vehicle per demand cell (up to n vehicles)
        n_floor = min(n, len(all_cells))
        floor_cells = all_cells[:n_floor]   # top cells by weight get the floor vehicle

        # Proportional pass: distribute remaining vehicles by weight
        remaining = n - n_floor
        if remaining > 0 and weights.sum() > 0:
            probs = weights / weights.sum()
            extra_cells = rng.choice(all_cells, size=remaining, replace=True, p=probs).tolist()
        else:
            extra_cells = []

        cells_seq = floor_cells + extra_cells
        rng.shuffle(cells_seq)  # shuffle so vehicle IDs aren't ordered by demand rank

    vehicles = []
    for i, h3_cell in enumerate(cells_seq):
        vehicles.append(
            Vehicle(
                id=f"v_{i:04d}",
                current_h3=h3_cell,
                state=VehicleState.IDLE,
                soc=config.soc_initial,
                charge_target_soc=config.soc_initial,
                battery_kwh=config.battery_kwh,
                kwh_per_mile=config.kwh_per_mile,
            )
        )
    return vehicles
