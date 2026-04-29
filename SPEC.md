# Robotaxi-Sim MVP Spec (Austin)
## Demand Control + Ops + Repositioning Simulator (Discrete-Event)

---

# 1. Purpose

This project is a **growth + operations design-space exploration tool** for Robotaxi systems.

It quantifies how demand-shaping mechanisms (flex windows, pooling, prebooking, off-peak incentives) and operational policies (dispatch, charging, repositioning) impact:

- Service level (wait times, SLA adherence)
- Fleet utilization
- Deadhead %
- Charging bottlenecks
- Fleet size requirements
- Itemized unit economics (energy, demand charges, maintenance, fleet fixed, infrastructure)

This is **not** a high-fidelity traffic digital twin. It is a fast, deterministic scenario simulator suitable for experimentation planning.

---

# 2. Scope

## Included
- Austin ODD (single city only for MVP)
- Discrete-event simulation (event queue)
- Fleet with SOC tracking (normalized)
- Depots with charger count + site power limit + queues
- Dispatch policy (nearest feasible vehicle)
- Repositioning policy (required; configurable on/off)
- H3-based routing cache (resolution 8 default)
- Demand shaping layer (growth levers)
- Baseline vs variant scenario comparison
- Deterministic tests

## Not Included
- Lane-level traffic simulation
- Multi-city support
- Perfect VRP solver
- ML model training
- Utility grid hosting capacity modeling
- High-fidelity UI animation

---

# 3. Technical Requirements

## Must run via
- `docker compose up --build` starts:
  - FastAPI backend
  - OSRM backend (Texas PBF or equivalent, used for Austin-area routing)

## Tests
- `pytest -q` must pass

## Determinism
- Same seed ⇒ same results within tolerance

---

# 4. Simulation Architecture

## 4.1 Simulation Type
Discrete-event simulation:
- Maintain a priority queue of events sorted by timestamp.
- Events can schedule future events (including at the same timestamp).

## 4.2 Horizon
Simulation horizon is configurable:
- `duration_minutes` (default 360) when `demand.repeat_num_days` is 1 (single `load_requests` window).
- When `demand.repeat_num_days` > 1, the engine uses **effective** horizon minutes: `repeat_num_days × (demand.duration_minutes_per_day or 1440)`. Top-level `duration_minutes` is ignored for horizon length unless `repeat_num_days` is 1; if it differs from the effective value, the API logs a note to stderr.
- `timeseries_bucket_minutes` (default 1): spacing of SNAPSHOT events and each returned `timeseries` bucket (minutes); also sets the HTML dashboard scrubber step size.
- `REQUEST_ARRIVAL` events are only scheduled for `t ≤ effective_horizon_minutes * 60`.
- The event queue is drained through `duration_s` only; there is no post-horizon
  charging sweep. Final vehicle SOC and charging metrics reflect state at the
  horizon cutoff.
- At initialization, each vehicle that starts in `IDLE` receives a `VEHICLE_IDLE`
  event at `t=0` (after `REQUEST_ARRIVAL` events at `t=0` are scheduled). Default
  fleet configs often set `soc_initial` equal to `soc_target` and `soc_charge_start`,
  so this pass is usually a no-op for charging—but it is required when
  `soc_initial < soc_charge_start` (cold start below the charge hysteresis), and it
  ensures repositioning and other idle-branch logic runs once at sim start instead
  of only after a vehicle’s first trip completion.

---

# 5. Spatial Model

## 5.1 H3 Spatial Indexing
- Default H3 resolution: **8**
- All request origins/destinations and vehicle locations represented as `h3_cell` at res 8.

H3 used for:
- Travel-time caching keys
- Heatmaps / spatial metrics
- Repositioning target selection

The Austin ODD is defined as the City of Austin municipal boundary (not full metro counties).

---

# 6. Entities

## 6.1 Vehicle
Fields:
- id
- current_h3 (H3 res 8)
- state (see Vehicle States section)
- soc ∈ [0, 1]
- battery_kwh (configurable, e.g. 75)
- kwh_per_mile (configurable, e.g. 0.20)
- assigned_request_id (nullable)
- reposition_target_h3 (nullable)
- last_became_idle_time (seconds)

Derived:
- remaining_range_miles = soc * battery_kwh / kwh_per_mile

---

## 6.2 Request
Fields:
- id
- request_time (seconds)
- origin_h3
- destination_h3
- latest_departure_time (nullable, seconds)
- max_wait_time_seconds
- pooled_allowed (bool)
- status: PENDING | SERVED | UNSERVED

---

## 6.3 Depot
Fields:
- id
- h3_cell
- chargers_count
- charger_kw
- site_power_kw
- queue (FIFO vehicle ids waiting to charge)
- active_chargers (int)

Effective per-vehicle charging power:
- min(charger_kw, site_power_kw / active_chargers)

---

# 7. Vehicle States (MVP)

Vehicles move through a small set of states to support dispatch, utilization metrics, charging queues, and repositioning experiments.

### 1) IDLE
- Vehicle is available for dispatch and currently not moving.
- Dispatch eligibility: ✅ eligible

### 2) TO_PICKUP
- Vehicle has been assigned to a request and is traveling to the pickup origin.
- Dispatch eligibility: ❌ not eligible (prevents double-assign)

### 3) IN_TRIP
- Vehicle is carrying a rider from origin to destination.
- Dispatch eligibility: ❌ not eligible

### 4) TO_DEPOT
- Vehicle is traveling to a depot OR waiting in that depot’s charging queue (MVP simplification).
- Dispatch eligibility: ❌ not eligible

### 5) CHARGING
- Vehicle is actively charging and occupying a charger resource.
- Dispatch eligibility: ❌ not eligible

### 6) REPOSITIONING
- Vehicle is cruising toward a target H3 cell to improve future pickup times.
- Dispatch eligibility: ✅ eligible (IMPORTANT)
  - If DISPATCH assigns a request to a repositioning vehicle, the reposition plan is canceled (preempted) and the vehicle transitions to TO_PICKUP.

Repositioning must be configurable via `reposition_enabled` (on/off) for A/B comparisons.

---

# 8. Events (minimal set)

This simulator uses the following event types only:

1) REQUEST_ARRIVAL  
2) DISPATCH  
3) TRIP_START  
4) POOL_PICKUP  
5) TRIP_COMPLETE  
6) ARRIVE_DEPOT  
7) CHARGING_COMPLETE  
8) REPOSITION_COMPLETE  
9) VEHICLE_IDLE  
10) SNAPSHOT (internal — time-series bucket, never causes side effects)  

## 8.0 Event Counting

Every processed event increments a counter keyed by `EventType`.
`engine.run()` returns `event_counts: dict[str, int]` alongside `metrics` and `timeseries`.
This is the primary diagnostic for understanding how algorithm changes affect event frequency
(e.g., more REPOSITION_COMPLETE events = more repositioning churn).

## 8.1 Event Definitions

### 1) REQUEST_ARRIVAL
When: at request.request_time  
Purpose: introduce new demand  
Actions:
- create Request(status=PENDING)
- add to pending set
- schedule DISPATCH at time t (guard against duplicates)

### 2) DISPATCH
When: triggered by REQUEST_ARRIVAL or VEHICLE_IDLE  
Purpose: match pending requests to available vehicles  
Actions:
- consider Vehicles in state IDLE and REPOSITIONING as dispatch-eligible
- select nearest feasible vehicle (pickup ETA + SOC feasibility)
- set vehicle state TO_PICKUP and assigned_request_id
- schedule TRIP_START at t + pickup_travel_time_seconds
Notes:
- use a “dispatch scheduled at time t” guard to prevent repeated scheduling

### 3) TRIP_START
When: vehicle arrives at pickup origin  
Purpose: begin passenger trip  
Actions:
- set vehicle state IN_TRIP
- compute trip travel time origin→destination
- schedule TRIP_COMPLETE at t + trip_time_seconds (+ optional pickup dwell)

### 4) POOL_PICKUP
When: vehicle detours to second pooled rider's origin  
Purpose: add a second passenger mid-trip (pooling only)  
Actions:
- vehicle travels to second rider's origin while carrying first rider
- schedule TRIP_COMPLETE for both riders at respective destinations

### 5) TRIP_COMPLETE
When: vehicle reaches destination  
Purpose: close trip and update state variables  
Actions:
- update vehicle current_h3 to destination_h3
- deduct SOC for trip energy (and pickup leg energy if not already accounted)
- mark request SERVED
- clear assigned_request_id
- schedule VEHICLE_IDLE at time t

### 5) ARRIVE_DEPOT
When: vehicle arrives at depot after a charge decision  
Purpose: implement depot queueing + charger assignment  
Configurable: `charging_queue_policy` (`jit` | `fifo`, default `jit`).

**Hard-lock reservation:** the charging reservation created at `CHARGE_DEPARTURE` scheduling time is **held through travel** (TO_DEPOT). It is released at `ARRIVE_DEPOT` (after arrival bookkeeping) or earlier if the vehicle is **preempted by dispatch** (`_assign_vehicle_to_request` clears it). The slot planner accounts for both active sessions and held reservations when computing feasibility, so a reservation represents an *advisory* slot booking — it tells the planner "try to have a plug free around `slot_time`". However, admission at `ARRIVE_DEPOT` is always gated by the **live cap** `active_chargers < chargers_count`, regardless of reservation state. `effective_charger_kw` varies with active load so planned end-times drift; honoring a reservation unconditionally would let multiple overlapping reservations oversubscribe plugs. When a reserved vehicle arrives to find the cap saturated, it follows the same queue / bounce path as an un-reserved arrival (see below).

**Reservation generation counter:** each reservation carries a monotonically increasing `gen` integer. The `CHARGE_DEPARTURE` event payload includes the `gen` of the reservation that created it. When `_handle_charge_departure` fires, the handler compares `event.payload["gen"]` to the vehicle's current reservation `gen`; if they differ the event is **stale** (superseded by a newer reservation) and is silently discarded. This prevents the race where a vehicle re-reserves (e.g. via a VEHICLE_IDLE recheck) before the first `CHARGE_DEPARTURE` fires, which would otherwise cause the second event to clear the newer reservation mid-travel and result in an arrival without a reservation.

Actions:
- Clear the vehicle's reservation (if any) — it has been consumed.
- if `active_chargers < chargers_count`:
  - start charging immediately: state CHARGING, increment active_chargers
  - schedule CHARGING_COMPLETE at t + charge_duration_seconds (duration includes `min_plug_duration_minutes` floor per config §11)
- else if `charging_queue_policy == fifo`:
  - append vehicle id to `depot.queue` (FIFO); vehicle remains TO_DEPOT (in-place wait)
  - record enqueue time for queue-wait metrics
- else (`jit`):
  - return vehicle to IDLE and re-plan charging (JIT bounce + session counter); the `depot_jit_plug_full_*` counter is incremented whether or not the vehicle held a reservation

Invariant: `active_chargers ≤ chargers_count` always holds (see §12).

### 6) CHARGING_COMPLETE
When: vehicle reaches soc_target  
Purpose: release charger capacity and return vehicle to decision loop  
Actions:
- set vehicle soc = soc_target
- decrement active_chargers
- schedule VEHICLE_IDLE at time t for the completing vehicle
- if `charging_queue_policy == fifo` and `depot.queue` not empty:
  - pop next vehicle, assign charger, schedule its CHARGING_COMPLETE
  - record queue wait time (dequeue time − enqueue time) for `depot_queue_p90_min`

### 7) REPOSITION_COMPLETE
When: vehicle reaches reposition target  
Purpose: finish reposition movement  
Actions:
- update vehicle current_h3 = reposition_target_h3
- deduct SOC for reposition travel energy
- set vehicle state IDLE
- clear reposition_target_h3
- schedule VEHICLE_IDLE at time t

### 8) VEHICLE_IDLE
When: immediately after TRIP_COMPLETE, CHARGING_COMPLETE, or REPOSITION_COMPLETE  
Purpose: single clean decision point  
Decision order:
1) If pending requests exist: schedule DISPATCH at time t.
2) Else if SOC < soc_charge_start: reserve a future charger slot and schedule CHARGE_DEPARTURE so ARRIVE_DEPOT is just-in-time with plug availability (no depot queueing).
3) Else if reposition_enabled and idle time >= reposition_min_idle_minutes: schedule reposition (see Repositioning section).
4) Else remain IDLE (no event scheduled).

---

# 9. Routing + Caching

## 9.1 OSRM routing calls
- Use OSRM to obtain:
  - travel time seconds
  - distance meters
between (origin_h3, dest_h3).

## 9.2 H3 travel cache
- Cache key: (origin_h3, dest_h3)
- Cache value: {time_seconds, distance_meters}
- Default: in-memory during run + persisted cache for one-time setup (see Precompute section).

---

# 10. Dispatch Policy (MVP)

- Candidate vehicles: state in {IDLE, REPOSITIONING}
- Choose vehicle with minimum pickup ETA that is feasible.

Feasibility checks:
- pickup ETA ≤ request.max_wait_time_seconds
- SOC sufficient for:
  - travel vehicle→pickup + pickup→dropoff + buffer

If request exceeds max wait:
- mark UNSERVED

## 10.1 H3 Spatial Pre-filter (VehicleIndex)

To scale dispatch to large fleets without O(V) per-request scans:
- A `VehicleIndex` maps H3 cell → set of eligible vehicle IDs (IDLE or REPOSITIONING).
- Dispatch radius: `max_radius = int(max_wait_time_seconds / 83.0) + 3` rings.
  - `83 s/ring` approximates travel at ~40 km/h over H3 res-8 ring spacing (~0.92 km).
  - `+3 ring` safety buffer accounts for road-network detours vs. straight-line H3 distance.
- Only vehicles within `max_radius` rings are evaluated; state and SOC checks still applied.
- Index is kept in sync: `add` on IDLE/REPOSITIONING entry, `remove` on dispatch/charging.

## 10.2 Dispatch Strategies

- `nearest` (default): minimum pickup ETA among all feasible candidates.
- `first_feasible`: return first candidate with ETA ≤ `first_feasible_threshold_seconds`;
  falls back to nearest-within-max-wait if none qualify.

---

# 11. Charging Logic

Config:
- soc_min (e.g., 0.20)
- soc_charge_start (e.g., 0.80)
- soc_target (e.g., 0.80)
- soc_buffer (e.g., 0.05)
- charging_queue_policy: `jit` | `fifo` (see §8.1 ARRIVE_DEPOT / CHARGING_COMPLETE)
- charging_depot_selection: `fastest` | `fastest_balanced` (default `fastest`)
- charging_depot_balance_slack_minutes (default `3.0`): used only when `fastest_balanced`
- charge_supply_ratio (default `2.0`): when `eligible_count / pending_count >= ratio`, idle vehicles with SOC < `soc_charge_start` may charge even while requests are pending
- max_concurrent_charging_pct (default `0.15`): hard cap on fraction of fleet in TO_DEPOT or CHARGING simultaneously
- min_plug_duration_minutes (default `0.0`): minimum plug dwell per session (minutes). Charge session duration used for `CHARGING_COMPLETE` scheduling and for reservation slot length is `max(energy_seconds_to_reach_soc_target, min_plug_duration_minutes × 60)`. **During live simulation**, intermediate SOC while plugged in is not modeled: `vehicle.soc` stays at plug-in SOC until `CHARGING_COMPLETE`, when it is set to `soc_target`. **End-of-run metrics** (§16 `fleet_battery_pct`, `fleet_soc_median_pct`, `vehicles_below_soc_target_*`): immediately before computing metrics, for each vehicle still in state `CHARGING`, the engine replaces SOC with `min(soc_target, plug_in_soc + kW_session_start × elapsed_seconds / (3600 × battery_kwh))`, where `elapsed_seconds` is time since session start capped at the scheduled `CHARGING_COMPLETE` time, and `kW_session_start` is `depot.effective_charger_kw()` at plug-in (same instant as session duration calculation). Time-series snapshots during the run record **live** stair-step `vehicle.soc` (unchanged for dispatch). Each bucket also includes **`fleet_mean_soc_pct`**: mean of `vehicle.soc × 100` across the fleet at that snapshot (vehicles in `CHARGING` still show plug-in SOC until `CHARGING_COMPLETE`; this is **not** the same as end-of-run horizon interpolation in §16).

**Repeated-day demand (one clock):** `load_requests_repeated_days(parquet_path, duration_minutes_per_day, num_days, …)` loads one synthetic day via `load_requests`, then duplicates it `num_days` times with request times offset by `k × duration_minutes_per_day × 60` seconds and unique ids `req_d{k}_{base_id}`. Total horizon is `num_days × duration_minutes_per_day` minutes on a single `SimulationEngine` run (no midnight state reset).
- **Effective `soc_charge_start` when `min_plug_duration_minutes > 0`:** voluntary charging (VEHICLE_IDLE priorities 1–2, CHARGE_DEPARTURE eligibility) uses `min(configured soc_charge_start, soc_target − Δ)` where `Δ = min(kW_slow × min_plug_duration_minutes / 60 / battery_kwh, soc_target − soc_min)` and `kW_slow` is the minimum `effective_charger_kw()` across depots (conservative). This avoids routing vehicles for top-ups smaller than one minimum dwell. `soc_min` mandatory charging is unchanged. End-of-run metrics that take `soc_charge_start` (e.g. below-target exemption with min plug) use the same effective value.

### 11.0 Charging priority in VEHICLE_IDLE

1. **Mandatory charge** (Priority 0): if `vehicle.soc < soc_min` and depots exist, force-route to charger and remove from dispatch pool. `find_best_vehicle` also rejects vehicles below `soc_min`.
2. **Supply-aware charging** (within Priority 1): if requests are pending but `eligible/pending >= charge_supply_ratio` AND `charging_count < max_concurrent_charging_pct * fleet_size`, vehicles with `soc < soc_charge_start` may reserve a charger while staying dispatch-eligible.
3. **Idle charging** (Priority 2): if no requests pending and `soc < soc_charge_start`, reserve as before.

When a vehicle becomes idle and SOC < soc_charge_start:
- reserve a charging slot and schedule CHARGE_DEPARTURE / ARRIVE_DEPOT per §8.1 (not “nearest depot” only).
- The reservation is a **hard lock**: it remains in the per-vehicle and per-depot reservation maps through TO_DEPOT travel. It blocks the slot for other vehicles’ reservation attempts (via `_charging_jobs_for_depot`). It is released only at ARRIVE_DEPOT (consumed) or by dispatch preempt (`_assign_vehicle_to_request`). Each reservation carries a `gen` counter; the corresponding `CHARGE_DEPARTURE` event embeds this `gen` so that stale events from superseded reservations are discarded (see §5 ARRIVE_DEPOT).

### 11.1 Depot selection (reservation)

For each depot the engine computes earliest feasible plug window after travel and the implied **depart_time** (when the vehicle should leave for the depot to meet that window).

- **`fastest`:** choose the depot minimizing `(depart_time, travel_seconds, depot_id)` (deterministic tie-break).
- **`fastest_balanced`:** let `t*` = minimum depart_time across depots. Among depots with `depart_time ≤ t* + charging_depot_balance_slack_minutes` (converted to seconds), choose minimizing `(pressure, depart_time, travel_seconds, depot_id)` where **pressure** = `len(depot.queue) + active_chargers + count(reservations at that depot)` after clearing this vehicle’s prior reservation. If the slack set is empty (should not happen), fall back to all depots.

This spreads load across similarly-fast options without forcing long detours to distant empty sites.

---

# 12. Repositioning (MVP Architecture)

Repositioning is an optional policy layer to reduce pickup ETAs by moving idle supply toward expected demand. It must be configurable for A/B testing.

### Configuration (required)
- reposition_enabled: bool (default true)
- reposition_alpha: float in [0,1] (default 0.6)
  - 1.0 purely reactive, 0.0 purely forecast
- reposition_half_life_minutes: int (default 45)
- reposition_forecast_horizon_minutes: int (default 30)
- max_reposition_travel_minutes: int (default 12)
- max_vehicles_targeting_cell: int (default 3)
- reposition_min_idle_minutes: int (default 2)
- reposition_top_k_cells: int (default 50)

### Signals
(A) Reactive score:
- maintain reactive_score[h3] with exponential decay on REQUEST_ARRIVAL:
  - decay = 0.5 ** (Δt_minutes / reposition_half_life_minutes)
  - reactive_score[h3] = reactive_score[h3] * decay + 1

(B) Forecast score:
- forecast_score[h3] = expected request arrivals in next horizon window
- Derived from:
  - historical RideAustin arrival rate by time-of-day
  - spatial distribution of requests aggregated by H3

Forecasts are computed from the preprocessed RideAustin dataset rather than synthetic POI weights.

Blended score:
- target_score[h3] = reposition_alpha*reactive_score[h3] + (1-reposition_alpha)*forecast_score[h3]

### Target selection
On VEHICLE_IDLE (after dispatch + charging checks), if repositioning triggers:
1) Candidate cells = top K by target_score within ODD.
2) Filter:
   - vehicles_targeting[h3] < max_vehicles_targeting_cell
   - travel_time_to_cell ≤ max_reposition_travel_minutes
3) Choose target maximizing:
   - utility = target_score[h3] - λ * travel_time_minutes
4) If chosen:
   - set state REPOSITIONING, set reposition_target_h3
   - schedule REPOSITION_COMPLETE

Dispatch preemption:
- If a REPOSITIONING vehicle is dispatched, cancel reposition_target_h3 and transition to TO_PICKUP.

## 12.1 Repositioning Oversupply Guard

At high fleet/demand ratios, all repositioning slots fill quickly but idle vehicles keep
calling the expensive `select_target` scan (O(top_k) routing lookups) on every VEHICLE_IDLE.

Guard implemented in engine:
- Track `_repositioning_count` (active vehicles in REPOSITIONING state).
- Maximum slots = `reposition_top_k_cells × max_vehicles_targeting_cell` (default 150).
- If `_repositioning_count ≥ max_slots`, skip `select_target` entirely (O(1) check).
- `_repositioning_count` is incremented on reposition start; decremented on
  REPOSITION_COMPLETE or dispatch preemption.

## 12.2 Reposition lambda (travel cost weight)

- `reposition_lambda` (default 0.05): penalizes long repositioning trips.
- `utility = blended_score - lambda * travel_time_minutes`

---

# 13. Demand Input (MVP: RideAustin-Based)

For MVP, demand is sourced from a preprocessed RideAustin trip dataset rather than synthetic POI-based generation.

## 13.1 Source Data

RideAustin dataset fields used:
- started_on (trip start timestamp)
- completed_on (trip end timestamp)
- start_location_lat / start_location_long (note: column names differ from generic lat/lng)
- end_location_lat / end_location_long
- distance_travelled (meters; optional validation only)

Only origin/destination coordinates and timestamps are required for MVP.

## 13.2 One-Time Preprocessing (Required)

Script:
- `scripts/preprocess_rideaustin_requests.py`

Output artifact:
- `data/requests_austin_h3_r8.parquet` (867,791 trips after cleaning)

Required columns:
- `request_time_seconds` (seconds since midnight — see §13.3 below)
- `origin_h3` (H3 resolution 8)
- `destination_h3` (H3 resolution 8)

Optional (for validation only):
- `observed_trip_duration_seconds`
- `observed_distance_miles` (converted from meters)

### Cleaning filters applied (in order)
1. Austin ODD bbox: lat 30.0–30.7°N, lng 98.2–97.4°W (removes ~800 coordinate outliers)
2. Duration filter: 1–240 minutes (removes negative and multi-day anomalies)
3. Distance filter: ≤ 35 miles / 56,327 meters (removes ~900 extreme outliers at p99.9+)
4. Same-cell filter: origin_h3 ≠ destination_h3

Final dataset characteristics:
- 867,791 trips
- Median trip duration: 11.3 min | p90: 22.5 min
- Median trip distance: 3.75 miles | p90: 11.7 miles
- 1,677 unique origin H3 cells, 1,957 destination cells

## 13.3 Synthetic Collapsed Day

All historical trips are collapsed to a single synthetic 24-hour day by retaining only
time-of-day (hour/minute/second) from each timestamp — the calendar date is discarded.

This overlays ~8 months of historical trips onto one 86,400-second clock, producing a
dense, representative Austin demand profile with preserved time-of-day peaks.

`request_time_seconds` = seconds since midnight (0–86399).

Austin demand peaks at 2–4 AM (nightlife) with ~65,000 trips/hour in the collapsed day.
The quietest period is 9–11 AM (~8,000–10,000 trips/hour).

## 13.4 Runtime Behavior

- `day_offset_seconds` selects the start of the simulation window within the 24-hour day.
- `duration_minutes` sets the window length when `demand.repeat_num_days` is 1.
- `demand.repeat_num_days` (default 1): when greater than 1, requests are loaded with `load_requests_repeated_days` using `demand.duration_minutes_per_day` (default 1440 when omitted) for the template slice, tiled `repeat_num_days` times on one clock; the simulation horizon matches `repeat_num_days × (duration_minutes_per_day or 1440)` minutes.
- `demand_scale` subsamples trips (1.0 = all trips in window, 0.02 = 2%, 2.0 = double via sampling with replacement). Subsampling is seeded for determinism.
- Simulator schedules REQUEST_ARRIVAL events based on `request_time_seconds`.
- Demand-control layer (flex, pooling, prebooking, off-peak shift) modifies requests after loading but before dispatch decisions.

### demand_scale guidance (200-vehicle fleet, 24-hour window)
- `0.01` → ~8,700 trips/day (~360/hr avg) — light load
- `0.02` → ~17,400 trips/day (~725/hr avg) — default, moderate with peak stress
- `0.05` → ~43,400 trips/day (~1,800/hr avg) — fleet saturated at peak
- `1.0`  → ~867,800 trips/day — stress test / fleet sizing research

## 13.5 Synthetic Demand Model (First-Principles)

A first-principles demand generation model produces synthetic trip parquets from
Census, POI, transit, and travel-time data — no RideAustin dependency. The output
uses the same schema (`request_time_seconds`, `origin_h3`, `destination_h3`) so the
sim engine requires zero changes.

### Data sources

| Source | What it provides |
|--------|-----------------|
| Census LODES | Home-to-work OD flows (commute matrix) at census block level for Travis County |
| Census ACS | Population density, car-free household rate, median income, median age by tract |
| OSMnx POIs | Categorized amenities: entertainment, medical, shopping, university, hotel, airport, leisure |
| Capital Metro GTFS | Transit stop locations + peak-hour service frequency → transit access score per H3 cell |
| OSRM travel cache | H3-to-H3 travel times for gravity model impedance (reuses sim's existing cache) |
| NHTS 2022 | Hourly trip-start distributions by purpose (commute, social, errands, tourism) and day type |
| ABIA stats | Published passenger statistics (~18M/year) for airport special generator calibration |

### Model structure

1. **Trip generation**: Per-cell per-hour rates by purpose, modulated by population, car-free demographics, transit access, and temporal profile.
2. **Trip distribution**: Gravity model `P(j|i) = A_j * exp(-beta * t_ij) / Z_i` with purpose-specific attraction vectors and AM/PM commute symmetry.
3. **Airport generator**: Separate Poisson process for ABIA with flight-schedule-aligned temporal profile.
4. **Event injection**: Optional demand hotspot at a specific cell and time window.
5. **Poisson sampling**: Seeded `numpy.random.Generator` for deterministic output.

### Tunable parameters (18 total)

| Parameter | Type | Range | Default | Effect |
|-----------|------|-------|---------|--------|
| demand_intensity | float | 0.1–5.0 | 1.0 | Master volume knob |
| duration_hours | int | 1–168 | 24 | Hours of demand |
| day_type | enum | weekday/saturday/sunday | weekday | Temporal profile shape |
| peak_sharpness | float | 0.1–3.0 | 1.0 | Peak exaggeration |
| beta | float | 0.01–0.30 | 0.08 | Gravity distance decay |
| commute_weight | float | 0.0–1.0 | 0.40 | Commute vs activity split |
| transit_suppression | float | 0.0–1.0 | 0.3 | Transit competition |
| tourism_intensity | float | 0.0–5.0 | 1.0 | Visitor trip multiplier |
| airport_boost | float | 0.5–10.0 | 1.0 | ABIA mode-share multiplier; 1.0 ≈ 6% of pax |
| entertainment_weight | float | 0.0–5.0 | 1.5 | Nightlife/restaurant pull |
| employment_weight | float | 0.0–5.0 | 1.0 | Jobs pull |
| medical_weight | float | 0.0–3.0 | 0.8 | Hospital pull |
| carfree_boost | float | 1.0–5.0 | 2.0 | Car-free household multiplier |
| event_h3 | str/null | any H3 cell | null | Event location |
| event_start_hour | float/null | 0–23 | null | Event start time |
| event_duration_hours | float/null | 1–8 | null | Event duration |
| event_multiplier | float/null | 2.0–20.0 | null | Event demand spike |
| seed | int | any | 42 | RNG seed |

### Usage

```bash
# Generate with defaults
python -m demand_model.generate --output data/synthetic_demand.parquet

# Generate weekend scenario with high tourism
python -m demand_model.generate --day-type saturday --tourism-intensity 3.0 --output data/weekend_tourism.parquet

# Validate output
python -m demand_model.validate data/synthetic_demand.parquet
```

### Experimenter integration

The autonomous experimenter can set `demand_config` on any `SimRun` to use synthetic
demand instead of RideAustin. When `demand_config` is null, the run uses the existing
parquet. When set, the generated script calls the demand model before running the sim.
Parquets are cached by config fingerprint to avoid regeneration.

---

# 14. Demand Control Layer (Growth Levers)

## 14.1 Flex Window
- flex_pct, flex_minutes
- if selected: latest_departure_time = request_time + flex_minutes

## 14.2 Pooling
- pool_pct, max_detour_pct
- MVP: merge two eligible requests if they appear within X minutes and detour <= max_detour_pct

## 14.3 Prebooking
- prebook_pct, eta_threshold_minutes, prebook_shift_minutes
- if predicted wait > threshold: shift earlier by prebook_shift_minutes

## 14.4 Off-Peak Incentive
- offpeak_shift_pct
- move fraction of peak demand into shoulder windows

---

# 15. One-time Setup (MVP Required)

Two one-time setup tasks are required:

## 1) Preprocess RideAustin Requests

- Convert RideAustin CSV into H3-indexed request dataset.
- Script: `scripts/preprocess_rideaustin_requests.py`
- Output: `data/requests_austin_h3_r8.parquet`

This dataset is required for simulation demand input.

## 2) Precompute H3↔H3 Travel Cache

Script: `scripts/precompute_h3_travel_cache.py`
Output: `data/h3_travel_cache.parquet`

Uses a three-layer hybrid strategy derived from dataset analysis
(1,992 active cells out of 7,094 Austin bbox cells; 83,208 actual O-D pairs):

### Layer 1 — Data-driven pairs (~83K)
All actual origin→destination H3 pairs from `data/requests_austin_h3_r8.parquet`.
These cover 100% of trip dispatch lookups.

### Layer 2 — Active-cell k-NN (~20K, default k=10)
k nearest H3 grid neighbours (via `h3.grid_ring`) for each of the 1,992 active
cells. Covers repositioning moves to nearby cells not in historical O-D data.
Only active cells are considered as neighbours (ignores the 5,114 dead cells).

### Layer 3 — Depot routing (~2K × n_depots)
Every active cell ↔ each depot cell (bidirectional). Covers TO_DEPOT routing
for charging decisions.

**Continuous multi-day harness** (`scripts/run_exp63_continuous_multiday_steady_state.py`):
by default depots are the **`n_sites` H3 cells with highest trip-origin counts** in
`data/requests_austin_h3_r8.parquet` (same ordering as `top_demand_cells(n)`).
Helpers `top_destination_cells(n)` and `top_origin_plus_destination_cells(n)` use the same
parquet for **destination-only** or **origin+destination** counts when choosing depot lists.
To override, pass **`depot_h3_cells=[...]`** into `run_continuous_experiment` (list length must
equal **`n_sites`**) or CLI **`--depot-cells h3_a,h3_b,...`** with **`--sites`** equal to the
number of comma-separated ids. Optional kwargs / CLI **`charging_queue_policy`**: **`"fifo"`** (default) or **`"jit"`** (`--charging-queue-policy` on the harness script). Depot cells must remain in the active demand / cache footprint
so Layer 3 pairs exist.

Total: ~105K unique pairs (vs naive k-NN on all Austin cells: 141K pairs that
would miss 42K actual trip pairs and query 5,114 cells the sim never uses).

The runtime simulator:
- Loads travel cache at startup into an in-memory dict keyed by (origin_h3, dest_h3).
- Falls back to live OSRM call on cache miss (result stored in memory for the run).

---

# 16. Metrics (Required Outputs)

## Service
- p10_wait_min
- median_wait_min (p50 rider wait; same distribution as p10/p90)
- p90_wait_min
- served_pct
- unserved_count

## Fleet
- trips_per_vehicle_per_day
- utilization_pct
- deadhead_pct
- avg_dispatch_distance

## Charging
- depot_queue_p90_min
- depot_queue_max_concurrent
- depot_queue_max_at_site
- charger_utilization_pct — `100 × (sum of scheduled charge-session durations in seconds) / (total physical plugs × simulation duration in seconds)`. Same as the time-average fraction of the **fleet-wide** plug inventory that is busy; **not** per-depot utilization (many idle microsites suppress the percentage even when some sites have long FIFO queues).
- charger_utilization_by_depot_pct — map `depot_id → float` (percentage). For each depot: `100 × (sum of that depot’s scheduled charge-session durations) / (that depot’s chargers_count × simulation duration)`. Plug-count-weighted average of these values equals `charger_utilization_pct` within rounding.
- fleet_battery_pct (mean fleet SOC × 100 at horizon) — uses **interpolated** SOC for vehicles still in `CHARGING` at horizon (§11); all others use `vehicle.soc` as stored after the last event.
- fleet_soc_median_pct (median fleet SOC × 100 at horizon) — same interpolation rule as `fleet_battery_pct`.
- vehicles_below_soc_target_count — after applying horizon charging SOC interpolation, count vehicles with SOC strictly below `soc_target`, **except** when `min_plug_duration_minutes > 0`: do not count vehicles in state CHARGING whose SOC is already ≥ `soc_charge_start` (ops: mandatory dwell in progress).
- vehicles_below_soc_target_strict_count — after interpolation, always count vehicles with SOC strictly below `soc_target` at horizon (companion to above).
- total_charge_sessions (sum of completed charge sessions across the fleet)
- **Depot throughput (simulation clock, not wall clock):** Hour index \(h = \lfloor t / 3600\text{s} \rfloor\) for event time \(t\). Peaks are maxima over \(h\) within \([0, \text{duration\_minutes} \times 60)\).
- depot_arrivals_total — count of processed `ARRIVE_DEPOT` events (vehicle reaches depot cell): includes sessions that immediately plug in, FIFO waiters, and JIT bounces (arrival without reservation when plugs are full).
- depot_arrivals_by_depot_id — map `depot_id → int` arrival count at that depot.
- depot_jit_plug_full_total — arrivals under JIT policy where `active_chargers == chargers_count` at arrival time (vehicle returns to `IDLE` without plugging in). Increments regardless of reservation status — the cap is enforced uniformly. Sum over depots.
- depot_jit_plug_full_by_depot_id — map `depot_id → int` JIT bounce count.
- depot_charge_completions_total — count of `CHARGING_COMPLETE` events (successful charge sessions ending at `soc_target`).
- depot_charge_completions_by_depot_id — map `depot_id → int` completion count.
- depot_arrivals_peak_fleet_per_hour — \(\max_h \sum_d \text{arrivals}(d, h)\): busiest simulation-hour for **fleet-wide** depot arrivals (same-hour sum across depots).
- depot_arrivals_peak_max_site_per_hour — \(\max_d \big(\max_h \text{arrivals}(d,h)\big)\): over depots, the largest single-depot single-hour arrival count.
- depot_charge_completions_peak_fleet_per_hour — same as arrivals peak, but using completion counts.
- depot_charge_completions_peak_max_site_per_hour — same as arrivals max-site peak, but for completions.
- charging_session_duration_median_min / charging_session_duration_p90_min — minutes from plug-in (`CHARGING` session start) to `CHARGING_COMPLETE`, across completed sessions only (empty run → `0`).

## Economics (itemized cost model)

### Vehicle presets

Two presets define the vehicle-specific parameters. `vehicle_cost_usd` is computed dynamically:
`vehicle_cost_usd = base_vehicle_cost_usd + battery_kwh × battery_cost_per_kwh` (battery_cost_per_kwh = $100/kWh).

| Param | Tesla (Cybercab) | Waymo (Ioniq 5) |
|-------|-----------------|-----------------|
| `base_vehicle_cost_usd` | 22,500 | 72,500 |
| `vehicle_cost_usd` (at 75 kWh) | 30,000 | 80,000 |
| `vehicle_cost_usd` (at 40 kWh) | 26,500 | 76,500 |
| `kwh_per_mile` | 0.20 | 0.30 |
| `maintenance_cost_per_mile` | 0.03 | 0.05 |

### Charger tiers

| Tier | kW | Capital/post | Amortized $/post/day (10yr) |
|------|----|--------------|-----------------------------|
| Wall Connector | 11.5 | $2,850 | $0.78 |
| V2 Shared Pair | 75 | $31,250 | $8.56 |
| V3 Shared 4-post | 96 | $40,313 | $11.04 |
| V4 Shared 8-post | 150 | $62,500 | $17.12 |

### Cost formula

```
vehicle_cost_usd = base_vehicle_cost_usd + battery_kwh × battery_cost_per_kwh

energy_cost      = total_miles × kwh_per_mile × electricity_cost_per_kwh
demand_cost      = (n_sites × plugs_per_site × charger_kw) × demand_charge_per_kw_month × (sim_days/30)
maintenance_cost = total_miles × maintenance_cost_per_mile
fleet_fixed_cost = n_vehicles × sim_days × (vehicle_cost_usd/(lifespan_years×365)
                   + insurance + teleops + cleaning)
infra_cost       = sim_days × (n_sites × cost_per_site_day + Σ(depot_plugs × tier_cost_per_day))
total_system_cost_per_trip = (energy + demand + maintenance + fleet + infra) / served_count
```

### Economics config params

| Param | Default | Unit |
|-------|---------|------|
| `electricity_cost_per_kwh` | 0.068 | $/kWh |
| `demand_charge_per_kw_month` | 13.56 | $/kW-month |
| `maintenance_cost_per_mile` | 0.03 | $/mile |
| `insurance_cost_per_vehicle_day` | 4.00 | $/veh/day |
| `teleops_cost_per_vehicle_day` | 3.50 | $/veh/day |
| `cleaning_cost_per_vehicle_day` | 6.00 | $/veh/day |
| `base_vehicle_cost_usd` | 22,500 | $ |
| `battery_cost_per_kwh` | 100 | $/kWh |
| `vehicle_cost_usd` | computed | $ (= base + battery_kwh × 100) |
| `vehicle_lifespan_years` | 5 | years |
| `cost_per_site_day` | 250 | $/site/day |

### Output metrics (economics)

- `energy_cost` — total energy cost ($)
- `demand_cost` — demand charge cost ($)
- `maintenance_cost` — total maintenance cost ($)
- `fleet_fixed_cost` — fleet depreciation + insurance + teleops + cleaning ($)
- `infra_cost` — depot site + plug amortization ($)
- `total_system_cost` — sum of all five ($)
- `total_system_cost_per_trip` — total_system_cost / served_count ($)
- `system_margin_per_trip` — avg_revenue_per_trip − total_system_cost_per_trip ($)
- `depreciation_per_vehicle_day` — vehicle_cost_usd / (lifespan × 365) ($/veh/day)
- `cost_per_trip` — alias for total_system_cost_per_trip ($)
- `cost_per_mile` — total_system_cost / total_miles ($)
- `fixed_cost_total` — alias for fleet_fixed_cost ($)
- `avg_revenue_per_trip` — mean fare per served trip ($)
- `revenue_total` — sum of all fares ($)
- `contribution_margin_per_trip` — alias for system_margin_per_trip ($)
- `total_margin` — revenue_total − total_system_cost ($)

---

# 17. API

## POST /run
Input: scenario config (fleet, depots, demand, demand-control, repositioning, `charging_queue_policy`, `charging_depot_selection`, `charging_depot_balance_slack_minutes`, `min_plug_duration_minutes`, seed)
Returns:
- metrics
- timeseries: list of buckets with `t_minutes`, `idle_count`, `to_pickup_count` (vehicles in `TO_PICKUP`), `in_trip_count`, `charging_count` (includes `CHARGING` + `TO_DEPOT`), `repositioning_count`, `pending_requests`, `eligible_count`, `served_cumulative`, `unserved_cumulative`, `fleet_mean_soc_pct`, and optional `depot_snapshots` for dashboard
- event_counts (dict[str, int] — one entry per EventType)
- routing_stats (cache_hits, cache_misses, hit_rate_pct, new_entries, cache_size)

## POST /compare
Input:
- baseline config
- variant config
- same seed
Returns:
- baseline metrics
- variant metrics
- deltas (scalar fields are variant − baseline; `charger_utilization_by_depot_pct` is per-depot pp delta, keys = union of depot ids)
- short insight strings

---

# 18. Tests (Acceptance Criteria)

Deterministic:
- same seed → identical metrics within tolerance

Invariants:
- served_count ≤ request_count
- 0 ≤ soc ≤ 1 always
- active_chargers ≤ chargers_count
- no negative travel times

Sanity:
- increasing fleet size should not worsen p90_wait (monotonic tolerance)

---

# 19. Definition of Done

- docker compose up launches backend + OSRM
- /docs loads
- /run returns valid metrics
- /compare returns baseline vs variant deltas
- pytest passes
- GROWTH_MAPPING.md links simulator knobs to robotaxi growth experiments