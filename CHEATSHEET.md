# Robotaxi-Sim Cheatsheet

Quick reference for all simulation parameters, output metrics, the cost model, and key terminology. Check `default_scenario.json` for the live defaults.

---

## Output Metrics


| Metric | What it means | Typical range |
| --- | --- | --- |
| `p10_wait_min` | p10 rider wait from request to vehicle arrival (minutes) | 1–5 min |
| `median_wait_min` | p50 rider wait from request to vehicle arrival (minutes) | 3–8 min |
| `p90_wait_min` | p90 rider wait — the "worst realistic" wait | 8–20 min |
| `served_pct` | % of requests that got a vehicle before expiry | 40–97% |
| `served_count` | Raw count of completed trips | — |
| `unserved_count` | Raw count of requests that expired unserved | — |
| `sla_adherence_pct` | % of **all** requests (incl. unserved) where wait ≤ `max_wait_time_seconds` | 30–97% |
| `trips_per_vehicle_per_day` | Average rides completed per vehicle per 24 h | 5–20 |
| `utilization_pct` | % of total fleet miles that are revenue miles (in-trip) | 40–70% |
| `deadhead_pct` | % of total miles that are non-revenue (pickup + reposition combined) | 30–60% |
| `repositioning_pct` | % of total miles spent proactively repositioning (subset of deadhead) | 0–10% |
| `avg_dispatch_distance` | Mean pickup (deadhead) distance per trip in miles | 0.5–3 mi |
| `depot_queue_p90_min` | p90 wait in queue for a charger (minutes) | 0–5 min |
| `depot_queue_max_concurrent` | Peak total charging wait-queue depth across all depots (waiting only, not active plugs) | 0–1000+ |
| `depot_queue_max_at_site` | Peak charging wait-queue depth at a single depot (waiting only, not active plugs) | 0–1000+ |
| `charger_utilization_pct` | `100 × Σ(charge session durations) ÷ (total plug count × sim length)` — time-average fleet-wide share of plugs delivering power | 1–15% typical for many microsites |
| `charger_utilization_by_depot_pct` | Object keyed by `depot_id`: per-site % utilization | 0–100 per site |
| `depot_charger_util_max_pct` | Highest single-depot charger utilization | 0–100% |
| `depot_charger_util_mean_pct` | Unweighted mean of per-depot charger utilization | 0–100% |
| `depot_charger_util_p90_pct` | p90 of per-depot charger utilization | 0–100% |
| `depot_charger_util_nonzero_count` | Number of depots with utilization > 0.01% | 0–n_sites |
| `fleet_battery_pct` | Mean fleet SOC at horizon × 100 (with CHARGING interpolation) | 40–90 |
| `fleet_soc_median_pct` | Median fleet SOC at horizon × 100 | 40–90 |
| `fleet_mean_soc_pct` (timeseries) | Per-bucket mean SOC × 100 (stair-step, not interpolated) | varies |
| `to_pickup_count` (timeseries) | Vehicles en route to pickup (`TO_PICKUP`); stacked with idle / in trip / charging / repositioning sums to fleet size | 0–fleet |
| `vehicles_below_soc_target_count` | After interpolation: SOC `< soc_target` (with min-plug exemption) | 0–fleet_size |
| `vehicles_below_soc_target_strict_count` | After interpolation: SOC strictly `< soc_target` (no exemption) | 0–fleet_size |
| `total_charge_sessions` | Sum of completed charge sessions across the fleet | — |
| `depot_arrivals_total` | Count of ARRIVE_DEPOT events (plug-in, FIFO wait, or JIT bounce) | — |
| `depot_arrivals_by_depot_id` | Per-depot arrival counts | — |
| `depot_jit_plug_full_total` | JIT policy: arrivals when every plug busy → immediate return to IDLE | 0+ |
| `depot_jit_plug_full_by_depot_id` | Per-depot JIT bounce counts | — |
| `depot_charge_completions_total` | Count of CHARGING_COMPLETE events | — |
| `depot_charge_completions_by_depot_id` | Per-depot completion counts | — |
| `depot_arrivals_peak_fleet_per_hour` | Max over sim hours of sum of arrivals at all depots | 0+ |
| `depot_arrivals_peak_max_site_per_hour` | Max of any single depot's busiest-hour arrivals | 0+ |
| `depot_charge_completions_peak_fleet_per_hour` | Fleet-wide busiest-hour completion count | 0+ |
| `depot_charge_completions_peak_max_site_per_hour` | Single-depot busiest-hour completion count | 0+ |
| `charging_session_duration_median_min` | Completed sessions: median plug-in → CHARGING_COMPLETE (minutes) | 0+ |
| `charging_session_duration_p90_min` | p90 of same session durations (minutes) | 0+ |
| `pool_match_pct` | % of pool-eligible requests that were paired with another rider | 0–60% |
| `energy_cost` | Total energy cost: `total_miles × kwh_per_mile × electricity_cost_per_kwh` ($) | varies |
| `demand_cost` | Demand charge cost: `installed_kw × $/kW-month × (sim_days / 30)` ($) | varies |
| `maintenance_cost` | Total maintenance: `total_miles × maintenance_cost_per_mile` ($) | varies |
| `fleet_fixed_cost` | Fleet overhead: `n_vehicles × sim_days × (depreciation + insurance + teleops + cleaning)` ($) | varies |
| `infra_cost` | Infrastructure: `sim_days × (n_sites × site_cost + total_plugs × plug_cost_per_day)` ($) | varies |
| `total_system_cost` | Sum of all five cost categories ($) | varies |
| `total_system_cost_per_trip` | `total_system_cost / served_count` ($) | $5–$50 |
| `system_margin_per_trip` | `avg_revenue_per_trip − total_system_cost_per_trip` ($) | varies |
| `depreciation_per_vehicle_day` | `vehicle_cost_usd / (vehicle_lifespan_years × 365)` ($/veh/day) | $8–$44 |
| `cost_per_trip` | Alias for `total_system_cost_per_trip` ($) | $5–$50 |
| `cost_per_mile` | `total_system_cost / total_miles` ($/mile) | varies |
| `fixed_cost_total` | Alias for `fleet_fixed_cost` ($) | varies |
| `avg_revenue_per_trip` | Mean fare collected per served trip ($) | $8–$16 |
| `revenue_total` | Sum of all fares ($) | varies |
| `contribution_margin_per_trip` | `avg_revenue_per_trip − total_system_cost_per_trip` ($) | varies |
| `total_margin` | `revenue_total − total_system_cost` ($) | varies |


---

## Scenario Parameters

### Top-level


| Parameter | Default | What it does |
| --- | --- | --- |
| `duration_minutes` | `1440` | Simulation length in minutes (1440 = full day). If `demand.repeat_num_days` > 1, the horizon is `repeat_num_days × (duration_minutes_per_day or 1440)`; set this to the same product to avoid a stderr note |
| `seed` | `42` | RNG seed — change for a different trip draw, same seed = deterministic |
| `timeseries_bucket_minutes` | `1` | Snapshot interval for time-series output (dashboard scrubber step) |
| `charging_queue_policy` | `jit` | `jit` = if plugs busy at arrival, return to IDLE and replan; `fifo` = wait in depot queue |
| `charging_depot_selection` | `fastest` | `fastest` = pick depot with earliest time-to-plug; `fastest_balanced` = among depots within slack, pick lowest load |
| `charging_depot_balance_slack_minutes` | `3.0` | Max extra minutes past the best depart_time still considered "as fast" for `fastest_balanced` |
| `min_plug_duration_minutes` | `0.0` | Minimum scheduled charge session length; tightens effective `soc_charge_start` when >0 |


### `fleet`


| Parameter | Default | What it does |
| --- | --- | --- |
| `size` | `200` | Number of vehicles in the fleet |
| `battery_kwh` | `75.0` | Usable battery capacity per vehicle (Tesla Cybercab: 35 kWh) |
| `kwh_per_mile` | `0.20` | Energy consumption rate (see vehicle presets below) |
| `soc_initial` | `0.80` | Starting state-of-charge (0–1) |
| `soc_min` | `0.20` | SOC floor — vehicle heads to depot for charging below this |
| `soc_charge_start` | `0.80` | Hysteresis start threshold — idle vehicle begins charging below this SOC |
| `soc_target` | `0.80` | SOC level at which charging stops |
| `soc_buffer` | `0.05` | Safety margin before forced dispatch despite low SOC |


### `demand`


| Parameter | Default | What it does |
| --- | --- | --- |
| `demand_scale` | `0.02` | Fraction of the collapsed day's trips to simulate (0.02 = 2% ≈ 4k requests/day) |
| `demand_flatten` | `0.0` | 0 = historical peak/trough pattern; 1 = fully uniform |
| `max_wait_time_seconds` | `600` | Per-request SLA: expire (unserved) after this many seconds |
| `repeat_num_days` | `1` | Tile the same synthetic-day demand this many times on one continuous clock (`/run`, dashboard). `1` = single window from `duration_minutes` (default behavior) |
| `duration_minutes_per_day` | `null` | When `repeat_num_days` > 1: length of each template day in minutes; `null` → **1440** (full synthetic day) |
| `pool_pct` | `0.0` | Fraction of requests eligible for ride-pooling (0 = disabled) |
| `coverage_polygon` | `null` | Optional GeoJSON polygon to restrict service area |


### `dispatch`


| Parameter | Default | What it does |
| --- | --- | --- |
| `strategy` | `"nearest"` | `"nearest"` = minimize ETA; `"first_feasible"` = take first vehicle under threshold |
| `first_feasible_threshold_seconds` | `300` | ETA cutoff for `first_feasible` strategy (5 min) |


### `repositioning`


| Parameter | Default | What it does |
| --- | --- | --- |
| `reposition_enabled` | `true` | Enable/disable the repositioning policy entirely |
| `reposition_alpha` | `0.6` | Blend weight: 0 = pure reactive (historical demand), 1 = pure forecast |
| `reposition_lambda` | `0.05` | Travel cost penalty in utility: `utility = blended_score − lambda × travel_min` |
| `reposition_half_life_minutes` | `45` | Decay half-life for historical demand heat |
| `reposition_forecast_horizon_minutes` | `30` | How far ahead the forecast looks for demand |
| `max_reposition_travel_minutes` | `12` | Maximum travel time to a reposition target |
| `max_vehicles_targeting_cell` | `3` | Cap on how many vehicles can reposition toward the same H3 cell |
| `reposition_min_idle_minutes` | `2` | Minimum idle time before a vehicle is eligible for repositioning |
| `reposition_top_k_cells` | `50` | Number of top-scored candidate cells considered |


### `demand_control`


| Parameter | Default | What it does |
| --- | --- | --- |
| `max_detour_pct` | `0.0` | Maximum detour allowed to pick up a pool partner (0 = pooling off) |


### Depot / Infrastructure

These parameters control the charging depot network. In experiment scripts they appear as `n_sites`, `plugs_per_site`, and `charger_kw`. In the API they map to `depots[].chargers_count` and `depots[].charger_kw`.

| Parameter | Default | What it does |
| --- | --- | --- |
| `n_sites` | `1` | Number of charging depot sites |
| `plugs_per_site` | `20` | Charger plugs per depot |
| `charger_kw` | `150.0` | kW per plug (must be a real charger tier: 11.5, 75, 96, or 150) |


### Economics (itemized cost model)


| Parameter | Tesla default | Waymo default | What it does |
| --- | --- | --- | --- |
| `electricity_cost_per_kwh` | `0.068` | `0.068` | $/kWh at the meter (Austin Energy SV2) |
| `demand_charge_per_kw_month` | `13.56` | `13.56` | $/kW-month peak demand charge (Austin Energy SV2) |
| `maintenance_cost_per_mile` | `0.03` | `0.05` | $/mile — EV fleet avg; AV sensors add cost for Waymo |
| `insurance_cost_per_vehicle_day` | `4.00` | `4.00` | ~$1,400/yr liability-dominated |
| `teleops_cost_per_vehicle_day` | `3.50` | `3.50` | ~1:40 remote-ops ratio industry avg |
| `cleaning_cost_per_vehicle_day` | `6.00` | `6.00` | 1–2 cleanings/day |
| `base_vehicle_cost_usd` | `22,500` | `72,500` | Chassis + AV stack before battery |
| `battery_cost_per_kwh` | `100` | `100` | Pack-level battery cost ($/kWh, fixed) |
| `vehicle_cost_usd` | `30,000` | `80,000` | **Computed:** `base_vehicle_cost_usd + battery_kwh × battery_cost_per_kwh` |
| `vehicle_lifespan_years` | `5` | `5` | Straight-line depreciation period |
| `cost_per_site_day` | `250` | `250` | Lease + ops per depot site per day |
| `revenue_base` | `2.50` | `2.50` | Flag-fall base fee per trip ($) |
| `revenue_per_mile` | `1.50` | `1.50` | Per-mile component of fare ($) |
| `revenue_per_minute` | `0.35` | `0.35` | Per-minute component of fare ($) |
| `revenue_min_fare` | `5.00` | `5.00` | Minimum fare floor ($) |
| `pool_discount_pct` | `0.25` | `0.25` | Discount for pool-matched riders |


---

## Vehicle Presets

Only 3 parameters differ between presets. Use `vehicle_preset: "tesla"` or `"waymo"` in experiment runs. `vehicle_cost_usd` is computed dynamically: `base_vehicle_cost_usd + battery_kwh × battery_cost_per_kwh ($100/kWh)`.

| Parameter | Tesla (Cybercab) | Waymo (Ioniq 5) |
| --- | --- | --- |
| `base_vehicle_cost_usd` | $22,500 | $72,500 |
| `vehicle_cost_usd` (at 75 kWh) | $30,000 | $80,000 |
| `vehicle_cost_usd` (at 40 kWh) | $26,500 | $76,500 |
| `kwh_per_mile` | 0.20 | 0.30 |
| `maintenance_cost_per_mile` | $0.03 | $0.05 |

All other costs (electricity, demand charges, insurance, teleops, cleaning, infrastructure) are identical. Reducing `battery_kwh` lowers `vehicle_cost_usd` (cheaper fleet) but reduces range and increases charging frequency.

---

## Charger Tiers

Real-world charger products with 10-year straight-line amortization:

| Tier | kW/post | Capital cost/post | Amortized $/post/day |
| --- | --- | --- | --- |
| Wall Connector | 11.5 | $2,850 | $0.78 |
| V2 Shared Pair | 75 | $31,250 | $8.56 |
| V3 Shared 4-post | 96 | $40,313 | $11.04 |
| V4 Shared 8-post | 150 | $62,500 | $17.12 |

The simulation uses `charger_kw` to look up the correct tier for cost computation.

---

## Revenue & Cost Model

### Fare formula

```
gross_fare = max(revenue_min_fare,
                 revenue_base
                 + revenue_per_mile × trip_miles_direct
                 + revenue_per_minute × trip_duration_minutes)

rider_fare = gross_fare × (1 − pool_discount_pct)   if rider is pool_matched
           = gross_fare                               otherwise
```

**Key details:**

- `trip_miles_direct` and `trip_duration_minutes` always reflect the rider's **direct** origin→destination route, even if their vehicle took a detour (for pool trips).
- Default calibration: a 4.5 mi / 14 min trip yields `2.50 + 1.50×4.5 + 0.35×14 = $14.15` (solo); `$10.61` with the 25% pool discount.

### Itemized cost model

```
energy_cost      = total_miles × kwh_per_mile × electricity_cost_per_kwh
demand_cost      = total_installed_kw × demand_charge_per_kw_month × (sim_days / 30)
maintenance_cost = total_miles × maintenance_cost_per_mile
fleet_fixed_cost = n_vehicles × sim_days × (vehicle_cost_usd / (lifespan_years × 365)
                   + insurance + teleops + cleaning)
infra_cost       = sim_days × (n_sites × cost_per_site_day
                   + total_plugs × plug_cost_per_day)

total_system_cost = energy_cost + demand_cost + maintenance_cost + fleet_fixed_cost + infra_cost
total_system_cost_per_trip = total_system_cost / served_count
system_margin_per_trip = avg_revenue_per_trip − total_system_cost_per_trip
```

Where:
- `vehicle_cost_usd = base_vehicle_cost_usd + battery_kwh × battery_cost_per_kwh` (battery_cost_per_kwh = $100)
- `total_installed_kw = n_sites × plugs_per_site × charger_kw`
- `total_plugs = n_sites × plugs_per_site`
- `plug_cost_per_day` = amortized daily cost from the charger tier table above
- `depreciation_per_vehicle_day = vehicle_cost_usd / (vehicle_lifespan_years × 365)`

---

## Synthetic Demand Model Parameters

The demand model (`scripts/demand_model/`) generates trip parquets from first-principles data (Census, POIs, GTFS, OSRM). Set via `demand_config` in experimenter runs or CLI flags on `generate.py`. When `demand_config` is null, the sim uses the RideAustin parquet as before.

**Important**: When using `demand_config`, set `demand_scale` to **1.0** — the synthetic model controls volume via `demand_intensity`; subsampling would discard intentionally generated trips.

| Parameter | Default | Range | Effect |
| --- | --- | --- | --- |
| `demand_intensity` | 1.0 | 0.1–5.0 | Master volume knob: trips/person/hour multiplier |
| `duration_hours` | 24 | 1–168 | Hours of demand to generate |
| `day_type` | weekday | weekday/saturday/sunday | Selects NHTS temporal profile |
| `peak_sharpness` | 1.0 | 0.1–3.0 | Exponent on temporal profile peaks |
| `beta` | 0.08 | 0.01–0.30 | Gravity model distance decay (higher = more local trips) |
| `commute_weight` | 0.40 | 0.0–1.0 | Fraction of demand following LODES commute OD pairs |
| `transit_suppression` | 0.3 | 0.0–1.0 | How much good transit access reduces demand in a cell |
| `tourism_intensity` | 1.0 | 0.0–5.0 | Visitor trip multiplier (3.0+ = SXSW/ACL week) |
| `airport_boost` | 1.0 | 0.5–10.0 | ABIA mode-share multiplier; 1.0 ≈ 6% of pax → ~13% of trips |
| `entertainment_weight` | 1.5 | 0.0–5.0 | Pull of nightlife + restaurants |
| `employment_weight` | 1.0 | 0.0–5.0 | Pull of employment centers |
| `medical_weight` | 0.8 | 0.0–3.0 | Pull of hospitals/clinics |
| `carfree_boost` | 2.0 | 1.0–5.0 | Trip multiplier for car-free households |
| `event_h3` | null | any H3 cell | Special event location (null = no event) |
| `event_start_hour` | null | 0–23 | Event crowd arrival hour |
| `event_duration_hours` | null | 1–8 | Event surge duration |
| `event_multiplier` | null | 2.0–20.0 | Demand spike at event cell |
| `seed` | 42 | any int | Poisson sampling seed |

---

## Glossary


| Term | Definition |
| --- | --- |
| **Deadhead** | Any miles driven without a paying passenger — includes pickup miles and repositioning miles. |
| **Utilization** | Fraction of total vehicle-miles that are revenue (in-trip) miles. Higher = better vehicle economics. |
| **SLA (Service Level Agreement)** | The per-request wait-time target (`max_wait_time_seconds`). A request "adheres" if served within this window. |
| **H3 cell** | A hexagonal spatial unit from Uber's H3 library. Resolution 8 cells are ~0.5 km² each. |
| **Dispatch ETA** | Estimated time for a vehicle to reach a rider's pickup location (OSRM-backed). |
| **Pool match** | When two riders share a trip with a detour. Both get a discounted fare. |
| **Reposition utility** | Score: `utility = blended_demand_score − reposition_lambda × travel_minutes`. Positive = worth going. |
| **Blended demand score** | `alpha × forecast + (1−alpha) × reactive`. |
| **demand_scale** | Controls trip request volume. `0.02` means ~4k requests over 24h. |
| **demand_flatten** | Blends historical timestamps toward uniform. At `0.5`, each timestamp is halfway between real and uniform. |
| **Contribution margin (CM)** | Revenue minus total system cost per trip. |
| **total_margin** | `revenue_total − total_system_cost`. Direct output metric from `compute_metrics()`. |
| **SOC** | State of Charge — battery level as a fraction (0 = empty, 1 = full). |
| **Discrete-event simulation** | The sim advances by processing events in time order (min-heap). No fixed timestep. |
| **Pareto frontier** | The set of configs where no other config achieves both lower cost AND higher served%. Plotted as cost/trip (X) vs served% (Y). |
| **Charger tier** | A real-world charger product class (Wall Connector 11.5 kW, V2 75 kW, V3 96 kW, V4 150 kW) with associated capital cost and amortized daily cost. |
| **Vehicle preset** | Predefined set of vehicle-specific params (Tesla Cybercab or Waymo Ioniq 5). Only `vehicle_cost_usd`, `kwh_per_mile`, and `maintenance_cost_per_mile` differ. |
| **Demand charge** | Monthly utility charge based on peak installed kW capacity, independent of energy consumed. |
| **Installed capacity** | `n_sites × plugs_per_site × charger_kw` — total kW of charging infrastructure, drives demand charges. |
| **Steady-state stability** | A config is stable if day-3 served% is within 2 percentage points of day-1 served%. Unstable configs indicate vehicles can't recharge fast enough to sustain service. |
| **Gravity model** | Trip distribution model: `P(dest j | origin i) = A_j × exp(-beta × t_ij) / Z`. Attraction `A_j` is purpose-specific (employment, entertainment, etc.). |
| **LODES** | Census Longitudinal Employer-Household Dynamics Origin-Destination data. Provides home-to-work commute flows at census block level. |
| **NHTS** | National Household Travel Survey. Provides hourly trip-start distributions by trip purpose used as temporal profiles. |
| **Transit suppression** | Demand reduction in H3 cells with good public transit access (Capital Metro). Controlled by `transit_suppression` parameter. |
| **Trip purpose** | Demand model generates four types: commute (LODES-based), social (evening/entertainment), errands (midday/shopping), tourism (hotel-based). |
| **AM/PM symmetry** | Commute trips reverse direction: home→work in AM, work→home in PM. Gravity model uses employment attraction AM, population attraction PM. |
| **Demand config fingerprint** | SHA-256 hash of all 18 demand model parameters. Used to cache generated parquets — same params = same file. |


