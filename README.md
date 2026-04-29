# robotaxi-sim (Austin) — Demand Control + Ops Simulator

A discrete-event robotaxi simulator for evaluating how demand steering mechanisms (flex windows, pooling, prebooking, off-peak incentives) and operational policies (dispatch, charging, repositioning) impact service reliability and fleet economics.

This simulator uses:

- Real Austin ride-hailing demand (RideAustin dataset — 867,791 cleaned trips)
- H3 spatial indexing (resolution 8 default)
- OSRM-based routing (Texas PBF)
- H3-to-H3 travel time caching
- Configurable repositioning (reactive + forecast blend)
- Baseline vs variant A/B comparison via `/compare`
- End-of-run fleet SOC metrics (`fleet_battery_pct`, `fleet_soc_median_pct`, below-target counts) linearly estimate SOC for vehicles **still charging** at the horizon; during the run, SOC still updates only at `CHARGING_COMPLETE` (see `SPEC.md` §11).

See:
- `SPEC.md` for full implementation contract
- `EXPERIMENTS.md` for a curated summary of the public-facing findings
- `GROWTH_MAPPING.md` for alignment to Robotaxi Growth strategy

---

# Setup

## Prerequisites

- Docker Desktop
- Python 3.11 (`brew install python@3.11`)

## Step 1 — Install Python dependencies (tests + scripts)

```bash
python3.11 -m pip install h3 numpy pandas pyarrow pytest pytest-asyncio \
  fastapi httpx "pydantic>=2.6" uvicorn
```

## Step 2 — Preprocess RideAustin data (already done if parquet exists)

Place the RideAustin CSV in `data/raw/`. Then run:

```bash
python3.11 scripts/preprocess_rideaustin_requests.py
```

This produces `data/requests_austin_h3_r8.parquet` (867,791 trips).

**What the script does:**
- Reads `data/raw/*.csv`
- Normalizes column names (handles `start_location_lat/long` naming)
- Filters to Austin city bbox (30.0–30.7°N, 98.2–97.4°W)
- Removes trips with invalid durations (< 1 min or > 4 hours)
- Removes distance outliers (> 35 miles — ~0.1% of rows)
- **Collapses all trips to a single synthetic 24-hour day** using time-of-day only
  (strips the date, keeps hour/minute/second) — overlays all historical days to create
  a dense, representative Austin demand profile
- Converts coordinates to H3 cells at resolution 8
- Drops same-origin/destination cell trips

## Step 3 — Run tests

```bash
pytest -q
```

All 14 tests run without OSRM or the parquet file.

## Step 4 — Download Texas road network (one-time, ~1 GB)

```bash
python3.11 scripts/download_osrm_pbf.py
```

Downloads `osrm/data/texas-latest.osm.pbf` from Geofabrik. Only needed once — subsequent
`docker compose up` runs detect the file and skip the download.

## Step 5 — Start OSRM + API

```bash
docker compose up --build
```

**First run after PBF download (~5–15 min):** OSRM preprocesses the road network
(extract → partition → customize) and writes processed files to `osrm/data/`.
Subsequent starts detect these files and begin serving immediately (~5 seconds).

Services:
- **API**: http://localhost:8000 — FastAPI backend
- **OSRM**: http://localhost:5000 — routing engine
- **Swagger docs**: http://localhost:8000/docs

## Step 6 — Precompute H3 travel cache (requires OSRM running)

```bash
python3.11 scripts/precompute_h3_travel_cache.py
```

Uses a **hybrid three-layer strategy** (~105K pairs total):

- **Layer 1** — all 83K actual O-D pairs from the parquet (exact trip dispatch coverage)
- **Layer 2** — k=10 nearest active-cell neighbours per cell (~20K repositioning pairs)
- **Layer 3** — every active cell ↔ each depot (~2K charging routing pairs)

Only the 1,992 cells that actually appear in the dataset are queried — the 5,114 dead cells
(parks, highways, industrial zones) are skipped entirely. The sim loads this at startup and
falls back to live OSRM calls on any remaining cache miss.

**Dashboard map polylines (`data/route_geometry_cache.json`):** built when you run `scripts/dashboard.py`. If OSRM was offline, two-point “straight” segments get cached; remove them with `python3 scripts/prune_route_geometry_straight_fallbacks.py` (requires OSRM), then regenerate the HTML.

**Optional maps (Folium + OSMnx, writes HTML under `scripts/`):** `map_exp70_depots_central_vs_peripheral.py` (central vs OSM-industrial depots); `map_exp72_origins_destinations_industrial.py` (top origin/destination cells vs downtown + industrial overlap).

**Static “why central depots” plot (Matplotlib, no OSRM):** `plot_exp71_origin_nearest_depot_access.py` — demand-weighted ECDF of drive time from each trip **origin** to the **nearest** depot using `data/h3_travel_cache.parquet`; writes `plots/exp71_origin_nearest_depot_access.png`.

**N=2 site search (proxy, seconds):** `search_n2_depot_pairs_origin_access.py` — enumerates pairs from the top-M origin cells (plus optional top-K destinations), ranks by trip-weighted mean origin→nearest-depot time; use to screen pairs before a full `run_exp63` / Exp71-style sim. **`map_n2_depot_pair_search.py`** writes `scripts/map_n2_depot_pair_search.html` (Exp71 central vs top proxy pairs + OSM **industrial** / **commercial+retail** landuse polygons on a Folium map).

---

# Running the Simulator

## Run default scenario (full 24-hour day, 200 vehicles)

```bash
curl -X POST http://localhost:8000/run \
  -H "Content-Type: application/json" \
  -d @backend/app/default_scenario.json
```

**Three identical synthetic days (one continuous clock):** set `demand.repeat_num_days` to **3** and `duration_minutes` to **4320** (or omit `duration_minutes_per_day` so each template day is 1440 minutes). Example file: `backend/app/scenario_repeat_3d.json`.

```bash
curl -X POST http://localhost:8000/run \
  -H "Content-Type: application/json" \
  -d @backend/app/scenario_repeat_3d.json
```

Rider wait distribution (served trips only): `p10_wait_min`, `median_wait_min` (p50), and `p90_wait_min` (all in minutes).

Charging-related outputs in `metrics` include:
- `depot_queue_p90_min`
- `depot_queue_max_concurrent` (peak summed queue depth across depots)
- `depot_queue_max_at_site` (peak queue depth at one depot)
- Depot throughput (see `SPEC.md` §16): `depot_arrivals_total`, `depot_arrivals_by_depot_id`, `depot_jit_plug_full_*`, `depot_charge_completions_*`, peak arrivals/completions per simulation hour (`depot_*_peak_fleet_per_hour`, `depot_*_peak_max_site_per_hour`), and `charging_session_duration_median_min` / `charging_session_duration_p90_min` (completed sessions only)
- `charger_utilization_pct` (fleet-wide average over all plugs × horizon)
- `charger_utilization_by_depot_pct` (map of depot id → % for that site’s plugs only)

## Compare baseline vs variant (repositioning off vs on)

```bash
curl -X POST http://localhost:8000/compare \
  -H "Content-Type: application/json" \
  -d @backend/app/compare_scenario.json
```

---

# Default Scenario

The default scenario (`backend/app/default_scenario.json`) runs a full 24-hour synthetic
Austin day with 200 vehicles:

```json
{
  "seed": 123,
  "duration_minutes": 1440,
  "fleet": { "size": 200, "battery_kwh": 75, "kwh_per_mile": 0.20,
             "soc_initial": 0.80, "soc_min": 0.20, "soc_charge_start": 0.80, "soc_target": 0.80 },
  "depots": [{ "id": "depot_1", "h3_cell": "88489e3467fffff",
               "chargers_count": 20, "charger_kw": 150, "site_power_kw": 1500 }],
  "demand": { "max_wait_time_seconds": 600, "day_offset_seconds": 0, "demand_scale": 0.02 },
  "repositioning": { "reposition_enabled": true, "reposition_alpha": 0.6 },
  "charging_queue_policy": "jit",
  "charging_depot_selection": "fastest",
  "charging_depot_balance_slack_minutes": 3.0,
  "min_plug_duration_minutes": 0.0
}
```

`timeseries_bucket_minutes` defaults to **1** so `/run` time series and the `scripts/dashboard.py` map scrubber advance in **one-minute** steps (raise it if you need smaller outputs).

**`demand_scale` guide** (with 200 vehicles, 24-hour window):

| `demand_scale` | Approx trips/day | Avg trips/hr | Character |
|---|---|---|---|
| `0.01` | ~8,700 | ~360 | Light load, vehicles mostly idle |
| `0.02` | ~17,400 | ~725 | Default — moderate load, peak stress at 2–4 AM |
| `0.05` | ~43,400 | ~1,800 | Fleet saturated at peak, heavy queuing |
| `1.0` | ~867,800 | ~36,000 | Stress test / fleet sizing research |

**`day_offset_seconds` guide** (time-of-day windows with `duration_minutes: 360`):

| Value | Window | Character |
|---|---|---|
| `0` | Midnight–6 AM | Bar-close rush (Austin peak) |
| `25200` | 7 AM–1 PM | Morning commute + midday |
| `64800` | 6 PM–midnight | Evening peak |

---

# Demand Dataset

The preprocessed parquet represents a **synthetic collapsed day** — all 867,791 historical
RideAustin trips overlaid by time-of-day. This gives a stable, dense demand baseline
suitable for A/B comparisons.

**Hourly trip density (collapsed day):**

```
00:00  55,014  ██████████████████
01:00  57,492  ███████████████████
02:00  56,587  ██████████████████
03:00  61,527  ████████████████████
04:00  64,907  █████████████████████  ← peak (Austin nightlife)
05:00  57,734  ███████████████████
06:00  47,752  ███████████████
07:00  40,372  █████████████
08:00  26,115  ████████
09:00   9,989  ███
10:00   8,220  ██                     ← trough
11:00   8,403  ██
12:00  11,518  ███
13:00  18,226  ██████
14:00  25,509  ████████
15:00  27,808  █████████
16:00  30,283  ██████████
17:00  32,078  ██████████
18:00  33,399  ███████████
19:00  33,902  ███████████
20:00  35,318  ███████████
21:00  38,037  ████████████
22:00  41,456  █████████████
23:00  46,986  ███████████████
```

**Trip characteristics:**
- Median duration: 11.3 min | p90: 22.5 min | p99: 40 min
- Median distance: 3.75 miles | p90: 11.7 miles | p99: 22.5 miles
- 1,677 unique origin cells, 1,957 destination cells (H3 res 8)

---

# Architecture

```
POST /run ──► load_requests() ──► apply_demand_control() ──► SimulationEngine
  (For multi-day studies on **one** continuous clock, scripts can use
  `load_requests_repeated_days()` to tile the same synthetic day; see
  `scripts/run_exp63_continuous_multiday_steady_state.py` — optional **`--battery-kwh`** overrides the default **75** kWh pack; optional **`--depot-cells`** (comma-separated H3 res-8 ids, same count as **`--sites`**) overrides default top-origin depot placement; optional **`--charging-queue-policy fifo|jit`** (default **fifo**);
  `scripts/run_exp70_n2_depot_plug_kw_sweep.py` — same **`--depot-cells`** override when combined with **`--configs plugs:kw,...`**; **`run_exp70_proxy_best_depots_154_308.py`** runs **154p×20** and **308p×20** with the **proxy-best** N=2 pair from `search_n2_depot_pairs_origin_access.py`; **`scripts/run_exp74_n2_central_20kw_plug_sweep.py`** sweeps **200–400** plugs @ **20 kW** on **N=2** default central depots (Exp71-class harness); **`scripts/run_exp75_n1_vs_n2_same_fleet_kw.py`** compares **N=1** (**616p×20**) vs **N=2** (**308p×20** ×2) at **12,320 kW** fleet nameplate;
  `scripts/run_exp66_n77_demand_scale_sweep.py` sweeps `demand_scale` at fixed N=77, 3-day continuous;
  `scripts/run_exp69_scale02_repeat_exp68_battery40.py` repeats the Exp68 plug/site grid at **40** kWh.)
                                                                    │
                                                              heapq event loop
                                                                    │
                                                   ┌────────────────┼────────────────┐
                                               dispatch          charging       repositioning
                                                   │                │                │
                                              RoutingCache      Depot queue    RepositioningPolicy
                                           (parquet + OSRM)    (FIFO + kW)   (reactive+forecast)
                                                   │
                                            compute_metrics()
                                                   │
                                             RunResponse
```

- Discrete-event simulation (heapq, no wall-clock dependency)
- H3 spatial indexing at resolution 8
- OSRM routing backend (Texas PBF)
- Deterministic execution via seed (stable heap ordering via monotonic seq counter)
- Repositioning preemptible by dispatch
- Charging uses reservation-timed departures (just-in-time arrival, no depot FIFO queue)

---

# Development Roadmap

## MVP (Phase 1) — Complete
- RideAustin-based demand (collapsed synthetic day)
- H3 travel cache
- Repositioning A/B
- Demand control knobs (flex, pool, prebook, off-peak)
- `/run` and `/compare` endpoints

## Phase 2 — Synthetic Demand Model (Implemented)
- First-principles demand generation from Census LODES/ACS, OSMnx POIs, Capital Metro GTFS, and OSRM
- 18 tunable parameters (demand_intensity, day_type, beta, tourism_intensity, etc.)
- Gravity model with purpose-specific OD distributions and AM/PM commute symmetry
- Airport (ABIA) special generator with flight-schedule temporal profile
- Transit access scoring from Capital Metro GTFS
- Event injection for concerts, sports, festivals
- Integrated with autonomous experimenter via `demand_config`

### Quick start — Synthetic Demand

```bash
# Generate with defaults (weekday, 24h)
cd scripts && python -m demand_model.generate --output ../data/synthetic_demand.parquet

# Generate a Saturday with high tourism (SXSW-like)
python -m demand_model.generate --day-type saturday --tourism-intensity 3.0 --peak-sharpness 1.5

# Validate the output
python -m demand_model.validate ../data/synthetic_demand.parquet --output-dir ../plots/demand_validation
```

The generated parquet uses the same schema as the RideAustin data, so the sim engine needs no changes.
See `CHEATSHEET.md` for the full parameter reference.

## Future
- Multi-city support
- Enhanced economic modeling
- UI dashboard

---

# Blog / Personal Site

A static Astro + MDX + React site lives under `site/` and publishes the
long-form blog post on the four main simulator findings. The post renders
Recharts components off JSON aggregated from three dedicated sweep scripts.

## Data sweeps that back the blog post

All three are idempotent: each cell writes a full JSON to disk and rebuilds
`index.json`. Re-running skips completed cells (`--force` to redo).

```bash
# 1. Fleet sizing knee (13 fleets × 3 seeds = 39 runs)
PYTHONHASHSEED=0 python3.11 scripts/run_blog_fleet_sizing_sweep.py

# 2. Geographic / charger / battery anchors (7 configs × 3 seeds = 21 runs)
PYTHONHASHSEED=0 python3.11 scripts/run_blog_anchor_replicates.py

# 3. Pareto frontier (10 corners × 2 vehicle presets = 20 runs)
PYTHONHASHSEED=0 python3.11 scripts/run_blog_pareto_sweep.py
```

Outputs land in `data/blog_fleet_sweep/`, `data/blog_anchor_replicates/`,
and `data/blog_pareto/` respectively. These generated run artifacts are ignored
by git; `scripts/extract_blog_data.py` writes the checked-in aggregate used by
the site.

## Extract + plot

```bash
# Aggregate all three sweeps (+ the Exp71 congestion sweep) into one JSON
# that the Recharts components read at build time.
python3.11 scripts/extract_blog_data.py
# → site/src/content/experiments.json

# Optional static PNG/SVG of the Pareto frontier (for fallbacks / thumbnails)
python3.11 scripts/plot_pareto_frontier.py
# → plots/pareto_frontier.{png,svg}
```

## Run the site locally

```bash
cd site
npm install
npm run dev      # http://localhost:4321
npm run build    # produces site/dist/ (static)
```

The post lives at `site/src/pages/robotaxi/index.mdx`. Hero and depot-map
iframes are pre-generated HTML under `site/public/sim/`.
