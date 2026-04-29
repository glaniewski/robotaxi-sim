"""
Generate a self-contained HTML dashboard from a simulation run.

Uses deck.gl (TripsLayer, ArcLayer, ScatterplotLayer) + Plotly.js + MapLibre GL,
all via CDN.  Opens in any browser — no server required.

Usage:
    python3 scripts/dashboard.py                            # default scenario
    python3 scripts/dashboard.py --scenario config.json     # custom scenario
    python3 scripts/dashboard.py --output my_dash.html      # custom output
    python3 scripts/dashboard.py --sample-vehicles 500      # override auto-sample
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import defaultdict
from pathlib import Path

import h3
import numpy as np
from tqdm import tqdm

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "backend"))

from app.main import _load_default_scenario, _run_scenario  # noqa: E402
from app.schemas import ScenarioConfig  # noqa: E402

TRAIL_SAMPLE_SIZE = 300  # default max vehicles for animated trails
OSRM_URL = "http://localhost:5001"
OSRM_MAX_WORKERS = 50
ROUTE_CACHE_PATH = str(ROOT / "data" / "route_geometry_cache.json")


# ------------------------------------------------------------------
# Data processing helpers
# ------------------------------------------------------------------

def _h3_to_lnglat(cell: str) -> list[float]:
    lat, lng = h3.cell_to_latlng(cell)
    return [lng, lat]


def _collect_h3_pairs(
    transitions: list, vehicle_ids: set | None = None
) -> set[tuple[str, str]]:
    """Extract unique (origin_h3, dest_h3) pairs from consecutive transitions."""
    by_vehicle: dict[str, list[str]] = defaultdict(list)
    for _t, vid, h3_cell, _state in transitions:
        if vehicle_ids is not None and vid not in vehicle_ids:
            continue
        by_vehicle[vid].append(h3_cell)

    pairs: set[tuple[str, str]] = set()
    for cells in by_vehicle.values():
        for i in range(1, len(cells)):
            if cells[i] != cells[i - 1]:
                pairs.add((cells[i - 1], cells[i]))
    return pairs


def _load_route_cache() -> dict[tuple[str, str], list[list[float]]]:
    if not os.path.exists(ROUTE_CACHE_PATH):
        return {}
    with open(ROUTE_CACHE_PATH) as f:
        raw = json.load(f)
    return {tuple(k.split("|")): v for k, v in raw.items()}


def _save_route_cache(cache: dict[tuple[str, str], list[list[float]]]) -> None:
    raw = {f"{a}|{b}": v for (a, b), v in cache.items()}
    os.makedirs(os.path.dirname(ROUTE_CACHE_PATH), exist_ok=True)
    with open(ROUTE_CACHE_PATH, "w") as f:
        json.dump(raw, f, separators=(",", ":"))


def _fetch_route_geometries(
    h3_pairs: set[tuple[str, str]],
    osrm_url: str = OSRM_URL,
    max_workers: int = OSRM_MAX_WORKERS,
) -> dict[tuple[str, str], list[list[float]]]:
    """Fetch simplified road geometries from OSRM, with persistent disk cache."""
    disk_cache = _load_route_cache()
    missing = h3_pairs - disk_cache.keys()

    if not missing:
        print(f"[visualize] All {len(h3_pairs)} route geometries loaded from cache")
        return {p: disk_cache[p] for p in h3_pairs}

    import httpx
    from concurrent.futures import ThreadPoolExecutor, as_completed

    print(f"[visualize] {len(h3_pairs) - len(missing)} cached, "
          f"fetching {len(missing)} from OSRM...")

    def _fetch_one(pair: tuple[str, str]):
        h3_a, h3_b = pair
        a_lat, a_lng = h3.cell_to_latlng(h3_a)
        b_lat, b_lng = h3.cell_to_latlng(h3_b)
        url = (
            f"{osrm_url}/route/v1/driving/{a_lng},{a_lat};{b_lng},{b_lat}"
            f"?geometries=geojson&overview=simplified"
        )
        try:
            resp = httpx.get(url, timeout=10.0)
            data = resp.json()
            if data.get("code") == "Ok":
                return pair, data["routes"][0]["geometry"]["coordinates"]
        except Exception:
            pass
        return pair, [[a_lng, a_lat], [b_lng, b_lat]]

    fetched: dict[tuple[str, str], list[list[float]]] = {}
    fallbacks: set[tuple[str, str]] = set()
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_fetch_one, p): p for p in missing}
        for fut in tqdm(as_completed(futures), total=len(futures), desc="OSRM routes"):
            pair, coords = fut.result()
            if len(coords) > 2:
                fetched[pair] = coords
            else:
                fallbacks.add(pair)

    # Only cache real road geometries — straight-line fallbacks are NOT saved so
    # they will be retried on the next run when OSRM is available.
    disk_cache.update(fetched)
    _save_route_cache(disk_cache)
    print(f"[visualize] Route cache saved ({len(disk_cache)} total entries)")

    # For this run, provide straight-line coords for uncached/failed pairs
    result = dict(disk_cache)
    for pair in fallbacks:
        h3_a, h3_b = pair
        a_lat, a_lng = h3.cell_to_latlng(h3_a)
        b_lat, b_lng = h3.cell_to_latlng(h3_b)
        result[pair] = [[a_lng, a_lat], [b_lng, b_lat]]
    return {p: result[p] for p in h3_pairs if p in result}


def _build_vehicle_positions(
    transitions: list,
    vehicle_ids: set | None = None,
) -> list[dict]:
    """Per-vehicle timeline of (t, lng, lat, state_code) for an always-on dot layer.

    The trails layer only renders a fading trail behind moving vehicles, so
    idle/charging vehicles disappear between movements. This companion
    ScatterplotLayer shows every vehicle at all times using the last-known H3
    cell from its transition stream.
    """
    state_codes = {
        "IDLE": 0, "TO_PICKUP": 1, "IN_TRIP": 2,
        "TO_DEPOT": 3, "CHARGING": 4, "REPOSITIONING": 5,
    }

    by_vehicle: dict[str, list[list]] = defaultdict(list)
    cell_coords: dict[str, tuple[float, float]] = {}
    for t_s, vid, h3_cell, state in transitions:
        if vehicle_ids is not None and vid not in vehicle_ids:
            continue
        if h3_cell not in cell_coords:
            lng, lat = _h3_to_lnglat(h3_cell)
            cell_coords[h3_cell] = (round(lng, 6), round(lat, 6))
        lng, lat = cell_coords[h3_cell]
        code = state_codes.get(state, 0)
        by_vehicle[vid].append([round(float(t_s), 1), lng, lat, code])

    out: list[dict] = []
    for vid, entries in by_vehicle.items():
        entries.sort(key=lambda e: e[0])
        # Deterministic per-vehicle jitter (~30m) so many vehicles at the same
        # H3 cell render as a visible cluster instead of one overlapping dot.
        h = hash(vid) & 0xFFFFFFFF
        jlng = ((h & 0xFFFF) / 0xFFFF - 0.5) * 0.0006
        jlat = (((h >> 16) & 0xFFFF) / 0xFFFF - 0.5) * 0.0006
        ts = [e[0] for e in entries]
        lng_arr = [round(e[1] + jlng, 6) for e in entries]
        lat_arr = [round(e[2] + jlat, 6) for e in entries]
        st_arr = [e[3] for e in entries]
        out.append({"id": vid, "t": ts, "lng": lng_arr, "lat": lat_arr, "st": st_arr})
    return out


def _build_vehicle_paths(
    transitions: list,
    vehicle_ids: set | None = None,
    route_geometries: dict[tuple[str, str], list[list[float]]] | None = None,
) -> list[dict]:
    """Build movement segments with OSRM road geometries.

    Returns list of {path: [[lng,lat],...], timestamps: [t,...], state: str}.
    One entry per movement (consecutive transitions with different H3 cells).
    """
    by_vehicle: dict[str, list[tuple[float, str, str]]] = defaultdict(list)
    for t_s, vid, h3_cell, state in transitions:
        if vehicle_ids is not None and vid not in vehicle_ids:
            continue
        by_vehicle[vid].append((round(t_s, 1), h3_cell, state))

    segments: list[dict] = []
    for wps in by_vehicle.values():
        for i in range(1, len(wps)):
            t0, h3_a, st_a = wps[i - 1]
            t1, h3_b, _st_b = wps[i]
            if h3_a == h3_b or t1 <= t0:
                continue

            if route_geometries and (h3_a, h3_b) in route_geometries:
                coords = route_geometries[(h3_a, h3_b)]
            else:
                coords = [_h3_to_lnglat(h3_a), _h3_to_lnglat(h3_b)]

            n = len(coords)
            denom = max(n - 1, 1)
            timestamps = [round(t0 + (t1 - t0) * j / denom, 1) for j in range(n)]
            timestamps[-1] = t1  # snap to exact arrival time
            segments.append({"path": coords, "timestamps": timestamps, "state": st_a})

    return segments


def _build_trip_arcs(trip_log: list) -> list[dict]:
    """Build exact-timed trip arcs grouped by (origin, destination) H3 pair.

    Each entry has src/dst coords and a list of [t_start, t_end, served] per trip.
    The browser filters to trips active at the current scrubber time.
    """
    h3_coords: dict[str, list[float]] = {}
    od_groups: dict[tuple[str, str], list[list]] = defaultdict(list)

    for trip in trip_log:
        o, d = trip["o"], trip["d"]
        for cell in (o, d):
            if cell not in h3_coords:
                h3_coords[cell] = _h3_to_lnglat(cell)

        t_start = trip["rt"]
        served = trip["st"] == "SERVED"
        if served and trip.get("sa") is not None and trip.get("dur") is not None:
            t_end = trip["sa"] + trip["dur"]
        else:
            t_end = t_start + 300  # 5-min visibility for unserved

        od_groups[(o, d)].append([
            round(t_start, 1), round(t_end, 1), 1 if served else 0
        ])

    return [
        {"s": h3_coords[o], "d": h3_coords[d], "trips": trips}
        for (o, d), trips in od_groups.items()
    ]


def _build_depot_data(config: ScenarioConfig) -> list[dict]:
    austin_center = "88489e3467fffff"
    depots = []
    for d in config.depots:
        h3_cell = d.h3_cell or austin_center
        lng, lat = _h3_to_lnglat(h3_cell)
        depots.append({
            "id": d.id,
            "lng": lng,
            "lat": lat,
            "chargers": d.chargers_count,
        })
    return depots


# ------------------------------------------------------------------
# HTML generation
# ------------------------------------------------------------------

def _generate_html(data_json: str) -> str:
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Robotaxi Sim Dashboard</title>
<script src="https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.js"></script>
<link href="https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.css" rel="stylesheet" />
<script src="https://unpkg.com/deck.gl@9.1.3/dist.min.js"></script>
<script src="https://cdn.plot.ly/plotly-2.35.0.min.js"></script>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
         background: #0d1117; color: #e6edf3; overflow: hidden;
         display: flex; flex-direction: column; height: 100vh; }}

  /* Loading overlay */
  #loader {{ position: fixed; inset: 0; z-index: 9999; background: #0d1117;
             display: flex; align-items: center; justify-content: center;
             flex-direction: column; gap: 16px; }}
  #loader .spinner {{ width: 48px; height: 48px; border: 4px solid #30363d;
                      border-top-color: #58a6ff; border-radius: 50%;
                      animation: spin 0.8s linear infinite; }}
  @keyframes spin {{ to {{ transform: rotate(360deg); }} }}

  /* Controls bar */
  #controls {{ padding: 12px 20px; display: flex; align-items: center; gap: 16px;
               background: #161b22; border-bottom: 1px solid #30363d; }}
  #timeLabel {{ font-size: 22px; font-weight: 600; min-width: 80px; font-variant-numeric: tabular-nums; }}
  #timeSlider {{ flex: 1; accent-color: #58a6ff; height: 6px; cursor: pointer; }}
  .speed-btn {{ background: #21262d; border: 1px solid #30363d; color: #8b949e;
                padding: 4px 10px; border-radius: 6px; cursor: pointer; font-size: 13px; }}
  .speed-btn.active {{ color: #58a6ff; border-color: #58a6ff; }}
  #playBtn {{ background: #238636; border: none; color: #fff; padding: 6px 14px;
              border-radius: 6px; cursor: pointer; font-size: 14px; font-weight: 600; }}

  /* KPI row */
  #kpiRow {{ display: flex; gap: 8px; padding: 10px 20px; flex-wrap: wrap;
             background: #161b22; border-bottom: 1px solid #30363d; }}
  .kpi {{ background: #0d1117; border: 1px solid #30363d; border-radius: 8px;
          padding: 8px 14px; min-width: 120px; flex: 1; }}
  .kpi-label {{ font-size: 11px; color: #8b949e; text-transform: uppercase; letter-spacing: 0.5px; }}
  .kpi-value {{ font-size: 20px; font-weight: 600; font-variant-numeric: tabular-nums; margin-top: 2px; }}

  /* Map — clip canvas/controls so they never paint over the charts below */
  #map-container {{ position: relative; width: 100%; height: 55vh; overflow: hidden; }}
  #deck-canvas {{ position: absolute; inset: 0; z-index: 1; }}

  /* Legend / layer toggles */
  #legend {{ position: absolute; top: 12px; right: 12px; z-index: 10;
             background: rgba(13,17,23,0.92); border: 1px solid #30363d;
             border-radius: 8px; padding: 12px 14px; font-size: 12px; line-height: 1.7;
             max-height: calc(100% - 24px); overflow-y: auto; }}
  .legend-dot {{ display: inline-block; width: 10px; height: 10px; border-radius: 50%;
                 margin-right: 6px; vertical-align: middle; }}
  .layer-toggle {{ display: flex; align-items: center; gap: 6px; font-weight: 600;
                   cursor: pointer; user-select: none; }}
  .layer-toggle input {{ accent-color: #58a6ff; cursor: pointer; }}

  /* Summary panel */
  #summaryToggle {{ padding: 8px 20px; background: #161b22; border-bottom: 1px solid #30363d;
                    cursor: pointer; user-select: none; display: flex; align-items: center; gap: 8px; }}
  #summaryToggle:hover {{ background: #1c2129; }}
  #summaryToggle .arrow {{ transition: transform 0.2s; font-size: 12px; }}
  #summaryToggle .arrow.open {{ transform: rotate(90deg); }}
  #summaryPanel {{ display: none; padding: 12px 20px; background: #0d1117;
                   border-bottom: 1px solid #30363d; }}
  #summaryPanel.open {{ display: flex; gap: 10px; flex-wrap: wrap; }}
  .summary-card {{ background: #161b22; border: 1px solid #30363d; border-radius: 8px;
                   padding: 12px 18px; min-width: 160px; flex: 1; }}
  .summary-card .sc-label {{ font-size: 11px; color: #8b949e; text-transform: uppercase;
                             letter-spacing: 0.5px; }}
  .summary-card .sc-value {{ font-size: 24px; font-weight: 700; margin-top: 4px;
                             font-variant-numeric: tabular-nums; }}
  .summary-card .sc-sub {{ font-size: 11px; color: #8b949e; margin-top: 2px; }}

  /* Charts — flex:1 fills remaining height after map/controls; min-width 0 avoids flex clipping */
  #charts {{ display: flex; gap: 8px; padding: 8px 24px 10px; background: #0d1117; position: relative; z-index: 2; flex: 1; }}
  #charts > div {{ flex: 1; min-width: 0; overflow: visible; }}
</style>
</head>
<body>
<div id="loader"><div class="spinner"></div><div id="loaderStatus">Loading dashboard data...</div></div>

<!-- Controls -->
<div id="controls">
  <button id="playBtn">&#9654; Play</button>
  <span id="timeLabel">00:00</span>
  <input id="timeSlider" type="range" min="0" max="0" step="1" value="0" />
  <span id="timeRange"></span>
  <button class="speed-btn active" data-speed="1">1x</button>
  <button class="speed-btn" data-speed="2">2x</button>
  <button class="speed-btn" data-speed="5">5x</button>
  <button class="speed-btn" data-speed="10">10x</button>
</div>

<!-- KPI Cards (time-varying, update on scrub) -->
<div id="kpiRow">
  <div class="kpi"><div class="kpi-label">Vehicles Idle</div><div class="kpi-value" id="kv-idle">—</div></div>
  <div class="kpi"><div class="kpi-label">To Pickup</div><div class="kpi-value" id="kv-topickup">—</div></div>
  <div class="kpi"><div class="kpi-label">In Trip</div><div class="kpi-value" id="kv-intrip">—</div></div>
  <div class="kpi"><div class="kpi-label">Charging</div><div class="kpi-value" id="kv-charging">—</div></div>
  <div class="kpi"><div class="kpi-label">Repositioning</div><div class="kpi-value" id="kv-repo">—</div></div>
  <div class="kpi"><div class="kpi-label">Pending</div><div class="kpi-value" id="kv-pending">—</div></div>
  <div class="kpi"><div class="kpi-label">Served</div><div class="kpi-value" id="kv-served">—</div></div>
  <div class="kpi"><div class="kpi-label">Unserved</div><div class="kpi-value" id="kv-unserved">—</div></div>
  <div class="kpi"><div class="kpi-label">Fleet SOC</div><div class="kpi-value" id="kv-soc">—</div></div>
</div>

<!-- Summary Metrics (collapsible) -->
<div id="summaryToggle" role="button" tabindex="0" style="cursor:pointer">
  <span class="arrow" id="summaryArrow">&#9654;</span>
  <span style="font-size:13px;font-weight:600;">Run Summary</span>
  <span style="font-size:11px;color:#8b949e;margin-left:auto;">Final metrics for entire simulation</span>
</div>
<div id="summaryPanel">
  <div class="summary-card">
    <div class="sc-label">Fulfillment Rate</div>
    <div class="sc-value" id="sm-served-pct">—</div>
    <div class="sc-sub">% of requests served</div>
  </div>
  <div class="summary-card">
    <div class="sc-label">Wait Time P10 / P50 / P90</div>
    <div class="sc-value" id="sm-wait">—</div>
    <div class="sc-sub">Minutes to pickup</div>
  </div>
  <div class="summary-card">
    <div class="sc-label">Vehicle Utilization</div>
    <div class="sc-value" id="sm-util">—</div>
    <div class="sc-sub">% of miles with passenger</div>
  </div>
  <div class="summary-card">
    <div class="sc-label">Trips / Vehicle / Day</div>
    <div class="sc-value" id="sm-tpvd">—</div>
    <div class="sc-sub">Fleet throughput</div>
  </div>
  <div class="summary-card">
    <div class="sc-label">Deadhead Ratio</div>
    <div class="sc-value" id="sm-deadhead">—</div>
    <div class="sc-sub">% of miles empty</div>
  </div>
  <div class="summary-card">
    <div class="sc-label">Cost / Trip</div>
    <div class="sc-value" id="sm-cost">—</div>
    <div class="sc-sub">Total system cost per trip</div>
  </div>
</div>

<!-- Map -->
<div id="map-container">
  <div id="deck-canvas"></div>
  <div id="legend">
    <label class="layer-toggle"><input type="checkbox" id="tog-positions" checked> Parked Vehicles</label>
    <div style="font-size:11px;color:#8b949e">Idle (grey) &amp; charging (green) — no trail</div>
    <label class="layer-toggle" style="margin-top:8px"><input type="checkbox" id="tog-trails" checked> Vehicle Trails</label>
    <div><span class="legend-dot" style="background:#888"></span>Idle</div>
    <div><span class="legend-dot" style="background:#FFD700"></span>To Pickup</div>
    <div><span class="legend-dot" style="background:#4FC3F7"></span>In Trip</div>
    <div><span class="legend-dot" style="background:#66BB6A"></span>Charging</div>
    <div><span class="legend-dot" style="background:#FF9800"></span>Repositioning</div>
    <div><span class="legend-dot" style="background:#AB47BC"></span>To Depot</div>
    <label class="layer-toggle" style="margin-top:8px"><input type="checkbox" id="tog-heatmap" checked> Demand Heatmap</label>
    <div style="font-size:11px;color:#8b949e">Trip origins (trailing window)</div>
    <label class="layer-toggle" style="margin-top:8px"><input type="checkbox" id="tog-arcs" checked> Trip Arcs</label>
    <div><span class="legend-dot" style="background:#66BB6A"></span>Served</div>
    <div><span class="legend-dot" style="background:#f85149"></span>Unserved</div>
    <label class="layer-toggle" style="margin-top:8px"><input type="checkbox" id="tog-depots" checked> Depots</label>
    <div>Size = arrivals &middot; Color = charger load</div>
  </div>
</div>

<!-- Charts -->
<div id="charts">
  <div id="chart-fleet"></div>
  <div id="chart-demand"></div>
  <div id="chart-soc"></div>
</div>

<div id="tooltip" style="position:fixed;z-index:100;pointer-events:none;display:none;
     background:rgba(13,17,23,0.95);border:1px solid #30363d;border-radius:6px;
     padding:8px 12px;font-size:12px;color:#e6edf3;max-width:260px;"></div>

<script>
(function () {{
try {{
const DATA = {data_json};

const TS = DATA.timeseries;
const TRIP_ARCS = DATA.trip_arcs;
const VEHICLE_PATHS = DATA.vehicle_paths;
const VEHICLE_POSITIONS = DATA.vehicle_positions || [];
const DEPOTS = DATA.depots;
const METRICS = DATA.metrics;
const DAY_OFFSET_S = DATA.day_offset_s || 0;
const BUCKET_MIN = DATA.bucket_minutes;

const STATE_COLORS = {{
  IDLE: [136, 136, 136], TO_PICKUP: [255, 215, 0], IN_TRIP: [79, 195, 247],
  CHARGING: [102, 187, 106], REPOSITIONING: [255, 152, 0], TO_DEPOT: [171, 71, 188],
}};

// --- Time helpers ---
function fmtTime(bucketIdx) {{
  const simMin = TS[bucketIdx].t_minutes;
  const totalMin = (DAY_OFFSET_S / 60) + simMin;
  const h = Math.floor(totalMin / 60) % 24;
  const m = Math.floor(totalMin % 60);
  const ampm = h >= 12 ? 'PM' : 'AM';
  const hh = h % 12 || 12;
  return hh + ':' + String(m).padStart(2, '0') + ' ' + ampm;
}}

// --- Slider ---
const slider = document.getElementById('timeSlider');
const timeLabel = document.getElementById('timeLabel');
const timeRange = document.getElementById('timeRange');
slider.max = TS.length - 1;
timeRange.textContent = fmtTime(0) + ' — ' + fmtTime(TS.length - 1);

// --- Play button ---
let playing = false;
let playSpeed = 1;
let playTimer = null;
const playBtn = document.getElementById('playBtn');
playBtn.onclick = () => {{
  playing = !playing;
  playBtn.innerHTML = playing ? '&#9724; Pause' : '&#9654; Play';
  if (playing) startPlay(); else stopPlay();
}};
function startPlay() {{
  stopPlay();
  playTimer = setInterval(() => {{
    let v = +slider.value + 1;
    if (v > +slider.max) {{ v = 0; }}
    slider.value = v;
    scheduleDashboardUpdate();
  }}, Math.max(30, 200 / playSpeed));
}}
function stopPlay() {{ if (playTimer) clearInterval(playTimer); playTimer = null; }}
document.querySelectorAll('.speed-btn').forEach(btn => {{
  btn.onclick = () => {{
    document.querySelectorAll('.speed-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    playSpeed = +btn.dataset.speed;
    if (playing) startPlay();
  }};
}});

// --- KPI update (time-varying only) ---
function updateKPI(bucket) {{
  document.getElementById('kv-idle').textContent = bucket.idle_count;
  document.getElementById('kv-topickup').textContent = bucket.to_pickup_count ?? 0;
  document.getElementById('kv-intrip').textContent = bucket.in_trip_count;
  document.getElementById('kv-charging').textContent = bucket.charging_count;
  document.getElementById('kv-repo').textContent = bucket.repositioning_count;
  document.getElementById('kv-pending').textContent = bucket.pending_requests;
  document.getElementById('kv-served').textContent = bucket.served_cumulative.toLocaleString();
  document.getElementById('kv-unserved').textContent = bucket.unserved_cumulative.toLocaleString();
  document.getElementById('kv-soc').textContent = bucket.fleet_mean_soc_pct.toFixed(1) + '%';
}}

// --- Summary panel ---
function toggleSummary() {{
  const panel = document.getElementById('summaryPanel');
  const arrow = document.getElementById('summaryArrow');
  panel.classList.toggle('open');
  arrow.classList.toggle('open');
}}
document.getElementById('summaryToggle').addEventListener('click', toggleSummary);
document.getElementById('summaryToggle').addEventListener('keydown', (e) => {{
  if (e.key === 'Enter' || e.key === ' ') {{ e.preventDefault(); toggleSummary(); }}
}});
function populateSummary() {{
  const M = METRICS;
  document.getElementById('sm-served-pct').textContent = M.served_pct.toFixed(1) + '%';
  document.getElementById('sm-wait').textContent =
    M.p10_wait_min.toFixed(1) + ' / ' + M.median_wait_min.toFixed(1) + ' / ' + M.p90_wait_min.toFixed(1);
  document.getElementById('sm-util').textContent = M.utilization_pct.toFixed(1) + '%';
  document.getElementById('sm-tpvd').textContent = M.trips_per_vehicle_per_day.toFixed(1);
  document.getElementById('sm-deadhead').textContent = M.deadhead_pct.toFixed(1) + '%';
  document.getElementById('sm-cost').textContent = '$' + M.total_system_cost_per_trip.toFixed(2);
}}
populateSummary();

// --- Custom depot tooltip (updates on scrub, not just on hover) ---
const tooltipEl = document.getElementById('tooltip');
let hoveredDepotId = null;
let tooltipX = 0, tooltipY = 0;

function showDepotTooltip(depotId, x, y) {{
  hoveredDepotId = depotId;
  tooltipX = x; tooltipY = y;
  refreshDepotTooltip();
}}
function hideDepotTooltip() {{
  hoveredDepotId = null;
  tooltipEl.style.display = 'none';
}}
function refreshDepotTooltip() {{
  if (!hoveredDepotId) return;
  const idx = +slider.value;
  const snap = (TS[idx].depot_snapshots || {{}})[hoveredDepotId] || {{}};
  const depot = DEPOTS.find(d => d.id === hoveredDepotId);
  if (!depot) return;
  tooltipEl.innerHTML = '<b>' + depot.id + '</b><br/>' +
    'Chargers: ' + depot.chargers + '<br/>' +
    'Charging: ' + (snap.charging || 0) + '<br/>' +
    'Queue: ' + (snap.queue || 0) + '<br/>' +
    'Arrivals: ' + (snap.arrivals || 0);
  tooltipEl.style.display = 'block';
  tooltipEl.style.left = (tooltipX + 12) + 'px';
  tooltipEl.style.top = (tooltipY + 12) + 'px';
}}

// --- Layer toggles (wired in finishDashboardInit after updateDashboard exists) ---
const togPositions = document.getElementById('tog-positions');
const togTrails = document.getElementById('tog-trails');
const togHeatmap = document.getElementById('tog-heatmap');
const togArcs = document.getElementById('tog-arcs');
const togDepots = document.getElementById('tog-depots');
let updateDashboard = function() {{}};
let scheduleDashboardUpdate = function() {{}};

// --- deck.gl ---
const deckgl = new deck.DeckGL({{
  container: 'deck-canvas',
  mapStyle: 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json',
  initialViewState: {{
    longitude: DATA.center[0],
    latitude: DATA.center[1],
    zoom: 11,
    pitch: 45,
    bearing: 0,
  }},
  controller: true,
  layers: [],
}});

// Filter TRIP_ARCS to trips active at current time (exact timestamps)
function getActiveTrips(tSec) {{
  const arcData = [];
  const heatPoints = {{}};
  for (const arc of TRIP_ARCS) {{
    let n = 0, sv = 0;
    for (const tr of arc.trips) {{
      if (tr[0] <= tSec && tSec <= tr[1]) {{ n++; sv += tr[2]; }}
    }}
    if (n > 0) {{
      arcData.push({{ src: arc.s, dst: arc.d, n, sv }});
      const k = arc.s[0].toFixed(4) + ',' + arc.s[1].toFixed(4);
      if (!heatPoints[k]) heatPoints[k] = {{ position: arc.s, weight: 0 }};
      heatPoints[k].weight += n;
    }}
  }}
  return {{ arcData, heatData: Object.values(heatPoints) }};
}}

function depotDataForSnapshot(depotSnaps) {{
  const rows = DEPOTS.map(d => {{
    const snap = depotSnaps ? (depotSnaps[d.id] || {{}}) : {{}};
    return {{ ...d, _charging: snap.charging || 0, _queue: snap.queue || 0,
              _arrivals: snap.arrivals || 0 }};
  }});
  let maxArr = 1;
  for (let i = 0; i < rows.length; i++) {{
    const a = rows[i]._arrivals;
    if (a > maxArr) maxArr = a;
  }}
  return {{ rows, maxArr }};
}}

function maxTripN(arcData) {{
  if (!arcData.length) return 1;
  let m = 1;
  for (let i = 0; i < arcData.length; i++) {{
    const n = arcData[i].n;
    if (n > m) m = n;
  }}
  return m;
}}

// Precompute arc + heatmap per timeseries bucket. getActiveTrips() scans all TRIP_ARCS and
// nested trips — doing that on every scrub/play frame (especially with HeatmapLayer) caused lag.
const ARC_CACHE = new Array(TS.length);
const HEATMAP_CACHE = new Array(TS.length);
const ARC_MAXN_CACHE = new Array(TS.length);
const loaderStatusEl = document.getElementById('loaderStatus');
const CACHE_CHUNK = 100;

/** Prefer Layer#setProps when the UMD build exposes it; otherwise clone() (same id). */
function patchLayer(layer, props) {{
  if (layer && typeof layer.setProps === 'function') {{
    layer.setProps(props);
    return layer;
  }}
  return layer.clone(props);
}}

function fillCachesChunk(startIdx) {{
  const end = Math.min(startIdx + CACHE_CHUNK, TS.length);
  for (let bi = startIdx; bi < end; bi++) {{
    const tSec = TS[bi].t_minutes * 60;
    const {{ arcData, heatData }} = getActiveTrips(tSec);
    ARC_CACHE[bi] = arcData;
    HEATMAP_CACHE[bi] = heatData;
    ARC_MAXN_CACHE[bi] = maxTripN(arcData);
  }}
  if (loaderStatusEl) loaderStatusEl.textContent = 'Indexing trips ' + end + ' / ' + TS.length;
  if (end < TS.length) {{
    setTimeout(() => fillCachesChunk(end), 0);
  }} else {{
    finishDashboardInit();
  }}
}}

function finishDashboardInit() {{
// deck.gl layer instances (updated via patchLayer: setProps or clone).
let tripsLayer = new deck.TripsLayer({{
  id: 'trips',
  data: VEHICLE_PATHS,
  getPath: d => d.path,
  getTimestamps: d => d.timestamps,
  getColor: d => STATE_COLORS[d.state] || [136, 136, 136],
  currentTime: 0,
  trailLength: 600,
  opacity: 0.7,
  widthMinPixels: 2,
  rounded: true,
}});
let heatmapLayer = new deck.HeatmapLayer({{
  id: 'heatmap',
  data: [],
  getPosition: d => d.position,
  getWeight: d => d.weight,
  radiusPixels: 40,
  intensity: 1,
  threshold: 0.03,
  weightsTextureSize: 512,
  colorRange: [
    [255, 255, 178], [254, 204, 92], [253, 141, 60],
    [240, 59, 32], [189, 0, 38]
  ],
}});
let arcLayer = new deck.ArcLayer({{
  id: 'arcs',
  data: [],
  getSourcePosition: d => d.src,
  getTargetPosition: d => d.dst,
  getSourceColor: d => {{
    const ratio = d.n > 0 ? d.sv / d.n : 1;
    return [Math.round(248 * (1 - ratio) + 102 * ratio),
            Math.round(81 * (1 - ratio) + 187 * ratio),
            Math.round(73 * (1 - ratio) + 106 * ratio), 180];
  }},
  getTargetColor: d => {{
    const ratio = d.n > 0 ? d.sv / d.n : 1;
    return [Math.round(248 * (1 - ratio) + 102 * ratio),
            Math.round(81 * (1 - ratio) + 187 * ratio),
            Math.round(73 * (1 - ratio) + 106 * ratio), 120];
  }},
  getWidth: d => 1,
  greatCircle: true,
  widthMinPixels: 1,
  widthMaxPixels: 6,
}});
let depotLayer = new deck.ScatterplotLayer({{
  id: 'depots',
  data: [],
  getPosition: d => [d.lng, d.lat],
  getRadius: d => 200,
  getFillColor: [102, 187, 106, 200],
  pickable: true,
  radiusMinPixels: 8,
  radiusMaxPixels: 40,
  stroked: true,
  getLineColor: [88, 166, 255, 200],
  lineWidthMinPixels: 2,
  onHover: ({{object, x, y}}) => {{
    if (object) {{ showDepotTooltip(object.id, x, y); }}
    else {{ hideDepotTooltip(); }}
  }},
}});

// --- Vehicle positions (always-on dot per vehicle) ---
// Typed arrays + cursor indices for O(1) incremental lookup as the scrubber advances.
const STATE_COLOR_TABLE = [
  [136, 136, 136], [255, 215, 0], [79, 195, 247],
  [171, 71, 188], [102, 187, 106], [255, 152, 0],
];
const VP_CURSOR = new Int32Array(VEHICLE_POSITIONS.length);
let VP_LAST_T = -1;

function vehicleDots(tSec) {{
  const rewind = tSec < VP_LAST_T;
  VP_LAST_T = tSec;
  const out = [];
  for (let i = 0; i < VEHICLE_POSITIONS.length; i++) {{
    const v = VEHICLE_POSITIONS[i];
    const ts = v.t;
    if (!ts.length || tSec < ts[0]) continue;
    let idx = rewind ? 0 : VP_CURSOR[i];
    while (idx + 1 < ts.length && ts[idx + 1] <= tSec) idx++;
    VP_CURSOR[i] = idx;
    // Only show stationary vehicles: IDLE (code 0) or CHARGING (code 4).
    // Trails cover the four on-road states (TO_PICKUP, IN_TRIP, TO_DEPOT, REPOSITIONING).
    const s = v.st[idx];
    if (s !== 0 && s !== 4) continue;
    out.push({{ p: [v.lng[idx], v.lat[idx]], c: STATE_COLOR_TABLE[s] }});
  }}
  return out;
}}

let positionsLayer = new deck.ScatterplotLayer({{
  id: 'vehicle-positions',
  data: [],
  getPosition: d => d.p,
  getFillColor: d => d.c,
  getLineColor: [13, 17, 23, 255],
  getRadius: 80,
  radiusMinPixels: 4,
  radiusMaxPixels: 10,
  opacity: 0.95,
  stroked: true,
  lineWidthMinPixels: 1,
}});

// --- Plotly charts ---
const axisTitleFont = {{ size: 11, color: '#8b949e' }};
const plotLayout = {{
  paper_bgcolor: '#0d1117', plot_bgcolor: '#0d1117',
  font: {{ color: '#8b949e', size: 11 }},
  // automargin grows l/b if ticks + titles need it; legend sits in paper coords under the axis title
  margin: {{ l: 64, r: 16, t: 28, b: 88 }},
  xaxis: {{ gridcolor: '#21262d', ticksuffix: '', automargin: true }},
  yaxis: {{ gridcolor: '#21262d', automargin: true }},
  legend: {{
    orientation: 'h',
    y: -0.32,
    yanchor: 'top',
    x: 0.5,
    xanchor: 'center',
    font: {{ size: 9 }},
    entrywidthmode: 'fraction',
    entrywidth: 0.34,
  }},
  shapes: [],
}};

const tMinArr = TS.map(b => b.t_minutes);

// Fleet states stacked area (sum = fleet size; includes TO_PICKUP)
Plotly.newPlot('chart-fleet', [
  {{ x: tMinArr, y: TS.map(b => b.idle_count), name: 'Idle', stackgroup: 'one',
     line: {{ width: 0 }}, fillcolor: 'rgba(136,136,136,0.7)' }},
  {{ x: tMinArr, y: TS.map(b => (b.to_pickup_count ?? 0)), name: 'To Pickup', stackgroup: 'one',
     line: {{ width: 0 }}, fillcolor: 'rgba(255,215,0,0.75)' }},
  {{ x: tMinArr, y: TS.map(b => b.in_trip_count), name: 'In Trip', stackgroup: 'one',
     line: {{ width: 0 }}, fillcolor: 'rgba(79,195,247,0.7)' }},
  {{ x: tMinArr, y: TS.map(b => b.charging_count), name: 'Charging', stackgroup: 'one',
     line: {{ width: 0 }}, fillcolor: 'rgba(102,187,106,0.7)' }},
  {{ x: tMinArr, y: TS.map(b => b.repositioning_count), name: 'Repositioning', stackgroup: 'one',
     line: {{ width: 0 }}, fillcolor: 'rgba(255,152,0,0.7)' }},
], {{ ...plotLayout,
  title: {{ text: 'Fleet State', font: {{ size: 13, color: '#e6edf3' }} }},
  xaxis: {{ ...plotLayout.xaxis,
    title: {{ text: 'Time (minutes from sim start)', font: axisTitleFont, standoff: 8 }} }},
  yaxis: {{ ...plotLayout.yaxis,
    title: {{ text: 'Vehicles (count)', font: axisTitleFont, standoff: 8 }} }},
}},
{{ responsive: true, displayModeBar: false }});

// Demand lines
Plotly.newPlot('chart-demand', [
  {{ x: tMinArr, y: TS.map(b => b.pending_requests), name: 'Pending',
     line: {{ color: '#FFD700', width: 2 }} }},
  {{ x: tMinArr, y: TS.map(b => b.served_cumulative), name: 'Served (cum.)',
     line: {{ color: '#66BB6A', width: 2 }} }},
  {{ x: tMinArr, y: TS.map(b => b.unserved_cumulative), name: 'Unserved (cum.)',
     line: {{ color: '#f85149', width: 2 }} }},
], {{ ...plotLayout,
  title: {{ text: 'Demand', font: {{ size: 13, color: '#e6edf3' }} }},
  xaxis: {{ ...plotLayout.xaxis,
    title: {{ text: 'Time (minutes from sim start)', font: axisTitleFont, standoff: 8 }} }},
  yaxis: {{ ...plotLayout.yaxis,
    title: {{ text: 'Requests (pending & cumulative)', font: axisTitleFont, standoff: 8 }} }},
}},
{{ responsive: true, displayModeBar: false }});

// SOC
Plotly.newPlot('chart-soc', [
  {{ x: tMinArr, y: TS.map(b => b.fleet_mean_soc_pct), name: 'Mean SOC',
     line: {{ color: '#58a6ff', width: 2 }}, fill: 'tozeroy',
     fillcolor: 'rgba(88,166,255,0.1)' }},
], {{ ...plotLayout,
  title: {{ text: 'Fleet SOC', font: {{ size: 13, color: '#e6edf3' }} }},
  xaxis: {{ ...plotLayout.xaxis,
    title: {{ text: 'Time (minutes from sim start)', font: axisTitleFont, standoff: 8 }} }},
  yaxis: {{ ...plotLayout.yaxis, range: [0, 100], ticksuffix: '%',
    title: {{ text: 'Fleet mean state of charge', font: axisTitleFont, standoff: 8 }} }},
}},
{{ responsive: true, displayModeBar: false }});

function updatePlotlyCursor(tMin) {{
  const shape = {{
    type: 'line', x0: tMin, x1: tMin, y0: 0, y1: 1, yref: 'paper',
    line: {{ color: '#58a6ff', width: 1.5, dash: 'dot' }},
  }};
  Plotly.relayout('chart-fleet', {{ shapes: [shape] }});
  Plotly.relayout('chart-demand', {{ shapes: [shape] }});
  Plotly.relayout('chart-soc', {{ shapes: [shape] }});
}}

// --- Master update ---
updateDashboard = function() {{
  const idx = +slider.value;
  const bucket = TS[idx];
  const tSec = bucket.t_minutes * 60;
  timeLabel.textContent = fmtTime(idx);
  updateKPI(bucket);

  const arcData = ARC_CACHE[idx];
  const heatData = HEATMAP_CACHE[idx];
  const maxN = ARC_MAXN_CACHE[idx];

  tripsLayer = patchLayer(tripsLayer, {{ currentTime: tSec }});

  const layers = [];
  if (togHeatmap.checked && heatData.length) {{
    heatmapLayer = patchLayer(heatmapLayer, {{ data: heatData }});
    layers.push(heatmapLayer);
  }}
  if (togTrails.checked) layers.push(tripsLayer);
  if (togArcs.checked && arcData.length) {{
    arcLayer = patchLayer(arcLayer, {{
      data: arcData,
      getWidth: d => 1 + 5 * (d.n / maxN),
    }});
    layers.push(arcLayer);
  }}
  if (togDepots.checked) {{
    const {{ rows, maxArr }} = depotDataForSnapshot(bucket.depot_snapshots);
    depotLayer = patchLayer(depotLayer, {{
      data: rows,
      getRadius: d => 200 + 600 * (d._arrivals / maxArr),
      getFillColor: d => {{
        const load = d.chargers > 0 ? d._charging / d.chargers : 0;
        return [Math.round(102 + 154 * load), Math.round(187 - 106 * load),
                Math.round(106 - 33 * load), 200];
      }},
    }});
    layers.push(depotLayer);
  }}
  // Parked (idle + charging) vehicles pushed LAST so dots sit on top of depot disk.
  if (togPositions.checked && VEHICLE_POSITIONS.length) {{
    positionsLayer = patchLayer(positionsLayer, {{ data: vehicleDots(tSec) }});
    layers.push(positionsLayer);
  }}
  deckgl.setProps({{ layers }});

  refreshDepotTooltip();
  updatePlotlyCursor(bucket.t_minutes);
}};

let dashboardRafId = null;
scheduleDashboardUpdate = function() {{
  if (dashboardRafId !== null) return;
  dashboardRafId = requestAnimationFrame(() => {{
    dashboardRafId = null;
    updateDashboard();
  }});
}};

togPositions.onchange = togTrails.onchange = togHeatmap.onchange = togArcs.onchange = togDepots.onchange = () => updateDashboard();
slider.oninput = scheduleDashboardUpdate;

// Initial render
if (loaderStatusEl) loaderStatusEl.textContent = 'Rendering map…';
updateDashboard();
document.getElementById('loader').style.display = 'none';
}}

fillCachesChunk(0);
}} catch (err) {{
  console.error(err);
  const ld = document.getElementById('loader');
  if (ld) {{
    ld.innerHTML = '<div style="color:#f85149;padding:20px;max-width:520px;line-height:1.5">'
      + '<b>Dashboard failed to load.</b><br/><br/>' + (err && err.message ? String(err.message) : String(err))
      + '<br/><br/>Open the browser devtools console (F12) for the full stack trace.</div>';
  }}
}}}})();
</script>
</body>
</html>"""


# ------------------------------------------------------------------
# Public API — call from any experiment script with existing results
# ------------------------------------------------------------------

def generate_visualization(
    result: dict,
    config: ScenarioConfig,
    output_path: str | None = None,
    sample_vehicles: int | None = None,
    open_browser: bool = True,
) -> str:
    """Generate a dashboard HTML from an already-computed sim result dict.

    Args:
        result: Raw engine output dict (must include vehicle_transitions,
                trip_log, timeseries, metrics).
        config: The ScenarioConfig used for this run.
        output_path: Where to write the HTML.  Defaults to scripts/visualize_<timestamp>.html.
        sample_vehicles: Max vehicles to animate.  None = auto (default 300).
        open_browser: If True, open the HTML in the default browser.

    Returns:
        The absolute path to the generated HTML file.
    """
    import webbrowser

    timeseries = result["timeseries"]
    metrics = result["metrics"]
    transitions = result["vehicle_transitions"]
    trip_log = result["trip_log"]

    fleet_size = config.fleet.size
    trail_n = sample_vehicles if sample_vehicles is not None else min(TRAIL_SAMPLE_SIZE, fleet_size)

    vehicle_ids = None
    if trail_n < fleet_size:
        all_vids = list({t[1] for t in transitions})
        rng = np.random.default_rng(config.seed)
        chosen = rng.choice(all_vids, size=min(trail_n, len(all_vids)), replace=False)
        vehicle_ids = set(chosen)
        print(f"[visualize] Sampled {len(vehicle_ids)} of {fleet_size} vehicles for trails")
    else:
        print(f"[visualize] Rendering all {fleet_size} vehicle trails")

    print("[visualize] Collecting unique H3 movement pairs...")
    h3_pairs = _collect_h3_pairs(transitions, vehicle_ids)
    print(f"[visualize] Found {len(h3_pairs)} unique H3 pairs")

    print("[visualize] Fetching OSRM route geometries...")
    route_geometries = _fetch_route_geometries(h3_pairs)
    osrm_hit = sum(1 for v in route_geometries.values() if len(v) > 2)
    print(f"[visualize] OSRM routes: {osrm_hit} road geometries, "
          f"{len(route_geometries) - osrm_hit} straight-line fallbacks")

    print("[visualize] Building vehicle paths...")
    vehicle_paths = _build_vehicle_paths(transitions, vehicle_ids, route_geometries)
    print(f"[visualize] {len(vehicle_paths)} movement segments")

    print("[visualize] Building per-vehicle position timelines...")
    vehicle_positions = _build_vehicle_positions(transitions, vehicle_ids)
    total_pts = sum(len(v["t"]) for v in vehicle_positions)
    print(f"[visualize] {len(vehicle_positions)} vehicle timelines, {total_pts} total transitions")

    print("[visualize] Building exact-timed trip arcs...")
    trip_arcs = _build_trip_arcs(trip_log)
    total_trips = sum(len(a["trips"]) for a in trip_arcs)
    print(f"[visualize] {total_trips} trips across {len(trip_arcs)} unique O-D pairs")

    depot_data = _build_depot_data(config)

    if depot_data:
        center_lng = sum(d["lng"] for d in depot_data) / len(depot_data)
        center_lat = sum(d["lat"] for d in depot_data) / len(depot_data)
    else:
        center_lng, center_lat = -97.7431, 30.2672

    payload = {
        "timeseries": timeseries,
        "metrics": metrics,
        "vehicle_paths": vehicle_paths,
        "vehicle_positions": vehicle_positions,
        "trip_arcs": trip_arcs,
        "depots": depot_data,
        "center": [center_lng, center_lat],
        "day_offset_s": config.demand.day_offset_seconds,
        "bucket_minutes": config.timeseries_bucket_minutes,
    }

    print("[visualize] Generating HTML...")
    data_json = json.dumps(payload, separators=(",", ":"))
    html = _generate_html(data_json)

    if output_path is None:
        ts = time.strftime("%Y%m%d_%H%M%S")
        output_path = str(ROOT / "scripts" / f"visualize_{ts}.html")

    with open(output_path, "w") as f:
        f.write(html)

    abs_path = os.path.abspath(output_path)
    file_size_mb = os.path.getsize(abs_path) / (1024 * 1024)
    print(f"[visualize] Written to {abs_path}  ({file_size_mb:.1f} MB)")
    if file_size_mb > 50:
        print("[visualize] WARNING: File exceeds 50 MB. Consider --sample-vehicles.")

    if open_browser:
        webbrowser.open(f"file://{abs_path}")

    return abs_path


# ------------------------------------------------------------------
# CLI main — runs its own sim then generates visualization
# ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Generate robotaxi sim visualization HTML")
    parser.add_argument("--scenario", type=str, default=None,
                        help="Path to a ScenarioConfig JSON file (default: default_scenario.json)")
    parser.add_argument("--output", type=str, default=None,
                        help="Output HTML file path (default: scripts/visualize_<timestamp>.html)")
    parser.add_argument("--sample-vehicles", type=int, default=None,
                        help=f"Max vehicles to animate (default: {TRAIL_SAMPLE_SIZE})")
    parser.add_argument("--no-open", action="store_true",
                        help="Don't auto-open the HTML in browser")
    args = parser.parse_args()

    if args.scenario:
        with open(args.scenario) as f:
            config = ScenarioConfig(**json.load(f))
        print(f"Loaded scenario from {args.scenario}")
    else:
        config = _load_default_scenario()
        print("Using default scenario")

    print(f"  fleet={config.fleet.size}  demand_scale={config.demand.demand_scale}  "
          f"duration={config.duration_minutes}min  depots={len(config.depots)}")

    print("\nRunning simulation...")
    t0 = time.time()

    bar_state = {"last": 0}
    bar = tqdm(total=1, desc="sim", unit="trips", ncols=100,
               bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]")

    def _progress(resolved, total):
        if bar.total != total:
            bar.total = total
            bar.refresh()
        delta = resolved - bar_state["last"]
        if delta > 0:
            bar.update(delta)
            bar_state["last"] = resolved

    result = _run_scenario(config, progress_callback=_progress)
    bar.update(bar.total - bar_state["last"])
    bar.close()
    wall_s = time.time() - t0
    print(f"Simulation complete in {wall_s:.1f}s\n")

    generate_visualization(
        result=result,
        config=config,
        output_path=args.output,
        sample_vehicles=args.sample_vehicles,
        open_browser=not args.no_open,
    )


if __name__ == "__main__":
    main()
