"""
Generate a self-contained interactive HTML map for the Robotaxi-Sim.

Features:
  - H3 hex heatmap of Austin trip density (log-scale blue→red, no border stripes)
  - Leaflet.draw polygon tool — draw a service zone directly on the map
  - Sidebar form: fleet size, fixed cost, demand scale, max wait, duration
  - "Run Simulation" → POST to /run → results displayed in-page

Usage:
    python3 scripts/generate_interactive_map.py
    # opens austin_interactive_map.html in your default browser
"""
from __future__ import annotations

import json
import math
import os
import sys
import webbrowser
from pathlib import Path

try:
    import pyarrow.parquet as pq
    import h3
except ImportError as e:
    print(f"Missing dependency: {e}\nRun: pip install pyarrow h3")
    sys.exit(1)

PARQUET = Path(__file__).parent.parent / "data" / "requests_austin_h3_r8.parquet"
OUTPUT = Path(__file__).parent / "austin_interactive_map.html"

if not PARQUET.exists():
    print(f"Parquet not found: {PARQUET}")
    print("Run scripts/preprocess_rideaustin_requests.py first.")
    sys.exit(1)

# ── Load data ────────────────────────────────────────────────────────────────
print(f"Loading {PARQUET} ...")
table = pq.read_table(PARQUET, columns=["origin_h3", "destination_h3"])
origins = table.column("origin_h3").to_pylist()
dests = table.column("destination_h3").to_pylist()
print(f"  {len(origins):,} trips")

combined: dict[str, int] = {}
for c in origins:
    combined[c] = combined.get(c, 0) + 1
for c in dests:
    combined[c] = combined.get(c, 0) + 1

print(f"  {len(combined):,} unique H3 cells")
max_count = max(combined.values())


def count_to_color(count: int, max_c: int) -> str:
    ratio = math.log1p(count) / math.log1p(max_c)
    r = int(255 * min(1.0, ratio * 2))
    g = int(255 * (1 - abs(ratio - 0.5) * 2))
    b = int(255 * max(0.0, 1.0 - ratio * 2))
    return f"#{r:02x}{g:02x}{b:02x}"


# ── Build hex data array ─────────────────────────────────────────────────────
print("Building hex boundary data ...")
hex_data = []
for cell, count in sorted(combined.items(), key=lambda x: x[1]):
    try:
        boundary = h3.cell_to_boundary(cell)  # [(lat, lng), ...]
        latlngs = [[lat, lng] for lat, lng in boundary]
        hex_data.append({
            "ll": latlngs,
            "c": count,
            "col": count_to_color(count, max_count),
        })
    except Exception:
        pass

hex_json = json.dumps(hex_data, separators=(",", ":"))
print(f"  {len(hex_data):,} cells encoded ({len(hex_json)//1024} KB)")

# ── Generate HTML ─────────────────────────────────────────────────────────────
html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1.0"/>
<title>Robotaxi-Sim — Austin Coverage Explorer</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/leaflet.draw/1.0.4/leaflet.draw.css"/>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
html,body{{height:100%;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#1a1a2e;color:#e0e0e0}}
#layout{{display:flex;height:100vh}}
#map{{flex:1;min-width:0}}
#sidebar{{
  width:320px;flex-shrink:0;background:#16213e;border-left:1px solid #0f3460;
  display:flex;flex-direction:column;overflow:hidden
}}
#sidebar-header{{
  padding:16px 18px 12px;background:#0f3460;border-bottom:1px solid #1a4a80
}}
#sidebar-header h1{{font-size:15px;font-weight:700;color:#e0e6ff;letter-spacing:.5px}}
#sidebar-header p{{font-size:11px;color:#8090b0;margin-top:3px}}
#sidebar-body{{flex:1;overflow-y:auto;padding:16px}}
.section{{margin-bottom:18px}}
.section-title{{
  font-size:10px;font-weight:600;text-transform:uppercase;letter-spacing:1px;
  color:#6080a0;margin-bottom:8px
}}
.field{{margin-bottom:10px}}
.field label{{display:block;font-size:12px;color:#a0b0c8;margin-bottom:4px}}
.field input,.field select{{
  width:100%;padding:7px 10px;background:#1e2d4a;border:1px solid #2a4060;
  border-radius:5px;color:#e0e6ff;font-size:13px;outline:none
}}
.field input:focus,.field select:focus{{border-color:#4a7abd}}
.hint{{font-size:10px;color:#506070;margin-top:3px}}
.zone-row{{display:flex;gap:8px;margin-bottom:14px}}
.btn{{
  flex:1;padding:9px 0;border:none;border-radius:5px;font-size:12px;
  font-weight:600;cursor:pointer;letter-spacing:.3px;transition:all .15s
}}
.btn-draw{{background:#1e4d8c;color:#a0c4ff}}
.btn-draw:hover{{background:#2a5ea8}}
.btn-draw.active{{background:#4a7abd;color:#fff;box-shadow:0 0 0 2px #7ab0ff}}
.btn-clear{{background:#2a1f3d;color:#c090ff}}
.btn-clear:hover{{background:#3a2a55}}
.btn-run{{
  width:100%;padding:11px 0;background:#1a7a5a;color:#80ffc0;border:none;
  border-radius:6px;font-size:14px;font-weight:700;cursor:pointer;
  letter-spacing:.5px;margin-bottom:14px;transition:all .15s
}}
.btn-run:hover{{background:#22a070}}
.btn-run:disabled{{background:#1a3a2a;color:#406050;cursor:default}}
#zone-status{{
  font-size:11px;color:#70a090;background:#0f2a1e;padding:6px 9px;
  border-radius:4px;margin-bottom:14px;border:1px solid #1a4a30;
  min-height:28px;display:flex;align-items:center
}}
#results{{flex:1}}
.result-card{{
  background:#0f2030;border:1px solid #1a3a55;border-radius:6px;padding:12px;
  margin-bottom:10px
}}
.result-title{{font-size:11px;font-weight:600;color:#6090c0;margin-bottom:8px;text-transform:uppercase;letter-spacing:.5px}}
.metric-row{{display:flex;justify-content:space-between;align-items:center;margin-bottom:4px}}
.metric-label{{font-size:12px;color:#8090a0}}
.metric-value{{font-size:13px;font-weight:600;color:#c0d8f0}}
.metric-value.good{{color:#60d090}}
.metric-value.warn{{color:#e0b060}}
.metric-value.bad{{color:#e06060}}
#spinner{{
  text-align:center;padding:24px;color:#4080c0;font-size:13px
}}
#error-msg{{
  background:#2a0f0f;border:1px solid #601010;border-radius:5px;
  padding:10px;font-size:12px;color:#ff8080;margin-bottom:10px
}}
.legend{{
  background:#1a2a3a;border:1px solid #2a3a4a;border-radius:5px;
  padding:10px;margin-bottom:14px
}}
.legend-title{{font-size:10px;color:#6080a0;margin-bottom:6px;font-weight:600;text-transform:uppercase;letter-spacing:.5px}}
.legend-bar{{
  height:12px;border-radius:3px;margin-bottom:4px;
  background:linear-gradient(to right,#0000ff,#00ff00,#ff0000)
}}
.legend-labels{{display:flex;justify-content:space-between;font-size:10px;color:#506070}}
</style>
</head>
<body>
<div id="layout">
  <div id="map"></div>
  <div id="sidebar">
    <div id="sidebar-header">
      <h1>&#x1F697; Robotaxi-Sim</h1>
      <p>Austin Coverage Explorer</p>
    </div>
    <div id="sidebar-body">

      <div class="legend">
        <div class="legend-title">Trip density (origin + destination)</div>
        <div class="legend-bar"></div>
        <div class="legend-labels"><span>Low</span><span>Medium</span><span>High</span></div>
      </div>

      <div class="section">
        <div class="section-title">&#x2316; Service Zone</div>
        <div class="zone-row">
          <button class="btn btn-draw" id="btn-draw" onclick="toggleDraw()">&#x270F; Draw Zone</button>
          <button class="btn btn-clear" onclick="clearZone()">&#x2715; Clear</button>
        </div>
        <div id="zone-status">No zone drawn — full Austin dataset will be used.</div>
      </div>

      <div class="section">
        <div class="section-title">&#x2699;&#xFE0F; Simulation Parameters</div>
        <div class="field">
          <label>Fleet size (vehicles)</label>
          <input type="number" id="fleet_size" value="200" min="10" max="2000" step="10"/>
        </div>
        <div class="field">
          <label>Fixed cost per vehicle/day ($)</label>
          <input type="number" id="fixed_cost" value="27.40" min="0" step="0.01"/>
          <div class="hint">$27.40 = depreciation only &nbsp;|&nbsp; $56 = comprehensive</div>
        </div>
        <div class="field">
          <label>Demand scale</label>
          <input type="number" id="demand_scale" value="0.02" min="0.001" max="1.0" step="0.001"/>
          <div class="hint">0.02 = ~330 trips/day &nbsp;|&nbsp; 1.0 = full ~16k trips</div>
        </div>
        <div class="field">
          <label>Max wait time (seconds)</label>
          <input type="number" id="max_wait" value="600" min="60" max="3600" step="60"/>
        </div>
        <div class="field">
          <label>Duration</label>
          <select id="duration">
            <option value="1440" selected>Full day (24h)</option>
            <option value="720">Half day (12h)</option>
            <option value="360">6 hours</option>
            <option value="120">2 hours</option>
          </select>
        </div>
        <div class="field">
          <label>Dispatch strategy</label>
          <select id="dispatch_strategy">
            <option value="nearest" selected>Nearest (min ETA)</option>
            <option value="first_feasible">First feasible (≤ threshold)</option>
          </select>
        </div>
      </div>

      <button class="btn-run" id="btn-run" onclick="runSim()">&#x25B6; Run Simulation</button>

      <div id="zone-status-run" style="display:none"></div>
      <div id="spinner" style="display:none">&#x23F3; Running simulation…<br/><small>This may take 30–120 seconds</small></div>
      <div id="error-msg" style="display:none"></div>
      <div id="results"></div>

    </div>
  </div>
</div>

<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/leaflet.draw/1.0.4/leaflet.draw.js"></script>
<script>
// ── Map init ──────────────────────────────────────────────────────────────
const map = L.map('map', {{
  center: [30.267, -97.743],
  zoom: 11,
  zoomControl: true,
}});

L.tileLayer('https://{{s}}.basemaps.cartocdn.com/dark_all/{{z}}/{{x}}/{{y}}{{r}}.png', {{
  attribution: '&copy; OpenStreetMap &copy; CARTO',
  subdomains: 'abcd',
  maxZoom: 19,
}}).addTo(map);

// ── Hex heatmap ───────────────────────────────────────────────────────────
const HEX_DATA = {hex_json};
const hexLayer = L.layerGroup().addTo(map);

HEX_DATA.forEach(h => {{
  L.polygon(h.ll, {{
    color: 'transparent',
    fillColor: h.col,
    fillOpacity: 0.70,
    weight: 0,
  }})
  .bindTooltip(`Trips (origin+dest): ${{h.c.toLocaleString()}}`, {{sticky: true}})
  .addTo(hexLayer);
}});

// ── Polygon draw ──────────────────────────────────────────────────────────
const drawnItems = new L.FeatureGroup().addTo(map);
const drawControl = new L.Control.Draw({{
  edit: {{ featureGroup: drawnItems, remove: true }},
  draw: {{
    polygon: {{ shapeOptions: {{ color: '#7ab0ff', fillColor: '#3060a0', fillOpacity: 0.15, weight: 2 }} }},
    polyline: false,
    rectangle: false,
    circle: false,
    circlemarker: false,
    marker: false,
  }}
}});

let drawnPolygon = null;
let drawing = false;

map.on(L.Draw.Event.CREATED, function(e) {{
  drawnItems.clearLayers();
  drawnItems.addLayer(e.layer);
  const latlngs = e.layer.getLatLngs()[0];
  // Convert Leaflet [lat,lng] → [lng,lat] for GeoJSON/backend
  drawnPolygon = latlngs.map(ll => [ll.lng, ll.lat]);
  // Close the ring
  if (drawnPolygon.length > 0) drawnPolygon.push(drawnPolygon[0]);
  const nCells = 'calculating…';
  document.getElementById('zone-status').textContent =
    `Zone drawn — ${{drawnPolygon.length - 1}} vertices. Will filter trips to this area.`;
  drawing = false;
  setDrawBtn(false);
  map.removeControl(drawControl);
}});

map.on(L.Draw.Event.DELETED, function() {{
  drawnPolygon = null;
  document.getElementById('zone-status').textContent = 'Zone cleared — full Austin dataset will be used.';
}});

function toggleDraw() {{
  if (drawing) {{
    map.removeControl(drawControl);
    drawing = false;
    setDrawBtn(false);
  }} else {{
    map.addControl(drawControl);
    drawing = true;
    setDrawBtn(true);
    // Auto-activate polygon tool
    setTimeout(() => {{
      const btn = document.querySelector('.leaflet-draw-draw-polygon');
      if (btn) btn.click();
    }}, 50);
  }}
}}

function setDrawBtn(active) {{
  const btn = document.getElementById('btn-draw');
  if (active) {{ btn.textContent = '⏹ Stop Drawing'; btn.classList.add('active'); }}
  else {{ btn.textContent = '✏ Draw Zone'; btn.classList.remove('active'); }}
}}

function clearZone() {{
  drawnItems.clearLayers();
  drawnPolygon = null;
  if (drawing) {{ map.removeControl(drawControl); drawing = false; setDrawBtn(false); }}
  document.getElementById('zone-status').textContent = 'No zone drawn — full Austin dataset will be used.';
}}

// ── Run simulation ────────────────────────────────────────────────────────
async function runSim() {{
  const btn = document.getElementById('btn-run');
  const spinner = document.getElementById('spinner');
  const errDiv = document.getElementById('error-msg');
  const resultsDiv = document.getElementById('results');

  btn.disabled = true;
  spinner.style.display = 'block';
  errDiv.style.display = 'none';
  resultsDiv.innerHTML = '';

  const payload = {{
    seed: 123,
    duration_minutes: parseInt(document.getElementById('duration').value),
    fleet: {{ size: parseInt(document.getElementById('fleet_size').value) }},
    demand: {{
      demand_scale: parseFloat(document.getElementById('demand_scale').value),
      max_wait_time_seconds: parseInt(document.getElementById('max_wait').value),
      coverage_polygon: drawnPolygon,
    }},
    economics: {{
      fixed_cost_per_vehicle_day: parseFloat(document.getElementById('fixed_cost').value),
    }},
    dispatch: {{
      strategy: document.getElementById('dispatch_strategy').value,
    }},
  }};

  try {{
    const resp = await fetch('http://localhost:8000/run', {{
      method: 'POST',
      headers: {{ 'Content-Type': 'application/json' }},
      body: JSON.stringify(payload),
    }});
    if (!resp.ok) {{
      const txt = await resp.text();
      throw new Error(`HTTP ${{resp.status}}: ${{txt.slice(0, 200)}}`);
    }}
    const data = await resp.json();
    renderResults(data.metrics);
  }} catch(e) {{
    errDiv.style.display = 'block';
    errDiv.textContent = `Error: ${{e.message}}`;
  }} finally {{
    btn.disabled = false;
    spinner.style.display = 'none';
  }}
}}

function cls(v, goodThresh, warnThresh, higherIsBetter=true) {{
  if (higherIsBetter) {{
    if (v >= goodThresh) return 'good';
    if (v >= warnThresh) return 'warn';
    return 'bad';
  }} else {{
    if (v <= goodThresh) return 'good';
    if (v <= warnThresh) return 'warn';
    return 'bad';
  }}
}}

function renderResults(m) {{
  const div = document.getElementById('results');
  const fmt = (v, dec=1) => v == null ? 'N/A' : Number(v).toFixed(dec);
  const fmtK = v => v == null ? 'N/A' : (v >= 1000 ? `${{(v/1000).toFixed(1)}}k` : fmt(v, 0));

  div.innerHTML = `
    <div class="result-card">
      <div class="result-title">&#x1F4CA; Service Level</div>
      <div class="metric-row"><span class="metric-label">Served</span>
        <span class="metric-value ${{cls(m.served_pct,80,60)}}">${{fmt(m.served_pct)}}%</span></div>
      <div class="metric-row"><span class="metric-label">SLA adherence</span>
        <span class="metric-value ${{cls(m.sla_adherence_pct,75,55)}}">${{fmt(m.sla_adherence_pct)}}%</span></div>
      <div class="metric-row"><span class="metric-label">Wait p50</span>
        <span class="metric-value ${{cls(m.median_wait_min,5,8,false)}}">${{fmt(m.median_wait_min)}} min</span></div>
      <div class="metric-row"><span class="metric-label">Wait p90</span>
        <span class="metric-value ${{cls(m.p90_wait_min,8,12,false)}}">${{fmt(m.p90_wait_min)}} min</span></div>
      <div class="metric-row"><span class="metric-label">Trips served</span>
        <span class="metric-value">${{m.served_count?.toLocaleString() ?? 'N/A'}}</span></div>
    </div>
    <div class="result-card">
      <div class="result-title">&#x1F697; Fleet</div>
      <div class="metric-row"><span class="metric-label">Utilization</span>
        <span class="metric-value ${{cls(m.utilization_pct,60,40)}}">${{fmt(m.utilization_pct)}}%</span></div>
      <div class="metric-row"><span class="metric-label">Trips/vehicle/day</span>
        <span class="metric-value">${{fmt(m.trips_per_vehicle_per_day)}}</span></div>
      <div class="metric-row"><span class="metric-label">Deadhead</span>
        <span class="metric-value ${{cls(m.deadhead_pct,30,45,false)}}">${{fmt(m.deadhead_pct)}}%</span></div>
      <div class="metric-row"><span class="metric-label">Repositioning</span>
        <span class="metric-value">${{fmt(m.repositioning_pct)}}%</span></div>
      <div class="metric-row"><span class="metric-label">Pool match</span>
        <span class="metric-value">${{fmt(m.pool_match_pct)}}%</span></div>
    </div>
    <div class="result-card">
      <div class="result-title">&#x1F4B0; Economics</div>
      <div class="metric-row"><span class="metric-label">Revenue</span>
        <span class="metric-value">${{fmtK(m.revenue_total)}}</span></div>
      <div class="metric-row"><span class="metric-label">Fixed cost</span>
        <span class="metric-value">${{fmtK(m.fixed_cost_total)}}</span></div>
      <div class="metric-row"><span class="metric-label">Total margin</span>
        <span class="metric-value ${{m.total_margin>=0?'good':'bad'}}">${{m.total_margin>=0?'':'-'}}${{fmtK(Math.abs(m.total_margin))}}</span></div>
      <div class="metric-row"><span class="metric-label">CM / trip</span>
        <span class="metric-value ${{cls(m.contribution_margin_per_trip,5,0)}}">${{fmt(m.contribution_margin_per_trip,2)}}</span></div>
      <div class="metric-row"><span class="metric-label">Revenue / trip</span>
        <span class="metric-value">${{fmt(m.avg_revenue_per_trip,2)}}</span></div>
    </div>
  `;
}}
</script>
</body>
</html>
"""

OUTPUT.write_text(html, encoding="utf-8")
print(f"\nSaved: {OUTPUT}  ({OUTPUT.stat().st_size // 1024} KB)")
print("Opening in browser ...")
webbrowser.open(f"file://{OUTPUT.resolve()}")
