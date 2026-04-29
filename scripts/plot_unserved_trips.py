"""
Plot unserved trips as origin→destination lines on a Folium map,
with an all-trips origin heatmap underneath to highlight outlier cells.

Run from repo root:
    python3 scripts/plot_unserved_trips.py

Output: scripts/unserved_trips_map.html  (open in any browser)
"""
from __future__ import annotations

import os
import sys
import time

# ── make backend importable ──────────────────────────────────────────────────
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "backend"))

import h3
import folium
from folium.plugins import HeatMap

from app.sim.demand import load_requests
from app.sim.engine import SimConfig, SimulationEngine, build_vehicles
from app.sim.entities import RequestStatus
from app.sim.repositioning import RepositioningPolicy
from app.sim.routing import RoutingCache

# ── config ───────────────────────────────────────────────────────────────────
FLEET_SIZE    = 3_000
DEMAND_SCALE  = 0.05
SEED          = 123
DURATION_MIN  = 1440  # full 24 h

REQUESTS_PATH    = os.path.join(ROOT, "data", "requests_austin_h3_r8.parquet")
TRAVEL_CACHE     = os.path.join(ROOT, "data", "h3_travel_cache.parquet")
OSRM_URL         = os.environ.get("OSRM_URL", "http://localhost:5001")
OUT_HTML         = os.path.join(ROOT, "scripts", "unserved_trips_map.html")


def h3_latlng(cell: str) -> tuple[float, float]:
    lat, lng = h3.cell_to_latlng(cell)
    return lat, lng


def build_forecast_table(requests, duration_min: float) -> dict[str, float]:
    from collections import defaultdict
    counts: dict[str, int] = defaultdict(int)
    for r in requests:
        counts[r.origin_h3] += 1
    duration_s = duration_min * 60.0
    return {cell: cnt / duration_s for cell, cnt in counts.items()}


# ── run sim ──────────────────────────────────────────────────────────────────
print(f"Loading requests  (scale={DEMAND_SCALE}) …")
requests = load_requests(
    parquet_path=REQUESTS_PATH,
    duration_minutes=DURATION_MIN,
    max_wait_time_seconds=600.0,
    demand_scale=DEMAND_SCALE,
    seed=SEED,
)
print(f"  {len(requests):,} requests loaded")

routing = RoutingCache(parquet_path=TRAVEL_CACHE, osrm_url=OSRM_URL)

sim_config = SimConfig(
    duration_minutes=DURATION_MIN,
    seed=SEED,
    fleet_size=FLEET_SIZE,
    max_wait_time_seconds=600.0,
    reposition_enabled=True,
    reposition_alpha=0.6,
    reposition_top_k_cells=50,
    max_vehicles_targeting_cell=3,
)
vehicles = build_vehicles(sim_config, depot_h3_cells=["88489e3467fffff"], seed=SEED)

forecast_table = build_forecast_table(requests, DURATION_MIN)
repo_policy = RepositioningPolicy(
    alpha=sim_config.reposition_alpha,
    half_life_minutes=sim_config.reposition_half_life_minutes,
    forecast_horizon_minutes=sim_config.reposition_forecast_horizon_minutes,
    max_reposition_travel_minutes=sim_config.max_reposition_travel_minutes,
    max_vehicles_targeting_cell=sim_config.max_vehicles_targeting_cell,
    min_idle_minutes=sim_config.reposition_min_idle_minutes,
    top_k_cells=sim_config.reposition_top_k_cells,
    reposition_lambda=sim_config.reposition_lambda,
    forecast_table=forecast_table,
)

total = len(requests)
done  = [0]
t0    = time.time()

def _fmt(s: float) -> str:
    if not (0 <= s < 36000):
        return "--:--"
    return f"{int(s//60):02d}:{int(s%60):02d}"

def _progress(n_done: int, n_total: int) -> None:
    done[0] = n_done
    pct     = n_done / n_total * 100 if n_total else 0
    elapsed = time.time() - t0
    rate    = n_done / elapsed if elapsed > 0 else 0
    eta     = (n_total - n_done) / rate if rate > 0 else float("inf")
    bar_w   = 40
    filled  = int(pct / 100 * bar_w)
    bar     = "█" * filled + "░" * (bar_w - filled)
    print(
        f"\r  [{bar}] {pct:5.1f}%  {n_done:,}/{n_total:,}"
        f"  {_fmt(elapsed)}<{_fmt(eta)}  {rate:,.0f} trips/s",
        end="", flush=True,
    )

print(f"Running sim  (fleet={FLEET_SIZE:,}, 24 h) …")
engine = SimulationEngine(
    config=sim_config,
    vehicles=vehicles,
    requests=requests,
    depots=[],
    routing=routing,
    reposition_policy=repo_policy,
    progress_callback=_progress,
)
engine.run()
elapsed = time.time() - t0
print(f"\n  Done in {elapsed:.0f}s")

# ── extract trips ─────────────────────────────────────────────────────────────
all_reqs    = list(engine.requests.values())
served      = [r for r in all_reqs if r.status == RequestStatus.SERVED]
unserved    = [r for r in all_reqs if r.status == RequestStatus.UNSERVED]
pending     = [r for r in all_reqs if r.status == RequestStatus.PENDING]   # edge: sim-end
unserved_eff = unserved + pending   # all trips that never got a ride

print(f"  Served   : {len(served):,}")
print(f"  Unserved : {len(unserved):,}  (+{len(pending)} still-pending at sim end)")
print(f"  Total    : {len(all_reqs):,}")

# ── build map ─────────────────────────────────────────────────────────────────
austin_center = [30.267, -97.743]
m = folium.Map(location=austin_center, zoom_start=11, tiles="CartoDB dark_matter")

# ── layer 1: all-trip origin heatmap ─────────────────────────────────────────
print("Building all-trip origin heatmap …")
heat_points = []
for r in all_reqs:
    lat, lng = h3_latlng(r.origin_h3)
    heat_points.append([lat, lng, 1])

HeatMap(
    heat_points,
    name="All trip origins (heatmap)",
    min_opacity=0.25,
    radius=14,
    blur=10,
    gradient={0.2: "#0000ff", 0.5: "#00ffff", 0.75: "#ffff00", 1.0: "#ff0000"},
).add_to(m)

# ── layer 2: unserved trip lines ──────────────────────────────────────────────
print(f"Drawing {len(unserved_eff):,} unserved trip lines …")
unserved_fg = folium.FeatureGroup(name=f"Unserved trips ({len(unserved_eff):,})", show=True)

for r in unserved_eff:
    o_lat, o_lng = h3_latlng(r.origin_h3)
    d_lat, d_lng = h3_latlng(r.destination_h3)

    # Origin marker
    folium.CircleMarker(
        location=[o_lat, o_lng],
        radius=4,
        color="#ff4444",
        fill=True,
        fill_color="#ff4444",
        fill_opacity=0.85,
        weight=1,
        tooltip=f"UNSERVED origin<br>req: {r.id}<br>t={r.request_time/3600:.2f}h",
    ).add_to(unserved_fg)

    # Origin → destination line
    folium.PolyLine(
        locations=[[o_lat, o_lng], [d_lat, d_lng]],
        color="#ff6600",
        weight=1.5,
        opacity=0.6,
        tooltip=f"{r.id}",
    ).add_to(unserved_fg)

    # Destination marker
    folium.CircleMarker(
        location=[d_lat, d_lng],
        radius=3,
        color="#ffaa00",
        fill=True,
        fill_color="#ffaa00",
        fill_opacity=0.6,
        weight=1,
    ).add_to(unserved_fg)

unserved_fg.add_to(m)

# ── layer 3: served trip origin density (separate control) ───────────────────
served_heat = []
for r in served:
    lat, lng = h3_latlng(r.origin_h3)
    served_heat.append([lat, lng, 1])

HeatMap(
    served_heat,
    name="Served trip origins",
    min_opacity=0.0,
    radius=14,
    blur=10,
    show=False,
    gradient={0.2: "#002200", 0.5: "#00aa44", 1.0: "#00ff88"},
).add_to(m)

# ── stats inset ───────────────────────────────────────────────────────────────
served_pct = len(served) / len(all_reqs) * 100 if all_reqs else 0
info_html = f"""
<div style="
    position: fixed; bottom: 30px; left: 30px; z-index: 1000;
    background: rgba(20,20,20,0.88); color: #eee;
    padding: 12px 16px; border-radius: 8px;
    font-family: monospace; font-size: 13px; line-height: 1.7;
    border: 1px solid #555;
">
  <b style="font-size:14px">Unserved trip analysis</b><br>
  Fleet: <b>{FLEET_SIZE:,}</b> &nbsp;|&nbsp; Demand scale: <b>{DEMAND_SCALE}</b><br>
  Total requests : <b>{len(all_reqs):,}</b><br>
  Served         : <b>{len(served):,}</b> ({served_pct:.1f}%)<br>
  Unserved       : <b style="color:#ff6655">{len(unserved_eff):,}</b>
    ({100-served_pct:.1f}%)<br>
  <hr style="border-color:#555; margin:6px 0">
  <span style="color:#ff4444">●</span> red dot = unserved origin<br>
  <span style="color:#ff6600">—</span> orange line = origin→destination<br>
  heatmap = all trip origin density
</div>
"""
m.get_root().html.add_child(folium.Element(info_html))

folium.LayerControl(collapsed=False).add_to(m)

m.save(OUT_HTML)
print(f"\nMap saved → {OUT_HTML}")
print("Open it in your browser.")
