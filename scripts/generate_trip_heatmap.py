"""
Generate a static HTML heatmap showing trip density by H3 cell across Austin.

Usage:
    pip install folium pyarrow pandas h3
    python3 scripts/generate_trip_heatmap.py

Output: scripts/austin_trip_heatmap.html  — open in any browser.

The map shows:
  - H3 cells colored by origin trip density (log scale)
  - Hover tooltip with cell ID and trip count
  - A legend explaining the color scale

To define a coverage polygon for the simulation:
  1. Open https://geojson.io
  2. Draw a polygon over the cells you want to cover
  3. Copy the 'coordinates' array from the GeoJSON output
  4. Pass it as "coverage_polygon" in DemandConfig when calling /run:

Example API call with downtown polygon:
  POST /run
  {
    "seed": 123,
    "duration_minutes": 1440,
    "demand": {
      "demand_scale": 0.02,
      "coverage_polygon": [
        [-97.76, 30.26], [-97.72, 30.26],
        [-97.72, 30.29], [-97.76, 30.29], [-97.76, 30.26]
      ]
    },
    "fleet": {"size": 200}
  }
"""
from __future__ import annotations
import sys
from pathlib import Path

try:
    import folium
    import h3
    import pandas as pd
    import pyarrow.parquet as pq
except ImportError as e:
    print(f"Missing dependency: {e}")
    print("Run: pip install folium pyarrow pandas h3")
    sys.exit(1)

PARQUET = Path(__file__).parent.parent / "data" / "requests_austin_h3_r8.parquet"
OUTPUT = Path(__file__).parent / "austin_trip_heatmap.html"
RESOLUTION = 8

if not PARQUET.exists():
    print(f"Parquet not found: {PARQUET}")
    print("Run scripts/preprocess_rideaustin_requests.py first.")
    sys.exit(1)

print(f"Loading {PARQUET} ...")
df = pd.read_parquet(PARQUET, columns=["origin_h3", "destination_h3"])
print(f"  {len(df):,} trips loaded")

# Count trips by origin cell
origin_counts = df["origin_h3"].value_counts().to_dict()
dest_counts = df["destination_h3"].value_counts().to_dict()
all_cells = set(origin_counts) | set(dest_counts)
combined = {c: origin_counts.get(c, 0) + dest_counts.get(c, 0) for c in all_cells}
print(f"  {len(combined):,} unique H3 cells")

# ── Build map ──────────────────────────────────────────────────────────────────
import math

# Austin city center
m = folium.Map(location=[30.267, -97.743], zoom_start=12, tiles="CartoDB positron")

max_count = max(combined.values())

def count_to_color(count: int, max_c: int) -> str:
    """Log-scale color: low = light blue, high = dark red."""
    ratio = math.log1p(count) / math.log1p(max_c)
    # Interpolate through blue → yellow → red
    r = int(255 * min(1.0, ratio * 2))
    g = int(255 * (1 - abs(ratio - 0.5) * 2))
    b = int(255 * max(0.0, 1.0 - ratio * 2))
    return f"#{r:02x}{g:02x}{b:02x}"

print("Rendering H3 polygons ...")
rendered = 0
for cell, count in sorted(combined.items(), key=lambda x: x[1]):
    try:
        boundary = h3.cell_to_boundary(cell)  # list of (lat, lng) tuples
        latlngs = [[lat, lng] for lat, lng in boundary]
        color = count_to_color(count, max_count)
        folium.Polygon(
            locations=latlngs,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.65,
            weight=0.5,
            tooltip=f"Cell: {cell}<br>Trips (origin+dest): {count:,}",
        ).add_to(m)
        rendered += 1
    except Exception:
        pass

print(f"  {rendered:,} cells rendered")

# ── Legend ─────────────────────────────────────────────────────────────────────
legend_html = """
<div style="position:fixed;bottom:30px;left:30px;z-index:1000;background:white;
            padding:12px;border:1px solid #ccc;border-radius:6px;font-size:13px;">
  <b>Trip density (origin + destination)</b><br>
  <span style="color:#0000ff">&#9632;</span> Low &nbsp;
  <span style="color:#00ff00">&#9632;</span> Medium &nbsp;
  <span style="color:#ff0000">&#9632;</span> High<br>
  <br>
  <i>Log scale. Hover cells for count.</i><br>
  <br>
  <b>To define a coverage zone:</b><br>
  1. Go to <a href="https://geojson.io" target="_blank">geojson.io</a><br>
  2. Draw a polygon over the area<br>
  3. Copy coordinates → paste into<br>
  &nbsp;&nbsp;&nbsp;<code>demand.coverage_polygon</code> in /run
</div>
"""
m.get_root().html.add_child(folium.Element(legend_html))

m.save(str(OUTPUT))
print(f"\nHeatmap saved to: {OUTPUT}")
print("Open it in your browser.")
