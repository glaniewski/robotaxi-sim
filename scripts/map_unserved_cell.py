"""
Map the unserved-trip origin cell (88489e30cbfffff) in the same style as
isolated_cells_map.html — Folium, CartoDB dark, hex boundary, tooltip.

Run from repo root:
    python3 scripts/map_unserved_cell.py
"""
from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "backend"))

import h3
import folium

# Unserved origin cell from Exp 27 analysis (both unserved trips)
UNSERVED_CELL = "88489e30cbfffff"
# Default depot (Austin ODD) for reference
DEPOT_CELL = "88489e3467fffff"
OUTPUT_HTML = os.path.join(ROOT, "scripts", "unserved_cell_map.html")


def _h3_hex_boundary(h3_cell: str) -> list[tuple[float, float]]:
    boundary = h3.cell_to_boundary(h3_cell)
    return [(lat, lng) for lat, lng in boundary]


def main() -> None:
    lat, lng = h3.cell_to_latlng(UNSERVED_CELL)
    depot_lat, depot_lng = h3.cell_to_latlng(DEPOT_CELL)
    center_lat = (lat + depot_lat) / 2
    center_lng = (lng + depot_lng) / 2

    m = folium.Map(
        location=[center_lat, center_lng],
        zoom_start=12,
        tiles="CartoDB dark_matter",
    )

    # Unserved cell: orange-red fill, gold border (same as isolated)
    boundary = _h3_hex_boundary(UNSERVED_CELL)
    folium.Polygon(
        locations=boundary,
        color="#FFD700",
        fill=True,
        fill_color="#FF4500",
        fill_opacity=0.85,
        weight=2,
        tooltip=(
            f"<b>Unserved-trip origin cell</b><br>"
            f"Cell: {UNSERVED_CELL}<br>"
            f"2 unserved requests (req_t ~4.54h, 6.89h)<br>"
            f"~19.3 min from depot by road"
        ),
    ).add_to(m)
    folium.CircleMarker(
        location=[lat, lng],
        radius=8,
        color="#FFD700",
        fill=True,
        fill_color="#FF4500",
        fill_opacity=1.0,
        weight=1.5,
        popup=folium.Popup(
            f"<b>Unserved cell</b>: {UNSERVED_CELL}<br>"
            "Both Exp 27 unserved trips originated here.",
            max_width=300,
        ),
    ).add_to(m)

    # Depot: marker for reference
    folium.CircleMarker(
        location=[depot_lat, depot_lng],
        radius=6,
        color="#00FF00",
        fill=True,
        fill_color="#00AA00",
        fill_opacity=1.0,
        weight=1.5,
        tooltip=f"Depot (Austin ODD)<br>{DEPOT_CELL}",
        popup=folium.Popup("Default depot", max_width=200),
    ).add_to(m)

    legend_html = """
    <div style="position:fixed; bottom:30px; left:30px; z-index:9999;
                background:rgba(30,30,30,0.85); padding:12px 16px;
                border-radius:8px; color:white; font-size:13px;
                border:1px solid #555; min-width:280px;">
      <b>Unserved-trip origin cell</b><br>
      <span style="color:#FF4500;">■</span> Cell 88489e30cbfffff (2 unserved)<br>
      <span style="color:#ffd700;">—</span> Gold border = cell outline<br>
      <span style="color:#00AA00;">●</span> Depot (Austin ODD)<br>
      <br>
      <span style="color:#aaa;">Same style as isolated_cells_map.html</span>
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))

    m.save(OUTPUT_HTML)
    print(f"Map saved → {OUTPUT_HTML}")


if __name__ == "__main__":
    main()
