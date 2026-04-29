"""
Isolated-cell analysis + demand heatmap with isolated cells highlighted.

For each demand cell Y, computes the minimum road-network travel time from
any other demand cell X to Y (inbound isolation analysis).  Cells where this
minimum exceeds max_wait_time_seconds are "road-isolated" — no vehicle parked
at any demand cell can reach a trip there within the dispatch budget.

Outputs
-------
  - Console table of isolated cells with their min-inbound travel times
  - Required max_wait_time_seconds to eliminate all isolated cells
  - HTML map: demand heatmap + isolated cells highlighted

Run from repo root:
    python3 scripts/analyze_isolated_cells.py
"""
from __future__ import annotations

import os, sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "backend"))

import math
import numpy as np
import pandas as pd
import h3
import folium
from folium.plugins import HeatMap
from branca.colormap import linear

REQUESTS_PATH = os.path.join(ROOT, "data", "requests_austin_h3_r8.parquet")
TRAVEL_CACHE  = os.path.join(ROOT, "data", "h3_travel_cache.parquet")
OUTPUT_MAP    = os.path.join(ROOT, "scripts", "isolated_cells_map.html")
CURRENT_MAX_WAIT = 600.0   # seconds


def _cell_latlng(h3_cell: str) -> tuple[float, float]:
    """H3 cell center as (lat, lng)."""
    lat, lng = h3.cell_to_latlng(h3_cell)
    return lat, lng


def _h3_hex_boundary(h3_cell: str) -> list[tuple[float, float]]:
    """Return boundary coordinates as [(lat,lng),...] for Folium polygons."""
    boundary = h3.cell_to_boundary(h3_cell)
    return [(lat, lng) for lat, lng in boundary]


def main():
    print("Loading demand cells …")
    df_req = pd.read_parquet(REQUESTS_PATH, columns=["origin_h3", "destination_h3"])
    origin_counts = df_req["origin_h3"].value_counts().to_dict()
    demand_cell_set = set(origin_counts.keys())
    print(f"  {len(demand_cell_set):,} unique demand cells")

    # Trip counts by cell for heatmap (origins only)
    # Also build a dict of total trips (origin + dest) for labelling
    dest_counts = df_req["destination_h3"].value_counts().to_dict()
    all_trip_counts = {}
    for cell in demand_cell_set:
        all_trip_counts[cell] = origin_counts.get(cell, 0) + dest_counts.get(cell, 0)

    print("\nLoading travel cache (full, no time filter) …")
    df_cache = pd.read_parquet(
        TRAVEL_CACHE,
        columns=["origin_h3", "destination_h3", "time_seconds"],
    )
    # Keep only demand×demand pairs, exclude self-routes
    df_cache = df_cache[
        df_cache["origin_h3"].isin(demand_cell_set)
        & df_cache["destination_h3"].isin(demand_cell_set)
        & (df_cache["origin_h3"] != df_cache["destination_h3"])
    ]
    print(f"  {len(df_cache):,} demand×demand pairs in cache (excl. self-routes)")

    # ── Inbound isolation analysis ─────────────────────────────────────────
    # For each destination cell Y, find the minimum travel time from any X.
    print("\nComputing per-cell minimum inbound travel time …")
    min_inbound = df_cache.groupby("destination_h3")["time_seconds"].min()

    # Cells completely absent from cache as destinations
    cells_no_cache = demand_cell_set - set(min_inbound.index)

    # Cells in cache but ALL inbound routes exceed CURRENT_MAX_WAIT
    cells_over_budget = set(min_inbound[min_inbound > CURRENT_MAX_WAIT].index)

    # Cells with no inbound cache entry at all (treated as infinity)
    isolated_cells = cells_no_cache | cells_over_budget
    print(f"  Cells absent from cache as destinations: {len(cells_no_cache)}")
    print(f"  Cells with min-inbound > {CURRENT_MAX_WAIT:.0f}s: {len(cells_over_budget)}")
    print(f"  Total inbound-isolated cells (no vehicle can reach in ≤{CURRENT_MAX_WAIT:.0f}s): {len(isolated_cells)}")

    # Required max_wait to cover all isolated cells
    # = max over isolated cells of (min inbound travel time)
    iso_min_times: dict[str, float] = {}
    for cell in isolated_cells:
        if cell in min_inbound.index:
            iso_min_times[cell] = float(min_inbound[cell])
        else:
            # Not in cache at all: check if we have any rows via slower path
            subset = df_cache[df_cache["destination_h3"] == cell]
            iso_min_times[cell] = float(subset["time_seconds"].min()) if len(subset) else math.inf

    finite_times = [t for t in iso_min_times.values() if math.isfinite(t)]
    truly_unreachable = [c for c, t in iso_min_times.items() if not math.isfinite(t)]

    required_max_wait = max(finite_times) if finite_times else CURRENT_MAX_WAIT
    required_max_wait_ceil = math.ceil(required_max_wait / 10) * 10  # round up to nearest 10s

    print(f"\n  Truly unreachable cells (no path in cache at all): {len(truly_unreachable)}")
    print(f"  Required max_wait to cover all reachable-but-slow cells: {required_max_wait:.1f}s")
    print(f"  → Round up to: {required_max_wait_ceil}s")

    # Sort isolated cells by min-inbound time descending for table
    sorted_iso = sorted(iso_min_times.items(), key=lambda x: x[1], reverse=True)

    print(f"\n{'─'*72}")
    print(f"  {'cell':<20}  {'min-inbound (s)':>16}  {'min-inbound (min)':>18}  {'origin_rank':>12}")
    print(f"{'─'*72}")
    cell_ranks = {c: i+1 for i, (c, _) in enumerate(
        sorted(origin_counts.items(), key=lambda x: -x[1])
    )}
    for cell, t in sorted_iso:
        rank = cell_ranks.get(cell, "—")
        t_str = f"{t:.1f}" if math.isfinite(t) else "∞"
        t_min_str = f"{t/60:.1f}" if math.isfinite(t) else "∞"
        print(f"  {cell:<20}  {t_str:>16}  {t_min_str:>18}  #{rank:>10}")
    print(f"{'─'*72}")
    print(f"  Total isolated cells: {len(isolated_cells)}")
    print(f"  Required max_wait_time_seconds = {required_max_wait_ceil}s  "
          f"({required_max_wait_ceil/60:.1f} min)")

    # ── Build HTML map ────────────────────────────────────────────────────
    print("\nBuilding map …")

    # Map center = centroid of all demand cells
    lats = [h3.cell_to_latlng(c)[0] for c in demand_cell_set]
    lngs = [h3.cell_to_latlng(c)[1] for c in demand_cell_set]
    center_lat = float(np.mean(lats))
    center_lng = float(np.mean(lngs))

    m = folium.Map(location=[center_lat, center_lng], zoom_start=11, tiles="CartoDB dark_matter")

    # ── Layer 1: demand heatmap (trip origins) ─────────────────────────────
    # Draw filled hexagons colored by origin demand density
    max_count = max(origin_counts.values())
    colormap = linear.YlOrRd_09.scale(0, max_count)
    colormap.caption = "Trip origin count (historical)"
    colormap.add_to(m)

    demand_layer = folium.FeatureGroup(name="Demand heatmap", show=True)
    for cell, count in origin_counts.items():
        if cell in isolated_cells:
            continue  # draw isolated cells separately
        boundary = _h3_hex_boundary(cell)
        opacity = 0.15 + 0.55 * (count / max_count)
        folium.Polygon(
            locations=boundary,
            color=None,
            fill=True,
            fill_color=colormap(count),
            fill_opacity=opacity,
            weight=0,
            tooltip=f"Cell: {cell}<br>Origins: {count:,}<br>Rank: #{cell_ranks.get(cell,'?')}",
        ).add_to(demand_layer)
    demand_layer.add_to(m)

    # ── Layer 2: isolated cells highlighted ────────────────────────────────
    isolated_layer = folium.FeatureGroup(name=f"Isolated cells (min-inbound > {CURRENT_MAX_WAIT:.0f}s)", show=True)

    for cell, t in sorted_iso:
        boundary = _h3_hex_boundary(cell)
        lat, lng = _cell_latlng(cell)
        rank = cell_ranks.get(cell, "?")
        origins = origin_counts.get(cell, 0)
        t_str = f"{t:.0f}s ({t/60:.1f}min)" if math.isfinite(t) else "unreachable"

        # Color: bright orange-red with a pulsing look
        fill_color = "#FF4500"
        border_color = "#FFD700"

        folium.Polygon(
            locations=boundary,
            color=border_color,
            fill=True,
            fill_color=fill_color,
            fill_opacity=0.80,
            weight=2,
            tooltip=(
                f"<b>ISOLATED CELL</b><br>"
                f"Cell: {cell}<br>"
                f"Origin rank: #{rank}<br>"
                f"Origins: {origins:,}<br>"
                f"Min inbound travel: {t_str}<br>"
                f"(no demand-cell vehicle can reach in ≤{CURRENT_MAX_WAIT:.0f}s)"
            ),
        ).add_to(isolated_layer)

        # Add circle marker at center for visibility at low zoom
        folium.CircleMarker(
            location=[lat, lng],
            radius=6,
            color=border_color,
            fill=True,
            fill_color=fill_color,
            fill_opacity=1.0,
            weight=1.5,
            popup=folium.Popup(
                f"<b>Isolated cell</b>: {cell}<br>"
                f"Rank #{rank} | {origins:,} origins<br>"
                f"Nearest demand cell: {t_str}",
                max_width=300,
            ),
        ).add_to(isolated_layer)

    isolated_layer.add_to(m)

    # ── Legend ─────────────────────────────────────────────────────────────
    legend_html = f"""
    <div style="position:fixed; bottom:30px; left:30px; z-index:9999;
                background:rgba(30,30,30,0.85); padding:12px 16px;
                border-radius:8px; color:white; font-size:13px;
                border:1px solid #555; min-width:260px;">
      <b>Isolated Demand Cells</b><br>
      <span style="color:#FF4500;">■</span> Isolated: min-inbound &gt; {CURRENT_MAX_WAIT:.0f}s<br>
      <span style="color:#ffd700;">—</span> Gold border = isolated cell outline<br>
      <br>
      <span style="color:#aaa;">Total isolated: {len(isolated_cells)} / {len(demand_cell_set):,} cells</span><br>
      <span style="color:#aaa;">Required max_wait: {required_max_wait_ceil}s ({required_max_wait_ceil/60:.1f}min)</span>
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))

    folium.LayerControl(collapsed=False).add_to(m)

    m.save(OUTPUT_MAP)
    print(f"\n  Map saved → {OUTPUT_MAP}")
    print(f"\n{'='*72}")
    print(f"  RESULT: Set max_wait_time_seconds = {required_max_wait_ceil}s to include all isolated cells")
    print(f"{'='*72}\n")

    return required_max_wait_ceil


if __name__ == "__main__":
    main()
