#!/bin/bash
set -e

PBF=/data/texas-latest.osm.pbf
OSRM=/data/texas-latest.osrm

if [ ! -f "$PBF" ]; then
  echo "ERROR: Texas PBF not found at $PBF"
  echo "Download it first on the host by running:"
  echo "  python3.11 scripts/download_osrm_pbf.py"
  exit 1
fi

if [ ! -f "$OSRM" ]; then
  echo "[osrm-init] Preprocessing road network (extract → partition → customize)..."
  echo "[osrm-init] This takes 5–15 minutes on first run."

  osrm-extract -p /opt/car.lua "$PBF"
  echo "[osrm-init] Extract complete."

  osrm-partition "$OSRM"
  echo "[osrm-init] Partition complete."

  osrm-customize "$OSRM"
  echo "[osrm-init] Customize complete. Preprocessing done."
fi

echo "[osrm] Starting osrm-routed..."
exec osrm-routed --algorithm mld "$OSRM" --max-table-size 10000
