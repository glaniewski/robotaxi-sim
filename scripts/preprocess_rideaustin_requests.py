#!/usr/bin/env python3
"""
Preprocess RideAustin CSV into H3-indexed request parquet.

Input:  data/raw/*.csv  (RideAustin trip data)
Output: data/requests_austin_h3_r8.parquet

Required CSV columns:
  started_on, start_lat, start_lng, end_lat, end_lng

Optional (kept for validation):
  completed_on, distance_travelled
"""
from __future__ import annotations

import glob
import os
import sys
from pathlib import Path

import h3
import pandas as pd

H3_RESOLUTION = 8
RAW_DIR = Path("data/raw")
OUTPUT_PATH = Path("data/requests_austin_h3_r8.parquet")


def main() -> None:
    csv_files = sorted(glob.glob(str(RAW_DIR / "*.csv")))
    if not csv_files:
        print(f"ERROR: No CSV files found in {RAW_DIR}/", file=sys.stderr)
        print("Place RideAustin CSV(s) in data/raw/ and retry.", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(csv_files)} CSV file(s): {csv_files}")

    dfs = []
    for path in csv_files:
        print(f"  Loading {path}...")
        df = pd.read_csv(path, low_memory=False)
        dfs.append(df)

    df = pd.concat(dfs, ignore_index=True)
    print(f"Total rows loaded: {len(df):,}")

    # Normalize column names (RideAustin uses snake_case)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Normalize to canonical names regardless of source column naming
    col_aliases = {
        "start_location_lat": "start_lat",
        "start_location_long": "start_lng",
        "start_location_lng": "start_lng",
        "end_location_lat": "end_lat",
        "end_location_long": "end_lng",
        "end_location_lng": "end_lng",
    }
    df = df.rename(columns={k: v for k, v in col_aliases.items() if k in df.columns})

    required = {"started_on", "start_lat", "start_lng", "end_lat", "end_lng"}
    missing = required - set(df.columns)
    if missing:
        print(f"ERROR: Missing required columns: {missing}", file=sys.stderr)
        print(f"Available columns: {list(df.columns)}", file=sys.stderr)
        sys.exit(1)

    # Parse timestamps
    df["started_on"] = pd.to_datetime(df["started_on"], utc=True)
    df = df.dropna(subset=["started_on", "start_lat", "start_lng", "end_lat", "end_lng"])

    # Filter to Austin ODD bounding box and drop coordinate outliers
    AUSTIN_LAT = (30.0, 30.7)
    AUSTIN_LNG = (-98.2, -97.4)
    df = df[
        df["start_lat"].between(*AUSTIN_LAT) &
        df["start_lng"].between(*AUSTIN_LNG) &
        df["end_lat"].between(*AUSTIN_LAT) &
        df["end_lng"].between(*AUSTIN_LNG)
    ]
    print(f"Rows after Austin bbox filter: {len(df):,}")

    # Filter unrealistic trip durations (negative or > 4 hours) and distances (> 35 miles)
    if "completed_on" in df.columns:
        df["completed_on"] = pd.to_datetime(df["completed_on"], utc=True)
        df["_duration_min"] = (df["completed_on"] - df["started_on"]).dt.total_seconds() / 60.0
        df = df[df["_duration_min"].between(1, 240)]
        print(f"Rows after duration filter (1–240 min): {len(df):,}")
    if "distance_travelled" in df.columns:
        # distance_travelled is in meters; 35 miles = 56,327 m
        df = df[df["distance_travelled"].between(0, 56327)]
        print(f"Rows after distance filter (≤35 miles): {len(df):,}")

    # Convert to H3
    print("Converting coordinates to H3 cells...")
    df["origin_h3"] = df.apply(
        lambda r: h3.latlng_to_cell(r["start_lat"], r["start_lng"], H3_RESOLUTION), axis=1
    )
    df["destination_h3"] = df.apply(
        lambda r: h3.latlng_to_cell(r["end_lat"], r["end_lng"], H3_RESOLUTION), axis=1
    )

    # Drop same-cell trips
    df = df[df["origin_h3"] != df["destination_h3"]]
    print(f"Rows after same-cell filter: {len(df):,}")

    # Collapse all trips to a single synthetic day using time-of-day only.
    # request_time_seconds = seconds since midnight (0–86399).
    # This overlays all historical days to create a dense, representative
    # Austin demand profile while preserving time-of-day peaks.
    df["request_time_seconds"] = (
        df["started_on"].dt.hour * 3600
        + df["started_on"].dt.minute * 60
        + df["started_on"].dt.second
    ).astype(float)

    # Build output dataframe
    out_cols = ["request_time_seconds", "origin_h3", "destination_h3"]
    if "completed_on" in df.columns:
        if "_duration_min" not in df.columns:
            df["completed_on"] = pd.to_datetime(df["completed_on"], utc=True)
        df["observed_trip_duration_seconds"] = (df["completed_on"] - df["started_on"]).dt.total_seconds()
        out_cols.append("observed_trip_duration_seconds")
    if "distance_travelled" in df.columns:
        # distance_travelled is in meters — convert to miles for validation
        df["observed_distance_miles"] = df["distance_travelled"] / 1609.344
        out_cols.append("observed_distance_miles")

    out = df[out_cols].sort_values("request_time_seconds").reset_index(drop=True)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OUTPUT_PATH, index=False)
    print(f"\nWrote {len(out):,} requests to {OUTPUT_PATH}")
    print(f"Time span: 0 – 24 hours (collapsed synthetic day)")
    print(f"Peak hour: {int(out['request_time_seconds'].apply(lambda s: int(s)//3600).mode()[0]):02d}:00")


if __name__ == "__main__":
    main()
