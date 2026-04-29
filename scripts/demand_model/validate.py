"""
Validation script for generated demand data.

Produces sanity-check plots:
  1. Spatial heatmap of trip origins (should show population-density hotspots)
  2. Temporal profile (trips/hour — should show AM/PM peaks on weekdays)
  3. Trip distance distribution (should be log-normal-ish)
  4. AM vs PM flow asymmetry (commute direction reversal)
  5. OD matrix sparsity
  6. Transit suppression effect

Usage:
    python -m demand_model.validate [path_to_parquet] [--output-dir plots/]
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    HAS_MPL = True
except ImportError:
    HAS_MPL = False


def _load(path: str | Path) -> pd.DataFrame:
    return pd.read_parquet(
        path, columns=["request_time_seconds", "origin_h3", "destination_h3"]
    )


def plot_temporal_profile(df: pd.DataFrame, output_dir: Path) -> None:
    """Trips per hour histogram."""
    df = df.copy()
    df["hour"] = (df["request_time_seconds"] / 3600).astype(int) % 24
    hourly = df.groupby("hour").size()

    fig, ax = plt.subplots(figsize=(10, 4))
    ax.bar(hourly.index, hourly.values, width=0.8, alpha=0.7, color="steelblue")
    ax.set_xlabel("Hour of Day")
    ax.set_ylabel("Trip Count")
    ax.set_title("Temporal Profile — Trips per Hour")
    ax.set_xticks(range(24))
    fig.tight_layout()
    fig.savefig(output_dir / "temporal_profile.png", dpi=150)
    plt.close(fig)
    logger.info("Saved temporal_profile.png")


def plot_trip_distance_distribution(df: pd.DataFrame, output_dir: Path) -> None:
    """Distribution of H3 grid distances between origin and destination."""
    import h3

    sample = df.sample(n=min(10_000, len(df)), random_state=42)
    distances = []
    for _, row in sample.iterrows():
        try:
            d = h3.grid_distance(row["origin_h3"], row["destination_h3"])
            distances.append(d)
        except Exception:
            pass

    if not distances:
        logger.warning("No valid distances computed — skipping distance plot")
        return

    fig, ax = plt.subplots(figsize=(8, 4))
    ax.hist(distances, bins=50, alpha=0.7, color="coral", edgecolor="white")
    ax.set_xlabel("H3 Grid Distance (cells)")
    ax.set_ylabel("Count")
    ax.set_title(f"Trip Distance Distribution (n={len(distances)} sample)")
    ax.axvline(np.median(distances), color="red", linestyle="--", label=f"Median={np.median(distances):.0f}")
    ax.legend()
    fig.tight_layout()
    fig.savefig(output_dir / "distance_distribution.png", dpi=150)
    plt.close(fig)
    logger.info("Saved distance_distribution.png")


def plot_am_pm_asymmetry(df: pd.DataFrame, output_dir: Path) -> None:
    """Compare top origin cells in AM vs PM to show commute reversal."""
    df = df.copy()
    df["hour"] = (df["request_time_seconds"] / 3600).astype(int) % 24

    am = df[(df["hour"] >= 7) & (df["hour"] < 10)]
    pm = df[(df["hour"] >= 16) & (df["hour"] < 19)]

    am_origins = am["origin_h3"].value_counts().head(15)
    pm_origins = pm["origin_h3"].value_counts().head(15)

    am_dests = am["destination_h3"].value_counts().head(15)
    pm_dests = pm["destination_h3"].value_counts().head(15)

    fig, axes = plt.subplots(2, 2, figsize=(14, 10))

    for ax, data, title in [
        (axes[0, 0], am_origins, "AM Origins (7-10)"),
        (axes[0, 1], am_dests, "AM Destinations (7-10)"),
        (axes[1, 0], pm_origins, "PM Origins (16-19)"),
        (axes[1, 1], pm_dests, "PM Destinations (16-19)"),
    ]:
        labels = [c[-6:] for c in data.index]
        ax.barh(range(len(data)), data.values, alpha=0.7)
        ax.set_yticks(range(len(data)))
        ax.set_yticklabels(labels, fontsize=8)
        ax.set_title(title)
        ax.invert_yaxis()

    fig.suptitle("AM/PM Flow Asymmetry — Commute Reversal Check", fontsize=13)
    fig.tight_layout()
    fig.savefig(output_dir / "am_pm_asymmetry.png", dpi=150)
    plt.close(fig)
    logger.info("Saved am_pm_asymmetry.png")


def plot_od_sparsity(df: pd.DataFrame, output_dir: Path) -> None:
    """Show OD pair frequency distribution (most pairs should have very few trips)."""
    od_counts = df.groupby(["origin_h3", "destination_h3"]).size()

    fig, ax = plt.subplots(figsize=(8, 4))
    ax.hist(od_counts.values, bins=100, alpha=0.7, color="mediumpurple", edgecolor="white", log=True)
    ax.set_xlabel("Trips per OD Pair")
    ax.set_ylabel("Number of OD Pairs (log scale)")
    ax.set_title(f"OD Matrix Sparsity — {len(od_counts)} unique pairs, {len(df)} total trips")
    fig.tight_layout()
    fig.savefig(output_dir / "od_sparsity.png", dpi=150)
    plt.close(fig)
    logger.info("Saved od_sparsity.png")


def plot_spatial_heatmap(df: pd.DataFrame, output_dir: Path) -> None:
    """Top origin cells by trip count (bar chart since we don't have a map renderer)."""
    top_origins = df["origin_h3"].value_counts().head(30)

    fig, ax = plt.subplots(figsize=(12, 6))
    labels = [c[-8:] for c in top_origins.index]
    ax.bar(range(len(top_origins)), top_origins.values, alpha=0.7, color="teal")
    ax.set_xticks(range(len(top_origins)))
    ax.set_xticklabels(labels, rotation=45, ha="right", fontsize=7)
    ax.set_ylabel("Trip Count")
    ax.set_title("Top 30 Origin Cells by Trip Volume")
    fig.tight_layout()
    fig.savefig(output_dir / "spatial_origins.png", dpi=150)
    plt.close(fig)
    logger.info("Saved spatial_origins.png")


def print_summary(df: pd.DataFrame) -> None:
    """Print text summary statistics."""
    n_trips = len(df)
    n_origins = df["origin_h3"].nunique()
    n_dests = df["destination_h3"].nunique()
    n_od_pairs = df.groupby(["origin_h3", "destination_h3"]).ngroups
    duration_h = df["request_time_seconds"].max() / 3600

    print(f"\n{'='*50}")
    print(f"  Demand Validation Summary")
    print(f"{'='*50}")
    print(f"  Total trips:       {n_trips:>10,}")
    print(f"  Unique origins:    {n_origins:>10,}")
    print(f"  Unique dests:      {n_dests:>10,}")
    print(f"  Unique OD pairs:   {n_od_pairs:>10,}")
    print(f"  Duration:          {duration_h:>10.1f} hours")
    print(f"  Avg trips/hour:    {n_trips / max(duration_h, 1):>10.0f}")

    # Hourly breakdown
    df_c = df.copy()
    df_c["hour"] = (df_c["request_time_seconds"] / 3600).astype(int) % 24
    hourly = df_c.groupby("hour").size()
    peak_hour = hourly.idxmax()
    min_hour = hourly.idxmin()
    print(f"  Peak hour:         {peak_hour:>10d} ({hourly[peak_hour]:,} trips)")
    print(f"  Min hour:          {min_hour:>10d} ({hourly[min_hour]:,} trips)")
    print(f"  Peak/min ratio:    {hourly[peak_hour] / max(hourly[min_hour], 1):>10.1f}x")
    print(f"{'='*50}\n")


def validate(parquet_path: str | Path, output_dir: str | Path = "plots/demand_validation") -> None:
    """Run all validation checks and generate plots."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Loading %s ...", parquet_path)
    df = _load(parquet_path)

    if df.empty:
        logger.error("Parquet is empty — nothing to validate")
        return

    print_summary(df)

    if not HAS_MPL:
        logger.warning("matplotlib not available — skipping plots")
        return

    plot_temporal_profile(df, output_dir)
    plot_spatial_heatmap(df, output_dir)
    plot_trip_distance_distribution(df, output_dir)
    plot_am_pm_asymmetry(df, output_dir)
    plot_od_sparsity(df, output_dir)

    logger.info("All validation plots saved to %s", output_dir)


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate generated demand parquet")
    parser.add_argument(
        "parquet", nargs="?", default="data/synthetic_demand.parquet",
        help="Path to demand parquet file",
    )
    parser.add_argument("--output-dir", default="plots/demand_validation", help="Directory for plots")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    validate(args.parquet, args.output_dir)


if __name__ == "__main__":
    main()
