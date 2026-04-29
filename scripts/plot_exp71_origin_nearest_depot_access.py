"""
Why **central** (top-origin) depots work — static **access** from trip starts.

For each Exp71 depot layout, compute **min drive time** from every demand **origin** cell
to **either** depot using the same **H3 travel cache** the simulator uses, then weight by
historical **origin trip counts** from ``requests_austin_h3_r8.parquet``.

This is **not** a full sim replay (no fleet, queues, or dispatch). It isolates the
**geographic alignment** story: central hubs sit where trips begin, so post-charge /
repositioning vehicles are **closer to the next pickup** in drive-time space.

Run from repo root:
    python3 scripts/plot_exp71_origin_nearest_depot_access.py

Output: ``plots/exp71_origin_nearest_depot_access.png``
"""
from __future__ import annotations

import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

import run_exp63_continuous_multiday_steady_state as e63  # noqa: E402
from industrial_high_od_cells import closest_centroid_pair_m, load_industrial_high_od_context  # noqa: E402

REQUESTS_PATH = ROOT / "data" / "requests_austin_h3_r8.parquet"
TRAVEL_CACHE = ROOT / "data" / "h3_travel_cache.parquet"
OUT_PNG = ROOT / "plots" / "exp71_origin_nearest_depot_access.png"

PERIPHERAL_EW = ("88489e3569fffff", "88489e341bfffff")


def _min_time_to_nearest_depot(
    cache_df: pd.DataFrame,
    depot_cells: tuple[str, ...],
) -> pd.Series:
    """origin_h3 -> min time_seconds to any depot (rows must exist in cache)."""
    dset = set(depot_cells)
    sub = cache_df[cache_df["destination_h3"].isin(dset)]
    return sub.groupby("origin_h3", sort=False)["time_seconds"].min()


def _weighted_cdf_minutes(t_sec: np.ndarray, w: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Return (minutes_sorted, cum_frac_weight) for ECDF."""
    t_min = t_sec / 60.0
    order = np.argsort(t_min)
    t_s = t_min[order]
    w_s = w[order]
    cum = np.cumsum(w_s) / np.sum(w_s)
    return t_s, cum


def _summarize(t_sec: np.ndarray, w: np.ndarray) -> dict[str, float]:
    tw = np.sum(t_sec * w) / np.sum(w)
    # weighted percentile via sort
    order = np.argsort(t_sec)
    ts = t_sec[order]
    ws = w[order]
    cw = np.cumsum(ws) / np.sum(ws)
    p90_idx = np.searchsorted(cw, 0.90)
    p90 = ts[min(p90_idx, len(ts) - 1)] / 60.0
    def frac_under(sec: float) -> float:
        return float(np.sum(w[t_sec <= sec]) / np.sum(w))

    return {
        "mean_min": tw / 60.0,
        "p90_min": p90,
        "frac_5m": frac_under(300),
        "frac_10m": frac_under(600),
        "frac_15m": frac_under(900),
    }


def main() -> None:
    print("Loading origin counts …")
    df_o = pd.read_parquet(REQUESTS_PATH, columns=["origin_h3"])
    vc = df_o["origin_h3"].value_counts()
    origins = vc.index.astype(str).tolist()
    weights = vc.values.astype(np.float64)

    print(f"Loading travel cache ({TRAVEL_CACHE.name}) …")
    cache_df = pd.read_parquet(
        TRAVEL_CACHE, columns=["origin_h3", "destination_h3", "time_seconds"]
    )

    ctx = load_industrial_high_od_context()
    (c1, c2), sep_m = closest_centroid_pair_m(ctx.high_industrial)
    layouts: list[tuple[str, tuple[str, ...], str]] = [
        ("Central (top-2 origins)", tuple(e63.top_demand_cells(2)), "#00ACC1"),
        ("Peripheral (OSM E/W)", PERIPHERAL_EW, "#FB8C00"),
        (f"Peripheral #2 (closest high-industrial, {sep_m:.0f} m)", (c1, c2), "#AB47BC"),
    ]

    fig, axes = plt.subplots(1, 2, figsize=(11.5, 4.8), constrained_layout=True)

    ax0 = axes[0]
    summary_rows: list[dict] = []

    for label, depots, color in layouts:
        mins = _min_time_to_nearest_depot(cache_df, depots)
        t_list: list[float] = []
        w_list: list[float] = []
        missing = 0
        for cell, w in zip(origins, weights):
            if cell not in mins.index:
                missing += int(w)
                continue
            t_list.append(float(mins.loc[cell]))
            w_list.append(float(w))
        if missing:
            print(f"  [{label}] origins with no cache row to any depot: {missing:,} trips skipped")
        t_sec = np.array(t_list, dtype=np.float64)
        w = np.array(w_list, dtype=np.float64)
        tx, cy = _weighted_cdf_minutes(t_sec, w)
        ax0.plot(tx, cy * 100.0, color=color, lw=2.2, label=label)

        s = _summarize(t_sec, w)
        s["label"] = label
        s["depots"] = ",".join(depots)
        summary_rows.append(s)

    ax0.set_xlabel("Drive time from trip origin to nearest depot (min)")
    ax0.set_ylabel("Share of trip starts (%) ≤ this time")
    ax0.set_xlim(0, min(ax0.get_xlim()[1], 35))
    ax0.set_ylim(0, 100)
    ax0.grid(True, alpha=0.35)
    ax0.legend(loc="lower right", fontsize=8)
    ax0.set_title("Demand-weighted ECDF (H3 cache, same as sim)")

    ax1 = axes[1]
    x = np.arange(len(summary_rows))
    w_m = 0.35
    ax1.bar(x - w_m / 2, [r["mean_min"] for r in summary_rows], w_m, label="Mean", color="#546E7A")
    ax1.bar(x + w_m / 2, [r["p90_min"] for r in summary_rows], w_m, label="p90 (trip-weighted)", color="#78909C")
    ax1.set_xticks(x)
    ax1.set_xticklabels(["Central", "Periph E/W", "Periph #2"], fontsize=10)
    ax1.set_ylabel("Minutes")
    ax1.set_title("Origin → nearest depot (trip-weighted)")
    ax1.legend()
    ax1.grid(True, axis="y", alpha=0.35)

    fig.suptitle(
        "Exp71 — Why central depots align with demand (origin access only, no fleet sim)",
        fontsize=12,
        fontweight="bold",
    )

    OUT_PNG.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUT_PNG, dpi=150)
    plt.close(fig)
    print(f"\nSaved → {OUT_PNG}")

    print("\nTrip-weighted summary (origins with cache path to ≥1 depot):")
    for r in summary_rows:
        print(
            f"  {r['label']}\n"
            f"    mean {r['mean_min']:.2f} min · p90 {r['p90_min']:.2f} min · "
            f"≤5m {100*r['frac_5m']:.1f}% · ≤10m {100*r['frac_10m']:.1f}% · ≤15m {100*r['frac_15m']:.1f}%"
        )


if __name__ == "__main__":
    main()
