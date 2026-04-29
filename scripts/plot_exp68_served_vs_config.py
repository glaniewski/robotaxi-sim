"""
Experiment 68 — bar comparison of served% and charger utilization by config.

Data: RESULTS.md § Experiment 68 table (demand_scale=0.2, fleet=4000, 3-day continuous).
Update ROWS below if the table changes.

Run: python3 scripts/plot_exp68_served_vs_config.py
"""
from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

ROOT = Path(__file__).resolve().parent.parent

# (label, served_pct, charger_util_pct, site_kw) — keep in sync with RESULTS.md Exp68
ROWS: list[tuple[str, float, float, float]] = [
    ("N=231 (3×sites), 4p×20", 94.62, 39.7, 80.0),
    ("20p×20 kW (N=77)", 94.23, 25.2, 400.0),
    ("16p×20 kW (N=77)", 93.92, 31.4, 320.0),
    ("12p×20 kW (N=77)", 93.38, 41.4, 240.0),
    ("4p×60 kW (N=77)", 91.85, 36.2, 240.0),
    ("4p×40 kW (N=77)", 91.11, 56.3, 160.0),
    ("N=154 (2×sites), 4p×20", 90.39, 57.2, 80.0),
    ("4p×80 kW (N=77)", 90.26, 25.9, 320.0),
    ("8p×20 kW (N=77)", 90.15, 59.3, 160.0),
    ("4p×20 (N=77, Exp67 ref)", 76.20, 93.1, 80.0),
]


def main() -> None:
    labels = [r[0] for r in ROWS]
    served = np.array([r[1] for r in ROWS])
    chg_u = np.array([r[2] for r in ROWS])
    site_kw = np.array([r[3] for r in ROWS])

    y = np.arange(len(labels))
    h = 0.36

    fig, (ax_l, ax_r) = plt.subplots(
        1,
        2,
        figsize=(14, 9),
        sharey=True,
        gridspec_kw={"width_ratios": [1.15, 0.85], "wspace": 0.06},
    )

    colors = ["#2e7d32" if "N=231" in lb or "N=154" in lb else "#1565c0" for lb in labels]
    colors[-1] = "#c62828"  # baseline ref

    ax_l.barh(y + h / 2, served, height=h, color=colors, edgecolor="white", linewidth=0.6, alpha=0.9)
    ax_l.set_xlabel("Served %", fontsize=11)
    ax_l.set_title("Exp68 — served%", fontsize=12, fontweight="bold")
    ax_l.set_xlim(70, 98)
    ax_l.xaxis.set_major_locator(mticker.MultipleLocator(2))
    ax_l.grid(axis="x", alpha=0.35)
    for yi, s, sk in zip(y + h / 2, served, site_kw):
        ax_l.text(s + 0.25, yi, f"{s:.2f} ({sk:.0f} kW/site)", va="center", fontsize=7, color="#333")

    ax_r.barh(y + h / 2, chg_u, height=h, color="#78909c", edgecolor="white", linewidth=0.6, alpha=0.9)
    ax_r.set_xlabel("Charger util %", fontsize=11)
    ax_r.set_title("Exp68 — charger util", fontsize=12, fontweight="bold")
    ax_r.set_xlim(0, 100)
    ax_r.xaxis.set_major_locator(mticker.MultipleLocator(10))
    ax_r.grid(axis="x", alpha=0.35)
    for yi, u in zip(y + h / 2, chg_u):
        ax_r.text(u + 1.0, yi, f"{u:.1f}", va="center", fontsize=8, color="#333")

    ax_l.set_yticks(y + h / 2)
    ax_l.set_yticklabels(labels, fontsize=9)
    ax_l.invert_yaxis()

    fig.suptitle(
        "Experiment 68 — demand_scale=0.2, fleet=4000, 3-day continuous\n"
        "site_power_kw = plugs × charger_kw (labels show site kW in annotations)",
        fontsize=11,
        y=1.02,
    )
    fig.text(
        0.5,
        -0.02,
        "Green = multi-site (N=154 / N=231); blue = N=77 plug/kW sweep; red = Exp67 N=77 4p×20 baseline.",
        ha="center",
        fontsize=8,
        style="italic",
        color="#555",
    )

    out = ROOT / "plots" / "exp68_served_pct_vs_config.png"
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"Saved → {out}")


if __name__ == "__main__":
    main()
