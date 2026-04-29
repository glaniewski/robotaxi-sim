"""
Plot the cost-vs-service Pareto frontier from data/blog_pareto/.

X axis: total_system_cost_per_trip (USD)
Y axis: sla_adherence_pct  (% served within 10-min max wait)
Markers: Tesla (blue circles), Waymo (orange squares)
Frontier: non-dominated points connected by a dashed line.

Output:
  - site/public/plots/pareto_frontier.png   (web)
  - site/public/plots/pareto_frontier.svg   (vector fallback)

Run: python3.11 scripts/plot_pareto_frontier.py
"""
from __future__ import annotations

import json
from pathlib import Path

import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parent.parent
PARETO_DIR = ROOT / "data" / "blog_pareto"
OUT_DIR = ROOT / "site" / "public" / "plots"


def _load_pareto_cells() -> list[dict]:
    cells: list[dict] = []
    for p in sorted(PARETO_DIR.glob("*.json")):
        if p.name == "index.json":
            continue
        try:
            d = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        ax = d["metadata"]["sweep_axis"]
        m = d["metrics"]
        cells.append({
            "label": d["corner_label"],
            "preset": d["preset"],
            "n_sites": ax["n_sites"],
            "plugs_per_site": ax["plugs_per_site"],
            "charger_kw": ax["charger_kw"],
            "fleet_size": ax["fleet_size"],
            "battery_kwh": ax["battery_kwh"],
            "cost_per_trip": float(m["cost_per_trip"]),
            "sla_adherence_pct": float(m["sla_adherence_pct"]),
            "served_pct": float(m["served_pct"]),
            "contribution_margin_per_trip": float(m["contribution_margin_per_trip"]),
        })
    return cells


def _frontier(cells: list[dict]) -> list[dict]:
    """Non-dominated set: for each point, no other point has lower cost AND higher sla%."""
    out = []
    for c in cells:
        dominated = False
        for other in cells:
            if other is c:
                continue
            if (other["cost_per_trip"] <= c["cost_per_trip"]
                and other["sla_adherence_pct"] >= c["sla_adherence_pct"]
                and (other["cost_per_trip"] < c["cost_per_trip"]
                     or other["sla_adherence_pct"] > c["sla_adherence_pct"])):
                dominated = True
                break
        if not dominated:
            out.append(c)
    return sorted(out, key=lambda r: r["cost_per_trip"])


def main() -> None:
    cells = _load_pareto_cells()
    if not cells:
        print(f"[plot_pareto_frontier] no cells in {PARETO_DIR}; skipping")
        return

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # Split by preset
    tesla = [c for c in cells if c["preset"] == "tesla"]
    waymo = [c for c in cells if c["preset"] == "waymo"]
    front = _frontier(cells)

    fig, ax = plt.subplots(figsize=(9, 6.2), dpi=150)
    ax.scatter(
        [c["cost_per_trip"] for c in tesla],
        [c["sla_adherence_pct"] for c in tesla],
        s=80, c="#3b82f6", marker="o", label=f"Tesla-preset ({len(tesla)} corners)",
        edgecolors="white", linewidths=0.8, zorder=3,
    )
    ax.scatter(
        [c["cost_per_trip"] for c in waymo],
        [c["sla_adherence_pct"] for c in waymo],
        s=80, c="#f97316", marker="s", label=f"Waymo-preset ({len(waymo)} corners)",
        edgecolors="white", linewidths=0.8, zorder=3,
    )
    if len(front) >= 2:
        ax.plot(
            [c["cost_per_trip"] for c in front],
            [c["sla_adherence_pct"] for c in front],
            linestyle="--", color="#64748b", linewidth=1.8,
            label="Pareto frontier", zorder=2,
        )

    for c in front:
        n = c["n_sites"]
        kw = c["charger_kw"]
        label = f"N={n}, {kw:g} kW"
        ax.annotate(
            label, (c["cost_per_trip"], c["sla_adherence_pct"]),
            xytext=(6, 4), textcoords="offset points",
            fontsize=8, color="#334155",
        )

    ax.set_xlabel("Total system cost per trip  (USD)")
    ax.set_ylabel("SLA adherence  (% of requests served within 10 min)")
    ax.set_title("Cost vs service Pareto frontier\n"
                 "Each marker = one 3-day sim at demand_scale=0.2; frontier = non-dominated corners")
    ax.grid(alpha=0.25, zorder=0)
    ax.legend(loc="lower right", framealpha=0.95)

    fig.tight_layout()
    png_path = OUT_DIR / "pareto_frontier.png"
    svg_path = OUT_DIR / "pareto_frontier.svg"
    fig.savefig(png_path)
    fig.savefig(svg_path)
    plt.close(fig)

    print(f"Wrote {png_path}")
    print(f"Wrote {svg_path}")
    print(f"  n_cells={len(cells)} (tesla={len(tesla)}, waymo={len(waymo)})")
    print(f"  n_frontier={len(front)}")
    for c in front:
        print(f"    {c['preset']:<6} {c['label']:<36} "
              f"cost={c['cost_per_trip']:.2f}  sla={c['sla_adherence_pct']:.2f}")


if __name__ == "__main__":
    main()
