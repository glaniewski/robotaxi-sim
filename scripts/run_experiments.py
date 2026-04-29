"""
Run fleet sweep and SLA sensitivity experiments against the live API.
Prints a compact results table and appends findings to RESULTS.md.
"""
from __future__ import annotations
import json
import sys
import time
import urllib.request
import urllib.error

API = "http://localhost:8000"
# Base fields sent directly as ScenarioConfig (no {"scenario": ...} wrapper)
BASE_SCENARIO = {
    "seed": 123,
    "duration_minutes": 1440,
    "demand": {"demand_scale": 0.02},
}


def run(scenario_override: dict, label: str) -> dict:
    # /run accepts ScenarioConfig directly — NO wrapper key
    payload = json.dumps(scenario_override).encode()
    req = urllib.request.Request(
        f"{API}/run",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    t0 = time.time()
    print(f"  → {label} ...", end="", flush=True)
    try:
        with urllib.request.urlopen(req, timeout=600) as resp:
            data = json.loads(resp.read())
            elapsed = time.time() - t0
            print(f" done in {elapsed:.0f}s")
            return data["metrics"]
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f" FAILED {e.code}: {body[:200]}")
        return {}
    except Exception as e:
        print(f" ERROR: {e}")
        return {}


def fmt(m: dict) -> str:
    if not m:
        return "ERROR"
    return (
        f"served={m['served_pct']:.1f}%  sla={m['sla_adherence_pct']:.1f}%  "
        f"wait_p50={m['median_wait_min']:.1f}m  wait_p90={m['p90_wait_min']:.1f}m  "
        f"util={m['utilization_pct']:.1f}%  "
        f"cm/trip=${m['contribution_margin_per_trip']:.2f}  "
        f"total_margin=${m['total_margin']:.0f}  "
        f"avg_rev=${m['avg_revenue_per_trip']:.2f}  "
        f"repo={m['repositioning_pct']:.1f}%"
    )


# ── Fleet sweep ────────────────────────────────────────────────────────────────

print("\n" + "=" * 70)
print("EXPERIMENT A — Fleet sizing sweep (demand_scale=0.02, full day)")
print("=" * 70)

fleet_results: list[tuple[int, dict]] = []
for size in [100, 150, 200, 250, 300, 350, 400]:
    scenario = {**BASE_SCENARIO, "fleet": {"size": size}}
    label = f"fleet={size}"
    m = run(scenario, label)
    fleet_results.append((size, m))
    print(f"    {label:12s}  {fmt(m)}")

# ── SLA sensitivity ────────────────────────────────────────────────────────────

print("\n" + "=" * 70)
print("EXPERIMENT B — SLA sensitivity (fleet=200, demand_scale=0.02)")
print("=" * 70)

sla_results: list[tuple[int, dict]] = []
for max_wait in [420, 600, 900]:
    label_min = max_wait // 60
    scenario = {
        **BASE_SCENARIO,
        "fleet": {"size": 200},
        "demand": {"demand_scale": 0.02, "max_wait_time_seconds": float(max_wait)},
    }
    label = f"max_wait={label_min}min ({max_wait}s)"
    m = run(scenario, label)
    sla_results.append((max_wait, m))
    print(f"    {label:28s}  {fmt(m)}")

# ── Markdown summary ───────────────────────────────────────────────────────────

def md_row(cols: list) -> str:
    return "| " + " | ".join(str(c) for c in cols) + " |"


lines: list[str] = [
    "",
    "---",
    "",
    "## Experiment 8 — Fleet Sizing Sweep",
    "",
    "**Setup:** demand_scale=0.02, full 24h day, seed=123, all other params default.",
    "**Question:** Which fleet size maximises `total_margin`?",
    "",
    md_row(["fleet", "served_pct", "sla_pct", "wait_p50", "wait_p90",
            "util_pct", "cm/trip", "total_margin", "avg_rev", "repo_pct"]),
    md_row(["---"] * 10),
]
for size, m in fleet_results:
    if m:
        lines.append(md_row([
            size,
            f"{m['served_pct']:.1f}%",
            f"{m['sla_adherence_pct']:.1f}%",
            f"{m['median_wait_min']:.1f}m",
            f"{m['p90_wait_min']:.1f}m",
            f"{m['utilization_pct']:.1f}%",
            f"${m['contribution_margin_per_trip']:.2f}",
            f"${m['total_margin']:.0f}",
            f"${m['avg_revenue_per_trip']:.2f}",
            f"{m['repositioning_pct']:.1f}%",
        ]))

lines += [
    "",
    "**Key findings:** (fill in after reviewing table)",
    "",
    "---",
    "",
    "## Experiment 9 — SLA Sensitivity",
    "",
    "**Setup:** fleet=200, demand_scale=0.02, seed=123, vary max_wait_time_seconds.",
    "**Question:** Does relaxing or tightening the SLA improve total margin?",
    "",
    md_row(["max_wait", "served_pct", "sla_pct", "wait_p50", "wait_p90",
            "util_pct", "cm/trip", "total_margin", "avg_rev"]),
    md_row(["---"] * 9),
]
for max_wait, m in sla_results:
    if m:
        lines.append(md_row([
            f"{max_wait//60}min ({max_wait}s)",
            f"{m['served_pct']:.1f}%",
            f"{m['sla_adherence_pct']:.1f}%",
            f"{m['median_wait_min']:.1f}m",
            f"{m['p90_wait_min']:.1f}m",
            f"{m['utilization_pct']:.1f}%",
            f"${m['contribution_margin_per_trip']:.2f}",
            f"${m['total_margin']:.0f}",
            f"${m['avg_revenue_per_trip']:.2f}",
        ]))

lines.append("")
lines.append("**Key findings:** (fill in after reviewing table)")
lines.append("")

results_path = "/Users/lanie/Desktop/robotaxi-sim/RESULTS.md"
with open(results_path, "a") as f:
    f.write("\n".join(lines) + "\n")

print(f"\nResults appended to {results_path}")
