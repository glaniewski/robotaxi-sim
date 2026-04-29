"""
Experiment 10b — Extended fleet sweep (500–1000) with fixed costs.
Only runs NEW fleet sizes 500–1000 (100–450 results are already in RESULTS.md Exp 10).
Applies three fixed-cost scenarios analytically — no re-sim needed per cost level.
  A) $27.40/vehicle/day  — pure 3-yr depreciation on $30k vehicle
  B) $56.00/vehicle/day  — comprehensive (depreciation + insurance + maintenance + remote ops)
  C) $100.00/vehicle/day — conservative/premium AV cost assumption
"""
from __future__ import annotations
import json, threading, time, urllib.request, urllib.error

API = "http://localhost:8000"


def _run_with_ticker(label: str, payload: dict, timeout: int = 900) -> dict:
    data = json.dumps(payload).encode()
    req = urllib.request.Request(f"{API}/run", data=data,
                                 headers={"Content-Type": "application/json"}, method="POST")
    t0 = time.time()
    done = threading.Event()
    def _ticker():
        spin = ["⠋","⠙","⠹","⠸","⠼","⠴","⠦","⠧","⠇","⠏"]
        i = 0
        while not done.is_set():
            elapsed = time.time() - t0
            m, s = divmod(int(elapsed), 60)
            print(f"\r  {spin[i%len(spin)]}  {label}  —  {m:02d}:{s:02d}", end="", flush=True)
            i += 1; time.sleep(0.1)
    t = threading.Thread(target=_ticker, daemon=True)
    t.start()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            result = json.loads(resp.read())
            done.set(); t.join()
            elapsed = time.time() - t0
            m, s = divmod(int(elapsed), 60)
            print(f"\r  ✓  {label}  —  {m:02d}:{s:02d}" + " "*20)
            return result.get("metrics", {})
    except Exception as e:
        done.set(); t.join()
        print(f"\r  ✗  {label}  —  {e}"); return {}

# Only run NEW fleet sizes not covered by Experiment 10
FLEET_SIZES = [500, 600, 700, 800, 900, 1000]
FIXED_COSTS = [27.40, 56.00, 100.00]

BASE = {
    "seed": 123,
    "duration_minutes": 1440,
    "demand": {"demand_scale": 0.02},
}


def run(scenario: dict, label: str) -> dict:
    return _run_with_ticker(label, scenario)


# Run one sim per fleet size at zero fixed cost (economics applied analytically below)
print("=== Running fleet sizes 500–1000 (no fixed cost base) ===")
sim_cache: dict[int, dict] = {}
for size in FLEET_SIZES:
    scenario = {**BASE, "fleet": {"size": size}, "economics": {"fixed_cost_per_vehicle_day": 0.0}}
    m = run(scenario, f"fleet={size}")
    if m:
        sim_cache[size] = m


def adjust_for_fixed_cost(m: dict, fleet_size: int, fixed_cost_per_day: float) -> dict:
    """Recompute economics metrics given a different fixed cost assumption."""
    if not m:
        return {}
    duration_days = 1.0  # 1440 min = 1 day
    fixed_total = fleet_size * fixed_cost_per_day * duration_days
    variable_total = m["revenue_total"] - m["total_margin"] - m.get("fixed_cost_total", 0.0)
    new_total_cost = variable_total + fixed_total
    new_revenue = m["revenue_total"]
    new_total_margin = new_revenue - new_total_cost
    new_cm_per_trip = new_total_margin / m["served_count"] if m["served_count"] > 0 else 0.0
    return {
        **m,
        "fixed_cost_total": round(fixed_total, 2),
        "total_margin": round(new_total_margin, 2),
        "contribution_margin_per_trip": round(new_cm_per_trip, 4),
        "cost_per_trip": round(new_total_cost / m["served_count"], 4) if m["served_count"] > 0 else 0.0,
    }


print("\n" + "=" * 70)
print("EXPERIMENT 10b — Fleet sweep extension (500–1000) with fixed costs")
print("=" * 70)

all_results: dict[float, dict[int, dict]] = {}
for fc in FIXED_COSTS:
    all_results[fc] = {}
    print(f"\n--- fixed_cost_per_vehicle_day = ${fc:.2f} ---")
    print(f"{'fleet':>6}  {'served':>7}  {'util':>6}  {'fixed_$':>9}  {'margin':>9}  {'cm/trip':>8}  {'repo':>5}")
    for size in FLEET_SIZES:
        m_base = sim_cache.get(size, {})
        m = adjust_for_fixed_cost(m_base, size, fc)
        all_results[fc][size] = m
        if m:
            print(
                f"  {size:4d}  {m['served_pct']:6.1f}%  {m['utilization_pct']:5.1f}%"
                f"  ${m['fixed_cost_total']:8,.0f}  ${m['total_margin']:8,.0f}"
                f"  ${m['contribution_margin_per_trip']:7.2f}  {m['repositioning_pct']:4.1f}%"
            )


# Find efficient frontier for each FC level
print("\n=== Efficient frontier (peak total_margin) ===")
for fc in FIXED_COSTS:
    best_size = max(
        (s for s in FLEET_SIZES if all_results[fc].get(s)),
        key=lambda s: all_results[fc][s].get("total_margin", float("-inf")),
    )
    best_m = all_results[fc][best_size]
    print(
        f"  FC=${fc:.0f}/veh/day → peak at fleet={best_size}"
        f"  total_margin=${best_m['total_margin']:,.0f}"
        f"  served={best_m['served_pct']:.1f}%"
    )


# Write RESULTS.md section
def md_row(cols: list) -> str:
    return "| " + " | ".join(str(c) for c in cols) + " |"


lines: list[str] = [
    "",
    "---",
    "",
    "## Experiment 10b — Fleet Sweep Extension (500–1000 vehicles)",
    "",
    "**Setup:** demand_scale=0.02, full 24h day, seed=123, fleet 500–1000. Three fixed-cost scenarios:",
    "- A: $27.40/veh/day = pure depreciation ($30k vehicle, 3-year straight-line)",
    "- B: $56.00/veh/day = comprehensive (depreciation + insurance + maintenance + remote ops)",
    "- C: $100.00/veh/day = conservative/premium AV cost assumption",
    "",
    "Variable cost unchanged at $0.50/mile. Revenue model unchanged.",
    "",
]

for fc in FIXED_COSTS:
    label = {27.40: "A: $27.40/veh/day (depreciation only)", 56.0: "B: $56/veh/day (comprehensive)", 100.0: "C: $100/veh/day (premium AV)"}[fc]
    lines.append(f"### Scenario {label}")
    lines.append("")
    lines.append(md_row(["fleet", "served_pct", "util_pct", "fixed_cost", "total_margin", "cm/trip", "repo_pct"]))
    lines.append(md_row(["---"] * 7))
    for size in FLEET_SIZES:
        m = all_results[fc].get(size, {})
        if m:
            lines.append(md_row([
                size,
                f"{m['served_pct']:.1f}%",
                f"{m['utilization_pct']:.1f}%",
                f"${m['fixed_cost_total']:,.0f}",
                f"${m['total_margin']:,.0f}",
                f"${m['contribution_margin_per_trip']:.2f}",
                f"{m['repositioning_pct']:.1f}%",
            ]))
    lines.append("")

lines += [
    "**Key findings:** see summary printed to stdout above.",
    "",
]

results_path = "/Users/lanie/Desktop/robotaxi-sim/RESULTS.md"
with open(results_path, "a") as f:
    f.write("\n".join(lines) + "\n")

print(f"\nResults appended to {results_path}")
