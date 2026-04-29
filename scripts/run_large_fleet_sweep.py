"""
Experiment 11 — Large fleet sweep: 1000 / 2000 / 3000 vehicles, three fixed-cost scenarios.
Runs one sim per fleet size (no fixed cost base), then applies A/B/C analytically.
"""
from __future__ import annotations
import json, time, urllib.request, urllib.error

API = "http://localhost:8000"


def _run_with_ticker(label: str, payload: dict, timeout: int = 900) -> dict:
    import threading
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


FLEET_SIZES = [1000, 2000, 3000]
FIXED_COSTS = [27.40, 56.00, 100.00]
BASE = {"seed": 123, "duration_minutes": 1440, "demand": {"demand_scale": 0.02}}


def run_sim(fleet: int) -> dict:
    return _run_with_ticker(
        label=f"fleet={fleet}",
        payload={**BASE, "fleet": {"size": fleet}, "economics": {"fixed_cost_per_vehicle_day": 0.0}},
    )


def apply_fc(m: dict, fleet: int, fc: float) -> dict:
    if not m: return {}
    fixed = fleet * fc
    variable = m["revenue_total"] - m["total_margin"] - m.get("fixed_cost_total", 0.0)
    total_cost = variable + fixed
    margin = m["revenue_total"] - total_cost
    served = m["served_count"] or 1
    return {**m, "fixed_cost_total": round(fixed, 2),
            "total_margin": round(margin, 2),
            "contribution_margin_per_trip": round(margin / served, 4)}


print("=== Running base sims (no fixed cost) ===")
cache = {s: run_sim(s) for s in FLEET_SIZES}

FC_LABELS = {27.40: "A: $27.40/veh/day", 56.00: "B: $56/veh/day", 100.00: "C: $100/veh/day"}

print("\n" + "="*65)
print("EXPERIMENT 11 — Large fleet (1000 / 2000 / 3000) with fixed costs")
print("="*65)
for fc in FIXED_COSTS:
    print(f"\n--- {FC_LABELS[fc]} ---")
    print(f"{'fleet':>6}  {'served':>7}  {'util':>6}  {'fixed_$':>10}  {'margin':>10}  {'cm/trip':>8}  {'repo':>6}")
    for s in FLEET_SIZES:
        m = apply_fc(cache.get(s, {}), s, fc)
        if m:
            print(f"  {s:4d}  {m['served_pct']:6.1f}%  {m['utilization_pct']:5.1f}%"
                  f"  ${m['fixed_cost_total']:9,.0f}  ${m['total_margin']:9,.0f}"
                  f"  ${m['contribution_margin_per_trip']:7.2f}  {m['repositioning_pct']:5.1f}%")

# ── Append to RESULTS.md ──────────────────────────────────────────────────────
def row(cols): return "| " + " | ".join(str(c) for c in cols) + " |"

lines = ["", "---", "",
         "## Experiment 11 — Large Fleet Sweep (1000 / 2000 / 3000 vehicles)", "",
         "**Setup:** demand_scale=0.02, full 24h day, seed=123.",
         "Same three fixed-cost scenarios as Exp 10/10b. Variable cost $0.50/mile.", ""]

for fc in FIXED_COSTS:
    lines.append(f"### Scenario {FC_LABELS[fc]}")
    lines.append("")
    lines.append(row(["fleet", "served_pct", "util_pct", "fixed_cost", "total_margin", "cm/trip", "repo_pct"]))
    lines.append(row(["---"]*7))
    for s in FLEET_SIZES:
        m = apply_fc(cache.get(s, {}), s, fc)
        if m:
            lines.append(row([s, f"{m['served_pct']:.1f}%", f"{m['utilization_pct']:.1f}%",
                               f"${m['fixed_cost_total']:,.0f}", f"${m['total_margin']:,.0f}",
                               f"${m['contribution_margin_per_trip']:.2f}",
                               f"{m['repositioning_pct']:.1f}%"]))
    lines.append("")

with open("/Users/lanie/Desktop/robotaxi-sim/RESULTS.md", "a") as f:
    f.write("\n".join(lines) + "\n")
print("\nAppended to RESULTS.md")
