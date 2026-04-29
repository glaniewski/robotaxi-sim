"""
LLM-powered experiment planner — fully adaptive (no backlog).

Two calls:
  1. plan_next_experiment()   — reads CHEATSHEET + recent tail of RESULTS.md, proposes one ExperimentPlan
  2. interpret_experiment()   — writes the RESULTS.md finding paragraph

Planning context truncates RESULTS.md to the most recent portion (see ``_load_context``)
so prompts stay within model context limits; the full file remains on disk for humans
and the dashboard parser.
"""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

from .llm import LLMClient, Usage
from .models import ExperimentPlan, RunResult, SimRun

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[2]
RESULTS_MD = ROOT / "RESULTS.md"
CHEATSHEET_MD = ROOT / "CHEATSHEET.md"


# ---------------------------------------------------------------------------
# Context helpers
# ---------------------------------------------------------------------------

def _load_context() -> str:
    """
    Planning context: CHEATSHEET + the **most recent** slice of RESULTS.md.

    Sending the entire RESULTS file can exceed provider context and yield empty
    completions. By default we keep the **tail** of the file (newest experiments
    are appended at the bottom) and align to the first ``## Experiment`` heading
    in that tail when possible.

    Env:
    - ``EXPERIMENTER_FULL_RESULTS=1`` — send full RESULTS (not recommended for huge files).
    - ``EXPERIMENTER_RESULTS_MAX_CHARS`` — max UTF-8 chars of RESULTS body (default 120000).
    """
    cheatsheet = CHEATSHEET_MD.read_text() if CHEATSHEET_MD.exists() else ""
    results_raw = RESULTS_MD.read_text() if RESULTS_MD.exists() else "(no results yet)"

    if os.environ.get("EXPERIMENTER_FULL_RESULTS", "").strip().lower() in ("1", "true", "yes"):
        results_text = results_raw
    elif not isinstance(results_raw, str):
        results_text = results_raw
    else:
        max_chars = int(os.environ.get("EXPERIMENTER_RESULTS_MAX_CHARS", "120000"))
        if len(results_raw) <= max_chars:
            results_text = results_raw
        else:
            logger.warning(
                "Truncating RESULTS.md for planner: %d → ~%d chars (tail = newest experiments)",
                len(results_raw),
                max_chars,
            )
            tail = results_raw[-max_chars:]
            jo = tail.find("## Experiment")
            if jo > 0:
                tail = tail[jo:]
            results_text = (
                "**Note:** Older experiments are omitted here to fit the model context; "
                "the full history is in `RESULTS.md`.\n\n"
                + tail
            )

    return f"""# CHEATSHEET — Simulation Parameters and Output Metrics

{cheatsheet}

---

# PRIOR EXPERIMENT RESULTS (most recent first in this excerpt)

{results_text}
"""




# ---------------------------------------------------------------------------
# JSON schema descriptions used in prompts
# ---------------------------------------------------------------------------

_RUN_SCHEMA = """\
{
  "run_id": "run_a",                    // short snake_case id, e.g. run_a, run_b
  "description": "...",                 // 1 sentence: what this run tests
  "n_sites": 2,                         // depot count (1-77)
  "num_days": 3,                        // FIXED at 3
  "demand_scale": 0.2,                  // fraction of full demand (0.02-1.0)
  "fleet_size": 4000,                   // vehicles (500-10000)
  "plugs_per_site": 50,                 // charging plugs per depot (4-100)
  "charger_kw": 75.0,                   // MUST be one of: 11.5, 75, 96, 150
  "battery_kwh": null,                  // null = default 75 kWh; float MUST be >0 (engine). Stress 5-25 kWh; typical 35-100+
  "depot_h3_cells": null,               // null = auto top_demand_cells(n_sites); or H3 list
  "min_plug_duration_minutes": null,    // null = default 10 min; or any float >= 0 (no max — models dwell floor)
  "charging_queue_policy": null,        // null = "fifo"; or "jit"
  "vehicle_preset": "tesla",            // "tesla" or "waymo"
  "reposition_alpha": null,             // null = 0.6; or 0.0-1.0
  "demand_config": null                 // null = use RideAustin parquet; or dict e.g. {"day_type": "saturday", "tourism_intensity": 3.0}. See Priority I for all fields.
}"""

_BATCH_SCHEMA = """\
{
  "run_ids": ["run_a", "run_b"]         // runs to execute in parallel (up to 4)
}"""

_PLAN_SCHEMA = f"""\
{{
  "hypothesis": "...",                  // 1-2 sentence testable hypothesis
  "rationale": "...",                   // why this is the highest-value next experiment
  "runs": [{_RUN_SCHEMA}],
  "batches": [{_BATCH_SCHEMA}],        // sequential; runs within a batch are parallel
  "decision_tree": "..."               // what to investigate next based on outcomes
}}"""

_CANCEL_EVAL_SCHEMA = """\
{
  "decisions": [
    {"run_id": "run_b", "action": "CONTINUE", "reason": "..."},
    {"run_id": "run_c", "action": "CANCEL",   "reason": "..."}
  ]
}"""

_INTERPRET_SCHEMA = """\
{
  "finding": "...",                     // 2-4 sentence interpretation for RESULTS.md
  "outcome_one_line": "...",           // ≤15 words: confirmed/refuted + key number (for backlog)
  "key_metrics_table": [               // rows for the comparison table (include waits when available)
    {"label": "run_a — 308p×20kW", "served_pct": 92.7, "p10_wait_min": 1.2, "median_wait_min": 3.1, "p90_wait_min": 7.5}
  ],
  "next_priority": "..."               // most valuable follow-up experiment
}"""


# ---------------------------------------------------------------------------
# Planner calls
# ---------------------------------------------------------------------------

async def plan_next_experiment(
    client: LLMClient,
    exp_number: int,
    state_summary: str = "",
) -> tuple[ExperimentPlan, Usage]:
    """
    Ask the LLM to decide the single most valuable next experiment.
    Sees CHEATSHEET + recent RESULTS.md tail on every call (adaptive planning).
    """
    context = _load_context()

    system_msg = f"""{context}

---

# YOUR ROLE

You are an autonomous robotaxi simulation researcher mapping the **Pareto frontier**
of (total_system_cost_per_trip, served_pct) **and** rider wait quality
(`p10_wait_min`, `median_wait_min` = p50, `p90_wait_min`) for a robotaxi fleet in Austin, TX.

A config is Pareto-optimal if no other discovered config achieves strictly better outcomes
on cost, served%, and waits without trading off worse on another of these.

After each experiment, review ALL prior results and decide the single most valuable
next experiment — including **explicit follow-ups** when a **treatment beats the control**
per FOLLOW-UP PROTOCOL (favorable thresholds); otherwise **pivot** in `next_priority`.

## RESEARCH PRIORITIES (work down this list; rotate if a line is blocked by runtime)

### A — Runtime-safe high demand (avoid Exp143-style timeouts)
- For **`demand_scale >= 0.3`**: **`fleet_size <= 4000`** unless RESULTS.md shows a larger fleet finishing under the wall-time budget for that scale.
- Prefer **scale 0.25–0.35** with moderate fleet before the largest fleet×scale combos.

### B — Operational levers on a **known-good** anchor (e.g. N≈50, 10p×75kW, scale=0.2, fleet where you already get ~93–95% served)
- **`reposition_alpha`**: sweep **0.0, 0.3, 0.6, 1.0** (same infra); report deadhead%, repo%, cost/trip, waits.
- **`charging_queue_policy`**: **jit vs fifo** on the **same** config (paired arms).
- **`min_plug_duration_minutes`**: **0, 10, 30** and, when the hypothesis needs it, **60+** — there is **no planner cap** (only `>= 0`); long dwells model aggressive minimum session length.

### C — Charger power not yet well sampled on the frontier
- **`charger_kw` 96** and **150** vs **75** at matched plugs and `n_sites`.

### D — Closing the **~95% served** cliff
- Fine **fleet_size** steps (e.g. 5100–5600) and/or **`n_sites`** between prior sweet spots (e.g. 35–48 vs 50).

### E — **`battery_kwh`** (still useful at extremes; many mid-range runs already exist)
- Must be **`> 0`** (sim divides by pack size). **Tiny packs (5–15 kWh)** are valid *stress tests*; **25–100+ kWh** is the usual modeling band. Null = default 75.

### F — **Depot geography**
- **`depot_h3_cells`**: `null` (auto `top_demand_cells`) vs an alternate explicit list / second seed-draw narrative — test robustness of infra conclusions to site choice.

### G — **Preset symmetry**
- Run **both Tesla and Waymo** on configs where one preset shows a sharp tradeoff.

### H — **Frontier refinement**
- Map gaps between known Pareto points; one-factor-at-a-time sensitivity on **`total_system_cost_per_trip`**.

### I — **Demand scenario exploration** (synthetic demand model)
- Set `demand_config` to a **dict** to use the first-principles synthetic demand model (Census + POIs + GTFS + OSRM gravity model) instead of the RideAustin parquet.
- When `demand_config` is set, **set `demand_scale` to 1.0** — the synthetic model already generates the right volume via `demand_intensity`; subsampling would discard intentionally generated trips.
- Only include fields you want to change — omitted fields use defaults shown below.
- Example: `"demand_config": {"day_type": "saturday", "tourism_intensity": 3.0, "peak_sharpness": 1.5}`
- **Purpose**: Test whether fleet/infra conclusions from RideAustin hold under different spatial/temporal demand patterns. Compare same fleet config across weekday vs weekend, high vs low tourism, localized vs spread-out demand, etc.

**`demand_config` fields** (all optional):

| Field | Type | Range | Default | What it tests |
|-------|------|-------|---------|---------------|
| `demand_intensity` | float | 0.1–5.0 | 1.0 | Total trip volume (like demand_scale but from first principles) |
| `duration_hours` | int | 1–168 | 24 | Hours of demand (match num_days × 24 for multi-day) |
| `day_type` | enum | "weekday"/"saturday"/"sunday" | "weekday" | Temporal profile: commute peaks vs flat weekend |
| `peak_sharpness` | float | 0.1–3.0 | 1.0 | Sharper peaks = harder for fleet. >1 sharpens, <1 flattens |
| `beta` | float | 0.01–0.30 | 0.08 | Distance decay: high = local trips, low = cross-city trips |
| `commute_weight` | float | 0.0–1.0 | 0.40 | High = predictable tidal commute flows, low = random |
| `transit_suppression` | float | 0.0–1.0 | 0.3 | Good transit reduces robotaxi demand in those cells |
| `tourism_intensity` | float | 0.0–5.0 | 1.0 | Visitor multiplier (3.0+ = SXSW/ACL week) |
| `airport_boost` | float | 0.5–10.0 | 1.0 | Airport mode-share multiplier; 1.0 ≈ 6% of pax → ~13% of trips (tests airport depot value) |
| `entertainment_weight` | float | 0.0–5.0 | 1.5 | Nightlife/restaurant pull |
| `employment_weight` | float | 0.0–5.0 | 1.0 | Jobs pull (LODES employment data) |
| `medical_weight` | float | 0.0–3.0 | 0.8 | Hospital/clinic pull |
| `carfree_boost` | float | 1.0–5.0 | 2.0 | Car-free household trip multiplier |
| `seed` | int | any | 42 | RNG seed for Poisson sampling |

**Key experiments to try:**
- weekday vs saturday (same fleet → does served% change?)
- beta 0.04 vs 0.08 vs 0.15 (localized vs spread-out → repositioning impact?)
- tourism_intensity 1.0 vs 3.0 (baseline vs event week → fleet sizing?)
- peak_sharpness 0.5 vs 1.0 vs 2.0 (flat vs peaky → peak fleet utilization?)
- commute_weight 0.2 vs 0.7 (random vs predictable → reposition_alpha sensitivity?)

## FOLLOW-UP PROTOCOL (mandatory when a treatment **beats** control)
- Identify the **control arm** from the hypothesis / rationale (if not stated, default **`run_a`** as the reference baseline).
- A **treatment** arm triggers a mandatory follow-up in the **next** planned experiment **only** if it is **strictly more favorable** than control on at least one metric below (favorable = **higher** served%, **lower** p90 wait, **lower** cost/trip):
  - **`served_pct`** higher than control by **≥ 1.5 pp**, OR
  - **`p90_wait_min`** lower than control by **≥ 0.75 min**, OR
  - **`total_system_cost_per_trip`** lower than control by **≥ $0.08**.
- If **no** treatment beats control by these bars, **do not** force a same-lever follow-up solely from large *unfavorable* swings; instead use **`next_priority`** to pivot (different lever, wider diagnosis, or abandon that hypothesis branch).
- If results are **flat** (no treatment beats control and deltas stay small) across **2–3** consecutive experiments on the same lever, **rotate** to a different priority letter above.

## BATTERY COST FORMULA

Vehicle cost is now computed dynamically:
```
vehicle_cost_usd = base_vehicle_cost_usd + battery_kwh × battery_cost_per_kwh ($100/kWh)
```
- Tesla: base=$22,500 → at 75kWh=$30k, at 40kWh=$26.5k (saves $3,500/vehicle)
- Waymo: base=$72,500 → at 75kWh=$80k, at 40kWh=$76.5k (saves $3,500/vehicle)

Smaller battery = cheaper fleet_fixed_cost BUT less range = more charging trips.

## TUNABLE LEVERS (the ONLY parameters you may vary)

| Lever                 | Type  | Range / Options     | Cost impact                |
|-----------------------|-------|---------------------|----------------------------|
| fleet_size            | int   | 500–10,000          | Fleet fixed cost (major)   |
| n_sites               | int   | 2–77                | Infra site cost            |
| plugs_per_site        | int   | 4–100               | Infra plug cost + demand $ |
| charger_kw            | enum  | 11.5, 75, 96, 150   | Plug cost + demand charge  |
| demand_scale          | float | 0.02–1.0            | Trips generated (workload) |
| battery_kwh           | float | **> 0** (null=75); stress 5–25; typical 25–150 | Vehicle cost + range |
| charging_queue_policy | enum  | jit, fifo           | Charging efficiency        |
| min_plug_duration_min | float | **>= 0**, no upper cap | Charger dwell floor        |
| vehicle_preset        | enum  | tesla, waymo        | Vehicle cost + efficiency  |
| reposition_alpha      | float | 0.0–1.0             | Deadhead miles vs coverage |
| depot_h3_cells        | list or null | null = auto top cells | Spatial coverage / infra |
| demand_config         | dict or null | null = RideAustin; see Priority I for fields | Demand scenario (spatial/temporal shape) |

## FIXED PARAMETERS — do NOT change these

| Parameter             | Locked value | Why                                        |
|-----------------------|-------------|---------------------------------------------|
| num_days              | 3           | Day-over-day stability check (SOC init)     |
| seed                  | 42          | Deterministic trip draw for comparability   |
| soc_initial           | 0.80        | Must equal soc_target for honest steady-state|
| soc_target            | 0.80        | Defines "full charge" — coupled to soc_initial|
| soc_charge_start      | 0.80        | When to start charging — coupled to soc_target|
| soc_min               | 0.20        | Emergency floor — not an operational choice |
| max_wait_time_seconds | 600         | 10-min SLA is a business decision           |
| reposition_enabled    | true        | Always on in production                     |
| demand_flatten        | 0.0         | Use real demand patterns                    |
| pool_pct              | 0.0         | Pooling is a product decision, not infra    |

**STABILITY CHECK**: A configuration is only valid if day-3 served% is within 2pp
of day-1 served%. If not, flag it as unstable.

## EXPLORATION RULE

If your last 2–3 experiments on the same lever show <1% change in both cost and
served%, stop refining that lever and move to a different unexplored dimension.
Prioritize breadth over depth until all major levers have been tested at least once.

## RUNTIME BUDGET

Each run must complete in under 10 minutes. Runs projected beyond 20 minutes are
auto-killed.
- fleet_size × num_days is the primary cost driver. fleet=4000, days=3 ≈ 3 min.
- fleet=10000 or days=10 will likely exceed the limit.
- For high demand scales (≥0.3), use fleet ≤ 4000 (timeouts at 4500+×0.3 have already occurred).
- When reporting, cite **p10 / median (p50) / p90 wait** alongside served% and cost/trip.

## RULES
- Use 2–4 runs per experiment. All runs in a batch execute in parallel.
- num_days MUST be 3 for every run.
- charger_kw must be one of: 11.5, 75, 96, 150.
- Do not repeat parameter combinations already in RESULTS.md.
- Output ONLY the JSON — no markdown, no commentary.
"""

    user_msg = f"""Experimenter state: {state_summary or "starting fresh"}
Next experiment number: {exp_number}

Review ALL prior results above. Decide the single most valuable next experiment
for mapping the Pareto frontier (total_system_cost_per_trip vs served_pct).

Output ONLY a single JSON object:

{_PLAN_SCHEMA}

CRITICAL RULES:
- "runs" must be a JSON array of objects (not a dict, not a string)
- "batches" must be a JSON array of objects with "run_ids" key (not a list of lists)
- Every run_id in "batches" must match a run_id in "runs"
- charger_kw must be one of: 11.5, 75, 96, 150
- num_days must be 3
- vehicle_preset must be "tesla" or "waymo"
- Output ONLY the JSON object — no markdown fences, no extra text"""

    messages = [
        {"role": "system", "content": system_msg},
        {"role": "user", "content": user_msg},
    ]

    # cache_system=False: huge RESULTS tail + ephemeral cache has produced empty completions on OpenRouter/MiniMax
    data, usage = await client.complete_json(
        messages, max_tokens=8192, temperature=0.4, cache_system=False
    )
    try:
        plan = ExperimentPlan.from_dict(data, exp_number)
    except Exception as exc:
        logger.error("Failed to parse plan JSON: %s\nRaw JSON:\n%s", exc, json.dumps(data, indent=2))
        raise
    logger.info(
        "Planned Exp%d: %d runs, %d batches — %s",
        exp_number, len(plan.runs), len(plan.batches), plan.hypothesis[:80],
    )
    return plan, usage


async def evaluate_cancellations(
    client: LLMClient,
    completed: RunResult,
    completed_run_spec: SimRun,
    still_running: list[SimRun],
) -> tuple[list[str], Usage]:
    """
    After a run finishes, ask the LLM if any still-running runs should be cancelled.

    This is result-based cancellation only — the hard timeout is handled separately
    by the executor. Cancel here means "this run's result makes the other run
    pointless; we'd learn nothing new from letting it finish."

    Returns list of run_ids to cancel.
    """
    if not still_running:
        return [], Usage(model=client.model)

    running_desc = "\n".join(
        f"  - {r.run_id}: {r.description} "
        f"(n_sites={r.n_sites}, fleet={r.fleet_size}, "
        f"{r.plugs_per_site}p×{r.charger_kw}kW, scale={r.demand_scale}, days={r.num_days})"
        for r in still_running
    )

    clean_metrics = {
        k: round(v, 3) if isinstance(v, float) else v
        for k, v in completed.metrics.items()
        if not isinstance(v, list)
    }

    messages = [
        {
            "role": "system",
            "content": (
                "You are monitoring a robotaxi simulation experiment. "
                "A run just finished. Decide if any still-running runs should be cancelled. "
                "Cancel only if the finished result definitively answers what the running run was testing. "
                "Do NOT cancel just because results look bad — cancel only when continuing is pointless. "
                "Output JSON only."
            ),
        },
        {
            "role": "user",
            "content": f"""Just finished: {completed.run_id}
Description: {completed_run_spec.description}
Config: n_sites={completed_run_spec.n_sites}, fleet={completed_run_spec.fleet_size}, "
  {completed_run_spec.plugs_per_site}p×{completed_run_spec.charger_kw}kW, "
  scale={completed_run_spec.demand_scale}, days={completed_run_spec.num_days}

Result:
{json.dumps(clean_metrics, indent=2)}

Still running:
{running_desc}

For each still-running run: CONTINUE or CANCEL?
Output JSON:
{_CANCEL_EVAL_SCHEMA}""",
        },
    ]

    data, usage = await client.complete_json(
        messages, max_tokens=512, temperature=0.2, cache_system=False
    )
    cancel_ids = [
        d["run_id"] for d in data.get("decisions", []) if d.get("action") == "CANCEL"
    ]
    if cancel_ids:
        reasons = {d["run_id"]: d.get("reason", "") for d in data.get("decisions", [])}
        for rid in cancel_ids:
            logger.info(
                "Cancel evaluator: CANCEL %s — %s (triggered by %s)",
                rid, reasons.get(rid, ""), completed.run_id,
            )
    return cancel_ids, usage


async def interpret_experiment(
    client: LLMClient,
    plan: ExperimentPlan,
    results: dict[str, RunResult],
    exp_number: int,
) -> tuple[dict, Usage]:
    """
    Write the RESULTS.md finding paragraph for a completed experiment.
    Returns parsed dict with 'finding', 'key_metrics_table', 'next_priority'.
    """
    run_results_str = "\n\n".join(
        f"Run {run_id} ({plan.run_by_id(run_id).description if plan.run_by_id(run_id) else '?'}):\n"
        + (
            f"CANCELLED: {r.cancel_reason}"
            if r.cancelled
            else json.dumps(
                {k: round(v, 3) if isinstance(v, float) else v
                 for k, v in r.metrics.items()
                 if not isinstance(v, list)},
                indent=2,
            )
        )
        for run_id, r in results.items()
    )

    messages = [
        {
            "role": "system",
            "content": (
                "You are a robotaxi simulation researcher writing up results. "
                "Write concise, insight-dense findings. "
                "Explain what changed, why it happened mechanistically, and what it means for deployment. "
                "Include rider wait distribution: cite p10_wait_min, median_wait_min (p50), p90_wait_min when comparing arms. "
                "If a treatment arm **beats the control** per FOLLOW-UP PROTOCOL (favorable thresholds), next_priority MUST name a concrete follow-up to exploit that win. "
                "If nothing beats control, next_priority should pivot — do not mandate a follow-up only because metrics moved adversely. "
                "Output JSON only."
            ),
        },
        {
            "role": "user",
            "content": f"""Experiment {exp_number}: {plan.hypothesis}

Rationale: {plan.rationale}

Run results:
{run_results_str}

Decision tree from plan:
{plan.decision_tree}

Write a finding paragraph and metrics table for RESULTS.md.
Output JSON:
{_INTERPRET_SCHEMA}""",
        },
    ]

    data, usage = await client.complete_json(
        messages, max_tokens=4096, temperature=0.3, cache_system=False
    )
    return data, usage
