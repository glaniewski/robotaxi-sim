"""
RESULTS.md reporter — appends experiment results in the established format.

Appends immediately after each arm completes (partial results), then adds
the full group summary with interpretation paragraph when all arms are done.
Uses a simple file lock to avoid races if two arms finish simultaneously.
"""
from __future__ import annotations

import fcntl
import json
import logging
from datetime import date
from pathlib import Path
from typing import Any, Optional

from .models import RunResult, SimRun, ExperimentPlan

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[2]
RESULTS_MD = ROOT / "RESULTS.md"

# Metrics to show in the arm result table, in order
_TABLE_METRICS = [
    ("served_pct", "served%", ".2f"),
    ("p10_wait_min", "p10_wait_min", ".2f"),
    ("median_wait_min", "median_wait_min", ".2f"),
    ("p90_wait_min", "p90_wait_min", ".2f"),
    ("sla_adherence_pct", "sla_adherence_pct", ".1f"),
    ("total_system_cost_per_trip", "cost_per_trip", ".2f"),
    ("energy_cost", "energy_cost", ",.0f"),
    ("demand_cost", "demand_cost", ",.0f"),
    ("maintenance_cost", "maintenance_cost", ",.0f"),
    ("fleet_fixed_cost", "fleet_fixed_cost", ",.0f"),
    ("infra_cost", "infra_cost", ",.0f"),
    ("total_system_cost", "total_system_cost", ",.0f"),
    ("depreciation_per_vehicle_day", "deprec_per_veh_day", ".2f"),
    ("charger_utilization_pct", "charger_util%", ".1f"),
    ("fleet_battery_pct", "fleet_soc%", ".1f"),
    ("repositioning_pct", "repo%", ".1f"),
    ("deadhead_pct", "deadhead%", ".1f"),
    ("avg_dispatch_distance", "avg_dispatch_mi", ".3f"),
    ("depot_queue_p90_min", "depot_q_p90_min", ".2f"),
    ("contribution_margin_per_trip", "cm_per_trip", ".4f"),
]


def _fmt(v: Any, fmt: str) -> str:
    if v is None:
        return "—"
    try:
        return format(float(v), fmt)
    except (TypeError, ValueError):
        return str(v)


def _arm_config_line(spec: SimRun) -> str:
    parts = [
        f"n_sites={spec.n_sites}",
        f"fleet={spec.fleet_size}",
        f"scale={spec.demand_scale}",
        f"days={spec.num_days}",
        f"{spec.plugs_per_site}p×{spec.charger_kw:g}kW",
    ]
    if spec.vehicle_preset:
        parts.append(f"preset={spec.vehicle_preset}")
    if spec.reposition_alpha is not None:
        parts.append(f"alpha={spec.reposition_alpha}")
    if spec.battery_kwh is not None:
        parts.append(f"bat={spec.battery_kwh}kWh")
    if spec.depot_h3_cells:
        parts.append(f"depots=[{','.join(spec.depot_h3_cells)}]")
    if spec.min_plug_duration_minutes is not None:
        parts.append(f"min_plug={spec.min_plug_duration_minutes}m")
    if spec.charging_queue_policy:
        parts.append(f"policy={spec.charging_queue_policy}")
    return ", ".join(parts)


def _run_result_block(
    run_spec: SimRun,
    result: RunResult,
    exp_number: int,
) -> str:
    """Format one run's result as a markdown sub-section."""
    lines: list[str] = []

    if result.cancelled:
        lines.append(f"##### Run `{result.run_id}` — {run_spec.description} *(CANCELLED)*\n")
        lines.append(f"**Cancel reason:** {result.cancel_reason}  ")
        lines.append(f"**Wall time:** {result.wall_seconds:.0f}s\n")
        return "\n".join(lines)

    ok = result.exit_code == 0 or bool(result.metrics)  # metrics present = sim succeeded
    status = "✓" if ok else f"⚠ exit={result.exit_code}"
    lines.append(
        f"##### Run `{result.run_id}` {status} — {run_spec.description}  "
        f"*(wall {result.wall_seconds:.0f}s)*\n"
    )
    lines.append(f"**Config:** `{_arm_config_line(run_spec)}`\n")

    if result.metrics:
        lines.append("| Metric | Value |")
        lines.append("| ------ | ----- |")
        for key, label, fmt in _TABLE_METRICS:
            v = result.metrics.get(key)
            if v is not None:
                lines.append(f"| {label} | {_fmt(v, fmt)} |")

        daily = result.metrics.get("daily_served_pct", [])
        if daily:
            ds = ", ".join(f"{x:.1f}" for x in daily)
            lines.append(f"\n**Daily served%:** {ds}")

        depots = result.metrics.get("depot_h3_cells", [])
        if depots:
            lines.append(f"**Depots:** `{', '.join(depots)}`")
    else:
        lines.append("*No metrics parsed — script may have failed.*\n")
        if result.stdout:
            lines.append("<details><summary>stdout (last 20 lines)</summary>\n")
            lines.append("```")
            lines.extend(result.stdout.strip().splitlines()[-20:])
            lines.append("```\n</details>")

    return "\n".join(lines)


def _group_header(plan: ExperimentPlan, today: str) -> str:
    return f"""\n\n## Experiment {plan.exp_number} — {plan.hypothesis}

**Date:** {today}
**Hypothesis:** {plan.hypothesis}
**Rationale:** {plan.rationale}

"""


def append_run_result(
    plan: ExperimentPlan,
    run_spec: SimRun,
    result: RunResult,
    is_first_result: bool,
) -> None:
    """
    Append one run's result to RESULTS.md immediately after it completes.
    If this is the first result in the experiment, also write the experiment header.
    Subsequent runs in the same experiment just append their result block below.
    """
    today = date.today().isoformat()
    block = ""

    if is_first_result:
        block += _group_header(plan, today)

    block += _run_result_block(run_spec, result, plan.exp_number)
    block += "\n\n"

    _append_to_results(block)
    logger.info("Appended run %s result to RESULTS.md", result.run_id)


def append_group_summary(
    plan: ExperimentPlan,
    all_results: dict[str, RunResult],
    interpretation: dict,
) -> None:
    """
    Append the group-level interpretation and decision tree after all arms finish.
    """
    finding = interpretation.get("finding", "")
    next_priority = interpretation.get("next_priority", "")
    table_rows = interpretation.get("key_metrics_table", [])

    lines: list[str] = ["\n### Finding\n"]
    lines.append(finding)
    lines.append("")

    if table_rows:
        # Build comparison table from interpreter output
        lines.append("\n### Comparison\n")
        if table_rows:
            headers = list(table_rows[0].keys())
            lines.append("| " + " | ".join(headers) + " |")
            lines.append("| " + " | ".join("---" for _ in headers) + " |")
            for row in table_rows:
                lines.append("| " + " | ".join(str(row.get(h, "—")) for h in headers) + " |")
        lines.append("")

    if next_priority:
        lines.append(f"\n**Next priority:** {next_priority}\n")

    if plan.decision_tree:
        lines.append(f"\n**Decision tree:** {plan.decision_tree}\n")

    lines.append("\n---\n")

    _append_to_results("\n".join(lines))
    logger.info("Appended group summary for Exp%d to RESULTS.md", plan.exp_number)



def _append_to_results(text: str) -> None:
    """Append text to RESULTS.md with a file lock to prevent concurrent writes."""
    with open(RESULTS_MD, "a", encoding="utf-8") as f:
        try:
            fcntl.flock(f, fcntl.LOCK_EX)
            f.write(text)
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)
