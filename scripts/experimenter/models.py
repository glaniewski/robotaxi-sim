"""
Data models for the autonomous experimenter.

An ExperimentPlan has:
  - Multiple SimRun objects (each is one sim run with specific params)
  - A list of Batches (execution order: runs within a batch execute in parallel, up to MAX_PARALLEL=2)
  - CancelRules: if run X finishes with a bad result, cancel run Y immediately

The planner (LLM) outputs an ExperimentPlan as JSON; the orchestrator executes it.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class SimRun:
    """
    One simulation run. Maps directly to run_continuous_experiment() params.
    Each experiment contains 2–5 SimRuns testing different configurations.
    """

    run_id: str
    description: str

    # run_continuous_experiment required
    n_sites: int
    num_days: int
    demand_scale: float
    fleet_size: int
    plugs_per_site: int
    charger_kw: float

    # optional overrides (None = use e63 module defaults)
    battery_kwh: Optional[float] = None
    depot_h3_cells: Optional[list[str]] = None   # None → top_demand_cells(n_sites)
    min_plug_duration_minutes: Optional[float] = None
    charging_queue_policy: Optional[str] = None  # "fifo" | "jit"
    vehicle_preset: Optional[str] = None         # "tesla" | "waymo"; None → tesla
    reposition_alpha: Optional[float] = None     # 0.0–1.0; None → 0.6

    # Synthetic demand model config (None = use RideAustin parquet as before)
    demand_config: Optional[dict] = None

    def to_dict(self) -> dict:
        return {
            "run_id": self.run_id,
            "description": self.description,
            "n_sites": self.n_sites,
            "num_days": self.num_days,
            "demand_scale": self.demand_scale,
            "fleet_size": self.fleet_size,
            "plugs_per_site": self.plugs_per_site,
            "charger_kw": self.charger_kw,
            "battery_kwh": self.battery_kwh,
            "depot_h3_cells": self.depot_h3_cells,
            "min_plug_duration_minutes": self.min_plug_duration_minutes,
            "charging_queue_policy": self.charging_queue_policy,
            "vehicle_preset": self.vehicle_preset,
            "reposition_alpha": self.reposition_alpha,
            "demand_config": self.demand_config,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "SimRun":
        return cls(
            run_id=d["run_id"],
            description=d["description"],
            n_sites=int(d["n_sites"]),
            num_days=int(d["num_days"]),
            demand_scale=float(d["demand_scale"]),
            fleet_size=int(d["fleet_size"]),
            plugs_per_site=int(d["plugs_per_site"]),
            charger_kw=float(d["charger_kw"]),
            battery_kwh=float(d["battery_kwh"]) if d.get("battery_kwh") is not None else None,
            depot_h3_cells=d.get("depot_h3_cells"),
            min_plug_duration_minutes=float(d["min_plug_duration_minutes"])
            if d.get("min_plug_duration_minutes") is not None
            else None,
            charging_queue_policy=d.get("charging_queue_policy"),
            vehicle_preset=d.get("vehicle_preset"),
            reposition_alpha=float(d["reposition_alpha"])
            if d.get("reposition_alpha") is not None
            else None,
            demand_config=d.get("demand_config"),
        )


@dataclass
class CancelRule:
    """
    Result-based cancellation: when `trigger_run_id` completes, check its metric.
    If the condition is met, cancel `target_run_ids` (SIGTERM while still running).

    This is separate from the hard timeout (which cancels any run exceeding N minutes
    regardless of results). CancelRules fire when one run's outcome makes a sibling
    run pointless — e.g. if the low-plug-count run already shows served%<70%, there's
    no value in waiting for the high-plug-count run if the hypothesis was already answered.
    """

    trigger_run_id: str
    target_run_ids: list[str]
    metric: str          # e.g. "served_pct"
    operator: str        # "<" | ">" | "<=" | ">="
    value: float
    reason: str

    def evaluate(self, result: "RunResult") -> bool:
        """Return True if cancel condition is met."""
        if result.run_id != self.trigger_run_id:
            return False
        v = result.metrics.get(self.metric)
        if v is None:
            return False
        ops = {"<": float.__lt__, ">": float.__gt__, "<=": float.__le__, ">=": float.__ge__}
        fn = ops.get(self.operator)
        return bool(fn and fn(float(v), self.value))

    def to_dict(self) -> dict:
        return {
            "trigger_run_id": self.trigger_run_id,
            "target_run_ids": self.target_run_ids,
            "metric": self.metric,
            "operator": self.operator,
            "value": self.value,
            "reason": self.reason,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "CancelRule":
        return cls(
            trigger_run_id=d["trigger_run_id"],
            target_run_ids=d["target_run_ids"],
            metric=d["metric"],
            operator=d["operator"],
            value=float(d["value"]),
            reason=d["reason"],
        )


@dataclass
class Batch:
    """
    A group of sim runs to execute in parallel (up to MAX_PARALLEL=2 on M4 MacBook).
    Batches within an experiment execute sequentially — Batch N+1 starts only after
    all runs in Batch N have finished or been cancelled.

    Cancel rules are evaluated whenever any run in this batch completes.
    """

    run_ids: list[str]          # 2–4 runs executed in parallel
    cancel_rules: list[CancelRule] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "run_ids": self.run_ids,
            "cancel_rules": [k.to_dict() for k in self.cancel_rules],
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Batch":
        # Handle LLM returning cancel_rules as a dict instead of a list, or omitting it
        raw_rules = d.get("cancel_rules", [])
        if isinstance(raw_rules, dict):
            raw_rules = [raw_rules]
        elif not isinstance(raw_rules, list):
            raw_rules = []
        # Handle LLM returning run_ids as a string instead of a list
        run_ids = d["run_ids"]
        if isinstance(run_ids, str):
            run_ids = [run_ids]
        return cls(
            run_ids=run_ids,
            cancel_rules=[CancelRule.from_dict(k) for k in raw_rules],
        )


@dataclass
class ExperimentPlan:
    """
    One experiment proposed by the LLM planner.

    Contains all sim runs grouped into batches. Runs within a batch execute in
    parallel; batches execute sequentially. The planner also decides which runs
    can cancel each other based on results (separate from the hard timeout).
    """

    exp_number: int
    hypothesis: str
    rationale: str
    runs: list[SimRun]
    batches: list[Batch]
    decision_tree: str  # what to investigate next based on outcomes (stored in RESULTS.md)

    def run_by_id(self, run_id: str) -> Optional[SimRun]:
        return next((r for r in self.runs if r.run_id == run_id), None)

    def to_dict(self) -> dict:
        return {
            "exp_number": self.exp_number,
            "hypothesis": self.hypothesis,
            "rationale": self.rationale,
            "runs": [r.to_dict() for r in self.runs],
            "batches": [b.to_dict() for b in self.batches],
            "decision_tree": self.decision_tree,
        }

    @classmethod
    def from_dict(cls, d: dict, exp_number: int) -> "ExperimentPlan":
        # runs must be a list of dicts
        runs_raw = d["runs"]
        if isinstance(runs_raw, dict):
            runs_raw = list(runs_raw.values())

        # batches must be a list of dicts; LLM returns various formats
        batches_raw = d.get("batches", [])
        if isinstance(batches_raw, dict):
            batches_raw = list(batches_raw.values())
        # If batches is a flat list of run_id strings, wrap into a single batch
        if batches_raw and isinstance(batches_raw[0], str):
            batches_raw = [{"run_ids": batches_raw}]
        # If batches is a list of lists (e.g. [["run_a", "run_b"]]), convert each inner list
        elif batches_raw and isinstance(batches_raw[0], list):
            batches_raw = [{"run_ids": ids} for ids in batches_raw]

        return cls(
            exp_number=exp_number,
            hypothesis=d["hypothesis"],
            rationale=d["rationale"],
            runs=[SimRun.from_dict(r) for r in runs_raw],
            batches=[Batch.from_dict(b) for b in batches_raw],
            decision_tree=d.get("decision_tree", ""),
        )


@dataclass
class RunResult:
    """Result from one completed sim run."""

    run_id: str
    exp_number: int
    metrics: dict[str, Any]  # parsed EXPERIMENT_RESULT_JSON
    stdout: str
    exit_code: int
    wall_seconds: float
    cancelled: bool = False       # True if cancelled by result-based rule or timeout
    cancel_reason: str = ""
    script_path: str = ""

    @property
    def served_pct(self) -> float:
        return float(self.metrics.get("served_pct", 0.0))

    @property
    def p90_wait_min(self) -> float:
        return float(self.metrics.get("p90_wait_min", 0.0))

    def to_dict(self) -> dict:
        return {
            "run_id": self.run_id,
            "exp_number": self.exp_number,
            "metrics": self.metrics,
            "exit_code": self.exit_code,
            "wall_seconds": self.wall_seconds,
            "cancelled": self.cancelled,
            "cancel_reason": self.cancel_reason,
            "script_path": self.script_path,
        }
