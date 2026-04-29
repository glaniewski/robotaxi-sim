"""
Budget tracker — accumulates LLM token usage and cost, enforces hard limits.

Persisted to state.json so limits survive restarts.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Optional

from .llm import Usage

logger = logging.getLogger(__name__)

STATE_PATH = Path(__file__).parent / "state.json"


@dataclass
class State:
    """Persisted experimenter state."""

    next_exp_number: int = 76          # start after the 75 existing experiments
    total_spend_usd: float = 0.0
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    total_cache_read_tokens: int = 0
    experiments_completed: int = 0
    arms_completed: int = 0
    arms_killed: int = 0

    def save(self, path: Path = STATE_PATH) -> None:
        path.write_text(json.dumps(asdict(self), indent=2))

    @classmethod
    def load(cls, path: Path = STATE_PATH) -> "State":
        if path.exists():
            try:
                data = json.loads(path.read_text())
                return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})
            except Exception as exc:
                logger.warning("Could not load state.json (%s) — starting fresh", exc)
        return cls()

    def summary(self) -> str:
        return (
            f"Exp#{self.next_exp_number} | "
            f"spend=${self.total_spend_usd:.4f} | "
            f"exps={self.experiments_completed} | "
            f"arms={self.arms_completed} (killed={self.arms_killed})"
        )


class Budget:
    """
    Tracks cumulative spend and enforces limits.

    Usage:
        budget = Budget(max_usd=10.0, max_experiments=50)
        budget.record(usage)
        if budget.exhausted:
            break
    """

    def __init__(
        self,
        max_usd: float = 10.0,
        max_experiments: int = 100,
        state: Optional[State] = None,
    ) -> None:
        self.max_usd = max_usd
        self.max_experiments = max_experiments
        self.state = state or State.load()

    @property
    def exhausted(self) -> bool:
        if self.state.total_spend_usd >= self.max_usd:
            logger.warning(
                "Budget exhausted: $%.4f / $%.2f", self.state.total_spend_usd, self.max_usd
            )
            return True
        if self.state.experiments_completed >= self.max_experiments:
            logger.warning(
                "Max experiments reached: %d / %d",
                self.state.experiments_completed,
                self.max_experiments,
            )
            return True
        return False

    def record(self, usage: Usage) -> None:
        """Add a LLM usage record to the running total."""
        self.state.total_spend_usd += usage.cost_usd
        self.state.total_input_tokens += usage.input_tokens
        self.state.total_output_tokens += usage.output_tokens
        self.state.total_cache_read_tokens += usage.cache_read_tokens

    def finish_arm(self, killed: bool = False) -> None:
        self.state.arms_completed += 1
        if killed:
            self.state.arms_killed += 1

    def finish_experiment(self) -> None:
        self.state.experiments_completed += 1
        self.state.next_exp_number += 1

    def save(self) -> None:
        self.state.save()
