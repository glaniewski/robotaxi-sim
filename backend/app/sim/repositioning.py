"""
Backward-compatibility shim.

The repositioning policy implementations live in reposition_policies/.
This module re-exports RepositioningPolicy (= DemandScorePolicy) under the
old name so existing tests and scripts don't need to change.
"""
from .reposition_policies.demand_score import DemandScorePolicy as RepositioningPolicy
from .reposition_policies.base import BaseRepositioningPolicy

__all__ = ["RepositioningPolicy", "BaseRepositioningPolicy"]
