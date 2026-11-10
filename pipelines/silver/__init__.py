"""Pure, deterministic Silver → quarantine → Gold transformation core.

This package deliberately has no Spark, Delta, filesystem, or network dependency.
"""

from .core import (
    GoldDailySalesSegment,
    PipelineResult,
    QuarantineRecord,
    SilverRow,
    SuppressedCell,
    run_pipeline,
)

__all__ = (
    "GoldDailySalesSegment",
    "PipelineResult",
    "QuarantineRecord",
    "SilverRow",
    "SuppressedCell",
    "run_pipeline",
)
