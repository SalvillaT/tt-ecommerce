"""Root script for the Silver DEMO-001 materialization Job (bundle task).

Same workspace-exec bootstrap contract as the Fase 2 ingestion root script:
serverless ``spark_python_task`` with ``source: WORKSPACE`` executes this file
via ``exec`` without a reliable file-path global, so the bundle task passes
the FIXED internal argument ``--pipelines-root=${workspace.file_path}``
(deployment configuration, never a run-editable parameter). The bootstrap
validates the root and ranks it on ``sys.path`` BEFORE importing
``pipelines``, then strips the internal argument so the job CLI sees only
``--batch-id`` and ``--audit-timestamp``.

The only runtime surface is the injected Spark session: no secrets, no
Databricks SDK, no Azure calls, and no writes to the raw ingestion layer. The
job reads the three managed raw tables read-only and materializes the DEMO-001
scope.
"""

from __future__ import annotations

import sys
from pathlib import Path

_INTERNAL_ROOT_ARG = "--pipelines-root"


def _strip_internal_args(argv: list[str]) -> None:
    """Rewrite `sys.argv` WITHOUT the internal bootstrap argument(s)."""
    sys.argv[:] = [
        arg for arg in argv
        if not arg.startswith(_INTERNAL_ROOT_ARG + "=")
    ]


def _resolve_internal_root(argv: list[str]) -> Path:
    """Consume the internal `--pipelines-root=` argument (workspace exec design)."""
    if any(arg == _INTERNAL_ROOT_ARG for arg in argv):
        raise SystemExit(
            f"run_silver_demo_001_job: {_INTERNAL_ROOT_ARG} must be given as "
            f"'{_INTERNAL_ROOT_ARG}=<root>' in the bundle task parameters"
        )
    values = [
        arg[len(_INTERNAL_ROOT_ARG) + 1:]
        for arg in argv
        if arg.startswith(_INTERNAL_ROOT_ARG + "=")
    ]
    if not values:
        raise SystemExit(
            "run_silver_demo_001_job: missing internal "
            f"'{_INTERNAL_ROOT_ARG}=${{workspace.file_path}}' argument; "
            "the job must be started by the bundle task (not directly)"
        )
    root_text = values[0]
    if any(value != root_text for value in values[1:]):
        raise SystemExit(
            f"run_silver_demo_001_job: conflicting {_INTERNAL_ROOT_ARG} values: {values}"
        )
    return Path(root_text)


def _validate_internal_root(root: Path) -> None:
    """Fail safely (before any `pipelines` import) on a broken deployment."""
    if not str(root) or not root.is_absolute():
        raise SystemExit(
            "run_silver_demo_001_job: internal bootstrap root must be an "
            f"absolute workspace path, got {str(root)!r} (is "
            "${workspace.file_path} actually available in this target?)"
        )
    if not (root / "pipelines").is_dir():
        raise SystemExit(
            f"run_silver_demo_001_job: internal root {root} does not contain a "
            "'pipelines' directory; the bundle sync is incomplete or "
            f"{_INTERNAL_ROOT_ARG} (from ${{workspace.file_path}}) is wrong"
        )


def ensure_pipelines_importable() -> None:
    """Bootstrap: validate the internal workspace root, then rank `sys.path`."""
    argv = list(sys.argv)
    root = _resolve_internal_root(argv)
    _validate_internal_root(root)
    text = str(root)
    if text not in sys.path:
        sys.path.insert(0, text)
    _strip_internal_args(argv)


ensure_pipelines_importable()

from pipelines.silver.job import parse_job_args  # noqa: E402


def run() -> int:
    config = parse_job_args()
    from pipelines.silver.job import execute_materialization, get_default_spark  # lazy Spark
    spark = get_default_spark()
    execute_materialization(spark, config)
    return 0


if __name__ == "__main__":
    # Workspace-backed spark_python_task executes this script via IPython exec,
    # where even SystemExit(0) is reported as a workload failure.  Returning
    # naturally is still exit code 0 under a normal Python interpreter; an
    # uncaught run failure continues to propagate and fail the task.
    run()
