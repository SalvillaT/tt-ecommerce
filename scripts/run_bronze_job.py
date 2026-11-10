"""Root script for the Bronze Job executed by the Declarative Automation Bundle.

Serverless `spark_python_task` tasks with `source: WORKSPACE` execute this
file via `exec` WITHOUT a reliable file-path global, so the script cannot
discover the sibling `pipelines` package itself. Instead, the bundle task
passes a FIXED internal argument carrying the synced workspace root:

    --pipelines-root=${workspace.file_path}

It is a task parameter written inside the bundle YAML (never a job-level
parameter), so it is fixed deployment configuration and NOT operator-editable
per run. The bootstrap below consumes that argument, validates it (absolute
workspace path that really contains a `pipelines/` directory) and puts it on
`sys.path` BEFORE importing `pipelines`; then it strips the internal argument
from `sys.argv`, so the existing job CLI contract still sees only
`--batch-id`, `--arrival-date` and the fixed `--landing-root` (plus optional
pipeline/attempt ids), with or without `=`. Failures are reported before any
`pipelines` import so a malformed deployment cannot fail with a confusing
NameError/ImportError. No Databricks SDK usage, no secrets: the only cloud
surface is the injected Spark session.
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
    """Consume the internal `--pipelines-root=` argument (workspace exec design).

    The argument is emitted only by the bundle task (fixed YAML), so a bare
    `--pipelines-root` (space-separated value, targets could reorder it) is
    rejected, as are duplicated arguments carrying conflicting roots.
    """
    if any(arg == _INTERNAL_ROOT_ARG for arg in argv):
        raise SystemExit(
            f"run_bronze_job: {_INTERNAL_ROOT_ARG} must be given as "
            f"'{_INTERNAL_ROOT_ARG}=<root>' in the bundle task parameters"
        )
    values = [
        arg[len(_INTERNAL_ROOT_ARG) + 1:]
        for arg in argv
        if arg.startswith(_INTERNAL_ROOT_ARG + "=")
    ]
    if not values:
        raise SystemExit(
            "run_bronze_job: missing internal "
            f"'{_INTERNAL_ROOT_ARG}=${{workspace.file_path}}' argument; "
            "the job must be started by the bundle task (not directly)"
        )
    root_text = values[0]
    if any(value != root_text for value in values[1:]):
        raise SystemExit(
            f"run_bronze_job: conflicting {_INTERNAL_ROOT_ARG} values: {values}"
        )
    return Path(root_text)


def _validate_internal_root(root: Path) -> None:
    """Fail safely (before any `pipelines` import) on a broken deployment."""
    if not str(root) or not root.is_absolute():
        raise SystemExit(
            "run_bronze_job: internal bootstrap root must be an absolute "
            f"workspace path, got {str(root)!r} (is ${{workspace.file_path}} "
            "actually available in this target?)"
        )
    if not (root / "pipelines").is_dir():
        raise SystemExit(
            f"run_bronze_job: internal root {root} does not contain a "
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

from pipelines.bronze.entrypoint import parse_job_args  # noqa: E402


def run() -> int:
    config = parse_job_args()
    from pipelines.bronze.entrypoint import execute_run, get_default_spark  # lazy Spark
    spark = get_default_spark()
    execute_run(spark, config)  # FAILED events are appended; failures re-propagate
    return 0


if __name__ == "__main__":
    # Workspace-backed spark_python_task executes this script via IPython exec,
    # where even SystemExit(0) is reported as a workload failure.  Returning
    # naturally is still exit code 0 under a normal Python interpreter; an
    # uncaught run failure continues to propagate and fail the task.
    run()
