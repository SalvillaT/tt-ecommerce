"""Root script for the Bronze Job executed by the Declarative Automation Bundle.

Workspace-file tasks run the script with its own directory on `sys.path`; the
`pipelines` package lives in the sibling project root, so both are inserted
explicitly before importing the entrypoint. Job parameters arrive as command
line arguments (`--batch-id <id> --arrival-date <YYYY-MM-DD> --landing-root
<abfss://...>`, with or without `=`). No Databricks SDK usage, no secrets:
the only cloud surface is the injected Spark session.
"""

from __future__ import annotations

import sys
from pathlib import Path


def ensure_pipelines_importable() -> None:
    here = Path(__file__).resolve().parent          # .../scripts
    root = here.parent                              # project root with `pipelines/`
    for candidate in (here, root):
        text = str(candidate)
        if text and text not in sys.path:
            sys.path.insert(0, text)


ensure_pipelines_importable()

from pipelines.bronze.entrypoint import parse_job_args  # noqa: E402


def run() -> int:
    config = parse_job_args()
    from pipelines.bronze.entrypoint import execute_run, get_default_spark  # lazy Spark
    spark = get_default_spark()
    execute_run(spark, config)  # FAILED events are appended; failures re-propagate
    return 0


if __name__ == "__main__":
    sys.exit(run())
