"""Declarative Automation Bundle static tests (text assertions only).

No Databricks remote interaction, no deploy, no run: the bundle YAML is
asserted on its own text so secret leakage, schedules, concurrency/retry and
script wiring cannot drift silently. Static CLI validation
(`databricks bundle validate -t <target> ...`) is an operator command
(documented in the runbook), never part of the unittest suite.
"""

from __future__ import annotations

import json
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATABRICKS_YML = (ROOT / "databricks.yml").read_text(encoding="utf-8")
JOB_YML = (ROOT / "resources" / "bronze_job.yml").read_text(encoding="utf-8")
CONCURRENCY_BLOCK = "max_concurrent_runs: 1"


class BundleSecretHygieneTests(unittest.TestCase):
    def test_no_secret_material_in_bundle_files(self):
        blob = DATABRICKS_YML + "\n" + JOB_YML
        forbidden_value_patterns = [
            r"(?im)^\s*(client_secret|access_token|password|api_key|secret_value)\s*:",
            r"(?im)^\s*\w+:\s*['\"]?dapi",
            r"(?i)\bbearer\s+[a-z0-9]",
        ]
        for pattern in forbidden_value_patterns:
            self.assertIsNone(re.search(pattern, blob),
                              msg=f"forbidden secret pattern present: {pattern}")
        self.assertNotIn("oauth2", blob.lower())
        self.assertNotIn("dapi", blob)

    def test_run_as_is_a_bundle_variable_without_value(self):
        self.assertIn(
            "service_principal_name: ${var.run_as_service_principal_name}",
            JOB_YML,
        )
        self.assertIn("run_as_service_principal_name:", DATABRICKS_YML)
        self.assertIn('default: ""', DATABRICKS_YML)

    def test_landing_root_variable_present_and_non_secret(self):
        self.assertIn("landing_root:", DATABRICKS_YML)
        self.assertIn("abfss://", DATABRICKS_YML)
        # Deployment configuration, NOT a run-editable job parameter: the
        # bundle variable reaches the task as a FIXED argument (never
        # `{{job.parameters.landing_root}}`).
        self.assertNotIn("name: landing_root", JOB_YML)
        self.assertNotIn("{{job.parameters.landing_root}}", JOB_YML)
        self.assertIn("--landing-root=${var.landing_root}", JOB_YML)


class BronzeJobResourceTests(unittest.TestCase):
    def test_job_is_manual_without_schedule(self):
        self.assertIsNone(re.search(r"(?m)^\s*schedule:", JOB_YML))
        self.assertIsNone(re.search(r"(?m)^\s*continuous:", JOB_YML))
        self.assertIsNone(re.search(r"(?m)^\s*cron:", JOB_YML))

    def test_concurrency_and_retry_are_exactly_one_run_zero_retries(self):
        self.assertIn(CONCURRENCY_BLOCK, JOB_YML)
        self.assertIn("max_retries: 0", JOB_YML)

    def test_job_parameters_declared(self):
        for parameter in ("name: batch_id", "name: arrival_date"):
            self.assertIn(parameter, JOB_YML)
        # The landing root is deployment configuration: it must never appear
        # as a run-editable job parameter.
        self.assertNotIn("name: landing_root", JOB_YML)

    def test_python_root_script_wiring(self):
        self.assertIn("spark_python_task:", JOB_YML)
        self.assertIn("${workspace.file_path}/scripts/run_bronze_job.py", JOB_YML)
        self.assertIn("source: WORKSPACE", JOB_YML)
        self.assertIn("--batch-id={{job.parameters.batch_id}}", JOB_YML)
        self.assertIn("--arrival-date={{job.parameters.arrival_date}}", JOB_YML)
        # The landing root is a FIXED deployment argument from the bundle
        # variable, not a per-run parameter.
        self.assertIn("--landing-root=${var.landing_root}", JOB_YML)
        self.assertNotIn("{{job.parameters.landing_root}}", JOB_YML)

    def test_task_key_is_single(self):
        self.assertEqual(JOB_YML.count("task_key:"), 1)
        self.assertIn("task_key: bronze_ingestion", JOB_YML)


class BundleTopologyTests(unittest.TestCase):
    def test_bundle_name_and_resource_include(self):
        self.assertIn("name: tt-ecommerce-bronze", DATABRICKS_YML)
        self.assertIn("- resources/*.yml", DATABRICKS_YML)

    def test_sync_ships_only_runtime_code(self):
        self.assertIn("- pipelines/**", DATABRICKS_YML)
        self.assertIn("- scripts/run_bronze_job.py", DATABRICKS_YML)
        # Only the ONE root script the job executes is shipped: the operator
        # scripts stay out of the bundle runtime, and so do the code maps.
        self.assertNotIn("generate_synthetic", DATABRICKS_YML)
        self.assertNotIn("promote_batch", DATABRICKS_YML)
        self.assertIn("- pipelines/**/codemap.md", DATABRICKS_YML)
        for excluded in ("- tests/**", "- docs/**", "- data/**", "- infra/**"):
            self.assertIn(excluded, DATABRICKS_YML)

    def test_serverless_environment_key_is_conditional_to_target(self):
        self.assertIn("development_serverless:", DATABRICKS_YML)
        self.assertIn("default: true", DATABRICKS_YML)
        self.assertIn("environment_key: ${var.environment_key}", DATABRICKS_YML)
        classic_block = DATABRICKS_YML.split("development_classic:", 1)[1]
        self.assertNotIn("environment_key", classic_block)
        self.assertNotIn("environments:", classic_block)
        self.assertIn("development_classic:", DATABRICKS_YML)


if __name__ == "__main__":
    unittest.main()
