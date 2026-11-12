"""Static bundle tests for the Silver DEMO-001 materialization job.

Text assertions only: no Databricks remote interaction, no deploy, no run.
These tests lock the manual/no-schedule topology, secret hygiene, the separate
no-default run-as variable, serverless environment wiring, the fixed internal
bootstrap argument, the two ordered tasks, the verifier's read-only intent,
and the sync include — mirroring the Bronze bundle test contract.
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATABRICKS_YML = (ROOT / "databricks.yml").read_text(encoding="utf-8")
SILVER_JOB_YML = (ROOT / "resources" / "silver_demo_001_job.yml").read_text(encoding="utf-8")
ROOT_SCRIPT_TEXT = (ROOT / "scripts" / "run_silver_demo_001_job.py").read_text(encoding="utf-8")
VERIFIER_TEXT = (ROOT / "scripts" / "verify_silver_demo_001.py").read_text(encoding="utf-8")
BRONZE_JOB_YML = (ROOT / "resources" / "bronze_job.yml").read_text(encoding="utf-8")
CONCURRENCY_BLOCK = "max_concurrent_runs: 1"

RUN_AS_VARIABLE = "silver_run_as_service_principal_name"
MATERIALIZATION_TASK = "silver_materialization"
VERIFY_TASK = "silver_demo_001_verify"
DEFAULT_AUDIT_TIMESTAMP = "2026-01-20T12:00:00Z"


def _variable_block(name: str) -> str:
    """Extract one top-level variables entry (description + optional default)."""
    index = DATABRICKS_YML.index(f"  {name}:")
    match = re.match(r"[\s\S]*?(?=\n  \w)", DATABRICKS_YML[index:])
    if match is None:
        raise AssertionError(f"could not extract variable block: {name}")
    return match.group(0)


class SilverBundleSecretHygieneTests(unittest.TestCase):
    def test_no_secret_material_in_silver_bundle_files(self):
        blob = DATABRICKS_YML + "\n" + SILVER_JOB_YML
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

    def test_no_embedded_application_client_id(self):
        # Application (client) IDs are GUID-shaped; they may only arrive via
        # --var at validate/deploy/run time, never inside the bundle files.
        blob = DATABRICKS_YML + "\n" + SILVER_JOB_YML
        self.assertIsNone(re.search(
            r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b",
            blob), msg="GUID-shaped client id found in bundle files")

    def test_run_as_uses_the_dedicated_silver_variable(self):
        self.assertIn(
            f"service_principal_name: ${{var.{RUN_AS_VARIABLE}}}", SILVER_JOB_YML)
        self.assertIn(f"{RUN_AS_VARIABLE}:", DATABRICKS_YML)
        # The Bronze job keeps its own, separate run-as variable.
        self.assertIn("service_principal_name: ${var.run_as_service_principal_name}",
                      BRONZE_JOB_YML)

    def test_silver_run_as_variable_has_no_default(self):
        block = _variable_block(RUN_AS_VARIABLE)
        self.assertIsNone(re.search(r"(?m)^\s*default:", block),
                          msg="the Silver run-as variable must have no default")
        self.assertIn("DIFFERENT principal from the Bronze run-as", block)

    def test_landing_and_pipeline_roots_are_not_silver_job_inputs(self):
        self.assertNotIn("landing_root", SILVER_JOB_YML)
        self.assertNotIn("abfss://", SILVER_JOB_YML)
        # Catalog/schema/table/scope are code constants, not bundle values.
        self.assertNotIn("tt_ecommerce", SILVER_JOB_YML)


class SilverJobResourceTests(unittest.TestCase):
    def _task_blocks(self) -> list[str]:
        parts = SILVER_JOB_YML.split("\n        - task_key:")
        return parts[1:]

    def test_job_is_manual_without_schedule(self):
        self.assertIsNone(re.search(r"(?m)^\s*schedule:", SILVER_JOB_YML))
        self.assertIsNone(re.search(r"(?m)^\s*continuous:", SILVER_JOB_YML))
        self.assertIsNone(re.search(r"(?m)^\s*cron:", SILVER_JOB_YML))

    def test_concurrency_and_retry_semantics(self):
        self.assertIn(CONCURRENCY_BLOCK, SILVER_JOB_YML)
        blocks = self._task_blocks()
        self.assertEqual(len(blocks), 2)
        for block in blocks:
            self.assertIn("max_retries: 0", block)
            self.assertIn("timeout_seconds:", block)

    def test_job_is_clearly_named_and_materialization_task_present(self):
        self.assertIn("name: tt-ecommerce-silver-demo-001-materialization", SILVER_JOB_YML)
        self.assertIn(f"task_key: {MATERIALIZATION_TASK}", SILVER_JOB_YML)
        self.assertIn(f"task_key: {VERIFY_TASK}", SILVER_JOB_YML)
        self.assertEqual(len(self._task_blocks()), 2)

    def test_two_ordered_tasks_with_verify_dependency(self):
        self.assertLess(SILVER_JOB_YML.index(f"task_key: {MATERIALIZATION_TASK}"),
                        SILVER_JOB_YML.index(f"task_key: {VERIFY_TASK}"))
        self.assertIn("depends_on:", SILVER_JOB_YML)
        self.assertIn(f"- task_key: {MATERIALIZATION_TASK}", SILVER_JOB_YML)

    def test_job_parameters_are_only_batch_and_audit_timestamp(self):
        parameters_block = SILVER_JOB_YML.split("parameters:", 1)[1].split("run_as:", 1)[0]
        self.assertIn('name: batch_id', parameters_block)
        self.assertIn('default: "DEMO-001"', parameters_block)
        self.assertIn("name: audit_timestamp", parameters_block)
        self.assertIn(f'default: "{DEFAULT_AUDIT_TIMESTAMP}"', parameters_block)
        forwarded = set(re.findall(r"\{\{job\.parameters\.(\w+)\}\}", SILVER_JOB_YML))
        self.assertEqual(forwarded, {"batch_id", "audit_timestamp"})

    def test_parameters_forwarded_as_fixed_script_arguments(self):
        self.assertIn("--batch-id={{job.parameters.batch_id}}", SILVER_JOB_YML)
        self.assertIn(f"--audit-timestamp={{{{job.parameters.audit_timestamp}}}}",
                      SILVER_JOB_YML)

    def test_both_tasks_are_workspace_backed_python_tasks(self):
        self.assertEqual(SILVER_JOB_YML.count("spark_python_task:"), 2)
        for block in self._task_blocks():
            self.assertIn("source: WORKSPACE", block)
        self.assertIn("${workspace.file_path}/scripts/run_silver_demo_001_job.py",
                      SILVER_JOB_YML)
        self.assertIn("${workspace.file_path}/scripts/verify_silver_demo_001.py",
                      SILVER_JOB_YML)

    def test_verifier_task_has_no_run_editable_parameters(self):
        verifier_task = SILVER_JOB_YML.split(f"task_key: {VERIFY_TASK}", 1)[1]
        self.assertNotIn("parameters:", verifier_task)
        self.assertNotIn("{{job.parameters.", verifier_task)


class SilverWorkspaceRootBootstrapTests(unittest.TestCase):
    """The internal workspace-root bootstrap argument is fixed task config."""

    def test_task_passes_fixed_internal_pipelines_root(self):
        self.assertEqual(
            SILVER_JOB_YML.count("--pipelines-root=${workspace.file_path}"), 1)

    def test_internal_root_is_never_a_run_editable_parameter(self):
        self.assertNotIn("name: pipelines_root", SILVER_JOB_YML)
        self.assertNotIn("{{job.parameters.pipelines_root}}", SILVER_JOB_YML)

    def test_root_script_consumes_validates_and_ranks_internal_root(self):
        self.assertIn("_INTERNAL_ROOT_ARG = \"--pipelines-root\"", ROOT_SCRIPT_TEXT)
        self.assertIn("(root / \"pipelines\").is_dir()", ROOT_SCRIPT_TEXT)
        self.assertIn("is_absolute", ROOT_SCRIPT_TEXT)
        self.assertIn("def ensure_pipelines_importable", ROOT_SCRIPT_TEXT)
        self.assertIn("sys.path.insert(0, text)", ROOT_SCRIPT_TEXT)

    def test_root_script_has_no_file_or_environment_discovery(self):
        self.assertNotIn("__file__", ROOT_SCRIPT_TEXT)
        self.assertNotIn("sys.argv[0]", ROOT_SCRIPT_TEXT)
        self.assertNotIn("os.environ", ROOT_SCRIPT_TEXT)

    def test_root_script_imports_only_the_silver_job_module(self):
        imports = set(re.findall(r"from (pipelines[\w.]*) import", ROOT_SCRIPT_TEXT))
        self.assertEqual(imports, {"pipelines.silver.job"})


class SilverVerifierReadOnlyIntentTests(unittest.TestCase):
    def test_verifier_is_standalone_without_pipelines_imports(self):
        self.assertNotIn("pipelines", VERIFIER_TEXT)

    def test_verifier_never_mutates_or_exits(self):
        self.assertNotIn("SystemExit", VERIFIER_TEXT)
        self.assertNotIn("sys.exit", VERIFIER_TEXT)
        self.assertNotIn(".write", VERIFIER_TEXT)
        self.assertNotIn("saveAsTable", VERIFIER_TEXT)
        for forbidden in ("INSERT", "DELETE", "MERGE", "UPDATE ", "DROP", "ALTER",
                          "VACUUM", "OPTIMIZE", "CREATE TABLE"):
            self.assertNotIn(forbidden, VERIFIER_TEXT.upper())

    def test_verifier_statements_are_select_or_describe_only(self):
        self.assertIn("DESCRIBE TABLE", VERIFIER_TEXT)
        for statement in ("SELECT", "FROM tt_ecommerce_synthetic."):
            self.assertIn(statement, VERIFIER_TEXT)
        # Fixed to the single DEMO-001 scope; no generic scope parameter.
        self.assertEqual(VERIFIER_TEXT.count("materialization_scope = 'DEMO-001'"), 1)

    def test_root_script_and_verifier_never_touch_the_raw_layer(self):
        for text in (ROOT_SCRIPT_TEXT, VERIFIER_TEXT):
            self.assertNotIn("bronze", text.lower())
            self.assertNotIn("ingestion_run_log", text.lower())
            self.assertNotIn("landing", text.lower())
            self.assertNotIn("checkpoint", text.lower())


class SilverBundleTopologyTests(unittest.TestCase):
    def test_bundle_includes_resources_and_syncs_silver_runtime(self):
        self.assertIn("- resources/*.yml", DATABRICKS_YML)
        self.assertIn("- scripts/run_silver_demo_001_job.py", DATABRICKS_YML)
        self.assertIn("- scripts/verify_silver_demo_001.py", DATABRICKS_YML)

    def test_silver_jobs_present_in_both_targets(self):
        self.assertEqual(DATABRICKS_YML.count("silver_demo_001_materialization_job:"), 2)
        self.assertEqual(DATABRICKS_YML.count(f"task_key: {MATERIALIZATION_TASK}"), 2)
        self.assertEqual(DATABRICKS_YML.count(f"task_key: {VERIFY_TASK}"), 2)

    def test_serverless_environment_wiring_is_conditional_to_target(self):
        serverless_block = DATABRICKS_YML.split("development_serverless:", 1)[1]
        serverless_block = serverless_block.split("development_classic:", 1)[0]
        silver_serverless = serverless_block.split("silver_demo_001_materialization_job:", 1)[1]
        self.assertIn("environment_key: ${var.environment_key}", silver_serverless)
        self.assertIn("environments:", silver_serverless)
        self.assertIn("spec:\n                client: \"1\"", silver_serverless)
        self.assertEqual(silver_serverless.count("environment_key: ${var.environment_key}"), 3)

        classic_block = DATABRICKS_YML.split("development_classic:", 1)[1]
        self.assertNotIn("environment_key", classic_block)
        self.assertNotIn("environments:", classic_block)
        silver_classic = classic_block.split("silver_demo_001_materialization_job:", 1)[1]
        self.assertIn("job_cluster_key: silver_classic", silver_classic)
        self.assertIn("data_security_mode: SINGLE_USER", silver_classic)
        self.assertIn("autotermination_minutes: 30", silver_classic)

    def test_no_third_party_dependencies_declared(self):
        self.assertNotIn("libraries", SILVER_JOB_YML)
        self.assertNotIn("pip install", SILVER_JOB_YML.lower())


if __name__ == "__main__":
    unittest.main()
