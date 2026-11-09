"""Declarative Automation Bundle static tests (text assertions only).

No Databricks remote interaction, no deploy, no run: the bundle YAML is
asserted on its own text so secret leakage, schedules, concurrency/retry and
script wiring cannot drift silently. Static CLI validation
(`databricks bundle validate -t <target> ...`) is an operator command
(documented in the runbook), never part of the unittest suite.
"""

from __future__ import annotations

import re
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATABRICKS_YML = (ROOT / "databricks.yml").read_text(encoding="utf-8")
JOB_YML = (ROOT / "resources" / "bronze_job.yml").read_text(encoding="utf-8")
ROOT_SCRIPT_TEXT = (ROOT / "scripts" / "run_bronze_job.py").read_text(encoding="utf-8")
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


class BronzeWorkspaceRootBootstrapTests(unittest.TestCase):
    """The internal workspace-root bootstrap argument is fixed task config."""

    def test_task_passes_fixed_internal_pipelines_root(self):
        self.assertIn("--pipelines-root=${workspace.file_path}", JOB_YML)

    def test_internal_root_is_never_a_run_editable_parameter(self):
        self.assertNotIn("name: pipelines_root", JOB_YML)
        self.assertNotIn("{{job.parameters.pipelines_root}}", JOB_YML)

    def test_internal_root_argument_appears_exactly_once(self):
        self.assertEqual(
            JOB_YML.count("--pipelines-root=${workspace.file_path}"), 1,
        )

    def test_job_level_parameters_remain_only_batch_and_date(self):
        parameters_block = JOB_YML.split("parameters:", 1)[1].split("run_as:", 1)[0]
        self.assertIn("name: batch_id", parameters_block)
        self.assertIn("name: arrival_date", parameters_block)
        for forbidden in ("pipelines_root", "landing_root"):
            self.assertNotIn(f"name: {forbidden}:", parameters_block)
            self.assertNotIn("{{job.parameters." + forbidden + "}}", JOB_YML)


class RootScriptNoFilePathDiscoveryTests(unittest.TestCase):
    """The workspace script must bootstrap purely from the internal argument."""

    def test_script_has_no_file_or_argv0_or_environment_discovery(self):
        self.assertNotIn("__file__", ROOT_SCRIPT_TEXT)
        self.assertNotIn("sys.argv[0]", ROOT_SCRIPT_TEXT)
        self.assertNotIn("os.environ", ROOT_SCRIPT_TEXT)

    def test_script_consumes_validates_and_ranks_internal_root(self):
        self.assertIn("_INTERNAL_ROOT_ARG = \"--pipelines-root\"", ROOT_SCRIPT_TEXT)
        self.assertIn("(root / \"pipelines\").is_dir()", ROOT_SCRIPT_TEXT)
        self.assertIn("is_absolute", ROOT_SCRIPT_TEXT)
        self.assertIn("def ensure_pipelines_importable", ROOT_SCRIPT_TEXT)
        self.assertIn("sys.path.insert(0, text)", ROOT_SCRIPT_TEXT)


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


_FAKE_ENTRYPOINT = (
    "import sys\n"
    "\n"
    "PARSED_ARGV: list = []\n"
    "\n"
    "def parse_job_args(argv=None):\n"
    "    argv = list(sys.argv) if argv is None else argv\n"
    "    PARSED_ARGV[:] = argv\n"
    "    return \"parsed\"\n"
)


def _is_repo_local_path_entry(entry: str) -> bool:
    """True for `sys.path` entries that would resolve the REAL repository tree."""
    if entry in ("", "."):
        return True
    try:
        return Path(entry).resolve() == ROOT
    except OSError:
        return False


class _WorkspaceExec:
    """Replicates the Databricks workspace `exec` frame for the root script.

    The executing namespace deliberately has NO `__file__`, and the
    interpreter state is snapshotted with repo-local `sys.path` entries and
    `pipelines*` modules removed, so ONLY the explicit internal bootstrap
    root can resolve `pipelines` while the script runs.
    """

    def __init__(self) -> None:
        self.namespace: dict = {}

    def __enter__(self) -> "_WorkspaceExec":
        self._saved_path = list(sys.path)
        self._saved_argv = list(sys.argv)
        self._purged = {
            name: sys.modules.pop(name)
            for name in list(sys.modules)
            if name == "pipelines" or name.startswith("pipelines.")
        }
        sys.path[:] = [entry for entry in self._saved_path
                       if not _is_repo_local_path_entry(entry)]
        self._compiled = compile(ROOT_SCRIPT_TEXT, "<workspace exec>", "exec")
        return self

    def set_argv(self, argv: list[str]) -> None:
        sys.argv[:] = argv

    def run_script(self) -> dict:
        exec(self._compiled, self.namespace)  # noqa: S102 (intentional: workspace exec contract)
        return self.namespace

    def __exit__(self, *exc) -> bool:
        # Drop any `pipelines` module imported inside the exec frame (the
        # fake workspace package), then restore the originals for later tests.
        for name in [n for n in list(sys.modules)
                     if (n == "pipelines" or n.startswith("pipelines."))
                     and n not in self._purged]:
            sys.modules.pop(name, None)
        sys.modules.update(self._purged)
        sys.path[:] = self._saved_path
        sys.argv[:] = self._saved_argv
        return False


class WorkspaceExecBootstrapBehaviorTests(unittest.TestCase):
    """The root script bootstraps from the explicit root, NEVER from `__file__`."""

    def _workspace_root(self, *, with_pipelines: bool) -> Path:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        root = Path(tmp.name).resolve()
        if with_pipelines:
            (root / "pipelines" / "bronze").mkdir(parents=True)
            for rel in ("pipelines/__init__.py", "pipelines/bronze/__init__.py"):
                (root / rel).write_text("", encoding="utf-8")
            (root / "pipelines" / "bronze" / "entrypoint.py").write_text(
                _FAKE_ENTRYPOINT, encoding="utf-8")
        return root

    def _job_args(self) -> list[str]:
        return [
            "--batch-id=B1",
            "--arrival-date=2026-01-01",
            "--landing-root=abfss://landing@acct.dfs.core.windows.net",
        ]

    def _exec_failing(self, argv: list[str], expected_message: str):
        with _WorkspaceExec() as env:
            env.set_argv(argv)
            with self.assertRaises(SystemExit) as assertion:
                env.run_script()
            self.assertIn(expected_message, str(assertion.exception.code))
            # Still inside the snapshot: the failure happened BEFORE any
            # `pipelines` import attempt (purged at snapshot time, not back).
            self.assertNotIn("pipelines", sys.modules)

    def test_exec_without_file_global_uses_explicit_root_and_strips_internal_arg(self):
        root = self._workspace_root(with_pipelines=True)
        job_args = self._job_args()
        argv = ["run_bronze_job", f"--pipelines-root={root}", *job_args]
        with _WorkspaceExec() as env:
            env.set_argv(argv)
            namespace = env.run_script()
            # Workspace `exec` contract: the executing namespace has no
            # file-path global, and no repo-local sys.path entry exists here.
            self.assertNotIn("__file__", namespace)
            # The explicit root is ranked first for `pipelines` imports.
            self.assertEqual(sys.path[0], str(root))
            entrypoint = sys.modules["pipelines.bronze.entrypoint"]
            origin = getattr(entrypoint.__spec__, "origin", None)
            self.assertIsNotNone(origin)
            self.assertTrue(str(origin).startswith(str(root)),
                            msg=f"unexpected `pipelines` origin: {origin}")
            # The internal argument is stripped from sys.argv BEFORE
            # parse_job_args runs: the job CLI contract still sees only the
            # batch/date/landing arguments.
            self.assertEqual(sys.argv, ["run_bronze_job", *job_args])
            self.assertEqual(namespace["parse_job_args"](), "parsed")
            self.assertEqual(entrypoint.PARSED_ARGV, ["run_bronze_job", *job_args])

    def test_duplicated_identical_internal_root_is_accepted_and_stripped(self):
        root = self._workspace_root(with_pipelines=True)
        job_args = self._job_args()
        argv = ["run_bronze_job", f"--pipelines-root={root}",
                f"--pipelines-root={root}", *job_args]
        with _WorkspaceExec() as env:
            env.set_argv(argv)
            namespace = env.run_script()
            self.assertEqual(sys.argv, ["run_bronze_job", *job_args])
            self.assertEqual(namespace["parse_job_args"](), "parsed")

    def test_missing_internal_root_fails_before_pipelines_import(self):
        # Even with a perfectly synced root, a direct (non-bundle) start must
        # be refused BEFORE any `pipelines` import.
        self._workspace_root(with_pipelines=True)
        argv = ["run_bronze_job", *self._job_args()]
        self._exec_failing(argv, "missing internal")

    def test_relative_internal_root_is_rejected_before_pipelines_import(self):
        argv = ["run_bronze_job", "--pipelines-root=relative/root",
                *self._job_args()]
        self._exec_failing(argv, "absolute workspace path")

    def test_root_without_pipelines_directory_fails_safely(self):
        root = self._workspace_root(with_pipelines=False)
        argv = ["run_bronze_job", f"--pipelines-root={root}", *self._job_args()]
        self._exec_failing(argv, "'pipelines' directory")

    def test_conflicting_internal_roots_are_rejected(self):
        first = self._workspace_root(with_pipelines=False)
        second = self._workspace_root(with_pipelines=False)
        argv = ["run_bronze_job", f"--pipelines-root={first}",
                f"--pipelines-root={second}"]
        self._exec_failing(argv, "conflicting")

    def test_bare_internal_argument_without_value_is_rejected(self):
        root = self._workspace_root(with_pipelines=True)
        argv = ["run_bronze_job", "--pipelines-root", str(root),
                *self._job_args()]
        self._exec_failing(argv, "must be given as")


if __name__ == "__main__":
    unittest.main()
