"""Promotion tests: plan, order, safety operators and simulation of Azure CLI.

The Azure CLI is never invoked: `LandingClient` runs with an injected
`CommandRunner` fake that records argv and simulates ADLS Gen2 semantics and
failures. All locked flags are asserted on the recorded commands.
"""

from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import shutil
import tempfile
import unittest
from pathlib import Path

from pipelines.bronze.promotion import (
    AzureCliError,
    LandingClient,
    PromotionCollisionError,
    PromotionHashMismatchError,
    PromotionReadyIndeterminateError,
    build_promotion_plan,
    publish_promotion,
    ready_marker_bytes,
    relative_to_landing,
)

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "data" / "sample" / "DEMO-001"
ACCOUNT = "sttecomdemostorage"
BATCH = "DEMO-001"
ARRIVAL = "2026-01-10"
READY_REL = f"_control/ready/{BATCH}.json"


def _fixture_copy(destination: Path) -> Path:
    """Copy the exact fixture bytes into a flat batch dir (byte-for-byte)."""
    copied = destination / BATCH
    copied.mkdir()
    for name in ("orders_DEMO-001.jsonl", "customers_DEMO-001.csv",
                 "products_DEMO-001.csv", "manifest_DEMO-001.json"):
        shutil.copyfile(FIXTURE / name, copied / name)
    return copied


class FakeLandingRunner:
    """Simulates `az storage fs` (argv lists only, shell is never touched) with
    a minimal ADLS filesystem: directories and files keyed by landing-relative
    path. It enforces the locked auth/overwrite flags on every command and the
    ADLS Gen2 argument shapes: `--name` for directories, `--path` for files."""

    def __init__(self, *, fail_when=None, mutate_download=None, fail_upload_after_write=()):
        self.dirs: set[str] = set()
        self.files: dict[str, bytes] = {}
        self.commands: list[list[str]] = []
        self._fail_when = fail_when or (lambda argv: False)
        self._mutate_download = mutate_download or (lambda path: False)
        self._fail_upload_after_write = set(fail_upload_after_write)

    @staticmethod
    def _flag(argv: list[str], name: str) -> str | None:
        if name not in argv:
            return None
        return argv[argv.index(name) + 1]

    def run(self, argv) -> str:
        argv = [str(argument) for argument in argv]
        if argv[0] != "az":
            raise AzureCliError(f"only 'az' commands are expected, got {argv[0]!r}")
        for name, expected in (("--file-system", "landing"), ("--auth-mode", "login")):
            if self._flag(argv, name) != expected:
                raise AzureCliError(f"command missing locked flag {name}={expected}")
        if not self._flag(argv, "--account-name"):
            raise AzureCliError("command missing --account-name")
        resource, operation = argv[3], argv[4]
        # ADLS Gen2 argument shape: directories select by --name, files by --path.
        if resource == "directory" and "--path" in argv:
            raise AzureCliError("directory commands must target with --name, not --path")
        if resource == "file" and "--name" in argv:
            raise AzureCliError("file commands must target with --path, not --name")
        object_path = self._flag(argv, "--name" if resource == "directory" else "--path")
        if not object_path:
            raise AzureCliError("command missing its target argument")
        self.commands.append(list(argv))
        if self._fail_when(list(argv)):
            raise AzureCliError(f"simulated az failure: {' '.join(argv[:6])}")
        if operation == "exists":
            if resource == "file" and "--overwrite" in argv:
                raise AzureCliError("file exists must not pass --overwrite")
            exists = object_path in (self.dirs if resource == "directory" else self.files)
            return json.dumps({"exists": exists})
        if operation == "create":
            self.dirs.add(object_path)
            return ""
        if operation == "upload":
            if self._flag(argv, "--overwrite") != "false":
                raise AzureCliError("upload must run with --overwrite false")
            data = Path(self._flag(argv, "--source")).read_bytes()
            if object_path in self.files:
                raise AzureCliError(f"simulated conflict: {object_path} already exists")
            self.files[object_path] = data
            if object_path in self._fail_upload_after_write:
                # Simulates a transient Azure failure that surfaces AFTER the
                # service already persisted the object: the client cannot know.
                raise AzureCliError(f"simulated default-timeout after write: {object_path}")
            return ""
        if operation == "download":
            if self._flag(argv, "--overwrite") != "false":
                raise AzureCliError("download must run with --overwrite false")
            if object_path not in self.files:
                raise AzureCliError(f"simulated PathNotFound: {object_path}")
            data = self.files[object_path]
            if self._mutate_download(object_path):
                data = data + b"\n"
            Path(self._flag(argv, "--destination")).write_bytes(data)
            return ""
        raise AzureCliError(f"fake does not support operation: {' '.join(argv[:6])}")

    def has_upload_for(self, fragment: str) -> bool:
        return any("upload" in argv and any(fragment in argument for argument in argv)
                   for argv in self.commands)


def _publish(plan, runner, local_dir):
    client = LandingClient(ACCOUNT, runner)
    return publish_promotion(plan, client, local_batch_dir=local_dir)


class PromotionPlanTests(unittest.TestCase):
    def _plan(self, temp_root: str | None = None):
        temp_root = temp_root or tempfile.mkdtemp(prefix="plan-test-")
        local_dir = _fixture_copy(Path(temp_root))
        return build_promotion_plan(local_dir, BATCH, ARRIVAL)

    def test_plan_is_deterministic_and_order_is_exact(self):
        tmp1, tmp2 = tempfile.mkdtemp(), tempfile.mkdtemp()
        plan1, plan2 = self._plan(tmp1), self._plan(tmp2)
        self.assertEqual(plan1.step_signatures(), plan2.step_signatures())
        expected = [
            ("verify-absent", "_staging/DEMO-001"),
            ("verify-absent", f"_control/manifests/{BATCH}/manifest_{BATCH}.json"),
            ("verify-absent", f"orders/arrival_date={ARRIVAL}/batch_id={BATCH}/orders_{BATCH}.jsonl"),
            ("verify-absent", f"customers/arrival_date={ARRIVAL}/batch_id={BATCH}/customers_{BATCH}.csv"),
            ("verify-absent", f"products/arrival_date={ARRIVAL}/batch_id={BATCH}/products_{BATCH}.csv"),
            ("verify-absent", f"_control/ready/{BATCH}.json"),
            ("ensure-directory", "_staging"),
            ("ensure-directory", "_staging/DEMO-001"),
            ("upload-staging", f"_staging/{BATCH}/manifest_{BATCH}.json"),
            ("verify-remote-hash", f"_staging/{BATCH}/manifest_{BATCH}.json"),
            ("upload-staging", f"_staging/{BATCH}/orders_{BATCH}.jsonl"),
            ("verify-remote-hash", f"_staging/{BATCH}/orders_{BATCH}.jsonl"),
            ("upload-staging", f"_staging/{BATCH}/customers_{BATCH}.csv"),
            ("verify-remote-hash", f"_staging/{BATCH}/customers_{BATCH}.csv"),
            ("upload-staging", f"_staging/{BATCH}/products_{BATCH}.csv"),
            ("verify-remote-hash", f"_staging/{BATCH}/products_{BATCH}.csv"),
            ("ensure-directory", "_control"),
            ("ensure-directory", "_control/manifests"),
            ("ensure-directory", f"_control/manifests/{BATCH}"),
            ("promote-manifest", f"_control/manifests/{BATCH}/manifest_{BATCH}.json"),
            ("ensure-directory", "orders"),
            ("ensure-directory", f"orders/arrival_date={ARRIVAL}"),
            ("ensure-directory", f"orders/arrival_date={ARRIVAL}/batch_id={BATCH}"),
            ("promote-source", f"orders/arrival_date={ARRIVAL}/batch_id={BATCH}/orders_{BATCH}.jsonl"),
            ("ensure-directory", "customers"),
            ("ensure-directory", f"customers/arrival_date={ARRIVAL}"),
            ("ensure-directory", f"customers/arrival_date={ARRIVAL}/batch_id={BATCH}"),
            ("promote-source", f"customers/arrival_date={ARRIVAL}/batch_id={BATCH}/customers_{BATCH}.csv"),
            ("ensure-directory", "products"),
            ("ensure-directory", f"products/arrival_date={ARRIVAL}"),
            ("ensure-directory", f"products/arrival_date={ARRIVAL}/batch_id={BATCH}"),
            ("promote-source", f"products/arrival_date={ARRIVAL}/batch_id={BATCH}/products_{BATCH}.csv"),
            ("ensure-directory", "_control/ready"),
            ("publish-ready", f"_control/ready/{BATCH}.json"),
        ]
        self.assertEqual(plan1.step_signatures(), tuple(expected))
        self.assertEqual(plan1.steps[-1].action, "publish-ready")

    def test_plan_validates_fixture_via_receipts(self):
        plan = self._plan(tempfile.mkdtemp(prefix="receipts-test-"))
        self.assertEqual([r.source for r in plan.receipts], ["orders", "customers", "products"])
        self.assertEqual([r.rows_declared for r in plan.receipts], [26, 11, 7])
        self.assertEqual(sum(r.rows_declared for r in plan.receipts), 44)
        self.assertEqual(len(plan.manifest_sha256), 64)
        self.assertEqual(plan.receipts[0].contract_version, "v1")

    def test_plan_rejects_changed_fixture_bytes(self):
        temp = Path(tempfile.mkdtemp(prefix="bad-fixture-"))
        local_dir = _fixture_copy(temp)
        target = local_dir / f"orders_{BATCH}.jsonl"
        target.write_bytes(target.read_bytes() + b"\n")
        with self.assertRaises(ValueError):
            build_promotion_plan(local_dir, BATCH, ARRIVAL)

    def test_unsafe_arguments_are_rejected_by_layout_validation(self):
        temp = tempfile.mkdtemp(prefix="unsafe-args-")
        local_dir = _fixture_copy(Path(temp))
        with self.assertRaises(ValueError):
            build_promotion_plan(local_dir, "../bad", ARRIVAL)
        with self.assertRaises(ValueError):
            build_promotion_plan(local_dir, BATCH, "2026-13-01")
        with self.assertRaises(ValueError):
            build_promotion_plan(local_dir, BATCH, "2026-01-10/../x")
        self.assertEqual(LandingClient.__name__, "LandingClient")

    def test_logical_paths_split_from_landing_filesystem_paths(self):
        plan = self._plan(tempfile.mkdtemp(prefix="paths-split-"))
        for step in plan.steps:
            self.assertTrue(step.target_logical.startswith("landing/"))
            self.assertFalse(step.target_relative.startswith("landing"))
            self.assertEqual(step.target_relative, step.target_logical[len("landing/"):])
            self.assertNotEqual(step.target_logical, step.target_relative)
            self.assertNotIn("\\", step.target_relative)
        with self.assertRaises(Exception):
            relative_to_landing("other_fs/x")
        with self.assertRaises(Exception):
            relative_to_landing("landing//x")
        with self.assertRaises(Exception):
            relative_to_landing("landing/../x")
        self.assertEqual(relative_to_landing("landing/_staging/x.json"), "_staging/x.json")


def _publish_failure_case(test_case, fail_when, mutate_download=None, expect=Exception):
    runner = FakeLandingRunner(fail_when=fail_when, mutate_download=mutate_download)
    temp = tempfile.mkdtemp(prefix="publish-fail-")
    local_dir = _fixture_copy(Path(temp))
    plan = build_promotion_plan(local_dir, BATCH, ARRIVAL)
    with test_case.assertRaises(expect):
        _publish(plan, runner, local_dir)
    return runner, plan


class PromotionPublishTests(unittest.TestCase):
    def test_publish_completes_with_ready_marker_last_verified(self):
        runner = FakeLandingRunner()
        temp = tempfile.mkdtemp(prefix="happy-")
        local_dir = _fixture_copy(Path(temp))
        plan = build_promotion_plan(local_dir, BATCH, ARRIVAL)
        events = _publish(plan, runner, local_dir)
        self.assertTrue(events)
        self.assertEqual(events[-1]["action"], "publish-ready")
        ready_rel = f"_control/ready/{BATCH}.json"
        self.assertIn(ready_rel, runner.files)
        self.assertEqual(runner.commands[-1][4], "download")
        self.assertEqual(runner.commands[-1][runner.commands[-1].index("--path") + 1], ready_rel)
        marker = json.loads(runner.files[ready_rel].decode("utf-8"))
        self.assertEqual(marker["batch_id"], BATCH)
        self.assertEqual(marker["manifest_sha256"], plan.manifest_sha256)
        self.assertEqual(runner.files[ready_rel], ready_marker_bytes(
            BATCH, ARRIVAL, plan.manifest_sha256))
        for receipt in plan.receipts:
            staged_rel = f"_staging/{BATCH}/{receipt.filename}"
            self.assertIn(staged_rel, runner.files)
        self.assertIn(f"_control/manifests/{BATCH}/manifest_{BATCH}.json", runner.files)

    def test_publish_copies_final_files_equal_to_staging_uploads(self):
        runner = FakeLandingRunner()
        temp = tempfile.mkdtemp(prefix="equal-")
        local_dir = _fixture_copy(Path(temp))
        plan = build_promotion_plan(local_dir, BATCH, ARRIVAL)
        _publish(plan, runner, local_dir)
        from pipelines.bronze.layout import LandingLayout
        layout = LandingLayout()
        for receipt in plan.receipts:
            final_rel = relative_to_landing(layout.final_file(receipt.source, BATCH, ARRIVAL))
            staged_rel = f"_staging/{BATCH}/{receipt.filename}"
            self.assertEqual(runner.files[final_rel], runner.files[staged_rel])

    def test_preexisting_destination_rejected_without_upload(self):
        runner = FakeLandingRunner()
        runner.files[f"_control/ready/{BATCH}.json"] = b'{"batch_id":"other"}\n'
        temp = tempfile.mkdtemp(prefix="collision-ready-")
        local_dir = _fixture_copy(Path(temp))
        plan = build_promotion_plan(local_dir, BATCH, ARRIVAL)
        with self.assertRaises(PromotionCollisionError) as caught:
            _publish(plan, runner, local_dir)
        self.assertIn("reconciliation", str(caught.exception))
        self.assertFalse(runner.has_upload_for(""))

    def test_preexisting_staging_collision_rejected(self):
        runner = FakeLandingRunner()
        runner.dirs.add(f"_staging/{BATCH}")
        temp = tempfile.mkdtemp(prefix="collision-staging-")
        local_dir = _fixture_copy(Path(temp))
        plan = build_promotion_plan(local_dir, BATCH, ARRIVAL)
        with self.assertRaises(PromotionCollisionError):
            _publish(plan, runner, local_dir)
        self.assertTrue(all("upload" not in argv for argv in runner.commands))
        self.assertTrue(all("download" not in argv for argv in runner.commands))

    def test_exists_command_error_is_error_not_absence(self):
        runner, plan = _publish_failure_case(
            self,
            fail_when=lambda argv: argv[4] == "exists",
            expect=AzureCliError,
        )
        self.assertTrue(all("upload" not in argv and "download" not in argv
                            for argv in runner.commands))

    def test_remote_hash_divergence_aborts_before_ready(self):
        runner, plan = _publish_failure_case(
            self,
            fail_when=None,
            mutate_download=lambda path: path.startswith(f"orders/arrival_date={ARRIVAL}/"),
            expect=PromotionHashMismatchError,
        )
        ready_marker_rel = f"_control/ready/{BATCH}.json"
        self.assertFalse(runner.has_upload_for(ready_marker_rel))
        self.assertTrue(all(
            not ("download" in argv and FakeLandingRunner._flag(argv, "--path") == ready_marker_rel)
            for argv in runner.commands))
        self.assertNotIn(ready_marker_rel, runner.files)
        # Prior steps still happened: staging & control manifest got uploaded.
        self.assertIn(f"_control/manifests/{BATCH}/manifest_{BATCH}.json", runner.files)
        self.assertTrue(runner.has_upload_for(f"_staging/{BATCH}/manifest_{BATCH}.json"))
        # Later final files never attempted.
        self.assertFalse(runner.has_upload_for(f"customers/arrival_date={ARRIVAL}"))
        self.assertFalse(runner.has_upload_for(f"products/arrival_date={ARRIVAL}"))

    def test_intermediate_failure_never_publishes_ready(self):
        runner, plan = _publish_failure_case(
            self,
            fail_when=lambda argv: "upload" in argv
            and any(f"customers_{BATCH}.csv" in argument for argument in argv)
            and any("_staging" in argument for argument in argv),
            expect=AzureCliError,
        )
        self.assertNotIn(f"_control/ready/{BATCH}.json", runner.files)
        self.assertNotIn(f"_control/manifests/{BATCH}/manifest_{BATCH}.json", runner.files)
        self.assertIn(f"_staging/{BATCH}/manifest_{BATCH}.json", runner.files)
        self.assertIn(f"_staging/{BATCH}/orders_{BATCH}.jsonl", runner.files)
        self.assertTrue(all("download" not in argv or "--path" not in argv
                            or argv[argv.index("--path") + 1] != f"_control/ready/{BATCH}.json"
                            for argv in runner.commands))

    def test_locked_flags_present_on_every_azure_command(self):
        runner = FakeLandingRunner()
        temp = tempfile.mkdtemp(prefix="flags-")
        local_dir = _fixture_copy(Path(temp))
        plan = build_promotion_plan(local_dir, BATCH, ARRIVAL)
        _publish(plan, runner, local_dir)
        self.assertTrue(runner.commands)
        for argv in runner.commands:
            self.assertEqual(argv[:3], ["az", "storage", "fs"])
            self.assertIn("--account-name", argv)
            self.assertEqual(FakeLandingRunner._flag(argv, "--account-name"), ACCOUNT)
            self.assertIn("--file-system", argv)
            self.assertEqual(FakeLandingRunner._flag(argv, "--file-system"), "landing")
            self.assertIn("--auth-mode", argv)
            self.assertEqual(FakeLandingRunner._flag(argv, "--auth-mode"), "login")
            if "upload" in argv:
                self.assertIn("--overwrite", argv)
                self.assertEqual(FakeLandingRunner._flag(argv, "--overwrite"), "false")
                self.assertIn("--source", argv)
            if "download" in argv:
                self.assertIn("--destination", argv)

    def test_publish_is_injected_and_never_uses_shell(self):
        runner = FakeLandingRunner()
        temp = tempfile.mkdtemp(prefix="inject-")
        local_dir = _fixture_copy(Path(temp))
        plan = build_promotion_plan(local_dir, BATCH, ARRIVAL)
        client = LandingClient(ACCOUNT, runner)
        publish_promotion(plan, client, local_batch_dir=local_dir)
        for argv in runner.commands:
            self.assertIsInstance(argv, list)

    def test_adls_directory_commands_use_name_and_file_commands_use_path(self):
        runner = FakeLandingRunner()
        temp = tempfile.mkdtemp(prefix="arg-shape-")
        local_dir = _fixture_copy(Path(temp))
        plan = build_promotion_plan(local_dir, BATCH, ARRIVAL)
        _publish(plan, runner, local_dir)
        saw_directory = saw_file = False
        for argv in runner.commands:
            resource = argv[3]
            if resource == "directory":
                saw_directory = True
                self.assertIn("--name", argv)
                self.assertNotIn("--path", argv)
                self.assertEqual(FakeLandingRunner._flag(argv, "--name"), argv[argv.index("--name") + 1])
            else:
                saw_file = True
                self.assertIn("--path", argv)
                self.assertNotIn("--name", argv)
        self.assertTrue(saw_directory)
        self.assertTrue(saw_file)

    def test_file_download_carries_overwrite_false(self):
        runner = FakeLandingRunner()
        temp = tempfile.mkdtemp(prefix="download-flag-")
        local_dir = _fixture_copy(Path(temp))
        plan = build_promotion_plan(local_dir, BATCH, ARRIVAL)
        _publish(plan, runner, local_dir)
        downloads = [argv for argv in runner.commands if argv[4] == "download"]
        self.assertTrue(downloads)
        for argv in downloads:
            self.assertIn("--overwrite", argv)
            self.assertEqual(FakeLandingRunner._flag(argv, "--overwrite"), "false")

    def test_ready_upload_failure_leaves_ready_indeterminate(self):
        runner = FakeLandingRunner(fail_upload_after_write={READY_REL})
        temp = tempfile.mkdtemp(prefix="ready-indet-upload-")
        local_dir = _fixture_copy(Path(temp))
        plan = build_promotion_plan(local_dir, BATCH, ARRIVAL)
        with self.assertRaises(PromotionReadyIndeterminateError) as caught:
            _publish(plan, runner, local_dir)
        message = str(caught.exception)
        self.assertIn("indeterminate", message)
        self.assertIn("reconcil", message.replace("reconciles", "reconcil"))
        # The service side persisted the marker: the local guarantee stops here.
        self.assertIn(READY_REL, runner.files)
        persisted = json.loads(runner.files[READY_REL].decode("utf-8"))
        self.assertEqual(persisted["batch_id"], BATCH)
        self.assertEqual(persisted["manifest_sha256"], plan.manifest_sha256)
        # Nothing is cleaned or overwritten afterwards: the failed upload is
        # the very last command recorded.
        self.assertEqual(runner.commands[-1][4], "upload")
        last_path = runner.commands[-1][runner.commands[-1].index("--path") + 1]
        self.assertEqual(last_path, READY_REL)

    def test_ready_verification_failure_leaves_ready_indeterminate(self):
        runner = FakeLandingRunner(mutate_download=lambda path: path == READY_REL)
        temp = tempfile.mkdtemp(prefix="ready-indet-verify-")
        local_dir = _fixture_copy(Path(temp))
        plan = build_promotion_plan(local_dir, BATCH, ARRIVAL)
        with self.assertRaises(PromotionReadyIndeterminateError) as caught:
            _publish(plan, runner, local_dir)
        message = str(caught.exception)
        self.assertIn("indeterminate", message)
        # The upload succeeded (canonical bytes persisted); only the post-write
        # verification diverged, so no fix-up is attempted.
        self.assertIn(READY_REL, runner.files)
        persisted = json.loads(runner.files[READY_REL].decode("utf-8"))
        self.assertEqual(persisted["batch_id"], BATCH)
        self.assertEqual(persisted["manifest_sha256"], plan.manifest_sha256)
        self.assertEqual(runner.commands[-1][4], "download")
        self.assertEqual(runner.commands[-1][runner.commands[-1].index("--path") + 1], READY_REL)
        ready_uploads = sum(1 for argv in runner.commands
                            if "upload" in argv and any(READY_REL in argument for argument in argv))
        self.assertEqual(ready_uploads, 1)  # no fix-up overwrite attempts


class PromoteBatchCliTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        module_path = ROOT / "scripts" / "promote_batch.py"
        spec = importlib.util.spec_from_file_location("promote_batch", module_path)
        cls.cli = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.cli)

    def test_plan_cli_prints_legible_json_without_azure(self):
        temp = tempfile.mkdtemp(prefix="cli-plan-")
        local_dir = _fixture_copy(Path(temp))
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            exit_code = self.cli.main([
                "plan", "--batch-id", BATCH, "--arrival-date", ARRIVAL,
                "--local-batch-dir", str(local_dir),
            ])
        self.assertEqual(exit_code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["batch_id"], BATCH)
        self.assertTrue(payload["steps"])
        for step in payload["steps"]:
            self.assertIn("target_relative", step)
            self.assertIn("target_logical", step)
            self.assertTrue(step["target_logical"].startswith("landing/"))
            self.assertFalse(step["target_relative"].startswith("landing"))
        self.assertEqual([r["rows_declared"] for r in payload["receipts"]], [26, 11, 7])

    def test_publish_cli_requires_account_name(self):
        temp = tempfile.mkdtemp(prefix="cli-publish-")
        local_dir = _fixture_copy(Path(temp))
        with self.assertRaises(SystemExit):
            self.cli.main([
                "publish", "--batch-id", BATCH, "--arrival-date", ARRIVAL,
                "--local-batch-dir", str(local_dir),
            ])

    def test_cli_reports_local_errors_nonzero(self):
        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            exit_code = self.cli.main([
                "plan", "--batch-id", BATCH, "--arrival-date", ARRIVAL,
                "--local-batch-dir", str(ROOT / "data" / "unknown-batch"),
            ])
        self.assertNotEqual(exit_code, 0)
        payload = json.loads(stderr.getvalue())
        self.assertIn("message", payload)


if __name__ == "__main__":
    unittest.main()
