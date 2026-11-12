"""Promoción operatorial de un lote validado: `_staging` → landing final.

Fase A del cierre F2 (contrato D2-2, gate GF2-6). Solo stdlib + Azure CLI:

* `build_promotion_plan` valida el directorio local plano con
  `validate_manifest_batch` y produce un plan determinista, para auditoría,
  sin ninguna llamada a Azure.
* `publish_promotion` ejecuta el plan en orden exacto:
  verificaciones de ausencia → staging (manifest + fuentes, verificando
  descarga+SHA-256 tras cada upload) → manifest de control → tres archivos
  finales → ready marker (última operación del lote).
* Todo el acceso Azure pasa por `az storage fs` con `--account-name`,
  `--file-system landing` y `--auth-mode login` (directorios con `--name`,
  archivos con `--path`), con `--overwrite false` en upload y download, en
  argv plano (nunca `shell=True`, sin SDK, secretos ni bundles). La CLI se
  aísla tras `CommandRunner`, inyectable para pruebas sin `az`.
* Ante cualquier fallo: detener, no limpiar ni sobrescribir. Antes del intento
  final de `ready`, su ausencia queda garantizada; desde ese intento su estado
  remoto puede ser indeterminado y bloquea la ingesta hasta reconciliación
  humana. La siguiente ejecución del mismo batch encontrará la colisión y
  exigirá reconciliación; este módulo no muta nada ajeno a su plan.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Protocol, Sequence

from .contracts import BRONZE_CONTRACTS
from .layout import LandingLayout, validate_arrival_date, validate_batch_id
from .manifest import SourceReceipt, validate_manifest_batch


class PromotionError(RuntimeError):
    """Any promotion failure stops immediately; it never cleans or retries."""


class PromotionCollisionError(PromotionError):
    """A destination already exists: human reconciliation is required."""


class PromotionHashMismatchError(PromotionError):
    """An uploaded or copied object's SHA-256 diverged; abort before ready."""


class PromotionReadyIndeterminateError(PromotionError):
    """A publish-ready failure after the write attempt left the remote ready
    marker in an indeterminate state (written or not). Nothing is cleaned or
    overwritten; Bronze ingestion must stay blocked until a human reconciles
    the remote state with direct reads (`az storage fs file exists/download`)."""


class AzureCliError(PromotionError):
    """A real command failure is always an error, never an 'absent' answer."""


class CommandRunner(Protocol):
    """Boundary that isolates Azure CLI from testable promotion logic."""

    def run(self, argv: Sequence[str]) -> str: ...


class AzCliRunner:
    """Runs the real Azure CLI with an explicit argv list (shell disabled)."""

    def __init__(self, *, timeout_seconds: float = 600.0):
        self.timeout_seconds = timeout_seconds
        resolved = shutil.which("az")
        self.executable = resolved or "az"

    def run(self, argv: Sequence[str]) -> str:
        command = [self.executable, *argv]
        try:
            completed = subprocess.run(
                command,
                shell=False,
                capture_output=True,
                text=True,
                timeout=self.timeout_seconds,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise AzureCliError(f"azure cli invocation failed: {exc}") from exc
        if completed.returncode != 0:
            message = (completed.stderr or completed.stdout or "").strip()[:400]
            raise AzureCliError(
                f"azure cli exited with {completed.returncode}: {message}"
            )
        return completed.stdout


class LandingClient:
    """Narrow ADLS Gen2 adapter over `az storage fs` with locked auth flags."""

    def __init__(self, account_name: str, runner: CommandRunner, file_system: str = "landing"):
        if not account_name or len(account_name.strip()) == 0:
            raise PromotionError("account_name is required for publish")
        self.account_name = account_name.strip()
        self.file_system = file_system
        self._runner = runner

    def _argv(self, *command: str) -> list[str]:
        return [
            *command,
            "--account-name", self.account_name,
            "--file-system", self.file_system,
            "--auth-mode", "login",
        ]

    @staticmethod
    def _parse_exists(stdout: str) -> bool:
        try:
            payload = json.loads(stdout)
        except json.JSONDecodeError as exc:
            raise AzureCliError(f"unparseable exists payload: {stdout[:120]!r}") from exc
        if isinstance(payload, bool):
            return payload
        if isinstance(payload, dict) and isinstance(payload.get("exists"), bool):
            return payload["exists"]
        raise AzureCliError(f"unexpected exists payload: {stdout[:120]!r}")

    def directory_exists(self, relative_path: str) -> bool:
        output = self._runner.run(self._argv(
            "storage", "fs", "directory", "exists", "--name", relative_path,
        ))
        return self._parse_exists(output)

    def file_exists(self, relative_path: str) -> bool:
        output = self._runner.run(self._argv(
            "storage", "fs", "file", "exists", "--path", relative_path,
        ))
        return self._parse_exists(output)

    def create_directory(self, relative_path: str) -> None:
        self._runner.run(self._argv(
            "storage", "fs", "directory", "create", "--name", relative_path,
        ))

    def upload_file(self, local_path: Path, relative_path: str) -> None:
        self._runner.run(self._argv(
            "storage", "fs", "file", "upload",
            "--source", str(local_path), "--path", relative_path,
            "--overwrite", "false",
        ))

    def download_file(self, relative_path: str, destination: Path) -> Path:
        self._runner.run(self._argv(
            "storage", "fs", "file", "download",
            "--path", relative_path, "--destination", str(destination),
            "--overwrite", "false",
        ))
        return destination


def relative_to_landing(logical: str) -> str:
    """Strip exactly one `landing/` prefix and keep the path trivially safe.

    Every logical path in this module comes from `LandingLayout` over inputs
    validated for traversal (`batch_id`, `arrival_date`); this guard rejects
    anything that is not a plain slash path under the filesystem root.
    """
    prefix = "landing/"
    if not logical.startswith(prefix):
        raise PromotionError(f"logical path must be inside the landing filesystem: {logical!r}")
    relative = logical[len(prefix):]
    if not relative or relative.startswith("/") or relative.endswith("/"):
        raise PromotionError(f"unsafe landing-relative path: {logical!r}")
    for segment in relative.split("/"):
        if not segment or segment in (".", "..") or "\\" in segment or ":" in segment:
            raise PromotionError(f"unsafe landing path segment in {logical!r}")
    return relative


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def ready_marker_bytes(batch_id: str, arrival_date: str, manifest_sha256: str) -> bytes:
    """Deterministic final control artifact; written last, after everything."""
    payload = {
        "arrival_date": validate_arrival_date(arrival_date),
        "batch_id": validate_batch_id(batch_id),
        "manifest_sha256": manifest_sha256,
    }
    return (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


@dataclass(frozen=True)
class PromotionStep:
    """One auditable operation, both as `landing/...` logical path and as the
    filesystem-relative path inside the `landing` ADLS filesystem."""

    action: str
    target_logical: str
    target_relative: str
    detail: str


@dataclass(frozen=True)
class PromotionPlan:
    batch_id: str
    arrival_date: str
    manifest_sha256: str
    receipts: tuple[SourceReceipt, ...]
    steps: tuple[PromotionStep, ...]

    def step_signatures(self) -> tuple[tuple[str, str], ...]:
        return tuple((step.action, step.target_relative) for step in self.steps)

    def to_dict(self) -> dict:
        return {
            "batch_id": self.batch_id,
            "arrival_date": self.arrival_date,
            "manifest_sha256": self.manifest_sha256,
            "receipts": [
                {
                    "source": receipt.source,
                    "filename": receipt.filename,
                    "rows_declared": receipt.rows_declared,
                    "sha256": receipt.sha256,
                    "contract_version": receipt.contract_version,
                }
                for receipt in self.receipts
            ],
            "steps": [
                {
                    "action": step.action,
                    "target_logical": step.target_logical,
                    "target_relative": step.target_relative,
                    "detail": step.detail,
                }
                for step in self.steps
            ],
        }


def _directory_chain(relative_path: str, *, include_last: bool = False) -> list[str]:
    """Ascending ancestor directories; `include_last` adds the leaf segment
    itself (for directory paths). Duplication is resolved by the caller."""
    segments = relative_path.split("/")
    depth = len(segments) if include_last else len(segments) - 1
    return ["/".join(segments[:index + 1]) for index in range(depth)]


def build_promotion_plan(local_batch_dir: Path, batch_id: str, arrival_date: str) -> PromotionPlan:
    """Validate the local flat batch exactly like `data/sample/DEMO-001` and
    emit the deterministic publish order, with no Azure access."""
    batch_id = validate_batch_id(batch_id)
    arrival_date = validate_arrival_date(arrival_date)
    local_batch_dir = Path(local_batch_dir)
    receipts = validate_manifest_batch(local_batch_dir, batch_id)
    manifest_local = local_batch_dir / f"manifest_{batch_id}.json"
    manifest_sha256 = _sha256_file(manifest_local)

    layout = LandingLayout()
    staging_dir_logical = layout.staging_batch(batch_id)
    manifest_logical = layout.manifest(batch_id)
    ready_logical = layout.ready(batch_id)
    staging_dir = relative_to_landing(staging_dir_logical)
    manifest_relative = relative_to_landing(manifest_logical)
    ready_relative = relative_to_landing(ready_logical)

    steps: list[PromotionStep] = []
    planned_dirs: set[str] = set()

    def plan_ensure_directory(directory: str, detail: str) -> None:
        if directory in planned_dirs:
            return
        planned_dirs.add(directory)
        steps.append(PromotionStep(
            "ensure-directory", f"landing/{directory}", directory, detail))

    # Phase 0: every destination (staging dir, control manifest, three finals,
    # ready marker) must be absent; one collision aborts the whole publish.
    steps.append(PromotionStep(
        "verify-absent", staging_dir_logical, staging_dir,
        "staging batch directory must not exist before publish"))
    steps.append(PromotionStep(
        "verify-absent", manifest_logical, manifest_relative,
        "control manifest must not exist before publish"))
    for receipt in receipts:
        final_logical = layout.final_file(receipt.source, batch_id, arrival_date)
        steps.append(PromotionStep(
            "verify-absent", final_logical, relative_to_landing(final_logical),
            f"final file for {receipt.source} must not exist before publish"))
    steps.append(PromotionStep(
        "verify-absent", ready_logical, ready_relative,
        "ready marker must not exist before publish"))

    # Phase 1: isolated staging upload; manifest first, then contract order.
    for directory in _directory_chain(staging_dir, include_last=True):
        plan_ensure_directory(directory, "create the batch staging directory")
    manifest_name = manifest_relative.rsplit("/", 1)[-1]
    steps.append(PromotionStep(
        "upload-staging",
        f"{staging_dir_logical}/{manifest_name}",
        f"{staging_dir}/{manifest_name}",
        f"local={manifest_local}; sha256={manifest_sha256}"))
    steps.append(PromotionStep(
        "verify-remote-hash", f"{staging_dir_logical}/{manifest_name}",
        f"{staging_dir}/{manifest_name}",
        f"expected sha256={manifest_sha256}"))
    for receipt in receipts:
        staged_relative = f"{staging_dir}/{receipt.filename}"
        steps.append(PromotionStep(
            "upload-staging", f"{staging_dir_logical}/{receipt.filename}", staged_relative,
            f"local={local_batch_dir / receipt.filename}; sha256={receipt.sha256}"))
        steps.append(PromotionStep(
            "verify-remote-hash", f"{staging_dir_logical}/{receipt.filename}", staged_relative,
            f"expected sha256={receipt.sha256}"))

    # Phase 2: control manifest promotion, directories first, then the copy.
    for directory in _directory_chain(manifest_relative):
        plan_ensure_directory(directory, "create control directory")
    steps.append(PromotionStep(
        "promote-manifest", manifest_logical, manifest_relative,
        f"source={staging_dir}/{manifest_name}; expected sha256={manifest_sha256}"))

    # Phase 3: one final file per source; directories are explicit leaves.
    for receipt in receipts:
        final_logical = layout.final_file(receipt.source, batch_id, arrival_date)
        final_relative = relative_to_landing(final_logical)
        for directory in _directory_chain(final_relative):
            plan_ensure_directory(directory, f"create final directory for {receipt.source}")
        steps.append(PromotionStep(
            "promote-source", final_logical, final_relative,
            f"source={staging_dir}/{receipt.filename}; expected sha256={receipt.sha256}"))

    # Phase 4: ready marker is the very last write of the batch.
    for directory in _directory_chain(ready_relative):
        plan_ensure_directory(directory, "create ready directory")
    ready_content = ready_marker_bytes(batch_id, arrival_date, manifest_sha256)
    steps.append(PromotionStep(
        "publish-ready", ready_logical, ready_relative,
        f"expected sha256={hashlib.sha256(ready_content).hexdigest()}"))

    return PromotionPlan(
        batch_id=batch_id,
        arrival_date=arrival_date,
        manifest_sha256=manifest_sha256,
        receipts=receipts,
        steps=tuple(steps),
    )


def _verify_download(client: LandingClient, relative: str, expected_sha: str,
                     tag: str, temp_dir: Path) -> Path:
    destination = temp_dir / tag
    if destination.exists():
        destination = temp_dir / f"{len(list(temp_dir.iterdir())):04d}_{tag}"
    client.download_file(relative, destination)
    actual = _sha256_file(destination)
    if actual != expected_sha:
        raise PromotionHashMismatchError(
            f"sha256 mismatch after download of {relative}: expected {expected_sha}, got {actual}")
    return destination


def _execute_step(step: PromotionStep, client: LandingClient, *,
                  plan: PromotionPlan, local_batch_dir: Path, temp_dir: Path) -> dict:
    is_directory_target = step.action == "verify-absent" and step.target_relative == _staging_dir(plan)
    if step.action == "verify-absent":
        if is_directory_target:
            if client.directory_exists(step.target_relative):
                raise PromotionCollisionError(
                    f"staging destination already exists; human reconciliation required: {step.target_relative}")
        else:
            if client.file_exists(step.target_relative):
                raise PromotionCollisionError(
                    f"destination already exists; human reconciliation required: {step.target_relative}")
        return {"action": step.action, "target_relative": step.target_relative, "status": "ok"}
    if step.action == "ensure-directory":
        client.create_directory(step.target_relative)
        return {"action": step.action, "target_relative": step.target_relative, "status": "ok"}
    if step.action == "upload-staging":
        filename = step.target_relative.rsplit("/", 1)[-1]
        expected = (
            plan.manifest_sha256 if filename.startswith("manifest_")
            else _receipt(plan, filename).sha256
        )
        if client.file_exists(step.target_relative):
            raise PromotionCollisionError(
                f"destination already exists; human reconciliation required: {step.target_relative}")
        client.upload_file(local_batch_dir / filename, step.target_relative)
        return {"action": step.action, "target_relative": step.target_relative,
                "status": "ok", "sha256": expected}
    if step.action == "verify-remote-hash":
        expected = step.detail[len("expected sha256="):]
        if len(expected) != 64:
            raise PromotionError(f"malformed hash expectation in step: {step.detail!r}")
        tag = f"verify_{step.target_relative.rsplit('/', 1)[-1]}"
        _verify_download(client, step.target_relative, expected, tag, temp_dir)
        return {"action": step.action, "target_relative": step.target_relative,
                "status": "ok", "sha256": expected}
    if step.action == "promote-manifest":
        if client.file_exists(step.target_relative):
            raise PromotionCollisionError(
                f"destination already exists; human reconciliation required: {step.target_relative}")
        staged_manifest = f"{_staging_dir(plan)}/manifest_{plan.batch_id}.json"
        staged_path = _verify_download(client, staged_manifest,
                                       plan.manifest_sha256, "promote_manifest_source", temp_dir)
        client.upload_file(staged_path, step.target_relative)
        _verify_download(client, step.target_relative, plan.manifest_sha256,
                         "promote_manifest_verify", temp_dir)
        return {"action": step.action, "target_relative": step.target_relative,
                "status": "ok", "sha256": plan.manifest_sha256}
    if step.action == "promote-source":
        if client.file_exists(step.target_relative):
            raise PromotionCollisionError(
                f"destination already exists; human reconciliation required: {step.target_relative}")
        filename = step.target_relative.rsplit("/", 1)[-1]
        receipt = _receipt(plan, filename)
        staged_relative = next(
            s.target_relative for s in plan.steps
            if s.action == "upload-staging" and s.target_relative.endswith(f"/{filename}"))
        staged_path = _verify_download(client, staged_relative, receipt.sha256,
                                       f"promote_{receipt.source}", temp_dir)
        client.upload_file(staged_path, step.target_relative)
        _verify_download(client, step.target_relative, receipt.sha256,
                         f"verify_{receipt.source}", temp_dir)
        return {"action": step.action, "target_relative": step.target_relative,
                "status": "ok", "sha256": receipt.sha256}
    if step.action == "publish-ready":
        if client.file_exists(step.target_relative):
            raise PromotionCollisionError(
                f"destination already exists; human reconciliation required: {step.target_relative}")
        content = ready_marker_bytes(plan.batch_id, plan.arrival_date, plan.manifest_sha256)
        ready_file = temp_dir / "ready_marker.json"
        if ready_file.exists():
            raise PromotionError("local ready temp collision; aborting without publish")
        ready_file.write_bytes(content)
        # From the first write attempt onwards the remote ready state can no
        # longer be reasoned about locally: an Azure failure here may still
        # have persisted the marker, and a failed post-write verification may
        # hide a successful write from an earlier partial run.
        try:
            client.upload_file(ready_file, step.target_relative)
        except Exception as exc:
            raise PromotionReadyIndeterminateError(
                f"ready state indeterminate: the upload attempt for {step.target_relative} "
                "failed after being issued; the marker may or may not exist remotely. "
                "Nothing is cleaned or overwritten; ingestion stays blocked until a human "
                "reconciles the remote state with direct reads.") from exc
        try:
            downloaded = _verify_download(
                client, step.target_relative,
                hashlib.sha256(content).hexdigest(), "ready_marker", temp_dir)
            if downloaded.read_bytes() != content:
                raise PromotionHashMismatchError("ready marker content diverged after upload")
        except Exception as exc:
            raise PromotionReadyIndeterminateError(
                f"ready state indeterminate: post-upload verification for {step.target_relative} "
                "failed; the marker may exist with divergent content or may not exist. "
                "Nothing is cleaned or overwritten; ingestion stays blocked until a human "
                "reconciles the remote state with direct reads.") from exc
        return {"action": step.action, "target_relative": step.target_relative,
                "status": "ok", "sha256": hashlib.sha256(content).hexdigest()}
    raise PromotionError(f"unsupported promotion step action: {step.action}")


def _receipt(plan: PromotionPlan, filename: str) -> SourceReceipt:
    for receipt in plan.receipts:
        if receipt.filename == filename:
            return receipt
    raise PromotionError(f"no receipt for filename: {filename}")


def _staging_dir(plan: PromotionPlan) -> str:
    return relative_to_landing(LandingLayout().staging_batch(plan.batch_id))


def publish_promotion(plan: PromotionPlan, client: LandingClient, *,
                      local_batch_dir: Path,
                      on_event: Callable[[dict], None] | None = None) -> list[dict]:
    """Execute the plan in exact order; stop at the first failure.

    Guarantee boundaries:
      * A failure BEFORE the publish-ready step is attempted never writes the
        ready marker (it is the last step, and its write is guarded by
        existence checks that run just before the attempt).
      * A failure DURING publish-ready (upload attempt or its post-write
        download/SHA-256 verification) raises `PromotionReadyIndeterminateError`:
        the remote ready marker may or may not exist; nothing is cleaned or
        overwritten and Bronze ingestion stays blocked until a human
        reconciles via direct remote reads.

    Nothing is cleaned, overwritten or repaired remotely on any failure; a
    partially published batch must be reconciled by a human before re-running
    the same batch_id.
    """
    batch_dir = Path(local_batch_dir)
    temp_root = Path(tempfile.mkdtemp(prefix="tt-ecommerce-promotion-"))
    events: list[dict] = []

    def emit(event: dict) -> None:
        events.append(event)
        if on_event is not None:
            on_event(dict(event))

    try:
        for step in plan.steps:
            emit(_execute_step(step, client, plan=plan,
                               local_batch_dir=batch_dir, temp_dir=temp_root))
    except Exception:
        # Keep the temp dir for forensics; remote state stays untouched.
        raise
    shutil.rmtree(temp_root, ignore_errors=True)
    return events
