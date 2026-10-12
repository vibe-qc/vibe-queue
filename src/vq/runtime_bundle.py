"""Portable runtime artifacts, separate from legacy source-SHA venv slots.

Product builders own compilation and relocation. This local file transport
packs, authenticates and deploys their output without any build fallback.
Trust is an operator-maintained allowlist of exact manifest digests, obtained
out of band, not a checksum file shipped alongside an untrusted download.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import re
import shutil
import signal
import stat
import subprocess
import tarfile
import tempfile
from collections.abc import Iterator
from pathlib import Path, PurePosixPath
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from vq._storage import atomic_write_text
from vq.paths import require_test_path_within_sandbox
from vq.runtime_slots import layout

Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
Text = Annotated[str, Field(min_length=1, max_length=1024)]
_MAX_FILES = 10_000
_MAX_BYTES = 64 * 1024**3


class BundleError(ValueError):
    """An artifact or target failed a required gate."""


class BundleModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class TargetProfile(BundleModel):
    """Operator-qualified compute-node facts; not login-node autodetection."""

    os: Text
    arch: Text
    isa: list[Text]
    # Exact ABI tokens are conservative: no undocumented version ordering.
    # Use explicit "none" for static/no-Python/no-MPI/no-BLAS requirements.
    abi: dict[Text, Text]
    modules: list[Text] = Field(default_factory=list)

    @model_validator(mode="after")
    def complete_abi(self) -> TargetProfile:
        required = {"libc", "python", "blas", "mpi", "launcher"}
        if not required.issubset(self.abi):
            raise ValueError(f"ABI profile must declare {sorted(required)} (or explicit 'none')")
        return self


class BuildDescription(BundleModel):
    schema_name: Literal["vq.runtime-build/1"] = "vq.runtime-build/1"
    program: Text
    source_revision: Annotated[str, Field(pattern=r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")]
    recipe_sha256: Digest
    dependency_sha256: Digest
    target: TargetProfile
    variant: Literal["generic", "optimized"]
    toolchain: Text
    compiler_flags: list[Text]
    publisher: Text
    build_receipt: Text
    qualification_receipt: Text
    # Explicit argv, invoked only after trust and compatibility checks.
    smoke_argv: Annotated[list[Text], Field(min_length=1, max_length=64)]

    @model_validator(mode="after")
    def validate_probe(self) -> BuildDescription:
        _relative(self.smoke_argv[0])
        if self.variant == "generic" and any(
            re.search(r"(?:-march|-mcpu)\s*=\s*native(?:\s|$)", flag)
            for flag in self.compiler_flags
        ):
            raise ValueError("native ISA flags cannot qualify a generic artifact")
        return self

    @property
    def build_id(self) -> str:
        return _digest(_json(self))


class FileRecord(BundleModel):
    sha256: Digest
    size: Annotated[int, Field(ge=0, le=_MAX_BYTES)]
    executable: bool


class BundleManifest(BundleModel):
    schema_name: Literal["vq.runtime-bundle/1"] = "vq.runtime-bundle/1"
    build: BuildDescription
    archive_sha256: Digest
    files: dict[str, FileRecord]

    @model_validator(mode="after")
    def validate_inventory(self) -> BundleManifest:
        if not 1 <= len(self.files) <= _MAX_FILES:
            raise ValueError("bundle requires 1..10000 files")
        if sum(item.size for item in self.files.values()) > _MAX_BYTES:
            raise ValueError("bundle exceeds the 64 GiB unpacked limit")
        folded = {name.casefold() for name in self.files}
        if len(folded) != len(self.files):
            raise ValueError("case-colliding bundle paths are unsupported")
        for name in self.files:
            _relative(name)
            if any(parent.as_posix().casefold() in folded
                   for parent in PurePosixPath(name).parents):
                raise ValueError("file/directory path collision")
        probe = self.files.get(self.build.smoke_argv[0])
        if probe is None or not probe.executable:
            raise ValueError("smoke executable must be an executable inventory member")
        return self

    @property
    def artifact_id(self) -> str:
        return _digest(_json(self))


class TrustPolicy(BundleModel):
    schema_name: Literal["vq.runtime-trust/1"] = "vq.runtime-trust/1"
    publishers: dict[str, list[Digest]]


def _json(value: BaseModel | dict[str, Any]) -> bytes:
    body = value.model_dump(mode="json") if isinstance(value, BaseModel) else value
    return json.dumps(body, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _relative(name: str) -> str:
    path = PurePosixPath(name)
    if (not name or len(name) > 512 or "\\" in name or "\x00" in name
            or path.is_absolute() or path.as_posix() != name
            or any(part in {"", ".", ".."} for part in name.split("/"))):
        raise BundleError(f"unsafe bundle path: {name!r}")
    return name


def _read_json(path: Path) -> bytes:
    _regular(path)
    with path.open("rb") as handle:
        data = handle.read(2 * 1024 * 1024 + 1)
    if len(data) > 2 * 1024 * 1024:
        raise BundleError("JSON document exceeds 2 MiB")
    return data


def _regular(path: Path) -> os.stat_result:
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode):
        raise BundleError(f"expected a regular file without symlinks: {path}")
    return info


def _file_digest(path: Path) -> str:
    if _regular(path).st_size > _MAX_BYTES + 32 * 1024**2:
        raise BundleError("archive/file exceeds size limit")
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def load_manifest(directory: Path, trust: Path) -> BundleManifest:
    manifest = BundleManifest.model_validate_json(_read_json(directory / "manifest.json"))
    policy = TrustPolicy.model_validate_json(_read_json(trust))
    if manifest.artifact_id not in policy.publishers.get(manifest.build.publisher, []):
        raise BundleError("manifest is not pinned for this publisher by the operator trust policy")
    return manifest


def compatibility(manifest: BundleManifest, target: TargetProfile) -> list[str]:
    required = manifest.build.target
    blockers = []
    for field in ("os", "arch"):
        if getattr(required, field) != getattr(target, field):
            blockers.append(f"{field} mismatch")
    if not set(required.isa).issubset(target.isa):
        blockers.append("required ISA features are absent")
    for name, value in required.abi.items():
        if value != "none" and target.abi.get(name) != value:
            blockers.append(f"{name} ABI mismatch or unknown")
    if not set(required.modules).issubset(target.modules):
        blockers.append("required external modules are absent")
    return blockers


def _archive(manifest: BundleManifest, archive: Path, destination: Path | None = None) -> None:
    """Stream and validate every member; never call tar.extract/extractall."""
    if _file_digest(archive) != manifest.archive_sha256:
        raise BundleError("archive digest mismatch")
    seen: set[str] = set()
    files: set[str] = set()
    with tarfile.open(archive, "r|*") as stream:
        for member in stream:
            name = _relative(member.name)
            if name in seen or len(seen) >= 2 * _MAX_FILES:
                raise BundleError("duplicate archive member or member limit exceeded")
            seen.add(name)
            if member.isdir():
                if name in manifest.files or not any(
                    p.startswith(name + "/") for p in manifest.files
                ):
                    raise BundleError("unexpected archive directory")
                continue
            expected = manifest.files.get(name)
            if not member.isfile() or member.issparse() or expected is None:
                raise BundleError("links, special files and unlisted members are forbidden")
            if member.size != expected.size or member.mode & 0o7000:
                raise BundleError("archive member size or privileged mode mismatch")
            if bool(member.mode & 0o111) != expected.executable:
                raise BundleError("archive executable mode mismatch")
            source = stream.extractfile(member)
            if source is None:
                raise BundleError("unreadable archive member")
            digest = hashlib.sha256()
            with contextlib.ExitStack() as stack:
                stack.enter_context(source)
                output = None
                if destination is not None:
                    path = destination / name
                    path.parent.mkdir(parents=True, exist_ok=True)
                    output = stack.enter_context(path.open("xb"))
                while block := source.read(1024 * 1024):
                    digest.update(block)
                    if output is not None:
                        output.write(block)
                if output is not None:
                    output.flush()
                    os.fsync(output.fileno())
                    os.fchmod(output.fileno(), 0o755 if expected.executable else 0o644)
            if digest.hexdigest() != expected.sha256:
                raise BundleError("archive file digest mismatch")
            files.add(name)
    if files != manifest.files.keys():
        raise BundleError("archive is missing declared runtime files/data")
    if destination is not None:
        for directory in sorted(destination.rglob("*"), reverse=True):
            if directory.is_dir():
                _sync_directory(directory)
        _sync_directory(destination)


def _inventory(directory: Path) -> dict[str, FileRecord]:
    result = {}
    for path in sorted(directory.rglob("*")):
        info = path.lstat()
        if stat.S_ISDIR(info.st_mode):
            continue
        _regular(path)
        name = _relative(path.relative_to(directory).as_posix())
        result[name] = FileRecord(sha256=_file_digest(path), size=info.st_size,
                                  executable=bool(info.st_mode & 0o111))
        if len(result) > _MAX_FILES:
            raise BundleError("bundle file limit exceeded")
    return result


def _root(path: Path) -> Path:
    require_test_path_within_sandbox(path, "portable runtime store")
    # Reuse the existing physical path, ownership and non-writable-root gate.
    return layout(path).root


@contextlib.contextmanager
def _locks(*roots: Path) -> Iterator[None]:
    # Same physical/store fences as runtime lifecycle operations; no lock bypass.
    from vq.admin import toolset_lifecycle_lock

    with toolset_lifecycle_lock([], action="vq-runtime-bundle", extra_resources=tuple(
        ("checkout", str(root)) for root in roots
    )):
        yield


def _store(root: Path, kind: str, *, create: bool) -> None:
    _root(root)
    marker = root / "format.json"
    if marker.exists():
        if json.loads(_read_json(marker)) != {"schema": kind}:
            raise BundleError("incompatible runtime store format")
        return
    if root.exists() and any(root.iterdir()):
        raise BundleError("refusing a nonempty unrecognized/legacy runtime root")
    if create:
        root.mkdir(parents=True, exist_ok=True)
        atomic_write_text(marker, _json({"schema": kind}).decode())


def _directory(path: Path) -> None:
    if path.is_symlink() or not path.is_dir():
        raise BundleError(f"expected an ordinary directory: {path}")
    info = path.stat()
    if info.st_mode & 0o022 or (os.geteuid() != 0 and info.st_uid != os.geteuid()):
        raise BundleError(f"directory must be operator-owned and not group/world writable: {path}")


def _sync_directory(path: Path) -> None:
    with contextlib.suppress(OSError):
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def pack(directory: Path, description: Path, output: Path) -> BundleManifest:
    """Package already-built files. No compiler, installer or build script runs."""
    _directory(directory)
    directory = directory.resolve()
    output = _root(output)
    if output.is_relative_to(directory):
        raise BundleError("bundle output must be outside the runtime input directory")
    build = BuildDescription.model_validate_json(_read_json(description))
    inventory = _inventory(directory)
    with _locks(output):
        if output.exists():
            raise BundleError("bundle output already exists; it will not be overwritten")
        output.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix=".vq-pack-", dir=output.parent) as tmp:
            staging = Path(tmp) / "bundle"
            staging.mkdir()
            archive = staging / "bundle.tar"
            with tarfile.open(archive, "w") as tar:
                for name, record in sorted(inventory.items()):
                    member = tarfile.TarInfo(name)
                    member.size = record.size
                    member.mode = 0o755 if record.executable else 0o644
                    with (directory / name).open("rb") as handle:
                        tar.addfile(member, handle)
            manifest = BundleManifest(build=build, files=inventory,
                                      archive_sha256=_file_digest(archive))
            _archive(manifest, archive)
            atomic_write_text(staging / "manifest.json", _json(manifest).decode())
            with archive.open("rb") as handle:
                os.fsync(handle.fileno())
            staging.rename(output)
            _sync_directory(output.parent)
    return manifest


def publish(bundle: Path, store: Path, trust: Path) -> BundleManifest:
    """Publish a verified copy with an immutable content identity."""
    manifest = load_manifest(bundle, trust)
    _archive(manifest, bundle / "bundle.tar")
    store = _root(store)
    with _locks(store):
        _store(store, "vq.runtime-artifact-store/1", create=True)
        artifacts = store / "artifacts"
        artifacts.mkdir(exist_ok=True)
        _directory(artifacts)
        final = artifacts / manifest.artifact_id
        if final.exists() or final.is_symlink():
            _directory(final)
            if load_manifest(final, trust) != manifest:
                raise BundleError("published identity has different metadata")
            _archive(manifest, final / "bundle.tar")
            return manifest
        with tempfile.TemporaryDirectory(prefix=".vq-publish-", dir=artifacts) as tmp:
            staging = Path(tmp) / "artifact"
            staging.mkdir()
            shutil.copyfile(bundle / "bundle.tar", staging / "bundle.tar")
            _archive(manifest, staging / "bundle.tar")
            atomic_write_text(staging / "manifest.json", _json(manifest).decode())
            with (staging / "bundle.tar").open("rb") as handle:
                os.fsync(handle.fileno())
            staging.rename(final)
            _sync_directory(artifacts)
    return manifest


def _artifact(store: Path, artifact_id: str, trust: Path) -> tuple[Path, BundleManifest]:
    if not re.fullmatch(r"[0-9a-f]{64}", artifact_id):
        raise BundleError("artifact ID must be a full SHA256 digest")
    store = _root(store)
    _store(store, "vq.runtime-artifact-store/1", create=False)
    directory = store / "artifacts" / artifact_id
    _directory(directory.parent)
    _directory(directory)
    manifest = load_manifest(directory, trust)
    if manifest.artifact_id != artifact_id:
        raise BundleError("artifact identity mismatch")
    return directory, manifest


def _selection(root: Path) -> dict[str, Any]:
    path = root / "selection.json"
    if not path.exists() and not path.is_symlink():
        return {"schema": "vq.runtime-selection/1", "current": None, "previous": None}
    body = json.loads(_read_json(path))
    if not isinstance(body, dict) or set(body) != {"schema", "current", "previous"}:
        raise BundleError("invalid selection receipt")
    if body["schema"] != "vq.runtime-selection/1":
        raise BundleError("unsupported selection schema")
    for name in ("current", "previous"):
        value = body[name]
        if value is not None and (
            not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value)
        ):
            raise BundleError("invalid selection identity")
    return body


def deploy_plan(
    store: Path, artifact_id: str, root: Path, trust: Path, target: TargetProfile,
) -> dict[str, Any]:
    store, root = _root(store), _root(root)
    if store == root or store.is_relative_to(root) or root.is_relative_to(store):
        raise BundleError("artifact store and target must be independent roots")
    directory, manifest = _artifact(store, artifact_id, trust)
    _archive(manifest, directory / "bundle.tar")
    root = _root(root)
    _store(root, "vq.portable-runtime-target/1", create=False)
    selected = _selection(root)
    return {
        "schema": "vq.runtime-deploy-plan/1", "artifact_id": artifact_id,
        "build_id": manifest.build.build_id, "source_revision": manifest.build.source_revision,
        "target_profile_sha256": _digest(_json(target)),
        "generation": str(root / "generations" / artifact_id / "payload"),
        "current": selected["current"], "blockers": compatibility(manifest, target),
        "target_builds": 0, "smoke_required": True,
    }


def _smoke(payload: Path, build: BuildDescription, timeout: float) -> None:
    # Trusted product code runs as the invoking operator. This is not a sandbox.
    from vq.admin import _active_lifecycle_pass_fds

    env = dict(os.environ)
    for key in ("PYTHONPATH", "PYTHONHOME", "LD_PRELOAD", "LD_LIBRARY_PATH",
                "DYLD_LIBRARY_PATH", "DYLD_FALLBACK_LIBRARY_PATH", "DYLD_INSERT_LIBRARIES"):
        env.pop(key, None)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    with subprocess.Popen(
        [str(payload / build.smoke_argv[0]), *build.smoke_argv[1:]], cwd=payload,
        env=env, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL, start_new_session=True,
        pass_fds=_active_lifecycle_pass_fds(),
    ) as process:
        try:
            code = process.wait(timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
            raise BundleError("runtime smoke check timed out") from exc
    if code:
        raise BundleError(f"runtime smoke check failed (exit {code})")


def deploy(
    store: Path, artifact_id: str, root: Path, trust: Path, target: TargetProfile,
    *, timeout: float = 30.0,
) -> dict[str, Any]:
    """Verify, relocate, smoke-test, and atomically select; never compile."""
    if not 0 < timeout <= 120:
        raise BundleError("smoke timeout must be in (0, 120] seconds")
    store, root = _root(store), _root(root)
    if store == root or store.is_relative_to(root) or root.is_relative_to(store):
        raise BundleError("artifact store and target must be independent roots")
    # Preflight before any target mutation. Recheck under both physical locks.
    plan = deploy_plan(store, artifact_id, root, trust, target)
    if plan["blockers"]:
        raise BundleError("; ".join(plan["blockers"]))
    with _locks(store, root):
        plan = deploy_plan(store, artifact_id, root, trust, target)
        if plan["blockers"]:
            raise BundleError("; ".join(plan["blockers"]))
        directory, manifest = _artifact(store, artifact_id, trust)
        _store(root, "vq.portable-runtime-target/1", create=True)
        generations = root / "generations"
        generations.mkdir(exist_ok=True)
        _directory(generations)
        generation = generations / artifact_id
        if generation.exists() or generation.is_symlink():
            _directory(generation)
        else:
            with tempfile.TemporaryDirectory(prefix=".vq-deploy-", dir=generations) as tmp:
                staging = Path(tmp) / "generation"
                payload = staging / "payload"
                payload.mkdir(parents=True)
                _archive(manifest, directory / "bundle.tar", payload)
                atomic_write_text(staging / "manifest.json", _json(manifest).decode())
                staging.rename(generation)
                _sync_directory(generations)
        # A crash after publication or lost receipt can safely replay these gates.
        if BundleManifest.model_validate_json(_read_json(generation / "manifest.json")) != manifest:
            raise BundleError("retained generation metadata mismatch")
        payload = generation / "payload"
        _directory(payload)
        if _inventory(payload) != manifest.files:
            raise BundleError("retained generation content mismatch")
        _smoke(payload, manifest.build, timeout)
        if _inventory(payload) != manifest.files:
            raise BundleError("smoke check changed the immutable payload")
        # One atomic receipt commits current and previous together. Old paths
        # remain valid forever here: retention and JobSpec integration are later.
        selected = _selection(root)
        if selected["current"] != artifact_id:
            selected["previous"], selected["current"] = selected["current"], artifact_id
        receipt = {**plan, "schema": "vq.runtime-deployment/1", "verified": True}
        atomic_write_text(generation / "verification.json", _json(receipt).decode())
        atomic_write_text(root / "selection.json", _json(selected).decode())
        return receipt
