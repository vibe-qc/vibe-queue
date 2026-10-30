"""Scheduled product builds that end in a portable runtime bundle (#161).

The builder is the product's own: vq runs it as an ordinary job on this
host's daemon, with ``VQ_BUILD_DIR`` naming the directory it must fill, and
records the build beside the bundle it will become. Once the job has
completed, :func:`finish_build` packs that directory with the existing
:func:`vq.runtime_bundle.pack` (no compilation, the same refusals). Signing,
publication and deployment are unchanged, and a packed bundle is trusted
only once pinned or signed.

With a configured host, :func:`submit_remote_build` packs the source tree,
stages it and the description in that host's home and runs the same build
there; :func:`finish_remote_build` finishes it there, where the build
directory is.

The record, ``<output>.vq-build.json`` (``vq.runtime-build-record/1``), is
the durable link from a bundle to the job that built it: the job id, the
build and output paths, the validated build description and the digest of
the file it came from, and, once packed, the artifact identity.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import tarfile
import tempfile
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any

from vq import paths, runtime_bundle, transport
from vq._storage import atomic_write_text
from vq.runtime_bundle import BuildDescription, BundleError
from vq.spec import JobSpec, JobState

if TYPE_CHECKING:
    from vq.config import HostConfig

RECORD_SCHEMA = "vq.runtime-build-record/1"
BUILD_DIR_ENV = "VQ_BUILD_DIR"


def record_path(output: Path) -> Path:
    """Where the build record of the bundle at ``output`` lives."""
    return output.with_name(output.name + ".vq-build.json")


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def _write(record: Path, document: dict[str, Any]) -> None:
    atomic_write_text(record, json.dumps(document, indent=2, sort_keys=True) + "\n")


def _check_paths(build_dir: PurePosixPath | Path, output: PurePosixPath | Path,
                 builder: list[str]) -> None:
    if not builder:
        raise BundleError("a build needs the builder's command")
    for name, path in (("--build-dir", build_dir), ("--output", output)):
        if not path.is_absolute():
            raise BundleError(f"{name} must be an absolute path")
    if output.is_relative_to(build_dir):
        raise BundleError("--output must be outside --build-dir")


def submit_build(
    *, description: Path, build_dir: Path, output: Path, builder: list[str],
    source: Path | None = None, archive: Path | None = None,
    cpus: int = 1, mem_mb: int | None = None, wall_time_seconds: int | None = None,
    job_name: str | None = None, multi_user: bool = False,
) -> dict[str, Any]:
    """Submit ``builder`` as a local job and record the build.

    The job's workspace is a copy of the ``source`` tree or the unpacked
    ``archive`` (exactly one). The builder must write ``build_dir``, which
    must not exist yet, so a bundle packs only what this build wrote. Raises
    BundleError for a refused build and ValueError for a refused submission.
    """
    from vq.submit import submit_local

    if (source is None) == (archive is None):
        raise BundleError("a build takes exactly one of a source directory and an archive")
    _check_paths(build_dir, output, builder)
    if build_dir.exists() or build_dir.is_symlink():
        raise BundleError("--build-dir already exists; a build writes a fresh one")
    record = record_path(output)
    if any(p.exists() or p.is_symlink() for p in (output, record)):
        raise BundleError(f"{output} or its build record already exists")
    data = runtime_bundle._read_json(description)
    build = BuildDescription.model_validate_json(data)
    command = ["env", f"{BUILD_DIR_ENV}={build_dir}", *builder]
    job_id = submit_local(
        host="localhost", directory=None if source is None else str(source),
        archive=None if archive is None else str(archive), command=command, cpus=cpus,
        mem_mb=mem_mb, wall_time_seconds=wall_time_seconds, job_name=job_name,
        multi_user=multi_user)
    document: dict[str, Any] = {
        "schema": RECORD_SCHEMA, "job_id": job_id, "build_dir": str(build_dir),
        "output": str(output), "description": build.model_dump(mode="json"),
        "description_sha256": hashlib.sha256(data).hexdigest(), "builder": list(builder),
        "submitted_at": _now(), "artifact_id": None, "packed_at": None,
    }
    _write(record, document)
    return document


def finish_build(record: Path, *, multi_user: bool = False) -> dict[str, Any]:
    """Pack a completed build's directory, or say why it is not packed yet.

    The result is the record plus ``status``: ``building`` (with the job's
    ``state``), ``packed`` (just now) or ``already-packed``. A build whose
    job failed, was killed or otherwise did not complete raises BundleError.
    """
    try:
        document = json.loads(runtime_bundle._read_json(record))
    except ValueError as exc:
        raise BundleError(f"{record}: not a build record: {exc}") from None
    if not isinstance(document, dict) or document.get("schema") != RECORD_SCHEMA:
        raise BundleError(f"{record}: not a {RECORD_SCHEMA} record")
    output, build_dir = Path(document["output"]), Path(document["build_dir"])
    if document["artifact_id"]:
        manifest = runtime_bundle.read_manifest(output / "manifest.json")
        if manifest.artifact_id != document["artifact_id"]:
            raise BundleError(f"{output} is no longer the bundle this build packed")
        return {**document, "status": "already-packed"}
    try:
        spec = JobSpec.read(paths.resolve_spec_path(document["job_id"], multi_user=multi_user))
    except FileNotFoundError:
        raise BundleError(f"build job {document['job_id']} is not in this queue") from None
    if not spec.is_terminal:
        return {**document, "status": "building", "state": spec.state.value}
    if spec.state != JobState.COMPLETED:
        raise BundleError(f"build job {spec.id} ended {spec.state.value}; nothing to pack")
    build = BuildDescription.model_validate(document["description"])
    with tempfile.TemporaryDirectory(prefix="vq-build-finish-") as tmp:
        description = Path(tmp) / "build.json"
        description.write_text(build.model_dump_json())
        manifest = runtime_bundle.pack(build_dir, description, output)
    document.update(artifact_id=manifest.artifact_id, packed_at=_now())
    _write(record, document)
    return {**document, "status": "packed"}


def _remote_document(stdout: str, host: str) -> dict[str, Any]:
    try:
        document = json.loads(stdout)
    except ValueError:
        document = None
    if not isinstance(document, dict) or document.get("schema") != RECORD_SCHEMA:
        raise transport.RemoteOutcomeUnknown(
            f"{host} did not answer with a {RECORD_SCHEMA} record")
    return document


def submit_remote_build(
    host: str, host_cfg: HostConfig, *, source: Path, description: Path, build_dir: str,
    output: str, builder: list[str], cpus: int = 1, mem_mb: int | None = None,
    wall_time_seconds: int | None = None, job_name: str | None = None,
) -> dict[str, Any]:
    """Run :func:`submit_build` on ``host``, from this host's source tree.

    ``build_dir`` and ``output`` are paths on that host. What can be checked
    here is checked before anything is sent; the staged tar and description
    are removed afterwards, unless SSH cannot tell whether the build was
    accepted. A rerun is then refused by the record already there.
    """
    from vq.submit import (
        REMOTE_HOUSEKEEPING_STDERR_MAX_BYTES,
        REMOTE_HOUSEKEEPING_STDOUT_MAX_BYTES,
        REMOTE_SUBMIT_STDERR_MAX_BYTES,
        REMOTE_SUBMIT_STDOUT_MAX_BYTES,
    )

    _check_paths(PurePosixPath(build_dir), PurePosixPath(output), builder)
    BuildDescription.model_validate_json(runtime_bundle._read_json(description))
    remote_tar = transport.remote_temp_tar_path()
    remote_description = remote_tar.removesuffix(".tar") + ".build.json"
    argv = ["runtime-bundle", "build", "-a", remote_tar, "--description", remote_description,
            "--build-dir", build_dir, "--output", output, "--cpus", str(cpus)]
    for flag, value in (("--mem-mb", mem_mb), ("--time-limit", wall_time_seconds),
                        ("--name", job_name)):
        if value is not None:
            argv += [flag, str(value)]
    fd, name = tempfile.mkstemp(suffix=".tar", prefix="vq-build-")
    os.close(fd)
    local_tar = Path(name)
    staged = submitted = False
    try:
        with tarfile.open(local_tar, "w") as tar:
            for child in sorted(source.iterdir()):
                tar.add(child, arcname=child.name)
        staged = True
        transport.upload_file(host_cfg, local_tar, remote_tar)
        transport.upload_file(host_cfg, description, remote_description)
        submitted = True
        proc = transport.run_remote_vq(
            host_cfg, *argv, "--", *builder, owned_process_group=True,
            max_stdout_bytes=REMOTE_SUBMIT_STDOUT_MAX_BYTES,
            max_stderr_bytes=REMOTE_SUBMIT_STDERR_MAX_BYTES)
        document = _remote_document(proc.stdout, host)
    except transport.RemoteOutcomeUnknown as exc:
        if not submitted:
            raise
        staged = False  # it may still be in use: keep it for the operator
        raise transport.RemoteOutcomeUnknown(
            f"{exc}\nwhether {host} accepted the build is unknown; its staging "
            f"{remote_tar} and {remote_description} was kept, and a rerun is refused "
            f"while the build record exists") from None
    finally:
        local_tar.unlink(missing_ok=True)
        if staged:
            with contextlib.suppress(transport.RemoteError):  # never masks the outcome
                transport.run_remote_shell(
                    host_cfg, "rm", "-f", remote_tar, remote_description, check=False,
                    owned_process_group=True,
                    max_stdout_bytes=REMOTE_HOUSEKEEPING_STDOUT_MAX_BYTES,
                    max_stderr_bytes=REMOTE_HOUSEKEEPING_STDERR_MAX_BYTES)
    return {**document, "host": host}


def finish_remote_build(host: str, host_cfg: HostConfig, record: str) -> dict[str, Any]:
    """Run :func:`finish_build` on ``host`` for its build record ``record``."""
    from vq.submit import REMOTE_SUBMIT_STDERR_MAX_BYTES, REMOTE_SUBMIT_STDOUT_MAX_BYTES

    try:
        proc = transport.run_remote_vq(
            host_cfg, "runtime-bundle", "build-finish", record, owned_process_group=True,
            max_stdout_bytes=REMOTE_SUBMIT_STDOUT_MAX_BYTES,
            max_stderr_bytes=REMOTE_SUBMIT_STDERR_MAX_BYTES)
        stdout = proc.stdout
    except transport.RemoteCommandError as exc:
        if exc.returncode != 3:  # 3: still building, with the record on stdout
            raise
        stdout = exc.stdout
    return {**_remote_document(stdout, host), "host": host}
