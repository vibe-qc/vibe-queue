"""Scheduled product builds that end in a portable runtime bundle (#161).

The builder is the product's own: vq runs it as an ordinary job on this
host's daemon, with ``VQ_BUILD_DIR`` naming the directory it must fill, and
records the build beside the bundle it will become. Once the job has
completed, :func:`finish_build` packs that directory with the existing
:func:`vq.runtime_bundle.pack` (no compilation, the same refusals). Signing,
publication and deployment are unchanged, and a packed bundle is trusted
only once pinned or signed.

The record, ``<output>.vq-build.json`` (``vq.runtime-build-record/1``), is
the durable link from a bundle to the job that built it: the job id, the
build and output paths, the validated build description and the digest of
the file it came from, and, once packed, the artifact identity.
"""

from __future__ import annotations

import hashlib
import json
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from vq import paths, runtime_bundle
from vq._storage import atomic_write_text
from vq.runtime_bundle import BuildDescription, BundleError, BundleManifest
from vq.spec import JobSpec, JobState

RECORD_SCHEMA = "vq.runtime-build-record/1"
BUILD_DIR_ENV = "VQ_BUILD_DIR"


def record_path(output: Path) -> Path:
    """Where the build record of the bundle at ``output`` lives."""
    return output.with_name(output.name + ".vq-build.json")


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def _write(record: Path, document: dict[str, Any]) -> None:
    atomic_write_text(record, json.dumps(document, indent=2, sort_keys=True) + "\n")


def submit_build(
    *, source: Path, description: Path, build_dir: Path, output: Path, builder: list[str],
    cpus: int = 1, mem_mb: int | None = None, wall_time_seconds: int | None = None,
    job_name: str | None = None, multi_user: bool = False,
) -> dict[str, Any]:
    """Submit ``builder`` as a local job from ``source`` and record the build.

    The builder must write ``build_dir``, which must not exist yet, so a
    bundle packs only what this build wrote. Raises BundleError for a refused
    build and ValueError for a refused submission.
    """
    from vq.submit import submit_local

    if not builder:
        raise BundleError("a build needs the builder's command")
    for name, path in (("--build-dir", build_dir), ("--output", output)):
        if not path.is_absolute():
            raise BundleError(f"{name} must be an absolute path")
    if output.is_relative_to(build_dir):
        raise BundleError("--output must be outside --build-dir")
    if build_dir.exists() or build_dir.is_symlink():
        raise BundleError("--build-dir already exists; a build writes a fresh one")
    record = record_path(output)
    if any(p.exists() or p.is_symlink() for p in (output, record)):
        raise BundleError(f"{output} or its build record already exists")
    data = runtime_bundle._read_json(description)
    build = BuildDescription.model_validate_json(data)
    command = ["env", f"{BUILD_DIR_ENV}={build_dir}", *builder]
    job_id = submit_local(
        host="localhost", directory=str(source), command=command, cpus=cpus, mem_mb=mem_mb,
        wall_time_seconds=wall_time_seconds, job_name=job_name, multi_user=multi_user)
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
        manifest = BundleManifest.model_validate_json(
            runtime_bundle._read_json(output / "manifest.json"))
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
