"""Read-only, versioned resource estimates; no workload execution or placement.

The adapter preserves product estimates rather than reconstructing chemistry
inside the queue. Missing aggregation or calibration evidence stays unknown.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
import tarfile
import tomllib
from collections.abc import Iterator
from pathlib import Path
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

PositiveInt = Annotated[int, Field(strict=True, gt=0)]
RuntimeSeconds = Annotated[float, Field(ge=0, allow_inf_nan=False)]
Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


class EstimateError(ValueError):
    """The supplied evidence cannot be interpreted safely."""


class EstimateModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class ResourceFeatures(EstimateModel):
    """Optional product/operator evidence, never inferred from a basis name."""

    schema_version: Literal[1] = 1
    method: str | None = None
    algorithm: str | None = None
    basis: str | None = None
    nbf: PositiveInt | None = None
    basis_representation: Literal["spherical", "cartesian"] | None = None
    naux: PositiveInt | None = None
    occupied: PositiveInt | None = None
    virtual: PositiveInt | None = None
    roots: PositiveInt | None = None
    kpoints: PositiveInt | None = None
    task_type: str | None = None
    mpi_ranks: PositiveInt | None = None
    threads_per_rank: PositiveInt | None = None
    memory_scope: Literal["job", "node", "rank", "unspecified"] = "unspecified"


class MemoryEstimate(EstimateModel):
    peak_bytes: PositiveInt | None = None
    scope: Literal["job", "node", "rank", "unspecified"] = "unspecified"
    raw_bytes: PositiveInt | None = None
    headroom: Literal["included_by_program", "unknown"] = "unknown"
    # The program's configured factor is not recoverable from estimate_bytes.
    headroom_factor: float | None = None


class ResourceEstimate(EstimateModel):
    schema_name: Literal["vq.resource-estimate/1"] = "vq.resource-estimate/1"
    estimator: Literal["vq.vibeqc-manifest/1"] = "vq.vibeqc-manifest/1"
    program: Literal["vibeqc"] = "vibeqc"
    program_version: str | None = None
    manifest_sha256: Digest
    input_sha256: Digest
    input_files: list[Digest]
    input_binding: Literal["manifest_only", "caller_supplied_inputs"]
    features: ResourceFeatures
    feature_source: Literal["manifest", "manifest_and_operator_file"]
    memory: MemoryEstimate
    scratch_bytes: PositiveInt | None = None
    runtime_seconds: tuple[RuntimeSeconds, RuntimeSeconds] | None = None
    runtime_class: str | None = None
    hardware_class: str | None = None
    cpu_layout_candidates: list[dict[str, int]] = Field(default_factory=list)
    calibration: Literal["uncalibrated"] = "uncalibrated"
    calibration_id: str | None = None
    user_job_memory_bytes: PositiveInt | None = None
    effective_job_memory_bytes: PositiveInt | None = None
    unknown_fields: list[str]
    warnings: list[str]

    @field_validator("runtime_seconds")
    @classmethod
    def _ordered_runtime(cls, value: tuple[float, float] | None) -> tuple[float, float] | None:
        if value is not None and value[1] < value[0]:
            raise ValueError("runtime_seconds upper bound must not be below its lower bound")
        return value


#: How many input files one receipt fingerprints, and how large each may be.
MAX_INPUT_FILES = 64
MAX_INPUT_BYTES = 64 * 1024 * 1024
#: How many entries a walk of an input directory visits.
MAX_INPUT_ENTRIES = 10_000


def _read_bounded(path: Path, limit: int) -> bytes:
    # Only ordinary files: a pipe/device must not turn a preview into a wait.
    if not path.is_file():
        raise EstimateError(f"expected a regular file: {path}")
    with path.open("rb") as handle:
        data = handle.read(limit + 1)
    if len(data) > limit:
        raise EstimateError(f"file exceeds the {limit}-byte estimation limit: {path}")
    return data


def _raise(error: OSError) -> None:
    raise error


def _directory_files(directory: Path, room: int) -> tuple[list[Path], int]:
    """The regular files under ``directory`` in relative-path order, and the links skipped.

    Links (to files or directories) and special files are left out, since a
    `vq submit -d` payload carries a link as a link. More than ``room``
    files is refused rather than truncated.
    """
    found: list[Path] = []
    skipped = 0
    visited = 0
    for root, dirs, files in os.walk(directory, onerror=_raise):
        visited += len(dirs) + len(files)
        if visited > MAX_INPUT_ENTRIES:
            raise EstimateError(f"{directory} holds more than {MAX_INPUT_ENTRIES} entries "
                                "to search; name the files the calculation reads instead")
        # os.walk lists a link to a directory here but never descends into it.
        skipped += sum(os.path.islink(os.path.join(root, name)) for name in dirs)
        for name in files:
            path = Path(root, name)
            if not stat.S_ISREG(path.lstat().st_mode):
                skipped += 1
                continue
            found.append(path)
            if len(found) > room:
                raise EstimateError(
                    f"the inputs hold more than the {MAX_INPUT_FILES} files one estimate "
                    f"fingerprints (counting {directory}); name the files the calculation "
                    "reads instead")
    if not found:
        raise EstimateError(f"{directory} holds no regular file to fingerprint")
    found.sort(key=lambda path: os.fsencode(path.relative_to(directory)))
    return found, skipped


def _read_unlinked(path: Path) -> bytes:
    """A file found in an input directory, opened without following a link."""
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    try:
        with os.fdopen(os.open(path, flags), "rb") as handle:
            if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
                raise EstimateError(f"{path} changed while it was being fingerprinted")
            data = handle.read(MAX_INPUT_BYTES + 1)
    except OSError as exc:  # a file swapped for a link mid-walk lands here
        raise EstimateError(f"cannot fingerprint {path}: {exc.strerror or exc}") from None
    if len(data) > MAX_INPUT_BYTES:
        raise EstimateError(
            f"file exceeds the {MAX_INPUT_BYTES}-byte estimation limit: {path}")
    return data


def _fingerprint_inputs(inputs: tuple[Path, ...]) -> tuple[list[str], int]:
    """SHA256 of each input file, a directory's regular files in its place."""
    digests: list[str] = []
    skipped = 0
    for path in inputs:
        if path.is_dir():
            files, links = _directory_files(path, MAX_INPUT_FILES - len(digests))
            skipped += links
            digests += [hashlib.sha256(_read_unlinked(file)).hexdigest() for file in files]
        else:
            if len(digests) >= MAX_INPUT_FILES:
                raise EstimateError(
                    f"at most {MAX_INPUT_FILES} input files may be fingerprinted")
            digests.append(hashlib.sha256(_read_bounded(path, MAX_INPUT_BYTES)).hexdigest())
    return digests, skipped


def estimate_vibeqc(
    manifest: Path, *, inputs: tuple[Path, ...] = (),
    features_file: Path | None = None, memory_mb: int | None = None,
) -> ResourceEstimate:
    """Consume an existing dry-run manifest without executing its input.

    ``input_sha256`` identifies the exact evidence and optional input bytes
    presented at preview time. It does not certify that the manifest was
    generated from those inputs; legacy vibe-qc manifests lack that binding.
    """
    if len(inputs) > MAX_INPUT_FILES:
        raise EstimateError(f"at most {MAX_INPUT_FILES} input files may be fingerprinted")
    if memory_mb is not None and (type(memory_mb) is not int or memory_mb <= 0):
        raise EstimateError("memory_mb must be a positive integer")
    raw = _read_bounded(manifest, 1024 * 1024)
    try:
        body = tomllib.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeError) as exc:
        raise EstimateError(f"malformed vibe-qc manifest: {exc}") from exc
    plan = body.get("plan")
    outputs = body.get("outputs")
    if not isinstance(plan, dict) or not isinstance(outputs, dict):
        raise EstimateError("manifest requires [plan] and [outputs]")
    if outputs.get("status") != "dry_run":
        raise EstimateError("expected a dry_run manifest, not measured run output")
    known = {}
    for source, target in (("method", "method"), ("basis", "basis"), ("job_kind", "task_type")):
        value = plan.get(source)
        if value is not None and not isinstance(value, str):
            raise EstimateError(f"plan.{source} must be a string")
        if value:
            known[target] = value
    feature_bytes = b""
    if features_file is not None:
        feature_bytes = _read_bounded(features_file, 64 * 1024)
        supplied = ResourceFeatures.model_validate_json(feature_bytes)
        for name, value in supplied.model_dump(exclude_unset=True).items():
            if name in known and value != known[name]:
                raise EstimateError(f"feature {name} contradicts the program manifest")
            known[name] = value
    features = ResourceFeatures.model_validate(known)
    memory = body.get("memory", {})
    if not isinstance(memory, dict):
        raise EstimateError("memory must be a TOML table")
    peak = memory.get("estimate_bytes")
    if peak is not None and (type(peak) is not int or peak <= 0):
        raise EstimateError("memory.estimate_bytes must be a positive integer")
    program = body.get("vibeqc", {})
    if not isinstance(program, dict):
        raise EstimateError("vibeqc must be a TOML table")
    version = program.get("version")
    if version is not None and not isinstance(version, str):
        raise EstimateError("vibeqc.version must be a string")
    digests, skipped = _fingerprint_inputs(inputs)
    manifest_digest = hashlib.sha256(raw).hexdigest()
    evidence = json.dumps({
        "manifest": manifest_digest, "inputs": digests,
        "features": hashlib.sha256(feature_bytes).hexdigest(),
    }, sort_keys=True, separators=(",", ":")).encode()
    warnings = [
        "No workload was executed; the manifest-to-input association is caller supplied.",
        "Memory is an uncalibrated program estimate, not a measured upper bound.",
    ]
    if skipped:
        warnings.append(f"{skipped} link(s) or special file(s) under an input directory were "
                        "not fingerprinted; a submission carries a link as a link.")
    if features.memory_scope == "unspecified":
        warnings.append("Legacy manifests do not specify job/node/rank memory aggregation.")
    if features.mpi_ranks and features.mpi_ranks > 1:
        warnings.append("No multiplication or division by MPI ranks is inferred.")
    user_bytes = memory_mb * 1024 * 1024 if memory_mb is not None else None
    job_bytes = peak if features.memory_scope == "job" else None
    if user_bytes is not None and job_bytes is not None and user_bytes < job_bytes:
        warnings.append("Explicit job memory is below the program estimate; override preserved.")
    unknown = [f"features.{name}" for name, value in features.model_dump().items() if value is None]
    unknown += ["scratch_bytes", "runtime_seconds", "cpu_layout_candidates",
                "hardware_class", "runtime_class", "calibration_id", "memory.raw_bytes",
                "memory.headroom_factor", "manifest_input_binding"]
    if peak is None:
        unknown.append("memory.peak_bytes")
        warnings.append("Program supplied no estimate; unsupported routes remain unknown.")
    if features.memory_scope == "unspecified":
        unknown.append("memory.scope")
    if version is None:
        unknown.append("program_version")
    return ResourceEstimate(
        program_version=version, manifest_sha256=manifest_digest,
        input_sha256=hashlib.sha256(evidence).hexdigest(), input_files=digests,
        input_binding="caller_supplied_inputs" if inputs else "manifest_only",
        features=features,
        feature_source="manifest_and_operator_file" if features_file else "manifest",
        memory=MemoryEstimate(peak_bytes=peak, scope=features.memory_scope,
                              headroom="included_by_program" if peak else "unknown"),
        user_job_memory_bytes=user_bytes,
        effective_job_memory_bytes=user_bytes if user_bytes is not None else job_bytes,
        unknown_fields=unknown, warnings=warnings,
    )


# --- attaching a receipt to a submission (#159) ------------------------------

#: The largest receipt `vq submit --estimate` accepts.
MAX_RECEIPT_BYTES = 256 * 1024
#: How much of a payload is hashed to find the receipt's fingerprinted inputs.
MAX_PAYLOAD_FILES = 20_000
MAX_PAYLOAD_BYTES = 16 * 1024**3


def load_receipt(text: str) -> ResourceEstimate:
    """A ``vq estimate --json`` receipt, validated."""
    if len(text.encode()) > MAX_RECEIPT_BYTES:
        raise EstimateError(f"the estimate receipt is larger than {MAX_RECEIPT_BYTES} bytes")
    try:
        return ResourceEstimate.model_validate_json(text)
    except ValueError as exc:
        raise EstimateError(f"not a vq.resource-estimate/1 receipt: {exc}") from None


def receipt_record(receipt: ResourceEstimate) -> dict[str, Any]:
    """The receipt as a job records it."""
    record: dict[str, Any] = json.loads(receipt.model_dump_json())
    return record


def _hash(stream: Any) -> str:
    digest = hashlib.sha256()
    while chunk := stream.read(1024 * 1024):
        digest.update(chunk)
    return digest.hexdigest()


def _payload_digests(*, input_file: Path | None, directory: Path | None,
                     archive: Path | None) -> Iterator[str]:
    """SHA256 of every regular file a submission carries, within fixed bounds."""
    budget = {"files": MAX_PAYLOAD_FILES, "bytes": MAX_PAYLOAD_BYTES}

    def spend(size: int) -> None:
        budget["files"] -= 1
        budget["bytes"] -= size
        if budget["files"] < 0 or budget["bytes"] < 0:
            raise EstimateError(
                f"the payload is too large to find the estimate's inputs in (more than "
                f"{MAX_PAYLOAD_FILES} files or {MAX_PAYLOAD_BYTES} bytes)")

    if input_file is not None:
        spend(input_file.stat().st_size)
        with input_file.open("rb") as handle:
            yield _hash(handle)
    elif directory is not None:
        for root, dirs, files in os.walk(directory):
            dirs.sort()
            for name in sorted(files):
                path = Path(root) / name
                info = path.lstat()
                if not stat.S_ISREG(info.st_mode):
                    continue
                spend(info.st_size)
                with path.open("rb") as handle:
                    yield _hash(handle)
    elif archive is not None:
        with tarfile.open(archive, "r:*") as tar:
            for member in tar:
                if not member.isreg():
                    continue
                spend(member.size)
                extracted = tar.extractfile(member)
                if extracted is not None:
                    yield _hash(extracted)


def check_inputs(receipt: ResourceEstimate, *, input_file: Path | None = None,
                 directory: Path | None = None, archive: Path | None = None) -> None:
    """Refuse a receipt whose fingerprinted inputs are not all in the payload.

    A receipt fingerprints the inputs ``vq estimate --input`` was given; each
    must be a file of the submission, by content. A manifest-only receipt
    names no inputs and has nothing to check.
    """
    wanted = set(receipt.input_files)
    if not wanted:
        return
    try:
        for digest in _payload_digests(input_file=input_file, directory=directory,
                                       archive=archive):
            wanted.discard(digest)
            if not wanted:
                return
    except (OSError, tarfile.TarError) as exc:
        raise EstimateError(f"cannot read the payload to check the estimate: {exc}") from None
    raise EstimateError(
        f"the estimate fingerprinted {len(wanted)} input file(s) that this submission does "
        "not carry; it was made for other inputs, so it is not attached")
