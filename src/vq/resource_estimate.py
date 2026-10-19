"""Read-only, versioned resource estimates; no workload execution or placement.

The adapter preserves product estimates rather than reconstructing chemistry
inside the queue. Missing aggregation or calibration evidence stays unknown.
"""

from __future__ import annotations

import hashlib
import json
import tomllib
from pathlib import Path
from typing import Annotated, Literal

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


def _read_bounded(path: Path, limit: int) -> bytes:
    # Only ordinary files: a pipe/device must not turn a preview into a wait.
    if not path.is_file():
        raise EstimateError(f"expected a regular file: {path}")
    with path.open("rb") as handle:
        data = handle.read(limit + 1)
    if len(data) > limit:
        raise EstimateError(f"file exceeds the {limit}-byte estimation limit: {path}")
    return data


def estimate_vibeqc(
    manifest: Path, *, inputs: tuple[Path, ...] = (),
    features_file: Path | None = None, memory_mb: int | None = None,
) -> ResourceEstimate:
    """Consume an existing dry-run manifest without executing its input.

    ``input_sha256`` identifies the exact evidence and optional input bytes
    presented at preview time. It does not certify that the manifest was
    generated from those inputs; legacy vibe-qc manifests lack that binding.
    """
    if len(inputs) > 64:
        raise EstimateError("at most 64 input files may be fingerprinted")
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
    digests = [hashlib.sha256(_read_bounded(path, 64 * 1024 * 1024)).hexdigest()
               for path in inputs]
    manifest_digest = hashlib.sha256(raw).hexdigest()
    evidence = json.dumps({
        "manifest": manifest_digest, "inputs": digests,
        "features": hashlib.sha256(feature_bytes).hexdigest(),
    }, sort_keys=True, separators=(",", ":")).encode()
    warnings = [
        "No workload was executed; the manifest-to-input association is caller supplied.",
        "Memory is an uncalibrated program estimate, not a measured upper bound.",
    ]
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
