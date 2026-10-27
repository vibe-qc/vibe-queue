"""Validate product-owned allocation evidence without implementing its formulas."""
from __future__ import annotations

import json
from typing import Annotated, Any, Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SerializerFunctionWrapHandler,
    model_serializer,
    model_validator,
)

Count = Annotated[int, Field(strict=True, ge=0)]
Bytes = Annotated[int, Field(strict=True, gt=0)]
Scope = Literal["job", "node", "rank"]


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class DimensionDomain(_Model):
    min: Count
    max: Count | None = None

    @model_validator(mode="after")
    def ordered(self) -> Self:
        if self.max is not None and self.max < self.min:
            raise ValueError("inverted analytical dimension domain")
        return self


class AllocationTerm(_Model):
    name: Annotated[str, Field(min_length=1)]
    route: Annotated[str, Field(min_length=1)]
    inputs: dict[str, Count]
    units: Literal["bytes"]
    scope: Scope
    valid_domain: dict[str, DimensionDomain]
    value_bytes: Count

    @model_validator(mode="after")
    def domain(self) -> Self:
        if not self.inputs or self.inputs.keys() != self.valid_domain.keys():
            raise ValueError("analytical inputs require matching valid domains")
        for name, value in self.inputs.items():
            domain = self.valid_domain[name]
            if value < domain.min or (domain.max is not None and value > domain.max):
                raise ValueError(f"analytical input {name} is outside its valid domain")
        return self


Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
Revision = Annotated[str, Field(pattern=r"^[0-9a-f]{40}$")]


class RuntimeBaselineDomain(_Model):
    runtime_source: Revision
    native_sha256: Digest
    hardware_class: Annotated[str, Field(min_length=1)]
    harness_sha256: Digest
    method: Literal["rhf"]
    route: Literal["dense_conventional", "direct_scf"]
    threads_per_rank: Annotated[int, Field(strict=True, ge=1, le=1)]
    nbf: DimensionDomain

    @model_validator(mode="after")
    def bounded(self) -> Self:
        if self.nbf.min < 1 or self.nbf.max is None:
            raise ValueError("empirical baseline requires a bounded positive basis domain")
        return self


class BaselineTrainingObservation(_Model):
    observation_sha256: Digest
    peak_bytes: Bytes
    allocation_bytes: Count
    nbf: Annotated[int, Field(strict=True, ge=1)]


class EmpiricalBaseline(_Model):
    schema_name: Literal["vibeqc.runtime-memory-baseline/1"] = Field(alias="schema")

    @model_serializer(mode="wrap")
    def schema_alias(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        result: dict[str, Any] = handler(self)
        if "schema_name" in result:
            result["schema"] = result.pop("schema_name")
        return result
    method: Literal["max_positive_training_residual"]
    value_bytes: Count
    scope: Literal["rank"]
    profile_sha256: Digest
    domain: RuntimeBaselineDomain
    training: Annotated[list[BaselineTrainingObservation], Field(min_length=1)]

    @model_validator(mode="after")
    def training_domain(self) -> Self:
        if len({row.observation_sha256 for row in self.training}) != len(self.training):
            raise ValueError("duplicate empirical training observations")
        maximum = self.domain.nbf.max
        assert maximum is not None  # RuntimeBaselineDomain requires a finite bound.
        for row in self.training:
            if not self.domain.nbf.min <= row.nbf <= maximum:
                raise ValueError("empirical training observation outside domain")
        expected = max(max(0, row.peak_bytes - row.allocation_bytes) for row in self.training)
        if self.value_bytes != expected:
            raise ValueError("empirical baseline contradicts declared residual estimator")
        return self


class AnalyticalMemory(_Model):
    schema_name: Literal["vibeqc.analytical-memory/1"]
    scope: Scope
    complete: bool
    headroom_factor: Annotated[float, Field(ge=1, allow_inf_nan=False)]
    raw_peak_bytes: Bytes | None = None
    estimated_peak_bytes: Bytes | None = None
    terms: list[AllocationTerm]
    unknown: list[str]
    model_kind: Literal["analytical_allocations_with_empirical_residual"] | None = None
    empirical_baseline: EmpiricalBaseline | None = None

    @model_serializer(mode="wrap")
    def optional_provenance(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        result: dict[str, Any] = handler(self)
        for key in ("model_kind", "empirical_baseline"):
            if result.get(key) is None:
                result.pop(key, None)
        return result

    @model_validator(mode="after")
    def consistent(self) -> Self:
        if (self.model_kind is None) != (self.empirical_baseline is None):
            raise ValueError("empirical model kind requires baseline provenance")
        if self.empirical_baseline is not None:
            if self.empirical_baseline.scope != self.scope:
                raise ValueError("empirical baseline and allocation scopes disagree")
            domain = self.empirical_baseline.domain.nbf
            maximum = domain.max
            assert maximum is not None
            for term in self.terms:
                nbf = term.inputs.get("nbf")
                if nbf is not None and not domain.min <= nbf <= maximum:
                    raise ValueError("allocation outside empirical baseline domain")
        if any(term.scope != self.scope for term in self.terms):
            raise ValueError("analytical allocation and peak scopes disagree")
        if len({term.name for term in self.terms}) != len(self.terms):
            raise ValueError("duplicate analytical allocation names")
        if self.complete:
            if (self.unknown or not self.terms or self.raw_peak_bytes is None
                    or self.estimated_peak_bytes is None):
                raise ValueError("complete model requires terms and both peaks, without unknowns")
            if self.estimated_peak_bytes < self.raw_peak_bytes:
                raise ValueError("analytical headroom peak is below raw peak")
        elif self.raw_peak_bytes is not None or self.estimated_peak_bytes is not None:
            raise ValueError("incomplete analytical model must not claim a complete peak")
        return self


def from_manifest(block: Any) -> AnalyticalMemory:
    """Decode the product's flat TOML transport into an additive typed receipt."""
    if not isinstance(block, dict):
        raise ValueError("analytical_memory must be a TOML table")
    values = dict(block)
    values["schema_name"] = values.pop("schema", None)
    for encoded, target in (("terms_json", "terms"), ("unknown_json", "unknown")):
        raw = values.pop(encoded, None)
        if not isinstance(raw, str) or target in values:
            raise ValueError(f"analytical_memory requires {encoded}")
        values[target] = json.loads(raw)
    if "empirical_baseline_json" in values:
        raw = values.pop("empirical_baseline_json")
        if not isinstance(raw, str) or "empirical_baseline" in values:
            raise ValueError("invalid empirical baseline transport")
        values["empirical_baseline"] = json.loads(raw)
    return AnalyticalMemory.model_validate(values)
