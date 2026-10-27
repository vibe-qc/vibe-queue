"""Product allocation evidence is preserved without queue-side chemistry."""
from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from vq.estimate_collect import collect
from vq.resource_estimate import EstimateError, ResourceEstimate, estimate_vibeqc
from vq.spec import JobSpec, JobState


def _block(*, complete: bool = False) -> dict:
    result = {
        "schema": "vibeqc.analytical-memory/1", "scope": "rank",
        "complete": complete, "headroom_factor": 1.5,
        "terms_json": json.dumps([{
            "name": "eri", "route": "dense", "inputs": {"nbf": 20},
            "units": "bytes", "scope": "rank", "valid_domain": {"nbf": {"min": 1}},
            "value_bytes": 1280000,
        }]),
        "unknown_json": json.dumps([] if complete else ["unmodelled_workspace"]),
    }
    if complete:
        result.update(raw_peak_bytes=1280000, estimated_peak_bytes=1920000)
    return result


def _manifest(tmp_path: Path, block: dict | None, *, scope: str = "rank") -> Path:
    path = tmp_path / "calc.system"
    text = ('[plan]\nmethod="hf"\n[outputs]\nstatus="dry_run"\n'
            '[memory]\nestimate_bytes=9999999\n'
            '[resource_evidence]\nschema="vibeqc.resource-evidence/1"\n'
            'input_coverage="explicit_reads"\ninput_sha256=[]\nnbf=20\n'
            f'memory_scope="{scope}"\n')
    if block is not None:
        text += "[analytical_memory]\n"
        text += "".join(f"{key}={json.dumps(value)}\n" for key, value in block.items())
    path.write_text(text)
    return path


def test_partial_terms_never_turn_into_a_whole_route_peak(tmp_path: Path) -> None:
    result = estimate_vibeqc(_manifest(tmp_path, _block()))
    assert result.memory.peak_bytes is None
    assert result.memory.raw_bytes is None
    assert result.effective_job_memory_bytes is None
    assert result.memory.headroom_factor == 1.5
    assert "analytical_memory.unmodelled_workspace" in result.unknown_fields
    assert result.analytical_memory.terms[0].value_bytes == 1280000
    assert ResourceEstimate.model_validate_json(result.model_dump_json()) == result


def test_complete_product_peak_is_not_summed_or_scaled_by_queue(tmp_path: Path) -> None:
    result = estimate_vibeqc(_manifest(tmp_path, _block(complete=True)))
    assert result.memory.raw_bytes == 1280000
    assert result.memory.peak_bytes == 1920000
    assert result.memory.scope == "rank"
    assert result.effective_job_memory_bytes is None
    assert "memory.raw_bytes" not in result.unknown_fields
    assert "memory.headroom_factor" not in result.unknown_fields


def test_legacy_serialization_does_not_add_an_empty_model(tmp_path: Path) -> None:
    result = estimate_vibeqc(_manifest(tmp_path, None))
    assert "analytical_memory" not in result.model_dump()
    assert result.memory.peak_bytes == 9999999


def test_collection_preserves_analytical_terms_verbatim(tmp_path: Path) -> None:
    receipt = estimate_vibeqc(_manifest(tmp_path, _block())).model_dump(mode="json")
    spec = JobSpec(id="abc123456789", command=["true"], cwd=str(tmp_path), cpus=1,
                   state=JobState.COMPLETED, resource_estimate=receipt)
    summary = collect([spec], tmp_path / "collected", hardware_class="test-class", set_id="s")
    assert summary["cases"] == 1
    kept = json.loads((tmp_path / "collected/estimates/abc123456789.json").read_text())
    assert kept == receipt


def test_analytical_scope_has_product_provenance_without_input_coverage(tmp_path: Path) -> None:
    path = _manifest(tmp_path, _block())
    text = path.read_text()
    start, rest = text.split("[resource_evidence]\n")
    _, analytical = rest.split("[analytical_memory]\n")
    path.write_text(start + "[analytical_memory]\n" + analytical)
    result = estimate_vibeqc(path)
    assert result.feature_source == "product"
    assert result.feature_provenance["memory_scope"] == "product_resolved"
    assert result.product_input_coverage is None
    assert result.input_binding == "manifest_only"


@pytest.mark.parametrize("change", [
    {"units": "MiB"}, {"scope": "job"}, {"inputs": {"nbf": 0}},
    {"inputs": {"nbf": 21}}, {"inputs": {"nbf": True}},
    {"valid_domain": {}}, {"valid_domain": {"nbf": {"min": 21}}},
    {"value_bytes": -1},
])
def test_bad_term_or_conflicting_dimension_refuses(tmp_path: Path, change: dict) -> None:
    block = _block()
    terms = json.loads(block["terms_json"])
    terms[0].update(change)
    block["terms_json"] = json.dumps(terms)
    with pytest.raises(EstimateError):
        estimate_vibeqc(_manifest(tmp_path, block))


def test_scope_conflict_refuses_instead_of_aggregating(tmp_path: Path) -> None:
    with pytest.raises(EstimateError, match="scope contradicts"):
        estimate_vibeqc(_manifest(tmp_path, _block(), scope="job"))


@pytest.mark.parametrize("change", [
    {"complete": True}, {"headroom_factor": float("inf")},
    {"raw_peak_bytes": 1280000}, {"terms_json": "{}"},
    {"schema": "vibeqc.analytical-memory/2"},
])
def test_invalid_model_cannot_fall_back_to_scalar(tmp_path: Path, change: dict) -> None:
    block = copy.deepcopy(_block())
    block.update(change)
    with pytest.raises(EstimateError):
        estimate_vibeqc(_manifest(tmp_path, block))


def _empirical_manifest(tmp_path: Path, mutation=None) -> Path:
    baseline = {
        "schema": "vibeqc.runtime-memory-baseline/1",
        "method": "max_positive_training_residual", "value_bytes": 720000,
        "scope": "rank", "profile_sha256": "a" * 64,
        "domain": {"runtime_source": "b" * 40, "native_sha256": "c" * 64,
                   "hardware_class": "test-class", "harness_sha256": "d" * 64,
                   "method": "rhf", "route": "dense_conventional",
                   "threads_per_rank": 1, "nbf": {"min": 20, "max": 30}},
        "training": [
            {"observation_sha256": "e" * 64, "peak_bytes": 2000000,
             "allocation_bytes": 1280000, "nbf": 20},
            {"observation_sha256": "f" * 64, "peak_bytes": 2400000,
             "allocation_bytes": 1800000, "nbf": 30}],
    }
    if mutation:
        mutation(baseline)
    block = _block(complete=True)
    block.update(model_kind="analytical_allocations_with_empirical_residual",
                 empirical_baseline_json=json.dumps(baseline),
                 raw_peak_bytes=2000000, estimated_peak_bytes=3000000)
    path = _manifest(tmp_path, block)
    text = path.read_text().replace('method="hf"', 'method="RHF"')
    text = text.replace('[analytical_memory]',
                        'algorithm="dense_conventional"\nthreads_per_rank=1\n'
                        '[analytical_memory]')
    text += f'\n[vibeqc]\ngit_sha="{"b" * 40}"\nnative_core_sha256="{"c" * 64}"\n'
    path.write_text(text)
    return path


def test_empirical_baseline_remains_distinct_and_survives_collection(tmp_path):
    receipt = estimate_vibeqc(_empirical_manifest(tmp_path)).model_dump(mode="json")
    evidence = receipt["analytical_memory"]
    assert evidence["empirical_baseline"]["value_bytes"] == 720000
    assert evidence["terms"][0]["value_bytes"] == 1280000
    assert receipt["memory"]["raw_bytes"] == 2000000
    assert receipt["memory"]["peak_bytes"] == 3000000  # Program headroom applied once.
    assert ResourceEstimate.model_validate(receipt).model_dump(mode="json") == receipt
    assert (ResourceEstimate.model_validate(receipt)
            .model_dump(mode="json", by_alias=True) == receipt)
    spec = JobSpec(id="abc123456789", command=["true"], cwd=str(tmp_path), cpus=1,
                   state=JobState.COMPLETED, resource_estimate=receipt)
    collect([spec], tmp_path / "collected", hardware_class="test-class", set_id="s")
    assert json.loads((tmp_path / "collected/estimates/abc123456789.json").read_text()) == receipt
    legacy = estimate_vibeqc(_manifest(tmp_path, _block())).model_dump(mode="json")
    assert "empirical_baseline" not in legacy["analytical_memory"]
    assert "model_kind" not in legacy["analytical_memory"]


@pytest.mark.parametrize("mutation", [
    lambda x: x.update(value_bytes=720001),
    lambda x: x.update(scope="job"),
    lambda x: x.update(training=[]),
    lambda x: x["training"].append(x["training"][0]),
    lambda x: x["domain"].update(native_sha256="0" * 64),
    lambda x: x["domain"].update(runtime_source="0" * 40),
    lambda x: x["domain"].update(threads_per_rank=True),
    lambda x: x["domain"].update(route="direct_scf"),
    lambda x: x["domain"].update(nbf={"min": 21, "max": 30}),
    lambda x: x["domain"].update(nbf={"min": 1}),
])
def test_empirical_profile_refuses_inconsistent_provenance(tmp_path, mutation):
    with pytest.raises(EstimateError):
        estimate_vibeqc(_empirical_manifest(tmp_path, mutation))
