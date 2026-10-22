"""Read-only preview semantics: evidence, unknowns and authoritative limits."""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq.cli import main
from vq.resource_estimate import EstimateError, estimate_vibeqc


def _manifest(tmp_path: Path, memory: str = "estimate_bytes = 1200000") -> Path:
    path = tmp_path / "calculation.system"
    path.write_text(
        '[vibeqc]\nversion = "example"\n[plan]\nmethod = "hf"\n'
        'basis = "custom"\njob_kind = "single_point"\n'
        f'[outputs]\nstatus = "dry_run"\n[memory]\n{memory}\n'
    )
    return path


def test_legacy_estimate_keeps_headroom_and_unknown_aggregation(tmp_path: Path) -> None:
    result = estimate_vibeqc(_manifest(tmp_path))
    assert result.memory.peak_bytes == 1200000
    assert result.memory.headroom == "included_by_program"
    assert result.memory.raw_bytes is None
    assert result.memory.headroom_factor is None
    assert result.features.nbf is None  # Never guess from "custom".
    assert result.effective_job_memory_bytes is None
    assert result.runtime_seconds is None
    assert "memory.scope" in result.unknown_fields


def test_program_routes_with_same_basis_count_keep_their_own_estimate(tmp_path: Path) -> None:
    features = tmp_path / "features.json"
    estimates = []
    for algorithm, peak in (("dense-eri", 1600000), ("direct", 400000)):
        features.write_text(json.dumps({"nbf": 20, "algorithm": algorithm, "memory_scope": "job"}))
        result = estimate_vibeqc(_manifest(tmp_path, f"estimate_bytes = {peak}"),
                                 features_file=features)
        estimates.append(result)
    assert estimates[0].features.nbf == estimates[1].features.nbf
    assert estimates[0].memory.peak_bytes > estimates[1].memory.peak_bytes
    assert estimates[0].input_sha256 != estimates[1].input_sha256
    assert estimates[1].effective_job_memory_bytes == 400000


def test_rank_estimate_is_not_silently_scaled_and_override_survives(tmp_path: Path) -> None:
    features = tmp_path / "features.json"
    features.write_text('{"mpi_ranks": 8, "memory_scope": "rank"}')
    result = estimate_vibeqc(_manifest(tmp_path), features_file=features, memory_mb=1)
    assert result.memory.peak_bytes == 1200000
    assert result.effective_job_memory_bytes == 1024**2
    assert any("MPI ranks" in warning for warning in result.warnings)
    features.write_text('{"memory_scope": "job"}')
    result = estimate_vibeqc(_manifest(tmp_path), features_file=features, memory_mb=1)
    assert result.effective_job_memory_bytes == 1024**2
    assert any("override preserved" in warning for warning in result.warnings)


@pytest.mark.parametrize("value", ["true", "-1", '"1200"', "1.5"])
def test_malformed_memory_is_explicit(tmp_path: Path, value: str) -> None:
    with pytest.raises(EstimateError, match="positive integer"):
        estimate_vibeqc(_manifest(tmp_path, f"estimate_bytes = {value}"))


def test_cli_never_executes_input_and_digest_invalidates(tmp_path: Path, monkeypatch) -> None:
    path = _manifest(tmp_path)
    script = tmp_path / "input.py"
    script.write_text("raise RuntimeError('this script must never run')\n")
    original = path.read_bytes()
    monkeypatch.setattr("subprocess.Popen", lambda *a, **kw: pytest.fail("executed an input"))
    arguments = ["estimate", str(path), "--input", str(script), "--json"]
    first = CliRunner().invoke(main, arguments)
    assert first.exit_code == 0, first.output
    before = json.loads(first.output)
    script.write_text("raise RuntimeError('changed input')\n")
    second = CliRunner().invoke(main, arguments)
    assert second.exit_code == 0, second.output
    assert before["input_sha256"] != json.loads(second.output)["input_sha256"]
    assert path.read_bytes() == original


def test_unsupported_route_and_invalid_evidence(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path, "")
    result = estimate_vibeqc(manifest)
    assert result.memory.peak_bytes is None
    assert "memory.peak_bytes" in result.unknown_fields
    features = tmp_path / "features.json"
    features.write_text('{"method": "different"}')
    with pytest.raises(EstimateError, match="contradicts"):
        estimate_vibeqc(manifest, features_file=features)
    manifest.write_text("[broken")
    result_cli = CliRunner().invoke(main, ["estimate", str(manifest), "--json"])
    assert result_cli.exit_code != 0
    assert "malformed" in result_cli.output


def test_completed_manifest_is_not_used_as_a_prediction(tmp_path: Path) -> None:
    path = _manifest(tmp_path)
    path.write_text(path.read_text().replace('"dry_run"', '"complete"'))
    with pytest.raises(EstimateError, match="dry_run"):
        estimate_vibeqc(path)
