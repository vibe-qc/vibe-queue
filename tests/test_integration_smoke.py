"""Focused regressions for the live integration-smoke definitions."""

from __future__ import annotations

import shlex
from pathlib import Path

import pytest

from tests import integration_smoke as smoke


def _binary_record(name: str, binary: Path) -> dict[str, object]:
    return {
        "name": name,
        "kind": "binary",
        "status": "OK",
        "reason": "",
        "binary": str(binary),
    }


def test_orca_submit_uses_separate_primary_and_converter_records(
    tmp_path: Path,
) -> None:
    engine = smoke._engine_orca()
    orca = tmp_path / "ORCA install" / "orca"
    orca_2mkl = tmp_path / "ORCA install" / "orca_2mkl"

    args = engine.submit(
        tmp_path,
        _binary_record("orca", orca),
        {"orca_2mkl": _binary_record("orca_2mkl", orca_2mkl)},
    )

    runner = (tmp_path / "run_orca_smoke.sh").read_text(encoding="utf-8")
    orca_command = f"{shlex.quote(str(orca))} h2.inp"
    converter_command = f"{shlex.quote(str(orca_2mkl))} h2 -molden"
    assert runner.index(orca_command) < runner.index(converter_command)
    assert "test -s h2.molden.input" in runner
    assert args[-2:] == ["bash", "run_orca_smoke.sh"]
    assert engine.companion_programs == {"orca_2mkl": "binary"}
    assert "h2.molden.input" in engine.must_have_files
    assert engine.must_have_nonempty_files == ["h2.molden.input"]


def test_orca_smoke_skips_before_submit_without_converter_record(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unexpected_submit(*_args: str) -> str:
        pytest.fail("vq submit must not run without the companion record")

    monkeypatch.setattr(smoke, "vq", unexpected_submit)

    result = smoke.run_engine(
        smoke._engine_orca(),
        fetch_dir=tmp_path / "fetched",
        programs={"orca": _binary_record("orca", tmp_path / "orca")},
    )

    assert result.status == "SKIP"
    assert "required companion" in result.reason
    assert "orca_2mkl" in result.reason


@pytest.mark.parametrize(
    ("molden_contents", "expected_status"),
    [(b"", "FAIL"), (b"[Molden Format]\n", "PASS")],
)
def test_orca_smoke_requires_nonempty_molden_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    molden_contents: bytes,
    expected_status: str,
) -> None:
    monkeypatch.setattr(smoke, "vq", lambda *_args: "job-1\n")
    monkeypatch.setattr(
        smoke,
        "poll_until_terminal",
        lambda _jobid, *, max_wait_seconds: ("completed", 0.1),
    )

    def fake_fetch(jobid: str, dest: Path) -> Path:
        workspace = dest / jobid
        workspace.mkdir(parents=True)
        (workspace / "stdout.log").write_text(
            "FINAL SINGLE POINT ENERGY\n",
            encoding="utf-8",
        )
        (workspace / "h2.gbw").write_bytes(b"gbw")
        (workspace / "h2.molden.input").write_bytes(molden_contents)
        return workspace

    monkeypatch.setattr(smoke, "fetch_workspace", fake_fetch)
    programs = {
        "orca": _binary_record("orca", tmp_path / "orca"),
        "orca_2mkl": _binary_record("orca_2mkl", tmp_path / "orca_2mkl"),
    }

    result = smoke.run_engine(
        smoke._engine_orca(),
        fetch_dir=tmp_path / "fetched",
        programs=programs,
    )

    assert result.status == expected_status
    if expected_status == "FAIL":
        assert result.reason == "empty artifacts: ['h2.molden.input']"
