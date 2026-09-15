"""Tests for contrib/run-orca.sh.

The ORCA wrapper hides two conventions vq's argv pass-through cannot express:
ORCA writes its main output to stdout, and ORCA should be invoked by absolute
path so sibling executables are discoverable. The tests use fake ORCA binaries;
no ORCA installation is required.
"""
from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    shutil.which("bash") is None,
    reason="bash not available; these tests exercise the bash wrapper",
)


WRAPPER = (Path(__file__).parent.parent / "contrib" / "run-orca.sh").resolve()


def _make_fake_bin(path: Path, body: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"#!/usr/bin/env bash\n{body}\n")
    path.chmod(0o755)
    return path


def _run_wrapper(
    tmp_path: Path,
    *args: str,
    env_overrides: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    env = {
        "PATH": "/usr/bin:/bin",
        "HOME": str(tmp_path),
    }
    if env_overrides:
        env.update(env_overrides)
    return subprocess.run(
        ["bash", str(WRAPPER), *args],
        cwd=str(tmp_path),
        env=env,
        capture_output=True,
        text=True,
    )


def test_orca_bin_override_redirects_stdout_and_cleans_scratch(tmp_path: Path) -> None:
    fake_orca = _make_fake_bin(
        tmp_path / "fake-orca",
        'echo "FAKE ORCA OK"\n'
        'echo "input=$1"\n'
        "touch cell.tmp1 cell_atom01.inp cell_atom01.out cell_atom01.gbw cell.bas\n"
        "mkdir -p cell.0proc",
    )
    (tmp_path / "cell.inp").write_text("! fake ORCA input\n")

    proc = _run_wrapper(
        tmp_path,
        "cell.inp",
        env_overrides={"ORCA_BIN": str(fake_orca)},
    )

    assert proc.returncode == 0, (
        f"wrapper exited {proc.returncode}\n"
        f"stdout: {proc.stdout!r}\nstderr: {proc.stderr!r}"
    )
    out = (tmp_path / "cell.out").read_text()
    assert "FAKE ORCA OK" in out
    assert "input=cell.inp" in out
    assert str(fake_orca) in proc.stderr
    assert "cleanup: removed" in proc.stderr
    assert not (tmp_path / "cell.tmp1").exists()
    assert not (tmp_path / "cell_atom01.inp").exists()
    assert not (tmp_path / "cell_atom01.out").exists()
    assert not (tmp_path / "cell_atom01.gbw").exists()
    assert not (tmp_path / "cell.bas").exists()
    assert not (tmp_path / "cell.0proc").exists()


def test_keep_scratch_preserves_success_scratch(tmp_path: Path) -> None:
    fake_orca = _make_fake_bin(
        tmp_path / "fake-orca",
        'echo "FAKE ORCA OK"\n'
        "touch cell.tmp1 cell_atom01.inp cell.bas\n"
        "mkdir -p cell.0proc",
    )
    (tmp_path / "cell.inp").write_text("! fake ORCA input\n")

    proc = _run_wrapper(
        tmp_path,
        "--keep-scratch",
        "cell.inp",
        env_overrides={"ORCA_BIN": str(fake_orca)},
    )

    assert proc.returncode == 0, proc.stderr
    assert (tmp_path / "cell.tmp1").exists()
    assert (tmp_path / "cell_atom01.inp").exists()
    assert (tmp_path / "cell.bas").exists()
    assert (tmp_path / "cell.0proc").is_dir()


def test_output_argument_overrides_default_outfile(tmp_path: Path) -> None:
    fake_orca = _make_fake_bin(
        tmp_path / "fake-orca",
        'echo "CUSTOM OUT OK"\n',
    )
    (tmp_path / "cell.inp").write_text("! fake ORCA input\n")

    proc = _run_wrapper(
        tmp_path,
        "cell.inp",
        "orca-main.out",
        env_overrides={"ORCA_BIN": str(fake_orca)},
    )

    assert proc.returncode == 0, proc.stderr
    assert not (tmp_path / "cell.out").exists()
    assert "CUSTOM OUT OK" in (tmp_path / "orca-main.out").read_text()
    assert "output: orca-main.out" in proc.stderr


def test_rejects_extra_positional_arguments(tmp_path: Path) -> None:
    (tmp_path / "cell.inp").write_text("! fake ORCA input\n")

    proc = _run_wrapper(tmp_path, "cell.inp", "cell.out", "extra")

    assert proc.returncode == 2
    assert "too many positional arguments: extra" in proc.stderr
    assert not (tmp_path / "cell.out").exists()


def test_rejects_output_that_would_overwrite_input(tmp_path: Path) -> None:
    input_file = tmp_path / "cell.inp"
    input_text = "! fake ORCA input\n"
    input_file.write_text(input_text)

    proc = _run_wrapper(tmp_path, "./cell.inp", "cell.inp")

    assert proc.returncode == 2
    assert "output would overwrite input: cell.inp" in proc.stderr
    assert input_file.read_text() == input_text


def test_path_lookup_still_finds_orca_when_env_unset(tmp_path: Path) -> None:
    bin_dir = tmp_path / "bin"
    fake_orca = _make_fake_bin(
        bin_dir / "orca",
        'echo "PATH ORCA OK"\n',
    )
    (tmp_path / "cell.inp").write_text("! fake ORCA input\n")

    proc = _run_wrapper(
        tmp_path,
        "cell.inp",
        env_overrides={"PATH": f"{bin_dir}:/usr/bin:/bin"},
    )

    assert proc.returncode == 0, proc.stderr
    assert "PATH ORCA OK" in (tmp_path / "cell.out").read_text()
    assert str(fake_orca) in proc.stderr


def test_missing_orca_reports_helpful_error(tmp_path: Path) -> None:
    (tmp_path / "cell.inp").write_text("! fake ORCA input\n")

    proc = _run_wrapper(tmp_path, "cell.inp")

    assert proc.returncode == 127
    assert "orca binary not found" in proc.stderr
    assert "PATH=/usr/bin:/bin" in proc.stderr
