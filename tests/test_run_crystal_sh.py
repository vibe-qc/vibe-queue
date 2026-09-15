"""Tests for contrib/run-crystal.sh env-var binary overrides (v0.5.19).

The CRYSTAL/PROPERTIES wrapper used to be purely PATH-driven (`command
-v crystal` etc.). v0.5.19 added env-var overrides so callers can point
at an absolute binary without touching the daemon's PATH:

  CRYSTAL_BIN, PCRYSTAL_BIN, PROPERTIES_BIN, PPROPERTIES_BIN

This module exercises the override logic against fake binaries on
disk. No real CRYSTAL install needed.

Why this matters: tests/integration_smoke.py reads
`vq programs --json` for the absolute path and passes it through here.
A daemon launched via ``nohup`` with a minimal PATH still runs CRYSTAL
correctly because the wrapper doesn't depend on a PATH lookup.
"""
from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

# All tests need bash. Should always be present on Linux/macOS; skip
# defensively on the off chance we're on Windows or a stripped-down
# image.
pytestmark = pytest.mark.skipif(
    shutil.which("bash") is None,
    reason="bash not available; these tests exercise the bash wrapper",
)


WRAPPER = (
    Path(__file__).parent.parent / "contrib" / "run-crystal.sh"
).resolve()


def _make_fake_bin(path: Path, body: str) -> Path:
    """Drop a tiny bash script at ``path``, chmod 755. Returns ``path``."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"#!/bin/bash\n{body}\n")
    path.chmod(0o755)
    return path


def _run_wrapper(
    tmp_path: Path,
    *args: str,
    env_overrides: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run the wrapper from ``tmp_path`` (so it sees the test's
    workspace). Default env has PATH=/usr/bin:/bin (no crystal anywhere
    — forces the env-var override path); ``env_overrides`` always wins,
    so a test that needs a custom PATH (parallel tests need a fake
    mpirun discoverable on PATH) can supply one."""
    env = {
        # Bash itself needs PATH to find subprocesses like cp, find,
        # mv (used by the parallel-mode cleanup). /usr/bin + /bin
        # covers the userspace utilities everywhere.
        "PATH": "/usr/bin:/bin",
        # bash needs HOME for tilde expansion; keep it.
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


class TestSerialEnvOverride:
    """``CRYSTAL_BIN=`` makes the wrapper invoke the override binary
    instead of doing a PATH lookup for ``crystal``."""

    def test_crystal_bin_used_when_set(self, tmp_path: Path) -> None:
        fake_bin = _make_fake_bin(
            tmp_path / "fake-crystal",
            'echo "FAKE CRYSTAL SERIAL OK"\ncat -',
        )
        (tmp_path / "h2.d12").write_text("dummy CRYSTAL input")

        proc = _run_wrapper(
            tmp_path,
            "--serial", "h2.d12",
            env_overrides={"CRYSTAL_BIN": str(fake_bin)},
        )
        assert proc.returncode == 0, (
            f"wrapper exited {proc.returncode}\n"
            f"stdout: {proc.stdout!r}\nstderr: {proc.stderr!r}"
        )
        # Wrapper redirects serial stdout+stderr to <input>.out
        out_file = tmp_path / "h2.out"
        assert out_file.exists(), "wrapper didn't create the output file"
        content = out_file.read_text()
        assert "FAKE CRYSTAL SERIAL OK" in content
        assert "dummy CRYSTAL input" in content
        # The wrapper's banner line should mention the absolute path
        # of the override binary (proves env override was applied).
        assert str(fake_bin) in proc.stderr

    def test_properties_bin_used_when_set(self, tmp_path: Path) -> None:
        """Symmetric: --properties --serial honors PROPERTIES_BIN."""
        fake_bin = _make_fake_bin(
            tmp_path / "fake-properties",
            'echo "FAKE PROPERTIES OK"\ncat -',
        )
        (tmp_path / "prop.d3").write_text("dummy PROPERTIES input")

        proc = _run_wrapper(
            tmp_path,
            "--serial", "--properties", "prop.d3",
            env_overrides={"PROPERTIES_BIN": str(fake_bin)},
        )
        assert proc.returncode == 0, (
            f"wrapper exited {proc.returncode}\nstderr: {proc.stderr!r}"
        )
        assert (tmp_path / "prop.out").read_text().__contains__(
            "FAKE PROPERTIES OK"
        )
        # Banner should mention PROPERTIES + the absolute override path
        assert "PROPERTIES" in proc.stderr
        assert str(fake_bin) in proc.stderr

    def test_path_fallback_still_works(self, tmp_path: Path) -> None:
        """Unset env var: wrapper falls back to PATH lookup. This is the
        pre-v0.5.19 behaviour; the regression we'd catch is if someone
        accidentally made the env var required."""
        # Put a fake crystal on a custom dir, prepend that dir to PATH.
        bin_dir = tmp_path / "bin"
        fake_bin = _make_fake_bin(
            bin_dir / "crystal",
            'echo "PATH CRYSTAL OK"\ncat -',
        )
        (tmp_path / "x.d12").write_text("path test")

        env = {
            "PATH": f"{bin_dir}:/usr/bin:/bin",
            "HOME": str(tmp_path),
        }
        proc = subprocess.run(
            ["bash", str(WRAPPER), "--serial", "x.d12"],
            cwd=str(tmp_path),
            env=env,
            capture_output=True,
            text=True,
        )
        assert proc.returncode == 0, (
            f"wrapper exited {proc.returncode}\nstderr: {proc.stderr!r}"
        )
        assert "PATH CRYSTAL OK" in (tmp_path / "x.out").read_text()
        # Banner should mention PATH-resolved binary
        assert str(fake_bin) in proc.stderr


class TestParallelEnvOverride:
    """``PCRYSTAL_BIN=`` makes the wrapper use the override Pcrystal,
    even when ``Pcrystal`` is nowhere on PATH. ``mpirun`` is still
    PATH-resolved (on real hosts that's /usr/bin/mpirun)."""

    def test_pcrystal_bin_used_when_set(self, tmp_path: Path) -> None:
        # Fake Pcrystal: reads ./INPUT (parallel convention), prints marker
        fake_pcrystal = _make_fake_bin(
            tmp_path / "fake-Pcrystal",
            'echo "FAKE PCRYSTAL OK"\n'
            '[[ -f INPUT ]] && echo "saw INPUT=$(cat INPUT)"',
        )
        # Fake mpirun: ignore -np N, exec the last arg.
        # Real mpirun signature: mpirun -np N <binary> [args...]
        bin_dir = tmp_path / "bin"
        _make_fake_bin(
            bin_dir / "mpirun",
            'shift; shift  # drop -np and rank count\nexec "$@"',
        )
        (tmp_path / "h2.d12").write_text("PARALLEL INPUT")

        proc = _run_wrapper(
            tmp_path,
            "--np", "4", "h2.d12",
            env_overrides={
                "PCRYSTAL_BIN": str(fake_pcrystal),
                "PATH": f"{bin_dir}:/usr/bin:/bin",
            },
        )
        assert proc.returncode == 0, (
            f"wrapper exited {proc.returncode}\n"
            f"stdout: {proc.stdout!r}\nstderr: {proc.stderr!r}"
        )
        # Parallel mode: stdout+stderr -> <name>.out
        out = (tmp_path / "h2.out").read_text()
        assert "FAKE PCRYSTAL OK" in out
        assert "saw INPUT=PARALLEL INPUT" in out
        # Wrapper banner should mention parallel + override path
        assert "parallel" in proc.stderr
        assert str(fake_pcrystal) in proc.stderr

    def test_parallel_accepts_input_file_already_named_INPUT(
        self, tmp_path: Path
    ) -> None:
        fake_pcrystal = _make_fake_bin(
            tmp_path / "fake-Pcrystal",
            'echo "FAKE PCRYSTAL OK"\n'
            '[[ -f INPUT ]] && echo "saw INPUT=$(cat INPUT)"',
        )
        bin_dir = tmp_path / "bin"
        _make_fake_bin(
            bin_dir / "mpirun",
            'shift; shift  # drop -np and rank count\nexec "$@"',
        )
        input_file = tmp_path / "INPUT"
        input_file.write_text("DIRECT INPUT")

        proc = _run_wrapper(
            tmp_path,
            "--np", "4", "INPUT",
            env_overrides={
                "PCRYSTAL_BIN": str(fake_pcrystal),
                "PATH": f"{bin_dir}:/usr/bin:/bin",
            },
        )

        assert proc.returncode == 0, (
            f"wrapper exited {proc.returncode}\n"
            f"stdout: {proc.stdout!r}\nstderr: {proc.stderr!r}"
        )
        assert input_file.read_text() == "DIRECT INPUT"
        assert not list(tmp_path.glob("INPUT.run-crystal.bak.*"))
        out = (tmp_path / "INPUT.out").read_text()
        assert "FAKE PCRYSTAL OK" in out
        assert "saw INPUT=DIRECT INPUT" in out

    def test_pproperties_bin_used_when_set(self, tmp_path: Path) -> None:
        """Parallel PROPERTIES uses PPROPERTIES_BIN, again with a fake
        mpirun on PATH."""
        fake_pp = _make_fake_bin(
            tmp_path / "fake-Pproperties",
            'echo "FAKE PPROPERTIES OK"',
        )
        bin_dir = tmp_path / "bin"
        _make_fake_bin(
            bin_dir / "mpirun",
            'shift; shift\nexec "$@"',
        )
        (tmp_path / "x.d3").write_text("dummy")

        proc = _run_wrapper(
            tmp_path,
            "--np", "2", "--properties", "x.d3",
            env_overrides={
                "PPROPERTIES_BIN": str(fake_pp),
                "PATH": f"{bin_dir}:/usr/bin:/bin",
            },
        )
        assert proc.returncode == 0, (
            f"wrapper exited {proc.returncode}\nstderr: {proc.stderr!r}"
        )
        out = (tmp_path / "x.out").read_text()
        assert "FAKE PPROPERTIES OK" in out


class TestArgumentSafety:
    def test_rejects_extra_positional_arguments(self, tmp_path: Path) -> None:
        (tmp_path / "h2.d12").write_text("dummy CRYSTAL input")

        proc = _run_wrapper(tmp_path, "--serial", "h2.d12", "h2.out", "extra")

        assert proc.returncode == 2
        assert "too many positional arguments: extra" in proc.stderr
        assert not (tmp_path / "h2.out").exists()

    def test_rejects_output_that_would_overwrite_input(
        self, tmp_path: Path
    ) -> None:
        input_file = tmp_path / "h2.d12"
        input_text = "dummy CRYSTAL input"
        input_file.write_text(input_text)

        proc = _run_wrapper(tmp_path, "--serial", "./h2.d12", "h2.d12")

        assert proc.returncode == 2
        assert "output would overwrite input: h2.d12" in proc.stderr
        assert input_file.read_text() == input_text


class TestMissingBinaryStillFails:
    """Sanity: if neither env var nor PATH lookup finds a binary, the
    wrapper still fails with a helpful message (exit 127). The env-var
    overrides MUST NOT silently mask a real configuration error."""

    def test_no_env_no_path_serial(self, tmp_path: Path) -> None:
        (tmp_path / "h2.d12").write_text("dummy")
        proc = _run_wrapper(tmp_path, "--serial", "h2.d12")
        assert proc.returncode == 127
        assert "binary not found" in proc.stderr

    def test_no_env_no_path_parallel(self, tmp_path: Path) -> None:
        (tmp_path / "h2.d12").write_text("dummy")
        proc = _run_wrapper(tmp_path, "--np", "2", "h2.d12")
        assert proc.returncode == 127
        # Either Pcrystal-missing OR mpirun-missing is acceptable —
        # both are real configuration errors the user must fix.
        assert "not found" in proc.stderr or "not on PATH" in proc.stderr


class TestHelpAdvertisesEnvVars:
    """`run-crystal.sh --help` should mention the env vars so users
    discover them without grepping the source."""

    def test_help_mentions_crystal_bin(self) -> None:
        proc = subprocess.run(
            ["bash", str(WRAPPER), "--help"],
            capture_output=True, text=True,
        )
        assert proc.returncode == 0
        assert "CRYSTAL_BIN" in proc.stdout
        assert "PCRYSTAL_BIN" in proc.stdout
        assert "PROPERTIES_BIN" in proc.stdout
        assert "PPROPERTIES_BIN" in proc.stdout


def test_parallel_requires_explicit_allocation_before_running(tmp_path: Path) -> None:
    (tmp_path / "h2.d12").write_text("scientific input")
    proc = _run_wrapper(tmp_path, "h2.d12")
    assert proc.returncode == 2
    assert "parallel execution requires --np N" in proc.stderr
    assert not (tmp_path / "INPUT").exists()
    assert not (tmp_path / "h2.out").exists()


@pytest.mark.parametrize("value", ["0", "-1", "invalid"])
def test_parallel_rejects_invalid_allocation(tmp_path: Path, value: str) -> None:
    (tmp_path / "h2.d12").write_text("scientific input")
    proc = _run_wrapper(tmp_path, "--np", value, "h2.d12")
    assert proc.returncode == 2
    assert "positive integer" in proc.stderr
    assert not (tmp_path / "INPUT").exists()
