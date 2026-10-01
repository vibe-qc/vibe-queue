"""#105 follow-up: retained allocation deploy logs are capped per program.

6226410 kept every ``deploy.*/`` attempt directory under
``<scratch_root>/.vq-admin/runtime-deploy/<program>/`` indefinitely. The
maintainer decided (2026-09-26) on a 100 MiB cap per program. These tests run
the real generated wrapper under ``bash`` with a fake ``sbatch`` and an
instant ``sleep``, and a cap lowered to a few KiB so the fixtures stay small.
"""
from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from vq import admin, config

FAKE_SBATCH = r"""#!/bin/sh
for arg do
  case "$arg" in
    --output=*) out=${arg#--output=} ;;
    --error=*) err=${arg#--error=} ;;
  esac
done
printf 'VQ-DEPLOY-METRIC runtime_store=reused\n' > "$out"
if [ -n "${BIG_KIB:-}" ]; then
  head -c $((BIG_KIB * 1024)) /dev/zero | tr '\0' 'x' >> "$out"
fi
: > "$err"
printf '4242\n'
exit 0
"""


@pytest.fixture
def wrapper(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    tools = tmp_path / "bin"
    tools.mkdir()
    (tools / "sbatch").write_text(FAKE_SBATCH)
    (tools / "sleep").write_text("#!/bin/sh\nexit 0\n")  # the settle loop, instantly
    for tool in tools.iterdir():
        tool.chmod(0o700)
    monkeypatch.setenv("PATH", f"{tools}{os.pathsep}{os.environ['PATH']}")
    scratch = tmp_path / "scratch"
    log_dir = scratch / ".vq-admin" / "runtime-deploy" / "p"
    log_dir.mkdir(parents=True)

    def run(*, cap_kib: int, big_kib: int = 0) -> subprocess.CompletedProcess:
        # raising=False only so this module can run against the uncapped
        # wrapper and fail there for the behavioural reason.
        monkeypatch.setattr(admin, "ALLOCATION_LOG_CAP_BYTES", cap_kib * 1024, raising=False)
        argv = admin._slurm_allocation_argv(
            config.SchedulerBuildAllocation(sbatch_args=["--partition=build"]),
            ["/site/bin/deploy"], SimpleNamespace(scratch_root=str(scratch)), "p",
        )
        env = {**os.environ, "BIG_KIB": str(big_kib) if big_kib else ""}
        return subprocess.run(argv, capture_output=True, text=True, env=env, timeout=60,
                              stdin=subprocess.DEVNULL, check=False)

    return SimpleNamespace(run=run, log_dir=log_dir, tmp=tmp_path)


def _old_attempts(log_dir: Path, sizes_kib: list[int]) -> list[Path]:
    """Attempt directories, newest first, each holding about ``size`` KiB."""
    now = time.time()
    made = []
    for index, size in enumerate(sizes_kib):
        attempt = log_dir / f"deploy.old{index:05d}"
        attempt.mkdir()
        (attempt / "deploy.out").write_bytes(b"x" * size * 1024)
        (attempt / "deploy.err").write_bytes(b"")
        stamp = now - 3600 * (index + 1)  # index 0 is the newest old attempt
        os.utime(attempt / "deploy.out", (stamp, stamp))
        os.utime(attempt, (stamp, stamp))
        made.append(attempt)
    return made


def _current(log_dir: Path, old: list[Path]) -> list[Path]:
    return [p for p in log_dir.iterdir() if p.is_dir() and p not in old]


def test_the_newest_attempts_that_fit_are_kept_and_the_rest_removed(wrapper) -> None:
    old = _old_attempts(wrapper.log_dir, [28, 28, 28, 28, 28])

    proc = wrapper.run(cap_kib=100)

    assert proc.returncode == 0
    assert "VQ-DEPLOY-METRIC runtime_store=reused" in proc.stdout  # streamed first
    assert [p.exists() for p in old] == [True, True, True, False, False]
    assert len(_current(wrapper.log_dir, old)) == 1


def test_removal_is_strictly_oldest_first(wrapper) -> None:
    # The newest old attempt alone passes the cap: it and every older attempt
    # go, although the older ones would fit on their own.
    old = _old_attempts(wrapper.log_dir, [200, 4, 4])

    wrapper.run(cap_kib=100)

    assert [p.exists() for p in old] == [False, False, False]
    assert len(_current(wrapper.log_dir, old)) == 1


def test_the_current_attempt_is_kept_even_when_it_alone_is_over_the_cap(wrapper) -> None:
    old = _old_attempts(wrapper.log_dir, [4])

    proc = wrapper.run(cap_kib=16, big_kib=64)

    [current] = _current(wrapper.log_dir, old)
    assert (current / "deploy.out").stat().st_size > 64 * 1024
    assert not old[0].exists()
    assert "runtime_store=reused" in proc.stdout


def test_the_legacy_shared_logs_are_removed(wrapper) -> None:
    for name in ("deploy.out", "deploy.err"):
        (wrapper.log_dir / name).write_text("pre-per-attempt transcript\n")

    wrapper.run(cap_kib=100)

    assert not (wrapper.log_dir / "deploy.out").exists()
    assert not (wrapper.log_dir / "deploy.err").exists()


def test_a_symlinked_entry_is_skipped_and_its_target_untouched(wrapper) -> None:
    outside = wrapper.tmp / "outside"
    outside.mkdir()
    (outside / "precious").write_bytes(b"y" * 64 * 1024)
    link = wrapper.log_dir / "deploy.link"
    link.symlink_to(outside, target_is_directory=True)
    old = _old_attempts(wrapper.log_dir, [200])  # passes the cap: removal mode
    # Older than the overflowing attempt, so it is reached in removal mode.
    # `rm -rf` of the link path alone would not touch the target either; the
    # guard is what leaves the link itself in place.
    stamp = time.time() - 7 * 86400
    os.utime(link, (stamp, stamp), follow_symlinks=False)

    wrapper.run(cap_kib=16)

    assert (outside / "precious").read_bytes() == b"y" * 64 * 1024
    assert link.is_symlink()
    assert not old[0].exists()


def test_the_default_cap_is_100_mib_and_reaches_the_script() -> None:
    assert admin.ALLOCATION_LOG_CAP_BYTES == 100 * 1024 * 1024
    script = admin._slurm_allocation_argv(
        config.SchedulerBuildAllocation(sbatch_args=[]), ["/site/bin/deploy"],
        SimpleNamespace(scratch_root="/site/scratch"), "p",
    )[2]
    assert "cap_kib=102400\n" in script
