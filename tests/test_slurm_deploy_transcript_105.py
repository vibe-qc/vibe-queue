"""Run the real allocation shell against deterministic scheduler/filesystem fakes."""
from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from vq import admin, config


@pytest.fixture
def allocation_shell(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    tools = tmp_path / "bin"
    tools.mkdir()
    sbatch = tools / "sbatch"
    sbatch.write_text('''#!/bin/sh
for arg do
    case "$arg" in
        --output=*) out=${arg#--output=} ;;
        --error=*) err=${arg#--error=} ;;
    esac
done
printf '%s\\n' "$out" > "$TRANSCRIPT_FIXTURE/out-path"
printf '%s\\n' "$err" > "$TRANSCRIPT_FIXTURE/err-path"
printf '%s\\n' "$out" >> "$TRANSCRIPT_FIXTURE/attempts"
printf '0\\n' > "$TRANSCRIPT_FIXTURE/ticks"
if [ "$TRANSCRIPT_MODE" = immediate ]; then
    printf 'VQ-DEPLOY-METRIC runtime_store=reused\\n' > "$out"
    : > "$err"
fi
printf '12345\\n'
exit "$TRANSCRIPT_RC"
''')
    sleep = tools / "sleep"
    sleep.write_text('''#!/bin/sh
n=$(cat "$TRANSCRIPT_FIXTURE/ticks")
n=$((n + 1))
printf '%s\\n' "$n" > "$TRANSCRIPT_FIXTURE/ticks"
out=$(cat "$TRANSCRIPT_FIXTURE/out-path")
err=$(cat "$TRANSCRIPT_FIXTURE/err-path")
if [ "$TRANSCRIPT_MODE" = delayed ]; then
    if [ "$n" -eq 2 ]; then
        printf 'starting\\n' > "$out"
        : > "$err"
    elif [ "$n" -eq 3 ]; then
        printf 'VQ-DEPLOY-METRIC runtime_store=reused\\ncomplete\\n' >> "$out"
        printf 'diagnostic\\n' > "$err"
    fi
elif [ "$TRANSCRIPT_MODE" = unstable ]; then
    printf 'part %s\\n' "$n" >> "$out"
    : > "$err"
fi
''')
    sbatch.chmod(0o700)
    sleep.chmod(0o700)
    monkeypatch.setenv("PATH", str(tools) + os.pathsep + os.environ["PATH"])
    monkeypatch.setenv("TRANSCRIPT_FIXTURE", str(tmp_path))

    def run(mode: str, rc: int = 0):
        monkeypatch.setenv("TRANSCRIPT_MODE", mode)
        monkeypatch.setenv("TRANSCRIPT_RC", str(rc))
        argv = admin._slurm_allocation_argv(
            config.SchedulerBuildAllocation(scheduler="slurm"),
            ["/site/bin/deploy-runtime", "--program", "engine"],
            config.HostConfig(ssh="cluster", scheduler="slurm", scheduler_dialect="slurm",
                              scheduler_driver="localhost", scratch_root=str(tmp_path)),
            "engine",
        )
        return subprocess.run(argv, capture_output=True, text=True, timeout=10, check=False)

    return run, tmp_path


@pytest.mark.parametrize("rc", [0, 7])
def test_delayed_transcript_is_read_after_completion(allocation_shell, rc):
    run, _root = allocation_shell

    result = run("delayed", rc)

    assert result.returncode == rc
    assert admin.parse_deploy_metrics(result.stdout) == {"runtime_store": "reused"}
    assert "complete" in result.stdout
    assert "diagnostic" in result.stderr


def test_each_attempt_keeps_its_own_transcript(allocation_shell):
    run, root = allocation_shell
    assert run("immediate").returncode == 0
    first = Path((root / "out-path").read_text().strip())
    first.write_text("earlier attempt evidence\n")

    assert run("immediate").returncode == 0
    second = Path((root / "out-path").read_text().strip())

    assert first != second
    assert first.read_text() == "earlier attempt evidence\n"
    assert "runtime_store=reused" in second.read_text()


@pytest.mark.parametrize("mode", ["empty", "unstable"])
def test_missing_or_unsettled_transcript_is_explicit_and_bounded(allocation_shell, mode):
    run, root = allocation_shell

    result = run(mode)

    assert result.returncode == 0
    assert "transcript" in result.stderr
    assert "incomplete" in result.stderr
    assert str(Path((root / "out-path").read_text().strip()).parent) in result.stderr
    assert 1 <= int((root / "ticks").read_text()) <= 10
