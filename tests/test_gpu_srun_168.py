"""#168: GPU processes of the job's srun steps count as the job's.

An ``srun`` step's tasks are slurmstepd's children, not the job script's, so
descent alone misses them. Slurm's cgroup path names the job,
``.../job_<SLURM_JOB_ID>/...`` (cgroup v1 and v2), so a GPU process in this
job's cgroup is attributed too. These checks run the real rendered job script
with a stand-in ``nvidia-smi`` and a stand-in ``/proc``.
"""
from __future__ import annotations

import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from vq import gpu_usage
from vq.gpu_usage import read_gpu_usage
from vq.scheduler_dialect import SlurmDialect
from vq.scheduler_dispatch import SchedulerDispatcher

sys.path.insert(0, str(Path(__file__).parent))
from test_scheduler_dispatch import FakeRunner, _install_fake_gnu_time  # noqa: E402

V1, V2, NEAR, BOTH = 900001, 900002, 900003, 900004  # not processes of this machine


def _nvidia_smi(tmp_path: Path) -> Path:
    """One device. The job script's own process (its pid in FAKE_GPU_PIDFILE)
    holds 1000 MB; four other pids hold 100, 200, 400 and 800 MB."""
    bin_dir = tmp_path / "fake-bin"
    bin_dir.mkdir()
    fake = bin_dir / "nvidia-smi"
    fake.write_text(textwrap.dedent(f"""\
        #!/bin/bash
        case "$*" in
          *--query-gpu=*) printf 'GPU-aaaa-1111, 0, 75, 4000, 81920, NVIDIA Test GPU\\n' ;;
          *--query-compute-apps=*)
            if [ -s "${{FAKE_GPU_PIDFILE:-/nonexistent}}" ]; then
              printf 'GPU-aaaa-1111, %s, 1000\\n' "$(cat "$FAKE_GPU_PIDFILE")"
            fi
            printf 'GPU-aaaa-1111, {V1}, 100\\nGPU-aaaa-1111, {V2}, 200\\n'
            printf 'GPU-aaaa-1111, {NEAR}, 400\\nGPU-aaaa-1111, {BOTH}, 800\\n' ;;
          *) exit 2 ;;
        esac
        """))
    fake.chmod(0o755)
    return bin_dir


def _proc(tmp_path: Path, job: str) -> Path:
    root = tmp_path / "proc"
    for pid, path in {
        V1: f"4:memory:/slurm/uid_1000/job_{job}/step_0/task_0",
        V2: f"0::/system.slice/slurmstepd.scope/job_{job}/step_1/user/task_0",
        NEAR: f"0::/system.slice/slurmstepd.scope/job_{job}0/step_0/user/task_0",
    }.items():
        (root / str(pid)).mkdir(parents=True)
        (root / str(pid) / "cgroup").write_text(path + "\n")
    return root


def _run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, env: dict[str, str]) -> Path:
    monkeypatch.setattr(gpu_usage, "SAMPLE_INTERVAL_SECONDS", 1)
    monkeypatch.setattr(gpu_usage, "PROC_ROOT", str(_proc(tmp_path, "4242")))
    workspace = tmp_path / "ws"
    workspace.mkdir()
    script = SchedulerDispatcher(
        SlurmDialect(), FakeRunner(), scratch_root=str(tmp_path),
        gnu_time_command=str(_install_fake_gnu_time(tmp_path, monkeypatch)),
    ).build_job_script(job_id="j1", command=["sh", "-c",
                                             'echo $$ > "$FAKE_GPU_PIDFILE"; sleep 2.5'],
                       remote_workspace=str(workspace), cpus=1, gpus=1)
    done = subprocess.run(
        ["/bin/bash", "-c", script], capture_output=True, timeout=30, check=False,
        env={"PATH": f"{_nvidia_smi(tmp_path)}:/usr/bin:/bin",
             "FAKE_GPU_PIDFILE": str(workspace / "gpu.pid"), **env})
    assert done.returncode == 0, done.stderr
    return workspace


def _attributed(workspace: Path) -> set[str]:
    rows = (workspace / "_vq" / "gpu-samples.txt").read_text().splitlines()
    return {row.split(", ")[1] for row in rows if row.startswith("P ")}


def test_a_steps_processes_in_the_jobs_cgroup_are_the_jobs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _run(tmp_path, monkeypatch, {"SLURM_JOB_ID": "4242"})
    own = (workspace / "gpu.pid").read_text().strip()
    # The script's own process, and both cgroup layouts; never job_42420.
    assert _attributed(workspace) == {own, str(V1), str(V2)}
    usage = read_gpu_usage(workspace, 1)
    assert usage is not None and usage["devices"][0]["job_peak_memory_mb"] == 1300.0


@pytest.mark.parametrize("env", [{}, {"SLURM_JOB_ID": ""}, {"SLURM_JOB_ID": "4242 x"},
                                 {"SLURM_JOB_ID": ".*"}])
def test_without_a_plain_slurm_job_id_only_descent_counts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, env: dict[str, str],
) -> None:
    workspace = _run(tmp_path, monkeypatch, env)
    assert _attributed(workspace) == {(workspace / "gpu.pid").read_text().strip()}


def test_a_process_that_qualifies_both_ways_is_counted_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Under Slurm the script's own children are in the job's cgroup as well.
    proc = tmp_path / "both-proc"
    monkeypatch.setattr(gpu_usage, "SAMPLE_INTERVAL_SECONDS", 1)
    monkeypatch.setattr(gpu_usage, "PROC_ROOT", str(proc))
    workspace = tmp_path / "ws"
    workspace.mkdir()
    script = SchedulerDispatcher(
        SlurmDialect(), FakeRunner(), scratch_root=str(tmp_path),
        gnu_time_command=str(_install_fake_gnu_time(tmp_path, monkeypatch)),
    ).build_job_script(
        job_id="j1", remote_workspace=str(workspace), cpus=1, gpus=1,
        command=["sh", "-c", f'mkdir -p {proc}/$$ && echo "0::/job_4242/step_batch" > '
                             f'{proc}/$$/cgroup && echo $$ > "$FAKE_GPU_PIDFILE"; sleep 2.5'])
    done = subprocess.run(
        ["/bin/bash", "-c", script], capture_output=True, timeout=30, check=False,
        env={"PATH": f"{_nvidia_smi(tmp_path)}:/usr/bin:/bin", "SLURM_JOB_ID": "4242",
             "FAKE_GPU_PIDFILE": str(workspace / "gpu.pid")})
    assert done.returncode == 0, done.stderr
    own = (workspace / "gpu.pid").read_text().strip()
    per_sample: list[int] = []
    for row in (workspace / "_vq" / "gpu-samples.txt").read_text().splitlines():
        if row.startswith("T "):
            per_sample.append(0)
        elif row.startswith("P ") and row.split(", ")[1] == own:
            per_sample[-1] += 1
    assert max(per_sample) == 1 and set(per_sample) <= {0, 1}
