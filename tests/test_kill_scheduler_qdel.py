"""`vq kill` must actually stop a scheduler job, not just relabel the spec.

Marking a spec terminal is not a kill for a batch job: the work runs on the
cluster and only ``qdel`` stops it. The daemon does escalate a terminal spec to
``qdel`` — but only for jobs in its in-memory ``_scheduler_running`` map, so a
job it is not tracking was never cancelled. That window is routine (the daemon
is down for every ``vq admin update``, which is exactly when the
``--restart-after-update`` flow runs) and, once such a spec is killed, it is
terminal, so the deferred-reattach retry skips it forever and it can never
become tracked. vq reported KILLED while the job kept burning allocation.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from vq import config, kill, paths
from vq.spec import JobSpec, JobState


@pytest.fixture
def state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    (tmp_path / "cfg" / "config.toml").write_text(
        "\n".join(
            [
                "[hosts.localhost]",
                'ssh = "localhost"',
                "",
                "[hosts.host_f]",
                'ssh = "host_f-login"',
                'scheduler = "pbs"',
                'scheduler_dialect = "torque"',
                'scratch_root = "/home/USER"',
                'scheduler_driver = "localhost"',
                "",
            ]
        )
    )
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    return tmp_path


class _Recorder:
    """Stands in for the dispatcher; records what would have been cancelled."""

    def __init__(self, fail: Exception | None = None) -> None:
        self.cancelled: list[tuple[str, str, int | None]] = []
        self._fail = fail

    def remote_workspace(self, job_id: str) -> str:
        return f"/home/USER/vq/{job_id}"

    def cancel(self, handle) -> None:  # type: ignore[no-untyped-def]
        if self._fail is not None:
            raise self._fail
        self.cancelled.append((handle.job_id, handle.remote_workspace, handle.array_size))


def _spec(state_dir: Path, *, state: JobState, job_id: str | None = "555.cluster") -> JobSpec:
    ws = state_dir / "ws"
    ws.mkdir(exist_ok=True)
    spec = JobSpec(
        id="sched1",
        command=["true"],
        cwd=str(ws),
        cpus=1,
        state=state,
        scheduler_target="host_f",
        scheduler_job_id=job_id,
    )
    spec.write(paths.queue_dir() / "sched1.json")
    return spec


def test_killing_an_untracked_running_scheduler_job_qdels_it(
    state: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The leak: the daemon only qdels jobs it tracks. This one it does not."""
    rec = _Recorder()
    monkeypatch.setattr(
        "vq.scheduler_dispatch.scheduler_dispatcher_for", lambda host_cfg: rec
    )
    spec = _spec(state, state=JobState.RUNNING)
    spec.array_index = 2
    spec.array_total = 5
    spec.array_group_id = "kill-array"
    spec.write(paths.queue_dir() / "sched1.json")

    msg = kill.kill_job("localhost", "sched1")

    assert rec.cancelled == [("555.cluster", "/home/USER/vq/sched1", None)]
    assert "cancelled scheduler job 555.cluster on host_f" in msg
    # The old message told the submitter about a local pid that never existed.
    assert "pid None not found" not in msg
    assert JobSpec.read(paths.queue_dir() / "sched1.json").state == JobState.KILLED


def test_killing_a_pending_scheduler_job_also_qdels(
    state: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A PENDING spec can already be qsub'd: RUNNING is claimed in two phases."""
    rec = _Recorder()
    monkeypatch.setattr(
        "vq.scheduler_dispatch.scheduler_dispatcher_for", lambda host_cfg: rec
    )
    _spec(state, state=JobState.PENDING)

    msg = kill.kill_job("localhost", "sched1")

    assert rec.cancelled == [("555.cluster", "/home/USER/vq/sched1", None)]
    assert "cancelled scheduler job" in msg


def test_a_failed_qdel_is_reported_loudly(
    state: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A kill that reports plain success while leaking a live job is the bug."""
    from vq.scheduler_dispatch import SchedulerError

    rec = _Recorder(fail=SchedulerError("ssh: connection refused"))
    monkeypatch.setattr(
        "vq.scheduler_dispatch.scheduler_dispatcher_for", lambda host_cfg: rec
    )
    _spec(state, state=JobState.RUNNING)

    msg = kill.kill_job("localhost", "sched1")

    assert "WARNING could not cancel scheduler job 555.cluster" in msg
    assert "may still be running" in msg
    # The operator asked for the kill, so the spec is still marked terminal.
    assert JobSpec.read(paths.queue_dir() / "sched1.json").state == JobState.KILLED


@pytest.mark.parametrize("job_id", [None, ""], ids=["missing", "empty"])
def test_a_spec_with_no_scheduler_id_says_so(
    state: Path, monkeypatch: pytest.MonkeyPatch, job_id: str | None
) -> None:
    """Nothing to cancel with — do not imply the cluster job was stopped."""
    monkeypatch.setattr(
        "vq.scheduler_dispatch.scheduler_dispatcher_for",
        lambda host_cfg: pytest.fail("must not build a dispatcher without an id"),
    )
    _spec(state, state=JobState.RUNNING, job_id=job_id)

    msg = kill.kill_job("localhost", "sched1")

    assert "NO scheduler job id was recorded" in msg
    assert "still live" in msg


def test_the_qdel_happens_outside_the_spec_lock(
    state: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """qdel crosses the network; the spec lock must never be held across it.

    Holding it would serialise every other vq verb on this job behind an SSH
    round-trip to the cluster.
    """
    seen: list[bool] = []

    class _LockProbe:
        def remote_workspace(self, job_id: str) -> str:
            return "/x"

        def cancel(self, handle) -> None:  # type: ignore[no-untyped-def]
            # Re-acquiring the same lock must succeed => it is not held.
            spec_path = paths.queue_dir() / "sched1.json"
            with paths.spec_lock(spec_path, timeout=2.0):
                seen.append(True)

    monkeypatch.setattr(
        "vq.scheduler_dispatch.scheduler_dispatcher_for", lambda host_cfg: _LockProbe()
    )
    _spec(state, state=JobState.RUNNING)

    kill.kill_job("localhost", "sched1")

    assert seen == [True]


def test_a_local_job_never_builds_a_dispatcher(
    state: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The local kill path must not pull in config or the transport stack."""
    monkeypatch.setattr(
        "vq.scheduler_dispatch.scheduler_dispatcher_for",
        lambda host_cfg: pytest.fail("local kill must not touch the scheduler"),
    )
    ws = state / "ws2"
    ws.mkdir()
    JobSpec(
        id="local1", command=["true"], cwd=str(ws), cpus=1, state=JobState.PENDING
    ).write(paths.queue_dir() / "local1.json")

    msg = kill.kill_job("localhost", "local1")

    assert msg == "killed pending job local1"
