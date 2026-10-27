"""Controlled engine lifecycle; never talks to a real daemon/container."""

import copy
from concurrent.futures import Future

import pytest

from vq import managed_container as mc
from vq.daemon import Daemon
from vq.kill import kill_job
from vq.spec import JobSpec, JobState
from vq.watchdog import Watchdog, WatchdogAction

DIGEST = "sha256:" + "a" * 64
CID = "b" * 64


class EngineFixture:
    executable = "/fixture/docker"
    endpoint = "unix:///fixture/docker.sock"

    def __init__(self):
        self.container = None
        self.creates = 0
        self.calls = []
        self.unknown = False
        self.lose_create = False

    def preflight(self, image):
        assert image == DIGEST
        return DIGEST

    def create(self, b):
        self.creates += 1
        self.container = {
            "Id": CID,
            "Image": DIGEST,
            "HostConfig": {"RestartPolicy": {"Name": "no"}, "AutoRemove": False},
            "Name": "/" + b["name"],
            "Config": {"Labels": {mc.LABEL: b["token"]}, "Image": b["image"]},
            "State": {"Status": "created", "Pid": 0, "ExitCode": 0},
        }
        if self.lose_create:
            raise mc.ContainerError("lost create response")
        return CID

    def inspect(self, target):
        self.calls.append(("inspect", target))
        if self.unknown or self.container is None:
            raise mc.ContainerError("unknown engine response")
        return copy.deepcopy(self.container)

    def call(self, *args):
        self.calls.append(args)
        assert args[-1] == CID
        if args[0] == "start":
            self.container["State"].update(Status="running", Pid=321)
        if args[0] == "rm":
            assert self.container["State"]["Status"] in {"created", "exited", "dead"}
        return ""

    def logs(self, target):
        assert target == CID
        return "worker stdout\n", "worker stderr\n"

    def finish(self, rc=0):
        self.container["State"].update(Status="exited", Pid=0, ExitCode=rc)


class ImmediateExecutor:
    def submit(self, fn):
        result = Future()
        try:
            result.set_result(fn())
        except BaseException as exc:
            result.set_exception(exc)
        return result


@pytest.fixture(autouse=True)
def controlled_engine_executor(monkeypatch):
    monkeypatch.setattr(mc, "_EXECUTOR", ImmediateExecutor())


@pytest.fixture
def owned(tmp_path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    spec = JobSpec(
        id="owned221",
        command=[mc.COMMAND, DIGEST, "--", "sh", "build.sh"],
        cwd=str(workspace),
        cpus=2,
        mem_mb=256,
    )
    path = tmp_path / "queue" / "owned221.json"
    spec.write(path)
    engine = EngineFixture()
    handle = mc.prepare(spec, path, engine=engine)
    return spec, path, engine, handle


def test_binding_precedes_start_and_worker_exit_is_authoritative(owned):
    spec, path, engine, handle = owned
    assert handle.poll() is None
    assert engine.creates == 1 and handle.result()["id"] == CID
    assert handle.poll() is None
    assert mc.pending(path)
    engine.finish(7)
    assert handle.poll() == 7 and not mc.pending(path)
    assert handle.poll() == 7 and engine.creates == 1


def test_lost_create_response_recovers_same_identity_without_duplicate(owned):
    spec, path, engine, handle = owned
    engine.lose_create = True
    assert handle.poll() is None
    assert handle.result()["phase"] == "creating"
    restored = mc.restore(spec, path, engine=engine)
    assert restored.poll() is None
    assert engine.creates == 1 and restored.result()["id"] == CID
    assert engine.container["State"]["Status"] == "running"


@pytest.mark.parametrize("field", ["id", "token", "name", "image", "restart", "unknown"])
def test_identity_failure_refuses_start_and_control_and_retains_capacity(owned, field):
    spec, path, engine, handle = owned
    handle.poll()
    if field == "id":
        engine.container["Id"] = "c" * 64
    if field == "token":
        engine.container["Config"]["Labels"][mc.LABEL] = "wrong"
    if field == "name":
        engine.container["Name"] = "/unrelated"
    if field == "image":
        engine.container["Image"] = "sha256:" + "c" * 64
    if field == "restart":
        engine.container["HostConfig"]["RestartPolicy"]["Name"] = "always"
    if field == "unknown":
        engine.unknown = True
    mc.request_stop(path, "killed", "test cancellation")
    before = list(engine.calls)
    assert handle.poll() is None and mc.pending(path)
    assert all(x[0] == "inspect" for x in engine.calls[len(before) :])
    assert handle.usage()[:2] == (None, None)


def test_cancel_waits_for_worker_confirmation_and_escalates(owned, monkeypatch):
    spec, path, engine, handle = owned
    handle.poll()
    spec.state = JobState.RUNNING
    spec.write(path)
    with monkeypatch.context() as m:
        m.setattr(mc.time, "time", lambda: 100.0)
        message = kill_job("localhost", spec.id, queue_dir=path.parent)
        assert "capacity retained" in message
        assert JobSpec.read(path).state == JobState.RUNNING
        assert handle.poll() is None
        assert ("kill", "--signal", "TERM", CID) in engine.calls
    with monkeypatch.context() as m:
        m.setattr(mc.time, "time", lambda: 111.0)
        assert handle.poll() is None
        assert ("kill", "--signal", "KILL", CID) in engine.calls
    assert mc.pending(path)
    engine.finish(137)
    assert handle.poll() == 137
    assert handle.result()["requested_terminal"] == "killed"


def test_cancel_before_creation_never_launches(owned):
    spec, path, engine, handle = owned
    mc.request_stop(path, "killed", "cancel before create")
    assert handle.poll() == 125
    assert engine.creates == 0 and not engine.calls


def test_changed_job_binding_fails_closed(owned):
    spec, path, engine, handle = owned
    spec.command[-1] = "replacement.sh"
    with pytest.raises(mc.ContainerError, match="binding changed"):
        mc.restore(spec, path, engine=engine)


def test_invalid_authority_is_a_durable_cleanup_fence(owned):
    spec, path, engine, handle = owned
    mc.binding_path(path).write_text("not JSON")
    assert mc.pending(path)
    fenced = mc.FencedHandle(path, "unknown")
    assert fenced.poll() is None and fenced.usage()[:2] == (None, None)
    fenced.terminate()
    assert not engine.calls


def test_authoritative_worker_usage_not_idle_client_controls_starvation(owned, monkeypatch):
    spec, path, engine, handle = owned
    handle.poll()
    handle.poll()
    monkeypatch.setattr(
        mc.cgroup, "cgroup_path_for_pid", lambda pid: "/sys/fs/cgroup/docker/" + CID
    )
    cpu = [0.0]
    monkeypatch.setattr(mc.cgroup, "read_cpu_usage_seconds", lambda path: cpu[0])
    monkeypatch.setattr(mc.cgroup, "read_memory_current_mb", lambda path: 12)
    wd = Watchdog(interval_seconds=1, starve_window_seconds=3)
    wd.register(spec.id, started_monotonic=100.0)
    for now, value in [(101.0, 0.0), (102.0, 1.0), (106.0, 5.0)]:
        cpu[0] = value
        monkeypatch.setattr("vq.watchdog.time.monotonic", lambda now=now: now)
        v = wd.evaluate(spec.id, pid=0, pgid=None, spec=spec, external_usage=handle.usage())
        assert v.action == WatchdogAction.OK
    for now in [107.0, 111.0]:
        monkeypatch.setattr("vq.watchdog.time.monotonic", lambda now=now: now)
        v = wd.evaluate(spec.id, pid=0, pgid=None, spec=spec, external_usage=handle.usage())
    assert v.terminal_state == JobState.STARVED


def test_recycled_pid_never_samples_another_cgroup(owned, monkeypatch):
    spec, path, engine, handle = owned
    handle.poll()
    handle.poll()
    monkeypatch.setattr(mc.cgroup, "cgroup_path_for_pid", lambda pid: "/sys/fs/cgroup/unrelated")
    monkeypatch.setattr(
        mc.cgroup, "read_cpu_usage_seconds", lambda path: pytest.fail("unrelated CPU read")
    )
    assert handle.usage()[:2] == (None, None)


def test_daemon_restart_restores_reservation_then_confirms_cancellation(tmp_path, monkeypatch):
    engine = EngineFixture()
    monkeypatch.setattr(mc.Engine, "local", lambda: engine)
    # Restored handle retains the exact engine endpoint through the adapter.
    monkeypatch.setattr(mc, "Engine", lambda executable, endpoint: engine)
    d = Daemon(max_cpus=2, queue_dir=tmp_path / "q", jobs_dir=tmp_path / "j")
    try:
        spec = JobSpec(
            id="restart221",
            command=[mc.COMMAND, DIGEST, "--", "sh", "build.sh"],
            cwd=str(tmp_path / "j" / "restart221"),
            cpus=2,
            mem_mb=256,
        )
        spec.write(d._spec_path(spec.id))
        mc.prepare(spec, d._spec_path(spec.id), engine=engine)
        spec.state = JobState.RUNNING
        spec.write(d._spec_path(spec.id))
        d._restore_managed_container(spec, d._spec_path(spec.id))
        assert sum(r.cpus for r in d._running.values()) == 2
        d._reconcile_running()
        assert engine.creates == 1
        # Client has no role in completion. Request survives object reconstruction.
        mc.request_stop(d._spec_path(spec.id), "killed", "test")
        d._running[spec.id].close_logs()
        d._running.clear()
        d._restore_managed_container(spec, d._spec_path(spec.id))
        d._reconcile_running()
        assert spec.id in d._running
        assert JobSpec.read(d._spec_path(spec.id)).state == JobState.RUNNING
        engine.finish(137)
        d._reconcile_running()
        assert spec.id not in d._running
        final = JobSpec.read(d._spec_path(spec.id))
        assert final.state == JobState.KILLED and final.exit_code == 137
    finally:
        for r in d._running.values():
            r.close_logs()
        d._queue_lock_fd.close()


def test_pending_binding_blocks_resubmit_and_cleanup_even_if_spec_is_terminal(owned):
    from vq.cleanup import archive_workspace, delete_job
    from vq.resubmit import resubmit_local

    spec, path, engine, handle = owned
    spec.state = JobState.KILLED
    spec.write(path)
    with pytest.raises(ValueError, match="container"):
        resubmit_local(spec.id, queue_dir=path.parent, jobs_dir=path.parent / "copies")
    with pytest.raises(ValueError, match="container"):
        archive_workspace(spec, queue_dir=path.parent, archive_dir=path.parent / "archive")
    with pytest.raises(ValueError, match="container"):
        delete_job(spec, queue_dir=path.parent)
    assert path.exists() and mc.pending(path)


def test_status_shows_pending_identity_without_ownership_token(owned):
    spec, path, engine, handle = owned
    handle.poll()
    projected = mc.status(path)
    assert projected["id"] == CID and "token" not in projected
    mc.request_stop(path, "killed", "cancel")
    assert mc.status(path)["requested_terminal"] == "killed"


@pytest.mark.parametrize("override", [{"retry_max": 1}, {"gpus": 1}, {"program": "app"}])
def test_unsupported_execution_refuses_before_creation(tmp_path, override):
    spec = JobSpec(
        id="refused221",
        command=[mc.COMMAND, DIGEST, "--", "sh", "run.sh"],
        cwd=str(tmp_path),
        cpus=1,
        **override,
    )
    engine = EngineFixture()
    with pytest.raises(mc.ContainerError, match="unsupported"):
        mc.prepare(spec, tmp_path / "q" / "refused221.json", engine=engine)
    assert engine.creates == 0 and not engine.calls


def test_actual_daemon_dispatch_uses_managed_handle_not_shell_client(tmp_path, monkeypatch):
    engine = EngineFixture()
    original_prepare = mc.prepare
    original_restore = mc.restore
    monkeypatch.setattr(
        mc, "prepare", lambda spec, path: original_prepare(spec, path, engine=engine)
    )
    monkeypatch.setattr(
        mc, "restore", lambda spec, path: original_restore(spec, path, engine=engine)
    )
    d = Daemon(max_cpus=2, queue_dir=tmp_path / "q", jobs_dir=tmp_path / "j")
    try:
        spec = JobSpec(
            id="dispatch221",
            command=[mc.COMMAND, DIGEST, "--", "sh", "build.sh"],
            cwd=str(tmp_path / "j" / "dispatch221"),
            cpus=2,
            mem_mb=256,
        )
        spec.write(d._spec_path(spec.id))
        assert d._start_job(spec)
        assert isinstance(d._running[spec.id].popen, mc.ContainerHandle)
        d._reconcile_running()
        assert engine.creates == 1 and mc.pending(d._spec_path(spec.id))
        engine.finish()
        d._reconcile_running()
        assert not d._running
        assert JobSpec.read(d._spec_path(spec.id)).state == JobState.COMPLETED
        assert (tmp_path / "j" / "dispatch221" / "stdout.log").read_text() == "worker stdout\n"
    finally:
        for r in d._running.values():
            r.close_logs()
        d._queue_lock_fd.close()


def test_unknown_usage_interval_does_not_accumulate_starvation(owned, monkeypatch):
    spec, path, engine, handle = owned
    wd = Watchdog(interval_seconds=1, starve_window_seconds=3)
    wd.register(spec.id, started_monotonic=100.0)
    for now, usage in [
        (101.0, (0.0, 1, CID)),
        (102.0, (0.0, 1, CID)),
        (106.0, (None, None, CID)),
        (107.0, (10.0, 1, CID)),
    ]:
        monkeypatch.setattr("vq.watchdog.time.monotonic", lambda now=now: now)
        verdict = wd.evaluate(spec.id, pid=0, pgid=None, spec=spec, external_usage=usage)
        assert verdict.action == WatchdogAction.OK
    assert wd._states[spec.id].starve_since_monotonic is None


def test_remote_engine_refused_before_container_creation(monkeypatch):
    monkeypatch.setattr(mc.sys, "platform", "linux")
    monkeypatch.setattr(mc.shutil, "which", lambda name: "/fixture/docker")
    monkeypatch.setenv("DOCKER_HOST", "tcp://remote.example:2375")
    with pytest.raises(mc.ContainerError, match="local Unix"):
        mc.Engine.local()


@pytest.mark.parametrize(
    "command",
    [
        [mc.COMMAND, "image:latest", "--", "sh"],
        [mc.COMMAND, DIGEST, "--", ""],
        [mc.COMMAND, "--bad@sha256:" + "a" * 64, "--", "sh"],
    ],
)
def test_malformed_or_mutable_payload_refused(command):
    with pytest.raises(mc.ContainerError):
        mc.payload(command)


def test_slow_engine_does_not_block_poll_or_cancellation(owned, monkeypatch):
    import threading
    import time
    from concurrent.futures import ThreadPoolExecutor

    spec, path, engine, handle = owned
    entered = threading.Event()
    release = threading.Event()
    create = engine.create

    def slow_create(binding):
        entered.set()
        assert release.wait(5)
        return create(binding)

    monkeypatch.setattr(engine, "create", slow_create)
    with ThreadPoolExecutor(max_workers=1) as pool:
        monkeypatch.setattr(mc, "_EXECUTOR", pool)
        assert handle.poll() is None
        assert entered.wait(2)
        started = time.monotonic()
        assert handle.poll() is None
        mc.request_stop(path, "killed", "cancel during create")
        assert time.monotonic() - started < 1
        release.set()
        handle._flight.result(timeout=3)
        assert handle.poll() == 125
    assert engine.creates == 1
    assert not any(call[0] == "start" for call in engine.calls)


def test_unknown_binding_restart_keeps_capacity_and_pending_work_waits(tmp_path, monkeypatch):
    engine = EngineFixture()
    restore = mc.restore
    monkeypatch.setattr(mc, "restore", lambda spec, path: restore(spec, path, engine=engine))
    d = Daemon(max_cpus=2, max_jobs=1, queue_dir=tmp_path / "q", jobs_dir=tmp_path / "j")
    try:
        spec = JobSpec(
            id="fenced221",
            command=[mc.COMMAND, DIGEST, "--", "sh", "build.sh"],
            cwd=str(tmp_path / "j" / "fenced221"),
            cpus=2,
            mem_mb=256,
        )
        spec.write(d._spec_path(spec.id))
        mc.prepare(spec, d._spec_path(spec.id), engine=engine)
        mc.binding_path(d._spec_path(spec.id)).write_text("unreadable")
        spec.state = JobState.RUNNING
        spec.write(d._spec_path(spec.id))
        d._reattach_or_interrupt_at_startup()
        assert isinstance(d._running[spec.id].popen, mc.FencedHandle)
        second = JobSpec(
            id="waiting221", command=["true"], cwd=str(tmp_path / "j" / "waiting221"), cpus=1
        )
        second.write(d._spec_path(second.id))
        d._dispatch_pending()
        assert JobSpec.read(d._spec_path(second.id)).state == JobState.PENDING
        assert sum(r.cpus for r in d._running.values()) == 2
    finally:
        for r in d._running.values():
            r.close_logs()
        d._queue_lock_fd.close()
