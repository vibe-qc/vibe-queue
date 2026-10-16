"""Runtime retention proves live jobs from their processes (#74)."""
from __future__ import annotations

import os
from pathlib import Path

import pytest

from tests import test_runtime_retention_74 as base
from vq import runtime_retention, runtime_slots
from vq.spec import JobState

pytestmark = [pytest.mark.no_autopatch_lifecycle_lock]
store = base.store
_kept = base._kept
_spec = base._spec


class FakeProc:
    """A /proc tree: one directory per pid with the files the census reads."""

    def __init__(self, root: Path) -> None:
        self.root = root
        (root / "self").mkdir(parents=True)
        (root / "self" / "stat").write_text("1 (self) S 0 1 1\n")

    def add(
        self,
        pid: int,
        *,
        pgid: int,
        exe: str = "/usr/bin/python3.12",
        cwd: str = "/tmp",
        cmdline: tuple[str, ...] = ("python3",),
        environ: dict[str, str] | None = None,
        maps: tuple[str, ...] = (),
        fds: tuple[str, ...] = (),
    ) -> Path:
        base = self.root / str(pid)
        (base / "fd").mkdir(parents=True)
        (base / "stat").write_text(f"{pid} (python3 (x)) S 1 {pgid} {pgid} 0\n")
        os.symlink(exe, base / "exe")
        os.symlink(cwd, base / "cwd")
        (base / "cmdline").write_bytes(b"\0".join(a.encode() for a in cmdline) + b"\0")
        env = environ or {"PATH": "/usr/bin"}
        (base / "environ").write_bytes(
            b"\0".join(f"{k}={v}".encode() for k, v in env.items()) + b"\0"
        )
        (base / "maps").write_text(
            "".join(f"7f00-7f01 r-xp 00000000 08:01 1 {path}\n" for path in maps)
        )
        for index, target in enumerate(fds):
            os.symlink(target, base / "fd" / str(index))
        return base


@pytest.fixture
def proc(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> FakeProc:
    fake = FakeProc(tmp_path / "proc")
    monkeypatch.setattr(runtime_retention, "PROC_ROOT", fake.root)
    return fake


def _python(root: Path, sha: str) -> str:
    return str(runtime_slots.slot_python(root, sha))


def test_a_started_job_is_proven_by_its_whole_process_group(store, proc) -> None:
    root, (a, b, _c, _d), cfg, specs, _services = store
    specs.append(_spec("job", JobState.RUNNING, pid=500, pgid=500))
    proc.add(500, pgid=500, cmdline=(_python(root, b), "-m", "vibeqc"))
    proc.add(501, pgid=500, cmdline=("sh", "-c", "sleep 1"))

    value = runtime_retention.plan("demo", cfg, keep=0)

    assert value["census"]["unresolved_live_jobs"] == []
    assert value["census"]["process_proven_jobs"] == ["job"]
    assert value["census"]["process_scan"] is True
    assert _kept(value)[b] == ["live job job", "live process"]
    assert value["candidates"] == [a]


def test_a_group_that_references_nothing_releases_every_old_generation(store, proc) -> None:
    _root, (a, b, _c, _d), cfg, specs, _services = store
    specs.append(_spec("job", JobState.RUNNING, pid=500, pgid=500))
    proc.add(500, pgid=500, cmdline=("sh", "payload.sh"))

    value = runtime_retention.plan("demo", cfg, keep=0)

    assert value["census"]["process_proven_jobs"] == ["job"]
    assert value["candidates"] == [b, a]


@pytest.mark.parametrize(
    ("via", "kwargs"),
    [
        ("slot env", lambda root, sha: {"environ": {"VQ_RUNTIME_SLOT_SHA": sha}}),
        ("exe", lambda root, sha: {"exe": _python(root, sha)}),
        ("cwd", lambda root, sha: {"cwd": str(runtime_slots.slot_source(root, sha))}),
        ("maps", lambda root, sha: {
            "maps": (str(runtime_slots.slot_source(root, sha) / "ext.so"),)}),
        ("open fd", lambda root, sha: {
            "fds": (str(runtime_slots.slot_source(root, sha) / "run.sh"),)}),
    ],
)
def test_every_process_source_proves_a_generation(store, proc, via, kwargs) -> None:
    root, (a, b, _c, _d), cfg, specs, _services = store
    specs.append(_spec("job", JobState.RUNNING, pid=500, pgid=500))
    proc.add(500, pgid=500, **kwargs(root, a))

    value = runtime_retention.plan("demo", cfg, keep=0)

    assert "live job job" in _kept(value)[a], via
    assert value["candidates"] == [b]


def test_a_process_outside_any_job_keeps_its_generation(store, proc) -> None:
    root, (a, b, _c, _d), cfg, _specs, _services = store
    proc.add(700, pgid=700, cmdline=(_python(root, a), "notebook.py"))

    value = runtime_retention.plan("demo", cfg, keep=0)

    assert _kept(value)[a] == ["live process"]
    assert value["candidates"] == [b]
    assert value["census"]["process_references"] == {a: [700]}


def test_a_partly_unreadable_group_proves_nothing(store, proc) -> None:
    root, (_a, b, _c, _d), cfg, specs, _services = store
    specs.append(_spec("job", JobState.RUNNING, pid=500, pgid=500))
    proc.add(500, pgid=500, cmdline=(_python(root, b),))
    hidden = proc.add(501, pgid=500)
    (hidden / "maps").chmod(0)
    try:
        value = runtime_retention.plan("demo", cfg, keep=0)
    finally:
        (hidden / "maps").chmod(0o600)

    assert value["census"]["unresolved_live_jobs"] == ["job"]
    assert value["candidates"] == []


def test_another_users_process_counts_only_by_its_command_line(
    store, proc, monkeypatch
) -> None:
    root, (a, b, _c, _d), cfg, specs, _services = store
    specs.append(_spec("job", JobState.RUNNING, pid=500, pgid=500))
    proc.add(500, pgid=500, environ={"VQ_RUNTIME_SLOT_SHA": b})
    proc.add(600, pgid=600, cmdline=(_python(root, a),))
    monkeypatch.setattr(runtime_retention, "_census_uid", lambda: os.geteuid() + 1)

    value = runtime_retention.plan("demo", cfg, keep=0)

    # Not our processes: the job cannot be proven, and a's reference still counts.
    assert value["census"]["unresolved_live_jobs"] == ["job"]
    assert "live process" in _kept(value)[a]
    assert value["candidates"] == []


def test_a_job_whose_group_is_gone_proves_nothing(store, proc) -> None:
    _root, _shas, cfg, specs, _services = store
    specs.append(_spec("job", JobState.RUNNING, pid=500, pgid=500))
    specs.append(_spec("nogroup", JobState.RUNNING))
    proc.add(900, pgid=900)

    value = runtime_retention.plan("demo", cfg, keep=0)

    assert value["census"]["unresolved_live_jobs"] == ["job", "nogroup"]
    assert value["candidates"] == []


def test_without_proc_nothing_changes(store, tmp_path, monkeypatch) -> None:
    _root, _shas, cfg, specs, _services = store
    monkeypatch.setattr(runtime_retention, "PROC_ROOT", tmp_path / "no-proc")
    specs.append(_spec("job", JobState.RUNNING, pid=500, pgid=500))

    value = runtime_retention.plan("demo", cfg, keep=0)

    assert value["census"]["process_scan"] is False
    assert value["census"]["unresolved_live_jobs"] == ["job"]
    assert value["candidates"] == []


def test_pid_churn_does_not_change_the_plan_id_and_apply_proceeds(store, proc) -> None:
    root, (a, b, c, d), cfg, specs, _services = store
    specs.append(_spec("job", JobState.RUNNING, pid=500, pgid=500))
    proc.add(500, pgid=500, cmdline=(_python(root, b),))
    proc.add(701, pgid=701, cmdline=(_python(root, b),))
    reviewed = runtime_retention.plan("demo", cfg, keep=0)

    # The helper restarted under a new pid; the outcome is the same.
    for name in ("exe", "cwd"):
        (proc.root / "701" / name).unlink()
    for entry in (proc.root / "701").iterdir():
        if entry.is_dir():
            entry.rmdir()
        else:
            entry.unlink()
    (proc.root / "701").rmdir()
    proc.add(702, pgid=702, cmdline=(_python(root, b),))

    receipt = runtime_retention.apply("demo", cfg, reviewed["plan_id"], keep=0)

    assert receipt["removed"] == [a]
    assert sorted(p.name for p in (root / "releases").iterdir()) == sorted([b, c, d])


def test_a_new_reference_after_review_refuses_the_stale_plan(store, proc) -> None:
    root, (a, _b, _c, _d), cfg, _specs, _services = store
    reviewed = runtime_retention.plan("demo", cfg, keep=0)
    proc.add(800, pgid=800, cmdline=(_python(root, a),))

    with pytest.raises(runtime_retention.RetentionError, match="plan again"):
        runtime_retention.apply("demo", cfg, reviewed["plan_id"], keep=0)
    assert (root / "releases" / a).is_dir()


@pytest.mark.skipif(
    not Path("/proc/self/stat").is_file(), reason="needs a Linux /proc"
)
def test_a_real_process_group_is_read_from_proc(store) -> None:
    import subprocess
    import sys

    root, (a, b, _c, _d), cfg, specs, _services = store
    child = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(30)"],
        cwd=runtime_slots.slot_source(root, a),
        start_new_session=True,
        stdin=subprocess.DEVNULL,
    )
    try:
        specs.append(_spec("job", JobState.RUNNING, pid=child.pid, pgid=child.pid))
        value = runtime_retention.plan("demo", cfg, keep=0)
    finally:
        child.kill()
        child.wait()

    assert value["census"]["process_proven_jobs"] == ["job"]
    assert "live job job" in _kept(value)[a]
    assert value["candidates"] == [b]

