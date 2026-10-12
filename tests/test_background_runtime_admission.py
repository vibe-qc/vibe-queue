"""Immutable replacement ownership must not park healthy-current calculations."""
from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from tests.test_daemon_scheduler import MockDispatcher, _inject
from tests.test_runtime_slots import SHA_A, SHA_B, _build
from vq import admin, cgroup, cli, config, daemon, drain, runtime_slots
from vq.scheduler_dialect import SchedulerPhase
from vq.spec import JobSpec, JobState, ProgramRuntimePin


def test_current_runtime_dispatch_survives_background_build_and_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "runtime"
    _build(root, SHA_A, "A")
    _build(root, SHA_B, "B")
    runtime_slots.activate(root, SHA_A)
    program = config.VenvProgram(
        kind="venv", python=str(tmp_path / "legacy/.venv/bin/python"),
        git_dir=str(tmp_path / "legacy"), runtime_slot_root=str(root),
        runtime_slot_execution=True,
    )
    cfg = config.Config(programs={"demo": program}, hosts={
        "host_f": config.HostConfig(ssh="host_f", scheduler="pbs",
                                  scheduler_dialect="torque", scratch_root="/scratch/jobs",
                                  scheduler_driver="localhost"),
    })
    monkeypatch.setattr(config, "load_config", lambda: cfg)
    monkeypatch.setattr(daemon, "load_config", lambda: cfg)
    # The existing structural slot fixture has no Git clone or interpreter
    # environment. Only its source-SHA query is supplied by this fixture.
    monkeypatch.setattr(config.VenvProgram, "current_git_sha",
                        lambda self, **kw: Path(self.git_dir).parent.name)
    monkeypatch.setattr(cgroup, "available", lambda: False)
    monkeypatch.setattr(cgroup, "cgroup_path_for_pid", lambda pid: None)
    monkeypatch.setattr(cgroup, "cgroup_path_for_scope", lambda *a, **kw: None)
    worker = daemon.Daemon(max_cpus=4, max_jobs=4)
    worker.queue_dir.mkdir(parents=True, exist_ok=True)
    worker.jobs_dir.mkdir(parents=True, exist_ok=True)
    dispatcher = MockDispatcher(phase=SchedulerPhase.RUNNING)
    _inject(worker, dispatcher)

    def pending(name, *, target=None, selected=SHA_A, exact=False, program_name="demo"):
        workspace = worker.jobs_dir / name
        workspace.mkdir()
        pin = ProgramRuntimePin(expected_git_sha=selected, enforce_git_sha=exact)
        if target:
            pin.scheduler_host = target
            pin.resolved_executable = f"/opt/runtime/{selected}/run"
        spec = JobSpec(
            id=name, cwd=str(workspace), command=["true"], cpus=1,
            program=program_name, scheduler_target=target, program_runtime_pin=pin,
        )
        spec.write(worker._spec_path(name))
        return spec

    def read(spec):
        return JobSpec.read(worker._spec_path(spec.id))

    # A real daemon submission boundary with only scheduler transport replaced.
    existing = pending("existing", target="host_f")
    assert worker._start_job(existing)
    old_pin = read(existing).program_runtime_pin.model_dump()
    marker = admin.acquire_admin_update_marker(
        ["scheduler-runtime:host_f:demo"], "host_f",
    )
    admin.transition_admin_update_state(admin.ADMIN_UPDATE_STATE_BUILDING)
    first = pending("during-build", target="host_f")
    other = pending("other-program", target="host_f", program_name="other")
    worker._dispatch_pending()
    assert set(dispatcher.submitted) == {existing.id, first.id, other.id}
    assert read(existing).state == JobState.RUNNING
    assert read(existing).program_runtime_pin.model_dump() == old_pin
    receipt = cli._submit_receipt(cfg, "host_f", [first.id])
    assert receipt["dispatch_holds"] == []
    assert receipt["background_admin_updates"][0]["state"] == "building"
    with pytest.raises(admin.AdminMarkerPresent):
        admin.acquire_admin_update_marker(["scheduler-runtime:host_f:demo"], "host_f")

    admin.transition_admin_update_state(admin.ADMIN_UPDATE_STATE_FAILED,
                                        failure_reason="replacement build refused")
    # A dead updater's receipt still serializes recovery, without holding work.
    monkeypatch.setattr(admin, "admin_update_marker_stale_reason", lambda m: "writer exited")
    failed = pending("after-failure", target="host_f")
    worker._dispatch_pending()
    assert dispatcher.submitted.count(failed.id) == 1
    assert admin.read_admin_update_marker().marker_id == marker.marker_id
    assert admin.read_admin_update_marker().state == admin.ADMIN_UPDATE_STATE_FAILED
    worker._dispatch_pending()
    assert len(dispatcher.submitted) == len(set(dispatcher.submitted))

    # An unavailable required identity remains a refusal, even without current.
    strict = pending("requires-new", selected="c" * 40, exact=True)
    assert "0 matching generations" in daemon._program_runtime_pin_dispatch_failure(strict)
    (root / "current").unlink()
    assert "0 matching generations" in daemon._program_runtime_pin_dispatch_failure(strict)
    runtime_slots.activate(root, SHA_A)

    # The direct-host producer must actually avoid the old pause bracket.
    monkeypatch.setattr(admin, "pause_token_scope_with_proof",
                        lambda *a, **kw: pytest.fail("immutable build paused jobs"))
    monkeypatch.setattr(admin, "resume_token_scope_with_proof",
                        lambda *a, **kw: pytest.fail("immutable build resumed jobs"))
    monkeypatch.setattr(admin, "_maybe_restart_daemon", lambda *a, **kw: None)
    monkeypatch.setattr(admin, "_maybe_refresh_console", lambda *a, **kw: None)
    monkeypatch.setattr(admin, "record_update_outcome", lambda *a, **kw: None)
    local = pending("local-current")

    def controlled_build(env, prog, **kwargs):
        active = next(m for m in admin.read_admin_update_markers()
                      if m.envs == ["runtime-slot:demo"])
        assert not active.owns_pause_scope
        assert not worker._admin_update_holds_spec(local)
        assert worker._start_job(local)
        assert read(local).program_runtime_pin.resolved_git_sha == SHA_A
        return admin.UpdateResult(env=env, git_dir=prog.git_dir, branch=prog.branch,
                                  update_script=None, work_errors=["controlled failure"])

    monkeypatch.setattr(admin, "_do_update_work", controlled_build)
    try:
        result = admin._update_env_logged(
            "demo", cfg, prog=program, multi_user=False, host="localhost",
            admin_token=None, expected_tag=None, expected_sha=SHA_B, force=False,
            restart_daemon=False, require_self_update=False, managed_daemon_restart=False,
            initial_self_update_probe=None, update_script_args=None,
        )
        assert not result.success
        assert "not paused" in result.paused_summary
        local_marker = next(m for m in admin.read_admin_update_markers()
                            if m.envs == ["runtime-slot:demo"])
        assert local_marker.state == admin.ADMIN_UPDATE_STATE_FAILED
        assert not worker._admin_update_holds_spec(local)
        with pytest.raises(admin.AdminMarkerPresent):
            admin.acquire_admin_update_marker(["demo"], "localhost")

        # Verified activation changes future selection, never an admitted pin.
        runtime_slots.activate(root, SHA_B)
        later = pending("after-activation")
        assert worker._start_job(later)
        assert read(later).program_runtime_pin.resolved_git_sha == SHA_B
        assert read(local).program_runtime_pin.resolved_git_sha == SHA_A
        assert runtime_slots.slot_python(root, SHA_A).is_file()

        # Recovery, in-place/helper mutation, global and maintenance fences stay.
        assert admin.admin_update_marker_scope(replace(local_marker, pause_token="owned")) == {
            admin.LOCAL_DISPATCH_SCOPE,
        }
        assert admin.admin_update_marker_scope(replace(marker, managed_transaction={})) == {
            "host_f",
        }
        for envs, held in ((["demo"], {admin.LOCAL_DISPATCH_SCOPE}),
                           (["scheduler:host_f"], {"host_f"}),
                           (["unknown:scope"], None)):
            assert admin.admin_update_marker_scope(replace(marker, envs=envs)) == held
        assert admin.admin_update_marker_scope(None) is None
        assert admin.admin_update_marker_scope(replace(marker, host="other-host")) is None
        maintenance = pending("maintenance", target="host_f")
        drain.write_drain_state(drain.DrainState(), via_rpc=False)
        worker._dispatch_pending()
        assert maintenance.id not in dispatcher.submitted
        assert read(maintenance).state == JobState.PENDING
    finally:
        for job in worker._running.values():
            job.popen.wait(timeout=10)
            job.close_logs()
