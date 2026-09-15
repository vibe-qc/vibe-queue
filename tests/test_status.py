"""Tests for the show_status formatter."""
from __future__ import annotations

import json
import os
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest

import vq.status as status_mod
from vq import events, spec_access
from vq.spec import JobSpec, JobState
from vq.status import show_status, show_status_json


def _setup(
    tmp_path: Path,
    jobid: str = "abc",
    stdout: str = "",
    stderr: str = "",
    **fields: object,
) -> Path:
    queue = tmp_path / "queue"
    queue.mkdir(parents=True)
    workspace = tmp_path / "ws" / jobid
    workspace.mkdir(parents=True)
    (workspace / "stdout.log").write_text(stdout)
    (workspace / "stderr.log").write_text(stderr)
    base: dict[str, object] = {
        "id": jobid,
        "command": ["echo", "ok"],
        "cwd": str(workspace),
        "cpus": 1,
    }
    base.update(fields)
    JobSpec(**base).write(queue / f"{jobid}.json")
    return queue


class TestShowStatus:
    def test_active_scheduler_status_refreshes_before_render(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        queue = _setup(
            tmp_path,
            state=JobState.RUNNING,
            scheduler_target="host_f",
            scheduler_job_id="12345",
        )
        calls: list[tuple[str, bool, float]] = []

        def refresh(
            jobid: str, *, multi_user: bool, timeout_seconds: float
        ) -> object:
            calls.append((jobid, multi_user, timeout_seconds))
            spec_path = queue / "abc.json"
            spec = JobSpec.read(spec_path)
            spec.state = JobState.COMPLETED
            spec.exit_code = 0
            spec.write(spec_path)
            return {
                "schema": "vq.scheduler.status_refresh/1",
                "completed": True,
                "observed_at": "2026-08-11T12:00:00+00:00",
            }

        monkeypatch.setattr(
            status_mod.rpc,
            "request_scheduler_status_refresh",
            refresh,
        )

        payload = json.loads(show_status_json("localhost", "abc", queue_dir=queue))

        assert payload["state"] == "completed"
        assert payload["scheduler_refresh"] == {
            "status": "fresh",
            "observed_at": "2026-08-11T12:00:00+00:00",
            "reason": None,
        }
        assert calls == [
            ("abc", False, status_mod.SCHEDULER_STATUS_REFRESH_SECONDS)
        ]

    @pytest.mark.parametrize("replacement", ["symlink", "fifo", "foreign_regular"])
    def test_scheduler_refresh_reread_rejects_path_swap(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        replacement: str,
    ) -> None:
        queue = _setup(
            tmp_path,
            state=JobState.RUNNING,
            scheduler_target="host_f",
            scheduler_job_id="12345",
            submitter="1001",
        )
        spec_path = queue / "abc.json"
        initial = JobSpec.read(spec_path)
        foreign_path = tmp_path / "foreign.json"
        JobSpec(
            id="abc",
            command=["echo", "foreign"],
            cwd=str(tmp_path / "foreign-workspace"),
            cpus=1,
            state=JobState.FAILED,
            exit_code=91,
            submitter="2002",
        ).write(foreign_path)

        monkeypatch.setattr(
            status_mod,
            "resolve_authorized_spec",
            lambda *_args, **_kwargs: (spec_path, initial),
        )
        monkeypatch.setattr(
            spec_access.paths,
            "resolve_spec_path",
            lambda *_args, **_kwargs: spec_path,
        )

        def authorize_opened_spec(
            spec: JobSpec, *, cfg: object = None, multi_user: bool = False
        ) -> None:
            del cfg
            assert multi_user is True
            if spec.submitter == "2002":
                raise spec_access.ownership.OwnershipError("foreign replacement")

        monkeypatch.setattr(
            spec_access.ownership,
            "check_owner",
            authorize_opened_spec,
        )

        def refresh(_jobid: str, **_kwargs: object) -> dict[str, object]:
            spec_path.unlink()
            if replacement == "symlink":
                spec_path.symlink_to(foreign_path)
            elif replacement == "fifo":
                os.mkfifo(spec_path)
            else:
                spec_path.write_bytes(foreign_path.read_bytes())
            return {
                "schema": "vq.scheduler.status_refresh/1",
                "completed": True,
                "observed_at": "2026-08-11T12:00:00+00:00",
                "reason": None,
            }

        monkeypatch.setattr(
            status_mod.rpc,
            "request_scheduler_status_refresh",
            refresh,
        )

        payload = json.loads(
            show_status_json(
                "localhost",
                "abc",
                queue_dir=queue,
                multi_user=True,
            )
        )

        assert payload["command"] == ["echo", "ok"]
        assert payload["submitter"] == "1001"
        assert payload["state"] == "running"
        assert payload["scheduler_refresh"] == {
            "status": "unavailable",
            "observed_at": None,
            "reason": "spec_reread_failed",
        }

    def test_scheduler_refresh_timeout_is_explicit_in_json(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        queue = _setup(
            tmp_path,
            state=JobState.RUNNING,
            scheduler_target="host_f",
            scheduler_job_id="12345",
        )
        monkeypatch.setattr(
            status_mod.rpc,
            "request_scheduler_status_refresh",
            lambda *_args, **_kwargs: {
                "schema": "vq.scheduler.status_refresh/1",
                "completed": False,
                "observed_at": None,
            },
        )

        payload = json.loads(show_status_json("localhost", "abc", queue_dir=queue))

        assert payload["state"] == "running"
        assert payload["scheduler_refresh"] == {
            "status": "unavailable",
            "observed_at": None,
            "reason": "timeout",
        }

    def test_includes_core_fields(self, tmp_path: Path) -> None:
        queue = _setup(tmp_path, stdout="line1\nline2\n")
        out = show_status("localhost", "abc", queue_dir=queue)
        assert "id:           abc" in out
        assert "state:        pending" in out
        assert "command:      echo ok" in out

    def test_shows_runtime_fields_when_set(self, tmp_path: Path) -> None:
        queue = _setup(
            tmp_path,
            state=JobState.COMPLETED,
            pid=1234,
            started_at="2026-05-02T10:00:00+00:00",
            finished_at="2026-05-02T10:00:05+00:00",
            exit_code=0,
        )
        out = show_status("localhost", "abc", queue_dir=queue)
        assert "pid:          1234" in out
        assert "started:      2026-05-02T10:00:00+00:00" in out
        assert "finished:     2026-05-02T10:00:05+00:00" in out
        assert "exit_code:    0" in out

    def test_unset_runtime_fields_omitted(self, tmp_path: Path) -> None:
        queue = _setup(tmp_path)
        out = show_status("localhost", "abc", queue_dir=queue)
        assert "pid:" not in out
        assert "started:" not in out
        assert "exit_code:" not in out

    def test_stdout_and_stderr_sections(self, tmp_path: Path) -> None:
        queue = _setup(tmp_path, stdout="hello\n", stderr="oops\n")
        out = show_status("localhost", "abc", queue_dir=queue)
        assert "--- stdout ---" in out
        assert "hello" in out
        assert "--- stderr ---" in out
        assert "oops" in out

    def test_missing_log_file_shows_placeholder(self, tmp_path: Path) -> None:
        queue = tmp_path / "queue"
        queue.mkdir(parents=True)
        ws = tmp_path / "ws"
        ws.mkdir()
        JobSpec(id="abc", command=["true"], cwd=str(ws), cpus=1).write(queue / "abc.json")
        out = show_status("localhost", "abc", queue_dir=queue)
        assert out.count("(no output)") == 2

    def test_empty_log_file_shows_placeholder(self, tmp_path: Path) -> None:
        queue = _setup(tmp_path, stdout="", stderr="")
        out = show_status("localhost", "abc", queue_dir=queue)
        assert "(empty)" in out

    def test_tail_truncates_long_output(self, tmp_path: Path) -> None:
        long_text = "\n".join(f"line {i}" for i in range(100)) + "\n"
        queue = _setup(tmp_path, stdout=long_text, stderr="")
        out = show_status("localhost", "abc", queue_dir=queue, tail=10)
        assert "earlier lines" in out
        assert "line 99" in out
        assert "line 0\n" not in out

    def test_tail_none_shows_full_output(self, tmp_path: Path) -> None:
        long_text = "\n".join(f"line {i}" for i in range(20)) + "\n"
        queue = _setup(tmp_path, stdout=long_text)
        out = show_status("localhost", "abc", queue_dir=queue, tail=None)
        assert "earlier lines" not in out
        assert "line 0" in out
        assert "line 19" in out

    def test_unknown_jobid_raises(self, tmp_path: Path) -> None:
        queue = tmp_path / "queue"
        queue.mkdir(parents=True)
        with pytest.raises(FileNotFoundError):
            show_status("localhost", "nope", queue_dir=queue)

    def test_remote_host_rejected(self) -> None:
        with pytest.raises(NotImplementedError):
            show_status("some.host", "abc")

    def test_terminal_status_stamps_last_status_at(self, tmp_path: Path) -> None:
        queue = _setup(
            tmp_path,
            state=JobState.COMPLETED,
            finished_at="2026-05-02T10:00:05+00:00",
            exit_code=0,
        )
        before = JobSpec.read(queue / "abc.json")
        assert before.last_status_at is None
        show_status("localhost", "abc", queue_dir=queue)
        after = JobSpec.read(queue / "abc.json")
        assert after.last_status_at is not None

    def test_pending_status_does_not_touch_spec(self, tmp_path: Path) -> None:
        # Non-terminal: don't write -- the daemon owns those specs.
        queue = _setup(tmp_path)
        show_status("localhost", "abc", queue_dir=queue)
        spec = JobSpec.read(queue / "abc.json")
        assert spec.last_status_at is None

    def test_archived_state_label_and_archive_path_shown(
        self, tmp_path: Path
    ) -> None:
        queue = _setup(
            tmp_path,
            state=JobState.COMPLETED,
            finished_at="2026-05-02T10:00:05+00:00",
            exit_code=0,
            archived_at="2026-05-03T00:00:00+00:00",
            archive_path="/tmp/vq-archive/abc.tar.bz2",
        )
        out = show_status("localhost", "abc", queue_dir=queue)
        assert "(archived)" in out
        assert "archive:      /tmp/vq-archive/abc.tar.bz2" in out
        assert "vq cleanup --restore" in out

    def test_job_name_shown_prominently_when_set(self, tmp_path: Path) -> None:
        """v0.5.34: ``name:`` line appears right after ``id:`` when set."""
        queue = _setup(tmp_path, job_name="mgo-pbe-rev2")
        out = show_status("localhost", "abc", queue_dir=queue)
        assert "name:         mgo-pbe-rev2" in out
        # name appears before state in the formatted output.
        name_pos = out.index("name:")
        state_pos = out.index("state:")
        assert name_pos < state_pos

    def test_job_name_omitted_when_unset(self, tmp_path: Path) -> None:
        """Pre-v0.5.34 status block unchanged when no name."""
        queue = _setup(tmp_path)
        out = show_status("localhost", "abc", queue_dir=queue)
        assert "name:" not in out

    def test_program_shown_when_set(self, tmp_path: Path) -> None:
        """v0.12.0: ``program:`` surfaces the submitted registry identity."""
        queue = _setup(tmp_path, program="orca")
        out = show_status("localhost", "abc", queue_dir=queue)
        assert "program:      orca" in out
        assert out.index("program:") < out.index("state:")

    def test_scheduler_tasks_shown_when_set(self, tmp_path: Path) -> None:
        queue = _setup(tmp_path, cpus=1, scheduler_tasks=8)

        out = show_status("localhost", "abc", queue_dir=queue)
        payload = json.loads(show_status_json("localhost", "abc", queue_dir=queue))

        assert "cpus:         1" in out
        assert "sched_tasks:  8" in out
        assert payload["scheduler_tasks"] == 8

    def test_pause_accounting_shown_when_set(self, tmp_path: Path) -> None:
        queue = _setup(
            tmp_path,
            state=JobState.SUSPENDED,
            paused_by="admin-update-abc123",
            paused_at="2026-07-02T20:39:51+00:00",
            paused_seconds_total=197.5,
        )

        out = show_status("localhost", "abc", queue_dir=queue)
        payload = json.loads(show_status_json("localhost", "abc", queue_dir=queue))

        assert "paused_by:    admin-update-abc123" in out
        assert "paused_now:" in out
        assert "since 2026-07-02T20:39:51+00:00" in out
        assert "paused_total: 3m18s" in out
        assert payload["paused_current_seconds"] is not None
        assert payload["paused_effective_seconds"] >= 197.5

    def test_pause_accounting_prefers_monotonic_anchor(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(status_mod.time, "monotonic", lambda: 1012.4)
        queue = _setup(
            tmp_path,
            state=JobState.SUSPENDED,
            paused_by="admin-update-abc123",
            paused_at="2020-01-01T00:00:00+00:00",
            paused_monotonic_at=1000.0,
            paused_seconds_total=197.5,
        )

        out = show_status("localhost", "abc", queue_dir=queue)
        payload = json.loads(show_status_json("localhost", "abc", queue_dir=queue))

        assert "paused_now:   12s since 2020-01-01T00:00:00+00:00" in out
        assert payload["paused_current_seconds"] == pytest.approx(12.4)
        assert payload["paused_effective_seconds"] == pytest.approx(209.9)


class TestStatusJsonMonitoring:
    """Machine-readable fields used by cockpit-style live monitors."""

    def test_public_payload_matches_status_json(self, tmp_path: Path) -> None:
        workdir = tmp_path / "workdirs" / "abc"
        workdir.mkdir(parents=True)
        checkpoint = workdir / "checkpoint.qvf"
        checkpoint.write_bytes(b"not-a-readable-qvf")
        queue = _setup(
            tmp_path,
            state=JobState.RUNNING,
            workdir=str(workdir),
        )
        spec = JobSpec.read(queue / "abc.json")

        direct = status_mod.monitoring_payload_for_spec(spec)
        rendered = json.loads(
            show_status_json("localhost", "abc", queue_dir=queue)
        )

        assert list(direct) == [
            "progress",
            "qvf_lifecycle",
            "runtime_workdir",
            "checkpoint_qvf_filename",
            "checkpoint_qvf_path",
            "checkpoint_qvf_exists",
        ]
        assert direct == {key: rendered[key] for key in direct}

    def test_local_job_includes_checkpoint_qvf_contract_when_unreadable(
        self, tmp_path: Path
    ) -> None:
        workdir = tmp_path / "workdirs" / "abc"
        workdir.mkdir(parents=True)
        checkpoint = workdir / "checkpoint.qvf"
        checkpoint.write_bytes(b"qvf")
        queue = _setup(
            tmp_path,
            state=JobState.RUNNING,
            submitted_at="2026-07-02T10:00:00+00:00",
            started_at="2026-07-02T10:01:00+00:00",
            workdir=str(workdir),
        )

        payload = json.loads(show_status_json("localhost", "abc", queue_dir=queue))

        assert payload["state"] == "running"
        assert payload["submitted_at"] == "2026-07-02T10:00:00+00:00"
        assert payload["started_at"] == "2026-07-02T10:01:00+00:00"
        assert payload["queue_handle"] == {
            "job_id": "abc",
            "host": "localhost",
            "submitted_at": "2026-07-02T10:00:00+00:00",
        }
        assert payload["progress"] is None
        assert payload["runtime_workdir"] == str(workdir)
        assert payload["checkpoint_qvf_filename"] == "checkpoint.qvf"
        assert payload["checkpoint_qvf_path"] == str(checkpoint)
        assert payload["checkpoint_qvf_exists"] is True
        assert "wall_elapsed_seconds" in payload
        assert "active_elapsed_seconds" in payload

    def test_local_job_reads_progress_from_checkpoint_qvf_manifest(
        self, tmp_path: Path
    ) -> None:
        workdir = tmp_path / "workdirs" / "abc"
        workdir.mkdir(parents=True)
        checkpoint = workdir / "checkpoint.qvf"
        manifest = {
            "provenance": {
                "run_status": "running",
                "checkpoint": {
                    "seq": 7,
                    "wall_time_s": 12.5,
                    "written_at": "2026-07-02T10:03:00Z",
                    "scf_iteration": 4,
                    "energy_eh": -75.98,
                },
            }
        }
        with zipfile.ZipFile(checkpoint, "w") as zf:
            zf.writestr("manifest.json", json.dumps(manifest))
        queue = _setup(
            tmp_path,
            state=JobState.RUNNING,
            submitted_at="2026-07-02T10:00:00+00:00",
            started_at="2026-07-02T10:01:00+00:00",
            workdir=str(workdir),
        )

        payload = json.loads(show_status_json("localhost", "abc", queue_dir=queue))

        assert payload["checkpoint_qvf_exists"] is True
        assert payload["progress"] == {
            "source": "checkpoint_qvf",
            "run_status": "running",
            "seq": 7,
            "wall_time_s": 12.5,
            "written_at": "2026-07-02T10:03:00Z",
            "scf_iteration": 4,
            "energy_eh": -75.98,
        }

    def test_status_json_exposes_pause_adjusted_runtime_accounting(
        self, tmp_path: Path
    ) -> None:
        queue = _setup(
            tmp_path,
            state=JobState.COMPLETED,
            started_at="2026-07-02T10:00:00+00:00",
            finished_at="2026-07-02T11:00:00+00:00",
            paused_seconds_total=600.0,
            wall_time_seconds=7200,
        )

        payload = json.loads(show_status_json("localhost", "abc", queue_dir=queue))

        assert payload["wall_elapsed_seconds"] == pytest.approx(3600.0)
        assert payload["active_elapsed_seconds"] == pytest.approx(3000.0)
        assert payload["active_walltime_percent"] == 42

    def test_status_json_exposes_scheduler_walltime_seconds(
        self, tmp_path: Path
    ) -> None:
        queue = _setup(
            tmp_path,
            state=JobState.RUNNING,
            scheduler_target="host_f",
            scheduler_job_id="555.cluster",
            scheduler_walltime_used="07:12:00",
            scheduler_walltime_limit="08:00:00",
        )

        payload = json.loads(show_status_json("localhost", "abc", queue_dir=queue))

        assert payload["scheduler_job_id"] == "555.cluster"
        assert payload["scheduler_id"] == "555.cluster"
        assert payload["scheduler_walltime_used_seconds"] == 25920
        assert payload["scheduler_walltime_limit_seconds"] == 28800
        assert payload["scheduler_walltime_percent"] == 90
        assert payload["scheduler_walltime_remaining_seconds"] == 2880

    def test_status_json_parses_slurm_day_scheduler_walltime(
        self, tmp_path: Path
    ) -> None:
        queue = _setup(
            tmp_path,
            state=JobState.RUNNING,
            scheduler_target="host_c",
            scheduler_job_id="12345",
            scheduler_walltime_used="1-02:00:00",
            scheduler_walltime_limit="2-00:00:00",
        )

        payload = json.loads(show_status_json("localhost", "abc", queue_dir=queue))

        assert payload["scheduler_walltime_used_seconds"] == 93600
        assert payload["scheduler_walltime_limit_seconds"] == 172800
        assert payload["scheduler_walltime_percent"] == 54
        assert payload["scheduler_walltime_remaining_seconds"] == 79200

    def test_status_json_terminal_diagnosis_is_null_for_running(
        self, tmp_path: Path
    ) -> None:
        queue = _setup(tmp_path, state=JobState.RUNNING)

        payload = json.loads(show_status_json("localhost", "abc", queue_dir=queue))

        assert payload["terminal_diagnosis"] is None

    def test_status_json_terminal_diagnosis_for_scheduler_walltime(
        self, tmp_path: Path
    ) -> None:
        queue = _setup(
            tmp_path,
            state=JobState.TIME_EXCEEDED,
            scheduler_target="host_f",
            scheduler_job_id="555.cluster",
            scheduler_walltime_used="08:00:40",
            scheduler_walltime_limit="08:00:00",
            failure_reason=(
                "scheduler walltime limit reached "
                "(08:00:40 >= 08:00:00); exit-marker missing"
            ),
        )

        payload = json.loads(show_status_json("localhost", "abc", queue_dir=queue))

        diagnosis = payload["terminal_diagnosis"]
        assert diagnosis["category"] == "scheduler_walltime"
        assert diagnosis["action_hint"] == "increase_walltime"
        assert diagnosis["reason"].endswith("exit-marker missing")

    def test_status_json_terminal_diagnosis_for_exit_137(
        self, tmp_path: Path
    ) -> None:
        queue = _setup(
            tmp_path,
            state=JobState.FAILED,
            exit_code=137,
        )

        payload = json.loads(show_status_json("localhost", "abc", queue_dir=queue))

        diagnosis = payload["terminal_diagnosis"]
        assert diagnosis["category"] == "sigkill"
        assert diagnosis["signal"] == "SIGKILL"
        assert diagnosis["action_hint"] == "increase_memory_or_check_external_kill"
        assert "SIGKILL" in diagnosis["exit_code_description"]

    @pytest.mark.parametrize("exit_code", [None, -1])
    def test_scheduler_submit_failure_is_not_reported_as_a_signal(
        self, tmp_path: Path, exit_code: int | None
    ) -> None:
        queue = _setup(
            tmp_path,
            state=JobState.FAILED,
            exit_code=exit_code,
            scheduler_target="host_c",
            failure_reason=(
                "scheduler submit to host_c failed: sbatch failed (exit 1): "
                "Requested time limit is invalid"
            ),
        )

        payload = json.loads(show_status_json("localhost", "abc", queue_dir=queue))

        diagnosis = payload["terminal_diagnosis"]
        assert diagnosis["category"] == "scheduler_submit_failed"
        assert diagnosis["action_hint"] == "inspect_scheduler_request_and_host_config"
        assert "signal" not in diagnosis
        assert "scheduler job ID" in diagnosis["summary"]

    def test_status_json_terminal_diagnosis_for_scheduler_missing_marker(
        self, tmp_path: Path
    ) -> None:
        queue = _setup(
            tmp_path,
            state=JobState.ABORTED_BY_QUEUE,
            scheduler_target="host_f",
            scheduler_job_id="555.cluster",
            failure_reason="scheduler job finished without an exit-marker",
        )

        payload = json.loads(show_status_json("localhost", "abc", queue_dir=queue))

        diagnosis = payload["terminal_diagnosis"]
        assert diagnosis["category"] == "scheduler_missing_exit_marker"
        assert diagnosis["action_hint"] == "queue_diagnostics"
        assert "exit marker" in diagnosis["summary"]

    def test_status_json_terminal_diagnosis_for_manual_kill_signal(
        self, tmp_path: Path
    ) -> None:
        queue = _setup(
            tmp_path,
            state=JobState.KILLED,
            exit_code=-15,
        )

        payload = json.loads(show_status_json("localhost", "abc", queue_dir=queue))

        diagnosis = payload["terminal_diagnosis"]
        assert diagnosis["category"] == "manual_kill"
        assert diagnosis["signal"] == "SIGTERM"
        assert diagnosis["action_hint"] == "operator_killed"

    def test_scheduler_job_derives_runtime_workdir_when_configured(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import vq.status as status_mod

        class FakeConfig:
            # `show_status_json` now gates on ownership (ISO-1), which asks the
            # config whether multi-user is on. Single-user makes the gate a
            # no-op, which is what this test wants -- it is about scheduler
            # workdir derivation, not access control.
            multi_user = SimpleNamespace(enabled=False)

            def host(self, name: str) -> object:
                assert name == "host_f"
                return object()

        class FakeDispatcher:
            def remote_workspace(self, jobid: str) -> str:
                return f"/remote/scheduler/jobs/{jobid}"

        monkeypatch.setattr(status_mod.config, "load_config", lambda: FakeConfig())
        monkeypatch.setattr(
            status_mod,
            "scheduler_dispatcher_for",
            lambda host_cfg: FakeDispatcher(),
        )
        queue = _setup(
            tmp_path,
            state=JobState.RUNNING,
            scheduler_target="host_f",
            scheduler_job_id="555.cluster",
        )

        payload = json.loads(show_status_json("localhost", "abc", queue_dir=queue))

        assert payload["queue_handle"] == {
            "job_id": "abc",
            "host": "host_f",
            "submitted_at": payload["submitted_at"],
        }
        assert payload["workdir"] is None
        assert payload["runtime_workdir"] == "/remote/scheduler/jobs/abc"
        assert (
            payload["checkpoint_qvf_path"]
            == "/remote/scheduler/jobs/abc/checkpoint.qvf"
        )
        assert payload["checkpoint_qvf_exists"] is None


class TestSchedulerStatus:
    """v1.0 scheduler backend: surface qstat detail in vq status."""

    def test_scheduler_detail_rendered(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import vq.status as status_mod

        monkeypatch.setattr(status_mod, "is_daemon_serving", lambda **k: True)
        queue = _setup(
            tmp_path,
            state=JobState.RUNNING,
            started_at="2026-06-25T10:00:00+00:00",
            scheduler_target="host_f",
            scheduler_job_id="555.cluster",
            scheduler_state="running",
            scheduler_exec_host="node07/0-19",
            scheduler_walltime_used="02:00:00",
            scheduler_walltime_limit="08:00:00",
        )
        out = show_status("localhost", "abc", queue_dir=queue)
        assert "scheduler:    host_f job=555.cluster" in out
        assert "sched_state:  running" in out
        assert "fetch_state:  live workspace on scheduler host" in out
        assert "exec_host:    node07/0-19" in out
        assert "sched_wall:   02:00:00 / 08:00:00 (25% used)" in out

    def test_scheduler_queued_state_explains_handoff(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import vq.status as status_mod

        monkeypatch.setattr(status_mod, "is_daemon_serving", lambda **k: True)
        queue = _setup(
            tmp_path,
            state=JobState.RUNNING,
            started_at="2026-06-30T15:19:15.097243+00:00",
            scheduler_target="host_f-big",
            scheduler_job_id="6620.pbs.cluster.example",
            scheduler_state="queued",
        )

        out = show_status("localhost", "abc", queue_dir=queue)

        assert (
            "state:        running (submitted to scheduler; scheduler queued)"
            in out
        )
        assert "scheduler:    host_f-big job=6620.pbs.cluster.example" in out
        assert "sched_state:  queued" in out
        assert (
            "started:      2026-06-30T15:19:15.097243+00:00 "
            "(scheduler handoff time, not node start)"
        ) in out

    def test_scheduler_queued_status_json_exposes_effective_state(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import vq.status as status_mod

        monkeypatch.setattr(status_mod, "is_daemon_serving", lambda **k: True)
        queue = _setup(
            tmp_path,
            state=JobState.RUNNING,
            started_at="2026-06-30T15:19:15.097243+00:00",
            scheduler_target="host_f-big",
            scheduler_job_id="6620.pbs.cluster.example",
            scheduler_state="queued",
        )

        payload = json.loads(show_status_json("localhost", "abc", queue_dir=queue))

        assert payload["state"] == "running"
        assert payload["scheduler_state"] == "queued"
        assert payload["effective_state"] == "queued"
        assert payload["scheduler_running_confirmed"] is False

    def test_scheduler_held_status_json_projects_exact_hold(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import vq.status as status_mod

        monkeypatch.setattr(status_mod, "is_daemon_serving", lambda **k: True)
        queue = _setup(
            tmp_path,
            state=JobState.SUSPENDED,
            scheduler_target="host_f-big",
            scheduler_job_id="6620.pbs.cluster.example",
            scheduler_state="held",
        )

        payload = json.loads(show_status_json("localhost", "abc", queue_dir=queue))

        assert payload["state"] == "suspended"
        assert payload["scheduler_state"] == "held"
        assert payload["effective_state"] == "held"
        assert payload["scheduler_running_confirmed"] is None

    @pytest.mark.parametrize(
        "scheduler_state",
        [
            "poll_failed",
            "finishing",
            "marker_probe_failed",
            "fetch_failed",
            "reattach_failed",
        ],
    )
    def test_scheduler_uncertain_status_json_is_not_effectively_running(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        scheduler_state: str,
    ) -> None:
        import vq.status as status_mod

        monkeypatch.setattr(status_mod, "is_daemon_serving", lambda **k: True)
        queue = _setup(
            tmp_path,
            state=JobState.RUNNING,
            scheduler_target="host_c",
            scheduler_job_id="27104831",
            scheduler_state=scheduler_state,
        )

        payload = json.loads(show_status_json("localhost", "abc", queue_dir=queue))

        assert payload["state"] == "running"
        assert payload["effective_state"] == scheduler_state
        assert payload["scheduler_running_confirmed"] is False

    def test_unknown_scheduler_phase_cannot_forge_status_text(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import vq.status as status_mod

        monkeypatch.setattr(status_mod, "is_daemon_serving", lambda **k: True)
        raw_phase = "running\nFORGED idle\x00" + ("x" * 300)
        queue = _setup(
            tmp_path,
            state=JobState.RUNNING,
            scheduler_target="host_c",
            scheduler_job_id="27104831",
            scheduler_state=raw_phase,
        )

        out = show_status("localhost", "abc", queue_dir=queue)
        payload = json.loads(show_status_json("localhost", "abc", queue_dir=queue))

        assert "FORGED idle" not in out
        assert "sched_state:  scheduler_unknown" in out
        assert (
            "fetch_state:  scheduler phase or ownership unknown; "
            "remote workspace state unknown"
        ) in out
        assert payload["scheduler_state"] == raw_phase
        assert payload["effective_state"] == "scheduler_unknown"
        assert payload["scheduler_running_confirmed"] is False

    def test_unsafe_scheduler_target_cannot_forge_status_confirmation(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import vq.status as status_mod

        monkeypatch.setattr(status_mod, "is_daemon_serving", lambda **k: True)
        raw_target = "host_c\nFORGED idle\x1b[31m" + ("x" * 150)
        queue = _setup(
            tmp_path,
            state=JobState.RUNNING,
            scheduler_target=raw_target,
            scheduler_job_id="27104831",
            scheduler_state="running",
        )

        out = show_status("localhost", "abc", queue_dir=queue)
        payload = json.loads(show_status_json("localhost", "abc", queue_dir=queue))

        assert "FORGED idle" not in out
        assert "scheduler:    scheduler-target-unknown job=27104831" in out
        assert (
            "fetch_state:  scheduler phase or ownership unknown; "
            "remote workspace state unknown"
        ) in out
        assert "fetch_state:  live workspace on scheduler host" not in out
        assert payload["scheduler_target"] == raw_target
        assert payload["effective_state"] == "scheduler_unknown"
        assert payload["scheduler_running_confirmed"] is False
        assert payload["queue_handle"]["host"] == "localhost"

    def test_unsafe_pending_scheduler_target_cannot_forge_queue_position(
        self, tmp_path: Path
    ) -> None:
        raw_target = "host_f\nFORGED queue position\x1b[31m" + ("x" * 150)
        queue = _setup(
            tmp_path,
            state=JobState.PENDING,
            scheduler_target=raw_target,
        )

        out = show_status("localhost", "abc", queue_dir=queue)

        assert "FORGED queue position" not in out
        assert "scheduler-target-unknown scheduler lane" in out
        assert "scheduler:    scheduler-target-unknown" in out

    def test_running_transaction_phase_does_not_claim_live_workspace(
        self, tmp_path: Path
    ) -> None:
        queue = _setup(
            tmp_path,
            state=JobState.RUNNING,
            scheduler_target="host_f",
            scheduler_job_id="27104831",
            scheduler_state="submit_evidence_conflict",
        )

        out = show_status("localhost", "abc", queue_dir=queue)
        payload = json.loads(show_status_json("localhost", "abc", queue_dir=queue))

        assert "sched_state:  submit_evidence_conflict" in out
        assert (
            "fetch_state:  scheduler phase or ownership unknown; "
            "remote workspace state unknown"
        ) in out
        assert "fetch_state:  live workspace on scheduler host" not in out
        assert payload["effective_state"] == "scheduler_unknown"
        assert payload["scheduler_running_confirmed"] is False

    @pytest.mark.parametrize(
        "scheduler_state",
        ["scheduler_reconciliation_quarantined", "future_phase"],
    )
    def test_suspended_unknown_scheduler_phase_does_not_claim_live_workspace(
        self,
        tmp_path: Path,
        scheduler_state: str,
    ) -> None:
        queue = _setup(
            tmp_path,
            state=JobState.SUSPENDED,
            scheduler_target="host_c",
            scheduler_job_id="27104831",
            scheduler_state=scheduler_state,
        )

        out = show_status("localhost", "abc", queue_dir=queue)

        assert (
            "fetch_state:  scheduler phase or ownership unknown; "
            "remote workspace state unknown"
        ) in out
        assert "fetch_state:  live workspace on scheduler host" not in out

    def test_scheduler_unpolled_state_explains_handoff_even_if_stale(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import vq.status as status_mod

        monkeypatch.setattr(status_mod, "is_daemon_serving", lambda **k: False)
        queue = _setup(
            tmp_path,
            state=JobState.RUNNING,
            started_at="2026-06-30T16:20:48.204564+00:00",
            scheduler_target="host_f-jtwin",
        )

        out = show_status("localhost", "abc", queue_dir=queue)

        assert (
            "state:        running (submitted to scheduler; scheduler unpolled)"
            in out
        )
        assert "daemon down" in out
        assert "may be stale" in out
        assert "sched_state:  unpolled" in out
        assert (
            "started:      2026-06-30T16:20:48.204564+00:00 "
            "(scheduler handoff time, not node start)"
        ) in out

    def test_scheduler_walltime_warning_near_limit(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import vq.status as status_mod

        monkeypatch.setattr(status_mod, "is_daemon_serving", lambda **k: True)
        queue = _setup(
            tmp_path,
            state=JobState.RUNNING,
            scheduler_target="host_f",
            scheduler_job_id="555.cluster",
            scheduler_state="running",
            scheduler_walltime_used="07:12:00",
            scheduler_walltime_limit="08:00:00",
        )
        out = show_status("localhost", "abc", queue_dir=queue)
        assert "sched_wall:   07:12:00 / 08:00:00 (90% used)" in out
        assert (
            "warning:      scheduler walltime nearly exhausted "
            "(90% used; 00:48:00 remaining)"
        ) in out

    def test_scheduler_walltime_warning_accepts_slurm_day_format(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import vq.status as status_mod

        monkeypatch.setattr(status_mod, "is_daemon_serving", lambda **k: True)
        queue = _setup(
            tmp_path,
            state=JobState.RUNNING,
            scheduler_target="host_c",
            scheduler_job_id="12345",
            scheduler_state="running",
            scheduler_walltime_used="1-21:00:00",
            scheduler_walltime_limit="2-00:00:00",
        )
        out = show_status("localhost", "abc", queue_dir=queue)
        assert "sched_wall:   1-21:00:00 / 2-00:00:00 (94% used)" in out
        assert (
            "warning:      scheduler walltime nearly exhausted "
            "(94% used; 03:00:00 remaining)"
        ) in out

    def test_scheduler_walltime_warning_omitted_below_threshold(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import vq.status as status_mod

        monkeypatch.setattr(status_mod, "is_daemon_serving", lambda **k: True)
        queue = _setup(
            tmp_path,
            state=JobState.RUNNING,
            scheduler_target="host_f",
            scheduler_job_id="555.cluster",
            scheduler_state="running",
            scheduler_walltime_used="07:00:00",
            scheduler_walltime_limit="08:00:00",
        )
        out = show_status("localhost", "abc", queue_dir=queue)
        assert "sched_wall:   07:00:00 / 08:00:00 (88% used)" in out
        assert "scheduler walltime nearly exhausted" not in out

    def test_scheduler_state_defaults_to_unpolled(self, tmp_path: Path) -> None:
        queue = _setup(tmp_path, scheduler_target="host_f")
        out = show_status("localhost", "abc", queue_dir=queue)
        assert "scheduler:    host_f" in out
        assert "sched_state:  unpolled" in out
        assert "fetch_state:  live workspace on scheduler host" in out

    def test_scheduler_finishing_shows_fetch_fence(self, tmp_path: Path) -> None:
        queue = _setup(
            tmp_path,
            state=JobState.RUNNING,
            scheduler_target="host_f",
            scheduler_job_id="555.cluster",
            scheduler_state="finishing",
        )
        out = show_status("localhost", "abc", queue_dir=queue)
        assert "sched_state:  finishing" in out
        assert "scheduler finished; exit marker and final artifacts pending" in out
        assert "fetch_state:  waiting for exit-marker/final artifact collection" in out

    def test_scheduler_poll_failed_shows_retry_state(self, tmp_path: Path) -> None:
        queue = _setup(
            tmp_path,
            state=JobState.RUNNING,
            scheduler_target="host_f",
            scheduler_job_id="555.cluster",
            scheduler_state="poll_failed",
        )
        out = show_status("localhost", "abc", queue_dir=queue)
        assert (
            "state:        running (scheduler poll failed; daemon will retry)"
            in out
        )
        assert "sched_state:  poll_failed" in out
        assert (
            "fetch_state:  scheduler poll failed; remote workspace state unknown"
            in out
        )

    def test_scheduler_marker_probe_failed_shows_retry_state(
        self, tmp_path: Path
    ) -> None:
        queue = _setup(
            tmp_path,
            state=JobState.RUNNING,
            scheduler_target="host_f",
            scheduler_job_id="555.cluster",
            scheduler_state="marker_probe_failed",
        )
        out = show_status("localhost", "abc", queue_dir=queue)
        assert (
            "state:        running "
            "(scheduler exit-marker probe failed; daemon will retry)"
            in out
        )
        assert "sched_state:  marker_probe_failed" in out
        assert "fetch_state:  exit-marker probe failed; daemon will retry" in out

    def test_scheduler_fetch_failed_shows_retry_state(self, tmp_path: Path) -> None:
        queue = _setup(
            tmp_path,
            state=JobState.RUNNING,
            scheduler_target="host_f",
            scheduler_job_id="555.cluster",
            scheduler_state="fetch_failed",
        )
        out = show_status("localhost", "abc", queue_dir=queue)
        assert (
            "state:        running (scheduler output fetch failed; daemon will retry)"
            in out
        )
        assert "sched_state:  fetch_failed" in out
        assert "fetch_state:  workspace fetch failed; daemon will retry" in out

    def test_scheduler_reattach_failed_shows_untracked_state(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import vq.status as status_mod

        monkeypatch.setattr(status_mod, "is_daemon_serving", lambda **k: True)
        queue = _setup(
            tmp_path,
            state=JobState.RUNNING,
            scheduler_target="host_f",
            scheduler_job_id="555.cluster",
            scheduler_state="reattach_failed",
        )

        out = show_status("localhost", "abc", queue_dir=queue)

        assert (
            "state:        running (scheduler reattach failed; scheduler job untracked)"
            in out
        )
        assert "sched_state:  reattach_failed" in out
        assert (
            "fetch_state:  scheduler job untracked; repair config or scheduler_job_id"
            in out
        )

    def test_terminal_scheduler_job_does_not_render_live_cluster_state(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import vq.status as status_mod

        monkeypatch.setattr(status_mod, "is_daemon_serving", lambda **k: True)
        queue = _setup(
            tmp_path,
            state=JobState.COMPLETED,
            finished_at="2026-06-30T10:00:00+00:00",
            exit_code=0,
            scheduler_target="host_f",
            scheduler_job_id="555.cluster",
            scheduler_state="running",
            # A really-completed scheduler job has been fetched by the daemon's
            # reconcile, so it carries last_fetched_at. Only that field is
            # evidence a fetch ran; finished_at is not (a `vq kill` stamps it
            # without any fetch ever happening).
            last_fetched_at="2026-06-30T10:00:00+00:00",
        )

        out = show_status("localhost", "abc", queue_dir=queue)

        assert "state:        completed" in out
        assert "sched_state:  running (last observed before local completed)" in out
        assert (
            "fetch_state:  workspace staged locally at 2026-06-30T10:00:00+00:00"
        ) in out
        assert "cluster:" not in out

    def test_terminal_scheduler_job_without_a_fetch_says_so(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A killed scheduler job must not claim its workspace came home.

        `finished_at` used to stand in for `last_fetched_at`, so any terminal
        spec reported "workspace staged locally" -- including one `vq kill` had
        just stamped, where no fetch ever ran. That reassured the submitter at
        exactly the moment they needed to know the remote workspace was never
        collected.
        """
        import vq.status as status_mod

        monkeypatch.setattr(status_mod, "is_daemon_serving", lambda **k: True)
        queue = _setup(
            tmp_path,
            state=JobState.KILLED,
            finished_at="2026-06-30T10:00:00+00:00",
            scheduler_target="host_f",
            scheduler_job_id="555.cluster",
            scheduler_state="running",
        )

        out = show_status("localhost", "abc", queue_dir=queue)

        assert "workspace staged locally" not in out
        assert "no fetch recorded" in out

    def test_aborted_scheduler_job_shows_missing_marker_summary(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import vq.status as status_mod

        monkeypatch.setattr(status_mod, "is_daemon_serving", lambda **k: True)
        queue = _setup(
            tmp_path,
            state=JobState.ABORTED_BY_QUEUE,
            finished_at="2026-07-01T10:00:00+00:00",
            scheduler_target="host_f",
            scheduler_job_id="555.cluster",
            scheduler_state="finishing",
        )
        workspace = tmp_path / "ws" / "abc"
        events.state_transition(
            workspace,
            "abc",
            from_state="running",
            to_state=JobState.ABORTED_BY_QUEUE.value,
            reason="scheduler job finished without an exit-marker after grace",
            scheduler_job_id="555.cluster",
            remote_workspace="/remote/abc",
            remote_exit_marker="/remote/abc/_vq/exit-code",
            local_exit_marker=str(workspace / "_vq" / "exit-code"),
            qstat_detail_rc=1,
            qstat_detail_stderr="qstat: Unknown Job Id\n",
            remote_stdout_tail="remote out",
            local_stderr_tail="local err",
        )

        out = show_status("localhost", "abc", queue_dir=queue)

        assert "scheduler diagnostics:" in out
        assert (
            "missing_marker: remote /remote/abc/_vq/exit-code; "
            f"local {workspace / '_vq' / 'exit-code'}"
        ) in out
        assert "remote_workspace: /remote/abc" in out
        assert "scheduler_job: 555.cluster" in out
        assert "qstat_detail: qstat: Unknown Job Id (rc=1)" in out
        assert (
            "captured_tails: remote stdout=yes, stderr=no; "
            "local stdout=no, stderr=yes"
        ) in out
        assert "full_evidence: `vq events <jobid>`" in out

    def test_aborted_scheduler_status_json_includes_missing_marker_event(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import vq.status as status_mod

        monkeypatch.setattr(status_mod, "is_daemon_serving", lambda **k: True)
        queue = _setup(
            tmp_path,
            state=JobState.ABORTED_BY_QUEUE,
            finished_at="2026-07-01T10:00:00+00:00",
            scheduler_target="host_f",
            scheduler_job_id="555.cluster",
            scheduler_state="finishing",
        )
        workspace = tmp_path / "ws" / "abc"
        events.state_transition(
            workspace,
            "abc",
            from_state="running",
            to_state=JobState.ABORTED_BY_QUEUE.value,
            reason="scheduler job finished without an exit-marker after grace",
            scheduler_job_id="555.cluster",
            remote_exit_marker="/remote/abc/_vq/exit-code",
            missing_marker_grace_seconds=120.0,
        )

        payload = json.loads(show_status_json("localhost", "abc", queue_dir=queue))

        event = payload["scheduler_missing_marker_event"]
        assert event["to"] == JobState.ABORTED_BY_QUEUE.value
        assert event["reason"] == (
            "scheduler job finished without an exit-marker after grace"
        )
        assert event["evidence"]["remote_exit_marker"] == (
            "/remote/abc/_vq/exit-code"
        )
        assert event["evidence"]["missing_marker_grace_seconds"] == 120.0

    def test_pending_scheduler_queue_position_uses_target_lane(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import vq.status as status_mod

        monkeypatch.setattr(status_mod, "is_daemon_serving", lambda **k: True)
        queue = _setup(
            tmp_path,
            jobid="target",
            state=JobState.PENDING,
            submitted_at="2026-06-30T10:00:00+00:00",
            scheduler_target="host_f-itwin",
        )
        ws_root = tmp_path / "ws"
        JobSpec(
            id="same",
            command=["true"],
            cwd=str(ws_root / "same"),
            cpus=1,
            state=JobState.PENDING,
            submitted_at="2026-06-30T11:00:00+00:00",
            scheduler_target="host_f-itwin",
        ).write(queue / "same.json")
        JobSpec(
            id="other",
            command=["true"],
            cwd=str(ws_root / "other"),
            cpus=1,
            state=JobState.PENDING,
            submitted_at="2026-06-30T08:00:00+00:00",
            scheduler_target="host_f-big",
        ).write(queue / "other.json")
        JobSpec(
            id="local",
            command=["true"],
            cwd=str(ws_root / "local"),
            cpus=1,
            state=JobState.PENDING,
            submitted_at="2026-06-30T07:00:00+00:00",
        ).write(queue / "local.json")

        out = show_status("localhost", "target", queue_dir=queue)

        assert "queue position: 1 of 2 pending" in out
        assert "host_f-itwin scheduler lane" in out


class TestStatusDaemonLiveness:
    """STATUS-1: a non-terminal state is only meaningful while the daemon is
    alive to reconcile it. When the daemon is down, flag the state as
    possibly-stale so the operator doesn't trust a RUNNING label for a job
    whose process already died."""

    def test_nonterminal_state_warns_when_daemon_down(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import vq.status as status_mod

        monkeypatch.setattr(status_mod, "is_daemon_serving", lambda **k: False)
        queue = _setup(
            tmp_path, state=JobState.RUNNING, pid=123,
            started_at="2026-05-02T10:00:00+00:00",
        )
        out = show_status("localhost", "abc", queue_dir=queue)
        assert "daemon down" in out
        assert "may be stale" in out
        # the underlying state is still rendered (the warning annotates it)
        assert "state:        running" in out

    def test_nonterminal_state_no_warning_when_daemon_up(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import vq.status as status_mod

        monkeypatch.setattr(status_mod, "is_daemon_serving", lambda **k: True)
        queue = _setup(
            tmp_path, state=JobState.RUNNING, pid=123,
            started_at="2026-05-02T10:00:00+00:00",
        )
        out = show_status("localhost", "abc", queue_dir=queue)
        assert "may be stale" not in out

    def test_terminal_state_never_warns_even_when_daemon_down(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import vq.status as status_mod

        monkeypatch.setattr(status_mod, "is_daemon_serving", lambda **k: False)
        queue = _setup(
            tmp_path, state=JobState.COMPLETED,
            finished_at="2026-05-02T10:00:05+00:00", exit_code=0,
        )
        out = show_status("localhost", "abc", queue_dir=queue)
        assert "may be stale" not in out


class TestStatusQueuePosition:
    """v0.9.2: vq status shows a PENDING job's dispatch-order queue position."""

    def test_pending_shows_position(self, tmp_path: Path) -> None:
        queue = _setup(tmp_path, jobid="aaa000000001", state=JobState.PENDING,
                       submitted_at="2026-06-05T10:00:00+00:00")
        # a second pending job submitted later -> the first is position 1 of 2
        JobSpec(id="bbb000000002", command=["echo", "ok"], cwd=str(tmp_path),
                cpus=1, state=JobState.PENDING,
                submitted_at="2026-06-05T11:00:00+00:00").write(queue / "bbb000000002.json")
        out = show_status("localhost", "aaa000000001", queue_dir=queue)
        assert "queue position: 1 of 2 pending" in out
        assert "queue eta:    ~0s before dispatch turn" in out

    def test_pending_shows_history_eta_for_jobs_ahead(self, tmp_path: Path) -> None:
        queue = _setup(
            tmp_path,
            jobid="target000001",
            state=JobState.PENDING,
            submitted_at="2026-06-05T12:00:00+00:00",
            tags=["paper"],
        )
        JobSpec(
            id="ahead0000001",
            command=["echo", "ok"],
            cwd=str(tmp_path / "ahead"),
            cpus=1,
            state=JobState.PENDING,
            submitted_at="2026-06-05T11:00:00+00:00",
            tags=["paper"],
        ).write(queue / "ahead0000001.json")
        JobSpec(
            id="done00000001",
            command=["echo", "ok"],
            cwd=str(tmp_path / "done"),
            cpus=1,
            state=JobState.COMPLETED,
            started_at="2026-06-05T09:00:00+00:00",
            finished_at="2026-06-05T10:00:00+00:00",
            tags=["paper"],
        ).write(queue / "done00000001.json")

        out = show_status("localhost", "target000001", queue_dir=queue)

        assert "queue position: 2 of 2 pending" in out
        assert (
            "queue eta:    ~1h00m before dispatch turn "
            "(1 jobs ahead; history: tag+command+cpus=1)"
        ) in out

    def test_pending_eta_unavailable_without_history(self, tmp_path: Path) -> None:
        queue = _setup(
            tmp_path,
            jobid="target000001",
            state=JobState.PENDING,
            submitted_at="2026-06-05T12:00:00+00:00",
        )
        JobSpec(
            id="ahead0000001",
            command=["echo", "ok"],
            cwd=str(tmp_path / "ahead"),
            cpus=1,
            state=JobState.PENDING,
            submitted_at="2026-06-05T11:00:00+00:00",
        ).write(queue / "ahead0000001.json")

        out = show_status("localhost", "target000001", queue_dir=queue)

        assert "queue eta:    unavailable" in out

    def test_pending_weak_history_eta_is_qualified(self, tmp_path: Path) -> None:
        """IID 293: a 1-sample generic command shape must not read as a
        confident numeric ETA."""
        queue = _setup(
            tmp_path,
            jobid="target000002",
            state=JobState.PENDING,
            submitted_at="2026-06-05T12:00:00+00:00",
        )
        command = ["bash", "vibeqc-release-python", "run.py"]
        JobSpec(
            id="ahead0000002",
            command=command,
            cwd=str(tmp_path / "ahead2"),
            cpus=16,
            state=JobState.PENDING,
            submitted_at="2026-06-05T11:00:00+00:00",
        ).write(queue / "ahead0000002.json")
        JobSpec(
            id="done00000002",
            command=command,
            cwd=str(tmp_path / "done2"),
            cpus=16,
            state=JobState.COMPLETED,
            started_at="2026-06-05T09:00:00+00:00",
            finished_at="2026-06-05T10:00:00+00:00",
        ).write(queue / "done00000002.json")

        out = show_status("localhost", "target000002", queue_dir=queue)

        assert (
            "queue eta:    unknown (too few samples for a reliable estimate;"
            in out
        )
        assert "history: command+cpus=1" in out

    def test_pending_reused_weak_history_eta_stays_unknown(
        self, tmp_path: Path
    ) -> None:
        """IID 293: three queued jobs cannot multiply one retained sample."""
        queue = _setup(
            tmp_path,
            jobid="target000003",
            state=JobState.PENDING,
            submitted_at="2026-06-05T14:00:00+00:00",
        )
        command = ["bash", "vibeqc-release-python", "run.py"]
        for i in range(3):
            JobSpec(
                id=f"ahead00000{i}",
                command=command,
                cwd=str(tmp_path / f"ahead{i}"),
                cpus=16,
                state=JobState.PENDING,
                submitted_at=f"2026-06-05T{10 + i:02d}:00:00+00:00",
            ).write(queue / f"ahead00000{i}.json")
        JobSpec(
            id="done00000003",
            command=command,
            cwd=str(tmp_path / "done3"),
            cpus=16,
            state=JobState.COMPLETED,
            started_at="2026-06-05T08:00:00+00:00",
            finished_at="2026-06-05T09:00:00+00:00",
        ).write(queue / "done00000003.json")

        out = show_status("localhost", "target000003", queue_dir=queue)

        assert (
            "queue eta:    unknown (too few samples for a reliable estimate;"
            in out
        )
        assert "history: command+cpus=1" in out
        assert "queue eta:    ~3h00m" not in out

    def test_scheduler_pending_eta_filters_to_scheduler_target(
        self, tmp_path: Path
    ) -> None:
        queue = _setup(
            tmp_path,
            jobid="twintarget01",
            state=JobState.PENDING,
            submitted_at="2026-06-05T12:00:00+00:00",
            scheduler_target="host_f",
        )
        JobSpec(
            id="twinahead001",
            command=["echo", "ok"],
            cwd=str(tmp_path / "twinahead"),
            cpus=1,
            state=JobState.PENDING,
            submitted_at="2026-06-05T11:00:00+00:00",
            scheduler_target="host_f",
        ).write(queue / "twinahead001.json")
        JobSpec(
            id="localahead01",
            command=["echo", "ok"],
            cwd=str(tmp_path / "localahead"),
            cpus=1,
            state=JobState.PENDING,
            submitted_at="2026-06-05T10:00:00+00:00",
        ).write(queue / "localahead01.json")
        JobSpec(
            id="twindone0001",
            command=["echo", "ok"],
            cwd=str(tmp_path / "twindone"),
            cpus=1,
            state=JobState.COMPLETED,
            scheduler_target="host_f",
            scheduler_walltime_used="00:30:00",
            started_at="2026-06-05T00:00:00+00:00",
            finished_at="2026-06-05T09:00:00+00:00",
        ).write(queue / "twindone0001.json")
        # Two more host_f samples lift the generic source past the IID 293
        # low-sample flag so this test keeps exercising the numeric path.
        for i in (2, 3):
            JobSpec(
                id=f"twindone000{i}",
                command=["echo", "ok"],
                cwd=str(tmp_path / f"twindone{i}"),
                cpus=1,
                state=JobState.COMPLETED,
                scheduler_target="host_f",
                scheduler_walltime_used="00:30:00",
                started_at="2026-06-05T00:00:00+00:00",
                finished_at="2026-06-05T09:00:00+00:00",
            ).write(queue / f"twindone000{i}.json")

        out = show_status("localhost", "twintarget01", queue_dir=queue)

        assert "queue position: 2 of 2 pending" in out
        assert "queue eta:    ~30m00s before dispatch turn" in out

    def test_running_job_has_no_position_line(self, tmp_path: Path) -> None:
        queue = _setup(tmp_path, state=JobState.RUNNING, pid=123,
                       started_at="2026-05-02T10:00:00+00:00")
        out = show_status("localhost", "abc", queue_dir=queue)
        assert "queue position" not in out


class TestPendingAdmissionReason:
    """v0.15.x paper-ops: explain local pending resource admission blocks."""

    def test_pending_status_shows_memory_admission_block(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import vq.status as status_mod

        monkeypatch.setattr(
            status_mod.capacity,
            "read_daemon_capacity",
            lambda **_kwargs: status_mod.capacity.DaemonCapacity(
                max_cpus=4,
                max_jobs=None,
                max_mem_mb=25_615,
                written_at="2026-07-01T06:00:00+00:00",
            ),
        )
        monkeypatch.setattr(
            status_mod.drain,
            "read_drain_state",
            lambda **_: None,
        )
        queue = _setup(
            tmp_path,
            state=JobState.PENDING,
            cpus=4,
            mem_mb=28_000,
        )

        out = show_status("localhost", "abc", queue_dir=queue)

        assert "state:        pending (over cap)" in out
        assert "queue eta:    unavailable (request exceeds configured" in out
        assert "queue eta:    ~" not in out
        assert "pending:      memory request exceeds configured cap" in out
        assert (
            "requested 28000 MB, cap is 25615 MB; cannot dispatch until the "
            "daemon cap changes or the job is resubmitted"
            in out
        )

    def test_pending_status_json_includes_admission_reason(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import vq.status as status_mod

        monkeypatch.setattr(
            status_mod.capacity,
            "read_daemon_capacity",
            lambda **_kwargs: status_mod.capacity.DaemonCapacity(
                max_cpus=4,
                max_jobs=None,
                max_mem_mb=25_615,
                written_at="2026-07-01T06:00:00+00:00",
            ),
        )
        monkeypatch.setattr(
            status_mod.drain,
            "read_drain_state",
            lambda **_: None,
        )
        queue = _setup(
            tmp_path,
            state=JobState.PENDING,
            cpus=4,
            mem_mb=28_000,
        )

        payload = json.loads(show_status_json("localhost", "abc", queue_dir=queue))

        assert payload["pending_over_capacity"] is True
        assert payload["configured_capacity_overages"] == [
            {
                "resource": "memory",
                "requested": 28_000,
                "limit": 25_615,
                "uses_default": False,
            }
        ]
        assert payload["pending_admission_reason"] == (
            "memory request exceeds configured cap: requested 28000 MB, cap "
            "is 25615 MB; cannot dispatch until the daemon cap changes or "
            "the job is resubmitted"
        )

    def test_pending_status_json_keeps_unknown_capacity_nullable(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            status_mod.capacity,
            "read_daemon_capacity",
            lambda **_kwargs: None,
        )
        queue = _setup(tmp_path, state=JobState.PENDING, cpus=64)

        payload = json.loads(show_status_json("localhost", "abc", queue_dir=queue))

        assert payload["pending_over_capacity"] is None
        assert payload["configured_capacity_overages"] == []

    def test_pending_status_json_keeps_proven_cpu_overage_with_partial_metadata(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mixed_version = status_mod.capacity.DaemonCapacity.model_validate(
            {
                "max_cpus": 4,
                "max_mem_mb": 4_000,
                "written_at": "2026-08-20T12:00:00+00:00",
            }
        )
        monkeypatch.setattr(
            status_mod.capacity,
            "read_daemon_capacity",
            lambda **_kwargs: mixed_version,
        )
        queue = _setup(
            tmp_path,
            state=JobState.PENDING,
            cpus=8,
            mem_mb=None,
        )

        payload = json.loads(show_status_json("localhost", "abc", queue_dir=queue))

        assert payload["pending_over_capacity"] is True
        assert payload["configured_capacity_overages"][0]["resource"] == "cpus"

    def test_pending_status_json_describes_update_accept_drain(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import vq.status as status_mod

        monkeypatch.setattr(
            status_mod.drain,
            "read_drain_state",
            lambda **_: status_mod.drain.DrainState(
                update_mode="accept",
                reason="fleet upgrade",
            ),
        )
        queue = _setup(tmp_path, state=JobState.PENDING)

        payload = json.loads(show_status_json("localhost", "abc", queue_dir=queue))

        assert payload["pending_admission_reason"] == (
            "paused for update; daemon is not dispatching, but new "
            "submissions are accepted for later: fleet upgrade"
        )

    def test_pending_status_json_describes_update_deny_drain(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import vq.status as status_mod

        monkeypatch.setattr(
            status_mod.drain,
            "read_drain_state",
            lambda **_: status_mod.drain.DrainState(
                update_mode="deny",
                reject_submits=True,
                reason="fleet upgrade",
            ),
        )
        queue = _setup(tmp_path, state=JobState.PENDING)

        payload = json.loads(show_status_json("localhost", "abc", queue_dir=queue))

        assert payload["pending_admission_reason"] == (
            "paused for update; daemon is not dispatching and new "
            "submissions are denied: fleet upgrade"
        )

    def test_pending_scheduler_status_json_describes_target_drain(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import vq.status as status_mod

        monkeypatch.setattr(
            status_mod.drain,
            "read_drain_state",
            lambda **_: status_mod.drain.DrainState(
                scheduler_hosts=["host_f"],
                reason="pbs_sched idle",
            ),
        )
        queue = _setup(
            tmp_path,
            state=JobState.PENDING,
            scheduler_target="host_f",
        )

        payload = json.loads(show_status_json("localhost", "abc", queue_dir=queue))

        assert payload["pending_over_capacity"] is None
        assert payload["pending_admission_reason"] == (
            "scheduler-target drain is active for host_f; daemon is not "
            "dispatching this scheduler lane: pbs_sched idle"
        )

    def test_scheduler_lease_reason_is_not_flattened_to_unrelated_cap(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import vq.status as status_mod

        legacy = status_mod.drain.DrainState(
            max_jobs=1,
            reason="unrelated local cap",
        )
        lease = status_mod.drain.SchedulerDrainLease(
            lease_id="host_f-rollout",
            scheduler_host="host_f",
            owner="fleet-rollout:test:host_f",
            reason="updating host_f runtimes",
        )
        effective = status_mod.drain.DrainState(
            max_jobs=1,
            scheduler_hosts=["host_f"],
            reason="unrelated local cap",
        )
        monkeypatch.setattr(
            status_mod.drain,
            "read_effective_drain_snapshot",
            lambda **_kwargs: (legacy, [lease], None, effective),
        )
        queue = _setup(
            tmp_path,
            state=JobState.PENDING,
            scheduler_target="host_f",
        )

        payload = json.loads(show_status_json("localhost", "abc", queue_dir=queue))
        reason = str(payload["pending_admission_reason"])

        assert "updating host_f runtimes" in reason
        assert "fleet-rollout:test:host_f" in reason
        assert "unrelated local cap" not in reason

    def test_pending_scheduler_status_shows_scheduler_cap_block(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import vq.status as status_mod

        monkeypatch.setattr(
            status_mod.capacity,
            "read_daemon_capacity",
            lambda **_kwargs: status_mod.capacity.DaemonCapacity(
                max_cpus=4,
                max_jobs=None,
                max_mem_mb=None,
                max_scheduler_jobs=1,
                written_at="2026-07-01T06:00:00+00:00",
            ),
        )
        monkeypatch.setattr(
            status_mod.drain,
            "read_drain_state",
            lambda **_: None,
        )
        queue = _setup(
            tmp_path,
            state=JobState.PENDING,
            scheduler_target="host_f",
        )
        JobSpec(
            id="twactive0001",
            command=["echo", "ok"],
            cwd=str(tmp_path / "ws" / "twactive0001"),
            cpus=1,
            state=JobState.RUNNING,
            scheduler_target="host_f",
            scheduler_state="running",
        ).write(queue / "twactive0001.json")

        out = show_status("localhost", "abc", queue_dir=queue)

        assert "pending:      scheduler job-count admission blocked" in out
        assert "1/1 scheduler jobs already active" in out

    def test_pending_scheduler_status_json_includes_scheduler_cap_block(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import vq.status as status_mod

        monkeypatch.setattr(
            status_mod.capacity,
            "read_daemon_capacity",
            lambda **_kwargs: status_mod.capacity.DaemonCapacity(
                max_cpus=4,
                max_jobs=None,
                max_mem_mb=None,
                max_scheduler_jobs=1,
                written_at="2026-07-01T06:00:00+00:00",
            ),
        )
        monkeypatch.setattr(
            status_mod.drain,
            "read_drain_state",
            lambda **_: None,
        )
        queue = _setup(
            tmp_path,
            state=JobState.PENDING,
            scheduler_target="host_f",
        )
        JobSpec(
            id="twactive0001",
            command=["echo", "ok"],
            cwd=str(tmp_path / "ws" / "twactive0001"),
            cpus=1,
            state=JobState.RUNNING,
            scheduler_target="host_f",
            scheduler_state="reattach_failed",
        ).write(queue / "twactive0001.json")

        payload = json.loads(show_status_json("localhost", "abc", queue_dir=queue))

        assert payload["pending_admission_reason"] == (
            "scheduler job-count admission blocked: "
            "1/1 scheduler jobs already active"
        )
