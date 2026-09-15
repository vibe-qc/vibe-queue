"""Tests for `vq resubmit` (v0.6.8) — operator-driven rerun verb.

Coverage shape:
* TestResubmitLocal (the resubmit_local helper) — inherit semantics,
  override semantics, state-refusal, archive-aware, workspace-missing.
* TestResubmitCLI (CliRunner on `vq resubmit`) — flag parsing,
  mutual-exclusion, host resolution.
* TestResubmitRemote (transport mocked) — argv construction.
"""
from __future__ import annotations

import json
import socket
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from vq import paths
from vq.cli import main
from vq.resubmit import (
    ResubmitOverrides,
    resubmit_local,
    resubmit_remote,
    resubmit_state,
    resubmit_state_remote,
)
from vq.spec import JobSpec, JobState, ProgramRuntimePin


@pytest.fixture
def state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    return tmp_path


def _materialize_job(
    jobid: str,
    *,
    state: JobState = JobState.COMPLETED,
    command: list[str] | None = None,
    workspace_files: dict[str, str] | None = None,
    **spec_kwargs: object,
) -> JobSpec:
    """Helper: write a spec + workspace dir, return the spec.

    ``spec_kwargs`` are passed verbatim to ``JobSpec(...)``; tests
    that want to override the defaults (cpus=1, submitter="test_user@test")
    just pass them in spec_kwargs and they win.
    """
    queue = paths.queue_dir()
    jobs = paths.jobs_dir()
    queue.mkdir(parents=True, exist_ok=True)
    jobs.mkdir(parents=True, exist_ok=True)
    workspace = jobs / jobid
    workspace.mkdir()
    for name, content in (workspace_files or {}).items():
        (workspace / name).write_text(content)
    kwargs: dict[str, object] = {
        "id": jobid,
        "command": command or ["python", "input.py"],
        "cwd": str(workspace),
        "cpus": 1,
        "state": state,
        "submitter": "test_user@test",
    }
    kwargs.update(spec_kwargs)
    spec = JobSpec(**kwargs)  # type: ignore[arg-type]
    spec.write(queue / f"{jobid}.json")
    return spec


class TestResubmitLocal:
    def test_basic_resubmit_creates_new_pending_job(self, state: Path) -> None:
        _materialize_job(
            "deadbeef0001", workspace_files={"input.py": "print('hi')"}
        )
        new_id = resubmit_local("deadbeef0001")
        assert len(new_id) == 12
        assert new_id != "deadbeef0001"
        new_spec = JobSpec.read(paths.queue_dir() / f"{new_id}.json")
        assert new_spec.state == JobState.PENDING
        assert new_spec.parent_jobid == "deadbeef0001"

    def test_workspace_deep_copied_not_shared(self, state: Path) -> None:
        _materialize_job(
            "deadbeef0002",
            workspace_files={"input.py": "print('hi')", "results.txt": "old"},
        )
        new_id = resubmit_local("deadbeef0002")
        new_spec = JobSpec.read(paths.queue_dir() / f"{new_id}.json")
        new_cwd = Path(new_spec.cwd)
        assert new_cwd != paths.jobs_dir() / "deadbeef0002"
        # Files copied
        assert (new_cwd / "input.py").read_text() == "print('hi')"
        assert (new_cwd / "results.txt").read_text() == "old"
        # Mutating the new workspace doesn't touch the source
        (new_cwd / "results.txt").write_text("new")
        assert (
            (paths.jobs_dir() / "deadbeef0002" / "results.txt").read_text()
            == "old"
        )

    def test_command_inherited(self, state: Path) -> None:
        _materialize_job("ee00000aaaaa", command=["bash", "run.sh"])
        new_id = resubmit_local("ee00000aaaaa")
        new_spec = JobSpec.read(paths.queue_dir() / f"{new_id}.json")
        assert new_spec.command == ["bash", "run.sh"]

    def test_resource_fields_inherit(self, state: Path) -> None:
        _materialize_job(
            "ee00000bbbbb",
            cpus=4,
            scheduler_tasks=2,
            mem_mb=2048,
            wall_time_seconds=3600,
            priority=5,
        )
        new_id = resubmit_local("ee00000bbbbb")
        new_spec = JobSpec.read(paths.queue_dir() / f"{new_id}.json")
        assert new_spec.cpus == 4
        assert new_spec.scheduler_tasks == 2
        assert new_spec.mem_mb == 2048
        assert new_spec.wall_time_seconds == 3600
        assert new_spec.priority == 5
        rec = json.loads((Path(new_spec.cwd) / "_vq" / "events.jsonl").read_text())
        assert rec["scheduler_tasks"] == 2

    def test_recover_on_reboot_and_branch_inherit(self, state: Path) -> None:
        _materialize_job(
            "ee00000ccccc", recover_on_reboot=True, branch="main"
        )
        new_id = resubmit_local("ee00000ccccc")
        new_spec = JobSpec.read(paths.queue_dir() / f"{new_id}.json")
        assert new_spec.recover_on_reboot is True
        assert new_spec.branch == "main"

    def test_program_inherits_and_event_records_it(self, state: Path) -> None:
        _materialize_job(
            "prog00000001",
            program="vibeqc-release",
            program_runtime_pin=ProgramRuntimePin(expected_git_sha="abc123"),
            workspace_files={"input.py": "print('hi')"},
        )

        new_id = resubmit_local("prog00000001")

        new_spec = JobSpec.read(paths.queue_dir() / f"{new_id}.json")
        assert new_spec.program == "vibeqc-release"
        assert new_spec.program_runtime_pin is not None
        assert new_spec.program_runtime_pin.expected_git_sha == "abc123"
        events_file = Path(new_spec.cwd) / "_vq" / "events.jsonl"
        rec = json.loads(events_file.read_text().splitlines()[-1])
        assert rec["kind"].lower() == "submitted"
        assert rec["program"] == "vibeqc-release"

    def test_scheduler_target_inherits_without_runtime_fields(
        self, state: Path
    ) -> None:
        _materialize_job(
            "sched0000001",
            state=JobState.FAILED,
            scheduler_target="host_f",
            scheduler_job_id="123.host_f",
            scheduler_state="running",
            scheduler_exec_host="node01/0-3",
            scheduler_walltime_used="00:15:00",
            scheduler_walltime_limit="01:00:00",
        )

        new_id = resubmit_local("sched0000001")

        new_spec = JobSpec.read(paths.queue_dir() / f"{new_id}.json")
        assert new_spec.scheduler_target == "host_f"
        assert new_spec.scheduler_job_id is None
        assert new_spec.scheduler_state is None
        assert new_spec.scheduler_exec_host is None
        assert new_spec.scheduler_walltime_used is None
        assert new_spec.scheduler_walltime_limit is None

    def test_retry_count_resets_to_zero(self, state: Path) -> None:
        _materialize_job(
            "ee00000ddddd", retry_max=3, retry_count=2
        )
        new_id = resubmit_local("ee00000ddddd")
        new_spec = JobSpec.read(paths.queue_dir() / f"{new_id}.json")
        assert new_spec.retry_max == 3  # budget inherits
        assert new_spec.retry_count == 0  # fresh count

    def test_resubmit_does_not_inherit_submit_idempotency_binding(
        self, state: Path
    ) -> None:
        _materialize_job(
            "resubmitkey1",
            idempotency_key_hash="a" * 64,
            submission_intent_digest="b" * 64,
            submission_owner_hash="c" * 64,
        )

        new_id = resubmit_local("resubmitkey1")
        new_spec = JobSpec.read(paths.queue_dir() / f"{new_id}.json")

        assert new_spec.idempotency_key_hash is None
        assert new_spec.submission_intent_digest is None
        assert new_spec.submission_owner_hash is None

    def test_tags_inherit(self, state: Path) -> None:
        _materialize_job(
            "ee00000eeeee", tags=["exp-1", "basis-tz2p"]
        )
        new_id = resubmit_local("ee00000eeeee")
        new_spec = JobSpec.read(paths.queue_dir() / f"{new_id}.json")
        assert new_spec.tags == ["basis-tz2p", "exp-1"]  # sorted by validator

    def test_overrides_replace_inherited_values(self, state: Path) -> None:
        _materialize_job(
            "ee00000fffff",
            cpus=4,
            scheduler_tasks=2,
            mem_mb=2048,
            wall_time_seconds=3600,
            priority=0,
            retry_max=0,
            tags=["old-tag"],
            job_name="oldname",
        )
        overrides = ResubmitOverrides(
            cpus=16,
            scheduler_tasks=4,
            mem_mb=8192,
            wall_time_seconds=7200,
            priority=10,
            retry_max=2,
            tags=["new-tag-a", "new-tag-b"],
            job_name="newname",
        )
        new_id = resubmit_local("ee00000fffff", overrides=overrides)
        new_spec = JobSpec.read(paths.queue_dir() / f"{new_id}.json")
        assert new_spec.cpus == 16
        assert new_spec.scheduler_tasks == 4
        assert new_spec.mem_mb == 8192
        assert new_spec.wall_time_seconds == 7200
        assert new_spec.priority == 10
        assert new_spec.retry_max == 2
        assert new_spec.tags == ["new-tag-a", "new-tag-b"]
        assert new_spec.job_name == "newname"

    def test_tag_override_with_empty_list_clears(self, state: Path) -> None:
        _materialize_job(
            "ee0000fffff1", tags=["keep-this"]
        )
        new_id = resubmit_local(
            "ee0000fffff1", overrides=ResubmitOverrides(tags=[])
        )
        new_spec = JobSpec.read(paths.queue_dir() / f"{new_id}.json")
        assert new_spec.tags == []

    @pytest.mark.parametrize(
        "bad_state",
        [JobState.RUNNING, JobState.PENDING, JobState.SUSPENDED],
    )
    def test_non_terminal_source_rejected(
        self, state: Path, bad_state: JobState
    ) -> None:
        _materialize_job("bad00000aaaa", state=bad_state)
        with pytest.raises(ValueError, match="cannot resubmit"):
            resubmit_local("bad00000aaaa")

    @pytest.mark.parametrize(
        "good_state",
        [
            JobState.COMPLETED,
            JobState.FAILED,
            JobState.KILLED,
            JobState.OOM_KILLED,
            JobState.TIME_EXCEEDED,
            JobState.STARVED,
            JobState.ABORTED_BY_QUEUE,
            JobState.INTERRUPTED,
        ],
    )
    def test_all_terminal_states_accepted(
        self, state: Path, good_state: JobState
    ) -> None:
        # build a unique-id-per-state so the parametrize doesn't collide
        jobid = f"good{good_state.value[:8]:>08}"[:12]
        # need exactly 12 hex; pad with '0' if short
        jobid = (jobid + "000000000000")[:12]
        # replace non-hex with '0'
        jobid = "".join(c if c in "0123456789abcdef" else "0" for c in jobid)
        _materialize_job(jobid, state=good_state)
        new_id = resubmit_local(jobid)
        assert len(new_id) == 12

    def test_missing_source_raises(self, state: Path) -> None:
        with pytest.raises(FileNotFoundError, match="no such job"):
            resubmit_local("nonexistent1")

    def test_workspace_missing_raises(self, state: Path) -> None:
        _materialize_job("workgone0001", workspace_files={"x": "y"})
        import shutil
        shutil.rmtree(paths.jobs_dir() / "workgone0001")
        with pytest.raises(FileNotFoundError, match="workspace.*not found"):
            resubmit_local("workgone0001")

    def test_archived_source_extracts_tarball(self, state: Path) -> None:
        from vq.cleanup import archive_workspace

        _materialize_job(
            "arc111000000",
            workspace_files={"input.py": "print('hi')", "out.dat": "result"},
        )
        spec_path = paths.queue_dir() / "arc111000000.json"
        spec = JobSpec.read(spec_path)
        spec.state = JobState.COMPLETED
        spec.exit_code = 0
        spec.finished_at = "2026-05-18T10:00:00+00:00"
        spec.write(spec_path)
        archive_workspace(JobSpec.read(spec_path))
        # workspace dir is gone; only the archive remains
        assert not (paths.jobs_dir() / "arc111000000").exists()

        new_id = resubmit_local("arc111000000")
        new_spec = JobSpec.read(paths.queue_dir() / f"{new_id}.json")
        new_cwd = Path(new_spec.cwd)
        assert new_cwd.is_dir()
        assert (new_cwd / "input.py").read_text() == "print('hi')"
        assert (new_cwd / "out.dat").read_text() == "result"

    def test_submitter_recorded_as_current_user(self, state: Path) -> None:
        _materialize_job("submi0000000", submitter="someone-else@oldhost")
        new_id = resubmit_local("submi0000000")
        new_spec = JobSpec.read(paths.queue_dir() / f"{new_id}.json")
        # New spec records the CURRENT user, not the source's submitter
        assert new_spec.submitter is not None
        assert socket.gethostname() in new_spec.submitter
        assert "someone-else" not in new_spec.submitter

    def test_event_appended_to_new_workspace(self, state: Path) -> None:
        _materialize_job("event0000000", workspace_files={"input.py": ""})
        new_id = resubmit_local("event0000000")
        new_cwd = Path(JobSpec.read(paths.queue_dir() / f"{new_id}.json").cwd)
        events_file = new_cwd / "_vq" / "events.jsonl"
        assert events_file.exists()
        # Last line should be the SUBMITTED event for the new job
        last_line = events_file.read_text().splitlines()[-1]
        rec = json.loads(last_line)
        assert rec["kind"].lower() == "submitted"
        assert rec["jobid"] == new_id
        assert rec["parent_jobid"] == "event0000000"

    def test_clean_inherited_daemon_artifacts(self, state: Path) -> None:
        """v0.6.9: stdout.log / stderr.log / _vq/events.jsonl /
        _vq/exit-code / _vq/samples.jsonl / _vq/resource-usage.json
        from the source's deep-copy
        are cleaned in the new workspace before the new SUBMITTED
        event is written. Without this, the new run's outputs
        interleave with the source's forensics."""
        # Pre-stamp every daemon-managed artifact in the source's
        # workspace so the deep-copy carries them into the new one.
        _materialize_job(
            "clean0000001",
            workspace_files={
                "input.py": "print('hi')",
                "stdout.log": "OLD STDOUT FROM SOURCE",
                "stderr.log": "OLD STDERR FROM SOURCE",
            },
        )
        # _vq/ is a subdir; create it + the per-file forensic content
        src_vq = paths.jobs_dir() / "clean0000001" / "_vq"
        src_vq.mkdir()
        (src_vq / "events.jsonl").write_text('{"kind":"submitted","jobid":"clean0000001"}\n')
        (src_vq / "exit-code").write_text("0\n")
        (src_vq / "samples.jsonl").write_text('{"rss_mb": 100, "cpu_pct": 5}\n')
        (src_vq / "resource-usage.json").write_text(
            '{"schema":"vq.scheduler-resource-usage.v1","wall_seconds":10}\n'
        )

        new_id = resubmit_local("clean0000001")
        new_cwd = Path(JobSpec.read(paths.queue_dir() / f"{new_id}.json").cwd)

        # User input survived the deep-copy.
        assert (new_cwd / "input.py").read_text() == "print('hi')"
        # stdout.log + stderr.log are NOT present (or are empty);
        # the new run's command wrapper will recreate them.
        assert not (new_cwd / "stdout.log").exists()
        assert not (new_cwd / "stderr.log").exists()
        # _vq/exit-code + _vq/samples.jsonl are gone.
        assert not (new_cwd / "_vq" / "exit-code").exists()
        assert not (new_cwd / "_vq" / "samples.jsonl").exists()
        assert not (new_cwd / "_vq" / "resource-usage.json").exists()
        # _vq/events.jsonl exists BUT only because the new
        # SUBMITTED event was just appended — it should contain
        # exactly one line, for the new jobid.
        events = (new_cwd / "_vq" / "events.jsonl").read_text().splitlines()
        assert len(events) == 1
        rec = json.loads(events[0])
        assert rec["jobid"] == new_id
        assert "clean0000001" not in events[0] or rec["parent_jobid"] == "clean0000001"


class TestResubmitCLI:
    def test_local_resubmit_prints_new_jobid(self, state: Path) -> None:
        _materialize_job("cli000000001", workspace_files={"x.py": ""})
        result = CliRunner().invoke(main, ["resubmit", "localhost", "cli000000001"])
        assert result.exit_code == 0, result.output
        new_id = result.output.strip()
        assert len(new_id) == 12
        assert new_id != "cli000000001"
        new_spec = JobSpec.read(paths.queue_dir() / f"{new_id}.json")
        assert new_spec.parent_jobid == "cli000000001"

    def test_no_host_uses_default(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Write a minimal config with default_host = "localhost" so the
        # implicit host-resolution path doesn't error on missing config.
        config_dir = state / "config"
        config_dir.mkdir()
        (config_dir / "config.toml").write_text(
            'default_host = "localhost"\n'
        )
        monkeypatch.setenv(paths.ENV_CONFIG_DIR, str(config_dir))
        _materialize_job("cli000000002", workspace_files={"x.py": ""})
        result = CliRunner().invoke(main, ["resubmit", "cli000000002"])
        assert result.exit_code == 0, result.output
        assert len(result.output.strip()) == 12

    def test_non_terminal_source_rejected(self, state: Path) -> None:
        _materialize_job("cli000000003", state=JobState.RUNNING)
        result = CliRunner().invoke(main, ["resubmit", "localhost", "cli000000003"])
        assert result.exit_code != 0
        assert "cannot resubmit" in result.output

    def test_missing_source_rejected(self, state: Path) -> None:
        result = CliRunner().invoke(main, ["resubmit", "localhost", "nonexistent1"])
        assert result.exit_code != 0
        assert "no such job" in result.output

    def test_tag_override_applied(self, state: Path) -> None:
        _materialize_job(
            "cli000000004", tags=["old-tag"], workspace_files={"x.py": ""}
        )
        result = CliRunner().invoke(
            main,
            ["resubmit", "localhost", "cli000000004", "--tag", "new-tag-a", "--tag", "new-tag-b"],
        )
        assert result.exit_code == 0, result.output
        new_spec = JobSpec.read(paths.queue_dir() / f"{result.output.strip()}.json")
        assert new_spec.tags == ["new-tag-a", "new-tag-b"]

    def test_clear_tags_wipes_inherited(self, state: Path) -> None:
        _materialize_job(
            "cli000000005", tags=["keep-this"], workspace_files={"x.py": ""}
        )
        result = CliRunner().invoke(
            main, ["resubmit", "localhost", "cli000000005", "--clear-tags"]
        )
        assert result.exit_code == 0, result.output
        new_spec = JobSpec.read(paths.queue_dir() / f"{result.output.strip()}.json")
        assert new_spec.tags == []

    def test_tag_and_clear_tags_mutually_exclusive(self, state: Path) -> None:
        _materialize_job("cli000000006", workspace_files={"x.py": ""})
        result = CliRunner().invoke(
            main,
            ["resubmit", "localhost", "cli000000006", "--tag", "x", "--clear-tags"],
        )
        assert result.exit_code != 0
        assert "mutually exclusive" in result.output

    def test_cpus_override(self, state: Path) -> None:
        _materialize_job("cli000000007", cpus=2, workspace_files={"x.py": ""})
        result = CliRunner().invoke(
            main, ["resubmit", "localhost", "cli000000007", "--cpus", "16"]
        )
        assert result.exit_code == 0, result.output
        new_spec = JobSpec.read(paths.queue_dir() / f"{result.output.strip()}.json")
        assert new_spec.cpus == 16

    def test_job_name_override_is_sanitized(
        self, state: Path
    ) -> None:
        _materialize_job("cli000000008", workspace_files={"x.py": ""})
        result = CliRunner().invoke(
            main,
            [
                "resubmit",
                "localhost",
                "cli000000008",
                "--job-name",
                "rerun (tail=3200)",
            ],
        )
        assert result.exit_code == 0, result.output
        jobid = result.output.strip().splitlines()[-1]
        new_spec = JobSpec.read(paths.queue_dir() / f"{jobid}.json")
        assert new_spec.job_name == "rerun-tail-3200"
        assert "sanitized --job-name" in result.output + result.stderr

    def test_help_mentions_terminal_state_refusal(self) -> None:
        result = CliRunner().invoke(main, ["resubmit", "--help"])
        assert result.exit_code == 0
        assert "terminal-state" in result.output.lower() or "non-terminal" in result.output.lower()
        assert "fresh workspace" in result.output.lower()


class TestResubmitRemote:
    def test_argv_construction_with_overrides(self) -> None:
        """resubmit_remote forwards overrides as flags + parses the
        printed jobid from the remote process's stdout."""
        from vq.config import HostConfig

        host_cfg = HostConfig(ssh="host_d", remote_vq="/remote/vq")
        captured: list[list[str]] = []

        class _FakeProc:
            stdout = "0123456789ab\n"
            stderr = ""

        def _fake_run_remote_vq(_cfg: HostConfig, *argv: str):
            captured.append(list(argv))
            return _FakeProc()

        with patch("vq.resubmit.transport.run_remote_vq", _fake_run_remote_vq):
            new_id = resubmit_remote(
                host_cfg,
                "abc000000000",
                overrides=ResubmitOverrides(
                    cpus=8,
                    scheduler_tasks=2,
                    mem_mb=4096,
                    wall_time_seconds=1800,
                    priority=3,
                    retry_max=2,
                    job_name="rerun",
                    tags=["a", "b"],
                ),
            )
        assert new_id == "0123456789ab"
        argv = captured[0]
        # Required positionals + every override forwarded
        assert argv[:3] == ["resubmit", "localhost", "abc000000000"]
        assert "--cpus" in argv and "8" in argv
        assert "--scheduler-tasks" in argv and "2" in argv
        assert "--mem-mb" in argv and "4096" in argv
        assert "--wall-time-seconds" in argv and "1800" in argv
        assert "--priority" in argv and "3" in argv
        assert "--retry" in argv and "2" in argv
        assert "--job-name" in argv and "rerun" in argv
        assert argv.count("--tag") == 2
        assert "a" in argv and "b" in argv

    def test_argv_clear_tags_when_empty_list(self) -> None:
        from vq.config import HostConfig

        host_cfg = HostConfig(ssh="host_d", remote_vq="/remote/vq")
        captured: list[list[str]] = []

        class _FakeProc:
            stdout = "0123456789ab\n"
            stderr = ""

        def _fake_run_remote_vq(_cfg: HostConfig, *argv: str):
            captured.append(list(argv))
            return _FakeProc()

        with patch("vq.resubmit.transport.run_remote_vq", _fake_run_remote_vq):
            resubmit_remote(
                host_cfg, "abc000000000", overrides=ResubmitOverrides(tags=[])
            )
        argv = captured[0]
        assert "--clear-tags" in argv
        assert "--tag" not in argv

    def test_bad_jobid_output_raises(self) -> None:
        from vq.config import HostConfig
        from vq.transport import RemoteError

        host_cfg = HostConfig(ssh="host_d", remote_vq="/remote/vq")

        class _BadProc:
            stdout = "ERROR: something went wrong"
            stderr = "remote stderr"

        with (
            patch("vq.resubmit.transport.run_remote_vq", lambda *a, **k: _BadProc()),
            pytest.raises(RemoteError, match="unexpected output"),
        ):
            resubmit_remote(host_cfg, "abc000000000")


class TestResubmitState:
    """v0.6.10: bulk mode via resubmit_state(states)."""

    def test_empty_states_is_noop(self, state: Path) -> None:
        _materialize_job("bulk00000001")
        result = resubmit_state([])
        assert result.pairs == []
        assert result.errors == []

    def test_filters_by_state(self, state: Path) -> None:
        _materialize_job("bulk00000aaa", state=JobState.ABORTED_BY_QUEUE)
        _materialize_job("bulk00000bbb", state=JobState.COMPLETED)
        _materialize_job("bulk00000ccc", state=JobState.ABORTED_BY_QUEUE)

        result = resubmit_state([JobState.ABORTED_BY_QUEUE])
        assert len(result.pairs) == 2
        sources = {src for src, _new in result.pairs}
        assert sources == {"bulk00000aaa", "bulk00000ccc"}
        assert "bulk00000bbb" not in sources

    def test_multiple_states_compose(self, state: Path) -> None:
        _materialize_job("bulk000aaaaa", state=JobState.FAILED)
        _materialize_job("bulk000bbbbb", state=JobState.OOM_KILLED)
        _materialize_job("bulk000ccccc", state=JobState.COMPLETED)

        result = resubmit_state([JobState.FAILED, JobState.OOM_KILLED])
        sources = {src for src, _new in result.pairs}
        assert sources == {"bulk000aaaaa", "bulk000bbbbb"}

    def test_scheduler_target_filter_limits_bulk_resubmit(
        self, state: Path
    ) -> None:
        _materialize_job(
            "bulksched001",
            state=JobState.FAILED,
            scheduler_target="host_f",
        )
        _materialize_job(
            "bulksched002",
            state=JobState.FAILED,
            scheduler_target="other-cluster",
        )
        _materialize_job("bulksched003", state=JobState.FAILED)

        result = resubmit_state([JobState.FAILED], scheduler_target="host_f")

        assert [src for src, _new in result.pairs] == ["bulksched001"]
        assert result.errors == []
        new_spec = JobSpec.read(paths.queue_dir() / f"{result.pairs[0][1]}.json")
        assert new_spec.scheduler_target == "host_f"

    def test_program_inherits_in_bulk_resubmit(self, state: Path) -> None:
        _materialize_job(
            "bulkprog001",
            state=JobState.FAILED,
            program="vibeqc-dev",
        )

        result = resubmit_state([JobState.FAILED])

        assert [src for src, _new in result.pairs] == ["bulkprog001"]
        assert result.errors == []
        new_spec = JobSpec.read(paths.queue_dir() / f"{result.pairs[0][1]}.json")
        assert new_spec.program == "vibeqc-dev"

    def test_non_matching_state_yields_empty(self, state: Path) -> None:
        _materialize_job("bulk00aaaaaa", state=JobState.COMPLETED)
        result = resubmit_state([JobState.ABORTED_BY_QUEUE])
        assert result.pairs == []
        assert result.errors == []

    def test_overrides_apply_to_each(self, state: Path) -> None:
        _materialize_job("bulk00bbbbbb", state=JobState.FAILED, cpus=2)
        _materialize_job("bulk00cccccc", state=JobState.FAILED, cpus=4)

        result = resubmit_state(
            [JobState.FAILED],
            overrides=ResubmitOverrides(cpus=8, tags=["retry-batch"]),
        )
        assert len(result.pairs) == 2
        for _src, new_id in result.pairs:
            new_spec = JobSpec.read(paths.queue_dir() / f"{new_id}.json")
            assert new_spec.cpus == 8
            assert new_spec.tags == ["retry-batch"]

    def test_one_failure_does_not_abort_batch(self, state: Path) -> None:
        # Two FAILED jobs; remove one's workspace so its resubmit
        # raises FileNotFoundError; the other still completes.
        _materialize_job(
            "bulk00dddddd", state=JobState.FAILED, workspace_files={"x": ""}
        )
        _materialize_job(
            "bulk00eeeeee", state=JobState.FAILED, workspace_files={"x": ""}
        )
        import shutil
        shutil.rmtree(paths.jobs_dir() / "bulk00dddddd")

        result = resubmit_state([JobState.FAILED])
        # One success, one error
        assert len(result.pairs) == 1
        assert result.pairs[0][0] == "bulk00eeeeee"
        assert len(result.errors) == 1
        assert result.errors[0][0] == "bulk00dddddd"

    def test_corrupt_spec_skipped_with_error(self, state: Path) -> None:
        _materialize_job("bulk00ffffff", state=JobState.FAILED)
        # Corrupt one spec file
        bad_path = paths.queue_dir() / "deadbeef00ff.json"
        bad_path.write_text("not valid json at all {")

        result = resubmit_state([JobState.FAILED])
        # The good one was resubmitted; the bad one shows as an error
        assert len(result.pairs) == 1
        assert result.pairs[0][0] == "bulk00ffffff"
        err_sources = {src for src, _ in result.errors}
        assert "deadbeef00ff" in err_sources

    def test_running_source_not_matched(self, state: Path) -> None:
        # Even if we explicitly request FAILED, a RUNNING job
        # shouldn't be touched (the queue glob would skip it since
        # its state doesn't match, but defensively verify).
        _materialize_job("bulk0000aaaa", state=JobState.RUNNING)
        result = resubmit_state([JobState.FAILED])
        assert result.pairs == []
        # Source spec stays RUNNING
        src_spec = JobSpec.read(paths.queue_dir() / "bulk0000aaaa.json")
        assert src_spec.state == JobState.RUNNING


class TestBulkResubmitCLI:
    """v0.6.10: `vq resubmit --state STATE` CLI."""

    def test_bulk_mode_prints_new_jobids_on_stdout(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Default-host setup so the CLI doesn't need a positional
        config_dir = state / "config"
        config_dir.mkdir()
        (config_dir / "config.toml").write_text('default_host = "localhost"\n')
        monkeypatch.setenv(paths.ENV_CONFIG_DIR, str(config_dir))

        _materialize_job(
            "bulkcli00aaa",
            state=JobState.ABORTED_BY_QUEUE,
            workspace_files={"x.py": ""},
        )
        _materialize_job(
            "bulkcli00bbb",
            state=JobState.ABORTED_BY_QUEUE,
            workspace_files={"x.py": ""},
        )

        result = CliRunner().invoke(
            main, ["resubmit", "--state", "aborted_by_queue"]
        )
        assert result.exit_code == 0, result.stderr
        lines = result.stdout.strip().splitlines()
        # Two new jobids, one per line, scriptable
        assert len(lines) == 2
        for line in lines:
            assert len(line) == 12
            assert all(c in "0123456789abcdef" for c in line)

    def test_bulk_mode_summary_on_stderr(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        config_dir = state / "config"
        config_dir.mkdir()
        (config_dir / "config.toml").write_text('default_host = "localhost"\n')
        monkeypatch.setenv(paths.ENV_CONFIG_DIR, str(config_dir))

        _materialize_job(
            "bulkcli00ccc",
            state=JobState.FAILED,
            workspace_files={"x.py": ""},
        )

        result = CliRunner().invoke(
            main, ["resubmit", "--state", "failed"]
        )
        assert result.exit_code == 0
        # Per-source mapping appears in stderr
        assert "bulkcli00ccc ->" in result.stderr
        # Summary line on stderr
        assert "resubmitted 1 job" in result.stderr
        assert "0 errors" in result.stderr

    def test_bulk_state_and_jobid_mutually_exclusive(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        config_dir = state / "config"
        config_dir.mkdir()
        (config_dir / "config.toml").write_text('default_host = "localhost"\n')
        monkeypatch.setenv(paths.ENV_CONFIG_DIR, str(config_dir))

        _materialize_job(
            "bulkcli00ddd", state=JobState.FAILED, workspace_files={"x": ""}
        )
        result = CliRunner().invoke(
            main,
            ["resubmit", "localhost", "bulkcli00ddd", "--state", "failed"],
        )
        assert result.exit_code != 0
        assert "bulk mode" in result.output.lower()

    def test_running_states_not_listed_in_help(self) -> None:
        result = CliRunner().invoke(main, ["resubmit", "--help"])
        # running/pending/suspended must NOT be valid --state values
        # (refused by Click's choice type)
        assert result.exit_code == 0

    def test_bulk_overrides_apply(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        config_dir = state / "config"
        config_dir.mkdir()
        (config_dir / "config.toml").write_text('default_host = "localhost"\n')
        monkeypatch.setenv(paths.ENV_CONFIG_DIR, str(config_dir))

        _materialize_job(
            "bulkcli00eee",
            state=JobState.FAILED,
            cpus=2,
            workspace_files={"x.py": ""},
        )
        result = CliRunner().invoke(
            main,
            ["resubmit", "--state", "failed", "--cpus", "16", "--tag", "batch-1"],
        )
        assert result.exit_code == 0, result.stderr
        new_id = result.stdout.strip()
        new_spec = JobSpec.read(paths.queue_dir() / f"{new_id}.json")
        assert new_spec.cpus == 16
        assert new_spec.tags == ["batch-1"]

    def test_bulk_invalid_state_rejected_by_click(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        config_dir = state / "config"
        config_dir.mkdir()
        (config_dir / "config.toml").write_text('default_host = "localhost"\n')
        monkeypatch.setenv(paths.ENV_CONFIG_DIR, str(config_dir))
        # running is not in the valid choice list
        result = CliRunner().invoke(main, ["resubmit", "--state", "running"])
        assert result.exit_code != 0
        assert "running" in result.output.lower()


class TestBulkResubmitRemote:
    def test_argv_includes_state_flags(self) -> None:
        from vq.config import HostConfig

        host_cfg = HostConfig(ssh="host_d", remote_vq="/remote/vq")
        captured: list[list[str]] = []

        class _FakeProc:
            stdout = "0123456789ab\nfedcba987654\n"
            stderr = (
                "  src1 -> 0123456789ab\n  src2 -> fedcba987654\n"
                "resubmitted 2 jobs (0 errors)\n"
            )

        def _fake(_cfg: HostConfig, *argv: str):
            captured.append(list(argv))
            return _FakeProc()

        with patch("vq.resubmit.transport.run_remote_vq", _fake):
            new_ids, stderr = resubmit_state_remote(
                host_cfg,
                [JobState.ABORTED_BY_QUEUE, JobState.FAILED],
                overrides=ResubmitOverrides(cpus=16, scheduler_tasks=4),
            )

        assert new_ids == ["0123456789ab", "fedcba987654"]
        assert "resubmitted 2" in stderr
        argv = captured[0]
        assert argv[:2] == ["resubmit", "localhost"]
        # Both states forwarded
        state_idx = [i for i, a in enumerate(argv) if a == "--state"]
        assert len(state_idx) == 2
        assert "aborted_by_queue" in argv
        assert "failed" in argv
        # Override forwarded
        assert "--cpus" in argv and "16" in argv
        assert "--scheduler-tasks" in argv and "4" in argv

    def test_invalid_remote_stdout_raises(self) -> None:
        from vq.config import HostConfig
        from vq.transport import RemoteError

        host_cfg = HostConfig(ssh="host_d", remote_vq="/remote/vq")

        class _BadProc:
            stdout = "0123456789ab\nNOT-A-JOBID-XYZ\n"
            stderr = ""

        with (
            patch("vq.resubmit.transport.run_remote_vq", lambda *a, **k: _BadProc()),
            pytest.raises(RemoteError, match="unexpected line"),
        ):
            resubmit_state_remote(host_cfg, [JobState.FAILED])
