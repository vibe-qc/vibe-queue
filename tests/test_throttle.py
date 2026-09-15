"""Tests for vq throttle: soft CPU-priority control via cgroup CPUWeight.

Most tests mock cgroup.set_cpu_weight so they run on macOS too (where
systemd-run is unavailable). One Linux-only test exercises the real
cgroup.available() probe to confirm the no-cgroup error path actually
fires when cgroup isn't enforced.
"""
from __future__ import annotations

import json
import platform
import subprocess
from datetime import UTC
from pathlib import Path

import pytest
from click.testing import CliRunner
from pydantic import ValidationError

from vq import cgroup, config, paths
from vq.cli import main
from vq.spec import JobSpec, JobState
from vq.throttle import (
    DEFAULT_CPU_WEIGHT,
    MAX_CPU_WEIGHT,
    MIN_CPU_WEIGHT,
    ThrottleError,
    restore_all,
    restore_job,
    throttle_all,
    throttle_job,
)

LINUX = platform.system() == "Linux"


@pytest.fixture
def state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    paths.jobs_dir().mkdir(parents=True, exist_ok=True)
    return tmp_path


def _write_spec(
    jobid: str,
    state_val: JobState = JobState.RUNNING,
    *,
    pgid: int | None = 12345,
) -> JobSpec:
    """Write a minimal spec to the queue dir; return it.

    ``pgid`` defaults to 12345 (a fake "looks valid but doesn't exist
    as a process group" value, so renice calls against it return
    ESRCH). Pass ``pgid=None`` to test the pre-v0.3-spec path through
    throttle's fallback."""
    workspace = paths.jobs_dir() / jobid
    workspace.mkdir(parents=True, exist_ok=True)
    spec = JobSpec(
        id=jobid,
        command=["sleep", "30"],
        cwd=str(workspace),
        cpus=1,
        state=state_val,
        pid=12345 if pgid is not None else None,
        pgid=pgid,
        started_at="2026-05-09T12:00:00+00:00",
    )
    spec.write(paths.spec_path(jobid))
    return spec


def _mock_cgroup_available(monkeypatch: pytest.MonkeyPatch, *, ok: bool) -> list:
    """Patch cgroup.available() and cgroup.set_cpu_weight() so we can run
    on macOS. Returns a list that will accumulate (scope, weight) tuples
    for the assertion calls."""
    monkeypatch.setattr(cgroup, "available", lambda: ok)
    calls: list[tuple[str, int]] = []
    def fake_set_cpu_weight(scope: str, weight: int) -> bool:
        calls.append((scope, weight))
        return ok
    monkeypatch.setattr(cgroup, "set_cpu_weight", fake_set_cpu_weight)
    return calls


class TestThrottleJob:
    def test_throttle_running_job_calls_cgroup_with_correct_scope(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls = _mock_cgroup_available(monkeypatch, ok=True)
        _write_spec("abcd00000001")
        msg = throttle_job("localhost", "abcd00000001", 20)
        assert "abcd00000001" in msg
        assert "CPUWeight=20" in msg
        assert calls == [("vq-job-abcd00000001.scope", 20)]

    def test_restore_job_uses_default_weight(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls = _mock_cgroup_available(monkeypatch, ok=True)
        _write_spec("abcd00000002")
        msg = restore_job("localhost", "abcd00000002")
        assert "restored" in msg
        assert f"CPUWeight={DEFAULT_CPU_WEIGHT}" in msg
        assert calls == [("vq-job-abcd00000002.scope", DEFAULT_CPU_WEIGHT)]

    def test_throttle_non_running_state_errors(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _mock_cgroup_available(monkeypatch, ok=True)
        _write_spec("abcd00000003", state_val=JobState.SUSPENDED)
        with pytest.raises(ThrottleError, match="state suspended"):
            throttle_job("localhost", "abcd00000003", 20)

    def test_throttle_completed_job_errors(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _mock_cgroup_available(monkeypatch, ok=True)
        _write_spec("abcd00000004", state_val=JobState.COMPLETED)
        with pytest.raises(ThrottleError, match="state completed"):
            throttle_job("localhost", "abcd00000004", 20)

    def test_throttle_unknown_jobid_raises_file_not_found(
        self, state: Path
    ) -> None:
        with pytest.raises(FileNotFoundError, match="no such job"):
            throttle_job("localhost", "no_such_job0", 20)

    def test_throttle_weight_below_range_errors(
        self, state: Path
    ) -> None:
        _write_spec("abcd00000005")
        with pytest.raises(ThrottleError, match="out of range"):
            throttle_job("localhost", "abcd00000005", 0)

    def test_throttle_weight_above_range_errors(
        self, state: Path
    ) -> None:
        _write_spec("abcd00000006")
        with pytest.raises(ThrottleError, match="out of range"):
            throttle_job("localhost", "abcd00000006", MAX_CPU_WEIGHT + 1)

    def test_throttle_no_cgroup_falls_back_to_renice(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """v0.5.21: non-cgroup hosts use renice fallback instead of
        raising. We mock the renice subprocess call to fail
        deterministically (rather than relying on the fake spec's
        pgid=12345 happening to be an unallocated process group on
        the host running the test — it is not guaranteed to be, and
        real-PID collisions have caused spurious failures), so we
        get a fallback-failed error rather than the old "not active"
        error. Real-pgid success coverage lives in TestRenicePgid and
        TestThrottleJobReniceFallback below."""
        from vq import throttle as throttle_mod
        monkeypatch.setattr(cgroup, "available", lambda: False)
        monkeypatch.setattr(
            throttle_mod.subprocess, "run",
            lambda cmd, **kw: subprocess.CompletedProcess(
                args=cmd, returncode=1, stdout="",
                stderr="renice: 12345: no such process\n",
            ),
        )
        _write_spec("abcd00000007")
        with pytest.raises(ThrottleError, match="renice fallback failed"):
            throttle_job("localhost", "abcd00000007", 20)

    def test_throttle_no_cgroup_no_pgid_errors(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Pre-v0.3 spec without pgid + cgroup off = no path forward."""
        monkeypatch.setattr(cgroup, "available", lambda: False)
        _write_spec("abcd0000000a", pgid=None)
        with pytest.raises(ThrottleError, match="no pgid"):
            throttle_job("localhost", "abcd0000000a", 20)

    def test_throttle_cgroup_set_failure_errors(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When systemctl set-property exits non-zero (e.g. scope is gone),
        set_cpu_weight returns False and throttle_job raises ThrottleError
        with the scope name in the message."""
        monkeypatch.setattr(cgroup, "available", lambda: True)
        monkeypatch.setattr(cgroup, "set_cpu_weight", lambda scope, w: False)
        _write_spec("abcd00000008")
        with pytest.raises(ThrottleError, match="failed to set CPUWeight"):
            throttle_job("localhost", "abcd00000008", 20)

    def test_throttle_remote_host_raises_not_implemented(self) -> None:
        with pytest.raises(NotImplementedError, match="CLI must dispatch via SSH"):
            throttle_job("fake-remote-test", "abcd00000009", 20)

    def test_throttle_records_event_in_jsonl(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The state-transition event log captures the throttle for forensics."""
        import json
        _mock_cgroup_available(monkeypatch, ok=True)
        _write_spec("abcd00000010")
        throttle_job("localhost", "abcd00000010", 20)
        events_path = paths.jobs_dir() / "abcd00000010" / "_vq" / "events.jsonl"
        assert events_path.exists()
        events = [json.loads(line) for line in events_path.read_text().splitlines()]
        throttle_event = next(
            (e for e in events if "throttle" in e.get("reason", "")), None
        )
        assert throttle_event is not None
        assert "CPUWeight=20" in throttle_event["reason"]


class TestThrottleAll:
    def test_throttle_all_running_jobs(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls = _mock_cgroup_available(monkeypatch, ok=True)
        for i in range(3):
            _write_spec(f"aaaa{i:08d}")
        msg = throttle_all("localhost", 20)
        assert "throttled 3 jobs to CPUWeight=20" in msg
        assert len(calls) == 3
        assert all(w == 20 for _, w in calls)

    def test_throttle_all_skips_suspended(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _mock_cgroup_available(monkeypatch, ok=True)
        _write_spec("running00001")
        _write_spec("suspended001", state_val=JobState.SUSPENDED)
        msg = throttle_all("localhost", 20)
        assert "throttled 1 job to CPUWeight=20" in msg
        assert "1 SUSPENDED skipped" in msg

    def test_throttle_all_skips_terminal(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _mock_cgroup_available(monkeypatch, ok=True)
        _write_spec("running00001")
        _write_spec("completed001", state_val=JobState.COMPLETED)
        _write_spec("failed000001", state_val=JobState.FAILED)
        msg = throttle_all("localhost", 20)
        assert "throttled 1 job to CPUWeight=20" in msg
        assert "2 not running" in msg

    def test_throttle_all_empty_queue(self, state: Path) -> None:
        msg = throttle_all("localhost", 20)
        assert "throttled 0 jobs to CPUWeight=20" in msg

    def test_restore_all_uses_default_and_message_reads_naturally(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls = _mock_cgroup_available(monkeypatch, ok=True)
        _write_spec("aaaa00000001")
        _write_spec("aaaa00000002")
        msg = restore_all("localhost")
        assert "restored" in msg
        assert "to default CPUWeight" in msg
        assert len(calls) == 2
        assert all(w == DEFAULT_CPU_WEIGHT for _, w in calls)

    def test_throttle_all_remote_raises_not_implemented(self) -> None:
        with pytest.raises(NotImplementedError, match="CLI must dispatch via SSH"):
            throttle_all("fake-remote-test", 20)


class TestThrottleCLI:
    def test_cli_throttle_job_with_default_host(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _mock_cgroup_available(monkeypatch, ok=True)
        _write_spec("clii00000001")
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
            '[hosts.localhost]\nssh = "localhost"\n'
        )
        result = CliRunner().invoke(
            main, ["throttle", "clii00000001", "--weight", "30"]
        )
        assert result.exit_code == 0, result.output
        assert "throttled job clii00000001 to CPUWeight=30" in result.output

    def test_cli_throttle_with_restore_flag(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _mock_cgroup_available(monkeypatch, ok=True)
        _write_spec("clii00000002")
        result = CliRunner().invoke(
            main, ["throttle", "localhost", "clii00000002", "--restore"]
        )
        assert result.exit_code == 0, result.output
        assert "restored job clii00000002" in result.output
        assert f"CPUWeight={DEFAULT_CPU_WEIGHT}" in result.output

    def test_cli_throttle_all_with_weight(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _mock_cgroup_available(monkeypatch, ok=True)
        _write_spec("clii00000003")
        _write_spec("clii00000004")
        result = CliRunner().invoke(
            main, ["throttle", "localhost", "--all", "--weight", "20"]
        )
        assert result.exit_code == 0, result.output
        assert "throttled 2 jobs to CPUWeight=20" in result.output

    def test_cli_throttle_all_with_restore(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _mock_cgroup_available(monkeypatch, ok=True)
        _write_spec("clii00000005")
        result = CliRunner().invoke(
            main, ["throttle", "localhost", "--all", "--restore"]
        )
        assert result.exit_code == 0, result.output
        assert "restored 1 job to default CPUWeight" in result.output

    def test_cli_weight_and_restore_mutex(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _mock_cgroup_available(monkeypatch, ok=True)
        result = CliRunner().invoke(
            main,
            ["throttle", "localhost", "abcd00000001", "--weight", "20", "--restore"],
        )
        assert result.exit_code != 0
        assert "mutually exclusive" in result.output

    def test_cli_neither_weight_nor_restore_errors(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _mock_cgroup_available(monkeypatch, ok=True)
        result = CliRunner().invoke(
            main, ["throttle", "localhost", "abcd00000001"]
        )
        assert result.exit_code != 0
        assert "--weight" in result.output or "--restore" in result.output

    def test_cli_all_with_jobid_rejected(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _mock_cgroup_available(monkeypatch, ok=True)
        result = CliRunner().invoke(
            main,
            ["throttle", "localhost", "abcd00000001", "--all", "--weight", "20"],
        )
        assert result.exit_code != 0
        assert "mutually exclusive" in result.output

    def test_cli_missing_jobid_helpful_error(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _mock_cgroup_available(monkeypatch, ok=True)
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
            '[hosts.localhost]\nssh = "localhost"\n'
        )
        result = CliRunner().invoke(main, ["throttle", "--weight", "20"])
        assert result.exit_code != 0
        assert "missing JOBID" in result.output

    def test_cli_throttle_help_shows_forms(self) -> None:
        result = CliRunner().invoke(main, ["throttle", "--help"])
        assert result.exit_code == 0
        for form_token in ("--weight", "--restore", "--all"):
            assert form_token in result.output


class TestPersistentThrottleState:
    """v0.5.15: throttle.json daemon-state for "new jobs inherit
    the throttled weight"."""

    def test_default_state_is_no_file(self, state: Path) -> None:
        from vq.throttle import read_throttle_state
        assert read_throttle_state() is None

    def test_write_then_read_roundtrip(self, state: Path) -> None:
        from vq.throttle import (
            ThrottleState,
            read_throttle_state,
            write_throttle_state,
        )
        write_throttle_state(ThrottleState(weight=20, reason="testing"))
        back = read_throttle_state()
        assert back is not None
        assert back.weight == 20
        assert back.reason == "testing"
        assert back.set_at  # auto-stamped

    @pytest.mark.parametrize(
        "weight", [MIN_CPU_WEIGHT - 1, -1, MAX_CPU_WEIGHT + 1]
    )
    def test_state_rejects_out_of_range_weight(self, weight: int) -> None:
        from vq.throttle import ThrottleState

        with pytest.raises(ValidationError):
            ThrottleState(weight=weight)

    @pytest.mark.parametrize("weight", [MIN_CPU_WEIGHT, MAX_CPU_WEIGHT])
    def test_state_accepts_cpu_weight_boundaries(self, weight: int) -> None:
        from vq.throttle import ThrottleState

        assert ThrottleState(weight=weight).weight == weight

    def test_replace_mapping_filters_unknown_and_writes_direct(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from vq import throttle as throttle_module

        writes: list[tuple[throttle_module.ThrottleState, bool]] = []
        monkeypatch.setattr(
            throttle_module,
            "write_throttle_state",
            lambda value, *, via_rpc=True: writes.append((value, via_rpc)),
        )

        weight = throttle_module.replace_throttle_state_from_mapping(
            {
                "weight": "30",
                "reason": "mapping boundary",
                "duration_seconds": 60,
                "future_field_unknown_to_daemon": {"writer": 2},
            }
        )

        assert weight == 30
        assert len(writes) == 1
        written, via_rpc = writes[0]
        assert written.weight == 30
        assert written.reason == "mapping boundary"
        assert written.duration_seconds == 60
        assert via_rpc is False

    @pytest.mark.parametrize(
        "duration_seconds",
        [-1, 0, True, "2", 2.0],
        ids=["negative", "zero", "boolean", "string", "float"],
    )
    def test_duration_rejects_invalid_new_values(
        self,
        duration_seconds: object,
    ) -> None:
        from vq.throttle import ThrottleState

        with pytest.raises(ValidationError, match="duration_seconds"):
            ThrottleState(weight=20, duration_seconds=duration_seconds)

    @pytest.mark.parametrize("duration_seconds", [None, 1, 3600])
    def test_duration_accepts_valid_new_values(
        self,
        duration_seconds: int | None,
    ) -> None:
        from vq.throttle import ThrottleState

        state = ThrottleState(weight=20, duration_seconds=duration_seconds)

        assert state.duration_seconds == duration_seconds

    @pytest.mark.parametrize(
        "duration_seconds",
        [-1, 0, True, "2", 2.0],
        ids=["negative", "zero", "boolean", "string", "float"],
    )
    def test_invalid_stored_duration_preserves_unbounded_throttle(
        self,
        state: Path,
        duration_seconds: object,
    ) -> None:
        from vq.throttle import read_throttle_state, throttle_state_path

        path = throttle_state_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {
                    "weight": 20,
                    "reason": "preserve me",
                    "set_at": "2026-08-26T00:00:00+00:00",
                    "duration_seconds": duration_seconds,
                }
            )
        )

        recovered = read_throttle_state(via_rpc=False)

        assert recovered is not None
        assert recovered.weight == 20
        assert recovered.reason == "preserve me"
        assert recovered.set_at == "2026-08-26T00:00:00+00:00"
        assert recovered.duration_seconds is None
        assert path.exists()

    @pytest.mark.parametrize(
        "duration_seconds",
        [-1, 0, True, "2", 2.0],
        ids=["negative", "zero", "boolean", "string", "float"],
    )
    def test_invalid_rpc_read_duration_preserves_unbounded_throttle(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
        duration_seconds: object,
    ) -> None:
        from vq import rpc
        from vq import throttle as throttle_module

        monkeypatch.setattr(
            rpc,
            "try_rpc_or_fallback",
            lambda *args, **kwargs: {
                "weight": 20,
                "reason": "remote throttle",
                "set_at": "2026-08-26T00:00:00+00:00",
                "duration_seconds": duration_seconds,
                "future_field_unknown_to_client": True,
            },
        )

        recovered = throttle_module.read_throttle_state(via_rpc=True)

        assert recovered is not None
        assert recovered.weight == 20
        assert recovered.reason == "remote throttle"
        assert recovered.set_at == "2026-08-26T00:00:00+00:00"
        assert recovered.duration_seconds is None

    @pytest.mark.parametrize(
        "duration_seconds",
        [-1, 0, True, "2", 2.0],
        ids=["negative", "zero", "boolean", "string", "float"],
    )
    def test_invalid_rpc_write_duration_refuses_without_write(
        self,
        monkeypatch: pytest.MonkeyPatch,
        duration_seconds: object,
    ) -> None:
        from vq import throttle as throttle_module

        writes: list[throttle_module.ThrottleState] = []
        monkeypatch.setattr(
            throttle_module,
            "write_throttle_state",
            lambda value, *, via_rpc=True: writes.append(value),
        )

        with pytest.raises(ValidationError, match="duration_seconds"):
            throttle_module.replace_throttle_state_from_mapping(
                {"weight": 20, "duration_seconds": duration_seconds}
            )

        assert writes == []

    @pytest.mark.parametrize(
        "weight", [MIN_CPU_WEIGHT - 1, -1, MAX_CPU_WEIGHT + 1]
    )
    def test_replace_mapping_rejects_out_of_range_without_write(
        self,
        weight: int,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from vq import throttle as throttle_module

        writes: list[throttle_module.ThrottleState] = []
        monkeypatch.setattr(
            throttle_module,
            "write_throttle_state",
            lambda value, *, via_rpc=True: writes.append(value),
        )

        with pytest.raises(ValidationError):
            throttle_module.replace_throttle_state_from_mapping(
                {"weight": weight}
            )

        assert writes == []

    def test_clear_idempotent(self, state: Path) -> None:
        from vq.throttle import clear_throttle_state
        assert clear_throttle_state() is False
        assert clear_throttle_state() is False

    def test_corrupt_state_treated_as_no_persistence(self, state: Path) -> None:
        from vq.throttle import (
            read_throttle_state,
            throttle_state_path,
        )
        throttle_state_path().parent.mkdir(parents=True, exist_ok=True)
        throttle_state_path().write_text("not valid json {{{")
        # Corrupt file MUST NOT block the daemon's dispatch loop.
        assert read_throttle_state() is None

    @pytest.mark.parametrize(
        "weight", [MIN_CPU_WEIGHT - 1, -1, MAX_CPU_WEIGHT + 1]
    )
    def test_out_of_range_stored_weight_is_inactive_before_cgroup_apply(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
        weight: int,
    ) -> None:
        from vq.throttle import (
            apply_persistent_throttle_if_set,
            throttle_state_path,
        )

        throttle_state_path().write_text(f'{{"weight": {weight}}}')
        calls: list[tuple[str, int]] = []
        monkeypatch.setattr(cgroup, "available", lambda: True)
        monkeypatch.setattr(
            cgroup,
            "set_cpu_weight",
            lambda scope, value: calls.append((scope, value)) or True,
        )

        assert apply_persistent_throttle_if_set("invalidweight01") is None
        assert calls == []

    def test_apply_persistent_throttle_when_no_state_is_noop(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """No throttle.json on disk → returns None, doesn't call cgroup."""
        from vq.throttle import apply_persistent_throttle_if_set
        monkeypatch.setattr(cgroup, "available", lambda: True)
        calls: list = []
        monkeypatch.setattr(
            cgroup, "set_cpu_weight",
            lambda scope, w: calls.append((scope, w)) or True,
        )
        assert apply_persistent_throttle_if_set("anyjob00001") is None
        assert calls == []

    def test_apply_persistent_throttle_when_state_present(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """throttle.json present → daemon calls set_cpu_weight on the new scope."""
        from vq.throttle import (
            ThrottleState,
            apply_persistent_throttle_if_set,
            write_throttle_state,
        )
        write_throttle_state(ThrottleState(weight=20))
        monkeypatch.setattr(cgroup, "available", lambda: True)
        calls: list = []
        monkeypatch.setattr(
            cgroup, "set_cpu_weight",
            lambda scope, w: calls.append((scope, w)) or True,
        )
        result = apply_persistent_throttle_if_set("newjob000001")
        assert result == 20
        assert calls == [("vq-job-newjob000001.scope", 20)]

    def test_apply_persistent_throttle_handles_cgroup_failure(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """set_cpu_weight returns False → apply_persistent_throttle returns
        None and the dispatch proceeds (best-effort, doesn't fail the job)."""
        from vq.throttle import (
            ThrottleState,
            apply_persistent_throttle_if_set,
            write_throttle_state,
        )
        write_throttle_state(ThrottleState(weight=20))
        monkeypatch.setattr(cgroup, "available", lambda: True)
        monkeypatch.setattr(cgroup, "set_cpu_weight", lambda s, w: False)
        result = apply_persistent_throttle_if_set("newjob000002")
        assert result is None

    def test_format_throttle_status_inactive(self, state: Path) -> None:
        from vq.throttle import format_throttle_status
        out = format_throttle_status()
        assert "inactive" in out

    def test_format_throttle_status_with_reason(self, state: Path) -> None:
        from vq.throttle import (
            ThrottleState,
            format_throttle_status,
            write_throttle_state,
        )
        write_throttle_state(ThrottleState(weight=15, reason="gaming"))
        out = format_throttle_status()
        assert "ACTIVE" in out
        assert "CPUWeight=15" in out
        assert "gaming" in out


class TestPersistentThrottleCLI:
    def test_cli_persist_requires_all(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _mock_cgroup_available(monkeypatch, ok=True)
        result = CliRunner().invoke(
            main, ["throttle", "localhost", "abcd00000001", "--weight", "20", "--persist"]
        )
        assert result.exit_code != 0
        assert "requires --all" in result.output

    def test_cli_persist_with_all_writes_state(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from vq.throttle import read_throttle_state
        _mock_cgroup_available(monkeypatch, ok=True)
        _write_spec("aaaa00000001")
        result = CliRunner().invoke(
            main,
            ["throttle", "localhost", "--all", "--weight", "20", "--persist",
             "--reason", "kids gaming"],
        )
        assert result.exit_code == 0, result.output
        assert "persistent" in result.output
        state_obj = read_throttle_state()
        assert state_obj is not None
        assert state_obj.weight == 20
        assert state_obj.reason == "kids gaming"

    def test_cli_persist_and_restore_mutex(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _mock_cgroup_available(monkeypatch, ok=True)
        result = CliRunner().invoke(
            main, ["throttle", "localhost", "--all", "--restore", "--persist"]
        )
        assert result.exit_code != 0
        assert "mutually exclusive" in result.output

    def test_cli_restore_all_clears_persistent_state(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """--restore --all does the v0.5.13 thing AND clears throttle.json."""
        from vq.throttle import (
            ThrottleState,
            read_throttle_state,
            write_throttle_state,
        )
        _mock_cgroup_available(monkeypatch, ok=True)
        _write_spec("aaaa00000002")
        write_throttle_state(ThrottleState(weight=20, reason="prev"))
        assert read_throttle_state() is not None

        result = CliRunner().invoke(
            main, ["throttle", "localhost", "--all", "--restore"]
        )
        assert result.exit_code == 0, result.output
        assert "persistent state cleared" in result.output
        assert read_throttle_state() is None

    def test_cli_release_persist_clears_state_without_touching_running(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from vq.throttle import (
            ThrottleState,
            read_throttle_state,
            write_throttle_state,
        )
        # No running jobs needed; just verify state file behavior.
        write_throttle_state(ThrottleState(weight=20))
        result = CliRunner().invoke(
            main, ["throttle", "localhost", "--release-persist"]
        )
        assert result.exit_code == 0, result.output
        assert "cleared" in result.output
        assert read_throttle_state() is None

    def test_cli_release_persist_noop_when_no_state(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        result = CliRunner().invoke(
            main, ["throttle", "localhost", "--release-persist"]
        )
        assert result.exit_code == 0
        assert "not set" in result.output or "no-op" in result.output

    def test_cli_status_inactive(self, state: Path) -> None:
        result = CliRunner().invoke(main, ["throttle", "localhost", "--status"])
        assert result.exit_code == 0
        assert "inactive" in result.output

    def test_cli_status_after_persist(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _mock_cgroup_available(monkeypatch, ok=True)
        _write_spec("aaaa00000003")
        CliRunner().invoke(
            main,
            ["throttle", "localhost", "--all", "--weight", "15", "--persist",
             "--reason", "maintenance"],
        )
        result = CliRunner().invoke(main, ["throttle", "localhost", "--status"])
        assert result.exit_code == 0
        assert "ACTIVE" in result.output
        assert "CPUWeight=15" in result.output
        assert "maintenance" in result.output


class TestPersistentThrottleAutoRelease:
    """v0.5.16: --duration on persistent throttle, auto-clears on
    expiry. Same shape as drain's auto-release."""

    def test_duration_field_stored(self, state: Path) -> None:
        from vq.throttle import (
            ThrottleState,
            read_throttle_state,
            write_throttle_state,
        )
        s = ThrottleState(weight=20, duration_seconds=3600)
        write_throttle_state(s)
        back = read_throttle_state()
        assert back is not None
        assert back.duration_seconds == 3600

    def test_expired_throttle_auto_cleared_on_read(self, state: Path) -> None:
        """duration_seconds elapsed → read_throttle_state silently clears
        the file and returns None."""
        from datetime import datetime, timedelta

        from vq.throttle import (
            ThrottleState,
            read_throttle_state,
            throttle_state_path,
            write_throttle_state,
        )
        past = datetime.now(UTC) - timedelta(seconds=120)
        s = ThrottleState(
            weight=20,
            duration_seconds=60,  # expired 60s ago
            set_at=past.isoformat(),
        )
        write_throttle_state(s)
        assert throttle_state_path().exists()
        assert read_throttle_state() is None
        assert not throttle_state_path().exists()

    def test_apply_persistent_throttle_respects_expiry(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """If the daemon's _start_job reads expired throttle state,
        it gets None and does NOT apply CPUWeight to the new scope.
        This is the primary correctness property of auto-release on
        the daemon path."""
        from datetime import datetime, timedelta

        from vq.throttle import (
            ThrottleState,
            apply_persistent_throttle_if_set,
            write_throttle_state,
        )
        past = datetime.now(UTC) - timedelta(hours=3)
        s = ThrottleState(weight=20, duration_seconds=3600, set_at=past.isoformat())
        write_throttle_state(s)
        monkeypatch.setattr(cgroup, "available", lambda: True)
        calls: list = []
        monkeypatch.setattr(
            cgroup, "set_cpu_weight",
            lambda scope, w: calls.append((scope, w)) or True,
        )
        result = apply_persistent_throttle_if_set("newjob000003")
        # Auto-released by the expiry check; daemon doesn't apply any weight.
        assert result is None
        assert calls == []

    def test_format_throttle_status_shows_remaining(self, state: Path) -> None:
        from vq.throttle import (
            ThrottleState,
            format_throttle_status,
            write_throttle_state,
        )
        write_throttle_state(ThrottleState(weight=20, duration_seconds=1800))
        out = format_throttle_status()
        assert "auto-release in" in out

    def test_cli_persist_with_duration(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from vq.throttle import read_throttle_state
        _mock_cgroup_available(monkeypatch, ok=True)
        _write_spec("aaaa00000010")
        result = CliRunner().invoke(
            main,
            ["throttle", "localhost", "--all", "--weight", "20",
             "--persist", "--duration", "2h"],
        )
        assert result.exit_code == 0, result.output
        assert "auto-release after 7200s" in result.output
        s = read_throttle_state()
        assert s is not None
        assert s.duration_seconds == 7200

    def test_cli_bad_duration_errors(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _mock_cgroup_available(monkeypatch, ok=True)
        _write_spec("aaaa00000011")
        result = CliRunner().invoke(
            main,
            ["throttle", "localhost", "--all", "--weight", "20",
             "--persist", "--duration", "notvalid"],
        )
        assert result.exit_code != 0
        assert "--duration" in result.output


# ----------------------------------------------------------------------
# v0.5.21: renice fallback for non-cgroup hosts
# ----------------------------------------------------------------------


class TestWeightToNiceMapping:
    """``_weight_to_nice`` maps cgroup CPUWeight to POSIX nice level
    for the non-cgroup fallback path. Coarse banding by design; the
    mapping table is documented in throttle.py."""

    def test_default_weight_maps_to_default_nice(self) -> None:
        from vq.throttle import _weight_to_nice
        assert _weight_to_nice(100) == 0

    def test_typical_throttle_weight_maps_to_nice_10(self) -> None:
        """weight=20 is the "step aside" default; should map to a
        clearly-de-prioritised nice level."""
        from vq.throttle import _weight_to_nice
        assert _weight_to_nice(20) == 10
        assert _weight_to_nice(30) == 10
        assert _weight_to_nice(49) == 10

    def test_gentle_throttle_maps_to_nice_5(self) -> None:
        from vq.throttle import _weight_to_nice
        assert _weight_to_nice(50) == 5
        assert _weight_to_nice(99) == 5

    def test_deep_throttle_maps_to_nice_15(self) -> None:
        from vq.throttle import _weight_to_nice
        assert _weight_to_nice(10) == 15
        assert _weight_to_nice(19) == 15

    def test_lowest_weight_maps_to_nice_19(self) -> None:
        from vq.throttle import _weight_to_nice
        assert _weight_to_nice(1) == 19
        assert _weight_to_nice(9) == 19

    def test_boost_weight_maps_to_negative_nice(self) -> None:
        """weight > 100 = boost. Will likely fail without root, but
        the mapping should still resolve."""
        from vq.throttle import _weight_to_nice
        assert _weight_to_nice(150) == -2
        assert _weight_to_nice(200) == -5
        assert _weight_to_nice(499) == -5
        assert _weight_to_nice(500) == -10
        assert _weight_to_nice(10_000) == -10

    def test_mapping_is_monotonic_decreasing(self) -> None:
        """Higher CPUWeight (higher priority) should produce lower or
        equal nice (more priority). Spot-check key bands."""
        from vq.throttle import _weight_to_nice
        weights = [1, 5, 10, 15, 20, 30, 50, 99, 100, 150, 200, 500, 10_000]
        nices = [_weight_to_nice(w) for w in weights]
        for i in range(len(nices) - 1):
            assert nices[i] >= nices[i + 1], (
                f"non-monotonic at weight {weights[i]}->{weights[i+1]}: "
                f"nice {nices[i]}->{nices[i+1]}"
            )


class TestRenicePgid:
    """``_renice_pgid`` invokes the ``renice`` binary. We mock
    subprocess.run so tests don't actually muck with our own process
    group's priority."""

    def test_success_returns_ok(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from vq import throttle as throttle_mod
        calls: list[list[str]] = []

        def fake_run(cmd, **kw):
            calls.append(cmd)
            return subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout="", stderr="",
            )

        monkeypatch.setattr(throttle_mod.subprocess, "run", fake_run)
        ok, msg = throttle_mod._renice_pgid(54321, 10)
        assert ok is True
        assert "ok" in msg
        assert calls == [["renice", "-n", "10", "-g", "54321"]]

    def test_failure_returns_stderr(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from vq import throttle as throttle_mod
        monkeypatch.setattr(
            throttle_mod.subprocess, "run",
            lambda cmd, **kw: subprocess.CompletedProcess(
                args=cmd, returncode=1, stdout="",
                stderr="renice: failed to set priority: Permission denied\n",
            ),
        )
        ok, msg = throttle_mod._renice_pgid(54321, -10)
        assert ok is False
        assert "Permission denied" in msg

    def test_nice_out_of_range_short_circuits(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """We pre-validate nice level so the user gets a clearer error
        than renice's stderr."""
        from vq import throttle as throttle_mod
        called = False

        def fake_run(*a, **kw):
            nonlocal called
            called = True
            return subprocess.CompletedProcess(args=[], returncode=0)

        monkeypatch.setattr(throttle_mod.subprocess, "run", fake_run)
        ok, msg = throttle_mod._renice_pgid(1, 100)  # nice 100 invalid
        assert ok is False
        assert "out of range" in msg
        assert called is False  # never reached renice

    def test_timeout_caught(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from vq import throttle as throttle_mod

        def fake_run(*a, **kw):
            raise subprocess.TimeoutExpired(cmd="renice", timeout=10)

        monkeypatch.setattr(throttle_mod.subprocess, "run", fake_run)
        ok, msg = throttle_mod._renice_pgid(1, 5)
        assert ok is False
        assert "invocation failed" in msg

    def test_oserror_caught(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from vq import throttle as throttle_mod

        def fake_run(*a, **kw):
            raise FileNotFoundError("renice")  # binary not on PATH

        monkeypatch.setattr(throttle_mod.subprocess, "run", fake_run)
        ok, msg = throttle_mod._renice_pgid(1, 5)
        assert ok is False
        assert "invocation failed" in msg


class TestThrottleJobReniceFallback:
    """End-to-end fallback: cgroup unavailable + valid pgid + renice
    succeeds. The message should mention renice + cgroup-unavailable
    so the user knows which path ran."""

    def test_throttle_falls_back_to_renice_when_cgroup_off(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from vq import throttle as throttle_mod
        monkeypatch.setattr(cgroup, "available", lambda: False)
        # Fake renice success.
        monkeypatch.setattr(
            throttle_mod.subprocess, "run",
            lambda cmd, **kw: subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout="", stderr="",
            ),
        )
        spec = _write_spec("renice000001")
        msg = throttle_job("localhost", spec.id, 20)
        assert "renice" in msg
        assert "cgroup unavailable" in msg
        # nice=10 for weight=20 per the heuristic
        assert "nice=10" in msg

    def test_throttle_message_includes_path_label(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """cgroup path: message should NOT mention renice fallback."""
        _mock_cgroup_available(monkeypatch, ok=True)
        # Jobid intentionally doesn't contain "renice" so the substring
        # check below is unambiguous.
        _write_spec("cgrouponly02")
        msg = throttle_job("localhost", "cgrouponly02", 20)
        assert "renice" not in msg
        assert "CPUWeight=20" in msg


class TestApplyPersistentThrottleReniceFallback:
    """``apply_persistent_throttle_if_set(jobid, pgid=...)`` should use
    the renice path when cgroup is off and pgid is known."""

    def test_apply_uses_renice_when_cgroup_off(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from vq import throttle as throttle_mod
        from vq.throttle import (
            ThrottleState,
            apply_persistent_throttle_if_set,
            write_throttle_state,
        )

        monkeypatch.setattr(cgroup, "available", lambda: False)
        renice_calls: list[list[str]] = []

        def fake_run(cmd, **kw):
            renice_calls.append(cmd)
            return subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout="", stderr="",
            )

        monkeypatch.setattr(throttle_mod.subprocess, "run", fake_run)
        write_throttle_state(ThrottleState(weight=20))

        applied = apply_persistent_throttle_if_set(
            "newjob000001", pgid=99887,
        )
        assert applied == 20
        # renice was called with pgid + the mapped nice level
        assert renice_calls == [["renice", "-n", "10", "-g", "99887"]]

    def test_apply_returns_none_when_no_pgid_and_no_cgroup(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from vq.throttle import (
            ThrottleState,
            apply_persistent_throttle_if_set,
            write_throttle_state,
        )
        monkeypatch.setattr(cgroup, "available", lambda: False)
        write_throttle_state(ThrottleState(weight=20))
        applied = apply_persistent_throttle_if_set("xyz000001", pgid=None)
        assert applied is None

    def test_apply_returns_none_when_renice_fails(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from vq import throttle as throttle_mod
        from vq.throttle import (
            ThrottleState,
            apply_persistent_throttle_if_set,
            write_throttle_state,
        )

        monkeypatch.setattr(cgroup, "available", lambda: False)
        monkeypatch.setattr(
            throttle_mod.subprocess, "run",
            lambda cmd, **kw: subprocess.CompletedProcess(
                args=cmd, returncode=1, stdout="", stderr="boom",
            ),
        )
        write_throttle_state(ThrottleState(weight=20))
        applied = apply_persistent_throttle_if_set(
            "fail000001", pgid=11111,
        )
        assert applied is None  # best-effort: silent failure

    def test_apply_returns_none_when_no_persistent_state(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from vq.throttle import apply_persistent_throttle_if_set
        monkeypatch.setattr(cgroup, "available", lambda: False)
        # No throttle.json written.
        applied = apply_persistent_throttle_if_set("none001", pgid=1234)
        assert applied is None


class TestThrottleAllReniceFallback:
    """``throttle_all`` should fall back to renice for each running job
    when cgroup is off — same per-job logic as throttle_job, exercised
    through the bulk-iteration path."""

    def test_throttle_all_falls_back_for_each_job(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from vq import throttle as throttle_mod
        monkeypatch.setattr(cgroup, "available", lambda: False)
        calls: list[list[str]] = []

        def fake_run(cmd, **kw):
            calls.append(cmd)
            return subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout="", stderr="",
            )

        monkeypatch.setattr(throttle_mod.subprocess, "run", fake_run)
        _write_spec("bulk000001")
        _write_spec("bulk000002")
        _write_spec("bulk000003", state_val=JobState.SUSPENDED)
        # SUSPENDED job should be skipped, so only 2 renice calls.
        msg = throttle_all("localhost", 20)
        assert "throttled 2 jobs" in msg
        assert len(calls) == 2
        assert "SUSPENDED skipped" in msg
