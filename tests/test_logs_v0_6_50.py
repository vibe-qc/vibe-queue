"""v0.6.50: `vq logs JOBID` — show stdout/stderr without typing the
workspace path.

Companion to `vq status` (metadata + small log tail) and `vq tail`
(generic file tail via `exec tail(1)`). `vq logs` is spec-aware:
knows both streams, has --json, and `-f` terminates automatically
on terminal-and-idle (no Ctrl-C needed for shell-script chaining).

Coverage shape:

* `TestTailFile` — the shared `tail_file` helper (also used by
  `vq status` after the v0.6.50 dedupe).
* `TestShowLogs` — banner-separated text rendering, single-stream
  mode, archived state, missing file, custom tail.
* `TestShowLogsJSON` — JSON shape + path inclusion + stream filter.
* `TestFollowLogs` — generator emits initial tail, then new bytes
  as the file grows; stops when spec hits terminal + idle.
  Multi-stream gets `[stderr]` prefix; single-stream stays raw.
* `TestLogsCLI` — end-to-end via CliRunner: default form, --tail,
  --stdout / --stderr mutual exclusion, --follow + --json rejection,
  unknown jobid, remote delegation, multi-user resolution.
"""
from __future__ import annotations

import contextlib
import io
import json
import os
import threading
from collections.abc import Iterator
from pathlib import Path
from typing import IO, Any

import pytest
from click.testing import CliRunner

from vq import config, logs, paths, transport
from vq.cli import main
from vq.scheduler_dispatch import SchedulerHandle
from vq.spec import JobSpec, JobState

# ----- shared helpers -----


def _setup_job(
    tmp_path: Path,
    jobid: str = "abc123",
    stdout: str = "",
    stderr: str = "",
    state: JobState = JobState.RUNNING,
    archived: bool = False,
    **fields: object,
) -> tuple[Path, Path]:
    """Create a queue dir + workspace + spec for `jobid`. Returns
    (state_root, workspace)."""
    state_root = tmp_path / "state"
    (state_root / "queue").mkdir(parents=True)
    (state_root / "jobs").mkdir(parents=True)
    workspace = tmp_path / "ws" / jobid
    workspace.mkdir(parents=True)
    (workspace / "stdout.log").write_text(stdout)
    (workspace / "stderr.log").write_text(stderr)
    spec_fields: dict[str, object] = {
        "id": jobid,
        "command": ["echo", "ok"],
        "cwd": str(workspace),
        "cpus": 1,
        "state": state,
    }
    if archived:
        spec_fields["archived_at"] = "2026-05-24T00:00:00Z"
        spec_fields["archive_path"] = str(tmp_path / "archive" / f"{jobid}.tar.bz2")
    spec_fields.update(fields)
    JobSpec(**spec_fields).write(state_root / "queue" / f"{jobid}.json")
    return state_root, workspace


@pytest.fixture
def state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Path:
    """Empty state + cfg dirs, env vars pointed at them."""
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    cfgdir = tmp_path / "cfg"
    cfgdir.mkdir()
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfgdir))
    (cfgdir / "config.toml").write_text('default_host = "localhost"\n')
    return tmp_path


# ===========================================================================
# tail_file helper
# ===========================================================================


class TestTailFile:
    def test_missing_file_returns_sentinel(self, tmp_path: Path) -> None:
        assert logs.tail_file(tmp_path / "nope.log", 10) == "(no output)"

    def test_empty_file_returns_sentinel(self, tmp_path: Path) -> None:
        f = tmp_path / "empty.log"
        f.write_text("")
        assert logs.tail_file(f, 10) == "(empty)"

    def test_tail_returns_last_n_lines(self, tmp_path: Path) -> None:
        f = tmp_path / "x.log"
        f.write_text("\n".join(f"line{i}" for i in range(100)) + "\n")
        out = logs.tail_file(f, 5)
        assert out.startswith("... (95 earlier lines)\n")
        assert "line95" in out
        assert "line99" in out
        assert "line94" not in out

    def test_tail_none_returns_all(self, tmp_path: Path) -> None:
        f = tmp_path / "x.log"
        f.write_text("alpha\nbeta\n")
        assert logs.tail_file(f, None) == "alpha\nbeta"

    def test_tail_larger_than_file_returns_all(self, tmp_path: Path) -> None:
        f = tmp_path / "x.log"
        f.write_text("only-line\n")
        assert logs.tail_file(f, 100) == "only-line"


# ===========================================================================
# show_logs (text)
# ===========================================================================


class TestShowLogs:
    def test_default_renders_both_streams_with_banners(
        self, state: Path
    ) -> None:
        _setup_job(state, stdout="hi\n", stderr="oops\n")
        out = logs.show_logs("localhost", "abc123")
        assert "--- stdout ---" in out
        assert "--- stderr ---" in out
        assert "hi" in out
        assert "oops" in out

    def test_stdout_only_no_banner(self, state: Path) -> None:
        _setup_job(state, stdout="hi\n", stderr="oops\n")
        out = logs.show_logs("localhost", "abc123", stream="stdout")
        assert out == "hi"
        assert "stderr" not in out

    def test_stderr_only_no_banner(self, state: Path) -> None:
        _setup_job(state, stdout="hi\n", stderr="oops\n")
        out = logs.show_logs("localhost", "abc123", stream="stderr")
        assert out == "oops"

    def test_archived_shows_restore_hint_for_both(
        self, state: Path
    ) -> None:
        _setup_job(state, stdout="hi\n", archived=True)
        out = logs.show_logs("localhost", "abc123")
        assert "archived" in out
        assert "vq cleanup --restore" in out

    def test_missing_log_file_shows_sentinel(self, state: Path) -> None:
        _setup_job(state, stdout="hi\n")  # stderr.log exists but empty
        (state / "ws" / "abc123" / "stdout.log").unlink()
        out = logs.show_logs("localhost", "abc123", stream="stdout")
        assert out == "(no output)"

    def test_tail_truncates(self, state: Path) -> None:
        body = "\n".join(f"L{i}" for i in range(200)) + "\n"
        _setup_job(state, stdout=body)
        out = logs.show_logs(
            "localhost", "abc123", tail=10, stream="stdout",
        )
        assert "earlier lines" in out
        assert "L199" in out
        assert "L189" not in out  # 10 lines tail = L190..L199

    def test_unknown_jobid_raises(self, state: Path) -> None:
        # Set up the dirs but no spec.
        (state / "state" / "queue").mkdir(parents=True)
        (state / "state" / "jobs").mkdir(parents=True)
        with pytest.raises(FileNotFoundError, match="no such job"):
            logs.show_logs("localhost", "nosuch")

    def test_remote_host_rejected(self) -> None:
        with pytest.raises(NotImplementedError, match="should be delegated"):
            logs.show_logs("host_d", "abc")

    def test_terminal_state_stamps_last_status_at(
        self, state: Path
    ) -> None:
        sr, _ = _setup_job(
            state, stdout="done\n", state=JobState.COMPLETED,
        )
        logs.show_logs("localhost", "abc123")
        spec = JobSpec.read(sr / "queue" / "abc123.json")
        assert spec.last_status_at is not None

    def test_terminal_stamp_preserves_concurrent_fetch_metadata(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sr, _ = _setup_job(
            state, stdout="done\n", state=JobState.COMPLETED,
        )
        spec_path = sr / "queue" / "abc123.json"
        stale = JobSpec.read(spec_path)
        fresh = JobSpec.read(spec_path)
        fresh.last_fetched_at = "2026-07-03T10:00:00+00:00"
        fresh.write(spec_path)
        monkeypatch.setattr(
            logs,
            "_resolve_spec",
            lambda jobid, *, multi_user: (spec_path, stale),
        )

        logs.show_logs("localhost", "abc123", stream="stdout")

        after = JobSpec.read(spec_path)
        assert after.last_fetched_at == "2026-07-03T10:00:00+00:00"
        assert after.last_status_at is not None


class _FakeSchedulerDispatcher:
    def __init__(
        self,
        *,
        stdout: str = "",
        stderr: str = "",
        stdout_sequence: list[str] | None = None,
    ) -> None:
        self.stdout = stdout
        self.stderr = stderr
        self.stdout_sequence = list(stdout_sequence or [])
        self.calls: list[tuple[str, int | None, str]] = []
        self.handles: list[SchedulerHandle] = []

    def remote_workspace(self, jobid: str) -> str:
        return f"/remote/ws/{jobid}"

    def tail_log(
        self,
        handle: SchedulerHandle,
        *,
        lines: int | None = 200,
        stream: str = "stdout",
        array_index=None,
    ) -> str:
        self.handles.append(handle)
        self.calls.append((handle.job_id, lines, stream))
        if stream == "stdout" and self.stdout_sequence:
            if len(self.stdout_sequence) > 1:
                return self.stdout_sequence.pop(0)
            return self.stdout_sequence[0]
        return self.stderr if stream == "stderr" else self.stdout


def _scheduler_cfg(state: Path) -> config.Config:
    (state / "cfg" / "config.toml").write_text(
        'default_host = "localhost"\n'
        "\n"
        "[hosts.host_f]\n"
        'ssh = "host_f"\n'
        'scheduler = "pbs"\n'
        'scheduler_dialect = "torque"\n'
        'scratch_root = "/home/USER"\n'
        'scheduler_driver = "localhost"\n'
    )
    return config.load_config()


class TestSchedulerLogs:
    """v1.0 scheduler backend: live logs come from the cluster workspace."""

    def test_live_scheduler_stdout_uses_dispatcher(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _setup_job(
            state,
            stdout="local stale\n",
            scheduler_target="host_f",
            scheduler_job_id="555.cluster",
            array_index=2,
            array_total=5,
            array_group_id="logs-array",
        )
        fake = _FakeSchedulerDispatcher(stdout="remote live\n")
        monkeypatch.setattr(logs, "scheduler_dispatcher_for", lambda host_cfg: fake)

        out = logs.show_logs(
            "localhost", "abc123", stream="stdout", cfg=_scheduler_cfg(state)
        )
        assert out == "remote live"
        assert fake.calls == [("555.cluster", 100, "stdout")]
        assert [
            (handle.job_id, handle.remote_workspace, handle.array_size)
            for handle in fake.handles
        ] == [("555.cluster", "/remote/ws/abc123", None)]

    def test_live_scheduler_logs_with_no_id_use_staged_local_output(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _setup_job(
            state,
            stdout="local only\n",
            scheduler_target="host_f",
            scheduler_job_id=None,
        )
        monkeypatch.setattr(
            logs,
            "scheduler_dispatcher_for",
            lambda host_cfg: pytest.fail(
                "missing scheduler id must not build a dispatcher"
            ),
        )

        out = logs.show_logs(
            "localhost", "abc123", stream="stdout", cfg=_scheduler_cfg(state)
        )

        assert out == "local only"

    def test_live_scheduler_logs_preserve_empty_scheduler_id(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _setup_job(
            state,
            stdout="local stale\n",
            scheduler_target="host_f",
            scheduler_job_id="",
        )
        fake = _FakeSchedulerDispatcher(stdout="remote live\n")
        monkeypatch.setattr(logs, "scheduler_dispatcher_for", lambda host_cfg: fake)

        out = logs.show_logs(
            "localhost", "abc123", stream="stdout", cfg=_scheduler_cfg(state)
        )

        assert out == "remote live"
        assert [
            (handle.job_id, handle.remote_workspace, handle.array_size)
            for handle in fake.handles
        ] == [("", "/remote/ws/abc123", None)]

    def test_live_scheduler_json_uses_remote_paths(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _setup_job(
            state,
            scheduler_target="host_f",
            scheduler_job_id="555.cluster",
        )
        fake = _FakeSchedulerDispatcher(stdout="remote out\n", stderr="remote err\n")
        monkeypatch.setattr(logs, "scheduler_dispatcher_for", lambda host_cfg: fake)

        payload = json.loads(
            logs.show_logs_json("localhost", "abc123", cfg=_scheduler_cfg(state))
        )
        assert payload["scheduler_target"] == "host_f"
        assert payload["scheduler_job_id"] == "555.cluster"
        assert payload["queue_handle"]["host"] == "host_f"
        assert payload["stdout"] == "remote out"
        assert payload["stderr"] == "remote err"
        assert payload["stdout_path"] == "/remote/ws/abc123/stdout.log"

    def test_terminal_scheduler_job_uses_staged_back_local_logs(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _setup_job(
            state,
            stdout="local final\n",
            state=JobState.COMPLETED,
            scheduler_target="host_f",
            scheduler_job_id="555.cluster",
        )

        def fail_if_called(host_cfg):
            raise AssertionError("terminal scheduler logs should use local workspace")

        monkeypatch.setattr(logs, "scheduler_dispatcher_for", fail_if_called)
        out = logs.show_logs(
            "localhost", "abc123", stream="stdout", cfg=_scheduler_cfg(state)
        )
        assert out == "local final"

    def test_follow_scheduler_logs_streams_new_remote_tail(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sr, _ = _setup_job(
            state,
            scheduler_target="host_f",
            scheduler_job_id="555.cluster",
        )
        spec_path = sr / "queue" / "abc123.json"
        fake = _FakeSchedulerDispatcher(
            stdout_sequence=[
                "line1\n",
                "line1\n",
                "line1\nline2\n",
                "line1\nline2\n",
            ]
        )
        monkeypatch.setattr(logs, "scheduler_dispatcher_for", lambda host_cfg: fake)

        tick = {"n": 0}

        def sleep(_: float) -> None:
            tick["n"] += 1
            if tick["n"] == 1:
                spec = JobSpec.read(spec_path)
                spec.state = JobState.COMPLETED
                spec.write(spec_path)

        chunks = list(
            logs.follow_logs(
                "localhost",
                "abc123",
                stream="stdout",
                poll_interval=0.0,
                idle_ticks_after_terminal=1,
                sleep=sleep,
                cfg=_scheduler_cfg(state),
            )
        )
        assert "line1" in "".join(chunks)
        assert "line2" in "".join(chunks)

    def test_follow_scheduler_logs_allows_unbounded_initial_snapshot(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sr, _ = _setup_job(
            state,
            scheduler_target="host_f",
            scheduler_job_id="555.cluster",
        )
        spec_path = sr / "queue" / "abc123.json"
        initial = "".join(f"L{i}\n" for i in range(50))
        fake = _FakeSchedulerDispatcher(
            stdout_sequence=[initial, initial + "L50\n"]
        )
        monkeypatch.setattr(logs, "scheduler_dispatcher_for", lambda host_cfg: fake)

        def sleep(_: float) -> None:
            spec = JobSpec.read(spec_path)
            spec.state = JobState.COMPLETED
            spec.write(spec_path)

        chunks = list(
            logs.follow_logs(
                "localhost",
                "abc123",
                stream="stdout",
                initial_tail=None,
                poll_interval=0.0,
                idle_ticks_after_terminal=1,
                sleep=sleep,
                cfg=_scheduler_cfg(state),
            )
        )

        joined = "".join(chunks)
        assert joined == initial + "L50\n"
        assert fake.calls
        assert all(lines is None for _jobid, lines, _stream in fake.calls)


# ===========================================================================
# show_logs_json
# ===========================================================================


class TestShowLogsJSON:
    def test_json_default_both_streams(self, state: Path) -> None:
        _setup_job(state, stdout="hi\n", stderr="oops\n")
        out = logs.show_logs_json("localhost", "abc123")
        payload = json.loads(out)
        assert payload["jobid"] == "abc123"
        assert payload["stream"] == "both"
        assert payload["queue_handle"] == {
            "job_id": "abc123",
            "host": "localhost",
            "submitted_at": payload["queue_handle"]["submitted_at"],
        }
        assert payload["stdout"] == "hi"
        assert payload["stderr"] == "oops"
        assert "stdout_path" in payload
        assert "stderr_path" in payload
        assert payload["stdout_path"].endswith("stdout.log")

    def test_json_stream_filter_drops_other(self, state: Path) -> None:
        _setup_job(state, stdout="hi\n", stderr="oops\n")
        out = logs.show_logs_json("localhost", "abc123", stream="stdout")
        payload = json.loads(out)
        assert "stdout" in payload
        assert "stderr" not in payload
        assert "stdout_path" in payload
        assert "stderr_path" not in payload

    def test_json_state_included(self, state: Path) -> None:
        _setup_job(state, stdout="hi\n", state=JobState.COMPLETED)
        payload = json.loads(
            logs.show_logs_json("localhost", "abc123")
        )
        assert payload["state"] == "completed"

    def test_json_terminal_stamp_preserves_concurrent_fetch_metadata(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sr, _ = _setup_job(
            state, stdout="done\n", state=JobState.COMPLETED,
        )
        spec_path = sr / "queue" / "abc123.json"
        stale = JobSpec.read(spec_path)
        fresh = JobSpec.read(spec_path)
        fresh.last_fetched_at = "2026-07-03T10:10:00+00:00"
        fresh.write(spec_path)
        monkeypatch.setattr(
            logs,
            "_resolve_spec",
            lambda jobid, *, multi_user: (spec_path, stale),
        )

        payload = json.loads(logs.show_logs_json("localhost", "abc123"))

        after = JobSpec.read(spec_path)
        assert payload["state"] == "completed"
        assert after.last_fetched_at == "2026-07-03T10:10:00+00:00"
        assert after.last_status_at is not None


# ===========================================================================
# follow_logs
# ===========================================================================


class TestFollowLogs:
    def test_initial_tail_yielded_first(self, state: Path) -> None:
        _setup_job(
            state, stdout="initial-line\n",
            state=JobState.COMPLETED,
        )

        gen = logs.follow_logs(
            "localhost", "abc123",
            stream="stdout",
            initial_tail=10,
            poll_interval=0.0,
            idle_ticks_after_terminal=1,
            sleep=lambda _: None,
        )
        first = next(gen)
        assert "initial-line" in first
        # Consume remainder so the generator can return cleanly.
        for _ in gen:
            pass

    def test_terminates_on_terminal_and_idle(self, state: Path) -> None:
        """Spec is COMPLETED at start; one idle tick later the
        generator returns (idle_ticks_after_terminal=1)."""
        _setup_job(
            state, stdout="done\n", state=JobState.COMPLETED,
        )
        chunks = list(
            logs.follow_logs(
                "localhost", "abc123",
                stream="stdout",
                poll_interval=0.0,
                idle_ticks_after_terminal=1,
                sleep=lambda _: None,
            )
        )
        # Got at least the initial tail; no infinite loop.
        assert any("done" in c for c in chunks)

    def test_streams_new_bytes_as_file_grows(
        self, state: Path
    ) -> None:
        """Append to stdout.log between ticks; follow_logs picks up
        the new bytes. We drive the loop via a sleep callback that
        appends to the file, then transitions the spec to COMPLETED
        so the loop terminates."""
        sr, ws = _setup_job(
            state, stdout="line1\n", state=JobState.RUNNING,
        )
        stdout_path = ws / "stdout.log"
        spec_path = sr / "queue" / "abc123.json"

        tick = {"n": 0}

        def driver_sleep(_: float) -> None:
            tick["n"] += 1
            if tick["n"] == 1:
                # First sleep: append a new line.
                with stdout_path.open("a") as f:
                    f.write("line2\n")
            elif tick["n"] == 2:
                # Second sleep: append again + flip spec to terminal.
                with stdout_path.open("a") as f:
                    f.write("line3\n")
                spec = JobSpec.read(spec_path)
                spec.state = JobState.COMPLETED
                spec.write(spec_path)
            # Third+ sleep: nothing happens; the loop sees idle +
            # terminal and bails.

        chunks = list(
            logs.follow_logs(
                "localhost", "abc123",
                stream="stdout",
                initial_tail=10,
                poll_interval=0.0,
                idle_ticks_after_terminal=1,
                sleep=driver_sleep,
            )
        )
        joined = "".join(chunks)
        assert "line1" in joined
        assert "line2" in joined
        assert "line3" in joined

    def test_initial_snapshot_cursor_keeps_a_boundary_append(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sr, ws = _setup_job(
            state,
            stdout="initial\n",
            state=JobState.RUNNING,
        )
        stdout_path = ws / "stdout.log"
        spec_path = sr / "queue" / "abc123.json"
        original_snapshot = logs._read_follow_file_snapshot
        appended = False

        def snapshot(path: Path, n: int | None) -> tuple[str, int]:
            nonlocal appended
            result = original_snapshot(path, n)
            if path == stdout_path and not appended:
                appended = True
                with stdout_path.open("ab") as stream:
                    stream.write(b"appended\n")
            return result

        def sleep(_: float) -> None:
            spec = JobSpec.read(spec_path)
            spec.state = JobState.COMPLETED
            spec.write(spec_path)

        monkeypatch.setattr(logs, "_read_follow_file_snapshot", snapshot)

        chunks = list(
            logs.follow_logs(
                "localhost",
                "abc123",
                stream="stdout",
                initial_tail=None,
                poll_interval=0.0,
                idle_ticks_after_terminal=1,
                sleep=sleep,
            )
        )

        assert chunks == ["initial\n", "appended\n"]
        assert "".join(chunks) == stdout_path.read_text()

    def test_incremental_cursor_counts_bytes_read_during_a_raced_append(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sr, ws = _setup_job(
            state,
            stdout="initial\n",
            state=JobState.RUNNING,
        )
        stdout_path = ws / "stdout.log"
        spec_path = sr / "queue" / "abc123.json"
        original_snapshot = logs._read_follow_file_snapshot
        original_stat = Path.stat
        original_open = Path.open
        append_after_stat = False
        appended_during_read = False

        def snapshot(path: Path, n: int | None) -> tuple[str, int]:
            result = original_snapshot(path, n)
            if path == stdout_path:
                with original_open(stdout_path, "ab") as stream:
                    stream.write(b"first\n")
            return result

        def stat(path: Path, *args: object, **kwargs: object) -> os.stat_result:
            nonlocal append_after_stat
            result = original_stat(path, *args, **kwargs)
            if path == stdout_path:
                append_after_stat = True
            return result

        def open_file(
            path: Path,
            mode: str = "r",
            *args: object,
            **kwargs: object,
        ) -> IO[Any]:
            nonlocal append_after_stat, appended_during_read
            if (
                path == stdout_path
                and mode == "rb"
                and append_after_stat
                and not appended_during_read
            ):
                with original_open(stdout_path, "ab") as stream:
                    stream.write(b"second\n")
                append_after_stat = False
                appended_during_read = True
            return original_open(path, mode, *args, **kwargs)

        def sleep(_: float) -> None:
            spec = JobSpec.read(spec_path)
            spec.state = JobState.COMPLETED
            spec.write(spec_path)

        monkeypatch.setattr(logs, "_read_follow_file_snapshot", snapshot)
        monkeypatch.setattr(Path, "stat", stat)
        monkeypatch.setattr(Path, "open", open_file)

        chunks = list(
            logs.follow_logs(
                "localhost",
                "abc123",
                stream="stdout",
                initial_tail=None,
                poll_interval=0.0,
                idle_ticks_after_terminal=1,
                sleep=sleep,
            )
        )

        assert chunks == ["initial\n", "first\nsecond\n"]
        assert "".join(chunks) == stdout_path.read_text()

    def test_both_streams_get_label_prefix(
        self, state: Path
    ) -> None:
        sr, ws = _setup_job(
            state, stdout="o\n", stderr="e\n",
            state=JobState.RUNNING,
        )
        stderr_path = ws / "stderr.log"
        spec_path = sr / "queue" / "abc123.json"

        tick = {"n": 0}

        def driver_sleep(_: float) -> None:
            tick["n"] += 1
            # Fire once: append a single stderr line + flip terminal.
            # Subsequent ticks are no-ops so the idle-after-terminal
            # counter can tip the generator into returning.
            if tick["n"] == 1:
                with stderr_path.open("a") as f:
                    f.write("err-new\n")
                spec = JobSpec.read(spec_path)
                spec.state = JobState.COMPLETED
                spec.write(spec_path)

        chunks = list(
            logs.follow_logs(
                "localhost", "abc123",
                stream="both",
                initial_tail=10,
                poll_interval=0.0,
                idle_ticks_after_terminal=1,
                sleep=driver_sleep,
            )
        )
        # New stderr line surfaced with the [stderr] prefix.
        assert any("[stderr] err-new" in c for c in chunks)

    def test_archived_returns_hint_and_stops(
        self, state: Path
    ) -> None:
        _setup_job(state, stdout="hi\n", archived=True)
        chunks = list(
            logs.follow_logs(
                "localhost", "abc123",
                poll_interval=0.0,
                sleep=lambda _: None,
            )
        )
        assert any("archived" in c for c in chunks)


# ===========================================================================
# CLI verb end-to-end
# ===========================================================================


class TestLogsCLI:
    def test_default_form(self, state: Path) -> None:
        _setup_job(state, stdout="hi\n", stderr="oops\n")
        result = CliRunner().invoke(main, ["logs", "abc123"])
        assert result.exit_code == 0, result.output
        assert "--- stdout ---" in result.output
        assert "hi" in result.output
        assert "oops" in result.output

    def test_tail_zero_means_full(self, state: Path) -> None:
        body = "\n".join(f"L{i}" for i in range(50)) + "\n"
        _setup_job(state, stdout=body)
        result = CliRunner().invoke(
            main, ["logs", "abc123", "-n", "0", "--stdout"]
        )
        assert result.exit_code == 0, result.output
        assert "L0" in result.output
        assert "L49" in result.output
        assert "earlier lines" not in result.output

    def test_follow_tail_zero_means_full_initial_snapshot(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        body = "".join(f"L{i}\n" for i in range(50))
        _setup_job(state, stdout=body, state=JobState.COMPLETED)
        monkeypatch.setattr(logs.time, "sleep", lambda _seconds: None)

        result = CliRunner().invoke(
            main, ["logs", "abc123", "--follow", "--tail", "0", "--stdout"]
        )

        assert result.exit_code == 0, result.output
        lines = [line for line in result.output.splitlines() if line.startswith("L")]
        assert lines == [f"L{i}" for i in range(50)]
        assert "earlier lines" not in result.output

    def test_follow_positive_tail_remains_bounded(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        body = "".join(f"L{i}\n" for i in range(50))
        _setup_job(state, stdout=body, state=JobState.COMPLETED)
        monkeypatch.setattr(logs.time, "sleep", lambda _seconds: None)

        result = CliRunner().invoke(
            main, ["logs", "abc123", "--follow", "--tail", "7", "--stdout"]
        )

        assert result.exit_code == 0, result.output
        lines = [line for line in result.output.splitlines() if line.startswith("L")]
        assert lines == [f"L{i}" for i in range(43, 50)]
        assert "... (43 earlier lines)" in result.output

    def test_stdout_and_stderr_mutually_exclusive(
        self, state: Path
    ) -> None:
        _setup_job(state, stdout="hi\n")
        result = CliRunner().invoke(
            main, ["logs", "abc123", "--stdout", "--stderr"]
        )
        assert result.exit_code != 0
        assert "mutually exclusive" in result.output.lower()

    def test_follow_plus_json_rejected(self, state: Path) -> None:
        _setup_job(state, stdout="hi\n", state=JobState.COMPLETED)
        result = CliRunner().invoke(
            main, ["logs", "abc123", "-f", "--json"]
        )
        assert result.exit_code != 0
        assert "--follow with --json" in result.output

    def test_json_form(self, state: Path) -> None:
        _setup_job(state, stdout="hi\n", stderr="oops\n")
        result = CliRunner().invoke(
            main, ["logs", "abc123", "--json"]
        )
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["jobid"] == "abc123"
        assert "stdout" in payload
        assert "stderr" in payload

    def test_unknown_jobid_clean_error(self, state: Path) -> None:
        (state / "state" / "queue").mkdir(parents=True)
        (state / "state" / "jobs").mkdir(parents=True)
        result = CliRunner().invoke(main, ["logs", "nope"])
        assert result.exit_code != 0
        assert "no such job" in result.output.lower()

    def test_remote_delegate_uses_logs_verb(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Add a remote host so HOST is treated as remote.
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
            '\n[hosts.host_d]\nssh = "p.invalid"\n'
        )

        captured: dict[str, object] = {}

        @contextlib.contextmanager
        def fake_stream(
            host_cfg: config.HostConfig, *args: str
        ) -> Iterator[transport.RemoteStream]:
            captured["host"] = host_cfg.ssh
            captured["args"] = list(args)
            yield transport.RemoteStream(io.BytesIO(b"remote-output\n"))

        monkeypatch.setattr("vq.cli.transport.stream_remote_vq", fake_stream)

        result = CliRunner().invoke(
            main, ["logs", "host_d", "abc123", "-n", "10", "-f"]
        )
        assert result.exit_code == 0, result.output
        assert captured["args"][:3] == ["logs", "localhost", "abc123"]
        assert "-n" in captured["args"]
        assert "10" in captured["args"]
        assert "-f" in captured["args"]
        assert "remote-output" in result.output

    def test_remote_json_rewrites_queue_handle_host(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
            '\n[hosts.host_d]\nssh = "p.invalid"\n'
        )
        stdout = json.dumps(
            {
                "jobid": "abc123",
                "state": "running",
                "queue_handle": {
                    "job_id": "abc123",
                    "host": "localhost",
                    "submitted_at": "2026-07-03T06:00:00+00:00",
                },
            }
        )
        captured: dict[str, object] = {}

        def fake_delegate(host, cfg, *args, stdin_data=None):
            captured["host"] = host
            captured["args"] = list(args)
            return f"{stdout}\n"

        monkeypatch.setattr("vq.cli._delegate_to_remote", fake_delegate)

        result = CliRunner().invoke(main, ["logs", "host_d", "abc123", "--json"])

        assert result.exit_code == 0, result.output
        assert captured["args"] == ["logs", "localhost", "abc123", "--json"]
        payload = json.loads(result.output)
        assert payload["queue_handle"]["host"] == "host_d"

    def test_scheduler_host_delegates_to_driver(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
            "\n"
            "[hosts.host_f-scheduler-test]\n"
            'ssh = "host_f.invalid"\n'
            'scheduler = "pbs"\n'
            'scheduler_dialect = "torque"\n'
            'scratch_root = "/home/USER"\n'
            'scheduler_driver = "driver-remote-test"\n'
            "\n"
            "[hosts.driver-remote-test]\n"
            'ssh = "driver.invalid"\n'
        )
        captured: dict[str, object] = {}

        def fake_delegate(host, cfg, *args, stdin_data=None):
            captured["host"] = host
            captured["args"] = list(args)
            return "driver-output\n"

        monkeypatch.setattr("vq.cli._delegate_to_remote", fake_delegate)

        result = CliRunner().invoke(
            main, ["logs", "host_f-scheduler-test", "abc123", "--stdout"]
        )
        assert result.exit_code == 0, result.output
        assert captured["host"] == "driver-remote-test"
        assert captured["args"] == ["logs", "localhost", "abc123", "--stdout"]
        assert "driver-output" in result.output

    def test_scheduler_host_json_preserves_scheduler_queue_handle(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
            "\n"
            "[hosts.host_f-scheduler-test]\n"
            'ssh = "host_f.invalid"\n'
            'scheduler = "pbs"\n'
            'scheduler_dialect = "torque"\n'
            'scratch_root = "/home/USER"\n'
            'scheduler_driver = "driver-remote-test"\n'
            "\n"
            "[hosts.driver-remote-test]\n"
            'ssh = "driver.invalid"\n'
        )
        stdout = json.dumps(
            {
                "jobid": "abc123",
                "state": "running",
                "scheduler_target": "host_f-scheduler-test",
                "queue_handle": {
                    "job_id": "abc123",
                    "host": "host_f-scheduler-test",
                    "submitted_at": "2026-07-03T06:00:00+00:00",
                },
            }
        )
        captured: dict[str, object] = {}

        def fake_delegate(host, cfg, *args, stdin_data=None):
            captured["host"] = host
            captured["args"] = list(args)
            return f"{stdout}\n"

        monkeypatch.setattr("vq.cli._delegate_to_remote", fake_delegate)

        result = CliRunner().invoke(
            main, ["logs", "host_f-scheduler-test", "abc123", "--json"]
        )

        assert result.exit_code == 0, result.output
        assert captured["host"] == "driver-remote-test"
        assert captured["args"] == ["logs", "localhost", "abc123", "--json"]
        payload = json.loads(result.output)
        assert payload["queue_handle"]["host"] == "host_f-scheduler-test"

    def test_follow_terminates_without_ctrl_c(
        self, state: Path
    ) -> None:
        """The CLI's --follow path must not hang on a terminal job."""
        _setup_job(
            state, stdout="done\n", state=JobState.COMPLETED,
        )
        result_box: dict[str, object] = {}

        def run():
            result_box["r"] = CliRunner().invoke(
                main, ["logs", "abc123", "-f", "-n", "5"]
            )

        t = threading.Thread(target=run)
        t.start()
        t.join(timeout=10)
        assert not t.is_alive(), "vq logs -f hung on a terminal job"
        r = result_box["r"]
        assert r.exit_code == 0, r.output
        assert "done" in r.output
