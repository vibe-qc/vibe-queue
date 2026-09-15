"""v0.6.51: ``vq submit --depends-on JOBID`` dispatch gate.

SLURM-style afterok dependencies — the daemon holds the dependent
PENDING until every predecessor reaches COMPLETED. If any predecessor
hits a non-COMPLETED terminal state (FAILED / KILLED / OOM_KILLED /
…), the dependent cascade-fails to FAILED with a work_errors entry
naming the failing predecessor.

Coverage shape:

* TestSpecRoundtrip      depends_on serializes / deserializes; default empty list;
                         pre-v0.6.51 specs read clean (additive field).
* TestSubmitValidation   --depends-on routes through to submit_local; rejects
                         unknown predecessor at submit time; self-dependency
                         rejected; submit_remote forwards --depends-on flags.
* TestDispatchGate       PENDING + non-terminal predecessor stays PENDING;
                         PENDING + COMPLETED predecessor dispatches; multi-
                         predecessor needs ALL to succeed; missing predecessor
                         conservatively waits (does not cascade-fail).
* TestCascadeFail        FAILED predecessor → dependent flips to FAILED with
                         work_errors entry; KILLED, OOM_KILLED, INTERRUPTED,
                         TIME_EXCEEDED, STARVED, ABORTED_BY_QUEUE all cascade.
* TestStatusAnnotation   `vq status` shows depends_on with the right one-token
                         readiness annotation (ready / waiting / failed /
                         unresolved).
* TestCLI                end-to-end --depends-on submit + vq status display.
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from vq import config, paths
from vq import submit as submit_module
from vq.cli import main
from vq.daemon import Daemon
from vq.spec import JobSpec, JobState
from vq.status import _depends_on_annotation, show_status, show_status_json

# ===========================================================================
# Spec roundtrip
# ===========================================================================


class TestSpecRoundtrip:
    def test_default_is_empty_list(self) -> None:
        s = JobSpec(id="a", command=["true"], cwd=".", cpus=1)
        assert s.depends_on == []

    def test_depends_on_roundtrip(self, tmp_path: Path) -> None:
        s = JobSpec(
            id="a", command=["true"], cwd=".", cpus=1,
            depends_on=["pred1", "pred2"],
        )
        p = tmp_path / "a.json"
        s.write(p)
        loaded = JobSpec.read(p)
        assert loaded.depends_on == ["pred1", "pred2"]

    def test_pre_v0_6_51_spec_reads_clean(self, tmp_path: Path) -> None:
        """Additive field: a spec written without depends_on (e.g.
        a v0.6.50 spec on disk after an upgrade) reads back as []."""
        p = tmp_path / "old.json"
        p.write_text(json.dumps({
            "id": "old1",
            "command": ["true"],
            "cwd": str(tmp_path),
            "cpus": 1,
        }))
        loaded = JobSpec.read(p)
        assert loaded.depends_on == []


# ===========================================================================
# Submit validation
# ===========================================================================


class TestSubmitValidation:
    def _setup_state(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> Path:
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        cfgdir = tmp_path / "cfg"
        cfgdir.mkdir()
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfgdir))
        (cfgdir / "config.toml").write_text('default_host = "localhost"\n')
        # Pre-create queue + jobs dirs.
        paths.queue_dir().mkdir(parents=True, exist_ok=True)
        paths.jobs_dir().mkdir(parents=True, exist_ok=True)
        return tmp_path

    def test_unknown_predecessor_rejected(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        self._setup_state(tmp_path, monkeypatch)
        input_file = tmp_path / "input.py"
        input_file.write_text("print('hi')\n")
        with pytest.raises(ValueError, match="no such job"):
            submit_module.submit_local(
                host="localhost",
                input_file=str(input_file),
                depends_on=["nonexistent-jobid"],
            )

    def test_predecessor_in_queue_accepted(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        self._setup_state(tmp_path, monkeypatch)
        # Pre-populate one valid predecessor.
        pred = JobSpec(
            id="pred12345abc", command=["true"],
            cwd=str(tmp_path), cpus=1,
        )
        pred.write(paths.queue_dir() / f"{pred.id}.json")

        input_file = tmp_path / "input.py"
        input_file.write_text("print('hi')\n")
        jobid = submit_module.submit_local(
            host="localhost",
            input_file=str(input_file),
            depends_on=[pred.id],
        )
        spec = JobSpec.read(paths.queue_dir() / f"{jobid}.json")
        assert spec.depends_on == [pred.id]

    def test_duplicate_predecessors_deduped_preserving_order(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        self._setup_state(tmp_path, monkeypatch)
        for pid in ("aaa111111111", "bbb222222222"):
            JobSpec(
                id=pid, command=["true"], cwd=str(tmp_path), cpus=1,
            ).write(paths.queue_dir() / f"{pid}.json")
        input_file = tmp_path / "input.py"
        input_file.write_text("print('hi')\n")
        jobid = submit_module.submit_local(
            host="localhost",
            input_file=str(input_file),
            depends_on=["aaa111111111", "bbb222222222", "aaa111111111"],
        )
        spec = JobSpec.read(paths.queue_dir() / f"{jobid}.json")
        assert spec.depends_on == ["aaa111111111", "bbb222222222"]


# ===========================================================================
# Dispatch gate
# ===========================================================================


def _make_daemon(tmp_path: Path) -> Daemon:
    d = Daemon(
        max_cpus=4,
        poll_interval=0.05,
        queue_dir=tmp_path / "queue",
        jobs_dir=tmp_path / "jobs",
    )
    d.queue_dir.mkdir(parents=True, exist_ok=True)
    d.jobs_dir.mkdir(parents=True, exist_ok=True)
    return d


def _write_spec(
    daemon: Daemon,
    jobid: str,
    *,
    state: JobState = JobState.PENDING,
    depends_on: list[str] | None = None,
    command: list[str] | None = None,
) -> JobSpec:
    workspace = daemon.jobs_dir / jobid
    workspace.mkdir(parents=True, exist_ok=True)
    spec = JobSpec(
        id=jobid,
        command=command or ["true"],
        cwd=str(workspace),
        cpus=1,
        state=state,
        depends_on=depends_on or [],
    )
    spec.write(daemon._spec_path(jobid))
    return spec


class TestDispatchGate:
    def test_pending_with_pending_predecessor_stays_pending(
        self, tmp_path: Path,
    ) -> None:
        d = _make_daemon(tmp_path)
        _write_spec(d, "pred", state=JobState.PENDING)
        _write_spec(d, "dep1", depends_on=["pred"])

        # Patch _start_job to record calls without actually running.
        started: list[str] = []

        def fake_start(spec: JobSpec) -> bool:
            started.append(spec.id)
            spec.state = JobState.RUNNING
            spec.write(d._spec_path(spec.id))
            return True

        with patch.object(d, "_start_job", side_effect=fake_start):
            d._dispatch_pending()

        # Pred was dispatchable; dep is gated.
        assert "pred" in started
        assert "dep1" not in started
        # dep1 still PENDING on disk.
        assert (
            JobSpec.read(d._spec_path("dep1")).state == JobState.PENDING
        )

    def test_pending_with_completed_predecessor_dispatches(
        self, tmp_path: Path,
    ) -> None:
        d = _make_daemon(tmp_path)
        _write_spec(d, "pred", state=JobState.COMPLETED)
        _write_spec(d, "dep1", depends_on=["pred"])

        started: list[str] = []

        def fake_start(spec: JobSpec) -> bool:
            started.append(spec.id)
            spec.state = JobState.RUNNING
            spec.write(d._spec_path(spec.id))
            return True

        with patch.object(d, "_start_job", side_effect=fake_start):
            d._dispatch_pending()
        # Pred is already terminal so not dispatched; dep1 fires.
        assert started == ["dep1"]

    def test_multiple_predecessors_all_must_complete(
        self, tmp_path: Path,
    ) -> None:
        d = _make_daemon(tmp_path)
        _write_spec(d, "preda", state=JobState.COMPLETED)
        _write_spec(d, "predb", state=JobState.RUNNING)  # not done
        _write_spec(d, "dep1", depends_on=["preda", "predb"])

        started: list[str] = []

        def fake_start(spec: JobSpec) -> bool:
            started.append(spec.id)
            return True

        with patch.object(d, "_start_job", side_effect=fake_start):
            d._dispatch_pending()
        assert "dep1" not in started

    def test_missing_predecessor_stays_pending(
        self, tmp_path: Path,
    ) -> None:
        """v0.6.51 conservative policy: a predecessor that doesn't
        exist (was vq cleanup --deleted, or never submitted) leaves
        the dependent PENDING. Operator must kill to unblock — we
        don't silently fail the dependent on a missing predecessor."""
        d = _make_daemon(tmp_path)
        _write_spec(d, "dep1", depends_on=["never-existed"])

        started: list[str] = []

        def fake_start(spec: JobSpec) -> bool:
            started.append(spec.id)
            return True

        with patch.object(d, "_start_job", side_effect=fake_start):
            d._dispatch_pending()
        assert started == []
        # Still PENDING (NOT cascade-failed).
        assert (
            JobSpec.read(d._spec_path("dep1")).state == JobState.PENDING
        )


# ===========================================================================
# Cascade-fail on predecessor failure
# ===========================================================================


class TestCascadeFail:
    @pytest.mark.parametrize(
        "failing_state",
        [
            JobState.FAILED,
            JobState.KILLED,
            JobState.OOM_KILLED,
            JobState.STARVED,
            JobState.TIME_EXCEEDED,
            JobState.INTERRUPTED,
            JobState.ABORTED_BY_QUEUE,
        ],
    )
    def test_every_non_completed_terminal_cascades(
        self,
        tmp_path: Path,
        failing_state: JobState,
    ) -> None:
        d = _make_daemon(tmp_path)
        _write_spec(d, "pred", state=failing_state)
        _write_spec(d, "dep1", depends_on=["pred"])

        with patch.object(d, "_start_job", return_value=True):
            d._dispatch_pending()

        dep = JobSpec.read(d._spec_path("dep1"))
        assert dep.state == JobState.FAILED
        # failure_reason carries the diagnostic.
        assert dep.failure_reason is not None
        assert "predecessor pred failed" in dep.failure_reason
        assert f"state={failing_state.value}" in dep.failure_reason
        # finished_at stamped.
        assert dep.finished_at is not None

    def test_completed_does_not_cascade(self, tmp_path: Path) -> None:
        d = _make_daemon(tmp_path)
        _write_spec(d, "pred", state=JobState.COMPLETED)
        _write_spec(d, "dep1", depends_on=["pred"])

        with patch.object(d, "_start_job", return_value=True):
            d._dispatch_pending()

        dep = JobSpec.read(d._spec_path("dep1"))
        # Either RUNNING (dispatched) or PENDING (race); definitely
        # NOT FAILED.
        assert dep.state != JobState.FAILED

    def test_one_failed_one_pending_cascades_immediately(
        self, tmp_path: Path,
    ) -> None:
        """Mixed predecessors: one FAILED, one still PENDING. Cascade
        wins — we know the outcome can't be COMPLETED, so flipping
        the dependent now avoids dispatching after the second one
        completes."""
        d = _make_daemon(tmp_path)
        _write_spec(d, "preda", state=JobState.FAILED)
        _write_spec(d, "predb", state=JobState.PENDING)
        _write_spec(d, "dep1", depends_on=["preda", "predb"])

        with patch.object(d, "_start_job", return_value=True):
            d._dispatch_pending()

        dep = JobSpec.read(d._spec_path("dep1"))
        assert dep.state == JobState.FAILED

    def test_cascade_fail_emits_state_transition_event(
        self, tmp_path: Path,
    ) -> None:
        """EVENT-2: a cascade-fail must be visible in the dependent's
        events.jsonl, not just written silently to the spec — every other
        terminal transition logs a state_transition event."""
        d = _make_daemon(tmp_path)
        _write_spec(d, "pred", state=JobState.FAILED)
        _write_spec(d, "dep1", depends_on=["pred"])

        with patch.object(d, "_start_job", return_value=True):
            d._dispatch_pending()

        assert JobSpec.read(d._spec_path("dep1")).state == JobState.FAILED

        events_file = d.jobs_dir / "dep1" / "_vq" / "events.jsonl"
        lines = [json.loads(ln) for ln in events_file.read_text().splitlines()]
        transitions = [
            e
            for e in lines
            if e.get("kind") == "state_transition" and e.get("to") == "failed"
        ]
        assert transitions, "cascade-fail must emit a state_transition event (EVENT-2)"
        assert transitions[-1]["from"] == "pending"
        assert "predecessor pred failed" in transitions[-1].get("reason", "")


# ===========================================================================
# Status annotation
# ===========================================================================


class TestStatusAnnotation:
    def _setup(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> Path:
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        cfgdir = tmp_path / "cfg"
        cfgdir.mkdir()
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfgdir))
        (cfgdir / "config.toml").write_text('default_host = "localhost"\n')
        paths.queue_dir().mkdir(parents=True, exist_ok=True)
        paths.jobs_dir().mkdir(parents=True, exist_ok=True)
        return tmp_path

    def test_ready_annotation(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        self._setup(tmp_path, monkeypatch)
        JobSpec(
            id="pred", command=["true"], cwd=str(tmp_path), cpus=1,
            state=JobState.COMPLETED,
        ).write(paths.queue_dir() / "pred.json")
        annot = _depends_on_annotation(["pred"], multi_user=False)
        assert annot == "ready"

    def test_waiting_annotation(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        self._setup(tmp_path, monkeypatch)
        JobSpec(
            id="pred", command=["true"], cwd=str(tmp_path), cpus=1,
            state=JobState.RUNNING,
        ).write(paths.queue_dir() / "pred.json")
        annot = _depends_on_annotation(["pred"], multi_user=False)
        assert annot == "waiting: pred (running)"

    def test_failed_annotation(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        self._setup(tmp_path, monkeypatch)
        JobSpec(
            id="pred", command=["true"], cwd=str(tmp_path), cpus=1,
            state=JobState.FAILED,
        ).write(paths.queue_dir() / "pred.json")
        annot = _depends_on_annotation(["pred"], multi_user=False)
        assert annot == "failed: pred"

    def test_unresolved_annotation(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        self._setup(tmp_path, monkeypatch)
        annot = _depends_on_annotation(["never-was"], multi_user=False)
        assert annot == "unresolved: never-was"

    @pytest.mark.parametrize("field", ["depends_on", "depends_on_any"])
    def test_invalid_durable_dependency_fails_before_status_annotation(
        self,
        field: str,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        self._setup(tmp_path, monkeypatch)
        payload = {
            "id": "dep1",
            "command": ["true"],
            "cwd": str(tmp_path),
            "cpus": 1,
            "state": "completed",
            field: ["../outside"],
        }
        (paths.queue_dir() / "dep1.json").write_text(json.dumps(payload))

        with (
            patch("vq.status._depends_on_annotation") as afterok,
            patch("vq.status._depends_on_any_annotation") as afterany,
            pytest.raises(ValueError, match=field),
        ):
            show_status("localhost", "dep1")

        afterok.assert_not_called()
        afterany.assert_not_called()

    def test_show_status_text_renders_depends_on_line(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        self._setup(tmp_path, monkeypatch)
        JobSpec(
            id="pred", command=["true"], cwd=str(tmp_path), cpus=1,
            state=JobState.RUNNING,
        ).write(paths.queue_dir() / "pred.json")
        dep_ws = tmp_path / "ws-dep"
        dep_ws.mkdir()
        JobSpec(
            id="dep1", command=["true"], cwd=str(dep_ws), cpus=1,
            depends_on=["pred"],
        ).write(paths.queue_dir() / "dep1.json")
        out = show_status("localhost", "dep1")
        assert "depends_on:   pred (waiting: pred (running))" in out

    def test_show_status_json_includes_depends_on_status(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        self._setup(tmp_path, monkeypatch)
        JobSpec(
            id="pred", command=["true"], cwd=str(tmp_path), cpus=1,
            state=JobState.COMPLETED,
        ).write(paths.queue_dir() / "pred.json")
        dep_ws = tmp_path / "ws-dep"
        dep_ws.mkdir()
        JobSpec(
            id="dep1", command=["true"], cwd=str(dep_ws), cpus=1,
            depends_on=["pred"],
        ).write(paths.queue_dir() / "dep1.json")
        payload = json.loads(show_status_json("localhost", "dep1"))
        assert payload["depends_on"] == ["pred"]
        assert payload["depends_on_status"] == "ready"


# ===========================================================================
# CLI end-to-end
# ===========================================================================


class TestCLI:
    def _setup(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> Path:
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        cfgdir = tmp_path / "cfg"
        cfgdir.mkdir()
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfgdir))
        (cfgdir / "config.toml").write_text('default_host = "localhost"\n')
        paths.queue_dir().mkdir(parents=True, exist_ok=True)
        paths.jobs_dir().mkdir(parents=True, exist_ok=True)
        return tmp_path

    def test_submit_with_depends_on_writes_spec(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        self._setup(tmp_path, monkeypatch)
        JobSpec(
            id="aaa111111111", command=["true"],
            cwd=str(tmp_path), cpus=1,
        ).write(paths.queue_dir() / "aaa111111111.json")
        input_file = tmp_path / "x.py"
        input_file.write_text("print('hi')\n")

        result = CliRunner().invoke(
            main,
            [
                "submit",
                "--depends-on", "aaa111111111",
                str(input_file),
            ],
        )
        assert result.exit_code == 0, result.output
        jobid = result.output.strip()
        spec = JobSpec.read(paths.queue_dir() / f"{jobid}.json")
        assert spec.depends_on == ["aaa111111111"]

    def test_submit_with_unknown_depends_on_clean_error(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        self._setup(tmp_path, monkeypatch)
        input_file = tmp_path / "x.py"
        input_file.write_text("print('hi')\n")
        result = CliRunner().invoke(
            main,
            ["submit", "--depends-on", "ghost", str(input_file)],
        )
        assert result.exit_code != 0
        assert "no such job" in result.output.lower()
