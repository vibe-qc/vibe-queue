"""Tests for v0.5.31 retry-on-failure (`vq submit --retry N`).

Covers: the JobSpec fields (retry_max / retry_count / not_before),
the --retry submit flag, the exponential-backoff helper, the daemon's
``_maybe_retry`` re-enqueue logic at both FAILED-transition sites
(_record_finish and _record_orphan_finish), the not_before dispatch
gate, the is_terminal-precedence guard (watchdog kills are NOT
retried), and that --retry composes with --auto-resume (the resume
sibling carries the retry budget forward).
"""
from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import config, paths
from vq.cli import main
from vq.daemon import (
    RETRY_BACKOFF_BASE_SECONDS,
    RETRY_BACKOFF_MAX_SECONDS,
    Daemon,
    _not_before_ready,
    _retry_backoff_seconds,
)
from vq.spec import JobSpec, JobState
from vq.submit import submit_local

# ----------------------------------------------------------------------
# JobSpec fields
# ----------------------------------------------------------------------


class TestRetrySpecFields:
    def test_defaults(self) -> None:
        spec = JobSpec(id="a" * 12, command=["true"], cwd="/tmp", cpus=1)
        assert spec.retry_max == 0
        assert spec.retry_count == 0
        assert spec.not_before is None

    def test_explicit_values(self) -> None:
        spec = JobSpec(
            id="b" * 12, command=["true"], cwd="/tmp", cpus=1,
            retry_max=5, retry_count=2,
            not_before="2026-05-14T12:00:00+00:00",
        )
        assert spec.retry_max == 5
        assert spec.retry_count == 2
        assert spec.not_before == "2026-05-14T12:00:00+00:00"

    def test_negative_retry_max_rejected(self) -> None:
        with pytest.raises(ValueError, match="greater than or equal to 0"):
            JobSpec(
                id="c" * 12, command=["true"], cwd="/tmp", cpus=1,
                retry_max=-1,
            )

    def test_old_spec_reads_clean(self, tmp_path: Path) -> None:
        """Additive fields: a pre-v0.5.31 spec JSON reads into the
        current model with the defaults — no SPEC_VERSION bump."""
        old = {
            "spec_version": 2,
            "id": "d" * 12,
            "command": ["true"],
            "cwd": "/tmp/d",
            "cpus": 1,
            "state": "pending",
            "submitted_at": "2026-05-09T12:00:00+00:00",
        }
        path = tmp_path / "old.json"
        path.write_text(json.dumps(old))
        spec = JobSpec.read(path)
        assert spec.retry_max == 0
        assert spec.retry_count == 0
        assert spec.not_before is None

    def test_roundtrips_through_disk(self, tmp_path: Path) -> None:
        spec = JobSpec(
            id="e" * 12, command=["true"], cwd="/tmp", cpus=1,
            retry_max=3, retry_count=1, not_before="2026-05-14T00:00:00+00:00",
        )
        path = tmp_path / "spec.json"
        spec.write(path)
        back = JobSpec.read(path)
        assert back.retry_max == 3
        assert back.retry_count == 1
        assert back.not_before == "2026-05-14T00:00:00+00:00"


# ----------------------------------------------------------------------
# Exponential backoff helper
# ----------------------------------------------------------------------


class TestRetryBackoff:
    def test_first_retry_is_base(self) -> None:
        assert _retry_backoff_seconds(1) == RETRY_BACKOFF_BASE_SECONDS

    def test_doubles_each_retry(self) -> None:
        assert _retry_backoff_seconds(1) == 10
        assert _retry_backoff_seconds(2) == 20
        assert _retry_backoff_seconds(3) == 40
        assert _retry_backoff_seconds(4) == 80
        assert _retry_backoff_seconds(5) == 160
        assert _retry_backoff_seconds(6) == 320

    def test_capped_at_max(self) -> None:
        # 10 * 2^6 = 640 > 600 -> capped
        assert _retry_backoff_seconds(7) == RETRY_BACKOFF_MAX_SECONDS
        assert _retry_backoff_seconds(20) == RETRY_BACKOFF_MAX_SECONDS


# ----------------------------------------------------------------------
# submit_local + CLI --retry
# ----------------------------------------------------------------------


@pytest.fixture
def submit_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    return tmp_path


class TestSubmitRetry:
    def test_submit_local_default_zero(self, submit_state: Path) -> None:
        script = submit_state / "job.py"
        script.write_text("print('hi')\n")
        jobid = submit_local(host="localhost", input_file=str(script))
        spec = JobSpec.read(paths.queue_dir() / f"{jobid}.json")
        assert spec.retry_max == 0

    def test_submit_local_retry_set(self, submit_state: Path) -> None:
        script = submit_state / "job.py"
        script.write_text("print('hi')\n")
        jobid = submit_local(
            host="localhost", input_file=str(script), retry=4,
        )
        spec = JobSpec.read(paths.queue_dir() / f"{jobid}.json")
        assert spec.retry_max == 4
        assert spec.retry_count == 0

    def test_cli_retry_flag(self, submit_state: Path) -> None:
        (submit_state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
        )
        script = submit_state / "job.py"
        script.write_text("print('hi')\n")
        result = CliRunner().invoke(
            main, ["submit", str(script), "--retry", "3"]
        )
        assert result.exit_code == 0, result.output
        spec = JobSpec.read(paths.queue_dir() / f"{result.output.strip()}.json")
        assert spec.retry_max == 3

    def test_cli_negative_retry_rejected(self, submit_state: Path) -> None:
        (submit_state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
        )
        script = submit_state / "job.py"
        script.write_text("print('hi')\n")
        result = CliRunner().invoke(
            main, ["submit", str(script), "--retry", "-1"]
        )
        assert result.exit_code != 0  # IntRange(min=0) rejects it

    def test_cli_help_mentions_retry(self) -> None:
        result = CliRunner().invoke(main, ["submit", "--help"])
        assert result.exit_code == 0
        assert "--retry" in result.output


# ----------------------------------------------------------------------
# Daemon: _maybe_retry core logic + FAILED-site integration
# ----------------------------------------------------------------------


@pytest.fixture
def daemon(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Daemon]:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    d = Daemon(
        max_cpus=8,
        poll_interval=0.05,
        queue_dir=tmp_path / "queue",
        jobs_dir=tmp_path / "jobs",
    )
    d.queue_dir.mkdir(parents=True, exist_ok=True)
    d.jobs_dir.mkdir(parents=True, exist_ok=True)
    yield d
    for rj in d._running.values():
        try:
            rj.popen.kill()
            rj.popen.wait(timeout=1)
        except Exception:
            pass
        rj.close_logs()


def _write_spec(
    daemon: Daemon,
    jobid: str,
    *,
    state: JobState = JobState.RUNNING,
    retry_max: int = 0,
    retry_count: int = 0,
    not_before: str | None = None,
    pgid: int | None = 4242,
    priority: int = 0,
) -> JobSpec:
    workspace = daemon.jobs_dir / jobid
    workspace.mkdir(parents=True, exist_ok=True)
    spec = JobSpec(
        id=jobid,
        command=["echo", "hi"],
        cwd=str(workspace),
        cpus=1,
        state=state,
        pid=999 if state == JobState.RUNNING else None,
        pgid=pgid if state == JobState.RUNNING else None,
        started_at="2026-05-14T00:00:00+00:00" if state == JobState.RUNNING else None,
        retry_max=retry_max,
        retry_count=retry_count,
        not_before=not_before,
        priority=priority,
    )
    spec.write(daemon._spec_path(jobid))
    return spec


class TestMaybeRetry:
    def test_no_budget_returns_false(self, daemon: Daemon) -> None:
        spec = _write_spec(daemon, "norebudget01", retry_max=0)
        assert daemon._maybe_retry(spec, rc=1) is False

    def test_re_enqueues_with_budget(self, daemon: Daemon) -> None:
        spec = _write_spec(daemon, "hasbudget001", retry_max=3, retry_count=0)
        before = datetime.now(UTC)
        assert daemon._maybe_retry(spec, rc=1) is True
        # The on-disk spec is now PENDING with retry_count bumped.
        reloaded = JobSpec.read(daemon._spec_path("hasbudget001"))
        assert reloaded.state == JobState.PENDING
        assert reloaded.retry_count == 1
        # Run-instance fields cleared for a clean re-dispatch.
        assert reloaded.pid is None
        assert reloaded.pgid is None
        assert reloaded.started_at is None
        assert reloaded.exit_code is None
        # not_before is ~10s (base backoff) in the future.
        assert reloaded.not_before is not None
        nb = datetime.fromisoformat(reloaded.not_before)
        delay = (nb - before).total_seconds()
        assert RETRY_BACKOFF_BASE_SECONDS - 1 <= delay <= RETRY_BACKOFF_BASE_SECONDS + 5

    def test_retry_does_not_inherit_submit_idempotency_binding(
        self, daemon: Daemon
    ) -> None:
        spec = _write_spec(daemon, "retrykey0001", retry_max=1)
        spec.idempotency_key_hash = "a" * 64
        spec.submission_intent_digest = "b" * 64
        spec.submission_owner_hash = "c" * 64
        spec.write(daemon._spec_path(spec.id))

        assert daemon._maybe_retry(spec, rc=1) is True

        reloaded = JobSpec.read(daemon._spec_path(spec.id))
        assert reloaded.idempotency_key_hash is None
        assert reloaded.submission_intent_digest is None
        assert reloaded.submission_owner_hash is None

    def test_retry_repairs_submit_claim_before_clearing_binding(
        self,
        daemon: Daemon,
        tmp_path: Path,
    ) -> None:
        source = tmp_path / "keyed.py"
        source.write_text("raise SystemExit(1)\n")
        first = submit_local(
            host="localhost",
            input_file=str(source),
            queue_dir=daemon.queue_dir,
            jobs_dir=daemon.jobs_dir,
            idempotency_key="crash-gap-retry",
            retry=1,
        )
        claim_paths = list(
            (daemon.queue_dir / ".submit-idempotency").rglob("*.json")
        )
        assert len(claim_paths) == 1
        claim_paths[0].unlink()  # simulate the spec-first claim crash gap
        spec = JobSpec.read(daemon._spec_path(first))

        assert daemon._maybe_retry(spec, rc=1) is True

        replay = submit_local(
            host="localhost",
            input_file=str(source),
            queue_dir=daemon.queue_dir,
            jobs_dir=daemon.jobs_dir,
            idempotency_key="crash-gap-retry",
            retry=1,
        )
        assert replay == first
        assert len(list(daemon.queue_dir.glob("*.json"))) == 1
        reloaded = JobSpec.read(daemon._spec_path(first))
        assert reloaded.idempotency_key_hash is None
        assert reloaded.submission_intent_digest is None
        assert reloaded.submission_owner_hash is None

    def test_exhausted_budget_returns_false(self, daemon: Daemon) -> None:
        spec = _write_spec(
            daemon, "exhausted001", retry_max=2, retry_count=2,
        )
        assert daemon._maybe_retry(spec, rc=1) is False

    def test_backoff_grows_with_retry_count(self, daemon: Daemon) -> None:
        """Second retry waits ~20s (2x base)."""
        spec = _write_spec(
            daemon, "secondretry1", retry_max=5, retry_count=1,
        )
        before = datetime.now(UTC)
        assert daemon._maybe_retry(spec, rc=1) is True
        reloaded = JobSpec.read(daemon._spec_path("secondretry1"))
        assert reloaded.retry_count == 2
        nb = datetime.fromisoformat(reloaded.not_before)
        delay = (nb - before).total_seconds()
        # retry_count is now 2 -> backoff 20s
        assert 19 <= delay <= 25

    def test_logs_state_transition_event(self, daemon: Daemon) -> None:
        spec = _write_spec(daemon, "eventret0001", retry_max=2)
        daemon._maybe_retry(spec, rc=7)
        events_file = daemon.jobs_dir / "eventret0001" / "_vq" / "events.jsonl"
        lines = [json.loads(ln) for ln in events_file.read_text().splitlines()]
        transitions = [e for e in lines if e.get("kind") == "state_transition"]
        assert len(transitions) == 1
        assert transitions[0]["to"] == "pending"
        assert transitions[0]["exit_code"] == 7
        assert "retry 1/2" in transitions[0]["reason"]


class TestRecordFinishRetry:
    """_record_finish (in-process job exit) routes through _maybe_retry."""

    def test_nonzero_exit_with_budget_re_enqueues(
        self, daemon: Daemon
    ) -> None:
        _write_spec(daemon, "infail000001", retry_max=2)
        daemon._record_finish("infail000001", rc=1)
        spec = JobSpec.read(daemon._spec_path("infail000001"))
        assert spec.state == JobState.PENDING  # NOT failed
        assert spec.retry_count == 1

    def test_nonzero_exit_exhausted_goes_failed(
        self, daemon: Daemon
    ) -> None:
        _write_spec(
            daemon, "infail000002", retry_max=2, retry_count=2,
        )
        daemon._record_finish("infail000002", rc=1)
        spec = JobSpec.read(daemon._spec_path("infail000002"))
        assert spec.state == JobState.FAILED
        assert spec.exit_code == 1

    def test_zero_exit_never_retried(self, daemon: Daemon) -> None:
        """rc=0 is success — even with retry budget, it COMPLETEs."""
        _write_spec(daemon, "insuccess001", retry_max=5)
        daemon._record_finish("insuccess001", rc=0)
        spec = JobSpec.read(daemon._spec_path("insuccess001"))
        assert spec.state == JobState.COMPLETED
        assert spec.retry_count == 0

    def test_no_retry_flag_goes_failed(self, daemon: Daemon) -> None:
        """retry_max=0 (the default) — unchanged pre-v0.5.31 behaviour."""
        _write_spec(daemon, "inplain00001", retry_max=0)
        daemon._record_finish("inplain00001", rc=1)
        spec = JobSpec.read(daemon._spec_path("inplain00001"))
        assert spec.state == JobState.FAILED

    def test_watchdog_kill_not_retried(self, daemon: Daemon) -> None:
        """A job the watchdog already marked OOM_KILLED has retry budget
        but is NOT retried — is_terminal precedence catches it before
        _maybe_retry. The watchdog killed it for a reason; silently
        bringing it back would be wrong."""
        spec = _write_spec(daemon, "inoom0000001", retry_max=3)
        # Watchdog got there first.
        spec.state = JobState.OOM_KILLED
        spec.write(daemon._spec_path("inoom0000001"))
        daemon._record_finish("inoom0000001", rc=137)
        reloaded = JobSpec.read(daemon._spec_path("inoom0000001"))
        assert reloaded.state == JobState.OOM_KILLED  # preserved, not retried
        assert reloaded.retry_count == 0


class TestRecordOrphanFinishRetry:
    """_record_orphan_finish (orphan exit recovered via marker) also
    routes through _maybe_retry."""

    def test_orphan_nonzero_with_budget_re_enqueues(
        self, daemon: Daemon
    ) -> None:
        spec = _write_spec(daemon, "orphfail0001", retry_max=2)
        daemon._record_orphan_finish(spec, rc=1, source="test-marker")
        reloaded = JobSpec.read(daemon._spec_path("orphfail0001"))
        assert reloaded.state == JobState.PENDING
        assert reloaded.retry_count == 1

    def test_orphan_zero_exit_completes(self, daemon: Daemon) -> None:
        spec = _write_spec(daemon, "orphok000001", retry_max=2)
        daemon._record_orphan_finish(spec, rc=0, source="test-marker")
        reloaded = JobSpec.read(daemon._spec_path("orphok000001"))
        assert reloaded.state == JobState.COMPLETED

    def test_orphan_exhausted_goes_failed(self, daemon: Daemon) -> None:
        spec = _write_spec(
            daemon, "orphexhaust1", retry_max=1, retry_count=1,
        )
        daemon._record_orphan_finish(spec, rc=2, source="test-marker")
        reloaded = JobSpec.read(daemon._spec_path("orphexhaust1"))
        assert reloaded.state == JobState.FAILED
        assert reloaded.exit_code == 2


class TestNotBeforeDispatchGate:
    """A PENDING job whose not_before is in the future is skipped by
    the dispatch loop (it's in retry-backoff)."""

    def _running_jobids(self, daemon: Daemon) -> set[str]:
        return {
            JobSpec.read(p).id
            for p in daemon.queue_dir.glob("*.json")
            if JobSpec.read(p).state == JobState.RUNNING
        }

    def test_future_not_before_skipped(self, daemon: Daemon) -> None:
        future = (datetime.now(UTC) + timedelta(hours=1)).isoformat()
        _write_spec(
            daemon, "backoffwait1", state=JobState.PENDING,
            not_before=future,
        )
        daemon._dispatch_pending()
        assert "backoffwait1" not in self._running_jobids(daemon)
        # still PENDING, untouched
        assert (
            JobSpec.read(daemon._spec_path("backoffwait1")).state
            == JobState.PENDING
        )

    def test_past_not_before_dispatched(self, daemon: Daemon) -> None:
        past = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
        _write_spec(
            daemon, "backoffready1", state=JobState.PENDING,
            not_before=past,
        )
        daemon._dispatch_pending()
        assert "backoffready1" in self._running_jobids(daemon)

    def test_none_not_before_dispatched(self, daemon: Daemon) -> None:
        """The common case — no not_before — dispatches normally."""
        _write_spec(
            daemon, "plainpending1", state=JobState.PENDING,
            not_before=None,
        )
        daemon._dispatch_pending()
        assert "plainpending1" in self._running_jobids(daemon)

    def test_corrupt_not_before_treated_as_ready(
        self, daemon: Daemon
    ) -> None:
        """A garbage not_before must not trap the job forever — treated
        as 'ready now'."""
        _write_spec(
            daemon, "corruptnb001", state=JobState.PENDING,
            not_before="not-a-timestamp",
        )
        daemon._dispatch_pending()
        assert "corruptnb001" in self._running_jobids(daemon)


class TestRetryComposesWithAutoResume:
    """--retry and --auto-resume are orthogonal: the auto-resume
    sibling carries the retry budget forward so a job that had spent
    2/3 retries before a reboot resumes with 2/3 still spent."""

    def test_auto_resume_sibling_carries_retry_budget(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr("vq.daemon._pgroup_alive", lambda pgid: False)
        _write_spec(
            daemon, "bothfeature1", state=JobState.RUNNING,
            retry_max=3, retry_count=2,
        )
        # Mark it recover_on_reboot too.
        spec = JobSpec.read(daemon._spec_path("bothfeature1"))
        spec.recover_on_reboot = True
        spec.idempotency_key_hash = "a" * 64
        spec.submission_intent_digest = "b" * 64
        spec.submission_owner_hash = "c" * 64
        spec.write(daemon._spec_path("bothfeature1"))

        daemon._reattach_or_interrupt_at_startup()

        siblings = [
            JobSpec.read(p) for p in daemon.queue_dir.glob("*.json")
            if JobSpec.read(p).parent_jobid == "bothfeature1"
        ]
        assert len(siblings) == 1
        sib = siblings[0]
        # Retry budget carried forward, not reset.
        assert sib.retry_max == 3
        assert sib.retry_count == 2
        # not_before is NOT carried (resume should dispatch promptly).
        assert sib.not_before is None
        assert sib.idempotency_key_hash is None
        assert sib.submission_intent_digest is None
        assert sib.submission_owner_hash is None


class TestNotBeforeReady:
    """Regression for RETRY-1: the not_before dispatch gate must treat a
    corrupt OR naive (tz-less) timestamp as 'ready now', and must never
    raise — a raise here aborts the whole _dispatch_pending tick and stalls
    ALL new dispatch every poll while one poisoned spec sits PENDING.
    """

    def test_none_is_ready(self) -> None:
        now = datetime.now(UTC)
        assert _not_before_ready(None, now) is True

    def test_future_aware_is_not_ready(self) -> None:
        now = datetime.now(UTC)
        future = (now + timedelta(hours=1)).isoformat()
        assert _not_before_ready(future, now) is False

    def test_past_aware_is_ready(self) -> None:
        now = datetime.now(UTC)
        past = (now - timedelta(hours=1)).isoformat()
        assert _not_before_ready(past, now) is True

    def test_naive_timestamp_does_not_raise_and_is_ready(self) -> None:
        # Pre-fix: `naive <= aware` raised TypeError (uncaught), wedging
        # the dispatch loop. A naive value (hand-edited spec) must be
        # treated as ready-now, not blow up.
        now = datetime.now(UTC)
        naive = datetime(2020, 1, 1, 0, 0, 0).isoformat()  # no tzinfo
        assert _not_before_ready(naive, now) is True

    def test_garbage_string_is_ready(self) -> None:
        now = datetime.now(UTC)
        assert _not_before_ready("not-a-timestamp", now) is True
        assert _not_before_ready("", now) is True
