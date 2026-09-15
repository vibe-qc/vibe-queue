"""Tests for vq.notify: webhook payload construction + delivery semantics."""
from __future__ import annotations

import json
from typing import Any

import pytest

from vq import notify
from vq.spec import JobSpec, JobState


def _terminal_spec(**overrides: object) -> JobSpec:
    base: dict[str, object] = {
        "id": "abc123def456",
        "command": ["python", "run.py"],
        "cwd": "/tmp/abc",
        "cpus": 4,
        "state": JobState.COMPLETED,
        "exit_code": 0,
        "submitted_at": "2026-05-15T10:00:00+00:00",
        "started_at": "2026-05-15T10:00:05+00:00",
        "finished_at": "2026-05-15T11:00:00+00:00",
    }
    base.update(overrides)
    return JobSpec(**base)


class _FakeResponse:
    """Minimal stand-in for the urlopen context-manager response, exposing the
    ``.status`` the notifier reads. Pre-fix this was undefined, so the mock
    urlopen's ``return _FakeResponse(...)`` raised NameError, which the
    best-effort webhook send swallowed, leaving the response-path tests
    asserting only that urlopen was reached (false-green)."""

    def __init__(self, status: int) -> None:
        self.status = status

    def __enter__(self) -> _FakeResponse:
        return self

    def __exit__(self, *exc: object) -> None:
        return None


class TestBuildPayload:
    def test_summary_string_uses_jobid_when_no_name(self) -> None:
        spec = _terminal_spec()
        p = notify.build_payload(spec, hostname="host_d")
        assert "abc123def456" in p["text"]
        assert "completed" in p["text"]
        assert "rc=0" in p["text"]
        assert "host_d" in p["text"]

    def test_summary_uses_name_dash_jobid_when_named(self) -> None:
        """v0.5.34: job_name flows into the notification summary too —
        consistent label across queue, status, fetch, archive,
        notification."""
        spec = _terminal_spec(job_name="mgo-pbe")
        p = notify.build_payload(spec, hostname="host_d")
        assert "mgo-pbe-abc123def456" in p["text"]

    def test_text_and_content_both_present_for_slack_discord_compat(
        self,
    ) -> None:
        """Slack reads ``text``, Discord reads ``content``, Mattermost
        reads ``text`` — having both means one URL works for any
        platform."""
        spec = _terminal_spec()
        p = notify.build_payload(spec)
        assert p["text"] == p["content"], (
            "text and content must carry the same human summary"
        )

    def test_structured_job_block_carries_full_summary(self) -> None:
        """Custom webhooks (your own HTTP endpoint, a relay) read the
        ``job`` block for the structured data instead of parsing the
        text string."""
        spec = _terminal_spec(
            job_name="tagged",
            state=JobState.OOM_KILLED,
            exit_code=137,
        )
        p = notify.build_payload(spec, hostname="host_d")
        assert p["job"]["id"] == "abc123def456"
        assert p["job"]["name"] == "tagged"
        assert p["job"]["state"] == "oom_killed"
        assert p["job"]["exit_code"] == 137
        assert p["job"]["host"] == "host_d"
        assert p["job"]["command"] == "python run.py"

    def test_structured_job_block_carries_failure_reason(self) -> None:
        spec = _terminal_spec(
            state=JobState.KILLED,
            exit_code=143,
            failure_reason=(
                "killed by vq/operator request: obsolete vibe-qc version"
            ),
        )
        p = notify.build_payload(spec, hostname="host_d")
        assert p["job"]["failure_reason"] == (
            "killed by vq/operator request: obsolete vibe-qc version"
        )

    def test_signal_exit_names_the_signal_in_summary(self) -> None:
        # v0.12.0: a 128+sig exit (SIGKILL=137) names the signal in the human
        # summary, so an OOM alert is legible instead of a bare rc=137. The
        # JSON payload keeps the raw integer.
        spec = _terminal_spec(state=JobState.OOM_KILLED, exit_code=137)
        p = notify.build_payload(spec, hostname="host_d")
        assert "rc=137, SIGKILL" in p["text"]
        assert p["job"]["exit_code"] == 137

    def test_plain_exit_has_no_signal_name_in_summary(self) -> None:
        spec = _terminal_spec(state=JobState.FAILED, exit_code=1)
        p = notify.build_payload(spec, hostname="host_d")
        assert "rc=1)" in p["text"]
        assert "SIG" not in p["text"]

    def test_payload_is_json_serializable(self) -> None:
        """The whole payload must round-trip through json.dumps without
        raising — fetch/notify path will json-encode it for the POST."""
        spec = _terminal_spec(job_name="x")
        p = notify.build_payload(spec)
        text = json.dumps(p)
        recovered = json.loads(text)
        assert recovered["job"]["name"] == "x"

    def test_no_name_renders_null_name_in_job_block(self) -> None:
        spec = _terminal_spec()
        p = notify.build_payload(spec)
        assert p["job"]["name"] is None


class TestSendTerminalNotification:
    """:func:`send_terminal_notification` is the daemon-facing entry
    point. Tests use ``blocking=True`` so we can assert on the POST
    inline (the default fire-and-forget thread would race the test)."""

    def test_no_op_when_webhook_url_is_none(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The most important invariant: notifications disabled by
        default. No URL = no HTTP call. Asserting on urlopen lets a
        future regression fail loudly instead of silently posting
        somewhere."""
        called = False

        def boom(*a: Any, **k: Any) -> None:
            nonlocal called
            called = True
            raise AssertionError("urlopen called with no webhook URL")

        monkeypatch.setattr(notify.urllib.request, "urlopen", boom)
        spec = _terminal_spec()
        notify.send_terminal_notification(spec, None, blocking=True)
        notify.send_terminal_notification(spec, "", blocking=True)
        assert called is False

    def test_no_op_when_state_not_terminal(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Defensive: caller misuse (passing a still-RUNNING spec)
        must not produce a spurious notification."""
        called = False

        def boom(*a: Any, **k: Any) -> None:
            nonlocal called
            called = True

        monkeypatch.setattr(notify.urllib.request, "urlopen", boom)
        spec = _terminal_spec(state=JobState.RUNNING, exit_code=None, finished_at=None)
        notify.send_terminal_notification(
            spec, "https://hook.example.com/x", blocking=True,
        )
        assert called is False

    def test_blocking_posts_to_webhook_with_json_body(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The POST body is the json-encoded payload, Content-Type is
        application/json, method is POST."""
        captured: dict[str, Any] = {}

        class FakeResp:
            status = 200
            def __enter__(self) -> FakeResp:
                return self
            def __exit__(self, *a: Any) -> None:
                pass
            def getcode(self) -> int:
                return 200

        def fake_urlopen(req: Any, timeout: float | None = None) -> FakeResp:
            captured["url"] = req.full_url
            captured["method"] = req.get_method()
            captured["headers"] = dict(req.header_items())
            captured["body"] = req.data
            captured["timeout"] = timeout
            return FakeResp()

        monkeypatch.setattr(notify.urllib.request, "urlopen", fake_urlopen)
        spec = _terminal_spec(job_name="mgo")
        notify.send_terminal_notification(
            spec, "https://hook.example.com/x", blocking=True,
        )
        assert captured["url"] == "https://hook.example.com/x"
        assert captured["method"] == "POST"
        # urllib lowercases header names internally.
        assert any(
            k.lower() == "content-type" and v == "application/json"
            for k, v in captured["headers"].items()
        )
        body = json.loads(captured["body"])
        assert body["job"]["name"] == "mgo"
        assert body["job"]["id"] == "abc123def456"
        assert body["job"]["state"] == "completed"
        # Timeout must be set — a hung webhook can't stall us forever.
        assert captured["timeout"] is not None and captured["timeout"] > 0

    def test_url_error_swallowed_silently(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A failed POST must NEVER raise out to the caller (the
        daemon). Best-effort: log + drop."""
        import urllib.error

        def fake_urlopen(*a: Any, **k: Any) -> None:
            raise urllib.error.URLError("connection refused")

        monkeypatch.setattr(notify.urllib.request, "urlopen", fake_urlopen)
        spec = _terminal_spec()
        # Must not raise.
        notify.send_terminal_notification(
            spec, "https://hook.example.com/x", blocking=True,
        )
        # And must log at WARNING so an operator can find out.
        assert any(
            "webhook notification" in r.message and "abc123def456" in r.message
            for r in caplog.records
        )

    def test_unexpected_exception_swallowed(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """The defensive catch-all handles urllib types not caught by
        URLError (e.g. socket errors on some Python versions)."""
        def fake_urlopen(*a: Any, **k: Any) -> None:
            raise RuntimeError("something weird")

        monkeypatch.setattr(notify.urllib.request, "urlopen", fake_urlopen)
        spec = _terminal_spec()
        notify.send_terminal_notification(
            spec, "https://hook.example.com/x", blocking=True,
        )
        assert any("RuntimeError" in r.message for r in caplog.records)

    def test_http_4xx_response_logged_as_warning(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A 4xx/5xx from the webhook should be logged. Doesn't raise
        (Slack returning 400 because we send invalid JSON shouldn't
        crash the daemon)."""
        class FakeResp:
            status = 403
            def __enter__(self) -> FakeResp:
                return self
            def __exit__(self, *a: Any) -> None:
                pass
            def getcode(self) -> int:
                return 403

        monkeypatch.setattr(
            notify.urllib.request, "urlopen",
            lambda *a, **k: FakeResp(),
        )
        spec = _terminal_spec()
        notify.send_terminal_notification(
            spec, "https://hook.example.com/x", blocking=True,
        )
        assert any("403" in r.message for r in caplog.records)

    def test_default_is_fire_and_forget_thread(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """blocking=False (default): a thread is spawned. The function
        returns immediately even if the post would have blocked."""
        # If we asserted on synchronous urlopen here we'd race the
        # thread; instead assert that send_terminal_notification
        # returns essentially immediately by faking _do_post to sleep
        # 2 seconds and timing the call.
        import time
        slow_marker: list[float] = []

        def slow_post(url: str, payload: dict, jobid: str) -> None:
            time.sleep(2.0)
            slow_marker.append(time.monotonic())

        monkeypatch.setattr(notify, "_do_post", slow_post)
        spec = _terminal_spec()
        start = time.monotonic()
        notify.send_terminal_notification(
            spec, "https://hook.example.com/x"  # default blocking=False
        )
        elapsed = time.monotonic() - start
        # Must return within a fraction of a second — the 2s sleep is
        # off-thread.
        assert elapsed < 0.5, (
            f"send_terminal_notification blocked the caller {elapsed:.2f}s; "
            "fire-and-forget thread isn't actually firing"
        )


# ----------------------------------------------------------------------
# v0.7.17 *Postel's Robustness* — notify_on_states filter
# ----------------------------------------------------------------------


class TestNotifyOnStatesFilter:
    """The v0.7.17 state filter: empty list (the default) preserves
    pre-v0.7.17 behaviour (fire on every terminal state); non-empty
    filters to only the listed states."""

    def test_empty_filter_fires_on_completed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Backward compat: a daemon with no filter configured still
        fires the webhook on COMPLETED (the most common pre-v0.7.17
        case)."""
        called: list[Any] = []

        def fake_urlopen(req: Any, timeout: float = 0) -> Any:
            called.append(req)
            return _FakeResponse(status=200)

        monkeypatch.setattr(notify.urllib.request, "urlopen", fake_urlopen)
        spec = _terminal_spec(state=JobState.COMPLETED, exit_code=0)
        notify.send_terminal_notification(
            spec, "https://hook.example.com/x", blocking=True,
            notify_on_states=[],
        )
        assert len(called) == 1, "empty filter should fire on every terminal"

    def test_none_filter_fires_on_completed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The kwarg defaults to None — passing it explicitly
        confirms the no-filter case."""
        called: list[Any] = []

        def fake_urlopen(req: Any, timeout: float = 0) -> Any:
            called.append(req)
            return _FakeResponse(status=200)

        monkeypatch.setattr(notify.urllib.request, "urlopen", fake_urlopen)
        spec = _terminal_spec(state=JobState.COMPLETED, exit_code=0)
        notify.send_terminal_notification(
            spec, "https://hook.example.com/x", blocking=True,
            # notify_on_states omitted entirely
        )
        assert len(called) == 1

    def test_filter_suppresses_completed_when_only_failures_wanted(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Canonical use case: operator configures notifications for
        FAILED only. A COMPLETED job must NOT fire."""
        called: list[Any] = []

        def fake_urlopen(req: Any, timeout: float = 0) -> Any:
            called.append(req)
            return _FakeResponse(status=200)

        monkeypatch.setattr(notify.urllib.request, "urlopen", fake_urlopen)
        spec = _terminal_spec(state=JobState.COMPLETED, exit_code=0)
        notify.send_terminal_notification(
            spec, "https://hook.example.com/x", blocking=True,
            notify_on_states=["failed", "oom_killed"],
        )
        assert called == [], (
            "COMPLETED should not fire when filter is ['failed', "
            "'oom_killed']"
        )

    def test_filter_fires_on_listed_state(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Same filter as the suppression test, but with a FAILED
        spec — this MUST fire."""
        called: list[Any] = []

        def fake_urlopen(req: Any, timeout: float = 0) -> Any:
            called.append(req)
            return _FakeResponse(status=200)

        monkeypatch.setattr(notify.urllib.request, "urlopen", fake_urlopen)
        spec = _terminal_spec(state=JobState.FAILED, exit_code=1)
        notify.send_terminal_notification(
            spec, "https://hook.example.com/x", blocking=True,
            notify_on_states=["failed", "oom_killed"],
        )
        assert len(called) == 1

    def test_filter_fires_on_oom_killed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The other listed state in the same filter."""
        called: list[Any] = []

        def fake_urlopen(req: Any, timeout: float = 0) -> Any:
            called.append(req)
            return _FakeResponse(status=200)

        monkeypatch.setattr(notify.urllib.request, "urlopen", fake_urlopen)
        spec = _terminal_spec(state=JobState.OOM_KILLED, exit_code=None)
        notify.send_terminal_notification(
            spec, "https://hook.example.com/x", blocking=True,
            notify_on_states=["failed", "oom_killed"],
        )
        assert len(called) == 1

    def test_filter_suppresses_unlisted_failure_state(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Filter = ['failed'] — a KILLED spec must NOT fire (even
        though KILLED is a failure-ish state, the operator listed
        only 'failed')."""
        called: list[Any] = []

        def fake_urlopen(req: Any, timeout: float = 0) -> Any:
            called.append(req)
            return _FakeResponse(status=200)

        monkeypatch.setattr(notify.urllib.request, "urlopen", fake_urlopen)
        spec = _terminal_spec(state=JobState.KILLED, exit_code=None)
        notify.send_terminal_notification(
            spec, "https://hook.example.com/x", blocking=True,
            notify_on_states=["failed"],
        )
        assert called == []


class TestNotifyOnStatesConfig:
    """Validation: ``notify_on_states`` rejects invalid state names
    at config-load time so the operator hears about a typo
    immediately, not after the daemon's been running and silently
    dropping their notifications."""

    def test_valid_states_normalise_to_lowercase(self) -> None:
        from vq.config import NotificationConfig
        cfg = NotificationConfig(
            webhook_url="https://x",
            notify_on_states=["FAILED", "Oom_Killed", "completed"],
        )
        assert cfg.notify_on_states == ["failed", "oom_killed", "completed"]

    def test_unknown_state_rejected_with_named_value(self) -> None:
        from vq.config import NotificationConfig
        with pytest.raises(Exception) as exc:
            NotificationConfig(
                webhook_url="https://x",
                notify_on_states=["failed", "not_a_real_state"],
            )
        assert "not_a_real_state" in str(exc.value)

    def test_duplicate_entries_deduped(self) -> None:
        from vq.config import NotificationConfig
        cfg = NotificationConfig(
            webhook_url="https://x",
            notify_on_states=["failed", "failed", "oom_killed", "FAILED"],
        )
        assert cfg.notify_on_states == ["failed", "oom_killed"]

    def test_empty_list_is_valid_default(self) -> None:
        from vq.config import NotificationConfig
        cfg = NotificationConfig(webhook_url="https://x")
        assert cfg.notify_on_states == []

    def test_pending_state_rejected_not_terminal(self) -> None:
        """PENDING is a JobState but not a TERMINAL state — the
        filter is specifically about terminal-state notifications."""
        from vq.config import NotificationConfig
        with pytest.raises(Exception) as exc:
            NotificationConfig(
                webhook_url="https://x",
                notify_on_states=["pending"],
            )
        assert "pending" in str(exc.value).lower()
