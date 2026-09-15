"""Tests for ``vq.web.console_status`` — what this console process is, and
whether it is running the same vq as the daemon beside it.

The staleness cases are the regression guard for the 2026-08-05 fleet
audit: a console served pages from a hand-staged tree far behind the vq
that owned it, reporting its own version as if it were a property of the
fleet, and nothing on any page said so.
"""
from __future__ import annotations

import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from threading import Barrier, Thread
from time import sleep

import pytest

import vq
from vq import admin, rpc
from vq.web import console_status

#: A vq version that is not this console's. Pinned to the drifted version
#: from the 2026-08-05 incident; every test using it asserts the two really
#: differ, so a future release bump can never make those tests vacuous.
OTHER_VERSION = "0.16.0"

_WEB_ENV = (
    "VQ_WEB_BIND",
    "VQ_WEB_FLEET",
    "VQ_WEB_FLEET_INTERVAL",
    "VQ_WEB_LOG_LEVEL",
    "VQ_WEB_PORT",
    "VQ_WEB_PUBLIC_BIND_ACK",
    "VQ_WEB_TITLE",
    "VQ_WEB_TOKEN_FILE",
)


@pytest.fixture(autouse=True)
def isolated_console(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep every probe away from the developer's real config and daemon,
    pin the source tree for tests that are not about source drift, and start
    each test with an empty staleness cache (the cache is module state, so it
    would otherwise leak a verdict between tests).

    ``TestSourceDriftDetection`` overrides both digest attributes explicitly,
    so its six source-drift contracts still exercise every production branch.
    """
    monkeypatch.setenv("VQ_CONFIG_DIR", str(tmp_path / "config"))
    for name in _WEB_ENV:
        monkeypatch.delenv(name, raising=False)
    stable_tree_digest = "0" * 64
    monkeypatch.setattr(
        console_status,
        "_STARTED_TREE_DIGEST",
        stable_tree_digest,
    )
    monkeypatch.setattr(
        console_status,
        "_tree_digest",
        lambda: stable_tree_digest,
    )
    console_status.reset_staleness_cache()
    yield
    console_status.reset_staleness_cache()


class _PingStub:
    """Stand-in for :func:`vq.rpc.ping` that records how it was called."""

    def __init__(self, payload: object = None, *, error: Exception | None = None):
        self.payload = payload
        self.error = error
        self.calls: list[bool] = []
        self.delay = 0.0

    def __call__(self, *, multi_user: bool = False) -> object:
        self.calls.append(multi_user)
        if self.delay:
            sleep(self.delay)
        if self.error is not None:
            raise self.error
        return self.payload


@pytest.fixture
def ping(monkeypatch: pytest.MonkeyPatch):
    """Install a stub on the ``vq.rpc`` module object.

    ``console_status`` imports ``vq.rpc`` *inside* its functions, so the
    attribute on the module is the only seam a test can hold.
    """

    def install(payload: object = None, *, error: Exception | None = None) -> _PingStub:
        stub = _PingStub(payload, error=error)
        monkeypatch.setattr(rpc, "ping", stub)
        return stub

    return install


class _Clock:
    """Deterministic replacement for the module's ``monotonic``."""

    def __init__(self, start: float = 1000.0):
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> _Clock:
    fake = _Clock()
    monkeypatch.setattr(console_status, "monotonic", fake)
    return fake


class TestConsoleIdentity:
    def test_identity_describes_this_process(self) -> None:
        identity = console_status.console_identity()
        assert identity.version == vq.__version__
        assert identity.executable == sys.executable
        assert identity.pid == os.getpid()
        assert identity.source_sha is None or isinstance(identity.source_sha, str)

    def test_started_at_is_parseable_utc_iso8601(self) -> None:
        identity = console_status.console_identity()
        started = datetime.fromisoformat(identity.started_at)
        assert started.tzinfo is not None
        assert started.utcoffset().total_seconds() == 0
        assert started <= datetime.now(UTC)

    def test_started_at_is_process_start_not_call_time(self) -> None:
        """Import-time capture: two calls describe the same process, so a
        console up for weeks reads as up for weeks."""
        first = console_status.console_identity()
        second = console_status.console_identity()
        assert first.started_at == second.started_at

    def test_describe_names_version_and_interpreter(self) -> None:
        identity = console_status.console_identity()
        line = identity.describe()
        assert vq.__version__ in line
        assert sys.executable in line
        assert "up since" in line

    def test_describe_carries_short_sha_when_known(self) -> None:
        identity = console_status.ConsoleIdentity(
            version="0.24.0",
            executable="/opt/vq/bin/python",
            source_sha="0123456789abcdef",
            started_at="2026-08-05T09:15:00+00:00",
            pid=4242,
        )
        line = identity.describe()
        assert "0123456789abcdef"[:9] in line
        assert "2026-08-05 09:15:00 UTC" in line

    def test_describe_omits_sha_when_underivable(self) -> None:
        identity = console_status.ConsoleIdentity(
            version="0.24.0",
            executable="/opt/vq/bin/python",
            source_sha=None,
            started_at="2026-08-05T09:15:00+00:00",
            pid=4242,
        )
        line = identity.describe()
        assert line.startswith("vq 0.24.0 · /opt/vq/bin/python")

    def test_identity_survives_a_failing_sha_probe(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A console that cannot introspect its checkout must still serve
        pages, so the SHA probe degrades to None rather than raising."""

        def boom(_anchor: Path) -> str:
            raise RuntimeError("git not on PATH")

        monkeypatch.setattr(admin, "running_source_sha", boom)
        identity = console_status.console_identity()
        assert identity.source_sha is None
        assert identity.version == vq.__version__


class TestStalenessVerdict:
    def test_matching_daemon_version_is_not_stale(self, ping) -> None:
        stub = ping({"version": vq.__version__})
        verdict = console_status.console_staleness()
        assert verdict.stale is False
        assert verdict.reason is None
        assert verdict.known is True
        assert verdict.console_version == vq.__version__
        assert verdict.daemon_version == vq.__version__
        assert stub.calls == [False]

    def test_diverged_daemon_version_names_both_and_says_what_to_do(
        self, ping
    ) -> None:
        """Regression guard for the 2026-08-05 incident: a console running
        vq 0.16.0 against a 0.24.0 install rendered every page normally and
        said nothing."""
        assert vq.__version__ != OTHER_VERSION
        ping({"version": OTHER_VERSION})
        verdict = console_status.console_staleness()
        assert verdict.stale is True
        assert verdict.known is True
        assert verdict.daemon_version == OTHER_VERSION
        assert verdict.console_version == vq.__version__
        assert verdict.reason is not None
        assert vq.__version__ in verdict.reason
        assert OTHER_VERSION in verdict.reason
        assert "restart the console" in verdict.reason.lower()

    def test_old_console_against_new_install_is_stale(
        self, ping, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The incident's exact pairing — the console is the stale half, so
        the reason must blame the console, not the daemon."""
        monkeypatch.setattr(console_status, "__version__", "0.16.0")
        ping({"version": "0.24.0"})
        verdict = console_status.console_staleness()
        assert verdict.stale is True
        assert verdict.console_version == "0.16.0"
        assert verdict.daemon_version == "0.24.0"
        assert "0.16.0" in verdict.reason
        assert "0.24.0" in verdict.reason
        assert "restart the console" in verdict.reason.lower()

    def test_reason_points_at_the_drifted_interpreter(self, ping) -> None:
        ping({"version": OTHER_VERSION})
        verdict = console_status.console_staleness()
        assert sys.executable in verdict.reason

    def test_unreachable_daemon_never_cries_wolf(self, ping) -> None:
        """No daemon means no comparison. An unknown verdict must never be
        reported as stale — the banner would be permanent on any host whose
        daemon is merely restarting."""
        ping(None)
        verdict = console_status.console_staleness()
        assert verdict.stale is False
        assert verdict.known is False
        assert verdict.daemon_version is None
        assert verdict.reason is None
        assert verdict.console_version == vq.__version__

    def test_probe_that_raises_is_unknown_not_stale(self, ping) -> None:
        ping(error=OSError("connection refused"))
        verdict = console_status.console_staleness()
        assert verdict.stale is False
        assert verdict.known is False
        assert verdict.daemon_version is None
        assert verdict.reason is None

    @pytest.mark.parametrize(
        "payload",
        [
            {},
            {"version": None},
            {"version": ""},
            {"version": "   "},
            {"version": 24},
            "pong",
            None,
        ],
        ids=[
            "no-version-key",
            "null-version",
            "empty-version",
            "blank-version",
            "non-string-version",
            "non-dict-payload",
            "no-payload",
        ],
    )
    def test_unusable_payload_is_unknown_not_stale(self, ping, payload) -> None:
        ping(payload)
        verdict = console_status.console_staleness()
        assert verdict.known is False
        assert verdict.stale is False

    def test_daemon_version_is_stripped_before_comparison(self, ping) -> None:
        ping({"version": f"  {vq.__version__}  "})
        verdict = console_status.console_staleness()
        assert verdict.daemon_version == vq.__version__
        assert verdict.stale is False

    def test_multi_user_flag_reaches_rpc_ping(self, ping) -> None:
        stub = ping({"version": vq.__version__})
        console_status.console_staleness(multi_user=True)
        assert stub.calls == [True]


class TestStalenessCache:
    def test_probes_once_within_the_ttl(self, ping, clock: _Clock) -> None:
        stub = ping({"version": vq.__version__})
        first = console_status.cached_staleness()
        for _ in range(5):
            assert console_status.cached_staleness() == first
        clock.advance(console_status.STALENESS_TTL_SECONDS - 0.5)
        assert console_status.cached_staleness() == first
        assert len(stub.calls) == 1

    def test_reprobes_after_the_ttl_expires(self, ping, clock: _Clock) -> None:
        stub = ping({"version": vq.__version__})
        console_status.cached_staleness()
        clock.advance(console_status.STALENESS_TTL_SECONDS + 0.5)
        console_status.cached_staleness()
        assert len(stub.calls) == 2

    def test_ttl_constant_is_the_knob(
        self, ping, clock: _Clock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(console_status, "STALENESS_TTL_SECONDS", 0.0)
        stub = ping({"version": vq.__version__})
        console_status.cached_staleness()
        console_status.cached_staleness()
        assert len(stub.calls) == 2

    def test_reset_forces_a_reprobe(self, ping, clock: _Clock) -> None:
        stub = ping({"version": vq.__version__})
        console_status.cached_staleness()
        assert len(stub.calls) == 1
        console_status.reset_staleness_cache()
        console_status.cached_staleness()
        assert len(stub.calls) == 2

    def test_cached_verdict_equals_the_uncached_one(self, ping) -> None:
        ping({"version": OTHER_VERSION})
        assert console_status.cached_staleness() == console_status.console_staleness()

    def test_stale_verdict_is_served_from_cache_unchanged(
        self, ping, clock: _Clock
    ) -> None:
        ping({"version": OTHER_VERSION})
        first = console_status.cached_staleness()
        assert first.stale is True
        again = console_status.cached_staleness()
        assert again.stale is True
        assert again.reason == first.reason

    def test_failing_probe_still_yields_a_usable_verdict(
        self, ping, clock: _Clock
    ) -> None:
        """A request handler renders the banner unconditionally, so the
        cached path must hand back a verdict even when the probe blew up."""
        ping(error=RuntimeError("socket gone"))
        verdict = console_status.cached_staleness()
        assert verdict.stale is False
        assert verdict.known is False

    def test_multi_user_flag_is_threaded_through_the_cache(self, ping) -> None:
        stub = ping({"version": vq.__version__})
        console_status.cached_staleness(multi_user=True)
        assert stub.calls == [True]


class TestStalenessCacheConcurrency:
    def test_concurrent_callers_agree_and_never_raise(self, ping) -> None:
        """The console serves requests from a thread pool; the module-level
        cache is shared state on that path."""
        stub = ping({"version": OTHER_VERSION})
        stub.delay = 0.01
        threads = 8
        gate = Barrier(threads)
        verdicts: list[console_status.ConsoleStaleness] = []
        errors: list[BaseException] = []

        def worker() -> None:
            try:
                gate.wait(timeout=10)
                verdicts.append(console_status.cached_staleness())
            except BaseException as e:  # noqa: BLE001 — reported, not swallowed
                errors.append(e)

        workers = [Thread(target=worker) for _ in range(threads)]
        for t in workers:
            t.start()
        for t in workers:
            t.join(timeout=10)
            assert not t.is_alive()

        assert errors == []
        assert len(verdicts) == threads
        assert all(v == verdicts[0] for v in verdicts)
        assert verdicts[0].stale is True
        assert verdicts[0].daemon_version == OTHER_VERSION

    def test_reset_racing_readers_never_raises(self, ping) -> None:
        stub = ping({"version": vq.__version__})
        stub.delay = 0.001
        errors: list[BaseException] = []

        def reader() -> None:
            try:
                for _ in range(20):
                    assert console_status.cached_staleness().stale is False
            except BaseException as e:  # noqa: BLE001 — reported, not swallowed
                errors.append(e)

        def resetter() -> None:
            try:
                for _ in range(20):
                    console_status.reset_staleness_cache()
            except BaseException as e:  # noqa: BLE001 — reported, not swallowed
                errors.append(e)

        workers = [Thread(target=reader) for _ in range(6)]
        workers += [Thread(target=resetter) for _ in range(2)]
        for t in workers:
            t.start()
        for t in workers:
            t.join(timeout=10)
            assert not t.is_alive()
        assert errors == []


class TestStalenessCacheIsKeyedOnMode:
    """One cache slot per daemon, not one slot total.

    ``multi_user`` selects which daemon socket the probe dials. A single
    shared slot let a single-user query serve a multi-user caller the
    other daemon's version for up to the TTL -- reporting the wrong
    daemon's version being the exact failure this module exists to catch.
    """

    def test_modes_do_not_serve_each_others_verdicts(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        console_status.reset_staleness_cache()
        seen: list[bool] = []

        def fake_ping(*, multi_user: bool = False):
            seen.append(multi_user)
            return {"version": "9.9.9" if multi_user else console_status.__version__}

        monkeypatch.setattr(rpc, "ping", fake_ping)

        single = console_status.cached_staleness(multi_user=False)
        multi = console_status.cached_staleness(multi_user=True)

        assert seen == [False, True], "each mode must probe its own daemon"
        assert single.stale is False
        assert single.daemon_version == console_status.__version__
        assert multi.stale is True
        assert multi.daemon_version == "9.9.9"

    def test_each_mode_still_caches_independently(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        console_status.reset_staleness_cache()
        calls: list[bool] = []

        def fake_ping(*, multi_user: bool = False):
            calls.append(multi_user)
            return {"version": console_status.__version__}

        monkeypatch.setattr(rpc, "ping", fake_ping)
        for _ in range(3):
            console_status.cached_staleness(multi_user=False)
            console_status.cached_staleness(multi_user=True)
        assert calls == [False, True], "one probe per mode within the TTL"

    def test_reset_clears_every_mode(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        console_status.reset_staleness_cache()
        calls: list[bool] = []

        def fake_ping(*, multi_user: bool = False):
            calls.append(multi_user)
            return {"version": console_status.__version__}

        monkeypatch.setattr(rpc, "ping", fake_ping)
        console_status.cached_staleness(multi_user=False)
        console_status.cached_staleness(multi_user=True)
        console_status.reset_staleness_cache()
        console_status.cached_staleness(multi_user=False)
        console_status.cached_staleness(multi_user=True)
        assert calls == [False, True, False, True]


class TestStalenessCacheTimestamp:
    def test_entry_is_stamped_after_the_probe_not_before(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A probe that blocks must not age its own cache entry.

        Stamping with the pre-probe clock reading meant a slow probe
        stored an entry that was already expired, so the next render
        probed again -- turning one slow daemon into a probe per page.
        """
        console_status.reset_staleness_cache()
        clock = {"t": 1000.0}
        calls: list[int] = []

        def fake_monotonic() -> float:
            return clock["t"]

        def slow_ping(*, multi_user: bool = False):
            calls.append(1)
            # The probe blocks for longer than the whole TTL.
            clock["t"] += console_status.STALENESS_TTL_SECONDS * 2
            return {"version": console_status.__version__}

        monkeypatch.setattr(console_status, "monotonic", fake_monotonic)
        monkeypatch.setattr(rpc, "ping", slow_ping)

        console_status.cached_staleness()
        console_status.cached_staleness()
        assert calls == [1], (
            "the second call must be served from cache; the entry was "
            "stamped when the probe returned, not when it started"
        )


class TestSourceDriftDetection:
    """Catch a console whose source was replaced underneath it.

    The version comparison catches a console left behind by an upgrade.
    It cannot catch what happened on 2026-08-05: a checkout move deleted
    a running console's source while both sides still reported 0.24.0.
    A digest captured at import, compared against the digest on disk now,
    is what notices -- a running process keeps executing what it already
    imported.
    """

    def test_unchanged_source_is_not_drift(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        console_status.reset_staleness_cache()
        monkeypatch.setattr(console_status, "_STARTED_TREE_DIGEST", "abc123")
        monkeypatch.setattr(console_status, "_tree_digest", lambda: "abc123")
        monkeypatch.setattr(
            rpc, "ping", lambda **_k: {"version": console_status.__version__}
        )
        assert console_status.console_staleness().stale is False

    def test_changed_source_is_drift_even_at_the_same_version(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        console_status.reset_staleness_cache()
        monkeypatch.setattr(console_status, "_STARTED_TREE_DIGEST", "abc123def456")
        monkeypatch.setattr(console_status, "_tree_digest", lambda: "999888777666")
        monkeypatch.setattr(
            rpc, "ping", lambda **_k: {"version": console_status.__version__}
        )
        verdict = console_status.console_staleness()
        assert verdict.stale is True
        assert "changed on disk" in (verdict.reason or "")
        assert "abc123def456"[:12] in (verdict.reason or "")
        assert "999888777666"[:12] in (verdict.reason or "")

    def test_undrivable_startup_digest_never_alarms(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A console that cannot tell must not claim it knows."""
        console_status.reset_staleness_cache()
        monkeypatch.setattr(console_status, "_STARTED_TREE_DIGEST", None)
        monkeypatch.setattr(console_status, "_tree_digest", lambda: "whatever")
        monkeypatch.setattr(
            rpc, "ping", lambda **_k: {"version": console_status.__version__}
        )
        assert console_status.console_staleness().stale is False

    def test_undrivable_current_digest_never_alarms(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        console_status.reset_staleness_cache()
        monkeypatch.setattr(console_status, "_STARTED_TREE_DIGEST", "abc123")
        monkeypatch.setattr(console_status, "_tree_digest", lambda: None)
        monkeypatch.setattr(
            rpc, "ping", lambda **_k: {"version": console_status.__version__}
        )
        assert console_status.console_staleness().stale is False

    def test_source_drift_is_reported_with_no_daemon_at_all(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Source drift is self-observable: it needs no daemon to confirm."""
        console_status.reset_staleness_cache()
        monkeypatch.setattr(console_status, "_STARTED_TREE_DIGEST", "aaa111bbb222")
        monkeypatch.setattr(console_status, "_tree_digest", lambda: "ccc333ddd444")
        monkeypatch.setattr(rpc, "ping", lambda **_k: None)
        verdict = console_status.console_staleness()
        assert verdict.stale is True
        assert verdict.daemon_version is None

    def test_version_drift_still_wins_the_message(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A version mismatch is the more specific diagnosis; keep it."""
        console_status.reset_staleness_cache()
        monkeypatch.setattr(console_status, "_STARTED_TREE_DIGEST", "aaa")
        monkeypatch.setattr(console_status, "_tree_digest", lambda: "bbb")
        monkeypatch.setattr(rpc, "ping", lambda **_k: {"version": "9.9.9"})
        verdict = console_status.console_staleness()
        assert verdict.stale is True
        assert "9.9.9" in (verdict.reason or "")
