"""The session store's lock budget has to survive real worker contention (#60).

`vq web` workers are separate processes, so the session store's writers really
do contend: the docstring on ``_auth_db`` says as much. The budget for that
contention was 2 s, and CI crossed it -- pipeline 5703 job 12025 failed
``test_concurrent_first_sessions_share_one_complete_secret`` with
``sqlite3.OperationalError: database is locked`` on a commit that was otherwise
healthy, and a hand retry of the same job at the same SHA passed.

Reproducing that by waiting for a loaded runner is not a test. These pin the
mechanism instead: the configured budget reaches the connection, a lock held
past it produces exactly that error, the same lock released inside it does not,
and the shipped value is above the one CI exceeded.

The store's hardening is pinned here too, because raising a timeout must not be
a way to quietly change what ``_auth_db`` will open. Its 0700 directory and
0600 mode already have a test; the link count and ``trusted_schema`` did not.
"""
from __future__ import annotations

import sqlite3
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import pytest

from vq import auth, config, paths
from vq.web import authn

# The budget in place when CI failed. The shipped value has to clear it.
_BUDGET_CI_EXCEEDED_SECONDS = 2.0

# Long enough that a short budget expires against it, short enough to stay a
# bounded wait. Both directions are asserted against the same hold.
_LOCK_HOLD_SECONDS = 0.6


@pytest.fixture
def web_state(tmp_path, monkeypatch):
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "config"))
    monkeypatch.delenv(auth.ENV_WEB_TOKEN_FILE, raising=False)
    monkeypatch.setenv("VQ_WEB_FLEET", "1")
    paths.queue_dir().mkdir(parents=True)
    paths.jobs_dir().mkdir(parents=True)
    authn.add_user("alice", "correct", "operator")
    return tmp_path


def _session_db() -> Path:
    return config.config_dir() / "web-auth" / "sessions.sqlite3"


@contextmanager
def _write_lock_held(seconds: float) -> Iterator[None]:
    """Hold the store's write lock for ``seconds``, as another worker would.

    A second connection is a second writer as far as SQLite is concerned, in
    this process or any other, so this is the contention ``_auth_db`` budgets
    for without needing a second interpreter.
    """
    acquired = threading.Event()
    released = threading.Event()
    failure: list[BaseException] = []

    def hold() -> None:
        # The connection is created, used and closed in this thread: sqlite3
        # objects are not shareable across threads by default.
        try:
            holder = sqlite3.connect(_session_db(), timeout=0)
            try:
                holder.execute("BEGIN IMMEDIATE")
                acquired.set()
                time.sleep(seconds)
                holder.rollback()
            finally:
                holder.close()
        except BaseException as exc:  # noqa: BLE001 - re-raised in the caller
            failure.append(exc)
        finally:
            acquired.set()
            released.set()

    thread = threading.Thread(target=hold, daemon=True)
    thread.start()
    # Bounded for liveness only; the hold itself is the timing claim.
    assert acquired.wait(30), "the write lock was never taken"
    if failure:
        raise failure[0]
    try:
        yield
    finally:
        released.wait(seconds + 30)
        thread.join(timeout=30)
        if failure:
            raise failure[0]


class TestTheBudgetReachesSQLite:
    def test_the_connection_carries_the_configured_budget(
        self, web_state,
    ) -> None:
        """``connect(timeout=...)`` is the connection's ``busy_timeout``.

        So it already covers every statement on the connection, not only the
        first lock acquisition, and a separate ``PRAGMA busy_timeout`` would be
        the same value written twice. This pins that the constant is what
        actually arrives.
        """
        authn.issue_session("alice", "operator")

        with authn._auth_db() as db:
            budget_ms = db.execute("PRAGMA busy_timeout").fetchone()[0]

        assert budget_ms == int(authn.SESSION_DB_BUSY_TIMEOUT_SECONDS * 1000)

    def test_the_shipped_budget_clears_the_one_ci_exceeded(self) -> None:
        """A regression guard on the number itself."""
        assert (
            authn.SESSION_DB_BUSY_TIMEOUT_SECONDS > _BUDGET_CI_EXCEEDED_SECONDS
        )


class TestTheBudgetIsWhatDecidesTheOutcome:
    def test_a_lock_held_past_the_budget_reports_database_is_locked(
        self, web_state, monkeypatch,
    ) -> None:
        """The reported failure, made deterministic.

        This is the CI error reproduced without a loaded runner: the budget,
        not the workload, is what turns contention into an exception.
        """
        authn.issue_session("alice", "operator")
        monkeypatch.setattr(
            authn, "SESSION_DB_BUSY_TIMEOUT_SECONDS", _LOCK_HOLD_SECONDS / 4,
        )

        with (
            _write_lock_held(_LOCK_HOLD_SECONDS),
            pytest.raises(sqlite3.OperationalError, match="database is locked"),
        ):
            authn.issue_session("alice", "operator")

    def test_the_same_lock_released_inside_the_budget_does_not_fail(
        self, web_state, monkeypatch,
    ) -> None:
        """Same hold, bigger budget: the caller waits instead of failing."""
        first = authn.issue_session("alice", "operator")
        monkeypatch.setattr(
            authn, "SESSION_DB_BUSY_TIMEOUT_SECONDS", _LOCK_HOLD_SECONDS + 10.0,
        )

        with _write_lock_held(_LOCK_HOLD_SECONDS):
            second = authn.issue_session("alice", "operator")

        assert second != first
        assert authn.verify_session(second)
        assert authn.verify_session(first)

    def test_the_shipped_budget_survives_that_hold(
        self, web_state,
    ) -> None:
        """The value actually shipped, against a lock held past the old one."""
        authn.issue_session("alice", "operator")

        with _write_lock_held(_BUDGET_CI_EXCEEDED_SECONDS + 0.3):
            token = authn.issue_session("alice", "operator")

        assert authn.verify_session(token)


class TestRaisingTheBudgetChangedNothingElse:
    def test_the_store_is_still_opened_under_its_hardening(
        self, web_state,
    ) -> None:
        """The issue's own check: confirm ``_auth_db``'s guarantees hold."""
        authn.issue_session("alice", "operator")
        directory = config.config_dir() / "web-auth"
        db = _session_db()

        assert directory.stat().st_mode & 0o777 == 0o700
        assert db.stat().st_mode & 0o777 == 0o600
        assert db.stat().st_nlink == 1
        assert not db.is_symlink()
        with authn._auth_db() as connection:
            assert connection.execute("PRAGMA trusted_schema").fetchone()[0] == 0

    @pytest.mark.parametrize("damage", ["wide", "hardlink"])
    def test_an_unsafe_store_is_still_refused(
        self, web_state, tmp_path, damage,
    ) -> None:
        """A wider mode or a second link still fails closed, not open."""
        token = authn.issue_session("alice", "operator")
        db = _session_db()
        if damage == "wide":
            db.chmod(0o644)
        else:
            (tmp_path / "second-link").hardlink_to(db)

        assert authn.verify_session(token) is None

    def test_no_journal_sidecar_is_left_beside_the_store(
        self, web_state,
    ) -> None:
        """Rollback journalling, so the hardening covers every file that exists.

        WAL was considered for #60 and deliberately not taken: it adds ``-wal``
        and ``-shm`` sidecars, and ``_auth_db`` guards the database and its
        journal sidecars against untrusted replacement, so that hardening would
        have to be extended in the same change. This fails if the journal mode
        is ever switched without revisiting it.
        """
        authn.issue_session("alice", "operator")
        directory = config.config_dir() / "web-auth"

        assert sorted(p.name for p in directory.iterdir()) == ["sessions.sqlite3"]
        with authn._auth_db() as db:
            assert db.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
