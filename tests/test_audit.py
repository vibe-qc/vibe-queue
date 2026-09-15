"""v0.8.6 *Codd's Audit* — RPC audit-trail tests.

Pins the v0.8.6 contract:

1. **Every `set_*` call appends one line.**
2. **Read calls don't log** (ping, get_methods, get_admin_status).
3. **Failed calls still audit** (forensically interesting).
4. **Schema is stable** — ``ts``, ``method``, ``uid``, ``ok``,
   ``args_summary`` always present.
5. **Args summary captures the operator-meaningful bits** without
   ever leaking tokens or full state dicts.
6. **Audit path follows the multi-user convention** —
   `state_root` single-user, `multi_user_root` multi-user.
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
import time
from pathlib import Path

import pytest

from vq import admin, audit, drain, paths, rpc, throttle


@pytest.fixture
def state_dir(monkeypatch: pytest.MonkeyPatch) -> Path:
    """Short tempdir for AF_UNIX socket paths (~104 byte limit on
    macOS). Same shape as test_rpc.py's fixture."""
    tmpdir = Path(
        tempfile.mkdtemp(
            prefix="vqaudit-",
            dir=os.environ.get("VQ_TEST_SHORT_TMPDIR"),
        )
    )
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmpdir / "state"))
    (tmpdir / "state").mkdir()
    yield tmpdir
    shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.fixture
def running_server(state_dir: Path) -> rpc.RPCServer:
    """A running server with admin-status + drain + throttle
    methods registered. Audit fires for set_* calls automatically."""
    server = rpc.RPCServer(multi_user=False)
    rpc.register_get_admin_status_method(
        server,
        lambda: admin.read_admin_status(via_rpc=False),
    )
    rpc.register_set_admin_status_method(
        server,
        admin.replace_admin_status_record_from_mapping,
    )
    rpc.register_get_drain_state_method(
        server,
        lambda: drain.read_drain_state(
            via_rpc=False,
            multi_user=server.multi_user,
        ),
    )
    rpc.register_get_scheduler_drain_leases_method(
        server,
        lambda: drain.read_scheduler_drain_leases(
            via_rpc=False,
            multi_user=server.multi_user,
        ),
        schema_version=drain.SCHEDULER_DRAIN_LEASE_SCHEMA_VERSION,
    )
    rpc.register_legacy_scheduler_drain_release_method(
        server,
        lambda host, expected_reason, expected_set_at: (
            drain.release_legacy_scheduler_host(
                host,
                via_rpc=False,
                expected_reason=expected_reason,
                expected_set_at=expected_set_at,
                multi_user=server.multi_user,
            )
        ),
    )
    rpc.register_set_drain_state_method(
        server,
        clear_state=lambda: drain.clear_drain(
            via_rpc=False,
            multi_user=server.multi_user,
        ),
        replace_state=lambda state: drain.replace_drain_state_from_mapping(
            state,
            multi_user=server.multi_user,
        ),
    )
    rpc.register_set_scheduler_drain_lease_method(
        server,
        lambda lease, release_id, release_host, release_owner, release_all: (
            drain.apply_scheduler_drain_lease_mapping_mutation(
                lease=lease,
                release_id=release_id,
                release_host=release_host,
                release_owner=release_owner,
                release_all=release_all,
                multi_user=server.multi_user,
            )
        ),
        schema_version=drain.SCHEDULER_DRAIN_LEASE_SCHEMA_VERSION,
    )
    rpc.register_get_throttle_state_method(
        server,
        lambda: throttle.read_throttle_state(via_rpc=False),
    )
    rpc.register_set_throttle_state_method(
        server,
        clear_state=lambda: throttle.clear_throttle_state(via_rpc=False),
        replace_state=throttle.replace_throttle_state_from_mapping,
    )
    server.start()
    time.sleep(0.05)
    yield server
    server.stop()


# ----------------------------------------------------------------------
# Audit path resolution
# ----------------------------------------------------------------------


class TestAuditPath:
    def test_single_user_path(self, state_dir: Path) -> None:
        p = audit.audit_log_path(multi_user=False)
        assert p == paths.state_root() / "rpc-audit.jsonl"

    def test_multi_user_path(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        tmpdir = Path(
            tempfile.mkdtemp(
                prefix="vqaudit-",
                dir=os.environ.get("VQ_TEST_SHORT_TMPDIR"),
            )
        )
        try:
            monkeypatch.setenv(
                paths.ENV_MULTI_USER_ROOT, str(tmpdir / "mu"),
            )
            (tmpdir / "mu").mkdir()
            p = audit.audit_log_path(multi_user=True)
            assert p == tmpdir / "mu" / "rpc-audit.jsonl"
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)


# ----------------------------------------------------------------------
# What gets logged + what doesn't
# ----------------------------------------------------------------------


class TestSetMethodsAreAudited:
    def test_set_drain_state_appends_line(
        self, running_server: rpc.RPCServer,
    ) -> None:
        rpc.call("set_drain_state", {
            "state": {"enabled": True, "reason": "audit-test"},
        })
        lines = audit.read_audit_log()
        assert len(lines) == 1
        assert lines[0]["method"] == "set_drain_state"
        assert lines[0]["ok"] is True
        assert "set" in lines[0]["args_summary"]

    def test_set_throttle_state_appends_line(
        self, running_server: rpc.RPCServer,
    ) -> None:
        rpc.call("set_throttle_state", {"state": {"weight": 25}})
        lines = audit.read_audit_log()
        assert len(lines) == 1
        assert lines[0]["method"] == "set_throttle_state"
        assert "weight=25" in lines[0]["args_summary"]

    def test_set_admin_status_appends_line(
        self, running_server: rpc.RPCServer,
    ) -> None:
        rpc.call("set_admin_status", {
            "env": "vibeqc-dev",
            "record": {
                "last_updated_at": "2026-05-29T10:00:00+00:00",
                "last_success": True,
            },
        })
        lines = audit.read_audit_log()
        assert len(lines) == 1
        assert lines[0]["method"] == "set_admin_status"
        assert "env=vibeqc-dev" in lines[0]["args_summary"]

    def test_scheduler_lease_audit_names_exact_owner_and_claim(
        self, running_server: rpc.RPCServer,
    ) -> None:
        rpc.call(
            drain.SCHEDULER_DRAIN_LEASE_RPC_METHOD,
            {
                "schema_version": 1,
                "lease": {
                    "lease_id": "rollout-lease-a",
                    "scheduler_host": "host_f",
                    "owner": "fleet-rollout:run-a:host_f",
                },
            },
        )

        line = audit.read_audit_log()[0]
        assert line["method"] == drain.SCHEDULER_DRAIN_LEASE_RPC_METHOD
        assert line["ok"] is True
        assert "host=host_f" in line["args_summary"]
        assert "owner=fleet-rollout:run-a:host_f" in line["args_summary"]
        assert "lease_id=rollout-lease-a" in line["args_summary"]

    def test_legacy_scheduler_release_audit_names_host_only(
        self,
        running_server: rpc.RPCServer,
    ) -> None:
        drain.write_drain_state(
            drain.DrainState(scheduler_hosts=["host_f"]),
            via_rpc=False,
        )

        rpc.call(
            drain.LEGACY_SCHEDULER_DRAIN_RELEASE_RPC_METHOD,
            {"host": "host_f", "token": "must-not-be-logged"},
        )

        line = audit.read_audit_log()[0]
        assert line["method"] == drain.LEGACY_SCHEDULER_DRAIN_RELEASE_RPC_METHOD
        assert line["ok"] is True
        assert line["args_summary"] == "release legacy host=host_f"
        assert "token" not in line["args_summary"]

    def test_owned_full_release_is_audited_without_token_value(
        self,
        state_dir: Path,
    ) -> None:
        server = rpc.RPCServer(multi_user=False)
        rpc.register_owned_full_drain_release_method(
            server,
            lambda _reason, _set_at: True,
        )
        try:
            server.start()
            time.sleep(0.05)
            result = rpc.call(
                drain.OWNED_FULL_DRAIN_RELEASE_RPC_METHOD,
                {
                    "expected_reason": "fleet-rollout:operation-123",
                    "expected_set_at": "2026-08-10T16:00:00+00:00",
                    "token": "must-never-be-audited",
                },
            )
        finally:
            server.stop()

        assert result == {"changed": True, "ok": True}
        lines = audit.read_audit_log()
        assert len(lines) == 1
        assert lines[0]["method"] == "set_owned_full_drain_release"
        assert lines[0]["ok"] is True
        assert "must-never-be-audited" not in lines[0]["args_summary"]
        assert lines[0]["args_summary"] == (
            "args=expected_reason,expected_set_at"
        )

    def test_clear_via_set_none_logged_as_clear(
        self, running_server: rpc.RPCServer,
    ) -> None:
        """``set_drain_state(state=None)`` is the clear path; the
        audit log says so explicitly."""
        rpc.call("set_drain_state", {"state": None})
        lines = audit.read_audit_log()
        assert len(lines) == 1
        assert lines[0]["args_summary"] == "clear"

    def test_multiple_set_calls_stack(
        self, running_server: rpc.RPCServer,
    ) -> None:
        rpc.call("set_drain_state", {"state": {"enabled": True}})
        rpc.call("set_throttle_state", {"state": {"weight": 30}})
        rpc.call("set_drain_state", {"state": None})
        lines = audit.read_audit_log()
        assert len(lines) == 3
        assert [entry["method"] for entry in lines] == [
            "set_drain_state", "set_throttle_state", "set_drain_state",
        ]


class TestReadMethodsAreNotAudited:
    def test_ping_does_not_log(
        self, running_server: rpc.RPCServer,
    ) -> None:
        rpc.call("ping")
        assert audit.read_audit_log() == []

    def test_get_methods_does_not_log(
        self, running_server: rpc.RPCServer,
    ) -> None:
        rpc.call("get_methods")
        assert audit.read_audit_log() == []

    def test_get_drain_state_does_not_log(
        self, running_server: rpc.RPCServer,
    ) -> None:
        rpc.call("get_drain_state")
        rpc.call("get_admin_status")
        rpc.call("get_throttle_state")
        assert audit.read_audit_log() == []


# ----------------------------------------------------------------------
# Failed calls still audit (forensically interesting)
# ----------------------------------------------------------------------


class TestFailedCallsLogged:
    def test_bad_args_to_set_state_logged_with_error(
        self, running_server: rpc.RPCServer,
    ) -> None:
        """A set_* call that fails for any reason still leaves a
        line — the failure attempt is what forensics cares about."""
        with pytest.raises(rpc.RPCError):
            rpc.call("set_drain_state", {"wrong_arg": "x"})
        lines = audit.read_audit_log()
        assert len(lines) == 1
        assert lines[0]["ok"] is False
        assert "error" in lines[0]


# ----------------------------------------------------------------------
# Schema stability
# ----------------------------------------------------------------------


class TestSchemaStable:
    def test_required_fields_always_present(
        self, running_server: rpc.RPCServer,
    ) -> None:
        rpc.call("set_drain_state", {"state": {"enabled": True}})
        line = audit.read_audit_log()[0]
        required = {"ts", "method", "uid", "ok", "args_summary"}
        assert required.issubset(set(line.keys()))

    def test_ts_is_iso8601(
        self, running_server: rpc.RPCServer,
    ) -> None:
        from datetime import datetime
        rpc.call("set_drain_state", {"state": {"enabled": True}})
        line = audit.read_audit_log()[0]
        # Should parse without error.
        datetime.fromisoformat(line["ts"])

    def test_no_tokens_in_args_summary(
        self, running_server: rpc.RPCServer,
    ) -> None:
        """Tokens are NEVER in the audit summary — the whole point
        of having an audit trail is being able to share it without
        leaking credentials."""
        rpc.call("set_drain_state", {
            "state": {"enabled": True, "reason": "x"},
            "token": "very-secret-bearer-token",
        })
        line = audit.read_audit_log()[0]
        assert "very-secret-bearer-token" not in json.dumps(line)


# ----------------------------------------------------------------------
# Direct audit module API
# ----------------------------------------------------------------------


class TestAuditModuleAPI:
    def test_append_then_read_roundtrip(
        self, state_dir: Path,
    ) -> None:
        audit.append_audit_line(
            method="set_drain_state",
            uid=1000,
            ok=True,
            args_summary="set full",
        )
        lines = audit.read_audit_log()
        assert len(lines) == 1
        assert lines[0]["method"] == "set_drain_state"
        assert lines[0]["uid"] == 1000

    def test_read_with_no_file(self, state_dir: Path) -> None:
        assert audit.read_audit_log() == []

    def test_corrupt_lines_skipped_silently(
        self, state_dir: Path,
    ) -> None:
        """A partial / corrupt line shouldn't take down the read."""
        path = audit.audit_log_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            '{"ts": "2026-05-29T10:00:00+00:00", "method": "set_x"}\n'
            'this is not json\n'
            '{"ts": "2026-05-29T11:00:00+00:00", "method": "set_y"}\n'
        )
        lines = audit.read_audit_log()
        assert len(lines) == 2
        assert [entry["method"] for entry in lines] == ["set_x", "set_y"]

    def test_summarise_args_redacts_tokens(self) -> None:
        """Unknown set_* method args summary excludes 'token' key."""
        summary = audit.summarise_args(
            "set_future_method",
            {"token": "secret", "config": "...", "weight": 10},
        )
        assert "token" not in summary
        assert "config" in summary or "weight" in summary
