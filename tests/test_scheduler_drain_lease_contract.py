"""M1 contract for independently owned scheduler drain leases.

The legacy ``drain.json`` document remains the compatibility surface for
whole-host and partial local drains.  Scheduler maintenance leases need a
separate, daemon-owned atomic mutation path so two cluster updaters cannot
overwrite each other's ownership and an older whole-object writer cannot lift
a newer scoped lease.

The characterization tests in this file pass on the pre-M1 implementation.
The defect regressions deliberately fail there and are intended to turn green
one production increment at a time.
"""
from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from vq import admin, auth, config, drain, paths, rpc

_LEASE_MUTATION_RPC_METHOD = getattr(
    drain,
    "SCHEDULER_DRAIN_LEASE_RPC_METHOD",
    "set_scheduler_drain_lease",
)


@pytest.fixture
def state_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Isolated single-user state with no daemon or live RPC socket."""
    root = tmp_path / "state"
    cfg = tmp_path / "config"
    root.mkdir()
    cfg.mkdir()
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(root))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfg))
    return root


def _read_direct() -> drain.DrainState | None:
    return drain.read_drain_state(via_rpc=False)


class TestLegacyDrainCompatibility:
    """Existing whole/partial drain behavior stays readable and unchanged."""

    @pytest.mark.parametrize(
        ("payload", "is_full", "max_jobs", "max_cpus"),
        (
            (
                {
                    "enabled": True,
                    "max_jobs": None,
                    "max_cpus": None,
                    "set_at": "2026-08-08T12:00:00+00:00",
                    "reason": "legacy full drain",
                },
                True,
                None,
                None,
            ),
            (
                {
                    "enabled": True,
                    "max_jobs": 2,
                    "max_cpus": 8,
                    "set_at": "2026-08-08T12:00:00+00:00",
                    "reason": "legacy partial drain",
                },
                False,
                2,
                8,
            ),
        ),
    )
    def test_legacy_whole_document_remains_readable(
        self,
        state_root: Path,
        payload: dict[str, object],
        is_full: bool,
        max_jobs: int | None,
        max_cpus: int | None,
    ) -> None:
        (state_root / drain.DRAIN_FILENAME).write_text(json.dumps(payload))

        state = _read_direct()

        assert state is not None
        assert state.is_full_drain is is_full
        assert state.max_jobs == max_jobs
        assert state.max_cpus == max_cpus

    def test_legacy_release_one_lane_preserves_the_other(
        self, state_root: Path
    ) -> None:
        _ = state_root
        drain.write_drain_state(
            drain.DrainState(scheduler_hosts=["host_c", "host_f"]),
            via_rpc=False,
        )

        assert drain.release_scheduler_host("host_c", via_rpc=False) is True

        state = _read_direct()
        assert state is not None
        assert state.scheduler_hosts == ["host_f"]

    def test_drain_wait_is_a_deadline_not_a_persisted_lease_ttl(
        self,
        state_root: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _ = state_root
        captured: dict[str, object] = {}

        def record_add(host: str, **kwargs: object) -> bool:
            captured["host"] = host
            captured.update(kwargs)
            return True

        def record_acquire(
            host: str, **kwargs: object
        ) -> tuple[SimpleNamespace, bool]:
            captured["host"] = host
            captured.update(kwargs)
            return SimpleNamespace(lease_id="test-lease"), True

        monkeypatch.setattr(drain, "add_scheduler_host", record_add)
        if hasattr(drain, "acquire_scheduler_drain_lease"):
            monkeypatch.setattr(
                drain,
                "acquire_scheduler_drain_lease",
                record_acquire,
            )
        monkeypatch.setattr(admin.os, "getpid", lambda: 43123)

        assert admin._take_scheduler_drain_lane(  # noqa: SLF001 - contract seam
            "host_c",
            drain_wait_seconds=4 * 60 * 60,
            lease_id="test-lease",
            admin_token="scheduler-secret",
        )
        assert captured["host"] == "host_c"
        assert captured["owner_pid"] == 43123
        assert captured["reason"] == "vq admin update host_c"
        assert captured["token"] == "scheduler-secret"
        assert "duration_seconds" not in captured
        assert "ttl_seconds" not in captured
        assert "expires_at" not in captured

    def test_release_failure_names_exact_owner_recovery(
        self,
        state_root: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _ = state_root

        def fail_release(_lease_id: str, **_kwargs: object) -> bool:
            raise drain.SchedulerDrainLeaseError("daemon response lost")

        monkeypatch.setattr(
            drain,
            "release_scheduler_drain_lease",
            fail_release,
        )
        owner = "admin-update:host_f:requested-id"

        error = admin._release_scheduler_drain_lane(  # noqa: SLF001
            "host_f",
            "echoed-id",
            lease_owner=owner,
            admin_token="scheduler-secret",
            recovery_driver="driver",
        )

        assert error is not None
        assert (
            "--lease-owner admin-update:host_f:requested-id localhost" in error
        )
        assert "run on scheduler driver 'driver'" in error
        assert "broad scheduler-host release" in error
        assert "--scheduler-host host_f --release`" not in error

    def test_admin_take_and_release_keep_one_daemon_scope(
        self,
        state_root: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _ = state_root
        observed: list[tuple[str, bool | None]] = []

        def acquire(_host: str, **kwargs: object):
            observed.append(("acquire", kwargs.get("multi_user")))
            return SimpleNamespace(lease_id="stable-scope-lease"), True

        def release(_lease_id: str, **kwargs: object) -> bool:
            observed.append(("release", kwargs.get("multi_user")))
            return True

        monkeypatch.setattr(drain, "acquire_scheduler_drain_lease", acquire)
        monkeypatch.setattr(drain, "release_scheduler_drain_lease", release)

        lease_id = admin._take_scheduler_drain_lane(  # noqa: SLF001
            "host_f",
            drain_wait_seconds=60,
            lease_id="stable-scope-lease",
            multi_user=True,
        )
        assert lease_id == "stable-scope-lease"
        assert admin._release_scheduler_drain_lane(  # noqa: SLF001
            "host_f",
            lease_id,
            multi_user=True,
        ) is None
        assert observed == [("acquire", True), ("release", True)]


class TestIndependentSchedulerLeaseRegressions:
    """Defects that the pre-M1 shared owner/reason document cannot satisfy."""

    def test_releasing_host_c_projects_the_surviving_host_f_owner(
        self,
        state_root: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _ = state_root
        host_c_pid = 41001
        host_f_pid = 41002
        start_times = {host_c_pid: 101, host_f_pid: 202}
        monkeypatch.setattr(
            drain, "_pid_start_time", lambda pid: start_times.get(pid)
        )

        acquire = getattr(drain, "acquire_scheduler_drain_lease", None)
        release = getattr(drain, "release_scheduler_drain_lease", None)
        read_leases = getattr(drain, "read_scheduler_drain_leases", None)
        read_effective = getattr(drain, "read_effective_drain_state", None)
        assert callable(acquire), "owner-scoped lease acquisition is missing"
        assert callable(release), "owner-scoped lease release is missing"
        assert callable(read_leases), "owner-scoped lease reads are missing"
        assert callable(read_effective), "effective drain composition is missing"

        host_c, host_c_added = acquire(
            "host_c",
            reason="vq admin update host_c",
            owner="admin-update:host_c:test",
            owner_pid=host_c_pid,
            lease_id="host_c-lease",
            via_rpc=False,
        )
        host_f, host_f_added = acquire(
            "host_f",
            reason="vq admin update host_f",
            owner="admin-update:host_f:test",
            owner_pid=host_f_pid,
            lease_id="host_f-lease",
            via_rpc=False,
        )
        assert host_c_added is True
        assert host_f_added is True
        assert release(host_c.lease_id, via_rpc=False) is True

        leases = read_leases(via_rpc=False)
        assert [lease.lease_id for lease in leases] == [host_f.lease_id]
        assert leases[0].scheduler_host == "host_f"
        assert leases[0].reason == "vq admin update host_f"
        assert leases[0].owner_pid == host_f_pid
        assert leases[0].owner_pid_start_time == start_times[host_f_pid]

        state = read_effective(via_rpc=False)
        assert state is not None
        assert state.drains_scheduler_target("host_c") is False
        assert state.drains_scheduler_target("host_f") is True

    def test_stale_legacy_whole_object_write_cannot_erase_scoped_lease(
        self, state_root: Path
    ) -> None:
        _ = state_root
        acquire = getattr(drain, "acquire_scheduler_drain_lease", None)
        read_effective = getattr(drain, "read_effective_drain_state", None)
        assert callable(acquire), "owner-scoped lease acquisition is missing"
        assert callable(read_effective), "effective drain composition is missing"

        lease, added = acquire(
            "host_c",
            reason="vq admin update host_c",
            owner="admin-update:host_c:test",
            owner_pid=42001,
            lease_id="host_c-lease",
            via_rpc=False,
        )
        assert lease.lease_id == "host_c-lease"
        assert added is True

        # Model an old client that read before the lease existed and later
        # replaces the whole legacy document.  The local cap update is valid,
        # but it has no authority to remove the independently owned lease.
        stale_old_writer = drain.DrainState(
            enabled=True,
            max_jobs=1,
            reason="legacy partial drain",
        )
        drain.write_drain_state(stale_old_writer, via_rpc=False)

        state = read_effective(via_rpc=False)
        assert state is not None
        assert state.max_jobs == 1
        assert state.drains_scheduler_target("host_c") is True

    def test_sidecar_lease_ignores_disabled_legacy_policy(
        self,
        state_root: Path,
    ) -> None:
        _ = state_root
        drain.write_drain_state(
            drain.DrainState(
                enabled=False,
                full_dispatch=True,
                max_jobs=1,
                scheduler_hosts=["host_c"],
                reject_submits=True,
                update_mode="deny",
                reason="stale disabled record",
                owner_pid=9876,
            ),
            via_rpc=False,
        )
        drain.acquire_scheduler_drain_lease(
            "host_f",
            owner="rollout:test",
            lease_id="host_f-lease",
            via_rpc=False,
        )

        state = drain.read_effective_drain_state(via_rpc=False)
        payload = drain.status_payload()

        assert state is not None
        assert state.enabled is True
        assert state.is_scheduler_target_drain is True
        assert state.drains_scheduler_target("host_f") is True
        assert state.drains_scheduler_target("host_c") is False
        assert state.is_full_drain is False
        assert state.max_jobs is None
        assert state.reject_submits is False
        assert state.update_mode is None
        assert payload["active"] is True
        assert payload["is_scheduler_target_drain"] is True
        assert payload["mode"] == "scheduler-target"
        assert payload["legacy_scheduler_hosts"] == []
        assert payload["submit_policy"] == "accept_pending"
        assert payload["orphaned_lane_reason"] is None

    def test_response_loss_retry_with_same_id_returns_first_claim(
        self, state_root: Path
    ) -> None:
        _ = state_root
        first, first_changed = drain.acquire_scheduler_drain_lease(
            "host_f",
            owner="fleet-rollout:run:host_f",
            reason="first attempt",
            owner_pid=1234,
            lease_id="stable-lease-id",
            via_rpc=False,
        )
        retried, retry_changed = drain.acquire_scheduler_drain_lease(
            "host_f",
            owner="fleet-rollout:run:host_f",
            reason="response-loss retry",
            owner_pid=5678,
            lease_id="stable-lease-id",
            via_rpc=False,
        )

        assert first_changed is True
        assert retry_changed is False
        assert retried == first
        assert retried.reason == "first attempt"
        assert retried.owner_pid == 1234


class TestSchedulerLeaseMappingMutation:
    """Strict payload parsing belongs to the direct domain transaction."""

    def test_valid_mapping_is_parsed_and_written_strictly(
        self,
        state_root: Path,
    ) -> None:
        _ = state_root
        payload: dict[str, object] = {
            "lease_id": "mapping-lease",
            "scheduler_host": "host_f",
            "owner": "rollout:mapping",
            "owner_pid": 1234,
        }

        leases, acquired, changed = (
            drain.apply_scheduler_drain_lease_mapping_mutation(
                lease=payload,
                multi_user=False,
            )
        )

        assert changed is True
        assert acquired is not None
        assert acquired.lease_id == "mapping-lease"
        assert acquired.owner_pid == 1234
        assert [lease.lease_id for lease in leases] == ["mapping-lease"]
        assert payload == {
            "lease_id": "mapping-lease",
            "scheduler_host": "host_f",
            "owner": "rollout:mapping",
            "owner_pid": 1234,
        }

    def test_unknown_mapping_fields_are_sorted_and_do_not_touch_store(
        self,
        state_root: Path,
    ) -> None:
        store = state_root / drain.SCHEDULER_DRAIN_LEASES_FILENAME
        drain.acquire_scheduler_drain_lease(
            "host_c",
            owner="existing-owner",
            lease_id="existing-lease",
            via_rpc=False,
        )
        before = store.read_bytes()

        with pytest.raises(
            ValueError,
            match="unknown scheduler drain lease fields: alpha, zeta",
        ):
            drain.apply_scheduler_drain_lease_mapping_mutation(
                lease={
                    "lease_id": "invalid-lease",
                    "scheduler_host": "host_f",
                    "owner": "invalid-owner",
                    "zeta": 1,
                    "alpha": 2,
                },
                multi_user=False,
            )

        assert store.read_bytes() == before

    def test_coerced_known_mapping_field_does_not_touch_store(
        self,
        state_root: Path,
    ) -> None:
        store = state_root / drain.SCHEDULER_DRAIN_LEASES_FILENAME
        drain.acquire_scheduler_drain_lease(
            "host_c",
            owner="existing-owner",
            lease_id="existing-lease",
            via_rpc=False,
        )
        before = store.read_bytes()

        with pytest.raises(ValueError):
            drain.apply_scheduler_drain_lease_mapping_mutation(
                lease={
                    "lease_id": "coerced-lease",
                    "scheduler_host": "host_f",
                    "owner": "invalid-owner",
                    "owner_pid": True,
                },
                multi_user=False,
            )

        assert store.read_bytes() == before


class TestSchedulerLeaseRpcContract:
    """Capability negotiation and fail-closed scoped mutation."""

    def test_daemon_advertises_atomic_scheduler_lease_mutation(self) -> None:
        server = rpc.RPCServer(multi_user=False)
        rpc.register_legacy_scheduler_drain_release_method(
            server,
            lambda _host, _expected_reason, _expected_set_at: False,
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

        methods = server._handle_get_methods()["methods"]  # noqa: SLF001

        assert _LEASE_MUTATION_RPC_METHOD in methods
        assert drain.LEGACY_SCHEDULER_DRAIN_RELEASE_RPC_METHOD in methods

    def test_client_rejects_coerced_lease_list_response(
        self,
        state_root: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _ = state_root
        monkeypatch.setattr(config, "load_config", lambda: config.Config())
        monkeypatch.setattr(
            config, "system_multi_user_enabled", lambda: False
        )
        monkeypatch.setattr(
            rpc,
            "call",
            lambda *_args, **_kwargs: {
                "schema_version": 1,
                "leases": [
                    {
                        "lease_id": "coerced-owner",
                        "scheduler_host": "host_f",
                        "owner": "rollout:test",
                        "owner_pid": True,
                    }
                ],
            },
        )

        with pytest.raises(
            drain.SchedulerDrainLeaseError,
            match="invalid scheduler drain lease",
        ):
            drain.read_scheduler_drain_leases()

    def test_effective_snapshot_resolves_client_scope_once(
        self,
        state_root: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _ = state_root
        scope_calls: list[bool] = []
        rpc_scopes: list[tuple[str, bool]] = []

        def resolve_scope() -> bool:
            scope_calls.append(True)
            if len(scope_calls) > 1:
                raise AssertionError("client scope was resolved twice")
            return True

        def legacy_call(
            method: str,
            *,
            multi_user: bool,
            fallback: object,
        ) -> dict[str, object]:
            _ = fallback
            rpc_scopes.append((method, multi_user))
            return drain.DrainState(max_jobs=2).model_dump(mode="json")

        def lease_call(
            method: str,
            *_args: object,
            multi_user: bool,
            **_kwargs: object,
        ) -> dict[str, object]:
            rpc_scopes.append((method, multi_user))
            return {
                "schema_version": 1,
                "leases": [
                    {
                        "lease_id": "system-lease",
                        "scheduler_host": "host_f",
                        "owner": "rollout:test",
                    }
                ],
            }

        monkeypatch.setattr(drain, "_drain_multi_user", resolve_scope)
        monkeypatch.setattr(rpc, "try_rpc_or_fallback", legacy_call)
        monkeypatch.setattr(rpc, "call", lease_call)

        legacy, leases, error, effective = (
            drain.read_effective_drain_snapshot()
        )

        assert len(scope_calls) == 1
        assert rpc_scopes == [
            ("get_drain_state", True),
            (drain.SCHEDULER_DRAIN_LEASES_RPC_METHOD, True),
        ]
        assert legacy is not None and legacy.max_jobs == 2
        assert [lease.lease_id for lease in leases] == ["system-lease"]
        assert error is None
        assert effective is not None
        assert effective.scheduler_hosts == ["host_f"]

    @pytest.mark.parametrize(
        ("server_mode", "live_mode", "expected_reason"),
        (
            (True, False, "system daemon"),
            (False, True, "personal daemon"),
        ),
    )
    def test_rpc_server_keeps_its_startup_state_scope_after_config_flip(
        self,
        state_root: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        server_mode: bool,
        live_mode: bool,
        expected_reason: str,
    ) -> None:
        system_root = tmp_path / "system-state"
        system_root.mkdir()
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(system_root))
        personal_host = "personal-lane"
        system_host = "system-lane"
        personal_set_at = "2026-08-09T11:00:00+00:00"
        system_set_at = "2026-08-09T12:00:00+00:00"
        (state_root / drain.DRAIN_FILENAME).write_text(
            drain.DrainState(
                reason="personal daemon",
                set_at=personal_set_at,
                full_dispatch=True,
                scheduler_hosts=[personal_host],
            ).model_dump_json()
        )
        (system_root / drain.DRAIN_FILENAME).write_text(
            drain.DrainState(
                reason="system daemon",
                set_at=system_set_at,
                full_dispatch=True,
                scheduler_hosts=[system_host],
            ).model_dump_json()
        )
        monkeypatch.setattr(paths, "is_multi_user", lambda: live_mode)
        monkeypatch.setattr(auth, "verify_admin_token", lambda _token: True)
        server = rpc.RPCServer(multi_user=server_mode)
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

        state_payload = server._methods["get_drain_state"]()  # noqa: SLF001
        mutation = server._methods[  # noqa: SLF001
            drain.SCHEDULER_DRAIN_LEASE_RPC_METHOD
        ]
        mutation(
            schema_version=1,
            lease={
                "lease_id": "startup-scope",
                "scheduler_host": "host_f",
                "owner": "rollout:test",
            },
            token="admin-token",
        )
        lease_payload = server._methods[  # noqa: SLF001
            drain.SCHEDULER_DRAIN_LEASES_RPC_METHOD
        ]()
        release_legacy = server._methods[  # noqa: SLF001
            drain.LEGACY_SCHEDULER_DRAIN_RELEASE_RPC_METHOD
        ]
        selected_host = system_host if server_mode else personal_host
        selected_set_at = system_set_at if server_mode else personal_set_at
        preserved_host = personal_host if server_mode else system_host
        with pytest.raises(
            drain.SchedulerDrainLeaseError,
            match="changed after inspection",
        ):
            release_legacy(
                host=selected_host,
                expected_reason=expected_reason,
                expected_set_at="2000-01-01T00:00:00+00:00",
                token="admin-token",
            )
        release_result = release_legacy(
            host=f" {selected_host} ",
            expected_reason=expected_reason,
            expected_set_at=selected_set_at,
            token="admin-token",
        )

        assert state_payload is not None
        assert state_payload["reason"] == expected_reason
        assert lease_payload["schema_version"] == 1
        assert [
            lease["lease_id"] for lease in lease_payload["leases"]
        ] == ["startup-scope"]
        assert release_result == {"changed": True, "ok": True}
        selected_legacy = drain.read_drain_state(
            via_rpc=False,
            multi_user=server_mode,
        )
        preserved_legacy = drain.read_drain_state(
            via_rpc=False,
            multi_user=not server_mode,
        )
        assert selected_legacy is not None
        assert selected_legacy.scheduler_hosts == []
        assert preserved_legacy is not None
        assert preserved_legacy.scheduler_hosts == [preserved_host]
        set_legacy = server._methods["set_drain_state"]  # noqa: SLF001
        replace_result = set_legacy(
            state={
                "enabled": True,
                "full_dispatch": True,
                "reason": "startup-scope replacement",
                "future_field_unknown_to_daemon": "ignored",
            },
            token="admin-token",
        )
        assert replace_result == {"enabled": True, "ok": True}
        replaced_legacy = drain.read_drain_state(
            via_rpc=False,
            multi_user=server_mode,
        )
        assert replaced_legacy is not None
        assert replaced_legacy.reason == "startup-scope replacement"
        assert not hasattr(
            replaced_legacy,
            "future_field_unknown_to_daemon",
        )
        preserved_after_replace = drain.read_drain_state(
            via_rpc=False,
            multi_user=not server_mode,
        )
        assert preserved_after_replace == preserved_legacy
        clear_result = set_legacy(state=None, token="admin-token")
        assert clear_result == {"cleared": True, "ok": True}
        assert (
            drain.read_drain_state(
                via_rpc=False,
                multi_user=server_mode,
            )
            is None
        )
        assert (
            drain.read_drain_state(
                via_rpc=False,
                multi_user=not server_mode,
            )
            == preserved_legacy
        )
        expected = drain.read_scheduler_drain_leases(
            via_rpc=False,
            multi_user=server_mode,
        )
        other = drain.read_scheduler_drain_leases(
            via_rpc=False,
            multi_user=not server_mode,
        )
        assert [lease.lease_id for lease in expected] == ["startup-scope"]
        assert other == []

    def test_legacy_lane_migration_is_one_daemon_side_mutation(
        self,
        state_root: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _ = state_root
        calls: list[tuple[str, dict[str, Any] | None]] = []

        def atomic_call(
            method: str,
            args: dict[str, Any] | None = None,
            **_kwargs: object,
        ) -> object:
            calls.append((method, args))
            return {"changed": True, "ok": True}

        monkeypatch.setattr(rpc, "call", atomic_call)
        monkeypatch.setattr(config, "load_config", lambda: config.Config())
        monkeypatch.setattr(
            config, "system_multi_user_enabled", lambda: False
        )

        changed = drain.release_legacy_scheduler_host("host_f")

        assert changed is True
        assert calls == [
            (
                drain.LEGACY_SCHEDULER_DRAIN_RELEASE_RPC_METHOD,
                {
                    "host": "host_f",
                    "expected_reason": None,
                    "expected_set_at": None,
                    "token": None,
                },
            )
        ]

    def test_acquire_wraps_commit_response_transport_loss(
        self,
        state_root: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _ = state_root
        monkeypatch.setattr(
            drain,
            "_require_scheduler_lease_rpc",
            lambda **_kwargs: None,
        )
        monkeypatch.setattr(config, "load_config", lambda: config.Config())
        monkeypatch.setattr(
            config, "system_multi_user_enabled", lambda: False
        )
        monkeypatch.setattr(
            rpc,
            "call",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                ConnectionError("response timed out after commit")
            ),
        )

        with pytest.raises(
            drain.SchedulerDrainCapabilityError,
            match="refusing an unsafe direct-file fallback",
        ):
            drain.acquire_scheduler_drain_lease(
                "host_f",
                owner="rollout:test",
                lease_id="response-loss",
            )

    @pytest.mark.parametrize(
        "payload",
        (
            {"schema_version": True, "release_all": True},
            {
                "schema_version": 1,
                "release_all": True,
                "release_owner": "rollout:a",
            },
            {
                "schema_version": 1,
                "release_id": "lease-a",
                "release_owner": "rollout:a",
            },
            {
                "schema_version": 1,
                "release_host": "host_f",
                "release_all": True,
            },
            {
                "schema_version": 1,
                "lease": {
                    "lease_id": "coerced-owner",
                    "scheduler_host": "host_f",
                    "owner": "rollout:a",
                    "owner_pid": True,
                },
            },
            {
                "schema_version": 1,
                "lease": {
                    "lease_id": "unknown-field",
                    "scheduler_host": "host_f",
                    "owner": "rollout:a",
                    "future_b": 2,
                    "future_a": 1,
                },
            },
        ),
    )
    def test_daemon_rejects_ambiguous_or_coerced_mutations(
        self,
        state_root: Path,
        payload: dict[str, object],
    ) -> None:
        _ = state_root
        server = rpc.RPCServer(multi_user=False)
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
        handler = server._methods[_LEASE_MUTATION_RPC_METHOD]  # noqa: SLF001

        with pytest.raises((ValueError, drain.SchedulerDrainLeaseError)):
            handler(**payload)

        assert drain.read_scheduler_drain_leases(via_rpc=False) == []

    def test_store_mutation_preserves_future_fields(
        self,
        state_root: Path,
    ) -> None:
        store = state_root / drain.SCHEDULER_DRAIN_LEASES_FILENAME
        store.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "future_store_field": {"writer": 2},
                    "leases": [
                        {
                            "lease_id": "future-a",
                            "scheduler_host": "host_f",
                            "owner": "rollout:a",
                            "future_lease_field": "preserve-me",
                        }
                    ],
                }
            )
        )

        drain.acquire_scheduler_drain_lease(
            "host_c",
            owner="rollout:b",
            lease_id="future-b",
            via_rpc=False,
        )

        persisted = json.loads(store.read_text())
        assert persisted["future_store_field"] == {"writer": 2}
        future = next(
            item for item in persisted["leases"]
            if item["lease_id"] == "future-a"
        )
        assert future["future_lease_field"] == "preserve-me"

    @pytest.mark.parametrize(
        "payload",
        (
            {"schema_version": True, "leases": []},
            {"schema_version": 1.0, "leases": []},
            {"schema_version": 1},
            {"schema_version": 1, "leases": {}},
        ),
    )
    def test_malformed_store_discriminator_fails_closed(
        self,
        state_root: Path,
        payload: dict[str, object],
    ) -> None:
        (state_root / drain.SCHEDULER_DRAIN_LEASES_FILENAME).write_text(
            json.dumps(payload)
        )

        with pytest.raises(drain.SchedulerDrainLeaseError):
            drain.read_scheduler_drain_leases(via_rpc=False)

        state = drain.read_effective_drain_state(via_rpc=False)
        assert state is not None
        assert state.is_full_drain is True

    @pytest.mark.parametrize(
        "raw",
        (
            '{"schema_version":1,"leases":[{'
            '"lease_id":"held","scheduler_host":"host_f",'
            '"owner":"operator"}],"leases":[]}',
            '{"schema_version":1,"leases":[],"future":'
            + "[" * 10_000
            + "0"
            + "]" * 10_000
            + "}",
            '{"schema_version":1,"leases":[],"future":NaN}',
            '{"schema_version":1,"leases":[],"future":1e9999}',
        ),
        ids=(
            "duplicate-key",
            "excessive-depth",
            "nonstandard-number",
            "overflowing-number",
        ),
    )
    def test_pathological_json_store_fails_closed(
        self,
        state_root: Path,
        raw: str,
    ) -> None:
        (state_root / drain.SCHEDULER_DRAIN_LEASES_FILENAME).write_text(raw)

        with pytest.raises(drain.SchedulerDrainLeaseError):
            drain.read_scheduler_drain_leases(via_rpc=False)
        effective = drain.read_effective_drain_state(via_rpc=False)
        assert effective is not None
        assert effective.is_full_drain is True

    @pytest.mark.parametrize("owner_pid", (True, "1"))
    def test_known_store_fields_are_validated_strictly(
        self,
        state_root: Path,
        owner_pid: object,
    ) -> None:
        (state_root / drain.SCHEDULER_DRAIN_LEASES_FILENAME).write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "leases": [
                        {
                            "lease_id": "coerced-owner",
                            "scheduler_host": "host_f",
                            "owner": "rollout:test",
                            "owner_pid": owner_pid,
                        }
                    ],
                }
            )
        )

        with pytest.raises(drain.SchedulerDrainLeaseError):
            drain.read_scheduler_drain_leases(via_rpc=False)
        effective = drain.read_effective_drain_state(via_rpc=False)
        assert effective is not None
        assert effective.is_full_drain is True

    def test_invalid_utf8_store_fails_closed(self, state_root: Path) -> None:
        (state_root / drain.SCHEDULER_DRAIN_LEASES_FILENAME).write_bytes(
            b"\xff\xfe"
        )

        with pytest.raises(drain.SchedulerDrainLeaseError):
            drain.read_scheduler_drain_leases(via_rpc=False)
        effective = drain.read_effective_drain_state(via_rpc=False)
        assert effective is not None
        assert effective.is_full_drain is True

    def test_direct_daemon_reads_use_process_mode_not_system_autodetect(
        self,
        state_root: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        system_root = tmp_path / "system-state"
        system_root.mkdir()
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(system_root))
        monkeypatch.setattr(paths, "is_multi_user", lambda: False)
        monkeypatch.setattr(
            config, "system_multi_user_enabled", lambda: True
        )
        (state_root / drain.DRAIN_FILENAME).write_text(
            drain.DrainState(reason="personal daemon").model_dump_json()
        )
        (system_root / drain.DRAIN_FILENAME).write_text(
            drain.DrainState(reason="system daemon").model_dump_json()
        )
        (state_root / drain.SCHEDULER_DRAIN_LEASES_FILENAME).write_text(
            json.dumps({"schema_version": 1, "leases": []})
        )
        (system_root / drain.SCHEDULER_DRAIN_LEASES_FILENAME).write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "leases": [
                        {
                            "lease_id": "system-only",
                            "scheduler_host": "host_f",
                            "owner": "system daemon",
                        }
                    ],
                }
            )
        )

        state = drain.read_drain_state(via_rpc=False)
        leases = drain.read_scheduler_drain_leases(via_rpc=False)

        assert state is not None
        assert state.reason == "personal daemon"
        assert leases == []

    def test_client_rpc_fallback_keeps_detected_system_scope(
        self,
        state_root: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        system_root = tmp_path / "system-state"
        system_root.mkdir()
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(system_root))
        monkeypatch.setattr(paths, "is_multi_user", lambda: False)
        monkeypatch.setattr(config, "load_config", lambda: config.Config())
        monkeypatch.setattr(
            config, "system_multi_user_enabled", lambda: True
        )
        (state_root / drain.DRAIN_FILENAME).write_text(
            drain.DrainState(reason="personal daemon").model_dump_json()
        )
        (system_root / drain.DRAIN_FILENAME).write_text(
            drain.DrainState(reason="system daemon").model_dump_json()
        )
        (state_root / drain.SCHEDULER_DRAIN_LEASES_FILENAME).write_text(
            json.dumps({"schema_version": 1, "leases": []})
        )
        (system_root / drain.SCHEDULER_DRAIN_LEASES_FILENAME).write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "leases": [
                        {
                            "lease_id": "system-only",
                            "scheduler_host": "host_f",
                            "owner": "system daemon",
                        }
                    ],
                }
            )
        )

        monkeypatch.setattr(
            rpc,
            "try_rpc_or_fallback",
            lambda _method, *, fallback, **_kwargs: fallback(),
        )
        monkeypatch.setattr(
            rpc,
            "call",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                ConnectionError("system daemon unavailable")
            ),
        )

        state = drain.read_drain_state()
        leases = drain.read_scheduler_drain_leases()

        assert state is not None
        assert state.reason == "system daemon"
        assert [lease.lease_id for lease in leases] == ["system-only"]

    def test_store_read_oserror_fails_closed(
        self,
        state_root: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        store_path = state_root / drain.SCHEDULER_DRAIN_LEASES_FILENAME
        store_path.write_text('{"schema_version":1,"leases":[]}')
        original_open = drain.os.open

        def fail_store_open(
            path: object,
            flags: int,
            *args: object,
            **kwargs: object,
        ) -> int:
            if str(path) == str(store_path):
                raise PermissionError("store is root-only")
            return original_open(path, flags, *args, **kwargs)  # type: ignore[arg-type]

        monkeypatch.setattr(drain.os, "open", fail_store_open)

        with pytest.raises(drain.SchedulerDrainLeaseError):
            drain.read_scheduler_drain_leases(via_rpc=False)
        effective = drain.read_effective_drain_state(via_rpc=False)
        assert effective is not None
        assert effective.is_full_drain is True

    def test_corrupt_lease_store_preserves_legacy_submit_policy(
        self,
        state_root: Path,
    ) -> None:
        set_at = "2099-08-08T12:00:00+00:00"
        drain.write_drain_state(
            drain.DrainState(
                update_mode="deny",
                reject_submits=True,
                max_jobs=2,
                reason="operator maintenance",
                set_at=set_at,
                duration_seconds=3600,
            ),
            via_rpc=False,
        )
        (state_root / drain.SCHEDULER_DRAIN_LEASES_FILENAME).write_text(
            "not valid json {{{"
        )

        first = drain.read_effective_drain_state(via_rpc=False)
        second = drain.read_effective_drain_state(via_rpc=False)

        assert first is not None
        assert second is not None
        assert first.is_full_drain is True
        assert first.update_mode == "deny"
        assert first.reject_submits is True
        assert first.max_jobs == 2
        assert first.set_at == set_at
        assert second.set_at == set_at
        assert first.duration_seconds is None
        assert drain.status_payload()["duration_releases_everything"] is False
        assert "auto-release" not in drain.format_status()
        assert "operator maintenance" in (first.reason or "")
        assert "scheduler drain leases unreadable" in (first.reason or "")

    @pytest.mark.parametrize("owner_pid", (0, -1, 2_147_483_648, 10**100))
    def test_invalid_owner_pid_is_rejected_at_the_public_api(
        self,
        state_root: Path,
        owner_pid: int,
    ) -> None:
        _ = state_root
        with pytest.raises(
            drain.SchedulerDrainLeaseError,
            match="invalid scheduler drain lease",
        ):
            drain.acquire_scheduler_drain_lease(
                "host_f",
                owner="rollout:test",
                owner_pid=owner_pid,
                lease_id="bad-owner-pid",
                via_rpc=False,
            )

    def test_invalid_stored_owner_pid_fails_closed_without_status_crash(
        self,
        state_root: Path,
    ) -> None:
        (state_root / drain.SCHEDULER_DRAIN_LEASES_FILENAME).write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "leases": [
                        {
                            "lease_id": "bad-owner-pid",
                            "scheduler_host": "host_f",
                            "owner": "rollout:test",
                            "owner_pid": 10**100,
                        }
                    ],
                }
            )
        )

        payload = drain.status_payload()
        rendered = drain.format_status()

        assert payload["active"] is True
        assert payload["is_full_drain"] is True
        assert payload["scheduler_leases"] == []
        assert "owner_pid" in str(payload["scheduler_leases_error"])
        assert "SCHEDULER LEASE ERROR" in rendered

    def test_corrupt_sidecar_overrides_disabled_legacy_fail_closed(
        self,
        state_root: Path,
    ) -> None:
        drain.write_drain_state(
            drain.DrainState(
                enabled=False,
                full_dispatch=True,
                max_jobs=1,
                scheduler_hosts=["host_c"],
                reject_submits=True,
                update_mode="deny",
                reason="stale disabled record",
            ),
            via_rpc=False,
        )
        (state_root / drain.SCHEDULER_DRAIN_LEASES_FILENAME).write_text(
            "not valid json {{{"
        )

        state = drain.read_effective_drain_state(via_rpc=False)

        assert state is not None
        assert state.enabled is True
        assert state.is_full_drain is True
        assert state.max_jobs is None
        assert state.scheduler_hosts == []
        assert state.reject_submits is False
        assert state.update_mode is None
        assert "stale disabled record" not in (state.reason or "")
        payload = drain.status_payload()
        assert payload["legacy_scheduler_hosts"] == []
        assert payload["submit_policy"] == "accept_pending"

    def test_new_client_negotiates_before_using_atomic_mutation(
        self,
        state_root: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _ = state_root
        calls: list[tuple[str, dict[str, Any]]] = []

        def fake_call(
            method: str,
            args: dict[str, Any] | None = None,
            **_kwargs: object,
        ) -> object:
            payload = dict(args or {})
            calls.append((method, payload))
            if method == "get_methods":
                return {"methods": [_LEASE_MUTATION_RPC_METHOD]}
            if method == _LEASE_MUTATION_RPC_METHOD:
                return {
                    "ok": True,
                    "schema_version": 1,
                    "changed": True,
                    "lease": payload["lease"],
                }
            # Keep the old path executable so this regression fails on call
            # order/selection rather than on an artificial mock exception.
            if method == "get_drain_state":
                return None
            if method == "set_drain_state":
                return {"ok": True}
            raise AssertionError(f"unexpected RPC method {method!r}")

        monkeypatch.setattr(rpc, "call", fake_call)
        monkeypatch.setattr(config, "load_config", lambda: config.Config())

        acquire = getattr(drain, "acquire_scheduler_drain_lease", None)
        assert callable(acquire), "owner-scoped lease acquisition is missing"

        lease, added = acquire(
            "host_c",
            reason="vq admin update host_c",
            owner="admin-update:host_c:test",
            owner_pid=43001,
            lease_id="host_c-lease",
            via_rpc=True,
        )

        assert added is True
        assert lease.lease_id == "host_c-lease"
        assert [method for method, _args in calls] == [
            "get_methods",
            _LEASE_MUTATION_RPC_METHOD,
        ]
        mutation = calls[1][1]
        lease_payload = mutation["lease"]
        assert lease_payload["scheduler_host"] == "host_c"
        assert lease_payload["reason"] == "vq admin update host_c"
        assert lease_payload["owner_pid"] == 43001

    @pytest.mark.parametrize(
        "mutation_result",
        (
            {
                "schema_version": True,
                "changed": True,
                "lease": {
                    "lease_id": "host_c-lease",
                    "scheduler_host": "host_c",
                    "owner": "admin-update:host_c:test",
                },
            },
            {
                "schema_version": 1,
                "changed": 1,
                "lease": {
                    "lease_id": "host_c-lease",
                    "scheduler_host": "host_c",
                    "owner": "admin-update:host_c:test",
                },
            },
            {
                "schema_version": 1,
                "changed": True,
                "lease": {"not": "a lease"},
            },
            {
                "schema_version": 1,
                "changed": True,
                "lease": {
                    "lease_id": "host_c-lease",
                    "scheduler_host": "host_c",
                    "owner": "admin-update:host_c:test",
                    "owner_pid": "1",
                },
            },
            {
                "schema_version": 1,
                "changed": True,
                "lease": {
                    "lease_id": "different-id",
                    "scheduler_host": "host_c",
                    "owner": "admin-update:host_c:test",
                },
            },
        ),
    )
    def test_client_rejects_malformed_mutation_responses(
        self,
        state_root: Path,
        monkeypatch: pytest.MonkeyPatch,
        mutation_result: dict[str, object],
    ) -> None:
        _ = state_root

        def fake_call(
            method: str,
            args: dict[str, Any] | None = None,
            **_kwargs: object,
        ) -> object:
            _ = args
            if method == "get_methods":
                return {"methods": [_LEASE_MUTATION_RPC_METHOD]}
            if method == _LEASE_MUTATION_RPC_METHOD:
                return mutation_result
            raise AssertionError(method)

        monkeypatch.setattr(rpc, "call", fake_call)
        monkeypatch.setattr(config, "load_config", lambda: config.Config())

        with pytest.raises(drain.SchedulerDrainLeaseError):
            drain.acquire_scheduler_drain_lease(
                "host_c",
                owner="admin-update:host_c:test",
                lease_id="host_c-lease",
            )

    def test_old_rpc_method_set_refuses_owner_scoped_mutation(
        self,
        state_root: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _ = state_root
        calls: list[str] = []

        def old_daemon_call(
            method: str,
            args: dict[str, Any] | None = None,
            **_kwargs: object,
        ) -> object:
            _ = args
            calls.append(method)
            if method == "get_methods":
                return {"methods": ["get_drain_state", "set_drain_state"]}
            if method == "get_drain_state":
                return None
            if method == "set_drain_state":
                return {"ok": True}
            raise AssertionError(f"unexpected RPC method {method!r}")

        monkeypatch.setattr(rpc, "call", old_daemon_call)
        monkeypatch.setattr(config, "load_config", lambda: config.Config())

        with pytest.raises(
            RuntimeError,
            match="capab|atomic|scheduler drain lease|upgrade|restart",
        ):
            acquire = getattr(drain, "acquire_scheduler_drain_lease", None)
            assert callable(acquire), "owner-scoped lease acquisition is missing"
            acquire(
                "host_f",
                reason="vq admin update host_f",
                owner="admin-update:host_f:test",
                owner_pid=44001,
                lease_id="host_f-lease",
                via_rpc=True,
            )

        assert calls == ["get_methods"]
        assert _read_direct() is None

    def test_rpc_unavailable_owner_scoped_mutation_fails_closed(
        self,
        state_root: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _ = state_root

        def daemon_down(
            method: str,
            args: dict[str, Any] | None = None,
            **_kwargs: object,
        ) -> object:
            _ = args
            raise ConnectionError(f"daemon unavailable for {method}")

        monkeypatch.setattr(rpc, "call", daemon_down)
        monkeypatch.setattr(config, "load_config", lambda: config.Config())

        lease_store_path = getattr(
            drain,
            "scheduler_drain_leases_path",
            lambda: state_root / "scheduler-drain-leases.json",
        )
        with pytest.raises((ConnectionError, rpc.RPCError, RuntimeError)):
            acquire = getattr(drain, "acquire_scheduler_drain_lease", None)
            assert callable(acquire), "owner-scoped lease acquisition is missing"
            acquire(
                "host_f",
                reason="vq admin update host_f",
                owner="admin-update:host_f:test",
                owner_pid=45001,
                lease_id="host_f-lease",
                via_rpc=True,
            )

        # The failed capability/mutation round trip must not degrade to the
        # current unlocked direct read/modify/write fallback.
        assert _read_direct() is None
        assert lease_store_path().exists() is False
