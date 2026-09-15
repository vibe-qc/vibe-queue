"""Characterize the RPC handler boundary before Milestone 1A extraction.

These tests deliberately pin the current domain-import edges, daemon method
surface, envelopes, authorization, audit, and no-mutation behavior.  Handler
implementations can then move behind narrow callbacks without silently
changing the Unix-socket contract.  The positive import-edge characterization
is temporary: update it as each edge is removed, then replace it with the final
no-SCC guard when Milestone 1A is complete.
"""

from __future__ import annotations

import ast
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from collections.abc import Iterator
from dataclasses import asdict
from pathlib import Path

import pytest

from vq import admin, audit, auth, capacity, config, drain, paths, rpc, throttle

_DOMAIN_MODULES = frozenset({"admin", "drain", "rpc", "throttle"})
_SOURCE_ROOT = Path(__file__).resolve().parents[1] / "src" / "vq"

_EXPECTED_IMPORT_SITES = {
    ("admin", "drain", "<module>"),
    ("admin", "rpc", "_verify_restarted_daemon"),
    ("admin", "rpc", "_current_runtime_identity"),
    ("admin", "rpc", "read_admin_status"),
    ("admin", "rpc", "write_admin_status"),
    ("drain", "rpc", "_require_scheduler_lease_rpc"),
    ("drain", "rpc", "read_scheduler_drain_leases"),
    ("drain", "rpc", "acquire_scheduler_drain_lease"),
    ("drain", "rpc", "release_scheduler_drain_lease"),
    ("drain", "rpc", "release_scheduler_drain_leases"),
    ("drain", "rpc", "clear_scheduler_drain_leases"),
    ("drain", "rpc", "read_drain_state"),
    ("drain", "rpc", "read_only_status_payload"),
    ("drain", "rpc", "write_drain_state"),
    ("drain", "rpc", "clear_drain"),
    ("drain", "rpc", "release_legacy_scheduler_host"),
    ("drain", "rpc", "release_owned_full_drain"),
    ("throttle", "rpc", "read_throttle_state"),
    ("throttle", "rpc", "write_throttle_state"),
    ("throttle", "rpc", "clear_throttle_state"),
}

_EXPECTED_METHODS = {
    "get_admin_status",
    "get_daemon_capacity",
    "get_drain_state",
    "get_drain_read_only_snapshot",
    "get_methods",
    "get_process_identity",
    "get_scheduler_status_refresh",
    "get_scheduler_drain_leases",
    "get_throttle_state",
    "ping",
    "set_admin_status",
    "set_config_reload",
    "set_drain_state",
    "set_legacy_scheduler_drain_release",
    "set_owned_full_drain_release",
    "set_scheduler_drain_lease",
    "set_throttle_state",
}

_MUTATION_REQUESTS: dict[str, dict[str, object]] = {
    "set_admin_status": {
        "env": "vibeqc-dev",
        "record": {
            "last_updated_at": "2026-08-09T12:00:00+00:00",
            "last_success": True,
        },
    },
    "set_config_reload": {},
    "set_drain_state": {
        "state": {"enabled": True, "reason": "must not be written"},
    },
    "set_legacy_scheduler_drain_release": {"host": "host_f"},
    "set_owned_full_drain_release": {
        "expected_reason": "fleet-rollout:operation-123",
        "expected_set_at": "2026-08-10T16:00:00+00:00",
    },
    "set_scheduler_drain_lease": {
        "schema_version": 1,
        "lease": {
            "lease_id": "missing-auth-lease",
            "scheduler_host": "host_f",
            "owner": "contract-test",
        },
    },
    "set_throttle_state": {
        "state": {"weight": 25, "reason": "must not be written"},
    },
}


class _ImportCollector(ast.NodeVisitor):
    """Collect imports among the four modules, including lazy imports."""

    def __init__(self, source: str) -> None:
        self.source = source
        self.function_stack: list[str] = []
        self.sites: set[tuple[str, str, str]] = set()

    @property
    def scope(self) -> str:
        return self.function_stack[-1] if self.function_stack else "<module>"

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self.function_stack.append(node.name)
        self.generic_visit(node)
        self.function_stack.pop()

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self.function_stack.append(node.name)
        self.generic_visit(node)
        self.function_stack.pop()

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            parts = alias.name.split(".")
            if len(parts) == 2 and parts[0] == "vq" and parts[1] in _DOMAIN_MODULES:
                self.sites.add((self.source, parts[1], self.scope))

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        if node.module == "vq":
            for alias in node.names:
                if alias.name in _DOMAIN_MODULES:
                    self.sites.add((self.source, alias.name, self.scope))
            return
        if node.module is None:
            return
        parts = node.module.split(".")
        if len(parts) == 2 and parts[0] == "vq" and parts[1] in _DOMAIN_MODULES:
            self.sites.add((self.source, parts[1], self.scope))


class _ReloadProbe:
    def __init__(self) -> None:
        self.calls = 0

    def request_config_reload(self) -> None:
        self.calls += 1


@pytest.fixture
def running_multi_user_server(
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[tuple[rpc.RPCServer, _ReloadProbe, Path]]:
    """Start the full daemon RPC surface under a short, isolated root."""
    root = Path(
        tempfile.mkdtemp(
            prefix="vqrpc-contract-",
            dir=os.environ.get("VQ_TEST_SHORT_TMPDIR"),
        )
    )
    state_root = root / "state"
    system_root = root / "system"
    config_root = root / "config"
    for path in (state_root, system_root, config_root):
        path.mkdir()
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(state_root))
    monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(system_root))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(config_root))
    monkeypatch.setenv(auth.ENV_WEB_TOKEN_FILE, str(root / "missing-token"))

    reload_probe = _ReloadProbe()
    server = rpc.RPCServer(multi_user=True)
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
    drain.prepare_read_only_drain_snapshot_locks(multi_user=True)
    rpc.register_get_drain_read_only_snapshot_method(
        server,
        lambda: drain.read_locked_drain_snapshot(multi_user=True),
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
    rpc.register_owned_full_drain_release_method(
        server,
        lambda expected_reason, expected_set_at: (
            drain.release_owned_full_drain(
                expected_reason=expected_reason,
                expected_set_at=expected_set_at,
                via_rpc=False,
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
    rpc.register_capacity_methods(
        server,
        capacity.DaemonCapacity(
            max_cpus=8,
            max_jobs=2,
            max_mem_mb=32_000,
            written_at="2026-08-09T12:00:00+00:00",
        ),
    )
    rpc.register_get_scheduler_status_refresh_method(
        server,
        lambda jobid, timeout_seconds: {
            "schema": "vq.scheduler.status_refresh/1",
            "completed": True,
            "observed_at": "2026-08-11T12:00:00+00:00",
            "reason": None,
        },
    )
    rpc.register_reload_methods(server, reload_probe)
    server.start()
    time.sleep(0.05)
    try:
        yield server, reload_probe, root
    finally:
        server.stop()
        shutil.rmtree(root, ignore_errors=True)


def _raw_request(
    socket_path: Path,
    method: str,
    args: dict[str, object],
) -> dict[str, object]:
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client.settimeout(5.0)
    try:
        client.connect(str(socket_path))
        client.sendall(json.dumps({"method": method, "args": args}).encode("utf-8") + b"\n")
        response = bytearray()
        while b"\n" not in response:
            chunk = client.recv(4096)
            if not chunk:
                break
            response.extend(chunk)
    finally:
        client.close()
    payload = json.loads(bytes(response).split(b"\n", 1)[0])
    assert isinstance(payload, dict)
    return payload


def _files_below(root: Path, *, excluding: set[Path]) -> dict[str, bytes]:
    return {
        str(path.relative_to(root)): path.read_bytes()
        for path in root.rglob("*")
        if path.is_file() and path not in excluding
    }


def test_domain_import_edges_and_scopes_are_characterized() -> None:
    actual: set[tuple[str, str, str]] = set()
    for source in sorted(_DOMAIN_MODULES):
        source_path = _SOURCE_ROOT / f"{source}.py"
        collector = _ImportCollector(source)
        collector.visit(ast.parse(source_path.read_text(encoding="utf-8")))
        actual.update(collector.sites)

    assert actual == _EXPECTED_IMPORT_SITES


def test_domain_import_graph_has_no_multi_module_cycle() -> None:
    edges: set[tuple[str, str]] = set()
    for source in sorted(_DOMAIN_MODULES):
        source_path = _SOURCE_ROOT / f"{source}.py"
        collector = _ImportCollector(source)
        collector.visit(ast.parse(source_path.read_text(encoding="utf-8")))
        edges.update(
            (item_source, target)
            for item_source, target, _scope in collector.sites
        )

    reachable = set(edges)
    for intermediate in _DOMAIN_MODULES:
        reachable.update(
            (source, target)
            for source, first in tuple(reachable)
            for second, target in tuple(reachable)
            if first == intermediate == second
        )

    cycles = {
        frozenset((source, target))
        for source, target in reachable
        if source != target and (target, source) in reachable
    }
    assert cycles == set()


def test_importing_rpc_transport_does_not_eagerly_import_domains() -> None:
    code = (
        "import sys; import vq.rpc; "
        "unexpected = {'vq.admin', 'vq.drain', 'vq.throttle'} "
        "& sys.modules.keys(); "
        "assert not unexpected, sorted(unexpected)"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_daemon_rpc_method_surface_is_exact(
    running_multi_user_server: tuple[rpc.RPCServer, _ReloadProbe, Path],
) -> None:
    _server, _reload_probe, _root = running_multi_user_server

    result = rpc.call("get_methods", multi_user=True)

    assert isinstance(result, dict)
    assert result["methods"] == sorted(_EXPECTED_METHODS)
    assert {method for method in result["methods"] if method.startswith("set_")} == set(
        _MUTATION_REQUESTS
    )


def test_scheduler_status_refresh_client_uses_registered_method(
    running_multi_user_server: tuple[rpc.RPCServer, _ReloadProbe, Path],
) -> None:
    result = rpc.request_scheduler_status_refresh(
        "scheduler-job-1",
        multi_user=True,
        timeout_seconds=1.0,
    )

    assert result == {
        "schema": "vq.scheduler.status_refresh/1",
        "completed": True,
        "observed_at": "2026-08-11T12:00:00+00:00",
        "reason": None,
    }


def test_get_admin_status_registration_keeps_injected_reader() -> None:
    calls = 0
    record = admin.AdminUpdateRecord(
        last_updated_at="2026-08-10T12:00:00+00:00",
        last_success=True,
        last_sha="a" * 40,
    )

    def read_status() -> dict[str, admin.AdminUpdateRecord]:
        nonlocal calls
        calls += 1
        return {"vibeqc-dev": record}

    server = rpc.RPCServer(multi_user=False)
    rpc.register_get_admin_status_method(server, read_status)
    # The remaining setter registrar runs afterwards in daemon bootstrap.  It
    # must not silently replace the injected read callback.
    rpc.register_set_admin_status_method(
        server,
        admin.replace_admin_status_record_from_mapping,
    )

    result = server._methods["get_admin_status"]()  # noqa: SLF001

    assert calls == 1
    assert result == {"vibeqc-dev": asdict(record)}


def test_set_admin_status_registration_keeps_injected_mutator() -> None:
    calls: list[tuple[str, dict[str, object]]] = []

    def replace_record(env: str, record: dict[str, object]) -> None:
        calls.append((env, record))

    server = rpc.RPCServer(multi_user=False)
    rpc.register_set_admin_status_method(server, replace_record)
    raw_record = {
        "last_updated_at": "2026-08-10T12:00:00+00:00",
        "last_success": True,
        "future_field_unknown_to_transport": {"writer": 2},
    }

    result = server._methods["set_admin_status"](  # noqa: SLF001
        env="vibeqc-dev",
        record=raw_record,
        token="single-user-token-is-ignored",
    )

    assert calls == [("vibeqc-dev", raw_record)]
    assert calls[0][1] is raw_record
    assert result == {"env": "vibeqc-dev", "ok": True}


def test_set_admin_status_auth_precedes_injected_mutator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[object] = []
    monkeypatch.setattr(auth, "verify_admin_token", lambda _token: False)
    server = rpc.RPCServer(multi_user=True)
    rpc.register_set_admin_status_method(
        server,
        lambda _env, _record: calls.append(object()),
    )

    with pytest.raises(PermissionError, match="admin token required"):
        server._methods["set_admin_status"](  # noqa: SLF001
            env="vibeqc-dev",
            record={
                "last_updated_at": "2026-08-10T12:00:00+00:00",
                "last_success": True,
            },
            token="invalid-token",
        )

    assert calls == []


def test_get_drain_state_registration_keeps_the_injected_reader() -> None:
    calls = 0

    def read_state() -> drain.DrainState:
        nonlocal calls
        calls += 1
        return drain.DrainState(reason="injected reader")

    server = rpc.RPCServer(multi_user=False)
    rpc.register_get_drain_state_method(server, read_state)

    result = server._methods["get_drain_state"]()  # noqa: SLF001

    assert calls == 1
    assert result is not None
    assert result["reason"] == "injected reader"


def test_get_scheduler_drain_leases_registration_keeps_injected_reader() -> None:
    calls = 0

    def read_leases() -> list[drain.SchedulerDrainLease]:
        nonlocal calls
        calls += 1
        return [
            drain.SchedulerDrainLease(
                lease_id="injected-lease",
                scheduler_host="host_f",
                owner="contract-test",
                set_at="2026-08-09T12:00:00+00:00",
                future_field={"writer": 2},
            )
        ]

    server = rpc.RPCServer(multi_user=False)
    rpc.register_get_scheduler_drain_leases_method(
        server,
        read_leases,
        schema_version=drain.SCHEDULER_DRAIN_LEASE_SCHEMA_VERSION,
    )

    result = server._methods[  # noqa: SLF001
        drain.SCHEDULER_DRAIN_LEASES_RPC_METHOD
    ]()

    assert calls == 1
    assert result == {
        "schema_version": 1,
        "leases": [
            {
                "lease_id": "injected-lease",
                "scheduler_host": "host_f",
                "owner": "contract-test",
                "set_at": "2026-08-09T12:00:00+00:00",
                "reason": None,
                "owner_pid": None,
                "owner_pid_start_time": 0,
                "future_field": {"writer": 2},
            }
        ],
    }


def test_get_drain_read_only_snapshot_binds_one_reader_and_provenance() -> None:
    calls = 0

    def read_snapshot() -> dict[str, object]:
        nonlocal calls
        calls += 1
        return {
            "observed_at": "2026-08-11T12:00:00+00:00",
            "legacy_state": None,
            "legacy_error": None,
            "scheduler_leases": [],
            "scheduler_leases_error": None,
        }

    server = rpc.RPCServer(
        multi_user=True,
        source_sha_reader=lambda: "a" * 40,
        source_tree_sha256_reader=lambda: "b" * 64,
    )
    rpc.register_get_drain_read_only_snapshot_method(server, read_snapshot)

    result = server._methods[drain.DRAIN_READ_ONLY_SNAPSHOT_RPC_METHOD]()  # noqa: SLF001

    assert calls == 1
    assert result["schema"] == "vq.drain.read_only_snapshot/1"
    assert result["coverage"] == {
        "legacy_state": True,
        "scheduler_leases": True,
    }
    assert result["provenance"] == {
        "method": "get_drain_read_only_snapshot",
        "version": rpc.__version__,
        "source_sha": "a" * 40,
        "source_tree_sha256": "b" * 64,
        "multi_user": True,
    }


def test_get_throttle_state_registration_keeps_injected_reader() -> None:
    calls = 0

    def read_state() -> throttle.ThrottleState:
        nonlocal calls
        calls += 1
        return throttle.ThrottleState(
            weight=27,
            set_at="2026-08-10T12:00:00+00:00",
            reason="injected reader",
            duration_seconds=900,
        )

    server = rpc.RPCServer(multi_user=False)
    rpc.register_get_throttle_state_method(server, read_state)
    # The setter registrar runs afterwards in daemon bootstrap.  It must not
    # silently replace the injected read callback.
    rpc.register_set_throttle_state_method(
        server,
        clear_state=lambda: False,
        replace_state=lambda _state: 100,
    )

    result = server._methods["get_throttle_state"]()  # noqa: SLF001

    assert calls == 1
    assert result == {
        "weight": 27,
        "set_at": "2026-08-10T12:00:00+00:00",
        "reason": "injected reader",
        "duration_seconds": 900,
    }


def test_set_throttle_state_registration_keeps_injected_mutators() -> None:
    clear_calls = 0
    replace_calls: list[dict[str, object]] = []

    def clear_state() -> bool:
        nonlocal clear_calls
        clear_calls += 1
        return False

    def replace_state(state: dict[str, object]) -> int:
        replace_calls.append(state)
        return 31

    server = rpc.RPCServer(multi_user=False)
    rpc.register_set_throttle_state_method(
        server,
        clear_state=clear_state,
        replace_state=replace_state,
    )
    payload = {
        "weight": "31",
        "reason": "domain-owned validation",
        "future_field": {"writer": 2},
    }

    set_result = server._methods["set_throttle_state"](  # noqa: SLF001
        state=payload,
        token="single-user-token-is-ignored",
    )
    clear_result = server._methods["set_throttle_state"](  # noqa: SLF001
        state=None,
        token="single-user-token-is-ignored",
    )

    assert replace_calls == [payload]
    assert replace_calls[0] is payload
    assert clear_calls == 1
    assert set_result == {"weight": 31, "ok": True}
    assert clear_result == {"cleared": False, "ok": True}


@pytest.mark.parametrize("state", (None, {"weight": 25}))
def test_set_throttle_state_auth_precedes_injected_mutators(
    state: dict[str, object] | None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    monkeypatch.setattr(auth, "verify_admin_token", lambda _token: False)
    server = rpc.RPCServer(multi_user=True)
    rpc.register_set_throttle_state_method(
        server,
        clear_state=lambda: calls.append("clear") or True,
        replace_state=lambda _state: calls.append("replace") or 25,
    )

    with pytest.raises(PermissionError, match="admin token required"):
        server._methods["set_throttle_state"](  # noqa: SLF001
            state=state,
            token="invalid-token",
        )

    assert calls == []


def test_legacy_scheduler_release_registration_keeps_injected_mutator() -> None:
    calls: list[tuple[str, str | None, str | None]] = []

    def release_host(
        host: str,
        expected_reason: str | None,
        expected_set_at: str | None,
    ) -> bool:
        calls.append((host, expected_reason, expected_set_at))
        return True

    server = rpc.RPCServer(multi_user=False)
    rpc.register_legacy_scheduler_drain_release_method(server, release_host)

    result = server._methods[  # noqa: SLF001
        drain.LEGACY_SCHEDULER_DRAIN_RELEASE_RPC_METHOD
    ](
        host=" host_f ",
        expected_reason="rollout migration",
        expected_set_at="2026-08-09T12:00:00+00:00",
        token="single-user-token-is-ignored",
    )

    assert calls == [
        (
            "host_f",
            "rollout migration",
            "2026-08-09T12:00:00+00:00",
        )
    ]
    assert result == {"changed": True, "ok": True}


def test_owned_full_release_registration_keeps_injected_transaction() -> None:
    calls: list[tuple[str, str]] = []

    def release_owned(expected_reason: str, expected_set_at: str) -> bool:
        calls.append((expected_reason, expected_set_at))
        return True

    server = rpc.RPCServer(multi_user=False)
    rpc.register_owned_full_drain_release_method(server, release_owned)

    result = server._methods[  # noqa: SLF001
        drain.OWNED_FULL_DRAIN_RELEASE_RPC_METHOD
    ](
        expected_reason="fleet-rollout:operation-123",
        expected_set_at="2026-08-10T16:00:00+00:00",
        token="single-user-token-is-ignored",
    )

    assert calls == [
        (
            "fleet-rollout:operation-123",
            "2026-08-10T16:00:00+00:00",
        )
    ]
    assert result == {"changed": True, "ok": True}


def test_set_drain_state_registration_keeps_injected_mutators() -> None:
    clear_calls = 0
    replace_calls: list[dict[str, object]] = []

    def clear_state() -> bool:
        nonlocal clear_calls
        clear_calls += 1
        return True

    def replace_state(state: dict[str, object]) -> bool:
        replace_calls.append(state)
        return False

    server = rpc.RPCServer(multi_user=False)
    rpc.register_set_drain_state_method(
        server,
        clear_state=clear_state,
        replace_state=replace_state,
    )

    payload = {
        "enabled": False,
        "future_field_unknown_to_transport": "domain-owned",
    }
    set_result = server._methods["set_drain_state"](  # noqa: SLF001
        state=payload,
        token="single-user-token-is-ignored",
    )
    clear_result = server._methods["set_drain_state"](  # noqa: SLF001
        state=None,
        token="single-user-token-is-ignored",
    )

    assert replace_calls == [payload]
    assert clear_calls == 1
    assert set_result == {"enabled": False, "ok": True}
    assert clear_result == {"cleared": True, "ok": True}


def test_scheduler_lease_registration_forwards_raw_domain_transaction() -> None:
    calls: list[tuple[object, object, object, object, object]] = []
    returned = drain.SchedulerDrainLease(
        lease_id="returned-lease",
        scheduler_host="host_f",
        owner="contract-test",
        set_at="2026-08-09T12:00:00+00:00",
        future_field={"writer": 2},
    )

    def mutate_leases(
        lease: dict[str, object] | None,
        release_id: str | None,
        release_host: str | None,
        release_owner: str | None,
        release_all: bool,
    ) -> tuple[
        list[drain.SchedulerDrainLease],
        drain.SchedulerDrainLease | None,
        bool,
    ]:
        calls.append(
            (lease, release_id, release_host, release_owner, release_all)
        )
        return [returned], returned, False

    server = rpc.RPCServer(multi_user=False)
    rpc.register_set_scheduler_drain_lease_method(
        server,
        mutate_leases,
        schema_version=drain.SCHEDULER_DRAIN_LEASE_SCHEMA_VERSION,
    )
    raw_lease = {
        "lease_id": "raw-lease",
        "scheduler_host": "host_c",
        "owner": "raw-owner",
        "field_owned_by_domain": {"future": True},
    }

    result = server._methods["set_scheduler_drain_lease"](  # noqa: SLF001
        schema_version=1,
        lease=raw_lease,
        release_id=" raw-id ",
        release_host=" host_f ",
        release_owner=" raw-owner ",
        release_all=True,
        token="single-user-token-is-ignored",
    )

    assert calls == [
        (raw_lease, " raw-id ", " host_f ", " raw-owner ", True)
    ]
    assert calls[0][0] is raw_lease
    assert result == {
        "ok": True,
        "schema_version": 1,
        "changed": False,
        "lease": {
            "lease_id": "returned-lease",
            "scheduler_host": "host_f",
            "owner": "contract-test",
            "set_at": "2026-08-09T12:00:00+00:00",
            "reason": None,
            "owner_pid": None,
            "owner_pid_start_time": 0,
            "future_field": {"writer": 2},
        },
        "leases": [
            {
                "lease_id": "returned-lease",
                "scheduler_host": "host_f",
                "owner": "contract-test",
                "set_at": "2026-08-09T12:00:00+00:00",
                "reason": None,
                "owner_pid": None,
                "owner_pid_start_time": 0,
                "future_field": {"writer": 2},
            }
        ],
    }


@pytest.mark.parametrize(
    ("payload", "message"),
    (
        ({"schema_version": True}, "unsupported scheduler drain lease schema"),
        ({"schema_version": 2}, "unsupported scheduler drain lease schema"),
        (
            {"schema_version": 1, "release_all": 1},
            "release_all must be a boolean",
        ),
        (
            {"schema_version": 1, "release_id": " "},
            "release_id must be a non-empty string",
        ),
        (
            {"schema_version": 1, "release_host": 1},
            "release_host must be a non-empty string",
        ),
        (
            {"schema_version": 1, "release_owner": ""},
            "release_owner must be a non-empty string",
        ),
        (
            {"schema_version": 1, "lease": []},
            "lease must be an object",
        ),
    ),
)
def test_scheduler_lease_transport_validation_precedes_callback(
    payload: dict[str, object],
    message: str,
) -> None:
    calls: list[object] = []
    server = rpc.RPCServer(multi_user=False)
    rpc.register_set_scheduler_drain_lease_method(
        server,
        lambda *_args: calls.append(object()),
        schema_version=drain.SCHEDULER_DRAIN_LEASE_SCHEMA_VERSION,
    )

    with pytest.raises(ValueError, match=message):
        server._methods["set_scheduler_drain_lease"](**payload)  # noqa: SLF001

    assert calls == []


def test_all_multi_user_mutators_fail_auth_and_leave_only_failed_audits(
    running_multi_user_server: tuple[rpc.RPCServer, _ReloadProbe, Path],
) -> None:
    server, reload_probe, root = running_multi_user_server
    audit_path = audit.audit_log_path(multi_user=True)
    before = _files_below(root, excluding={audit_path})

    for method, args in _MUTATION_REQUESTS.items():
        response = _raw_request(server._socket_path, method, args)  # noqa: SLF001
        assert set(response) == {"ok", "error"}, method
        assert response["ok"] is False, method
        assert isinstance(response["error"], str), method
        assert f"handler {method} raised PermissionError:" in response["error"], response
        assert "admin token required" in response["error"], response

    assert _files_below(root, excluding={audit_path}) == before
    assert reload_probe.calls == 0
    lines = audit.read_audit_log(multi_user=True)
    assert [line["method"] for line in lines] == list(_MUTATION_REQUESTS)
    assert all(line["ok"] is False for line in lines)
    assert all("PermissionError" in line["error"] for line in lines)
    assert all("token" not in line["args_summary"] for line in lines)

    assert not admin.admin_status_path().exists()
    assert not drain.drain_state_path(multi_user=True).exists()
    assert not drain.scheduler_drain_leases_path(multi_user=True).exists()
    assert not throttle.throttle_state_path().exists()
