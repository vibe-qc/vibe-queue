"""v0.8.0 *Dahl's Simula* — Unix-socket RPC tests.

Pins the v0.8.0 contract:

1. **Server starts + accepts + dispatches** — the daemon's RPC
   server binds the socket, accepts a client, calls the
   registered handler, returns the result.
2. **Protocol shape** — request/response are line-delimited JSON
   objects with the documented keys.
3. **Permissions** — single-user 0600, multi-user 0660; chown
   to admin group when configured.
4. **Error paths** — unknown method, malformed JSON, handler
   exception, missing socket, daemon down → distinct + named.
5. **Fallback** — ``try_rpc_or_fallback`` calls the fallback on
   any RPC failure; in multi-user mode logs WARNING.
6. **Admin-status round-trip** — ``get_admin_status`` returns
   what ``set_admin_status`` wrote, via the server.
7. **Multi-user auth** — ``set_admin_status`` requires the
   admin token when ``multi_user=True``; ``get_admin_status``
   is open.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import socket
import stat
import sys
import tempfile
import threading
import time
from pathlib import Path

import pytest

from vq import admin, audit, auth, capacity, drain, paths, rpc, throttle


@pytest.fixture
def state_dir(monkeypatch: pytest.MonkeyPatch) -> Path:
    """v0.8.0: AF_UNIX socket paths are limited to ~104 bytes on
    macOS / ~108 on Linux. Pytest's default tmp_path is too deep
    (``/private/var/folders/.../pytest-of-USER/pytest-N/test_X/``)
    so we use a short ``/tmp/vqrpc-XXXXXX/state`` instead."""
    tmpdir = Path(
        tempfile.mkdtemp(
            prefix="vqrpc-",
            dir=os.environ.get("VQ_TEST_SHORT_TMPDIR"),
        )
    )
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmpdir / "state"))
    (tmpdir / "state").mkdir()
    yield tmpdir
    shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.fixture
def running_server(state_dir: Path) -> rpc.RPCServer:
    """A started RPCServer in single-user mode with admin-status,
    drain, throttle, and capacity methods registered. Auto-stopped on test
    teardown.

    v0.8.1: drain + throttle methods are now part of the canonical
    server surface, matching what daemon.run() wires up.
    """
    rpc_source = Path(rpc.__file__)
    server = rpc.RPCServer(
        multi_user=False,
        source_sha_reader=lambda: admin.running_source_sha(rpc_source),
        source_tree_sha256_reader=lambda: admin.running_source_tree_sha256(
            rpc_source.resolve().parent,
        ),
    )
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
            max_cpus=12,
            max_jobs=3,
            max_mem_mb=48_000,
            default_job_mem_mb=4_000,
            written_at="2026-08-09T12:00:00+00:00",
        ),
    )
    server.start()
    # Give the accept-loop thread a moment to be in select.
    time.sleep(0.05)
    yield server
    server.stop()


def test_capacity_rpc_survives_missing_advertisement_file(
    running_server: rpc.RPCServer,
) -> None:
    assert not capacity.capacity_path().exists()

    advertised = capacity.read_daemon_capacity()

    assert advertised is not None
    assert advertised.max_cpus == 12
    assert advertised.max_jobs == 3
    assert advertised.max_mem_mb == 48_000
    assert advertised.default_job_mem_mb == 4_000


def test_capacity_rpc_is_open_on_multi_user_socket(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    system_state = state_dir / "mu"
    monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(system_state))
    system_state.mkdir()
    snapshot = capacity.DaemonCapacity(
        max_cpus=12,
        max_jobs=None,
        max_mem_mb=48_000,
        written_at="2026-08-09T12:00:00+00:00",
    )
    server = rpc.RPCServer(multi_user=True)
    rpc.register_capacity_methods(server, snapshot)
    try:
        server.start()
        time.sleep(0.05)
        advertised = capacity.read_daemon_capacity(multi_user=True)
        assert advertised == snapshot
        assert not capacity.capacity_path(multi_user=True).exists()
    finally:
        server.stop()


# ----------------------------------------------------------------------
# Socket path resolution
# ----------------------------------------------------------------------


class TestSocketPath:
    def test_single_user_path(self, state_dir: Path) -> None:
        p = rpc.socket_path(multi_user=False)
        assert p == paths.state_root() / "daemon.sock"

    def test_multi_user_path(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        tmpdir = Path(
            tempfile.mkdtemp(
                prefix="vqrpc-",
                dir=os.environ.get("VQ_TEST_SHORT_TMPDIR"),
            )
        )
        try:
            monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(tmpdir / "mu"))
            (tmpdir / "mu").mkdir()
            p = rpc.socket_path(multi_user=True)
            assert p == tmpdir / "mu" / "daemon.sock"
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)


class TestUserSocketPath:
    """host_d regression (2026-08-02): ``$VQ_STATE_DIR`` silently defeated
    ``socket_path(multi_user=False)``.

    Admins on a multi-user host are documented to run admin verbs as
    ``VQ_STATE_DIR=/var/lib/vq vq admin ...`` (``docs/state_file_audit.md``),
    and the multi-user unit carries that value too. The post-restart
    provenance ping then landed on the *root* daemon, whose ``source_sha``
    described ``/opt/vq`` rather than the user daemon that had just been
    restarted — so two correct restarts were reported as stale-SHA failures.
    """

    def test_ordinary_override_is_still_honoured(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A relocated single-user state root must keep working — the
        fallback is narrow on purpose, and tests rely on this override."""
        tmpdir = Path(
            tempfile.mkdtemp(
                prefix="vqrpc-",
                dir=os.environ.get("VQ_TEST_SHORT_TMPDIR"),
            )
        )
        try:
            monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmpdir))
            monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(tmpdir / "mu"))
            assert rpc.user_socket_path() == tmpdir / "daemon.sock"
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_state_dir_pointed_at_multi_user_root_falls_back_to_xdg(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The exact host_d shape: VQ_STATE_DIR == the multi-user root."""
        tmpdir = Path(
            tempfile.mkdtemp(
                prefix="vqrpc-",
                dir=os.environ.get("VQ_TEST_SHORT_TMPDIR"),
            )
        )
        try:
            mu = tmpdir / "mu"
            monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(mu))
            monkeypatch.setenv(paths.ENV_STATE_DIR, str(mu))
            monkeypatch.setenv("XDG_DATA_HOME", str(tmpdir / "xdg"))

            # Precondition: the plain flag cannot tell the two apart.
            assert rpc.socket_path(multi_user=False) == rpc.socket_path(
                multi_user=True
            )

            resolved = rpc.user_socket_path()
            assert resolved == tmpdir / "xdg" / "vq" / "daemon.sock"
            assert resolved != rpc.socket_path(multi_user=True)
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_xdg_state_root_ignores_the_override(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        tmpdir = Path(
            tempfile.mkdtemp(
                prefix="vqrpc-",
                dir=os.environ.get("VQ_TEST_SHORT_TMPDIR"),
            )
        )
        try:
            monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmpdir / "multi-user"))
            monkeypatch.setenv("XDG_DATA_HOME", str(tmpdir))
            assert paths.state_root() == tmpdir / "multi-user"
            assert paths.xdg_state_root() == tmpdir / "vq"
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)


# ----------------------------------------------------------------------
# Server lifecycle
# ----------------------------------------------------------------------


class TestServerLifecycle:
    def test_start_creates_socket_with_right_mode_single_user(
        self, state_dir: Path,
    ) -> None:
        server = rpc.RPCServer(multi_user=False)
        try:
            server.start()
            sock_path = rpc.socket_path(multi_user=False)
            assert sock_path.exists()
            mode = stat.S_IMODE(sock_path.stat().st_mode)
            assert mode == 0o600, f"expected 0600, got {mode:o}"
        finally:
            server.stop()

    def test_stop_removes_socket(self, state_dir: Path) -> None:
        server = rpc.RPCServer(multi_user=False)
        server.start()
        sock_path = rpc.socket_path(multi_user=False)
        assert sock_path.exists()
        server.stop()
        assert not sock_path.exists()

    def test_start_handles_stale_socket(self, state_dir: Path) -> None:
        """A leftover socket from a previous crashed daemon shouldn't
        block startup — the server unlinks before bind."""
        sock_path = rpc.socket_path(multi_user=False)
        sock_path.parent.mkdir(parents=True, exist_ok=True)
        sock_path.touch()  # stale file
        server = rpc.RPCServer(multi_user=False)
        try:
            server.start()
            # Survived the bind.
            assert sock_path.exists()
        finally:
            server.stop()

    def test_double_start_is_noop(self, state_dir: Path) -> None:
        server = rpc.RPCServer(multi_user=False)
        try:
            server.start()
            server.start()  # should not raise
        finally:
            server.stop()


# ----------------------------------------------------------------------
# Ping — the simplest round-trip
# ----------------------------------------------------------------------


def _checkout_sha_stub(
    tracked: str | None, untracked: str | None = None,
):
    """Stand in for `admin.current_source_sha`, which answers differently
    depending on whether the running file is actually tracked by the
    repository git discovered by walking up from it."""

    def _stub(path=None, *, require_tracked: bool = False) -> str | None:
        return tracked if require_tracked else (untracked or tracked)

    return _stub


class TestPing:
    def test_process_identity_is_a_separate_open_capability(self) -> None:
        server = rpc.RPCServer(multi_user=True)

        assert set(server._handle_ping()) == {
            "version",
            "multi_user",
            "source_sha",
            "source_tree_sha256",
        }
        payload = server._handle_get_process_identity()

        assert payload["pid"] == os.getpid()
        assert payload["euid"] == os.geteuid()
        assert payload["python_executable"] == sys.executable
        assert payload["argv"] == sys.argv
        assert payload["version"] == rpc.__version__
        assert payload["multi_user"] is True
        assert payload["source_sha"] is None
        assert payload["source_tree_sha256"] is None
        assert payload["socket_path"] == str(server._socket_path)
        assert "get_process_identity" in server._handle_get_methods()["methods"]

    def test_source_readers_are_captured_once_at_construction(self) -> None:
        source_sha = ["a" * 40]
        tree_sha256 = ["b" * 64]
        calls = {"source_sha": 0, "tree_sha256": 0}

        def read_source_sha() -> str:
            calls["source_sha"] += 1
            return source_sha[0]

        def read_tree_sha256() -> str:
            calls["tree_sha256"] += 1
            return tree_sha256[0]

        server = rpc.RPCServer(
            multi_user=False,
            source_sha_reader=read_source_sha,
            source_tree_sha256_reader=read_tree_sha256,
        )
        source_sha[0] = "c" * 40
        tree_sha256[0] = "d" * 64

        assert server._handle_ping()["source_sha"] == "a" * 40
        assert server._handle_ping()["source_tree_sha256"] == "b" * 64
        assert calls == {"source_sha": 1, "tree_sha256": 1}

    def test_running_source_sha_prefers_git_checkout(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        checkout_sha = "a" * 40
        marker_sha = "b" * 40
        monkeypatch.setattr(admin, "current_source_sha", _checkout_sha_stub(checkout_sha))
        monkeypatch.setattr(admin, "read_source_sha_marker", lambda: marker_sha)

        assert admin.running_source_sha(Path(rpc.__file__)) == checkout_sha

    def test_running_source_sha_falls_back_to_installed_marker(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        marker_sha = "b" * 40
        monkeypatch.setattr(admin, "current_source_sha", _checkout_sha_stub(None))
        monkeypatch.setattr(admin, "read_source_sha_marker", lambda: marker_sha)

        assert admin.running_source_sha(Path(rpc.__file__)) == marker_sha

    def test_an_installed_marker_beats_an_untracked_checkout_sha(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A root-owned `/opt/vq/venv` install is deliberately non-editable, so
        the package files are a *copy*. If that venv sits anywhere inside some
        unrelated work tree, git happily answers with that tree's HEAD -- a SHA
        describing code the daemon is not running. The marker is the installer's
        own statement about what it installed; it wins.
        """
        marker_sha = "b" * 40
        monkeypatch.setattr(
            admin, "current_source_sha", _checkout_sha_stub(None, untracked="a" * 40)
        )
        monkeypatch.setattr(admin, "read_source_sha_marker", lambda: marker_sha)

        assert admin.running_source_sha(Path(rpc.__file__)) == marker_sha

    def test_an_untracked_checkout_sha_is_still_better_than_nothing(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """No marker installed and no tracked checkout: report what we have.

        Reporting `None` here would read as "this daemon has no identity" and
        fail every scheduler-compat gate on hosts that report fine today. This
        change tightens *precedence*, never availability.
        """
        monkeypatch.setattr(
            admin, "current_source_sha", _checkout_sha_stub(None, untracked="a" * 40)
        )
        monkeypatch.setattr(admin, "read_source_sha_marker", lambda: None)

        assert admin.running_source_sha(Path(rpc.__file__)) == "a" * 40

    def test_unreadable_source_identity_reports_none(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def boom(*_args: object, **_kwargs: object) -> str:
            raise OSError("source identity unavailable")

        monkeypatch.setattr(admin, "current_source_sha", boom)

        assert admin.running_source_sha(Path(rpc.__file__)) is None

    def test_ping_returns_version_and_mode(
        self, running_server: rpc.RPCServer,
    ) -> None:
        result = rpc.ping(multi_user=False)
        assert result is not None
        assert "version" in result
        assert result["multi_user"] is False

    def test_ping_reports_the_running_source_tree_digest(
        self, running_server: rpc.RPCServer,
    ) -> None:
        """Content-derived provenance, alongside the declared SOURCE-SHA.

        A marker can be copied or survive an upgrade; this cannot -- it is
        computed from the package bytes themselves.
        """
        result = rpc.ping(multi_user=False)

        assert result is not None
        assert result["source_tree_sha256"] == admin.source_tree_sha256(
            Path(rpc.__file__).resolve().parent
        )

    def test_get_methods_carries_the_digest_too(
        self, running_server: rpc.RPCServer,
    ) -> None:
        result = rpc.call("get_methods", multi_user=False)

        assert isinstance(result, dict)
        assert result["source_tree_sha256"] == running_server._source_tree_sha256

    def test_the_tree_digest_is_captured_once_at_construction(
        self,
    ) -> None:
        """Capture-once is the whole safety property.

        A digest read live per ping would describe the files on disk *now*,
        which after an editable-install upgrade are the new ones even though
        the process is still running the old code. That would report a
        successful reload for a daemon that never restarted -- exactly the
        failure the provenance probe exists to catch.
        """
        digest = ["e" * 64]
        server = rpc.RPCServer(
            multi_user=False,
            source_tree_sha256_reader=lambda: digest[0],
        )
        digest[0] = "f" * 64

        assert server._handle_ping()["source_tree_sha256"] == "e" * 64

    def test_an_undigestible_tree_reports_none_rather_than_raising(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Health stays useful without a digest -- same contract as the SHA."""
        def boom(root=None):
            raise admin.AdminError("vq source tree contains no files")

        monkeypatch.setattr(admin, "source_tree_sha256", boom)

        assert admin.running_source_tree_sha256(
            Path(rpc.__file__).resolve().parent,
        ) is None

    def test_source_reader_errors_do_not_block_server_construction(self) -> None:
        def boom() -> str:
            raise RuntimeError("provenance unavailable")

        server = rpc.RPCServer(
            multi_user=False,
            source_sha_reader=boom,
            source_tree_sha256_reader=boom,
        )

        assert server._handle_ping()["source_sha"] is None
        assert server._handle_ping()["source_tree_sha256"] is None

    def test_ping_returns_none_when_daemon_down(
        self, state_dir: Path,
    ) -> None:
        # No server started — socket missing.
        assert rpc.ping(multi_user=False) is None


# ----------------------------------------------------------------------
# Protocol — error paths
# ----------------------------------------------------------------------


class TestProtocolErrors:
    @pytest.mark.parametrize("bad_args", ([], False, 0, ""))
    def test_client_rejects_non_object_args_before_transport(
        self,
        bad_args: object,
    ) -> None:
        with pytest.raises(TypeError, match="args must be a dict"):
            rpc.call("ping", bad_args)  # type: ignore[arg-type]

    def test_unknown_method_returns_named_error(
        self, running_server: rpc.RPCServer,
    ) -> None:
        with pytest.raises(rpc.RPCError, match="unknown method"):
            rpc.call("nosuchmethod")

    def test_malformed_json_returns_error(
        self, running_server: rpc.RPCServer,
    ) -> None:
        sock_path = rpc.socket_path(multi_user=False)
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.settimeout(5.0)
        s.connect(str(sock_path))
        s.sendall(b"{ not json }\n")
        # Read the response line.
        buf = b""
        while b"\n" not in buf:
            chunk = s.recv(4096)
            if not chunk:
                break
            buf += chunk
        s.close()
        payload = json.loads(buf.split(b"\n")[0])
        assert payload["ok"] is False
        assert "malformed JSON" in payload["error"]

    @pytest.mark.parametrize("bad_args", (None, False, 0, "", []))
    def test_falsy_non_object_args_cannot_clear_drain_state(
        self,
        running_server: rpc.RPCServer,
        bad_args: object,
    ) -> None:
        _ = running_server
        rpc.call(
            "set_drain_state",
            {"state": {"enabled": True, "reason": "keep this hold"}},
        )
        sock_path = rpc.socket_path(multi_user=False)
        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        client.settimeout(5.0)
        client.connect(str(sock_path))
        client.sendall(
            json.dumps(
                {"method": "set_drain_state", "args": bad_args}
            ).encode()
            + b"\n"
        )
        response = b""
        while b"\n" not in response:
            chunk = client.recv(4096)
            if not chunk:
                break
            response += chunk
        client.close()

        payload = json.loads(response.split(b"\n", 1)[0])
        assert payload["ok"] is False
        assert "'args' must be an object" in payload["error"]
        state = rpc.call("get_drain_state")
        assert state["reason"] == "keep this hold"
        audit_line = audit.read_audit_log()[-1]
        assert audit_line["method"] == "set_drain_state"
        assert audit_line["ok"] is False
        assert audit_line["args_summary"] == (
            f"invalid args type={type(bad_args).__name__}"
        )

    def test_handler_exception_returns_named_error(
        self, state_dir: Path,
    ) -> None:
        server = rpc.RPCServer(multi_user=False)

        @server.register("boom")
        def _boom() -> None:
            raise RuntimeError("intentional")

        try:
            server.start()
            time.sleep(0.05)
            with pytest.raises(rpc.RPCError, match="intentional"):
                rpc.call("boom")
        finally:
            server.stop()

    def test_bad_args_returns_error(
        self, state_dir: Path,
    ) -> None:
        server = rpc.RPCServer(multi_user=False)

        @server.register("needs_arg")
        def _h(required_arg: str) -> str:
            return required_arg

        try:
            server.start()
            time.sleep(0.05)
            with pytest.raises(rpc.RPCError, match="bad args"):
                rpc.call("needs_arg", {"wrong_kwarg": "x"})
        finally:
            server.stop()

    def test_missing_socket_raises_connection_error(
        self, state_dir: Path,
    ) -> None:
        with pytest.raises(ConnectionError, match="socket not found"):
            rpc.call("ping")

    def test_socket_probe_error_is_normalized_to_connection_error(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        socket_path = tmp_path / "daemon.sock"
        original_exists = Path.exists

        def fail_probe(path: Path) -> bool:
            if path == socket_path:
                raise PermissionError("probe denied")
            return original_exists(path)

        monkeypatch.setattr(Path, "exists", fail_probe)
        with pytest.raises(ConnectionError, match="socket probe failed"):
            rpc.call("ping", socket_override=socket_path)

    @pytest.mark.parametrize("failure_phase", ("send", "read"))
    def test_socket_io_errors_are_normalized_to_connection_error(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        failure_phase: str,
    ) -> None:
        socket_path = tmp_path / "daemon.sock"
        socket_path.touch()

        class BrokenSocket:
            def settimeout(self, _timeout: float) -> None:
                return None

            def connect(self, _path: str) -> None:
                return None

            def sendall(self, _payload: bytes) -> None:
                if failure_phase == "send":
                    raise TimeoutError("send timed out after possible commit")

            def recv(self, _size: int) -> bytes:
                raise TimeoutError("response timed out after commit")

            def close(self) -> None:
                return None

        monkeypatch.setattr(
            rpc.socket,
            "socket",
            lambda *_args, **_kwargs: BrokenSocket(),
        )

        with pytest.raises(ConnectionError, match="transport failed"):
            rpc.call("set_scheduler_drain_lease", socket_override=socket_path)

    def test_socket_constructor_error_is_normalized_to_connection_error(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        socket_path = tmp_path / "daemon.sock"
        socket_path.touch()
        monkeypatch.setattr(
            rpc.socket,
            "socket",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                OSError("too many open files")
            ),
        )

        with pytest.raises(ConnectionError, match="socket setup failed"):
            rpc.call("set_scheduler_drain_lease", socket_override=socket_path)

    @pytest.mark.parametrize(
        "reply",
        (
            b"\xff\n",
            b'{"ok":true,"result":' + b"[" * 2000 + b"0"
            + b"]" * 2000 + b"}\n",
        ),
        ids=("invalid-utf8", "excessive-depth"),
    )
    def test_malformed_response_bytes_raise_rpc_error(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        reply: bytes,
    ) -> None:
        socket_path = tmp_path / "daemon.sock"
        socket_path.touch()

        class ReplySocket:
            def settimeout(self, _timeout: float) -> None:
                return None

            def connect(self, _path: str) -> None:
                return None

            def sendall(self, _payload: bytes) -> None:
                return None

            def recv(self, _size: int) -> bytes:
                return reply

            def close(self) -> None:
                return None

        monkeypatch.setattr(
            rpc.socket,
            "socket",
            lambda *_args, **_kwargs: ReplySocket(),
        )
        with pytest.raises(rpc.RPCError, match="malformed response"):
            rpc.call("ping", socket_override=socket_path)

    @pytest.mark.parametrize("ok_value", (1, "false", None))
    def test_response_ok_field_must_be_an_exact_boolean(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        ok_value: object,
    ) -> None:
        socket_path = tmp_path / "daemon.sock"
        socket_path.touch()
        reply = json.dumps({"ok": ok_value, "result": {}}).encode() + b"\n"

        class ReplySocket:
            def settimeout(self, _timeout: float) -> None:
                return None

            def connect(self, _path: str) -> None:
                return None

            def sendall(self, _payload: bytes) -> None:
                return None

            def recv(self, _size: int) -> bytes:
                return reply

            def close(self) -> None:
                return None

        monkeypatch.setattr(
            rpc.socket,
            "socket",
            lambda *_args, **_kwargs: ReplySocket(),
        )
        with pytest.raises(rpc.RPCError, match="bad response shape"):
            rpc.call("ping", socket_override=socket_path)


# ----------------------------------------------------------------------
# Fallback helper
# ----------------------------------------------------------------------


class TestTryRpcOrFallback:
    def test_fallback_runs_when_daemon_down(
        self, state_dir: Path,
    ) -> None:
        fallback_ran = []

        def fb() -> str:
            fallback_ran.append(True)
            return "from-fallback"

        result = rpc.try_rpc_or_fallback(
            "ping", fallback=fb, multi_user=False,
        )
        assert result == "from-fallback"
        assert fallback_ran == [True]

    def test_rpc_used_when_daemon_up(
        self, running_server: rpc.RPCServer,
    ) -> None:
        called = []

        def fb() -> str:
            called.append("fallback")
            return "fallback"

        result = rpc.try_rpc_or_fallback(
            "ping", fallback=fb, multi_user=False,
        )
        assert "version" in result
        assert called == [], "fallback should not run when RPC succeeded"

    def test_multi_user_fallback_logs_warning(
        self, state_dir: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """In multi-user mode, falling back to the local file means a
        stale view — operator gets a WARNING."""
        caplog.set_level(logging.WARNING, logger="vq.rpc")
        # No server up; multi-user requested.
        result = rpc.try_rpc_or_fallback(
            "ping",
            fallback=lambda: "ok",
            multi_user=True,
        )
        assert result == "ok"
        # Warning surfaces the cause.
        assert any(
            "falling back to direct file" in rec.message
            for rec in caplog.records
        )

    def test_single_user_fallback_silent(
        self, state_dir: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Single-user fallback is silent — the fallback file IS the
        canonical file, no divergence risk."""
        caplog.set_level(logging.WARNING, logger="vq.rpc")
        rpc.try_rpc_or_fallback(
            "ping",
            fallback=lambda: "ok",
            multi_user=False,
        )
        assert not any(
            "falling back" in rec.message for rec in caplog.records
        )


# ----------------------------------------------------------------------
# Admin-status methods — the v0.8.0 use case
# ----------------------------------------------------------------------


class TestAdminStatusRoundtrip:
    def test_get_returns_empty_when_no_file(
        self, running_server: rpc.RPCServer,
    ) -> None:
        result = rpc.call("get_admin_status")
        assert result == {}

    def test_set_then_get_returns_the_record(
        self, running_server: rpc.RPCServer,
    ) -> None:
        rpc.call("set_admin_status", {
            "env": "vibeqc-dev",
            "record": {
                "last_updated_at": "2026-05-30T10:00:00+00:00",
                "last_success": True,
                "last_sha": "abc123def456",
            },
        })
        result = rpc.call("get_admin_status")
        assert "vibeqc-dev" in result
        assert result["vibeqc-dev"]["last_sha"] == "abc123def456"
        assert result["vibeqc-dev"]["last_success"] is True

    def test_set_multiple_envs(
        self, running_server: rpc.RPCServer,
    ) -> None:
        for env in ("vibeqc-dev", "vibeqc-release"):
            rpc.call("set_admin_status", {
                "env": env,
                "record": {
                    "last_updated_at": "2026-05-30T10:00:00+00:00",
                    "last_success": True,
                    "last_sha": f"sha-of-{env}",
                },
            })
        result = rpc.call("get_admin_status")
        assert set(result.keys()) == {"vibeqc-dev", "vibeqc-release"}

    def test_set_unknown_fields_stripped(
        self, running_server: rpc.RPCServer,
    ) -> None:
        """A future client writing a field the daemon doesn't know
        about must not crash the daemon's dataclass construction."""
        rpc.call("set_admin_status", {
            "env": "vibeqc-dev",
            "record": {
                "last_updated_at": "2026-05-30T10:00:00+00:00",
                "last_success": True,
                "future_field_unknown_to_daemon": "ignored",
            },
        })
        # No exception → success. Verify the record was written
        # without the unknown field.
        result = rpc.call("get_admin_status")
        assert "future_field_unknown_to_daemon" not in result["vibeqc-dev"]


# ----------------------------------------------------------------------
# admin.read_admin_status / write_admin_status via_rpc plumbing
# ----------------------------------------------------------------------


class TestViaRpcPlumbing:
    def test_read_via_rpc_uses_daemon_view(
        self, running_server: rpc.RPCServer,
    ) -> None:
        """A record written via RPC is visible to
        ``read_admin_status(via_rpc=True)``."""
        rpc.call("set_admin_status", {
            "env": "vibeqc-dev",
            "record": {
                "last_updated_at": "2026-05-30T10:00:00+00:00",
                "last_success": True,
                "last_sha": "deadbeef0123",
            },
        })
        records = admin.read_admin_status(via_rpc=True)
        assert "vibeqc-dev" in records
        assert records["vibeqc-dev"].last_sha == "deadbeef0123"

    def test_read_via_rpc_falls_back_when_daemon_down(
        self, state_dir: Path,
    ) -> None:
        """No daemon running — read still works via the file path
        (single-user; correct)."""
        # Write directly to the file (simulating a daemon that
        # was up earlier but is now down).
        rec = admin.AdminUpdateRecord(
            last_updated_at="2026-05-30T09:00:00+00:00",
            last_success=False,
            last_sha="oldsha000000",
        )
        admin.write_admin_status({"vibeqc-dev": rec}, via_rpc=False)
        # Now read with via_rpc=True; should fall back.
        records = admin.read_admin_status(via_rpc=True)
        assert records["vibeqc-dev"].last_sha == "oldsha000000"

    def test_internal_via_rpc_false_short_circuits(
        self, state_dir: Path,
    ) -> None:
        """The daemon-internal callers pass ``via_rpc=False`` to avoid
        recursing back into the RPC. Pin that path works without
        needing a server up."""
        rec = admin.AdminUpdateRecord(
            last_updated_at="2026-05-30T08:00:00+00:00",
            last_success=True,
            last_sha="internal0000",
        )
        admin.write_admin_status({"e": rec}, via_rpc=False)
        records = admin.read_admin_status(via_rpc=False)
        assert records["e"].last_sha == "internal0000"


# ----------------------------------------------------------------------
# Multi-user auth on set_admin_status
# ----------------------------------------------------------------------


class TestMultiUserAuth:
    def test_set_without_token_rejected_in_multi_user(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(state_dir / "mu"))
        (state_dir / "mu").mkdir()
        server = rpc.RPCServer(multi_user=True)
        rpc.register_set_admin_status_method(
            server,
            admin.replace_admin_status_record_from_mapping,
        )
        try:
            server.start()
            time.sleep(0.05)
            with pytest.raises(rpc.RPCError, match="admin token required"):
                rpc.call(
                    "set_admin_status",
                    {
                        "env": "vibeqc-dev",
                        "record": {
                            "last_updated_at": "2026-05-30T10:00:00+00:00",
                            "last_success": True,
                        },
                    },
                    multi_user=True,
                )
        finally:
            server.stop()

    def test_get_open_in_multi_user(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Read methods don't require auth — the data isn't sensitive."""
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(state_dir / "mu"))
        (state_dir / "mu").mkdir()
        server = rpc.RPCServer(multi_user=True)
        rpc.register_get_admin_status_method(
            server,
            lambda: admin.read_admin_status(via_rpc=False),
        )
        try:
            server.start()
            time.sleep(0.05)
            result = rpc.call("get_admin_status", multi_user=True)
            assert result == {}
        finally:
            server.stop()


# ----------------------------------------------------------------------
# Concurrency — many simultaneous reads must all succeed
# ----------------------------------------------------------------------


class TestConcurrency:
    def test_eight_concurrent_pings_all_succeed(
        self, running_server: rpc.RPCServer,
    ) -> None:
        """Eight client threads each call ping(). All eight must
        return a result; the server's single-thread accept loop
        handles them serially without dropping any."""
        results: list = []
        errors: list = []

        def worker() -> None:
            try:
                r = rpc.ping(multi_user=False)
                results.append(r)
            except Exception as e:  # noqa: BLE001
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10.0)
        assert errors == [], f"errors: {errors}"
        assert len(results) == 8
        for r in results:
            assert r is not None and "version" in r


# ----------------------------------------------------------------------
# Drain RPC methods — v0.8.1 *Karp's Reduction*
# ----------------------------------------------------------------------


class TestDrainStateRoundtrip:
    def test_get_returns_none_when_no_drain(
        self, running_server: rpc.RPCServer,
    ) -> None:
        result = rpc.call("get_drain_state")
        assert result is None

    def test_set_full_drain_then_get(
        self, running_server: rpc.RPCServer,
    ) -> None:
        rpc.call("set_drain_state", {
            "state": {
                "enabled": True,
                "max_jobs": None,
                "max_cpus": None,
                "reason": "kids gaming",
            },
        })
        result = rpc.call("get_drain_state")
        assert result is not None
        assert result["enabled"] is True
        assert result["reason"] == "kids gaming"
        assert result["max_jobs"] is None

    def test_set_partial_drain(
        self, running_server: rpc.RPCServer,
    ) -> None:
        rpc.call("set_drain_state", {
            "state": {
                "enabled": True,
                "max_jobs": 1,
                "max_cpus": 4,
                "reason": "small jobs only",
            },
        })
        result = rpc.call("get_drain_state")
        assert result["max_jobs"] == 1
        assert result["max_cpus"] == 4

    def test_set_none_clears_drain(
        self, running_server: rpc.RPCServer,
    ) -> None:
        rpc.call("set_drain_state", {
            "state": {"enabled": True, "reason": "x"},
        })
        # Confirm it's set.
        assert rpc.call("get_drain_state") is not None
        # Clear by passing None.
        result = rpc.call("set_drain_state", {"state": None})
        assert result["cleared"] is True
        assert result["ok"] is True
        assert rpc.call("get_drain_state") is None

    def test_set_none_when_no_drain_returns_cleared_false(
        self, running_server: rpc.RPCServer,
    ) -> None:
        """Clearing a non-existent drain is idempotent — returns
        cleared=False rather than raising."""
        result = rpc.call("set_drain_state", {"state": None})
        assert result["cleared"] is False
        assert result["ok"] is True

    def test_set_unknown_fields_stripped(
        self, running_server: rpc.RPCServer,
    ) -> None:
        """A future client writing a field the daemon doesn't know
        about must not crash the daemon's pydantic validation."""
        rpc.call("set_drain_state", {
            "state": {
                "enabled": True,
                "reason": "newer client",
                "future_field_unknown_to_daemon": "ignored",
            },
        })
        result = rpc.call("get_drain_state")
        assert "future_field_unknown_to_daemon" not in result
        assert result["reason"] == "newer client"


class TestDrainViaRpcPlumbing:
    def test_read_via_rpc_uses_daemon_view(
        self, running_server: rpc.RPCServer,
    ) -> None:
        """A drain set via RPC is visible to ``read_drain_state(via_rpc=True)``."""
        rpc.call("set_drain_state", {
            "state": {"enabled": True, "reason": "rpc-write"},
        })
        state = drain.read_drain_state(via_rpc=True)
        assert state is not None
        assert state.reason == "rpc-write"

    def test_write_via_rpc_visible_to_direct_read(
        self, running_server: rpc.RPCServer,
    ) -> None:
        """A drain written via the via_rpc=True API is observable via
        a direct file read (single-user — same file)."""
        st = drain.DrainState(
            enabled=True, max_jobs=2, reason="from-cli",
        )
        drain.write_drain_state(st, via_rpc=True)
        # Direct file read.
        direct = drain.read_drain_state(via_rpc=False)
        assert direct is not None
        assert direct.reason == "from-cli"
        assert direct.max_jobs == 2

    def test_clear_via_rpc(
        self, running_server: rpc.RPCServer,
    ) -> None:
        st = drain.DrainState(enabled=True, reason="to-clear")
        drain.write_drain_state(st, via_rpc=True)
        cleared = drain.clear_drain(via_rpc=True)
        assert cleared is True
        assert drain.read_drain_state(via_rpc=True) is None

    def test_owned_full_release_via_rpc_is_one_daemon_transaction(
        self,
        running_server: rpc.RPCServer,
    ) -> None:
        owned = drain.DrainState(
            full_dispatch=True,
            scheduler_hosts=["host_f"],
            max_jobs=2,
            reason="fleet-rollout:operation-123",
            set_at="2026-08-10T16:00:00+00:00",
        )
        drain.write_drain_state(owned, via_rpc=False)

        assert drain.release_owned_full_drain(
            expected_reason=owned.reason or "",
            expected_set_at=owned.set_at,
            multi_user=False,
        ) is True

        stored = drain.read_drain_state(via_rpc=False)
        assert stored is not None
        assert stored.scheduler_hosts == ["host_f"]
        assert stored.max_jobs == 2
        assert stored.full_dispatch is False

    def test_read_via_rpc_falls_back_when_daemon_down(
        self, state_dir: Path,
    ) -> None:
        """No daemon running — read still works via the file path."""
        st = drain.DrainState(enabled=True, reason="written-direct")
        drain.write_drain_state(st, via_rpc=False)
        # Read with default via_rpc=True; should fall back.
        result = drain.read_drain_state(via_rpc=True)
        assert result is not None
        assert result.reason == "written-direct"

    def test_internal_via_rpc_false_short_circuits(
        self, state_dir: Path,
    ) -> None:
        """Daemon-internal callers pass via_rpc=False — pin that path
        works without a server up."""
        st = drain.DrainState(enabled=True, reason="internal")
        drain.write_drain_state(st, via_rpc=False)
        result = drain.read_drain_state(via_rpc=False)
        assert result is not None
        assert result.reason == "internal"


class TestDrainMultiUserAuth:
    def test_set_without_token_rejected_in_multi_user(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(state_dir / "mu"))
        (state_dir / "mu").mkdir()
        server = rpc.RPCServer(multi_user=True)
        mutation_calls: list[str] = []
        rpc.register_set_drain_state_method(
            server,
            clear_state=lambda: mutation_calls.append("clear") or True,
            replace_state=lambda _state: (
                mutation_calls.append("replace") or True
            ),
        )
        try:
            server.start()
            time.sleep(0.05)
            with pytest.raises(rpc.RPCError, match="admin token required"):
                rpc.call(
                    "set_drain_state",
                    {"state": {"enabled": True}},
                    multi_user=True,
                )
            assert mutation_calls == []
        finally:
            server.stop()

    def test_clear_without_token_rejected_in_multi_user(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Clearing (state=None) also goes through set_drain_state,
        so the token gate fires the same way."""
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(state_dir / "mu"))
        (state_dir / "mu").mkdir()
        server = rpc.RPCServer(multi_user=True)
        mutation_calls: list[str] = []
        rpc.register_set_drain_state_method(
            server,
            clear_state=lambda: mutation_calls.append("clear") or True,
            replace_state=lambda _state: (
                mutation_calls.append("replace") or True
            ),
        )
        try:
            server.start()
            time.sleep(0.05)
            with pytest.raises(rpc.RPCError, match="admin token required"):
                rpc.call(
                    "set_drain_state",
                    {"state": None},
                    multi_user=True,
                )
            assert mutation_calls == []
        finally:
            server.stop()

    def test_owned_full_release_auth_precedes_callback(
        self,
        state_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        server = rpc.RPCServer(multi_user=True)
        calls: list[tuple[str, str]] = []
        rpc.register_owned_full_drain_release_method(
            server,
            lambda expected_reason, expected_set_at: (
                calls.append((expected_reason, expected_set_at)) or True
            ),
        )
        monkeypatch.setattr(auth, "verify_admin_token", lambda _token: False)

        with pytest.raises(PermissionError, match="admin token required"):
            server._methods[  # noqa: SLF001
                drain.OWNED_FULL_DRAIN_RELEASE_RPC_METHOD
            ](
                expected_reason="fleet-rollout:operation-123",
                expected_set_at="2026-08-10T16:00:00+00:00",
                token="wrong",
            )

        assert calls == []

    @pytest.mark.parametrize(
        ("expected_reason", "expected_set_at"),
        [
            ("", "2026-08-10T16:00:00+00:00"),
            ("   ", "2026-08-10T16:00:00+00:00"),
            ("fleet-rollout:operation-123", ""),
            ("fleet-rollout:operation-123", "   "),
            (None, "2026-08-10T16:00:00+00:00"),
            ("fleet-rollout:operation-123", None),
        ],
    )
    def test_owned_full_release_rejects_invalid_pair_before_callback(
        self,
        expected_reason: object,
        expected_set_at: object,
    ) -> None:
        server = rpc.RPCServer(multi_user=False)
        calls: list[object] = []
        rpc.register_owned_full_drain_release_method(
            server,
            lambda *_args: calls.append(object()) or True,
        )

        with pytest.raises(ValueError, match="non-empty string"):
            server._methods[  # noqa: SLF001
                drain.OWNED_FULL_DRAIN_RELEASE_RPC_METHOD
            ](
                expected_reason=expected_reason,
                expected_set_at=expected_set_at,
            )

        assert calls == []

    def test_owned_full_release_registration_forwards_exact_pair(self) -> None:
        server = rpc.RPCServer(multi_user=False)
        calls: list[tuple[str, str]] = []
        rpc.register_owned_full_drain_release_method(
            server,
            lambda expected_reason, expected_set_at: (
                calls.append((expected_reason, expected_set_at)) or True
            ),
        )

        result = server._methods[  # noqa: SLF001
            drain.OWNED_FULL_DRAIN_RELEASE_RPC_METHOD
        ](
            expected_reason="fleet-rollout:operation-123",
            expected_set_at="2026-08-10T16:00:00+00:00",
        )

        assert calls == [
            (
                "fleet-rollout:operation-123",
                "2026-08-10T16:00:00+00:00",
            )
        ]
        assert result == {"changed": True, "ok": True}

    def test_scheduler_lease_mutation_without_token_is_rejected(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(state_dir / "mu"))
        (state_dir / "mu").mkdir()
        server = rpc.RPCServer(multi_user=True)
        mutation_calls: list[object] = []
        rpc.register_set_scheduler_drain_lease_method(
            server,
            lambda *_args: mutation_calls.append(object()),
            schema_version=drain.SCHEDULER_DRAIN_LEASE_SCHEMA_VERSION,
        )
        try:
            server.start()
            time.sleep(0.05)
            with pytest.raises(rpc.RPCError, match="admin token required"):
                rpc.call(
                    drain.SCHEDULER_DRAIN_LEASE_RPC_METHOD,
                    {
                        "schema_version": 1,
                        "lease": {
                            "lease_id": "audit-auth-lease",
                            "scheduler_host": "host_f",
                            "owner": "fleet-rollout:test:host_f",
                        },
                    },
                    multi_user=True,
                )
            assert mutation_calls == []
        finally:
            server.stop()

    def test_get_open_in_multi_user(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Read is open — drain state is operator-visible, not sensitive."""
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(state_dir / "mu"))
        (state_dir / "mu").mkdir()
        server = rpc.RPCServer(multi_user=True)
        rpc.register_get_drain_state_method(
            server,
            lambda: drain.read_drain_state(
                via_rpc=False,
                multi_user=server.multi_user,
            ),
        )
        try:
            server.start()
            time.sleep(0.05)
            result = rpc.call("get_drain_state", multi_user=True)
            assert result is None
        finally:
            server.stop()


# ----------------------------------------------------------------------
# Throttle RPC methods — v0.8.1 *Karp's Reduction*
# ----------------------------------------------------------------------


class TestThrottleStateRoundtrip:
    def test_get_returns_none_when_no_throttle(
        self, running_server: rpc.RPCServer,
    ) -> None:
        result = rpc.call("get_throttle_state")
        assert result is None

    def test_get_preserves_auto_expiry(
        self, running_server: rpc.RPCServer,
    ) -> None:
        throttle.write_throttle_state(
            throttle.ThrottleState(
                weight=20,
                set_at="2000-01-01T00:00:00+00:00",
                duration_seconds=1,
            ),
            via_rpc=False,
        )
        assert throttle.throttle_state_path().exists()

        result = rpc.call("get_throttle_state")

        assert result is None
        assert not throttle.throttle_state_path().exists()

    def test_set_then_get_returns_the_weight(
        self, running_server: rpc.RPCServer,
    ) -> None:
        rpc.call("set_throttle_state", {
            "state": {"weight": 20, "reason": "kids gaming"},
        })
        result = rpc.call("get_throttle_state")
        assert result is not None
        assert result["weight"] == 20
        assert result["reason"] == "kids gaming"

    def test_set_none_clears_throttle(
        self, running_server: rpc.RPCServer,
    ) -> None:
        rpc.call("set_throttle_state", {"state": {"weight": 50}})
        assert rpc.call("get_throttle_state") is not None
        result = rpc.call("set_throttle_state", {"state": None})
        assert result["cleared"] is True
        assert rpc.call("get_throttle_state") is None

    def test_set_unknown_fields_stripped(
        self, running_server: rpc.RPCServer,
    ) -> None:
        rpc.call("set_throttle_state", {
            "state": {
                "weight": 30,
                "future_field_unknown_to_daemon": "ignored",
            },
        })
        result = rpc.call("get_throttle_state")
        assert "future_field_unknown_to_daemon" not in result
        assert result["weight"] == 30


class TestThrottleViaRpcPlumbing:
    def test_read_via_rpc_uses_daemon_view(
        self, running_server: rpc.RPCServer,
    ) -> None:
        rpc.call("set_throttle_state", {
            "state": {"weight": 25, "reason": "rpc-write"},
        })
        state = throttle.read_throttle_state(via_rpc=True)
        assert state is not None
        assert state.weight == 25
        assert state.reason == "rpc-write"

    def test_write_via_rpc_visible_to_direct_read(
        self, running_server: rpc.RPCServer,
    ) -> None:
        st = throttle.ThrottleState(weight=15, reason="from-cli")
        throttle.write_throttle_state(st, via_rpc=True)
        direct = throttle.read_throttle_state(via_rpc=False)
        assert direct is not None
        assert direct.weight == 15
        assert direct.reason == "from-cli"

    def test_clear_via_rpc(
        self, running_server: rpc.RPCServer,
    ) -> None:
        st = throttle.ThrottleState(weight=40)
        throttle.write_throttle_state(st, via_rpc=True)
        cleared = throttle.clear_throttle_state(via_rpc=True)
        assert cleared is True
        assert throttle.read_throttle_state(via_rpc=True) is None

    def test_read_via_rpc_falls_back_when_daemon_down(
        self, state_dir: Path,
    ) -> None:
        """No daemon running — read still works via the file path."""
        st = throttle.ThrottleState(weight=10, reason="written-direct")
        throttle.write_throttle_state(st, via_rpc=False)
        result = throttle.read_throttle_state(via_rpc=True)
        assert result is not None
        assert result.weight == 10
        assert result.reason == "written-direct"

    def test_internal_via_rpc_false_short_circuits(
        self, state_dir: Path,
    ) -> None:
        st = throttle.ThrottleState(weight=99, reason="internal")
        throttle.write_throttle_state(st, via_rpc=False)
        result = throttle.read_throttle_state(via_rpc=False)
        assert result is not None
        assert result.weight == 99


class TestThrottleMultiUserAuth:
    def test_set_without_token_rejected_in_multi_user(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(state_dir / "mu"))
        (state_dir / "mu").mkdir()
        server = rpc.RPCServer(multi_user=True)
        rpc.register_set_throttle_state_method(
            server,
            clear_state=lambda: throttle.clear_throttle_state(via_rpc=False),
            replace_state=throttle.replace_throttle_state_from_mapping,
        )
        try:
            server.start()
            time.sleep(0.05)
            with pytest.raises(rpc.RPCError, match="admin token required"):
                rpc.call(
                    "set_throttle_state",
                    {"state": {"weight": 50}},
                    multi_user=True,
                )
        finally:
            server.stop()

    def test_clear_without_token_rejected_in_multi_user(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(state_dir / "mu"))
        (state_dir / "mu").mkdir()
        server = rpc.RPCServer(multi_user=True)
        rpc.register_set_throttle_state_method(
            server,
            clear_state=lambda: throttle.clear_throttle_state(via_rpc=False),
            replace_state=throttle.replace_throttle_state_from_mapping,
        )
        try:
            server.start()
            time.sleep(0.05)
            with pytest.raises(rpc.RPCError, match="admin token required"):
                rpc.call(
                    "set_throttle_state",
                    {"state": None},
                    multi_user=True,
                )
        finally:
            server.stop()

    def test_get_open_in_multi_user(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(state_dir / "mu"))
        (state_dir / "mu").mkdir()
        server = rpc.RPCServer(multi_user=True)
        rpc.register_get_throttle_state_method(
            server,
            lambda: throttle.read_throttle_state(via_rpc=False),
        )
        rpc.register_set_throttle_state_method(
            server,
            clear_state=lambda: throttle.clear_throttle_state(via_rpc=False),
            replace_state=throttle.replace_throttle_state_from_mapping,
        )
        try:
            server.start()
            time.sleep(0.05)
            result = rpc.call("get_throttle_state", multi_user=True)
            assert result is None
        finally:
            server.stop()


# ----------------------------------------------------------------------
# Cross-method: all method sets register without collision
# ----------------------------------------------------------------------


# ----------------------------------------------------------------------
# v0.8.4 *Brooks's Mythical* — RPC method introspection
# ----------------------------------------------------------------------


class TestGetMethodsIntrospection:
    def test_get_methods_returns_sorted_list(
        self, running_server: rpc.RPCServer,
    ) -> None:
        """``get_methods`` returns the registered method names + the
        daemon version + multi-user flag. The methods list is sorted
        so monitoring-script diffs are stable across daemon restarts.

        The running_server fixture registers admin-status + drain + throttle +
        capacity, plus the built-in ``ping`` + ``get_methods``."""
        result = rpc.call("get_methods")
        assert isinstance(result, dict)
        assert "methods" in result and "version" in result
        methods = result["methods"]
        assert isinstance(methods, list)
        # Sorted-ness check.
        assert methods == sorted(methods)
        # Every expected method present.
        expected = {
            "ping", "get_methods", "get_process_identity",
            "get_admin_status", "set_admin_status",
            "get_drain_state", "set_drain_state",
            "set_owned_full_drain_release",
            "get_throttle_state", "set_throttle_state",
            "get_daemon_capacity",
        }
        assert expected.issubset(set(methods)), (
            f"missing methods: {expected - set(methods)}"
        )

    def test_get_methods_includes_version(
        self, running_server: rpc.RPCServer,
    ) -> None:
        from vq import __version__
        result = rpc.call("get_methods")
        assert result["version"] == __version__
        assert result["multi_user"] is False

    def test_get_methods_registered_by_default(
        self, state_dir: Path,
    ) -> None:
        """Even a bare RPCServer with no extra method sets has
        ``ping`` + identity + ``get_methods`` available — they're built-ins
        registered in ``__init__``."""
        server = rpc.RPCServer(multi_user=False)
        try:
            server.start()
            time.sleep(0.05)
            result = rpc.call("get_methods")
            assert sorted(result["methods"]) == [
                "get_methods",
                "get_process_identity",
                "ping",
            ]
        finally:
            server.stop()

    def test_get_methods_open_in_multi_user(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Method introspection is open — no auth gate. Caller doesn't
        even know what to ask for without it."""
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(state_dir / "mu"))
        (state_dir / "mu").mkdir()
        server = rpc.RPCServer(multi_user=True)
        try:
            server.start()
            time.sleep(0.05)
            result = rpc.call("get_methods", multi_user=True)
            assert "methods" in result
            assert result["multi_user"] is True
        finally:
            server.stop()


class TestAllRegisteredTogether:
    def test_all_three_method_sets_coexist(
        self, running_server: rpc.RPCServer,
    ) -> None:
        """The daemon registers admin-status + drain + throttle on the
        same RPCServer. None of them should collide on method names;
        each handler responds independently."""
        # Use one of each — confirms the dispatcher routes to the
        # right handler for each.
        admin_result = rpc.call("get_admin_status")
        drain_result = rpc.call("get_drain_state")
        throttle_result = rpc.call("get_throttle_state")
        # Initial state: all empty / None.
        assert admin_result == {}
        assert drain_result is None
        assert throttle_result is None
        # Write to each independently.
        rpc.call("set_drain_state", {"state": {"enabled": True}})
        rpc.call("set_throttle_state", {"state": {"weight": 25}})
        # Both writes visible; admin still empty.
        assert rpc.call("get_admin_status") == {}
        assert rpc.call("get_drain_state")["enabled"] is True
        assert rpc.call("get_throttle_state")["weight"] == 25


class TestSchedulerRefreshIsolation:
    """#766: actual sockets stay responsive while scheduler observations wait."""

    @staticmethod
    def refresh() -> dict:
        return rpc.call(
            rpc.SCHEDULER_STATUS_REFRESH_RPC_METHOD,
            {"jobid": "95cfbc03cf3e", "timeout_seconds": 5.0},
            timeout=6.0,
        )

    @staticmethod
    def unavailable() -> dict:
        return {
            "schema": "vq.scheduler.status_refresh/1",
            "completed": False,
            "observed_at": None,
            "reason": "timeout",
        }

    def test_wait_does_not_block_health_or_admin(self, running_server):
        from concurrent.futures import ThreadPoolExecutor

        entered, release = threading.Event(), threading.Event()

        def refresh(jobid, timeout):
            entered.set()
            assert release.wait(5)
            return self.unavailable()

        rpc.register_get_scheduler_status_refresh_method(running_server, refresh)
        with ThreadPoolExecutor(max_workers=1) as clients:
            pending = clients.submit(self.refresh)
            try:
                assert entered.wait(2)
                assert rpc.call("ping", timeout=0.5)["version"]
                assert "ping" in rpc.call("get_methods", timeout=0.5)["methods"]
                rpc.call("set_drain_state", {"state": {"enabled": True}}, timeout=0.5)
                assert rpc.call("get_drain_state", timeout=0.5)["enabled"]
                assert not pending.done()
            finally:
                release.set()
            assert pending.result(timeout=2) == self.unavailable()

    def test_saturation_refuses_without_blocking_health(self, running_server):
        from concurrent.futures import ThreadPoolExecutor

        release = threading.Event()
        entered = [threading.Event() for _ in range(4)]
        count = 0
        lock = threading.Lock()

        def refresh(jobid, timeout):
            nonlocal count
            with lock:
                position = count
                count += 1
            if position < 4:
                entered[position].set()
            assert release.wait(5)
            return self.unavailable()

        rpc.register_get_scheduler_status_refresh_method(running_server, refresh)
        with ThreadPoolExecutor(max_workers=4) as clients:
            pending = [clients.submit(self.refresh) for _ in range(4)]
            try:
                assert all(event.wait(2) for event in entered)
                with pytest.raises(rpc.RPCError, match="scheduler refresh busy"):
                    rpc.call(
                        rpc.SCHEDULER_STATUS_REFRESH_RPC_METHOD,
                        {"jobid": "95cfbc03cf3e", "timeout_seconds": 1.0},
                        timeout=0.5,
                    )
                assert count == 4
                assert rpc.call("ping", timeout=0.5)["version"]
            finally:
                release.set()
            assert all(p.result(timeout=2) == self.unavailable() for p in pending)
        assert self.refresh() == self.unavailable()

    def test_handler_errors_release_capacity(self, running_server):
        def broken(jobid, timeout):
            raise ValueError("observation refused")

        rpc.register_get_scheduler_status_refresh_method(running_server, broken)
        for _ in range(8):
            with pytest.raises(rpc.RPCError, match="observation refused"):
                self.refresh()
        assert rpc.call("ping", timeout=0.5)["version"]

    def test_disconnect_releases_wait_capacity(self, running_server):
        entered, release = threading.Event(), threading.Event()

        def refresh(jobid, timeout):
            entered.set()
            assert release.wait(5)
            return self.unavailable()

        rpc.register_get_scheduler_status_refresh_method(running_server, refresh)
        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            client.connect(str(rpc.socket_path()))
            client.sendall((json.dumps({
                "method": rpc.SCHEDULER_STATUS_REFRESH_RPC_METHOD,
                "args": {"jobid": "95cfbc03cf3e", "timeout_seconds": 5.0},
            }) + "\n").encode())
            assert entered.wait(2)
            client.close()
            assert rpc.call("ping", timeout=0.5)["version"]
        finally:
            release.set()
            client.close()
        # A disconnected callback may finish only after its bounded wait. A
        # second full four-reader wave proves that it eventually returns capacity.
        deadline = time.monotonic() + 2
        with running_server._refresh_lock:
            active = bool(running_server._refresh_connections)
        while active and time.monotonic() < deadline:
            time.sleep(0.01)
            with running_server._refresh_lock:
                active = bool(running_server._refresh_connections)
        assert not active
        self.test_saturation_refuses_without_blocking_health(running_server)

    def test_stop_disconnects_waiter_before_replacement(self, state_dir):
        from concurrent.futures import ThreadPoolExecutor

        entered, release = threading.Event(), threading.Event()
        server, replacement = rpc.RPCServer(), rpc.RPCServer()

        def refresh(jobid, timeout):
            entered.set()
            assert release.wait(5)
            return self.unavailable()

        rpc.register_get_scheduler_status_refresh_method(server, refresh)
        server.start()
        with ThreadPoolExecutor(max_workers=1) as clients:
            pending = clients.submit(self.refresh)
            try:
                assert entered.wait(2)
                server.stop()
                with pytest.raises(rpc.RPCError):
                    pending.result(timeout=1)
                replacement.start()
                assert rpc.call("ping", timeout=0.5)["version"]
                assert not release.is_set()
            finally:
                release.set()
                replacement.stop()
                server.stop()

    def test_worker_start_failure_returns_capacity(self, running_server, monkeypatch):
        original = threading.Thread.start

        def refuse(thread):
            if thread.name == "vq-rpc-scheduler-refresh":
                raise RuntimeError("cannot start refresh worker")
            return original(thread)

        rpc.register_get_scheduler_status_refresh_method(
            running_server, lambda jobid, timeout: self.unavailable(),
        )
        monkeypatch.setattr(threading.Thread, "start", refuse)
        for _ in range(5):
            with pytest.raises(rpc.RPCError):
                self.refresh()
        monkeypatch.setattr(threading.Thread, "start", original)
        assert self.refresh() == self.unavailable()
