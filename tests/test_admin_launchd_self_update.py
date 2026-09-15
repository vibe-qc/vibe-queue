"""macOS launchd self-update restart and provenance gates."""
from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

from vq import admin, config

pytestmark = pytest.mark.no_autopatch_self_update_probe

EXPECTED_SHA = "a" * 40
STALE_SHA = "b" * 40


def _ok_proc(stdout: str = "", stderr: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess([], 0, stdout=stdout, stderr=stderr)


def _fail_proc(rc: int, stderr: str) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess([], rc, stdout="", stderr=stderr)


def _program(tmp_path: Path) -> config.VenvProgram:
    venv_bin = tmp_path / "repo" / ".venv" / "bin"
    venv_bin.mkdir(parents=True)
    python = venv_bin / "python"
    python.write_text("")
    return config.VenvProgram(
        kind="venv",
        python=str(python),
        git_dir=str(tmp_path / "repo"),
        branch="main",
    )


def _successful_update(prog: config.VenvProgram) -> admin.UpdateResult:
    return admin.UpdateResult(
        env="vibeqc-queue",
        git_dir=str(prog.git_dir),
        branch="main",
        update_script=None,
        git_pull_rc=0,
    )


def _launchd_probe() -> admin._SelfUpdateProbe:
    return admin._SelfUpdateProbe(
        is_self_update=True,
        daemon_running=True,
        service_manager="launchd",
        manager_available=True,
        diagnostic="launchd agent matches managed venv",
    )


def test_service_manager_selection_is_platform_deterministic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(admin.sys, "platform", "darwin")
    monkeypatch.setattr(admin.shutil, "which", lambda name: "/bin/launchctl")
    monkeypatch.setattr(admin, "_systemctl_user_available", lambda: True)

    assert admin._select_daemon_service_manager() is admin._DaemonServiceManager.LAUNCHD

    monkeypatch.setattr(admin.sys, "platform", "linux")
    assert admin._select_daemon_service_manager() is admin._DaemonServiceManager.SYSTEMD


def test_launchd_agent_detection_matches_generated_service(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prog = _program(tmp_path)
    launchd_output = (
        "gui/501/com.vq.daemon = {\n"
        f"\tprogram = {prog.python}\n"
        "\tstate = running\n"
        "\tpid = 4321\n"
        "}\n"
    )
    monkeypatch.setattr(
        admin,
        "_select_daemon_service_manager",
        lambda: admin._DaemonServiceManager.LAUNCHD,
    )
    monkeypatch.setattr(admin.sys, "executable", str(prog.python))
    monkeypatch.setattr(admin, "_query_launchd_daemon", lambda: (0, launchd_output))
    monkeypatch.setattr(
        admin,
        "_launchd_plist_argv",
        lambda: [str(prog.python), "-m", "vq", "daemon", "run"],
    )

    probe = admin._detect_vq_self_update(prog)

    assert probe.is_self_update is True
    assert probe.daemon_running is True
    assert probe.service_manager == "launchd"
    assert probe.manager_available is True
    assert "pid=4321" in probe.diagnostic


def test_systemd_restart_command_is_preserved(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(admin, "_query_daemon_service_pid", lambda manager: 22)
    with patch("vq.admin.subprocess.run", return_value=_ok_proc()) as run:
        ok, message = admin._restart_vq_daemon(
            manager=admin._DaemonServiceManager.SYSTEMD,
            pre_pid=11,
        )

    assert ok is True
    assert "PID 11 -> 22" in message
    assert run.call_args.args[0] == [
        "systemctl",
        "--user",
        "restart",
        "vq-daemon",
    ]


def test_launchd_restart_success_uses_kickstart(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(admin, "_query_daemon_service_pid", lambda manager: 22)
    with patch("vq.admin.subprocess.run", return_value=_ok_proc()) as run:
        ok, message = admin._restart_vq_daemon(
            manager=admin._DaemonServiceManager.LAUNCHD,
            pre_pid=11,
        )

    assert ok is True
    assert "PID 11 -> 22" in message
    assert run.call_args.args[0] == [
        "launchctl",
        "kickstart",
        "-k",
        f"gui/{admin.os.getuid()}/com.vq.daemon",
    ]


def test_launchd_restart_failure_preserves_stderr() -> None:
    with patch(
        "vq.admin.subprocess.run",
        return_value=_fail_proc(5, "Could not find service com.vq.daemon"),
    ):
        ok, message = admin._restart_vq_daemon(
            manager=admin._DaemonServiceManager.LAUNCHD,
            pre_pid=11,
        )

    assert ok is False
    assert "rc=5" in message
    assert "Could not find service com.vq.daemon" in message


def test_launchd_self_update_requires_verified_rpc_source(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prog = _program(tmp_path)
    result = _successful_update(prog)
    monkeypatch.setattr(admin, "_detect_vq_self_update", lambda unused: _launchd_probe())
    monkeypatch.setattr(admin, "_query_daemon_service_pid", lambda manager: 11)
    monkeypatch.setattr(admin, "current_source_sha", lambda path=None: EXPECTED_SHA)
    monkeypatch.setattr(
        admin,
        "_restart_vq_daemon",
        lambda **kwargs: (True, "launchctl kickstart succeeded"),
    )
    monkeypatch.setattr(
        admin,
        "_verify_restarted_daemon",
        lambda sha, *, expected_tree_sha256=None: admin.DaemonProvenance(
            verified=False,
            actual_sha=STALE_SHA,
            actual_tree_sha256=None,
            detail=(
                f"RPC source SHA {STALE_SHA} does not match "
                f"expected {EXPECTED_SHA}"
            ),
        ),
    )

    admin._maybe_restart_daemon(prog, result, restart_daemon=True)

    assert result.daemon_restart_attempted is True
    assert result.daemon_restart_succeeded is False
    assert result.daemon_health_verified is False
    assert result.daemon_expected_source_sha == EXPECTED_SHA
    assert result.daemon_actual_source_sha == STALE_SHA
    assert result.success is False
    assert "does not match" in result.daemon_restart_message


def test_post_restart_rpc_provenance_failure_is_explicit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from vq import rpc

    monkeypatch.delenv("VQ_DAEMON_HEALTH_TIMEOUT", raising=False)
    monkeypatch.setattr(admin, "DAEMON_HEALTH_TIMEOUT_SECONDS", 0.01)
    monkeypatch.setattr(
        rpc,
        "ping_user_daemon",
        lambda: {
            "version": "0.12.0",
            "multi_user": False,
            "source_sha": STALE_SHA,
        },
    )

    provenance = admin._verify_restarted_daemon(EXPECTED_SHA)

    assert provenance.verified is False
    assert provenance.actual_sha == STALE_SHA
    assert STALE_SHA in provenance.detail
    assert EXPECTED_SHA in provenance.detail


def test_restart_verification_pings_the_daemon_it_restarted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """host_a regression: the provenance ping must target the user daemon.

    The restart lane only ever restarts the user daemon (``systemctl
    --user`` / launchd user domain). On a host with ``[multi_user]
    enabled=true`` in ``/etc/vq/config.toml``, routing the post-restart
    ping by the multi-user flag sent it to the root system daemon — a
    non-git install whose ping carries no ``source_sha`` — so verification
    could never pass and every host_a update needed ``mark-ok``.

    Asserted at the socket layer, not the flag layer: host_d showed that
    ``multi_user=False`` alone does not pin the destination, because
    ``$VQ_STATE_DIR`` reroutes it (see the ``user_socket_path`` tests).
    """
    from vq import rpc

    seen: list[Path | None] = []

    def fake_call(method, args=None, **kwargs):  # type: ignore[no-untyped-def]
        seen.append(kwargs.get("socket_override"))
        assert kwargs.get("multi_user", False) is False
        return {"version": "0.12.1", "source_sha": EXPECTED_SHA}

    monkeypatch.setattr(rpc, "call", fake_call)

    provenance = admin._verify_restarted_daemon(EXPECTED_SHA)

    assert provenance.verified is True
    assert provenance.actual_sha == EXPECTED_SHA
    assert seen == [rpc.user_socket_path()]


def test_multi_user_answer_is_a_routing_fault_not_a_sha_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """host_d regression: never grade the root daemon's provenance.

    host_d's root daemon *does* carry a ``source_sha`` (a ``/opt/vq``
    ``SOURCE-SHA`` marker pinned one release back), so when the ping was
    misrouted to it the failure read as a confident provenance mismatch
    naming that commit — indistinguishable from a user daemon frozen a
    release behind, and the reason two verified-correct restarts were
    reported as failures. An envelope that says ``multi_user`` can never be
    the daemon this lane restarted, so it must be reported as a misroute.
    """
    from vq import rpc

    monkeypatch.delenv("VQ_DAEMON_HEALTH_TIMEOUT", raising=False)
    monkeypatch.setattr(admin, "DAEMON_HEALTH_TIMEOUT_SECONDS", 0.01)
    monkeypatch.setattr(
        rpc,
        "ping_user_daemon",
        lambda: {
            "version": "0.24.0",
            "multi_user": True,
            "source_sha": STALE_SHA,
        },
    )

    provenance = admin._verify_restarted_daemon(EXPECTED_SHA)

    assert provenance.verified is False
    # The root daemon's SHA must not be presented as the user daemon's.
    assert provenance.actual_sha is None
    assert STALE_SHA not in provenance.detail
    assert "multi-user daemon" in provenance.detail
    assert "VQ_STATE_DIR" in provenance.detail


def _spec_files(queue: Path, count: int) -> None:
    queue.mkdir(parents=True, exist_ok=True)
    for i in range(count):
        (queue / f"job{i:05d}.json").write_text("{}")


def test_daemon_health_window_scales_with_queued_specs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """host_c2 regression 2026-07-25: a driver carrying ~12.6k job
    specs kept the daemon's startup resume scan 10-30 s past the flat
    30 s readiness window, so two healthy self-update restarts reported
    "daemon restart FAILED" and each needed a manual ``vq admin
    mark-ok``. The window must grow with the spec population the
    restarted daemon has to scan."""
    from vq import paths

    state = tmp_path / "scaled-state"
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(state))
    monkeypatch.delenv("VQ_DAEMON_HEALTH_TIMEOUT", raising=False)

    # Empty (or absent) queue dir: base window only.
    assert admin._daemon_health_timeout() == pytest.approx(
        admin.DAEMON_HEALTH_TIMEOUT_SECONDS
    )

    _spec_files(state / "queue", 4000)
    # Lock sidecars must not inflate the count.
    (state / "queue" / "job00000.json.lock").write_text("")
    expected = (
        admin.DAEMON_HEALTH_TIMEOUT_SECONDS
        + 4000 * admin.DAEMON_HEALTH_SECONDS_PER_SPEC
    )
    assert admin._daemon_health_timeout() == pytest.approx(expected)

    # The shipped per-spec allowance must cover the observed incident:
    # RPC verified ~10-30 s after the flat 30 s window expired at
    # 12618 specs, so the scaled window has to clear that with margin.
    incident = (
        admin.DAEMON_HEALTH_TIMEOUT_SECONDS
        + 12618 * admin.DAEMON_HEALTH_SECONDS_PER_SPEC
    )
    assert incident >= 120


def test_daemon_health_window_env_override_and_cap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from vq import paths

    state = tmp_path / "capped-state"
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(state))
    _spec_files(state / "queue", 50)

    # Operator override wins outright.
    monkeypatch.setenv("VQ_DAEMON_HEALTH_TIMEOUT", "900")
    assert admin._daemon_health_timeout() == 900.0

    # Invalid / non-positive overrides fall back to the scaled window.
    expected = (
        admin.DAEMON_HEALTH_TIMEOUT_SECONDS
        + 50 * admin.DAEMON_HEALTH_SECONDS_PER_SPEC
    )
    for bad in ("not-a-number", "0", "-5", "nan", "inf", "-inf"):
        monkeypatch.setenv("VQ_DAEMON_HEALTH_TIMEOUT", bad)
        assert admin._daemon_health_timeout() == pytest.approx(expected)

    # The scaled window stays bounded on a pathological state dir.
    monkeypatch.delenv("VQ_DAEMON_HEALTH_TIMEOUT", raising=False)
    monkeypatch.setattr(admin, "DAEMON_HEALTH_SECONDS_PER_SPEC", 60.0)
    assert (
        admin._daemon_health_timeout()
        == admin.DAEMON_HEALTH_TIMEOUT_MAX_SECONDS
    )


def test_restart_verification_survives_slow_state_scan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The verification poll must keep pinging past the flat 30 s base
    window when the scaled deadline allows it. Simulates the host_c2
    daemon: RPC silent for 45 s (startup scan of the spec dir), then
    healthy with the right provenance."""
    from vq import rpc

    clock = {"now": 0.0}
    monkeypatch.setattr(admin.time, "monotonic", lambda: clock["now"])
    monkeypatch.setattr(
        admin.time,
        "sleep",
        lambda s: clock.__setitem__("now", clock["now"] + s),
    )
    monkeypatch.setattr(admin, "_daemon_health_timeout", lambda: 156.0)

    def scanning_then_healthy():  # type: ignore[no-untyped-def]
        if clock["now"] < 45.0:
            return None
        return {"version": "0.15.58", "source_sha": EXPECTED_SHA}

    monkeypatch.setattr(rpc, "ping_user_daemon", scanning_then_healthy)

    provenance = admin._verify_restarted_daemon(EXPECTED_SHA)

    assert provenance.verified is True
    assert provenance.actual_sha == EXPECTED_SHA
    assert "verified" in provenance.detail


def test_no_response_failure_names_window_and_override_knob(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from vq import rpc

    clock = {"now": 0.0}
    monkeypatch.setattr(admin.time, "monotonic", lambda: clock["now"])
    monkeypatch.setattr(
        admin.time,
        "sleep",
        lambda s: clock.__setitem__("now", clock["now"] + s),
    )
    monkeypatch.setattr(admin, "_daemon_health_timeout", lambda: 156.0)
    monkeypatch.setattr(rpc, "ping_user_daemon", lambda: None)

    provenance = admin._verify_restarted_daemon(EXPECTED_SHA)

    assert provenance.verified is False
    assert provenance.actual_sha is None
    assert "within 156s" in provenance.detail
    assert "VQ_DAEMON_HEALTH_TIMEOUT" in provenance.detail


def test_launchd_self_update_accepts_verified_rpc_source(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prog = _program(tmp_path)
    result = _successful_update(prog)
    monkeypatch.setattr(admin, "_detect_vq_self_update", lambda unused: _launchd_probe())
    monkeypatch.setattr(admin, "_query_daemon_service_pid", lambda manager: 11)
    monkeypatch.setattr(admin, "current_source_sha", lambda path=None: EXPECTED_SHA)
    monkeypatch.setattr(
        admin,
        "_restart_vq_daemon",
        lambda **kwargs: (True, "launchctl kickstart succeeded"),
    )
    monkeypatch.setattr(
        admin,
        "_verify_restarted_daemon",
        lambda sha, *, expected_tree_sha256=None: admin.DaemonProvenance(
            verified=True,
            actual_sha=EXPECTED_SHA,
            actual_tree_sha256=None,
            detail=f"RPC healthy; source SHA {EXPECTED_SHA} verified",
        ),
    )

    admin._maybe_restart_daemon(prog, result, restart_daemon=True)

    assert result.daemon_restart_succeeded is True
    assert result.daemon_health_verified is True
    assert result.daemon_actual_source_sha == EXPECTED_SHA
    assert result.success is True


# --- verification by installed-tree digest, not by a declared SHA -----------

EXPECTED_TREE = "c" * 64
STALE_TREE = "d" * 64


def _ping(**fields: object):
    def _call() -> dict[str, object]:
        return {"version": "0.24.0", "multi_user": False, **fields}

    return _call


def test_a_matching_tree_digest_verifies_past_a_stale_provenance_marker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The host_d 2026-08-02 shape: new PID, correct code, stale SHA string.

    ``expected_source_sha`` is the checkout's ``git rev-parse HEAD``; the
    daemon reports whatever its installed package *declares* -- a SOURCE-SHA
    marker, or an enclosing work tree's HEAD for a non-editable install. Those
    are different objects, so a disagreement is not evidence of stale code. The
    digest compares the bytes themselves, and when it matches, the update is
    verified rather than handed off.
    """
    from vq import rpc

    monkeypatch.delenv("VQ_DAEMON_HEALTH_TIMEOUT", raising=False)
    monkeypatch.setattr(admin, "DAEMON_HEALTH_TIMEOUT_SECONDS", 0.01)
    monkeypatch.setattr(
        rpc,
        "ping_user_daemon",
        _ping(source_sha=STALE_SHA, source_tree_sha256=EXPECTED_TREE),
    )

    provenance = admin._verify_restarted_daemon(
        EXPECTED_SHA,
        expected_tree_sha256=EXPECTED_TREE,
    )

    assert provenance.verified is True
    assert provenance.actual_tree_sha256 == EXPECTED_TREE
    assert provenance.actual_sha == STALE_SHA
    # The operator is told which of the two halves is wrong, and how to fix it.
    assert "the declaration is wrong, not the code" in provenance.detail
    assert f"--write-marker {EXPECTED_SHA}" in provenance.detail


def test_a_mismatched_tree_digest_is_reported_as_stale_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The inverse: both halves disagree, so the code really is old.

    Pre-fix the operator saw two commit names and no way to tell this case
    apart from the one above -- which is what turned a diagnosis into a handoff.
    """
    from vq import rpc

    monkeypatch.delenv("VQ_DAEMON_HEALTH_TIMEOUT", raising=False)
    monkeypatch.setattr(admin, "DAEMON_HEALTH_TIMEOUT_SECONDS", 0.01)
    monkeypatch.setattr(
        rpc,
        "ping_user_daemon",
        _ping(source_sha=STALE_SHA, source_tree_sha256=STALE_TREE),
    )

    provenance = admin._verify_restarted_daemon(
        EXPECTED_SHA,
        expected_tree_sha256=EXPECTED_TREE,
    )

    assert provenance.verified is False
    assert provenance.actual_tree_sha256 == STALE_TREE
    assert "stale CODE, not a stale marker" in provenance.detail


def test_a_matching_sha_still_verifies_when_no_digest_is_available(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Soft-fail: a daemon predating the ping key must keep verifying.

    Every host in the fleet runs a daemon older than this change on the rollout
    that ships it. Requiring the digest would fail all of them at once.
    """
    from vq import rpc

    monkeypatch.delenv("VQ_DAEMON_HEALTH_TIMEOUT", raising=False)
    monkeypatch.setattr(admin, "DAEMON_HEALTH_TIMEOUT_SECONDS", 0.01)
    monkeypatch.setattr(rpc, "ping_user_daemon", _ping(source_sha=EXPECTED_SHA))

    provenance = admin._verify_restarted_daemon(
        EXPECTED_SHA,
        expected_tree_sha256=EXPECTED_TREE,
    )

    assert provenance.verified is True
    assert provenance.actual_tree_sha256 is None


def test_a_digestless_daemon_with_a_stale_sha_says_so_explicitly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from vq import rpc

    monkeypatch.delenv("VQ_DAEMON_HEALTH_TIMEOUT", raising=False)
    monkeypatch.setattr(admin, "DAEMON_HEALTH_TIMEOUT_SECONDS", 0.01)
    monkeypatch.setattr(rpc, "ping_user_daemon", _ping(source_sha=STALE_SHA))

    provenance = admin._verify_restarted_daemon(
        EXPECTED_SHA,
        expected_tree_sha256=EXPECTED_TREE,
    )

    assert provenance.verified is False
    assert "predates the source_tree_sha256 ping key" in provenance.detail


def test_no_digest_expectation_is_named_rather_than_silently_assumed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A broken venv mid-update yields no expectation; say so in the verdict."""
    from vq import rpc

    monkeypatch.delenv("VQ_DAEMON_HEALTH_TIMEOUT", raising=False)
    monkeypatch.setattr(admin, "DAEMON_HEALTH_TIMEOUT_SECONDS", 0.01)
    monkeypatch.setattr(
        rpc,
        "ping_user_daemon",
        _ping(source_sha=STALE_SHA, source_tree_sha256=EXPECTED_TREE),
    )

    provenance = admin._verify_restarted_daemon(EXPECTED_SHA)

    assert provenance.verified is False
    assert "no tree digest expectation available" in provenance.detail


def test_a_digest_never_rescues_a_ping_from_the_multi_user_daemon(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The routing-fault branch must win over the digest branch.

    The root daemon on a multi-user host is never the daemon this lane
    restarted, so nothing it reports -- digest included -- may be graded as
    this update's provenance.
    """
    from vq import rpc

    monkeypatch.delenv("VQ_DAEMON_HEALTH_TIMEOUT", raising=False)
    monkeypatch.setattr(admin, "DAEMON_HEALTH_TIMEOUT_SECONDS", 0.01)
    monkeypatch.setattr(
        rpc,
        "ping_user_daemon",
        lambda: {
            "version": "0.24.0",
            "multi_user": True,
            "source_sha": STALE_SHA,
            "source_tree_sha256": EXPECTED_TREE,
        },
    )

    provenance = admin._verify_restarted_daemon(
        EXPECTED_SHA,
        expected_tree_sha256=EXPECTED_TREE,
    )

    assert provenance.verified is False
    assert provenance.actual_tree_sha256 is None
    assert "multi-user daemon" in provenance.detail


def test_the_digest_expectation_is_asked_of_the_updated_interpreter(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Not of this process: on a multi-user host they are different installs."""
    prog = _program(tmp_path)
    result = _successful_update(prog)
    seen: list[list[str]] = []

    class _Proc:
        returncode = 0
        stdout = EXPECTED_TREE + "\n"
        stderr = ""

    def fake_run(argv, **kwargs):  # type: ignore[no-untyped-def]
        seen.append(list(argv))
        return _Proc()

    monkeypatch.setattr(admin.subprocess, "run", fake_run)
    monkeypatch.setattr(admin, "_detect_vq_self_update", lambda unused: _launchd_probe())
    monkeypatch.setattr(admin, "_query_daemon_service_pid", lambda manager: 11)
    monkeypatch.setattr(admin, "current_source_sha", lambda path=None: EXPECTED_SHA)
    monkeypatch.setattr(
        admin,
        "_restart_vq_daemon",
        lambda **kwargs: (True, "launchctl kickstart succeeded"),
    )
    captured: dict[str, object] = {}

    def fake_verify(sha, *, expected_tree_sha256=None):  # type: ignore[no-untyped-def]
        captured["tree"] = expected_tree_sha256
        return admin.DaemonProvenance(
            verified=True,
            actual_sha=sha,
            actual_tree_sha256=expected_tree_sha256,
            detail="verified",
        )

    monkeypatch.setattr(admin, "_verify_restarted_daemon", fake_verify)

    admin._maybe_restart_daemon(prog, result, restart_daemon=True)

    assert [prog.python, "-m", "vq", "source-tree-sha256"] in seen
    assert captured["tree"] == EXPECTED_TREE
    assert result.daemon_expected_source_tree_sha256 == EXPECTED_TREE
    assert result.daemon_actual_source_tree_sha256 == EXPECTED_TREE


def test_a_venv_that_cannot_run_vq_yields_no_expectation_not_a_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def boom(argv, **kwargs):  # type: ignore[no-untyped-def]
        raise OSError("no such interpreter")

    monkeypatch.setattr(admin.subprocess, "run", boom)

    assert admin._installed_tree_digest("/nope/python") is None


def test_a_non_digest_stdout_is_rejected_rather_than_trusted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Proc:
        returncode = 0
        stdout = "Traceback (most recent call last):\n"
        stderr = ""

    monkeypatch.setattr(admin.subprocess, "run", lambda argv, **kw: _Proc())

    assert admin._installed_tree_digest("/some/python") is None


def test_a_matching_sha_verifies_even_when_the_tree_digests_disagree(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The asymmetry is deliberate, so it is pinned rather than assumed.

    The digest may rescue a verification the SHA would have failed; it may not
    fail one the SHA passes. Making it authoritative would tighten
    *availability*: on a multi-user host the admin CLI and the daemon resolve
    different installs by design, and every such host would start failing
    updates that are correct today.
    """
    from vq import rpc

    monkeypatch.delenv("VQ_DAEMON_HEALTH_TIMEOUT", raising=False)
    monkeypatch.setattr(admin, "DAEMON_HEALTH_TIMEOUT_SECONDS", 0.01)
    monkeypatch.setattr(
        rpc,
        "ping_user_daemon",
        _ping(source_sha=EXPECTED_SHA, source_tree_sha256=STALE_TREE),
    )

    provenance = admin._verify_restarted_daemon(
        EXPECTED_SHA,
        expected_tree_sha256=EXPECTED_TREE,
    )

    assert provenance.verified is True
    assert provenance.actual_tree_sha256 == STALE_TREE


def test_the_digest_probe_does_not_import_a_vq_from_the_current_directory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`python -m vq` prepends CWD to sys.path.

    An admin update runs from wherever the operator happened to be, and the
    checkout's own `src/` contains a `vq/` package -- importing that would
    digest the wrong tree and produce an expectation the daemon can never
    match.
    """
    seen: dict[str, object] = {}

    def record(argv, **kwargs):  # type: ignore[no-untyped-def]
        seen.update(kwargs)
        return subprocess.CompletedProcess(argv, 0, stdout="c" * 64, stderr="")

    monkeypatch.setattr(admin.subprocess, "run", record)

    assert admin._installed_tree_digest("/some/python") == "c" * 64
    assert seen["cwd"] == "/"


def test_the_pass_by_digest_message_does_not_prescribe_a_marker_blindly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An editable install has no marker; writing one there is wrong advice."""
    from vq import rpc

    monkeypatch.delenv("VQ_DAEMON_HEALTH_TIMEOUT", raising=False)
    monkeypatch.setattr(admin, "DAEMON_HEALTH_TIMEOUT_SECONDS", 0.01)
    monkeypatch.setattr(
        rpc,
        "ping_user_daemon",
        _ping(source_sha=STALE_SHA, source_tree_sha256=EXPECTED_TREE),
    )

    provenance = admin._verify_restarted_daemon(
        EXPECTED_SHA,
        expected_tree_sha256=EXPECTED_TREE,
    )

    assert "On a non-editable install" in provenance.detail
    assert "do NOT write a marker there" in provenance.detail


@pytest.mark.parametrize('form', ['console', 'module'])
def test_launchd_identity_accepts_both_daemon_entry_points(tmp_path, monkeypatch, form):
    import os
    import plistlib

    prog = _program(tmp_path)
    venv = Path(prog.python).parent.parent
    argv = ([str(venv / 'bin/vq'), 'daemon', 'run'] if form == 'console'
            else [prog.python, '-m', 'vq', 'daemon', 'run'])
    plist = Path.home() / 'Library/LaunchAgents' / f'{admin.LAUNCHD_DAEMON_LABEL}.plist'
    plist.parent.mkdir(parents=True, exist_ok=True)
    plist.write_bytes(plistlib.dumps({'ProgramArguments': argv}))
    monkeypatch.setattr(admin, '_select_daemon_service_manager',
                        lambda: admin._DaemonServiceManager.LAUNCHD)
    monkeypatch.setattr(admin, '_query_launchd_daemon',
                        lambda: (0, f'program = {argv[0]}\npid = 4321\n'))
    probe = admin._detect_vq_self_update(prog)
    assert probe.is_self_update and probe.daemon_running
    lifecycle = admin._ManagedDaemonUpdate(
        manager=admin._DaemonServiceManager.LAUNCHD, env="vibeqc-queue", pre_pid=4321,
        was_running=True, was_stopped=True, pre_source_sha=EXPECTED_SHA,
        pre_source_tree_sha256='a' * 64, pre_checkout_branch='main',
        venv_path=venv, venv_backup=None, service_executable=argv[0],
        owner_uid=os.geteuid(), service_command=tuple(argv),
    )
    assert admin._lifecycle_service_executable_matches(argv[0], lifecycle)
    assert admin._reattest_service_before_start(lifecycle)[0]


@pytest.mark.parametrize('damage', [
    'web-command', 'other-venv', 'loaded-plist-mismatch', 'symlinked-plist',
    'missing-command',
])
def test_launchd_console_identity_still_fails_closed(tmp_path, monkeypatch, damage):
    import plistlib

    prog = _program(tmp_path)
    executable = str(Path(prog.python).parent / 'vq')
    argv = [executable, 'daemon', 'run']
    plist = Path.home() / 'Library/LaunchAgents' / f'{admin.LAUNCHD_DAEMON_LABEL}.plist'
    plist.parent.mkdir(parents=True, exist_ok=True)
    if damage == 'web-command':
        argv[1] = 'web'
    elif damage == 'other-venv':
        executable = str(tmp_path / 'other/bin/vq')
        argv[0] = executable
    elif damage == 'loaded-plist-mismatch':
        argv = [prog.python, '-m', 'vq', 'daemon', 'run']
    elif damage == 'missing-command':
        argv = [executable]
    plist.write_bytes(plistlib.dumps({'ProgramArguments': argv}))
    if damage == 'symlinked-plist':
        other = plist.with_suffix('.backup')
        plist.rename(other)
        plist.symlink_to(other)
    assert not admin._service_executable_matches(
        executable, prog, manager=admin._DaemonServiceManager.LAUNCHD,
    )
