"""Failure-atomic admin self-update and shared lifecycle-lock coverage."""

from __future__ import annotations

import os
import subprocess
import sys
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace

import pytest

from vq import admin, config, fleet_rollout

pytestmark = [
    pytest.mark.no_autopatch_branch_check,
    pytest.mark.no_autopatch_lifecycle_lock,
]

SHA = "a" * 40
TREE = "12" * 32


def _systemd_execstart(
    executable: str,
    *,
    start_time: str = "[n/a]",
    stop_time: str = "[n/a]",
    pid: int = 0,
    code: str = "(null)",
    status: str = "0/0",
) -> str:
    return (
        f"{{ path={executable} ; argv[]={executable} daemon run ; "
        f"ignore_errors=no ; start_time={start_time} ; "
        f"stop_time={stop_time} ; pid={pid} ; code={code} ; "
        f"status={status} }}"
    )


def _git(cwd: Path, *args: str) -> str:
    env = {
        **os.environ,
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_CONFIG_SYSTEM": "/dev/null",
        "GIT_AUTHOR_NAME": "vq-test",
        "GIT_AUTHOR_EMAIL": "vq-test@example.invalid",
        "GIT_COMMITTER_NAME": "vq-test",
        "GIT_COMMITTER_EMAIL": "vq-test@example.invalid",
    }
    proc = subprocess.run(
        ["git", "-C", str(cwd), *args],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    return proc.stdout.strip()


def _real_program(tmp_path: Path) -> config.VenvProgram:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "--initial-branch", "main")
    (repo / "README.md").write_text("test\n", encoding="utf-8")
    _git(repo, "add", ".")
    _git(repo, "commit", "-m", "initial")

    venv = tmp_path / "managed-venv"
    (venv / "bin").mkdir(parents=True)
    python = venv / "bin" / "python"
    python.write_text("test interpreter placeholder\n", encoding="utf-8")
    return config.VenvProgram(
        kind="venv",
        python=str(python),
        git_dir=str(repo),
        branch="main",
        update_script="vibe-queue/scripts/update.sh",
    )


def _probe(*, running: bool) -> admin._SelfUpdateProbe:
    return admin._SelfUpdateProbe(
        is_self_update=True,
        daemon_running=running,
        service_manager="systemd",
        manager_available=True,
        diagnostic="verified systemd test service",
    )


def _service_state(
    prog: config.VenvProgram,
    *,
    running: bool,
    pid: int | None,
    executable: str | None = None,
    command_identity: tuple[str, ...] | None = None,
) -> admin._DaemonServiceState:
    service_executable = executable or str(Path(prog.python).parent / "vq")
    return admin._DaemonServiceState(
        manager=admin._DaemonServiceManager.SYSTEMD,
        running=running,
        pid=pid,
        executable=service_executable,
        diagnostic="test service state",
        command_identity=command_identity
        or ("systemd-execstart", _systemd_execstart(service_executable)),
    )


def test_query_daemon_service_state_keeps_one_complete_execstart(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw = (
        "{ path=/opt/vq/bin/vq ; argv[]=/opt/vq/bin/vq daemon run ; "
        "ignore_errors=no ; start_time=[n/a] ; stop_time=[n/a] ; "
        "pid=0 ; code=(null) ; status=0/0 }"
    )
    monkeypatch.setattr(
        admin.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args[0],
            0,
            stdout=f"MainPID=0\nExecStart={raw}\nActiveState=inactive\n",
            stderr="",
        ),
    )

    state = admin._query_daemon_service_state(
        admin._DaemonServiceManager.SYSTEMD,
    )

    assert state.command_identity == ("systemd-execstart", raw)
    assert state.executable == "/opt/vq/bin/vq"
    assert state.running is False


@pytest.mark.parametrize(
    "stdout",
    [
        pytest.param(
            "MainPID=42\n"
            "ExecStart={ path=/bin/false ; argv[]=/bin/false ; "
            "ignore_errors=no ; start_time=[n/a] ; stop_time=[n/a] ; "
            "pid=0 ; code=(null) ; status=0/0 }\n"
            "ExecStart={ path=/opt/vq/bin/vq ; "
            "argv[]=/opt/vq/bin/vq daemon run ; ignore_errors=no ; "
            "start_time=[n/a] ; stop_time=[n/a] ; pid=0 ; "
            "code=(null) ; status=0/0 }\n"
            "ActiveState=active\n",
            id="duplicate-execstart",
        ),
        pytest.param(
            "MainPID=42\n"
            "ExecStart={ path=/opt/vq/bin/vq ; argv[]=/opt/vq/bin/vq\n"
            "daemon run ; ignore_errors=no ; }\n"
            "ActiveState=active\n",
            id="multiline-execstart",
        ),
        pytest.param(
            "MainPID=42\n"
            "ExecStart={ path=/bin/false ; argv[]=/bin/false ; "
            "ignore_errors=no ; start_time=[n/a] ; stop_time=[n/a] ; "
            "pid=0 ; code=(null) ; status=0/0 } "
            "{ path=/opt/vq/bin/vq ; argv[]=/opt/vq/bin/vq daemon run ; "
            "ignore_errors=no ; start_time=[n/a] ; stop_time=[n/a] ; "
            "pid=0 ; code=(null) ; status=0/0 }\n"
            "ActiveState=active\n",
            id="multiple-structs-one-line",
        ),
    ],
)
def test_query_daemon_service_state_rejects_ambiguous_execstart(
    monkeypatch: pytest.MonkeyPatch,
    stdout: str,
) -> None:
    monkeypatch.setattr(
        admin.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args[0],
            0,
            stdout=stdout,
            stderr="",
        ),
    )

    state = admin._query_daemon_service_state(
        admin._DaemonServiceManager.SYSTEMD,
    )

    assert state.command_identity is None
    assert state.executable is None
    assert state.running is None
    assert "ambiguous" in state.diagnostic


def _fake_program(tmp_path: Path) -> config.VenvProgram:
    repo = tmp_path / "repo"
    (repo / ".git").mkdir(parents=True)
    venv = tmp_path / "venv"
    (venv / "bin").mkdir(parents=True)
    (venv / "bin" / "python").write_text("placeholder\n", encoding="utf-8")
    (venv / "pyvenv.cfg").write_text("home = /usr/bin\n", encoding="utf-8")
    return config.VenvProgram(
        kind="venv",
        python=str(venv / "bin" / "python"),
        git_dir=str(repo),
        branch="main",
        update_script="vibe-queue/scripts/update.sh",
    )


def test_admin_mutator_refuses_canonical_multi_user_migration_receipt(
    tmp_path: Path,
) -> None:
    prog = _real_program(tmp_path)
    receipt = Path(prog.git_dir) / ".git" / "vq-multi-user-bootstrap.json"
    receipt.write_text("{}\n", encoding="utf-8")
    entered = False

    with (
        pytest.raises(admin.AdminUpdateInProgress, match="multi-user migration"),
        admin.toolset_lifecycle_lock([prog], action="test-migration-fence"),
    ):
        entered = True

    assert entered is False
    receipt.unlink()
    with admin.toolset_lifecycle_lock([prog], action="test-migration-released"):
        entered = True
    assert entered is True


def _stub_begin_identity(
    monkeypatch: pytest.MonkeyPatch,
    prog: config.VenvProgram,
    *,
    state: admin._DaemonServiceState,
    derived_tree: str = TREE,
) -> list[list[str]]:
    commands: list[list[str]] = []
    monkeypatch.setattr(admin, "_query_daemon_service_state", lambda manager: state)
    monkeypatch.setattr(admin, "_query_daemon_service_pid", lambda manager: state.pid)
    monkeypatch.setattr(admin, "_capture_checkout_state", lambda path: (SHA, "main"))
    monkeypatch.setattr(admin, "_run_git_status_porcelain", lambda path: (0, ""))
    monkeypatch.setattr(admin, "_guard_git_index_unlocked", lambda path: None)
    monkeypatch.setattr(admin, "_installed_tree_digest", lambda python: TREE)
    monkeypatch.setattr(admin, "_vq_project_root_for_program", lambda unused: Path(prog.git_dir))
    monkeypatch.setattr(
        admin,
        "_canonical_lifecycle_checkout",
        lambda unused: Path(prog.git_dir),
    )
    monkeypatch.setattr(
        admin,
        "source_tree_sha256_at_git_commit",
        lambda project, sha: derived_tree,
    )

    def command(argv: list[str], *, display: str) -> tuple[bool, str]:
        commands.append(argv)
        return True, f"{display} ... done"

    monkeypatch.setattr(admin, "_run_daemon_service_command", command)
    monkeypatch.setattr(admin, "_persist_managed_update_receipt", lambda *args: None)
    monkeypatch.setattr(admin, "_clear_managed_update_receipt", lambda: None)
    return commands


def _invoke_lock_helper(
    *,
    helper: Path,
    checkout: Path,
    target: Path,
    env: dict[str, str],
    pass_fds: tuple[int, ...] = (),
) -> subprocess.CompletedProcess[str]:
    body = r'''
set -euo pipefail
. "$1"
vibe_toolset_acquire_lifecycle_lock "$2" "$3" "$4" test-child
printf '%s\n%s\n' \
  "$VIBE_TOOLSET_LIFECYCLE_LOCK_CHECKOUT" \
  "$VIBE_TOOLSET_LIFECYCLE_LOCK_TARGET"
vibe_toolset_release_lifecycle_lock
'''
    return subprocess.run(
        [
            "bash",
            "-c",
            body,
            "vq-lock-test",
            str(helper),
            sys.executable,
            str(checkout),
            str(target),
        ],
        capture_output=True,
        text=True,
        check=False,
        env=env,
        pass_fds=pass_fds,
    )


def test_python_lock_contends_with_direct_shell_and_hands_fds_to_child(
    tmp_path: Path,
) -> None:
    prog = _real_program(tmp_path)
    checkout = Path(prog.git_dir).resolve()
    target = Path(prog.python).parent.parent.resolve()
    helper = Path(__file__).resolve().parents[1] / "scripts" / "_lifecycle_lock.sh"
    clean_env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("VIBE_TOOLSET_")
    }

    with admin.toolset_lifecycle_lock([prog], action="test-admin"):
        contender = _invoke_lock_helper(
            helper=helper,
            checkout=checkout,
            target=target,
            env=clean_env,
        )
        assert contender.returncode != 0
        assert "already active on this checkout" in contender.stderr

        handoff_env, pass_fds = admin._current_toolset_lock_handoff(
            checkout,
            target,
        )
        inherited = _invoke_lock_helper(
            helper=helper,
            checkout=checkout,
            target=target,
            env={**clean_env, **handoff_env},
            pass_fds=pass_fds,
        )

    assert inherited.returncode == 0, inherited.stdout + inherited.stderr
    assert inherited.stdout.splitlines() == [str(checkout), str(target)]


def test_source_only_fence_blocks_direct_shell_checkout_mutation(
    tmp_path: Path,
) -> None:
    prog = _real_program(tmp_path)
    checkout = Path(prog.git_dir).resolve()
    target = Path(prog.python).parent.parent.resolve()
    helper = Path(__file__).resolve().parents[1] / "scripts" / "_lifecycle_lock.sh"
    clean_env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("VIBE_TOOLSET_")
    }

    with admin.toolset_lifecycle_lock(
        [],
        action="scheduler source stage",
        extra_resources=(("checkout", str(checkout)),),
    ):
        contender = _invoke_lock_helper(
            helper=helper,
            checkout=checkout,
            target=target,
            env=clean_env,
        )

    assert contender.returncode != 0
    assert "already active on this checkout" in contender.stderr


@pytest.mark.skipif(
    sys.platform != "darwin",
    reason="case aliases exercise the default case-insensitive macOS volume",
)
def test_case_aliased_existing_resources_share_python_and_shell_lock(
    tmp_path: Path,
) -> None:
    prog = _real_program(tmp_path)
    checkout = Path(prog.git_dir)
    target = Path(prog.python).parent.parent
    checkout_alias = checkout.parent / checkout.name.upper()
    target_alias = target.parent / target.name.upper()
    assert os.path.samefile(checkout_alias, checkout)
    assert os.path.samefile(target_alias, target)
    aliased = prog.model_copy(
        update={
            "git_dir": str(checkout_alias),
            "python": str(target_alias / "bin" / "python"),
        }
    )
    helper = Path(__file__).resolve().parents[1] / "scripts" / "_lifecycle_lock.sh"
    clean_env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("VIBE_TOOLSET_")
    }

    with admin.toolset_lifecycle_lock([aliased], action="case-alias"):
        contender = _invoke_lock_helper(
            helper=helper,
            checkout=checkout,
            target=target,
            env=clean_env,
        )

    assert contender.returncode != 0
    assert "already active on this checkout" in contender.stderr


@pytest.mark.skipif(
    sys.platform != "darwin",
    reason="future-name aliases exercise case-insensitive macOS semantics",
)
def test_case_aliased_future_targets_have_one_lock_identity(tmp_path: Path) -> None:
    prog = _real_program(tmp_path)
    checkout = Path(prog.git_dir)
    helper = Path(__file__).resolve().parents[1] / "scripts" / "_lifecycle_lock.sh"
    lower = tmp_path / "future-venv"
    upper = tmp_path / "FUTURE-VENV"
    clean_env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("VIBE_TOOLSET_")
    }

    assert admin._canonical_lifecycle_target(lower) == (
        admin._canonical_lifecycle_target(upper)
    )
    first = _invoke_lock_helper(
        helper=helper,
        checkout=checkout,
        target=lower,
        env=clean_env,
    )
    second = _invoke_lock_helper(
        helper=helper,
        checkout=checkout,
        target=upper,
        env=clean_env,
    )

    assert first.returncode == 0, first.stdout + first.stderr
    assert second.returncode == 0, second.stdout + second.stderr
    assert first.stdout.splitlines() == second.stdout.splitlines()


def test_descendant_last_close_retains_lock_after_parent_context_exits(
    tmp_path: Path,
) -> None:
    prog = _real_program(tmp_path)
    checkout = Path(prog.git_dir).resolve()
    target = Path(prog.python).parent.parent.resolve()
    helper = Path(__file__).resolve().parents[1] / "scripts" / "_lifecycle_lock.sh"
    clean_env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("VIBE_TOOLSET_")
    }
    release_read, release_write = os.pipe()
    ready_read, ready_write = os.pipe()
    child_pid = -1
    try:
        with admin.toolset_lifecycle_lock([prog], action="test-parent-close"):
            _payload, lifecycle_fds = admin._active_toolset_lifecycle_handoff()
            child_pid = os.fork()
            if child_pid == 0:  # pragma: no cover - assertions stay in parent
                try:
                    os.close(release_write)
                    os.close(ready_read)
                    os.write(ready_write, b"1")
                    if os.read(release_read, 1) != b"1":
                        os._exit(91)
                    for fd in lifecycle_fds:
                        os.close(fd)
                    os._exit(0)
                except BaseException:
                    os._exit(92)
            os.close(release_read)
            release_read = -1
            os.close(ready_write)
            ready_write = -1
            assert os.read(ready_read, 1) == b"1"

        still_held = _invoke_lock_helper(
            helper=helper,
            checkout=checkout,
            target=target,
            env=clean_env,
        )
        assert still_held.returncode != 0
        assert "already active on this checkout" in still_held.stderr

        os.write(release_write, b"1")
        os.close(release_write)
        release_write = -1
        waited, status = os.waitpid(child_pid, 0)
        assert waited == child_pid
        assert os.waitstatus_to_exitcode(status) == 0
        child_pid = -1

        released = _invoke_lock_helper(
            helper=helper,
            checkout=checkout,
            target=target,
            env=clean_env,
        )
        assert released.returncode == 0, released.stdout + released.stderr
    finally:
        for fd in (release_read, release_write, ready_read, ready_write):
            if fd >= 0:
                os.close(fd)
        if child_pid > 0:
            os.kill(child_pid, 9)
            os.waitpid(child_pid, 0)


def test_reentry_subprocess_adopts_both_fences_without_parent_close_gap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prog = _real_program(tmp_path)
    checkout = Path(prog.git_dir).resolve()
    target = Path(prog.python).parent.parent.resolve()
    state_root = tmp_path / "state"
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: state_root)
    rollout_id = "v0.24.0-reentry-no-gap"
    capability = fleet_rollout.RolloutReentryCapability(
        rollout_id=rollout_id,
        operation_id="a" * 64,
        request_sha256="b" * 64,
        report_digest_sha256="c" * 64,
    )
    helper = Path(__file__).resolve().parents[1] / "scripts" / "_lifecycle_lock.sh"
    clean_env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("VIBE_TOOLSET_")
    }
    ready_read, ready_write = os.pipe()
    release_read, release_write = os.pipe()
    stack = ExitStack()
    child: subprocess.Popen[str] | None = None
    try:
        stack.enter_context(fleet_rollout.rollout_execution_lock(rollout_id))
        stack.enter_context(
            admin.toolset_lifecycle_lock([prog], action="test-reentry-parent")
        )
        lifecycle_handoff, _ = admin._active_toolset_lifecycle_handoff()
        resources = admin._active_toolset_lifecycle_resources()
        reentry_handoff, reentry_fds = (
            fleet_rollout._active_rollout_reentry_handoff(
                capability,
                lifecycle_handoff=lifecycle_handoff,
            )
        )
        script = r'''
import os
import sys
from pathlib import Path
from vq import admin, config, fleet_rollout

rollout_id, checkout, target = sys.argv[1:4]
ready_fd, release_fd = map(int, sys.argv[4:6])
resources = (("checkout", checkout), ("target", target))
prog = config.VenvProgram(
    kind="venv",
    python=str(Path(target) / "bin" / "python"),
    git_dir=checkout,
    branch="main",
    update_script="vibe-queue/scripts/update.sh",
)
with fleet_rollout.adopt_rollout_reentry_handoff(
    expected_rollout_id=rollout_id,
    expected_lifecycle_resources=resources,
) as adopted:
    assert adopted is not None
    with fleet_rollout.rollout_execution_lock(rollout_id):
        with admin.toolset_lifecycle_lock([prog], action="test-reentry-child"):
            os.write(ready_fd, b"1")
            assert os.read(release_fd, 1) == b"1"
'''
        child_env = {
            **clean_env,
            "VQ_STATE_DIR": str(state_root),
            fleet_rollout.ENV_ROLLOUT_REENTRY_HANDOFF: reentry_handoff,
        }
        child = subprocess.Popen(
            [
                sys.executable,
                "-I",
                "-c",
                script,
                rollout_id,
                str(checkout),
                str(target),
                str(ready_write),
                str(release_read),
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            close_fds=True,
            pass_fds=(*reentry_fds, ready_write, release_read),
            env=child_env,
        )
        os.close(ready_write)
        ready_write = -1
        os.close(release_read)
        release_read = -1
        assert os.read(ready_read, 1) == b"1"

        # Closing every parent context after the successor has adopted must
        # leave the same open-file descriptions locked in that successor.
        stack.close()
        with pytest.raises(
            fleet_rollout.FleetRolloutError,
            match="another fleet rollout is already executing",
        ), fleet_rollout.rollout_execution_lock("contender"):
            pytest.fail("contender acquired the inherited rollout lock")
        lifecycle_contender = _invoke_lock_helper(
            helper=helper,
            checkout=checkout,
            target=target,
            env=clean_env,
        )
        assert lifecycle_contender.returncode != 0
        assert "already active on this checkout" in lifecycle_contender.stderr

        os.write(release_write, b"1")
        os.close(release_write)
        release_write = -1
        stdout, stderr = child.communicate(timeout=10)
        assert child.returncode == 0, stdout + stderr
        child = None

        with fleet_rollout.rollout_execution_lock("after-reentry"):
            pass
        lifecycle_released = _invoke_lock_helper(
            helper=helper,
            checkout=checkout,
            target=target,
            env=clean_env,
        )
        assert lifecycle_released.returncode == 0, (
            lifecycle_released.stdout + lifecycle_released.stderr
        )
        assert resources == (
            ("checkout", str(checkout)),
            ("target", str(target)),
        )
    finally:
        stack.close()
        for fd in (ready_read, ready_write, release_read, release_write):
            if fd >= 0:
                os.close(fd)
        if child is not None:
            child.kill()
            child.communicate(timeout=5)


def test_begin_reattests_and_stops_a_loaded_inactive_service(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prog = _fake_program(tmp_path)
    state = _service_state(prog, running=False, pid=None)
    commands = _stub_begin_identity(monkeypatch, prog, state=state)

    lifecycle = admin._begin_managed_daemon_update(prog, _probe(running=False))

    assert lifecycle.was_running is False
    assert lifecycle.was_stopped is True
    assert commands == [["systemctl", "--user", "stop", "vq-daemon"]]


def test_begin_accepts_runtime_fields_changed_by_stop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Post-stop identity checks ignore systemd's last-run status fields."""
    prog = _fake_program(tmp_path)
    executable = str(Path(prog.python).parent / "vq")
    before = _service_state(
        prog,
        running=True,
        pid=42,
        command_identity=(
            "systemd-execstart",
            _systemd_execstart(
                executable,
                start_time="[Mon 2026-08-24 10:00:00 UTC]",
                pid=42,
            ),
        ),
    )
    after = _service_state(
        prog,
        running=False,
        pid=None,
        command_identity=(
            "systemd-execstart",
            _systemd_execstart(
                executable,
                start_time="[Mon 2026-08-24 10:00:00 UTC]",
                stop_time="[Mon 2026-08-24 10:01:00 UTC]",
                pid=42,
                code="exited",
                status="0",
            ),
        ),
    )
    commands = _stub_begin_identity(monkeypatch, prog, state=before)
    states = iter((before, after, after))
    monkeypatch.setattr(
        admin,
        "_query_daemon_service_state",
        lambda unused: next(states, after),
    )

    lifecycle = admin._begin_managed_daemon_update(prog, _probe(running=True))

    assert lifecycle.was_stopped is True
    assert commands == [["systemctl", "--user", "stop", "vq-daemon"]]


def test_begin_rejects_service_executable_drift_before_stop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prog = _fake_program(tmp_path)
    state = _service_state(
        prog,
        running=True,
        pid=42,
        executable=str(tmp_path / "other-venv" / "bin" / "vq"),
    )
    commands = _stub_begin_identity(monkeypatch, prog, state=state)

    with pytest.raises(admin.AdminError, match="(identity|executable|serving)"):
        admin._begin_managed_daemon_update(prog, _probe(running=True))

    assert commands == []


def test_begin_rejects_non_daemon_systemd_command_before_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prog = _fake_program(tmp_path)
    executable = str(Path(prog.python).parent / "vq")
    state = _service_state(
        prog,
        running=True,
        pid=42,
        command_identity=(
            "systemd-execstart",
            _systemd_execstart(executable).replace(
                " daemon run ;",
                " admin daemon run ;",
            ),
        ),
    )
    commands = _stub_begin_identity(monkeypatch, prog, state=state)

    def unexpected_receipt(*args: object) -> None:
        raise AssertionError("non-daemon command must be rejected before receipt")

    monkeypatch.setattr(
        admin,
        "_persist_managed_update_receipt",
        unexpected_receipt,
    )

    with pytest.raises(admin.AdminError, match="systemd command is not canonical"):
        admin._begin_managed_daemon_update(prog, _probe(running=True))

    assert commands == []


def test_begin_rejects_incoherent_preupdate_tree_before_stop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prog = _fake_program(tmp_path)
    state = _service_state(prog, running=True, pid=42)
    commands = _stub_begin_identity(
        monkeypatch,
        prog,
        state=state,
        derived_tree="34" * 32,
    )

    with pytest.raises(admin.AdminError, match="(tree|digest|identity)"):
        admin._begin_managed_daemon_update(prog, _probe(running=True))

    assert commands == []


def test_begin_reports_direct_replacement_receipt_before_git_cleanliness(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prog = _fake_program(tmp_path)
    state = _service_state(prog, running=True, pid=42)
    commands = _stub_begin_identity(monkeypatch, prog, state=state)
    venv = Path(prog.python).parent.parent
    receipt = venv.parent / f".{venv.name}.vq-venv-replacement.json"
    receipt.write_text("durable recovery fence\n", encoding="utf-8")

    def unexpected_status(path: Path) -> tuple[int, str]:
        raise AssertionError("Git status must not mask the direct receipt recovery fence")

    monkeypatch.setattr(admin, "_run_git_status_porcelain", unexpected_status)

    with pytest.raises(admin.AdminError, match="direct lifecycle replacement needs recovery"):
        admin._begin_managed_daemon_update(prog, _probe(running=True))

    assert commands == []


@pytest.mark.parametrize(
    "lock_contents",
    [
        pytest.param(b"", id="zero-byte-stale-shape"),
        pytest.param(b"writer may still own this\n", id="nonempty-live-shape"),
    ],
)
def test_begin_rejects_existing_git_index_lock_before_daemon_stop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    lock_contents: bytes,
) -> None:
    """Lock contents never prove whether a Git writer is live or stale."""
    real_guard = admin._guard_git_index_unlocked
    prog = _real_program(tmp_path)
    venv = Path(prog.python).parent.parent
    (venv / "pyvenv.cfg").write_text("home = /usr/bin\n", encoding="utf-8")
    state = _service_state(prog, running=True, pid=42)
    stopped = _service_state(prog, running=False, pid=None)
    commands = _stub_begin_identity(monkeypatch, prog, state=state)
    monkeypatch.setattr(admin, "_guard_git_index_unlocked", real_guard)
    states = iter((state, stopped))
    monkeypatch.setattr(
        admin,
        "_query_daemon_service_state",
        lambda unused: next(states, stopped),
    )
    monkeypatch.setattr(
        admin,
        "_wait_for_managed_daemon_quiescence",
        lambda *args, **kwargs: (True, "test daemon stopped"),
    )
    lock = Path(
        _git(
            Path(prog.git_dir),
            "rev-parse",
            "--path-format=absolute",
            "--git-path",
            "index.lock",
        )
    )
    lock.write_bytes(lock_contents)
    before_sha = _git(Path(prog.git_dir), "rev-parse", "HEAD")
    before_branch = _git(
        Path(prog.git_dir), "symbolic-ref", "--short", "HEAD",
    )
    receipts: list[object] = []
    starts: list[object] = []
    monkeypatch.setattr(
        admin,
        "_persist_managed_update_receipt",
        lambda *args: receipts.append(args),
    )
    monkeypatch.setattr(
        admin,
        "_start_managed_daemon_update",
        lambda lifecycle: starts.append(lifecycle),
    )

    with pytest.raises(admin.AdminError, match=r"index\.lock") as excinfo:
        admin._begin_managed_daemon_update(prog, _probe(running=True))

    assert str(lock) in str(excinfo.value)
    assert commands == []
    assert receipts == []
    assert starts == []
    assert lock.exists()
    assert lock.read_bytes() == lock_contents
    assert venv.is_dir()
    assert list(tmp_path.glob(".managed-venv.vq-admin-backup-*")) == []
    assert _git(Path(prog.git_dir), "rev-parse", "HEAD") == before_sha
    assert (
        _git(Path(prog.git_dir), "symbolic-ref", "--short", "HEAD")
        == before_branch
    )


def test_git_index_lock_path_follows_linked_worktree_and_custom_index(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prog = _real_program(tmp_path)
    repo = Path(prog.git_dir)
    linked = tmp_path / "linked"
    _git(repo, "worktree", "add", "--detach", str(linked))

    index = Path(
        _git(
            linked,
            "rev-parse",
            "--path-format=absolute",
            "--git-path",
            "index",
        )
    )
    assert admin._git_index_lock_path(linked) == Path(str(index) + ".lock")
    admin._guard_git_index_unlocked(linked)

    custom_index = tmp_path / "custom-index"
    monkeypatch.setenv("GIT_INDEX_FILE", str(custom_index))
    assert admin._git_index_lock_path(linked) == Path(
        str(custom_index) + ".lock"
    )


def test_git_index_lock_resolution_failure_is_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        admin.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args[0], 128, stdout="", stderr="fatal: cannot resolve git dir\n",
        ),
    )

    with pytest.raises(
        admin.AdminError,
        match="could not resolve.*index lock.*daemon was not stopped",
    ):
        admin._guard_git_index_unlocked(tmp_path / "checkout")


def test_git_cleanliness_probe_disables_optional_locks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen_env: dict[str, str] = {}

    def run(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        seen_env.update(kwargs["env"])  # type: ignore[arg-type]
        return subprocess.CompletedProcess(args[0], 0, stdout="", stderr="")

    monkeypatch.setattr(admin.subprocess, "run", run)

    assert admin._run_git_status_porcelain(tmp_path) == (0, "")
    assert seen_env["GIT_OPTIONAL_LOCKS"] == "0"


@pytest.mark.parametrize(
    "selector",
    ["GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_COMMON_DIR"],
)
def test_begin_rejects_ambient_git_selector_before_daemon_stop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    selector: str,
) -> None:
    prog = _fake_program(tmp_path)
    state = _service_state(prog, running=True, pid=42)
    commands = _stub_begin_identity(monkeypatch, prog, state=state)
    real_environment_guard = admin._guard_managed_git_environment
    monkeypatch.setattr(
        admin,
        "_guard_managed_git_environment",
        real_environment_guard,
    )
    monkeypatch.setenv(selector, str(tmp_path / "retargeted"))

    with pytest.raises(admin.AdminError, match=selector):
        admin._begin_managed_daemon_update(prog, _probe(running=True))

    assert commands == []


def test_begin_requires_post_stop_quiescence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prog = _fake_program(tmp_path)
    state = _service_state(prog, running=True, pid=42)
    commands = _stub_begin_identity(monkeypatch, prog, state=state)
    monkeypatch.setattr(admin, "DAEMON_RESTART_TIMEOUT_SECONDS", 0)

    with pytest.raises(admin.AdminError, match="(quiescent|still running|stop)"):
        admin._begin_managed_daemon_update(prog, _probe(running=True))

    assert commands[0] == ["systemctl", "--user", "stop", "vq-daemon"]


def test_complete_uses_work_verdict_before_arming_restart_fields(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prog = _fake_program(tmp_path)
    lifecycle = admin._ManagedDaemonUpdate(
        manager=admin._DaemonServiceManager.SYSTEMD,
        env="vibeqc-queue",
        pre_pid=41,
        was_running=True,
        was_stopped=True,
        pre_source_sha="b" * 40,
        pre_source_tree_sha256="34" * 32,
        pre_checkout_branch="main",
        venv_path=Path(prog.python).parent.parent,
        venv_backup=None,
        service_executable=str(Path(prog.python).parent / "vq"),
        service_command=("systemd-execstart", str(Path(prog.python).parent / "vq")),
        terminal_verified=False,
    )
    result = admin.UpdateResult(
        env="vibeqc-queue",
        git_dir=prog.git_dir,
        branch="main",
        update_script=None,
        git_pull_rc=0,
        expected_sha=SHA,
        actual_sha=SHA,
        sha_check_rc=0,
    )
    starts: list[object] = []
    monkeypatch.setattr(admin, "transition_admin_update_state", lambda *args: None)
    monkeypatch.setattr(admin, "_vq_project_root_for_program", lambda unused: tmp_path)
    monkeypatch.setattr(
        admin,
        "source_tree_sha256_at_git_commit",
        lambda project, sha: TREE,
    )
    monkeypatch.setattr(admin, "_installed_tree_digest", lambda python: TREE)
    monkeypatch.setattr(admin, "_persist_managed_update_receipt", lambda *args: None)
    monkeypatch.setattr(admin, "_clear_managed_update_receipt", lambda: None)
    monkeypatch.setattr(
        admin,
        "_commit_managed_update_files",
        lambda prog, state: (True, "rollback virtualenv committed"),
    )
    monkeypatch.setattr(
        admin,
        "_reattest_service_before_start",
        lambda state: (True, "service definition verified"),
    )

    def start(state: object) -> tuple[bool, str]:
        starts.append(state)
        return True, "service started"

    monkeypatch.setattr(admin, "_start_managed_daemon_update", start)
    monkeypatch.setattr(
        admin,
        "_verify_restarted_daemon",
        lambda *args, **kwargs: admin.DaemonProvenance(
            verified=True,
            actual_sha=SHA,
            actual_tree_sha256=TREE,
            detail="strict identity verified",
        ),
    )

    admin._complete_managed_daemon_update(prog, result, lifecycle)

    assert starts == [lifecycle]
    assert result.daemon_restart_attempted is True
    assert result.daemon_restart_succeeded is True
    assert result.daemon_health_verified is True
    assert result.success is True


def _run_logged_update(
    prog: config.VenvProgram,
    *,
    managed: bool,
    probe: admin._SelfUpdateProbe | None = None,
) -> admin.UpdateResult:
    return admin._update_env_logged(
        "vibeqc-queue",
        config.Config(programs={"vibeqc-queue": prog}),
        prog=prog,
        multi_user=False,
        host="localhost",
        admin_token=None,
        expected_tag=None,
        expected_sha=SHA if managed else None,
        force=False,
        restart_daemon=managed,
        require_self_update=managed,
        managed_daemon_restart=managed,
        initial_self_update_probe=probe or _probe(running=True),
        update_script_args=None,
    )


def _patch_update_bracket(
    monkeypatch: pytest.MonkeyPatch,
    *,
    transition=None,
) -> list[tuple[tuple[object, ...], dict[str, object]]]:
    resumed: list[tuple[tuple[object, ...], dict[str, object]]] = []
    monkeypatch.setattr(admin, "_guard_admin_update_marker", lambda **kwargs: None)
    monkeypatch.setattr(
        admin,
        "pause_token_scope_with_proof",
        lambda *args, **kwargs: SimpleNamespace(
            summary="paused", require_quiescent=lambda: None,
        ),
    )
    monkeypatch.setattr(admin, "acquire_admin_update_marker", lambda **kwargs: None)
    monkeypatch.setattr(admin, "_record_admin_update_pause_scope", lambda **kwargs: None)
    monkeypatch.setattr(
        admin,
        "transition_admin_update_state",
        transition or (lambda *args, **kwargs: None),
    )

    def resume(*args: object, **kwargs: object) -> SimpleNamespace:
        resumed.append((args, kwargs))
        return SimpleNamespace(summary="resumed", require_clear=lambda: None)

    monkeypatch.setattr(admin, "resume_token_scope_with_proof", resume)
    monkeypatch.setattr(
        admin,
        "_disarm_proven_pause_scope_without_managed_receipt",
        lambda: None,
    )
    return resumed


def test_late_git_admission_refusal_resumes_and_clears_marker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A post-pause lock race leaves neither jobs paused nor a stale marker."""
    prog = _fake_program(tmp_path)
    resumed: list[str] = []
    monkeypatch.setattr(
        admin,
        "pause_token_scope_with_proof",
        lambda *args, **kwargs: SimpleNamespace(
            summary="paused", require_quiescent=lambda: None,
        ),
    )

    def resume(*args: object, **kwargs: object) -> SimpleNamespace:
        resumed.append("clear")
        return SimpleNamespace(summary="resumed", require_clear=lambda: None)

    monkeypatch.setattr(admin, "resume_token_scope_with_proof", resume)
    service_commands: list[object] = []
    monkeypatch.setattr(
        admin,
        "_begin_managed_daemon_update",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            admin._ManagedGitAdmissionError("late index.lock")
        ),
    )
    monkeypatch.setattr(
        admin,
        "_run_daemon_service_command",
        lambda *args, **kwargs: service_commands.append(args),
    )

    with pytest.raises(admin.AdminError, match="late index.lock"):
        _run_logged_update(prog, managed=True)

    assert resumed == ["clear"]
    assert service_commands == []
    assert admin.read_admin_update_markers() == []


def test_marker_claim_failure_after_pause_still_resumes_jobs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prog = _fake_program(tmp_path)
    resumed = _patch_update_bracket(monkeypatch)
    monkeypatch.setattr(
        admin,
        "acquire_admin_update_marker",
        lambda **kwargs: (_ for _ in ()).throw(admin.AdminError("claim lost")),
    )

    with pytest.raises(admin.AdminError, match="claim lost"):
        _run_logged_update(prog, managed=False)

    assert len(resumed) == 1


def test_batch_marker_claim_failure_after_pause_still_resumes_jobs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prog = _fake_program(tmp_path)
    cfg = config.Config(programs={"vibeqc-queue": prog})
    resumed = _patch_update_bracket(monkeypatch)
    monkeypatch.setattr(
        admin,
        "_detect_vq_self_update",
            lambda unused: admin._SelfUpdateProbe(
                is_self_update=False,
                daemon_running=False,
                service_manager="systemd",
                manager_available=True,
                diagnostic="authoritatively not serving",
            ),
    )
    monkeypatch.setattr(
        admin,
        "acquire_admin_update_marker",
        lambda **kwargs: (_ for _ in ()).throw(admin.AdminError("claim lost")),
    )

    with pytest.raises(admin.AdminError, match="claim lost"):
        admin._update_all_owned(
            cfg,
            host="localhost",
            resolved_progs=[("vibeqc-queue", prog)],
        )

    assert len(resumed) == 1


def test_resuming_transition_failure_cannot_skip_resume(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prog = _fake_program(tmp_path)

    def transition(state: str, **kwargs: object) -> None:
        if state == admin.ADMIN_UPDATE_STATE_RESUMING:
            raise RuntimeError("state store unavailable")

    resumed = _patch_update_bracket(monkeypatch, transition=transition)
    monkeypatch.setattr(
        admin,
        "_do_update_work",
        lambda *args, **kwargs: admin.UpdateResult(
            env="vibeqc-queue",
            git_dir=prog.git_dir,
            branch="main",
            update_script=None,
            git_pull_rc=0,
        ),
    )

    with pytest.raises(RuntimeError, match="state store unavailable"):
        _run_logged_update(prog, managed=False)

    assert len(resumed) == 1


def test_recovery_exception_cannot_skip_resume(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prog = _fake_program(tmp_path)
    resumed = _patch_update_bracket(monkeypatch)
    lifecycle = object()
    monkeypatch.setattr(
        admin, "_begin_managed_daemon_update", lambda *args, **kwargs: lifecycle,
    )
    monkeypatch.setattr(
        admin,
        "_do_update_work",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("build exploded")),
    )
    monkeypatch.setattr(
        admin,
        "_recover_managed_daemon_after_exception",
        lambda *args: (_ for _ in ()).throw(RuntimeError("recovery exploded")),
    )

    with pytest.raises(RuntimeError, match="recovery exploded"):
        _run_logged_update(prog, managed=True)

    assert len(resumed) == 1


def test_failed_completion_attempts_recovery_before_resume(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prog = _fake_program(tmp_path)
    resumed = _patch_update_bracket(monkeypatch)
    lifecycle = object()
    recovered: list[object] = []
    result = admin.UpdateResult(
        env="vibeqc-queue",
        git_dir=prog.git_dir,
        branch="main",
        update_script=None,
        git_pull_rc=0,
        expected_sha=SHA,
        actual_sha=SHA,
        sha_check_rc=0,
    )
    monkeypatch.setattr(
        admin, "_begin_managed_daemon_update", lambda *args, **kwargs: lifecycle,
    )
    monkeypatch.setattr(admin, "_do_update_work", lambda *args, **kwargs: result)

    def fail_completion(*args: object) -> None:
        result.daemon_restart_attempted = True
        result.daemon_restart_succeeded = False
        result.daemon_health_verified = False

    monkeypatch.setattr(admin, "_complete_managed_daemon_update", fail_completion)

    def recover(*args: object) -> tuple[bool, str]:
        recovered.append(args[-1])
        return True, "old daemon restored"

    monkeypatch.setattr(admin, "_recover_managed_daemon_after_exception", recover)

    returned = _run_logged_update(prog, managed=True)

    assert returned is result
    assert recovered == [lifecycle]
    assert len(resumed) == 1
