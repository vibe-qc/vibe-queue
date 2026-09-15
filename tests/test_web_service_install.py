"""Safety and failure-propagation tests for ``vq web install``."""

from __future__ import annotations

import os
import plistlib
import subprocess
import sys
from pathlib import Path

import pytest

from vq import config, paths
from vq.web import install


@pytest.fixture
def service_environment(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    executable = tmp_path / "venv with spaces" / "bin" / "vq%test"
    executable.parent.mkdir(parents=True)
    executable.write_text("#!/bin/sh\n", encoding="utf-8")
    executable.chmod(0o755)
    # v0.26.1: build_plan resolves through resolve_service_command(kind), so
    # the daemon and the console share one implementation. Patch the seam that
    # both use; the point of this fixture is the awkward executable path.
    monkeypatch.setattr(
        install,
        "resolve_service_command",
        lambda kind: [str(executable), *kind.verb],
    )
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "config"))
    (tmp_path / "state").mkdir()
    (tmp_path / "config").mkdir()
    return executable


@pytest.mark.parametrize(
    "name",
    ["../vq-web", "vq/web", ".", "vq-web.service", "vq web", ""],
)
def test_unit_name_rejects_path_traversal_and_backend_syntax(name: str) -> None:
    with pytest.raises(install.InstallError, match="invalid service name"):
        install.validate_unit_name(name)


def test_systemd_command_quotes_spaces_and_percent_specifiers(
    service_environment: Path,
) -> None:
    plan = install.build_plan(manager="systemd-user")
    unit = plan.writes[0].content

    escaped = str(service_environment).replace("%", "%%")
    assert f'ExecStart="{escaped}" "web" "run"' in unit
    assert 'Environment="VQ_CONFIG_DIR=' in unit
    assert 'Environment="VQ_STATE_DIR=' in unit


def test_launchd_preserves_explicit_vq_roots(
    service_environment: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    test_root = service_environment.parents[2]
    multi_user_root = test_root / "multi user"
    monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(multi_user_root))

    plan = install.build_plan(manager="launchd-user")
    payload = plistlib.loads(plan.writes[0].content.encode("utf-8"))

    assert payload["EnvironmentVariables"] == {
        config.ENV_CONFIG_DIR: str(test_root / "config"),
        paths.ENV_MULTI_USER_ROOT: str(multi_user_root),
        paths.ENV_STATE_DIR: str(test_root / "state"),
    }


def test_system_service_requires_explicit_config_and_state_roots(
    service_environment: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(config.ENV_CONFIG_DIR)
    monkeypatch.delenv(paths.ENV_STATE_DIR)

    with pytest.raises(install.InstallError, match="explicit absolute VQ_CONFIG_DIR"):
        install.build_plan(manager="systemd-system", service_user="vq-web")


def test_system_service_requires_and_records_an_unprivileged_account(
    service_environment: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(install.InstallError, match="requires --service-user"):
        install.build_plan(manager="systemd-system")

    service_home = service_environment.parents[2] / "service home"
    service_home.mkdir()
    monkeypatch.setattr(
        install,
        "_service_account",
        lambda username: install.ServiceAccount(
            username,
            service_home,
            os.getuid(),
            frozenset(os.getgroups() or [os.getgid()]),
        ),
    )
    plan = install.build_plan(manager="systemd-system", service_user="vq-web")
    unit = plan.writes[0].content

    assert "User=vq-web" in unit
    assert f'Environment="HOME={service_home}"' in unit
    assert "User=root" not in unit


def test_system_service_rejects_an_executable_the_account_cannot_run(
    service_environment: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service_environment.chmod(0o700)
    monkeypatch.setattr(
        install,
        "_service_account",
        lambda username: install.ServiceAccount(
            username,
            Path("/srv/vq"),
            os.getuid() + 10000,
            frozenset({os.getgid() + 10000}),
        ),
    )

    with pytest.raises(install.InstallError, match="cannot traverse|lacks required access"):
        install.build_plan(manager="systemd-system", service_user="vq-web")


def test_root_is_never_a_valid_web_service_account() -> None:
    with pytest.raises(install.InstallError, match="must not run as root"):
        install._service_account("root")


def test_install_command_failure_raises_and_does_not_write_marker(
    tmp_path: Path,
) -> None:
    unit = tmp_path / "vq-web.service"
    marker = tmp_path / "web-console-install.json"
    plan = install.InstallPlan(
        manager="systemd-user",
        unit_name="vq-web",
        writes=[install.FileWrite(unit, "unit\n")],
        commands=[
            install.Command(
                [sys.executable, "-c", "raise SystemExit(17)"],
                "intentional failure",
            )
        ],
        final_writes=[install.FileWrite(marker, "{}\n", mode=0o600)],
    )

    with pytest.raises(install.InstallError, match="command failed"):
        install.apply_plan(plan)

    assert unit.read_text(encoding="utf-8") == "unit\n"
    assert not marker.exists()


def test_install_writes_provenance_only_after_commands_succeed(
    tmp_path: Path,
) -> None:
    marker = tmp_path / "web-console-install.json"
    plan = install.InstallPlan(
        manager="systemd-user",
        unit_name="vq-web",
        commands=[install.Command([sys.executable, "-c", "pass"], "success")],
        final_writes=[install.FileWrite(marker, "{}\n", mode=0o600)],
    )

    log = install.apply_plan(plan)

    assert marker.exists()
    assert marker.stat().st_mode & 0o777 == 0o600
    assert log[-1] == f"wrote {marker}"


def test_launchd_bootout_ignores_absence_but_not_permission_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    marker = tmp_path / "marker"
    command = install.Command(
        ["launchctl", "bootout", "gui/501/com.vq.web"],
        allow_failure=True,
    )
    plan = install.InstallPlan(
        manager="launchd-user",
        unit_name="vq-web",
        commands=[command],
        final_writes=[install.FileWrite(marker, "ok\n")],
    )

    monkeypatch.setattr(
        install.subprocess,
        "run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess(
            command.argv, 3, "", "Boot-out failed: 3: No such process"
        ),
    )
    install.apply_plan(plan)
    assert marker.exists()

    marker.unlink()
    monkeypatch.setattr(
        install.subprocess,
        "run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess(
            command.argv, 1, "", "Operation not permitted"
        ),
    )
    with pytest.raises(install.InstallError, match="Operation not permitted"):
        install.apply_plan(plan)
    assert not marker.exists()


def test_uninstall_failure_keeps_the_unit_for_operator_recovery(
    tmp_path: Path,
) -> None:
    unit = tmp_path / "vq-web.service"
    unit.write_text("unit\n", encoding="utf-8")
    plan = install.UninstallPlan(
        manager="systemd-user",
        unit_name="vq-web",
        commands_before_remove=[
            install.Command(
                [sys.executable, "-c", "raise SystemExit(18)"],
                "intentional failure",
            )
        ],
        removals=[unit],
    )

    with pytest.raises(install.InstallError, match="command failed"):
        install.apply_uninstall_plan(plan)

    assert unit.exists()


def test_uninstall_reload_failure_keeps_the_provenance_marker(
    tmp_path: Path,
) -> None:
    unit = tmp_path / "vq-web.service"
    marker = tmp_path / "web-console-install.json"
    unit.write_text("unit\n", encoding="utf-8")
    marker.write_text("{}\n", encoding="utf-8")
    plan = install.UninstallPlan(
        manager="systemd-user",
        unit_name="vq-web",
        removals=[unit],
        commands_after_remove=[
            install.Command(
                [sys.executable, "-c", "raise SystemExit(19)"],
                "intentional reload failure",
            )
        ],
        final_removals=[marker],
    )

    with pytest.raises(install.InstallError, match="command failed"):
        install.apply_uninstall_plan(plan)

    assert not unit.exists()
    assert marker.exists()


def test_systemd_uninstall_reloads_only_after_the_unit_is_removed(
    service_environment: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = install.build_uninstall_plan(manager="systemd-user")
    unit = plan.removals[0]
    unit.parent.mkdir(parents=True, exist_ok=True)
    unit.write_text("unit\n", encoding="utf-8")
    observed: list[tuple[list[str], bool]] = []

    def fake_run(argv: list[str], **_: object) -> subprocess.CompletedProcess[str]:
        observed.append((argv, unit.exists()))
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(install.subprocess, "run", fake_run)

    install.apply_uninstall_plan(plan)

    assert observed[0][0][-3:] == ["disable", "--now", "vq-web"]
    assert observed[0][1] is True
    assert observed[-1][0][-1] == "daemon-reload"
    assert observed[-1][1] is False


def test_systemd_uninstall_is_idempotent_when_unit_is_already_absent(
    service_environment: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = install.build_uninstall_plan(manager="systemd-user")

    def fake_run(argv: list[str], **_: object) -> subprocess.CompletedProcess[str]:
        if "disable" in argv:
            return subprocess.CompletedProcess(
                argv,
                1,
                "",
                "Failed to disable unit: Unit file vq-web.service does not exist.",
            )
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(install.subprocess, "run", fake_run)

    log = install.apply_uninstall_plan(plan)

    assert log[0].startswith("skipped: systemctl --user disable --now vq-web")
    assert log[-1] == "ok: systemctl --user daemon-reload"


@pytest.mark.parametrize('kind', [install.CONSOLE_SERVICE, install.DAEMON_SERVICE])
@pytest.mark.parametrize('bootstrap_rc', [1, 5])
def test_launchd_reinstall_waits_for_unload_and_retries_transient_bootstrap(
    service_environment, monkeypatch, kind, bootstrap_rc,
):
    plan = install.build_plan(manager='launchd-user', kind=kind)
    marker = plan.final_writes[0].path
    assert '$ launchctl print ' in plan.render()
    responses = iter([
        ('bootout', 0, ''),
        ('print', 0, 'state = running'),
        ('print', 113, 'Could not find service'),
        ('bootstrap', bootstrap_rc, 'Bootstrap failed: 5: Input/output error'),
        ('print', 113, 'Could not find service'),
        ('bootstrap', 0, ''),
        ('enable', 0, ''),
        ('kickstart', 0, ''),
    ])
    seen = []
    now = [0.0]
    monkeypatch.setattr(install.time, 'monotonic', lambda: now[0])
    monkeypatch.setattr(install.time, 'sleep', lambda seconds: now.__setitem__(0, now[0] + seconds))

    def run(argv, **kwargs):
        verb, code, output = next(responses)
        assert argv[1] == verb
        assert not marker.exists()
        assert 0 < kwargs['timeout'] <= 60
        seen.append(verb)
        return subprocess.CompletedProcess(argv, code, '', output)

    monkeypatch.setattr(install.subprocess, 'run', run)
    install.apply_plan(plan)
    assert marker.exists()
    assert seen.count('bootstrap') == 2
    assert next(responses, None) is None


@pytest.mark.parametrize('failure', ['loaded', 'eio', 'permission', 'unknown-state'])
def test_launchd_reinstall_fails_bounded_without_provenance(
    service_environment, monkeypatch, failure,
):
    plan = install.build_plan(manager='launchd-user')
    marker = plan.final_writes[0].path
    now = [0.0]
    monkeypatch.setattr(install, '_LAUNCHD_REPLACE_TIMEOUT_SECONDS', 0.3)
    monkeypatch.setattr(install.time, 'monotonic', lambda: now[0])
    monkeypatch.setattr(install.time, 'sleep', lambda seconds: now.__setitem__(0, now[0] + seconds))
    calls = []

    def run(argv, **kwargs):
        calls.append(argv[1])
        if argv[1] == 'bootout':
            code, output = 3, 'No such process'
        elif argv[1] == 'print':
            code, output = ((0, 'still loaded') if failure == 'loaded'
                            else (113, 'Could not find service'))
            if failure == 'unknown-state':
                code, output = 1, 'Operation not permitted'
        elif argv[1] == 'bootstrap':
            code, output = ((5, 'Bootstrap failed: 5: Input/output error') if failure == 'eio'
                            else (1, 'Operation not permitted'))
        else:
            pytest.fail(f'unexpected command after failure: {argv}')
        return subprocess.CompletedProcess(argv, code, '', output)

    monkeypatch.setattr(install.subprocess, 'run', run)
    with pytest.raises(install.InstallError):
        install.apply_plan(plan)
    assert not marker.exists()
    assert now[0] <= 0.3
    if failure == 'permission':
        assert calls.count('bootstrap') == 1
    if failure in {'loaded', 'unknown-state'}:
        assert 'bootstrap' not in calls
