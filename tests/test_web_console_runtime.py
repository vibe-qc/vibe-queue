"""`vq web status` and `vq doctor` notice a console whose vq cannot run it (#28).

host_0, 2026-09-09 to 2026-09-12: `vq-web.service` pointed at a venv that
had been rebuilt without the `web` extra, and the unit failed on every start,
5230 restarts, with the evidence only in the journal. `vq web install` refuses
that since `aa825fd`, but a venv that loses its runtime after the install went
unreported.

The probe runs the interpreter recorded in the install marker, out of process
and with a timeout: the vq answering `vq web status` may not be the vq the
unit runs.
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import config, doctor, paths
from vq.cli import main
from vq.web import install


@pytest.fixture
def cfg_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    d = tmp_path / "cfg"
    d.mkdir()
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(d))
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    return d


def _write_marker(
    cfg_dir: Path, python: str, *, kind: install.ServiceKind = install.CONSOLE_SERVICE,
) -> None:
    (cfg_dir / kind.marker_name).write_text(
        json.dumps({
            "vq_version": install.__version__,
            kind.command_key: [python, "-m", "vq", *kind.verb],
            "python": python,
            # No manager: the status must not ask systemd or launchd here.
            "manager": "",
            "unit_name": kind.unit_name,
            "unit_path": "/nonexistent/unit",
            "installed_at": "2026-09-13T00:00:00+00:00",
        }),
        encoding="utf-8",
    )


def _isolated_interpreter(tmp_path: Path) -> Path:
    """This Python with no site-packages: it cannot import uvicorn."""
    wrapper = tmp_path / "bin" / "python"
    wrapper.parent.mkdir(parents=True)
    wrapper.write_text(
        f'#!/bin/sh\nexec "{sys.executable}" -I -S "$@"\n', encoding="utf-8",
    )
    wrapper.chmod(0o755)
    return wrapper


class TestStatus:
    def test_a_recorded_interpreter_without_the_web_runtime_is_reported(
        self, cfg_dir: Path, tmp_path: Path,
    ) -> None:
        _write_marker(cfg_dir, str(_isolated_interpreter(tmp_path)))

        status = install.console_service_status()

        assert status.installed is True
        assert status.runtime_ok is False
        assert "uvicorn" in (status.runtime_detail or "")
        assert status.runtime_remedy

    def test_a_recorded_interpreter_with_the_runtime_is_ok(
        self, cfg_dir: Path,
    ) -> None:
        pytest.importorskip("uvicorn")
        _write_marker(cfg_dir, sys.executable)

        status = install.console_service_status()

        assert status.runtime_ok is True
        assert status.runtime_remedy is None

    def test_a_missing_interpreter_is_reported(
        self, cfg_dir: Path, tmp_path: Path,
    ) -> None:
        _write_marker(cfg_dir, str(tmp_path / "gone" / "bin" / "python"))

        status = install.console_service_status()

        assert status.runtime_ok is False
        assert "gone" in (status.runtime_detail or "")

    def test_a_probe_that_does_not_answer_is_unknown_not_broken(
        self, cfg_dir: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        slow = tmp_path / "bin" / "python"
        slow.parent.mkdir(parents=True)
        slow.write_text("#!/bin/sh\nsleep 30\n", encoding="utf-8")
        slow.chmod(0o755)
        _write_marker(cfg_dir, str(slow))
        monkeypatch.setattr(install, "CONSOLE_RUNTIME_PROBE_TIMEOUT_SECONDS", 0.3)

        status = install.console_service_status()

        assert status.runtime_ok is None
        assert "did not answer" in (status.runtime_detail or "")

    def test_the_daemon_service_is_not_probed_for_the_web_runtime(
        self, cfg_dir: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _write_marker(
            cfg_dir, str(_isolated_interpreter(tmp_path)),
            kind=install.DAEMON_SERVICE,
        )

        def forbidden(*args: object, **kwargs: object) -> None:
            raise AssertionError("the daemon needs no web runtime probe")

        monkeypatch.setattr(install, "probe_console_runtime", forbidden)

        status = install.console_service_status(install.DAEMON_SERVICE)

        assert status.runtime_ok is None
        assert status.runtime_detail is None

    def test_no_console_installed_has_no_runtime_verdict(self, cfg_dir: Path) -> None:
        status = install.console_service_status()

        assert status.installed is False
        assert status.runtime_ok is None


class TestWebStatusVerb:
    def test_json_carries_the_verdict_and_keeps_the_old_fields(
        self, cfg_dir: Path, tmp_path: Path,
    ) -> None:
        _write_marker(cfg_dir, str(_isolated_interpreter(tmp_path)))

        result = CliRunner().invoke(main, ["web", "status", "--json"])

        assert result.exit_code == 0, result.output
        payload = json.loads(result.stdout)
        assert payload["runtime_ok"] is False
        assert "uvicorn" in payload["runtime_detail"]
        assert payload["runtime_remedy"]
        for field in (
            "installed", "manager", "unit_name", "unit_path",
            "installed_by_version", "installed_at", "running_version",
            "drifted", "active", "detail",
        ):
            assert field in payload

    def test_text_names_the_broken_runtime_and_the_remedy(
        self, cfg_dir: Path, tmp_path: Path,
    ) -> None:
        _write_marker(cfg_dir, str(_isolated_interpreter(tmp_path)))

        result = CliRunner().invoke(main, ["web", "status"])

        assert result.exit_code == 0, result.output
        assert "runtime:" in result.stdout
        assert "uvicorn" in result.output
        assert "cannot serve the console" in result.stderr


class TestDoctor:
    def test_local_doctor_fails_the_console_runtime_check(
        self, cfg_dir: Path, tmp_path: Path,
    ) -> None:
        _write_marker(cfg_dir, str(_isolated_interpreter(tmp_path)))
        cfg = config.load_config()

        checks = doctor.console_runtime_checks(cfg, "localhost", check_timeout=30)

        assert [c["name"] for c in checks] == ["console_runtime"]
        assert checks[0]["ok"] is False
        assert "uvicorn" in str(checks[0]["message"])

    def test_local_doctor_is_silent_without_a_console(self, cfg_dir: Path) -> None:
        cfg = config.load_config()

        assert doctor.console_runtime_checks(cfg, "localhost", check_timeout=30) == []

    def test_diagnose_host_includes_the_check(
        self, cfg_dir: Path, tmp_path: Path,
    ) -> None:
        _write_marker(cfg_dir, str(_isolated_interpreter(tmp_path)))
        cfg = config.load_config()

        payload = doctor.diagnose_host(cfg, "localhost", timeout=1, check_timeout=30)

        runtime = [c for c in payload["checks"] if c["name"] == "console_runtime"]
        assert runtime and runtime[0]["ok"] is False
        assert payload["ok"] is False

    @pytest.mark.parametrize(
        ("payload", "expected"),
        [
            ({"installed": True, "runtime_ok": False,
              "runtime_detail": "uvicorn is missing", "runtime_remedy": "fix it"},
             [False]),
            ({"installed": True, "runtime_ok": True}, [True]),
            ({"installed": True, "drifted": False}, []),
            ({"installed": False, "runtime_ok": None}, []),
        ],
        ids=["broken", "healthy", "older-remote-vq", "no-console"],
    )
    def test_remote_doctor_reads_the_host_s_own_verdict(
        self,
        cfg_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
        payload: dict[str, object],
        expected: list[bool],
    ) -> None:
        (cfg_dir / "config.toml").write_text(
            '[hosts.remote]\nssh = "remote.example.invalid"\n', encoding="utf-8",
        )
        cfg = config.load_config()
        calls: list[tuple[str, ...]] = []

        def fake_remote(host_cfg: object, *vq_args: str, **kwargs: object):
            calls.append(vq_args)
            return subprocess.CompletedProcess(
                ["vq", *vq_args], 0, json.dumps(payload), "",
            )

        monkeypatch.setattr(doctor, "_run_remote_vq_with_deadline", fake_remote)

        checks = doctor.console_runtime_checks(cfg, "remote", check_timeout=30)

        assert calls == [("web", "status", "--json")]
        assert [c["ok"] for c in checks] == expected
