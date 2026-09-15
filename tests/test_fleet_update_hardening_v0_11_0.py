"""v0.11.0 — fleet-update hardening.

Covers the four robustness fixes that make ``vq admin update`` survive
real-world fleet conditions (cold native-dep rebuilds, Arch perl layout,
flapping LAN/WAN links) and default to the safer serial fan-out:

* ``transport.run_remote_vq(retry_transient=N)`` — retry a transient SSH
  *transport* failure (exit 255 / connect timeout) N times with linear
  backoff; opt-in, so the daemon's bookkeeping calls stay fast-fail.
* ``admin._update_script_timeout()`` — ``VQ_UPDATE_SCRIPT_TIMEOUT`` override
  of the 14400 s update_script cap, for unusually slow cold builds.
* ``admin._augment_build_path()`` — prepend Arch/Manjaro perl dirs (pod2man)
  so a cold libecpint/libcerf build doesn't die "command not found".
* ``vq admin update --all-hosts`` defaults to ``--serial``.

All subprocess / sleep is mocked — no real ssh, no real wall-clock delay.
"""
from __future__ import annotations

import json
import subprocess
from typing import Any

import pytest
from click.testing import CliRunner

from vq import admin, cli, transport
from vq.cli import main
from vq.config import HostConfig, SchedulerRuntimeDeployment


@pytest.fixture
def host_cfg() -> HostConfig:
    return HostConfig(ssh="host_d", remote_vq="vq", remote_python=None)


def _seq_run_factory(returncodes: list[int], calls: list[list[str]]):
    """Fake subprocess.run that returns the next returncode from
    ``returncodes`` on each call (recording argv into ``calls``). Raises
    IndexError if called more often than scripted — that itself is a useful
    assertion (too many retries)."""
    seq = iter(returncodes)

    def fake(cmd: list[str], **kw: Any) -> subprocess.CompletedProcess[str]:
        calls.append(cmd)
        return subprocess.CompletedProcess(
            args=cmd, returncode=next(seq), stdout="out\n", stderr="err\n"
        )

    return fake


class TestRetryTransient:
    """run_remote_vq(retry_transient=N): retry SSH exit 255 / connect
    timeout, never a real remote non-zero exit, never when N == 0."""

    def test_default_does_not_retry_255(
        self, host_cfg: HostConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """retry_transient defaults to 0 → a single 255 raises immediately
        (the daemon's fast-fail bookkeeping path is unchanged)."""
        calls: list[list[str]] = []
        slept: list[float] = []
        monkeypatch.setattr(transport.subprocess, "run",
                            _seq_run_factory([255], calls))
        monkeypatch.setattr(transport.time, "sleep", lambda s: slept.append(s))
        with pytest.raises(transport.RemoteError, match="255"):
            transport.run_remote_vq(host_cfg, "queue", "localhost")
        assert len(calls) == 1
        assert slept == []

    def test_retries_255_then_succeeds(
        self, host_cfg: HostConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Two transient 255s then a clean exit → success, 3 attempts,
        2 backoff sleeps (linear: 2 s, 4 s)."""
        calls: list[list[str]] = []
        slept: list[float] = []
        monkeypatch.setattr(transport.subprocess, "run",
                            _seq_run_factory([255, 255, 0], calls))
        monkeypatch.setattr(transport.time, "sleep", lambda s: slept.append(s))
        proc = transport.run_remote_vq(
            host_cfg, "queue", "localhost", retry_transient=2
        )
        assert proc.returncode == 0
        assert len(calls) == 3
        assert slept == [
            transport._SSH_RETRY_BACKOFF_SECONDS * 1,
            transport._SSH_RETRY_BACKOFF_SECONDS * 2,
        ]

    def test_retry_launch_failure_keeps_earlier_outcome_ambiguous(
        self, host_cfg: HostConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls = 0
        slept: list[float] = []

        def fake(
            cmd: list[str],
            **_kwargs: Any,
        ) -> subprocess.CompletedProcess[str]:
            nonlocal calls
            calls += 1
            if calls == 1:
                return subprocess.CompletedProcess(
                    args=cmd,
                    returncode=255,
                    stdout="",
                    stderr="connection lost",
                )
            raise FileNotFoundError(2, "No such file or directory", "ssh")

        monkeypatch.setattr(transport.subprocess, "run", fake)
        monkeypatch.setattr(transport.time, "sleep", slept.append)

        with pytest.raises(
            transport.RemoteOutcomeUnknown,
            match="earlier ambiguous transport attempt",
        ):
            transport.run_remote_vq(
                host_cfg,
                "queue",
                "localhost",
                retry_transient=2,
            )

        assert calls == 2
        assert slept == [transport._SSH_RETRY_BACKOFF_SECONDS]

    def test_exhausts_retries_on_persistent_255(
        self, host_cfg: HostConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Persistent 255 → RemoteError after retry_transient+1 attempts."""
        calls: list[list[str]] = []
        monkeypatch.setattr(transport.subprocess, "run",
                            _seq_run_factory([255, 255, 255], calls))
        monkeypatch.setattr(transport.time, "sleep", lambda s: None)
        with pytest.raises(transport.RemoteError, match="255"):
            transport.run_remote_vq(
                host_cfg, "queue", "localhost", retry_transient=2
            )
        assert len(calls) == 3

    def test_real_remote_nonzero_exit_is_never_retried(
        self, host_cfg: HostConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A genuine remote-vq failure (exit 2, not 255) is the command's
        own result — surfaced on the first attempt, never retried, even
        with retry_transient set."""
        calls: list[list[str]] = []
        slept: list[float] = []
        monkeypatch.setattr(transport.subprocess, "run",
                            _seq_run_factory([2], calls))
        monkeypatch.setattr(transport.time, "sleep", lambda s: slept.append(s))
        with pytest.raises(transport.RemoteError, match="exit 2"):
            transport.run_remote_vq(
                host_cfg, "queue", "localhost", retry_transient=2
            )
        assert len(calls) == 1
        assert slept == []

    def test_exit_127_reports_missing_remote_vq_hint(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        host_cfg = HostConfig(
            ssh="host_e",
            remote_vq="/opt/vq/missing/bin/vq",
            remote_python=None,
        )

        def fake(cmd: list[str], **kw: Any) -> subprocess.CompletedProcess[str]:
            return subprocess.CompletedProcess(
                args=cmd,
                returncode=127,
                stdout="",
                stderr=(
                    "zsh:1: no such file or directory: "
                    "/opt/vq/missing/bin/vq\n"
                ),
            )

        monkeypatch.setattr(transport.subprocess, "run", fake)

        with pytest.raises(transport.RemoteError) as excinfo:
            transport.run_remote_vq(host_cfg, "queue", "localhost")

        message = str(excinfo.value)
        assert "remote vq failed (exit 127) on host_e" in message
        assert "configured remote_vq command was not found" in message
        assert "configured remote_vq: /opt/vq/missing/bin/vq" in message
        assert "vq doctor host_e --verbose" in message
        assert "vq host down host_e --reason \"remote_vq missing\"" in message
        assert "vq host up host_e" in message

    def test_retries_timeout_then_succeeds(
        self, host_cfg: HostConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A connect-timeout (TimeoutExpired) is transient too: retried,
        then a clean run succeeds."""
        calls: list[int] = []
        slept: list[float] = []

        def fake(cmd: list[str], **kw: Any) -> subprocess.CompletedProcess[str]:
            calls.append(1)
            if len(calls) == 1:
                raise subprocess.TimeoutExpired(cmd, kw.get("timeout", 0))
            return subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout="ok\n", stderr=""
            )

        monkeypatch.setattr(transport.subprocess, "run", fake)
        monkeypatch.setattr(transport.time, "sleep", lambda s: slept.append(s))
        proc = transport.run_remote_vq(
            host_cfg, "queue", "localhost", retry_transient=1, timeout=1.0
        )
        assert proc.stdout == "ok\n"
        assert len(calls) == 2
        assert slept == [transport._SSH_RETRY_BACKOFF_SECONDS * 1]

    def test_timeout_exhausted_raises_remote_error(
        self, host_cfg: HostConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Persistent timeout → RemoteError('timed out') after exhausting
        retries."""
        calls: list[int] = []

        def fake(cmd: list[str], **kw: Any) -> subprocess.CompletedProcess[str]:
            calls.append(1)
            raise subprocess.TimeoutExpired(cmd, kw.get("timeout", 0))

        monkeypatch.setattr(transport.subprocess, "run", fake)
        monkeypatch.setattr(transport.time, "sleep", lambda s: None)
        with pytest.raises(transport.RemoteError, match="timed out"):
            transport.run_remote_vq(
                host_cfg, "queue", "localhost", retry_transient=2, timeout=1.0
            )
        assert len(calls) == 3


class TestUpdateScriptTimeout:
    """admin._update_script_timeout(): VQ_UPDATE_SCRIPT_TIMEOUT override of
    the 14400 s default, with garbage / non-positive falling back."""

    def test_default_when_unset(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("VQ_UPDATE_SCRIPT_TIMEOUT", raising=False)
        assert admin.UPDATE_SCRIPT_TIMEOUT_SECONDS == 14400
        assert admin._update_script_timeout() == 14400.0

    def test_delegated_default_leaves_cleanup_margin(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("VQ_UPDATE_SCRIPT_TIMEOUT", raising=False)
        monkeypatch.delenv("VQ_BUILD_STALL_TIMEOUT", raising=False)
        monkeypatch.delenv("VQ_REMOTE_ADMIN_UPDATE_TIMEOUT", raising=False)
        assert transport.DEFAULT_REMOTE_ADMIN_UPDATE_TIMEOUT_SECONDS == 15000.0
        remote_env, outer = cli._remote_admin_update_contract()
        assert remote_env["VQ_UPDATE_SCRIPT_TIMEOUT"] == "14400.0"
        assert outer == 15000.0
        assert outer == (
            admin._update_script_timeout()
            + cli._REMOTE_ADMIN_UPDATE_TIMEOUT_MARGIN_SECONDS
        )

    def test_scheduler_runtime_default_allows_cold_native_build(self) -> None:
        deployment = SchedulerRuntimeDeployment(
            update_command="/site/bin/deploy",
            verify_command="/site/bin/verify",
        )
        assert deployment.timeout_seconds == 14400.0

    def test_valid_override(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("VQ_UPDATE_SCRIPT_TIMEOUT", "7200")
        assert admin._update_script_timeout() == 7200.0

    def test_float_override(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("VQ_UPDATE_SCRIPT_TIMEOUT", "5400.5")
        assert admin._update_script_timeout() == 5400.5

    @pytest.mark.parametrize(
        "bad", ["", "  ", "garbage", "0", "-1", "nan", "inf", "-inf"]
    )
    def test_garbage_or_nonpositive_falls_back(
        self, monkeypatch: pytest.MonkeyPatch, bad: str
    ) -> None:
        monkeypatch.setenv("VQ_UPDATE_SCRIPT_TIMEOUT", bad)
        assert admin._update_script_timeout() == float(
            admin.UPDATE_SCRIPT_TIMEOUT_SECONDS
        )


class TestAugmentBuildPath:
    """admin._augment_build_path(): prepend the existing Arch/Manjaro perl
    dirs to env['PATH'] in place; no-op where none exist."""

    def test_prepends_existing_dirs(
        self, tmp_path: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        present = tmp_path / "core_perl"
        present.mkdir()
        absent = tmp_path / "vendor_perl"  # not created
        monkeypatch.setattr(
            admin, "_BUILD_PATH_DIRS", (str(present), str(absent))
        )
        env = {"PATH": "/usr/bin:/bin"}
        admin._augment_build_path(env)
        assert env["PATH"] == f"{present}:/usr/bin:/bin"

    def test_noop_when_none_exist(
        self, tmp_path: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            admin, "_BUILD_PATH_DIRS",
            (str(tmp_path / "nope1"), str(tmp_path / "nope2")),
        )
        env = {"PATH": "/usr/bin:/bin"}
        admin._augment_build_path(env)
        assert env["PATH"] == "/usr/bin:/bin"

    def test_handles_missing_path_key(
        self, tmp_path: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        present = tmp_path / "core_perl"
        present.mkdir()
        monkeypatch.setattr(admin, "_BUILD_PATH_DIRS", (str(present),))
        env: dict = {}
        admin._augment_build_path(env)
        assert env["PATH"] == str(present)


class TestConfiguredBuildPathDirs:
    """`build_path_dirs` extends the policy without a vq release.

    The 2026-09-10 migration confirmed on host_b, host_e and host_d that a
    non-login shell has no `/usr/bin/core_perl`, and expected "other
    Manjaro-specific PATH gaps". The built-in list is what has bitten; this
    is how the next one gets fixed without a release and a fleet rollout.
    """

    def test_configured_dirs_precede_the_built_in_ones(
        self, tmp_path: Any, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        builtin = tmp_path / "core_perl"
        builtin.mkdir()
        configured = tmp_path / "opt-tools"
        configured.mkdir()
        monkeypatch.setattr(admin, "_BUILD_PATH_DIRS", (str(builtin),))
        monkeypatch.setattr(
            admin.config, "load_config",
            lambda: admin.config.Config(build_path_dirs=[str(configured)]),
        )
        env = {"PATH": "/usr/bin:/bin"}

        admin._augment_build_path(env)

        assert env["PATH"] == f"{configured}:{builtin}:/usr/bin:/bin"

    def test_a_directory_absent_on_this_host_is_skipped(
        self, tmp_path: Any, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """One config is shared across a mixed fleet, so naming a directory
        that exists on only some hosts must stay safe on the others."""
        monkeypatch.setattr(admin, "_BUILD_PATH_DIRS", ())
        monkeypatch.setattr(
            admin.config, "load_config",
            lambda: admin.config.Config(
                build_path_dirs=[str(tmp_path / "not-on-this-host")]
            ),
        )
        env = {"PATH": "/usr/bin:/bin"}

        admin._augment_build_path(env)

        assert env["PATH"] == "/usr/bin:/bin"

    def test_a_directory_named_twice_appears_once(
        self, tmp_path: Any, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        shared = tmp_path / "core_perl"
        shared.mkdir()
        monkeypatch.setattr(admin, "_BUILD_PATH_DIRS", (str(shared),))
        monkeypatch.setattr(
            admin.config, "load_config",
            lambda: admin.config.Config(build_path_dirs=[str(shared)]),
        )
        env = {"PATH": "/usr/bin"}

        admin._augment_build_path(env)

        assert env["PATH"] == f"{shared}:/usr/bin"

    def test_an_unreadable_config_does_not_fail_the_build(
        self, tmp_path: Any, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        builtin = tmp_path / "core_perl"
        builtin.mkdir()
        monkeypatch.setattr(admin, "_BUILD_PATH_DIRS", (str(builtin),))

        def broken() -> None:
            raise admin.config.ConfigError("unreadable")

        monkeypatch.setattr(admin.config, "load_config", broken)
        env = {"PATH": "/usr/bin"}

        admin._augment_build_path(env)

        assert env["PATH"] == f"{builtin}:/usr/bin"

    def test_a_relative_entry_is_refused_at_load_time(self) -> None:
        """A relative PATH entry is resolved against whatever directory the
        build happens to run in, which is not a thing to discover at compile
        time on a remote host."""
        with pytest.raises(Exception, match="must be an absolute path"):
            admin.config.Config(build_path_dirs=["tools/bin"])


class TestSerialDefault:
    """vq admin update --all-hosts defaults to --serial."""

    def test_help_advertises_serial_default(self) -> None:
        result = CliRunner().invoke(main, ["admin", "update", "--help"])
        assert result.exit_code == 0
        assert "--serial" in result.output
        assert "--parallel" in result.output

    def test_serial_option_default_is_true(self) -> None:
        """The Click parameter default is True (serial) so an operator who
        omits the flag gets the safe one-host-at-a-time fan-out."""
        params = {p.name: p for p in main.commands["admin"].commands["update"].params}
        assert params["serial"].default is True


def test_a_real_build_script_sees_the_extra_path_dirs(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The end-to-end guarantee, checked by what the build resolves.

    The three unit tests above assert what `_augment_build_path` puts in a
    dict. This one runs an actual `update_script` through the real invocation
    path and reads the PATH the script itself saw -- the difference between
    "the helper is correct" and "the build gets it", which is the distinction
    the 2026-09-10 migration was repeatedly caught by.
    """
    perl_dir = tmp_path / "core_perl"
    perl_dir.mkdir()
    (perl_dir / "pod2man").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    (perl_dir / "pod2man").chmod(0o755)
    monkeypatch.setattr(admin, "_BUILD_PATH_DIRS", (str(perl_dir),))

    git_dir = tmp_path / "checkout"
    (git_dir / "scripts").mkdir(parents=True)
    script = git_dir / "scripts" / "update.sh"
    script.write_text(
        "#!/usr/bin/env bash\n"
        'printf "PATH=%s\\n" "$PATH"\n'
        'command -v pod2man || { echo "pod2man NOT FOUND"; exit 2; }\n',
        encoding="utf-8",
    )
    script.chmod(0o755)

    work_errors: list[str] = []
    rc, output, seconds = admin._run_update_script(
        git_dir, "scripts/update.sh", work_errors=work_errors,
    )

    assert rc == 0, output
    assert work_errors == []
    # v0.26.1: the lane reports how long it took. Most of the fleet builds
    # here, and until this it reported nothing at all.
    assert seconds is not None and seconds >= 0.0
    assert str(perl_dir / "pod2man") in output
    assert "pod2man NOT FOUND" not in output


class TestVenvLaneIsMeasured:
    """The `update_script` lane reports duration and deploy metrics.

    `SchedulerRuntimeUpdateResult` has carried `metrics` -- dependency-cache
    decision, ccache hit rate, per-phase durations parsed from
    `VQ-DEPLOY-METRIC` lines -- since the runtime deployers landed. That
    covers host_f and host_c. `UpdateResult`, which is where most of the fleet
    builds, carried neither metrics nor a duration, so "did that update take
    three minutes or seventy" had no answer. You cannot speed up a build you
    cannot time.
    """

    def _script(self, tmp_path: Any, body: str) -> Any:
        from pathlib import Path

        git_dir = Path(tmp_path) / "checkout"
        (git_dir / "scripts").mkdir(parents=True)
        script = git_dir / "scripts" / "update.sh"
        script.write_text(f"#!/usr/bin/env bash\n{body}\n", encoding="utf-8")
        script.chmod(0o755)
        return git_dir

    def test_a_script_that_ran_reports_how_long_it_took(
        self, tmp_path: Any,
    ) -> None:
        git_dir = self._script(tmp_path, "echo building")
        errors: list[str] = []

        rc, _output, seconds = admin._run_update_script(
            git_dir, "scripts/update.sh", work_errors=errors,
        )

        assert rc == 0
        assert seconds is not None and seconds >= 0.0

    def test_a_script_that_never_ran_reports_no_duration(
        self, tmp_path: Any,
    ) -> None:
        """None, not 0.0: "did not run" and "ran instantly" are different."""
        from pathlib import Path

        errors: list[str] = []

        rc, _output, seconds = admin._run_update_script(
            Path(tmp_path), "scripts/missing.sh", work_errors=errors,
        )

        assert rc is None
        assert seconds is None
        assert errors

    def test_deploy_metrics_are_parsed_from_this_lane_too(self) -> None:
        parsed = admin.parse_deploy_metrics(
            "configuring\n"
            "VQ-DEPLOY-METRIC compiler_cache_hit_rate_percent=97\n"
            "VQ-DEPLOY-METRIC native_deps_rebuilt=no\n"
            "done\n"
        )

        assert parsed == {
            "compiler_cache_hit_rate_percent": "97",
            "native_deps_rebuilt": "no",
        }

    def test_the_summary_shows_the_duration_and_the_metrics(self) -> None:
        result = admin.UpdateResult(
            env="vibeqc-dev",
            git_dir="/srv/vibeqc-dev",
            branch="main",
            update_script="scripts/update.sh",
            update_script_rc=0,
            update_script_output="built",
            update_script_seconds=4223.7,
            metrics={"compiler_cache_hit_rate_percent": "97"},
        )

        rendered = admin.format_update_result(result)

        # Minutes, because nobody reads 4223.7 as seventy minutes.
        assert "took 70m23s" in rendered
        assert "-- deploy metrics --" in rendered
        assert "compiler_cache_hit_rate_percent: 97" in rendered

    def test_a_lane_that_emits_no_metrics_grows_no_empty_section(self) -> None:
        """Absence is worth seeing, but not as a heading with nothing in it."""
        result = admin.UpdateResult(
            env="vibeqc-dev",
            git_dir="/srv/vibeqc-dev",
            branch="main",
            update_script="scripts/update.sh",
            update_script_rc=0,
            update_script_output="built",
            update_script_seconds=12.0,
        )

        rendered = admin.format_update_result(result)

        assert "deploy metrics" not in rendered
        assert "took 12.0s" in rendered

    def test_the_json_payload_carries_both(self) -> None:
        result = admin.UpdateResult(
            env="vibeqc-dev",
            git_dir="/srv/vibeqc-dev",
            branch="main",
            update_script="scripts/update.sh",
            update_script_seconds=61.5,
            metrics={"native_deps_rebuilt": "no"},
        )

        payload = json.loads(admin.format_update_result_json(result))

        assert payload["update_script_seconds"] == 61.5
        assert payload["metrics"] == {"native_deps_rebuilt": "no"}
