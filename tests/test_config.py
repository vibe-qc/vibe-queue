"""Tests for vq.config: TOML parsing, host lookup, default-host resolution."""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest
from pydantic import ValidationError

from vq import config


def _default_config_dir_in_subprocess(
    tmp_path: Path,
    *,
    xdg_config: Path | None = None,
) -> Path:
    """Exercise the implicit config path outside pytest and a real HOME."""
    env = dict(os.environ)
    for name in (
        config.ENV_CONFIG_DIR,
        "PYTEST_CURRENT_TEST",
        "PYTEST_VERSION",
        "PYTEST_ADDOPTS",
        "XDG_CONFIG_HOME",
    ):
        env.pop(name, None)
    home = tmp_path / "subprocess-home"
    home.mkdir()
    env["HOME"] = str(home)
    if xdg_config is not None:
        env["XDG_CONFIG_HOME"] = str(xdg_config)
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1] / "src")
    result = subprocess.run(
        [sys.executable, "-c", "from vq import config; print(config.config_dir())"],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    return Path(result.stdout.strip())


@pytest.fixture
def cfg_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point $VQ_CONFIG_DIR at tmp_path/cfg so each test starts fresh."""
    d = tmp_path / "cfg"
    d.mkdir()
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(d))
    return d


class TestConfigDir:
    def test_env_override(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "x"))
        assert config.config_dir() == tmp_path / "x"

    def test_xdg_fallback(self, tmp_path: Path) -> None:
        xdg_config = tmp_path / "xdg"
        assert _default_config_dir_in_subprocess(
            tmp_path,
            xdg_config=xdg_config,
        ) == xdg_config / "vq"

    def test_default_to_dot_config(self, tmp_path: Path) -> None:
        assert _default_config_dir_in_subprocess(
            tmp_path,
        ) == tmp_path / "subprocess-home" / ".config" / "vq"


class TestLoadConfig:
    def test_missing_file_returns_empty_config(self, cfg_dir: Path) -> None:
        c = config.load_config()
        assert c.default_host is None
        assert c.hosts == {}

    def test_empty_file_returns_empty_config(self, cfg_dir: Path) -> None:
        (cfg_dir / "config.toml").write_text("")
        c = config.load_config()
        assert c.default_host is None
        assert c.hosts == {}

    def test_minimal_valid_file(self, cfg_dir: Path) -> None:
        (cfg_dir / "config.toml").write_text(
            'default_host = "host_d"\n'
            '\n'
            '[hosts.host_d]\n'
            'ssh = "host_d"\n'
        )
        c = config.load_config()
        assert c.default_host == "host_d"
        assert c.hosts["host_d"].ssh == "host_d"
        assert c.hosts["host_d"].remote_vq == "vq"  # default
        assert c.hosts["host_d"].remote_python is None

    def test_full_host_config(self, cfg_dir: Path) -> None:
        # Use RFC 2606 example.com for the host fixture so we don't
        # leak any real network topology in the public test suite.
        (cfg_dir / "config.toml").write_text(
            '[hosts.compute]\n'
            'ssh = "user@compute.example.com"\n'
            'remote_vq = "/home/user/.local/bin/vq"\n'
            'remote_python = "/home/user/vibeqc/.venv/bin/python"\n'
        )
        c = config.load_config()
        h = c.hosts["compute"]
        assert h.ssh == "user@compute.example.com"
        assert h.remote_vq == "/home/user/.local/bin/vq"
        assert h.remote_python == "/home/user/vibeqc/.venv/bin/python"

    def test_multiple_hosts(self, cfg_dir: Path) -> None:
        (cfg_dir / "config.toml").write_text(
            '[hosts.compute]\n'
            'ssh = "compute"\n'
            '\n'
            '[hosts.compute2]\n'
            'ssh = "user@compute2"\n'
        )
        c = config.load_config()
        assert set(c.hosts.keys()) == {"compute", "compute2"}

    def test_bad_toml_raises_config_error(self, cfg_dir: Path) -> None:
        (cfg_dir / "config.toml").write_text("not = valid = toml")
        with pytest.raises(config.ConfigError, match="failed to parse"):
            config.load_config()

    def test_extra_top_level_field_is_ignored_not_fatal(
        self, cfg_dir: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        """v0.26.1: an unknown top-level key costs a warning, not the host.

        Adding ``pin_source_repos`` made vq 0.25.7 reject whole configs with a
        pydantic ``extra_forbidden`` dump, so a driver config a host's older vq
        could not parse took that host out entirely."""
        (cfg_dir / "config.toml").write_text(
            'default_host = "host_d"\nunknown_top_level = "x"\n'
        )
        cfg = config.load_config()
        assert cfg.default_host == "host_d"
        warning = capsys.readouterr().err
        assert "vq: warning:" in warning
        assert "unknown_top_level" in warning
        assert str(cfg_dir / "config.toml") in warning

    def test_extra_host_field_rejected(self, cfg_dir: Path) -> None:
        (cfg_dir / "config.toml").write_text(
            '[hosts.host_d]\n'
            'ssh = "x"\n'
            'mystery = "y"\n'
        )
        with pytest.raises(config.ConfigError, match="invalid config"):
            config.load_config()

    @pytest.mark.parametrize("value", ['"yes"', "1"])
    def test_multi_user_enabled_requires_a_boolean(
        self, cfg_dir: Path, value: str
    ) -> None:
        (cfg_dir / "config.toml").write_text(
            f"[multi_user]\nenabled = {value}\n"
        )
        with pytest.raises(config.ConfigError, match="invalid config"):
            config.load_config()

    def test_host_missing_required_ssh_field(self, cfg_dir: Path) -> None:
        (cfg_dir / "config.toml").write_text('[hosts.host_d]\nremote_vq = "vq"\n')
        with pytest.raises(config.ConfigError, match="invalid config"):
            config.load_config()


class TestHostLookup:
    def test_known_host_returns_config(self) -> None:
        c = config.Config(
            default_host="host_d",
            hosts={"host_d": config.HostConfig(ssh="host_d")},
        )
        assert c.host("host_d").ssh == "host_d"

    def test_admin_token_file_must_be_absolute(self) -> None:
        host = config.HostConfig(
            ssh="host_d",
            admin_token_file="/etc/vq/web-token",
        )
        assert host.admin_token_file == "/etc/vq/web-token"

        with pytest.raises(
            ValueError,
            match="admin_token_file must be an absolute path",
        ):
            config.HostConfig(
                ssh="host_d",
                admin_token_file=".config/vq/web-token",
            )

    def test_unknown_host_raises(self) -> None:
        c = config.Config()
        with pytest.raises(config.ConfigError, match="host 'host_d' not found"):
            c.host("host_d")


class TestBinaryProgramAvailability:
    def _write_executable(self, path: Path, text: str) -> None:
        path.write_text(text)
        path.chmod(0o755)

    def test_orca_binary_checks_mpi_startup(self, tmp_path: Path) -> None:
        orca = tmp_path / "orca"
        startup = tmp_path / "orca_startup_mpi"
        self._write_executable(orca, "#!/bin/sh\nexit 0\n")
        self._write_executable(
            startup,
            "#!/bin/sh\n"
            "echo 'Fatal Error (ORCA_StartUp): no input files' >&2\n"
            "exit 1\n",
        )

        ok, reason = config.BinaryProgram(kind="binary", binary=str(orca)).availability()

        assert ok is True
        assert "ORCA MPI startup loads" in reason

    def test_orca_binary_reports_serial_only_when_mpi_runtime_missing(
        self, tmp_path: Path
    ) -> None:
        orca = tmp_path / "orca"
        startup = tmp_path / "orca_startup_mpi"
        self._write_executable(orca, "#!/bin/sh\nexit 0\n")
        self._write_executable(
            startup,
            "#!/bin/sh\n"
            "echo 'dyld[123]: Library not loaded: libmpi.40.dylib' >&2\n"
            "exit 134\n",
        )

        ok, reason = config.BinaryProgram(kind="binary", binary=str(orca)).availability()

        assert ok is True
        assert "ORCA MPI startup cannot load runtime" in reason
        assert "libmpi.40.dylib" in reason
        assert "serial ORCA only" in reason

    def test_orca_binary_reports_serial_only_for_linux_mpi_loader_error(
        self, tmp_path: Path
    ) -> None:
        orca = tmp_path / "orca"
        startup = tmp_path / "orca_startup_mpi"
        self._write_executable(orca, "#!/bin/sh\nexit 0\n")
        self._write_executable(
            startup,
            "#!/bin/sh\n"
            "echo './orca_startup_mpi: error while loading shared libraries: "
            "libmpi.so.40: cannot open shared object file: "
            "No such file or directory' >&2\n"
            "exit 127\n",
        )

        ok, reason = config.BinaryProgram(kind="binary", binary=str(orca)).availability()

        assert ok is True
        assert "ORCA MPI startup cannot load runtime" in reason
        assert "libmpi.so.40" in reason
        assert "serial ORCA only" in reason


class TestResolveHost:
    def test_explicit_arg_wins(self) -> None:
        c = config.Config(default_host="host_d")
        assert c.resolve_host("localhost") == "localhost"

    def test_falls_back_to_default(self) -> None:
        c = config.Config(default_host="host_d")
        assert c.resolve_host(None) == "host_d"

    def test_no_arg_no_default_raises(self) -> None:
        c = config.Config()
        with pytest.raises(config.ConfigError, match="HOST is required"):
            c.resolve_host(None)

    def test_empty_string_treated_as_unset(self) -> None:
        c = config.Config(default_host="host_d")
        # '' is falsy, so it falls through to default_host
        assert c.resolve_host("") == "host_d"


class TestBranchRouting:
    """v0.5.6: [hosts.X.branches] / [hosts.X.branch_aliases] for routing
    `vq submit --branch NAME` to specific Python interpreters."""

    def test_branches_table_parses(self, cfg_dir: Path) -> None:
        (cfg_dir / "config.toml").write_text(
            '[hosts.compute]\n'
            'ssh = "compute"\n'
            '\n'
            '[hosts.compute.branches]\n'
            'main = "/home/user/vibeqc-dev/.venv/bin/python"\n'
            'release = "/home/user/vibeqc-release/.venv/bin/python"\n'
        )
        c = config.load_config()
        h = c.hosts["compute"]
        assert h.branches == {
            "main": "/home/user/vibeqc-dev/.venv/bin/python",
            "release": "/home/user/vibeqc-release/.venv/bin/python",
        }
        assert h.branch_aliases == {}

    def test_branch_aliases_table_parses(self, cfg_dir: Path) -> None:
        (cfg_dir / "config.toml").write_text(
            '[hosts.host_d]\n'
            'ssh = "host_d"\n'
            '\n'
            '[hosts.host_d.branches]\n'
            'main = "/dev/python"\n'
            'release = "/release/python"\n'
            '\n'
            '[hosts.host_d.branch_aliases]\n'
            'dev = "main"\n'
            'development = "main"\n'
            'latest = "release"\n'
        )
        c = config.load_config()
        h = c.hosts["host_d"]
        assert h.branch_aliases == {
            "dev": "main",
            "development": "main",
            "latest": "release",
        }

    def test_resolve_branch_canonical(self) -> None:
        h = config.HostConfig(
            ssh="host_d",
            branches={"main": "/dev/py", "release": "/rel/py"},
        )
        assert h.resolve_branch("main") == "/dev/py"
        assert h.resolve_branch("release") == "/rel/py"

    def test_resolve_branch_alias(self) -> None:
        h = config.HostConfig(
            ssh="host_d",
            branches={"main": "/dev/py", "release": "/rel/py"},
            branch_aliases={"dev": "main", "development": "main", "latest": "release"},
        )
        assert h.resolve_branch("dev") == "/dev/py"
        assert h.resolve_branch("development") == "/dev/py"
        assert h.resolve_branch("latest") == "/rel/py"

    def test_resolve_branch_unknown_returns_none(self) -> None:
        h = config.HostConfig(
            ssh="host_d",
            branches={"main": "/dev/py"},
            branch_aliases={"dev": "main"},
        )
        assert h.resolve_branch("nonsense") is None

    def test_resolve_branch_with_no_config(self) -> None:
        """Default empty branches table -> resolve always returns None."""
        h = config.HostConfig(ssh="host_d")
        assert h.resolve_branch("main") is None

    def test_known_branch_names_combines_canonical_and_aliases(self) -> None:
        h = config.HostConfig(
            ssh="host_d",
            branches={"main": "/dev/py", "release": "/rel/py"},
            branch_aliases={"latest": "release", "dev": "main"},
        )
        assert h.known_branch_names() == ["dev", "latest", "main", "release"]

    def test_alias_target_must_exist_in_branches(self, cfg_dir: Path) -> None:
        """An alias pointing at a non-existent branch fails at load time,
        not at runtime when someone tries to use it."""
        (cfg_dir / "config.toml").write_text(
            '[hosts.host_d]\n'
            'ssh = "host_d"\n'
            '\n'
            '[hosts.host_d.branches]\n'
            'main = "/dev/py"\n'
            '\n'
            '[hosts.host_d.branch_aliases]\n'
            'latest = "release"\n'   # release doesn't exist
        )
        with pytest.raises(config.ConfigError, match="not found in branches"):
            config.load_config()

    def test_alias_cannot_collide_with_branch_name(self, cfg_dir: Path) -> None:
        """An alias with the same name as a canonical branch would be
        silently shadowed -- error at load time."""
        (cfg_dir / "config.toml").write_text(
            '[hosts.host_d]\n'
            'ssh = "host_d"\n'
            '\n'
            '[hosts.host_d.branches]\n'
            'main = "/dev/py"\n'
            'release = "/rel/py"\n'
            '\n'
            '[hosts.host_d.branch_aliases]\n'
            'main = "release"\n'   # collides with canonical "main"
        )
        with pytest.raises(config.ConfigError, match="collides with branches"):
            config.load_config()


class TestNotificationConfig:
    """v0.5.35: ``[notifications]`` section in config.toml drives the
    daemon's webhook notifications."""

    def test_absent_section_defaults_to_disabled(self, cfg_dir: Path) -> None:
        """No ``[notifications]`` in config → notifications disabled
        (the daemon's notification calls become no-ops). This is the
        critical "no surprise POSTs from upgrade" guarantee for users
        who never opt in."""
        (cfg_dir / "config.toml").write_text("")
        c = config.load_config()
        assert c.notifications.webhook_url is None

    def test_section_with_webhook_url_parsed(self, cfg_dir: Path) -> None:
        (cfg_dir / "config.toml").write_text(
            '[notifications]\n'
            'webhook_url = "https://hooks.slack.com/services/T/B/X"\n'
        )
        c = config.load_config()
        assert c.notifications.webhook_url == (
            "https://hooks.slack.com/services/T/B/X"
        )

    def test_extra_notification_field_rejected(self, cfg_dir: Path) -> None:
        """Forbid extra fields so a typo (`webhook_urls = ...`) fails
        loudly at load time instead of silently leaving notifications
        disabled."""
        (cfg_dir / "config.toml").write_text(
            '[notifications]\n'
            'webhook_url = "https://x"\n'
            'unknown_extra = "oops"\n'
        )
        with pytest.raises(config.ConfigError, match="invalid config"):
            config.load_config()

    def test_pre_v0_5_35_config_still_loads(self, cfg_dir: Path) -> None:
        """A config from before v0.5.35 (with hosts + default_host but
        no notifications section) must continue to load cleanly.
        Additive field with a default — guaranteed clean upgrade."""
        (cfg_dir / "config.toml").write_text(
            'default_host = "host_d"\n'
            '\n'
            '[hosts.host_d]\n'
            'ssh = "host_d"\n'
        )
        c = config.load_config()
        assert c.default_host == "host_d"
        assert c.notifications.webhook_url is None


class TestProvidesBranches:
    """v0.5.47: VenvProgram.provides_branches optional config field."""

    def test_defaults_to_none_when_unset(self, cfg_dir: Path) -> None:
        (cfg_dir / "config.toml").write_text(
            '[programs.vibeqc-dev]\n'
            'kind = "venv"\n'
            'python = "/x/python"\n'
            'git_dir = "/x"\n'
        )
        c = config.load_config()
        prog = c.programs["vibeqc-dev"]
        assert isinstance(prog, config.VenvProgram)
        assert prog.provides_branches is None

    def test_parses_list_of_strings(self, cfg_dir: Path) -> None:
        (cfg_dir / "config.toml").write_text(
            '[programs.vibeqc-dev]\n'
            'kind = "venv"\n'
            'python = "/x/python"\n'
            'git_dir = "/x"\n'
            'provides_branches = ["main", "dev", "development"]\n'
        )
        c = config.load_config()
        prog = c.programs["vibeqc-dev"]
        assert isinstance(prog, config.VenvProgram)
        assert prog.provides_branches == ["main", "dev", "development"]

    def test_empty_list_explicit(self, cfg_dir: Path) -> None:
        """Empty list is treated identically to None at the admin-update
        level (bool([]) is False, so the surgical-pause guard falls
        back to pause_all). Here we just ensure the empty list parses
        without complaint."""
        (cfg_dir / "config.toml").write_text(
            '[programs.vibeqc-dev]\n'
            'kind = "venv"\n'
            'python = "/x/python"\n'
            'git_dir = "/x"\n'
            'provides_branches = []\n'
        )
        c = config.load_config()
        prog = c.programs["vibeqc-dev"]
        assert isinstance(prog, config.VenvProgram)
        assert prog.provides_branches == []


class TestSchedulerConfig:
    """v1.0 external-scheduler backend fields on HostConfig (PBS/SGE).

    Generic placeholders only — the real host_f queue/account/host live in the
    maintainer's local provisioning notes, never in the repo.
    """

    def test_local_is_the_default_and_carries_no_scheduler_settings(self) -> None:
        h = config.HostConfig(ssh="h")
        assert h.scheduler == "local"
        assert h.scheduler_dialect is None
        assert h.remote_scheduler_host is None
        assert h.submit_extra == []

    def test_pbs_requires_dialect(self) -> None:
        with pytest.raises(ValidationError, match="requires scheduler_dialect"):
            config.HostConfig(ssh="h", scheduler="pbs")

    def test_pbs_requires_scratch_root(self) -> None:
        with pytest.raises(ValidationError, match="requires scratch_root"):
            config.HostConfig(ssh="h", scheduler="pbs", scheduler_dialect="torque")

    def test_pbs_valid_defaults_remote_scheduler_host_to_ssh(self) -> None:
        h = config.HostConfig(
            ssh="h",
            scheduler="pbs",
            scheduler_dialect="torque",
            scratch_root="/home/USER",
            scheduler_driver="driver",
        )
        # remote_scheduler_host defaults to the ssh alias (common single-host case).
        assert h.remote_scheduler_host == "h"
        assert h.scheduler_driver == "driver"

    def test_pbs_requires_scheduler_driver(self) -> None:
        with pytest.raises(ValidationError, match="requires scheduler_driver"):
            config.HostConfig(
                ssh="h",
                scheduler="pbs",
                scheduler_dialect="torque",
                scratch_root="/home/USER",
            )

    def test_explicit_remote_scheduler_host_is_kept(self) -> None:
        h = config.HostConfig(
            ssh="h",
            scheduler="pbs",
            scheduler_dialect="torque",
            scratch_root="/home/USER",
            scheduler_driver="driver",
            remote_scheduler_host="login-node",
        )
        assert h.remote_scheduler_host == "login-node"

    def test_local_host_forbids_a_dialect(self) -> None:
        with pytest.raises(ValidationError, match="must be unset when scheduler = 'local'"):
            config.HostConfig(ssh="h", scheduler_dialect="torque")

    def test_scheduler_host_accepts_mem_directive_omit(self) -> None:
        h = config.HostConfig(
            ssh="h",
            scheduler="pbs",
            scheduler_dialect="torque",
            scratch_root="/home/USER",
            scheduler_driver="driver",
            scheduler_mem_directive="omit",
        )
        assert h.scheduler_mem_directive == "omit"

    def test_scheduler_mem_directive_defaults_to_request(self) -> None:
        h = config.HostConfig(
            ssh="h",
            scheduler="pbs",
            scheduler_dialect="torque",
            scratch_root="/home/USER",
            scheduler_driver="driver",
        )
        assert h.scheduler_mem_directive == "request"

    def test_scheduler_gnu_time_command_accepts_absolute_path(self) -> None:
        h = config.HostConfig(
            ssh="h",
            scheduler="pbs",
            scheduler_dialect="torque",
            scratch_root="/home/USER",
            scheduler_driver="driver",
            scheduler_gnu_time_command="/home/USER/.local/bin/gnu-time",
        )
        assert h.scheduler_gnu_time_command == "/home/USER/.local/bin/gnu-time"

    def test_scheduler_gnu_time_command_rejects_relative_path(self) -> None:
        with pytest.raises(
            ValidationError,
            match="scheduler_gnu_time_command must be an absolute path",
        ):
            config.HostConfig(
                ssh="h",
                scheduler="pbs",
                scheduler_dialect="torque",
                scratch_root="/home/USER",
                scheduler_driver="driver",
                scheduler_gnu_time_command="bin/time",
            )

    @pytest.mark.parametrize(
        "value",
        ["/usr/bin/time\nmodule load time", "/usr/bin/time\x00--version"],
        ids=["newline", "nul"],
    )
    def test_scheduler_gnu_time_command_rejects_non_path_bytes(
        self, value: str
    ) -> None:
        with pytest.raises(
            ValidationError,
            match="scheduler_gnu_time_command must be a single absolute path",
        ):
            config.HostConfig(
                ssh="h",
                scheduler="pbs",
                scheduler_dialect="torque",
                scratch_root="/home/USER",
                scheduler_driver="driver",
                scheduler_gnu_time_command=value,
            )

    def test_local_host_forbids_custom_scheduler_gnu_time_command(self) -> None:
        with pytest.raises(
            ValidationError,
            match="scheduler_gnu_time_command must be left at '/usr/bin/time'",
        ):
            config.HostConfig(
                ssh="h",
                scheduler_gnu_time_command="/home/USER/.local/bin/gnu-time",
            )

    def test_scheduler_max_wall_time_accepts_strict_positive_integer(self) -> None:
        h = config.HostConfig(
            ssh="cluster",
            scheduler="slurm",
            scheduler_dialect="slurm",
            scratch_root="/workspace/USER",
            scheduler_driver="driver",
            scheduler_max_wall_time_seconds=28_800,
        )
        assert h.scheduler_max_wall_time_seconds == 28_800

    @pytest.mark.parametrize("value", [0, -1, True, 28_800.0, "28800"])
    def test_scheduler_max_wall_time_rejects_non_strict_positive_integer(
        self, value: object
    ) -> None:
        with pytest.raises(ValidationError, match="scheduler_max_wall_time_seconds"):
            config.HostConfig(
                ssh="cluster",
                scheduler="slurm",
                scheduler_dialect="slurm",
                scratch_root="/workspace/USER",
                scheduler_driver="driver",
                scheduler_max_wall_time_seconds=value,
            )

    def test_local_host_forbids_scheduler_max_wall_time(self) -> None:
        with pytest.raises(
            ValidationError,
            match="scheduler_max_wall_time_seconds must be unset",
        ):
            config.HostConfig(
                ssh="local",
                scheduler_max_wall_time_seconds=28_800,
            )

    def test_scheduler_aliases_may_have_independent_wall_time_limits(
        self, cfg_dir: Path
    ) -> None:
        (cfg_dir / "config.toml").write_text(
            "[hosts.driver]\n"
            'ssh = "driver"\n'
            "\n"
            "[hosts.short]\n"
            'ssh = "cluster-login"\n'
            'scheduler = "slurm"\n'
            'scheduler_dialect = "slurm"\n'
            'scratch_root = "/workspace/USER"\n'
            'scheduler_driver = "driver"\n'
            'scheduler_max_wall_time_seconds = 28800\n'
            "\n"
            "[hosts.long]\n"
            'ssh = "cluster-login"\n'
            'scheduler = "slurm"\n'
            'scheduler_dialect = "slurm"\n'
            'scratch_root = "/workspace/USER"\n'
            'scheduler_driver = "driver"\n'
            'scheduler_max_wall_time_seconds = 86400\n',
            encoding="utf-8",
        )

        loaded = config.load_config()

        assert loaded.host("short").scheduler_max_wall_time_seconds == 28_800
        assert loaded.host("long").scheduler_max_wall_time_seconds == 86_400

    def test_local_host_forbids_mem_directive_omit(self) -> None:
        with pytest.raises(
            ValidationError, match="must be left at 'request' when scheduler = 'local'"
        ):
            config.HostConfig(ssh="h", scheduler_mem_directive="omit")

    def test_slurm_valid_defaults_remote_scheduler_host_to_ssh(self) -> None:
        h = config.HostConfig(
            ssh="host_c",
            scheduler="slurm",
            scheduler_dialect="slurm",
            scratch_root="/workspace/USER",
            submit_extra=["--account", "<group-account>", "--partition", "debug"],
            node_scratch_dir="/tmp/$USER",
            scheduler_driver="driver",
        )
        assert h.remote_scheduler_host == "host_c"
        assert h.scheduler_dialect == "slurm"
        assert h.submit_extra == [
            "--account",
            "<group-account>",
            "--partition",
            "debug",
        ]

    def test_slurm_requires_slurm_dialect(self) -> None:
        with pytest.raises(ValidationError, match="requires scheduler_dialect = 'slurm'"):
            config.HostConfig(
                ssh="host_c",
                scheduler="slurm",
                scheduler_dialect="torque",
                scratch_root="/workspace/USER",
                scheduler_driver="driver",
            )

    def test_pbs_rejects_slurm_dialect(self) -> None:
        with pytest.raises(ValidationError, match="cannot use scheduler_dialect"):
            config.HostConfig(
                ssh="h",
                scheduler="pbs",
                scheduler_dialect="slurm",
                scratch_root="/home/USER",
                scheduler_driver="driver",
            )

    def test_malformed_submit_extra_fails_fast(self) -> None:
        # A dangling -q (no value) is a site-config typo caught at load time,
        # not a cryptic qsub rejection at first submit.
        with pytest.raises(ValidationError, match="missing its value"):
            config.HostConfig(
                ssh="h",
                scheduler="pbs",
                scheduler_dialect="torque",
                scratch_root="/home/USER",
                submit_extra=["-q"],
            )

    def test_scheduler_host_loads_from_toml(self, cfg_dir: Path) -> None:
        # The full operational shape (placeholders for the real host_f values)
        # round-trips through load_config — proving the dispatcher config and
        # the operational config are one schema.
        (cfg_dir / "config.toml").write_text(
            "[hosts.cluster]\n"
            'ssh = "cluster"\n'
            'scheduler = "pbs"\n'
            'scheduler_dialect = "torque"\n'
            'remote_scheduler_host = "cluster"\n'
            'submit_extra = ["-q", "compute", "-A", "proj1"]\n'
            'scratch_root = "/home/USER"\n'
            'node_scratch_dir = "/tmp1/$USER"\n'
            'scheduler_gnu_time_command = "/home/USER/.local/bin/gnu-time"\n'
            'scheduler_prologue = ["module purge", "source /home/USER/cluster-env.sh"]\n'
            'scheduler_epilogue = ["rm -f scratch.tmp"]\n'
            'scheduler_driver = "driver"\n'
            'scheduler_update_command = "/home/USER/vibeqc-dev/scripts/update_cluster.sh"\n'
            'scheduler_update_host = "cluster-build"\n'
            'scheduler_update_stage = "/shared/vq-admin/cluster"\n'
            'scheduler_install_command = "/home/USER/vibeqc-dev/scripts/install_cluster.sh"\n'
            'scheduler_update_timeout_seconds = 3600\n'
            "\n"
            "[hosts.cluster.scheduler_program_hooks.orca]\n"
            'prologue = ["source /home/USER/orca-env.sh"]\n'
            'epilogue = ["cp -f orca.out artifacts/ 2>/dev/null || true"]\n'
            'command_wrapper = ["/home/USER/bin/orcasub", "--scheduler"]\n'
            "\n"
            "[hosts.cluster.scheduler_runtime_deployments.vibeqc-release]\n"
            'update_command = "/site/bin/deploy-vibeqc"\n'
            'install_command = "/site/bin/install-vibeqc"\n'
            'update_host = "cluster-build"\n'
            'verify_command = "/site/bin/verify-vibeqc"\n'
            "timeout_seconds = 5400\n"
            "\n"
            "[hosts.cluster.branches]\n"
            'main = "/home/USER/vibeqc-dev/.venv/bin/python"\n'
            'release = "/home/USER/vibeqc-release/.venv/bin/python"\n'
        )
        c = config.load_config()
        h = c.hosts["cluster"]
        assert h.scheduler == "pbs"
        assert h.scheduler_dialect == "torque"
        assert h.submit_extra == ["-q", "compute", "-A", "proj1"]
        assert h.scratch_root == "/home/USER"
        assert h.node_scratch_dir == "/tmp1/$USER"
        assert h.scheduler_gnu_time_command == "/home/USER/.local/bin/gnu-time"
        assert h.scheduler_prologue == [
            "module purge",
            "source /home/USER/cluster-env.sh",
        ]
        assert h.scheduler_epilogue == ["rm -f scratch.tmp"]
        assert h.scheduler_program_hooks["orca"].prologue == [
            "source /home/USER/orca-env.sh"
        ]
        assert h.scheduler_program_hooks["orca"].epilogue == [
            "cp -f orca.out artifacts/ 2>/dev/null || true"
        ]
        assert h.scheduler_program_hooks["orca"].command_wrapper == [
            "/home/USER/bin/orcasub",
            "--scheduler",
        ]
        assert h.scheduler_driver == "driver"
        assert h.scheduler_update_command == (
            "/home/USER/vibeqc-dev/scripts/update_cluster.sh"
        )
        assert h.scheduler_update_host == "cluster-build"
        assert h.scheduler_update_stage == "/shared/vq-admin/cluster"
        assert h.scheduler_install_command == (
            "/home/USER/vibeqc-dev/scripts/install_cluster.sh"
        )
        assert h.scheduler_update_timeout_seconds == 3600
        runtime = h.scheduler_runtime_deployments["vibeqc-release"]
        assert runtime.update_command == "/site/bin/deploy-vibeqc"
        assert runtime.install_command == "/site/bin/install-vibeqc"
        assert runtime.update_host == "cluster-build"
        assert runtime.verify_command == "/site/bin/verify-vibeqc"
        assert runtime.timeout_seconds == 5400
        assert h.resolve_branch("main") == "/home/USER/vibeqc-dev/.venv/bin/python"

    def test_local_host_rejects_scheduler_update_command(self) -> None:
        with pytest.raises(
            ValidationError, match="scheduler_update_command must be unset"
        ):
            config.HostConfig(
                ssh="h",
                scheduler_update_command="/home/USER/vibeqc-dev/scripts/update_cluster.sh",
            )

    def test_local_host_rejects_scheduler_update_host(self) -> None:
        with pytest.raises(
            ValidationError, match="scheduler_update_host must be unset"
        ):
            config.HostConfig(ssh="h", scheduler_update_host="cluster-build")

    def test_local_host_rejects_scheduler_update_stage(self) -> None:
        with pytest.raises(
            ValidationError, match="scheduler_update_stage must be unset"
        ):
            config.HostConfig(ssh="h", scheduler_update_stage="/shared/vq-admin")

    def test_scheduler_update_stage_must_be_absolute(self) -> None:
        with pytest.raises(
            ValidationError, match="scheduler_update_stage must be an absolute path"
        ):
            config.HostConfig(
                ssh="cluster",
                scheduler="slurm",
                scheduler_dialect="slurm",
                scheduler_driver="driver",
                scratch_root="/shared/jobs",
                scheduler_update_stage="relative/stage",
            )

    @pytest.mark.parametrize(
        ("field", "value", "message"),
        [
            ("submit_extra", ["-q", "compute"], "submit_extra must be unset"),
            ("remote_scheduler_host", "cluster", "remote_scheduler_host must be unset"),
            ("scratch_root", "/home/USER", "scratch_root must be unset"),
            ("node_scratch_dir", "/tmp1/$USER", "node_scratch_dir must be unset"),
            ("scheduler_driver", "driver", "scheduler_driver must be unset"),
        ],
    )
    def test_local_host_rejects_scheduler_only_fields(
        self, field: str, value: object, message: str
    ) -> None:
        with pytest.raises(ValidationError, match=message):
            config.HostConfig(ssh="h", **{field: value})

    def test_local_host_rejects_scheduler_prologue(self) -> None:
        with pytest.raises(
            ValidationError, match="scheduler_prologue must be unset"
        ):
            config.HostConfig(ssh="h", scheduler_prologue=["module purge"])

    def test_local_host_rejects_scheduler_epilogue(self) -> None:
        with pytest.raises(
            ValidationError, match="scheduler_epilogue must be unset"
        ):
            config.HostConfig(ssh="h", scheduler_epilogue=["rm -f scratch.tmp"])

    def test_local_host_rejects_scheduler_program_hooks(self) -> None:
        with pytest.raises(
            ValidationError, match="scheduler_program_hooks must be unset"
        ):
            config.HostConfig(
                ssh="h",
                scheduler_program_hooks={
                    "orca": config.SchedulerProgramHooks(
                        prologue=["source /home/USER/orca-env.sh"]
                    )
                },
            )

    def test_local_host_rejects_scheduler_runtime_deployments(self) -> None:
        with pytest.raises(
            ValidationError, match="scheduler_runtime_deployments must be unset"
        ):
            config.HostConfig(
                ssh="h",
                scheduler_runtime_deployments={
                    "vibeqc-release": config.SchedulerRuntimeDeployment(
                        update_command="/site/bin/deploy",
                        verify_command="/site/bin/verify",
                    )
                },
            )

    def test_scheduler_hooks_must_be_single_lines(self) -> None:
        with pytest.raises(ValidationError, match="single shell lines"):
            config.HostConfig(
                ssh="h",
                scheduler="pbs",
                scheduler_dialect="torque",
                scratch_root="/home/USER",
                scheduler_driver="driver",
                scheduler_prologue=["module purge\nmodule load python"],
            )

    def test_scheduler_program_hook_names_must_be_safe(self) -> None:
        with pytest.raises(ValidationError, match="scheduler_program_hooks keys"):
            config.HostConfig(
                ssh="h",
                scheduler="pbs",
                scheduler_dialect="torque",
                scratch_root="/home/USER",
                scheduler_driver="driver",
                scheduler_program_hooks={
                    "orca;rm": config.SchedulerProgramHooks(prologue=["true"])
                },
            )

    def test_scheduler_program_hooks_must_be_single_lines(self) -> None:
        with pytest.raises(ValidationError, match="single shell lines"):
            config.HostConfig(
                ssh="h",
                scheduler="pbs",
                scheduler_dialect="torque",
                scratch_root="/home/USER",
                scheduler_driver="driver",
                scheduler_program_hooks={
                    "orca": config.SchedulerProgramHooks(
                        epilogue=["cp out artifacts/\nrm out"]
                    )
                },
            )

    def test_scheduler_program_command_wrapper_must_be_single_argv_entries(
        self,
    ) -> None:
        with pytest.raises(ValidationError, match="single argv tokens"):
            config.HostConfig(
                ssh="h",
                scheduler="pbs",
                scheduler_dialect="torque",
                scratch_root="/home/USER",
                scheduler_driver="driver",
                scheduler_program_hooks={
                    "orca": config.SchedulerProgramHooks(
                        command_wrapper=["/home/USER/bin/orca\nsub"]
                    )
                },
            )

    def test_scheduler_program_command_wrapper_must_not_be_blank(self) -> None:
        with pytest.raises(ValidationError, match="non-empty"):
            config.HostConfig(
                ssh="h",
                scheduler="pbs",
                scheduler_dialect="torque",
                scratch_root="/home/USER",
                scheduler_driver="driver",
                scheduler_program_hooks={
                    "orca": config.SchedulerProgramHooks(command_wrapper=[""])
                },
            )

    def test_scheduler_update_host_must_not_be_blank(self) -> None:
        with pytest.raises(
            ValidationError, match="scheduler_update_host must be non-empty"
        ):
            config.HostConfig(
                ssh="h",
                scheduler="pbs",
                scheduler_dialect="torque",
                scratch_root="/home/USER",
                scheduler_driver="driver",
                scheduler_update_host="  ",
            )

    def test_scheduler_scratch_root_must_be_absolute(self) -> None:
        with pytest.raises(ValidationError, match="scratch_root must be an absolute"):
            config.HostConfig(
                ssh="h",
                scheduler="pbs",
                scheduler_dialect="torque",
                scratch_root="relative/scratch",
                scheduler_driver="driver",
            )

    def test_scheduler_path_fields_must_be_single_lines(self) -> None:
        with pytest.raises(ValidationError, match="node_scratch_dir must be a single"):
            config.HostConfig(
                ssh="h",
                scheduler="pbs",
                scheduler_dialect="torque",
                scratch_root="/home/USER",
                node_scratch_dir="/tmp1/$USER\n/tmp2/$USER",
                scheduler_driver="driver",
            )

    def test_scheduler_driver_must_not_be_blank(self) -> None:
        with pytest.raises(ValidationError, match="scheduler_driver must be non-empty"):
            config.HostConfig(
                ssh="h",
                scheduler="pbs",
                scheduler_dialect="torque",
                scratch_root="/home/USER",
                scheduler_driver=" ",
            )

    def test_scheduler_update_timeout_must_be_positive_even_on_local_host(
        self,
    ) -> None:
        with pytest.raises(
            ValidationError,
            match="scheduler_update_timeout_seconds must be finite and positive",
        ):
            config.HostConfig(ssh="h", scheduler_update_timeout_seconds=0)

    def test_scheduler_update_timeout_must_be_positive(self) -> None:
        with pytest.raises(
            ValidationError,
            match="scheduler_update_timeout_seconds must be finite and positive",
        ):
            config.HostConfig(
                ssh="h",
                scheduler="pbs",
                scheduler_dialect="torque",
                scratch_root="/home/USER",
                scheduler_driver="driver",
                scheduler_update_timeout_seconds=0,
            )

    @pytest.mark.parametrize(
        "field",
        [
            "timeout_seconds",
            "verify_timeout_seconds",
            "prepare_timeout_seconds",
        ],
    )
    @pytest.mark.parametrize(
        "value",
        [float("nan"), float("inf"), float("-inf")],
        ids=["nan", "positive-infinity", "negative-infinity"],
    )
    def test_scheduler_runtime_timeout_must_be_finite(
        self, field: str, value: float
    ) -> None:
        with pytest.raises(
            ValidationError,
            match=rf"{field} must be finite and positive",
        ):
            config.SchedulerRuntimeDeployment(
                update_command="/site/bin/deploy",
                verify_command="/site/bin/verify",
                **{field: value},
            )

    @pytest.mark.parametrize(
        "field",
        [
            "timeout_seconds",
            "verify_timeout_seconds",
            "prepare_timeout_seconds",
        ],
    )
    @pytest.mark.parametrize("value", [0.25, None], ids=["positive", "none"])
    def test_scheduler_runtime_timeout_accepts_supported_values(
        self, field: str, value: float | None
    ) -> None:
        deployment = config.SchedulerRuntimeDeployment(
            update_command="/site/bin/deploy",
            verify_command="/site/bin/verify",
            **{field: value},
        )

        assert getattr(deployment, field) == value

    @pytest.mark.parametrize(
        "value",
        [float("nan"), float("inf"), float("-inf")],
        ids=["nan", "positive-infinity", "negative-infinity"],
    )
    def test_scheduler_update_timeout_must_be_finite(self, value: float) -> None:
        with pytest.raises(
            ValidationError,
            match="scheduler_update_timeout_seconds must be finite and positive",
        ):
            config.HostConfig(
                ssh="host",
                scheduler_update_timeout_seconds=value,
            )

    @pytest.mark.parametrize("value", [0.25, None], ids=["positive", "none"])
    def test_scheduler_update_timeout_accepts_supported_values(
        self, value: float | None
    ) -> None:
        host = config.HostConfig(
            ssh="host",
            scheduler_update_timeout_seconds=value,
        )

        assert host.scheduler_update_timeout_seconds == value


class TestDaemonRunConfig:
    """v0.16.0: [daemon] — config-file defaults for `vq daemon run` caps."""

    def test_absent_section_all_none(self, cfg_dir: Path) -> None:
        (cfg_dir / "config.toml").write_text('default_host = "x"\n')
        c = config.load_config()
        assert c.daemon.max_cpus is None
        assert c.daemon.max_jobs is None
        assert c.daemon.max_scheduler_jobs is None
        assert c.daemon.max_mem_mb is None
        assert c.daemon.default_job_mem_mb is None

    def test_daemon_section_parses(self, cfg_dir: Path) -> None:
        (cfg_dir / "config.toml").write_text(
            "[daemon]\n"
            "max_cpus = 8\n"
            "max_jobs = 2\n"
            "max_scheduler_jobs = 4\n"
            "max_mem_mb = 51218\n"
            "default_job_mem_mb = 4000\n"
        )
        c = config.load_config()
        assert c.daemon.max_cpus == 8
        assert c.daemon.max_jobs == 2
        assert c.daemon.max_scheduler_jobs == 4
        assert c.daemon.max_mem_mb == 51218
        assert c.daemon.default_job_mem_mb == 4000

    def test_daemon_caps_must_be_positive(self) -> None:
        with pytest.raises(ValidationError):
            config.DaemonRunConfig(max_mem_mb=0)
        with pytest.raises(ValidationError):
            config.DaemonRunConfig(default_job_mem_mb=-1)

    def test_daemon_section_rejects_unknown_keys(self, cfg_dir: Path) -> None:
        (cfg_dir / "config.toml").write_text("[daemon]\nmax_memory = 1000\n")
        with pytest.raises(config.ConfigError):
            config.load_config()


class TestFleetRolloutTopology:
    def test_old_host_config_defaults_to_auto(self) -> None:
        host = config.HostConfig(ssh="box")
        assert host.fleet_role == "auto"
        assert host.fleet_canonical_host is None

    def test_alias_requires_canonical_host(self) -> None:
        with pytest.raises(
            ValidationError,
            match="fleet_role = 'alias' requires fleet_canonical_host",
        ):
            config.HostConfig(ssh="cluster", fleet_role="alias")

    def test_non_alias_rejects_canonical_host(self) -> None:
        with pytest.raises(
            ValidationError,
            match="only valid when fleet_role = 'alias'",
        ):
            config.HostConfig(
                ssh="box",
                fleet_role="managed",
                fleet_canonical_host="other",
            )

    def test_alias_target_must_exist_and_be_canonical(self) -> None:
        alias = config.HostConfig(
            ssh="cluster",
            fleet_role="alias",
            fleet_canonical_host="missing",
        )
        with pytest.raises(
            ValidationError,
            match="unknown fleet_canonical_host 'missing'",
        ):
            config.Config(hosts={"campaign": alias})

        canonical_alias = config.HostConfig(
            ssh="cluster",
            fleet_role="alias",
            fleet_canonical_host="other",
        )
        other_alias = config.HostConfig(
            ssh="cluster",
            fleet_role="alias",
            fleet_canonical_host="canonical",
        )
        managed = config.HostConfig(ssh="cluster", fleet_role="managed")
        with pytest.raises(
            ValidationError,
            match="aliases must point to a canonical managed/auto host",
        ):
            config.Config(
                hosts={
                    "campaign": canonical_alias,
                    "other": other_alias,
                    "canonical": managed,
                }
            )

    def test_rollout_order_is_known_and_unique(self) -> None:
        host = config.HostConfig(ssh="box", fleet_role="managed")
        with pytest.raises(
            ValidationError,
            match="fleet_rollout_order contains duplicate",
        ):
            config.Config(
                hosts={"box": host},
                fleet_rollout_order=["box", "box"],
            )
        with pytest.raises(
            ValidationError,
            match="fleet_rollout_order references unknown",
        ):
            config.Config(
                hosts={"box": host},
                fleet_rollout_order=["missing"],
            )

    def test_explicit_topology_parses_from_toml(self, cfg_dir: Path) -> None:
        (cfg_dir / "config.toml").write_text(
            'fleet_rollout_order = ["host_f", "host_d", "coordinator"]\n'
            "\n"
            "[hosts.host_f]\n"
            'ssh = "host_f"\n'
            'fleet_role = "managed"\n'
            'scheduler = "pbs"\n'
            'scheduler_dialect = "torque"\n'
            'scratch_root = "/home/USER"\n'
            'scheduler_driver = "host_d"\n'
            "\n"
            "[hosts.host_f-campaign]\n"
            'ssh = "host_f"\n'
            'fleet_role = "alias"\n'
            'fleet_canonical_host = "host_f"\n'
            'scheduler = "pbs"\n'
            'scheduler_dialect = "torque"\n'
            'scratch_root = "/home/USER"\n'
            'scheduler_driver = "host_d"\n'
            "\n"
            "[hosts.host_d]\n"
            'ssh = "host_d"\n'
            'fleet_role = "managed"\n'
            "\n"
            "[hosts.coordinator]\n"
            'ssh = "coordinator"\n'
            'fleet_role = "vq-only"\n'
        )
        cfg = config.load_config()
        assert cfg.fleet_rollout_order == ["host_f", "host_d", "coordinator"]
        assert cfg.hosts["host_f-campaign"].fleet_canonical_host == "host_f"
        assert cfg.hosts["coordinator"].fleet_role == "vq-only"


@pytest.mark.parametrize("system", [False, True])
def test_repeated_policy_reads_parse_unchanged_bytes_once(
    cfg_dir: Path, monkeypatch: pytest.MonkeyPatch, system: bool,
) -> None:
    """A retained queue must not parse the same fleet TOML per job (#547)."""
    path = cfg_dir / "config.toml"
    path.write_text('default_host = "parse-once-policy"\n')
    if system:
        monkeypatch.setattr(config, "SYSTEM_CONFIG_PATH", path)
    loader = config.load_system_config if system else config.load_config
    original = config.tomllib.loads
    original_load = config.tomllib.load
    parsed = []

    def counting_loads(source, **kwargs):
        parsed.append(source)
        return original(source, **kwargs)

    def counting_load(stream, **kwargs):
        parsed.append("stream")
        return original_load(stream, **kwargs)

    monkeypatch.setattr(config.tomllib, "load", counting_load)
    monkeypatch.setattr(config.tomllib, "loads", counting_loads)
    for _ in range(100):
        assert loader().default_host == "parse-once-policy"
    assert len(parsed) <= 1


@pytest.mark.parametrize("system", [False, True])
def test_policy_revocation_ignores_preserved_file_metadata(
    cfg_dir: Path, monkeypatch: pytest.MonkeyPatch, system: bool,
) -> None:
    path = cfg_dir / "config.toml"
    path.write_text('[multi_user]\nenabled = true\nadmin_group = "group-a"\n')
    if system:
        monkeypatch.setattr(config, "SYSTEM_CONFIG_PATH", path)
    loader = config.load_system_config if system else config.load_config
    assert loader().multi_user.admin_group == "group-a"
    before = path.stat()
    path.write_text('[multi_user]\nenabled = true\nadmin_group = "group-b"\n')
    os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
    assert path.stat().st_size == before.st_size
    assert loader().multi_user.admin_group == "group-b"


@pytest.mark.parametrize("system", [False, True])
def test_loaded_config_does_not_share_mutable_policy_or_host_containers(
    cfg_dir: Path, monkeypatch: pytest.MonkeyPatch, system: bool,
) -> None:
    path = cfg_dir / "config.toml"
    path.write_text(
        '[multi_user]\nenabled = true\nadmin_group = "original-group"\n'
        '[hosts.cluster]\nssh = "cluster"\nscheduler = "slurm"\n'
        'scheduler_dialect = "slurm"\nscratch_root = "/scratch"\n'
        'scheduler_driver = "driver"\nsubmit_extra = ["original"]\n'
    )
    if system:
        monkeypatch.setattr(config, "SYSTEM_CONFIG_PATH", path)
    loader = config.load_system_config if system else config.load_config
    first = loader()
    first.multi_user.admin_group = "revoked-group"
    first.hosts["cluster"].submit_extra.append("changed")
    second = loader()
    assert second.multi_user.admin_group == "original-group"
    assert second.hosts["cluster"].submit_extra == ["original"]


@pytest.mark.parametrize("system", [False, True])
def test_previously_read_policy_does_not_hide_corruption_or_read_denial(
    cfg_dir: Path, monkeypatch: pytest.MonkeyPatch, system: bool,
) -> None:
    path = cfg_dir / "config.toml"
    path.write_text('[multi_user]\nenabled = true\n')
    if system:
        monkeypatch.setattr(config, "SYSTEM_CONFIG_PATH", path)
    loader = config.load_system_config if system else config.load_config
    assert loader().multi_user.enabled
    path.write_text("[malformed\n")
    with pytest.raises(config.ConfigError, match="failed to parse"):
        loader()
    path.write_text('[multi_user]\nenabled = true\n')
    original = Path.open

    def deny_policy(self, *args, **kwargs):
        if self == path:
            raise PermissionError("policy read revoked")
        return original(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", deny_policy)
    with pytest.raises((config.ConfigError, PermissionError), match="policy read revoked"):
        loader()


def test_system_mode_hint_reads_new_policy_and_never_returns_cached_failure(
    cfg_dir: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = cfg_dir / "system.toml"
    monkeypatch.setattr(config, "SYSTEM_CONFIG_PATH", path)
    path.write_text('[multi_user]\nenabled = true\n')
    assert config.system_multi_user_enabled()
    path.write_text('[multi_user]\nenabled = false\n')
    assert not config.system_multi_user_enabled()
    path.write_text("[broken\n")
    assert not config.system_multi_user_enabled()
    path.write_text('[multi_user]\nenabled = true\n')
    assert config.system_multi_user_enabled()
    path.unlink()
    assert not config.system_multi_user_enabled()


class TestConfigForwardCompatibility:
    """v0.26.1: an older vq degrades on a newer config instead of vanishing.

    Every case here is the 2026-09-08 split migration replayed: the schema
    ``/3`` work added ``pin_source_repos``, and the maintainer's interactive
    ``vq`` -- still symlinked at the pre-split 0.25.7 runtime -- died on the
    whole file. On a fleet that failure is total rather than degraded.
    """

    def test_unknown_top_level_keys_do_not_hide_the_rest_of_the_config(
        self, cfg_dir: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        (cfg_dir / "config.toml").write_text(
            'default_host = "host_d"\n'
            "[pin_source_repos]\n"
            '"mpei/vibe-view" = "/home/USER/vibe-view-dev"\n'
            "[future_section]\n"
            'key = "value"\n'
            "[hosts.host_d]\n"
            'ssh = "host_d"\n'
        )
        cfg = config.load_config()
        assert cfg.default_host == "host_d"
        assert set(cfg.hosts) == {"host_d"}
        assert "future_section" in capsys.readouterr().err

    def test_every_unknown_key_is_named_once(
        self, cfg_dir: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        (cfg_dir / "config.toml").write_text(
            'from_a_newer_vq = 1\nalso_newer = 2\n'
        )
        config.load_config()
        first = capsys.readouterr().err
        assert "also_newer, from_a_newer_vq" in first
        # A retained queue reloads policy per row (#547); one process must not
        # emit one warning per read.
        for _ in range(50):
            config.load_config()
        assert capsys.readouterr().err == ""

    def test_an_edited_config_warns_again(
        self, cfg_dir: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Dedupe is keyed by what was ignored, not by the file's identity."""
        path = cfg_dir / "config.toml"
        path.write_text("first_unknown = 1\n")
        config.load_config()
        capsys.readouterr()
        path.write_text("second_unknown = 1\n")
        config.load_config()
        assert "second_unknown" in capsys.readouterr().err

    def test_tolerance_stops_at_the_top_level(self, cfg_dir: Path) -> None:
        """Inside a section a stray key is a typo, and silence is the worse
        outcome: ``webhook_urls`` disables notifications rather than losing a
        key this vq could not have used."""
        (cfg_dir / "config.toml").write_text(
            "[notifications]\n"
            'webhook_url = "https://x"\n'
            'webhook_urls = "https://y"\n'
        )
        with pytest.raises(config.ConfigError, match="invalid config"):
            config.load_config()

    def test_constructing_a_config_in_code_stays_strict(self) -> None:
        """The tolerance belongs to reading a file another vq wrote, not to
        the model: a keyword typo in vq's own source must still fail."""
        with pytest.raises(ValidationError):
            config.Config(default_hostt="host_d")

    @pytest.mark.parametrize("system", [False, True])
    def test_a_floor_this_vq_cannot_meet_names_the_version(
        self, cfg_dir: Path, monkeypatch: pytest.MonkeyPatch, system: bool,
    ) -> None:
        path = cfg_dir / "config.toml"
        path.write_text('min_vq_version = "9.9.9"\ndefault_host = "host_d"\n')
        if system:
            monkeypatch.setattr(config, "SYSTEM_CONFIG_PATH", path)
        loader = config.load_system_config if system else config.load_config
        with pytest.raises(config.ConfigError) as excinfo:
            loader()
        message = str(excinfo.value)
        assert "requires vq >= 9.9.9" in message
        assert config.vq.__version__ in message
        # The point of the key: a sentence, not a pydantic dump.
        assert "extra_forbidden" not in message

    @pytest.mark.parametrize("declared", ["0.0.1", "0.26.0"])
    def test_a_floor_this_vq_meets_loads_normally(
        self, cfg_dir: Path, monkeypatch: pytest.MonkeyPatch, declared: str,
    ) -> None:
        monkeypatch.setattr(config.vq, "__version__", "0.26.0")
        (cfg_dir / "config.toml").write_text(
            f'min_vq_version = "{declared}"\ndefault_host = "host_d"\n'
        )
        cfg = config.load_config()
        assert cfg.default_host == "host_d"
        assert cfg.min_vq_version == declared

    def test_a_dev_build_is_compared_by_its_release_part(
        self, cfg_dir: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(config.vq, "__version__", "0.26.0.dev3+g28620dc")
        (cfg_dir / "config.toml").write_text('min_vq_version = "0.26.0"\n')
        assert config.load_config().min_vq_version == "0.26.0"

    @pytest.mark.parametrize(
        "declared",
        [
            '"v0.26.0"', '"0.26"', '"latest"', "26",
            # Read by a tolerant parser these are all "0.26.0", which is not
            # what any of them says. The running version may carry a suffix
            # (`0.26.0.dev3+g28620dc`); a floor somebody wrote may not.
            '"0.26.0.1"', '"0.26.0-rc1"', '"0.26.0 or newer"',
        ],
    )
    def test_a_malformed_floor_is_refused_rather_than_assumed_met(
        self, cfg_dir: Path, declared: str,
    ) -> None:
        (cfg_dir / "config.toml").write_text(f"min_vq_version = {declared}\n")
        with pytest.raises(config.ConfigError, match="min_vq_version"):
            config.load_config()

    def test_the_floor_is_reported_before_the_keys_it_exists_to_explain(
        self, cfg_dir: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        """The unknown keys are the symptom; the version is the diagnosis."""
        (cfg_dir / "config.toml").write_text(
            'min_vq_version = "9.9.9"\npin_source_repos_v9 = 1\n'
        )
        with pytest.raises(config.ConfigError, match="requires vq >= 9.9.9"):
            config.load_config()
        assert "pin_source_repos_v9" not in capsys.readouterr().err

    def test_a_floor_survives_a_schema_this_vq_cannot_parse(
        self, cfg_dir: Path,
    ) -> None:
        """Read from the raw mapping, so the refusal still works when every
        other key in the file postdates this vq."""
        (cfg_dir / "config.toml").write_text(
            'min_vq_version = "9.9.9"\n'
            "[some_future_section]\n"
            "nested = { deeply = true }\n"
        )
        with pytest.raises(config.ConfigError, match="requires vq >= 9.9.9"):
            config.load_config()


class TestVibeqcSourceRepoIsOneSetting:
    """`scheduler_runtime_source_repo` is `pin_source_repos["mpei/vibe-qc"]`.

    The split added the per-repository mapping beside the global one, and the
    two then described overlapping things. A driver that had migrated fully to
    `pin_source_repos` was locked out of source staging, tag resolution and
    auto-update tag discovery, because each still read the old key alone.
    """

    def test_the_modern_spelling_is_enough(self, cfg_dir: Path) -> None:
        (cfg_dir / "config.toml").write_text(
            "[pin_source_repos]\n"
            '"mpei/vibe-qc" = "/srv/vibe-qc"\n'
        )
        assert config.load_config().vibeqc_source_repo == "/srv/vibe-qc"

    def test_the_deprecated_spelling_still_resolves(
        self, cfg_dir: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        (cfg_dir / "config.toml").write_text(
            'scheduler_runtime_source_repo = "/srv/vibe-qc"\n'
        )
        assert config.load_config().vibeqc_source_repo == "/srv/vibe-qc"

    def test_using_the_deprecated_spelling_warns_once(
        self, cfg_dir: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        config._DEPRECATED_SOURCE_REPO_WARNED = False
        try:
            (cfg_dir / "config.toml").write_text(
                'scheduler_runtime_source_repo = "/srv/vibe-qc"\n'
            )
            config.load_config()
            warning = capsys.readouterr().err
            assert "scheduler_runtime_source_repo is deprecated" in warning
            assert "mpei/vibe-qc" in warning
            # Policy is re-read per queue row (#547).
            for _ in range(20):
                config.load_config()
            assert capsys.readouterr().err == ""
        finally:
            config._DEPRECATED_SOURCE_REPO_WARNED = False

    def test_agreeing_spellings_are_accepted(self, cfg_dir: Path) -> None:
        (cfg_dir / "config.toml").write_text(
            'scheduler_runtime_source_repo = "/srv/vibe-qc"\n'
            "[pin_source_repos]\n"
            '"mpei/vibe-qc" = "/srv/vibe-qc"\n'
        )
        assert config.load_config().vibeqc_source_repo == "/srv/vibe-qc"

    def test_disagreeing_spellings_are_refused(self, cfg_dir: Path) -> None:
        """No reading of that config is obviously right, so vq does not pick."""
        (cfg_dir / "config.toml").write_text(
            'scheduler_runtime_source_repo = "/srv/old-monorepo"\n'
            "[pin_source_repos]\n"
            '"mpei/vibe-qc" = "/srv/vibe-qc"\n'
        )
        with pytest.raises(config.ConfigError, match="different vibe-qc checkouts"):
            config.load_config()

    def test_stage_source_accepts_the_modern_spelling(
        self, cfg_dir: Path,
    ) -> None:
        """The half-unified state locked a migrated driver out of staging."""
        (cfg_dir / "config.toml").write_text(
            "[pin_source_repos]\n"
            '"mpei/vibe-qc" = "/srv/vibe-qc"\n'
            "\n"
            "[hosts.host_f]\n"
            'ssh = "host_f"\n'
            'scheduler = "pbs"\n'
            'scheduler_dialect = "torque"\n'
            'scratch_root = "/scratch"\n'
            'scheduler_driver = "driver"\n'
            "\n"
            "[hosts.host_f.scheduler_runtime_deployments.vibeqc-release]\n"
            'update_command = "/site/bin/deploy"\n'
            'verify_command = "/site/bin/verify"\n'
            "stage_source = true\n"
            "\n"
            "[hosts.driver]\n"
            'ssh = "driver"\n'
        )
        cfg = config.load_config()
        assert cfg.vibeqc_source_repo == "/srv/vibe-qc"

    def test_stage_source_with_no_checkout_names_the_modern_spelling(
        self, cfg_dir: Path,
    ) -> None:
        (cfg_dir / "config.toml").write_text(
            "[hosts.host_f]\n"
            'ssh = "host_f"\n'
            'scheduler = "pbs"\n'
            'scheduler_dialect = "torque"\n'
            'scratch_root = "/scratch"\n'
            'scheduler_driver = "driver"\n'
            "\n"
            "[hosts.host_f.scheduler_runtime_deployments.vibeqc-release]\n"
            'update_command = "/site/bin/deploy"\n'
            'verify_command = "/site/bin/verify"\n'
            "stage_source = true\n"
            "\n"
            "[hosts.driver]\n"
            'ssh = "driver"\n'
        )
        with pytest.raises(config.ConfigError) as excinfo:
            config.load_config()
        assert "pin_source_repos" in str(excinfo.value)
        assert "mpei/vibe-qc" in str(excinfo.value)


@pytest.mark.parametrize('damage', [
    'missing-authorization', 'blank-reason', 'undated', 'timezone-free',
    'empty-bindings', 'invalid-digest', 'active-host', 'unknown-field',
])
def test_retirement_configuration_requires_explicit_bound_decision(damage):
    declaration = {
        'retired_at': '2026-09-12T17:00:00+00:00',
        'reason': 'permanent hardware retirement',
        'authorization_reference': 'maintainer decision in issue 13',
        'retained_receipts': {'v0.15.118-example': 'a' * 64},
    }
    if damage == 'missing-authorization':
        declaration.pop('authorization_reference')
    elif damage == 'blank-reason':
        declaration['reason'] = '  '
    elif damage == 'undated':
        declaration['retired_at'] = 'yesterday'
    elif damage == 'timezone-free':
        declaration['retired_at'] = '2026-09-12T17:00:00'
    elif damage == 'empty-bindings':
        declaration['retained_receipts'] = {}
    elif damage == 'invalid-digest':
        declaration['retained_receipts'] = {'v0.15.118-example': 'guess'}
    elif damage == 'unknown-field':
        declaration['skip_validation'] = True
    hosts = {'retired': {'ssh': 'retired'}} if damage == 'active-host' else {}
    with pytest.raises(ValueError):
        config.Config.model_validate({
            'hosts': hosts, 'fleet': {'retired_hosts': {'retired': declaration}},
        })


def test_retirement_audit_is_separate_from_active_host_inventory():
    cfg = config.Config.model_validate({
        'hosts': {'live': {'ssh': 'live'}},
        'fleet': {'retired_hosts': {'retired': {
            'retired_at': '2026-09-12T17:00:00+00:00',
            'reason': 'hardware failure',
            'authorization_reference': 'maintainer ticket 13',
            'retained_receipts': {'historical-rollout': 'a' * 64},
        }}},
    })
    assert set(cfg.hosts) == {'live'}
    assert set(cfg.fleet.retired_hosts) == {'retired'}
