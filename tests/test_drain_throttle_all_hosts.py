"""v0.8.5 *Knuth's Concrete* — `vq drain --all` + `vq throttle --all-hosts` tests.

Pins the v0.8.5 contract:

1. **`vq drain --all` fans out** across every configured host.
2. **`--all` and HOST are mutually exclusive.**
3. **Per-host failure isolation** — one bad SSH doesn't hide the rest.
4. **`--all --release` clears drain across the fleet.**
5. **`vq throttle --all-hosts --status` aggregates throttle state.**
6. **`vq throttle --all-hosts --release-persist` clears persistent throttle fleet-wide.**
7. **`vq throttle --all-hosts` rejects per-job ops** (jobids are host-local).

SSH transport is mocked so the tests run hermetically.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import config as _config
from vq import drain as drain_module
from vq import paths
from vq import throttle as throttle_module
from vq import transport as _transport
from vq.cli import main


@pytest.fixture
def fleet_state(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> Path:
    """Hermetic state dir + config dir with three hosts (one local,
    two fakes)."""
    state = tmp_path / "state"
    state.mkdir()
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(state))
    cfg_dir = tmp_path / "cfg"
    cfg_dir.mkdir()
    (cfg_dir / "config.toml").write_text(
        "[hosts.localhost]\nssh = \"localhost\"\n"
        "[hosts.alpha]\nssh = \"vq@alpha.example.com\"\n"
        "remote_vq = \"/usr/local/bin/vq\"\n"
        "[hosts.beta]\nssh = \"vq@beta.example.com\"\n"
        "remote_vq = \"/usr/local/bin/vq\"\n"
    )
    monkeypatch.setenv(_config.ENV_CONFIG_DIR, str(cfg_dir))
    return tmp_path


class _FakeProc:
    def __init__(self, stdout: str) -> None:
        self.stdout = stdout
        self.returncode = 0


def _fake_run_ok(stdout: str):
    def _run(host_cfg, *vq_args, stdin_data=None):
        return _FakeProc(stdout)
    return _run


def _fake_run_raises(message: str):
    def _run(host_cfg, *vq_args, stdin_data=None):
        raise _transport.RemoteError(message)
    return _run


@pytest.fixture
def scheduler_fleet_state(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> Path:
    """Local driver plus a daemonless scheduler target."""
    state = tmp_path / "state"
    state.mkdir()
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(state))
    cfg_dir = tmp_path / "cfg"
    cfg_dir.mkdir()
    (cfg_dir / "config.toml").write_text(
        "[hosts.localhost]\n"
        'ssh = "localhost"\n'
        "\n"
        "[hosts.host_f]\n"
        'ssh = "host_f.invalid"\n'
        'scheduler = "pbs"\n'
        'scheduler_dialect = "torque"\n'
        'scratch_root = "/home/USER"\n'
        'scheduler_driver = "localhost"\n'
    )
    monkeypatch.setenv(_config.ENV_CONFIG_DIR, str(cfg_dir))
    return tmp_path


# ----------------------------------------------------------------------
# `vq drain --all`
# ----------------------------------------------------------------------


class TestDrainAll:
    def test_all_and_host_are_mutually_exclusive(
        self, fleet_state: Path,
    ) -> None:
        runner = CliRunner()
        result = runner.invoke(
            main, ["drain", "--all", "alpha"],
        )
        assert result.exit_code != 0
        assert "mutually exclusive" in result.output

    def test_all_with_no_hosts_configured(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
    ) -> None:
        """Empty [hosts.X] → graceful message, not crash."""
        cfg = tmp_path / "cfg"
        cfg.mkdir()
        monkeypatch.setenv(_config.ENV_CONFIG_DIR, str(cfg))
        runner = CliRunner()
        result = runner.invoke(main, ["drain", "--all"])
        assert result.exit_code == 0
        assert "no [hosts.X] configured" in result.output

    def test_all_set_drain_aggregates_per_host(
        self, fleet_state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Setting drain --all calls the remote for each non-local
        host and runs the local arm in-process."""
        monkeypatch.setattr(
            _transport, "run_remote_vq",
            _fake_run_ok("drain set (full drain; no new dispatches)\n"),
        )
        runner = CliRunner()
        result = runner.invoke(
            main, ["drain", "--all", "--reason", "maintenance"],
        )
        assert result.exit_code == 0, result.output
        # Per-host banners.
        assert "==== localhost ====" in result.output
        assert "==== alpha ====" in result.output
        assert "==== beta ====" in result.output
        # Local arm wrote the drain state.
        state = drain_module.read_drain_state(via_rpc=False)
        assert state is not None
        assert state.enabled is True
        assert state.reason == "maintenance"

    def test_all_release_clears_drain(
        self, fleet_state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Pre-seed a drain locally so --release has something to clear.
        drain_module.write_drain_state(
            drain_module.DrainState(enabled=True, reason="x"),
            via_rpc=False,
        )
        monkeypatch.setattr(
            _transport, "run_remote_vq",
            _fake_run_ok(
                "drain released; daemon back to configured caps\n",
            ),
        )
        runner = CliRunner()
        result = runner.invoke(main, ["drain", "--all", "--release"])
        assert result.exit_code == 0, result.output
        # Local drain cleared.
        assert drain_module.read_drain_state(via_rpc=False) is None
        # Per-host banners present.
        assert "==== localhost ====" in result.output
        assert "==== alpha ====" in result.output

    def test_all_isolates_per_host_failure(
        self, fleet_state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A bad SSH on one host doesn't break the aggregate."""
        monkeypatch.setattr(
            _transport, "run_remote_vq",
            _fake_run_raises("ssh: connect timed out"),
        )
        runner = CliRunner()
        result = runner.invoke(main, ["drain", "--all"])
        assert result.exit_code == 0, result.output
        # Local succeeded.
        assert drain_module.read_drain_state(via_rpc=False) is not None
        # Remote hosts surfaced the error inline.
        assert "(error querying alpha:" in result.output
        assert "(error querying beta:" in result.output

    def test_all_status_aggregates(
        self, fleet_state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """--all --status reads drain state on every host and stacks."""
        monkeypatch.setattr(
            _transport, "run_remote_vq",
            _fake_run_ok(
                "drain: inactive (daemon dispatches normally)\n",
            ),
        )
        runner = CliRunner()
        result = runner.invoke(main, ["drain", "--all", "--status"])
        assert result.exit_code == 0, result.output
        assert "==== localhost ====" in result.output
        assert "drain:" in result.output

    def test_all_delegates_explicit_localhost(
        self, fleet_state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Regression: --all must delegate an explicit ``localhost`` so
        each remote acts on its OWN daemon rather than resolving through
        its default_host (which would otherwise mis-route the command to
        an unrelated host)."""
        captured: list[list[str]] = []

        def _capture(host_cfg, *vq_args, stdin_data=None):
            captured.append(list(vq_args))
            return _FakeProc(
                "drain released; daemon back to configured caps\n",
            )

        monkeypatch.setattr(_transport, "run_remote_vq", _capture)
        runner = CliRunner()
        result = runner.invoke(main, ["drain", "--all", "--release"])
        assert result.exit_code == 0, result.output
        assert captured, "no remote delegation captured"
        for args in captured:
            assert "localhost" in args, f"missing local target: {args}"
            assert "--all" not in args, f"must not re-fan-out: {args}"

    def test_scheduler_host_wraps_driver_drain(
        self, scheduler_fleet_state: Path
    ) -> None:
        runner = CliRunner()
        result = runner.invoke(main, ["drain", "host_f", "--status"])

        assert result.exit_code == 0, result.output
        assert "daemonless scheduler host" in result.output
        assert "driver-level drain state" in result.output
        assert "drain:" in result.output

    def test_all_includes_scheduler_host_without_remote_vq(
        self,
        scheduler_fleet_state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def fail_remote(*args, **kwargs):
            raise AssertionError("scheduler host must use local driver, not SSH")

        monkeypatch.setattr(_transport, "run_remote_vq", fail_remote)
        runner = CliRunner()
        result = runner.invoke(main, ["drain", "--all", "--status"])

        assert result.exit_code == 0, result.output
        assert "==== localhost ====" in result.output
        assert "==== host_f ====" in result.output
        assert "driver-level drain state" in result.output


# ----------------------------------------------------------------------
# `vq throttle --all-hosts`
# ----------------------------------------------------------------------


class TestThrottleAllHosts:
    def test_all_hosts_and_positional_mutex(
        self, fleet_state: Path,
    ) -> None:
        runner = CliRunner()
        result = runner.invoke(
            main, ["throttle", "--all-hosts", "--status", "alpha"],
        )
        assert result.exit_code != 0
        assert "mutually exclusive" in result.output

    def test_all_hosts_requires_compatible_op(
        self, fleet_state: Path,
    ) -> None:
        """Per-job throttle (--weight without --persist) can't fan
        out — jobids are host-local."""
        runner = CliRunner()
        result = runner.invoke(
            main, ["throttle", "--all-hosts", "--weight", "20"],
        )
        assert result.exit_code != 0
        assert "per-job throttle" in result.output or \
               "only valid with" in result.output

    def test_all_hosts_status_aggregates(
        self, fleet_state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(
            _transport, "run_remote_vq",
            _fake_run_ok(
                "persistent throttle: inactive "
                "(new jobs dispatch at default CPUWeight=100)\n",
            ),
        )
        runner = CliRunner()
        result = runner.invoke(
            main, ["throttle", "--all-hosts", "--status"],
        )
        assert result.exit_code == 0, result.output
        assert "==== localhost ====" in result.output
        assert "==== alpha ====" in result.output
        assert "persistent throttle" in result.output

    def test_all_hosts_status_delegates_localhost(
        self, fleet_state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Regression: --all-hosts must delegate an explicit ``localhost``
        so each remote acts on its own daemon, not its default_host."""
        captured: list[list[str]] = []

        def _capture(host_cfg, *vq_args, stdin_data=None):
            captured.append(list(vq_args))
            return _FakeProc("persistent throttle: inactive\n")

        monkeypatch.setattr(_transport, "run_remote_vq", _capture)
        runner = CliRunner()
        result = runner.invoke(
            main, ["throttle", "--all-hosts", "--status"],
        )
        assert result.exit_code == 0, result.output
        assert captured, "no remote delegation captured"
        for args in captured:
            assert "localhost" in args, f"missing local target: {args}"

    def test_all_hosts_release_persist_clears_local(
        self, fleet_state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Pre-seed persistent throttle locally.
        throttle_module.write_throttle_state(
            throttle_module.ThrottleState(weight=20, reason="x"),
            via_rpc=False,
        )
        monkeypatch.setattr(
            _transport, "run_remote_vq",
            _fake_run_ok("persistent throttle was not set (no-op)\n"),
        )
        runner = CliRunner()
        result = runner.invoke(
            main, ["throttle", "--all-hosts", "--release-persist"],
        )
        assert result.exit_code == 0, result.output
        # Local throttle cleared.
        assert throttle_module.read_throttle_state(via_rpc=False) is None

    def test_all_hosts_persist_set_locally(
        self, fleet_state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(
            _transport, "run_remote_vq",
            _fake_run_ok(
                "persistent throttle set (CPUWeight=25)\n",
            ),
        )
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "throttle", "--all-hosts", "--persist",
                "--weight", "25", "--reason", "kids gaming",
            ],
        )
        assert result.exit_code == 0, result.output
        # Local throttle state was written.
        state = throttle_module.read_throttle_state(via_rpc=False)
        assert state is not None
        assert state.weight == 25
        assert state.reason == "kids gaming"

    def test_all_hosts_isolates_per_host_failure(
        self, fleet_state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(
            _transport, "run_remote_vq",
            _fake_run_raises("ssh: host unreachable"),
        )
        runner = CliRunner()
        result = runner.invoke(
            main, ["throttle", "--all-hosts", "--status"],
        )
        assert result.exit_code == 0, result.output
        # Both remotes surfaced the error inline.
        assert "(error querying alpha:" in result.output
        assert "(error querying beta:" in result.output

    def test_all_hosts_persist_requires_weight(
        self, fleet_state: Path,
    ) -> None:
        """--persist --all-hosts without --weight fails fast."""
        runner = CliRunner()
        result = runner.invoke(
            main, ["throttle", "--all-hosts", "--persist"],
        )
        assert result.exit_code != 0
        assert "--weight" in result.output

    def test_scheduler_host_throttle_status_is_not_applicable(
        self, scheduler_fleet_state: Path
    ) -> None:
        runner = CliRunner()
        result = runner.invoke(main, ["throttle", "host_f", "--status"])

        assert result.exit_code == 0, result.output
        assert "daemonless scheduler host" in result.output
        assert "scheduler jobs are controlled by the batch scheduler" in result.output

    def test_scheduler_host_throttle_mutation_is_explicitly_unsupported(
        self, scheduler_fleet_state: Path
    ) -> None:
        runner = CliRunner()
        result = runner.invoke(main, ["throttle", "host_f", "--all", "--weight", "20"])

        assert result.exit_code != 0
        assert "CPUWeight/cgroup throttling does not control scheduler jobs" in result.output

    def test_all_hosts_throttle_skips_scheduler_target(
        self,
        scheduler_fleet_state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def fail_remote(*args, **kwargs):
            raise AssertionError("scheduler host must be skipped, not SSHed")

        monkeypatch.setattr(_transport, "run_remote_vq", fail_remote)
        runner = CliRunner()
        result = runner.invoke(main, ["throttle", "--all-hosts", "--status"])

        assert result.exit_code == 0, result.output
        assert "==== host_f ====" in result.output
        assert "scheduler jobs are controlled by the batch scheduler" in result.output
