"""Regressions for the 2026-07-22 host_f/host_c fleet-update incident.

Six defects blocked calculation submission for over a week. Each one is pinned
here by its *symptom*, because in every case the underlying code "worked" and
the operator-visible behaviour was the bug:

* BUG 1 — a successful helper update reported FAILED and poisoned the marker,
  because the post-install verify raced the site script's atomic activation.
* BUG 2 — the documented marker recovery ran clean and cleared nothing, because
  a scheduler host's marker lives on its driver, not on the cluster.
* BUG 3 — ``command_wrapper`` double-wrapped an interpreter, handing a bash
  launcher to its own python (~250 campaign jobs).
* BUG 4 — the daemon cached scheduler dispatchers for its whole life, so a
  config fix on disk had no effect until a full restart.
* GAP A — three rapid staging retries all landed inside one transport blip and
  aborted a two-hour deploy.
* GAP B — the active-job guard (correct, and it protected live paper jobs) had
  no supported path to ever be satisfied on a never-idle shared node.
"""
from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

# Cross-test imports (e.g. `from tests.test_scheduler_dispatch import ...`)
# need the parent of the tests directory on sys.path at collection time.
_tests_root = str(Path(__file__).resolve().parent.parent)
if _tests_root not in sys.path:
    sys.path.insert(0, _tests_root)

import pytest  # noqa: E402
from click.testing import CliRunner  # noqa: E402

from vq import admin, config, drain, paths, rpc, transport  # noqa: E402
from vq.cli import main  # noqa: E402
from vq.scheduler_dialect import SchedulerPhase  # noqa: E402
from vq.scheduler_dispatch import wrapper_already_applied  # noqa: E402
from vq.spec import JobSpec, JobState  # noqa: E402

_DRIVER_SHA = "a" * 40
_TREE_SHA256 = "b" * 64
_STALE_TREE = "c" * 64


@pytest.fixture
def state_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[Path]:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    paths.jobs_dir().mkdir(parents=True, exist_ok=True)

    def fake_stage(host, host_cfg, command_host_cfg, result, *, expected_sha=None):  # type: ignore[no-untyped-def]
        result.stage_root = "/shared/vq-admin/host_f"
        result.stage_path = f"{result.stage_root}/generations/{_DRIVER_SHA}-x"
        result.stage_uploaded = True
        result.archive_sha256 = "34" * 32
        result.expected_source_sha = _DRIVER_SHA
        result.expected_source_tree_sha256 = _TREE_SHA256
        return result.stage_path

    monkeypatch.setattr("vq.admin._stage_scheduler_helper_source", fake_stage)
    # Hermetic default: the reconcile probe never opens a socket in tests.
    # "Every id still known" reproduces the pre-reconcile census, so tests that
    # predate reconciliation keep their exact expectations; tests that care
    # about reaping override this.
    monkeypatch.setattr(
        "vq.admin._poll_scheduler_phases",
        lambda host_cfg, specs: {
            str(s.scheduler_job_id): SchedulerPhase.RUNNING for s in specs
        },
    )
    socket_path = Path(os.environ["VQ_TEST_SHORT_TMPDIR"]) / (
        f"vq-fleet-{os.getpid()}-{id(tmp_path)}.sock"
    )
    monkeypatch.setattr(rpc, "socket_path", lambda **_kwargs: socket_path)
    server = rpc.RPCServer(multi_user=False)
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
    server.start()
    try:
        yield tmp_path
    finally:
        server.stop()


def _write_config(cfg_dir: Path, *, driver: str = "localhost") -> None:
    host_block = (
        ["[hosts.localhost]", 'ssh = "localhost"']
        if driver == "localhost"
        else ["[hosts.driver]", 'ssh = "driver"']
    )
    (cfg_dir / "config.toml").write_text(
        "\n".join(
            [
                *host_block,
                "",
                "[hosts.host_f]",
                'ssh = "host_f-login"',
                'scheduler = "pbs"',
                'scheduler_dialect = "torque"',
                'scratch_root = "/home/USER"',
                f'scheduler_driver = "{driver}"',
                'fleet_role = "managed"',
                'scheduler_update_command = "/site/update_cluster.sh"',
                "scheduler_update_timeout_seconds = 123",
                "",
            ]
        )
    )


def _ok(stdout: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(args=[], returncode=0, stdout=stdout, stderr="")


def _install_shell_stub(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "vq.admin.transport.run_remote_shell",
        lambda *a, **k: _ok("installed\n"),
    )


def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("vq.admin.SCHEDULER_HELPER_READINESS_INTERVAL_SECONDS", 0)
    monkeypatch.setattr("vq.admin.SCHEDULER_HELPER_ACTIVATION_INTERVAL_SECONDS", 0)


# ----------------------------------------------------------------------
# BUG 1 — verify raced the atomic activation
# ----------------------------------------------------------------------


def test_helper_verify_waits_out_a_late_activation_flip(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A flip the login node has not observed yet must not read as failure.

    The host_c symptom exactly: the deploy succeeded (rc=0, new root
    published), but the first digest read still saw the pre-flip tree. vq used
    to call that a failed update and write a sticky FAILED marker that blocked
    every later admin update on the host.
    """
    _write_config(state_dir / "cfg")
    cfg = config.load_config()
    _no_sleep(monkeypatch)
    _install_shell_stub(monkeypatch)
    tree_reads = 0

    def helper(host_cfg, *vq_args, **kwargs):  # type: ignore[no-untyped-def]
        nonlocal tree_reads
        if vq_args == ("--version",):
            return _ok("vq 0.12.0\n")
        if vq_args == ("source-tree-sha256",):
            tree_reads += 1
            # Pre-flip for the first two reads, then the activation lands.
            return _ok(f"{_STALE_TREE if tree_reads <= 2 else _TREE_SHA256}\n")
        return _ok(f"{_DRIVER_SHA}\n")

    monkeypatch.setattr("vq.admin.transport.run_remote_vq", helper)

    result = admin.update_scheduler_host("host_f", cfg)

    assert result.success is True
    assert result.remote_source_tree_sha256 == _TREE_SHA256
    assert tree_reads == 3
    assert result.activation_wait_attempts == 3
    # The whole point: a successful update leaves no marker behind.
    assert result.marker_cleared is True
    assert admin.admin_update_marker_exists() is False


def test_helper_verify_still_rejects_a_permanently_stale_tree(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Waiting out a flip must not become "eventually accept anything"."""
    _write_config(state_dir / "cfg")
    cfg = config.load_config()
    _no_sleep(monkeypatch)
    _install_shell_stub(monkeypatch)

    def helper(host_cfg, *vq_args, **kwargs):  # type: ignore[no-untyped-def]
        if vq_args == ("--version",):
            return _ok("vq 0.12.0\n")
        if vq_args == ("source-tree-sha256",):
            return _ok(f"{_STALE_TREE}\n")
        return _ok(f"{_DRIVER_SHA}\n")

    monkeypatch.setattr("vq.admin.transport.run_remote_vq", helper)

    result = admin.update_scheduler_host("host_f", cfg)

    assert result.success is False
    assert "source-tree digest mismatch" in result.work_errors[-1]


def test_helper_verify_does_not_retry_a_hard_read_failure(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Only a well-formed mismatch is a flip-in-progress signal.

    A non-zero rc means the helper is broken, not that activation is pending —
    readiness already proved it executes. Retrying would just delay the report.
    """
    _write_config(state_dir / "cfg")
    cfg = config.load_config()
    _no_sleep(monkeypatch)
    _install_shell_stub(monkeypatch)
    tree_reads = 0

    def helper(host_cfg, *vq_args, **kwargs):  # type: ignore[no-untyped-def]
        nonlocal tree_reads
        if vq_args == ("--version",):
            return _ok("vq 0.12.0\n")
        tree_reads += 1
        return subprocess.CompletedProcess(
            args=[], returncode=3, stdout="", stderr="boom\n"
        )

    monkeypatch.setattr("vq.admin.transport.run_remote_vq", helper)

    result = admin.update_scheduler_host("host_f", cfg)

    assert result.success is False
    assert tree_reads == 1
    assert "no valid digest" in result.work_errors[-1]


def test_failed_marker_reason_names_the_real_error_not_rc_zero(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A verify failure must not record ``failure_reason='scheduler update rc=0'``.

    The rc-first reason string is what the operator read off the poisoned
    host_c marker: it literally named the successful command as the cause.
    """
    _write_config(state_dir / "cfg")
    cfg = config.load_config()
    _no_sleep(monkeypatch)
    _install_shell_stub(monkeypatch)

    def helper(host_cfg, *vq_args, **kwargs):  # type: ignore[no-untyped-def]
        if vq_args == ("--version",):
            return _ok("vq 0.12.0\n")
        if vq_args == ("source-tree-sha256",):
            return _ok(f"{_STALE_TREE}\n")
        return _ok(f"{_DRIVER_SHA}\n")

    monkeypatch.setattr("vq.admin.transport.run_remote_vq", helper)

    result = admin.update_scheduler_host("host_f", cfg)

    assert result.success is False
    assert result.command_rc == 0
    marker = admin.read_admin_update_marker()
    assert marker is not None
    assert marker.state == admin.ADMIN_UPDATE_STATE_FAILED
    assert marker.failure_reason is not None
    assert "rc=0" not in marker.failure_reason
    assert "source-tree digest mismatch" in marker.failure_reason


def test_scheduler_status_text_shows_the_marker_banner(
    state_dir: Path,
) -> None:
    """Recovery step 1 says "`vq admin status HOST` shows the marker banner"."""
    (state_dir / "cfg" / "config.toml").write_text(
        "\n".join(
            [
                "[hosts.localhost]",
                'ssh = "localhost"',
                "",
                "[hosts.host_f]",
                'ssh = "host_f-login"',
                'scheduler = "pbs"',
                'scheduler_dialect = "torque"',
                'scratch_root = "/home/USER"',
                'scheduler_driver = "localhost"',
                'scheduler_update_command = "/site/update_cluster.sh"',
                "",
                "[hosts.host_f.scheduler_runtime_deployments.vibeqc-release]",
                'update_command = "/site/bin/deploy"',
                'verify_command = "/site/bin/verify"',
                "",
                "[programs.vibeqc-release]",
                'kind = "binary"',
                'binary = "/opt/x"',
                "",
            ]
        )
    )
    cfg = config.load_config()
    admin.acquire_admin_update_marker(envs=["scheduler:host_f"], host="host_f")

    text = admin.format_scheduler_runtime_status("host_f", cfg)

    assert "scheduler:host_f" in text
    assert "admin-update-in-progress" in text


# ----------------------------------------------------------------------
# BUG 2 — the documented marker recovery could not clear the marker
# ----------------------------------------------------------------------


def test_clear_update_marker_clears_a_scheduler_marker_on_the_driver(
    state_dir: Path,
) -> None:
    """``vq admin clear-update-marker host_f`` must clear the DRIVER's marker.

    The marker is written by ``update_scheduler_host`` into the driver's state
    root with envs=['scheduler:host_f']. Pre-fix, this command SSHed to the
    cluster login node, found nothing, and exited 0 — while the real marker
    kept blocking every admin update.
    """
    _write_config(state_dir / "cfg", driver="localhost")
    admin.acquire_admin_update_marker(envs=["scheduler:host_f"], host="host_f")
    assert admin.admin_update_marker_exists() is True

    result = CliRunner().invoke(
        main, ["admin", "clear-update-marker", "host_f", "--yes", "--force-live"]
    )

    assert result.exit_code == 0, result.output
    assert "marker cleared" in result.output
    assert admin.admin_update_marker_exists() is False


def test_clear_update_marker_routes_to_a_remote_driver(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """With a remote driver, the clear must be delegated THERE, not to host_f."""
    _write_config(state_dir / "cfg", driver="driver")
    captured: dict[str, object] = {}

    def fake_delegate(host, cfg, *args, **kwargs):  # type: ignore[no-untyped-def]
        captured["host"] = host
        captured["argv"] = list(args)
        return "marker cleared\n"

    monkeypatch.setattr("vq.cli._delegate_to_remote", fake_delegate)

    result = CliRunner().invoke(
        main, ["admin", "clear-update-marker", "host_f", "--yes"]
    )

    assert result.exit_code == 0, result.output
    assert captured["host"] == "driver"
    # The cluster name is forwarded so the driver re-resolves to itself; the
    # pre-fix code delegated to "host_f" with a hardcoded "localhost".
    assert captured["argv"] == ["admin", "clear-update-marker", "--yes", "host_f"]


def test_clear_update_marker_says_which_host_it_looked_at(
    state_dir: Path,
) -> None:
    """"no marker present" must not read the same as "cleared successfully"."""
    _write_config(state_dir / "cfg", driver="localhost")

    result = CliRunner().invoke(main, ["admin", "clear-update-marker", "host_f"])

    assert result.exit_code == 0, result.output
    assert "no admin-update-in-progress marker present on" in result.output
    assert "scheduler driver for host_f" in result.output


def test_clear_update_marker_refuses_the_hostless_form_with_a_local_marker(
    state_dir: Path,
) -> None:
    """The no-arg form resolves to default_host — a trap when a marker is local."""
    (state_dir / "cfg" / "config.toml").write_text(
        "\n".join(
            [
                'default_host = "host_a"',
                "",
                "[hosts.localhost]",
                'ssh = "localhost"',
                "",
                "[hosts.host_a]",
                'ssh = "host_a"',
                "",
            ]
        )
    )
    admin.acquire_admin_update_marker(envs=["scheduler:host_f"], host="host_f")

    result = CliRunner().invoke(main, ["admin", "clear-update-marker", "--yes"])

    assert result.exit_code != 0
    assert "no HOST was given" in result.output
    assert "clear-update-marker localhost" in result.output
    # Refusing must not have touched the marker.
    assert admin.admin_update_marker_exists() is True


def test_clear_update_marker_plain_host_path_is_unchanged(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A non-scheduler host still delegates to itself with "localhost"."""
    (state_dir / "cfg" / "config.toml").write_text(
        "\n".join(
            ["[hosts.localhost]", 'ssh = "localhost"', "", "[hosts.host_a]", 'ssh = "host_a"', ""]
        )
    )
    captured: dict[str, object] = {}

    def fake_delegate(host, cfg, *args, **kwargs):  # type: ignore[no-untyped-def]
        captured["host"] = host
        captured["argv"] = list(args)
        return "no marker\n"

    monkeypatch.setattr("vq.cli._delegate_to_remote", fake_delegate)

    result = CliRunner().invoke(
        main, ["admin", "clear-update-marker", "host_a", "--yes"]
    )

    assert result.exit_code == 0, result.output
    assert captured["host"] == "host_a"
    assert captured["argv"] == ["admin", "clear-update-marker", "--yes", "localhost"]


# ----------------------------------------------------------------------
# BUG 3 — command_wrapper structurally double-wrapped
# ----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("wrapper", "command_head"),
    [
        # The shape that cost ~250 jobs: absolute wrapper, bare-name command
        # head. The exact-argv guard added earlier misses this entirely.
        ("/home/USER/bin/vibeqc-release-python", "vibeqc-release-python"),
        ("/home/USER/bin/vibeqc-release-python", "~/bin/vibeqc-release-python"),
        ("/home/USER/bin/vibeqc-release-python", "./vibeqc-release-python"),
        ("vibeqc-release-python", "/home/USER/bin/vibeqc-release-python"),
    ],
)
def test_wrapper_dedup_across_equivalent_spellings(
    wrapper: str, command_head: str
) -> None:
    assert wrapper_already_applied([wrapper], [command_head, "run.py"]) is True


def test_wrapper_dedup_when_wrapper_carries_extra_args() -> None:
    """Prefix equality fails on element 1, but the head is still the same."""
    wrapper = ["/home/USER/bin/vibeqc-release-python", "--flag"]
    command = ["/home/USER/bin/vibeqc-release-python", "run.py"]
    assert wrapper_already_applied(wrapper, command) is True


def test_wrapper_still_injected_for_a_genuinely_different_program() -> None:
    """A real site wrapper around a real binary must keep wrapping."""
    assert (
        wrapper_already_applied(
            ["/site/bin/orcasub", "--scheduler"], ["/opt/orca/orca", "in.inp"]
        )
        is False
    )


def test_wrapper_dedup_tolerates_empty_argv() -> None:
    assert wrapper_already_applied([], ["python", "x.py"]) is False
    assert wrapper_already_applied(["/bin/w"], []) is False


def test_build_job_script_does_not_double_wrap_a_bare_name_interpreter() -> None:
    """End to end: the rendered PBS line must launch the wrapper exactly once."""
    from tests.test_scheduler_dispatch import FakeRunner, make_dispatcher
    from vq.config import SchedulerProgramHooks

    wrapper = "/home/USER/bin/vibeqc-release-python"
    d = make_dispatcher(
        FakeRunner(),
        scheduler_program_hooks={
            "vibeqc-release": SchedulerProgramHooks(command_wrapper=[wrapper])
        },
    )
    script = d.build_job_script(
        job_id="j1",
        # What a single-file submit actually produces: [interpreter, script].
        command=["vibeqc-release-python", "run_batch.py"],
        remote_workspace=d.remote_workspace("j1"),
        cpus=2,
        program="vibeqc-release",
    )

    assert 'vibeqc-release-python run_batch.py > "$__vq_stdout"' in script
    assert f"{wrapper} vibeqc-release-python" not in script


def test_doctor_flags_a_wrapper_that_is_also_the_interpreter(
    state_dir: Path,
) -> None:
    """``vq doctor`` reported ok:true for this config regardless. It must not."""
    (state_dir / "cfg" / "config.toml").write_text(
        "\n".join(
            [
                "[hosts.localhost]",
                'ssh = "localhost"',
                "",
                "[hosts.host_f]",
                'ssh = "host_f-login"',
                'scheduler = "pbs"',
                'scheduler_dialect = "torque"',
                'scratch_root = "/home/USER"',
                'scheduler_driver = "localhost"',
                "",
                "[hosts.host_f.branches]",
                'release = "/home/USER/bin/vibeqc-release-python"',
                "",
                "[hosts.host_f.scheduler_program_hooks.vibeqc-release]",
                'command_wrapper = ["vibeqc-release-python"]',
                "",
                "[programs.vibeqc-release]",
                'kind = "binary"',
                'binary = "/opt/x"',
                "",
            ]
        )
    )
    cfg = config.load_config()
    from vq.cli import _doctor_scheduler_command_wrapper_check

    check = _doctor_scheduler_command_wrapper_check(cfg.host("host_f"))

    assert check["ok"] is False
    assert "branches.release" in str(check["message"])
    assert "double-wrap" in str(check["message"])


def test_doctor_accepts_a_wrapper_distinct_from_the_interpreter(
    state_dir: Path,
) -> None:
    (state_dir / "cfg" / "config.toml").write_text(
        "\n".join(
            [
                "[hosts.localhost]",
                'ssh = "localhost"',
                "",
                "[hosts.host_f]",
                'ssh = "host_f-login"',
                'scheduler = "pbs"',
                'scheduler_dialect = "torque"',
                'scratch_root = "/home/USER"',
                'scheduler_driver = "localhost"',
                "",
                "[hosts.host_f.branches]",
                'release = "/home/USER/vibeqc-release/.venv/bin/python"',
                "",
                "[hosts.host_f.scheduler_program_hooks.orca]",
                'command_wrapper = ["/site/bin/orcasub"]',
                "",
                "[programs.orca]",
                'kind = "binary"',
                'binary = "/opt/orca/orca"',
                "",
            ]
        )
    )
    cfg = config.load_config()
    from vq.cli import _doctor_scheduler_command_wrapper_check

    check = _doctor_scheduler_command_wrapper_check(cfg.host("host_f"))

    assert check["ok"] is True


# ----------------------------------------------------------------------
# GAP A — staging retries were too shallow to survive a transient blip
# ----------------------------------------------------------------------


def test_upload_retries_transient_255_on_a_fresh_connection(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The mux socket is the failure mode; a retry must bypass it.

    The fleet's ~/.ssh/config uses ControlMaster+ControlPersist, so when a
    gateway moves out from under a persisted master every attempt over that
    socket fails identically. Retrying without ControlPath=none is a no-op.
    """
    local = tmp_path / "f.txt"
    local.write_text("x")
    argvs: list[list[str]] = []
    sleeps: list[float] = []

    def fake_run(cmd, **kwargs):  # type: ignore[no-untyped-def]
        argvs.append(list(cmd))
        if len(argvs) == 1:
            return subprocess.CompletedProcess(
                args=cmd, returncode=255, stdout="", stderr="scp: Connection closed\n"
            )
        return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="", stderr="")

    monkeypatch.setattr(transport, "run_owned_subprocess", fake_run)
    monkeypatch.setattr(transport.time, "sleep", sleeps.append)

    transport.upload_file(
        config.HostConfig(ssh="host_c"), local, "/remote/f.txt", retry_transient=2
    )

    assert len(argvs) == 2
    assert "ControlPath=none" not in argvs[0]
    assert "ControlPath=none" in argvs[1]
    assert "ControlMaster=no" in argvs[1]
    assert sleeps == [transport._UPLOAD_RETRY_BACKOFF_BASE_SECONDS]


def test_upload_does_not_retry_a_real_scp_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """"No space left on device" is not transient; burning the window is wrong."""
    local = tmp_path / "f.txt"
    local.write_text("x")
    calls = 0

    def fake_run(cmd, **kwargs):  # type: ignore[no-untyped-def]
        nonlocal calls
        calls += 1
        return subprocess.CompletedProcess(
            args=cmd, returncode=1, stdout="", stderr="No space left on device\n"
        )

    monkeypatch.setattr(transport, "run_owned_subprocess", fake_run)
    monkeypatch.setattr(transport.time, "sleep", lambda _s: None)

    with pytest.raises(transport.RemoteError):
        transport.upload_file(
            config.HostConfig(ssh="host_c"), local, "/remote/f.txt", retry_transient=4
        )

    assert calls == 1


def test_upload_backoff_is_exponential_and_capped(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    local = tmp_path / "f.txt"
    local.write_text("x")
    sleeps: list[float] = []

    monkeypatch.setattr(
        transport,
        "run_owned_subprocess",
        lambda cmd, **k: subprocess.CompletedProcess(
            args=cmd, returncode=255, stdout="", stderr="closed\n"
        ),
    )
    monkeypatch.setattr(transport.time, "sleep", sleeps.append)

    with pytest.raises(transport.RemoteError):
        transport.upload_file(
            config.HostConfig(ssh="host_c"), local, "/r", retry_transient=5
        )

    assert sleeps == [5.0, 10.0, 20.0, 40.0, 80.0]
    # Five retries span ~2.5 minutes, comfortably outliving a gateway cutover;
    # the pre-fix loop made three attempts inside a single second.
    assert sum(sleeps) > 120


def test_upload_backoff_respects_the_cap() -> None:
    assert transport._retry_delay(10, base=5.0, cap=120.0) == 120.0


def test_staging_loop_backs_off_between_attempts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The pre-fix loop retried instantly, so all 3 attempts hit the same blip."""
    sleeps: list[float] = []
    monkeypatch.setattr("vq.admin.time.sleep", sleeps.append)
    monkeypatch.setattr(
        "vq.admin.refresh_admin_update_marker_heartbeat", lambda _m: None
    )

    for attempt in range(3):
        admin._stage_retry_sleep(attempt, 6, "host_c", "upload", "closed")

    assert sleeps == [5.0, 10.0, 20.0]


def test_staging_attempt_budget_outlives_a_blip() -> None:
    assert admin._RUNTIME_SOURCE_STAGE_ATTEMPTS >= 6


# ----------------------------------------------------------------------
# GAP B — the active-job guard had no supported path to satisfaction
# ----------------------------------------------------------------------


def _pending_scheduler_job(state: JobState, jobid: str = "j1") -> Path:
    spec = JobSpec(
        id=jobid,
        command=["true"],
        cwd="/tmp",
        cpus=1,
        workspace="/tmp",
        state=state,
        scheduler_target="host_f",
        scheduler_job_id="123.cluster",
    )
    path = paths.queue_dir() / f"{jobid}.json"
    spec.write(path)
    return path


def test_update_still_refuses_immediately_without_drain_wait(
    state_dir: Path,
) -> None:
    """The guard is correct and stays the default. Do not weaken it."""
    _write_config(state_dir / "cfg")
    cfg = config.load_config()
    _pending_scheduler_job(JobState.RUNNING)

    result = admin.update_scheduler_host("host_f", cfg)

    assert result.success is False
    assert result.active_jobs == ["j1(running)"]
    assert "active submitted job" in result.work_errors[0]
    # And it points at the supported way through.
    assert "--drain-wait" in result.work_errors[0]
    assert drain.read_effective_drain_state() is None


def test_drain_wait_holds_a_lane_and_proceeds_once_the_node_goes_quiet(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The supported maintenance window on a never-idle shared node."""
    _write_config(state_dir / "cfg")
    cfg = config.load_config()
    spec_path = _pending_scheduler_job(JobState.RUNNING)
    _no_sleep(monkeypatch)
    _install_shell_stub(monkeypatch)
    lane_seen: list[bool] = []

    def fake_sleep(_seconds: float) -> None:
        # Also stands in for the later readiness sleeps, hence missing_ok.
        if not spec_path.exists():
            return
        # While vq waits, the lane must be held so no NEW work is dispatched.
        state = drain.read_effective_drain_state()
        lane_seen.append(state is not None and "host_f" in state.scheduler_hosts)
        spec_path.unlink()  # the cluster job finishes

    monkeypatch.setattr("vq.admin._scheduler_drain_wait_sleep", fake_sleep)

    def helper(host_cfg, *vq_args, **kwargs):  # type: ignore[no-untyped-def]
        if vq_args == ("--version",):
            return _ok("vq 0.12.0\n")
        if vq_args == ("source-tree-sha256",):
            return _ok(f"{_TREE_SHA256}\n")
        return _ok(f"{_DRIVER_SHA}\n")

    monkeypatch.setattr("vq.admin.transport.run_remote_vq", helper)

    result = admin.update_scheduler_host("host_f", cfg, drain_wait_seconds=600)

    assert result.success is True
    assert lane_seen == [True]
    assert result.drain_lane_held is True
    # The lane vq added is released again; it must not linger.
    assert drain.read_effective_drain_state() is None


def test_drain_wait_reports_a_timeout_with_what_is_still_running(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(state_dir / "cfg")
    cfg = config.load_config()
    _pending_scheduler_job(JobState.RUNNING)
    monkeypatch.setattr("vq.admin._scheduler_drain_wait_sleep", lambda _s: None)

    result = admin.update_scheduler_host("host_f", cfg, drain_wait_seconds=0.01)

    assert result.success is False
    assert result.active_jobs == ["j1(running)"]
    assert "drain-wait" in result.work_errors[0]
    assert drain.read_effective_drain_state() is None


def test_drain_wait_fails_fast_when_every_remaining_job_is_held(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A qhold'd job never completes, so the wait can never converge.

    Burning a multi-hour deadline and then reporting a bare timeout would hide
    the actual answer from the operator.
    """
    _write_config(state_dir / "cfg")
    cfg = config.load_config()
    _pending_scheduler_job(JobState.SUSPENDED)
    slept: list[float] = []
    monkeypatch.setattr("vq.admin._scheduler_drain_wait_sleep", slept.append)

    result = admin.update_scheduler_host("host_f", cfg, drain_wait_seconds=86400)

    assert result.success is False
    assert slept == []
    assert "SUSPENDED" in result.work_errors[0]
    assert "cannot converge" in result.work_errors[0]


def test_drain_wait_leaves_an_operators_existing_lane_alone(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """host_f has carried a deliberate hold for days; vq must not lift it."""
    _write_config(state_dir / "cfg")
    cfg = config.load_config()
    _pending_scheduler_job(JobState.RUNNING)
    drain.add_scheduler_host("host_f", reason="operator hold")
    monkeypatch.setattr("vq.admin._scheduler_drain_wait_sleep", lambda _s: None)

    result = admin.update_scheduler_host("host_f", cfg, drain_wait_seconds=0.01)

    assert result.drain_lane_held is True
    state = drain.read_effective_drain_state()
    assert state is not None
    assert "host_f" in state.scheduler_hosts
    assert state.reason == "operator hold"
    assert result.success is False


def test_add_scheduler_host_reports_whether_it_added_the_lane() -> None:
    """The add/release asymmetry is what protects an operator's hold."""
    assert drain.add_scheduler_host("host_f") is True
    assert drain.add_scheduler_host("host_f") is False


def test_add_scheduler_host_preserves_an_existing_full_drain(
    state_dir: Path,
) -> None:
    drain.write_drain_state(drain.DrainState(enabled=True))
    assert drain.add_scheduler_host("host_f") is True
    state = drain.read_effective_drain_state()
    assert state is not None
    assert state.full_dispatch is True
    assert state.scheduler_hosts == ["host_f"]


def test_drain_wait_is_rejected_for_a_venv_env(state_dir: Path) -> None:
    _write_config(state_dir / "cfg")

    result = CliRunner().invoke(
        main, ["admin", "update", "vibeqc-dev", "--drain-wait", "1h"]
    )

    assert result.exit_code != 0
    assert "--drain-wait requires a scheduler host" in result.output


# ----------------------------------------------------------------------
# BUG 4 — the daemon used config nobody was running anymore
# ----------------------------------------------------------------------


def _daemon_config(cfg_dir: Path, wrapper: str) -> None:
    (cfg_dir / "config.toml").write_text(
        "\n".join(
            [
                "[hosts.localhost]",
                'ssh = "localhost"',
                "",
                "[hosts.host_f]",
                'ssh = "host_f-login"',
                'scheduler = "pbs"',
                'scheduler_dialect = "torque"',
                'scratch_root = "/home/USER"',
                'scheduler_driver = "localhost"',
                "",
                "[hosts.host_f.scheduler_program_hooks.orca]",
                f'command_wrapper = ["{wrapper}"]',
                "",
                "[programs.orca]",
                'kind = "binary"',
                'binary = "/opt/orca/orca"',
                "",
            ]
        )
    )


@pytest.fixture
def daemon_with_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):  # type: ignore[no-untyped-def]
    from vq.daemon import Daemon

    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    _daemon_config(tmp_path / "cfg", "/site/bin/wrap-v1")
    d = Daemon(
        max_cpus=4,
        poll_interval=0.05,
        queue_dir=tmp_path / "queue",
        jobs_dir=tmp_path / "jobs",
    )
    d.queue_dir.mkdir(parents=True, exist_ok=True)
    d.jobs_dir.mkdir(parents=True, exist_ok=True)
    return d


def test_dispatcher_is_rebuilt_when_config_changes_on_disk(
    daemon_with_config, tmp_path: Path
) -> None:
    """The 17:50 fix must not be ignored until 18:26.

    The dispatcher snapshots ``scheduler_program_hooks`` at build time and the
    cache was never invalidated, so a config fix on disk had no effect for the
    rest of the daemon's life — while it dispatched a whole released backlog.
    """
    first = daemon_with_config._scheduler_dispatcher_for("host_f")
    assert first.scheduler_program_hooks["orca"].command_wrapper == [
        "/site/bin/wrap-v1"
    ]
    # Same config => same object, no needless rebuild.
    assert daemon_with_config._scheduler_dispatcher_for("host_f") is first

    config_file = tmp_path / "cfg" / "config.toml"
    before = config_file.stat()
    _daemon_config(tmp_path / "cfg", "/site/bin/wrap-v2")
    # Reproduce a coarse/overlay filesystem: the new content has the same
    # length, and restoring the old timestamp makes a stat-only fingerprint
    # identical even though the command wrapper changed.
    assert config_file.stat().st_size == before.st_size
    os.utime(
        config_file,
        ns=(config_file.stat().st_atime_ns, before.st_mtime_ns),
    )

    second = daemon_with_config._scheduler_dispatcher_for("host_f")
    assert second is not first
    assert second.scheduler_program_hooks["orca"].command_wrapper == [
        "/site/bin/wrap-v2"
    ]


def test_injected_dispatchers_are_pinned_across_config_changes(
    daemon_with_config, tmp_path: Path
) -> None:
    """A programmatically injected dispatcher must not be swapped out.

    This is the seam the scheduler daemon tests rely on; a config change is not
    a reason to discard an override the caller installed deliberately.
    """
    sentinel = object()
    daemon_with_config._scheduler_dispatchers["host_f"] = sentinel  # type: ignore[assignment]

    _daemon_config(tmp_path / "cfg", "/site/bin/wrap-v2")

    assert daemon_with_config._scheduler_dispatcher_for("host_f") is sentinel


def test_reload_clears_the_dispatcher_cache(daemon_with_config) -> None:
    first = daemon_with_config._scheduler_dispatcher_for("host_f")
    daemon_with_config.request_config_reload()
    daemon_with_config._maybe_apply_config_reload()
    assert daemon_with_config._scheduler_dispatchers == {}
    assert daemon_with_config._scheduler_dispatcher_for("host_f") is not first


def test_reload_refuses_a_config_that_does_not_parse(
    daemon_with_config, tmp_path: Path
) -> None:
    """Adopting a broken config would be strictly worse than staleness.

    The refused reload keeps the cache intact, and the dispatch gate then parks
    the queue rather than letting jobs run against a config that cannot be
    read. Loud and recoverable beats quiet and wrong.
    """
    first = daemon_with_config._scheduler_dispatcher_for("host_f")
    (tmp_path / "cfg" / "config.toml").write_text("this is not valid toml {[")

    daemon_with_config.request_config_reload()
    daemon_with_config._maybe_apply_config_reload()

    assert daemon_with_config._scheduler_dispatchers["host_f"] is first
    assert daemon_with_config._config_unusable() is True


def test_dispatch_is_parked_while_the_config_is_unparseable(
    daemon_with_config, tmp_path: Path
) -> None:
    """Refuse to dispatch rather than silently use superseded state."""
    assert daemon_with_config._config_unusable() is False
    (tmp_path / "cfg" / "config.toml").write_text("nope {[")
    assert daemon_with_config._config_unusable() is True
    # And it recovers on its own once the file is valid again.
    _daemon_config(tmp_path / "cfg", "/site/bin/wrap-v3")
    assert daemon_with_config._config_unusable() is False


def test_sighup_requests_a_reload(daemon_with_config) -> None:
    import signal as _signal

    assert daemon_with_config._config_reload_requested is False
    daemon_with_config._handle_reload_signal(_signal.SIGHUP, None)
    assert daemon_with_config._config_reload_requested is True
    daemon_with_config._maybe_apply_config_reload()
    assert daemon_with_config._config_reload_requested is False


# ----------------------------------------------------------------------
# Regressions in the fixes themselves (found by adversarial review)
# ----------------------------------------------------------------------


def test_drain_lane_is_released_when_the_wait_is_interrupted(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Ctrl-C during a multi-hour --drain-wait must not leave host_f drained.

    The first cut carried the did-we-add-it flag out on the returned outcome,
    so an interrupt skipped the assignment, the `finally` saw lane_added=False,
    and the lane stayed held forever. A retry could not clear it either: the
    second run's add returned False (already held) so its release was skipped
    too, and vq's own "never lift an operator's hold" rule then protected vq's
    own leaked lane. host_f dispatch would stop until a manual `vq drain
    --scheduler-host host_f --release`.
    """
    _write_config(state_dir / "cfg")
    cfg = config.load_config()
    _pending_scheduler_job(JobState.RUNNING)

    def interrupt(_seconds: float) -> None:
        raise KeyboardInterrupt("operator ctrl-c")

    monkeypatch.setattr("vq.admin._scheduler_drain_wait_sleep", interrupt)

    with pytest.raises(KeyboardInterrupt):
        admin.update_scheduler_host("host_f", cfg, drain_wait_seconds=3600)

    state = drain.read_effective_drain_state()
    assert state is None or "host_f" not in state.scheduler_hosts


def test_drain_lane_is_released_when_the_heartbeat_fails(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Not Ctrl-C-specific: any exception from inside the wait leaked it too."""
    _write_config(state_dir / "cfg")
    cfg = config.load_config()
    _pending_scheduler_job(JobState.RUNNING)

    def boom(_message: str) -> None:
        raise OSError("state dir went read-only")

    monkeypatch.setattr(
        "vq.admin.refresh_admin_update_marker_heartbeat", boom
    )
    monkeypatch.setattr("vq.admin._scheduler_drain_wait_sleep", lambda _s: None)

    with pytest.raises(OSError):
        admin.update_scheduler_host("host_f", cfg, drain_wait_seconds=3600)

    state = drain.read_effective_drain_state()
    assert state is None or "host_f" not in state.scheduler_hosts


def test_drain_lane_releases_the_existing_id_echoed_by_the_daemon(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An idempotent acquire can return an older claim for this owner."""
    _write_config(state_dir / "cfg")
    cfg = config.load_config()
    _pending_scheduler_job(JobState.RUNNING)
    released: list[str] = []

    def acquire_existing(_host: str, **_kwargs: object):
        return type("Lease", (), {"lease_id": "existing-owner-lease"})(), False

    monkeypatch.setattr(drain, "acquire_scheduler_drain_lease", acquire_existing)
    monkeypatch.setattr(
        drain,
        "release_scheduler_drain_lease",
        lambda lease_id, **_kwargs: released.append(lease_id) or True,
    )
    monkeypatch.setattr(
        "vq.admin._scheduler_drain_wait_sleep",
        lambda _seconds: (_ for _ in ()).throw(KeyboardInterrupt()),
    )

    with pytest.raises(KeyboardInterrupt):
        admin.update_scheduler_host("host_f", cfg, drain_wait_seconds=3600)

    assert released == ["existing-owner-lease"]


def test_remote_driver_release_failure_names_exact_on_driver_recovery(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(state_dir / "cfg", driver="driver")
    cfg = config.load_config()
    _pending_scheduler_job(JobState.RUNNING)
    captured: dict[str, object] = {}

    def acquire(_host: str, **kwargs: object):
        captured.update(kwargs)
        lease_id = str(kwargs["lease_id"])
        return type("Lease", (), {"lease_id": lease_id})(), True

    monkeypatch.setattr(drain, "acquire_scheduler_drain_lease", acquire)
    monkeypatch.setattr(
        drain,
        "release_scheduler_drain_lease",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            drain.SchedulerDrainLeaseError("release response lost")
        ),
    )
    monkeypatch.setattr(
        "vq.admin._scheduler_drain_wait_sleep", lambda _seconds: None
    )

    result = admin.update_scheduler_host(
        "host_f",
        cfg,
        drain_wait_seconds=0.01,
    )

    owner = str(captured["owner"])
    recovery = next(
        message for message in result.work_errors
        if "release response lost" in message
    )
    assert "run on scheduler driver 'driver'" in recovery
    assert f"--lease-owner {owner} localhost" in recovery
    assert "Do not use a broad scheduler-host release" in recovery


def test_daemon_parks_dispatch_when_the_config_file_vanishes(
    daemon_with_config, tmp_path: Path
) -> None:
    """A vanished config must park, not terminally FAIL every scheduler job.

    Rebuilding a dispatcher against a missing config raises ConfigError, which
    the dispatch path turns into a terminal FAILED. Before the cache was
    invalidated on config change those jobs rode the cached dispatcher and
    survived, so this would have been a regression introduced by the fix.
    """
    daemon_with_config._scheduler_dispatcher_for("host_f")
    (tmp_path / "cfg" / "config.toml").unlink()

    assert daemon_with_config._config_unusable() is True


# ----------------------------------------------------------------------
# Item 2/9: the guard must not trust vq's own job state
# ----------------------------------------------------------------------


def _phase_stub(mapping: dict[str, SchedulerPhase]):
    def _poll(host_cfg, specs):  # type: ignore[no-untyped-def]
        return {
            str(s.scheduler_job_id): mapping.get(
                str(s.scheduler_job_id), SchedulerPhase.RUNNING
            )
            for s in specs
        }

    return _poll


def test_phantom_jobs_no_longer_block_maintenance(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Six host_f jobs blocked maintenance; the scheduler had finished them.

    vq stamps state=RUNNING before the qsub even runs and keeps the scheduler's
    real phase in a separate field, so its own state is not a statement about
    the cluster. The guard read that state directly.
    """
    _write_config(state_dir / "cfg")
    cfg = config.load_config()
    _pending_scheduler_job(JobState.RUNNING, jobid="ghost")
    _no_sleep(monkeypatch)
    _install_shell_stub(monkeypatch)
    monkeypatch.setattr(
        "vq.admin._poll_scheduler_phases",
        _phase_stub({"123.cluster": SchedulerPhase.FINISHED}),
    )

    def helper(host_cfg, *vq_args, **kwargs):  # type: ignore[no-untyped-def]
        if vq_args == ("--version",):
            return _ok("vq 0.12.0\n")
        if vq_args == ("source-tree-sha256",):
            return _ok(f"{_TREE_SHA256}\n")
        return _ok(f"{_DRIVER_SHA}\n")

    monkeypatch.setattr("vq.admin.transport.run_remote_vq", helper)

    result = admin.update_scheduler_host("host_f", cfg)

    assert result.success is True


def test_a_live_scheduler_job_still_blocks(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The reconcile only ever REMOVES; it must never weaken the guard."""
    _write_config(state_dir / "cfg")
    cfg = config.load_config()
    _pending_scheduler_job(JobState.RUNNING)
    monkeypatch.setattr(
        "vq.admin._poll_scheduler_phases",
        _phase_stub({"123.cluster": SchedulerPhase.RUNNING}),
    )

    result = admin.update_scheduler_host("host_f", cfg)

    assert result.success is False
    assert result.active_jobs == ["j1(running)"]


def test_a_queued_scheduler_job_still_blocks(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Four of host_f's six were merely QUEUED — not started, but not gone.

    A queued job can start at any moment, including mid-rebuild, so it keeps
    blocking. Only a job the scheduler reports FINISHED is discounted.
    """
    _write_config(state_dir / "cfg")
    cfg = config.load_config()
    _pending_scheduler_job(JobState.RUNNING)
    monkeypatch.setattr(
        "vq.admin._poll_scheduler_phases",
        _phase_stub({"123.cluster": SchedulerPhase.PENDING}),
    )

    result = admin.update_scheduler_host("host_f", cfg)

    assert result.success is False


def test_a_failed_probe_fails_closed(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A broken probe must never be the thing that green-lights a rebuild."""
    _write_config(state_dir / "cfg")
    cfg = config.load_config()
    _pending_scheduler_job(JobState.RUNNING)

    def boom(host_cfg, specs):  # type: ignore[no-untyped-def]
        raise RuntimeError("qstat unreachable")

    monkeypatch.setattr("vq.admin._poll_scheduler_phases", boom)

    result = admin.update_scheduler_host("host_f", cfg)

    assert result.success is False
    assert result.active_jobs == ["j1(running)"]
    assert "could NOT reconcile" in result.work_errors[0]
    assert "qstat unreachable" in result.work_errors[0]


def test_a_job_with_no_scheduler_id_blocks_but_is_named(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Unprobeable is not the same as gone: an untracked batch job may be live.

    It keeps blocking, but the operator is told which spec and what to do,
    rather than being handed a bare count to re-derive from qstat.
    """
    _write_config(state_dir / "cfg")
    cfg = config.load_config()
    spec = JobSpec(
        id="orphan",
        command=["true"],
        cwd="/tmp",
        cpus=1,
        state=JobState.RUNNING,
        scheduler_target="host_f",
        scheduler_job_id=None,
    )
    spec.write(paths.queue_dir() / "orphan.json")

    result = admin.update_scheduler_host("host_f", cfg)

    assert result.success is False
    assert "no scheduler job id recorded" in result.work_errors[0]
    assert "orphan" in result.work_errors[0]


def test_drain_wait_fails_fast_when_nothing_can_be_observed(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An unobservable job will never be seen to finish — do not wait 4h for it."""
    _write_config(state_dir / "cfg")
    cfg = config.load_config()
    JobSpec(
        id="orphan",
        command=["true"],
        cwd="/tmp",
        cpus=1,
        state=JobState.RUNNING,
        scheduler_target="host_f",
        scheduler_job_id=None,
    ).write(paths.queue_dir() / "orphan.json")
    slept: list[float] = []
    monkeypatch.setattr("vq.admin._scheduler_drain_wait_sleep", slept.append)

    result = admin.update_scheduler_host("host_f", cfg, drain_wait_seconds=86400)

    assert result.success is False
    assert slept == []
    assert "cannot converge" in result.work_errors[0]


def test_drain_wait_converges_once_the_scheduler_reports_finished(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The wait keys off the SCHEDULER, not off vq's own state.

    Without the reconcile a stale RUNNING spec kept the count above zero and
    the wait burned its entire deadline — the exact way host_f would have
    defeated the maintenance window this flow exists to provide.
    """
    _write_config(state_dir / "cfg")
    cfg = config.load_config()
    _pending_scheduler_job(JobState.RUNNING)
    _no_sleep(monkeypatch)
    _install_shell_stub(monkeypatch)
    phases = {"123.cluster": SchedulerPhase.RUNNING}
    monkeypatch.setattr("vq.admin._poll_scheduler_phases", _phase_stub(phases))

    def finish_on_first_sleep(_seconds: float) -> None:
        phases["123.cluster"] = SchedulerPhase.FINISHED

    monkeypatch.setattr(
        "vq.admin._scheduler_drain_wait_sleep", finish_on_first_sleep
    )

    def helper(host_cfg, *vq_args, **kwargs):  # type: ignore[no-untyped-def]
        if vq_args == ("--version",):
            return _ok("vq 0.12.0\n")
        if vq_args == ("source-tree-sha256",):
            return _ok(f"{_TREE_SHA256}\n")
        return _ok(f"{_DRIVER_SHA}\n")

    monkeypatch.setattr("vq.admin.transport.run_remote_vq", helper)

    result = admin.update_scheduler_host("host_f", cfg, drain_wait_seconds=3600)

    assert result.success is True
    assert drain.read_effective_drain_state() is None
