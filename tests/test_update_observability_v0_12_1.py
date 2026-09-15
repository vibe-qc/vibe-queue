"""Feedback an agent chat gets from vq when it submits, and when it updates.

The 2026-07-22 fleet update was opaque in both directions. A two-hour
``vq admin update`` printed nothing while it ran and discarded all but the last
80 lines of what it did; the scheduler lanes kept nothing at all. A ``vq
submit`` answered with a bare 12-hex id that could not distinguish a job about
to run from one parked behind a drain.

These pin the narration channel, the per-update transcript, and the submit
receipt.
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

# Cross-test imports (e.g. `from tests.test_scheduler_dispatch import ...`)
# need the parent of the tests directory on sys.path at collection time.
_tests_root = str(Path(__file__).resolve().parent.parent)
if _tests_root not in sys.path:
    sys.path.insert(0, _tests_root)

import pytest  # noqa: E402
from click.testing import CliRunner  # noqa: E402

from vq import admin, config, drain, output, paths  # noqa: E402
from vq.cli import main  # noqa: E402
from vq.scheduler_dialect import SchedulerPhase  # noqa: E402
from vq.spec import JobSpec, JobState  # noqa: E402

_DRIVER_SHA = "a" * 40
_TREE_SHA256 = "b" * 64


@pytest.fixture
def state_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    paths.jobs_dir().mkdir(parents=True, exist_ok=True)

    def fake_stage(host, host_cfg, command_host_cfg, result, *, expected_sha=None):  # type: ignore[no-untyped-def]
        result.stage_root = "/shared/vq-admin/host_f"
        result.stage_path = f"{result.stage_root}/generations/{_DRIVER_SHA}-x"
        result.stage_uploaded = True
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
    return tmp_path


def _scheduler_config(cfg_dir: Path) -> None:
    (cfg_dir / "config.toml").write_text(
        "\n".join(
            [
                'default_host = "localhost"',
                "",
                "[hosts.localhost]",
                'ssh = "localhost"',
                "",
                "[hosts.host_f]",
                'ssh = "host_f-login"',
                'scheduler = "pbs"',
                'scheduler_dialect = "torque"',
                'scratch_root = "/home/USER"',
                'scheduler_driver = "localhost"',
                'fleet_role = "managed"',
                'scheduler_update_command = "/site/update_cluster.sh"',
                "",
            ]
        )
    )


def _ok(stdout: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(args=[], returncode=0, stdout=stdout, stderr="")


# ----------------------------------------------------------------------
# The narration channel
# ----------------------------------------------------------------------


def test_narrate_is_a_noop_without_a_channel() -> None:
    """A library module must stay callable with nobody listening."""
    output.narrate("this goes nowhere")  # must not raise


def test_narration_reaches_an_installed_sink() -> None:
    seen: list[str] = []
    with output.channel(sink=seen.append):
        output.narrate("hello")
    assert seen == ["hello"]
    # Restored on exit.
    output.narrate("ignored")
    assert seen == ["hello"]


def test_channel_filters_by_level() -> None:
    seen: list[str] = []
    with output.channel(sink=seen.append, level=output.Level.QUIET):
        output.narrate("milestone", output.Level.QUIET)
        output.narrate("detail", output.Level.VERBOSE)
    assert seen == ["milestone"]


def test_run_log_records_lines_the_terminal_filtered_out(tmp_path: Path) -> None:
    """The transcript is for someone who was NOT watching and cannot re-run."""
    run_log = output.RunLog(tmp_path / "t.log")
    seen: list[str] = []
    with output.channel(sink=seen.append, level=output.Level.QUIET, run_log=run_log):
        output.narrate("milestone", output.Level.QUIET)
        output.narrate("detail", output.Level.VERBOSE)
    run_log.close()
    text = (tmp_path / "t.log").read_text()
    assert seen == ["milestone"]
    assert "milestone" in text
    assert "detail" in text


def test_run_log_survives_an_unwritable_path(tmp_path: Path) -> None:
    """A deploy must never fail because its transcript could not be opened."""
    blocker = tmp_path / "blocker"
    blocker.write_text("not a directory")
    run_log = output.RunLog(blocker / "sub" / "t.log")
    run_log.write("still fine")  # must not raise
    run_log.close()


def test_terminal_narration_is_off_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    """Library code must not print; only the CLI may turn the terminal on."""
    monkeypatch.setattr(output, "_terminal_enabled", False)
    ch = output.Channel()
    assert ch._sink is output._null_sink


def test_resolve_level_falls_back_on_a_typo(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(output.ENV_OUTPUT_LEVEL, "VERBOSE")
    assert output.resolve_level() == output.Level.VERBOSE
    monkeypatch.setenv(output.ENV_OUTPUT_LEVEL, "LOUD")
    assert output.resolve_level() == output.Level.NORMAL


# ----------------------------------------------------------------------
# Per-update transcripts
# ----------------------------------------------------------------------


def test_scheduler_update_writes_a_transcript(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The full narrative of an update survives the process that ran it.

    Previously the scheduler lanes persisted nothing: host_f/host_c deploy
    output was unrecoverable the moment the command returned.
    """
    _scheduler_config(state_dir / "cfg")
    cfg = config.load_config()
    monkeypatch.setattr("vq.admin.SCHEDULER_HELPER_READINESS_INTERVAL_SECONDS", 0)
    monkeypatch.setattr("vq.admin.SCHEDULER_HELPER_ACTIVATION_INTERVAL_SECONDS", 0)
    monkeypatch.setattr(
        "vq.admin.transport.run_remote_shell", lambda *a, **k: _ok("installed\n")
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
    assert result.run_log_path is not None
    transcript = Path(result.run_log_path)
    assert transcript.is_file()
    text = transcript.read_text()
    # Header identifies the operation without needing the surrounding context.
    assert "admin update host_f" in text
    assert "# started:" in text
    assert "# finished:" in text
    # Phase narration is the spine of the timeline.
    assert "[paused]" in text
    assert "[building]" in text
    # The command's own output is teed into the transcript. The scheduler
    # lanes buffer through run_remote_shell rather than streaming, so this
    # block is the only durable copy of what the update command said —
    # without it a failed deploy reads back as a bare rc under
    # `vq admin logs`.
    assert "--- scheduler update command output (rc=0) ---" in text
    assert "installed" in text


def test_failed_runtime_deploy_output_reaches_the_transcript(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed deploy's own words survive into ``vq admin logs``.

    host_f's v0.15.52 deploy failed with *"expected commit is absent from
    authoritative mirror"* on the build host's stderr — but the transcript
    recorded only ``deployment command rc=1``, and diagnosing it meant
    re-running the deploy. The captured output must be teed into the run
    log, not just parked on the result object for the formatter.
    """
    (state_dir / "cfg" / "config.toml").write_text(
        "\n".join(
            [
                'default_host = "localhost"',
                "",
                "[hosts.localhost]",
                'ssh = "localhost"',
                "",
                "[hosts.host_f]",
                'ssh = "host_f-login"',
                'scheduler = "pbs"',
                'scheduler_dialect = "torque"',
                'scratch_root = "/home/USER"',
                'scheduler_driver = "localhost"',
                'fleet_role = "managed"',
                "",
                "[hosts.host_f.scheduler_runtime_deployments.vibeqc-release]",
                'update_command = "/site/bin/deploy-runtime"',
                'verify_command = "/site/bin/verify-runtime"',
                'update_host = "cluster-build"',
                "",
                "[programs.vibeqc-release]",
                'kind = "binary"',
                'binary = "/opt/vibeqc/bin/vibeqc"',
                "",
            ]
        )
    )
    cfg = config.load_config()
    monkeypatch.setattr(
        "vq.admin.transport.run_remote_shell",
        lambda *a, **k: subprocess.CompletedProcess(
            args=[],
            returncode=1,
            stdout="",
            stderr="expected commit is absent from authoritative mirror\n",
        ),
    )

    result = admin.update_scheduler_runtime(
        "host_f", "vibeqc-release", cfg, expected_sha="c" * 40
    )

    assert result.success is False
    assert result.run_log_path is not None
    text = Path(result.run_log_path).read_text()
    assert "--- deployment command output (rc=1) ---" in text
    assert "expected commit is absent from authoritative mirror" in text
    # Failed run: the marker is sticky by design; clean it so this test
    # leaves no state behind for siblings sharing the tmp fixture.
    admin.clear_admin_update_marker()


def test_transcripts_are_pruned_to_a_bound(state_dir: Path) -> None:
    """A new unbounded growth surface would be a regression, not a feature."""
    target_dir = paths.admin_update_log_target_dir("vibeqc-dev")
    target_dir.mkdir(parents=True, exist_ok=True)
    for i in range(25):
        (target_dir / f"2026-07-{i:02d}T00-00-00.log").write_text("x")
    removed = paths.prune_admin_update_logs("vibeqc-dev", keep=20)
    assert len(removed) == 5
    assert len(list(target_dir.glob("*.log"))) == 20


def test_pruning_one_target_never_touches_another(state_dir: Path) -> None:
    """A suffix glob made `vibeqc-dev` also match — and DELETE — `host_f-vibeqc-dev`.

    Retention for one target silently destroying another target's transcripts
    is the worst possible bug in a forensic log, so this is pinned explicitly.
    """
    victim = paths.admin_update_log_target_dir("host_f-vibeqc-dev")
    victim.mkdir(parents=True, exist_ok=True)
    for i in range(25):
        (victim / f"2026-07-{i:02d}T00-00-00.log").write_text("keep me")
    mine = paths.admin_update_log_target_dir("vibeqc-dev")
    mine.mkdir(parents=True, exist_ok=True)
    (mine / "2026-07-01T00-00-00.log").write_text("x")

    removed = paths.prune_admin_update_logs("vibeqc-dev", keep=20)

    assert removed == []
    assert len(list(victim.glob("*.log"))) == 25


def test_transcript_filename_cannot_escape_the_log_dir(state_dir: Path) -> None:
    """The target name reaches this from config; it must not be a path."""
    path = paths.admin_update_logfile("../../etc/passwd", "2026-07-23T00:00:00")
    # Containment: every separator collapses to "-", so both the target
    # directory and the filename are single components under the log root.
    root = paths.admin_update_log_dir().resolve()
    assert "/" not in path.name
    assert path.parent.parent.resolve() == root
    assert root in path.resolve().parents


def test_admin_logs_reads_back_the_newest_transcript(state_dir: Path) -> None:
    _scheduler_config(state_dir / "cfg")
    target_dir = paths.admin_update_log_target_dir("vibeqc-dev")
    target_dir.mkdir(parents=True, exist_ok=True)
    (target_dir / "2026-07-22T10-00-00.log").write_text("older run\n")
    (target_dir / "2026-07-23T10-00-00.log").write_text("newer run\n")

    result = CliRunner().invoke(main, ["admin", "logs", "vibeqc-dev"])

    assert result.exit_code == 0, result.output
    assert "newer run" in result.stdout
    assert "older run" not in result.stdout


def test_admin_logs_lists_what_is_available(state_dir: Path) -> None:
    _scheduler_config(state_dir / "cfg")
    target_dir = paths.admin_update_log_target_dir("vibeqc-dev")
    target_dir.mkdir(parents=True, exist_ok=True)
    (target_dir / "2026-07-23T10-00-00.log").write_text("run\n")

    result = CliRunner().invoke(main, ["admin", "logs", "--list", "--json"])

    assert result.exit_code == 0, result.output
    rows = json.loads(result.stdout)
    assert len(rows) == 1
    # The target is the directory name, so it is exact rather than parsed back
    # out of a filename (which produced a mangled value).
    assert rows[0]["target"] == "vibeqc-dev"
    assert rows[0]["started_at"] == "2026-07-23T10-00-00"


def test_admin_logs_says_so_when_there_is_nothing(state_dir: Path) -> None:
    """A missing transcript must not look like an empty one."""
    _scheduler_config(state_dir / "cfg")

    result = CliRunner().invoke(main, ["admin", "logs", "vibeqc-dev"])

    assert result.exit_code != 0
    assert "no admin update transcript" in result.output


def test_admin_logs_follows_the_scheduler_driver(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A scheduler host's transcript lives on its driver, like its marker."""
    (state_dir / "cfg" / "config.toml").write_text(
        "\n".join(
            [
                "[hosts.driver]",
                'ssh = "driver"',
                "",
                "[hosts.host_f]",
                'ssh = "host_f-login"',
                'scheduler = "pbs"',
                'scheduler_dialect = "torque"',
                'scratch_root = "/home/USER"',
                'scheduler_driver = "driver"',
                "",
            ]
        )
    )
    captured: dict[str, object] = {}

    def fake_delegate(host, cfg, *args, **kwargs):  # type: ignore[no-untyped-def]
        captured["host"] = host
        captured["argv"] = list(args)
        return "transcript\n"

    monkeypatch.setattr("vq.cli._delegate_to_remote", fake_delegate)

    result = CliRunner().invoke(main, ["admin", "logs", "host_f", "--host", "host_f"])

    assert result.exit_code == 0, result.output
    assert captured["host"] == "driver"
    assert captured["argv"] == ["admin", "logs", "host_f", "--host", "localhost"]


def test_admin_logs_without_a_target_delegates_correctly(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """HOST is an option so an omitted TARGET cannot swallow the host name.

    With two positionals, `vq admin logs --host host_a` sent "localhost" into the
    TARGET slot, so any host whose default_host is remote reported zero
    transcripts.
    """
    (state_dir / "cfg" / "config.toml").write_text(
        'default_host = "host_a"\n\n[hosts.host_a]\nssh = "host_a"\n'
    )
    captured: dict[str, object] = {}

    def fake_delegate(host, cfg, *args, **kwargs):  # type: ignore[no-untyped-def]
        captured["argv"] = list(args)
        return "transcript\n"

    monkeypatch.setattr("vq.cli._delegate_to_remote", fake_delegate)

    result = CliRunner().invoke(main, ["admin", "logs"])

    assert result.exit_code == 0, result.output
    assert captured["argv"] == ["admin", "logs", "--host", "localhost"]


# ----------------------------------------------------------------------
# Submit receipt
# ----------------------------------------------------------------------


def _local_config(cfg_dir: Path) -> None:
    (cfg_dir / "config.toml").write_text(
        'default_host = "localhost"\n[hosts.localhost]\nssh = "localhost"\n'
    )


def test_submit_stdout_is_still_exactly_the_jobid(
    state_dir: Path, tmp_path: Path
) -> None:
    """The bare-jobid contract every wrapper in the fleet parses is unchanged."""
    _local_config(state_dir / "cfg")
    script = tmp_path / "j.py"
    script.write_text("print(1)\n")

    result = CliRunner().invoke(main, ["submit", "localhost", str(script)])

    assert result.exit_code == 0, result.output
    assert len(result.stdout.strip().splitlines()) == 1
    assert len(result.stdout.strip()) == 12


def test_submit_json_receipt_names_the_holds(
    state_dir: Path, tmp_path: Path
) -> None:
    """A bare id cannot distinguish "about to run" from "parked behind a drain"."""
    _local_config(state_dir / "cfg")
    script = tmp_path / "j.py"
    script.write_text("print(1)\n")
    drain.write_drain_state(drain.DrainState(enabled=True), via_rpc=False)

    result = CliRunner().invoke(
        main, ["submit", "localhost", "--json", str(script)]
    )

    assert result.exit_code == 0, result.output
    receipt = json.loads(result.stdout)
    assert len(receipt["jobids"]) == 1
    assert receipt["host"] == "localhost"
    assert any("full drain" in h for h in receipt["dispatch_holds"])
    assert receipt["next"][0].startswith("vq status localhost ")


def test_submit_json_receipt_is_clean_when_nothing_holds(
    state_dir: Path, tmp_path: Path
) -> None:
    _local_config(state_dir / "cfg")
    script = tmp_path / "j.py"
    script.write_text("print(1)\n")

    result = CliRunner().invoke(
        main, ["submit", "localhost", "--json", str(script)]
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["dispatch_holds"] == []


def test_submit_receipt_reports_an_admin_update_in_flight(
    state_dir: Path, tmp_path: Path
) -> None:
    """The most confusing PENDING of all: the queue is mid-rebuild."""
    _local_config(state_dir / "cfg")
    script = tmp_path / "j.py"
    script.write_text("print(1)\n")
    admin.acquire_admin_update_marker(envs=["vibeqc-dev"], host="localhost")

    result = CliRunner().invoke(
        main, ["submit", "localhost", "--json", str(script)]
    )

    assert result.exit_code == 0, result.output
    holds = json.loads(result.stdout)["dispatch_holds"]
    assert any("admin update is in progress" in h for h in holds)


def test_submit_receipt_never_breaks_a_submit(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Best-effort means best-effort."""
    _local_config(state_dir / "cfg")
    cfg = config.load_config()
    monkeypatch.setattr(
        "vq.cli.drain_module.read_drain_state",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    from vq.cli import _submit_receipt

    receipt = _submit_receipt(cfg, "localhost", ["abcdef123456"])

    assert receipt["jobids"] == ["abcdef123456"]
    assert receipt["dispatch_holds"] == []


# ----------------------------------------------------------------------
# Dispatch logging — what would have made the double-wrap obvious
# ----------------------------------------------------------------------


def test_dispatch_logs_the_run_line_before_submitting(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The rendered script never came home until the job was terminal.

    A job that died IN its launcher therefore left nothing locally
    inspectable, which is why ~250 host_f jobs failed identically for a week.
    """
    import logging

    from tests.test_scheduler_dispatch import (
        FakeRunner,
        _qsub_ok,
        make_dispatcher,
    )

    d = make_dispatcher(FakeRunner(responder=_qsub_ok()))
    with caplog.at_level(logging.INFO, logger="vq.scheduler_dispatch"):
        d.submit(job_id="j1", command=["python", "run.py"], cpus=1)

    messages = [r.getMessage() for r in caplog.records]
    assert any("run line: python run.py" in m for m in messages)


def test_effective_command_is_one_source_of_truth() -> None:
    """What gets logged can never drift from what gets run."""
    from tests.test_scheduler_dispatch import FakeRunner, make_dispatcher
    from vq.config import SchedulerProgramHooks

    wrapper = "/site/bin/orcasub"
    d = make_dispatcher(
        FakeRunner(),
        scheduler_program_hooks={
            "orca": SchedulerProgramHooks(command_wrapper=[wrapper])
        },
    )
    composed = d.effective_command(["orca", "in.inp"], "orca")
    script = d.build_job_script(
        job_id="j1",
        command=["orca", "in.inp"],
        remote_workspace=d.remote_workspace("j1"),
        cpus=1,
        program="orca",
    )

    assert composed == [wrapper, "orca", "in.inp"]
    assert f"{wrapper} orca in.inp" in script


def test_pending_scheduler_job_spec_is_not_disturbed(state_dir: Path) -> None:
    """Sanity: the receipt helpers do not mutate queue state."""
    _local_config(state_dir / "cfg")
    spec = JobSpec(
        id="abc123456789",
        command=["true"],
        cwd="/tmp",
        cpus=1,
        state=JobState.PENDING,
    )
    spec.write(paths.queue_dir() / "abc123456789.json")
    cfg = config.load_config()
    from vq.cli import _submit_receipt

    _submit_receipt(cfg, "localhost", ["abc123456789"])

    reread = JobSpec.read(paths.queue_dir() / "abc123456789.json")
    assert reread.state == JobState.PENDING
