"""Operator start fence for an uncertain scheduler attempt (#179).

vq cannot prove that a lost qsub/sbatch was never admitted, but an attempt that
carries a daemon-issued token can be fenced: an exclusive claim in its shared
workspace that the job script must win before it runs anything. These checks
run the real fence script and the real job-side claim block against a local
workspace standing in for the shared filesystem.
"""
from __future__ import annotations

import json
import os
import subprocess
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest
from click.testing import CliRunner

from tests.test_daemon_scheduler import MockDispatcher, _inject, _unconfirmed_scheduler_spec
from tests.test_daemon_scheduler import (
    daemon as scheduler_daemon,  # noqa: F401 — shared daemon fixture
)
from tests.test_scheduler_recovery import evidence as positive_evidence
from vq import config, driver_identity, rpc
from vq import daemon as daemon_mod
from vq import scheduler_recovery as recovery
from vq import scheduler_start_fence as fence
from vq.cli import main as cli
from vq.scheduler_dialect import SchedulerPhase
from vq.spec import JobSpec, JobState


class LocalRunner:
    """Run remote shell argv locally: the workspace stands in for shared storage."""

    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    def run(self, argv, check=False, **kwargs):
        self.calls.append(list(argv))
        done = subprocess.run(argv, capture_output=True, text=True, check=False)
        return SimpleNamespace(returncode=done.returncode, stdout=done.stdout,
                               stderr=done.stderr)


@pytest.fixture
def fenceable(scheduler_daemon, monkeypatch, tmp_path):  # noqa: F811
    daemon = scheduler_daemon
    cfg = config.Config(hosts={"host_f": config.HostConfig(
        ssh="scheduler.example", scheduler="pbs", scheduler_dialect="torque",
        scratch_root=str(tmp_path / "remote"), scheduler_driver="driver",
    ), "driver": config.HostConfig(ssh="driver.example")})
    monkeypatch.setattr(daemon_mod, "load_config", lambda: cfg)
    driver_identity.driver_id()
    workspace = tmp_path / "remote" / "j1"
    workspace.mkdir(parents=True)
    spec = _unconfirmed_scheduler_spec(daemon, "j1", state=JobState.SUBMIT_OUTCOME_UNKNOWN)
    spec.scheduler_remote_workspace = str(workspace)
    spec.write(daemon._spec_path(spec.id))
    token = fence.issue_token("j1", owner_uid=None, multi_user=False, retry_count=0,
                              remote_workspace=str(workspace))
    mock = MockDispatcher(phase=SchedulerPhase.RUNNING)
    mock.runner = LocalRunner()
    _inject(daemon, mock)
    return daemon, spec, mock, workspace, token


def _apply(daemon):
    preview = daemon.scheduler_start_fence("j1")
    assert preview["applied"] is False
    return daemon.scheduler_start_fence(
        "j1", apply=True, expected_plan_digest=preview["plan_digest"],
        operator_uid=os.geteuid(),
    )


def _run_job_claim(workspace: Path, token: str, *, scheduler_id: str = "123.server",
                   script_token: str | None = None) -> bool:
    """Run the real claim block; True when the job would go on to run."""
    marker_dir = workspace / "_vq"
    marker_dir.mkdir(exist_ok=True)
    script = workspace / "job.pbs"
    lines = fence.job_script_lines(
        token=token, workspace=str(workspace), marker_dir=str(marker_dir),
        script_path=str(script), job_id_env="PBS_JOBID",
    )
    staged = fence.job_script_lines(
        token=script_token or token, workspace=str(workspace),
        marker_dir=str(marker_dir), script_path=str(script), job_id_env="PBS_JOBID",
    )
    script.write_text("\n".join(staged) + "\n")
    runnable = workspace / "late.sh"
    runnable.write_text("set -u\n" + "\n".join(lines) + f"\ntouch {workspace}/ran\n")
    (workspace / "ran").unlink(missing_ok=True)
    subprocess.run(["sh", str(runnable)], env={**os.environ, "PBS_JOBID": scheduler_id},
                   capture_output=True, check=False)
    return (workspace / "ran").exists()


def _refusals(workspace: Path) -> list[str]:
    return sorted(p.name for p in (workspace / "_vq").glob("*.refused-start.*"))


def test_fence_settles_the_attempt_and_a_late_start_never_runs(fenceable):
    daemon, spec, mock, workspace, token = fenceable
    other = _unconfirmed_scheduler_spec(daemon, "other", state=JobState.SUBMIT_OUTCOME_UNKNOWN)

    first = _apply(daemon)
    assert JobSpec.read(daemon._spec_path("j1")).state == JobState.SUBMIT_OUTCOME_UNKNOWN
    again = daemon.scheduler_start_fence(
        "j1", apply=True, expected_plan_digest=first["plan_digest"],
        operator_uid=os.geteuid(),
    )
    assert again == {**first, "replayed": True}
    daemon._reattach_or_interrupt_at_startup()

    fenced = JobSpec.read(daemon._spec_path("j1"))
    assert fenced.state == JobState.FAILED
    assert fenced.scheduler_state == fence.FENCED_STATE
    assert fenced.failure_reason == fence.FENCED_REASON
    assert fenced.exit_code is None and fenced.retry_count == spec.retry_count
    assert fenced.program_runtime_pin == spec.program_runtime_pin
    claim = workspace / "_vq" / fence.claim_name(token)
    assert claim.read_text().strip() == first["claim_line"]
    assert mock.submitted == mock.cancelled == []
    untouched = JobSpec.read(daemon._spec_path(other.id))
    assert untouched.state == JobState.SUBMIT_OUTCOME_UNKNOWN
    assert untouched.scheduler_state != fence.FENCED_STATE
    assert fence.read_intent("other", owner_uid=None, multi_user=False, retry_count=0) is None

    # A late admission of the fenced attempt exits before running anything.
    assert _run_job_claim(workspace, token) is False
    assert _refusals(workspace)
    assert not (workspace / "_vq" / "scheduler-job-id").exists()


def test_a_job_that_started_first_wins_and_resumes_from_its_marker(fenceable):
    daemon, _spec, mock, workspace, token = fenceable
    assert _run_job_claim(workspace, token) is True
    _apply(daemon)

    daemon._reattach_or_interrupt_at_startup()
    assert JobSpec.read(daemon._spec_path("j1")).state == JobState.SUBMIT_OUTCOME_UNKNOWN
    result = fence._read(fence._private_path("j1", "result", owner_uid=None,
                                             multi_user=False, retry_count=0))
    assert result["outcome"] == "started" and result["scheduler_job_id"] == "123.server"

    mock.recorded = "123.server"  # the job-start marker the started job wrote
    daemon._reattach_or_interrupt_at_startup()
    resumed = JobSpec.read(daemon._spec_path("j1"))
    assert resumed.state == JobState.RUNNING and resumed.scheduler_job_id == "123.server"
    assert mock.submitted == mock.cancelled == []


def test_a_rerun_of_the_same_admitted_job_still_runs(tmp_path):
    token = fence.new_token()
    assert _run_job_claim(tmp_path, token, scheduler_id="7.server") is True
    assert _run_job_claim(tmp_path, token, scheduler_id="7.server") is True
    # A second admission of the same attempt under another id never runs.
    assert _run_job_claim(tmp_path, token, scheduler_id="8.server") is False


def test_a_late_older_attempt_cannot_run_a_newer_attempts_payload(tmp_path):
    old = fence.new_token()
    assert _run_job_claim(tmp_path, old, script_token=fence.new_token()) is False
    assert any("superseded" in (tmp_path / "_vq" / name).read_text()
               for name in _refusals(tmp_path))


def test_the_claim_race_has_exactly_one_winner(tmp_path):
    for round_ in range(20):
        workspace = tmp_path / f"w{round_}"
        workspace.mkdir()
        token = fence.new_token()
        intent = {"token": token, "claim_line": f"fenced {token} {'f' * 32}",
                  "binding": {"intent": {"scheduler_remote_workspace": str(workspace)}}}
        replies: list[str] = []
        ran: list[bool] = []

        def operator(intent=intent, replies=replies):
            replies.append(LocalRunner().run(fence.fence_argv(intent)).stdout)

        def job(workspace=workspace, token=token, ran=ran):
            ran.append(_run_job_claim(workspace, token))

        threads = [threading.Thread(target=operator), threading.Thread(target=job)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        outcome = fence.interpret(intent, replies[0])["outcome"]
        assert (outcome == "fenced") != ran[0], (outcome, ran)


def test_a_legacy_attempt_without_a_token_cannot_be_fenced(fenceable):
    daemon, _spec, _mock, _workspace, _token = fenceable
    fence._private_path("j1", "token", owner_uid=None, multi_user=False,
                        retry_count=0).unlink()
    with pytest.raises(ValueError, match="predates the start-fence protocol"):
        daemon.scheduler_start_fence("j1")


def test_a_token_for_another_workspace_is_refused(fenceable):
    daemon, _spec, _mock, _workspace, _token = fenceable
    path = fence._private_path("j1", "token", owner_uid=None, multi_user=False, retry_count=0)
    record = json.loads(path.read_text())
    path.unlink()
    fence._create(path, {**record, "remote_workspace": "/elsewhere"})
    with pytest.raises(ValueError, match="does not match the original attempt"):
        daemon.scheduler_start_fence("j1")


def test_a_recorded_fence_blocks_positive_import(fenceable):
    daemon, _spec, _mock, _workspace, _token = fenceable
    data = positive_evidence(daemon.scheduler_submit_recovery("j1"))
    _apply(daemon)
    with pytest.raises(ValueError, match="start fence is already recorded"):
        daemon.scheduler_submit_recovery("j1", data)


def test_a_recorded_positive_import_blocks_a_fence(fenceable, monkeypatch):
    daemon, _spec, _mock, _workspace, _token = fenceable
    monkeypatch.setattr(recovery, "read_record", lambda *a, **k: {"plan_digest": "x"})
    with pytest.raises(ValueError, match="positive scheduler evidence"):
        daemon.scheduler_start_fence("j1")


def test_foreign_claim_content_is_preserved_as_conflict(fenceable):
    daemon, _spec, mock, workspace, token = fenceable
    (workspace / "_vq").mkdir()
    (workspace / "_vq" / fence.claim_name(token)).write_text("something else\n")
    _apply(daemon)
    daemon._reattach_or_interrupt_at_startup()
    current = JobSpec.read(daemon._spec_path("j1"))
    assert current.state == JobState.SUBMIT_OUTCOME_UNKNOWN
    assert current.scheduler_state == "submit_evidence_conflict"
    assert (workspace / "_vq" / fence.claim_name(token)).read_text() == "something else\n"
    assert mock.submitted == mock.cancelled == []


def test_an_unsafe_workspace_or_lost_host_stays_unknown(fenceable, tmp_path):
    daemon, _spec, mock, workspace, _token = fenceable
    (tmp_path / "elsewhere").mkdir()
    (workspace / "_vq").symlink_to(tmp_path / "elsewhere")
    _apply(daemon)
    daemon._reattach_or_interrupt_at_startup()
    assert JobSpec.read(daemon._spec_path("j1")).state == JobState.SUBMIT_OUTCOME_UNKNOWN
    assert not list((tmp_path / "elsewhere").iterdir())

    mock.runner = SimpleNamespace(run=lambda *a, **k: (_ for _ in ()).throw(OSError("ssh")))
    daemon._reattach_or_interrupt_at_startup()
    assert JobSpec.read(daemon._spec_path("j1")).state == JobState.SUBMIT_OUTCOME_UNKNOWN


def test_a_claim_placed_before_a_crash_is_adopted_not_repeated(fenceable):
    daemon, _spec, _mock, workspace, token = fenceable
    applied = _apply(daemon)
    (workspace / "_vq").mkdir()
    (workspace / "_vq" / fence.claim_name(token)).write_text(applied["claim_line"] + "\n")
    daemon._reattach_or_interrupt_at_startup()
    assert JobSpec.read(daemon._spec_path("j1")).scheduler_state == fence.FENCED_STATE
    result = fence._read(fence._private_path("j1", "result", owner_uid=None,
                                             multi_user=False, retry_count=0))
    assert result["outcome"] == "fenced" and result["created"] is False


def test_the_daemon_issues_the_token_before_submitting(scheduler_daemon, monkeypatch):  # noqa: F811
    from tests.test_daemon_scheduler import _submit_scheduler

    daemon = scheduler_daemon
    mock = MockDispatcher(phase=SchedulerPhase.RUNNING)
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "jt")
    daemon._start_scheduler_job(spec)
    record = fence.read_token("jt", owner_uid=None, multi_user=False, retry_count=0)
    assert mock.submitted == ["jt"]
    assert mock.submit_kwargs[0]["start_token"] == record["token"]


def test_build_job_script_claims_before_any_marker_or_command():
    from tests.test_scheduler_dispatch import FakeRunner, make_dispatcher
    from vq.scheduler_dispatch import SchedulerError

    dispatcher = make_dispatcher(FakeRunner())
    token = fence.new_token()
    script = dispatcher.build_job_script(
        job_id="j1", command=["python3", "run.py"], remote_workspace="/w", cpus=1,
        start_token=token,
    )
    claim = script.index(fence.claim_name(token))
    assert claim < script.index("__vq_record_scheduler_id")
    assert claim < script.index("trap ")
    assert claim < script.index("run.py")
    assert f"__vq_start_token={token}" in script.splitlines()
    plain = dispatcher.build_job_script(
        job_id="j1", command=["python3", "run.py"], remote_workspace="/w", cpus=1,
    )
    assert "__vq_start_token" not in plain
    with pytest.raises(SchedulerError, match="single jobs only"):
        dispatcher.build_job_script(
            job_id="j1", command=["true"], remote_workspace="/w", cpus=1,
            array_size=2, start_token=token,
        )


def test_cli_preview_and_apply_use_the_owning_daemon(fenceable, monkeypatch):
    daemon, _spec, mock, _workspace, _token = fenceable
    monkeypatch.setattr(config, "load_config", lambda: config.Config())
    server = rpc.RPCServer(multi_user=False)
    rpc.register_scheduler_start_fence_methods(server, daemon.scheduler_start_fence)
    calls = []

    def local_rpc(method, args, **kwargs):
        calls.append(method)
        return server._methods[method](**args)

    monkeypatch.setattr(rpc, "call", local_rpc)
    runner = CliRunner()
    preview = runner.invoke(cli, ["admin", "reconcile-submit", "j1", "--fence-unstarted"])
    assert preview.exit_code == 0, preview.output
    digest = json.loads(preview.output)["plan_digest"]
    applied = runner.invoke(cli, ["admin", "reconcile-submit", "j1", "--fence-unstarted",
                                  "--apply", "--plan-digest", digest])
    assert applied.exit_code == 0, applied.output
    assert json.loads(applied.output)["applied"]
    assert calls == ["get_scheduler_start_fence", "set_scheduler_start_fence"]
    refused = runner.invoke(cli, ["admin", "reconcile-submit", "j1", "--fence-unstarted",
                                  "--attest-authoritative"])
    assert refused.exit_code == 2
    assert mock.submitted == mock.cancelled == []
