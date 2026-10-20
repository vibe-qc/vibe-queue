"""#168: GPU allocation for scheduler jobs.

`vq submit TARGET --gpus N` records the request on the job and renders it as
the scheduler's own directive (Torque ``nodes=1:ppn=C:gpus=N``, Slurm
``--gpus=N``, a job total either way). A daemon host has no GPU admission,
so it refuses the option rather than ignoring it. The console shows the
allocation; nothing measures GPU use yet. An unset request leaves every
existing plan, wire argument, call and idempotency hash as it was.
"""
from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import resubmit, status
from vq import submit as submit_module
from vq.cli import main
from vq.listing import format_table
from vq.scheduler_dialect import ResourceRequest, SlurmDialect, TorqueDialect
from vq.spec import JobSpec, JobState
from vq.web import calculations, view_context

sys.path.insert(0, str(Path(__file__).parent))
from test_daemon_scheduler import (  # noqa: E402
    MockDispatcher,
    _inject,
    _submit_scheduler,
    daemon,  # noqa: F401 -- pytest fixture
)
from test_scheduler_dispatch import (  # noqa: E402
    FakeRunner,
    _qsub_ok,
    make_dispatcher,
    make_slurm_dispatcher,
)
from test_submit_plan_contract import PlanHarness, plan_harness  # noqa: E402, F401


class TestDirectives:
    def test_torque_puts_them_on_the_jobs_node(self) -> None:
        assert "-l nodes=1:ppn=4:gpus=2" in TorqueDialect().resource_directives(
            ResourceRequest(cpus=4, gpus=2))
        assert "-l nodes=1:ppn=4" in TorqueDialect().resource_directives(ResourceRequest(cpus=4))

    def test_slurm_asks_for_a_job_total(self) -> None:
        directives = SlurmDialect().resource_directives(ResourceRequest(cpus=4, gpus=2))
        assert "--gpus=2" in directives
        assert not any(d.startswith("--gpus") for d in
                       SlurmDialect().resource_directives(ResourceRequest(cpus=4)))

    @pytest.mark.parametrize(("make", "directive"), [
        (make_dispatcher, "#PBS -l nodes=1:ppn=4:gpus=2"),
        (make_slurm_dispatcher, "#SBATCH --gpus=2"),
    ])
    def test_the_job_script_carries_them(self, make, directive: str) -> None:  # type: ignore[no-untyped-def]
        script = make(FakeRunner()).build_job_script(
            job_id="abc", command=["true"], remote_workspace="/cluster/ws/abc", cpus=4, gpus=2)
        assert directive in script.splitlines()


    def test_a_submission_writes_them_into_its_script(self) -> None:
        runner = FakeRunner(responder=_qsub_ok())
        make_dispatcher(runner).submit(job_id="job1", command=["true"], cpus=4, gpus=2)
        script = runner.first("sh").stdin
        assert script is not None and "#PBS -l nodes=1:ppn=4:gpus=2" in script.splitlines()


class TestDaemon:
    def test_a_requested_count_reaches_the_dispatcher_and_the_job(
        self, daemon,  # noqa: F811
    ) -> None:
        from vq.daemon import _vq_job_env
        from vq.scheduler_dialect import SchedulerPhase

        mock = MockDispatcher(phase=SchedulerPhase.RUNNING)
        _inject(daemon, mock)
        spec = _submit_scheduler(daemon, "j1")
        spec.gpus = 2
        spec.write(daemon._spec_path("j1"))
        daemon._start_scheduler_job(spec)
        assert mock.submit_kwargs[-1]["gpus"] == 2
        assert _vq_job_env(spec, workdir="/w")["VQ_GPUS"] == "2"

    def test_without_a_request_the_call_is_as_before(self, daemon) -> None:  # noqa: F811
        from vq.daemon import _vq_job_env
        from vq.scheduler_dialect import SchedulerPhase

        mock = MockDispatcher(phase=SchedulerPhase.RUNNING)
        _inject(daemon, mock)
        spec = _submit_scheduler(daemon, "j1")
        daemon._start_scheduler_job(spec)
        assert "gpus" not in mock.submit_kwargs[-1]
        assert "VQ_GPUS" not in _vq_job_env(spec, workdir="/w")


class TestSubmit:
    def test_a_scheduler_target_records_the_request(self, plan_harness: PlanHarness) -> None:  # noqa: F811
        source = plan_harness.root / "gpu.py"
        source.write_text("pass\n", encoding="utf-8")
        result = CliRunner().invoke(main, [
            "submit", "--host", "local-host_f", "--gpus", "2", "--python",
            "/local-cluster/runtime/python", "--json", str(source)])
        assert result.exit_code == 0, result.output
        ((kind, kwargs),) = plan_harness.calls
        assert (kind, kwargs["gpus"], kwargs["scheduler_target"]) == ("local", 2, "local-host_f")

    def test_without_the_option_the_plan_is_as_before(self, plan_harness: PlanHarness) -> None:  # noqa: F811
        source = plan_harness.root / "plain.py"
        source.write_text("pass\n", encoding="utf-8")
        result = CliRunner().invoke(main, [
            "submit", "--host", "local-host_f", "--python", "/local-cluster/runtime/python",
            "--json", str(source)])
        assert result.exit_code == 0, result.output
        assert "gpus" not in plan_harness.calls[0][1]

    def test_a_daemon_host_refuses_it(self, plan_harness: PlanHarness) -> None:  # noqa: F811
        source = plan_harness.root / "gpu.py"
        source.write_text("pass\n", encoding="utf-8")
        result = CliRunner().invoke(main, ["submit", "--host", "localhost", "--gpus", "1",
                                           str(source)])
        assert result.exit_code != 0
        assert "--gpus is supported only for scheduler targets" in result.output
        assert plan_harness.calls == []

    def test_the_spec_keeps_it(self, tmp_path: Path) -> None:
        source = tmp_path / "run.py"
        source.write_text("pass\n")
        jobid = submit_module.submit_local(
            host="localhost", input_file=str(source), python="/cluster/bin/python", gpus=2,
            scheduler_target="host_f",
            queue_dir=tmp_path / "queue", jobs_dir=tmp_path / "jobs")
        assert JobSpec.read(tmp_path / "queue" / f"{jobid}.json").gpus == 2

    def test_the_remote_wire_carries_it_only_when_set(self) -> None:
        common = dict(
            remote_tar="/tmp/x.tar", remote_input_file=None, remote_command=["true"], cpus=4,
            scheduler_tasks=None, mem_mb=None, wall_time_seconds=None, priority=0,
            auto_resume=False, retry=0, job_name=None, branch=None, program=None,
            expected_sha=None, tags=None, not_before=None, depends_on=None, depends_on_any=None,
            clean_workdir_on_terminal=False, array=1, chain=1, rerun_until_file_exists=None,
            rerun_max=10, refresh_before=None, scheduler_target="host_f", qvf_force=False)
        with_gpus = submit_module._build_remote_submit_argv(**common, gpus=2)
        assert with_gpus[with_gpus.index("--gpus") + 1] == "2"
        assert "--gpus" not in submit_module._build_remote_submit_argv(**common)

    def test_it_is_part_of_the_keyed_intent_only_when_set(self, tmp_path: Path) -> None:
        def digest(**fields: object) -> str:
            spec = JobSpec(id="abc", command=["true"], cwd=str(tmp_path), cpus=4,
                           scheduler_target="host_f", **fields)
            return submit_module._submission_intent_digest(
                spec, payload_kind="file", payload_digest="0" * 64, vibeqc_preflight=False)

        assert digest(gpus=2) != digest(gpus=1) != digest()
        # Without a request the intent hashes exactly as before the field
        # existed (computed with the previous code), so keys claimed by an
        # older vq replay instead of conflicting.
        spec = JobSpec(id="abc", command=["true"], cwd="/ws", cpus=4, scheduler_target="host_f")
        assert submit_module._submission_intent_digest(
            spec, payload_kind="file", payload_digest="0" * 64, vibeqc_preflight=False,
        ) == "eda89c1ba8023e2d7cb89df52737afc21c239b80870e248be42a074a7cff2db8"


def test_resubmit_inherits_it(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from vq import config, paths

    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    queue, jobs = paths.queue_dir(), paths.jobs_dir()
    queue.mkdir(parents=True)
    (jobs / "src000000001").mkdir(parents=True)
    JobSpec(id="src000000001", command=["true"], cwd=str(jobs / "src000000001"), cpus=4,
            gpus=2, scheduler_target="host_f", state=JobState.FAILED).write(
        queue / "src000000001.json")
    new_id = resubmit.resubmit_local("src000000001")
    assert JobSpec.read(queue / f"{new_id}.json").gpus == 2


def test_listing_and_status_show_it(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    gpu = JobSpec(id="aaa000000001", command=["true"], cwd=str(tmp_path), cpus=4, gpus=2,
                  scheduler_target="host_f", submitted_at=datetime.now(UTC).isoformat())
    plain = JobSpec(id="aaa000000002", command=["true"], cwd=str(tmp_path), cpus=4,
                    submitted_at=datetime.now(UTC).isoformat())
    table = format_table([gpu, plain])
    assert "GPUS" in table.splitlines()[0]
    assert "GPUS" not in format_table([plain]).splitlines()[0]
    queue = tmp_path / "queue"
    queue.mkdir()
    gpu.write(queue / f"{gpu.id}.json")
    monkeypatch.setattr(status.rpc, "request_scheduler_status_refresh", lambda *a, **k: None)
    assert "gpus:         2" in status.show_status("localhost", gpu.id, queue_dir=queue)


class TestConsole:
    def _row(self, state: str, gpus: int | None) -> dict[str, object]:
        return {"id": f"j-{state}-{gpus}", "queue_host": "host_f", "state": state,
                "effective_state": state, "scheduler_target": "host_f", "cpus": 4,
                "gpus": gpus}

    def test_the_summary_counts_running_allocation_only(self) -> None:
        rows = [self._row("running", 2), self._row("running", None), self._row("pending", 4),
                self._row("completed", 8)]
        summary = calculations.calculation_summary(rows, now=datetime.now(UTC))
        assert summary["allocated_gpus"] == 2
        assert view_context.fleet_row_view(rows[0])["gpus"] == 2

    def test_the_table_shows_it(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        from fastapi.testclient import TestClient

        from vq import auth, config, paths
        from vq.overview import HostOverview
        from vq.web import create_app
        from vq.web import fleet as fleet_mod

        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
        monkeypatch.delenv(auth.ENV_WEB_TOKEN_FILE, raising=False)
        monkeypatch.setenv("VQ_WEB_FLEET", "1")
        paths.queue_dir().mkdir(parents=True, exist_ok=True)
        app = create_app()
        app.state.fleet_cache.set_snapshot(fleet_mod.FleetSnapshot(
            gathered_at=datetime.now(UTC).isoformat(), duration_seconds=1.0,
            hosts=[fleet_mod.HostSnapshot(host="host_f", overview=HostOverview(host="host_f"),
                                          jobs=[self._row("running", 2)])]))
        html = TestClient(app).get("/fleet/jobs/_table").text
        assert ">+2 gpu</span>" in html
        assert '<span class="tile-value">2</span><span class="tile-label">gpus allocated</span>' \
            in html


def test_the_option_is_documented_in_help() -> None:
    result = CliRunner().invoke(main, ["submit", "--help"])
    assert "--gpus N" in result.output and "refused there rather" in json.dumps(result.output)
