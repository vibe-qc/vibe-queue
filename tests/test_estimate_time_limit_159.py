"""#159: `vq submit --time-from-estimate` takes a job's wall-time limit from a calibrated estimate.

`vq estimate --calibration` applies the matching runtime group too: the
estimate's runtime upper bound times its multiplier, or `uncalibrated` and
why. Opt-in and narrow: only a calibrated runtime gives a limit (rounded up to
whole minutes), never beside an explicit --time-limit/--wall-time-seconds. The
source is recorded on the job and checked where the job is created.
Calibrations are fitted for real.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from vq import config, paths, resubmit, status
from vq import estimate_calibration as cal
from vq import submit as submit_module
from vq.cli import main
from vq.estimate_evaluation import EvaluationError
from vq.resource_estimate import estimate_vibeqc
from vq.spec import JobSpec, JobState

sys.path.insert(0, str(Path(__file__).parent))
from test_estimate_calibration_159 import _dir, _set  # noqa: E402
from test_estimate_evaluation_159 import _case, _estimate, _usage  # noqa: E402
from test_submit_plan_contract import PlanHarness, plan_harness  # noqa: E402, F401

GIB = 1024**3
UPPER = 1000.1  # x1.5 = 1500.15 s, so 26 whole minutes


def _calibration(tmp_path: Path) -> Path:
    """hf on class-a: five completed runs against (10, 100) s, the worst at 150 s."""
    d = _dir(tmp_path / "train")
    cases = [_case(f"r{i}", _estimate(d, f"r{i}", peak=4 * GIB, runtime=(10.0, 100.0)),
                   _usage(d, f"r{i}", rss=GIB, wall=150.0 if i == 0 else 80.0, processes=1))
             for i in range(5)]
    path = tmp_path / "cal.json"
    path.write_text(cal.document_json(cal.fit(_set(d, "train-1", cases), min_cases=5)))
    return path


def _document(tmp_path: Path, payload: Path | None = None, *,
              runtime: tuple[float, float] | None = (60.0, UPPER),
              hardware: str = "class-a") -> dict[str, Any]:
    manifest = tmp_path / "calc.system"
    manifest.write_text('[vibeqc]\nversion = "example"\n[plan]\nmethod = "hf"\n'
                        '[outputs]\nstatus = "dry_run"\n[memory]\nestimate_bytes = 1048576\n')
    receipt = estimate_vibeqc(manifest, inputs=(payload / "run.py",) if payload else ())
    receipt = receipt.model_copy(update={"runtime_seconds": runtime})
    return cal.apply(receipt, cal.load(_calibration(tmp_path)), hardware_class=hardware)


class TestApply:
    def test_the_runtime_group_scales_the_upper_bound(self, tmp_path: Path) -> None:
        document = _document(tmp_path)
        assert {k: document[k] for k in cal._RUNTIME_FIELDS} == {
            "runtime_status": "calibrated", "runtime_reason": None, "runtime_multiplier": 1.5,
            "runtime_multiplier_is_lower_bound": False,
            "calibrated_runtime_upper_seconds": 1500.15}
        assert document["estimate"]["runtime_seconds"] == [60.0, UPPER]  # raw, unchanged
        assert submit_module.wall_time_from_calibrated_estimate(document) == 26 * 60

    @pytest.mark.parametrize(("kwargs", "reason"), [
        ({"runtime": None}, "the estimate has no runtime interval"),
        ({"runtime": (0.0, 0.0)}, "the estimate's runtime upper bound is zero"),
        ({"hardware": "class-b"}, "no runtime group of this calibration matches"),
    ])
    def test_otherwise_the_runtime_is_uncalibrated_and_says_why(
        self, tmp_path: Path, kwargs: dict[str, Any], reason: str,
    ) -> None:
        document = _document(tmp_path, **kwargs)
        assert document["runtime_status"] == "uncalibrated"
        assert document["runtime_reason"].startswith(reason)
        assert (document["runtime_multiplier"], document["calibrated_runtime_upper_seconds"],
                document["runtime_multiplier_is_lower_bound"]) == (None, None, False)
        with pytest.raises(ValueError, match="the runtime is uncalibrated"):
            submit_module.wall_time_from_calibrated_estimate(document)

    def test_too_few_decided_runtime_cases_leave_it_uncalibrated(self, tmp_path: Path) -> None:
        d = _dir(tmp_path / "train")
        cases = [_case("only", _estimate(d, "only", peak=GIB, runtime=(10.0, 100.0)),
                       _usage(d, "only", rss=GIB, wall=80.0, processes=1))]
        calibration = cal.Calibration.model_validate_json(json.dumps(
            cal.fit(_set(d, "t", cases), min_cases=2)))
        manifest = tmp_path / "calc.system"
        manifest.write_text('[vibeqc]\nversion = "example"\n[plan]\nmethod = "hf"\n'
                            '[outputs]\nstatus = "dry_run"\n')
        receipt = estimate_vibeqc(manifest).model_copy(update={"runtime_seconds": (1.0, 2.0)})
        document = cal.apply(receipt, calibration, hardware_class="class-a")
        assert document["runtime_reason"] == (
            "its runtime group has 1 decided case(s), fewer than the calibration's 2")

    def test_a_lower_bound_multiplier_is_flagged(self, tmp_path: Path) -> None:
        d = _dir(tmp_path / "train")
        cases = [_case("done", _estimate(d, "done", peak=GIB, runtime=(10.0, 100.0)),
                       _usage(d, "done", rss=GIB, wall=80.0, processes=1)),
                 _case("limited", _estimate(d, "limited", peak=GIB, runtime=(10.0, 100.0)),
                       None, outcome="time_exceeded", wall_limit_seconds=300)]
        calibration = cal.Calibration.model_validate_json(json.dumps(
            cal.fit(_set(d, "t", cases), min_cases=2)))
        manifest = tmp_path / "calc.system"
        manifest.write_text('[vibeqc]\nversion = "example"\n[plan]\nmethod = "hf"\n'
                            '[outputs]\nstatus = "dry_run"\n')
        receipt = estimate_vibeqc(manifest).model_copy(update={"runtime_seconds": (1.0, 2.0)})
        document = cal.apply(receipt, calibration, hardware_class="class-a")
        assert (document["runtime_multiplier"], document["runtime_multiplier_is_lower_bound"],
                document["calibrated_runtime_upper_seconds"]) == (3.0, True, 6.0)

    def test_a_document_from_before_runtime_groups_were_applied_still_loads(
        self, tmp_path: Path,
    ) -> None:
        document = _document(tmp_path)
        for name in cal._RUNTIME_FIELDS:
            del document[name]
        _receipt, record = cal.check_calibrated(document)
        assert not set(cal._RUNTIME_FIELDS) & set(record)
        with pytest.raises(ValueError, match="has no runtime part"):
            submit_module.wall_time_from_calibrated_estimate(document)

    @pytest.mark.parametrize(("damage", "message"), [
        ({"calibrated_runtime_upper_seconds": 1600.0}, "not the estimate's upper bound times"),
        ({"runtime_reason": "x"}, "not the estimate's upper bound times"),
        ({"runtime_status": None}, "need a runtime_status"),
        ({"runtime_status": "uncalibrated", "runtime_reason": "x"}, "carries no multiplier"),
        ({"runtime_status": "uncalibrated", "runtime_multiplier": None,
          "calibrated_runtime_upper_seconds": None}, "carries no multiplier and says why"),
    ])
    def test_inconsistent_runtime_fields_are_refused(
        self, tmp_path: Path, damage: dict[str, Any], message: str,
    ) -> None:
        with pytest.raises(EvaluationError, match=message):
            cal.check_calibrated({**_document(tmp_path), **damage})

    def test_vq_estimate_names_the_calibrated_runtime(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        calibration = _calibration(tmp_path)
        manifest = tmp_path / "calc.system"
        manifest.write_text('[vibeqc]\nversion = "example"\n[plan]\nmethod = "hf"\n'
                            '[outputs]\nstatus = "dry_run"\n')
        args = ["estimate", str(manifest), "--calibration", str(calibration),
                "--hardware-class", "class-a"]
        result = CliRunner().invoke(main, args)
        assert result.exit_code == 0, result.output
        assert ("Calibrated runtime: uncalibrated (the estimate has no runtime interval)"
                in result.output)
        # The vibe-qc adapter gives no interval yet; one that does is scaled.
        from vq import resource_estimate

        real = resource_estimate.estimate_vibeqc
        monkeypatch.setattr(resource_estimate, "estimate_vibeqc", lambda *a, **k: real(
            *a, **k).model_copy(update={"runtime_seconds": (60.0, UPPER)}))
        result = CliRunner().invoke(main, args)
        assert "Calibrated runtime upper bound: 1500.15 s (x1.5)" in result.output


class TestSubmit:
    @pytest.fixture(autouse=True)
    def _state(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))

    def _files(self, tmp_path: Path, **kwargs: Any) -> tuple[Path, Path, dict[str, Any]]:
        payload = tmp_path / "payload"
        payload.mkdir(exist_ok=True)
        (payload / "run.py").write_text("pass\n")
        document = _document(tmp_path, payload, **kwargs)
        path = tmp_path / "calibrated.json"
        path.write_text(json.dumps(document))
        return payload, path, document

    def _submit(self, *args: str) -> Any:
        return CliRunner().invoke(main, ["submit", "localhost", *args])

    def test_the_limit_and_its_source_are_recorded(self, tmp_path: Path) -> None:
        payload, path, _document_ = self._files(tmp_path)
        result = self._submit("--estimate", str(path), "--time-from-estimate",
                              "-d", str(payload), "--", "true")
        assert result.exit_code == 0, result.output
        jobid = result.output.strip().splitlines()[-1]
        spec = JobSpec.read(paths.spec_path(jobid))
        assert (spec.wall_time_seconds, spec.wall_time_source) == (1560, "calibrated-estimate")
        assert spec.mem_mb is None and spec.mem_mb_source is None
        assert "wall_time:    1560s (from the calibrated estimate)" in (
            status.show_status("localhost", jobid))
        # A resubmission keeps it, unless the limit is overridden.
        spec.state = JobState.COMPLETED
        spec.write(paths.spec_path(jobid))
        kept = JobSpec.read(paths.spec_path(resubmit.resubmit_local(jobid)))
        assert (kept.wall_time_seconds, kept.wall_time_source) == (1560, "calibrated-estimate")
        overridden = JobSpec.read(paths.spec_path(resubmit.resubmit_local(
            jobid, overrides=resubmit.ResubmitOverrides(wall_time_seconds=7200))))
        assert (overridden.wall_time_seconds, overridden.wall_time_source) == (7200, None)

    @pytest.mark.parametrize(("extra", "kwargs", "message"), [
        (["--time-limit", "01:00:00"], {}, "mutually exclusive; an explicit limit stays"),
        (["--wall-time-seconds", "60"], {}, "mutually exclusive; an explicit limit stays"),
        ([], {"hardware": "class-b"}, "the runtime is uncalibrated"),
        ([], {"runtime": None}, "the runtime is uncalibrated"),
    ])
    def test_refusals(self, tmp_path: Path, extra: list[str], kwargs: dict[str, Any],
                      message: str) -> None:
        payload, path, _document_ = self._files(tmp_path, **kwargs)
        result = self._submit("--estimate", str(path), "--time-from-estimate", *extra,
                              "-d", str(payload), "--", "true")
        assert result.exit_code == 2 and message in result.output
        assert not list(paths.queue_dir().glob("*.json"))

    def test_without_an_estimate_or_with_a_raw_one_it_is_refused(self, tmp_path: Path) -> None:
        payload, _path, document = self._files(tmp_path)
        bare = self._submit("--time-from-estimate", "-d", str(payload), "--", "true")
        assert bare.exit_code == 2 and "--time-from-estimate needs --estimate" in bare.output
        raw = tmp_path / "raw.json"
        raw.write_text(json.dumps(document["estimate"]))
        refused = self._submit("--estimate", str(raw), "--time-from-estimate", "-d",
                               str(payload), "--", "true")
        assert refused.exit_code == 2 and "needs a calibrated estimate" in refused.output

    def test_the_wire_form_is_checked_where_the_job_is_created(self, tmp_path: Path) -> None:
        payload, path, document = self._files(tmp_path)
        wire = ["--estimate-json", path.read_text(), "--wall-time-source", "calibrated-estimate"]
        good = self._submit(*wire, "--wall-time-seconds", "1560", "-d", str(payload), "--",
                            "true")
        assert good.exit_code == 0, good.output
        bad = self._submit(*wire, "--wall-time-seconds", "1500", "-d", str(payload), "--",
                           "true")
        assert bad.exit_code != 0
        assert "the wall-time limit is not the calibrated runtime upper bound" in bad.output
        raw = self._submit("--estimate-json", json.dumps(document["estimate"]),
                           "--wall-time-source", "calibrated-estimate",
                           "--wall-time-seconds", "1560", "-d", str(payload), "--", "true")
        assert raw.exit_code != 0 and "needs a calibrated estimate" in raw.output

    def test_submit_local_refuses_an_unknown_source(self, tmp_path: Path) -> None:
        payload, _path, document = self._files(tmp_path)
        with pytest.raises(ValueError, match="unknown wall-time limit source 'guess'"):
            submit_module.submit_local(host="localhost", directory=str(payload),
                                       command=["true"], resource_estimate=document,
                                       wall_time_seconds=60, wall_time_source="guess")

    def test_the_wire_and_intent_carry_it_only_when_set(self) -> None:
        common: dict[str, Any] = dict(
            remote_tar="/tmp/x.tar", remote_input_file=None, remote_command=["true"], cpus=1,
            scheduler_tasks=None, mem_mb=None, wall_time_seconds=1560, priority=0,
            auto_resume=False, retry=0, job_name=None, branch=None, program=None,
            expected_sha=None, tags=None, not_before=None, depends_on=None, depends_on_any=None,
            clean_workdir_on_terminal=False, array=1, chain=1, rerun_until_file_exists=None,
            rerun_max=10, refresh_before=None, scheduler_target=None, qvf_force=False)
        argv = submit_module._build_remote_submit_argv(**common,
                                                       wall_time_source="calibrated-estimate")
        assert argv[argv.index("--wall-time-source") + 1] == "calibrated-estimate"
        assert "--wall-time-source" not in submit_module._build_remote_submit_argv(**common)

        def digest(source: str | None) -> str:
            spec = JobSpec(id="abc", command=["true"], cwd="/ws", cpus=4,
                           wall_time_seconds=1560, scheduler_target="host_f",
                           wall_time_source=source)
            return submit_module._submission_intent_digest(
                spec, payload_kind="file", payload_digest="0" * 64, vibeqc_preflight=False)

        assert digest("calibrated-estimate") != digest(None)


def test_a_job_recovered_after_a_reboot_keeps_its_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from vq.daemon import Daemon

    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    monkeypatch.setattr("vq.daemon._pgroup_alive", lambda pgid: False)
    daemon = Daemon(max_cpus=4, poll_interval=0.05)
    daemon.queue_dir.mkdir(parents=True, exist_ok=True)
    workspace = tmp_path / "jobs" / "rebootjob0002"
    workspace.mkdir(parents=True)
    JobSpec(id="rebootjob0002", command=["true"], cwd=str(workspace), cpus=1,
            wall_time_seconds=1560, wall_time_source="calibrated-estimate",
            state=JobState.RUNNING, pgid=999002,
            recover_on_reboot=True).write(daemon._spec_path("rebootjob0002"))
    daemon._reattach_or_interrupt_at_startup()
    (sibling,) = [s for s in (JobSpec.read(p) for p in daemon.queue_dir.glob("*.json"))
                  if s.parent_jobid == "rebootjob0002"]
    assert (sibling.wall_time_seconds, sibling.wall_time_source) == (1560, "calibrated-estimate")


def test_a_remote_submission_carries_the_limit_and_its_source(
    tmp_path: Path, plan_harness: PlanHarness,  # noqa: F811
) -> None:
    payload = plan_harness.root / "payload"
    payload.mkdir()
    (payload / "run.py").write_text("pass\n")
    path = tmp_path / "calibrated.json"
    path.write_text(json.dumps(_document(tmp_path, payload)))
    result = CliRunner().invoke(main, ["submit", "--host", "remote", "--estimate", str(path),
                                       "--time-from-estimate", "-d", str(payload), "--json",
                                       "--", "true"])
    assert result.exit_code == 0, result.output
    ((kind, kwargs),) = plan_harness.calls
    assert kind == "remote"
    assert (kwargs["wall_time_seconds"], kwargs["wall_time_source"]) == (
        1560, "calibrated-estimate")
