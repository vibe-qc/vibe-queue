"""#159: a calibration applied to the estimate a job is submitted with.

`vq estimate --calibration CAL --hardware-class LABEL` prints a
`vq.calibrated-estimate/1`: the unchanged `vq.resource-estimate/1` receipt
and, beside it, its calibrated peak. `vq submit --estimate` accepts either
document; a calibrated one is checked, its receipt checked against the
payload as before, and both are recorded on the job. Calibrations are fitted
for real from a training set.
"""
from __future__ import annotations

import json
import math
import sys
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from vq import config, paths, resubmit, status
from vq import estimate_calibration as cal
from vq import estimate_evaluation as ev
from vq import submit as submit_module
from vq.cli import main
from vq.resource_estimate import estimate_vibeqc, receipt_record
from vq.spec import JobSpec, JobState

sys.path.insert(0, str(Path(__file__).parent))
from test_estimate_calibration_159 import _dir, _rank_case, _set  # noqa: E402

GIB = 1024**3


def _calibration(tmp_path: Path, *, observed: float = 2.0, cases: int = 5) -> Path:
    """hf, rank scope, vibeqc 'example' on class-a: the worst ratio is ``observed``."""
    d = _dir(tmp_path / "train")
    training = _set(d, "train-1", [_rank_case(d, f"t{i}", peak=4 * GIB,
                                              observed=int(4 * GIB * observed))
                                   for i in range(cases)])
    path = tmp_path / "cal.json"
    path.write_text(cal.document_json(cal.fit(training, min_cases=5)))
    return path


def _manifest(tmp_path: Path, *, peak: int | None = 3 * GIB, method: str = "hf") -> Path:
    manifest = tmp_path / "calc.system"
    memory = f"estimate_bytes = {peak}" if peak is not None else ""
    manifest.write_text('[vibeqc]\nversion = "example"\n[plan]\n'
                        f'method = "{method}"\n[outputs]\nstatus = "dry_run"\n[memory]\n{memory}\n')
    (tmp_path / "features.json").write_text(json.dumps({"memory_scope": "rank"}))
    return manifest


def _receipt(tmp_path: Path, *inputs: Path, **kwargs: Any) -> Any:
    return estimate_vibeqc(_manifest(tmp_path, **kwargs), inputs=inputs,
                           features_file=tmp_path / "features.json")


class TestApply:
    def test_a_matching_group_calibrates_and_the_receipt_is_unchanged(
        self, tmp_path: Path,
    ) -> None:
        receipt = _receipt(tmp_path)
        document = cal.apply(receipt, cal.load(_calibration(tmp_path)), hardware_class="class-a")
        assert document["schema_name"] == "vq.calibrated-estimate/1"
        assert document["estimate"] == receipt_record(receipt)
        assert (document["status"], document["multiplier"], document["calibrated_peak_bytes"],
                document["reason"]) == ("calibrated", 2.0, 6 * GIB, None)
        inner, record = cal.check_calibrated(document)
        assert inner == receipt and "estimate" not in record
        assert record["calibrated_peak_bytes"] == 6 * GIB

    def test_the_calibrated_peak_rounds_up(self, tmp_path: Path) -> None:
        receipt = _receipt(tmp_path, peak=1000)
        document = cal.apply(receipt, cal.load(_calibration(tmp_path, observed=1.0001)),
                             hardware_class="class-a")
        assert document["calibrated_peak_bytes"] == math.ceil(1000 * 1.0001) == 1001

    @pytest.mark.parametrize(("kwargs", "hardware", "reason"), [
        ({"peak": None}, "class-a", "the estimate has no memory figure"),
        ({}, "class-b", "no group of this calibration matches"),
        ({"method": "mp2"}, "class-a", "no group of this calibration matches"),
    ])
    def test_otherwise_it_is_uncalibrated_and_says_why(
        self, tmp_path: Path, kwargs: dict[str, Any], hardware: str, reason: str,
    ) -> None:
        document = cal.apply(_receipt(tmp_path, **kwargs), cal.load(_calibration(tmp_path)),
                             hardware_class=hardware)
        assert document["status"] == "uncalibrated" and reason in document["reason"]
        assert document["multiplier"] is None and document["calibrated_peak_bytes"] is None

    def test_a_lower_bound_multiplier_says_so(self, tmp_path: Path) -> None:
        from test_estimate_calibration_159 import _case, _estimate

        d = _dir(tmp_path / "train")
        cases = [_rank_case(d, f"r{i}", peak=4 * GIB, observed=4 * GIB) for i in range(4)]
        cases.append(_case("oom", _estimate(d, "oom", peak=4 * GIB, scope="rank"), None,
                           outcome="oom_killed", memory_limit_bytes=6 * GIB,
                           memory_limit_scope="rank"))
        path = tmp_path / "bound.json"
        path.write_text(cal.document_json(cal.fit(_set(d, "t", cases), min_cases=5)))
        document = cal.apply(_receipt(tmp_path), cal.load(path), hardware_class="class-a")
        assert (document["multiplier"], document["multiplier_is_lower_bound"]) == (1.5, True)
        text = CliRunner().invoke(main, ["estimate", str(_manifest(tmp_path)), "--features",
                                         str(tmp_path / "features.json"), "--calibration",
                                         str(path), "--hardware-class", "class-a"])
        assert "(x1.5 (a lower bound), calibration" in text.output

    def test_a_group_with_too_few_cases_is_uncalibrated(self, tmp_path: Path) -> None:
        document = cal.apply(_receipt(tmp_path), cal.load(_calibration(tmp_path, cases=4)),
                             hardware_class="class-a")
        assert document["reason"] == (
            "its group has 4 decided case(s), fewer than the calibration's 5")


def _document(tmp_path: Path) -> dict[str, Any]:
    return cal.apply(_receipt(tmp_path), cal.load(_calibration(tmp_path)),
                     hardware_class="class-a")


class TestCheck:
    @pytest.mark.parametrize(("change", "message"), [
        (lambda d: {**d, "estimate": {**d["estimate"], "program_version": "other"}},
         "does not match its estimate_sha256"),
        (lambda d: {**d, "calibrated_peak_bytes": d["calibrated_peak_bytes"] + 1},
         "is not the estimate times its multiplier"),
        (lambda d: {**d, "multiplier": None}, "is not the estimate times its multiplier"),
        (lambda d: {**d, "reason": "why not"}, "is not the estimate times its multiplier"),
        (lambda d: {**d, "status": "uncalibrated"}, "carries no multiplier and says why"),
        (lambda d: {**d, "status": "uncalibrated", "multiplier": None,
                    "calibrated_peak_bytes": None}, "carries no multiplier and says why"),
        (lambda d: {**d, "status": "uncalibrated", "multiplier": None,
                    "calibrated_peak_bytes": None, "reason": "x",
                    "multiplier_is_lower_bound": True}, "carries no multiplier and says why"),
        (lambda d: {**d, "status": "uncalibrated", "calibrated_peak_bytes": None,
                    "reason": "x"}, "carries no multiplier and says why"),
        (lambda d: {**d, "schema_name": "vq.calibrated-estimate/2"}, "not a vq.calibrated"),
        (lambda d: {**d, "estimate": {"schema_name": "nope"}}, "not a vq.calibrated"),
        (lambda d: {**d, "multiplier": 0.5}, "not a vq.calibrated"),
    ])
    def test_an_inconsistent_document_is_refused(
        self, tmp_path: Path, change: Any, message: str,
    ) -> None:
        with pytest.raises(ev.EvaluationError, match=message):
            cal.check_calibrated(change(_document(tmp_path)))


class TestEstimateCommand:
    def test_it_prints_the_calibrated_document_or_line(self, tmp_path: Path) -> None:
        runner = CliRunner()
        calibration = _calibration(tmp_path)
        base = ["estimate", str(_manifest(tmp_path)), "--features",
                str(tmp_path / "features.json"), "--calibration", str(calibration)]
        as_json = runner.invoke(main, [*base, "--hardware-class", "class-a", "--json"])
        assert as_json.exit_code == 0, as_json.output
        document = json.loads(as_json.output)
        assert document["calibrated_peak_bytes"] == 6 * GIB
        cal.check_calibrated(document)
        text = runner.invoke(main, [*base, "--hardware-class", "class-a"])
        identity = cal.load(calibration).calibration_id
        assert (f"Calibrated memory: {6 * GIB} bytes (x2, calibration {identity[:12]} on "
                "class-a)") in text.output
        other = runner.invoke(main, [*base, "--hardware-class", "class-b"])
        assert "Calibrated memory: uncalibrated (no group of this calibration" in other.output
        lone = runner.invoke(main, base)
        assert lone.exit_code == 2 and "go together" in lone.output
        plain = runner.invoke(main, ["estimate", str(_manifest(tmp_path)), "--json"])
        assert json.loads(plain.output)["schema_name"] == "vq.resource-estimate/1"


class TestSubmit:
    @pytest.fixture(autouse=True)
    def _state(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))

    def _payload(self, tmp_path: Path) -> Path:
        payload = tmp_path / "payload"
        payload.mkdir()
        (payload / "run.py").write_text("pass\n")
        return payload

    def _calibrated_file(self, tmp_path: Path, payload: Path) -> tuple[Path, dict[str, Any]]:
        document = cal.apply(_receipt(tmp_path, payload / "run.py"),
                             cal.load(_calibration(tmp_path)), hardware_class="class-a")
        path = tmp_path / "calibrated.json"
        path.write_text(json.dumps(document))
        return path, document

    def test_a_calibrated_estimate_is_recorded_beside_its_receipt(self, tmp_path: Path) -> None:
        payload = self._payload(tmp_path)
        path, document = self._calibrated_file(tmp_path, payload)
        result = CliRunner().invoke(main, ["submit", "localhost", "--estimate", str(path),
                                           "-d", str(payload), "--", "true"])
        assert result.exit_code == 0, result.output
        jobid = result.output.strip().splitlines()[-1]
        spec = JobSpec.read(paths.spec_path(jobid))
        assert spec.resource_estimate == document["estimate"]
        _inner, record = cal.check_calibrated(document)
        assert spec.resource_estimate_calibration == record
        shown = status.show_status("localhost", jobid)
        assert (f"; calibrated 6144 MiB (x2.0, calibration {record['calibration_id'][:12]} "
                "on class-a)") in shown
        wire = CliRunner().invoke(main, ["submit", "localhost", "--estimate-json",
                                         path.read_text(), "-d", str(payload), "--", "true"])
        assert wire.exit_code == 0, wire.output
        # A finished job resubmitted keeps both.
        spec.state = JobState.COMPLETED
        spec.write(paths.spec_path(jobid))
        again = JobSpec.read(paths.spec_path(resubmit.resubmit_local(jobid)))
        assert again.resource_estimate_calibration == record

    def test_an_uncalibrated_estimate_says_why_in_status(self, tmp_path: Path) -> None:
        payload = self._payload(tmp_path)
        document = cal.apply(_receipt(tmp_path, payload / "run.py"),
                             cal.load(_calibration(tmp_path)), hardware_class="class-b")
        path = tmp_path / "uncalibrated.json"
        path.write_text(json.dumps(document))
        result = CliRunner().invoke(main, ["submit", "localhost", "--estimate", str(path),
                                           "-d", str(payload), "--", "true"])
        assert result.exit_code == 0, result.output
        shown = status.show_status("localhost", result.output.strip().splitlines()[-1])
        assert (f"; uncalibrated by {document['calibration_id'][:12]} (no group of this "
                "calibration matches") in shown

    def test_a_tampered_or_mismatched_calibrated_estimate_is_refused(
        self, tmp_path: Path,
    ) -> None:
        payload = self._payload(tmp_path)
        path, document = self._calibrated_file(tmp_path, payload)
        path.write_text(json.dumps({**document, "calibrated_peak_bytes": 1}))
        refused = CliRunner().invoke(main, ["submit", "localhost", "--estimate", str(path),
                                            "-d", str(payload), "--", "true"])
        assert refused.exit_code == 2
        assert "--estimate: its calibrated peak is not the estimate times" in refused.output
        path.write_text(json.dumps(document))
        (payload / "run.py").write_text("changed\n")  # not the input it was made for
        other = CliRunner().invoke(main, ["submit", "localhost", "--estimate", str(path),
                                          "-d", str(payload), "--", "true"])
        assert other.exit_code != 0 and "does not carry" in other.output
        assert not list(paths.queue_dir().glob("*.json"))

    def test_the_wire_and_intent_carry_it_only_when_set(self, tmp_path: Path) -> None:
        document = _document(tmp_path)
        common: dict[str, Any] = dict(
            remote_tar="/tmp/x.tar", remote_input_file=None, remote_command=["true"], cpus=1,
            scheduler_tasks=None, mem_mb=None, wall_time_seconds=None, priority=0,
            auto_resume=False, retry=0, job_name=None, branch=None, program=None,
            expected_sha=None, tags=None, not_before=None, depends_on=None, depends_on_any=None,
            clean_workdir_on_terminal=False, array=1, chain=1, rerun_until_file_exists=None,
            rerun_max=10, refresh_before=None, scheduler_target=None, qvf_force=False)
        argv = submit_module._build_remote_submit_argv(**common, resource_estimate=document)
        assert json.loads(argv[argv.index("--estimate-json") + 1]) == document

        receipt, record = cal.check_calibrated(document)

        def digest(calibration: dict[str, Any] | None) -> str:
            spec = JobSpec(id="abc", command=["true"], cwd="/ws", cpus=4,
                           scheduler_target="host_f", resource_estimate=receipt_record(receipt),
                           resource_estimate_calibration=calibration)
            return submit_module._submission_intent_digest(
                spec, payload_kind="file", payload_digest="0" * 64, vibeqc_preflight=False)

        assert digest(record) != digest(None)


def test_a_job_recovered_after_a_reboot_keeps_its_calibration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from vq.daemon import Daemon

    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    monkeypatch.setattr("vq.daemon._pgroup_alive", lambda pgid: False)
    daemon = Daemon(max_cpus=4, poll_interval=0.05)
    daemon.queue_dir.mkdir(parents=True, exist_ok=True)
    receipt, record = cal.check_calibrated(_document(tmp_path))
    workspace = tmp_path / "jobs" / "rebootjob0001"
    workspace.mkdir(parents=True)
    JobSpec(id="rebootjob0001", command=["true"], cwd=str(workspace), cpus=1,
            state=JobState.RUNNING, pgid=999001, recover_on_reboot=True,
            resource_estimate=receipt_record(receipt), resource_estimate_calibration=record,
            ).write(daemon._spec_path("rebootjob0001"))
    daemon._reattach_or_interrupt_at_startup()
    (sibling,) = [s for s in (JobSpec.read(p) for p in daemon.queue_dir.glob("*.json"))
                  if s.parent_jobid == "rebootjob0001"]
    assert sibling.resource_estimate_calibration == record
