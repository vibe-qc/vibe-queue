"""#159: `vq submit --mem-from-estimate` takes a job's memory limit from a calibrated estimate.

Opt-in and narrow: only a calibrated, job-scope `vq.calibrated-estimate/1`
gives a limit (its calibrated peak rounded up to MiB), never with an explicit
--mem-mb. The limit is derived before placement, its source is recorded on
the job and checked where the job is created. Calibrations are fitted for real.
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
from vq.resource_estimate import estimate_vibeqc
from vq.spec import JobSpec, JobState

sys.path.insert(0, str(Path(__file__).parent))
from test_estimate_calibration_159 import _dir, _set  # noqa: E402
from test_estimate_evaluation_159 import _case, _estimate, _usage  # noqa: E402
from test_submit_plan_contract import PlanHarness, plan_harness  # noqa: E402, F401

GIB = 1024**3
MIB = 1024**2


def _calibration(tmp_path: Path) -> Path:
    """hf on class-a: job scope (single-process runs) and rank scope, worst ratio 1.5."""
    d = _dir(tmp_path / "train")
    cases = [_case(f"{scope}{i}", _estimate(d, f"{scope}{i}", peak=4 * GIB, scope=scope),
                   _usage(d, f"{scope}{i}", rss=6 * GIB if i == 0 else 4 * GIB, processes=1))
             for scope in ("job", "rank") for i in range(5)]
    path = tmp_path / "cal.json"
    path.write_text(cal.document_json(cal.fit(_set(d, "train-1", cases), min_cases=5)))
    return path


def _document(tmp_path: Path, payload: Path | None = None, *, scope: str = "job",
              hardware: str = "class-a", peak: int = 3 * GIB + 1) -> dict[str, Any]:
    manifest = tmp_path / "calc.system"
    manifest.write_text('[vibeqc]\nversion = "example"\n[plan]\nmethod = "hf"\n'
                        f'[outputs]\nstatus = "dry_run"\n[memory]\nestimate_bytes = {peak}\n')
    features = tmp_path / "features.json"
    features.write_text(json.dumps({"memory_scope": scope}))
    receipt = estimate_vibeqc(manifest, features_file=features,
                              inputs=(payload / "run.py",) if payload else ())
    return cal.apply(receipt, cal.load(_calibration(tmp_path)), hardware_class=hardware)


class TestDerivation:
    def test_a_calibrated_job_scope_estimate_gives_its_peak_in_mib(self, tmp_path: Path) -> None:
        document = _document(tmp_path)
        # (3 GiB + 1 byte) x 1.5, rounded up to a byte and then to a MiB.
        assert document["calibrated_peak_bytes"] == (3 * GIB + 1) * 3 // 2 + 1
        assert submit_module.mem_mb_from_calibrated_estimate(document) == 4608 + 1

    @pytest.mark.parametrize(("kwargs", "message"), [
        ({"scope": "rank"}, "only a job-scope estimate can set a job's memory limit; "
                            "this one is rank-scope"),
        ({"hardware": "class-b"}, "the estimate is uncalibrated (no group"),
    ])
    def test_anything_else_gives_none(self, tmp_path: Path, kwargs: dict[str, Any],
                                      message: str) -> None:
        with pytest.raises(ValueError, match=message.replace("(", r"\(")):
            submit_module.mem_mb_from_calibrated_estimate(_document(tmp_path, **kwargs))

    def test_a_raw_receipt_gives_none(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="needs a calibrated estimate"):
            submit_module.mem_mb_from_calibrated_estimate(_document(tmp_path)["estimate"])


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
        payload, path, document = self._files(tmp_path)
        expected = submit_module.mem_mb_from_calibrated_estimate(document)
        result = self._submit("--estimate", str(path), "--mem-from-estimate",
                              "-d", str(payload), "--", "true")
        assert result.exit_code == 0, result.output
        jobid = result.output.strip().splitlines()[-1]
        spec = JobSpec.read(paths.spec_path(jobid))
        assert (spec.mem_mb, spec.mem_mb_source) == (expected, "calibrated-estimate")
        assert f"mem_mb:       {expected} (from the calibrated estimate)" in (
            status.show_status("localhost", jobid))
        # A resubmission keeps it, unless the limit is overridden.
        spec.state = JobState.COMPLETED
        spec.write(paths.spec_path(jobid))
        kept = JobSpec.read(paths.spec_path(resubmit.resubmit_local(jobid)))
        assert (kept.mem_mb, kept.mem_mb_source) == (expected, "calibrated-estimate")
        overridden = JobSpec.read(paths.spec_path(resubmit.resubmit_local(
            jobid, overrides=resubmit.ResubmitOverrides(mem_mb=2048))))
        assert (overridden.mem_mb, overridden.mem_mb_source) == (2048, None)

    def test_it_is_derived_before_placement(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        payload, path, document = self._files(tmp_path)
        seen: list[int | None] = []

        def resolve(*_args: Any, mem_mb: int | None = None, **_kwargs: Any) -> Any:
            seen.append(mem_mb)
            raise ValueError("stop here")

        monkeypatch.setattr(submit_module, "_resolve_requested_submit_target", resolve)
        self._submit("--estimate", str(path), "--mem-from-estimate", "-d", str(payload),
                     "--", "true")
        assert seen == [submit_module.mem_mb_from_calibrated_estimate(document)]

    @pytest.mark.parametrize(("extra", "kwargs", "message"), [
        (["--mem-mb", "1024"], {}, "mutually exclusive; an explicit limit stays authoritative"),
        ([], {"scope": "rank"}, "this one is rank-scope"),
        ([], {"hardware": "class-b"}, "the estimate is uncalibrated"),
    ])
    def test_refusals(self, tmp_path: Path, extra: list[str], kwargs: dict[str, Any],
                      message: str) -> None:
        payload, path, _document_ = self._files(tmp_path, **kwargs)
        result = self._submit("--estimate", str(path), "--mem-from-estimate", *extra,
                              "-d", str(payload), "--", "true")
        assert result.exit_code == 2 and message in result.output
        assert not list(paths.queue_dir().glob("*.json"))

    def test_without_an_estimate_or_with_a_raw_one_it_is_refused(self, tmp_path: Path) -> None:
        payload, _path, document = self._files(tmp_path)
        bare = self._submit("--mem-from-estimate", "-d", str(payload), "--", "true")
        assert bare.exit_code == 2 and "--mem-from-estimate needs --estimate" in bare.output
        raw = tmp_path / "raw.json"
        raw.write_text(json.dumps(document["estimate"]))
        refused = self._submit("--estimate", str(raw), "--mem-from-estimate", "-d",
                               str(payload), "--", "true")
        assert refused.exit_code == 2 and "needs a calibrated estimate" in refused.output

    def test_the_wire_form_is_checked_where_the_job_is_created(self, tmp_path: Path) -> None:
        payload, path, document = self._files(tmp_path)
        expected = submit_module.mem_mb_from_calibrated_estimate(document)
        wire = ["--estimate-json", path.read_text(), "--mem-mb-source", "calibrated-estimate"]
        good = self._submit(*wire, "--mem-mb", str(expected), "-d", str(payload), "--", "true")
        assert good.exit_code == 0, good.output
        bad = self._submit(*wire, "--mem-mb", str(expected + 1), "-d", str(payload), "--",
                           "true")
        assert bad.exit_code != 0
        assert "the memory limit is not the calibrated estimate's peak in MiB" in bad.output
        raw = self._submit("--estimate-json", json.dumps(document["estimate"]),
                           "--mem-mb-source", "calibrated-estimate", "--mem-mb",
                           str(expected), "-d", str(payload), "--", "true")
        assert raw.exit_code != 0 and "needs a calibrated estimate" in raw.output

    def test_submit_local_refuses_an_unknown_source(self, tmp_path: Path) -> None:
        payload, _path, document = self._files(tmp_path)
        with pytest.raises(ValueError, match="unknown memory limit source 'guess'"):
            submit_module.submit_local(host="localhost", directory=str(payload),
                                       command=["true"], resource_estimate=document,
                                       mem_mb=1, mem_mb_source="guess")

    def test_the_wire_and_intent_carry_it_only_when_set(self, tmp_path: Path) -> None:
        common: dict[str, Any] = dict(
            remote_tar="/tmp/x.tar", remote_input_file=None, remote_command=["true"], cpus=1,
            scheduler_tasks=None, mem_mb=4609, wall_time_seconds=None, priority=0,
            auto_resume=False, retry=0, job_name=None, branch=None, program=None,
            expected_sha=None, tags=None, not_before=None, depends_on=None, depends_on_any=None,
            clean_workdir_on_terminal=False, array=1, chain=1, rerun_until_file_exists=None,
            rerun_max=10, refresh_before=None, scheduler_target=None, qvf_force=False)
        argv = submit_module._build_remote_submit_argv(**common,
                                                       mem_mb_source="calibrated-estimate")
        assert argv[argv.index("--mem-mb-source") + 1] == "calibrated-estimate"
        assert "--mem-mb-source" not in submit_module._build_remote_submit_argv(**common)

        def digest(source: str | None) -> str:
            spec = JobSpec(id="abc", command=["true"], cwd="/ws", cpus=4, mem_mb=4609,
                           scheduler_target="host_f", mem_mb_source=source)
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
    workspace = tmp_path / "jobs" / "rebootjob0001"
    workspace.mkdir(parents=True)
    JobSpec(id="rebootjob0001", command=["true"], cwd=str(workspace), cpus=1, mem_mb=4609,
            mem_mb_source="calibrated-estimate", state=JobState.RUNNING, pgid=999001,
            recover_on_reboot=True).write(daemon._spec_path("rebootjob0001"))
    daemon._reattach_or_interrupt_at_startup()
    (sibling,) = [s for s in (JobSpec.read(p) for p in daemon.queue_dir.glob("*.json"))
                  if s.parent_jobid == "rebootjob0001"]
    assert (sibling.mem_mb, sibling.mem_mb_source) == (4609, "calibrated-estimate")


def test_a_remote_submission_carries_the_limit_and_its_source(
    tmp_path: Path, plan_harness: PlanHarness,  # noqa: F811
) -> None:
    payload = plan_harness.root / "payload"
    payload.mkdir()
    (payload / "run.py").write_text("pass\n")
    document = _document(tmp_path, payload)
    path = tmp_path / "calibrated.json"
    path.write_text(json.dumps(document))
    result = CliRunner().invoke(main, ["submit", "--host", "remote", "--estimate", str(path),
                                       "--mem-from-estimate", "-d", str(payload), "--json",
                                       "--", "true"])
    assert result.exit_code == 0, result.output
    ((kind, kwargs),) = plan_harness.calls
    assert kind == "remote"
    assert (kwargs["mem_mb"], kwargs["mem_mb_source"]) == (
        submit_module.mem_mb_from_calibrated_estimate(document), "calibrated-estimate")
