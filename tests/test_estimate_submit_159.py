"""#159: the estimate a job was submitted with, kept for held-out evaluation.

`vq submit --estimate RECEIPT` attaches a `vq estimate --json` receipt to the
job once every input file it fingerprinted is found in the payload by
content: a single file, a `--dir` tree or a `--compressed` archive's members.
`vq estimate-collect` then pairs each finished job's receipt with the usage
receipt its collector left, into a `vq.estimate-evaluation-set/1` that
`vq estimate-evaluate` reads. The decisive case runs end to end through a
real daemon.
"""

from __future__ import annotations

import hashlib
import json
import sys
import tarfile
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from vq import config, estimate_collect, paths, resubmit, status
from vq import resource_estimate as estimates
from vq import submit as submit_module
from vq.cli import main
from vq.daemon import Daemon
from vq.spec import JobSpec, JobState

sys.path.insert(0, str(Path(__file__).parent))
from test_broker_relay_64 import client as _client_fixture  # noqa: E402

client = _client_fixture


def _receipt(
    tmp_path: Path, *inputs: Path, peak: int = 64 * 1024**2, product: bool = False
) -> Path:
    """A receipt from the real estimator, fingerprinting ``inputs``."""
    manifest = tmp_path / "calc.system"
    manifest.write_text(
        '[vibeqc]\nversion = "example"\n[plan]\nmethod = "hf"\n'
        f'[outputs]\nstatus = "dry_run"\n[memory]\nestimate_bytes = {peak}\n'
    )
    if product:
        with manifest.open("a") as stream:
            stream.write(
                '[resource_evidence]\nschema = "vibeqc.resource-evidence/1"\n'
                'input_coverage = "explicit_reads"\nnbf = 7\ninput_sha256 = '
                + json.dumps([hashlib.sha256(p.read_bytes()).hexdigest() for p in inputs])
                + "\n"
            )
    features = tmp_path / "features.json"
    features.write_text(json.dumps({"memory_scope": "job"}))
    receipt = estimates.estimate_vibeqc(manifest, inputs=inputs, features_file=features)
    path = tmp_path / "receipt.json"
    path.write_text(receipt.model_dump_json(indent=2))
    return path


def _payload(tmp_path: Path, name: str = "payload", script: str = "pass\n") -> Path:
    payload = tmp_path / name
    (payload / "inputs").mkdir(parents=True)
    (payload / "inputs" / "run.py").write_text(script)
    (payload / "README").write_text("calculation\n")
    return payload


class TestCheckInputs:
    def test_every_payload_shape_is_searched_by_content(self, tmp_path: Path) -> None:
        payload = _payload(tmp_path)
        script = payload / "inputs" / "run.py"
        receipt = estimates.load_receipt(_receipt(tmp_path, script).read_text())
        estimates.check_inputs(receipt, input_file=script)
        estimates.check_inputs(receipt, directory=payload)
        archive = tmp_path / "payload.tar.gz"
        with tarfile.open(archive, "w:gz") as tar:
            tar.add(payload, arcname=".")
        estimates.check_inputs(receipt, archive=archive)

    def test_an_estimate_for_other_inputs_is_refused(self, tmp_path: Path) -> None:
        payload = _payload(tmp_path)
        other = tmp_path / "other.py"
        other.write_text("print('a different input')\n")
        receipt = estimates.load_receipt(_receipt(tmp_path, other).read_text())
        with pytest.raises(estimates.EstimateError, match="1 input file.* not carry"):
            estimates.check_inputs(receipt, directory=payload)
        # A link to the input is not the input: the payload must carry the file.
        (payload / "linked.py").symlink_to(other)
        with pytest.raises(estimates.EstimateError, match="not carry"):
            estimates.check_inputs(receipt, directory=payload)

    def test_a_manifest_only_receipt_has_nothing_to_check(self, tmp_path: Path) -> None:
        receipt = estimates.load_receipt(_receipt(tmp_path).read_text())
        estimates.check_inputs(receipt, directory=tmp_path / "does-not-exist")

    @pytest.mark.parametrize(
        ("bound", "value"), [("MAX_PAYLOAD_FILES", 1), ("MAX_PAYLOAD_BYTES", 8)]
    )
    def test_the_search_is_bounded(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, bound: str, value: int
    ) -> None:
        payload = _payload(tmp_path)
        other = tmp_path / "other.py"
        other.write_text("absent\n")
        receipt = estimates.load_receipt(_receipt(tmp_path, other).read_text())
        monkeypatch.setattr(estimates, bound, value)
        with pytest.raises(estimates.EstimateError, match="too large"):
            estimates.check_inputs(receipt, directory=payload)

    @pytest.mark.parametrize(
        ("text", "message"),
        [
            ("{}", "not a vq.resource-estimate/1 receipt"),
            ("x" * (estimates.MAX_RECEIPT_BYTES + 1), "larger than"),
        ],
    )
    def test_a_bad_receipt_is_refused(self, text: str, message: str) -> None:
        with pytest.raises(estimates.EstimateError, match=message):
            estimates.load_receipt(text)


class TestSubmit:
    @pytest.fixture(autouse=True)
    def _state(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))

    def _submit(self, *args: str) -> Any:
        return CliRunner().invoke(main, ["submit", "localhost", *args])

    def test_the_job_keeps_the_receipt(self, tmp_path: Path) -> None:
        payload = _payload(tmp_path)
        receipt = _receipt(tmp_path, payload / "inputs" / "run.py")
        result = self._submit("--estimate", str(receipt), "-d", str(payload), "--", "true")
        assert result.exit_code == 0, result.output
        jobid = result.output.strip().splitlines()[-1]
        spec = JobSpec.read(paths.spec_path(jobid))
        assert spec.resource_estimate == json.loads(receipt.read_text())
        assert "estimate:     hf; memory 64 MiB (job scope, uncalibrated); evidence " in (
            status.show_status("localhost", jobid)
        )

    def test_an_estimate_for_other_inputs_refuses_the_submission(self, tmp_path: Path) -> None:
        payload = _payload(tmp_path)
        other = tmp_path / "elsewhere.py"
        other.write_text("other\n")
        result = self._submit(
            "--estimate", str(_receipt(tmp_path, other)), "-d", str(payload), "--", "true"
        )
        assert result.exit_code != 0
        assert "--estimate: the estimate fingerprinted 1 input file(s)" in result.output
        assert not list(paths.queue_dir().glob("*.json"))

    def test_the_wire_form_is_accepted_and_both_forms_are_not(self, tmp_path: Path) -> None:
        payload = _payload(tmp_path)
        receipt = _receipt(tmp_path, payload / "inputs" / "run.py")
        wire = self._submit(
            "--estimate-json", receipt.read_text(), "-d", str(payload), "--", "true"
        )
        assert wire.exit_code == 0, wire.output
        both = self._submit(
            "--estimate",
            str(receipt),
            "--estimate-json",
            receipt.read_text(),
            "-d",
            str(payload),
            "--",
            "true",
        )
        assert both.exit_code != 0 and "mutually exclusive" in both.output

    def test_a_broker_refuses_a_raw_receipt(
        self,
        client: list[Any],
        tmp_path: Path,  # noqa: F811
    ) -> None:
        # A broker ranks only by a calibrated runtime (#159/#160); see
        # tests/test_broker_runtime_estimate_159.py.
        receipt = _receipt(tmp_path)
        result = CliRunner().invoke(
            main,
            [
                "submit",
                "fleet",
                "-d",
                str(_payload(tmp_path)),
                "--cpus",
                "1",
                "--mem-mb",
                "100",
                "--estimate",
                str(receipt),
                "--",
                "true",
            ],
        )
        assert result.exit_code == 2 and "not a raw receipt" in result.output
        assert client == []

    def test_submit_local_validates_a_direct_caller(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="--estimate: not a vq.resource-estimate/1"):
            submit_module.submit_local(
                host="localhost",
                input_file=str(tmp_path / "x.py"),
                resource_estimate={"schema_name": "nope"},
            )

    def test_the_wire_and_intent_carry_it_only_when_set(self, tmp_path: Path) -> None:
        record = json.loads(_receipt(tmp_path).read_text())
        common: dict[str, Any] = dict(
            remote_tar="/tmp/x.tar",
            remote_input_file=None,
            remote_command=["true"],
            cpus=1,
            scheduler_tasks=None,
            mem_mb=None,
            wall_time_seconds=None,
            priority=0,
            auto_resume=False,
            retry=0,
            job_name=None,
            branch=None,
            program=None,
            expected_sha=None,
            tags=None,
            not_before=None,
            depends_on=None,
            depends_on_any=None,
            clean_workdir_on_terminal=False,
            array=1,
            chain=1,
            rerun_until_file_exists=None,
            rerun_max=10,
            refresh_before=None,
            scheduler_target=None,
            qvf_force=False,
        )
        argv = submit_module._build_remote_submit_argv(**common, resource_estimate=record)
        assert json.loads(argv[argv.index("--estimate-json") + 1]) == record
        assert "--estimate-json" not in submit_module._build_remote_submit_argv(**common)

        def digest(estimate: dict[str, Any] | None) -> str:
            spec = JobSpec(
                id="abc",
                command=["true"],
                cwd="/ws",
                cpus=4,
                scheduler_target="host_f",
                resource_estimate=estimate,
            )
            return submit_module._submission_intent_digest(
                spec, payload_kind="file", payload_digest="0" * 64, vibeqc_preflight=False
            )

        # Unchanged without one (the pre-field hash, see tests/test_gpus_168.py).
        assert digest(None) == "eda89c1ba8023e2d7cb89df52737afc21c239b80870e248be42a074a7cff2db8"
        assert digest(record) != digest(None)


class TestCollect:
    def _spec(self, tmp_path: Path, jobid: str, state: JobState, **fields: Any) -> JobSpec:
        workspace = tmp_path / "jobs" / jobid
        (workspace / "_vq").mkdir(parents=True)
        return JobSpec(
            id=jobid,
            command=["true"],
            cwd=str(workspace),
            cpus=1,
            state=state,
            resource_estimate=json.loads(_receipt(tmp_path).read_text()),
            **fields,
        )

    def test_censoring_limits_and_skipped_jobs(self, tmp_path: Path) -> None:
        specs = [
            self._spec(
                tmp_path, "aaa000000001", JobState.OOM_KILLED, mem_mb=512, wall_time_seconds=60
            ),
            self._spec(
                tmp_path, "aaa000000002", JobState.TIME_EXCEEDED, wall_time_seconds=60, mem_mb=512
            ),
            self._spec(tmp_path, "aaa000000003", JobState.INTERRUPTED),
            self._spec(tmp_path, "aaa000000004", JobState.RUNNING),
            JobSpec(
                id="aaa000000005",
                command=["true"],
                cwd=str(tmp_path),
                cpus=1,
                state=JobState.COMPLETED,
            ),
        ]
        array = self._spec(tmp_path, "aaa000000006", JobState.COMPLETED)
        (Path(array.cwd) / "_vq" / "resource-usage.json.0").write_text("{}")
        summary = estimate_collect.collect(
            [*specs, array], tmp_path / "set", hardware_class="laptop", set_id="held-out"
        )
        cases = {
            c["case_id"]: c
            for c in json.loads((tmp_path / "set" / "set.json").read_text())["cases"]
        }
        assert set(cases) == {"aaa000000001", "aaa000000002"}
        assert cases["aaa000000001"]["memory_limit_bytes"] == 512 * 1024**2
        assert "wall_limit_seconds" not in cases["aaa000000001"]
        assert cases["aaa000000002"]["wall_limit_seconds"] == 60
        assert "memory_limit_bytes" not in cases["aaa000000002"]
        assert "usage" not in cases["aaa000000001"]  # a kill left no receipt
        assert [s["job"] for s in summary["skipped"]] == ["aaa000000003", "aaa000000006"]

    def test_a_usage_receipt_is_never_followed_or_read_whole(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        linked = self._spec(tmp_path, "aaa000000001", JobState.COMPLETED)
        target = tmp_path / "elsewhere.json"
        target.write_text("{}")
        (Path(linked.cwd) / "_vq" / "resource-usage.json").symlink_to(target)
        large = self._spec(tmp_path, "aaa000000002", JobState.COMPLETED)
        (Path(large.cwd) / "_vq" / "resource-usage.json").write_text("x" * 65)
        kept = self._spec(tmp_path, "aaa000000003", JobState.FAILED)
        monkeypatch.setattr(estimate_collect, "MAX_USAGE_BYTES", 64)
        summary = estimate_collect.collect(
            [linked, large, kept], tmp_path / "set", hardware_class="laptop", set_id="s"
        )
        assert summary["cases"] == 1
        reasons = {s["job"]: s["reason"] for s in summary["skipped"]}
        assert reasons["aaa000000001"].startswith("usage receipt unreadable")
        assert "larger than 64 bytes" in reasons["aaa000000002"]

    def test_it_never_writes_into_an_existing_set(self, tmp_path: Path) -> None:
        (tmp_path / "set").mkdir()
        (tmp_path / "set" / "keep").write_text("x")
        with pytest.raises(estimate_collect.CollectError, match="not an empty directory"):
            estimate_collect.collect(
                [self._spec(tmp_path, "aaa000000001", JobState.FAILED)],
                tmp_path / "set",
                hardware_class="laptop",
                set_id="s",
            )
        with pytest.raises(estimate_collect.CollectError, match="no finished job"):
            estimate_collect.collect([], tmp_path / "fresh", hardware_class="laptop", set_id="s")
        assert not (tmp_path / "fresh").exists()


@pytest.fixture
def daemon(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Daemon]:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    queue = Daemon(max_cpus=4, poll_interval=0.05)
    queue.queue_dir.mkdir(parents=True, exist_ok=True)
    yield queue
    for running in queue._running.values():
        running.popen.kill()
        running.popen.wait(timeout=10)
        running.close_logs()


@pytest.mark.parametrize("product", [False, True])
def test_end_to_end_estimated_jobs_become_an_evaluated_set(
    tmp_path: Path,
    daemon: Daemon,
    product: bool,
) -> None:
    jobs = {}
    for name, command in (("ok", "true"), ("fails", "exit 3")):
        payload = _payload(tmp_path, f"payload-{name}", script=f"# {name}\n")
        receipt = _receipt(
            tmp_path, payload / "inputs" / "run.py", peak=512 * 1024**2, product=product
        )
        result = CliRunner().invoke(
            main,
            [
                "submit",
                "localhost",
                "--estimate",
                str(receipt),
                "-d",
                str(payload),
                "--",
                "sh",
                "-c",
                command,
            ],
        )
        assert result.exit_code == 0, result.output
        jobs[name] = result.output.strip().splitlines()[-1]
    plain = CliRunner().invoke(
        main, ["submit", "localhost", "-d", str(_payload(tmp_path, "plain")), "--", "true"]
    )
    assert plain.exit_code == 0, plain.output
    daemon._dispatch_pending()
    deadline = time.monotonic() + 30
    while daemon._running:
        assert time.monotonic() < deadline
        daemon._reconcile_running()
        time.sleep(0.05)

    collected = CliRunner().invoke(
        main, ["estimate-collect", str(tmp_path / "set"), "--hardware-class", "test-host", "--json"]
    )
    assert collected.exit_code == 0, collected.output
    summary = json.loads(collected.output)
    assert (summary["cases"], summary["skipped"]) == (2, [])
    document = json.loads((tmp_path / "set" / "set.json").read_text())
    outcomes = {c["case_id"]: (c["outcome"], c.get("usage")) for c in document["cases"]}
    assert outcomes == {
        jobs["ok"]: ("completed", f"usage/{jobs['ok']}.json"),
        jobs["fails"]: ("failed", f"usage/{jobs['fails']}.json"),
    }

    evaluated = CliRunner().invoke(
        main, ["estimate-evaluate", str(tmp_path / "set" / "set.json"), "--json"]
    )
    assert evaluated.exit_code == 0, evaluated.output
    report = json.loads(evaluated.output)
    rows = {row["case_id"]: row for row in report["cases"]}
    # The completed shell used far less than 512 MiB; the failed run is no measurement.
    assert rows[jobs["ok"]]["observed_peak_rss_bytes"] < 512 * 1024**2
    assert rows[jobs["ok"]]["memory"] in {"within", "not_refuted"}
    assert rows[jobs["fails"]]["memory"] == "excluded"

    # A resubmission is the same calculation, so it keeps the estimate.
    again = resubmit.resubmit_local(jobs["ok"])
    kept = JobSpec.read(paths.spec_path(jobs["ok"])).resource_estimate
    if product:
        assert kept["input_binding"] == "product_observed_inputs"
        assert kept["feature_provenance"]["nbf"] == "product_resolved"
    assert kept is not None and JobSpec.read(paths.spec_path(again)).resource_estimate == kept


def test_a_job_recovered_after_a_reboot_keeps_its_estimate(
    tmp_path: Path,
    daemon: Daemon,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("vq.daemon._pgroup_alive", lambda pgid: False)
    workspace = tmp_path / "jobs" / "rebootjob0001"
    workspace.mkdir(parents=True)
    record = json.loads(_receipt(tmp_path).read_text())
    JobSpec(
        id="rebootjob0001",
        command=["true"],
        cwd=str(workspace),
        cpus=1,
        state=JobState.RUNNING,
        pgid=999001,
        recover_on_reboot=True,
        resource_estimate=record,
    ).write(daemon._spec_path("rebootjob0001"))
    daemon._reattach_or_interrupt_at_startup()
    (sibling,) = [
        s
        for s in (JobSpec.read(p) for p in daemon.queue_dir.glob("*.json"))
        if s.parent_jobid == "rebootjob0001"
    ]
    assert sibling.resource_estimate == record
