"""#161: a product build scheduled through vq that ends in a portable bundle.

`vq runtime-bundle build` submits the product's own builder as an ordinary
local job, with `VQ_BUILD_DIR` naming the directory it must fill, and writes
`<output>.vq-build.json`. `vq runtime-bundle build-finish` exits 3 while the
job is building, refuses a build that did not complete, and packs a completed
one through the existing `pack`. A real daemon runs a real builder here.
"""
from __future__ import annotations

import json
import sys
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from vq import config, paths
from vq import runtime_bundle as rb
from vq import runtime_bundle_build as build
from vq.cli import main
from vq.daemon import Daemon
from vq.spec import JobSpec, JobState

sys.path.insert(0, str(Path(__file__).parent))
from test_runtime_bundle import _description  # noqa: E402

BUILDER = ["sh", "-c", 'mkdir -p "$VQ_BUILD_DIR/bin" "$VQ_BUILD_DIR/data" && '
           'cp probe "$VQ_BUILD_DIR/bin/probe" && chmod 755 "$VQ_BUILD_DIR/bin/probe" && '
           'cp value "$VQ_BUILD_DIR/data/value"']


@pytest.fixture
def host(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[dict[str, Any]]:
    """A build host: its queue, a product source tree and a build description."""
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    source = tmp_path / "source"
    source.mkdir()
    (source / "probe").write_text("#!/bin/sh\ntest -f data/value\n")
    (source / "value").write_text("42\n")
    daemon = Daemon(max_cpus=4, poll_interval=0.05)
    paths.jobs_dir().mkdir(parents=True, exist_ok=True)
    try:
        yield {"source": source, "description": _description(tmp_path),
               "build_dir": tmp_path / "out", "output": tmp_path / "bundles" / "fixture",
               "daemon": daemon}
    finally:
        for rj in daemon._running.values():
            rj.popen.kill()
            rj.popen.wait(timeout=10)
        daemon._close_running_logs()
        daemon._queue_lock_fd.close()


def _build(host: dict[str, Any], *builder: str) -> Any:
    return CliRunner().invoke(main, [
        "runtime-bundle", "build", "-d", str(host["source"]),
        "--description", str(host["description"]), "--build-dir", str(host["build_dir"]),
        "--output", str(host["output"]), "--name", "fixture-build", "--",
        *(builder or BUILDER)])


def _finish(host: dict[str, Any]) -> Any:
    return CliRunner().invoke(main, ["runtime-bundle", "build-finish",
                                     str(build.record_path(host["output"]))])


def _run(host: dict[str, Any], job_id: str) -> JobState:
    daemon = host["daemon"]
    daemon._dispatch_pending()
    deadline = time.monotonic() + 30
    while not (spec := JobSpec.read(paths.resolve_spec_path(job_id))).is_terminal:
        assert time.monotonic() < deadline
        daemon._reconcile_running()
        time.sleep(0.05)
    return spec.state


def test_a_completed_build_is_packed_once(host: dict[str, Any]) -> None:
    submitted = _build(host)
    assert submitted.exit_code == 0, submitted.output
    document = json.loads(submitted.output)
    record = build.record_path(host["output"])
    assert record.name == "fixture.vq-build.json"
    assert json.loads(record.read_text()) == document
    assert (document["schema"], document["build_dir"], document["output"]) == (
        "vq.runtime-build-record/1", str(host["build_dir"]), str(host["output"]))
    assert document["description"]["program"] == "fixture"
    assert document["builder"] == BUILDER and document["artifact_id"] is None
    spec = JobSpec.read(paths.resolve_spec_path(document["job_id"]))
    assert spec.command == ["env", f"VQ_BUILD_DIR={host['build_dir']}", *BUILDER]
    assert spec.job_name == "fixture-build"

    waiting = _finish(host)
    assert waiting.exit_code == 3, waiting.output
    assert json.loads(waiting.output)["status"] == "building"
    assert json.loads(waiting.output)["state"] == "pending"
    assert not host["output"].exists()

    assert _run(host, document["job_id"]) == JobState.COMPLETED
    packed = _finish(host)
    assert packed.exit_code == 0, packed.output
    result = json.loads(packed.output)
    manifest = rb.BundleManifest.model_validate_json(
        (host["output"] / "manifest.json").read_bytes())
    assert (result["status"], result["artifact_id"]) == ("packed", manifest.artifact_id)
    assert manifest.build == rb.BuildDescription.model_validate_json(
        host["description"].read_bytes())
    assert set(manifest.files) == {"bin/probe", "data/value"}
    assert manifest.files["bin/probe"].executable
    stored = json.loads(record.read_text())
    assert stored["artifact_id"] == manifest.artifact_id and stored["packed_at"]

    again = _finish(host)
    assert again.exit_code == 0, again.output
    assert json.loads(again.output)["status"] == "already-packed"


def test_a_failed_build_is_not_packed(host: dict[str, Any]) -> None:
    submitted = _build(host, "sh", "-c", 'mkdir "$VQ_BUILD_DIR"; exit 1')
    assert submitted.exit_code == 0, submitted.output
    assert _run(host, json.loads(submitted.output)["job_id"]) == JobState.FAILED
    refused = _finish(host)
    assert refused.exit_code == 1 and "ended failed; nothing to pack" in refused.output
    assert not host["output"].exists()


def test_a_running_build_is_still_building(host: dict[str, Any]) -> None:
    job_id = json.loads(_build(host, "sleep", "30").output)["job_id"]
    host["daemon"]._dispatch_pending()
    waiting = _finish(host)
    assert waiting.exit_code == 3, waiting.output
    assert json.loads(waiting.output)["state"] == "running"
    assert JobSpec.read(paths.resolve_spec_path(job_id)).state == JobState.RUNNING


def test_a_replaced_bundle_is_not_reported_as_this_build(host: dict[str, Any]) -> None:
    document = json.loads(_build(host).output)
    _run(host, document["job_id"])
    assert _finish(host).exit_code == 0
    record = build.record_path(host["output"])
    stored = json.loads(record.read_text())
    record.write_text(json.dumps({**stored, "artifact_id": "0" * 64}))
    refused = _finish(host)
    assert refused.exit_code == 1 and "no longer the bundle this build packed" in refused.output


@pytest.mark.parametrize(("change", "message"), [
    ({"build_dir": Path("relative/out")}, "--build-dir must be an absolute path"),
    ({"output": Path("relative/bundle")}, "--output must be an absolute path"),
    ({"output": "inside"}, "--output must be outside --build-dir"),
    ({"output": "same"}, "--output must be outside --build-dir"),
    ({"existing": "build_dir"}, "--build-dir already exists"),
    ({"existing": "output"}, "or its build record already exists"),
    ({"existing": "record"}, "or its build record already exists"),
    ({"builder": []}, "needs the builder's command"),
    ({"description": "{}"}, "validation error"),
])
def test_a_refused_build_submits_nothing(host: dict[str, Any], change: dict[str, Any],
                                         message: str) -> None:
    kwargs: dict[str, Any] = {"source": host["source"], "description": host["description"],
                              "build_dir": host["build_dir"], "output": host["output"],
                              "builder": BUILDER}
    if change.get("output") == "inside":
        kwargs["output"] = host["build_dir"] / "bundle"
    elif change.get("output") == "same":
        kwargs["output"] = host["build_dir"]
    elif "existing" in change:
        target = {"build_dir": host["build_dir"], "output": host["output"],
                  "record": build.record_path(host["output"])}[change["existing"]]
        target.parent.mkdir(parents=True, exist_ok=True)
        target.symlink_to(host["source"] / "missing")  # even a dangling link is there
    elif "description" in change:
        kwargs["description"] = host["source"] / "build.json"
        kwargs["description"].write_text(change["description"])
    else:
        kwargs.update(change)
    with pytest.raises(ValueError, match=message):
        build.submit_build(**kwargs)
    assert list(paths.queue_dir().glob("*.json")) == []


@pytest.mark.parametrize(("content", "message"), [
    ("[]", "not a vq.runtime-build-record/1 record"),
    ('{"schema": "other"}', "not a vq.runtime-build-record/1 record"),
    ("{", "not a build record"),
])
def test_only_a_build_record_is_finished(tmp_path: Path, content: str, message: str) -> None:
    record = tmp_path / "x.vq-build.json"
    record.write_text(content)
    with pytest.raises(rb.BundleError, match=message):
        build.finish_build(record)


def test_a_build_whose_job_is_gone_is_refused(host: dict[str, Any]) -> None:
    document = json.loads(_build(host).output)
    paths.resolve_spec_path(document["job_id"]).unlink()
    refused = _finish(host)
    assert refused.exit_code == 1
    assert f"build job {document['job_id']} is not in this queue" in refused.output
