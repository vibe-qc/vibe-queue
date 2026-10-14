"""#165: campaign graphs, artifact manifests and journal-driven progression.

The campaign under test is the issue's first increment: calculate on one
host, validate on another, render only after a passed validation, plus a
cleanup node on a completion edge.
"""
from __future__ import annotations

import copy
import hashlib
import json
import os
import re
import shutil
from pathlib import Path

import pytest
from click.testing import CliRunner
from pydantic import ValidationError

from vq import campaign as cp
from vq.cli import main

RUNTIME = "a" * 64
VIEWER = "b" * 64


def _campaign(**overrides: object) -> dict:
    data: dict = {
        "schema_name": "vq.campaign/1",
        "campaign_id": "h2o-pipeline",
        "nodes": [
            {"id": "calculate", "kind": "compute", "host": "cluster-a", "program": "vibe-qc",
             "runtime_digest": RUNTIME, "command": ["python", "run.py"], "cpus": 16,
             "mem_mb": 64000, "outputs": [{"name": "result", "path": "out", "kind": "directory"}]},
            {"id": "validate", "kind": "validate", "host": "workstation", "program": "vibe-qc",
             "command": ["python", "check_scf.py", "in"], "cpus": 1,
             "inputs": [{"name": "result", "source": "calculate.result", "path": "in"}],
             "outputs": [{"name": "verdict", "path": "verdict.json", "kind": "file"}]},
            {"id": "render", "kind": "render", "host": "workstation", "program": "vibe-view",
             "runtime_digest": VIEWER, "command": ["vibe-view", "render", "in"], "cpus": 1,
             "inputs": [{"name": "result", "source": "calculate.result", "path": "in"}],
             "after": [{"node": "validate", "on": "success"}],
             "outputs": [{"name": "figure", "path": "figure.png", "kind": "file"}]},
            {"id": "cleanup", "kind": "other", "command": ["true"], "cpus": 1,
             "after": [{"node": "calculate", "on": "completion"}]},
        ],
    }
    data.update(overrides)
    return data


def _parsed(data: dict | None = None) -> cp.Campaign:
    return cp.Campaign.model_validate(data or _campaign())


def _node(data: dict, node_id: str) -> dict:
    return next(n for n in data["nodes"] if n["id"] == node_id)


class TestCampaignFile:
    def test_digest_order_and_edges(self) -> None:
        campaign = _parsed()
        reordered = _parsed(json.loads(json.dumps(_campaign(), sort_keys=True)))
        assert cp.campaign_digest(campaign) == cp.campaign_digest(reordered)
        assert campaign.order() == ["calculate", "cleanup", "validate", "render"]
        assert campaign.node("render").edges() == {"calculate": "success", "validate": "success"}
        assert campaign.node("cleanup").edges() == {"calculate": "completion"}

    @pytest.mark.parametrize("path", ["../out", "/abs/out", "a//b", "a/./b", "a\\b", "a/..", ""])
    def test_unsafe_paths_are_refused(self, path: str) -> None:
        data = _campaign()
        _node(data, "calculate")["outputs"][0]["path"] = path
        with pytest.raises(ValidationError, match="path"):
            _parsed(data)

    @pytest.mark.parametrize(("mutate", "message"), [
        (lambda d: _node(d, "validate")["outputs"].append(
            {"name": "inner", "path": "in/report", "kind": "file"}),
         "paths 'in' and 'in/report' overlap"),
        (lambda d: _node(d, "validate")["inputs"][0].update(source="nobody.result"),
         "names unknown node 'nobody'"),
        (lambda d: _node(d, "validate")["inputs"][0].update(source="calculate.figure"),
         "calculate declares no output 'figure'"),
        (lambda d: _node(d, "cleanup")["after"].append({"node": "cleanup", "on": "success"}),
         "after names itself"),
        (lambda d: _node(d, "calculate").setdefault("after", []).append(
            {"node": "render", "on": "success"}),
         "cycle: calculate -> render -> calculate"),
        (lambda d: _node(d, "render")["after"].append({"node": "calculate", "on": "completion"}),
         "contradicts"),
        (lambda d: _node(d, "render").update(after=[{"node": "validate", "on": "completion"}]),
         "a render node needs a validate node upstream"),
        (lambda d: _node(d, "calculate").update(env={"TOKEN": "x"}),
         "Extra inputs are not permitted"),
        (lambda d: d["nodes"].append(dict(d["nodes"][0])), "each node id may appear once"),
    ])
    def test_invalid_graphs_are_refused(self, mutate, message: str) -> None:
        data = _campaign()
        mutate(data)
        with pytest.raises(ValidationError, match=message):
            _parsed(data)


def _tree(root: Path) -> Path:
    out = root / "out"
    (out / "frames").mkdir(parents=True)
    (out / "h2o.qvf").write_bytes(b"qvf-bytes" * 100)
    (out / "frames" / "0001.json").write_text('{"energy": -76.0}', encoding="utf-8")
    (out / "empty").mkdir()
    return out


def _publish(root: Path, attempt: int = 1) -> cp.ArtifactManifest:
    return cp.publish_artifact(root, _parsed(), "calculate", attempt, "result",
                               runtime_digest=RUNTIME)


class TestArtifacts:
    def test_publish_lists_every_entry_with_its_digest(self, tmp_path: Path) -> None:
        out = _tree(tmp_path)
        manifest = _publish(tmp_path)
        assert manifest.kind == "directory" and manifest.campaign_sha256 == cp.campaign_digest(
            _parsed())
        assert [(e.path, e.type) for e in manifest.entries] == [
            ("empty", "directory"), ("frames", "directory"),
            ("frames/0001.json", "file"), ("h2o.qvf", "file")]
        qvf = next(e for e in manifest.entries if e.path == "h2o.qvf")
        assert qvf.sha256 == hashlib.sha256((out / "h2o.qvf").read_bytes()).hexdigest()
        assert manifest.total_bytes == 900 + len('{"energy": -76.0}')
        assert cp.verify_artifact(out, manifest)["status"] == "verified"

    def test_a_file_artifact(self, tmp_path: Path) -> None:
        (tmp_path / "verdict.json").write_text('{"passed": true}', encoding="utf-8")
        manifest = cp.publish_artifact(tmp_path, _parsed(), "validate", 1, "verdict")
        assert (manifest.kind, manifest.size, manifest.entries) == ("file", 16, [])
        staged = tmp_path / "staged.json"
        shutil.copy(tmp_path / "verdict.json", staged)
        assert cp.verify_artifact(staged, manifest)["status"] == "verified"
        staged.write_text('{"passed": false}', encoding="utf-8")
        assert cp.verify_artifact(staged, manifest)["status"] == "digest_mismatch"

    def test_publish_refuses_links_special_files_and_missing_outputs(
        self, tmp_path: Path,
    ) -> None:
        with pytest.raises(cp.CampaignError, match="output result is missing"):
            _publish(tmp_path)
        out = _tree(tmp_path)
        (out / "link").symlink_to(out / "h2o.qvf")
        with pytest.raises(cp.CampaignError, match="contains link, which is not a regular"):
            _publish(tmp_path)
        (out / "link").unlink()
        os.mkfifo(out / "pipe")
        with pytest.raises(cp.CampaignError, match="contains pipe"):
            _publish(tmp_path)
        (out / "pipe").unlink()
        shutil.move(out, tmp_path / "real")
        (tmp_path / "out").symlink_to(tmp_path / "real")
        with pytest.raises(cp.CampaignError, match="is a symbolic link"):
            _publish(tmp_path)

    @pytest.mark.parametrize(("damage", "status", "path"), [
        (lambda s: (s / "h2o.qvf").unlink(), "missing", "h2o.qvf"),
        (lambda s: (s / "h2o.qvf").write_bytes(b"qvf"), "partial", "h2o.qvf"),
        (lambda s: (s / "h2o.qvf").write_bytes(b"QVF-BYTES" * 100), "digest_mismatch", "h2o.qvf"),
        (lambda s: (s / "stray.txt").write_text("x"), "unexpected", "stray.txt"),
        (lambda s: ((s / "frames" / "0001.json").unlink(),
                    (s / "frames" / "0001.json").symlink_to(s / "h2o.qvf")),
         "unsafe", "frames/0001.json"),
        (lambda s: shutil.rmtree(s / "frames"), "missing", "frames"),
    ])
    def test_damaged_copies_are_not_verified(self, tmp_path: Path, damage, status: str,
                                             path: str) -> None:
        _tree(tmp_path)
        manifest = _publish(tmp_path)
        staged = tmp_path / "staged"
        shutil.copytree(tmp_path / "out", staged)
        damage(staged)
        result = cp.verify_artifact(staged, manifest)
        assert result["status"] == status
        assert path in [p["path"] for p in result["problems"]]

    def test_the_worst_problem_names_the_status(self, tmp_path: Path) -> None:
        _tree(tmp_path)
        manifest = _publish(tmp_path)
        staged = tmp_path / "staged"
        shutil.copytree(tmp_path / "out", staged)
        (staged / "stray.txt").write_text("x")
        (staged / "h2o.qvf").unlink()
        result = cp.verify_artifact(staged, manifest)
        assert result["status"] == "missing"
        assert {p["category"] for p in result["problems"]} == {"missing", "unexpected"}
        assert cp.verify_artifact(tmp_path / "absent", manifest)["status"] == "missing"
        (tmp_path / "linked").symlink_to(tmp_path / "out")
        assert cp.verify_artifact(tmp_path / "linked", manifest)["status"] == "unsafe"

    def test_inconsistent_manifests_are_refused(self, tmp_path: Path) -> None:
        _tree(tmp_path)
        data = _publish(tmp_path).model_dump(mode="json")
        orphan = copy.deepcopy(data)
        orphan["entries"] = [e for e in orphan["entries"] if e["path"] != "frames"]
        with pytest.raises(ValidationError, match="its directory frames is not listed"):
            cp.ArtifactManifest.model_validate(orphan)
        data["total_bytes"] += 1
        with pytest.raises(ValidationError, match="total_bytes"):
            cp.ArtifactManifest.model_validate(data)


DIGEST = cp.campaign_digest(_parsed())
M1, M2 = "1" * 64, "2" * 64


def _attempt(node: str, attempt: int, state: str, **fields: object) -> dict:
    record: dict = {"node": node, "attempt": attempt, "state": state,
                    "idempotency_key": cp.idempotency_key(DIGEST, node, attempt)}
    if attempt > 1:
        record["retry_of"] = attempt - 1
    if state in {"accepted", "running", "succeeded"}:
        record["job"] = {"host": "cluster-a" if node == "calculate" else "workstation",
                         "job_id": f"{node}-{attempt}"}
    record.update(fields)
    return record


def _verified(manifest: str = M1) -> dict:
    return {"result": {"manifest_sha256": manifest, "status": "verified"}}


def _bound(attempt: int = 1, manifest: str = M1) -> dict:
    return {"result": {"producer_attempt": attempt, "manifest_sha256": manifest}}


def _status(*attempts: dict) -> dict:
    journal = cp.Journal.model_validate(
        {"schema_name": "vq.campaign-journal/1", "campaign_sha256": DIGEST,
         "attempts": list(attempts)})
    return cp.campaign_status(_parsed(), journal)


def _nodes(report: dict) -> dict[str, dict]:
    return {n["node"]: n for n in report["nodes"]}


class TestProgression:
    def test_a_fresh_campaign_starts_with_its_roots(self) -> None:
        nodes = _nodes(report := _status())
        assert report["status"] == "in_progress"
        assert nodes["calculate"]["status"] == "ready"
        assert nodes["calculate"]["next"] == {
            "action": "submit", "attempt": 1,
            "idempotency_key": cp.idempotency_key(DIGEST, "calculate", 1), "stage": []}
        assert nodes["validate"]["status"] == "waiting"
        assert nodes["validate"]["reasons"] == ["waits for calculate to succeed"]
        assert nodes["cleanup"]["reasons"] == ["waits for calculate to finish"]

    def test_an_attempt_that_may_have_landed_is_reconciled_not_resubmitted(self) -> None:
        for _ in range(2):  # a controller restart reads the same journal
            calculate = _nodes(_status(_attempt("calculate", 1, "submitting")))["calculate"]
            assert calculate["status"] == "uncertain"
            assert calculate["next"]["action"] == "reconcile"
            assert calculate["next"]["idempotency_key"] == cp.idempotency_key(
                DIGEST, "calculate", 1)
        accepted = _nodes(_status(_attempt("calculate", 1, "accepted")))["calculate"]
        assert (accepted["status"], accepted["next"]) == ("running", {"action": "wait",
                                                                      "attempt": 1})

    @pytest.mark.parametrize(("artifacts", "reason"), [
        ({}, "artifact calculate.result is not published"),
        ({"result": {"manifest_sha256": M1, "status": "partial"}},
         "artifact calculate.result is partial"),
        ({"result": {"manifest_sha256": M1, "status": "digest_mismatch"}},
         "artifact calculate.result is digest_mismatch"),
    ])
    def test_unverified_artifacts_block_their_consumers(self, artifacts: dict,
                                                        reason: str) -> None:
        report = _status(_attempt("calculate", 1, "succeeded", artifacts=artifacts))
        nodes = _nodes(report)
        assert nodes["calculate"]["next"]["action"] == "publish"
        assert nodes["validate"]["status"] == "blocked"
        assert nodes["validate"]["reasons"] == [reason]
        assert nodes["cleanup"]["status"] == "ready"
        assert report["status"] == "blocked"

    def test_a_verified_artifact_is_staged_from_its_producing_attempt(self) -> None:
        nodes = _nodes(_status(_attempt("calculate", 1, "succeeded", artifacts=_verified())))
        assert nodes["validate"]["status"] == "ready"
        assert nodes["validate"]["next"]["stage"] == [
            {"input": "result", "from": "calculate.result", "to": "in",
             "producer_attempt": 1, "manifest_sha256": M1}]
        assert nodes["render"]["reasons"] == ["waits for validate to succeed"]

    def test_a_failed_validation_never_renders_or_succeeds(self) -> None:
        report = _status(
            _attempt("calculate", 1, "succeeded", artifacts=_verified()),
            _attempt("validate", 1, "failed", inputs=_bound()),
            _attempt("cleanup", 1, "succeeded"),
        )
        nodes = _nodes(report)
        assert nodes["render"]["status"] == "blocked"
        assert nodes["render"]["reasons"] == ["needs validate to succeed; it failed"]
        assert report["status"] == "failed"
        # The calculation result stays retained: render still needs it.
        assert report["retain"] == [{"node": "calculate", "attempt": 1,
                                     "needed_by": ["render", "validate"]}]

    def test_a_dependency_failure_blocks_success_edges_but_not_completion_edges(self) -> None:
        report = _status(_attempt("calculate", 1, "failed"))
        nodes = _nodes(report)
        assert nodes["validate"]["status"] == "blocked"
        assert nodes["render"]["reasons"] == ["needs calculate to succeed; it failed",
                                              "needs validate to succeed; it is blocked"]
        assert nodes["cleanup"]["status"] == "ready"
        assert nodes["calculate"]["next"]["detail"] == (
            "attempt 1 failed; a retry is an explicit attempt 2")
        assert report["status"] == "failed"

    def test_retry_lineage_is_visible_and_used_for_staging(self) -> None:
        report = _status(
            _attempt("calculate", 1, "failed"),
            _attempt("calculate", 2, "succeeded", artifacts=_verified(M2)),
            _attempt("cleanup", 1, "succeeded"),
        )
        nodes = _nodes(report)
        assert [(a["attempt"], a["state"], a["retry_of"])
                for a in nodes["calculate"]["attempts"]] == [(1, "failed", None),
                                                             (2, "succeeded", 1)]
        assert nodes["validate"]["next"]["stage"][0]["producer_attempt"] == 2
        assert nodes["validate"]["next"]["stage"][0]["manifest_sha256"] == M2

    def test_a_complete_campaign_succeeds_and_retains_nothing(self) -> None:
        report = _status(
            _attempt("calculate", 1, "succeeded", artifacts=_verified()),
            _attempt("validate", 1, "succeeded", inputs=_bound(),
                     artifacts={"verdict": {"manifest_sha256": M2, "status": "verified"}}),
            _attempt("render", 1, "succeeded", inputs=_bound(),
                     artifacts={"figure": {"manifest_sha256": M2, "status": "verified"}}),
            _attempt("cleanup", 1, "succeeded"),
        )
        assert report["status"] == "succeeded" and report["retain"] == []

    def test_a_render_without_its_figure_published_is_not_success(self) -> None:
        report = _status(
            _attempt("calculate", 1, "succeeded", artifacts=_verified()),
            _attempt("validate", 1, "succeeded", inputs=_bound(),
                     artifacts={"verdict": {"manifest_sha256": M2, "status": "verified"}}),
            _attempt("render", 1, "succeeded", inputs=_bound()),
            _attempt("cleanup", 1, "succeeded"),
        )
        assert report["status"] == "in_progress"
        assert _nodes(report)["render"]["next"]["outputs"] == ["figure"]

    @pytest.mark.parametrize(("attempts", "message"), [
        ([_attempt("calculate", 1, "succeeded", idempotency_key="f" * 64)],
         "idempotency key does not derive"),
        ([_attempt("calculate", 2, "failed")], "numbered 1, 2"),
        ([_attempt("calculate", 1, "succeeded", artifacts=_verified()),
          _attempt("calculate", 2, "submitting")],
         "only a failed or cancelled attempt is retried"),
        ([_attempt("calculate", 1, "failed"),
          {**_attempt("calculate", 2, "submitting"), "retry_of": None}],
         "must record retry_of 1"),
        ([{**_attempt("calculate", 1, "accepted"), "job": None}], "names its job"),
        ([_attempt("nobody", 1, "submitting")], "unknown node 'nobody'"),
        ([_attempt("calculate", 1, "running"), _attempt("validate", 1, "submitting")],
         "validate attempt 1 started although"),
        # A consumer staged from a failed attempt's files instead of the
        # verified retry: the substitution the issue forbids.
        ([_attempt("calculate", 1, "failed"),
          _attempt("calculate", 2, "succeeded", artifacts=_verified(M2)),
          _attempt("validate", 1, "running", inputs=_bound(1, M1))],
         "must be staged from calculate attempt 2"),
    ])
    def test_inconsistent_journals_are_refused(self, attempts: list[dict],
                                               message: str) -> None:
        with pytest.raises(cp.CampaignError, match=message):
            _status(*attempts)

    def test_a_journal_for_another_campaign_is_refused(self) -> None:
        journal = cp.Journal.model_validate({"schema_name": "vq.campaign-journal/1",
                                             "campaign_sha256": "e" * 64, "attempts": []})
        with pytest.raises(cp.CampaignError, match="a changed campaign starts a new journal"):
            cp.campaign_status(_parsed(), journal)


def test_cli_round_trip(tmp_path: Path) -> None:
    runner = CliRunner()
    campaign_file = tmp_path / "campaign.json"
    campaign_file.write_text(json.dumps(_campaign()), encoding="utf-8")

    validated = runner.invoke(main, ["campaign", "validate", str(campaign_file), "--json"])
    assert validated.exit_code == 0, validated.output
    assert json.loads(validated.output)["campaign_sha256"] == DIGEST

    workspace = tmp_path / "ws"
    _tree(workspace)
    published = runner.invoke(main, ["campaign", "artifact-publish", str(campaign_file),
                                     str(workspace), "--node", "calculate", "--attempt", "1",
                                     "--output", "result", "--runtime-digest", RUNTIME])
    assert published.exit_code == 0, published.output
    manifest_file = tmp_path / "manifest.json"
    manifest_file.write_text(published.output, encoding="utf-8")
    manifest = cp.load_manifest(manifest_file)

    verified = runner.invoke(main, ["campaign", "artifact-verify", str(manifest_file),
                                    str(workspace / "out")])
    assert verified.exit_code == 0 and "verified" in verified.output
    (workspace / "out" / "h2o.qvf").write_bytes(b"x")
    damaged = runner.invoke(main, ["campaign", "artifact-verify", str(manifest_file),
                                   str(workspace / "out")])
    assert damaged.exit_code == 1 and "partial: h2o.qvf" in damaged.output

    journal_file = tmp_path / "journal.json"
    journal_file.write_text(json.dumps({
        "schema_name": "vq.campaign-journal/1", "campaign_sha256": DIGEST,
        "attempts": [_attempt("calculate", 1, "succeeded", artifacts={"result": {
            "manifest_sha256": cp.manifest_digest(manifest), "status": "verified"}})],
    }), encoding="utf-8")
    status = runner.invoke(main, ["campaign", "status", str(campaign_file), str(journal_file)])
    assert status.exit_code == 0, status.output
    assert re.search(r"^validate\s+validate\s+workstation\s+ready\s+submit$", status.output,
                     re.MULTILINE)
    assert "calculate attempt 1: needed by render, validate" in status.output

    campaign_file.write_text(json.dumps(_campaign(campaign_id="other")), encoding="utf-8")
    refused = runner.invoke(main, ["campaign", "status", str(campaign_file), str(journal_file)])
    assert refused.exit_code == 1 and "starts a new journal" in refused.output
