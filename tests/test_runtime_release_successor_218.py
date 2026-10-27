"""Only terminal reconciliation may cross a provider implementation update."""

import copy
from types import SimpleNamespace

import pytest

from vq import config, submit
from vq import runtime_admission as admission

SOURCE = "a" * 40
OLD, NEW = "1" * 64, "2" * 64
JOB = "123456abcdef"


@pytest.fixture
def successor(monkeypatch):
    identity = dict(
        layout="packed-v1",
        program="program",
        source_sha=SOURCE,
        generation="/runtime/releases/" + SOURCE,
        source_root="/runtime/releases/" + SOURCE,
        python="/launchers/program-1.0-" + SOURCE + "-python",
        native_sha256="3" * 64,
        content_sha256="4" * 64,
        archive_sha256="5" * 64,
        launcher_sha256="6" * 64,
        healthy=True,
    )
    descriptor = dict(
        schema=admission.SCHEMA,
        identity=identity,
        authority="cluster",
        adapter_sha256=OLD,
        reservation=dict(key="7" * 64, owner="8" * 64),
    )
    spec = SimpleNamespace(
        id=JOB,
        program="program",
        scheduler_target="cluster",
        submission_intent_digest="9" * 64,
        program_runtime_pin=SimpleNamespace(
            managed_generation=descriptor,
            expected_git_sha=SOURCE,
            resolved_executable=identity["python"],
            scheduler_host="cluster",
        ),
    )
    binding = dict(
        owner="8" * 64, intent="9" * 64, program="program", source=SOURCE, identity=identity
    )
    proof = dict(job=JOB, queue_state="completed")
    terminal = dict(driver=proof, original_adapter_sha256=OLD, scheduler=dict(allocation="12345"))
    row = dict(key="7" * 64, binding=binding, state="released", job=JOB, terminal_proof=terminal)
    receipt = dict(
        reservation=row,
        release_reconciliation=dict(
            schema="vq.generation-release-successor/1",
            original_adapter_sha256=OLD,
            current_adapter_sha256=NEW,
            binding_sha256=admission.digest(binding),
            job=JOB,
            terminal_proof_sha256=admission.digest(terminal),
        ),
    )
    monkeypatch.setattr(config, "load_config", lambda: object())
    monkeypatch.setattr(submit, "_scheduler_deployment_authority", lambda *_: "cluster")
    calls = []

    def provider(cfg, authority, op, fields, *, adapter):
        calls.append(dict(authority=authority, op=op, fields=fields, adapter=adapter))
        return copy.deepcopy(receipt), NEW

    monkeypatch.setattr(admission, "_provider_call", provider)
    return spec, proof, receipt, calls


def test_release_successor_keeps_original_pin_and_binds_exact_proof(successor):
    spec, proof, receipt, calls = successor
    original = copy.deepcopy(vars(spec.program_runtime_pin))
    result = admission._call_bound(spec, "release", dict(job=JOB, terminal_proof=proof))
    assert result["state"] == "released"
    assert vars(spec.program_runtime_pin) == original
    assert calls[0]["adapter"] is None
    assert calls[0]["fields"]["release_predecessor_adapter_sha256"] == OLD
    admission._call_bound(spec, "check", dict(job=JOB))
    assert calls[1]["adapter"] == OLD
    assert "release_predecessor_adapter_sha256" not in calls[1]["fields"]


@pytest.mark.parametrize(
    "change",
    [
        "missing",
        "wrong_successor",
        "wrong_original",
        "binding",
        "job",
        "proof",
        "live",
        "missing_scheduler",
        "authority",
    ],
)
def test_changed_or_unproven_successor_refused(successor, monkeypatch, change):
    spec, proof, receipt, calls = successor
    if change == "missing":
        del receipt["release_reconciliation"]
    if change == "wrong_successor":
        receipt["release_reconciliation"]["current_adapter_sha256"] = "b" * 64
    if change == "wrong_original":
        receipt["release_reconciliation"]["original_adapter_sha256"] = "b" * 64
    if change == "binding":
        receipt["reservation"]["binding"]["intent"] = "b" * 64
    if change == "job":
        receipt["reservation"]["job"] = "000000abcdef"
    if change == "proof":
        receipt["reservation"]["terminal_proof"]["driver"] = dict(job=JOB, queue_state="failed")
    if change == "live":
        receipt["reservation"]["state"] = "committed"
    if change == "missing_scheduler":
        receipt["reservation"]["terminal_proof"]["scheduler"] = {}
    if change == "authority":
        monkeypatch.setattr(submit, "_scheduler_deployment_authority", lambda *_: "foreign")
    with pytest.raises(admission.AdmissionError):
        admission._call_bound(spec, "release", dict(job=JOB, terminal_proof=proof))
    if change == "authority":
        assert not calls
