"""Retry failed local generations without discarding remote-job uncertainty."""
from types import SimpleNamespace

import pytest

from vq import admin, runtime_slots

SHA = "b" * 40


def failed_slot(root):
    runtime_slots.begin_slot_build(root, SHA, transaction_id="1" * 32,
                                   in_use=lambda: set())
    (runtime_slots.slot_path(root, SHA) / "partial-build").write_text("retained")


def configure(monkeypatch, specs):
    def host(ssh, *, scheduler="slurm", canonical=None):
        return SimpleNamespace(ssh=ssh, scheduler=scheduler,
                               fleet_canonical_host=canonical)
    hosts = {
        "batch": host("batch.example.invalid"),
        "batch-long": host("batch.example.invalid", canonical="batch"),
        "local-batch": host("user@localhost"),
        "local-alias": host("other.example.invalid", canonical="local-batch"),
        "workstation": host("remote.example.invalid", scheduler=None),
    }
    monkeypatch.setattr(admin.config, "load_config", lambda: SimpleNamespace(hosts=hosts))
    monkeypatch.setattr(admin, "_runtime_slot_spec_snapshot", lambda: specs)


@pytest.mark.parametrize("target", ["batch", "batch-long"])
@pytest.mark.parametrize("command", [["bash", "run.sh"],
                                    ["/other/runtime/releases/" + SHA + "/python"]])
def test_remote_unknown_job_allows_retry_without_changing_job(tmp_path, monkeypatch,
                                                             target, command):
    spec = SimpleNamespace(is_terminal=False, command=command,
                           scheduler_target=target, state="submit_outcome_unknown")
    before = dict(vars(spec))
    configure(monkeypatch, [spec])
    failed_slot(tmp_path)
    assert runtime_slots.begin_slot_build(
        tmp_path, SHA, transaction_id="2" * 32,
        in_use=lambda: admin._runtime_slot_in_use_snapshot(str(tmp_path)),
    )
    assert not (runtime_slots.slot_path(tmp_path, SHA) / "partial-build").exists()
    assert vars(spec) == before


@pytest.mark.parametrize("target", [None, "", "removed-target", "localhost",
                                    "local-batch", "local-alias", "workstation"])
def test_local_or_ambiguous_job_still_blocks_retry(tmp_path, monkeypatch, target):
    spec = SimpleNamespace(is_terminal=False, command=["bash", "run.sh"],
                           scheduler_target=target, state="submit_outcome_unknown")
    configure(monkeypatch, [spec])
    failed_slot(tmp_path)
    with pytest.raises(runtime_slots.RuntimeSlotError, match="referenced"):
        runtime_slots.begin_slot_build(
            tmp_path, SHA, transaction_id="2" * 32,
            in_use=lambda: admin._runtime_slot_in_use_snapshot(str(tmp_path)),
        )
    assert (runtime_slots.slot_path(tmp_path, SHA) / "partial-build").read_text() == "retained"


@pytest.mark.parametrize("binding", ["exact", "current", "pin"])
def test_local_reference_overrides_remote_target(tmp_path, monkeypatch, binding):
    command = [str(tmp_path / ("releases/" + SHA if binding == "exact" else "current")
                   / "source/.venv/bin/python")]
    pin = None
    if binding == "pin":
        command = ["bash", "run.sh"]
        pin = SimpleNamespace(resolved_runtime_root=str(tmp_path))
    spec = SimpleNamespace(is_terminal=False, command=command,
                           scheduler_target="batch", program_runtime_pin=pin)
    configure(monkeypatch, [spec])
    failed_slot(tmp_path)
    with pytest.raises(runtime_slots.RuntimeSlotError, match="referenced"):
        runtime_slots.begin_slot_build(
            tmp_path, SHA, transaction_id="2" * 32,
            in_use=lambda: admin._runtime_slot_in_use_snapshot(str(tmp_path)),
        )
