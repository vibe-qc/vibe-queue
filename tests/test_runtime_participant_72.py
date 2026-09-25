"""Runtime migration participants (#72 piece 2): protocol and read-only plan.

The participant is a scripted fake adapter (tests/fixtures/
fake_runtime_participant.py) run locally in place of ssh. Hosts are ``cluster``
and ``second-driver``; paths are ``/site/...``. Numbers in test names refer to
the acceptance list in the approved design (§4.7).
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from vq import admin, config, paths, runtime_migration, runtime_participant

FAKE = Path(__file__).parent / "fixtures" / "fake_runtime_participant.py"
SHA = "b" * 40


class Site:
    def __init__(self, state: Path) -> None:
        self.state = state

    def script(self, op: str, *answers: dict) -> None:
        path = self.state / "table.json"
        table = json.loads(path.read_text()) if path.exists() else {}
        table.setdefault(op, []).extend(answers)
        path.write_text(json.dumps(table))

    def requests(self) -> list[dict]:
        log = self.state / "requests.jsonl"
        return [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []


@pytest.fixture
def site(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Site:
    state = tmp_path / "site"
    state.mkdir()

    def local_runner(host_cfg, argv, stdin, timeout):
        proc = subprocess.run(argv, input=stdin, capture_output=True, text=True,
                              timeout=timeout, check=False)
        return proc.returncode, proc.stdout, proc.stderr

    monkeypatch.setattr(runtime_participant, "RUNNER", local_runner)
    command = f"{sys.executable} {FAKE} {state}"
    cfg = config.Config(hosts={
        "cluster": config.HostConfig(ssh="cluster", runtime_participant_command=command),
        "second-driver": config.HostConfig(ssh="second-driver"),
        "bare": config.HostConfig(ssh="bare"),
    })
    monkeypatch.setattr(config, "load_config", lambda: cfg)
    site = Site(state)
    site.other_authority = {}  # key overrides the second driver reports

    def second_authority(cfg_, authority, host):
        document = runtime_participant.runtime_bindings(cfg_, host)
        document["bindings"].update(site.other_authority)
        document["bindings_sha256"] = runtime_participant._digest(
            runtime_participant._json(document["bindings"]))
        return document

    monkeypatch.setattr(runtime_participant, "AUTHORITY_BINDINGS", second_authority)
    site.inventory_recorded = []  # (authority, host, record) sent to other authorities
    monkeypatch.setattr(runtime_participant, "INVENTORY_RECORDER",
                        lambda cfg_, authority, host, record:
                        site.inventory_recorded.append((authority, host, record)))
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    return site


def _manifest(**changes) -> runtime_migration.Manifest:
    fields = dict(
        component="vibeqc-dev", program="vibeqc-dev", source_sha=SHA, external_consumers=[],
        participants=[{"host": "cluster", "kind": "scheduler-packed", "programs": ["vibeqc-dev"],
                       "authorities": ["localhost", "second-driver"]}],
    )
    fields.update(changes)
    return runtime_migration.Manifest.model_validate(fields)


def test_1_plan_is_one_inventory_per_participant_and_no_lock(site, monkeypatch) -> None:
    monkeypatch.setattr(admin, "admin_update_ownership",
                        lambda *a, **k: pytest.fail("planning must not take ownership"))
    site.script("inventory", {"receipt": {"store": "mirror", "current": "c" * 40}})

    value = runtime_migration.plan(_manifest())

    assert [r["op"] for r in site.requests()] == ["inventory"]
    assert value["blockers"] == [] and value["mode"] == "participants"
    assert value["participants"][0]["inventory"] == {"store": "mirror", "current": "c" * 40}
    assert value["participants"][0]["adapter_sha256"] == "a" * 64
    site.script("inventory", {"receipt": {"store": "canonical", "current": "c" * 40}})
    assert runtime_migration.plan(_manifest())["plan_id"] != value["plan_id"]


@pytest.mark.parametrize("case", ["both-lists", "no-command", "mixed", "unknown-authority"])
def test_2_unreviewed_or_ambiguous_participants_block(site, case) -> None:
    if case == "both-lists":
        manifest = _manifest(external_consumers=["cluster"])
        expected = "both a participant and an external consumer"
    elif case == "no-command":
        manifest = _manifest(participants=[{
            "host": "bare", "kind": "scheduler-packed", "programs": ["vibeqc-dev"],
            "authorities": ["localhost"]}])
        expected = "no reviewed participant command"
    elif case == "mixed":
        manifest = _manifest(runtime_root="/site/runtimes/vibeqc-dev",
                             registries=[{"path": "/site/a.toml", "candidate": "/site/b.toml"}])
        expected = "separate transactions"
    else:
        manifest = _manifest(participants=[{
            "host": "cluster", "kind": "scheduler-packed", "programs": ["vibeqc-dev"],
            "authorities": ["localhost", "third-driver"]}])
        expected = "third-driver is not a configured host"

    blockers = runtime_migration.plan(manifest)["blockers"]

    assert any(expected in blocker for blocker in blockers), blockers


def test_2_an_inventory_that_is_not_ok_blocks(site) -> None:
    site.script("inventory", {"outcome": "refused", "errors": ["publication lock held"]})

    blockers = runtime_migration.plan(_manifest())["blockers"]

    assert blockers == ["cluster inventory answered refused: publication lock held"]


@pytest.mark.parametrize("answer, message", [
    ({"raw": '{"schema": "vq.runtime-participant/1", "schema": "x"}'}, "duplicate key"),
    ({"raw": '{"a": NaN}'}, "non-finite"),
    ({"op_id": "someone-else"}, "does not answer this request"),
    ({"raw": "not json"}, "not JSON"),
])
def test_13_malformed_responses_refuse(site, answer, message) -> None:
    site.script("inventory", answer)

    with pytest.raises(runtime_participant.ParticipantError, match=message):
        runtime_participant.request(config.load_config().host("cluster"), "inventory",
                                    fields={})


def test_13_an_adapter_change_refuses(site) -> None:
    site.script("inventory", {"adapter": "e" * 64})

    with pytest.raises(runtime_participant.ParticipantError, match="adapter changed"):
        runtime_participant.request(config.load_config().host("cluster"), "inventory",
                                    fields={}, expected_adapter="a" * 64)


def test_13_oversized_and_tampered_documents_refuse() -> None:
    with pytest.raises(runtime_participant.ParticipantError, match="16 MiB"):
        runtime_participant.parse_document(b" " * (runtime_participant.MAX_DOCUMENT_BYTES + 1))


def test_13_a_receipt_digest_mismatch_refuses(site, monkeypatch) -> None:
    doc = json.loads(runtime_participant._json({
        "schema": runtime_participant.SCHEMA, "op": "inventory", "op_id": "X",
        "outcome": "ok", "receipt": {"op": "inventory"}, "receipt_sha256": "0" * 64,
        "errors": [], "participant": {"adapter_sha256": "a" * 64}}))

    def runner(host_cfg, argv, stdin, timeout):
        doc["op_id"] = json.loads(stdin)["op_id"]
        return 0, json.dumps(doc), ""

    monkeypatch.setattr(runtime_participant, "RUNNER", runner)
    with pytest.raises(runtime_participant.ParticipantError, match="digest"):
        runtime_participant.request(config.load_config().host("cluster"), "inventory",
                                    fields={})


def test_14_fixtures_use_only_placeholder_hosts_and_site_paths() -> None:
    text = FAKE.read_text() + Path(__file__).read_text()
    # Built at run time so this check does not match its own source.
    for personal in ("/" + "home" + "/", "/" + "Users" + "/"):
        assert personal not in text


def test_runtime_bindings_report_only_binding_keys_with_a_stable_digest(site) -> None:
    cfg = config.load_config()

    first = runtime_participant.runtime_bindings(cfg, "cluster")
    second = runtime_participant.runtime_bindings(cfg, "cluster")

    assert set(first["bindings"]) == {
        "remote_vq", "remote_python", "branches", "branch_aliases", "command_wrappers",
        "scheduler_runtime_deployments", "fleet_role", "scheduler_driver"}
    assert "runtime_participant_command" not in json.dumps(first)
    assert first["bindings_sha256"] == second["bindings_sha256"]


def test_4_the_plan_binds_every_authority_and_tracks_its_bindings(site) -> None:
    value = runtime_migration.plan(_manifest())
    digests = value["participants"][0]["authority_bindings_sha256"]
    assert set(digests) == {"localhost", "second-driver"}

    site.other_authority = {"fleet_role": "vq-only"}
    changed = runtime_migration.plan(_manifest())

    assert changed["blockers"] == []  # fleet_role may differ between drivers
    assert changed["plan_id"] != value["plan_id"]


def test_4_authorities_disagreeing_on_binding_paths_block(site) -> None:
    site.other_authority = {"remote_python": "/site/other/python",
                            "branches": {"main": "/site/other-launcher"}}

    blockers = runtime_migration.plan(_manifest())["blockers"]

    assert blockers == ["authorities disagree on cluster bindings: branches, remote_python"]


def test_4_an_authority_that_cannot_report_blocks(site, monkeypatch) -> None:
    def unreachable(cfg_, authority, host):
        raise OSError("connection refused")

    monkeypatch.setattr(runtime_participant, "AUTHORITY_BINDINGS", unreachable)

    blockers = runtime_migration.plan(_manifest())["blockers"]

    assert blockers == ["authority second-driver could not report bindings for cluster: "
                        "connection refused"]


def _status_row(host: str, program: str) -> None:
    from vq import admin

    records = admin.load_scheduler_runtime_status()
    records[f"{host}:{program}"] = admin.SchedulerRuntimeUpdateRecord(
        host=host, program=program, last_updated_at="2026-09-25T00:00:00+00:00",
        last_success=True, expected_sha=SHA, actual_sha=SHA, healthy=True)
    admin._write_scheduler_runtime_status(records)


def _record(**programs) -> dict:
    return {"schema": runtime_participant.RECORD_SCHEMA, "host": "cluster",
            "plan_id": "p" * 64, "ready_id": "r" * 64,
            "programs": {name: {"receipt_sha256": "d" * 64, "store": "canonical",
                                "launcher_schema": 3} for name in programs}}


def test_11_a_migration_is_recorded_on_the_lane_and_survives_updates(site) -> None:
    from vq import admin

    _status_row("cluster", "vibeqc-dev")

    keys = runtime_participant.record_migration("cluster", _record(**{"vibeqc-dev": 1}))

    assert keys == ["cluster:vibeqc-dev"]
    row = admin.load_scheduler_runtime_status()["cluster:vibeqc-dev"]
    assert row.migration["store"] == "canonical" and row.migration["plan_id"] == "p" * 64
    admin.record_scheduler_runtime_outcome(admin.SchedulerRuntimeUpdateResult(
        host="cluster", program="vibeqc-dev", mode="update", command="/site/deploy",
        command_ssh="cluster", verify_command="/site/verify", verify_ssh="cluster",
        expected_sha=SHA))
    assert admin.load_scheduler_runtime_status()["cluster:vibeqc-dev"].migration == row.migration


def test_11_recording_needs_an_existing_row_and_the_right_host(site) -> None:
    with pytest.raises(runtime_participant.ParticipantError, match="no status row"):
        runtime_participant.record_migration("cluster", _record(**{"vibeqc-dev": 1}))
    with pytest.raises(runtime_participant.ParticipantError, match="not for this host"):
        runtime_participant.record_migration("second-driver", _record(**{"vibeqc-dev": 1}))


def test_cli_prints_bindings_and_records(site, tmp_path) -> None:
    from click.testing import CliRunner

    from vq.cli import main

    shown = CliRunner().invoke(main, ["admin", "runtime-bindings", "cluster", "--json"])
    assert shown.exit_code == 0, shown.output
    assert json.loads(shown.output)["schema"] == runtime_participant.BINDINGS_SCHEMA

    _status_row("cluster", "vibeqc-dev")
    receipt = tmp_path / "record.json"
    receipt.write_text(json.dumps(_record(**{"vibeqc-dev": 1})))
    recorded = CliRunner().invoke(main, ["admin", "runtime-bindings", "cluster",
                                         "--record-migration", str(receipt)])
    assert recorded.exit_code == 0, recorded.output
    assert json.loads(recorded.output) == {"recorded": ["cluster:vibeqc-dev"]}


def _ops(site: Site) -> list[str]:
    return [r["op"] for r in site.requests()]


def test_3_prepare_once_per_participant_and_repeat_is_the_same_ready_plan(site) -> None:
    site.script("prepare", {"receipt": {"release": SHA, "activation": "none"}})
    plan_id = runtime_migration.plan(_manifest())["plan_id"]

    ready = runtime_migration.prepare(_manifest(), plan_id)

    assert _ops(site) == ["inventory", "inventory", "prepare", "inventory"]
    assert ready["status"] == "ready" and len(ready["ready_id"]) == 64
    first_op = ready["preparation"][0]["op_id"]
    assert runtime_participant.load_ready(ready["ready_id"]) == ready
    site.script("prepare", {"receipt": {"release": SHA, "activation": "none"}})

    again = runtime_migration.prepare(_manifest(), plan_id)

    assert again["ready_id"] == ready["ready_id"]
    replayed = [r for r in site.requests() if r["op"] == "prepare"]
    assert [r["op_id"] for r in replayed] == [first_op, first_op]


def test_3_a_changed_inventory_before_preparation_is_stale(site) -> None:
    plan_id = runtime_migration.plan(_manifest())["plan_id"]
    site.script("inventory", {"receipt": {"store": "changed"}})

    with pytest.raises(runtime_migration.MigrationError, match="plan is stale"):
        runtime_migration.prepare(_manifest(), plan_id)
    assert "prepare" not in _ops(site)


def test_3_a_different_receipt_on_replay_refuses(site) -> None:
    plan_id = runtime_migration.plan(_manifest())["plan_id"]
    runtime_migration.prepare(_manifest(), plan_id)
    site.script("prepare", {"receipt": {"release": "c" * 40}})

    with pytest.raises(runtime_migration.MigrationError, match="different preparation receipt"):
        runtime_migration.prepare(_manifest(), plan_id)


def test_3_pending_preparation_is_followed_through_inspect(site, monkeypatch) -> None:
    monkeypatch.setattr(runtime_participant, "SLEEP", lambda seconds: None)
    site.script("prepare", {"outcome": "pending"})
    site.script("inspect",
                {"receipt": {"status": "in_progress", "job_id": "42"}},
                {"receipt": {"status": "applied", "receipt": {"release": SHA}}})
    plan_id = runtime_migration.plan(_manifest())["plan_id"]

    ready = runtime_migration.prepare(_manifest(), plan_id)

    assert ready["preparation"][0]["receipt"] == {"release": SHA}
    inspects = [r for r in site.requests() if r["op"] == "inspect"]
    assert {r["target_op_id"] for r in inspects} == {ready["preparation"][0]["op_id"]}


def test_3_a_preparation_still_running_at_the_deadline_refuses(site, monkeypatch) -> None:
    ticks = iter([0.0, 1e9])
    monkeypatch.setattr(runtime_participant, "CLOCK", lambda: next(ticks))
    site.script("prepare", {"outcome": "pending"})
    plan_id = runtime_migration.plan(_manifest())["plan_id"]

    with pytest.raises(runtime_migration.MigrationError, match="still running"):
        runtime_migration.prepare(_manifest(), plan_id)
    assert runtime_participant.load_ready(plan_id) is None


# --- P4: apply, verify, rollback, lease, journal, resume, record, acknowledge ---

HEALTHY = {"receipt": {"healthy": True, "activation": "atomic", "quiescent": True,
                       "store": "canonical", "launcher_schema": 3}}


@pytest.fixture
def tx(site, monkeypatch, tmp_path):
    """A prepared participant migration with lease and recorder fakes."""
    leases: list[list[str]] = []
    recorded: list[tuple[str, str]] = []
    remote_failures: dict[str, str] = {}

    def lease(argv):
        leases.append(list(argv))
        return 0, ""

    def recorder(cfg_, authority, host, record):
        if authority in remote_failures:
            raise OSError(remote_failures[authority])
        recorded.append((authority, host))

    monkeypatch.setattr(runtime_participant, "LEASE", lease)
    monkeypatch.setattr(runtime_participant, "RECORDER", recorder)
    monkeypatch.setattr(runtime_participant, "SLEEP", lambda seconds: None)
    _status_row("cluster", "vibeqc-dev")
    plan_id = runtime_migration.plan(_manifest())["plan_id"]
    ready = runtime_migration.prepare(_manifest(), plan_id)
    site.leases, site.recorded, site.remote_failures = leases, recorded, remote_failures
    site.ready, site.receipt = ready, tmp_path / "transaction"
    return site


def _apply(tx):
    return runtime_migration.apply(_manifest(), tx.ready["ready_id"], tx.receipt)


def _journal(tx) -> dict:
    return json.loads((tx.receipt / "transaction.json").read_text())


def _ops_after_prepare(tx) -> list[str]:
    ops = _ops(tx)
    return ops[ops.index("prepare") + 2:]  # after prepare and its post inventory


def _held(tx) -> bool:
    return _journal(tx)["lease"]["held"]


def test_4_apply_needs_a_ready_id(site, tmp_path) -> None:
    plan_id = runtime_migration.plan(_manifest())["plan_id"]

    with pytest.raises(runtime_migration.MigrationError, match="ready_id"):
        runtime_migration.apply(_manifest(), plan_id, tmp_path / "t")


def test_4_changed_bindings_or_inventory_refuse_before_any_mutation(tx) -> None:
    tx.other_authority = {"remote_python": "/site/other"}
    with pytest.raises(runtime_migration.MigrationError, match="authority bindings changed"):
        _apply(tx)
    tx.other_authority = {}
    tx.script("inventory", {"receipt": {"store": "drifted"}})
    with pytest.raises(runtime_migration.MigrationError, match="inventory changed"):
        _apply(tx)
    assert "apply" not in _ops(tx) and tx.leases == []


def test_4_apply_sends_expected_before_from_the_ready_plan(tx) -> None:
    tx.script("verify", HEALTHY)

    state = _apply(tx)

    applied = [r for r in tx.requests() if r["op"] == "apply"]
    assert applied[0]["expected_before"] == tx.ready["post_inventory"]["cluster"]["inventory"]
    assert state["state"] == "applied"
    assert state["records"] == {"localhost:cluster": "recorded",
                                "second-driver:cluster": "recorded"}


def test_5_a_refused_apply_is_resumed_with_identical_request_bytes(tx) -> None:
    tx.script("apply", {"outcome": "refused", "errors": ["publication lock held"]})
    with pytest.raises(runtime_migration.MigrationError, match="refused"):
        _apply(tx)
    assert _journal(tx)["state"] == "applying" and not _held(tx)
    tx.script("verify", HEALTHY)

    state = runtime_migration.recover(tx.receipt)

    first, second = [r for r in tx.requests() if r["op"] == "apply"]
    assert first == second
    assert state["state"] == "applied"


@pytest.mark.parametrize("inspected, expect", [
    ({"status": "applied", "receipt": {"applied": True}}, "applied"),
    ({"status": "not_applied"}, "resend"),
    ({"status": "in_progress", "job_id": "77"}, "refuse"),
])
def test_6_an_unknown_apply_is_resolved_through_inspect(tx, inspected, expect) -> None:
    tx.script("apply", {"outcome": "unknown"})
    with pytest.raises(runtime_migration.MigrationError, match="unknown outcome"):
        _apply(tx)
    assert _held(tx)  # the outcome is unknown: the lease stays
    tx.script("inspect", {"receipt": inspected})
    tx.script("verify", HEALTHY)

    if expect == "refuse":
        with pytest.raises(runtime_migration.MigrationError, match="job 77"):
            runtime_migration.recover(tx.receipt)
        assert [r["op"] for r in tx.requests()].count("apply") == 1
        return
    state = runtime_migration.recover(tx.receipt)

    applies = [r for r in tx.requests() if r["op"] == "apply"]
    assert len(applies) == (1 if expect == "applied" else 2)
    assert len({r["op_id"] for r in applies}) == 1
    assert state["state"] == "applied" and not _held(tx)


def test_7_a_crash_after_the_participant_answered_never_repeats_apply(tx, monkeypatch) -> None:
    real = runtime_participant.request

    def lost_answer(host_cfg, op, **kwargs):
        response = real(host_cfg, op, **kwargs)
        if op == "apply":
            raise OSError("connection reset after the answer was sent")
        return response

    monkeypatch.setattr(runtime_participant, "request", lost_answer)
    with pytest.raises(runtime_migration.MigrationError, match="unknown outcome"):
        _apply(tx)
    monkeypatch.setattr(runtime_participant, "request", real)
    tx.script("inspect", {"receipt": {"status": "applied", "receipt": {"applied": True}}})
    tx.script("verify", HEALTHY)

    state = runtime_migration.recover(tx.receipt)

    assert [r["op"] for r in tx.requests()].count("apply") == 1
    assert state["state"] == "applied"


@pytest.mark.parametrize("verdict", [{"receipt": {"healthy": False}},
                                     {"receipt": {"healthy": True, "activation": "staged",
                                                  "quiescent": True}}])
def test_8_an_unhealthy_verify_rolls_back_and_records_nothing(tx, verdict) -> None:
    tx.script("verify", verdict, HEALTHY)

    state = _apply(tx)

    assert state["state"] == "rolled_back"
    assert _ops_after_prepare(tx)[-3:] == ["verify", "rollback", "verify"]
    assert state["records"] == {} and tx.recorded == []
    assert not _held(tx)


def test_9_rollback_after_a_complete_apply_uses_the_after_state(tx) -> None:
    tx.script("verify", HEALTHY, HEALTHY)
    applied = _apply(tx)

    state = runtime_migration.recover(tx.receipt, rollback=True)

    rollback = [r for r in tx.requests() if r["op"] == "rollback"][0]
    apply_row = [r for r in applied["rows"] if r["step"] == "apply"][0]
    assert rollback["expected_after"] == apply_row["response"]["receipt"]
    assert state["state"] == "rolled_back" and not _held(tx)
    assert state["records"]["second-driver:cluster"] == "recorded"


def test_10_the_lease_brackets_every_attempt_with_its_own_owner(tx) -> None:
    tx.script("verify", HEALTHY)

    _apply(tx)

    owner = f"runtime-migration:{tx.ready['plan_id']}"
    acquire, release = tx.leases
    assert acquire[:2] == ["drain", "--scheduler-host"] and owner in acquire
    assert release[:2] == ["drain", "--release"] and owner in release
    assert "--release" not in acquire


def test_10_a_plan_that_never_applied_leaves_no_lease(tx) -> None:
    runtime_migration.plan(_manifest())

    assert tx.leases == []


def test_11_a_failed_record_leaves_the_transaction_unacknowledged(tx) -> None:
    tx.remote_failures["second-driver"] = "authority unreachable"
    tx.script("verify", HEALTHY)

    state = _apply(tx)

    assert state["state"] == "applied"
    assert state["records"]["second-driver:cluster"] == "failed: authority unreachable"
    with pytest.raises(runtime_migration.MigrationError, match="second-driver:cluster"):
        runtime_migration.recover(tx.receipt, acknowledge=True)
    tx.remote_failures.clear()
    resumed = runtime_migration.recover(tx.receipt)
    assert resumed["records"]["second-driver:cluster"] == "recorded"


def test_12_acknowledge_is_terminal_and_grants_no_cleanup(tx) -> None:
    from vq import runtime_retention

    tx.script("verify", HEALTHY)
    _apply(tx)

    state = runtime_migration.recover(tx.receipt, acknowledge=True)

    assert state["state"] == "acknowledged"
    assert _ops(tx)[-1] == "acknowledge"
    assert "rollback" not in _ops(tx)
    with pytest.raises(runtime_retention.RetentionError):
        runtime_retention.plan("vibeqc-dev", config.load_config())
    with pytest.raises(runtime_migration.MigrationError, match="acknowledged"):
        runtime_migration.recover(tx.receipt)


# --- P5: the participant inventory is status on the lane's row ---

def _inventory(current: str, previous: str | None, *, store: str = "canonical") -> dict:
    return {"schema": runtime_participant.INVENTORY_SCHEMA, "kind": "scheduler-packed",
            "state_sha256": "5" * 64, "observed_at": "2026-09-25T12:00:00+00:00",
            "programs": {"vibeqc-dev": {
                "store": store, "current": current, "previous": previous,
                "launcher_schema": 3, "cache": "warm", "stable_launcher": "/site/bin/vq",
                "releases": {}, "immutable_launchers": [], "lock": None,
                "inventory_sha256": "6" * 64}},
            "helper": {}, "workspace": {"path": "/site/ws",
                                        "lease_expires_at": "2026-10-01T00:00:00+00:00",
                                        "extensions_left": 2},
            "slurm_jobs": []}


def _row_inventory() -> dict | None:
    return admin.load_scheduler_runtime_status()["cluster:vibeqc-dev"].inventory


def test_p5_summary_keeps_status_fields_and_ignores_other_receipts() -> None:
    summary = runtime_participant.summarize_inventory(_inventory("c" * 40, "p" * 40))

    assert summary == {"vibeqc-dev": {
        "store": "canonical", "current": "c" * 40, "previous": "p" * 40,
        "launcher_schema": 3, "cache": "warm", "inventory_sha256": "6" * 64,
        "lease_expires_at": "2026-10-01T00:00:00+00:00",
        "observed_at": "2026-09-25T12:00:00+00:00"}}
    assert runtime_participant.summarize_inventory({"op": "inventory"}) == {}
    assert runtime_participant.summarize_inventory(None) == {}


def test_p5_record_inventory_touches_only_the_inventory_field(site) -> None:
    _status_row("cluster", "vibeqc-dev")
    before = admin.load_scheduler_runtime_status()["cluster:vibeqc-dev"]
    record = {"schema": runtime_participant.INVENTORY_RECORD_SCHEMA, "host": "cluster",
              "programs": {**runtime_participant.summarize_inventory(
                  _inventory("c" * 40, None)), "unregistered": {"store": "none"}}}

    assert runtime_participant.record_inventory("cluster", record) == ["cluster:vibeqc-dev"]

    after = admin.load_scheduler_runtime_status()["cluster:vibeqc-dev"]
    assert after.inventory["current"] == "c" * 40 and after.inventory["recorded_at"]
    after.inventory = None
    assert after == before  # LAST OK and everything else unchanged
    with pytest.raises(runtime_participant.ParticipantError, match="not for this host"):
        runtime_participant.record_inventory("second-driver", record)
    admin.record_scheduler_runtime_outcome(admin.SchedulerRuntimeUpdateResult(
        host="cluster", program="vibeqc-dev", mode="update", command="/site/deploy",
        command_ssh="cluster", verify_command="/site/verify", verify_ssh="cluster",
        expected_sha=SHA))
    assert _row_inventory()["current"] == "c" * 40  # carried forward


def test_p5_the_read_only_plan_records_nothing(site) -> None:
    _status_row("cluster", "vibeqc-dev")
    site.script("inventory", {"receipt": _inventory("c" * 40, None)})

    runtime_migration.plan(_manifest())

    assert _row_inventory() is None and site.inventory_recorded == []


def test_p5_prepare_records_the_post_preparation_inventory_on_every_authority(site) -> None:
    _status_row("cluster", "vibeqc-dev")
    site.script("inventory", {"receipt": _inventory("c" * 40, None)},
                {"receipt": _inventory("c" * 40, None)},
                {"receipt": _inventory("c" * 40, None, store="mirror")})
    plan_id = runtime_migration.plan(_manifest())["plan_id"]

    runtime_migration.prepare(_manifest(), plan_id)

    assert _row_inventory()["store"] == "mirror"
    [(authority, host, record)] = site.inventory_recorded
    assert (authority, host) == ("second-driver", "cluster")
    assert record["programs"]["vibeqc-dev"]["store"] == "mirror"


def test_p5_apply_records_the_inventory_observed_after_verification(tx) -> None:
    tx.script("verify", HEALTHY)
    tx.script("inventory", {"receipt": tx.ready["post_inventory"]["cluster"]["inventory"]},
              {"receipt": _inventory("n" * 40, "c" * 40)})

    state = _apply(tx)

    assert _ops_after_prepare(tx)[-3:] == ["apply", "verify", "inventory"]
    assert state["state"] == "applied"
    row = _row_inventory()
    assert (row["current"], row["previous"], row["launcher_schema"]) == ("n" * 40, "c" * 40, 3)
    lane = admin.load_scheduler_runtime_status()["cluster:vibeqc-dev"]
    assert (lane.actual_sha, lane.last_ok_sha) == (SHA, None)  # LAST OK is the verifier's


def test_p5_a_failed_status_inventory_still_records_the_migration(tx) -> None:
    tx.script("verify", HEALTHY)
    tx.script("inventory", {"receipt": tx.ready["post_inventory"]["cluster"]["inventory"]},
              {"outcome": "refused", "errors": ["busy"]})

    state = _apply(tx)

    assert state["state"] == "applied" and state["records"]["localhost:cluster"] == "recorded"
    row = admin.load_scheduler_runtime_status()["cluster:vibeqc-dev"]
    assert row.migration is not None and row.inventory is None


def test_p5_cli_records_an_inventory(site, tmp_path) -> None:
    from click.testing import CliRunner

    from vq.cli import main

    _status_row("cluster", "vibeqc-dev")
    record = tmp_path / "inventory.json"
    record.write_text(json.dumps({
        "schema": runtime_participant.INVENTORY_RECORD_SCHEMA, "host": "cluster",
        "programs": runtime_participant.summarize_inventory(_inventory("c" * 40, None))}))

    result = CliRunner().invoke(main, ["admin", "runtime-bindings", "cluster",
                                       "--record-inventory", str(record)])

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == {"recorded": ["cluster:vibeqc-dev"]}
    both = CliRunner().invoke(main, ["admin", "runtime-bindings", "cluster",
                                     "--record-inventory", str(record),
                                     "--record-migration", str(record)])
    assert both.exit_code == 2 and "exclusive" in both.output


# --- Review of P1–P5 against the site adapter ---

def test_review_1_the_real_ssh_runner_reaches_the_adapter(site, monkeypatch) -> None:
    from vq import transport

    monkeypatch.setattr(runtime_participant, "RUNNER", runtime_participant._ssh_runner)
    monkeypatch.setattr(transport, "_ssh_base", lambda host_cfg, **kw: ["sh", "-c"])

    response = runtime_participant.request(
        config.load_config().host("cluster"), "inventory",
        fields={"programs": ["vibeqc-dev"], "kind": "scheduler-packed"})

    assert response.outcome == "ok"


def _volatile(state: str, observed: str, jobs: list) -> dict:
    receipt = _inventory("c" * 40, None)
    return {**receipt, "state_sha256": state, "observed_at": observed, "slurm_jobs": jobs}


def test_review_2_volatile_inventory_keys_neither_stale_nor_change_a_plan(site, tmp_path,
                                                                          monkeypatch) -> None:
    monkeypatch.setattr(runtime_participant, "LEASE", lambda argv: (0, ""))
    monkeypatch.setattr(runtime_participant, "RECORDER", lambda *a: None)
    site.script("inventory",
                {"receipt": _volatile("5" * 64, "2026-09-25T12:00:00+00:00", [])},
                {"receipt": _volatile("5" * 64, "2026-09-25T12:05:00+00:00", [{"id": 1}])},
                {"receipt": _volatile("5" * 64, "2026-09-25T12:06:00+00:00", [])},
                {"receipt": _volatile("5" * 64, "2026-09-25T12:09:00+00:00", [{"id": 2}])})
    site.script("verify", HEALTHY)
    plan_id = runtime_migration.plan(_manifest())["plan_id"]

    ready = runtime_migration.prepare(_manifest(), plan_id)
    state = runtime_migration.apply(_manifest(), ready["ready_id"], tmp_path / "t")

    assert state["state"] == "applied"


def test_review_2_a_changed_state_digest_is_still_stale(site) -> None:
    site.script("inventory", {"receipt": _volatile("5" * 64, "t1", [])},
                {"receipt": _volatile("7" * 64, "t1", [])})
    plan_id = runtime_migration.plan(_manifest())["plan_id"]

    with pytest.raises(runtime_migration.MigrationError, match="stale"):
        runtime_migration.prepare(_manifest(), plan_id)


def test_review_3_verify_names_the_identity_and_mode(tx) -> None:
    tx.script("apply", {"receipt": {"before": {"current": "o" * 40}, "after": {}}})
    tx.script("verify", HEALTHY, HEALTHY)
    _apply(tx)
    runtime_migration.recover(tx.receipt, rollback=True)

    after_apply, after_rollback = [r for r in tx.requests() if r["op"] == "verify"]
    assert after_apply["mode"] == after_rollback["mode"] == "active"
    assert after_apply["identity"] == {"source_sha": SHA}
    assert after_rollback["identity"] == {"source_sha": "o" * 40}


def test_review_4_prepare_stages_the_source_and_names_the_allocation(site, monkeypatch) -> None:
    cfg = config.load_config()
    cfg.hosts["cluster"] = cfg.hosts["cluster"].model_copy(update={
        "scheduler_runtime_deployments": {"vibeqc-dev": config.SchedulerRuntimeDeployment(
            update_command="/site/bin/deploy", verify_command="/site/bin/verify",
            stage_source=True,
            update_allocation=config.SchedulerBuildAllocation(sbatch_args=["--time=02:00:00"]),
        )}})
    staged: list[tuple] = []

    def stage(host, build_host_cfg, program, sha, cfg_, result):
        staged.append((host, program, sha))
        return f"/site/scratch/.vq-admin/runtime-source/{program}/{sha}-x/src.tar.gz"

    monkeypatch.setattr(runtime_participant, "STAGE_SOURCE", stage)
    plan_id = runtime_migration.plan(_manifest())["plan_id"]

    runtime_migration.prepare(_manifest(), plan_id)

    [prepare] = [r for r in site.requests() if r["op"] == "prepare"]
    assert staged == [("cluster", "vibeqc-dev", SHA)]
    assert prepare["source_archive"].startswith("/site/scratch/")
    assert prepare["allocation"] == {"sbatch_args": ["--time=02:00:00"]}


def test_review_5_verify_and_apply_use_the_deployment_timeouts(site, monkeypatch) -> None:
    cfg = config.load_config()
    cfg.hosts["cluster"] = cfg.hosts["cluster"].model_copy(update={
        "scheduler_runtime_deployments": {"vibeqc-dev": config.SchedulerRuntimeDeployment(
            update_command="/site/bin/deploy", verify_command="/site/bin/verify",
            timeout_seconds=7200.0, verify_timeout_seconds=1800.0)}})

    timeout = runtime_participant._call_timeout
    assert timeout(cfg, "cluster", "vibeqc-dev", "verify") == 1800.0
    assert timeout(cfg, "cluster", "vibeqc-dev", "apply") == 7200.0
    assert timeout(cfg, "cluster", "vibeqc-dev", "rollback") == 7200.0
    assert timeout(cfg, "cluster", "vibeqc-dev", "acknowledge") == (
        runtime_participant.DEFAULT_TIMEOUT_SECONDS)
    assert timeout(cfg, "cluster", "other", "verify") == runtime_participant.DEFAULT_TIMEOUT_SECONDS


def test_review_6_a_refused_verify_after_apply_keeps_the_lease(tx) -> None:
    tx.script("verify", {"outcome": "refused", "errors": ["partition busy"]}, HEALTHY)

    with pytest.raises(runtime_migration.MigrationError, match="lease is kept"):
        _apply(tx)
    assert _held(tx)

    state = runtime_migration.recover(tx.receipt)
    assert state["state"] == "applied" and not _held(tx)


def test_review_6_an_unhealthy_rollback_keeps_the_lease_and_reverifies(tx) -> None:
    tx.script("verify", HEALTHY, {"receipt": {"healthy": False}}, HEALTHY)
    _apply(tx)

    with pytest.raises(runtime_migration.MigrationError, match="after rollback"):
        runtime_migration.recover(tx.receipt, rollback=True)
    assert _journal(tx)["state"] == "rollback_unverified" and _held(tx)

    state = runtime_migration.recover(tx.receipt)
    assert state["state"] == "rolled_back" and not _held(tx)
    assert [r["op"] for r in tx.requests()].count("rollback") == 1
    verifies = [r for r in tx.requests() if r["op"] == "verify"]
    assert len({r["op_id"] for r in verifies[1:]}) == 2  # a fresh re-verification
