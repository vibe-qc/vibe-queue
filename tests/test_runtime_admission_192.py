"""Reservation/cleanup transaction controls; all authorities are synthetic."""

import copy
import os
from pathlib import Path

import pytest

from vq.runtime_admission import AdmissionError, ReservationStore, validate_identity

SOURCE = "a" * 40
KEY, OWNER, INTENT = "1" * 64, "2" * 64, "3" * 64
JOB = "123456abcdef"


@pytest.fixture
def setup_store(tmp_path):
    generation = tmp_path / "program/releases" / SOURCE
    (generation / "source/.venv/bin").mkdir(parents=True)
    (generation / "source/.venv/bin/python").write_text("synthetic immutable interpreter")
    identity = dict(
        program="program",
        source_sha=SOURCE,
        generation=str(generation),
        source_root=str(generation / "source"),
        python=str(generation / "source/.venv/bin/python"),
        native_sha256="4" * 64,
        content_sha256="5" * 64,
        healthy=True,
    )
    directory = tmp_path / "admissions"
    directory.mkdir(mode=0o700)
    store = ReservationStore(directory, lambda program, source: copy.deepcopy(identity))
    return store, identity


def reserve(store, identity, **kwargs):
    return store.reserve(
        **(
            dict(
                key=KEY,
                owner=OWNER,
                intent=INTENT,
                program="program",
                source=SOURCE,
                identity=identity,
            )
            | kwargs
        )
    )


def test_reference_handoff_has_no_cleanup_gap(setup_store):
    store, identity = setup_store
    with store.transaction():
        first = reserve(store, identity)
        assert first["state"] == "reserved"
        assert store.protects(identity["generation"]) == [KEY]
        assert reserve(store, identity) == first
        committed = store.commit(KEY, owner=OWNER, intent=INTENT, job=JOB)
        assert committed["state"] == "committed"
        assert store.protects(identity["generation"]) == [KEY]
        assert store.check(KEY, owner=OWNER, intent=INTENT, job=JOB) == committed
        proof = {"job": JOB, "archived": True, "scheduler_terminal": "finished"}
        released = store.release(
            KEY,
            owner=OWNER,
            intent=INTENT,
            job=JOB,
            terminal_proof=proof,
            verify_terminal=lambda job, p: p == proof,
        )
        assert released["state"] == "released"
        assert store.protects(identity["generation"]) == []
        assert (
            store.release(
                KEY,
                owner=OWNER,
                intent=INTENT,
                job=JOB,
                terminal_proof=proof,
                verify_terminal=lambda *_: False,
            )
            == released
        )
        with pytest.raises(AdmissionError, match="released"):
            reserve(store, identity)


def test_cleanup_and_admission_share_exclusion(setup_store):
    store, identity = setup_store
    cleanup = ReservationStore(store.directory, store.verify)
    with (cleanup.transaction(), pytest.raises(AdmissionError, match="busy"),
          store.transaction()):
        reserve(store, identity)
    with store.transaction():
        reserve(store, identity)
        with pytest.raises(AdmissionError, match="busy"), cleanup.transaction():
            raise AssertionError("cleanup entered protected admission window")
    with cleanup.transaction():
        assert cleanup.protects(identity["generation"]) == [KEY]


@pytest.mark.parametrize(
    "field,value",
    [
        ("content_sha256", "6" * 64),
        ("native_sha256", "7" * 64),
        ("healthy", False),
        ("source_sha", "b" * 40),
    ],
)
def test_changed_generation_refuses_replay_and_dispatch(setup_store, field, value):
    store, identity = setup_store
    original = copy.deepcopy(identity)
    with store.transaction():
        reserve(store, original)
        store.commit(KEY, owner=OWNER, intent=INTENT, job=JOB)
    identity[field] = value
    with store.transaction():
        with pytest.raises(AdmissionError):
            reserve(store, original)
        with pytest.raises(AdmissionError):
            store.check(KEY, owner=OWNER, intent=INTENT, job=JOB)
        assert store.protects(original["generation"]) == [KEY]


def test_lost_reserve_response_is_durable_and_not_expirable(setup_store, monkeypatch):
    store, identity = setup_store
    original = os.fsync

    def fail_directory(fd):
        original(fd)
        if Path(f"/dev/fd/{fd}").is_dir():
            raise OSError("lost answer after durable reservation publication")

    with store.transaction(), monkeypatch.context() as m:
        m.setattr(os, "fsync", fail_directory)
        with pytest.raises(OSError):
            reserve(store, identity)
    restarted = ReservationStore(store.directory, store.verify)
    with restarted.transaction():
        assert restarted.protects(identity["generation"]) == [KEY]
        assert reserve(restarted, identity)["state"] == "reserved"
        with pytest.raises(AdmissionError, match="unknown"):
            restarted.release(
                KEY,
                owner=OWNER,
                intent=INTENT,
                job=JOB,
                terminal_proof={"expired": True},
                verify_terminal=lambda *_: True,
            )


@pytest.mark.parametrize(
    "change", [{"owner": "8" * 64}, {"intent": "9" * 64}, {"source": "b" * 40}]
)
def test_idempotency_key_cannot_change_intent(setup_store, change):
    store, identity = setup_store
    with store.transaction():
        reserve(store, identity)
        with pytest.raises(AdmissionError):
            reserve(store, identity, **change)
        assert store.protects(identity["generation"]) == [KEY]


def test_only_exact_committed_job_can_dispatch_or_release(setup_store):
    store, identity = setup_store
    with store.transaction():
        reserve(store, identity)
        with pytest.raises(AdmissionError):
            store.check(KEY, owner=OWNER, intent=INTENT, job=JOB)
        store.commit(KEY, owner=OWNER, intent=INTENT, job=JOB)
        for wrong in ["abcdef123456", "not-a-job"]:
            with pytest.raises(AdmissionError):
                store.commit(KEY, owner=OWNER, intent=INTENT, job=wrong)
        with pytest.raises(AdmissionError):
            store.release(
                KEY,
                owner=OWNER,
                intent=INTENT,
                job=JOB,
                terminal_proof={"driver_says_done": True},
                verify_terminal=lambda *_: False,
            )
        assert store.protects(identity["generation"]) == [KEY]


@pytest.mark.parametrize(
    "field,value",
    [
        ("python", "/outside/python"),
        ("source_root", "/outside/source"),
        ("generation", "/tmp/../releases/" + SOURCE),
        ("native_sha256", "a" * 40),
        ("healthy", 1),
    ],
)
def test_identity_shape_refusals(setup_store, field, value):
    _, identity = setup_store
    identity[field] = value
    with pytest.raises(AdmissionError):
        validate_identity(identity, "program", SOURCE)


def test_corrupt_state_blocks_cleanup(setup_store):
    store, identity = setup_store
    with store.transaction():
        reserve(store, identity)
    (store.directory / (KEY + ".json")).write_text('{"schema":"bad"}')
    with store.transaction(), pytest.raises(AdmissionError):
        store.protects(identity["generation"])


def test_symlink_authority_and_changed_lock_refuse(setup_store, tmp_path):
    store, _ = setup_store
    alias = tmp_path / "alias"
    alias.symlink_to(store.directory, target_is_directory=True)
    with pytest.raises(AdmissionError), ReservationStore(alias, store.verify).transaction():
        pass
    with store.transaction():
        (store.directory / "authority.lock").unlink()
        (store.directory / "authority.lock").touch(mode=0o600)
        with pytest.raises(AdmissionError, match="authority"):
            store.protects("/irrelevant")


def test_operations_require_actual_transaction(setup_store):
    store, identity = setup_store
    with pytest.raises(AdmissionError, match="transaction"):
        reserve(store, identity)


@pytest.fixture
def provider_submission(tmp_path, monkeypatch):
    import json
    import subprocess

    from vq import config, paths, runtime_participant, transport
    from vq.runtime_admission import SCHEMA, digest

    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    cfg = tmp_path / "cfg"
    cfg.mkdir()
    cfg.joinpath("config.toml").write_text("""default_host = "localhost"
[hosts.localhost]
ssh = "localhost"
[hosts.cluster]
ssh = "cluster-login"
scheduler = "pbs"
scheduler_dialect = "torque"
scratch_root = "/scratch/test"
scheduler_driver = "localhost"
runtime_participant_command = "/site/runtime-provider"
[hosts.cluster.scheduler_runtime_deployments.vibeqc-release]
update_command = "/site/update"
verify_command = "/site/verify"
[hosts.alias]
ssh = "cluster-login"
scheduler = "pbs"
scheduler_dialect = "torque"
scratch_root = "/scratch/test"
scheduler_driver = "localhost"
fleet_role = "alias"
fleet_canonical_host = "cluster"
[programs.vibeqc-release]
kind = "scheduler"
""")
    generation = tmp_path / "provider/vibeqc-release/releases" / SOURCE
    (generation / "source/.venv/bin").mkdir(parents=True)
    (generation / "source/.venv/bin/python").write_text("synthetic interpreter")
    identity = dict(
        program="vibeqc-release",
        source_sha=SOURCE,
        generation=str(generation),
        source_root=str(generation / "source"),
        python=str(generation / "source/.venv/bin/python"),
        native_sha256="4" * 64,
        content_sha256="5" * 64,
        healthy=True,
    )
    current = generation.parent.parent / "current"
    (generation.parent / ("b" * 40)).mkdir()
    current.symlink_to("releases/" + "b" * 40, target_is_directory=True)
    leases = tmp_path / "leases"
    leases.mkdir(mode=0o700)
    store = ReservationStore(leases, lambda *_: copy.deepcopy(identity))
    calls = []

    def runner(host, argv, stdin, timeout):
        assert host.ssh == "cluster-login" and argv == ["/site/runtime-provider"]
        request = json.loads(stdin)
        op = request["op"].removeprefix("generation-")
        fields = {k: v for k, v in request.items() if k not in {"schema", "op", "op_id"}}
        calls.append(op)
        with store.transaction():
            if op == "describe":
                result = dict(identity=copy.deepcopy(identity), cleanup_contract=SCHEMA)
            elif op == "reserve":
                result = dict(
                    reservation=store.reserve(
                        key=fields["key"],
                        owner=fields["owner"],
                        intent=fields["intent"],
                        program=fields["program"],
                        source=fields["source_sha"],
                        identity=fields["identity"],
                    )
                )
            elif op in {"commit", "check"}:
                result = dict(
                    reservation=getattr(store, op)(
                        fields["key"],
                        owner=fields["owner"],
                        intent=fields["intent"],
                        job=fields["job"],
                    )
                )
            elif op == "release":
                result = dict(
                    reservation=store.release(
                        fields["key"],
                        owner=fields["owner"],
                        intent=fields["intent"],
                        job=fields["job"],
                        terminal_proof=fields["terminal_proof"],
                        verify_terminal=lambda _, p: p["scheduler_state"] == "completed",
                    )
                )
            else:
                raise AssertionError(op)
        receipt = dict(schema=SCHEMA, request_sha256=digest(fields), **result)
        answer = dict(
            schema=runtime_participant.SCHEMA,
            op=request["op"],
            op_id=request["op_id"],
            outcome="ok",
            receipt=receipt,
            receipt_sha256=runtime_participant._digest(runtime_participant._json(receipt)),
            participant={"adapter_sha256": "6" * 64},
            errors=[],
        )
        return 0, json.dumps(answer), ""

    monkeypatch.setattr(runtime_participant, "RUNNER", runner)
    monkeypatch.setattr(
        transport,
        "run_remote_vq",
        lambda *a, **k: subprocess.CompletedProcess(a, 2, "", "not used"),
    )
    monkeypatch.setattr("vq.submit._probe_scheduler_helper", lambda *_: {"ok": True})
    script = tmp_path / "job.py"
    script.write_text("print('synthetic workload')\n")
    return store, identity, calls, script


def cli_submit(script, target="cluster", *extra):
    from click.testing import CliRunner

    from vq.cli import main

    return CliRunner().invoke(
        main,
        [
            "submit",
            target,
            str(script),
            "--program",
            "vibeqc-release",
            "--managed-generation",
            SOURCE,
            "--idempotency-key",
            "managed-contract-1",
            "--retry",
            "0",
            "--json",
            *extra,
        ],
    )


@pytest.mark.parametrize("target", ["cluster", "alias"])
def test_actual_submit_dispatch_archive_handoff(provider_submission, target):
    from vq import paths
    from vq.cleanup import archive_workspace
    from vq.daemon import _program_runtime_pin_dispatch_resolution
    from vq.spec import JobSpec, JobState

    store, identity, calls, script = provider_submission
    result = cli_submit(script, target)
    assert result.exit_code == 0, result.output
    specs = list(paths.queue_dir().glob("*.json"))
    assert len(specs) == 1
    spec = JobSpec.read(specs[0])
    assert spec.command[0] == identity["python"]
    assert (Path(identity["generation"]).parent.parent / "current").resolve().name == "b" * 40
    assert spec.program_runtime_pin.managed_generation["authority"] == "cluster"
    with store.transaction():
        assert store.protects(identity["generation"])
    again = cli_submit(script, target)
    assert again.exit_code == 0, again.output
    assert len(list(paths.queue_dir().glob("*.json"))) == 1
    resolution = _program_runtime_pin_dispatch_resolution(spec)
    assert resolution.failure is None, resolution.failure
    assert resolution.runtime_python == identity["python"]
    spec.state = JobState.COMPLETED
    spec.scheduler_job_id = "123.cluster"
    spec.scheduler_state = "completed"
    spec.exit_code = 0
    spec.write(specs[0])
    archive_workspace(spec)
    with store.transaction():
        assert not store.protects(identity["generation"])
    assert calls.count("reserve") == 1 and calls.count("release") == 1


def test_unsupported_provider_creates_no_spec(provider_submission, monkeypatch):
    from vq import paths, runtime_participant

    *_, script = provider_submission
    monkeypatch.setattr(runtime_participant, "RUNNER", lambda *_: (1, "", "unsupported operation"))
    result = cli_submit(script)
    assert result.exit_code != 0
    assert "unsupported operation" in result.output
    assert not list(paths.queue_dir().glob("*.json"))


def test_changed_native_refuses_actual_dispatch(provider_submission):
    from vq import paths
    from vq.daemon import _program_runtime_pin_dispatch_resolution
    from vq.spec import JobSpec

    store, identity, _, script = provider_submission
    result = cli_submit(script)
    assert result.exit_code == 0, result.output
    spec = JobSpec.read(next(paths.queue_dir().glob("*.json")))
    identity["native_sha256"] = "f" * 64
    resolution = _program_runtime_pin_dispatch_resolution(spec)
    assert resolution.failure is not None
    with store.transaction():
        assert store.protects(identity["generation"])


@pytest.mark.parametrize("lost_op", ["reserve", "commit"])
def test_lost_provider_answer_retains_reference_and_never_duplicates(
    provider_submission, monkeypatch, lost_op
):
    import json

    from vq import paths, runtime_participant
    from vq.spec import JobSpec

    store, identity, calls, script = provider_submission
    original = runtime_participant.RUNNER
    lost = [False]

    def runner(*args):
        answer = original(*args)
        if json.loads(args[2])["op"] == "generation-" + lost_op and not lost[0]:
            lost[0] = True
            raise OSError("lost response after provider durable write")
        return answer

    monkeypatch.setattr(runtime_participant, "RUNNER", runner)
    cli_submit(script)
    with store.transaction():
        assert store.protects(identity["generation"])
    again = cli_submit(script)
    assert again.exit_code == 0, again.output
    assert len(list(paths.queue_dir().glob("*.json"))) == 1
    spec = JobSpec.read(next(paths.queue_dir().glob("*.json")))
    with store.transaction():
        row = store._read(spec.program_runtime_pin.managed_generation["reservation"]["key"])
        assert row["job"] == spec.id and row["state"] == "committed"


def test_request_replay_or_wrong_provider_receipt_is_rejected(provider_submission, monkeypatch):
    import json

    from vq import paths, runtime_participant

    _, _, _, script = provider_submission
    original = runtime_participant.RUNNER

    def stale(*args):
        rc, raw, err = original(*args)
        value = json.loads(raw)
        value["receipt"]["request_sha256"] = "f" * 64
        value["receipt_sha256"] = runtime_participant._digest(
            runtime_participant._json(value["receipt"])
        )
        return rc, json.dumps(value), err

    monkeypatch.setattr(runtime_participant, "RUNNER", stale)
    result = cli_submit(script)
    assert result.exit_code != 0 and "does not bind this request" in result.output
    assert not list(paths.queue_dir().glob("*.json"))


@pytest.mark.parametrize("option", ["--managed-native-sha256", "--managed-content-sha256"])
def test_requested_exact_digest_refuses_before_publication(provider_submission, option):
    from vq import paths

    store, identity, calls, script = provider_submission
    result = cli_submit(script, "cluster", option, "f" * 64)
    assert result.exit_code != 0 and "does not match requested" in result.output
    assert "reserve" not in calls
    assert not list(paths.queue_dir().glob("*.json"))


def test_managed_job_environment_keeps_content_immutable(provider_submission):
    from vq import daemon, paths
    from vq.spec import JobSpec

    _, identity, _, script = provider_submission
    result = cli_submit(
        script,
        "cluster",
        "--managed-native-sha256",
        identity["native_sha256"],
        "--managed-content-sha256",
        identity["content_sha256"],
    )
    assert result.exit_code == 0, result.output
    spec = JobSpec.read(next(paths.queue_dir().glob("*.json")))
    # The same job environment is passed to the scheduler dispatch script.
    env = daemon._vq_job_env(spec, workdir=spec.cwd)
    assert env["PYTHONDONTWRITEBYTECODE"] == "1"
    assert env["VQ_PROGRAM_NATIVE_SHA256"] == identity["native_sha256"]
    assert env["VQ_PROGRAM_CONTENT_SHA256"] == identity["content_sha256"]


def test_remote_wire_keeps_exact_managed_identity(tmp_path, monkeypatch):
    import subprocess

    from vq import submit, transport
    from vq.config import HostConfig

    script = tmp_path / "payload.py"
    script.write_text("print('synthetic')\n")
    sent = []
    monkeypatch.setattr(transport, "upload_file", lambda *a, **k: None)
    monkeypatch.setattr(
        transport, "run_remote_shell", lambda *a, **k: subprocess.CompletedProcess([], 0, "", "")
    )

    def remote(*args, **kwargs):
        sent.append(args[1:])
        return subprocess.CompletedProcess([], 0, "123456abcdef\n", "")

    monkeypatch.setattr(transport, "run_remote_vq", remote)
    submit.submit_remote(
        host="driver",
        host_cfg=HostConfig(ssh="driver", remote_vq="vq"),
        input_file=str(script),
        program="simulation",
        scheduler_target="cluster",
        idempotency_key="remote-managed-1",
        retry=0,
        managed_generation=SOURCE,
        managed_native_sha256="4" * 64,
        managed_content_sha256="5" * 64,
    )
    assert len(sent) == 1
    for name, value in [
        ("managed-generation", SOURCE),
        ("managed-native-sha256", "4" * 64),
        ("managed-content-sha256", "5" * 64),
    ]:
        assert sent[0][sent[0].index("--" + name) + 1] == value
    with pytest.raises(ValueError, match="explicit full generation"):
        submit.submit_remote(
            host="driver",
            host_cfg=HostConfig(ssh="driver", remote_vq="vq"),
            input_file=str(script),
            managed_native_sha256="4" * 64,
        )
    assert len(sent) == 1
