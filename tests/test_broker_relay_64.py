"""Client relay to a broker and the broker's proxy verbs (#64 B5)."""
from __future__ import annotations

import io
import json
import subprocess
import tarfile
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from click.testing import CliRunner

from tests.test_broker_store_64 import _archive, _intent
from vq import broker_relay, broker_service, broker_store, config, transport
from vq.cli import main


def _request(**changes):
    fields = dict(command=("python3", "run.py"), cpus=2, mem_mb=2000)
    fields.update(changes)
    return broker_relay.RelayRequest(**fields)


# -- client relay ------------------------------------------------------------------


class Remote:
    """Doubles for upload, remote vq and remote shell."""

    def __init__(self, answer="b-0123456789ab", error=None):
        self.answer = answer
        self.error = error
        self.uploads = []
        self.vq = []
        self.shell = []

    def install(self, monkeypatch):
        monkeypatch.setattr(transport, "remote_temp_tar_path", lambda: ".vq-upload-x.tar")
        monkeypatch.setattr(transport, "upload_file", self.upload)
        monkeypatch.setattr(transport, "run_remote_vq", self.run_vq)
        monkeypatch.setattr(transport, "run_remote_shell", self.run_shell)

    def upload(self, host_cfg, local, remote, **kwargs):
        with tarfile.open(local) as tf:
            self.uploads.append((remote, sorted(tf.getnames())))

    def run_vq(self, host_cfg, *argv, **kwargs):
        self.vq.append(list(argv))
        if self.error is not None:
            raise self.error
        return subprocess.CompletedProcess(argv, 0, stdout=self.answer + "\n")

    def run_shell(self, host_cfg, *argv, **kwargs):
        self.shell.append(list(argv))
        return subprocess.CompletedProcess(argv, 0)


HOST = SimpleNamespace(ssh="fleet")


def test_a_directory_is_relayed_accepted_and_its_upload_removed(tmp_path, monkeypatch):
    remote = Remote()
    remote.install(monkeypatch)
    source = tmp_path / "job"
    source.mkdir()
    (source / "run.py").write_text("print(1)\n")
    (source / "data.txt").write_text("x")

    job_id = broker_relay.submit_via_broker(
        HOST, _request(program="vibeqc", tags=("a",), pool="gpu", priority=3),
        directory=str(source),
    )

    assert job_id == "b-0123456789ab"
    assert remote.uploads == [(".vq-upload-x.tar", ["data.txt", "run.py"])]
    (argv,) = remote.vq
    assert argv[:3] == ["broker", "accept", ".vq-upload-x.tar"]
    assert argv[argv.index("--"):] == ["--", "python3", "run.py"]
    for flag, value in (("--cpus", "2"), ("--mem-mb", "2000"), ("--program", "vibeqc"),
                        ("--pool", "gpu"), ("--priority", "3"), ("--tag", "a")):
        assert argv[argv.index(flag) + 1] == value
    assert remote.shell == [["rm", "-f", ".vq-upload-x.tar"]]


def test_the_relay_argv_is_what_the_broker_accept_command_parses(tmp_path, monkeypatch):
    from vq import paths

    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    (tmp_path / "state").mkdir()
    cfg = config.Config(hosts={"alpha": config.HostConfig(ssh="alpha")},
                        broker={"enabled": True, "targets": ["alpha"]},
                        pools={"gpu": {"hosts": ["alpha"]}})
    monkeypatch.setattr(config, "load_config", lambda: cfg)
    payload = _archive(tmp_path / "upload.tar")
    request = _request(wall_time_seconds=600, retry=2, auto_resume=True, job_name="scan",
                       program="vibeqc", expected_sha="abcdef1234", tags=("a", "b"),
                       clean_workdir_on_terminal=True, rerun_until_file_exists="DONE",
                       rerun_max=4, pool="gpu", priority=-1)

    result = CliRunner().invoke(main, broker_relay.accept_argv(str(payload), request))

    assert result.exit_code == 0, result.output
    job_id = result.output.strip()
    with broker_store.BrokerStore.open() as store:
        intent = store.get(job_id).intent
    assert intent == broker_store.BrokerIntent(
        command=("python3", "run.py"), cpus=2, mem_mb=2000, wall_time_seconds=600,
        priority=-1, auto_resume=True, retry=2, job_name="scan", program="vibeqc",
        expected_sha="abcdef1234", tags=("a", "b"), clean_workdir_on_terminal=True,
        rerun_until_file_exists="DONE", rerun_max=4, pool="gpu",
    )


def test_a_refused_accept_removes_the_upload(tmp_path, monkeypatch):
    remote = Remote(error=transport.RemoteCommandError(
        "refused", returncode=1, stderr="Error: unknown pool 'x'"))
    remote.install(monkeypatch)
    with pytest.raises(transport.RemoteError):
        broker_relay.submit_via_broker(HOST, _request(), archive=str(_archive(tmp_path / "a.tar")))
    assert remote.shell == [["rm", "-f", ".vq-upload-x.tar"]]


@pytest.mark.parametrize(
    ("remote", "message"),
    [
        (Remote(error=transport.RemoteOutcomeUnknown("ssh timed out")), "may have accepted"),
        (Remote(answer="garbage"), "not a broker job id"),
    ],
)
def test_an_unknown_accept_outcome_keeps_the_upload_and_says_so(
    tmp_path, monkeypatch, remote, message
):
    remote.install(monkeypatch)
    with pytest.raises(broker_relay.BrokerRelayUncertain, match=message):
        broker_relay.submit_via_broker(HOST, _request(), archive=str(_archive(tmp_path / "a.tar")))
    assert remote.shell == []


def test_a_single_file_needs_an_explicit_interpreter(tmp_path, monkeypatch):
    Remote().install(monkeypatch)
    script = tmp_path / "calc.py"
    script.write_text("print(1)\n")
    with pytest.raises(broker_relay.BrokerRelayError, match="--python"):
        broker_relay.submit_via_broker(HOST, _request(command=()), input_file=str(script))
    remote = Remote()
    remote.install(monkeypatch)
    broker_relay.submit_via_broker(HOST, _request(command=()), input_file=str(script),
                                   python="/opt/py/bin/python")
    assert remote.vq[0][-3:] == ["--", "/opt/py/bin/python", "calc.py"]


def test_memory_is_mandatory(tmp_path, monkeypatch):
    Remote().install(monkeypatch)
    with pytest.raises(broker_relay.BrokerRelayError, match="--mem-mb"):
        broker_relay.submit_via_broker(HOST, _request(mem_mb=None),
                                       archive=str(_archive(tmp_path / "a.tar")))


# -- client CLI -------------------------------------------------------------------


@pytest.fixture
def client(monkeypatch):
    cfg = config.Config(hosts={
        "fleet": config.HostConfig(ssh="coordinator", broker=True),
        "coordinator": config.HostConfig(ssh="coordinator"),
    })
    monkeypatch.setattr(config, "load_config", lambda: cfg)
    sent = []
    monkeypatch.setattr(broker_relay, "submit_via_broker",
                        lambda host_cfg, relay, **kw: sent.append((host_cfg, relay, kw))
                        or "b-0123456789ab")
    return sent


def test_submit_to_a_broker_entry_relays(client, tmp_path):
    source = tmp_path / "job"
    source.mkdir()
    result = CliRunner().invoke(main, ["submit", "fleet", "-d", str(source), "--cpus", "4",
                                       "--mem-mb", "8000", "--", "bash", "run.sh"])
    assert result.exit_code == 0, result.output
    assert result.output.strip() == "b-0123456789ab"
    ((host_cfg, relay, kw),) = client
    assert host_cfg.ssh == "coordinator" and relay.cpus == 4 and relay.mem_mb == 8000
    assert relay.command == ("bash", "run.sh") and kw["directory"] == str(source)


@pytest.mark.parametrize(
    "extra",
    [["--array", "2"], ["--depends-on", "0123456789ab"], ["--at", "2030-01-01T00:00:00Z"]],
)
def test_submit_to_a_broker_refuses_what_a_broker_job_cannot_carry(client, tmp_path, extra):
    source = tmp_path / "job"
    source.mkdir()
    result = CliRunner().invoke(main, ["submit", "fleet", "-d", str(source), "--cpus", "1",
                                       "--mem-mb", "100", *extra, "--", "bash", "run.sh"])
    assert result.exit_code == 2 and "is a broker" in result.output, result.output
    assert client == []


def test_a_broker_job_id_routes_to_the_broker_entry(client):
    from vq.cli import _resolve_job_host

    cfg = config.load_config()
    assert _resolve_job_host(cfg, None, "b-0123456789ab") == "fleet"


# -- broker side -------------------------------------------------------------------


@pytest.fixture
def broker(tmp_path, monkeypatch):
    from vq import paths

    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    (tmp_path / "state").mkdir()
    cfg = config.Config(hosts={"alpha": config.HostConfig(ssh="alpha")},
                        broker={"enabled": True, "targets": ["alpha"]})
    monkeypatch.setattr(config, "load_config", lambda: cfg)
    payload = _archive(tmp_path / "upload.tar")

    def job(state="held"):
        record = broker_service.accept(cfg, payload, _intent(), clock=lambda: 100.0)
        if state == "held":
            return record
        with broker_store.BrokerStore.open() as store:
            from tests.test_broker_store_64 import _snapshot
            from vq import overview

            decision = overview.place_broker_job(
                [_snapshot()], record.intent.placement_request(), now=105.0,
                max_age_seconds=60.0)
            record = store.reserve(record.broker_job_id, 1, decision=decision,
                                   snapshot=_snapshot(), target_state_root="/s", now=106.0)
            record = store.begin_transfer(record.broker_job_id, record.revision, now=107.0)
            if state == "placed":
                record = store.record_placed(
                    record.broker_job_id, record.revision,
                    request_fingerprint=record.current_attempt.request_fingerprint,
                    target_job_id="0123456789ab", now=108.0)
            return record

    calls = []

    def run_vq(host_cfg, *argv, **kwargs):
        calls.append((host_cfg.ssh, list(argv)))
        if argv[0] == "status":
            return subprocess.CompletedProcess(argv, 0, stdout=json.dumps({"state": "running"}))
        return subprocess.CompletedProcess(argv, 0, stdout=f"{argv[0]} ok\n", stderr="")

    monkeypatch.setattr(transport, "run_remote_vq", run_vq)
    return SimpleNamespace(job=job, calls=calls, cfg=cfg)


def test_status_of_a_held_job_is_the_broker_record(broker):
    record = broker.job()
    result = CliRunner().invoke(main, ["status", "localhost", record.broker_job_id, "--json"])
    assert result.exit_code == 0, result.output
    view = json.loads(result.output)
    assert (view["state"], view["target_host"]) == ("held", None)
    assert broker.calls == []


def test_status_of_a_placed_job_includes_the_targets_status(broker):
    record = broker.job("placed")
    result = CliRunner().invoke(main, ["status", "localhost", record.broker_job_id, "--json"])
    view = json.loads(result.output)
    assert view["target_status"] == {"state": "running"}
    assert broker.calls == [("alpha", ["status", "localhost", "0123456789ab", "--json"])]


def test_logs_are_proxied_to_the_target_job(broker):
    record = broker.job("placed")
    result = CliRunner().invoke(main, ["logs", "localhost", record.broker_job_id, "-n", "5",
                                       "--stderr"])
    assert result.exit_code == 0, result.output
    assert broker.calls == [("alpha", ["logs", "localhost", "0123456789ab", "-n", "5",
                                       "--stderr"])]


def test_logs_of_an_unplaced_job_say_there_is_no_target(broker):
    record = broker.job()
    result = CliRunner().invoke(main, ["logs", "localhost", record.broker_job_id])
    assert result.exit_code == 1 and "no target job yet" in result.output


def test_kill_of_a_held_job_cancels_it(broker):
    record = broker.job()
    result = CliRunner().invoke(main, ["kill", "localhost", record.broker_job_id])
    assert "cancelled before placement" in result.output
    assert broker.calls == []


def test_kill_of_a_placed_job_kills_the_target_job(broker):
    record = broker.job("placed")
    result = CliRunner().invoke(main, ["kill", "localhost", record.broker_job_id])
    assert result.exit_code == 0, result.output
    assert broker.calls == [("alpha", ["kill", "localhost", "0123456789ab", "--reason",
                                       "vq kill through the broker"])]
    with broker_store.BrokerStore.open() as store:
        assert store.get(record.broker_job_id).cancellation is not None


def test_kill_of_an_uncertain_transfer_names_the_operator_action(broker):
    record = broker.job("transferring")
    result = CliRunner().invoke(main, ["kill", "localhost", record.broker_job_id])
    assert "OPERATOR ACTION" in result.output and broker.calls == []


def test_fetch_streams_the_targets_workspace_through(broker, monkeypatch):
    record = broker.job("placed")
    seen = []

    @contextmanager
    def stream(host_cfg, *argv):
        seen.append((host_cfg.ssh, list(argv)))
        yield SimpleNamespace(stdout=io.BytesIO(b"TARBYTES"))

    monkeypatch.setattr(transport, "stream_remote_vq", stream)
    result = CliRunner().invoke(main, ["tar-workspace", record.broker_job_id,
                                       "--fetch-protocol-token", "a" * 32])
    assert result.exit_code == 0, result.output
    assert result.stdout_bytes == b"TARBYTES"
    assert seen == [("alpha", ["tar-workspace", "0123456789ab",
                               "--fetch-protocol-token", "a" * 32])]


def test_mark_fetched_is_forwarded_with_the_target_job_id(broker):
    record = broker.job("placed")
    result = CliRunner().invoke(main, ["mark-fetched", record.broker_job_id, "--submitted-at",
                                       "2026-09-27T00:00:00+00:00", "--state", "completed"])
    assert result.exit_code == 0, result.output
    assert broker.calls[0] == ("alpha", ["mark-fetched", "0123456789ab", "--submitted-at",
                                         "2026-09-27T00:00:00+00:00", "--state", "completed"])


def test_a_broker_id_on_a_host_that_is_not_a_broker_is_refused(broker, monkeypatch):
    monkeypatch.setattr(config, "load_config", lambda: config.Config())
    result = CliRunner().invoke(main, ["status", "localhost", "b-0123456789ab"])
    assert result.exit_code == 1 and "not a broker" in result.output


@pytest.mark.parametrize(
    ("argv", "forwarded"),
    [
        (["tar-workdir", "{id}"], ["tar-workdir", "0123456789ab"]),
        (["tar-artifact", "{id}", "out.log", "--workdir", "--subdir", "run"],
         ["tar-artifact", "0123456789ab", "out.log", "--workdir", "--subdir", "run"]),
    ],
)
def test_workdir_and_artifact_fetches_are_proxied_too(broker, monkeypatch, argv, forwarded):
    record = broker.job("placed")
    seen = []

    @contextmanager
    def stream(host_cfg, *remote_argv):
        seen.append(list(remote_argv))
        yield SimpleNamespace(stdout=io.BytesIO(b"T"))

    monkeypatch.setattr(transport, "stream_remote_vq", stream)
    result = CliRunner().invoke(main, [a.replace("{id}", record.broker_job_id) for a in argv])
    assert result.exit_code == 0, result.output
    assert seen == [forwarded]


# -- review follow-ups: the real CLI path -------------------------------------------


def test_pool_and_rerun_until_reach_the_relay_through_the_cli(client, tmp_path):
    source = tmp_path / "job"
    source.mkdir()
    result = CliRunner().invoke(main, [
        "submit", "fleet", "-d", str(source), "--cpus", "1", "--mem-mb", "100",
        "--pool", "gpu", "--rerun-until", "DONE", "--rerun-max", "3", "--time", "01:00:00",
        "--", "bash", "run.sh"])
    assert result.exit_code == 0, result.output
    ((_host_cfg, relay, _kw),) = client
    assert (relay.pool, relay.rerun_until_file_exists, relay.rerun_max) == ("gpu", "DONE", 3)
    assert relay.wall_time_seconds == 3600


def test_chain_is_refused_by_name_for_a_broker(client, tmp_path):
    source = tmp_path / "job"
    source.mkdir()
    result = CliRunner().invoke(main, ["submit", "fleet", "-d", str(source), "--cpus", "1",
                                       "--mem-mb", "100", "--chain", "2", "--", "bash", "x"])
    assert result.exit_code == 2 and "is a broker" in result.output and "--chain" in result.output


def test_a_missing_staged_entrypoint_is_refused_before_upload(tmp_path, monkeypatch):
    remote = Remote()
    remote.install(monkeypatch)
    source = tmp_path / "job"
    source.mkdir()
    with pytest.raises(broker_relay.BrokerRelayError, match="not present"):
        broker_relay.submit_via_broker(HOST, _request(command=("python3", "missing.py")),
                                       directory=str(source))
    assert remote.uploads == []


@pytest.mark.parametrize("state", ["held", "placed"])
def test_kill_resubmit_holds_a_fresh_copy_once_the_job_stopped(broker, state):
    record = broker.job(state)
    result = CliRunner().invoke(main, ["kill", "--resubmit", "--reason", "upgrade",
                                       "localhost", record.broker_job_id])
    assert result.exit_code == 0, result.output
    fresh_id = result.output.strip().splitlines()[-1].split(" -> ")[-1]
    assert result.output.strip().endswith(f"resubmitted {record.broker_job_id} -> {fresh_id}")
    with broker_store.BrokerStore.open() as store:
        old = store.get(record.broker_job_id)
        fresh = store.get(fresh_id)
    assert old.cancellation.reason == "upgrade"
    assert (fresh.state, fresh.intent, fresh.payload_sha256) == (
        "held", record.intent, record.payload_sha256)
    if state == "placed":
        assert broker.calls == [("alpha", ["kill", "localhost", "0123456789ab",
                                           "--reason", "upgrade"])]


def test_kill_resubmit_refuses_a_transfer_in_doubt(broker):
    record = broker.job("transferring")
    result = CliRunner().invoke(main, ["kill", "--resubmit", "localhost", record.broker_job_id])
    assert result.exit_code == 1
    assert "OPERATOR ACTION" in result.output and "not resubmitted" in result.output
    with broker_store.BrokerStore.open() as store:
        assert len(store.scan()[0]) == 1


def test_kill_resubmit_does_not_resubmit_when_the_target_kill_fails(broker, monkeypatch):
    record = broker.job("placed")
    monkeypatch.setattr(transport, "run_remote_vq", lambda *a, **k: subprocess.CompletedProcess(
        a, 1, stdout="", stderr="permission denied"))
    result = CliRunner().invoke(main, ["kill", "--resubmit", "localhost", record.broker_job_id])
    assert result.exit_code == 1 and "failed" in result.output
    with broker_store.BrokerStore.open() as store:
        assert len(store.scan()[0]) == 1


def test_killing_an_already_finished_target_job_is_reported_not_failed(broker, monkeypatch):
    record = broker.job("placed")

    def run_vq(host_cfg, *argv, **kwargs):
        return subprocess.CompletedProcess(argv, 2, stdout="",
                                           stderr="job 0123456789ab already in terminal state")

    monkeypatch.setattr(transport, "run_remote_vq", run_vq)
    result = CliRunner().invoke(main, ["kill", "localhost", record.broker_job_id])
    assert result.exit_code == 0 and "terminal" in result.output


def test_a_target_no_longer_configured_is_a_clean_error(broker, monkeypatch):
    record = broker.job("placed")
    monkeypatch.setattr(config, "load_config", lambda: config.Config(
        hosts={"beta": config.HostConfig(ssh="beta")},
        broker={"enabled": True, "targets": ["beta"]}))
    for argv in (["status", "localhost", record.broker_job_id],
                 ["tar-workspace", record.broker_job_id]):
        result = CliRunner().invoke(main, argv)
        assert result.exit_code == 1 and "no longer configured" in result.output, argv
        assert not isinstance(result.exception, broker_store.BrokerStoreError)


def test_fetch_through_the_broker_end_to_end(tmp_path, monkeypatch):
    """A real target workspace stream, relayed unchanged, fetched by the client."""
    from vq import paths
    from vq.spec import JobSpec, JobState

    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    target_id, broker_id = "0123456789ab", "b-0123456789ab"
    queue, jobs = paths.queue_dir(), paths.jobs_dir()
    queue.mkdir(parents=True)
    workspace = jobs / target_id
    workspace.mkdir(parents=True)
    (workspace / "result.txt").write_text("42\n")
    spec = JobSpec(id=target_id, command=["echo"], cwd=str(workspace), cpus=1,
                   submitter="t@example.invalid")
    spec.state = JobState.COMPLETED
    spec.finished_at = "2026-09-27T00:00:00+00:00"
    spec.write(queue / f"{target_id}.json")

    cfg = config.Config(hosts={"fleet": config.HostConfig(ssh="coordinator", broker=True)})
    monkeypatch.setattr(config, "load_config", lambda: cfg)
    streamed, acks = [], []

    @contextmanager
    def stream(host_cfg, verb, jobid, *rest):
        # The broker proxies `tar-workspace b-...` to the target's own verb.
        streamed.append(jobid)
        result = CliRunner().invoke(main, [verb, target_id, *rest])
        assert result.exit_code == 0, result.output
        yield SimpleNamespace(stdout=io.BytesIO(result.stdout_bytes), stderr_text="")

    def run_vq(host_cfg, *argv, **kwargs):
        if argv[0] == "status":
            return subprocess.CompletedProcess(argv, 0, stdout=json.dumps(
                {"state": "placed", "target_job_id": target_id}))
        assert argv[0] == "mark-fetched"
        acks.append(argv[1])
        return subprocess.CompletedProcess(argv, 0, stdout="2026-09-27T01:00:00+00:00\n")

    monkeypatch.setattr(transport, "stream_remote_vq", stream)
    monkeypatch.setattr(transport, "run_remote_vq", run_vq)
    out = tmp_path / "out"
    result = CliRunner().invoke(main, ["fetch", "fleet", broker_id, "-o", str(out)])

    assert result.exit_code == 0, result.output
    assert (out / target_id / "result.txt").read_text() == "42\n"
    assert streamed == [broker_id] and acks == [broker_id]


# -- second review follow-ups -------------------------------------------------------


def test_an_unreadable_payload_is_a_clean_refusal_and_leaves_no_temp(tmp_path, monkeypatch):
    import glob
    import tempfile

    Remote().install(monkeypatch)
    source = tmp_path / "job"
    source.mkdir()
    locked = source / "locked.dat"
    locked.write_text("x")
    locked.chmod(0)
    before = set(glob.glob(f"{tempfile.gettempdir()}/vq-broker-*.tar"))
    try:
        with pytest.raises(broker_relay.BrokerRelayError, match="could not pack"):
            broker_relay.submit_via_broker(HOST, _request(command=("bash", "run.sh")),
                                           directory=str(source))
    finally:
        locked.chmod(0o600)
    assert set(glob.glob(f"{tempfile.gettempdir()}/vq-broker-*.tar")) == before


def test_rerun_max_zero_is_accepted_by_the_broker(tmp_path, monkeypatch):
    from vq import paths

    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    (tmp_path / "state").mkdir()
    cfg = config.Config(hosts={"alpha": config.HostConfig(ssh="alpha")},
                        broker={"enabled": True, "targets": ["alpha"]})
    monkeypatch.setattr(config, "load_config", lambda: cfg)
    payload = _archive(tmp_path / "upload.tar")
    argv = broker_relay.accept_argv(str(payload), _request(
        rerun_until_file_exists="DONE", rerun_max=0))
    result = CliRunner().invoke(main, argv)
    assert result.exit_code == 0, result.output


def test_an_empty_expected_sha_is_forwarded_not_dropped(client, tmp_path):
    source = tmp_path / "job"
    source.mkdir()
    result = CliRunner().invoke(main, ["submit", "fleet", "-d", str(source), "--cpus", "1",
                                       "--mem-mb", "100", "--program", "vibeqc",
                                       "--expected-sha", "", "--", "bash", "x"])
    # Forwarded as given, so the broker's accept refuses it exactly as a
    # direct submit would, instead of silently dropping the pin.
    assert result.exit_code == 0, result.output
    assert client[0][1].expected_sha == ""
    with pytest.raises(broker_store.BrokerStoreError, match="expected-sha"):
        _intent(program="vibeqc", expected_sha="")


def test_branch_is_refused_by_name_for_a_broker(client, tmp_path):
    source = tmp_path / "job"
    source.mkdir()
    result = CliRunner().invoke(main, ["submit", "fleet", "-d", str(source), "--cpus", "1",
                                       "--mem-mb", "100", "--branch", "main", "--", "bash", "x"])
    assert result.exit_code == 2 and "cannot use --branch" in result.output


def test_pool_works_when_the_default_host_is_the_broker(client, tmp_path, monkeypatch):
    cfg = config.load_config().model_copy(update={"default_host": "fleet"})
    monkeypatch.setattr(config, "load_config", lambda: cfg)
    source = tmp_path / "job"
    source.mkdir()
    result = CliRunner().invoke(main, ["submit", "-d", str(source), "--cpus", "1",
                                       "--mem-mb", "100", "--pool", "gpu", "--", "bash", "x"])
    assert result.exit_code == 0, result.output
    assert client[0][1].pool == "gpu"


def test_an_old_target_without_mark_fetched_keeps_the_client_fallback(broker, monkeypatch):
    record = broker.job("placed")

    def run_vq(host_cfg, *argv, **kwargs):
        raise transport.RemoteCommandError(
            "old", returncode=2, stderr="Usage: ...\nError: No such command 'mark-fetched'.")

    monkeypatch.setattr(transport, "run_remote_vq", run_vq)
    result = CliRunner().invoke(main, ["mark-fetched", record.broker_job_id, "--submitted-at",
                                       "2026-09-27T00:00:00+00:00", "--state", "completed"])
    assert result.exit_code == 2
    assert result.stderr.strip().splitlines()[-1] == "Error: No such command 'mark-fetched'."


def test_fetch_of_a_broker_id_on_the_broker_itself_is_refused_clearly(broker):
    record = broker.job("placed")
    result = CliRunner().invoke(main, ["fetch", "localhost", record.broker_job_id])
    assert result.exit_code == 2 and "fetch it from a client" in result.output
