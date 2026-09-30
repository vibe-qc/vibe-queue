"""`vq logs --follow` for a broker job (#64): wait for placement, then stream."""
from __future__ import annotations

import contextlib
import io

import pytest
from click.testing import CliRunner

from tests.test_broker_relay_64 import broker, client  # noqa: F401  (fixtures)
from vq import broker_relay, broker_store, transport
from vq.cli import main


class _Chunks(io.RawIOBase):
    def __init__(self, chunks):
        self.chunks = list(chunks)

    def readable(self):
        return True

    def readinto(self, buffer):
        if not self.chunks:
            return 0
        chunk = self.chunks.pop(0)
        buffer[: len(chunk)] = chunk
        return len(chunk)


@pytest.fixture
def streams(monkeypatch):
    opened = []

    @contextlib.contextmanager
    def fake_stream(host_cfg, *argv):
        opened.append((host_cfg.ssh, list(argv)))
        yield transport.RemoteStream(io.BufferedReader(_Chunks([b"line 1\n", b"line 2\n"])))

    monkeypatch.setattr(transport, "stream_remote_vq", fake_stream)
    return opened


def test_a_placed_job_streams_the_targets_follow(broker, streams):  # noqa: F811
    record = broker.job("placed")
    result = CliRunner().invoke(main, ["logs", "localhost", record.broker_job_id, "-f",
                                       "-n", "5", "--stderr"])
    assert result.exit_code == 0, result.output
    assert result.stdout_bytes == b"line 1\nline 2\n"
    assert streams == [("alpha", ["logs", "localhost", "0123456789ab", "-n", "5", "-f",
                                  "--stderr"])]


def test_a_held_job_waits_for_placement_then_streams(broker, streams):  # noqa: F811
    held = broker.job()
    sleeps = []

    def pass_places_it(seconds):
        # Stand in for the broker pass placing the job while we wait.
        sleeps.append(seconds)
        if len(sleeps) == 2:
            _place(held)

    notes = []
    cfg, record = broker_relay.resolve_local(held.broker_job_id)
    broker_relay.follow_logs(
        cfg, record, "logs", "localhost", "{target}", "-n", "100", "-f",
        notify=notes.append, poll_seconds=0.0, sleep=pass_places_it,
    )
    assert notes[0] == f"{held.broker_job_id} is held; waiting for placement"
    assert notes[1].startswith(f"{held.broker_job_id} placed on alpha as ")
    assert len(sleeps) == 2 and len(notes) == 2
    assert streams == [("alpha", ["logs", "localhost", "0123456789ab", "-n", "100", "-f"])]


def _place(record):
    from tests.test_broker_store_64 import _snapshot
    from vq import overview

    with broker_store.BrokerStore.open() as store:
        decision = overview.place_broker_job(
            [_snapshot()], record.intent.placement_request(), now=105.0,
            max_age_seconds=60.0)
        record = store.reserve(record.broker_job_id, record.revision, decision=decision,
                               snapshot=_snapshot(), target_state_root="/s", now=106.0)
        record = store.begin_transfer(record.broker_job_id, record.revision, now=107.0)
        store.record_placed(
            record.broker_job_id, record.revision,
            request_fingerprint=record.current_attempt.request_fingerprint,
            target_job_id="0123456789ab", now=108.0)


def test_a_job_cancelled_before_placement_is_an_error(broker, streams):  # noqa: F811
    record = broker.job()
    assert CliRunner().invoke(main, ["kill", "localhost", record.broker_job_id]).exit_code == 0
    result = CliRunner().invoke(main, ["logs", "localhost", record.broker_job_id, "-f"])
    assert result.exit_code == 1 and "cancelled before placement" in result.output
    assert streams == []


def test_a_client_follow_is_streamed_from_the_broker(client, streams):  # noqa: F811
    result = CliRunner().invoke(main, ["logs", "fleet", "b-0123456789ab", "-f"])
    assert result.exit_code == 0, result.output
    assert result.stdout_bytes == b"line 1\nline 2\n"
    assert streams == [("coordinator", ["logs", "localhost", "b-0123456789ab", "-f"])]

