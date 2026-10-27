"""#159: an HTTPS gateway submission carries its calibrated runtime estimate.

`POST /v1/jobs` accepts an optional `runtime_seconds: [low, high]`, checked
as the broker intent checks it and recorded there for the completion-time
policy to rank by. `vq remote submit --estimate CALIBRATED` sends the same
interval as `vq submit BROKER --estimate`. Relay connectors receive the key
only when it is set. The real gateway app, client and broker store.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from tests.test_broker_store_64 import _intent
from tests.test_estimate_time_limit_159 import UPPER, _document
from tests.test_gateway_client_163 import _credentials, _request, cli_main
from tests.test_gateway_client_163 import connected as connected  # noqa: F401 — fixture
from tests.test_gateway_submit_163 import gateway as gateway  # noqa: F401 — fixture
from vq import broker_store
from vq.gateway import client as gw_client
from vq.gateway import jobs as gateway_jobs


def _upload(client: Any, payload: Path) -> str:
    archive, temporary = gw_client.pack(payload)
    try:
        return str(client.upload(archive))
    finally:
        if temporary:
            archive.unlink()


def _stored(broker_root: Path, job_id: str) -> broker_store.BrokerIntent:
    with broker_store.BrokerStore.open(broker_root) as store:
        return store.get(job_id).intent


def test_the_interval_is_recorded_on_the_broker_intent(connected: Any) -> None:  # noqa: F811
    client, _adapter, payload, broker_root, _secret = connected
    upload_id = _upload(client, payload)
    job = client.submit({**_request(upload_id), "runtime_seconds": [60, 1500.15]})
    assert _stored(broker_root, job["job_id"]).runtime_seconds == (60, 1500.15)
    plain = client.submit(_request(_upload(client, payload), key="paper-run-0002"))
    assert _stored(broker_root, plain["job_id"]).runtime_seconds is None


@pytest.mark.parametrize(("runtime", "message"), [
    ([20, 10], "runtime_seconds must be finite seconds 0 <= low <= high"),
    ([1], "runtime_seconds must be finite seconds"),
    ([True, 2], "runtime_seconds must be finite seconds"),
    (["1", 2], "runtime_seconds must be finite seconds"),
    ({"low": 1, "high": 2}, "runtime_seconds must be a list [low, high]"),
    ("1-2", "runtime_seconds must be a list [low, high]"),
])
def test_a_bad_interval_is_refused_and_nothing_held(connected: Any, runtime: Any,  # noqa: F811
                                                    message: str) -> None:
    client, _adapter, payload, broker_root, _secret = connected
    upload_id = _upload(client, payload)
    with pytest.raises(gw_client.ClientError, match=message.replace("[", r"\[")) as caught:
        client.submit({**_request(upload_id), "runtime_seconds": runtime})
    assert caught.value.status == 400
    with broker_store.BrokerStore.open(broker_root) as store:
        assert store.scan()[0] == []


def test_a_connector_sees_the_key_only_when_it_is_set() -> None:
    ranked = _intent(runtime_seconds=(60.0, 1500.15))
    request = gateway_jobs.intent_request(ranked)
    assert request["runtime_seconds"] == [60.0, 1500.15]
    assert gateway_jobs.intent_from_request(request) == ranked
    plain = gateway_jobs.intent_request(_intent())
    assert "runtime_seconds" not in plain
    assert gateway_jobs.intent_from_request(plain) == _intent()


class TestClient:
    def _estimate(self, tmp_path: Path, payload: Path, **kwargs: Any) -> Path:
        path = tmp_path / "calibrated.json"
        path.write_text(json.dumps(_document(tmp_path, payload, **kwargs)))
        return path

    def _submit(self, connected: Any, tmp_path: Path, *extra: str) -> Any:  # noqa: F811
        _client, _adapter, payload, _root, secret = connected
        return CliRunner().invoke(cli_main(), [
            "remote", "submit", str(payload), "--program", "vibeqc-release", "--cpus", "2",
            "--mem-mb", "1024", "--wall-time", "600", "--idempotency-key", "cli-0001",
            "--credentials", str(_credentials(tmp_path, secret)), *extra,
            "--", "python3", "run.py"])

    def test_remote_submit_sends_the_calibrated_runtime(
        self, connected: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,  # noqa: F811
    ) -> None:
        _client, adapter, payload, broker_root, _secret = connected
        monkeypatch.setattr(gw_client, "urllib_send", lambda creds: adapter)
        result = self._submit(connected, tmp_path, "--estimate",
                              str(self._estimate(tmp_path, payload)))
        assert result.exit_code == 0, result.output
        job_id = result.output.strip().splitlines()[0]
        assert _stored(broker_root, job_id).runtime_seconds == (60.0, UPPER * 1.5)

    @pytest.mark.parametrize(("kwargs", "raw", "message"), [
        ({"runtime": None}, False, "the runtime is uncalibrated"),
        ({}, True, "not a raw receipt"),
    ])
    def test_what_cannot_rank_is_refused_before_any_upload(
        self, connected: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,  # noqa: F811
        kwargs: dict[str, Any], raw: bool, message: str,
    ) -> None:
        _client, adapter, payload, _root, _secret = connected
        monkeypatch.setattr(gw_client, "urllib_send", lambda creds: adapter)
        path = self._estimate(tmp_path, payload, **kwargs)
        if raw:
            path.write_text(json.dumps(json.loads(path.read_text())["estimate"]))
        result = self._submit(connected, tmp_path, "--estimate", str(path))
        assert result.exit_code == 2 and message in result.output, result.output
        assert adapter.calls == []
