"""#159/#160: a broker job ranked by its calibrated runtime estimate.

`vq submit BROKER --estimate CALIBRATED` sends the estimate's runtime
interval (the receipt's lower bound, the calibrated upper bound) through the
relay; `vq broker accept --runtime-seconds LOW HIGH` records it on the
intent, written only when set; the completion-time policy plans with it, so
a host whose predicted execution would overrun a walltime limit is refused.
Real store, real accept command, real placer and planner, real calibration.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from tests.test_broker_completion_160 import _fleet_file, _place, _record, world  # noqa: F401
from tests.test_broker_relay_64 import Remote, _request
from tests.test_broker_store_64 import _archive, _intent
from vq import broker_relay, broker_store, config, paths
from vq import submit as submit_module
from vq.cli import main

sys.path.insert(0, str(Path(__file__).parent))
from test_estimate_time_limit_159 import UPPER, _document  # noqa: E402

#: The intent keys a vq without this field writes: unchanged for a job without it.
LEGACY_KEYS = {"command", "cpus", "mem_mb", "wall_time_seconds", "priority", "auto_resume",
               "retry", "job_name", "program", "expected_sha", "tags",
               "clean_workdir_on_terminal", "rerun_until_file_exists", "rerun_max", "pool"}


class TestIntent:
    def test_it_is_written_only_when_set(self) -> None:
        plain = _intent()
        assert set(plain.to_json()) == LEGACY_KEYS
        assert broker_store.BrokerIntent.from_json(plain.to_json()) == plain
        ranked = _intent(runtime_seconds=(60.0, 1500.15))
        assert ranked.to_json()["runtime_seconds"] == [60.0, 1500.15]
        assert broker_store.BrokerIntent.from_json(ranked.to_json()) == ranked
        assert ranked.digest() != plain.digest()

    @pytest.mark.parametrize("bounds", [
        (10.0, 5.0), (-1.0, 5.0), (0.0, float("inf")), (float("nan"), 1.0), (True, 2.0),
        (1.0,), (1.0, 2.0, 3.0), [1.0, 2.0], ("1", 2.0)])
    def test_only_an_ordered_finite_interval_is_accepted(self, bounds: Any) -> None:
        with pytest.raises(broker_store.BrokerStoreError, match="runtime_seconds must be"):
            _intent(runtime_seconds=bounds)

    def test_a_record_needs_the_interval_as_a_list(self) -> None:
        data = {**_intent().to_json(), "runtime_seconds": "60-1500"}
        with pytest.raises(broker_store.BrokerRecordCorrupt, match="must be a list"):
            broker_store.BrokerIntent.from_json(data)
        with pytest.raises(broker_store.BrokerRecordCorrupt, match="unexpected fields"):
            broker_store.BrokerIntent.from_json({**_intent().to_json(), "other": 1})


@pytest.fixture
def broker_host(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    (tmp_path / "state").mkdir()
    cfg = config.Config(hosts={"alpha": config.HostConfig(ssh="alpha")},
                        broker={"enabled": True, "targets": ["alpha"]})
    monkeypatch.setattr(config, "load_config", lambda: cfg)


def test_the_relay_argv_records_the_interval(tmp_path: Path, broker_host: None) -> None:
    request = _request(runtime_seconds=(60.0, 1500.15))
    argv = broker_relay.accept_argv(str(_archive(tmp_path / "upload.tar")), request)
    assert argv[argv.index("--runtime-seconds"):][:3] == ["--runtime-seconds", "60.0", "1500.15"]
    result = CliRunner().invoke(main, argv)
    assert result.exit_code == 0, result.output
    with broker_store.BrokerStore.open() as store:
        intent = store.get(result.output.strip()).intent
    assert intent.runtime_seconds == (60.0, 1500.15)
    assert "--runtime-seconds" not in broker_relay.accept_argv("x", _request())


def test_broker_accept_refuses_a_bad_interval(tmp_path: Path, broker_host: None) -> None:
    result = CliRunner().invoke(main, [
        "broker", "accept", str(_archive(tmp_path / "upload.tar")), "--cpus", "1",
        "--mem-mb", "100", "--runtime-seconds", "20", "10", "--", "true"])
    assert result.exit_code != 0 and "runtime_seconds must be" in result.output


class TestSubmit:
    @pytest.fixture
    def relay(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Remote:
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        (tmp_path / "cfg").mkdir()
        (tmp_path / "cfg" / "config.toml").write_text(
            '[hosts.fleet]\nssh = "fleet"\nbroker = true\n')
        remote = Remote()
        remote.install(monkeypatch)
        return remote

    def _files(self, tmp_path: Path, **kwargs: Any) -> tuple[Path, Path]:
        payload = tmp_path / "payload"
        payload.mkdir(exist_ok=True)
        (payload / "run.py").write_text("pass\n")
        path = tmp_path / "calibrated.json"
        path.write_text(json.dumps(_document(tmp_path, payload, **kwargs)))
        return payload, path

    def _submit(self, payload: Path, *args: str) -> Any:
        return CliRunner().invoke(main, ["submit", "fleet", "--mem-mb", "2000", *args,
                                         "-d", str(payload), "--", "python3", "run.py"])

    def test_a_calibrated_runtime_goes_to_the_broker(self, tmp_path: Path,
                                                     relay: Remote) -> None:
        payload, path = self._files(tmp_path)
        result = self._submit(payload, "--estimate", str(path))
        assert result.exit_code == 0, result.output
        (argv,) = relay.vq
        start = argv.index("--runtime-seconds")
        assert argv[start:start + 3] == ["--runtime-seconds", "60.0", repr(UPPER * 1.5)]

    @pytest.mark.parametrize(("kwargs", "extra", "message"), [
        ({"runtime": None}, [], "the runtime is uncalibrated"),
        ({"hardware": "class-b"}, [], "the runtime is uncalibrated"),
        ({}, ["--mem-from-estimate"], "cannot use --mem-from-estimate"),
        ({}, ["--time-from-estimate"], "cannot use --time-from-estimate"),
    ])
    def test_what_cannot_rank_a_broker_job_is_refused(
        self, tmp_path: Path, relay: Remote, kwargs: dict[str, Any], extra: list[str],
        message: str, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        payload, path = self._files(tmp_path, **kwargs)
        if extra == ["--mem-from-estimate"]:
            # This calibration has no memory group; give the limit it would derive.
            monkeypatch.setattr(submit_module, "mem_mb_from_calibrated_estimate",
                                lambda _document: 2000)
            result = CliRunner().invoke(main, [
                "submit", "fleet", "--estimate", str(path), "--mem-from-estimate",
                "-d", str(payload), "--", "python3", "run.py"])
        else:
            result = self._submit(payload, "--estimate", str(path), *extra)
        assert result.exit_code == 2 and message in result.output, result.output
        assert relay.vq == [] and relay.uploads == []

    def test_a_raw_receipt_is_refused(self, tmp_path: Path, relay: Remote) -> None:
        payload, path = self._files(tmp_path)
        path.write_text(json.dumps(json.loads(path.read_text())["estimate"]))
        result = self._submit(payload, "--estimate", str(path))
        assert result.exit_code == 2 and "not a raw receipt" in result.output
        assert relay.vq == []


class TestRanking:
    def _fleet(self, world: Any) -> None:  # noqa: F811
        # alpha is twice as fast but runs nothing longer than 30 minutes.
        _fleet_file(world.tmp_path,
                    {**_record("alpha", speed=2.0), "walltime_unlimited": False,
                     "max_walltime_seconds": 1800},
                    _record("beta", speed=1.0))

    def test_a_host_the_estimate_would_overrun_is_refused(self, world: Any) -> None:  # noqa: F811
        self._fleet(world)
        outcome = _place(world, runtime_seconds=(1000.0, 4000.0))
        assert outcome.outcome == "placed" and outcome.host == "beta"
        blockers = dict(world.store.get(outcome.broker_job_id).current_attempt.blockers)
        assert blockers == {"alpha": "completion-time: predicted execution up to 2000 s "
                                     "exceeds the 1800 s walltime limit"}

    def test_without_an_estimate_the_faster_host_is_chosen(self, world: Any) -> None:  # noqa: F811
        self._fleet(world)
        assert _place(world).host == "alpha"

    def test_a_job_its_own_walltime_cannot_hold_is_held(self, world: Any) -> None:  # noqa: F811
        outcome = _place(world, wall_time_seconds=600, runtime_seconds=(1000.0, 1400.0))
        # alpha (speed 1) needs up to 1400 s, beta (speed 2) up to 700 s: both overrun.
        assert outcome.outcome == "held" and world.target.calls == []
        assert dict(outcome.blockers) == {
            "alpha": "completion-time: predicted execution up to 1400 s exceeds the "
                     "requested 600 s walltime",
            "beta": "completion-time: predicted execution up to 700 s exceeds the "
                    "requested 600 s walltime"}


def test_broker_show_names_the_figures_a_blocker_rests_on(
    tmp_path: Path, broker_host: None,
) -> None:
    def accept(*extra: str) -> str:
        result = CliRunner().invoke(main, [
            "broker", "accept", str(_archive(tmp_path / f"upload{len(extra)}.tar")),
            "--cpus", "1", "--mem-mb", "100", *extra, "--", "true"])
        assert result.exit_code == 0, result.output
        return str(result.output.strip())

    ranked = accept("--wall-time-seconds", "3600", "--runtime-seconds", "60", "1500.15")
    plain = accept()
    shown = CliRunner().invoke(main, ["broker", "show", ranked])
    assert shown.exit_code == 0, shown.output
    assert "wall_time_seconds: 3600\n" in shown.output
    assert "runtime estimate: 60-1500.15 s on a speed-1.0 target\n" in shown.output
    view = json.loads(CliRunner().invoke(main, ["broker", "show", ranked, "--json"]).output)
    assert (view["wall_time_seconds"], view["runtime_seconds"]) == (3600, [60.0, 1500.15])
    bare = CliRunner().invoke(main, ["broker", "show", plain])
    assert "wall_time_seconds" not in bare.output and "runtime estimate" not in bare.output
    listed = {v["broker_job_id"]: v for v in json.loads(
        CliRunner().invoke(main, ["broker", "list", "--json"]).output)["jobs"]}
    assert (listed[plain]["wall_time_seconds"], listed[plain]["runtime_seconds"]) == (None, None)
    with broker_store.BrokerStore.open() as store:
        status = broker_relay.status_payload(config.load_config(), store.get(ranked))
    assert status["runtime_seconds"] == [60.0, 1500.15]
