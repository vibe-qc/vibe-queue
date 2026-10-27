"""#160: a broker places on a scheduler lane whose driver daemon runs on another host.

The broker and the driver each describe the lane from their own config.
Per-machine routing differs between them (the driver is its own `localhost`,
the lane's SSH alias and driver name are local names), so they compare only
the lane's definition, `lane_sha256` over `LANE_FIELDS`. The driver must
drive the lane itself. The payload is staged in the driver's home for the
keyed submit, removed once the driver answers and kept while its answer is
unknown. The driver side is the real `sample_local`; SSH and scp are stand-ins.
"""
from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

import pytest

from tests.test_broker_capacity_64 import _admission, host  # noqa: F401
from tests.test_broker_store_64 import _archive, _intent
from vq import (
    broker_capacity,
    broker_placement,
    broker_relay,
    broker_service,
    broker_store,
    broker_target,
    config,
    transport,
)

DEPLOYMENT = config.SchedulerRuntimeDeployment(update_command="update.sh",
                                               verify_command="verify.sh")
LANE = {"scheduler": "slurm", "scheduler_dialect": "slurm", "scratch_root": "/scratch",
        "submit_extra": ["--partition", "compute"], "scheduler_max_jobs": 3}


def broker_cfg(**lane: Any) -> config.Config:
    """The broker's view: the driver is a daemon host reached over SSH."""
    return config.Config(hosts={
        "driver": config.HostConfig(ssh="driver-node", remote_vq="/opt/vq/bin/vq"),
        "cluster": config.HostConfig(ssh="cluster-login", scheduler_driver="driver",
                                     **{**LANE, **lane}),
    }, broker={"enabled": True, "targets": ["cluster"]})


def driver_cfg(**lane: Any) -> config.Config:
    """The driver's own view of the same lane, in its own names."""
    return config.Config(hosts={
        "self": config.HostConfig(ssh="localhost"),
        "cluster": config.HostConfig(ssh="login01", scheduler_driver="self",
                                     **{**LANE, **lane}),
    })


def driver_payload(host: Any, cfg: config.Config) -> dict[str, Any]:  # noqa: F811
    """What `vq broker capacity --scheduler-target cluster --json` answers on the driver."""
    _, binding = broker_target.route(cfg, "cluster")
    host.admission = _admission(scheduler_targets={"cluster": {
        "holds": [], "free_jobs": 2, "can_attempt": True, "queue_ready": True,
        "authority": binding}})
    payload: dict[str, Any] = broker_capacity.sample_local(
        cfg, now=lambda: 10000, scheduler_target="cluster")
    return payload


def fetch(payload: dict[str, Any], cfg: config.Config | None = None) -> Any:
    calls = []

    def run(h: Any, *argv: str, **kwargs: Any) -> Any:
        calls.append(h.ssh)
        return subprocess.CompletedProcess(argv, 0, json.dumps(payload), "")

    result = broker_capacity.fetch_payload(cfg or broker_cfg(), "cluster", runner=run)
    assert calls == ["driver-node"]
    return result


class TestLane:
    def test_the_binding_names_the_lane_not_the_whole_entry(self) -> None:
        driver, binding = broker_target.route(broker_cfg(), "cluster")
        assert driver.ssh == "driver-node"
        assert binding == {"target": "cluster", "driver": "driver", "ssh": "driver-node",
                           "remote_vq": "/opt/vq/bin/vq", "scheduler": "slurm",
                           "lane_sha256": broker_target.lane_digest(
                               broker_cfg().hosts["cluster"])}
        # A driver on the broker machine keeps today's whole-entry digest.
        local = broker_cfg()
        local.hosts["driver"] = config.HostConfig(ssh="localhost")
        assert set(broker_target.route(local, "cluster")[1]) == {
            "target", "driver", "ssh", "remote_vq", "scheduler", "target_config_sha256"}

    def test_only_the_lane_fields_count(self) -> None:
        base = broker_target.lane_digest(broker_cfg().hosts["cluster"])
        same = broker_cfg().hosts["cluster"].model_copy(update={
            "ssh": "other-alias", "scheduler_driver": "elsewhere", "remote_vq": "/x/vq",
            "scheduler_prologue": ["module load x"], "remote_scheduler_host": "login02"})
        assert broker_target.lane_digest(same) == base
        for name, value in (("submit_extra", ["--partition", "gpu"]),
                            ("scratch_root", "/other"), ("scheduler_max_jobs", 4),
                            ("scheduler_max_cpus", 64), ("scheduler_max_wall_time_seconds", 60),
                            ("node_scratch_dir", "/tmp/n"), ("scheduler_dialect", "torque"),
                            ("scheduler_runtime_deployments", {"vibeqc": DEPLOYMENT})):
            changed = broker_cfg().hosts["cluster"].model_copy(update={name: value})
            assert broker_target.lane_digest(changed) != base, name
        assert set(broker_target.LANE_FIELDS) <= set(config.HostConfig.model_fields)


class TestEvidence:
    def test_a_matching_driver_gives_evidence_bound_to_the_broker(self, host: Any,  # noqa: F811
                                                                  ) -> None:
        payload = driver_payload(host, driver_cfg())
        assert payload["lane_sha256"] == broker_target.lane_digest(
            broker_cfg().hosts["cluster"])
        checked = fetch(payload)
        assert checked["authority"] == broker_target.route(broker_cfg(), "cluster")[1]
        evidence = broker_capacity.evidence_from_payload("cluster", checked, observed_at=100)
        assert evidence.snapshot.scheduler_admission is True
        assert evidence.snapshot.authority == checked["authority"]

    def test_a_driver_that_defines_the_lane_differently_gives_none(self, host: Any,  # noqa: F811
                                                                    ) -> None:
        payload = driver_payload(host, driver_cfg(submit_extra=["--partition", "gpu"]))
        with pytest.raises(ValueError, match="defines this lane differently"):
            fetch(payload)

    def test_a_driver_that_hops_on_gives_none(self, host: Any) -> None:  # noqa: F811
        cfg = driver_cfg()
        cfg.hosts["next"] = config.HostConfig(ssh="next-node")
        cfg.hosts["cluster"] = cfg.hosts["cluster"].model_copy(
            update={"scheduler_driver": "next"})
        with pytest.raises(ValueError, match="does not drive this lane itself"):
            fetch(driver_payload(host, cfg))

    @pytest.mark.parametrize(("change", "message"), [
        ({"lane_sha256": None}, "upgrade it for remote-driver placement"),
        ({"scheduler_target": "other"}, "different scheduler authority"),
        ({"authority": None}, "different scheduler authority"),
        ({"authority": "target=cluster"}, "different scheduler authority"),
    ])
    def test_an_answer_that_is_not_this_lane_gives_none(self, host: Any,  # noqa: F811
                                                        change: dict[str, Any],
                                                        message: str) -> None:
        payload = {**driver_payload(host, driver_cfg()), **change}
        with pytest.raises(ValueError, match=message):
            fetch(payload)

    @pytest.mark.parametrize(("field", "value"), [("target", "elsewhere"),
                                                  ("scheduler", "pbs")])
    def test_the_reported_lane_must_be_this_one(self, host: Any, field: str,  # noqa: F811
                                                value: str) -> None:
        payload = driver_payload(host, driver_cfg())
        payload["authority"] = {**payload["authority"], field: value}
        with pytest.raises(ValueError, match="different scheduler authority"):
            fetch(payload)


def test_config_and_placer_accept_a_driver_elsewhere() -> None:
    cfg = broker_cfg()
    assert broker_placement.remote_target_refusal(cfg)("cluster") is None
    cfg.hosts["cluster"] = cfg.hosts["cluster"].model_copy(update={"scheduler_driver": "gone"})
    assert broker_placement.remote_target_refusal(cfg)("cluster") == (
        "target cluster scheduler lane requires a daemon driver")


class Driver:
    """Stand-ins for scp, the driver's vq and its shell, with a home directory."""

    def __init__(self, home: Path) -> None:
        self.home = home
        self.calls: list[tuple[str, Any]] = []
        self.submit_outcomes: list[Any] = []

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(transport, "upload_file", self.upload)
        monkeypatch.setattr(transport, "run_remote_vq", self.vq)
        monkeypatch.setattr(transport, "run_remote_shell", self.shell)

    def upload(self, host_cfg: Any, local: Path, remote: str, **_kw: Any) -> None:
        assert host_cfg.ssh == "driver-node" and "/" not in remote
        self.calls.append(("upload", remote))
        (self.home / remote).write_bytes(local.read_bytes())

    def vq(self, host_cfg: Any, *argv: str, **_kw: Any) -> Any:
        assert host_cfg.ssh == "driver-node"
        self.calls.append(("vq", argv))
        if argv[0] == "submit":
            staged = argv[argv.index("-c") + 1] if "-c" in argv else None  # the archive
            # A home-relative staged copy, never the broker's own payload path.
            assert staged is not None and "/" not in staged and (self.home / staged).is_file()
            outcome = self.submit_outcomes.pop(0)
            if isinstance(outcome, Exception):
                raise outcome
            return subprocess.CompletedProcess(argv, 0, outcome + "\n", "")
        return subprocess.CompletedProcess(argv, 0, "{}", "")

    def shell(self, host_cfg: Any, *argv: str, **_kw: Any) -> Any:
        self.calls.append(("shell", argv))
        assert argv[:2] == ("rm", "-f")
        for name in argv[2:]:
            (self.home / name).unlink(missing_ok=True)
        return subprocess.CompletedProcess(argv, 0, "", "")

    def staged(self) -> list[str]:
        return sorted(p.name for p in self.home.iterdir())


def _evidence(cfg: config.Config) -> Any:
    _, binding = broker_target.route(cfg, "cluster")
    from tests.test_broker_capacity_64 import _payload

    return broker_capacity.evidence_from_payload("cluster", _payload(
        scheduler_target="cluster", authority=binding, free_cpus=None, free_mem_mb=None,
        runtime_pins={"vibeqc": "a" * 40}), observed_at=100)


def test_a_lost_answer_keeps_the_staging_and_the_replay_places_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = broker_cfg()
    home = tmp_path / "driver-home"
    home.mkdir()
    driver = Driver(home)
    driver.install(monkeypatch)
    driver.submit_outcomes = [transport.RemoteOutcomeUnknown("lost reply"), "abcdef012345"]
    root = tmp_path / "broker"
    record = broker_service.accept(cfg, _archive(tmp_path / "payload.tar"),
                                   _intent(program="vibeqc"), root=root, clock=lambda: 100)

    def collect(*_a: Any, **_k: Any) -> dict[str, Any]:
        return {"cluster": _evidence(cfg)}

    broker_service.run_pass(cfg, root=root, clock=lambda: 101, collect=collect)
    with broker_store.BrokerStore.open(root) as store:
        assert store.get(record.broker_job_id).state == "transferring"
    first = [c[1] for c in driver.calls if c[0] == "upload"]
    assert driver.staged() == first  # kept: the driver may still be reading it
    assert not any(c[0] == "shell" for c in driver.calls)

    broker_service.run_pass(cfg, root=root, clock=lambda: 1001, collect=collect)
    with broker_store.BrokerStore.open(root) as store:
        placed = store.get(record.broker_job_id)
    assert placed.current_attempt is not None and placed.state == "placed"
    assert placed.current_attempt.target_job_id == "abcdef012345"
    submits = [c[1] for c in driver.calls if c[0] == "vq" and c[1][0] == "submit"]
    assert len(submits) == 2 and all(
        s[1] == "cluster" and s[s.index("--idempotency-key") + 1] == record.broker_job_id
        for s in submits)
    second = [c[1] for c in driver.calls if c[0] == "upload"][1]
    assert ("shell", ("rm", "-f", second)) in driver.calls
    assert driver.staged() == first  # only the lost attempt's copy is left for the operator
    # Proxies reach the job through the same remote driver.
    assert broker_relay.target_of(cfg, placed) == (cfg.hosts["driver"], "abcdef012345")
    broker_relay.status_payload(cfg, placed)
    # Recovery re-checks the lane, not per-machine routing.
    renamed = broker_cfg()
    renamed.hosts["cluster"] = renamed.hosts["cluster"].model_copy(update={"ssh": "login-b"})
    assert broker_relay.target_of(renamed, placed) is not None
    with pytest.raises(ValueError, match="authority changed"):
        broker_relay.target_of(broker_cfg(scratch_root="/other"), placed)


@pytest.mark.parametrize("outcome", ["refused", "upload-unknown"])
def test_a_known_or_unsent_outcome_removes_the_staging(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, outcome: str,
) -> None:
    cfg = broker_cfg()
    home = tmp_path / "driver-home"
    home.mkdir()
    driver = Driver(home)
    driver.install(monkeypatch)
    if outcome == "upload-unknown":
        def upload(host_cfg: Any, local: Path, remote: str, **_kw: Any) -> None:
            driver.calls.append(("upload", remote))
            (home / remote).write_bytes(b"partial")
            raise transport.RemoteOutcomeUnknown("scp stopped")
        monkeypatch.setattr(transport, "upload_file", upload)
    else:
        driver.submit_outcomes = [transport.RemoteCommandError(
            "refused", returncode=1, stderr="Error: program not registered")]
    root = tmp_path / "broker"
    broker_service.accept(cfg, _archive(tmp_path / "payload.tar"),
                          _intent(program="vibeqc"), root=root, clock=lambda: 100)
    broker_service.run_pass(cfg, root=root, clock=lambda: 101,
                            collect=lambda *_a, **_k: {"cluster": _evidence(cfg)})
    assert driver.staged() == []
    assert [c[0] for c in driver.calls if c[0] in ("upload", "shell")] == ["upload", "shell"]
    if outcome == "upload-unknown":
        assert not any(c[0] == "vq" and c[1][0] == "submit" for c in driver.calls)
