"""The console must count toward the fleet convergence verdict.

Nothing in `vq admin rollout-latest` knew the web console existed, so
`--verify-only` could report the fleet `converged` while the coordinator
served pages from code 1081 commits behind the vq installed beside it.
That is what happened on the reference fleet for two weeks in 2026.

The hard part is not detecting drift. It is not degrading the ~13 hosts
that correctly have no console, and not degrading hosts whose vq is too
old to answer -- either mistake makes the check useless by making it
always red.
"""
from __future__ import annotations

import json
import subprocess
from types import SimpleNamespace

import pytest

from vq import fleet_rollout


def _proc(stdout: str = "", rc: int = 0, stderr: str = ""):
    return subprocess.CompletedProcess(
        args=["vq"], returncode=rc, stdout=stdout, stderr=stderr
    )


def _status(**over) -> str:
    payload = {
        "installed": True,
        "manager": "systemd-user",
        "unit_name": "vq-web",
        "unit_path": "/x",
        "installed_by_version": "0.25.0",
        "installed_at": "2026-08-06T00:00:00+00:00",
        "running_version": "0.25.0",
        "drifted": False,
        "active": True,
        "detail": None,
    }
    payload.update(over)
    return json.dumps(payload)


@pytest.fixture
def cfg() -> SimpleNamespace:
    return SimpleNamespace(host=lambda name: SimpleNamespace(ssh=name))


def _patch_probe(monkeypatch: pytest.MonkeyPatch, proc) -> None:
    from vq import transport

    monkeypatch.setattr(transport, "run_remote_vq", lambda *a, **k: proc)


class TestProbe:
    def test_no_console_installed_is_ok_and_not_applicable(
        self, cfg, monkeypatch
    ) -> None:
        """The correct steady state for every non-coordinator host."""
        _patch_probe(monkeypatch, _proc(_status(installed=False)))
        state = fleet_rollout.probe_console_state(cfg, "host_a")
        assert (state.installed, state.ok, state.degrades) == (False, True, False)

    def test_healthy_console_does_not_degrade(self, cfg, monkeypatch) -> None:
        _patch_probe(monkeypatch, _proc(_status()))
        assert fleet_rollout.probe_console_state(cfg, "erz").degrades is False

    def test_drifted_console_degrades_and_names_both_versions(
        self, cfg, monkeypatch
    ) -> None:
        _patch_probe(
            monkeypatch,
            _proc(_status(drifted=True, installed_by_version="0.16.0",
                          running_version="0.24.0")),
        )
        state = fleet_rollout.probe_console_state(cfg, "erz")
        assert state.degrades is True
        assert "0.16.0" in state.reason and "0.24.0" in state.reason

    def test_stopped_console_degrades(self, cfg, monkeypatch) -> None:
        _patch_probe(monkeypatch, _proc(_status(active=False)))
        state = fleet_rollout.probe_console_state(cfg, "erz")
        assert state.degrades is True
        assert "not running" in state.reason

    def test_an_old_vq_without_the_verb_is_unknown_not_degraded(
        self, cfg, monkeypatch
    ) -> None:
        """Rolling this check out must not mark the whole fleet degraded
        until every host has been upgraded. That would be backwards."""
        _patch_probe(monkeypatch, _proc(rc=2, stderr="Error: No such command 'status'."))
        state = fleet_rollout.probe_console_state(cfg, "old")
        assert (state.unknown, state.degrades) == (True, False)

    def test_unreachable_host_is_unknown_not_degraded(self, cfg, monkeypatch) -> None:
        from vq import transport

        def boom(*a, **k):
            raise transport.RemoteError("ssh: connect failed")

        monkeypatch.setattr(transport, "run_remote_vq", boom)
        state = fleet_rollout.probe_console_state(cfg, "asleep")
        assert (state.unknown, state.degrades) == (True, False)

    def test_garbage_output_is_unknown_not_degraded(self, cfg, monkeypatch) -> None:
        _patch_probe(monkeypatch, _proc("not json at all"))
        assert fleet_rollout.probe_console_state(cfg, "x").unknown is True

    def test_a_probe_never_raises(self, cfg, monkeypatch) -> None:
        from vq import transport

        monkeypatch.setattr(
            transport, "run_remote_vq", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom"))
        )
        assert fleet_rollout.probe_console_state(cfg, "x").degrades is False


class TestConsoleFailures:
    TOPOLOGY = {
        "erz": {"role": "vq-only"},
        "host_a": {"role": "managed"},
        "gone": {"role": "excluded"},
    }

    def _states(self, **kw) -> dict[str, fleet_rollout.ConsoleState]:
        return {h: s for h, s in kw.items()}

    def test_only_degraded_consoles_are_reported(self) -> None:
        states = self._states(
            erz=fleet_rollout.ConsoleState("erz", True, False, False, "stale"),
            host_a=fleet_rollout.ConsoleState("host_a", False, True, False, None),
        )
        assert fleet_rollout.console_failures(states, self.TOPOLOGY) == {
            "erz": ["stale"]
        }

    def test_excluded_hosts_are_ignored(self) -> None:
        states = self._states(
            gone=fleet_rollout.ConsoleState("gone", True, False, False, "stale")
        )
        assert fleet_rollout.console_failures(states, self.TOPOLOGY) == {}

    def test_no_probes_means_no_failures(self) -> None:
        assert fleet_rollout.console_failures({}, self.TOPOLOGY) == {}


class TestVerdictIntegration:
    """The whole point: `converged` must not survive a stale console."""

    #: A doctor sweep in which erz is healthy. Without this the host is
    #: degraded for "doctor result missing", which would mask the thing
    #: these tests are actually about.
    HEALTHY_DOCTOR = {"erz": {"host": "erz", "ok": True, "checks": []}}

    def _plan(self):
        return fleet_rollout.RolloutPlan(
            driver="erz",
            report=SimpleNamespace(),
            topology={"erz": {"role": "vq-only"}},
            actions=[],
            topology_errors=[],
        )

    def test_a_clean_fleet_with_no_console_is_converged(self) -> None:
        degraded = fleet_rollout.degraded_hosts(self._plan(), self.HEALTHY_DOCTOR, {})
        assert degraded == {}

    def test_a_stale_console_degrades_an_otherwise_clean_fleet(self) -> None:
        """Before this, every lane could be green and the verdict
        `converged` while the console served 1081-commit-stale pages."""
        consoles = {
            "erz": fleet_rollout.ConsoleState(
                "erz", True, False, False, "console installed by vq 0.16.0"
            )
        }
        degraded = fleet_rollout.degraded_hosts(self._plan(), self.HEALTHY_DOCTOR, consoles)
        assert list(degraded) == ["erz"]
        assert degraded["erz"][0].startswith("console: ")

    def test_an_unknown_console_does_not_degrade(self) -> None:
        consoles = {
            "erz": fleet_rollout.ConsoleState("erz", False, False, True, "too old")
        }
        assert fleet_rollout.degraded_hosts(self._plan(), self.HEALTHY_DOCTOR, consoles) == {}

    def test_consoles_default_to_none_for_existing_callers(self) -> None:
        """The parameter is optional, so every existing call site keeps
        its exact previous behaviour."""
        assert fleet_rollout.degraded_hosts(self._plan(), self.HEALTHY_DOCTOR) == {}
