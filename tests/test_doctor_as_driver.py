"""``vq doctor HOST --as-driver CANDIDATE`` — rehearse a driver migration.

The host_0 lesson (2026-07-23): ``vq doctor host_0`` passed while
host_0 had no vq config, no ssh config, and no key on either cluster,
because a host's own doctor proves only that its daemon answers. Nothing
exercised host_0's ability to DRIVE host_f or host_c, and the green was
misread as migration-ready. ``--as-driver`` substitutes a candidate into
every driver-side check — daemon health plus the delegated scheduler-probe,
which runs on the candidate and therefore proves the candidate's own config
resolves the host, its ssh route reaches the cluster, and the scheduler
clients answer from there — before any ``scheduler_driver`` is repointed.
"""
from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import config, paths
from vq import doctor as doctor_module
from vq.cli import main

DRIVER_SHA = "a" * 40
TREE_SHA256 = "12" * 32

_PROBE_PAYLOAD = {
    "dialect": "torque",
    "scheduler": "pbs",
    "version": "2.5.12",
    "confidence": "confirmed",
    "binaries": {
        "qsub": True,
        "qstat": True,
        "qdel": True,
        "qhold": True,
        "qrls": True,
    },
    "raw_version": "version: 2.5.12",
    "server_state": "Active",
    "pbs_sched_running": True,
    "queues": [{"name": "batch", "enabled": True, "started": True}],
    "notes": [],
}


@pytest.fixture
def doctor_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    monkeypatch.setattr(
        doctor_module,
        "_driver_identity_probe",
        lambda kind, _deadline: (
            (DRIVER_SHA, None)
            if kind == "source_sha"
            else (TREE_SHA256, None)
        ),
    )
    (tmp_path / "cfg").mkdir()
    (tmp_path / "cfg" / "config.toml").write_text(
        "\n".join(
            [
                "[hosts.driver]",
                'ssh = "driver-ssh"',
                "",
                "[hosts.erz]",
                'ssh = "erz-ssh"',
                "",
                "[hosts.host_a]",
                'ssh = "host_a"',
                "",
                "[hosts.host_f]",
                'ssh = "host_f-login"',
                'scheduler = "pbs"',
                'scheduler_dialect = "torque"',
                'scratch_root = "/scratch"',
                'scheduler_driver = "driver"',
                "",
            ]
        ),
        encoding="utf-8",
    )
    return tmp_path


def _fake_remote_factory(captured, *, candidate_probe):  # type: ignore[no-untyped-def]
    """A remote-vq stub that answers provenance, ping, and scheduler-probe."""

    def fake_remote(host_cfg, *args, **kwargs):  # type: ignore[no-untyped-def]
        captured.append((host_cfg.ssh, args))
        if args == ("source-tree-sha256",):
            return subprocess.CompletedProcess(
                args=["vq"], returncode=0, stdout=f"{TREE_SHA256}\n", stderr=""
            )
        if args == ("source-sha",):
            return subprocess.CompletedProcess(
                args=["vq"], returncode=0, stdout=f"{DRIVER_SHA}\n", stderr=""
            )
        if args[:2] == ("scheduler-probe", "host_f"):
            return candidate_probe(host_cfg)
        return subprocess.CompletedProcess(
            args=["vq"],
            returncode=0,
            stdout=json.dumps({"ok": True, "version": "0.12.1",
                               "source_sha": DRIVER_SHA, "source_tree_sha256": TREE_SHA256}),
            stderr="",
        )

    return fake_remote


class TestDoctorAsDriver:
    @pytest.mark.parametrize("helper_sha,helper_tree,candidate_ok,candidate_tree,expected_ok", [
        (DRIVER_SHA, TREE_SHA256, True, "22" * 32, False),
        ("c" * 40, "33" * 32, True, "22" * 32, False),
        ("b" * 40, "22" * 32, True, "22" * 32, True),
        (DRIVER_SHA, TREE_SHA256, True, None, False),
        (DRIVER_SHA, TREE_SHA256, False, "22" * 32, False),
    ], ids=["helper-matches-caller", "three-distinct-identities", "helper-matches-candidate",
            "candidate-identity-unavailable", "candidate-daemon-unavailable"])
    def test_helper_identity_uses_candidate_daemon_not_invoker(
        self, doctor_state, monkeypatch, helper_sha, helper_tree,
        candidate_ok, candidate_tree, expected_ok,
    ):
        calls = []
        local_probes = []
        def local_identity(kind, deadline):
            local_probes.append(kind)
            return (DRIVER_SHA, None) if kind == "source_sha" else (TREE_SHA256, None)

        monkeypatch.setattr(doctor_module, "_driver_identity_probe", local_identity)

        def remote(host_cfg, *args, **kwargs):
            calls.append((host_cfg.ssh, args))
            if args == ("--version",):
                output = "vq, version 0.26.32\n"
            elif args == ("source-identity",):
                output = json.dumps({"source_sha": helper_sha, "source_tree_sha256": helper_tree})
            elif args[:2] == ("daemon", "ping"):
                assert host_cfg.ssh == "erz-ssh"
                output = json.dumps({"ok": candidate_ok, "version": "0.26.32",
                                     "source_sha": "b" * 40, "source_tree_sha256": candidate_tree})
            elif args[:2] == ("scheduler-probe", "host_f"):
                output = json.dumps(_PROBE_PAYLOAD)
            else:
                raise AssertionError(args)
            return subprocess.CompletedProcess(args, 0, output, "")

        monkeypatch.setattr("vq.cli.transport.run_remote_vq", remote)
        result = CliRunner().invoke(main, ["doctor", "host_f", "--as-driver", "erz", "--json"])
        payload = json.loads(result.output)
        helper = next(item for item in payload["checks"] if item["name"] == "scheduler_remote_vq")
        assert helper["ok"] is expected_ok
        assert "candidate driver 'erz'" in helper["message"]
        assert helper["comparison_driver"] == "erz"
        if candidate_ok and candidate_tree:
            assert helper["comparison_source_sha"] == "b" * 40
            assert helper["comparison_source_tree_sha256"] == candidate_tree
        else:
            assert "unavailable" in helper["message"]
        assert not local_probes
        assert sum(args[:2] == ("daemon", "ping") for _, args in calls) == 1
        assert not any(ssh == "driver-ssh" for ssh, _ in calls)

    def test_healthy_candidate_substitutes_for_the_configured_driver(
        self, doctor_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Every driver-side call lands on the candidate, none on the driver."""
        captured: list[tuple[str, tuple[str, ...]]] = []

        def probe(host_cfg):  # type: ignore[no-untyped-def]
            return subprocess.CompletedProcess(
                args=["vq"],
                returncode=0,
                stdout=json.dumps(_PROBE_PAYLOAD),
                stderr="",
            )

        monkeypatch.setattr(
            "vq.cli.transport.run_remote_vq",
            _fake_remote_factory(captured, candidate_probe=probe),
        )

        result = CliRunner().invoke(
            main, ["doctor", "host_f", "--as-driver", "erz", "--json"]
        )

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["ok"] is True
        assert payload["as_driver"] == "erz"
        # The configured driver stays reported as such: --as-driver is a
        # rehearsal, not a config change.
        assert payload["driver"] == "driver"
        driver_side = [ssh for ssh, args in captured if ssh != "host_f-login"]
        assert driver_side and set(driver_side) == {"erz-ssh"}
        checks = {check["name"]: check for check in payload["checks"]}
        assert "candidate driver 'erz'" in checks["scheduler_driver"]["message"]
        assert checks["scheduler_clients"]["ok"] is True

    def test_candidate_without_the_host_in_its_config_fails(
        self, doctor_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The exact host_0 hole: the candidate's OWN config lacks the host."""
        captured: list[tuple[str, tuple[str, ...]]] = []

        def probe(host_cfg):  # type: ignore[no-untyped-def]
            return subprocess.CompletedProcess(
                args=["vq"],
                returncode=2,
                stdout="",
                stderr="no [hosts.host_f] configured\n",
            )

        monkeypatch.setattr(
            "vq.cli.transport.run_remote_vq",
            _fake_remote_factory(captured, candidate_probe=probe),
        )

        result = CliRunner().invoke(
            main, ["doctor", "host_f", "--as-driver", "erz", "--json"]
        )

        assert result.exit_code == 1
        payload = json.loads(result.output)
        assert payload["ok"] is False
        checks = {check["name"]: check for check in payload["checks"]}
        assert checks["scheduler_clients"]["ok"] is False
        assert "no [hosts.host_f] configured" in checks["scheduler_clients"]["message"]

    def test_unknown_candidate_is_a_usage_error_not_a_fleet_of_fails(
        self, doctor_state: Path
    ) -> None:
        result = CliRunner().invoke(main, ["doctor", "host_f", "--as-driver", "nope"])

        assert result.exit_code == 2
        assert "nope" in result.output

    def test_as_driver_on_a_venv_host_is_a_usage_error(
        self, doctor_state: Path
    ) -> None:
        result = CliRunner().invoke(main, ["doctor", "host_a", "--as-driver", "erz"])

        assert result.exit_code == 2
        assert "scheduler hosts" in result.output

    def test_text_header_names_the_rehearsal(
        self, doctor_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured: list[tuple[str, tuple[str, ...]]] = []

        def probe(host_cfg):  # type: ignore[no-untyped-def]
            return subprocess.CompletedProcess(
                args=["vq"],
                returncode=0,
                stdout=json.dumps(_PROBE_PAYLOAD),
                stderr="",
            )

        monkeypatch.setattr(
            "vq.cli.transport.run_remote_vq",
            _fake_remote_factory(captured, candidate_probe=probe),
        )

        result = CliRunner().invoke(main, ["doctor", "host_f", "--as-driver", "erz"])

        assert result.exit_code == 0, result.output
        assert "== vq doctor: host_f (as-driver erz) ==" in result.output
