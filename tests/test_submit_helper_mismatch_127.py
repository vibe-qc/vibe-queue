"""``vq submit`` checks that a scheduler helper is the driver's tree (#127).

The driver polls a scheduler job through the host's remote vq helper. On one
host a helper from another source tree left a job ``unpolled`` with no scheduler
job id, silently. ``vq doctor`` already reports the mismatch as
``scheduler_remote_vq``; the submit asks the same question on the driver
before it writes the spec. By maintainer decision a proven mismatch only
warns by default; ``[fleet] strict_helper_identity = true`` makes a verdict
that names both identities refuse. A helper that did not answer is always a
warning, and the job queues as before.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import config, doctor, paths, transport
from vq import submit as submit_module
from vq.cli import main

DRIVER_SHA = "d" * 40
DRIVER_TREE = "a" * 64
HELPER_TREE = "b" * 64
HELPER_SHA = "c" * 40

# These tests are the guard's own; the conftest stub must not answer for it.
pytestmark = pytest.mark.no_autopatch_scheduler_helper_guard

LOCAL_DRIVER_TOML = (
    '[hosts.host_f]\nssh = "host_f-login"\nscheduler = "pbs"\n'
    'scheduler_dialect = "torque"\nscratch_root = "/scratch/USER"\n'
    'scheduler_driver = "localhost"\nremote_vq = "vq"\n'
)


@pytest.fixture
def cli_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    (tmp_path / "cfg" / "config.toml").write_text(LOCAL_DRIVER_TOML, encoding="utf-8")
    (tmp_path / "payload").mkdir()
    return tmp_path


@pytest.fixture
def cfg(cli_state: Path) -> config.Config:
    return config.load_config()


STRICT_TOML = LOCAL_DRIVER_TOML + "\n[fleet]\nstrict_helper_identity = true\n"


@pytest.fixture
def strict_cfg(cli_state: Path) -> config.Config:
    (cli_state / "cfg" / "config.toml").write_text(STRICT_TOML, encoding="utf-8")
    return config.load_config()


def _prefix() -> str:
    return "vq available on host_f-login: vq, version 0.26.15 \"Raymond's Bazaar\""


def _tree_mismatch() -> dict[str, object]:
    return doctor.check(
        "scheduler_remote_vq",
        False,
        f"{_prefix()}; {doctor.HELPER_TREE_MISMATCH}: helper {HELPER_TREE}, "
        f"driver {DRIVER_TREE}",
    )


def _sha_mismatch() -> dict[str, object]:
    return doctor.check(
        "scheduler_remote_vq",
        False,
        f"{_prefix()}; {doctor.HELPER_SHA_MISMATCH}: helper {HELPER_SHA}, "
        f"driver {DRIVER_SHA}",
    )


def _timed_out() -> dict[str, object]:
    return doctor.check(
        "scheduler_remote_vq",
        False,
        "helper_source_identity timed out after 10.28s (outer deadline 10s)",
        timed_out=True,
        subprobe="helper_source_identity",
    )


def _match() -> dict[str, object]:
    return doctor.check(
        "scheduler_remote_vq",
        True,
        f"{_prefix()}; source-tree SHA-256 {DRIVER_TREE} and SOURCE-SHA "
        f"{DRIVER_SHA} match driver",
    )


def _answer(monkeypatch: pytest.MonkeyPatch, verdict: dict[str, object]) -> list[str]:
    asked: list[str] = []

    def probe(cfg: config.Config, scheduler_host: str) -> dict[str, object]:
        asked.append(scheduler_host)
        return verdict

    monkeypatch.setattr(submit_module, "_probe_scheduler_helper", probe)
    return asked


class TestRefusal:
    @pytest.mark.parametrize(
        "verdict,helper,driver",
        [
            (_tree_mismatch(), HELPER_TREE, DRIVER_TREE),
            (_sha_mismatch(), HELPER_SHA, DRIVER_SHA),
        ],
        ids=["source_tree", "source_sha"],
    )
    def test_a_proven_mismatch_refuses_and_names_both_identities(
        self, strict_cfg: config.Config, monkeypatch: pytest.MonkeyPatch,
        verdict: dict[str, object], helper: str, driver: str,
    ) -> None:
        asked = _answer(monkeypatch, verdict)
        warnings: list[str] = []
        with pytest.raises(ValueError, match="does not match this driver") as exc:
            submit_module._refuse_stale_scheduler_helper(
                strict_cfg, "host_f", allow_mismatch=False, warning_sink=warnings.append,
            )
        message = str(exc.value)
        assert helper in message and driver in message
        assert "vq admin update host_f" in message
        assert "--allow-helper-mismatch" in message
        assert asked == ["host_f"]
        assert warnings == []

    def test_by_default_a_proven_mismatch_only_warns(
        self, cfg: config.Config, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Maintainer decision: driver-first rollouts must not refuse submits."""
        _answer(monkeypatch, _sha_mismatch())
        warnings: list[str] = []
        submit_module._refuse_stale_scheduler_helper(
            cfg, "host_f", allow_mismatch=False, warning_sink=warnings.append,
        )
        assert len(warnings) == 1
        assert "warn-only by default" in warnings[0]
        assert HELPER_SHA in warnings[0] and DRIVER_SHA in warnings[0]
        assert "vq admin update host_f" in warnings[0]

    def test_the_override_turns_the_refusal_into_a_warning(
        self, strict_cfg: config.Config, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _answer(monkeypatch, _tree_mismatch())
        warnings: list[str] = []
        submit_module._refuse_stale_scheduler_helper(
            strict_cfg, "host_f", allow_mismatch=True, warning_sink=warnings.append,
        )
        assert len(warnings) == 1
        assert "--allow-helper-mismatch" in warnings[0]
        assert HELPER_TREE in warnings[0] and DRIVER_TREE in warnings[0]

    def test_a_helper_that_did_not_answer_warns_and_queues(
        self, cfg: config.Config, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A timeout is 'not known', not 'different': the job still queues."""
        _answer(monkeypatch, _timed_out())
        warnings: list[str] = []
        submit_module._refuse_stale_scheduler_helper(
            cfg, "host_f", allow_mismatch=False, warning_sink=warnings.append,
        )
        assert len(warnings) == 1
        assert "not verified" in warnings[0]
        assert "timed out" in warnings[0]

    def test_a_match_is_silent(
        self, cfg: config.Config, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _answer(monkeypatch, _match())
        warnings: list[str] = []
        submit_module._refuse_stale_scheduler_helper(
            cfg, "host_f", allow_mismatch=False, warning_sink=warnings.append,
        )
        assert warnings == []


class TestDoctorWiring:
    def test_the_predicate_reads_only_the_two_mismatch_verdicts(self) -> None:
        assert doctor.helper_identity_mismatch(_tree_mismatch())
        assert doctor.helper_identity_mismatch(_sha_mismatch())
        assert not doctor.helper_identity_mismatch(_timed_out())
        assert not doctor.helper_identity_mismatch(_match())
        assert not doctor.helper_identity_mismatch(
            doctor.check("scheduler_remote_vq", False, "vq failed on host_f-login (exit 1)")
        )

    def test_the_real_doctor_check_drives_the_refusal(
        self, strict_cfg: config.Config, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """No stubbed verdict: the helper answers over the remote-vq transport."""
        monkeypatch.setattr(
            doctor,
            "_driver_identity_probe",
            lambda kind, _deadline: (
                (DRIVER_SHA, None) if kind == "source_sha" else (DRIVER_TREE, None)
            ),
        )
        calls: list[tuple[str, tuple[str, ...]]] = []

        def fake_remote_vq(host_cfg, *args, **kwargs):  # type: ignore[no-untyped-def]
            calls.append((host_cfg.ssh, args))
            if args == ("--version",):
                return subprocess.CompletedProcess(
                    args=["vq"], returncode=0,
                    stdout="vq, version 0.26.15 \"Raymond's Bazaar\"\n", stderr="",
                )
            assert args == ("source-identity",), args
            return subprocess.CompletedProcess(
                args=["vq"], returncode=0,
                stdout=json.dumps(
                    {"source_tree_sha256": HELPER_TREE, "source_sha": HELPER_SHA}
                ),
                stderr="",
            )

        monkeypatch.setattr(transport, "run_remote_vq", fake_remote_vq)
        with pytest.raises(ValueError, match=doctor.HELPER_TREE_MISMATCH) as exc:
            submit_module._refuse_stale_scheduler_helper(
                strict_cfg, "host_f", allow_mismatch=False, warning_sink=None,
            )
        assert HELPER_TREE in str(exc.value) and DRIVER_TREE in str(exc.value)
        assert [c[0] for c in calls] == ["host_f-login", "host_f-login"]

    def test_the_probe_uses_the_fleet_check_budget(
        self, cli_state: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        (cli_state / "cfg" / "config.toml").write_text(
            LOCAL_DRIVER_TOML + "\n[fleet]\ncheck_timeout_seconds = 3.5\n",
            encoding="utf-8",
        )
        cfg = config.load_config()
        seen: dict[str, object] = {}

        def fake_check(host_cfg, *, host_label, check_timeout):  # type: ignore[no-untyped-def]
            seen.update(ssh=host_cfg.ssh, label=host_label, budget=check_timeout)
            return _match()

        monkeypatch.setattr(doctor, "scheduler_remote_vq_check", fake_check)
        submit_module._refuse_stale_scheduler_helper(
            cfg, "host_f", allow_mismatch=False, warning_sink=None,
        )
        assert seen == {"ssh": "host_f-login", "label": "host_f", "budget": 3.5}


class TestCli:
    def test_by_default_a_mismatch_queues_with_a_warning(
        self, cli_state: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _answer(monkeypatch, _tree_mismatch())
        executed: list[object] = []
        monkeypatch.setattr(
            submit_module, "_execute_submit_plan",
            lambda *a, **k: executed.append(a) or ["ab0000000001"],
        )
        result = CliRunner().invoke(
            main, ["submit", "host_f", "-d", str(cli_state / "payload"), "--", "true"],
        )
        assert result.exit_code == 0, result.output
        assert "warn-only by default" in result.output
        assert HELPER_TREE in result.output and DRIVER_TREE in result.output
        assert len(executed) == 1

    def test_a_mismatch_refuses_before_anything_is_queued(
        self, cli_state: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        (cli_state / "cfg" / "config.toml").write_text(STRICT_TOML, encoding="utf-8")
        _answer(monkeypatch, _tree_mismatch())
        executed: list[object] = []
        monkeypatch.setattr(
            submit_module, "_execute_submit_plan",
            lambda *a, **k: executed.append(a) or ["ab0000000001"],
        )
        result = CliRunner().invoke(
            main, ["submit", "host_f", "-d", str(cli_state / "payload"), "--", "true"],
        )
        assert result.exit_code == 2, result.output
        assert "does not match this driver" in result.output
        assert HELPER_TREE in result.output and DRIVER_TREE in result.output
        assert executed == []

    def test_the_flag_queues_and_prints_the_warning(
        self, cli_state: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _answer(monkeypatch, _tree_mismatch())
        monkeypatch.setattr(
            submit_module, "_execute_submit_plan", lambda *a, **k: ["ab0000000001"],
        )
        result = CliRunner().invoke(
            main,
            [
                "submit", "host_f", "-d", str(cli_state / "payload"),
                "--allow-helper-mismatch", "--", "true",
            ],
        )
        assert result.exit_code == 0, result.output
        assert result.stdout.strip() == "ab0000000001"
        assert "--allow-helper-mismatch" in result.stderr
        assert HELPER_TREE in result.stderr

    def test_a_remote_driver_is_not_probed_from_the_client(
        self, cli_state: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The client cannot see the helper; the driver checks on arrival."""
        (cli_state / "cfg" / "config.toml").write_text(
            LOCAL_DRIVER_TOML.replace(
                'scheduler_driver = "localhost"', 'scheduler_driver = "driver"',
            )
            + '\n[hosts.driver]\nssh = "queue-driver"\n',
            encoding="utf-8",
        )

        def never(cfg: config.Config, scheduler_host: str) -> dict[str, object]:
            raise AssertionError("the client must not probe the helper")

        monkeypatch.setattr(submit_module, "_probe_scheduler_helper", never)
        monkeypatch.setattr(
            submit_module, "_execute_submit_plan", lambda *a, **k: ["ab0000000001"],
        )
        result = CliRunner().invoke(
            main, ["submit", "host_f", "-d", str(cli_state / "payload"), "--", "true"],
        )
        assert result.exit_code == 0, result.output


class TestRelayWire:
    def _argv(self, **overrides: object) -> list[str]:
        base: dict[str, object] = {
            "remote_tar": "/home/USER/.vq/stage/x.tar", "remote_input_file": None,
            "remote_command": ["bash", "run.sh"], "cpus": 2, "scheduler_tasks": None,
            "mem_mb": None, "wall_time_seconds": None, "priority": 0,
            "auto_resume": False, "retry": 0, "job_name": None, "branch": None,
            "program": None, "expected_sha": None, "tags": None, "not_before": None,
            "depends_on": None, "depends_on_any": None,
            "clean_workdir_on_terminal": False, "array": 1, "chain": 1,
            "rerun_until_file_exists": None, "rerun_max": 10, "refresh_before": None,
            "scheduler_target": "host_f", "qvf_force": False,
        }
        base.update(overrides)
        return submit_module._build_remote_submit_argv(**base)  # type: ignore[arg-type]

    def test_the_flag_is_on_the_wire_only_when_set(self) -> None:
        """An older driver would refuse an option it lacks: default is silent."""
        plain = self._argv()
        assert "--allow-helper-mismatch" not in plain
        with_flag = self._argv(allow_helper_mismatch=True)
        assert with_flag.count("--allow-helper-mismatch") == 1
        assert [t for t in with_flag if t != "--allow-helper-mismatch"] == plain
        # Before the payload separator, where the driver parses options.
        assert with_flag.index("--allow-helper-mismatch") < with_flag.index("--")

    def test_the_plan_executor_forwards_the_flag_to_submit_remote(
        self, cli_state: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        (cli_state / "cfg" / "config.toml").write_text(
            LOCAL_DRIVER_TOML.replace(
                'scheduler_driver = "localhost"', 'scheduler_driver = "driver"',
            )
            + '\n[hosts.driver]\nssh = "queue-driver"\n',
            encoding="utf-8",
        )
        cfg = config.load_config()
        captured: list[dict[str, object]] = []
        monkeypatch.setattr(
            submit_module, "submit_remote",
            lambda **kwargs: captured.append(kwargs) or ["ab0000000001"],
        )
        target = submit_module._ResolvedSubmitExecutionTarget(
            requested_host="host_f", receipt_host="host_f", execution_host="driver",
            execution_host_config=cfg.host("driver"), scheduler_target="host_f",
        )

        def plan(allow: bool) -> submit_module._ResolvedSubmitPlan:
            request = submit_module._NormalizedSubmitRequest(
                input_file=None, directory=str(cli_state / "payload"), archive=None,
                command=["bash", "run.sh"], is_qvf_input=False, python=None, cpus=1,
                scheduler_tasks=None, mem_mb=None, wall_time_seconds=None,
                priority=0, auto_resume=False, retry=0, job_name=None, branch=None,
                program=None, expected_sha=None, tags=None, not_before=None,
                depends_on=None, depends_on_any=None, rerun_until_file_exists=None,
                rerun_max=10, clean_workdir_on_terminal=False,
                vibeqc_preflight=False, array=1, chain=1, refresh_before=None,
                qvf_force=False, allow_helper_mismatch=allow,
            )
            return submit_module._ResolvedSubmitPlan(
                target=target, request=request,
                program_runtime_pin=None, receipt_runtime_pin=None,
            )

        submit_module._execute_submit_plan(plan(False), multi_user=False)
        submit_module._execute_submit_plan(plan(True), multi_user=False)
        assert "allow_helper_mismatch" not in captured[0]
        assert captured[1]["allow_helper_mismatch"] is True
