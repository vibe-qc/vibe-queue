"""Unit tests for scheduler-dialect detection (vq/scheduler_probe.py).

The classification is exercised against canned ``qstat --version`` output and a
canned binary-presence sweep, with no SSH or cluster (the real transport path is
validated separately against a live login node). The Torque facts mirror the
*host_f* cluster: ``qstat --version`` prints a bare ``version: 2.5.12`` there.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from vq.scheduler_dispatch import RemoteResult
from vq.scheduler_probe import format_report, probe, to_json_dict


@dataclass
class FakeRunner:
    """Canned scheduler host: a binary set + a ``qstat --version`` string."""

    binaries: list[str] = field(default_factory=list)
    version_stdout: str = ""
    version_stderr: str = ""
    sbatch_version_stdout: str = ""
    sbatch_version_stderr: str = ""
    server_stdout: str = "server_state = Active\n"
    queues_stdout: str = "Queue: batch\n    enabled = True\n    started = True\n"
    daemon_probe_returncode: int = 0
    daemon_probe_stdout: str = "pbs_sched\n"
    squeue_returncode: int = 0
    squeue_stdout: str = ""
    squeue_stderr: str = ""

    def run(
        self,
        argv: list[str] | tuple[str, ...],
        *,
        stdin_data: str | None = None,
        check: bool = False,
    ) -> RemoteResult:
        argv = list(argv)
        if argv[:2] == ["qstat", "--version"]:
            return RemoteResult(0, self.version_stdout, self.version_stderr)
        if argv[:2] == ["sbatch", "--version"]:
            return RemoteResult(0, self.sbatch_version_stdout, self.sbatch_version_stderr)
        if argv[:2] == ["qstat", "-Bf"]:
            return RemoteResult(0, self.server_stdout, "")
        if argv[:2] == ["qstat", "-Qf"]:
            return RemoteResult(0, self.queues_stdout, "")
        if argv[:2] == ["sh", "-c"] and "pgrep -x" in argv[2]:
            return RemoteResult(self.daemon_probe_returncode, self.daemon_probe_stdout, "")
        if argv and argv[0] == "squeue":
            return RemoteResult(
                self.squeue_returncode,
                self.squeue_stdout,
                self.squeue_stderr,
            )
        if argv and argv[0] == "sh":  # the command -v sweep
            return RemoteResult(0, "\n".join(self.binaries) + "\n", "")
        return RemoteResult(0, "", "")


_PBS_BINS = ["qsub", "qstat", "qdel", "qhold", "qrls", "pbsnodes", "qmgr"]
_SLURM_BINS = ["sbatch", "squeue", "scancel", "sacct", "scontrol"]


def test_detects_torque_confirmed() -> None:
    r = probe(FakeRunner(binaries=_PBS_BINS, version_stdout="version: 2.5.12\n"))
    assert r.dialect == "torque"
    assert r.scheduler == "pbs"
    assert r.version == "2.5.12"
    assert r.confidence == "confirmed"
    assert r.binaries["pbsnodes"] is True
    assert r.binaries["qhost"] is False


def test_torque_version_on_stderr_is_still_detected() -> None:
    # Many builds print --version to stderr; both streams are considered.
    r = probe(FakeRunner(binaries=_PBS_BINS, version_stderr="version: 6.1.0\n"))
    assert r.dialect == "torque"
    assert r.version == "6.1.0"


def test_pbspro_is_likely_not_confirmed() -> None:
    r = probe(FakeRunner(binaries=_PBS_BINS, version_stdout="pbs_version = 19.1.3\n"))
    assert r.dialect == "pbspro"
    assert r.scheduler == "pbs"
    assert r.version == "19.1.3"
    assert r.confidence == "likely"
    assert any("torque vs pbspro" in n for n in r.notes)


def test_detects_sge_family() -> None:
    r = probe(FakeRunner(binaries=["qsub", "qstat", "qhost", "qconf"], version_stdout="GE 8.6.0\n"))
    assert r.dialect == "sge"
    assert r.scheduler == "sge"
    assert r.confidence == "likely"
    assert any("not yet implemented" in n for n in r.notes)


def test_detects_slurm_confirmed() -> None:
    r = probe(
        FakeRunner(
            binaries=_SLURM_BINS,
            sbatch_version_stdout="slurm 24.05.3\n",
        )
    )
    assert r.dialect == "slurm"
    assert r.scheduler == "slurm"
    assert r.version == "24.05.3"
    assert r.confidence == "confirmed"
    assert r.binaries["sbatch"] is True
    assert r.binaries["qsub"] is False
    assert r.slurm_squeue_ok is True
    assert r.slurm_squeue_error is None


def test_slurm_probe_reports_squeue_liveness_failure() -> None:
    r = probe(
        FakeRunner(
            binaries=_SLURM_BINS,
            sbatch_version_stdout="slurm 24.05.3\n",
            squeue_returncode=1,
            squeue_stderr="slurm_load_jobs error: Socket timed out",
        )
    )

    assert r.dialect == "slurm"
    assert r.slurm_squeue_ok is False
    assert r.slurm_squeue_error == "slurm_load_jobs error: Socket timed out"
    report = format_report(r, "host_c")
    assert "SLURM squeue: failed" in report
    assert "Socket timed out" in report


def test_unknown_when_no_tells() -> None:
    r = probe(FakeRunner(binaries=["qsub", "qstat"], version_stdout="weird output\n"))
    assert r.dialect is None
    assert r.scheduler is None
    assert r.confidence == "unknown"
    assert any("set scheduler_dialect by hand" in n for n in r.notes)


def test_flags_missing_scheduler_binaries() -> None:
    r = probe(FakeRunner(binaries=[], version_stdout=""))
    assert r.dialect is None
    assert any("scheduler host" in n for n in r.notes)


def test_json_payload_is_stable_dict() -> None:
    r = probe(FakeRunner(binaries=_PBS_BINS, version_stdout="version: 2.5.12\n"))
    payload = to_json_dict(r)
    assert payload["dialect"] == "torque"
    assert payload["scheduler"] == "pbs"
    assert payload["binaries"]["qsub"] is True  # type: ignore[index]
    assert payload["binaries"]["qhold"] is True  # type: ignore[index]
    assert payload["binaries"]["qrls"] is True  # type: ignore[index]
    assert payload["server_state"] == "Active"
    assert payload["pbs_sched_running"] is True
    assert payload["scheduler_daemons"] == ["pbs_sched"]
    assert payload["queues"] == [
        {"name": "batch", "enabled": True, "started": True}
    ]
    assert payload["slurm_squeue_ok"] is None
    assert payload["slurm_squeue_error"] is None
    assert isinstance(payload["notes"], list)


def test_pbs_liveness_reports_stopped_scheduler_and_queues() -> None:
    r = probe(
        FakeRunner(
            binaries=_PBS_BINS,
            version_stdout="version: 2.5.12\n",
            server_stdout="server_state = Idle\n",
            queues_stdout=(
                "Queue: host_f-big\n"
                "    enabled = True\n"
                "    started = False\n"
                "Queue: host_f-jtwin\n"
                "    enabled = True\n"
                "    started = False\n"
            ),
            daemon_probe_stdout="",
        )
    )

    assert r.server_state == "Idle"
    assert r.pbs_sched_running is False
    assert r.scheduler_daemons == ()
    assert [q.name for q in r.queues] == ["host_f-big", "host_f-jtwin"]
    assert all(q.enabled is True for q in r.queues)
    assert all(q.started is False for q in r.queues)
    report = format_report(r, "host_f")
    assert "pbs_sched: not running" in report
    assert "scheduler daemons: (none found)" in report
    assert "started=False queue(s): host_f-big, host_f-jtwin" in report


def test_pbs_liveness_accepts_maui_without_pbs_sched() -> None:
    # Torque + Maui: pbs_sched is intentionally absent and pbs_server reports
    # server_state=Idle although the external scheduler dispatches jobs.
    r = probe(
        FakeRunner(
            binaries=_PBS_BINS,
            version_stdout="version: 2.5.12\n",
            server_stdout="server_state = Idle\n",
            daemon_probe_stdout="maui\n",
        )
    )

    assert r.server_state == "Idle"
    assert r.pbs_sched_running is False
    assert r.scheduler_daemons == ("maui",)
    report = format_report(r, "host_f")
    assert "scheduler daemons: maui" in report


def test_pbs_liveness_daemon_probe_failure_is_inconclusive() -> None:
    r = probe(
        FakeRunner(
            binaries=_PBS_BINS,
            version_stdout="version: 2.5.12\n",
            daemon_probe_returncode=127,
        )
    )
    assert r.pbs_sched_running is None
    assert r.scheduler_daemons is None
    assert any("scheduler daemon process probe failed" in n for n in r.notes)


def test_torque_needs_pbsnodes_to_confirm() -> None:
    # The bare "version:" tell without pbsnodes is not enough to confirm torque.
    r = probe(FakeRunner(binaries=["qsub", "qstat"], version_stdout="version: 2.5.12\n"))
    assert r.dialect != "torque"


@pytest.mark.parametrize("present", [True, False])
def test_report_renders_config_for_detected_dialect(present: bool) -> None:
    binaries = _PBS_BINS if present else []
    version = "version: 2.5.12\n" if present else "??\n"
    report = format_report(probe(FakeRunner(binaries=binaries, version_stdout=version)), "host_f")
    assert report.startswith("scheduler-probe host_f:")
    if present:
        assert 'scheduler_dialect = "torque"' in report
        assert "qsub" in report
    else:
        assert "UNKNOWN" in report
