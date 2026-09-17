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
    pbsnodes_returncode: int = 0
    pbsnodes_stdout: str = ""
    pbsnodes_stderr: str = ""
    sinfo_nodes_returncode: int = 0
    sinfo_nodes_stdout: str = ""
    sinfo_nodes_stderr: str = ""

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
        if argv[:2] == ["pbsnodes", "-a"]:
            return RemoteResult(
                self.pbsnodes_returncode,
                self.pbsnodes_stdout,
                self.pbsnodes_stderr,
            )
        if argv and argv[0] == "sinfo" and "--Node" in argv:
            return RemoteResult(
                self.sinfo_nodes_returncode,
                self.sinfo_nodes_stdout,
                self.sinfo_nodes_stderr,
            )
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


# --- schedulable capacity (vibe-qc#148, queue tracker #23) -------------------
#
# The cluster state the incident was measured on: two 128-wide nodes, one busy
# and one down, so a ppn=128 request was admitted and then never started.

_WIDE_NARROW_PBSNODES = """\
node01
     state = free
     np = 64
     properties = wide
     ntype = cluster

node02
     state = job-exclusive
     np = 128
     properties = wide
     jobs = 0-127/12300.host_f
     ntype = cluster

node03
     state = down,offline
     np = 128
     properties = wide
     ntype = cluster

node20
     state = free
     np = 20
     properties = narrow
     jobs = 0/12301.host_f, 1/12301.host_f
"""


def _torque(**kwargs: object) -> FakeRunner:
    return FakeRunner(
        binaries=_PBS_BINS, version_stdout="version: 2.5.12\n", **kwargs
    )


def test_pbs_capacity_separates_what_can_start_from_what_could_ever_start() -> None:
    capacity = probe(_torque(pbsnodes_stdout=_WIDE_NARROW_PBSNODES)).capacity
    assert capacity is not None
    # A ppn=128 job cannot start: the only free node is 64 wide.
    assert capacity.max_cpus_now == 64
    # But it is not impossible -- node02 is 128 wide and merely busy.
    assert capacity.max_cpus_when_free == 128
    assert (capacity.usable_nodes, capacity.unusable_nodes) == (3, 1)


def test_pbs_capacity_counts_free_cores_on_a_partly_used_node() -> None:
    result = probe(_torque(pbsnodes_stdout=_WIDE_NARROW_PBSNODES))
    nodes = {n.name: n for n in result.capacity.nodes}
    assert nodes["node20"].free_cpus == 18  # np=20, two cores taken
    assert nodes["node02"].free_cpus == 0  # the 0-127 range is the whole node
    assert nodes["node01"].free_cpus == 64


def test_pbs_capacity_reports_a_down_node_as_unusable_not_merely_busy() -> None:
    result = probe(_torque(pbsnodes_stdout=_WIDE_NARROW_PBSNODES))
    nodes = {n.name: n for n in result.capacity.nodes}
    assert nodes["node03"].usable is False
    assert nodes["node03"].free_cpus == 0
    assert nodes["node03"].total_cpus == 128
    # Busy is not unusable: waiting fixes one and not the other.
    assert nodes["node02"].usable is True


def test_pbs_capacity_groups_by_node_property() -> None:
    result = probe(_torque(pbsnodes_stdout=_WIDE_NARROW_PBSNODES))
    groups = {g.name: g for g in result.capacity.groups}
    assert groups["wide"].max_cpus_now == 64
    assert groups["wide"].max_cpus_when_free == 128
    assert groups["wide"].unusable_nodes == 1
    assert groups["narrow"].max_cpus_now == 18


def test_report_warns_when_a_width_can_never_be_scheduled() -> None:
    # Both 128-wide nodes down: a ppn=128 request now queues forever, which
    # is the state no vq output could express when #148 was filed.
    pbsnodes = _WIDE_NARROW_PBSNODES.replace(
        "node02\n     state = job-exclusive", "node02\n     state = down"
    )
    result = probe(_torque(pbsnodes_stdout=pbsnodes))
    assert result.capacity.max_cpus_when_free == 64
    report = format_report(result, "host_f")
    assert "queue forever" in report
    assert "128 cpu" in report


def test_report_does_not_warn_while_the_width_is_merely_busy() -> None:
    report = format_report(probe(_torque(pbsnodes_stdout=_WIDE_NARROW_PBSNODES)), "host_f")
    assert "queue forever" not in report
    assert "largest job that can start now: 64" in report


def test_capacity_failure_makes_no_claim() -> None:
    # An unreadable census must never read as "nothing is free".
    result = probe(
        _torque(pbsnodes_returncode=127, pbsnodes_stderr="pbsnodes: not found")
    )
    assert result.capacity.error == "pbsnodes: not found"
    assert result.capacity.max_cpus_now is None
    assert result.capacity.max_cpus_when_free is None
    assert result.capacity.nodes == ()
    assert "capacity: unavailable" in format_report(result, "host_f")


def test_slurm_capacity_reads_idle_cores_and_partitions() -> None:
    runner = FakeRunner(
        binaries=[*_SLURM_BINS, "sinfo"],
        sbatch_version_stdout="slurm 23.02.7\n",
        sinfo_nodes_stdout=(
            "node001|wide*|idle|128|0/128/0/128\n"
            "node002|wide|mix|128|64/64/0/128\n"
            "node003|wide|drain|128|0/0/128/128\n"
            "node004|narrow|alloc|64|64/0/0/64\n"
        ),
    )
    capacity = probe(runner).capacity
    assert capacity is not None
    assert capacity.max_cpus_now == 128
    assert (capacity.usable_nodes, capacity.unusable_nodes) == (3, 1)
    nodes = {n.name: n for n in capacity.nodes}
    assert nodes["node002"].free_cpus == 64  # the idle field of A/I/O/T
    assert nodes["node003"].usable is False  # drained, not merely busy
    assert nodes["node004"].free_cpus == 0
    groups = {g.name: g for g in capacity.groups}
    assert groups["wide"].max_cpus_now == 128  # the default-partition * is stripped
    assert groups["narrow"].max_cpus_now == 0


def test_capacity_is_absent_when_no_census_binary_exists() -> None:
    # SGE ships neither pbsnodes nor sinfo: no census was attempted, which is
    # not the same as one that failed.
    result = probe(FakeRunner(binaries=["qhost", "qconf"], version_stdout="8.1.9\n"))
    assert result.capacity is None
    assert "capacity" not in format_report(result, "gridhost")


def test_capacity_json_shape_is_serializable() -> None:
    payload = to_json_dict(probe(_torque(pbsnodes_stdout=_WIDE_NARROW_PBSNODES)))
    assert payload["capacity"]["max_cpus_now"] == 64
    assert payload["capacity"]["max_cpus_when_free"] == 128
    assert {n["name"] for n in payload["capacity"]["nodes"]} == {
        "node01", "node02", "node03", "node20",
    }
    assert {g["name"] for g in payload["capacity"]["groups"]} == {"wide", "narrow"}
    import json

    json.dumps(payload)  # the --json surface must survive serialization
