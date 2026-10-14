"""PBS lane authority: what says a node property is a scheduling lane (#23).

Issue #23 closed with one question left to the maintainer -- on PBS a lane is a
node *property*, not a queue, and which site attribute is authoritative was
their call. The decision: a queue's ``resources_default.neednodes`` is
authoritative for the lane that queue routes to; where no queue declares one,
grouping by node property stands as an inference and must be marked as such.

Everything here runs against canned ``pbsnodes -a`` / ``qstat -Qf`` output
through a fake runner. No host, no scheduler, no SSH -- and the reads the
production path makes (``pbsnodes -a``, ``qstat -Qf``) are read-only on a real
cluster too, which is why they can be trusted on a shared one.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field

from vq import scheduler_probe
from vq.scheduler_probe import (
    LANE_AUTHORITY_NODE_PROPERTY,
    LANE_AUTHORITY_PARTITION,
    LANE_AUTHORITY_QUEUE_DEFAULT,
    format_report,
    probe,
    to_json_dict,
)

# Two 128-wide nodes -- one busy, one down -- plus a 64-wide node that carries
# *two* properties and a narrow node, which is the shape #23 was measured on
# with the multi-property case the old first-property guess could not express.
_PBSNODES = """\
node01
     state = free
     np = 64
     properties = wide,gpu
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

# `batch` routes to the `wide` property, `gpu` to `gpu` through the fuller
# `count:property:ppn=` spelling. Nothing routes to `narrow`.
_QUEUES_WITH_NEEDNODES = """\
Queue: batch
    queue_type = Execution
    resources_default.neednodes = wide
    resources_default.walltime = 24:00:00
    enabled = True
    started = True

Queue: gpu
    queue_type = Execution
    resources_default.neednodes = 1:gpu:ppn=64
    enabled = True
    started = True
"""

# The pre-decision shape: queues exist, none of them says where it routes.
_QUEUES_WITHOUT_NEEDNODES = """\
Queue: batch
    queue_type = Execution
    resources_default.walltime = 24:00:00
    enabled = True
    started = True
"""

_PBS_BINS = ["qsub", "qstat", "qdel", "qhold", "qrls", "pbsnodes", "qmgr"]
_SLURM_BINS = ["sbatch", "squeue", "scancel", "sacct", "scontrol", "sinfo"]


@dataclass
class FakeRunner:
    """Canned PBS/SLURM host that records every command it was asked to run."""

    binaries: list[str] = field(default_factory=list)
    version_stdout: str = "version: 2.5.12\n"
    sbatch_version_stdout: str = ""
    queues_stdout: str = _QUEUES_WITH_NEEDNODES
    queues_returncode: int = 0
    queues_stderr: str = ""
    pbsnodes_stdout: str = _PBSNODES
    sinfo_nodes_stdout: str = ""
    calls: list[tuple[str, ...]] = field(default_factory=list)

    def run(
        self,
        argv: list[str] | tuple[str, ...],
        *,
        stdin_data: str | None = None,
        check: bool = False,
    ):
        from vq.scheduler_dispatch import RemoteResult

        argv = list(argv)
        self.calls.append(tuple(argv))
        if argv[:2] == ["qstat", "--version"]:
            return RemoteResult(0, self.version_stdout, "")
        if argv[:2] == ["sbatch", "--version"]:
            return RemoteResult(0, self.sbatch_version_stdout, "")
        if argv[:2] == ["qstat", "-Bf"]:
            return RemoteResult(0, "server_state = Active\n", "")
        if argv[:2] == ["qstat", "-Qf"]:
            return RemoteResult(
                self.queues_returncode, self.queues_stdout, self.queues_stderr
            )
        if argv[:2] == ["sh", "-c"] and "pgrep -x" in argv[2]:
            return RemoteResult(0, "pbs_sched\n", "")
        if argv[:2] == ["pbsnodes", "-a"]:
            return RemoteResult(0, self.pbsnodes_stdout, "")
        if argv and argv[0] == "sinfo" and "--Node" in argv:
            return RemoteResult(0, self.sinfo_nodes_stdout, "")
        if argv and argv[0] == "sinfo":
            return RemoteResult(0, "", "")
        if argv and argv[0] == "squeue":
            return RemoteResult(0, "", "")
        if argv and argv[0] == "sh":  # the `command -v` sweep
            return RemoteResult(0, "\n".join(self.binaries) + "\n", "")
        return RemoteResult(0, "", "")


def _torque(**kwargs: object) -> FakeRunner:
    return FakeRunner(binaries=_PBS_BINS, **kwargs)


def _lanes(runner: FakeRunner) -> dict[str, object]:
    capacity = probe(runner).capacity
    assert capacity is not None
    return {group.name: group for group in capacity.groups}


# --- the decision itself ----------------------------------------------------


def test_a_queues_neednodes_is_the_authority_for_its_lane() -> None:
    lanes = _lanes(_torque())
    assert lanes["wide"].authority == LANE_AUTHORITY_QUEUE_DEFAULT
    assert lanes["wide"].queues == ("batch",)
    # Which is the whole point: a planner can now name the queue to submit to
    # for a given width, not merely a property that happens to group nodes.
    assert lanes["wide"].max_cpus_now == 64
    assert lanes["wide"].max_cpus_when_free == 128


def test_a_neednodes_spec_with_a_count_and_ppn_still_names_its_property() -> None:
    # `1:gpu:ppn=64` -- the count and the ppn term are not lanes; `gpu` is.
    lanes = _lanes(_torque())
    assert lanes["gpu"].authority == LANE_AUTHORITY_QUEUE_DEFAULT
    assert lanes["gpu"].queues == ("gpu",)


def test_a_property_no_queue_routes_to_is_marked_inferred() -> None:
    lanes = _lanes(_torque())
    assert lanes["narrow"].authority == LANE_AUTHORITY_NODE_PROPERTY
    assert lanes["narrow"].queues == ()
    # The figures are still true of the nodes carrying the property. Only the
    # claim that a queue sends work there is withheld.
    assert lanes["narrow"].max_cpus_now == 18


def test_without_any_neednodes_every_lane_falls_back_to_inference() -> None:
    lanes = _lanes(_torque(queues_stdout=_QUEUES_WITHOUT_NEEDNODES))
    assert {name: g.authority for name, g in lanes.items()} == {
        "wide": LANE_AUTHORITY_NODE_PROPERTY,
        "gpu": LANE_AUTHORITY_NODE_PROPERTY,
        "narrow": LANE_AUTHORITY_NODE_PROPERTY,
    }


def test_an_unreadable_qstat_qf_claims_no_authority_at_all() -> None:
    # A census that cannot see the queues must not promote a guess: the lanes
    # are still reported, and every one of them is marked as inferred.
    lanes = _lanes(
        _torque(queues_returncode=1, queues_stderr="qstat: cannot connect")
    )
    assert {g.authority for g in lanes.values()} == {LANE_AUTHORITY_NODE_PROPERTY}
    assert lanes["wide"].max_cpus_when_free == 128  # still a real census


def test_a_stopped_queue_still_says_where_it_routes() -> None:
    # `started = False` is an administrative hold on dispatch, reported
    # separately. It does not make the queue's own routing attribute untrue.
    stopped = _QUEUES_WITH_NEEDNODES.replace(
        "resources_default.neednodes = wide\n"
        "    resources_default.walltime = 24:00:00\n"
        "    enabled = True\n"
        "    started = True",
        "resources_default.neednodes = wide\n"
        "    resources_default.walltime = 24:00:00\n"
        "    enabled = True\n"
        "    started = False",
    )
    result = probe(_torque(queues_stdout=stopped))
    lanes = {g.name: g for g in result.capacity.groups}
    assert lanes["wide"].queues == ("batch",)
    assert "started=False queue(s): batch" in format_report(result, "host_f")


def test_two_queues_routing_to_one_lane_are_both_named() -> None:
    both = _QUEUES_WITH_NEEDNODES + (
        "\nQueue: long\n"
        "    queue_type = Execution\n"
        "    resources_default.neednodes = wide\n"
        "    enabled = True\n"
        "    started = True\n"
    )
    assert _lanes(_torque(queues_stdout=both))["wide"].queues == ("batch", "long")


def test_a_neednodes_naming_no_known_property_contributes_no_lane() -> None:
    # Some sites pin a queue to a host name rather than a property. vq cannot
    # tell the two apart syntactically, so the census decides: a token no node
    # declares is simply not a lane. An empty group would read as *unknown*
    # capacity, which is worse than not claiming a lane at all.
    only_host = _QUEUES_WITH_NEEDNODES.replace(
        "resources_default.neednodes = wide", "resources_default.neednodes = node01"
    )
    lanes = _lanes(_torque(queues_stdout=only_host))
    assert "node01" not in lanes
    assert lanes["wide"].authority == LANE_AUTHORITY_NODE_PROPERTY


# --- a node property is not a lane on its own -------------------------------


def test_a_node_counts_in_every_property_it_carries() -> None:
    # node01 is `wide,gpu`. The first-property guess put it in `wide` alone,
    # so the `gpu` lane did not exist at all and its capacity was invisible.
    lanes = _lanes(_torque())
    assert lanes["gpu"].usable_nodes == 1
    assert lanes["gpu"].max_cpus_when_free == 64
    assert lanes["wide"].usable_nodes == 2  # node01 and the busy node02


def test_property_order_does_not_decide_the_lane() -> None:
    reversed_props = _PBSNODES.replace(
        "properties = wide,gpu", "properties = gpu,wide"
    )
    as_written = _lanes(_torque())
    as_reversed = _lanes(_torque(pbsnodes_stdout=reversed_props))
    assert {name: (g.max_cpus_now, g.max_cpus_when_free, g.usable_nodes)
            for name, g in as_written.items()} == {
        name: (g.max_cpus_now, g.max_cpus_when_free, g.usable_nodes)
        for name, g in as_reversed.items()
    }


def test_every_property_is_listed_on_the_node() -> None:
    nodes = {n.name: n for n in probe(_torque()).capacity.nodes}
    assert nodes["node01"].lanes == ("wide", "gpu")
    assert nodes["node20"].lanes == ("narrow",)


def test_a_node_with_no_properties_is_in_no_lane() -> None:
    bare = _PBSNODES.replace("     properties = narrow\n", "")
    nodes = {n.name: n for n in probe(_torque(pbsnodes_stdout=bare)).capacity.nodes}
    assert nodes["node20"].lanes == ()
    assert "narrow" not in _lanes(_torque(pbsnodes_stdout=bare))


# --- the census costs no extra scheduler round trip -------------------------


def test_the_queue_read_is_not_paid_for_twice() -> None:
    runner = _torque()
    probe(runner)
    assert runner.calls.count(("qstat", "-Qf")) == 1
    assert runner.calls.count(("pbsnodes", "-a")) == 1


# --- SLURM is untouched: a partition IS the lane ----------------------------


_SINFO = (
    "node001|wide*|idle|128|0/128/0/128\n"
    "node002|wide|mix|128|64/64/0/128\n"
    "node003|wide|drain|128|0/0/128/128\n"
    "node004|narrow|alloc|64|64/0/0/64\n"
)


def _slurm() -> FakeRunner:
    return FakeRunner(
        binaries=_SLURM_BINS,
        sbatch_version_stdout="slurm 23.02.7\n",
        sinfo_nodes_stdout=_SINFO,
    )


def test_slurm_lanes_are_authoritative_partitions_not_inferences() -> None:
    lanes = _lanes(_slurm())
    assert {g.authority for g in lanes.values()} == {LANE_AUTHORITY_PARTITION}
    assert all(g.queues == () for g in lanes.values())


def test_slurm_capacity_figures_are_unchanged() -> None:
    capacity = probe(_slurm()).capacity
    assert capacity.max_cpus_now == 128
    assert (capacity.usable_nodes, capacity.unusable_nodes) == (3, 1)
    lanes = {g.name: g for g in capacity.groups}
    assert lanes["wide"].max_cpus_now == 128  # the default-partition * is stripped
    assert lanes["narrow"].max_cpus_now == 0
    nodes = {n.name: n for n in capacity.nodes}
    assert nodes["node001"].lanes == ("wide",)
    assert nodes["node003"].usable is False


def test_the_slurm_report_marks_nothing_as_inferred() -> None:
    report = format_report(probe(_slurm()), "slurm-cluster")
    assert "[inferred]" not in report
    assert "[queue:" not in report


# --- human output -----------------------------------------------------------


def test_the_report_names_the_routing_queue_and_flags_the_inferred_lane() -> None:
    report = format_report(probe(_torque()), "host_f")
    assert "wide: now=64 when-free=128 (2 usable, 1 unusable) [queue: batch]" in report
    assert "narrow: now=18 when-free=20 (1 usable, 0 unusable) [inferred]" in report
    # The marker explains itself, once, and only where it is used.
    assert (
        "[inferred]: a node property no queue's "
        "resources_default.neednodes names" in report
    )


def test_the_inferred_legend_is_absent_when_every_lane_is_declared() -> None:
    declared = _QUEUES_WITH_NEEDNODES + (
        "\nQueue: small\n"
        "    queue_type = Execution\n"
        "    resources_default.neednodes = narrow\n"
        "    enabled = True\n"
        "    started = True\n"
    )
    report = format_report(probe(_torque(queues_stdout=declared)), "host_f")
    assert "[inferred]" not in report
    assert "[queue: small]" in report


# --- the machine contract ---------------------------------------------------


def test_json_carries_the_authority_and_the_routing_queues() -> None:
    payload = to_json_dict(probe(_torque()))
    groups = {g["name"]: g for g in payload["capacity"]["groups"]}
    assert groups["wide"]["authority"] == "queue_default"
    assert groups["wide"]["queues"] == ["batch"]
    assert groups["narrow"]["authority"] == "node_property"
    assert groups["narrow"]["queues"] == []
    json.dumps(payload)  # the --json surface must survive serialization


def test_json_nodes_list_every_lane_and_no_single_group_key() -> None:
    payload = to_json_dict(probe(_torque()))
    nodes = {n["name"]: n for n in payload["capacity"]["nodes"]}
    assert nodes["node01"]["lanes"] == ["wide", "gpu"]
    # The replaced guess must not survive as a field a consumer could still
    # read as "the" lane.
    assert "group" not in nodes["node01"]


def test_json_carries_the_raw_queue_attribute_for_auditing() -> None:
    payload = to_json_dict(probe(_torque()))
    queues = {q["name"]: q for q in payload["queues"]}
    assert queues["batch"]["default_neednodes"] == "wide"
    assert queues["gpu"]["default_neednodes"] == "1:gpu:ppn=64"


def test_json_queue_attribute_is_null_when_the_queue_sets_none() -> None:
    payload = to_json_dict(probe(_torque(queues_stdout=_QUEUES_WITHOUT_NEEDNODES)))
    assert payload["queues"][0]["default_neednodes"] is None


def test_slurm_json_says_the_partition_is_the_authority() -> None:
    payload = to_json_dict(probe(_slurm()))
    assert {g["authority"] for g in payload["capacity"]["groups"]} == {"partition"}


def test_the_payload_names_the_schema_a_consumer_can_pin() -> None:
    """Without it a shape change reaches a consumer as an absent key (#83).

    This payload is published as an agent contract and had no version, while
    its neighbours `vq.fetch-manifest.v1` and `vq.scheduler-resource-usage.v1`
    did. Replacing `capacity.nodes[].group` with `lanes[]` was exactly the
    change a consumer had no way to detect.
    """
    for report in (probe(_torque()), probe(_slurm())):
        payload = to_json_dict(report)
        assert payload["schema"] == scheduler_probe.PROBE_SCHEMA
        assert payload["schema"] == "vq.scheduler-probe.v1"
        json.dumps(payload)


class TestAFoldedQstatLineDoesNotTruncateTheAttribution:
    """`qstat -f` wraps a long value onto a tab-indented continuation line.

    The fold is byte-exact and can fall anywhere, including mid-word. Parsing
    a line at a time truncates the value there, which costs the real lane its
    `queue_default` attribution -- it silently reports as inferred -- and
    turns the partial word into a lane of its own. The raw attribute is also
    published as "verbatim, auditable against the scheduler's own words", and
    a truncated value is not that.
    """

    FOLDED = (
        "Queue: gpu\n"
        "    queue_type = Execution\n"
        "    resources_default.neednodes = wide_partition_with_a_long_name:g\n"
        "\tpu_accelerated\n"
        "    enabled = True\n"
        "    started = True\n"
    )

    def test_the_value_is_rejoined_before_it_is_read(self) -> None:
        queue = scheduler_probe._parse_queue_liveness(self.FOLDED)[0]
        assert queue.default_neednodes == (
            "wide_partition_with_a_long_name:gpu_accelerated"
        )
        assert queue.enabled is True and queue.started is True

    def test_the_lane_keeps_its_whole_name(self) -> None:
        queue = scheduler_probe._parse_queue_liveness(self.FOLDED)[0]
        assert scheduler_probe._neednodes_lanes(queue.default_neednodes or "") == (
            "wide_partition_with_a_long_name",
            "gpu_accelerated",
        )

    def test_unfolding_leaves_unfolded_output_byte_identical(self) -> None:
        plain = "Queue: a\n    enabled = True\n    started = False\n"
        assert scheduler_probe._unfold_qstat(plain) == plain

    def test_a_space_indented_line_is_an_attribute_not_a_continuation(self) -> None:
        """qstat indents ordinary attributes with spaces; only a tab continues."""
        text = "Queue: a\n    enabled = True\n    started = True\n"
        queue = scheduler_probe._parse_queue_liveness(text)[0]
        assert (queue.enabled, queue.started) == (True, True)


class TestANumericPropertyNameIsNotMistakenForANodeCount:
    """A digit-only term is a node count only where a count can stand.

    Torque writes the count as the first term of a spec (`2:wide:ppn=64`).
    Dropping every digit-only term also dropped a property whose name is
    numeric -- a GPU generation such as `2080` -- and silently left the queue
    routing to it unattributed.
    """

    def test_a_leading_number_is_still_a_count(self) -> None:
        assert scheduler_probe._neednodes_lanes("2:wide:ppn=64") == ("wide",)
        assert scheduler_probe._neednodes_lanes("1:ppn=128") == ()
        assert scheduler_probe._neednodes_lanes("2080") == ()

    def test_a_numeric_property_after_the_count_is_a_lane(self) -> None:
        assert scheduler_probe._neednodes_lanes("2:2080") == ("2080",)
        assert scheduler_probe._neednodes_lanes("4:2080:ppn=8") == ("2080",)

    def test_each_spec_gets_its_own_count_position(self) -> None:
        assert scheduler_probe._neednodes_lanes("1:gpu:ppn=8+2:2080:ppn=4") == (
            "gpu", "2080",
        )
