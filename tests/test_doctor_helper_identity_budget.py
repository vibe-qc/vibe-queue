"""The scheduler-helper doctor check on a slow login node.

host_f, 2026-09-10: every remote vq call took 1.6-2.5 s (python start-up of the
editable helper), ``scheduler_remote_vq`` ran three of them plus two owned
driver identity probes inside one 10 s budget, and ``source-sha`` -- the one
read rollout planning needs -- timed out in three sweeps out of four. The
helper's live SHA was missing from those sweeps, ``_helper_lane_state`` read
LAST OK false, and ``--supersede-plan-hold`` refused a host standing exactly
at the pin with a verified success record.

Two things were wrong and both are pinned here. The timeout was reported as a
plain failure ("SOURCE-SHA check failed: remote vq timed out after 2.37s")
because transport raises it ``from None`` and the doctor walked only
``__cause__``. And the check paid a round trip per identity question: a
helper that answers ``vq source-identity`` now pays one, and
``[fleet] check_timeout_seconds`` lets the sweep carry a budget sized for a
helper that cannot.
"""

from __future__ import annotations

import json
import subprocess
from collections.abc import Callable
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import __version__, admin, config, doctor, fleet_rollout, transport
from vq.cli import main

PIN = "4" * 40
OTHER = "9" * 40
TREE = "12" * 32

# The shape recorded on host_f: remote python start-ups of up to 2.5 s, and the
# driver's own editable install costing 1.3 s per owned identity probe.
host_f_REMOTE_LATENCY = 2.5
DRIVER_PROBE_LATENCY = 1.3

# What ``[fleet] check_timeout_seconds = 30`` hands the rollout sweep.
CONFIGURED_CHECK_TIMEOUT = 30.0


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def monotonic(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def _raise_transport_timeout(timeout: float, ssh: str) -> None:
    """Raise exactly what ``transport.run_remote_vq`` raises on a timeout.

    The ``from None`` is the point: it severs ``__cause__`` so a token-bearing
    ssh argv never rides the chain, and leaves only ``__context__`` for the
    doctor to recognize the timeout by.
    """
    try:
        raise subprocess.TimeoutExpired(["ssh", ssh, "PRIVATE-REMOTE-ARGV"], timeout)
    except subprocess.TimeoutExpired:
        raise transport.RemoteOutcomeUnknown(
            f"remote vq timed out after {timeout}s on {ssh}:\n"
            "  cmd: PRIVATE-REMOTE-ARGV"
        ) from None


def _unknown_command(args: tuple[str, ...]) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(
        ["vq", *args],
        2,
        "",
        "Usage: vq [OPTIONS] COMMAND [ARGS]...\n"
        "Try 'vq --help' for help.\n\n"
        f"Error: No such command '{args[0]}'.\n",
    )


class _SlowLoginNode:
    """A scheduler helper whose every python start-up costs ``latency``.

    Emulates transport faithfully: a call whose latency exceeds the timeout it
    was handed burns that whole timeout and then raises the production
    ``RemoteOutcomeUnknown ... from None``. A helper at or before 0.26.0
    answers ``source-identity`` the way click does for a command it predates.
    """

    def __init__(
        self,
        clock: _Clock,
        *,
        latency: float,
        version: str,
        sha: str = PIN,
        tree: str = TREE,
        identity: Callable[[], subprocess.CompletedProcess[str]] | None = None,
        latencies: dict[tuple[str, ...], float] | None = None,
    ) -> None:
        self.clock = clock
        self.latency = latency
        self.latencies = latencies or {}
        self.version = version
        self.sha = sha
        self.tree = tree
        self.identity = identity
        self.calls: list[tuple[str, ...]] = []
        self.timeouts: list[float] = []

    def has_source_identity(self) -> bool:
        return doctor._helper_answers_source_identity(self.version)  # noqa: SLF001

    def remote_vq(
        self,
        host_cfg: config.HostConfig,
        *args: str,
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        self.calls.append(args)
        assert kwargs["owned_process_group"] is True
        timeout = float(kwargs["timeout"])  # type: ignore[arg-type]
        self.timeouts.append(timeout)
        latency = self.latencies.get(args, self.latency)
        if latency > timeout:
            self.clock.advance(timeout)
            _raise_transport_timeout(timeout, host_cfg.ssh)
        self.clock.advance(latency)
        if args == ("--version",):
            return subprocess.CompletedProcess(
                ["vq", *args], 0, f"vq, version {self.version}\n", ""
            )
        if args == ("source-tree-sha256",):
            return subprocess.CompletedProcess(["vq", *args], 0, f"{self.tree}\n", "")
        if args == ("source-sha",):
            return subprocess.CompletedProcess(["vq", *args], 0, f"{self.sha}\n", "")
        if args == ("source-identity",):
            if self.identity is not None:
                return self.identity()
            if not self.has_source_identity():
                return _unknown_command(args)
            payload = {
                "version": self.version,
                "source_tree_sha256": self.tree,
                "source_tree_sha256_error": None,
                "source_sha": self.sha,
                "source_sha_error": None,
            }
            return subprocess.CompletedProcess(
                ["vq", *args], 0, json.dumps(payload, sort_keys=True) + "\n", ""
            )
        raise AssertionError(f"unexpected remote vq call {args!r}")


def _owned_driver_probes(
    clock: _Clock,
    *,
    latency: float,
    sha: str = PIN,
    tree: str = TREE,
) -> Callable[..., subprocess.CompletedProcess[str]]:
    def run(argv: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        timeout = float(kwargs["timeout"])  # type: ignore[arg-type]
        if latency > timeout:
            clock.advance(timeout)
            raise subprocess.TimeoutExpired(argv, timeout)
        clock.advance(latency)
        value = sha if argv[-1] == "source_sha" else tree
        return subprocess.CompletedProcess(argv, 0, f"{value}\n", "")

    return run


def _host_f() -> config.HostConfig:
    return config.HostConfig(
        ssh="host_f-login",
        remote_vq="/home/USER/vibe-queue/.venv/bin/vq",
        scheduler="pbs",
        scheduler_dialect="torque",
        scratch_root="/scratch",
        scheduler_driver="driver",
    )


def _run_check(
    monkeypatch: pytest.MonkeyPatch,
    helper: _SlowLoginNode,
    *,
    clock: _Clock,
    check_timeout: float,
    driver_latency: float = DRIVER_PROBE_LATENCY,
) -> dict[str, object]:
    monkeypatch.setattr(doctor.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(
        transport,
        "run_owned_subprocess",
        _owned_driver_probes(clock, latency=driver_latency),
    )
    monkeypatch.setattr(transport, "run_remote_vq", helper.remote_vq)
    return doctor.scheduler_remote_vq_check(
        _host_f(),
        host_label="host_f",
        check_timeout=check_timeout,
    )


def _helper_lane(check: dict[str, object]) -> fleet_rollout.LaneState:
    """The planner's helper lane for host_f, whose canonical record is at PIN."""
    admin_status = {
        "host_f": {
            "helper": {
                "configured": True,
                "last": {
                    "actual_sha": PIN,
                    "last_success": True,
                    "healthy": True,
                },
            }
        }
    }
    doctor_snapshot = {"host_f": {"ok": check["ok"], "checks": [check]}}
    return fleet_rollout._helper_lane_state(  # noqa: SLF001 - planner seam
        admin_status,
        doctor_snapshot,
        host="host_f",
        configured=True,
    )


# ---------------------------------------------------------------------------
# The timeout is structured even though transport severs the cause chain.
# ---------------------------------------------------------------------------


def test_remote_vq_timeout_raised_from_none_is_a_structured_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """host_f's actual message was a plain failure; it must be ``timed_out``."""
    clock = _Clock()
    helper = _SlowLoginNode(clock, latency=host_f_REMOTE_LATENCY, version="0.26.0")

    result = _run_check(
        monkeypatch,
        helper,
        clock=clock,
        check_timeout=doctor.DEFAULT_CHECK_TIMEOUT_SECONDS,
    )

    assert helper.calls == [
        ("--version",),
        ("source-tree-sha256",),
        ("source-sha",),
    ]
    assert result["ok"] is False
    assert result["timed_out"] is True
    assert result["subprobe"] == "helper_source_sha"
    assert result["elapsed_seconds"] == pytest.approx(10.0)
    assert result["timeout_seconds"] == pytest.approx(10.0)
    assert "SOURCE-SHA check failed" not in str(result)
    assert "PRIVATE-REMOTE-ARGV" not in str(result)
    assert "source_sha" not in result


@pytest.mark.parametrize(
    ("stalled", "subprobe"),
    [
        (("--version",), "helper_version"),
        (("source-tree-sha256",), "helper_source_tree_sha256"),
        (("source-sha",), "helper_source_sha"),
    ],
)
def test_inner_stall_inside_a_wide_budget_is_a_marked_failure(
    monkeypatch: pytest.MonkeyPatch,
    stalled: tuple[str, ...],
    subprobe: str,
) -> None:
    """A 30 s remote stall inside a wide budget is not the deadline's verdict
    -- the message is the transport's -- but it is still an absent answer,
    and whichever call stalled, the verdict carries the mark that says so."""
    clock = _Clock()
    helper = _SlowLoginNode(
        clock, latency=0.1, version="0.26.0", latencies={stalled: 31.0}
    )

    result = _run_check(monkeypatch, helper, clock=clock, check_timeout=120.0)

    assert helper.calls[-1] == stalled
    assert result["ok"] is False
    assert "remote vq timed out after 30.0s" in str(result["message"])
    assert result["timed_out"] is True
    assert result["subprobe"] == subprobe
    assert "elapsed_seconds" not in result
    assert "source_sha" not in result
    assert _helper_lane(result).probe_unavailable is True


def test_timeout_metadata_fires_on_the_production_from_none_shape() -> None:
    """The mark was added beside a test that set ``__cause__`` by hand; the
    transport never does, so in production it fired for nobody."""
    try:
        _raise_transport_timeout(2.37, "host_f-login")
    except transport.RemoteError as exc:
        caught = exc
    assert caught.__cause__ is None

    marked = doctor._timeout_metadata(caught, subprobe="helper_source_sha")  # noqa: SLF001

    assert marked == {"timed_out": True, "subprobe": "helper_source_sha"}


# ---------------------------------------------------------------------------
# One round trip on a helper that answers ``source-identity``.
# ---------------------------------------------------------------------------


def test_newer_helper_answers_identity_in_one_round_trip(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _Clock()
    helper = _SlowLoginNode(clock, latency=host_f_REMOTE_LATENCY, version="0.27.0")

    result = _run_check(
        monkeypatch,
        helper,
        clock=clock,
        check_timeout=doctor.DEFAULT_CHECK_TIMEOUT_SECONDS,
    )

    assert helper.calls == [("--version",), ("source-identity",)]
    assert result["ok"] is True
    assert result["version"] == "0.27.0"
    assert result["source_sha"] == PIN
    assert result["source_tree_sha256"] == TREE
    assert clock.now - 1000.0 == pytest.approx(
        2 * host_f_REMOTE_LATENCY + 2 * DRIVER_PROBE_LATENCY
    )
    # Its timeouts are the same shared budget the legacy pair drew from.
    assert helper.timeouts[0] == pytest.approx(10.0)
    assert helper.timeouts[1] == pytest.approx(10.0 - 2.5 - 2 * 1.3)


def test_identity_timeout_is_attributed_to_its_own_subprobe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _Clock()
    helper = _SlowLoginNode(clock, latency=4.0, version="0.27.0")

    result = _run_check(monkeypatch, helper, clock=clock, check_timeout=8.0)

    assert helper.calls == [("--version",), ("source-identity",)]
    assert result["timed_out"] is True
    assert result["subprobe"] == "helper_source_identity"


@pytest.mark.parametrize(
    ("version", "expected"),
    [
        (None, False),
        ("garbage", False),
        ("0.25.7", False),
        ("0.26.0", False),
        ("0.26.0.dev4", False),
        ("0.26.1", True),
        ("0.27.0", True),
        ("0.27.0.dev3", True),
        ("1.0.0", True),
    ],
)
def test_source_identity_version_gate(version: str | None, expected: bool) -> None:
    assert doctor._helper_answers_source_identity(version) is expected  # noqa: SLF001


def test_gate_misjudged_helper_falls_back_to_the_legacy_pair(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The gate decides round trips, never the verdict: a helper that says
    "No such command" is asked the legacy pair and still verifies."""
    clock = _Clock()
    helper = _SlowLoginNode(
        clock,
        latency=0.1,
        version="0.27.0",
        identity=lambda: _unknown_command(("source-identity",)),
    )

    result = _run_check(monkeypatch, helper, clock=clock, check_timeout=10.0)

    assert helper.calls == [
        ("--version",),
        ("source-identity",),
        ("source-tree-sha256",),
        ("source-sha",),
    ]
    assert result["ok"] is True
    assert result["source_sha"] == PIN


def test_unknown_command_detection_matches_what_click_prints() -> None:
    """The fallback keys off click's own usage error, so pin that text."""
    result = CliRunner().invoke(main, ["source-identity-from-the-future"])
    proc = subprocess.CompletedProcess(
        ["vq", "source-identity-from-the-future"],
        result.exit_code,
        "",
        result.output,
    )
    assert result.exit_code == 2
    assert doctor._looks_like_unknown_command(proc) is True  # noqa: SLF001
    assert doctor._looks_like_unknown_command(  # noqa: SLF001
        subprocess.CompletedProcess(["vq"], 2, "", "Error: Missing argument.\n")
    ) is False
    assert doctor._looks_like_unknown_command(  # noqa: SLF001
        subprocess.CompletedProcess(["vq"], 1, "", "no such command here\n")
    ) is False


@pytest.mark.parametrize(
    ("identity", "fragment", "source_sha", "lane_ok"),
    [
        (
            {
                "version": "0.27.0",
                "source_tree_sha256": TREE,
                "source_tree_sha256_error": None,
                "source_sha": OTHER,
                "source_sha_error": None,
            },
            f"SOURCE-SHA mismatch: helper {OTHER}, driver {PIN}",
            OTHER,
            False,
        ),
        (
            {
                "version": "0.27.0",
                "source_tree_sha256": TREE,
                "source_tree_sha256_error": None,
                "source_sha": None,
                "source_sha_error": "no SOURCE-SHA marker installed",
            },
            "no SOURCE-SHA marker reported by scheduler helper; this usually "
            "means the helper predates the provenance contract or was "
            "installed outside `vq admin update <scheduler-host>`; "
            "source-identity reported no SOURCE-SHA: no SOURCE-SHA marker "
            "installed",
            None,
            False,
        ),
        # A tree mismatch against the DRIVER is not the helper lane's
        # concern: the lane is proven by the canonical record plus the live
        # SHA, so a driver checkout ahead of the pin cannot make a helper
        # at the pin look undeployed. The verdict still carries the SHA it
        # read, exactly as the legacy pair did.
        (
            {
                "version": "0.27.0",
                "source_tree_sha256": "34" * 32,
                "source_tree_sha256_error": None,
                "source_sha": PIN,
                "source_sha_error": None,
            },
            f"source-tree SHA-256 mismatch: helper {'34' * 32}, driver {TREE}",
            PIN,
            True,
        ),
        (
            {
                "version": "0.27.0",
                "source_tree_sha256": None,
                "source_tree_sha256_error": "package root unreadable",
                "source_sha": PIN,
                "source_sha_error": None,
            },
            f"source-tree SHA-256 mismatch: helper (missing), driver {TREE}",
            PIN,
            True,
        ),
    ],
)
def test_one_shot_identity_keeps_every_failure_verdict(
    monkeypatch: pytest.MonkeyPatch,
    identity: dict[str, object],
    fragment: str,
    source_sha: str | None,
    lane_ok: bool,
) -> None:
    """Fewer round trips change nothing about what a wrong answer means."""
    clock = _Clock()
    helper = _SlowLoginNode(
        clock,
        latency=0.1,
        version="0.27.0",
        identity=lambda: subprocess.CompletedProcess(
            ["vq", "source-identity"], 0, json.dumps(identity) + "\n", ""
        ),
    )

    result = _run_check(monkeypatch, helper, clock=clock, check_timeout=10.0)

    assert helper.calls == [("--version",), ("source-identity",)]
    assert result["ok"] is False
    assert fragment in str(result["message"])
    assert result.get("source_sha") == source_sha
    assert _helper_lane(result).last_ok is lane_ok


@pytest.mark.parametrize(
    ("proc", "fragment"),
    [
        (
            subprocess.CompletedProcess(["vq"], 0, "not json\n", ""),
            "source-identity returned malformed output",
        ),
        (
            subprocess.CompletedProcess(["vq"], 0, "[1, 2]\n", ""),
            "source-identity returned malformed output",
        ),
        (
            subprocess.CompletedProcess(["vq"], 1, "", "Traceback: boom\n"),
            "source-identity exit 1: Traceback: boom",
        ),
    ],
)
def test_one_shot_identity_rejects_answers_it_cannot_read(
    monkeypatch: pytest.MonkeyPatch,
    proc: subprocess.CompletedProcess[str],
    fragment: str,
) -> None:
    clock = _Clock()
    helper = _SlowLoginNode(clock, latency=0.1, version="0.27.0", identity=lambda: proc)

    result = _run_check(monkeypatch, helper, clock=clock, check_timeout=10.0)

    assert helper.calls == [("--version",), ("source-identity",)]
    assert result["ok"] is False
    assert fragment in str(result["message"])
    assert result["version"] == "0.27.0"
    assert "source_sha" not in result


# ---------------------------------------------------------------------------
# The helper side: ``vq source-identity``.
# ---------------------------------------------------------------------------


class TestSourceIdentityCLI:
    def test_prints_one_json_answer_the_doctor_accepts(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(admin, "source_tree_sha256", lambda: TREE)
        monkeypatch.setattr(
            admin,
            "inspect_source_sha_marker",
            lambda: admin.SourceShaMarkerStatus(
                path=Path("/opt/vq/SOURCE-SHA"), present=True, sha=PIN
            ),
        )

        result = CliRunner().invoke(main, ["source-identity"])

        assert result.exit_code == 0, result.output
        assert json.loads(result.output) == {
            "version": __version__,
            "source_tree_sha256": TREE,
            "source_tree_sha256_error": None,
            "source_sha": PIN,
            "source_sha_error": None,
        }

        # Round trip: the doctor reads exactly what the CLI printed. The
        # driver's own version answers the command once it is newer than the
        # gate; until then a helper from the next release stands in.
        gated = doctor._helper_answers_source_identity(__version__)  # noqa: SLF001
        clock = _Clock()
        helper = _SlowLoginNode(
            clock,
            latency=0.1,
            version=__version__ if gated else "0.27.0",
            identity=lambda: subprocess.CompletedProcess(
                ["vq", "source-identity"], 0, result.output, ""
            ),
        )
        check = _run_check(monkeypatch, helper, clock=clock, check_timeout=10.0)
        assert helper.calls == [("--version",), ("source-identity",)]
        assert check["ok"] is True
        assert check["source_sha"] == PIN
        assert check["source_tree_sha256"] == TREE

    @pytest.mark.parametrize(
        ("status", "error"),
        [
            (
                admin.SourceShaMarkerStatus(
                    path=Path("/opt/vq/SOURCE-SHA"), present=False
                ),
                "no SOURCE-SHA marker installed",
            ),
            (
                admin.SourceShaMarkerStatus(
                    path=Path("/opt/vq/SOURCE-SHA"),
                    present=True,
                    recorded_sha=OTHER,
                    recorded_tree_sha256="ab" * 32,
                    actual_tree_sha256=TREE,
                    stale=True,
                ),
                f"SOURCE-SHA marker at /opt/vq/SOURCE-SHA claims {OTHER} but the "
                "package beside it has changed since (recorded tree "
                f"{'ab' * 32}, actual {TREE})",
            ),
        ],
    )
    def test_marker_problems_are_fields_not_exit_codes(
        self,
        monkeypatch: pytest.MonkeyPatch,
        status: admin.SourceShaMarkerStatus,
        error: str,
    ) -> None:
        """The digest beside a bad marker still answers in the same call."""
        monkeypatch.setattr(admin, "source_tree_sha256", lambda: TREE)
        monkeypatch.setattr(admin, "inspect_source_sha_marker", lambda: status)

        result = CliRunner().invoke(main, ["source-identity"])

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["source_tree_sha256"] == TREE
        assert payload["source_sha"] is None
        assert payload["source_sha_error"] == error

    def test_undigestible_package_is_a_field_too(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def explode() -> str:
            raise admin.AdminError("package root unreadable")

        monkeypatch.setattr(admin, "source_tree_sha256", explode)
        monkeypatch.setattr(
            admin,
            "inspect_source_sha_marker",
            lambda: admin.SourceShaMarkerStatus(
                path=Path("/opt/vq/SOURCE-SHA"), present=True, sha=PIN
            ),
        )

        result = CliRunner().invoke(main, ["source-identity"])

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["source_tree_sha256"] is None
        assert payload["source_tree_sha256_error"] == "package root unreadable"
        assert payload["source_sha"] == PIN


# ---------------------------------------------------------------------------
# What the rollout planner sees: the helper lane resolves on a slow node.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("latency", [2.0, host_f_REMOTE_LATENCY])
@pytest.mark.parametrize(
    ("version", "check_timeout"),
    [
        # A helper at the current pin (no ``source-identity``) under the
        # budget a configured sweep carries.
        ("0.26.0", CONFIGURED_CHECK_TIMEOUT),
        # A helper newer than the pin under the doctor's own default.
        ("0.27.0", doctor.DEFAULT_CHECK_TIMEOUT_SECONDS),
    ],
)
def test_helper_lane_resolves_on_a_slow_login_node(
    monkeypatch: pytest.MonkeyPatch,
    latency: float,
    version: str,
    check_timeout: float,
) -> None:
    """A slow remote vq (2 s and 2.5 s per call) still yields the live SHA,
    and the lane the supersede gate reads is healthy and exactly at the pin."""
    assert CONFIGURED_CHECK_TIMEOUT > doctor.DEFAULT_CHECK_TIMEOUT_SECONDS
    clock = _Clock()
    helper = _SlowLoginNode(clock, latency=latency, version=version)

    check = _run_check(monkeypatch, helper, clock=clock, check_timeout=check_timeout)

    assert check["ok"] is True, check
    assert check["source_sha"] == PIN
    lane = _helper_lane(check)
    assert lane.last_ok is True
    assert lane.probe_unavailable is False
    assert lane.current_sha == PIN
    assert lane.detail == "canonical helper record matches live provenance probe"


def test_host_f_shape_under_the_doctor_default_is_the_recorded_refusal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The failure the sweep budget exists for, kept as a regression: the
    same host, the same helper, the doctor's own 10 s -- no live SHA, LAST OK
    false but the probe marked unavailable, and a detail that names the
    timeout and its remedy rather than reading like a wrong SHA."""
    clock = _Clock()
    helper = _SlowLoginNode(clock, latency=host_f_REMOTE_LATENCY, version="0.26.0")

    check = _run_check(
        monkeypatch,
        helper,
        clock=clock,
        check_timeout=doctor.DEFAULT_CHECK_TIMEOUT_SECONDS,
    )

    assert check["timed_out"] is True
    lane = _helper_lane(check)
    assert lane.last_ok is False
    assert lane.probe_unavailable is True
    assert lane.current_sha == PIN  # the canonical record, not live evidence
    assert lane.detail == (
        "live helper provenance probe timed out (helper_source_sha after 10s "
        "of a 10s check budget); retry, and if it persists widen "
        "`[fleet] check_timeout_seconds`"
    )


def test_wrong_live_sha_still_refuses_however_fast_the_helper_answers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The semantics of a real probe failure are untouched."""
    clock = _Clock()
    helper = _SlowLoginNode(clock, latency=0.1, version="0.27.0", sha=OTHER)

    check = _run_check(monkeypatch, helper, clock=clock, check_timeout=10.0)

    assert check["ok"] is False
    assert check["source_sha"] == OTHER
    lane = _helper_lane(check)
    assert lane.last_ok is False
    assert lane.probe_unavailable is False  # it answered; a wrong answer is evidence
    assert lane.current_sha == OTHER
    assert lane.detail == (
        "canonical helper record and live provenance probe disagree"
    )


def test_plain_probe_failure_detail_is_unchanged() -> None:
    lane = _helper_lane(
        {
            "name": "scheduler_remote_vq",
            "ok": False,
            "message": "ssh transport failed for host_f-login (exit 255)",
        }
    )
    assert lane.last_ok is False
    assert lane.detail == "live helper provenance probe unavailable"
