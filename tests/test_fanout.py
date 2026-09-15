"""v0.7.6 *Tanenbaum's Mailbox* — parallel fleet fan-out tests.

Covers the ``_run_per_host`` / ``_aggregate_per_host`` /
``_aggregate_per_host_json`` helpers in :mod:`vq.cli`. The
production codepaths (``vq admin status --all``, ``vq admin update
--all-hosts``, ``vq admin auto-update --all-hosts``, ``vq admin
audit-recovery --all``) all funnel through these — verifying the
helpers covers the operator-visible behaviour for every
fan-out-aware verb.

Three invariants the tests pin:

1. Per-host calls happen in parallel (wall-time savings vs. sum of
   per-host costs).
2. Output ordering is deterministic alphabetical regardless of
   completion order — operators see the same banner sequence
   they did pre-v0.7.6.
3. Per-host failures don't take down the fan-out — bad-host
   surfaces as an error line, the rest of the hosts still report.
"""

from __future__ import annotations

import json
import time

import pytest

from vq import cli, config


def _make_cfg(host_names: list[str]) -> config.Config:
    """Synthesize a Config object with N fake hosts. We don't need
    them to be reachable — the per_host_fn closures stub everything
    we care about."""
    hosts = {name: config.HostConfig(ssh=f"{name}.invalid") for name in host_names}
    return config.Config(hosts=hosts)


# ----------------------------------------------------------------------
# _fanout_serial_requested + _fanout_max_workers
# ----------------------------------------------------------------------


class TestFanoutEnv:
    """The two env-var escape hatches."""

    @pytest.mark.parametrize(
        "value, expected",
        [
            ("", False),
            ("0", False),
            ("false", False),
            ("no", False),
            ("1", True),
            ("true", True),
            ("TRUE", True),
            ("yes", True),
            ("on", True),
        ],
    )
    def test_serial_env_var(
        self,
        monkeypatch: pytest.MonkeyPatch,
        value: str,
        expected: bool,
    ) -> None:
        monkeypatch.setenv("VQ_FANOUT_SERIAL", value)
        assert cli._fanout_serial_requested() is expected

    def test_serial_env_unset_means_parallel(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("VQ_FANOUT_SERIAL", raising=False)
        assert cli._fanout_serial_requested() is False

    def test_workers_default_cap(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("VQ_FANOUT_WORKERS", raising=False)
        # Default cap at 8.
        assert cli._fanout_max_workers(20) == 8
        # But cap to n_hosts when below default.
        assert cli._fanout_max_workers(3) == 3

    def test_workers_env_override(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("VQ_FANOUT_WORKERS", "4")
        assert cli._fanout_max_workers(20) == 4
        # Still capped by n_hosts.
        assert cli._fanout_max_workers(2) == 2

    def test_workers_garbage_env_falls_back(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("VQ_FANOUT_WORKERS", "not-a-number")
        assert cli._fanout_max_workers(20) == 8
        monkeypatch.setenv("VQ_FANOUT_WORKERS", "0")
        # 0 is invalid; falls back to default.
        assert cli._fanout_max_workers(20) == 8
        monkeypatch.setenv("VQ_FANOUT_WORKERS", "-5")
        assert cli._fanout_max_workers(20) == 8

    def test_workers_floor_for_zero_hosts(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # n_hosts=0 is a corner case (caller shouldn't pass it,
        # but ThreadPoolExecutor needs max_workers >= 1).
        monkeypatch.delenv("VQ_FANOUT_WORKERS", raising=False)
        assert cli._fanout_max_workers(0) == 1


# ----------------------------------------------------------------------
# _safe_per_host: wraps per_host_fn so one bad host doesn't take down
# the fan-out
# ----------------------------------------------------------------------


class TestSafePerHost:
    def test_returns_rstripped(self) -> None:
        def fn(h: str) -> str:
            return f"hello {h}\n\n"
        assert cli._safe_per_host("alpha", fn) == "hello alpha"

    def test_click_exception_caught(self) -> None:
        import click as _click

        def fn(h: str) -> str:
            raise _click.ClickException("ssh: unreachable")

        out = cli._safe_per_host("alpha", fn)
        assert "alpha" in out
        assert "ssh: unreachable" in out

    def test_arbitrary_exception_caught(self) -> None:
        def fn(h: str) -> str:
            raise RuntimeError("transport bug")
        out = cli._safe_per_host("alpha", fn)
        assert "alpha" in out
        assert "RuntimeError" in out
        assert "transport bug" in out


# ----------------------------------------------------------------------
# _run_per_host: parallel dispatch + ordering invariant
# ----------------------------------------------------------------------


class TestRunPerHost:
    def test_parallel_default_saves_wall_time(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """4 hosts × 0.2s each should complete in ~0.2s in parallel,
        not ~0.8s serial."""
        monkeypatch.delenv("VQ_FANOUT_SERIAL", raising=False)
        monkeypatch.delenv("VQ_FANOUT_WORKERS", raising=False)

        def slow(h: str) -> str:
            time.sleep(0.2)
            return f"ok {h}"

        t0 = time.monotonic()
        results = cli._run_per_host(
            ["a", "b", "c", "d"], slow,
        )
        elapsed = time.monotonic() - t0
        assert elapsed < 0.7, (
            f"parallel fan-out should be much faster than serial "
            f"(0.8s); got {elapsed:.2f}s"
        )
        assert results == {"a": "ok a", "b": "ok b", "c": "ok c", "d": "ok d"}

    def test_serial_env_forces_serial(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("VQ_FANOUT_SERIAL", "1")

        def slow(h: str) -> str:
            time.sleep(0.05)
            return f"ok {h}"

        t0 = time.monotonic()
        results = cli._run_per_host(
            ["a", "b", "c", "d"], slow,
        )
        elapsed = time.monotonic() - t0
        # Serial: ~0.2s; parallel would be ~0.05s. Pick a threshold
        # comfortably between.
        assert elapsed >= 0.15, (
            f"VQ_FANOUT_SERIAL=1 should force serial; got "
            f"{elapsed:.2f}s — looks parallel"
        )
        assert len(results) == 4

    def test_parallel_false_kwarg_forces_serial(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The kwarg overrides the env (it's the test-and-debug
        knob)."""
        monkeypatch.delenv("VQ_FANOUT_SERIAL", raising=False)

        def slow(h: str) -> str:
            time.sleep(0.05)
            return f"ok {h}"

        t0 = time.monotonic()
        cli._run_per_host(
            ["a", "b", "c", "d"], slow, parallel=False,
        )
        elapsed = time.monotonic() - t0
        assert elapsed >= 0.15

    def test_single_host_always_serial(self) -> None:
        """One-host fan-out shouldn't spawn a thread pool."""
        calls = []

        def fn(h: str) -> str:
            calls.append(h)
            return f"ok {h}"

        results = cli._run_per_host(["only"], fn)
        assert results == {"only": "ok only"}
        assert calls == ["only"]

    def test_empty_host_list(self) -> None:
        results = cli._run_per_host([], lambda h: "")
        assert results == {}

    def test_per_host_failures_isolated(self) -> None:
        """One host raises — others still get their results."""
        import click as _click

        def fn(h: str) -> str:
            if h == "bad":
                raise _click.ClickException("ssh broken")
            return f"ok {h}"

        results = cli._run_per_host(["good1", "bad", "good2"], fn)
        assert "ok good1" in results["good1"]
        assert "ok good2" in results["good2"]
        assert "bad" in results["bad"]
        assert "ssh broken" in results["bad"]


# ----------------------------------------------------------------------
# _aggregate_per_host: deterministic alphabetical render + banner shape
# ----------------------------------------------------------------------


class TestAggregatePerHost:
    def test_renders_alphabetically_even_when_completion_order_varies(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A host that finishes last is still rendered first if its
        name sorts first. The operator sees the same banner sequence
        regardless of network/scheduler jitter."""
        monkeypatch.delenv("VQ_FANOUT_SERIAL", raising=False)
        cfg = _make_cfg(["alpha", "bravo", "charlie", "delta"])

        # Reverse-order delays: alpha takes longest, delta shortest.
        # Without alphabetical re-ordering, output would lead with delta.
        delays = {"alpha": 0.10, "bravo": 0.07, "charlie": 0.04, "delta": 0.01}

        def fn(h: str) -> str:
            time.sleep(delays[h])
            return f"data-{h}"

        out = cli._aggregate_per_host(cfg, fn)
        # Banners appear in alphabetical order.
        a = out.index("==== alpha ====")
        b = out.index("==== bravo ====")
        c = out.index("==== charlie ====")
        d = out.index("==== delta ====")
        assert a < b < c < d, (
            f"banners must render alphabetically; got: {out!r}"
        )
        # All host data made it into the output.
        for h in ("alpha", "bravo", "charlie", "delta"):
            assert f"data-{h}" in out

    def test_failure_does_not_take_down_aggregate(self) -> None:
        import click as _click
        cfg = _make_cfg(["alpha", "beta", "gamma"])

        def fn(h: str) -> str:
            if h == "beta":
                raise _click.ClickException("host down")
            return f"data-{h}"

        out = cli._aggregate_per_host(cfg, fn)
        assert "data-alpha" in out
        assert "data-gamma" in out
        assert "host down" in out
        assert "beta" in out

    def test_empty_config_explains_rather_than_silently_empty(
        self,
    ) -> None:
        cfg = config.Config()
        out = cli._aggregate_per_host(cfg, lambda h: "")
        assert "no hosts configured" in out


# ----------------------------------------------------------------------
# _aggregate_per_host_json: JSON aggregation in parallel
# ----------------------------------------------------------------------


class TestAggregatePerHostJson:
    def test_returns_object_keyed_by_host(self) -> None:
        cfg = _make_cfg(["alpha", "bravo"])

        def fn(h: str) -> str:
            return json.dumps({"host": h, "ok": True})

        payload = cli._aggregate_per_host_json(cfg, fn)
        assert payload == {
            "alpha": {"host": "alpha", "ok": True},
            "bravo": {"host": "bravo", "ok": True},
        }

    def test_per_host_failure_lands_as_error_dict(self) -> None:
        import click as _click
        cfg = _make_cfg(["alpha", "bravo"])

        def fn(h: str) -> str:
            if h == "bravo":
                raise _click.ClickException("ssh timeout")
            return json.dumps({"ok": True})

        payload = cli._aggregate_per_host_json(cfg, fn)
        assert payload["alpha"] == {"ok": True}
        assert "error" in payload["bravo"]
        assert "ssh timeout" in payload["bravo"]["error"]

    def test_invalid_json_from_host_lands_as_error_dict(self) -> None:
        cfg = _make_cfg(["alpha", "bravo"])

        def fn(h: str) -> str:
            if h == "bravo":
                return "this is not json {{{"
            return json.dumps({"ok": True})

        payload = cli._aggregate_per_host_json(cfg, fn)
        assert payload["alpha"] == {"ok": True}
        assert "error" in payload["bravo"]
        assert "invalid JSON" in payload["bravo"]["error"]

    def test_empty_config_returns_empty_dict(self) -> None:
        cfg = config.Config()
        payload = cli._aggregate_per_host_json(cfg, lambda h: "{}")
        assert payload == {}

    def test_parallel_default_saves_wall_time(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Same wall-time invariant as the text aggregator."""
        monkeypatch.delenv("VQ_FANOUT_SERIAL", raising=False)
        cfg = _make_cfg(["a", "b", "c", "d"])

        def fn(h: str) -> str:
            time.sleep(0.15)
            return json.dumps({"host": h})

        t0 = time.monotonic()
        payload = cli._aggregate_per_host_json(cfg, fn)
        elapsed = time.monotonic() - t0
        assert elapsed < 0.5, (
            f"JSON fan-out should parallelize; got {elapsed:.2f}s"
        )
        assert len(payload) == 4


# ----------------------------------------------------------------------
# v0.12.1 — `vq admin status` fleet-sweep flag spelling.
#
# vq deliberately spells the fleet sweep two ways: read verbs (doctor,
# programs, queue, admin status) take ``--all``; write fan-outs (admin
# update, admin auto-update, drain, fetch-all) take ``--all-hosts``.
# An operator running a rollout types `--all-hosts` all day, then hits
# the verification step and the read verb rejects it — which is how the
# fleet_update_runbook.md verification block shipped an uncallable
# command. `admin status` therefore accepts BOTH spellings.
# ----------------------------------------------------------------------


class TestAdminStatusAllHostsAlias:
    def test_both_spellings_are_accepted(self) -> None:
        """--all and --all-hosts both bind the same flag."""
        params = {
            opt
            for param in cli.admin_status.params
            for opt in getattr(param, "opts", [])
        }
        assert "--all" in params
        assert "--all-hosts" in params

    def test_alias_targets_the_all_hosts_destination(self) -> None:
        """Both spellings must drive one destination, not two flags."""
        flag = next(
            p for p in cli.admin_status.params if "--all-hosts" in getattr(p, "opts", [])
        )
        assert flag.name == "all_hosts"
        assert "--all" in flag.opts

    @pytest.mark.parametrize("spelling", ["--all", "--all-hosts"])
    def test_spelling_conflicts_with_positional_host(self, spelling: str) -> None:
        """Either spelling plus a HOST is contradictory, and says so."""
        from click.testing import CliRunner

        result = CliRunner().invoke(cli.main, ["admin", "status", spelling, "host_a"])
        assert result.exit_code != 0
        assert "mutually exclusive" in result.output
