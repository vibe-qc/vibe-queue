"""v0.11.0 *Baran's Detour* — route a no-host per-job lookup around a
down ``default_host``.

Covers ``_host_has_job`` / ``_require_inferred_default_queue_snapshot`` /
``_locate_job_host`` / ``_resolve_job_host`` in :mod:`vq.cli`, plus the
end-to-end ``vq status JOBID`` / ``vq kill JOBID`` (no host arg) paths when
``default_host`` is marked ``vq host down``.

Regression for the reported bug: with host_d (``default_host``) unreachable
and marked down, ``vq status JOBID`` (no host) delegated the lookup straight
to host_d over SSH, hit exit 255, and hard-errored — even though the job
lived on a reachable host (host_a). The fix skips a down ``default_host`` and
locates the job across the remaining up hosts; one unreachable host in the
fan-out must NOT abort the search.
"""
from __future__ import annotations

import json
import subprocess
from pathlib import Path

import click
import pytest
from click.testing import CliRunner

from vq import cli, config, host_status, paths
from vq.cli import main


def _make_cfg(host_names: list[str]) -> config.Config:
    """Synthesize a Config with N fake hosts; ``default_host`` is the
    first. The ``*-test`` / ``*-box`` suffixes guarantee ``is_local_host``
    is False on every real machine (including the fleet boxes), so the
    remote probe path is always exercised."""
    hosts = {n: config.HostConfig(ssh=n) for n in host_names}
    return config.Config(
        hosts=hosts, default_host=host_names[0] if host_names else None
    )


def _down(host: str) -> host_status.DownEntry:
    return host_status.DownEntry(host=host, reason="temporarily down", since="")


# ----------------------------------------------------------------------
# _host_has_job — the per-host read-only probe
# ----------------------------------------------------------------------


class TestHostHasJob:
    def test_remote_uses_durable_queue_listing_not_status(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ownership discovery reads JobSpecs; it must not recurse through status."""
        cfg = _make_cfg(["owner-box"])
        calls: list[tuple[str, ...]] = []

        def fake_run(_host_cfg, *args, **kwargs):  # type: ignore[no-untyped-def]
            calls.append(args)
            assert args == ("queue", "localhost", "--show-archived", "--json")
            assert kwargs == {
                "check": False,
                "timeout": cli._LOCATE_PROBE_TIMEOUT_SECONDS,
                "owned_process_group": True,
            }
            return subprocess.CompletedProcess(
                [], 0, json.dumps([{"id": "jid", "scheduler_target": None}]), ""
            )

        monkeypatch.setattr(cli.transport, "run_remote_vq", fake_run)

        assert cli._host_has_job(cfg, "owner-box", "jid", multi_user=False) == "found"
        assert calls == [("queue", "localhost", "--show-archived", "--json")]

    def test_scheduler_target_is_located_on_shared_driver_without_alias_collision(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Two scheduler aliases may share one driver but remain distinct lanes."""
        driver = config.HostConfig(ssh="driver-box")
        scheduler_common = {
            "ssh": "scheduler-login",
            "scheduler": "slurm",
            "scheduler_dialect": "slurm",
            "scratch_root": "/scheduler/scratch",
            "scheduler_driver": "driver-box",
        }
        cfg = config.Config(
            default_host="driver-box",
            hosts={
                "driver-box": driver,
                "host_c": config.HostConfig(**scheduler_common),
                "host_c-campaign": config.HostConfig(**scheduler_common),
            },
        )
        calls: list[tuple[str, tuple[str, ...]]] = []

        def fake_run(host_cfg, *args, **kwargs):  # type: ignore[no-untyped-def]
            calls.append((host_cfg.ssh, args))
            assert host_cfg.ssh == "driver-box"
            assert args == ("queue", "localhost", "--show-archived", "--json")
            return subprocess.CompletedProcess(
                [],
                0,
                json.dumps(
                    [{"id": "jid", "scheduler_target": "host_c-campaign"}]
                ),
                "",
            )

        monkeypatch.setattr(cli.transport, "run_remote_vq", fake_run)

        assert cli._host_has_job(cfg, "driver-box", "jid", multi_user=False) == "absent"
        assert cli._host_has_job(cfg, "host_c", "jid", multi_user=False) == "absent"
        assert (
            cli._host_has_job(cfg, "host_c-campaign", "jid", multi_user=False)
            == "found"
        )
        assert all(call[0] == "driver-box" for call in calls)

    def test_scheduler_lane_on_unenrolled_localhost_driver_is_locatable(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A configured scheduler lane can use localhost without a host row."""
        cfg = config.Config(
            default_host="host_f",
            hosts={
                "host_f": config.HostConfig(
                    ssh="scheduler-login",
                    scheduler="pbs",
                    scheduler_dialect="torque",
                    scratch_root="/scheduler/scratch",
                    scheduler_driver="localhost",
                )
            },
        )
        scheduler_spec = type(
            "Spec", (), {"id": "jid", "scheduler_target": "host_f"}
        )()
        direct_spec = type(
            "Spec", (), {"id": "direct", "scheduler_target": None}
        )()
        monkeypatch.setattr(
            cli, "list_jobs", lambda *a, **k: [scheduler_spec, direct_spec]
        )

        assert cli._host_has_job(cfg, "host_f", "jid", multi_user=False) == "found"
        # With no configured direct localhost handle, its plain local row is
        # intentionally not attributed to the scheduler lane.
        assert cli._host_has_job(cfg, "host_f", "direct", multi_user=False) == "absent"

    def test_remote_found_rc0(self, monkeypatch: pytest.MonkeyPatch) -> None:
        cfg = _make_cfg(["owner-box"])
        monkeypatch.setattr(
            cli.transport, "run_remote_vq",
            lambda *a, **k: subprocess.CompletedProcess(
                [], 0, '[{"id":"jid","scheduler_target":null}]', ""
            ),
        )
        assert cli._host_has_job(cfg, "owner-box", "jid", multi_user=False) == "found"

    @pytest.mark.parametrize(
        "payload",
        [
            "",
            "null",
            "{}",
            "[42]",
            '[{"id": 42}]',
            '[{"id": ""}]',
            '[{"id": "   "}]',
            '[{"id": "other", "scheduler_target": 42}]',
        ],
    )
    def test_remote_malformed_listing_is_unreachable(
        self, monkeypatch: pytest.MonkeyPatch, payload: str
    ) -> None:
        """Invalid queue JSON cannot provide negative ownership evidence."""
        cfg = _make_cfg(["owner-box"])
        monkeypatch.setattr(
            cli.transport,
            "run_remote_vq",
            lambda *a, **k: subprocess.CompletedProcess([], 0, payload, ""),
        )
        assert (
            cli._host_has_job(cfg, "owner-box", "jid", multi_user=False)
            == "unreachable"
        )

    def test_remote_valid_empty_listing_is_absent(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An authoritative empty list remains exact negative evidence."""
        cfg = _make_cfg(["owner-box"])
        monkeypatch.setattr(
            cli.transport,
            "run_remote_vq",
            lambda *a, **k: subprocess.CompletedProcess([], 0, "[]", ""),
        )
        assert cli._host_has_job(cfg, "owner-box", "jid", multi_user=False) == "absent"

    def test_remote_nonzero_without_listing_is_unreachable(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # A queue listing has no job-not-found exit code.  Any non-zero result
        # lacks durable evidence and must not be mistaken for an absent job.
        cfg = _make_cfg(["owner-box"])
        monkeypatch.setattr(
            cli.transport, "run_remote_vq",
            lambda *a, **k: subprocess.CompletedProcess([], 2, "", "queue failed"),
        )
        assert (
            cli._host_has_job(cfg, "owner-box", "jid", multi_user=False)
            == "unreachable"
        )

    def test_remote_exit255_is_unreachable(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cfg = _make_cfg(["down-box"])
        monkeypatch.setattr(
            cli.transport, "run_remote_vq",
            lambda *a, **k: subprocess.CompletedProcess([], 255, "", "broken pipe"),
        )
        assert cli._host_has_job(cfg, "down-box", "jid", multi_user=False) == "unreachable"

    def test_remote_timeout_is_unreachable(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cfg = _make_cfg(["down-box"])

        def boom(*a, **k):
            raise cli.transport.RemoteError("remote vq timed out")

        monkeypatch.setattr(cli.transport, "run_remote_vq", boom)
        assert cli._host_has_job(cfg, "down-box", "jid", multi_user=False) == "unreachable"

    def test_unknown_host_is_unreachable(self) -> None:
        # A host not in the config can't be probed -> unreachable (skip it).
        cfg = _make_cfg(["owner-box"])
        assert cli._host_has_job(cfg, "ghost-box", "jid", multi_user=False) == "unreachable"

    def test_local_found(self, monkeypatch: pytest.MonkeyPatch) -> None:
        cfg = _make_cfg(["localhost"])
        spec = type("Spec", (), {"id": "jid", "scheduler_target": None})()
        monkeypatch.setattr(cli, "list_jobs", lambda *a, **k: [spec])
        assert cli._host_has_job(cfg, "localhost", "jid", multi_user=False) == "found"

    def test_local_absent(self, monkeypatch: pytest.MonkeyPatch) -> None:
        cfg = _make_cfg(["localhost"])
        monkeypatch.setattr(cli, "list_jobs", lambda *a, **k: [])
        assert cli._host_has_job(cfg, "localhost", "jid", multi_user=False) == "absent"

    def test_local_multiuser_empty_listing_is_absent(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cfg = _make_cfg(["localhost"])
        monkeypatch.setattr(cli, "list_jobs", lambda *a, **k: [])
        assert cli._host_has_job(cfg, "localhost", "jid", multi_user=True) == "absent"


# ----------------------------------------------------------------------
# _locate_job_host — the fan-out locator
# ----------------------------------------------------------------------


class TestLocateJobHost:
    def test_builtin_local_aliases_are_one_logical_owner(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Local spellings share one direct queue even without fleet metadata."""
        cfg = config.Config(
            default_host="localhost",
            hosts={
                "127.0.0.1": config.HostConfig(ssh="localhost"),
                "localhost": config.HostConfig(ssh="localhost"),
            },
        )
        spec = type("Spec", (), {"id": "jid", "scheduler_target": None})()
        monkeypatch.setattr(cli.host_status, "load_down", lambda: {})
        monkeypatch.setattr(cli, "list_jobs", lambda *args, **kwargs: [spec])

        assert cli._locate_job_host(cfg, "jid", multi_user=False) == "localhost"

    def test_direct_fleet_alias_does_not_duplicate_one_real_owner(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A daemon alias is one queue authority, not a second owner."""
        monkeypatch.setenv("VQ_FANOUT_SERIAL", "1")
        cfg = config.Config(
            default_host="owner-box",
            hosts={
                "owner-box": config.HostConfig(ssh="owner-box"),
                "owner-alias": config.HostConfig(
                    ssh="owner-box",
                    fleet_role="alias",
                    fleet_canonical_host="owner-box",
                ),
            },
        )
        calls: list[str] = []
        monkeypatch.setattr(cli.host_status, "load_down", lambda: {})

        def fake_run(host_cfg, *args, **kwargs):  # type: ignore[no-untyped-def]
            calls.append(host_cfg.ssh)
            return subprocess.CompletedProcess(
                [], 0, json.dumps([{"id": "jid", "scheduler_target": None}]), ""
            )

        monkeypatch.setattr(cli.transport, "run_remote_vq", fake_run)

        assert cli._locate_job_host(cfg, "jid", multi_user=False) == "owner-box"
        assert calls == ["owner-box"]

    def test_ambiguous_queue_records_on_real_hosts_refuse_to_guess(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The durable listing may name two real owners; neither wins by order."""
        monkeypatch.setenv("VQ_FANOUT_SERIAL", "1")
        cfg = _make_cfg(["alpha-box", "beta-box"])
        monkeypatch.setattr(cli.host_status, "load_down", lambda: {})

        def fake_run(_host_cfg, *args, **kwargs):  # type: ignore[no-untyped-def]
            assert args == ("queue", "localhost", "--show-archived", "--json")
            return subprocess.CompletedProcess(
                [], 0, json.dumps([{"id": "jid", "scheduler_target": None}]), ""
            )

        monkeypatch.setattr(cli.transport, "run_remote_vq", fake_run)

        with pytest.raises(click.ClickException, match="multiple hosts"):
            cli._locate_job_host(cfg, "jid", multi_user=False)

    def test_scheduler_lanes_sharing_driver_read_queue_once(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """One physical driver snapshot classifies every logical lane."""
        monkeypatch.setenv("VQ_FANOUT_SERIAL", "1")
        scheduler_common = {
            "ssh": "scheduler-login",
            "scheduler": "slurm",
            "scheduler_dialect": "slurm",
            "scratch_root": "/scheduler/scratch",
            "scheduler_driver": "driver-box",
        }
        cfg = config.Config(
            default_host="driver-box",
            hosts={
                "driver-box": config.HostConfig(ssh="driver-box"),
                "host_c": config.HostConfig(**scheduler_common),
                "host_c-campaign": config.HostConfig(**scheduler_common),
            },
        )
        monkeypatch.setattr(cli.host_status, "load_down", lambda: {})
        calls: list[str] = []

        def fake_run(host_cfg, *args, **kwargs):  # type: ignore[no-untyped-def]
            calls.append(host_cfg.ssh)
            return subprocess.CompletedProcess(
                [],
                0,
                json.dumps(
                    [{"id": "jid", "scheduler_target": "host_c-campaign"}]
                ),
                "",
            )

        monkeypatch.setattr(cli.transport, "run_remote_vq", fake_run)

        assert (
            cli._locate_job_host(cfg, "jid", multi_user=False)
            == "host_c-campaign"
        )
        assert calls == ["driver-box"]

    def test_scheduler_lanes_sharing_driver_remain_distinct_owners(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Grouping the driver must not merge exact scheduler-target lanes."""
        scheduler_common = {
            "ssh": "scheduler-login",
            "scheduler": "slurm",
            "scheduler_dialect": "slurm",
            "scratch_root": "/scheduler/scratch",
            "scheduler_driver": "driver-box",
        }
        cfg = config.Config(
            hosts={
                "driver-box": config.HostConfig(ssh="driver-box"),
                "host_c": config.HostConfig(**scheduler_common),
                "host_c-campaign": config.HostConfig(**scheduler_common),
            },
        )
        monkeypatch.setattr(cli.host_status, "load_down", lambda: {})
        monkeypatch.setattr(
            cli,
            "_queue_rows_for_authority",
            lambda *args, **kwargs: [
                {"id": "jid", "scheduler_target": "host_c"},
                {"id": "jid", "scheduler_target": "host_c-campaign"},
            ],
        )

        with pytest.raises(click.ClickException, match="multiple hosts"):
            cli._locate_job_host(cfg, "jid", multi_user=False)

    def test_unconfigured_scheduler_target_blocks_a_different_owner(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A stale durable lane is ownership evidence, not an absent row."""
        scheduler = config.HostConfig(
            ssh="scheduler-login",
            scheduler="slurm",
            scheduler_dialect="slurm",
            scratch_root="/scheduler/scratch",
            scheduler_driver="driver-box",
        )
        cfg = config.Config(
            hosts={
                "driver-box": config.HostConfig(ssh="driver-box"),
                "host_f": scheduler,
                "owner-box": config.HostConfig(ssh="owner-box"),
            },
        )
        monkeypatch.setattr(cli.host_status, "load_down", lambda: {})

        def fake_rows(_cfg, host, *, multi_user):  # type: ignore[no-untyped-def]
            if host == "driver-box":
                return [{"id": "jid", "scheduler_target": "retired-lane"}]
            if host == "owner-box":
                return [{"id": "jid", "scheduler_target": None}]
            raise AssertionError(host)

        monkeypatch.setattr(cli, "_queue_rows_for_authority", fake_rows)

        with pytest.raises(
            click.ClickException,
            match="unconfigured scheduler_target.*retired-lane",
        ):
            cli._locate_job_host(cfg, "jid", multi_user=False)

    @pytest.mark.parametrize("skip_kind", ["down", "exclude"])
    def test_removed_scheduler_target_keeps_explicit_skip_semantics(
        self,
        monkeypatch: pytest.MonkeyPatch,
        skip_kind: str,
    ) -> None:
        """A retained skip marker is enough after a lane leaves config."""
        cfg = config.Config(
            hosts={
                "driver-box": config.HostConfig(ssh="driver-box"),
                "owner-box": config.HostConfig(ssh="owner-box"),
            },
        )
        down = (
            {"retired-lane": _down("retired-lane")}
            if skip_kind == "down"
            else {}
        )
        exclude = (
            frozenset({"retired-lane"})
            if skip_kind == "exclude"
            else frozenset()
        )
        monkeypatch.setattr(cli.host_status, "load_down", lambda: down)

        def fake_rows(_cfg, host, *, multi_user):  # type: ignore[no-untyped-def]
            if host == "driver-box":
                return [{"id": "jid", "scheduler_target": "retired-lane"}]
            if host == "owner-box":
                return [{"id": "jid", "scheduler_target": None}]
            raise AssertionError(host)

        monkeypatch.setattr(cli, "_queue_rows_for_authority", fake_rows)

        assert (
            cli._locate_job_host(
                cfg,
                "jid",
                multi_user=False,
                exclude=exclude,
            )
            == "owner-box"
        )

    def test_configured_down_scheduler_target_does_not_block_other_owner(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Known down lanes retain the established skip-and-route semantics."""
        scheduler = config.HostConfig(
            ssh="scheduler-login",
            scheduler="slurm",
            scheduler_dialect="slurm",
            scratch_root="/scheduler/scratch",
            scheduler_driver="driver-box",
        )
        cfg = config.Config(
            hosts={
                "driver-box": config.HostConfig(ssh="driver-box"),
                "host_f": scheduler,
                "owner-box": config.HostConfig(ssh="owner-box"),
            },
        )
        monkeypatch.setattr(
            cli.host_status,
            "load_down",
            lambda: {"host_f": _down("host_f")},
        )

        def fake_rows(_cfg, host, *, multi_user):  # type: ignore[no-untyped-def]
            if host == "driver-box":
                return [{"id": "jid", "scheduler_target": "host_f"}]
            if host == "owner-box":
                return [{"id": "jid", "scheduler_target": None}]
            raise AssertionError(host)

        monkeypatch.setattr(cli, "_queue_rows_for_authority", fake_rows)

        assert cli._locate_job_host(cfg, "jid", multi_user=False) == "owner-box"

    def test_unenrolled_local_direct_row_is_a_locatable_owner(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Reading a local scheduler driver also exposes its direct lane."""
        cfg = config.Config(
            hosts={
                "host_f": config.HostConfig(
                    ssh="scheduler-login",
                    scheduler="pbs",
                    scheduler_dialect="torque",
                    scratch_root="/scheduler/scratch",
                    scheduler_driver="localhost",
                ),
                "empty-box": config.HostConfig(ssh="empty-box"),
            },
        )
        direct_spec = type(
            "Spec", (), {"id": "jid", "scheduler_target": None}
        )()
        monkeypatch.setattr(cli.host_status, "load_down", lambda: {})
        monkeypatch.setattr(cli, "list_jobs", lambda *args, **kwargs: [direct_spec])
        monkeypatch.setattr(
            cli.transport,
            "run_remote_vq",
            lambda *args, **kwargs: subprocess.CompletedProcess([], 0, "[]", ""),
        )

        assert cli._locate_job_host(cfg, "jid", multi_user=False) == "localhost"

    def test_unenrolled_local_direct_duplicate_refuses_remote_owner(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An implicit local row cannot be ignored in favour of a remote row."""
        cfg = config.Config(
            hosts={
                "host_f": config.HostConfig(
                    ssh="scheduler-login",
                    scheduler="pbs",
                    scheduler_dialect="torque",
                    scratch_root="/scheduler/scratch",
                    scheduler_driver="localhost",
                ),
                "owner-box": config.HostConfig(ssh="owner-box"),
            },
        )
        direct_spec = type(
            "Spec", (), {"id": "jid", "scheduler_target": None}
        )()
        monkeypatch.setattr(cli.host_status, "load_down", lambda: {})
        monkeypatch.setattr(cli, "list_jobs", lambda *args, **kwargs: [direct_spec])
        monkeypatch.setattr(
            cli.transport,
            "run_remote_vq",
            lambda *args, **kwargs: subprocess.CompletedProcess(
                [],
                0,
                json.dumps([{"id": "jid", "scheduler_target": None}]),
                "",
            ),
        )

        with pytest.raises(click.ClickException, match="multiple hosts"):
            cli._locate_job_host(cfg, "jid", multi_user=False)

    def test_unreachable_host_in_fanout_does_not_block_locate(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """THE regression test: one host is unreachable in the fan-out, the
        job lives on a reachable host — the lookup must still return the
        reachable host rather than aborting on the unreachable one."""
        monkeypatch.setenv("VQ_FANOUT_SERIAL", "1")
        cfg = _make_cfg(["unreachable-box", "owner-box", "empty-box"])
        monkeypatch.setattr(cli.host_status, "load_down", lambda: {})

        def fake_rows(c, host, *, multi_user):
            if host == "unreachable-box":
                return None
            if host == "owner-box":
                return [{"id": "jid", "scheduler_target": None}]
            return []

        monkeypatch.setattr(cli, "_queue_rows_for_authority", fake_rows)
        assert cli._locate_job_host(cfg, "jid", multi_user=False) == "owner-box"

    def test_admin_down_host_is_never_probed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A ``vq host down`` host is skipped entirely — never probed (so a
        dead box doesn't even cost a ConnectTimeout)."""
        monkeypatch.setenv("VQ_FANOUT_SERIAL", "1")
        cfg = _make_cfg(["down-box", "owner-box", "empty-box"])
        monkeypatch.setattr(cli.host_status, "load_down", lambda: {"down-box": _down("down-box")})
        probed: list[str] = []

        def fake_rows(c, host, *, multi_user):
            probed.append(host)
            if host == "owner-box":
                return [{"id": "jid", "scheduler_target": None}]
            return []

        monkeypatch.setattr(cli, "_queue_rows_for_authority", fake_rows)
        assert cli._locate_job_host(cfg, "jid", multi_user=False) == "owner-box"
        assert "down-box" not in probed
        assert set(probed) == {"owner-box", "empty-box"}

    def test_scheduler_alias_is_skipped_when_its_driver_is_down(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cfg = config.Config(
            hosts={
                "driver-box": config.HostConfig(ssh="driver-box"),
                "host_f": config.HostConfig(
                    ssh="scheduler-login",
                    scheduler="pbs",
                    scheduler_dialect="torque",
                    scratch_root="/scheduler/scratch",
                    scheduler_driver="driver-box",
                ),
            }
        )
        monkeypatch.setattr(
            cli.host_status,
            "load_down",
            lambda: {"driver-box": _down("driver-box")},
        )

        def boom(*args, **kwargs):  # type: ignore[no-untyped-def]
            raise AssertionError("a down scheduler driver must not be probed")

        monkeypatch.setattr(cli, "_queue_rows_for_authority", boom)

        with pytest.raises(click.ClickException, match="no reachable host"):
            cli._locate_job_host(cfg, "jid", multi_user=False)

    def test_not_found_anywhere_lists_skipped_down(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("VQ_FANOUT_SERIAL", "1")
        cfg = _make_cfg(["down-box", "owner-box"])
        monkeypatch.setattr(cli.host_status, "load_down", lambda: {"down-box": _down("down-box")})
        monkeypatch.setattr(
            cli, "_queue_rows_for_authority", lambda c, h, *, multi_user: []
        )
        with pytest.raises(click.ClickException) as ei:
            cli._locate_job_host(cfg, "jid", multi_user=False)
        msg = ei.value.message
        assert "not found on any reachable host" in msg
        # The operator is told the down box was skipped (it may be there).
        assert "down-box" in msg

    def test_not_found_lists_unreachable(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("VQ_FANOUT_SERIAL", "1")
        cfg = _make_cfg(["flaky-box", "owner-box"])
        monkeypatch.setattr(cli.host_status, "load_down", lambda: {})
        monkeypatch.setattr(
            cli,
            "_queue_rows_for_authority",
            lambda c, h, *, multi_user: None if h == "flaky-box" else [],
        )
        with pytest.raises(click.ClickException) as ei:
            cli._locate_job_host(cfg, "jid", multi_user=False)
        assert "unreachable: flaky-box" in ei.value.message

    def test_ambiguous_multiple_owners_refuses_to_guess(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("VQ_FANOUT_SERIAL", "1")
        cfg = _make_cfg(["a-box", "b-box"])
        monkeypatch.setattr(cli.host_status, "load_down", lambda: {})
        monkeypatch.setattr(
            cli,
            "_queue_rows_for_authority",
            lambda c, h, *, multi_user: [
                {"id": "jid", "scheduler_target": None}
            ],
        )
        with pytest.raises(click.ClickException) as ei:
            cli._locate_job_host(cfg, "jid", multi_user=False)
        assert "multiple hosts" in ei.value.message

    def test_all_candidates_down_raises_no_reachable(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cfg = _make_cfg(["down-box"])
        monkeypatch.setattr(cli.host_status, "load_down", lambda: {"down-box": _down("down-box")})
        with pytest.raises(click.ClickException) as ei:
            cli._locate_job_host(cfg, "jid", multi_user=False)
        assert "no reachable host" in ei.value.message


# ----------------------------------------------------------------------
# _resolve_job_host — the resolution seam every per-job verb shares
# ----------------------------------------------------------------------


class TestResolveJobHost:
    def test_status_opt_in_locates_even_when_default_is_not_marked_down(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cfg = _make_cfg(["host_a-test", "owner-box"])
        monkeypatch.setattr(cli.host_status, "is_down", lambda _host: None)

        def probe_boom(*args, **kwargs):  # type: ignore[no-untyped-def]
            raise AssertionError("status must use the full ownership locator")

        monkeypatch.setattr(
            cli,
            "_require_inferred_default_queue_snapshot",
            probe_boom,
        )
        monkeypatch.setattr(
            cli,
            "_locate_job_host",
            lambda c, j, *, multi_user: "owner-box",
        )

        assert (
            cli._resolve_job_host(
                cfg,
                None,
                "jid",
                multi_user=False,
                locate_when_default_up=True,
            )
            == "owner-box"
        )

    def test_status_keeps_unconfigured_local_default_as_authority(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The current process owns localhost; it needs no fleet enrollment."""
        cfg = config.Config(default_host="localhost", hosts={})
        monkeypatch.setattr(cli.host_status, "is_down", lambda _host: None)

        def boom(*args, **kwargs):  # type: ignore[no-untyped-def]
            raise AssertionError("local status must not fan out")

        monkeypatch.setattr(cli, "_locate_job_host", boom)
        monkeypatch.setattr(
            cli,
            "_require_inferred_default_queue_snapshot",
            boom,
        )

        assert (
            cli._resolve_job_host(
                cfg,
                None,
                "jid",
                multi_user=False,
                locate_when_default_up=True,
            )
            == "localhost"
        )

    def test_explicit_host_passthrough_even_if_down(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An explicitly named host is honoured verbatim — we never reroute
        a target the operator typed, even a down one."""
        cfg = _make_cfg(["host_a-test"])
        monkeypatch.setattr(cli.host_status, "is_down", lambda h: _down(h))

        def boom(*a, **k):
            raise AssertionError("explicit host must not trigger a probe")

        monkeypatch.setattr(cli, "_locate_job_host", boom)
        monkeypatch.setattr(
            cli,
            "_require_inferred_default_queue_snapshot",
            boom,
        )
        assert cli._resolve_job_host(cfg, "host_d-test", "jid", multi_user=False) == "host_d-test"

    @pytest.mark.parametrize(
        "rows",
        [
            pytest.param(
                [{"id": "jid", "scheduler_target": None}],
                id="found",
            ),
            pytest.param([], id="absent"),
            pytest.param(
                [
                    {"id": "jid", "scheduler_target": None},
                    {"id": "jid", "scheduler_target": None},
                ],
                id="ambiguous",
            ),
        ],
    )
    def test_reachable_remote_default_preserves_default_ownership(
        self, monkeypatch: pytest.MonkeyPatch, rows: list[dict[str, object]]
    ) -> None:
        cfg = _make_cfg(["host_a-test", "host_d-test"])  # default_host = host_a-test
        monkeypatch.setattr(cli.host_status, "is_down", lambda h: None)
        probes: list[tuple[str, bool]] = []

        def probe(c, host, *, multi_user):  # type: ignore[no-untyped-def]
            assert c is cfg
            probes.append((host, multi_user))
            return rows

        def locate_boom(*a, **k):  # type: ignore[no-untyped-def]
            raise AssertionError("a reachable default must not fan out")

        monkeypatch.setattr(cli, "_queue_rows_for_authority", probe)
        monkeypatch.setattr(cli, "_locate_job_host", locate_boom)
        assert cli._resolve_job_host(cfg, None, "jid", multi_user=False) == "host_a-test"
        assert probes == [("host_a-test", False)]

    def test_unreachable_remote_default_fails_before_action(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cfg = _make_cfg(["retired-test"])
        monkeypatch.setattr(cli.host_status, "is_down", lambda _host: None)
        monkeypatch.setattr(
            cli,
            "_queue_rows_for_authority",
            lambda *args, **kwargs: None,
        )

        with pytest.raises(click.ClickException) as exc_info:
            cli._resolve_job_host(cfg, None, "jid", multi_user=False)

        assert exc_info.value.message == (
            "configured default_host 'retired-test' is unavailable or did "
            "not return a valid queue listing; name a host explicitly or "
            "update default_host"
        )

    def test_scheduler_default_preflights_driver_and_returns_public_lane(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cfg = config.Config(
            default_host="scheduler-test",
            hosts={
                "driver-test": config.HostConfig(ssh="driver-test"),
                "scheduler-test": config.HostConfig(
                    ssh="scheduler-login",
                    scheduler="slurm",
                    scheduler_dialect="slurm",
                    scratch_root="/scheduler/scratch",
                    scheduler_driver="driver-test",
                ),
            },
        )
        monkeypatch.setattr(cli.host_status, "is_down", lambda _host: None)
        probes: list[str] = []

        def probe(_cfg, host, *, multi_user):  # type: ignore[no-untyped-def]
            assert multi_user is False
            probes.append(host)
            return []

        monkeypatch.setattr(cli, "_queue_rows_for_authority", probe)

        assert (
            cli._resolve_job_host(cfg, None, "jid", multi_user=False)
            == "scheduler-test"
        )
        assert probes == ["driver-test"]

    def test_scheduler_default_with_local_driver_needs_no_remote_probe(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cfg = config.Config(
            default_host="scheduler-test",
            hosts={
                "scheduler-test": config.HostConfig(
                    ssh="scheduler-login",
                    scheduler="slurm",
                    scheduler_dialect="slurm",
                    scratch_root="/scheduler/scratch",
                    scheduler_driver="localhost",
                ),
            },
        )
        monkeypatch.setattr(cli.host_status, "is_down", lambda _host: None)

        def probe_boom(*args, **kwargs):  # type: ignore[no-untyped-def]
            raise AssertionError("a local action queue needs no remote probe")

        monkeypatch.setattr(cli, "_queue_rows_for_authority", probe_boom)

        assert (
            cli._resolve_job_host(cfg, None, "jid", multi_user=False)
            == "scheduler-test"
        )

    def test_marked_down_scheduler_driver_is_not_probed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cfg = config.Config(
            default_host="scheduler-test",
            hosts={
                "driver-test": config.HostConfig(ssh="driver-test"),
                "scheduler-test": config.HostConfig(
                    ssh="scheduler-login",
                    scheduler="slurm",
                    scheduler_dialect="slurm",
                    scratch_root="/scheduler/scratch",
                    scheduler_driver="driver-test",
                ),
            },
        )
        monkeypatch.setattr(
            cli.host_status,
            "is_down",
            lambda host: _down(host) if host == "driver-test" else None,
        )

        def probe_boom(*args, **kwargs):  # type: ignore[no-untyped-def]
            raise AssertionError("a marked-down action queue must not be probed")

        monkeypatch.setattr(cli, "_queue_rows_for_authority", probe_boom)

        with pytest.raises(click.ClickException) as exc_info:
            cli._resolve_job_host(cfg, None, "jid", multi_user=False)

        assert "default_host 'scheduler-test'" in exc_info.value.message
        assert "queue authority 'driver-test'" in exc_info.value.message

    def test_unknown_scheduler_driver_keeps_precise_config_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cfg = config.Config(
            default_host="scheduler-test",
            hosts={
                "scheduler-test": config.HostConfig(
                    ssh="scheduler-login",
                    scheduler="slurm",
                    scheduler_dialect="slurm",
                    scratch_root="/scheduler/scratch",
                    scheduler_driver="missing-driver",
                ),
            },
        )
        monkeypatch.setattr(cli.host_status, "is_down", lambda _host: None)

        with pytest.raises(click.UsageError, match="names driver 'missing-driver'"):
            cli._resolve_job_host(cfg, None, "jid", multi_user=False)

    def test_fleet_alias_preflight_uses_the_action_endpoint(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cfg = config.Config(
            default_host="owner-alias",
            hosts={
                "owner-box": config.HostConfig(ssh="canonical-ssh"),
                "owner-alias": config.HostConfig(
                    ssh="alias-ssh",
                    fleet_role="alias",
                    fleet_canonical_host="owner-box",
                ),
            },
        )
        monkeypatch.setattr(cli.host_status, "is_down", lambda _host: None)
        calls: list[str] = []

        def fake_run(host_cfg, *args, **kwargs):  # type: ignore[no-untyped-def]
            calls.append(host_cfg.ssh)
            if host_cfg.ssh == "canonical-ssh":
                return subprocess.CompletedProcess(
                    [], 0, '[{"id":"jid","scheduler_target":null}]', ""
                )
            return subprocess.CompletedProcess([], 255, "", "alias unreachable")

        monkeypatch.setattr(cli.transport, "run_remote_vq", fake_run)

        with pytest.raises(click.ClickException, match="owner-alias.*unavailable"):
            cli._resolve_job_host(cfg, None, "jid", multi_user=False)

        assert calls == ["alias-ssh"]

    def test_scheduler_driver_alias_preflight_uses_the_action_endpoint(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cfg = config.Config(
            default_host="scheduler-test",
            hosts={
                "driver-box": config.HostConfig(ssh="canonical-driver-ssh"),
                "driver-alias": config.HostConfig(
                    ssh="driver-alias-ssh",
                    fleet_role="alias",
                    fleet_canonical_host="driver-box",
                ),
                "scheduler-test": config.HostConfig(
                    ssh="scheduler-login",
                    scheduler="slurm",
                    scheduler_dialect="slurm",
                    scratch_root="/scheduler/scratch",
                    scheduler_driver="driver-alias",
                ),
            },
        )
        monkeypatch.setattr(cli.host_status, "is_down", lambda _host: None)
        calls: list[str] = []

        def fake_run(host_cfg, *args, **kwargs):  # type: ignore[no-untyped-def]
            calls.append(host_cfg.ssh)
            if host_cfg.ssh == "canonical-driver-ssh":
                return subprocess.CompletedProcess(
                    [],
                    0,
                    '[{"id":"jid","scheduler_target":"scheduler-test"}]',
                    "",
                )
            return subprocess.CompletedProcess(
                [], 255, "", "driver alias unreachable"
            )

        monkeypatch.setattr(cli.transport, "run_remote_vq", fake_run)

        with pytest.raises(
            click.ClickException, match="scheduler-test.*unavailable"
        ):
            cli._resolve_job_host(cfg, None, "jid", multi_user=False)

        assert calls == ["driver-alias-ssh"]

    def test_down_default_triggers_locate(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cfg = _make_cfg(["host_d-test", "host_a-test"])  # default_host = host_d-test
        monkeypatch.setattr(
            cli.host_status, "is_down",
            lambda h: _down(h) if h == "host_d-test" else None,
        )

        def probe_boom(*args, **kwargs):  # type: ignore[no-untyped-def]
            raise AssertionError("a marked-down default must use the locator")

        monkeypatch.setattr(
            cli,
            "_require_inferred_default_queue_snapshot",
            probe_boom,
        )
        monkeypatch.setattr(cli, "_locate_job_host", lambda c, j, *, multi_user: "host_a-test")
        assert cli._resolve_job_host(cfg, None, "jid", multi_user=False) == "host_a-test"

    def test_no_default_no_host_raises_usage(self) -> None:
        cfg = config.Config(hosts={"host_a-test": config.HostConfig(ssh="host_a-test")})
        with pytest.raises(click.UsageError):
            cli._resolve_job_host(cfg, None, "jid", multi_user=False)


# ----------------------------------------------------------------------
# End-to-end through the CLI: `vq status JOBID` / `vq kill JOBID` with the
# default_host marked `vq host down`.
# ----------------------------------------------------------------------


@pytest.fixture
def fleet_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Hermetic config + state dir: default_host=host_d-test (down) plus a
    reachable host_a-test. Fan-out forced serial for deterministic stubbing."""
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    monkeypatch.setenv("VQ_FANOUT_SERIAL", "1")
    (tmp_path / "cfg").mkdir()
    (tmp_path / "cfg" / "config.toml").write_text(
        'default_host = "host_d-test"\n'
        "\n"
        "[hosts.host_d-test]\n"
        'ssh = "host_d-test"\n'
        'remote_vq = "vq"\n'
        "\n"
        "[hosts.host_a-test]\n"
        'ssh = "host_a-test"\n'
        'remote_vq = "vq"\n'
    )
    host_status.mark_down("host_d-test", "temporarily down")
    return tmp_path


class TestEndToEndReroute:
    @pytest.mark.parametrize("verb", ["logs", "kill", "fetch"])
    def test_unreachable_unmarked_default_blocks_per_job_action(
        self,
        verb: str,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """An inferred dead default reports its name before any action runs."""
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
        (tmp_path / "cfg").mkdir()
        (tmp_path / "cfg" / "config.toml").write_text(
            'default_host = "retired-test"\n'
            "[hosts.retired-test]\n"
            'ssh = "retired-test"\n'
            'remote_vq = "vq"\n'
        )
        calls: list[tuple[str, tuple[str, ...]]] = []

        def fake_run(host_cfg, *args, **kwargs):  # type: ignore[no-untyped-def]
            calls.append((host_cfg.ssh, args))
            if args == ("queue", "localhost", "--show-archived", "--json"):
                return subprocess.CompletedProcess(
                    [], 255, "", "ssh: Network is unreachable"
                )
            raise AssertionError(f"{verb} action ran after a failed preflight")

        def fetch_boom(*args, **kwargs):  # type: ignore[no-untyped-def]
            raise AssertionError("fetch action ran after a failed preflight")

        monkeypatch.setattr(cli.transport, "run_remote_vq", fake_run)
        monkeypatch.setattr(cli, "fetch_remote", fetch_boom)
        args = [verb, "jid"]
        if verb == "fetch":
            args.extend(["-o", str(tmp_path / "out")])

        result = CliRunner().invoke(main, args)

        assert result.exit_code == 1
        assert "configured default_host 'retired-test' is unavailable" in result.output
        assert "name a host explicitly or update default_host" in result.output
        assert "Network is unreachable" not in result.output
        assert calls == [
            ("retired-test", ("queue", "localhost", "--show-archived", "--json"))
        ]

    def test_status_unmarked_dead_default_locates_owner_from_queue(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Regression for IID 109: an unmarked dead host_a is never status-polled."""
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
        monkeypatch.setenv("VQ_FANOUT_SERIAL", "1")
        (tmp_path / "cfg").mkdir()
        (tmp_path / "cfg" / "config.toml").write_text(
            'default_host = "host_a-test"\n'
            "\n"
            "[hosts.host_a-test]\n"
            'ssh = "host_a-test"\n'
            'remote_vq = "vq"\n'
            "\n"
            "[hosts.owner-box]\n"
            'ssh = "owner-box"\n'
            'remote_vq = "vq"\n'
        )
        calls: list[tuple[str, tuple[str, ...]]] = []

        def fake_run(host_cfg, *args, check=True, **kwargs):  # type: ignore[no-untyped-def]
            calls.append((host_cfg.ssh, args))
            if args[0] == "queue":
                assert args == ("queue", "localhost", "--show-archived", "--json")
                if host_cfg.ssh == "host_a-test":
                    return subprocess.CompletedProcess([], 255, "", "host unreachable")
                return subprocess.CompletedProcess(
                    [],
                    0,
                    json.dumps([{"id": "jid", "scheduler_target": None}]),
                    "",
                )
            assert args == ("status", "localhost", "jid")
            if host_cfg.ssh == "host_a-test":
                raise cli.transport.RemoteError("remote vq failed (exit 255) on host_a-test")
            return subprocess.CompletedProcess([], 0, "state: running\n", "")

        monkeypatch.setattr(cli.transport, "run_remote_vq", fake_run)

        result = CliRunner().invoke(main, ["status", "jid"])

        assert result.exit_code == 0, result.output
        assert result.output == "state: running\n"
        status_hosts = [host for host, args in calls if args[0] == "status"]
        assert status_hosts == ["owner-box"]

    def test_explicit_status_host_remains_authoritative(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Negative control: a host supplied by the operator is never located."""
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
        (tmp_path / "cfg").mkdir()
        (tmp_path / "cfg" / "config.toml").write_text(
            'default_host = "owner-box"\n'
            "[hosts.host_a-test]\n"
            'ssh = "host_a-test"\n'
            "[hosts.owner-box]\n"
            'ssh = "owner-box"\n'
        )
        calls: list[tuple[str, tuple[str, ...]]] = []

        def fake_run(host_cfg, *args, **kwargs):  # type: ignore[no-untyped-def]
            calls.append((host_cfg.ssh, args))
            assert args == ("status", "localhost", "jid")
            return subprocess.CompletedProcess([], 0, "state: explicit\n", "")

        monkeypatch.setattr(cli.transport, "run_remote_vq", fake_run)

        result = CliRunner().invoke(main, ["status", "host_a-test", "jid"])

        assert result.exit_code == 0, result.output
        assert result.output == "state: explicit\n"
        assert calls == [("host_a-test", ("status", "localhost", "jid"))]

    def test_bare_scheduler_status_keeps_lane_handle_and_uses_driver(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Located scheduler aliases still use the established driver refresh path."""
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        monkeypatch.setenv("VQ_FANOUT_SERIAL", "1")
        (tmp_path / "cfg").mkdir()
        (tmp_path / "cfg" / "config.toml").write_text(
            'default_host = "host_a-test"\n'
            "[hosts.host_a-test]\n"
            'ssh = "host_a-test"\n'
            "[hosts.driver-box]\n"
            'ssh = "driver-box"\n'
            "[hosts.host_c]\n"
            'ssh = "scheduler-login"\n'
            'scheduler = "slurm"\n'
            'scheduler_dialect = "slurm"\n'
            'scratch_root = "/scheduler/scratch"\n'
            'scheduler_driver = "driver-box"\n'
            "[hosts.host_c-campaign]\n"
            'ssh = "scheduler-login"\n'
            'scheduler = "slurm"\n'
            'scheduler_dialect = "slurm"\n'
            'scratch_root = "/scheduler/scratch"\n'
            'scheduler_driver = "driver-box"\n'
        )
        calls: list[tuple[str, tuple[str, ...]]] = []

        def fake_run(host_cfg, *args, **kwargs):  # type: ignore[no-untyped-def]
            calls.append((host_cfg.ssh, args))
            if args[0] == "queue":
                if host_cfg.ssh == "host_a-test":
                    return subprocess.CompletedProcess([], 255, "", "host unreachable")
                return subprocess.CompletedProcess(
                    [],
                    0,
                    json.dumps(
                        [{"id": "jid", "scheduler_target": "host_c-campaign"}]
                    ),
                    "",
                )
            assert host_cfg.ssh == "driver-box"
            assert args == ("status", "localhost", "jid", "--json")
            return subprocess.CompletedProcess(
                [],
                0,
                json.dumps(
                    {
                        "id": "jid",
                        "state": "running",
                        "scheduler_target": "host_c-campaign",
                        "queue_handle": {
                            "job_id": "jid",
                            "host": "host_c-campaign",
                            "submitted_at": None,
                        },
                    }
                ),
                "",
            )

        monkeypatch.setattr(cli.transport, "run_remote_vq", fake_run)

        result = CliRunner().invoke(main, ["status", "jid", "--json"])

        assert result.exit_code == 0, result.output
        payload = json.loads(result.stdout)
        assert payload["scheduler_target"] == "host_c-campaign"
        assert payload["queue_handle"]["host"] == "host_c-campaign"
        status_calls = [(host, args) for host, args in calls if args[0] == "status"]
        assert status_calls == [
            ("driver-box", ("status", "localhost", "jid", "--json"))
        ]

    def test_located_owner_disappearing_is_not_called_the_default_host(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A queue/status race must not emit the old default-host hint."""
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
        (tmp_path / "cfg").mkdir()
        (tmp_path / "cfg" / "config.toml").write_text(
            'default_host = "host_a-test"\n'
            "[hosts.host_a-test]\n"
            'ssh = "host_a-test"\n'
            "[hosts.owner-box]\n"
            'ssh = "owner-box"\n'
        )
        monkeypatch.setattr(
            cli,
            "_locate_job_host",
            lambda *args, **kwargs: "owner-box",
        )

        def vanished(_host_cfg, *args, **kwargs):  # type: ignore[no-untyped-def]
            raise cli.transport.RemoteError(
                "remote vq failed (exit 2) on owner-box:\n"
                "  stderr: Error: no such job: jid"
            )

        monkeypatch.setattr(cli.transport, "run_remote_vq", vanished)

        result = CliRunner().invoke(main, ["status", "jid"])

        assert result.exit_code == 1
        assert "no such job: jid" in result.output
        assert "searched only" not in result.output
        assert "default_host" not in result.output

    def test_status_no_host_reroutes_around_down_default(
        self, fleet_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """`vq status abc...` (no host) finds the job on the reachable
        host_a-test and never contacts the down host_d-test."""

        def fake_run(cmd: list[str], **kw):
            host = cmd[-2]
            remote_cmd = cmd[-1]
            if host == "host_d-test":
                raise AssertionError("down default_host host_d-test was contacted")
            if host == "host_a-test":
                if " queue " in f" {remote_cmd} ":  # the durable locate probe
                    return subprocess.CompletedProcess(
                        cmd,
                        0,
                        '[{"id":"abc123def456","scheduler_target":null}]',
                        "",
                    )
                return subprocess.CompletedProcess(cmd, 0, "state: running\n", "")
            return subprocess.CompletedProcess(cmd, 2, "", "no such job")

        monkeypatch.setattr(cli.transport.subprocess, "run", fake_run)
        monkeypatch.setattr(cli.transport, "run_owned_subprocess", fake_run)
        result = CliRunner().invoke(main, ["status", "abc123def456"])
        assert result.exit_code == 0, result.output
        assert "running" in result.output

    def test_kill_no_host_acts_only_on_the_owning_host(
        self, fleet_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A mutating verb reroutes too, and the kill lands on exactly the
        host that owns the jobid — never broadcast, never on the down box."""
        contacted: list[tuple[str, str]] = []

        def fake_run(cmd: list[str], **kw):
            host = cmd[-2]
            remote_cmd = cmd[-1]
            contacted.append((host, remote_cmd))
            if host == "host_d-test":
                raise AssertionError("down default_host host_d-test was contacted")
            if host == "host_a-test":
                if " queue " in f" {remote_cmd} ":  # durable locate probe
                    return subprocess.CompletedProcess(
                        cmd,
                        0,
                        '[{"id":"abc123def456","scheduler_target":null}]',
                        "",
                    )
                return subprocess.CompletedProcess(cmd, 0, "killed: abc123def456\n", "")
            return subprocess.CompletedProcess(cmd, 2, "", "no such job")

        monkeypatch.setattr(cli.transport.subprocess, "run", fake_run)
        monkeypatch.setattr(cli.transport, "run_owned_subprocess", fake_run)
        result = CliRunner().invoke(main, ["kill", "abc123def456"])
        assert result.exit_code == 0, result.output
        assert "killed" in result.output
        # The kill (non-probe) call only ever went to host_a-test.
        kill_targets = {h for (h, rc) in contacted if "kill" in rc}
        assert kill_targets == {"host_a-test"}

    def test_fetch_no_host_retries_located_owner_after_default_miss(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A reachable default host can still be the wrong owner.

        Release-paper jobs on host_a reproduced this as ``vq status`` being able
        to locate the job while ``vq fetch JOBID --workdir`` delegated to the
        default host and got remote ``no such job``. The no-host fetch path now
        treats that miss as a locator trigger and retries the owning host.
        """
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
        (tmp_path / "cfg").mkdir()
        (tmp_path / "cfg" / "config.toml").write_text(
            'default_host = "host_d-test"\n'
            "\n"
            "[hosts.host_d-test]\n"
            'ssh = "host_d-test"\n'
            'remote_vq = "vq"\n'
            "\n"
            "[hosts.host_a-test]\n"
            'ssh = "host_a-test"\n'
            'remote_vq = "vq"\n'
        )

        contacted: list[str] = []

        def fake_fetch_workdir_remote(host_cfg, jobid, output_dir):
            contacted.append(host_cfg.ssh)
            if host_cfg.ssh == "host_d-test":
                raise cli.transport.RemoteError("remote stderr: no such job")
            dst = output_dir / f"job-{jobid}-workdir"
            sidecar = dst / "_vq" / "fetch-manifest.json"
            sidecar.parent.mkdir(parents=True)
            fetched_at = "2026-08-25T12:34:56+00:00"
            sidecar.write_text(
                json.dumps(
                    {
                        "schema": "vq.fetch-manifest.v1",
                        "jobid": jobid,
                        "job_name": "job",
                        "fetched_at": fetched_at,
                        "refresh_attempted_at": fetched_at,
                        "source_host": host_cfg.ssh,
                        "source_kind": "workdir",
                        "source_path": None,
                        "transport": "ssh-stream",
                        "stale": False,
                        "refresh_error": None,
                    }
                )
            )
            return dst

        monkeypatch.setattr(cli, "fetch_workdir_remote", fake_fetch_workdir_remote)
        monkeypatch.setattr(
            cli,
            "_queue_rows_for_authority",
            lambda *args, **kwargs: [],
        )
        monkeypatch.setattr(
            cli,
            "_locate_job_host",
            lambda c, j, *, multi_user, exclude=frozenset(): "host_a-test",
        )

        result = CliRunner().invoke(
            main,
            ["fetch", "abc123def456", "--workdir", "-o", str(tmp_path / "out")],
        )

        assert result.exit_code == 0, result.output
        assert contacted == ["host_d-test", "host_a-test"]
        assert "host_a-test" in result.output

    def test_status_not_found_anywhere_errors_cleanly(
        self, fleet_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When no reachable host owns the job, the command fails with a
        clear message (not an SSH stacktrace) and names the skipped down box."""

        def fake_run(cmd: list[str], **kw):
            host = cmd[-2]
            if host == "host_d-test":
                raise AssertionError("down default_host host_d-test was contacted")
            return subprocess.CompletedProcess(cmd, 2, "", "no such job")

        monkeypatch.setattr(cli.transport.subprocess, "run", fake_run)
        result = CliRunner().invoke(main, ["status", "abc123def456"])
        assert result.exit_code != 0
        assert "not found on any reachable host" in result.output
        assert "host_d-test" in result.output
