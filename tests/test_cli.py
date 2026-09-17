"""CLI behavior: smoke tests + submit/queue integration via CliRunner."""
from __future__ import annotations

import io
import json
import os
import shlex
import subprocess
import sys
import tarfile
from datetime import UTC
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import admin_detached, capacity, cli, config, host_status, paths, transport
from vq import submit as submit_module
from vq.cli import main
from vq.spec import JobSpec, JobState

FULL_SHA = "a" * 40


@pytest.fixture
def cli_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Redirect vq state and config into tmp_path so tests are hermetic.

    Without the config-dir override, tests would read the user's real
    ~/.config/vq/config.toml -- so a developer-supplied default_host or
    [hosts.X] would change behavior under their feet.
    """
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    return tmp_path


@pytest.fixture
def cli_state_with_default(cli_state: Path) -> Path:
    """cli_state plus a config.toml with default_host = "remote-mock"."""
    (cli_state / "cfg" / "config.toml").write_text(
        'default_host = "remote-mock"\n'
        '\n'
        '[hosts.remote-mock]\n'
        'ssh = "remote-mock"\n'
    )
    return cli_state


def _init_git_repo(path: Path) -> str:
    path.mkdir()
    subprocess.run(["git", "init"], cwd=path, check=True, capture_output=True)
    (path / "README.md").write_text("test repo\n", encoding="utf-8")
    subprocess.run(["git", "add", "README.md"], cwd=path, check=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=vq tests",
            "-c",
            "user.email=vq-tests@example.invalid",
            "commit",
            "-m",
            "seed",
        ],
        cwd=path,
        check=True,
        capture_output=True,
    )
    return subprocess.run(
        ["git", "rev-parse", "--short=12", "HEAD"],
        cwd=path,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _write_test_fetch_manifest(
    dst: Path,
    jobid: str,
    *,
    source_host: str = "driver.invalid",
    source_kind: str = "workspace",
) -> None:
    fetched_at = "2026-08-25T12:34:56+00:00"
    sidecar = dst / "_vq" / "fetch-manifest.json"
    sidecar.parent.mkdir(parents=True, exist_ok=True)
    sidecar.write_text(
        json.dumps(
            {
                "schema": "vq.fetch-manifest.v1",
                "jobid": jobid,
                "job_name": None,
                "fetched_at": fetched_at,
                "refresh_attempted_at": fetched_at,
                "source_host": source_host,
                "source_kind": source_kind,
                "source_path": None,
                "transport": "ssh-stream",
                "stale": False,
                "refresh_error": None,
            }
        )
    )


class TestHelp:
    def test_top_level_lists_all_verbs(self) -> None:
        result = CliRunner().invoke(main, ["--help"])
        assert result.exit_code == 0
        for verb in (
            "submit", "queue", "status", "kill",
            "pause", "resume",  # v0.5.1
            "daemon", "fetch", "web",
        ):
            assert verb in result.output

    def test_web_subgroup_lists_run_and_init_token(self) -> None:
        result = CliRunner().invoke(main, ["web", "--help"])
        assert result.exit_code == 0
        assert "run" in result.output
        assert "init-token" in result.output

    def test_daemon_subgroup_lists_verbs(self) -> None:
        result = CliRunner().invoke(main, ["daemon", "--help"])
        assert result.exit_code == 0
        for verb in ("start", "stop", "status"):
            assert verb in result.output

    def test_version_flag(self) -> None:
        # Assert against the package's own version rather than a literal: vq is
        # versioned independently of vibe-qc and is bumped by whichever dev
        # chat lands the work, so a hardcoded string turns every routine bump
        # into a spurious test failure.
        from vq import __version__

        result = CliRunner().invoke(main, ["--version"])
        assert result.exit_code == 0
        assert __version__ in result.output


class TestSubmitIdempotency:
    def test_cli_same_key_returns_original_job(self, cli_state: Path) -> None:
        source = cli_state / "input.py"
        source.write_text("pass\n")
        runner = CliRunner()

        first = runner.invoke(
            main,
            [
                "submit",
                "localhost",
                "--idempotency-key",
                "cli-receipt-1",
                str(source),
            ],
        )
        second = runner.invoke(
            main,
            [
                "submit",
                "localhost",
                "--idempotency-key",
                "cli-receipt-1",
                str(source),
            ],
        )

        assert first.exit_code == 0, first.output
        assert second.exit_code == 0, second.output
        assert second.output == first.output
        assert len(list((cli_state / "state" / "queue").glob("*.json"))) == 1

    def test_cli_json_replay_keeps_capacity_warning_in_receipt(
        self, cli_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            submit_module.capacity,
            "read_daemon_capacity",
            lambda **_kwargs: capacity.DaemonCapacity(
                max_cpus=4,
                written_at="2026-08-20T12:00:00+00:00",
            ),
        )
        source = cli_state / "too-wide.py"
        source.write_text("pass\n")
        args = [
            "submit",
            "localhost",
            "--idempotency-key",
            "cli-over-cap-replay",
            "--cpus",
            "8",
            "--json",
            str(source),
        ]
        runner = CliRunner()

        first = runner.invoke(main, args)
        replay = runner.invoke(main, args)

        assert first.exit_code == 0, first.output
        assert replay.exit_code == 0, replay.output
        first_receipt = json.loads(first.stdout)
        replay_receipt = json.loads(replay.stdout)
        assert replay_receipt["jobids"] == first_receipt["jobids"]
        assert replay_receipt["capacity_warnings"] == first_receipt[
            "capacity_warnings"
        ]
        assert len(replay_receipt["capacity_warnings"]) == 1
        assert replay.stderr.startswith("vq: warning: requested 8 CPUs")

    def test_cli_key_rejects_array_before_queue_mutation(
        self, cli_state: Path
    ) -> None:
        source = cli_state / "input.py"
        source.write_text("pass\n")

        result = CliRunner().invoke(
            main,
            [
                "submit",
                "localhost",
                "--idempotency-key",
                "single-only",
                "--array",
                "2",
                str(source),
            ],
        )

        assert result.exit_code == 2
        assert "single logical job" in result.output
        assert not (cli_state / "state" / "queue").exists()


class TestSourceShaCLI:
    def test_write_and_read_source_sha_marker(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        marker = tmp_path / "vq" / "SOURCE-SHA"
        monkeypatch.setattr(
            "vq.cli.admin_module.source_sha_marker_path",
            lambda: marker,
        )

        write = CliRunner().invoke(main, ["source-sha", "--write-marker", FULL_SHA])
        assert write.exit_code == 0, write.output
        assert marker.read_text(encoding="utf-8") == FULL_SHA + "\n"
        assert FULL_SHA in write.output

        read = CliRunner().invoke(main, ["source-sha"])
        assert read.exit_code == 0, read.output
        assert read.output.strip() == FULL_SHA

    def test_source_tree_sha256_prints_content_digest(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        digest = "12" * 32
        monkeypatch.setattr("vq.cli.admin_module.source_tree_sha256", lambda: digest)

        result = CliRunner().invoke(main, ["source-tree-sha256"])

        assert result.exit_code == 0, result.output
        assert result.output.strip() == digest

    def test_source_stage_prune_emits_structured_cleanup_result(
        self, tmp_path: Path
    ) -> None:
        stage_root = tmp_path / "stage"
        generations = stage_root / "generations"
        generations.mkdir(parents=True)
        older = generations / f"{'a' * 40}-{'1' * 32}"
        current = generations / f"{'b' * 40}-{'2' * 32}"
        older.mkdir()
        current.mkdir()
        os.utime(older, (100, 100))
        os.utime(current, (200, 200))

        result = CliRunner().invoke(
            main,
            [
                "source-stage-prune",
                str(stage_root),
                "--keep",
                "1",
                "--preserve",
                str(current),
                "--json",
            ],
        )

        assert result.exit_code == 0, result.output
        payload = json.loads(result.stdout)
        assert payload["removed"] == [str(older)]
        assert payload["retained"] == [str(current)]
        assert current.is_dir()
        assert not older.exists()


class TestDaemonStatusCLI:
    def test_status_trusts_rpc_when_pidfile_missing(
        self, cli_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr("vq.rpc.ping", lambda **kw: {"version": "9.9.9"})

        result = CliRunner().invoke(main, ["daemon", "status"])

        assert result.exit_code == 0, result.output
        assert "daemon: running (RPC healthy; no pidfile)" in result.output
        assert "daemon: not running" not in result.output


class TestSubmitCLI:
    def test_single_file_submit_prints_jobid(self, cli_state: Path) -> None:
        src = cli_state / "input.py"
        src.write_text("print('hi')")
        result = CliRunner().invoke(main, ["submit", "localhost", str(src)])
        assert result.exit_code == 0, result.output
        jobid = result.output.strip()
        assert len(jobid) == 12
        spec_path = paths.queue_dir() / f"{jobid}.json"
        assert spec_path.exists()

    def test_impossible_cpu_request_warns_without_changing_jobid_stdout(
        self, cli_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            submit_module.capacity,
            "read_daemon_capacity",
            lambda **_kwargs: capacity.DaemonCapacity(
                max_cpus=4,
                max_mem_mb=8192,
                written_at="2026-08-10T12:00:00+00:00",
            ),
        )
        src = cli_state / "too-wide.py"
        src.write_text("pass\n")

        result = CliRunner().invoke(
            main,
            ["submit", "localhost", "--cpus", "8", str(src)],
        )

        assert result.exit_code == 0, result.output
        jobid = result.stdout.strip()
        assert len(jobid) == 12
        assert result.stdout == f"{jobid}\n"
        assert result.stderr == (
            "vq: warning: requested 8 CPUs but this daemon caps at "
            f"--max-cpus 4; job {jobid} will park PENDING until the daemon "
            "is restarted with a higher cap\n"
        )
        assert (paths.queue_dir() / f"{jobid}.json").exists()

    def test_impossible_memory_warning_keeps_json_stdout_parseable(
        self, cli_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            submit_module.capacity,
            "read_daemon_capacity",
            lambda **_kwargs: capacity.DaemonCapacity(
                max_cpus=16,
                max_mem_mb=1024,
                written_at="2026-08-10T12:00:00+00:00",
            ),
        )
        src = cli_state / "too-large.py"
        src.write_text("pass\n")

        result = CliRunner().invoke(
            main,
            [
                "submit",
                "localhost",
                "--mem-mb",
                "2048",
                "--json",
                str(src),
            ],
        )

        assert result.exit_code == 0, result.output
        receipt = json.loads(result.stdout)
        [jobid] = receipt["jobids"]
        assert receipt["host"] == "localhost"
        warning = (
            "requested 2048 MB memory but this daemon caps at "
            f"--max-mem-mb 1024 MB; job {jobid} will park PENDING until the "
            "daemon is restarted with a higher cap"
        )
        assert receipt["capacity_warnings"] == [warning]
        assert result.stderr == (
            "vq: warning: requested 2048 MB memory but this daemon caps at "
            f"--max-mem-mb 1024 MB; job {jobid} will park PENDING until the "
            "daemon is restarted with a higher cap\n"
        )
        assert (paths.queue_dir() / f"{jobid}.json").exists()

        listing = CliRunner().invoke(
            main,
            ["list", "localhost", "--json"],
        )
        assert listing.exit_code == 0, listing.output
        row = next(
            item for item in json.loads(listing.stdout) if item["id"] == jobid
        )
        assert row["state"] == "pending"
        assert row["pending_over_capacity"] is True
        assert row["configured_capacity_overages"] == [
            {
                "resource": "memory",
                "requested": 2048,
                "limit": 1024,
                "uses_default": False,
            }
        ]

        text_listing = CliRunner().invoke(main, ["list", "localhost"])
        assert text_listing.exit_code == 0, text_listing.output
        assert "pending (over cap)" in text_listing.stdout

    def test_queue_json_capacity_classification_is_tristate(
        self, cli_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            capacity,
            "read_daemon_capacity",
            lambda **_kwargs: None,
        )
        src = cli_state / "unknown-capacity.py"
        src.write_text("pass\n")
        submitted = CliRunner().invoke(
            main,
            ["submit", "localhost", "--cpus", "9", str(src)],
        )
        assert submitted.exit_code == 0, submitted.output

        listing = CliRunner().invoke(main, ["list", "localhost", "--json"])

        assert listing.exit_code == 0, listing.output
        [row] = json.loads(listing.stdout)
        assert row["pending_over_capacity"] is None
        assert row["configured_capacity_overages"] == []

        monkeypatch.setattr(
            capacity,
            "read_daemon_capacity",
            lambda **_kwargs: capacity.DaemonCapacity.model_validate(
                {
                    "max_cpus": 8,
                    "max_mem_mb": 4_000,
                    "written_at": "2026-08-20T12:00:00+00:00",
                }
            ),
        )
        provable_listing = CliRunner().invoke(
            main,
            ["list", "localhost", "--json"],
        )

        assert provable_listing.exit_code == 0, provable_listing.output
        [provable_row] = json.loads(provable_listing.stdout)
        assert provable_row["pending_over_capacity"] is True
        assert provable_row["configured_capacity_overages"][0]["resource"] == (
            "cpus"
        )

        monkeypatch.setattr(
            capacity,
            "read_daemon_capacity",
            lambda **_kwargs: capacity.DaemonCapacity(
                max_cpus=16,
                max_mem_mb=4_000,
                default_job_mem_mb=None,
                written_at="2026-08-20T12:00:00+00:00",
            ),
        )
        known_listing = CliRunner().invoke(
            main,
            ["list", "localhost", "--json"],
        )

        assert known_listing.exit_code == 0, known_listing.output
        [known_row] = json.loads(known_listing.stdout)
        assert known_row["pending_over_capacity"] is False
        assert known_row["configured_capacity_overages"] == []

    def test_remote_capacity_warning_is_in_json_acceptance_receipt(
        self, cli_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (cli_state / "cfg" / "config.toml").write_text(
            "[hosts.host_d]\n"
            'ssh = "host_d"\n'
        )
        warning = (
            "requested 32 CPUs but this daemon caps at --max-cpus 16; "
            "job abc123def456 will park PENDING until the daemon is "
            "restarted with a higher cap"
        )

        def fake_submit_remote(**kwargs: object) -> list[str]:
            warning_sink = kwargs["warning_sink"]
            assert callable(warning_sink)
            warning_sink(warning)
            return ["abc123def456"]

        monkeypatch.setattr(
            submit_module,
            "submit_remote",
            fake_submit_remote,
        )
        source = cli_state / "remote-wide.py"
        source.write_text("pass\n")

        result = CliRunner().invoke(
            main,
            ["submit", "--host", "host_d", "--json", str(source)],
        )

        assert result.exit_code == 0, result.output
        assert json.loads(result.stdout)["capacity_warnings"] == [warning]
        assert result.stderr == f"vq: warning: {warning}\n"

    def _scheduler_cfg(self, cli_state: Path) -> None:
        # host_f is a scheduler host driven by the "drv" daemon (design doc §17).
        (cli_state / "cfg" / "config.toml").write_text(
            "[hosts.host_f]\n"
            'ssh = "host_f"\n'
            'scheduler = "pbs"\n'
            'scheduler_dialect = "torque"\n'
            'scratch_root = "/home/USER"\n'
            'scheduler_driver = "drv"\n'
            "\n"
            "[hosts.drv]\n"
            'ssh = "drv-ssh"\n'
        )

    def test_scheduler_host_forwards_to_driver(
        self, cli_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._scheduler_cfg(cli_state)
        captured: dict[str, object] = {}

        def fake_submit_remote(**kw: object) -> list[str]:
            captured.update(kw)
            return ["jid123abc456"]

        monkeypatch.setattr(submit_module, "submit_remote", fake_submit_remote)
        src = cli_state / "in.py"
        src.write_text("print('hi')")
        result = CliRunner().invoke(
            main,
            [
                "submit",
                "--host",
                "host_f",
                "--program",
                "orca",
                "--ntasks",
                "2",
                str(src),
            ],
        )
        assert result.exit_code == 0, result.output
        # Forwarded to the DRIVER, tagged with the cluster as scheduler_target.
        assert captured["scheduler_target"] == "host_f"
        assert captured["host"] == "drv"
        assert captured["host_cfg"].ssh == "drv-ssh"  # type: ignore[attr-defined]
        assert captured["program"] == "orca"
        assert captured["scheduler_tasks"] == 2

    def test_remote_outcome_unknown_is_typed_json_and_not_retryable(
        self, cli_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._scheduler_cfg(cli_state)

        def ambiguous_submit(**_kwargs: object) -> list[str]:
            raise transport.RemoteOutcomeUnknown("submit observer lost")

        monkeypatch.setattr(submit_module, "submit_remote", ambiguous_submit)
        source = cli_state / "ambiguous.py"
        source.write_text("pass\n")

        result = CliRunner().invoke(
            main,
            ["submit", "--host", "host_f", "--json", str(source)],
        )

        assert result.exit_code != 0
        payload = json.loads(result.stdout)
        assert payload == {
            "error": {
                "message": "submit observer lost",
                "retry_safe": False,
                "type": "remote_outcome_unknown",
            },
            "host": "host_f",
            "jobids": [],
            "outcome": "unknown",
        }
        assert result.stderr == ""

    @pytest.mark.parametrize("submit_json", [False, True], ids=["plain", "json"])
    def test_remote_acceptance_survives_receipt_enrichment_interrupt(
        self,
        cli_state: Path,
        monkeypatch: pytest.MonkeyPatch,
        submit_json: bool,
    ) -> None:
        self._scheduler_cfg(cli_state)
        monkeypatch.setattr(
            submit_module,
            "submit_remote",
            lambda **_kwargs: ["abc123def456"],
        )
        monkeypatch.setattr(
            "vq.cli.drain_module.read_effective_drain_state",
            lambda: (_ for _ in ()).throw(
                KeyboardInterrupt("receipt enrichment interrupted")
            ),
        )
        source = cli_state / "accepted.py"
        source.write_text("pass\n")
        args = ["submit", "--host", "host_f"]
        if submit_json:
            args.append("--json")
        args.append(str(source))

        result = CliRunner().invoke(main, args)

        assert result.exit_code == 0, result.output
        if submit_json:
            assert json.loads(result.stdout)["jobids"] == ["abc123def456"]
        else:
            assert result.stdout == "abc123def456\n"

    def test_scheduler_host_with_local_driver_submits_locally(
        self, cli_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (cli_state / "cfg" / "config.toml").write_text(
            "[hosts.host_f]\n"
            'ssh = "host_f"\n'
            'scheduler = "pbs"\n'
            'scheduler_dialect = "torque"\n'
            'scratch_root = "/home/USER"\n'
            'scheduler_driver = "localhost"\n'
        )
        captured: dict[str, object] = {}

        def fake_submit_local(**kw: object) -> str:
            captured.update(kw)
            return "localjid123"

        def fail_submit_remote(**_kw: object) -> list[str]:
            raise AssertionError("local scheduler drivers must not use submit_remote")

        monkeypatch.setattr(submit_module, "submit_local", fake_submit_local)
        monkeypatch.setattr(submit_module, "submit_remote", fail_submit_remote)
        src = cli_state / "in.py"
        src.write_text("print('hi')")
        result = CliRunner().invoke(main, ["submit", "--host", "host_f", str(src)])
        assert result.exit_code == 0, result.output
        assert result.output.strip() == "localjid123"
        assert captured["host"] == "localhost"
        assert captured["scheduler_target"] == "host_f"

    def test_scheduler_host_array_forwards_to_driver(
        self, cli_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._scheduler_cfg(cli_state)
        captured: dict[str, object] = {}

        def fake_submit_remote(**kw: object) -> list[str]:
            captured.update(kw)
            return ["arr000000001", "arr000000002", "arr000000003"]

        monkeypatch.setattr(submit_module, "submit_remote", fake_submit_remote)
        src = cli_state / "in.py"
        src.write_text("x")
        result = CliRunner().invoke(
            main,
            [
                "submit",
                "--host",
                "host_f",
                "--array",
                "3",
                "--rerun-until",
                "$VQ_WORKDIR/DONE",
                "--rerun-max",
                "4",
                str(src),
            ],
        )
        assert result.exit_code == 0, result.output
        assert result.output.splitlines() == [
            "arr000000001",
            "arr000000002",
            "arr000000003",
        ]
        assert captured["host"] == "drv"
        assert captured["scheduler_target"] == "host_f"
        assert captured["array"] == 3
        assert captured["rerun_until_file_exists"] == "$VQ_WORKDIR/DONE"
        assert captured["rerun_max"] == 4

    def test_scheduler_host_chain_forwards_to_driver(
        self, cli_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._scheduler_cfg(cli_state)
        captured: dict[str, object] = {}

        def fake_submit_remote(**kw: object) -> list[str]:
            captured.update(kw)
            return ["chn000000001", "chn000000002"]

        monkeypatch.setattr(submit_module, "submit_remote", fake_submit_remote)
        src = cli_state / "in.py"
        src.write_text("x")
        result = CliRunner().invoke(
            main,
            [
                "submit",
                "--host",
                "host_f",
                "--chain",
                "2",
                "--rerun-until",
                "$VQ_WORKDIR/DONE",
                "--rerun-max",
                "5",
                str(src),
            ],
        )
        assert result.exit_code == 0, result.output
        assert result.output.splitlines() == ["chn000000001", "chn000000002"]
        assert captured["host"] == "drv"
        assert captured["scheduler_target"] == "host_f"
        assert captured["chain"] == 2
        assert captured["rerun_until_file_exists"] == "$VQ_WORKDIR/DONE"
        assert captured["rerun_max"] == 5

    def test_scheduler_host_array_with_local_driver_submits_local_array(
        self, cli_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (cli_state / "cfg" / "config.toml").write_text(
            "[hosts.host_f]\n"
            'ssh = "host_f"\n'
            'scheduler = "pbs"\n'
            'scheduler_dialect = "torque"\n'
            'scratch_root = "/home/USER"\n'
            'scheduler_driver = "localhost"\n'
        )
        captured: dict[str, object] = {}

        def fake_submit_local_array(**kw: object) -> list[str]:
            captured.update(kw)
            return ["locarr000001", "locarr000002"]

        def fail_submit_remote(**_kw: object) -> list[str]:
            raise AssertionError("local scheduler drivers must not use submit_remote")

        monkeypatch.setattr(
            submit_module,
            "submit_local_array",
            fake_submit_local_array,
        )
        monkeypatch.setattr(submit_module, "submit_remote", fail_submit_remote)
        src = cli_state / "in.py"
        src.write_text("x")
        result = CliRunner().invoke(
            main,
            [
                "submit",
                "--host",
                "host_f",
                "--array",
                "2",
                "--rerun-until",
                "$VQ_WORKDIR/DONE",
                "--rerun-max",
                "6",
                str(src),
            ],
        )
        assert result.exit_code == 0, result.output
        assert result.output.splitlines() == ["locarr000001", "locarr000002"]
        assert captured["host"] == "localhost"
        assert captured["scheduler_target"] == "host_f"
        assert captured["array"] == 2
        assert captured["rerun_until_file_exists"] == "$VQ_WORKDIR/DONE"
        assert captured["rerun_max"] == 6

    def test_dir_submit_with_command(self, cli_state: Path) -> None:
        d = cli_state / "ws"
        d.mkdir()
        (d / "run.py").write_text("")
        result = CliRunner().invoke(
            main, ["submit", "localhost", "-d", str(d), "--", "python", "run.py"]
        )
        assert result.exit_code == 0, result.output

    def test_job_name_persisted_on_spec(self, cli_state: Path) -> None:
        """v0.5.34: ``vq submit --job-name NAME`` writes the name into
        the spec on disk."""
        src = cli_state / "input.py"
        src.write_text("print('hi')")
        result = CliRunner().invoke(
            main,
            ["submit", "localhost", "--job-name", "mgo-pbe", str(src)],
        )
        assert result.exit_code == 0, result.output
        jobid = result.output.strip()
        spec = JobSpec.read(paths.queue_dir() / f"{jobid}.json")
        assert spec.job_name == "mgo-pbe"

    def test_job_name_invalid_charset_is_sanitized_with_warning(
        self, cli_state: Path
    ) -> None:
        """Bad but salvageable names are cleaned instead of rejected."""
        src = cli_state / "input.py"
        src.write_text("print('hi')")
        result = CliRunner().invoke(
            main,
            [
                "submit",
                "localhost",
                "--job-name",
                "release paper (P01) tail=3200",
                str(src),
            ],
        )
        assert result.exit_code == 0, result.output
        jobid = result.output.strip().splitlines()[-1]
        spec = JobSpec.read(paths.queue_dir() / f"{jobid}.json")
        assert spec.job_name == "release-paper-P01-tail-3200"
        combined = result.output + result.stderr
        assert "sanitized --job-name" in combined
        assert "release paper (P01) tail=3200" in combined
        assert "release-paper-P01-tail-3200" in combined
        assert "ValidationError" not in combined
        assert "Traceback" not in combined

    def test_job_name_too_long_is_truncated_with_warning(
        self, cli_state: Path
    ) -> None:
        src = cli_state / "input.py"
        src.write_text("print('hi')")
        result = CliRunner().invoke(
            main,
            ["submit", "localhost", "--job-name", "x" * 60, str(src)],
        )
        assert result.exit_code == 0, result.output
        jobid = result.output.strip().splitlines()[-1]
        spec = JobSpec.read(paths.queue_dir() / f"{jobid}.json")
        assert spec.job_name == "x" * 50
        combined = result.output + result.stderr
        assert "sanitized --job-name" in combined
        assert "x" * 50 in combined

    def test_program_persisted_on_spec(self, cli_state: Path) -> None:
        src = cli_state / "input.py"
        src.write_text("print('hi')")
        (cli_state / "cfg" / "config.toml").write_text(
            "[programs.orca]\n"
            'kind = "binary"\n'
            'binary = "/bin/echo"\n'
        )
        result = CliRunner().invoke(
            main,
            ["submit", "localhost", "--program", "orca", str(src)],
        )
        assert result.exit_code == 0, result.output
        jobid = result.output.strip()
        spec = JobSpec.read(paths.queue_dir() / f"{jobid}.json")
        assert spec.program == "orca"
        assert spec.program_runtime_pin is None

    def test_venv_program_runtime_pin_snapshot_persisted(
        self, cli_state: Path
    ) -> None:
        src = cli_state / "input.py"
        src.write_text("print('hi')")
        git_dir = cli_state / "repo"
        submitted_sha = _init_git_repo(git_dir)
        (cli_state / "cfg" / "config.toml").write_text(
            "[programs.vibeqc-dev]\n"
            'kind = "venv"\n'
            f'python = "{sys.executable}"\n'
            f'git_dir = "{git_dir}"\n'
            f'expected_git_sha = "{submitted_sha}"\n'
        )

        result = CliRunner().invoke(
            main,
            ["submit", "localhost", "--program", "vibeqc-dev", str(src)],
        )

        assert result.exit_code == 0, result.output
        jobid = result.output.strip()
        spec = JobSpec.read(paths.queue_dir() / f"{jobid}.json")
        assert spec.program == "vibeqc-dev"
        assert spec.program_runtime_pin is not None
        assert spec.program_runtime_pin.expected_git_sha == submitted_sha

    def test_expected_sha_snapshot_persisted_for_directory_payload(
        self, cli_state: Path
    ) -> None:
        src_dir = cli_state / "docs-payload"
        src_dir.mkdir()
        (src_dir / "run_docs.sh").write_text("echo docs\n")
        git_dir = cli_state / "repo"
        submitted_sha = _init_git_repo(git_dir)
        (cli_state / "cfg" / "config.toml").write_text(
            "[programs.vibeqc-dev]\n"
            'kind = "venv"\n'
            f'python = "{sys.executable}"\n'
            f'git_dir = "{git_dir}"\n'
        )

        result = CliRunner().invoke(
            main,
            [
                "submit",
                "localhost",
                "-d",
                str(src_dir),
                "--program",
                "vibeqc-dev",
                "--expected-sha",
                submitted_sha[:7],
                "--",
                "bash",
                "run_docs.sh",
            ],
        )

        assert result.exit_code == 0, result.output
        jobid = result.output.strip()
        spec = JobSpec.read(paths.queue_dir() / f"{jobid}.json")
        assert spec.command == ["bash", "run_docs.sh"]
        assert spec.program == "vibeqc-dev"
        assert spec.program_runtime_pin is not None
        assert spec.program_runtime_pin.expected_git_sha == submitted_sha

    def test_expected_sha_too_short_rejected(self, cli_state: Path) -> None:
        src = cli_state / "input.py"
        src.write_text("print('hi')")
        git_dir = cli_state / "repo"
        (cli_state / "cfg" / "config.toml").write_text(
            "[programs.vibeqc-dev]\n"
            'kind = "venv"\n'
            f'python = "{sys.executable}"\n'
            f'git_dir = "{git_dir}"\n'
        )

        result = CliRunner().invoke(
            main,
            [
                "submit",
                "localhost",
                "--program",
                "vibeqc-dev",
                "--expected-sha",
                "abc123",
                str(src),
            ],
        )

        assert result.exit_code != 0
        assert "too short" in result.output
        assert "at least 7 hex characters" in result.output

    def test_expected_sha_mismatch_rejected_for_directory_payload(
        self, cli_state: Path
    ) -> None:
        src_dir = cli_state / "docs-payload"
        src_dir.mkdir()
        (src_dir / "run_docs.sh").write_text("echo docs\n")
        git_dir = cli_state / "repo"
        _init_git_repo(git_dir)
        (cli_state / "cfg" / "config.toml").write_text(
            "[programs.vibeqc-dev]\n"
            'kind = "venv"\n'
            f'python = "{sys.executable}"\n'
            f'git_dir = "{git_dir}"\n'
        )

        result = CliRunner().invoke(
            main,
            [
                "submit",
                "localhost",
                "-d",
                str(src_dir),
                "--program",
                "vibeqc-dev",
                "--expected-sha",
                "deadbeef",
                "--",
                "bash",
                "run_docs.sh",
            ],
        )

        assert result.exit_code != 0
        assert "expected git SHA deadbeef" in result.output
        assert "Traceback" not in result.output

    def test_expected_sha_requires_program(self, cli_state: Path) -> None:
        src = cli_state / "input.py"
        src.write_text("print('hi')")
        result = CliRunner().invoke(
            main,
            ["submit", "localhost", "--expected-sha", "deadbeef", str(src)],
        )
        assert result.exit_code != 0
        assert "--expected-sha requires --program" in result.output

    def test_unknown_local_program_rejected(self, cli_state: Path) -> None:
        src = cli_state / "input.py"
        src.write_text("print('hi')")
        result = CliRunner().invoke(
            main,
            ["submit", "localhost", "--program", "orca", str(src)],
        )
        assert result.exit_code != 0
        assert "unknown --program 'orca'" in result.output

    def test_local_program_runtime_pin_mismatch_rejected(
        self, cli_state: Path
    ) -> None:
        src = cli_state / "input.py"
        src.write_text("print('hi')")
        git_dir = cli_state / "repo"
        _init_git_repo(git_dir)
        (cli_state / "cfg" / "config.toml").write_text(
            "[programs.vibeqc-release]\n"
            'kind = "venv"\n'
            f'python = "{sys.executable}"\n'
            f'git_dir = "{git_dir}"\n'
            'expected_git_sha = "deadbeef"\n'
        )

        result = CliRunner().invoke(
            main,
            ["submit", "localhost", "--program", "vibeqc-release", str(src)],
        )

        assert result.exit_code != 0
        assert "runtime pin mismatch" in result.output
        assert "deadbeef" in result.output
        assert "Traceback" not in result.output

    def test_program_invalid_charset_rejected_with_usage_error(
        self, cli_state: Path
    ) -> None:
        src = cli_state / "input.py"
        src.write_text("print('hi')")
        result = CliRunner().invoke(
            main,
            ["submit", "localhost", "--program", "has space", str(src)],
        )
        assert result.exit_code != 0
        assert "--program" in result.output
        assert "invalid" in result.output.lower()
        assert "ValidationError" not in result.output

    def test_missing_host(self, cli_state: Path) -> None:
        result = CliRunner().invoke(main, ["submit"])
        assert result.exit_code != 0
        assert "HOST is required" in result.output

    def test_missing_input_and_flags(self, cli_state: Path) -> None:
        result = CliRunner().invoke(main, ["submit", "localhost"])
        assert result.exit_code != 0
        assert "input file" in result.output

    def test_dir_without_command(self, cli_state: Path) -> None:
        d = cli_state / "ws"
        d.mkdir()
        result = CliRunner().invoke(main, ["submit", "localhost", "-d", str(d)])
        assert result.exit_code != 0
        assert "require a command" in result.output

    def test_missing_compressed_archive_rejected_before_queueing(
        self, cli_state: Path
    ) -> None:
        archive = cli_state / "renamed-payload.tar.gz"

        result = CliRunner().invoke(
            main,
            ["submit", "localhost", "-c", str(archive), "--", "python", "run.py"],
        )

        assert result.exit_code != 0
        assert "Invalid value for '-c' / '--compressed'" in result.output
        assert "does not exist" in result.output
        assert not paths.queue_dir().exists()

    def test_dir_and_compressed_mutually_exclusive(self, cli_state: Path) -> None:
        d = cli_state / "ws"
        d.mkdir()
        a = cli_state / "a.tar"
        a.write_bytes(b"")
        result = CliRunner().invoke(
            main, ["submit", "localhost", "-d", str(d), "-c", str(a), "--", "x"]
        )
        assert result.exit_code != 0
        assert "mutually exclusive" in result.output

    def test_extra_positionals_after_input_rejected(self, cli_state: Path) -> None:
        f = cli_state / "x.py"
        f.write_text("")
        result = CliRunner().invoke(
            main, ["submit", "localhost", str(f), "--", "python", "x.py"]
        )
        assert result.exit_code != 0
        assert "single-file submit" in result.output

    def test_too_many_positional(self, cli_state: Path) -> None:
        f1 = cli_state / "a.py"
        f1.write_text("")
        f2 = cli_state / "b.py"
        f2.write_text("")
        result = CliRunner().invoke(main, ["submit", "localhost", str(f1), str(f2)])
        assert result.exit_code != 0
        assert "exactly one INPUT" in result.output

    def test_unknown_positional_falls_through_to_default_host_logic(
        self, cli_state: Path
    ) -> None:
        """In v0.2 an unknown first positional is treated as content, not
        a host. Without a default_host set the user gets the
        "HOST is required" message rather than the v0.1 "remote submit"
        not-implemented stub."""
        f = cli_state / "x.py"
        f.write_text("")
        result = CliRunner().invoke(main, ["submit", "some.other.host", str(f)])
        assert result.exit_code != 0
        # Two unrecognised positionals trip the multi-positional guard
        # before host resolution would; either error is acceptable but
        # the v0.1-era "remote submit" must be gone.
        assert "remote submit" not in result.output
        assert (
            "HOST is required" in result.output
            or "single-file submit takes exactly one INPUT" in result.output
        )

    def test_explicit_unknown_host_via_flag_rejected(self, cli_state: Path) -> None:
        """--host with a name that isn't in config is rejected by config lookup."""
        f = cli_state / "x.py"
        f.write_text("")
        result = CliRunner().invoke(
            main, ["submit", "--host", "nowhere", str(f)]
        )
        assert result.exit_code != 0
        assert "host 'nowhere' not found" in result.output

    def test_default_host_used_when_no_positional_host(
        self, cli_state_with_default: Path
    ) -> None:
        """With default_host = remote-mock and remote-mock not local, vq
        should attempt SSH submission, which fails because the mock host
        doesn't resolve. We just verify the code path is reached: failure
        comes from RemoteError (ssh exit non-zero) not from "HOST required"
        / "input file" / etc."""
        f = cli_state_with_default / "x.py"
        f.write_text("")
        result = CliRunner().invoke(main, ["submit", str(f)])
        assert result.exit_code != 0
        # Either RemoteError surfaces ("remote vq failed" / "scp upload failed")
        # or the test machine doesn't have ssh and we get a different
        # subprocess-level error -- but it must not be the resolution-stage
        # errors below:
        assert "HOST is required" not in result.output
        assert "provide an input file" not in result.output

    def test_cpus_flag_recorded(self, cli_state: Path) -> None:
        f = cli_state / "x.py"
        f.write_text("")
        result = CliRunner().invoke(
            main, ["submit", "localhost", "--cpus", "4", str(f)]
        )
        assert result.exit_code == 0, result.output
        from vq.spec import JobSpec
        jobid = result.output.strip()
        spec = JobSpec.read(paths.queue_dir() / f"{jobid}.json")
        assert spec.cpus == 4

    def test_python_flag_recorded_in_command(self, cli_state: Path) -> None:
        f = cli_state / "x.py"
        f.write_text("")
        result = CliRunner().invoke(
            main, ["submit", "localhost", "--python", "/custom/py", str(f)]
        )
        assert result.exit_code == 0, result.output
        from vq.spec import JobSpec
        jobid = result.output.strip()
        spec = JobSpec.read(paths.queue_dir() / f"{jobid}.json")
        assert spec.command == ["/custom/py", "x.py"]

    def test_python_flag_with_dir_prepends_the_interpreter(
        self, cli_state: Path
    ) -> None:
        """Was rejected until 2026-08-01; see submit._payload_command."""
        d = cli_state / "ws"
        d.mkdir()
        (d / "run.py").write_text("")
        result = CliRunner().invoke(
            main,
            [
                "submit", "localhost",
                "--python", "/custom/py",
                "-d", str(d), "--", "run.py",
            ],
        )
        assert result.exit_code == 0, result.output
        jobid = result.output.strip().splitlines()[-1]
        spec = JobSpec.read(cli_state / "state" / "queue" / f"{jobid}.json")
        assert spec.command == ["/custom/py", "run.py"]

    def test_branch_flag_resolves_canonical(self, cli_state: Path) -> None:
        """`vq submit localhost --branch main foo.py` looks up the
        host's [branches] table and uses the resolved path as the
        interpreter."""
        (cli_state / "cfg" / "config.toml").write_text(
            '[hosts.localhost]\n'
            'ssh = "localhost"\n'
            '\n'
            '[hosts.localhost.branches]\n'
            'main = "/dev/py"\n'
            'release = "/rel/py"\n'
        )
        f = cli_state / "x.py"
        f.write_text("")
        result = CliRunner().invoke(
            main, ["submit", "localhost", "--branch", "main", str(f)]
        )
        assert result.exit_code == 0, result.output
        from vq.spec import JobSpec
        spec = JobSpec.read(paths.queue_dir() / f"{result.output.strip()}.json")
        assert spec.command == ["/dev/py", "x.py"]

    def test_branch_flag_resolves_alias(self, cli_state: Path) -> None:
        """Aliases route to the same interpreter as their canonical
        target. `--branch latest` -> `release` -> /rel/py."""
        (cli_state / "cfg" / "config.toml").write_text(
            '[hosts.localhost]\n'
            'ssh = "localhost"\n'
            '\n'
            '[hosts.localhost.branches]\n'
            'main = "/dev/py"\n'
            'release = "/rel/py"\n'
            '\n'
            '[hosts.localhost.branch_aliases]\n'
            'dev = "main"\n'
            'latest = "release"\n'
        )
        f = cli_state / "x.py"
        f.write_text("")
        result = CliRunner().invoke(
            main, ["submit", "localhost", "--branch", "latest", str(f)]
        )
        assert result.exit_code == 0, result.output
        from vq.spec import JobSpec
        spec = JobSpec.read(paths.queue_dir() / f"{result.output.strip()}.json")
        assert spec.command == ["/rel/py", "x.py"]

    def test_branch_unknown_lists_known(self, cli_state: Path) -> None:
        """Typo'd branch name fails fast with a "known: ..." hint
        rather than after the workspace tarball is built."""
        (cli_state / "cfg" / "config.toml").write_text(
            '[hosts.localhost]\n'
            'ssh = "localhost"\n'
            '\n'
            '[hosts.localhost.branches]\n'
            'main = "/dev/py"\n'
            'release = "/rel/py"\n'
            '\n'
            '[hosts.localhost.branch_aliases]\n'
            'dev = "main"\n'
        )
        f = cli_state / "x.py"
        f.write_text("")
        result = CliRunner().invoke(
            main, ["submit", "localhost", "--branch", "stable", str(f)]
        )
        assert result.exit_code != 0
        assert "unknown --branch 'stable'" in result.output
        # Suggestion includes both canonical names and aliases:
        for name in ("main", "release", "dev"):
            assert name in result.output

    def test_branch_with_no_branches_configured_errors(
        self, cli_state: Path
    ) -> None:
        """Host exists in config but has no [branches] table -> tell
        the user to add one."""
        (cli_state / "cfg" / "config.toml").write_text(
            '[hosts.localhost]\n'
            'ssh = "localhost"\n'
        )
        f = cli_state / "x.py"
        f.write_text("")
        result = CliRunner().invoke(
            main, ["submit", "localhost", "--branch", "main", str(f)]
        )
        assert result.exit_code != 0
        assert "no [branches] configured" in result.output

    def test_branch_with_python_rejected(self, cli_state: Path) -> None:
        """--branch and --python serve the same purpose; passing both
        is a mistake the CLI catches."""
        (cli_state / "cfg" / "config.toml").write_text(
            '[hosts.localhost]\n'
            'ssh = "localhost"\n'
            '\n'
            '[hosts.localhost.branches]\n'
            'main = "/dev/py"\n'
        )
        f = cli_state / "x.py"
        f.write_text("")
        result = CliRunner().invoke(
            main,
            [
                "submit", "localhost",
                "--branch", "main",
                "--python", "/other/py",
                str(f),
            ],
        )
        assert result.exit_code != 0
        assert "--branch and --python are mutually exclusive" in result.output

    def test_branch_with_dir_resolves_and_prepends(self, cli_state: Path) -> None:
        """--branch now applies to -d too. Rejecting it meant a scheduler-host
        --dir submit had to hardcode that host's pinned wrapper path, which is
        the leak [hosts.HOST.branches] exists to prevent."""
        (cli_state / "cfg" / "config.toml").write_text(
            '[hosts.localhost]\n'
            'ssh = "localhost"\n'
            '\n'
            '[hosts.localhost.branches]\n'
            'main = "/dev/py"\n'
        )
        d = cli_state / "ws"
        d.mkdir()
        (d / "run.py").write_text("")
        result = CliRunner().invoke(
            main,
            [
                "submit", "localhost",
                "--branch", "main",
                "-d", str(d), "--", "run.py",
            ],
        )
        assert result.exit_code == 0, result.output
        jobid = result.output.strip().splitlines()[-1]
        spec = JobSpec.read(cli_state / "state" / "queue" / f"{jobid}.json")
        assert spec.command == ["/dev/py", "run.py"]
        assert spec.branch == "main"

    def test_branch_unknown_host_errors(self, cli_state: Path) -> None:
        """--branch needs a configured [hosts.X] section; passing it
        for a host with no entry in config errors with a clear pointer."""
        (cli_state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
        )
        f = cli_state / "x.py"
        f.write_text("")
        result = CliRunner().invoke(
            main,
            [
                "submit", "--host", "no-such-host",
                "--branch", "main",
                str(f),
            ],
        )
        assert result.exit_code != 0
        assert "--branch requires a [hosts.no-such-host] section" in result.output

    def test_mem_mb_flag_recorded(self, cli_state: Path) -> None:
        f = cli_state / "x.py"
        f.write_text("")
        result = CliRunner().invoke(
            main, ["submit", "localhost", "--mem-mb", "16000", str(f)]
        )
        assert result.exit_code == 0, result.output
        from vq.spec import JobSpec
        jobid = result.output.strip()
        spec = JobSpec.read(paths.queue_dir() / f"{jobid}.json")
        assert spec.mem_mb == 16000

    def test_wall_time_flag_recorded(self, cli_state: Path) -> None:
        f = cli_state / "x.py"
        f.write_text("")
        result = CliRunner().invoke(
            main,
            ["submit", "localhost", "--wall-time-seconds", "3600", str(f)],
        )
        assert result.exit_code == 0, result.output
        from vq.spec import JobSpec
        jobid = result.output.strip()
        spec = JobSpec.read(paths.queue_dir() / f"{jobid}.json")
        assert spec.wall_time_seconds == 3600

    def test_mem_mb_default_none(self, cli_state: Path) -> None:
        f = cli_state / "x.py"
        f.write_text("")
        result = CliRunner().invoke(main, ["submit", "localhost", str(f)])
        assert result.exit_code == 0, result.output
        from vq.spec import JobSpec
        jobid = result.output.strip()
        spec = JobSpec.read(paths.queue_dir() / f"{jobid}.json")
        assert spec.mem_mb is None
        assert spec.wall_time_seconds is None

    def test_mem_mb_zero_rejected(self, cli_state: Path) -> None:
        f = cli_state / "x.py"
        f.write_text("")
        result = CliRunner().invoke(
            main, ["submit", "localhost", "--mem-mb", "0", str(f)]
        )
        assert result.exit_code != 0
        # click.IntRange(min=1) rejects 0 with its own message

    def test_wall_time_zero_rejected(self, cli_state: Path) -> None:
        f = cli_state / "x.py"
        f.write_text("")
        result = CliRunner().invoke(
            main,
            ["submit", "localhost", "--wall-time-seconds", "0", str(f)],
        )
        assert result.exit_code != 0


class TestQueueCLI:
    def test_empty_queue(self, cli_state: Path) -> None:
        result = CliRunner().invoke(main, ["queue", "localhost"])
        assert result.exit_code == 0
        assert "(no jobs)" in result.output

    def test_queue_lists_submitted_jobs(self, cli_state: Path) -> None:
        f = cli_state / "x.py"
        f.write_text("")
        sub_result = CliRunner().invoke(main, ["submit", "localhost", str(f)])
        jobid = sub_result.output.strip()
        list_result = CliRunner().invoke(main, ["queue", "localhost"])
        assert list_result.exit_code == 0
        assert jobid in list_result.output
        assert "pending" in list_result.output

    def test_queue_trusts_rpc_when_pidfile_missing(
        self, cli_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr("vq.rpc.ping", lambda **kw: {"version": "9.9.9"})
        workspace = cli_state / "ws" / "rpcok0000001"
        workspace.mkdir(parents=True)
        paths.queue_dir().mkdir(parents=True)
        JobSpec(
            id="rpcok0000001",
            command=["sleep", "60"],
            cwd=str(workspace),
            cpus=1,
            state=JobState.RUNNING,
        ).write(paths.queue_dir() / "rpcok0000001.json")

        result = CliRunner().invoke(main, ["queue", "localhost"])

        assert result.exit_code == 0, result.output
        assert "rpcok0000001" in result.output
        assert "daemon is down" not in result.output


class TestTagsCLI:
    """v0.6.6: `vq submit --tag X` (repeatable), `vq queue --tag X`
    filter, and tag display in `vq status`."""

    def test_submit_with_tags_stores_them_on_spec(
        self, cli_state: Path
    ) -> None:
        f = cli_state / "x.py"
        f.write_text("")
        result = CliRunner().invoke(
            main,
            [
                "submit", "localhost", str(f),
                "--tag", "experiment-12",
                "--tag", "basisset-dev",
            ],
        )
        assert result.exit_code == 0, result.output
        jobid = result.output.strip()
        # Tags appear in vq status.
        status = CliRunner().invoke(main, ["status", "localhost", jobid])
        assert status.exit_code == 0
        assert "tags:" in status.output
        assert "basisset-dev" in status.output
        assert "experiment-12" in status.output

    def test_queue_tag_filter_AND_semantics(
        self, cli_state: Path
    ) -> None:
        """Two jobs, different tags. --tag X filter shows only the
        one with X. --tag X --tag Y shows only jobs with BOTH."""
        f1 = cli_state / "a.py"
        f1.write_text("")
        f2 = cli_state / "b.py"
        f2.write_text("")
        # Job A: tags = [foo, bar]
        jid_a = CliRunner().invoke(
            main, ["submit", "localhost", str(f1),
                   "--tag", "foo", "--tag", "bar"],
        ).output.strip()
        # Job B: tag = [foo]
        jid_b = CliRunner().invoke(
            main, ["submit", "localhost", str(f2), "--tag", "foo"],
        ).output.strip()

        # --tag foo → both
        r = CliRunner().invoke(main, ["queue", "localhost", "--tag", "foo"])
        assert r.exit_code == 0
        assert jid_a in r.output
        assert jid_b in r.output

        # --tag bar → only A
        r = CliRunner().invoke(main, ["queue", "localhost", "--tag", "bar"])
        assert r.exit_code == 0
        assert jid_a in r.output
        assert jid_b not in r.output

        # --tag foo --tag bar → only A (AND-semantics)
        r = CliRunner().invoke(
            main, ["queue", "localhost", "--tag", "foo", "--tag", "bar"],
        )
        assert r.exit_code == 0
        assert jid_a in r.output
        assert jid_b not in r.output

    def test_invalid_tag_charset_rejected(
        self, cli_state: Path
    ) -> None:
        """Same strict charset as --job-name: alnum + - _ . only.
        Spaces / slashes / etc. fail at submit."""
        f = cli_state / "x.py"
        f.write_text("")
        result = CliRunner().invoke(
            main, ["submit", "localhost", str(f), "--tag", "has space"],
        )
        assert result.exit_code != 0

    def test_help_mentions_tag(self) -> None:
        result = CliRunner().invoke(main, ["submit", "--help"])
        assert result.exit_code == 0
        assert "--tag" in result.output
        result = CliRunner().invoke(main, ["queue", "--help"])
        assert result.exit_code == 0
        assert "--tag" in result.output


class TestArrayGroupQueueFilter:
    """v0.6.53: `vq queue --array-group GID` filter — shows only
    elements of the named array submission. Composes with the
    other queue filters."""

    def test_filter_shows_only_matching_group(
        self, cli_state: Path
    ) -> None:
        # Two array submits (two distinct groups) + one regular
        # submit. Filter should pick exactly one group.
        f = cli_state / "x.py"
        f.write_text("")
        out_a = CliRunner().invoke(
            main, ["submit", "localhost", "--array", "3", str(f)],
        )
        assert out_a.exit_code == 0, out_a.output
        a_jids = [j for j in out_a.output.strip().split("\n") if j]
        out_b = CliRunner().invoke(
            main, ["submit", "localhost", "--array", "2", str(f)],
        )
        assert out_b.exit_code == 0
        b_jids = [j for j in out_b.output.strip().split("\n") if j]
        solo = CliRunner().invoke(
            main, ["submit", "localhost", str(f)],
        ).output.strip()

        # Pick group A's gid off any of its specs.
        a_spec = JobSpec.read(paths.queue_dir() / f"{a_jids[0]}.json")
        gid_a = a_spec.array_group_id
        assert gid_a is not None

        r = CliRunner().invoke(
            main, ["queue", "localhost", "--array-group", gid_a],
        )
        assert r.exit_code == 0, r.output
        for j in a_jids:
            assert j in r.output, f"expected {j} in array-A listing"
        for j in b_jids:
            assert j not in r.output, f"B's jobs leaked into A filter: {j}"
        assert solo not in r.output

    def test_unknown_group_returns_empty(
        self, cli_state: Path
    ) -> None:
        f = cli_state / "x.py"
        f.write_text("")
        CliRunner().invoke(
            main, ["submit", "localhost", "--array", "2", str(f)],
        )
        r = CliRunner().invoke(
            main, ["queue", "localhost", "--array-group", "deadbeef"],
        )
        assert r.exit_code == 0, r.output
        # No row rendered — format_table prints just the header
        # (or "no jobs" / empty body); the matched filter has 0
        # rows.
        spec_lines = [
            line for line in r.output.splitlines()
            if line and "ID" not in line and "STATE" not in line
            and "---" not in line
        ]
        # All non-header / non-decorative lines should be free of
        # the 12-hex jobid pattern.
        import re as _re
        hex12 = _re.compile(r"^[0-9a-f]{12}\b")
        assert not any(hex12.match(line) for line in spec_lines)

    def test_composes_with_state_filter(self, cli_state: Path) -> None:
        """--array-group + -s state composes (AND-semantics)."""
        f = cli_state / "x.py"
        f.write_text("")
        out = CliRunner().invoke(
            main, ["submit", "localhost", "--array", "3", str(f)],
        )
        jids = [j for j in out.output.strip().split("\n") if j]
        gid = JobSpec.read(
            paths.queue_dir() / f"{jids[0]}.json"
        ).array_group_id

        # Flip ONE element to COMPLETED state on disk; filter
        # should pick exactly that one with -s completed.
        spec = JobSpec.read(paths.queue_dir() / f"{jids[1]}.json")
        spec.state = JobState.COMPLETED
        spec.write(paths.queue_dir() / f"{jids[1]}.json")

        r = CliRunner().invoke(
            main,
            ["queue", "localhost", "--array-group", gid, "-s", "completed"],
        )
        assert r.exit_code == 0, r.output
        assert jids[1] in r.output
        assert jids[0] not in r.output
        assert jids[2] not in r.output

    def test_json_form_filters_too(self, cli_state: Path) -> None:
        f = cli_state / "x.py"
        f.write_text("")
        out_a = CliRunner().invoke(
            main, ["submit", "localhost", "--array", "2", str(f)],
        )
        a_jids = [j for j in out_a.output.strip().split("\n") if j]
        CliRunner().invoke(
            main, ["submit", "localhost", "--array", "2", str(f)],
        )
        gid_a = JobSpec.read(
            paths.queue_dir() / f"{a_jids[0]}.json"
        ).array_group_id

        r = CliRunner().invoke(
            main,
            ["queue", "localhost", "--array-group", gid_a, "--json"],
        )
        assert r.exit_code == 0, r.output
        import json as _json
        payload = _json.loads(r.output)
        assert isinstance(payload, list)
        assert len(payload) == 2
        assert all(p["array_group_id"] == gid_a for p in payload)

    def test_remote_delegate_forwards_array_group(
        self, cli_state: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Remote-host path: --array-group must be forwarded over SSH
        so the filter runs on the host with the specs (matching the
        --tag forwarding pattern)."""
        # Wire a fake remote host into the config so HOST is treated
        # as remote. (cli_state doesn't create a config.toml by
        # default — only cli_state_with_default does.)
        cfg_path = (
            Path(os.environ[config.ENV_CONFIG_DIR]) / "config.toml"
        )
        cfg_path.write_text(
            '[hosts.farhost]\nssh = "f.invalid"\n'
        )

        captured: dict[str, object] = {}

        def fake_delegate(host, cfg, *args, stdin_data=None):
            captured["host"] = host
            captured["args"] = list(args)
            return "ok\n"

        monkeypatch.setattr("vq.cli._delegate_to_remote", fake_delegate)

        r = CliRunner().invoke(
            main,
            ["queue", "farhost", "--array-group", "abcd1234"],
        )
        assert r.exit_code == 0, r.output
        assert "--array-group" in captured["args"]
        assert "abcd1234" in captured["args"]

    def test_scheduler_host_lists_driver_specs_only(self, cli_state: Path) -> None:
        """A daemonless scheduler host is represented by tagged specs on driver."""
        cfg_path = Path(os.environ[config.ENV_CONFIG_DIR]) / "config.toml"
        cfg_path.write_text(
            "[hosts.localhost]\n"
            'ssh = "localhost"\n'
            "\n"
            "[hosts.host_f]\n"
            'ssh = "host_f"\n'
            'scheduler = "pbs"\n'
            'scheduler_dialect = "torque"\n'
            'scratch_root = "/home/USER"\n'
            'scheduler_driver = "localhost"\n'
        )
        queue_dir = paths.queue_dir()
        queue_dir.mkdir(parents=True)
        JobSpec(
            id="aaa000000001",
            command=["python", "run.py"],
            cwd=str(cli_state / "host_f"),
            cpus=1,
            scheduler_target="host_f",
            scheduler_state="queued",
            scheduler_job_id="123.host_f",
            submitted_at="2026-06-29T08:00:00+00:00",
        ).write(queue_dir / "aaa000000001.json")
        JobSpec(
            id="bbb000000002",
            command=["python", "other.py"],
            cwd=str(cli_state / "local"),
            cpus=1,
            submitted_at="2026-06-29T08:01:00+00:00",
        ).write(queue_dir / "bbb000000002.json")

        import json as _json

        as_json = CliRunner().invoke(main, ["queue", "host_f", "--json"])
        assert as_json.exit_code == 0, as_json.output
        payload = _json.loads(as_json.output)
        assert [row["id"] for row in payload] == ["aaa000000001"]
        assert payload[0]["queue_handle"] == {
            "job_id": "aaa000000001",
            "host": "host_f",
            "submitted_at": "2026-06-29T08:00:00+00:00",
        }

        as_text = CliRunner().invoke(main, ["queue", "host_f"])
        assert as_text.exit_code == 0, as_text.output
        assert "aaa000000001" in as_text.output
        assert "host_f:vq=pending,sched=queued" in as_text.output
        assert "bbb000000002" not in as_text.output

    def test_scheduler_host_queue_json_includes_terminal_diagnosis(
        self, cli_state: Path,
    ) -> None:
        """Daemonless scheduler listings keep terminal diagnosis after filtering."""
        cfg_path = Path(os.environ[config.ENV_CONFIG_DIR]) / "config.toml"
        cfg_path.write_text(
            "[hosts.localhost]\n"
            'ssh = "localhost"\n'
            "\n"
            "[hosts.host_f]\n"
            'ssh = "host_f"\n'
            'scheduler = "pbs"\n'
            'scheduler_dialect = "torque"\n'
            'scratch_root = "/home/USER"\n'
            'scheduler_driver = "localhost"\n'
        )
        queue_dir = paths.queue_dir()
        queue_dir.mkdir(parents=True)
        JobSpec(
            id="ccc000000003",
            command=["python", "run.py"],
            cwd=str(cli_state / "host_f-wall"),
            cpus=8,
            state=JobState.TIME_EXCEEDED,
            scheduler_target="host_f",
            scheduler_state="exited",
            scheduler_job_id="456.host_f",
            failure_reason="PBS walltime exceeded",
            submitted_at="2026-06-29T08:00:00+00:00",
        ).write(queue_dir / "ccc000000003.json")

        result = CliRunner().invoke(main, ["queue", "host_f", "--json"])
        assert result.exit_code == 0, result.output
        payload = json.loads(result.stdout)
        assert [row["id"] for row in payload] == ["ccc000000003"]
        assert (
            payload[0]["terminal_diagnosis"]["category"]
            == "scheduler_walltime"
        )
        assert (
            payload[0]["terminal_diagnosis"]["action_hint"]
            == "increase_walltime"
        )


class TestScheduledSubmitCLI:
    """v0.6.12: `vq submit --at ISO8601`. The daemon's existing
    not_before-honoring path gates dispatch; the CLI work is just
    parsing + validation."""

    def test_at_with_utc_z_suffix_accepted(self, cli_state: Path) -> None:
        f = cli_state / "x.py"
        f.write_text("")
        result = CliRunner().invoke(
            main,
            ["submit", "localhost", str(f), "--at", "2026-05-20T22:00:00Z"],
        )
        assert result.exit_code == 0, result.output
        # Verify the spec recorded the canonical ISO form. fromisoformat
        # accepts `Z` (Python 3.11+) and re-emits with `+00:00`.
        jobid = result.output.strip()
        spec_path = cli_state / "state" / "queue" / f"{jobid}.json"
        import json as _json
        spec = _json.loads(spec_path.read_text())
        assert spec["not_before"] == "2026-05-20T22:00:00+00:00"

    def test_at_with_explicit_offset_accepted(self, cli_state: Path) -> None:
        f = cli_state / "x.py"
        f.write_text("")
        result = CliRunner().invoke(
            main,
            ["submit", "localhost", str(f),
             "--at", "2026-05-20T17:00:00-05:00"],
        )
        assert result.exit_code == 0, result.output
        jobid = result.output.strip()
        spec_path = cli_state / "state" / "queue" / f"{jobid}.json"
        import json as _json
        spec = _json.loads(spec_path.read_text())
        assert spec["not_before"] == "2026-05-20T17:00:00-05:00"

    def test_naive_timestamp_rejected(self, cli_state: Path) -> None:
        """No tzinfo → reject. Operator must be explicit about whose
        clock the timestamp refers to."""
        f = cli_state / "x.py"
        f.write_text("")
        result = CliRunner().invoke(
            main, ["submit", "localhost", str(f), "--at", "2026-05-20T22:00:00"],
        )
        assert result.exit_code != 0
        assert "naive" in result.output.lower() or "timezone" in result.output.lower()

    def test_malformed_iso8601_rejected(self, cli_state: Path) -> None:
        f = cli_state / "x.py"
        f.write_text("")
        result = CliRunner().invoke(
            main, ["submit", "localhost", str(f), "--at", "next tuesday"],
        )
        assert result.exit_code != 0
        assert "iso 8601" in result.output.lower()

    def test_no_at_leaves_not_before_none(self, cli_state: Path) -> None:
        f = cli_state / "x.py"
        f.write_text("")
        result = CliRunner().invoke(
            main, ["submit", "localhost", str(f)],
        )
        assert result.exit_code == 0
        jobid = result.output.strip()
        spec_path = cli_state / "state" / "queue" / f"{jobid}.json"
        import json as _json
        spec = _json.loads(spec_path.read_text())
        assert spec["not_before"] is None

    def test_past_timestamp_accepted(self, cli_state: Path) -> None:
        """Past --at values are NOT an error — the daemon's dispatch
        loop treats already-elapsed not_before as ready-now. Matches
        the v0.5.31 retry-backoff semantics (stale backoff = ready)."""
        f = cli_state / "x.py"
        f.write_text("")
        result = CliRunner().invoke(
            main, ["submit", "localhost", str(f), "--at", "2020-01-01T00:00:00Z"],
        )
        assert result.exit_code == 0

    def test_help_mentions_at(self) -> None:
        result = CliRunner().invoke(main, ["submit", "--help"])
        assert result.exit_code == 0
        assert "--at" in result.output
        # The reject-naive footgun guard is the key safety property;
        # make sure it's documented in help.
        assert "timezone" in result.output.lower() or "naive" in result.output.lower()

    def test_status_labels_scheduled_submit_not_retry_backoff(
        self, cli_state: Path
    ) -> None:
        """v0.6.12 follow-on polish: vq status displays not_before with
        "scheduled submit" when retry_count is 0, "retry backoff" when
        > 0. Sharing the field across the two paths means the label
        has to disambiguate."""
        f = cli_state / "x.py"
        f.write_text("")
        result = CliRunner().invoke(
            main,
            ["submit", "localhost", str(f), "--at", "2026-05-20T22:00:00Z"],
        )
        assert result.exit_code == 0
        jobid = result.output.strip()
        status = CliRunner().invoke(main, ["status", "localhost", jobid])
        assert status.exit_code == 0
        assert "not_before:" in status.output
        assert "scheduled submit" in status.output
        assert "retry backoff" not in status.output


class TestStatusCLI:
    def test_status_shows_spec_for_submitted_job(self, cli_state: Path) -> None:
        f = cli_state / "x.py"
        f.write_text("")
        jobid = CliRunner().invoke(main, ["submit", "localhost", str(f)]).output.strip()
        result = CliRunner().invoke(main, ["status", "localhost", jobid])
        assert result.exit_code == 0
        assert jobid in result.output
        assert "state:" in result.output
        assert "pending" in result.output

    def test_status_unknown_jobid_errors(self, cli_state: Path) -> None:
        result = CliRunner().invoke(main, ["status", "localhost", "deadbeef"])
        assert result.exit_code != 0
        assert "no such job" in result.output


class TestKillCLI:
    def test_kill_pending_job(self, cli_state: Path) -> None:
        f = cli_state / "x.py"
        f.write_text("")
        jobid = CliRunner().invoke(main, ["submit", "localhost", str(f)]).output.strip()
        result = CliRunner().invoke(main, ["kill", "localhost", jobid])
        assert result.exit_code == 0
        assert "killed" in result.output

    def test_kill_pending_job_records_reason_in_status(
        self, cli_state: Path
    ) -> None:
        f = cli_state / "x.py"
        f.write_text("")
        jobid = CliRunner().invoke(main, ["submit", "localhost", str(f)]).output.strip()
        result = CliRunner().invoke(
            main,
            [
                "kill",
                "--reason",
                "obsolete vibe-qc version; resubmit after update",
                "localhost",
                jobid,
            ],
        )
        assert result.exit_code == 0, result.output

        status = CliRunner().invoke(main, ["status", "localhost", jobid])
        assert status.exit_code == 0, status.output
        assert "failure:" in status.output
        assert "obsolete vibe-qc version; resubmit after update" in status.output

    def test_kill_pending_job_can_resubmit_after_update(
        self, cli_state: Path
    ) -> None:
        f = cli_state / "x.py"
        f.write_text("")
        jobid = CliRunner().invoke(main, ["submit", "localhost", str(f)]).output.strip()
        result = CliRunner().invoke(
            main,
            [
                "kill",
                "--reason",
                "obsolete vibe-qc runtime during fleet upgrade",
                "--restart-after-update",
                "localhost",
                jobid,
            ],
        )
        assert result.exit_code == 0, result.output
        assert f"resubmitted {jobid} -> " in result.output
        new_id = result.output.strip().split(" -> ")[-1]

        killed = JobSpec.read(paths.spec_path(jobid))
        restarted = JobSpec.read(paths.spec_path(new_id))
        assert killed.state == JobState.KILLED
        assert killed.failure_reason == (
            "killed by vq/operator request: "
            "obsolete vibe-qc runtime during fleet upgrade"
        )
        assert restarted.state == JobState.PENDING
        assert restarted.parent_jobid == jobid

    def test_kill_unknown_jobid_errors(self, cli_state: Path) -> None:
        result = CliRunner().invoke(main, ["kill", "localhost", "deadbeef"])
        assert result.exit_code != 0
        assert "no such job" in result.output

    def test_kill_already_killed_errors(self, cli_state: Path) -> None:
        f = cli_state / "x.py"
        f.write_text("")
        jobid = CliRunner().invoke(main, ["submit", "localhost", str(f)]).output.strip()
        CliRunner().invoke(main, ["kill", "localhost", jobid])  # first kill
        result = CliRunner().invoke(main, ["kill", "localhost", jobid])  # again
        assert result.exit_code != 0
        assert "terminal state" in result.output


class TestDefaultHostFallback:
    """Verbs without a positional host fall back to ``default_host`` from
    config. Each verb's local path is exercised here; the SSH path is
    covered by the dispatch tests below with mocked subprocess."""

    def test_queue_no_host_uses_default(self, cli_state: Path) -> None:
        # default_host=localhost via a small inline config
        (cli_state / "cfg" / "config.toml").write_text('default_host = "localhost"\n')
        result = CliRunner().invoke(main, ["queue"])
        assert result.exit_code == 0
        assert "(no jobs)" in result.output

    def test_status_one_positional_treats_it_as_jobid(self, cli_state: Path) -> None:
        (cli_state / "cfg" / "config.toml").write_text('default_host = "localhost"\n')
        f = cli_state / "x.py"
        f.write_text("")
        jobid = CliRunner().invoke(main, ["submit", "localhost", str(f)]).output.strip()
        # Single positional -> jobid (host comes from default_host)
        result = CliRunner().invoke(main, ["status", jobid])
        assert result.exit_code == 0, result.output
        assert jobid in result.output

    def test_kill_one_positional_treats_it_as_jobid(self, cli_state: Path) -> None:
        (cli_state / "cfg" / "config.toml").write_text('default_host = "localhost"\n')
        f = cli_state / "x.py"
        f.write_text("")
        jobid = CliRunner().invoke(main, ["submit", "localhost", str(f)]).output.strip()
        result = CliRunner().invoke(main, ["kill", jobid])
        assert result.exit_code == 0, result.output
        assert "killed" in result.output

    def test_no_default_no_positional_errors_with_helpful_message(
        self, cli_state: Path
    ) -> None:
        # No config file at all
        result = CliRunner().invoke(main, ["queue"])
        assert result.exit_code != 0
        assert "HOST is required" in result.output


def _patch_remote_vq_stdout(
    monkeypatch: pytest.MonkeyPatch, *, stdout: str, returncode: int = 0
) -> list[list[str]]:
    """Stub transport.run_remote_vq via subprocess.run. Returns the captured
    argv lists so the test can assert on what ssh would have been told to do."""
    captured: list[list[str]] = []

    def fake_run(cmd: list[str], **kw):
        captured.append(cmd)
        return subprocess.CompletedProcess(
            args=cmd, returncode=returncode, stdout=stdout, stderr=""
        )

    from vq import transport as transport_module
    monkeypatch.setattr(transport_module.subprocess, "run", fake_run)
    return captured


def _patch_remote_vq_failure(
    monkeypatch: pytest.MonkeyPatch, *, stderr: str, returncode: int = 255
) -> list[list[str]]:
    captured: list[list[str]] = []

    def fake_run(cmd: list[str], **kw):
        captured.append(cmd)
        return subprocess.CompletedProcess(
            args=cmd, returncode=returncode, stdout="", stderr=stderr
        )

    from vq import transport as transport_module
    monkeypatch.setattr(transport_module.subprocess, "run", fake_run)
    return captured


class TestRemoteDispatch:
    """For each user-facing verb, verify that pointing at a remote host
    produces the expected ``ssh <host> vq <args>`` invocation."""

    @pytest.fixture
    def remote_cfg(self, cli_state: Path) -> Path:
        # Use a hostname that's guaranteed *not* to match any real machine's
        # hostname or hostname prefix, so is_local_host() returns False on
        # every test box (including host_d, where 'host_d' would otherwise
        # be local). 'fake-remote-test' has a hyphen + suffix that cannot
        # match a typical hostname's short form.
        (cli_state / "cfg" / "config.toml").write_text(
            'default_host = "fake-remote-test"\n'
            '\n'
            '[hosts.fake-remote-test]\n'
            'ssh = "fake-remote-test"\n'
            'remote_vq = "vq"\n'
        )
        return cli_state

    # v0.5.32 note: as of the shlex-join fix, the captured cmd ends with
    # ``[..., host, joined_remote_cmd]`` — shlex.split of cmd[-1]
    # recovers the argv the remote will see. v0.6.17 inserted
    # ConnectTimeout + ServerAlive options between `ssh` and the host,
    # so we now match on tail (cmd[-2:]) rather than head (cmd[:2]).
    def _remote_argv(self, captured: list[list[str]]) -> list[str]:
        assert len(captured) == 1, captured
        cmd = captured[0]
        assert cmd[0] == "ssh", cmd
        assert cmd[-2] == "fake-remote-test", cmd
        return shlex.split(cmd[-1])

    def test_queue_remote_invokes_remote_vq(
        self, remote_cfg: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured = _patch_remote_vq_stdout(monkeypatch, stdout="(no jobs)\n")
        result = CliRunner().invoke(main, ["queue", "fake-remote-test"])
        assert result.exit_code == 0, result.output
        assert "(no jobs)" in result.output
        assert self._remote_argv(captured) == ["vq", "queue", "localhost"]

    def test_queue_remote_forwards_active_without_rewriting_it_as_states(
        self, remote_cfg: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured = _patch_remote_vq_stdout(monkeypatch, stdout="(no jobs)\n")

        result = CliRunner().invoke(
            main,
            ["queue", "fake-remote-test", "--active", "-s", "completed"],
        )

        assert result.exit_code == 0, result.output
        assert self._remote_argv(captured) == [
            "vq",
            "queue",
            "localhost",
            "-s",
            "completed",
            "--active",
        ]

    def test_queue_remote_json_rewrites_queue_handle_host(
        self, remote_cfg: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        stdout = json.dumps(
            [
                {
                    "id": "abc123def456",
                    "command": ["true"],
                    "cwd": "/tmp/abc123def456",
                    "cpus": 1,
                    "state": "running",
                    "submitted_at": "2026-07-03T05:00:00+00:00",
                    "effective_state": "stale-old-projection",
                    "queue_handle": {
                        "job_id": "abc123def456",
                        "host": "localhost",
                        "submitted_at": "2026-07-03T05:00:00+00:00",
                    },
                },
                {
                    "id": "invalidremote",
                    "command": ["true"],
                    "cwd": "/tmp/invalidremote",
                    "cpus": 1,
                    "state": "running",
                    "scheduler_target": "host_f",
                    "scheduler_state": ["running"],
                    "effective_state": "running",
                    "scheduler_running_confirmed": True,
                },
                {
                    "id": "emptytarget",
                    "command": ["true"],
                    "cwd": "/tmp/emptytarget",
                    "cpus": 1,
                    "state": "running",
                    "scheduler_target": "",
                    "scheduler_state": ["running"],
                    "effective_state": "running",
                    "scheduler_running_confirmed": True,
                    "queue_handle": {
                        "job_id": "emptytarget",
                        "host": "",
                    },
                },
            ]
        )
        captured = _patch_remote_vq_stdout(monkeypatch, stdout=f"{stdout}\n")

        result = CliRunner().invoke(main, ["queue", "fake-remote-test", "--json"])

        assert result.exit_code == 0, result.output
        assert self._remote_argv(captured) == [
            "vq",
            "queue",
            "localhost",
            "--json",
        ]
        payload = json.loads(result.stdout)
        assert payload[0]["queue_handle"]["host"] == "fake-remote-test"
        assert payload[0]["effective_state"] == "running"
        assert payload[0]["scheduler_running_confirmed"] is None
        assert payload[1]["effective_state"] == "scheduler_unknown"
        assert payload[1]["scheduler_running_confirmed"] is False
        assert payload[1]["queue_handle"]["host"] == "fake-remote-test"
        assert payload[2]["effective_state"] == "scheduler_unknown"
        assert payload[2]["scheduler_running_confirmed"] is False
        assert payload[2]["queue_handle"]["host"] == "fake-remote-test"

    def test_queue_remote_json_applies_active_locally_for_old_remote(
        self, remote_cfg: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        rows = [
            {
                "id": "submitting01",
                "command": ["true"],
                "cwd": "/tmp/submitting01",
                "cpus": 1,
                "state": "submitting",
            },
            {
                "id": "completed001",
                "command": ["true"],
                "cwd": "/tmp/completed001",
                "cpus": 1,
                "state": "completed",
            },
        ]
        captured = _patch_remote_vq_stdout(
            monkeypatch,
            stdout=json.dumps(rows) + "\n",
        )

        result = CliRunner().invoke(
            main,
            ["queue", "fake-remote-test", "--active", "--json"],
        )

        assert result.exit_code == 0, result.output
        assert self._remote_argv(captured) == [
            "vq",
            "queue",
            "localhost",
            "--json",
        ]
        assert [row["id"] for row in json.loads(result.stdout)] == [
            "submitting01"
        ]

    @pytest.mark.parametrize(
        ("selected_state", "expected_ids"),
        [
            ("poll_failed", ["pollfailed01"]),
            ("running", ["confirmed001", "pollfailed01"]),
        ],
    )
    def test_queue_remote_json_applies_monitor_filter_locally_for_old_remote(
        self,
        remote_cfg: Path,
        monkeypatch: pytest.MonkeyPatch,
        selected_state: str,
        expected_ids: list[str],
    ) -> None:
        rows = [
            {
                "id": "confirmed001",
                "command": ["true"],
                "cwd": "/tmp/confirmed001",
                "cpus": 1,
                "state": "running",
                "scheduler_target": "host_f",
                "scheduler_state": "running",
            },
            {
                "id": "pollfailed01",
                "command": ["true"],
                "cwd": "/tmp/pollfailed01",
                "cpus": 1,
                "state": "running",
                "scheduler_target": "host_f",
                "scheduler_state": "poll_failed",
            },
        ]
        captured = _patch_remote_vq_stdout(
            monkeypatch,
            stdout=json.dumps(rows) + "\n",
        )

        result = CliRunner().invoke(
            main,
            [
                "queue",
                "fake-remote-test",
                "-s",
                selected_state,
                "--json",
            ],
        )

        assert result.exit_code == 0, result.output
        assert self._remote_argv(captured) == [
            "vq",
            "queue",
            "localhost",
            "--json",
        ]
        payload = json.loads(result.stdout)
        assert [row["id"] for row in payload] == expected_ids
        by_id = {row["id"]: row for row in payload}
        if "pollfailed01" in by_id:
            assert by_id["pollfailed01"]["effective_state"] == "poll_failed"
            assert by_id["pollfailed01"]["scheduler_running_confirmed"] is False

    def test_scheduler_alias_refilters_old_driver_by_exact_phase(
        self, cli_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (cli_state / "cfg" / "config.toml").write_text(
            "[hosts.host_f-scheduler-test]\n"
            'ssh = "host_f.invalid"\n'
            'scheduler = "pbs"\n'
            'scheduler_dialect = "torque"\n'
            'scratch_root = "/home/USER"\n'
            'scheduler_driver = "driver-remote-test"\n'
            "\n"
            "[hosts.driver-remote-test]\n"
            'ssh = "driver-remote-test"\n'
            'remote_vq = "vq"\n'
        )
        rows = [
            {
                "id": "confirmed001",
                "command": ["true"],
                "cwd": "/tmp/confirmed001",
                "cpus": 1,
                "state": "running",
                "scheduler_target": "host_f-scheduler-test",
                "scheduler_state": "running",
            },
            {
                "id": "pollfailed01",
                "command": ["true"],
                "cwd": "/tmp/pollfailed01",
                "cpus": 1,
                "state": "running",
                "scheduler_target": "host_f-scheduler-test",
                "scheduler_state": "poll_failed",
            },
        ]
        captured = _patch_remote_vq_stdout(
            monkeypatch,
            stdout=json.dumps(rows) + "\n",
        )

        result = CliRunner().invoke(
            main,
            ["queue", "host_f-scheduler-test", "-s", "poll_failed", "--json"],
        )

        assert result.exit_code == 0, result.output
        assert shlex.split(captured[0][-1]) == [
            "vq",
            "queue",
            "localhost",
            "--json",
        ]
        payload = json.loads(result.stdout)
        assert [row["id"] for row in payload] == ["pollfailed01"]
        assert payload[0]["effective_state"] == "poll_failed"
        assert payload[0]["scheduler_running_confirmed"] is False

    def test_scheduler_alias_applies_active_locally_for_old_driver(
        self, cli_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (cli_state / "cfg" / "config.toml").write_text(
            "[hosts.host_f-scheduler-test]\n"
            'ssh = "host_f.invalid"\n'
            'scheduler = "pbs"\n'
            'scheduler_dialect = "torque"\n'
            'scratch_root = "/home/USER"\n'
            'scheduler_driver = "driver-remote-test"\n'
            "\n"
            "[hosts.driver-remote-test]\n"
            'ssh = "driver-remote-test"\n'
            'remote_vq = "vq"\n'
        )
        rows = [
            {
                "id": "submitting01",
                "command": ["true"],
                "cwd": "/tmp/submitting01",
                "cpus": 1,
                "state": "submitting",
                "scheduler_target": "host_f-scheduler-test",
                "scheduler_state": "submitting",
            },
            {
                "id": "completed001",
                "command": ["true"],
                "cwd": "/tmp/completed001",
                "cpus": 1,
                "state": "completed",
                "scheduler_target": "host_f-scheduler-test",
            },
        ]
        captured = _patch_remote_vq_stdout(
            monkeypatch,
            stdout=json.dumps(rows) + "\n",
        )

        result = CliRunner().invoke(
            main,
            ["queue", "host_f-scheduler-test", "--active", "--json"],
        )

        assert result.exit_code == 0, result.output
        assert shlex.split(captured[0][-1]) == [
            "vq",
            "queue",
            "localhost",
            "--json",
        ]
        assert [row["id"] for row in json.loads(result.stdout)] == [
            "submitting01"
        ]

    def test_scheduler_alias_malformed_running_row_fails_closed(
        self, cli_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (cli_state / "cfg" / "config.toml").write_text(
            "[hosts.host_f-scheduler-test]\n"
            'ssh = "host_f.invalid"\n'
            'scheduler = "pbs"\n'
            'scheduler_dialect = "torque"\n'
            'scratch_root = "/home/USER"\n'
            'scheduler_driver = "driver-remote-test"\n'
            "\n"
            "[hosts.driver-remote-test]\n"
            'ssh = "driver-remote-test"\n'
            'remote_vq = "vq"\n'
        )
        rows = [
            {
                "id": "confirmed001",
                "command": ["true"],
                "cwd": "/tmp/confirmed001",
                "cpus": 1,
                "state": "running",
                "scheduler_target": "host_f-scheduler-test",
                "scheduler_state": "running",
            },
            {
                "id": "malformed001",
                "command": ["true"],
                "cwd": "/tmp/malformed001",
                "cpus": 4,
                "state": "running",
                "scheduler_target": "host_f-scheduler-test",
                "scheduler_state": ["running"],
                "effective_state": "running",
                "pbs_state_label": "running",
                "scheduler_status_label": "running",
                "fetch_state_label": "live workspace on scheduler host",
                "scheduler_running_confirmed": True,
                "queue_handle": {
                    "job_id": "different-job",
                    "host": "untrusted-host",
                    "submitted_at": "not-the-row-time",
                },
            }
        ]
        _patch_remote_vq_stdout(monkeypatch, stdout=json.dumps(rows) + "\n")

        json_result = CliRunner().invoke(
            main,
            ["queue", "host_f-scheduler-test", "-s", "running", "--json"],
        )
        text_result = CliRunner().invoke(
            main,
            ["queue", "host_f-scheduler-test", "-s", "running"],
        )

        assert json_result.exit_code == 0, json_result.output
        payload = json.loads(json_result.stdout)
        assert [row["id"] for row in payload] == [
            "confirmed001",
            "malformed001",
        ]
        assert payload[1]["scheduler_state"] == ["running"]
        assert payload[1]["effective_state"] == "scheduler_unknown"
        assert payload[1]["scheduler_running_confirmed"] is False
        assert payload[1]["queue_handle"] == {
            "job_id": "malformed001",
            "host": "host_f-scheduler-test",
            "submitted_at": None,
        }
        assert text_result.exit_code == 0, text_result.output
        assert "malformed reservation row" in text_result.output
        assert "not confirmed running" in text_result.output
        assert "scheduler occupancy: 1/2" in text_result.output
        assert "scheduler_unknown=1" in text_result.output

    def test_queue_all_scheduler_alias_refilters_exact_phase_locally(
        self, cli_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (cli_state / "cfg" / "config.toml").write_text(
            "[hosts.host_f-scheduler-test]\n"
            'ssh = "host_f.invalid"\n'
            'scheduler = "pbs"\n'
            'scheduler_dialect = "torque"\n'
            'scratch_root = "/home/USER"\n'
            'scheduler_driver = "driver-remote-test"\n'
            "\n"
            "[hosts.driver-remote-test]\n"
            'ssh = "driver-remote-test"\n'
            'remote_vq = "vq"\n'
        )
        rows = [
            {
                "id": "confirmed001",
                "command": ["true"],
                "cwd": "/tmp/confirmed001",
                "cpus": 1,
                "state": "running",
                "scheduler_target": "host_f-scheduler-test",
                "scheduler_state": "running",
            },
            {
                "id": "pollfailed01",
                "command": ["true"],
                "cwd": "/tmp/pollfailed01",
                "cpus": 1,
                "state": "running",
                "scheduler_target": "host_f-scheduler-test",
                "scheduler_state": "poll_failed",
            },
        ]
        captured = _patch_remote_vq_stdout(
            monkeypatch,
            stdout=json.dumps(rows) + "\n",
        )
        monkeypatch.setenv("VQ_FANOUT_SERIAL", "1")

        result = CliRunner().invoke(
            main,
            ["queue", "--all", "-s", "poll_failed", "--json"],
        )

        assert result.exit_code == 0, result.output
        payload = json.loads(result.stdout)
        scheduler_rows = payload["host_f-scheduler-test"]
        assert [row["id"] for row in scheduler_rows] == ["pollfailed01"]
        assert scheduler_rows[0]["effective_state"] == "poll_failed"
        unfiltered_calls = [
            shlex.split(cmd[-1])
            for cmd in captured
            if "-s" not in shlex.split(cmd[-1])
        ]
        assert unfiltered_calls == [
            ["vq", "queue", "localhost", "--json"],
            ["vq", "queue", "localhost", "--json"],
        ]

    def test_queue_default_host_remote(
        self, remote_cfg: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured = _patch_remote_vq_stdout(monkeypatch, stdout="(no jobs)\n")
        result = CliRunner().invoke(main, ["queue"])  # default = fake-remote-test
        assert result.exit_code == 0
        assert self._remote_argv(captured) == ["vq", "queue", "localhost"]

    def test_queue_default_host_transport_failure_falls_back_to_localhost(
        self, remote_cfg: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured = _patch_remote_vq_failure(
            monkeypatch,
            stderr=(
                "ssh: connect to host fake-remote-test port 49999: "
                "Network is unreachable\n"
            ),
        )
        result = CliRunner().invoke(main, ["list"])
        assert result.exit_code == 0, result.output
        assert "(no jobs)" in result.output
        combined = result.output + result.stderr
        assert "default_host 'fake-remote-test' is unreachable" in combined
        assert "showing localhost" in combined
        assert self._remote_argv(captured) == ["vq", "queue", "localhost"]

    def test_queue_default_host_marked_down_skips_remote_probe(
        self, remote_cfg: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured = _patch_remote_vq_failure(
            monkeypatch,
            stderr="ssh should not be called\n",
        )
        host_status.mark_down("fake-remote-test", "off network")
        result = CliRunner().invoke(main, ["list"])
        assert result.exit_code == 0, result.output
        assert "(no jobs)" in result.output
        combined = result.output + result.stderr
        assert "default_host 'fake-remote-test' is marked down" in combined
        assert "showing localhost" in combined
        assert captured == []

    def test_queue_explicit_remote_transport_failure_still_errors(
        self, remote_cfg: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _patch_remote_vq_failure(
            monkeypatch,
            stderr=(
                "ssh: connect to host fake-remote-test port 49999: "
                "Network is unreachable\n"
            ),
        )
        result = CliRunner().invoke(main, ["list", "fake-remote-test"])
        assert result.exit_code != 0
        assert "remote vq failed" in result.output
        assert "showing localhost" not in result.output

    def test_status_remote_passes_jobid(
        self, remote_cfg: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured = _patch_remote_vq_stdout(monkeypatch, stdout="state: pending\n")
        result = CliRunner().invoke(main, ["status", "fake-remote-test", "abc123def456"])
        assert result.exit_code == 0
        assert self._remote_argv(captured) == [
            "vq", "status", "localhost", "abc123def456",
        ]

    def test_status_remote_passes_tail(
        self, remote_cfg: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured = _patch_remote_vq_stdout(monkeypatch, stdout="state: pending\n")
        CliRunner().invoke(
            main, ["status", "fake-remote-test", "abc123def456", "-n", "10"]
        )
        assert self._remote_argv(captured) == [
            "vq", "status", "localhost", "abc123def456", "-n", "10",
        ]

    def test_status_remote_json_rewrites_queue_handle_host(
        self, remote_cfg: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        stdout = json.dumps(
            {
                "id": "abc123def456",
                "command": ["true"],
                "cwd": "/tmp/abc123def456",
                "cpus": 1,
                "state": "running",
                "submitted_at": "2026-07-03T05:00:00+00:00",
                "effective_state": "stale-old-projection",
                "queue_handle": {
                    "job_id": "abc123def456",
                    "host": "localhost",
                    "submitted_at": "2026-07-03T05:00:00+00:00",
                },
            }
        )
        captured = _patch_remote_vq_stdout(monkeypatch, stdout=f"{stdout}\n")

        result = CliRunner().invoke(
            main, ["status", "fake-remote-test", "abc123def456", "--json"]
        )

        assert result.exit_code == 0, result.output
        assert self._remote_argv(captured) == [
            "vq", "status", "localhost", "abc123def456", "--json",
        ]
        payload = json.loads(result.stdout)
        assert payload["queue_handle"]["host"] == "fake-remote-test"
        assert payload["effective_state"] == "running"
        assert payload["scheduler_running_confirmed"] is None

    def test_status_remote_json_overrides_old_scheduler_projection(
        self, remote_cfg: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        stdout = json.dumps(
            {
                "id": "abc123def456",
                "command": ["true"],
                "cwd": "/tmp/abc123def456",
                "cpus": 1,
                "state": "running",
                "scheduler_target": "host_f-scheduler-test",
                "scheduler_state": "poll_failed",
                "submitted_at": "2026-07-03T05:00:00+00:00",
                "effective_state": "running",
                "queue_handle": {
                    "job_id": "abc123def456",
                    "host": "host_f-scheduler-test",
                    "submitted_at": "2026-07-03T05:00:00+00:00",
                },
            }
        )
        _patch_remote_vq_stdout(monkeypatch, stdout=f"{stdout}\n")

        result = CliRunner().invoke(
            main, ["status", "fake-remote-test", "abc123def456", "--json"]
        )

        assert result.exit_code == 0, result.output
        payload = json.loads(result.stdout)
        assert payload["state"] == "running"
        assert payload["scheduler_state"] == "poll_failed"
        assert payload["effective_state"] == "poll_failed"
        assert payload["scheduler_running_confirmed"] is False
        assert payload["pbs_state_label"] == "poll_failed"
        assert payload["scheduler_status_label"] == "poll_failed"
        assert payload["fetch_state_label"] == (
            "scheduler poll failed; remote workspace state unknown"
        )
        assert payload["queue_handle"]["host"] == "host_f-scheduler-test"

    def test_status_remote_json_bounds_unknown_scheduler_labels(
        self, remote_cfg: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        raw_phase = "future\nFORGED scheduler phase"
        stdout = json.dumps(
            {
                "id": "abc123def456",
                "command": ["true"],
                "cwd": "/tmp/abc123def456",
                "cpus": 1,
                "state": "running",
                "scheduler_target": "host_f-scheduler-test",
                "scheduler_state": raw_phase,
                "effective_state": "running",
                "pbs_state_label": raw_phase,
                "scheduler_status_label": raw_phase,
                "fetch_state_label": "live workspace on scheduler host",
            }
        )
        _patch_remote_vq_stdout(monkeypatch, stdout=f"{stdout}\n")

        result = CliRunner().invoke(
            main, ["status", "fake-remote-test", "abc123def456", "--json"]
        )

        assert result.exit_code == 0, result.output
        payload = json.loads(result.stdout)
        assert payload["scheduler_state"] == raw_phase
        assert payload["effective_state"] == "scheduler_unknown"
        assert payload["scheduler_running_confirmed"] is False
        assert payload["pbs_state_label"] == "scheduler_unknown"
        assert payload["scheduler_status_label"] == "scheduler_unknown"
        assert payload["fetch_state_label"] == (
            "scheduler phase or ownership unknown; remote workspace state unknown"
        )

    def test_status_remote_json_malformed_scheduler_row_fails_closed(
        self, remote_cfg: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        stdout = json.dumps(
            {
                "id": "abc123def456",
                "command": ["true"],
                "cwd": "/tmp/abc123def456",
                "cpus": 1,
                "state": "running",
                "scheduler_target": "",
                "scheduler_state": ["running"],
                "effective_state": "running",
                "scheduler_running_confirmed": True,
                "pbs_state_label": "running\nFORGED",
                "scheduler_status_label": "running\nFORGED",
                "fetch_state_label": "live workspace on scheduler host",
                "queue_handle": {
                    "job_id": "different-job",
                    "host": "untrusted-host",
                },
            }
        )
        _patch_remote_vq_stdout(monkeypatch, stdout=f"{stdout}\n")

        result = CliRunner().invoke(
            main, ["status", "fake-remote-test", "abc123def456", "--json"]
        )

        assert result.exit_code == 0, result.output
        payload = json.loads(result.stdout)
        assert payload["effective_state"] == "scheduler_unknown"
        assert payload["scheduler_running_confirmed"] is False
        assert payload["queue_handle"] == {
            "job_id": "abc123def456",
            "host": "fake-remote-test",
            "submitted_at": None,
        }
        assert payload["pbs_state_label"] == "scheduler_unknown"
        assert payload["scheduler_status_label"] == "scheduler_unknown"
        assert payload["fetch_state_label"] == (
            "scheduler phase or ownership unknown; remote workspace state unknown"
        )

    def test_status_scheduler_host_delegates_to_driver(
        self, cli_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (cli_state / "cfg" / "config.toml").write_text(
            "[hosts.host_f-scheduler-test]\n"
            'ssh = "host_f.invalid"\n'
            'scheduler = "pbs"\n'
            'scheduler_dialect = "torque"\n'
            'scratch_root = "/home/USER"\n'
            'scheduler_driver = "driver-remote-test"\n'
            "\n"
            "[hosts.driver-remote-test]\n"
            'ssh = "driver-remote-test"\n'
            'remote_vq = "vq"\n'
        )
        stdout = json.dumps(
            {
                "id": "abc123def456",
                "state": "running",
                "scheduler_target": "host_f-scheduler-test",
                "submitted_at": "2026-07-03T05:00:00+00:00",
                "queue_handle": {
                    "job_id": "abc123def456",
                    "host": "host_f-scheduler-test",
                    "submitted_at": "2026-07-03T05:00:00+00:00",
                },
            }
        )
        captured = _patch_remote_vq_stdout(monkeypatch, stdout=f"{stdout}\n")

        result = CliRunner().invoke(
            main, ["status", "host_f-scheduler-test", "abc123def456", "--json"]
        )

        assert result.exit_code == 0, result.output
        assert len(captured) == 1, captured
        cmd = captured[0]
        assert cmd[-2] == "driver-remote-test"
        assert shlex.split(cmd[-1]) == [
            "vq",
            "status",
            "localhost",
            "abc123def456",
            "--json",
        ]
        payload = json.loads(result.stdout)
        assert payload["queue_handle"]["host"] == "host_f-scheduler-test"

    def test_overview_scheduler_host_uses_driver_synthesis(
        self, cli_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (cli_state / "cfg" / "config.toml").write_text(
            "[hosts.host_f-scheduler-test]\n"
            'ssh = "host_f.invalid"\n'
            'scheduler = "pbs"\n'
            'scheduler_dialect = "torque"\n'
            'scratch_root = "/home/USER"\n'
            'scheduler_driver = "driver-remote-test"\n'
            "\n"
            "[hosts.driver-remote-test]\n"
            'ssh = "driver-remote-test"\n'
            'remote_vq = "vq"\n'
        )
        from vq.overview import HostOverview

        seen: list[str] = []

        def fake_scheduler_overview(host, host_cfg, cfg, **kw):
            seen.append(host)
            return HostOverview(host=host, queue_counts={"pending": 1})

        def fail_remote(*args, **kwargs):
            raise AssertionError("scheduler host must not use plain remote overview")

        monkeypatch.setattr(
            "vq.overview.gather_scheduler_overview",
            fake_scheduler_overview,
        )
        monkeypatch.setattr("vq.overview.gather_overview_remote", fail_remote)

        result = CliRunner().invoke(main, ["overview", "host_f-scheduler-test"])

        assert result.exit_code == 0, result.output
        assert seen == ["host_f-scheduler-test"]
        assert "pending" in result.output

    def test_kill_remote(
        self, remote_cfg: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured = _patch_remote_vq_stdout(monkeypatch, stdout="killed: abc123def456\n")
        result = CliRunner().invoke(main, ["kill", "fake-remote-test", "abc123def456"])
        assert result.exit_code == 0
        assert "killed" in result.output
        assert self._remote_argv(captured) == [
            "vq", "kill", "localhost", "abc123def456",
        ]

    def test_kill_remote_forwards_reason(
        self, remote_cfg: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured = _patch_remote_vq_stdout(monkeypatch, stdout="killed: abc123def456\n")
        result = CliRunner().invoke(
            main,
            [
                "kill",
                "--reason",
                "obsolete vibe-qc version; resubmit after update",
                "fake-remote-test",
                "abc123def456",
            ],
        )
        assert result.exit_code == 0
        assert self._remote_argv(captured) == [
            "vq",
            "kill",
            "--reason",
            "obsolete vibe-qc version; resubmit after update",
            "localhost",
            "abc123def456",
        ]

    def test_kill_remote_forwards_resubmit_after_update(
        self, remote_cfg: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured = _patch_remote_vq_stdout(
            monkeypatch,
            stdout="killed: abc123def456\nresubmitted abc123def456 -> def456abc123\n",
        )
        result = CliRunner().invoke(
            main,
            [
                "kill",
                "--reason",
                "obsolete vibe-qc version; resubmit after update",
                "--restart-after-update",
                "fake-remote-test",
                "abc123def456",
            ],
        )
        assert result.exit_code == 0
        assert self._remote_argv(captured) == [
            "vq",
            "kill",
            "--reason",
            "obsolete vibe-qc version; resubmit after update",
            "--resubmit",
            "localhost",
            "abc123def456",
        ]

    def test_kill_scheduler_host_delegates_to_driver(
        self, cli_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (cli_state / "cfg" / "config.toml").write_text(
            "[hosts.host_f-scheduler-test]\n"
            'ssh = "host_f.invalid"\n'
            'scheduler = "pbs"\n'
            'scheduler_dialect = "torque"\n'
            'scratch_root = "/home/USER"\n'
            'scheduler_driver = "driver-remote-test"\n'
            "\n"
            "[hosts.driver-remote-test]\n"
            'ssh = "driver-remote-test"\n'
            'remote_vq = "vq"\n'
        )
        captured = _patch_remote_vq_stdout(monkeypatch, stdout="killed: abc123def456\n")

        result = CliRunner().invoke(
            main, ["kill", "host_f-scheduler-test", "abc123def456"]
        )

        assert result.exit_code == 0, result.output
        assert len(captured) == 1, captured
        cmd = captured[0]
        assert cmd[-2] == "driver-remote-test"
        assert shlex.split(cmd[-1]) == [
            "vq",
            "kill",
            "localhost",
            "abc123def456",
        ]

    def test_wait_scheduler_host_delegates_to_driver(
        self, cli_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (cli_state / "cfg" / "config.toml").write_text(
            "[hosts.host_f-scheduler-test]\n"
            'ssh = "host_f.invalid"\n'
            'scheduler = "pbs"\n'
            'scheduler_dialect = "torque"\n'
            'scratch_root = "/home/USER"\n'
            'scheduler_driver = "driver-remote-test"\n'
            "\n"
            "[hosts.driver-remote-test]\n"
            'ssh = "driver-remote-test"\n'
            'remote_vq = "vq"\n'
        )
        captured = _patch_remote_vq_stdout(
            monkeypatch,
            stdout='{"state": "completed", "exit_code": 0}\n',
        )

        result = CliRunner().invoke(
            main,
            [
                "wait",
                "host_f-scheduler-test",
                "abc123def456",
                "--poll-interval",
                "0.1",
                "--timeout",
                "1",
            ],
        )

        assert result.exit_code == 0, result.output
        assert len(captured) == 1, captured
        cmd = captured[0]
        assert cmd[-2] == "driver-remote-test"
        assert shlex.split(cmd[-1]) == [
            "vq",
            "status",
            "localhost",
            "abc123def456",
            "--json",
        ]

    def test_resubmit_scheduler_host_delegates_to_driver(
        self, cli_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (cli_state / "cfg" / "config.toml").write_text(
            "[hosts.host_f-scheduler-test]\n"
            'ssh = "host_f.invalid"\n'
            'scheduler = "pbs"\n'
            'scheduler_dialect = "torque"\n'
            'scratch_root = "/home/USER"\n'
            'scheduler_driver = "driver-remote-test"\n'
            "\n"
            "[hosts.driver-remote-test]\n"
            'ssh = "driver-remote-test"\n'
            'remote_vq = "vq"\n'
        )
        captured = _patch_remote_vq_stdout(monkeypatch, stdout="0123456789ab\n")

        result = CliRunner().invoke(
            main, ["resubmit", "host_f-scheduler-test", "abc123def456"]
        )

        assert result.exit_code == 0, result.output
        assert result.output.strip() == "0123456789ab"
        assert len(captured) == 1, captured
        cmd = captured[0]
        assert cmd[-2] == "driver-remote-test"
        assert shlex.split(cmd[-1]) == [
            "vq",
            "resubmit",
            "host_f-scheduler-test",
            "abc123def456",
        ]

    def test_resubmit_state_scheduler_host_delegates_to_driver(
        self, cli_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (cli_state / "cfg" / "config.toml").write_text(
            "[hosts.host_f-scheduler-test]\n"
            'ssh = "host_f.invalid"\n'
            'scheduler = "pbs"\n'
            'scheduler_dialect = "torque"\n'
            'scratch_root = "/home/USER"\n'
            'scheduler_driver = "driver-remote-test"\n'
            "\n"
            "[hosts.driver-remote-test]\n"
            'ssh = "driver-remote-test"\n'
            'remote_vq = "vq"\n'
        )
        captured = _patch_remote_vq_stdout(monkeypatch, stdout="0123456789ab\n")

        result = CliRunner().invoke(
            main, ["resubmit", "host_f-scheduler-test", "--state", "failed"]
        )

        assert result.exit_code == 0, result.output
        assert result.output.strip() == "0123456789ab"
        assert len(captured) == 1, captured
        cmd = captured[0]
        assert cmd[-2] == "driver-remote-test"
        assert shlex.split(cmd[-1]) == [
            "vq",
            "resubmit",
            "host_f-scheduler-test",
            "--state",
            "failed",
        ]

    def test_remote_failure_surfaces_clean_message(
        self, remote_cfg: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from vq import transport as transport_module

        def fake_run(cmd: list[str], **kw):
            return subprocess.CompletedProcess(
                args=cmd,
                returncode=255,
                stdout="",
                stderr="ssh: Could not resolve hostname fake-remote-test\n",
            )

        monkeypatch.setattr(transport_module.subprocess, "run", fake_run)
        result = CliRunner().invoke(main, ["queue", "fake-remote-test"])
        assert result.exit_code != 0
        assert "Could not resolve hostname" in result.output


class TestFetchCLI:
    def test_fetch_local_copies_workspace(self, cli_state: Path) -> None:
        f = cli_state / "in.py"
        f.write_text("print('x')")
        jobid = CliRunner().invoke(main, ["submit", "localhost", str(f)]).output.strip()
        out = cli_state / "fetched"
        result = CliRunner().invoke(
            main, ["fetch", "localhost", jobid, "-o", str(out)]
        )
        assert result.exit_code == 0, result.output
        assert (out / jobid / "in.py").read_text() == "print('x')"

    def test_fetch_local_repeat_refreshes_and_reports_the_fetch_time(
        self, cli_state: Path
    ) -> None:
        """(#114): a repeated fetch succeeds AND refreshes.

        It used to return the previous destination untouched while printing a
        byte-identical ``fetched -> ...`` line, so a poller could not tell a
        refreshed snapshot from a frozen one. The line now carries
        ``fetched_at``.
        """
        f = cli_state / "repeat.py"
        f.write_text("print('repeat')")
        jobid = CliRunner().invoke(main, ["submit", "localhost", str(f)]).output.strip()
        out = cli_state / "fetched"

        first = CliRunner().invoke(
            main, ["fetch", "localhost", jobid, "-o", str(out)]
        )
        second = CliRunner().invoke(
            main, ["fetch", "localhost", jobid, "-o", str(out)]
        )

        assert first.exit_code == 0, first.output
        assert second.exit_code == 0, second.output
        assert f"fetched -> {out / jobid}" in second.output
        assert "fetched_at=" in second.output
        assert (out / jobid / "repeat.py").read_text() == "print('repeat')"

    @pytest.mark.parametrize("explicit_workspace", [False, True])
    def test_fetch_separate_local_workdir_requires_selection(
        self, cli_state: Path, explicit_workspace: bool
    ) -> None:
        payload = cli_state / "input.py"
        payload.write_text("print('input')")
        jobid = CliRunner().invoke(main, ["submit", "localhost", str(payload)]).output.strip()
        spec = JobSpec.read(paths.spec_path(jobid))
        workdir = cli_state / "scratch"
        workdir.mkdir()
        (workdir / "result.json").write_text('{"energy": -1.0}')
        spec.workdir = str(workdir)
        spec.state = JobState.COMPLETED
        spec.write(paths.spec_path(jobid))
        args = ["fetch", "localhost", jobid, "-o", str(cli_state / "out")]
        if explicit_workspace:
            args.append("--workspace")
        result = CliRunner().invoke(main, args)
        assert result.exit_code == (0 if explicit_workspace else 1), result.output
        if not explicit_workspace:
            assert "--workdir" in result.output
            assert "refusing to report" in result.output
        workdir_args = args[:-1] if explicit_workspace else args
        workdir_result = CliRunner().invoke(main, [*workdir_args, "--workdir"])
        assert workdir_result.exit_code == 0, workdir_result.output
        assert (cli_state / "out" / f"{jobid}-workdir" / "result.json").is_file()

    def test_fetch_legacy_remote_requires_explicit_workspace(
        self, cli_state: Path, monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        from vq import fetch as fetch_module

        (cli_state / "cfg" / "config.toml").write_text('[hosts.old]\nssh = "old.invalid"\n')
        jobid = "legacycli001"
        diagnosis = {
            "schema": "vq.terminal-diagnosis.v1", "jobid": jobid,
            "submitted_at": "2026-08-20T10:11:12+00:00", "state": "completed",
            "scheduler_target": None,
        }
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w") as tf:
            for name, content in {
                "input.py": "# submitted input\n",
                fetch_module.TERMINAL_DIAGNOSIS_SIDECAR: json.dumps(diagnosis),
            }.items():
                data = content.encode()
                member = tarfile.TarInfo(f"{jobid}/{name}")
                member.size = len(data)
                tf.addfile(member, io.BytesIO(data))

        class Stream:
            def __init__(self, *args, **kwargs):
                self.stdout = io.BytesIO(buffer.getvalue())
                self.stderr = io.BytesIO()
                self.returncode = 0

            def wait(self):
                return self.returncode

            def kill(self):
                self.returncode = -9

        monkeypatch.setattr(transport.subprocess, "Popen", Stream)
        monkeypatch.setattr(
            transport, "run_owned_subprocess",
            lambda cmd, **kwargs: subprocess.CompletedProcess(
                cmd, 2, stdout="", stderr="Error: No such command 'mark-fetched'.\n"
            ),
        )
        args = ["fetch", "old", jobid, "-o", str(cli_state / "out")]
        plain = CliRunner().invoke(main, args)
        assert plain.exit_code == 1, plain.output
        assert "--workdir" in plain.output
        explicit = CliRunner().invoke(main, [*args, "--workspace"])
        assert explicit.exit_code == 0, explicit.output
        assert "last_fetched_at was not recorded" in caplog.text
        assert (cli_state / "out" / jobid / "input.py").is_file()

    def test_fetch_local_json_reports_destination_and_queue_handle(
        self, cli_state: Path
    ) -> None:
        f = cli_state / "in.py"
        f.write_text("print('x')")
        jobid = CliRunner().invoke(
            main, ["submit", "localhost", str(f)]
        ).output.strip()
        spec_path = paths.spec_path(jobid)
        spec = JobSpec.read(spec_path)
        spec.submitted_at = "2026-08-02T12:34:56+00:00"
        spec.write(spec_path)
        out = cli_state / "fetched"

        result = CliRunner().invoke(
            main, ["fetch", "localhost", jobid, "-o", str(out), "--json"]
        )

        assert result.exit_code == 0, result.output
        payload = json.loads(result.stdout)
        assert payload["jobid"] == jobid
        assert payload["kind"] == "workspace"
        assert payload["destination"] == str(out / jobid)
        assert payload["queue_handle"] == {
            "job_id": jobid,
            "host": "localhost",
            "submitted_at": "2026-08-02T12:34:56+00:00",
        }
        assert (out / jobid / "in.py").read_text() == "print('x')"

    def test_fetch_json_keeps_requested_id_when_spec_inner_id_differs(
        self, cli_state: Path
    ) -> None:
        f = cli_state / "in.py"
        f.write_text("print('x')")
        requested_jobid = CliRunner().invoke(
            main, ["submit", "localhost", str(f)]
        ).output.strip()
        spec_path = paths.spec_path(requested_jobid)
        spec = JobSpec.read(spec_path)
        spec.id = "innerjob0001"
        spec.submitted_at = "2026-08-02T12:34:56+00:00"
        spec.write(spec_path)
        out = cli_state / "fetched"

        result = CliRunner().invoke(
            main,
            ["fetch", "localhost", requested_jobid, "-o", str(out), "--json"],
        )

        assert result.exit_code == 0, result.output
        payload = json.loads(result.stdout)
        assert payload["jobid"] == requested_jobid
        assert payload["queue_handle"] == {
            "job_id": requested_jobid,
            "host": "localhost",
            "submitted_at": "2026-08-02T12:34:56+00:00",
        }
        assert payload["destination"] == str(out / "innerjob0001")

    def test_fetch_json_never_uses_unsafe_scheduler_target_as_handle_host(
        self, cli_state: Path
    ) -> None:
        f = cli_state / "in.py"
        f.write_text("print('x')")
        jobid = CliRunner().invoke(
            main, ["submit", "localhost", str(f)]
        ).output.strip()
        spec_path = paths.spec_path(jobid)
        spec = JobSpec.read(spec_path)
        spec.scheduler_target = "host_f\nFORGED host\x1b[31m" + ("x" * 150)
        spec.write(spec_path)
        out = cli_state / "fetched"

        result = CliRunner().invoke(
            main, ["fetch", "localhost", jobid, "-o", str(out), "--json"]
        )

        assert result.exit_code == 0, result.output
        payload = json.loads(result.stdout)
        assert payload["queue_handle"]["host"] == "localhost"
        assert "FORGED" not in result.stdout

    def test_fetch_default_host_local(self, cli_state: Path) -> None:
        (cli_state / "cfg" / "config.toml").write_text('default_host = "localhost"\n')
        f = cli_state / "x.py"
        f.write_text("hi")
        jobid = CliRunner().invoke(main, ["submit", "localhost", str(f)]).output.strip()
        out = cli_state / "fetched"
        result = CliRunner().invoke(main, ["fetch", jobid, "-o", str(out)])
        assert result.exit_code == 0, result.output
        assert (out / jobid / "x.py").read_text() == "hi"

    def test_fetch_unknown_jobid_errors(self, cli_state: Path) -> None:
        result = CliRunner().invoke(
            main, ["fetch", "localhost", "deadbeefface", "-o", str(cli_state / "out")]
        )
        assert result.exit_code != 0
        assert "no such job" in result.output

    def test_fetch_scheduler_host_delegates_to_driver(
        self, cli_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (cli_state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
            "\n"
            "[hosts.host_f-scheduler-test]\n"
            'ssh = "host_f.invalid"\n'
            'scheduler = "pbs"\n'
            'scheduler_dialect = "torque"\n'
            'scratch_root = "/home/USER"\n'
            'scheduler_driver = "driver-remote-test"\n'
            "\n"
            "[hosts.driver-remote-test]\n"
            'ssh = "driver.invalid"\n'
        )
        captured: dict[str, object] = {}

        def fake_fetch_remote(host_cfg, jobid: str, output_dir: Path) -> Path:
            captured["ssh"] = host_cfg.ssh
            captured["jobid"] = jobid
            captured["output_dir"] = output_dir
            dst = output_dir / jobid
            _write_test_fetch_manifest(dst, jobid)
            return dst

        monkeypatch.setattr("vq.cli.fetch_remote", fake_fetch_remote)

        out = cli_state / "fetched"
        result = CliRunner().invoke(
            main, ["fetch", "host_f-scheduler-test", "abc123def456", "-o", str(out)]
        )

        assert result.exit_code == 0, result.output
        assert captured == {
            "ssh": "driver.invalid",
            "jobid": "abc123def456",
            "output_dir": out,
        }
        assert "fetched ->" in result.output

    @pytest.mark.parametrize("fetch_workdir", [False, True], ids=["workspace", "workdir"])
    @pytest.mark.parametrize(
        "manifest_bytes",
        [None, b'{"schema":"vq.fetch-manifest.v1"}', b"\xff"],
        ids=["missing", "schema-only", "non-utf8"],
    )
    def test_fetch_whole_tree_without_valid_manifest_fails_closed(
        self,
        cli_state: Path,
        monkeypatch: pytest.MonkeyPatch,
        fetch_workdir: bool,
        manifest_bytes: bytes | None,
    ) -> None:
        (cli_state / "cfg" / "config.toml").write_text(
            "[hosts.remote-test]\n"
            'ssh = "remote.invalid"\n'
        )

        def fake_fetch_remote(host_cfg, jobid: str, output_dir: Path) -> Path:
            dst = output_dir / jobid
            dst.mkdir(parents=True)
            if manifest_bytes is not None:
                sidecar = dst / "_vq" / "fetch-manifest.json"
                sidecar.parent.mkdir()
                sidecar.write_bytes(manifest_bytes)
            return dst

        function_name = "fetch_workdir_remote" if fetch_workdir else "fetch_remote"
        monkeypatch.setattr(f"vq.cli.{function_name}", fake_fetch_remote)

        out = cli_state / "fetched"
        args = ["fetch", "remote-test", "abc123def456", "-o", str(out)]
        if fetch_workdir:
            args.append("--workdir")
        result = CliRunner().invoke(
            main, args
        )

        assert result.exit_code != 0
        assert "fetch-manifest metadata" in result.output
        assert "refusing to report success" in result.output
        assert "fetched ->" not in result.output

    def test_fetch_single_artifact_does_not_require_manifest(
        self, cli_state: Path
    ) -> None:
        source = cli_state / "input.py"
        source.write_text("print('done')")
        jobid = CliRunner().invoke(
            main, ["submit", "localhost", str(source)]
        ).output.strip()
        spec = JobSpec.read(paths.spec_path(jobid))
        (Path(spec.cwd) / "result.qvf").write_text("archive bytes")
        out = cli_state / "fetched"

        result = CliRunner().invoke(
            main,
            [
                "fetch",
                "localhost",
                jobid,
                "--name",
                "result.qvf",
                "-o",
                str(out),
            ],
        )

        assert result.exit_code == 0, result.output
        assert (out / "result.qvf").read_text() == "archive bytes"
        assert not (out / "_vq" / "fetch-manifest.json").exists()

    def test_fetch_scheduler_host_json_keeps_scheduler_queue_handle_host(
        self, cli_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (cli_state / "cfg" / "config.toml").write_text(
            "[hosts.host_f-scheduler-test]\n"
            'ssh = "host_f.invalid"\n'
            'scheduler = "pbs"\n'
            'scheduler_dialect = "torque"\n'
            'scratch_root = "/home/USER"\n'
            'scheduler_driver = "driver-remote-test"\n'
            "\n"
            "[hosts.driver-remote-test]\n"
            'ssh = "driver.invalid"\n'
        )

        def fake_fetch_remote(host_cfg, jobid: str, output_dir: Path) -> Path:
            dst = output_dir / jobid
            _write_test_fetch_manifest(dst, jobid, source_host=host_cfg.ssh)
            return dst

        monkeypatch.setattr("vq.cli.fetch_remote", fake_fetch_remote)

        out = cli_state / "fetched"
        result = CliRunner().invoke(
            main,
            [
                "fetch",
                "host_f-scheduler-test",
                "abc123def456",
                "-o",
                str(out),
                "--json",
            ],
        )

        assert result.exit_code == 0, result.output
        payload = json.loads(result.stdout)
        assert payload["destination"] == str(out / "abc123def456")
        assert payload["queue_handle"] == {
            "job_id": "abc123def456",
            "host": "host_f-scheduler-test",
            "submitted_at": None,
        }

    def test_fetch_workdir_flag_pulls_workdir_payload(
        self, cli_state: Path
    ) -> None:
        """v0.7.7 *Cerf's Datagram*: `vq fetch JOBID --workdir` reads
        ``spec.workdir`` and lands it at ``<-o>/<jobid>-workdir/`` so
        operators don't have to ssh in to copy ``$VQ_WORKDIR``."""
        from vq.spec import JobSpec

        f = cli_state / "in.py"
        f.write_text("print('x')")
        jobid = CliRunner().invoke(
            main, ["submit", "localhost", str(f)]
        ).output.strip()

        # Materialize a workdir + point the spec at it (the daemon
        # normally does this at dispatch — we shortcut here).
        wd = cli_state / "wd" / jobid
        wd.mkdir(parents=True)
        (wd / "scratch.dat").write_text("scratchy")
        (wd / "checkpoint.h5").write_text("h5-bytes")
        spec_path = paths.queue_dir() / f"{jobid}.json"
        spec = JobSpec.read(spec_path)
        spec.workdir = str(wd)
        spec.write(spec_path)

        out = cli_state / "fetched"
        result = CliRunner().invoke(
            main, ["fetch", "localhost", jobid, "--workdir", "-o", str(out)]
        )
        assert result.exit_code == 0, result.output
        assert (out / f"{jobid}-workdir" / "scratch.dat").read_text() == "scratchy"
        assert (out / f"{jobid}-workdir" / "checkpoint.h5").read_text() == "h5-bytes"
        # Banner uses "fetched workdir" so the operator sees what
        # payload landed.
        assert "fetched workdir" in result.output

    def test_fetch_workdir_json_reports_workdir_kind(
        self, cli_state: Path
    ) -> None:
        from vq.spec import JobSpec

        f = cli_state / "in.py"
        f.write_text("print('x')")
        jobid = CliRunner().invoke(
            main, ["submit", "localhost", str(f)]
        ).output.strip()

        wd = cli_state / "wd" / jobid
        wd.mkdir(parents=True)
        (wd / "scratch.dat").write_text("scratchy")
        spec_path = paths.queue_dir() / f"{jobid}.json"
        spec = JobSpec.read(spec_path)
        spec.workdir = str(wd)
        spec.write(spec_path)

        out = cli_state / "fetched"
        result = CliRunner().invoke(
            main,
            [
                "fetch",
                "localhost",
                jobid,
                "--workdir",
                "-o",
                str(out),
                "--json",
            ],
        )

        assert result.exit_code == 0, result.output
        payload = json.loads(result.stdout)
        assert payload["kind"] == "workdir"
        assert payload["destination"] == str(out / f"{jobid}-workdir")
        assert payload["queue_handle"]["job_id"] == jobid

    def test_fetch_workdir_no_workdir_field_errors(
        self, cli_state: Path
    ) -> None:
        """The workdir verb refuses cleanly when the spec lacks a
        workdir — pre-v0.6.54 specs or a future --no-workdir submit."""
        f = cli_state / "in.py"
        f.write_text("pass")
        jobid = CliRunner().invoke(
            main, ["submit", "localhost", str(f)]
        ).output.strip()
        # The default submit path leaves spec.workdir = None.
        result = CliRunner().invoke(
            main,
            ["fetch", "localhost", jobid, "--workdir",
             "-o", str(cli_state / "out")],
        )
        assert result.exit_code != 0
        assert "no workdir" in result.output


class TestTarWorkspaceInternalVerb:
    def test_emits_tar_to_stdout(self, cli_state: Path) -> None:
        f = cli_state / "in.py"
        f.write_text("hello")
        jobid = CliRunner().invoke(main, ["submit", "localhost", str(f)]).output.strip()
        result = CliRunner().invoke(
            main, ["tar-workspace", jobid], standalone_mode=False
        )
        # CliRunner captures stdout as bytes when the command writes binary
        assert result.exit_code == 0, getattr(result, "output", "")
        # Click's runner stores stdout in result.stdout_bytes when binary;
        # fall back to result.output for text-only.
        raw = getattr(result, "stdout_bytes", None) or result.output.encode()
        with tarfile.open(fileobj=io.BytesIO(raw), mode="r|") as tf:
            names = tf.getnames()
        assert jobid in names
        assert f"{jobid}/in.py" in names

    def test_unknown_jobid_exits_nonzero(self, cli_state: Path) -> None:
        result = CliRunner().invoke(main, ["tar-workspace", "nope12345678"])
        assert result.exit_code != 0

    def test_tar_workspace_hidden_from_help(self) -> None:
        result = CliRunner().invoke(main, ["--help"])
        # The hidden=True flag means it shouldn't appear in --help
        assert "tar-workspace" not in result.output


class TestTarWorkdirInternalVerb:
    """v0.7.7: ``vq tar-workdir JOBID`` mirrors ``tar-workspace`` but
    streams ``spec.workdir`` instead of ``spec.cwd``. Hidden internal
    verb that feeds ``fetch_workdir_remote``."""

    def test_emits_tar_to_stdout(self, cli_state: Path) -> None:
        from vq.spec import JobSpec

        f = cli_state / "in.py"
        f.write_text("hi")
        jobid = CliRunner().invoke(
            main, ["submit", "localhost", str(f)]
        ).output.strip()
        wd = cli_state / "wd" / jobid
        wd.mkdir(parents=True)
        (wd / "scratch.bin").write_text("data")
        spec_path = paths.queue_dir() / f"{jobid}.json"
        spec = JobSpec.read(spec_path)
        spec.workdir = str(wd)
        spec.write(spec_path)

        result = CliRunner().invoke(
            main, ["tar-workdir", jobid], standalone_mode=False
        )
        assert result.exit_code == 0, getattr(result, "output", "")
        raw = getattr(result, "stdout_bytes", None) or result.output.encode()
        with tarfile.open(fileobj=io.BytesIO(raw), mode="r|") as tf:
            names = tf.getnames()
        # arcname carries the "-workdir" suffix.
        assert any(n.startswith(f"{jobid}-workdir") for n in names)
        assert any(n.endswith("scratch.bin") for n in names)

    def test_unknown_jobid_exits_nonzero(self, cli_state: Path) -> None:
        result = CliRunner().invoke(main, ["tar-workdir", "nopejobid0001"])
        assert result.exit_code != 0

    def test_tar_workdir_hidden_from_help(self) -> None:
        result = CliRunner().invoke(main, ["--help"])
        assert "tar-workdir" not in result.output


class TestCleanupCLI:
    def _materialize_terminal(
        self, cli_state: Path, jobid: str, *, days_ago: float = 40.0
    ) -> None:
        """Submit a job, then forcibly transition its spec to COMPLETED with
        a finished_at in the past so the age filter sees it as old."""
        from datetime import datetime, timedelta

        from vq.spec import JobSpec, JobState

        f = cli_state / f"{jobid}.py"
        f.write_text("pass")
        actual_id = CliRunner().invoke(
            main, ["submit", "localhost", str(f)]
        ).output.strip()
        spec_path = paths.queue_dir() / f"{actual_id}.json"
        spec = JobSpec.read(spec_path)
        spec.state = JobState.COMPLETED
        spec.finished_at = (
            datetime.now(UTC) - timedelta(days=days_ago)
        ).isoformat()
        spec.exit_code = 0
        spec.write(spec_path)
        # Make sure the workspace exists so size accounting has something
        # to look at.
        Path(spec.cwd).mkdir(parents=True, exist_ok=True)
        return actual_id

    def test_cleanup_no_action_lists_terminal_jobs(self, cli_state: Path) -> None:
        jobid = self._materialize_terminal(cli_state, "j1")
        result = CliRunner().invoke(main, ["cleanup", "localhost"])
        assert result.exit_code == 0, result.output
        assert jobid in result.output
        assert "completed" in result.output

    def test_cleanup_archive_dry_run_does_not_archive(
        self, cli_state: Path
    ) -> None:
        from vq.spec import JobSpec
        jobid = self._materialize_terminal(cli_state, "j1")
        result = CliRunner().invoke(
            main, ["cleanup", "localhost", "--archive", "--older-than", "30d"]
        )
        assert result.exit_code == 0, result.output
        assert "would archive" in result.output
        # No actual archival should have happened.
        spec = JobSpec.read(paths.queue_dir() / f"{jobid}.json")
        assert not spec.is_archived

    def test_cleanup_archive_execute_creates_tarball(
        self, cli_state: Path
    ) -> None:
        from vq.spec import JobSpec
        jobid = self._materialize_terminal(cli_state, "j1")
        result = CliRunner().invoke(
            main,
            ["cleanup", "localhost", "--archive", "--older-than", "30d", "-x"],
        )
        assert result.exit_code == 0, result.output
        spec = JobSpec.read(paths.queue_dir() / f"{jobid}.json")
        assert spec.is_archived
        assert Path(spec.archive_path).is_file()  # type: ignore[arg-type]
        assert not Path(spec.cwd).exists()

    def test_cleanup_delete_execute_removes_spec(self, cli_state: Path) -> None:
        jobid = self._materialize_terminal(cli_state, "j1")
        result = CliRunner().invoke(
            main,
            ["cleanup", "localhost", "--delete", "--older-than", "30d", "-x"],
        )
        assert result.exit_code == 0, result.output
        assert not (paths.queue_dir() / f"{jobid}.json").exists()

    def test_cleanup_archive_without_older_than_errors(
        self, cli_state: Path
    ) -> None:
        result = CliRunner().invoke(
            main, ["cleanup", "localhost", "--archive"]
        )
        assert result.exit_code != 0
        assert "older-than" in result.output.lower()

    def test_cleanup_mode_mutex(self, cli_state: Path) -> None:
        result = CliRunner().invoke(
            main,
            ["cleanup", "localhost", "--archive", "--delete", "--older-than", "30d"],
        )
        assert result.exit_code != 0
        assert "mutually exclusive" in result.output

    def test_cleanup_younger_than_threshold_excluded(
        self, cli_state: Path
    ) -> None:
        from vq.spec import JobSpec
        # Recent job (5 days ago); --older-than 30d should leave it alone.
        recent = self._materialize_terminal(cli_state, "j_recent", days_ago=5)
        old = self._materialize_terminal(cli_state, "j_old", days_ago=40)
        result = CliRunner().invoke(
            main,
            ["cleanup", "localhost", "--archive", "--older-than", "30d", "-x"],
        )
        assert result.exit_code == 0, result.output
        assert JobSpec.read(paths.queue_dir() / f"{recent}.json").is_archived is False
        assert JobSpec.read(paths.queue_dir() / f"{old}.json").is_archived is True

    def test_cleanup_restore_round_trip(self, cli_state: Path) -> None:
        from vq.spec import JobSpec
        jobid = self._materialize_terminal(cli_state, "j1")
        # Put a recognisable file in the workspace so we can verify restore.
        ws = paths.workspace_dir(jobid)
        (ws / "marker.txt").write_text("hello\n")
        # Archive.
        CliRunner().invoke(
            main,
            ["cleanup", "localhost", "--archive", "--older-than", "30d", "-x"],
        )
        # Restore.
        result = CliRunner().invoke(
            main, ["cleanup", "localhost", "--restore", jobid, "-x"]
        )
        assert result.exit_code == 0, result.output
        assert (ws / "marker.txt").read_text() == "hello\n"
        assert not JobSpec.read(paths.queue_dir() / f"{jobid}.json").is_archived


# ----------------------------------------------------------------------
# v0.5.36: --all cross-host aggregation on queue / programs / admin status
# ----------------------------------------------------------------------


class TestAllHostsAggregation:
    """v0.5.36: `vq queue --all`, `vq programs --all`, and
    `vq admin status --all` iterate every configured host and stack the
    per-host outputs under `==== <host> ====` banners. One bad host
    must not break the rest."""

    @pytest.fixture
    def two_host_cfg(self, cli_state: Path) -> Path:
        """A config with two non-local hosts (so both go via the SSH
        delegation path that we mock). Using clearly-not-local
        hostnames so is_local_host() returns False for both."""
        (cli_state / "cfg" / "config.toml").write_text(
            'default_host = "fake-alpha"\n'
            '\n'
            '[hosts.fake-alpha]\n'
            'ssh = "fake-alpha"\n'
            '\n'
            '[hosts.fake-beta]\n'
            'ssh = "fake-beta"\n'
        )
        return cli_state

    def _patch_per_host_responses(
        self,
        monkeypatch: pytest.MonkeyPatch,
        responses: dict[str, str],
    ) -> list[list[str]]:
        """Stub subprocess.run so each ssh dispatch returns the response
        keyed by the SSH host alias. Lets one host return real text while
        another returns an error simulated by a non-zero rc."""
        captured: list[list[str]] = []

        def fake_run(cmd: list[str], **kw):
            captured.append(cmd)
            # cmd is ["ssh", HOST, joined_remote_cmd] post-v0.5.32
            # v0.6.17: ssh argv now has ConnectTimeout opts between
            # `ssh` and the host; host moved from cmd[1] to cmd[-2].
            host = cmd[-2]
            text = responses.get(host, "")
            return subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout=text, stderr=""
            )

        from vq import transport as transport_module
        monkeypatch.setattr(transport_module.subprocess, "run", fake_run)
        return captured

    # ---- vq queue --all -----------------------------------------------

    def test_queue_all_emits_per_host_banners(
        self, two_host_cfg: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._patch_per_host_responses(
            monkeypatch,
            {
                "fake-alpha": "(no jobs)\n",
                "fake-beta": "ID  STATE\nabc12  running\n",
            },
        )
        result = CliRunner().invoke(main, ["queue", "--all"])
        assert result.exit_code == 0, result.output
        # Both host banners present, sorted alphabetically
        alpha_pos = result.output.find("==== fake-alpha ====")
        beta_pos = result.output.find("==== fake-beta ====")
        assert alpha_pos >= 0 and beta_pos >= 0
        assert alpha_pos < beta_pos, "hosts must be sorted alphabetically"
        # Per-host body content reaches the rendered output
        assert "(no jobs)" in result.output
        assert "abc12" in result.output

    def test_queue_all_rejects_positional_host(self, two_host_cfg: Path) -> None:
        """--all + HOST is contradictory and must error out clearly."""
        result = CliRunner().invoke(main, ["queue", "--all", "fake-alpha"])
        assert result.exit_code != 0
        assert "mutually exclusive" in result.output.lower()

    def test_queue_all_one_failing_host_doesnt_break_others(
        self, two_host_cfg: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Critical UX invariant: a single ssh failure on one host must
        render as an inline error, not abort the whole --all. The other
        host's output still shows."""
        captured: list[list[str]] = []

        def fake_run(cmd: list[str], **kw):
            captured.append(cmd)
            # v0.6.17: ssh argv now has ConnectTimeout opts between
            # `ssh` and the host; host moved from cmd[1] to cmd[-2].
            host = cmd[-2]
            if host == "fake-beta":
                # Simulate ssh failure for fake-beta
                return subprocess.CompletedProcess(
                    args=cmd, returncode=255,
                    stdout="", stderr="ssh: Connection timed out\n",
                )
            return subprocess.CompletedProcess(
                args=cmd, returncode=0,
                stdout="(no jobs)\n", stderr="",
            )

        from vq import transport as transport_module
        monkeypatch.setattr(transport_module.subprocess, "run", fake_run)

        result = CliRunner().invoke(main, ["queue", "--all"])
        assert result.exit_code == 0, result.output
        # fake-alpha succeeded
        assert "(no jobs)" in result.output
        # fake-beta failed — error rendered inline
        assert "error querying fake-beta" in result.output.lower()
        # Both hosts were attempted
        assert len(captured) == 2

    def test_queue_all_forwards_state_filter_to_each_host(
        self, two_host_cfg: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """`--all -s failed` must forward the filter to every per-host
        delegate so each one filters locally (no wasted bandwidth
        streaming the full listing only to drop most)."""
        captured = self._patch_per_host_responses(
            monkeypatch,
            {"fake-alpha": "(no jobs)\n", "fake-beta": "(no jobs)\n"},
        )
        result = CliRunner().invoke(
            main, ["queue", "--all", "-s", "failed"]
        )
        assert result.exit_code == 0, result.output
        # Each delegated call must include -s failed
        for cmd in captured:
            assert "-s" in shlex.split(cmd[-1])
            assert "failed" in shlex.split(cmd[-1])

    def test_queue_all_forwards_active_as_active(
        self, two_host_cfg: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured = self._patch_per_host_responses(
            monkeypatch,
            {"fake-alpha": "(no jobs)\n", "fake-beta": "(no jobs)\n"},
        )

        result = CliRunner().invoke(main, ["queue", "--all", "--active"])

        assert result.exit_code == 0, result.output
        for cmd in captured:
            remote_argv = shlex.split(cmd[-1])
            assert "--active" in remote_argv
            assert "-s" not in remote_argv

    def test_queue_all_json_emits_host_keyed_object(
        self, two_host_cfg: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """`vq queue --all --json` must be parseable fleet JSON, not
        banner-stacked text containing per-host JSON blocks."""
        captured = self._patch_per_host_responses(
            monkeypatch,
            {
                "fake-alpha": json.dumps(
                    [
                        {
                            "id": "alpha-job",
                            "command": ["true"],
                            "cwd": "/tmp/alpha-job",
                            "cpus": 1,
                            "state": "running",
                        }
                    ]
                )
                + "\n",
                "fake-beta": "[]\n",
            },
        )

        result = CliRunner().invoke(main, ["queue", "--all", "--json"])

        assert result.exit_code == 0, result.output
        payload = json.loads(result.stdout)
        assert payload == {
            "fake-alpha": [
                {
                    "id": "alpha-job",
                    "command": ["true"],
                    "cwd": "/tmp/alpha-job",
                    "cpus": 1,
                    "state": "running",
                    "effective_state": "running",
                    "scheduler_running_confirmed": None,
                    "queue_handle": {
                        "job_id": "alpha-job",
                        "host": "fake-alpha",
                        "submitted_at": None,
                    },
                }
            ],
            "fake-beta": [],
        }
        assert "====" not in result.output
        for cmd in captured:
            assert "--json" in shlex.split(cmd[-1])

    def test_queue_all_empty_config_explains(self, cli_state: Path) -> None:
        """Empty config (no [hosts.X] blocks) returns an explanation
        rather than silent empty output."""
        # cli_state has no config.toml
        result = CliRunner().invoke(main, ["queue", "--all"])
        assert result.exit_code == 0
        assert "no hosts configured" in result.output

    # ---- vq programs --all --------------------------------------------

    def test_programs_all_emits_per_host_banners(
        self, two_host_cfg: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._patch_per_host_responses(
            monkeypatch,
            {
                "fake-alpha":
                    "NAME    KIND    STATUS\ncrystal binary  OK\n",
                "fake-beta":
                    "NAME  KIND    STATUS\norca  binary  OK\n",
            },
        )
        result = CliRunner().invoke(main, ["programs", "--all"])
        assert result.exit_code == 0, result.output
        assert "==== fake-alpha ====" in result.output
        assert "==== fake-beta ====" in result.output
        assert "crystal" in result.output
        assert "orca" in result.output

    def test_programs_all_rejects_positional_host(self, two_host_cfg: Path) -> None:
        result = CliRunner().invoke(main, ["programs", "--all", "fake-alpha"])
        assert result.exit_code != 0
        assert "mutually exclusive" in result.output.lower()

    def test_programs_all_forwards_json_flag(
        self, two_host_cfg: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """--all --json should forward --json to each delegated call so
        the aggregate is a single host-keyed JSON object."""
        captured = self._patch_per_host_responses(
            monkeypatch,
            {"fake-alpha": "[]\n", "fake-beta": "[]\n"},
        )
        result = CliRunner().invoke(main, ["programs", "--all", "--json"])
        assert result.exit_code == 0, result.output
        assert json.loads(result.stdout) == {
            "fake-alpha": [],
            "fake-beta": [],
        }
        for cmd in captured:
            assert "--json" in shlex.split(cmd[-1])

    def test_programs_scheduler_host_renders_daemonless_notice(
        self, cli_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (cli_state / "cfg" / "config.toml").write_text(
            "[hosts.localhost]\n"
            'ssh = "localhost"\n'
            "\n"
            "[hosts.host_f]\n"
            'ssh = "host_f-login"\n'
            'scheduler = "pbs"\n'
            'scheduler_dialect = "torque"\n'
            'submit_extra = ["-q", "compute"]\n'
            'scratch_root = "/home/USER"\n'
            'scheduler_driver = "localhost"\n'
            'scheduler_max_wall_time_seconds = 28800\n'
        )

        def fail_delegate(*args, **kwargs):
            raise AssertionError("scheduler host must not be probed as remote vq")

        monkeypatch.setattr("vq.cli._delegate_to_remote", fail_delegate)

        text = CliRunner().invoke(main, ["programs", "host_f"])
        assert text.exit_code == 0, text.output
        assert "daemonless scheduler host" in text.output
        assert "vq scheduler-probe host_f" in text.output

        as_json = CliRunner().invoke(main, ["programs", "host_f", "--json"])
        assert as_json.exit_code == 0, as_json.output
        payload = json.loads(as_json.output)
        assert payload["scheduler_host"] is True
        assert payload["scheduler"] == "pbs"
        assert payload["driver"] == "localhost"
        assert payload["scheduler_lane"] == {
            "partition": "compute",
            "max_wall_time_seconds": 28_800,
            "max_cpus": None,
            "source": "host-config",
        }

    def test_programs_scheduler_host_with_remote_vq_delegates(
        self, cli_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (cli_state / "cfg" / "config.toml").write_text(
            "[hosts.localhost]\n"
            'ssh = "localhost"\n'
            "\n"
            "[hosts.host_f]\n"
            'ssh = "host_f-login"\n'
            'remote_vq = "/home/USER/vibe-queue/.venv/bin/vq"\n'
            'scheduler = "pbs"\n'
            'scheduler_dialect = "torque"\n'
            'submit_extra = ["-q", "compute"]\n'
            'scratch_root = "/home/USER"\n'
            'scheduler_driver = "localhost"\n'
            'scheduler_max_wall_time_seconds = 28800\n'
        )
        captured = _patch_remote_vq_stdout(
            monkeypatch,
            stdout='[{"name": "vibeqc-dev", "status": "OK"}]\n',
        )

        result = CliRunner().invoke(main, ["programs", "host_f", "--json"])

        assert result.exit_code == 0, result.output
        assert json.loads(result.stdout) == [
            {
                "name": "vibeqc-dev",
                "status": "OK",
                "scheduler_lane": {
                    "partition": "compute",
                    "max_wall_time_seconds": 28_800,
                    "max_cpus": None,
                    "source": "host-config",
                },
            }
        ]
        assert len(captured) == 1
        assert captured[0][-2] == "host_f-login"
        assert shlex.split(captured[0][-1]) == [
            "/home/USER/vibe-queue/.venv/bin/vq",
            "programs",
            "localhost",
            "--json",
        ]

    # ---- vq admin status --all ----------------------------------------

    def test_admin_status_all_emits_per_host_banners(
        self, two_host_cfg: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._patch_per_host_responses(
            monkeypatch,
            {
                "fake-alpha":
                    "NAME    BRANCH SHA   DESCRIBE\nvibeqc-dev main abc12 v0.7.5\n",
                "fake-beta":
                    "no venv programs registered.\n",
            },
        )
        result = CliRunner().invoke(main, ["admin", "status", "--all"])
        assert result.exit_code == 0, result.output
        assert "==== fake-alpha ====" in result.output
        assert "==== fake-beta ====" in result.output
        assert "vibeqc-dev" in result.output
        assert "no venv programs" in result.output

    def test_admin_status_scheduler_host_renders_daemonless_notice(
        self, cli_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (cli_state / "cfg" / "config.toml").write_text(
            "[hosts.localhost]\n"
            'ssh = "localhost"\n'
            "\n"
            "[hosts.host_f]\n"
            'ssh = "host_f-login"\n'
            'scheduler = "pbs"\n'
            'scheduler_dialect = "torque"\n'
            'scratch_root = "/home/USER"\n'
            'scheduler_driver = "localhost"\n'
        )

        def fail_delegate(*args, **kwargs):
            raise AssertionError("scheduler host must not be probed as remote vq")

        monkeypatch.setattr("vq.cli._delegate_to_remote", fail_delegate)

        text = CliRunner().invoke(main, ["admin", "status", "host_f"])
        assert text.exit_code == 0, text.output
        assert "daemonless scheduler host" in text.output
        assert "vq admin update host_f" in text.output

        as_json = CliRunner().invoke(main, ["admin", "status", "host_f", "--json"])
        assert as_json.exit_code == 0, as_json.output
        payload = json.loads(as_json.output)
        assert payload["scheduler_host"] is True
        assert payload["scheduler_dialect"] == "torque"
        assert payload["driver"] == "localhost"

    def test_admin_status_all_rejects_positional_host(
        self, two_host_cfg: Path
    ) -> None:
        result = CliRunner().invoke(
            main, ["admin", "status", "--all", "fake-alpha"]
        )
        assert result.exit_code != 0
        assert "mutually exclusive" in result.output.lower()

    # ---- help text ----------------------------------------------------

    def test_help_mentions_all(self) -> None:
        """The --all flag should be discoverable from each command's --help."""
        for verb_args in (
            ["queue", "--help"],
            ["programs", "--help"],
            ["admin", "status", "--help"],
        ):
            result = CliRunner().invoke(main, verb_args)
            assert result.exit_code == 0
            assert "--all" in result.output, (
                f"--all missing from `vq {' '.join(verb_args[:-1])}` help"
            )


# ----------------------------------------------------------------------
# v0.5.37: vq admin update --all-hosts (sequential per-host write)
# ----------------------------------------------------------------------


def _detached_observation_receipt(remote_cmd: str, payload: str) -> str:
    """A terminal observation answering one `admin observe-update` over ssh.

    A delegated venv update launches the remote work detached and then follows
    it, so an ssh fake that only answers the launch leaves the driver polling a
    run that never finishes. This gives the poll the receipt the real host
    would publish, carrying ``payload`` as the update's stdout.
    """
    parts = shlex.split(remote_cmd)
    run_id = parts[parts.index("observe-update") + 1]
    return json.dumps(
        {
            "schema": admin_detached.DETACHED_OBSERVATION_SCHEMA,
            "run_id": run_id,
            "state": admin_detached.STATE_COMPLETED,
            "detail": "stub completed",
            "target": "vibeqc-dev",
            "pid": 4242,
            "transcript": None,
            "transcript_offset": 0,
            "transcript_next_offset": 0,
            "transcript_size": 0,
            "transcript_base64": "",
            "outcome": "ok",
            "exit_code": 0,
            "payload": payload,
            "error": None,
        }
    )


def _detached_aware_ssh(captured, respond):
    """Wrap a subprocess-level ssh fake so it speaks the detached protocol.

    ``captured`` collects only the mutating launches -- the thing these tests
    are about -- while observations are answered from the launch's own stdout.
    """

    def fake_run(cmd: list[str], **_kw):
        proc = respond(cmd)
        if "admin observe-update" in cmd[-1]:
            return subprocess.CompletedProcess(
                args=cmd,
                returncode=0,
                stdout=_detached_observation_receipt(cmd[-1], proc.stdout),
                stderr="",
            )
        captured.append(cmd)
        return proc

    return fake_run


class TestAdminUpdateAllHosts:
    """v0.5.37: ``vq admin update --all-hosts`` runs the same env update
    on every host in config. Sequential. Failures don't abort the batch
    but DO non-zero the exit code (writes need failure visibility, unlike
    the v0.5.36 read-only ``--all``)."""

    @pytest.fixture(autouse=True)
    def _fast_detached_polling(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Keep these fan-out tests off the detached poll loop's real clock."""
        monkeypatch.setattr(cli.time, "sleep", lambda _seconds: None)
        monkeypatch.setattr(cli, "_DETACHED_POLL_INTERVAL_SECONDS", 0.0)
        monkeypatch.setattr(cli, "_DETACHED_UNCONFIRMED_GRACE_SECONDS", 0.0)
        monkeypatch.setattr(cli, "_DETACHED_OBSERVATION_GRACE_SECONDS", 0.0)

    @pytest.fixture
    def two_host_cfg(self, cli_state: Path) -> Path:
        (cli_state / "cfg" / "config.toml").write_text(
            'default_host = "fake-alpha"\n'
            '\n'
            '[hosts.fake-alpha]\n'
            'ssh = "fake-alpha"\n'
            '\n'
            '[hosts.fake-beta]\n'
            'ssh = "fake-beta"\n'
        )
        return cli_state

    def test_one_env_all_hosts_delegates_to_each(
        self, two_host_cfg: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """`vq admin update vibeqc-dev --all-hosts` issues one ssh per
        configured host, each running the same single-env update."""
        captured: list[list[str]] = []

        def respond(cmd: list[str]):
            return subprocess.CompletedProcess(
                args=cmd, returncode=0,
                stdout=f"updated vibeqc-dev OK on {cmd[-2]}\n", stderr="",
            )

        from vq import transport as transport_module
        monkeypatch.setattr(
            transport_module.subprocess, "run",
            _detached_aware_ssh(captured, respond),
        )

        result = CliRunner().invoke(
            main, ["admin", "update", "vibeqc-dev", "--all-hosts"]
        )
        assert result.exit_code == 0, result.output
        # One delegation per host
        assert len(captured) == 2
        hosts_targeted = sorted(cmd[-2] for cmd in captured)
        assert hosts_targeted == ["fake-alpha", "fake-beta"]
        # Each delegated argv includes "vibeqc-dev" + "localhost"
        for cmd in captured:
            parts = shlex.split(cmd[-1])
            assert "vibeqc-dev" in parts
            assert "localhost" in parts
        # Output stacked with per-host banners
        assert "==== fake-alpha ====" in result.output
        assert "==== fake-beta ====" in result.output

    def test_all_hosts_uses_each_remote_admin_token_file(
        self, cli_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Each multi-user host reads its own token; bearer bytes never
        traverse the driver or the SSH stdin pipe."""
        (cli_state / "cfg" / "config.toml").write_text(
            'default_host = "fake-alpha"\n'
            "\n"
            "[hosts.fake-alpha]\n"
            'ssh = "fake-alpha"\n'
            'admin_token_file = "/etc/vq/alpha-token"\n'
            "\n"
            "[hosts.fake-beta]\n"
            'ssh = "fake-beta"\n'
            'admin_token_file = "/etc/vq/beta-token"\n'
        )
        captured: list[dict[str, object]] = []

        def fake_run_remote_vq(host_cfg, *vq_args, stdin_data=None, **kwargs):
            if tuple(vq_args[:2]) == ("admin", "observe-update"):
                return subprocess.CompletedProcess(
                    args=["ssh"],
                    returncode=0,
                    stdout=_detached_observation_receipt(
                        shlex.join(vq_args), "ok\n"
                    ),
                    stderr="",
                )
            captured.append(
                {
                    "host": host_cfg.ssh,
                    "args": list(vq_args),
                    "stdin": stdin_data,
                }
            )
            return subprocess.CompletedProcess(
                args=["ssh"], returncode=0, stdout="{}", stderr=""
            )

        # Captured at the transport, because a delegated venv update now
        # launches detached rather than going through _delegate_to_remote.
        # The property under test is unchanged: each host's own token file is
        # named on its argv, and no bearer bytes ride the stdin pipe.
        monkeypatch.setattr(transport, "run_remote_vq", fake_run_remote_vq)

        result = CliRunner().invoke(
            main, ["admin", "update", "vibeqc-dev", "--all-hosts"]
        )

        assert result.exit_code == 0, result.output
        by_host = {str(call["host"]): call for call in captured}
        assert set(by_host) == {"fake-alpha", "fake-beta"}
        for host, path in (
            ("fake-alpha", "/etc/vq/alpha-token"),
            ("fake-beta", "/etc/vq/beta-token"),
        ):
            args = by_host[host]["args"]
            assert isinstance(args, list)
            assert args[args.index("--token-file") + 1] == path
            assert "--token-stdin" not in args
            assert by_host[host]["stdin"] is None

    def test_single_remote_host_uses_remote_admin_token_file(
        self, cli_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (cli_state / "cfg" / "config.toml").write_text(
            'default_host = "fake-alpha"\n'
            "\n"
            "[hosts.fake-alpha]\n"
            'ssh = "fake-alpha"\n'
            'admin_token_file = "/etc/vq/web-token"\n'
        )
        captured: dict[str, object] = {}

        def fake_run_remote_vq(host_cfg, *vq_args, stdin_data=None, **kwargs):
            if tuple(vq_args[:2]) == ("admin", "observe-update"):
                return subprocess.CompletedProcess(
                    args=["ssh"],
                    returncode=0,
                    stdout=_detached_observation_receipt(
                        shlex.join(vq_args), "ok\n"
                    ),
                    stderr="",
                )
            captured.update(
                host=host_cfg.ssh,
                args=list(vq_args),
                stdin=stdin_data,
            )
            return subprocess.CompletedProcess(
                args=["ssh"], returncode=0, stdout="{}", stderr=""
            )

        # See the sibling test: the launch is what carries the credential now.
        monkeypatch.setattr(transport, "run_remote_vq", fake_run_remote_vq)

        result = CliRunner().invoke(
            main, ["admin", "update", "vibeqc-dev", "fake-alpha"]
        )

        assert result.exit_code == 0, result.output
        assert captured["host"] == "fake-alpha"
        assert "--token-file" in captured["args"]
        assert "/etc/vq/web-token" in captured["args"]
        assert captured["stdin"] is None

    def test_all_envs_all_hosts_combines_both_dimensions(
        self, two_host_cfg: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """`vq admin update --all --all-hosts` is the full-fleet refresh.
        Each remote call passes --all and localhost; no positional ENV."""
        captured: list[list[str]] = []

        def respond(cmd: list[str]):
            return subprocess.CompletedProcess(
                args=cmd, returncode=0,
                stdout=f"all envs OK on {cmd[-2]}\n", stderr="",
            )

        from vq import transport as transport_module
        monkeypatch.setattr(
            transport_module.subprocess, "run",
            _detached_aware_ssh(captured, respond),
        )

        result = CliRunner().invoke(
            main, ["admin", "update", "--all", "--all-hosts"]
        )
        assert result.exit_code == 0, result.output
        assert len(captured) == 2
        for cmd in captured:
            parts = shlex.split(cmd[-1])
            assert "--all" in parts
            assert "localhost" in parts
            # No ENV positional in --all mode
            assert "vibeqc-dev" not in parts

    def test_tag_forwards_to_each_host(
        self, two_host_cfg: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """`--all-hosts ENV --tag` verifies the same tag on every host."""
        captured: list[list[str]] = []

        def respond(cmd: list[str]):
            return subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout="ok\n", stderr="",
            )

        from vq import transport as transport_module
        monkeypatch.setattr(
            transport_module.subprocess, "run",
            _detached_aware_ssh(captured, respond),
        )

        result = CliRunner().invoke(main, [
            "admin", "update", "vibeqc-release", "--all-hosts",
            "--tag", "v0.8.0",
        ])
        assert result.exit_code == 0, result.output
        for cmd in captured:
            parts = shlex.split(cmd[-1])
            assert "--tag" in parts
            assert "v0.8.0" in parts

    def test_expected_sha_forwards_to_each_host(
        self, two_host_cfg: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """`--all-hosts ENV --expected-sha` pins the same env SHA remotely."""
        captured: list[list[str]] = []
        sha = "a" * 40

        def respond(cmd: list[str]):
            return subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout="ok\n", stderr="",
            )

        from vq import transport as transport_module

        monkeypatch.setattr(
            transport_module.subprocess, "run",
            _detached_aware_ssh(captured, respond),
        )

        result = CliRunner().invoke(main, [
            "admin", "update", "vibeqc-dev", "--all-hosts",
            "--expected-sha", sha,
        ])
        assert result.exit_code == 0, result.output
        for cmd in captured:
            parts = shlex.split(cmd[-1])
            assert "--expected-sha" in parts
            assert sha in parts

    def test_all_hosts_skips_daemonless_scheduler_host(
        self, cli_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Venv-update fan-out must not SSH to scheduler-only targets."""
        (cli_state / "cfg" / "config.toml").write_text(
            'default_host = "fake-alpha"\n'
            '\n'
            "[hosts.fake-alpha]\n"
            'ssh = "fake-alpha"\n'
            '\n'
            "[hosts.host_f]\n"
            'ssh = "host_f-login"\n'
            'scheduler = "pbs"\n'
            'scheduler_dialect = "torque"\n'
            'scratch_root = "/home/USER"\n'
            'scheduler_driver = "fake-alpha"\n'
            'scheduler_update_command = "/home/USER/update-vq.sh"\n'
        )
        captured: list[list[str]] = []

        def respond(cmd: list[str]):
            return subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout=f"updated OK on {cmd[-2]}\n", stderr=""
            )

        from vq import transport as transport_module

        monkeypatch.setattr(
            transport_module.subprocess, "run",
            _detached_aware_ssh(captured, respond),
        )

        result = CliRunner().invoke(
            main, ["admin", "update", "vibeqc-queue", "--all-hosts"]
        )

        assert result.exit_code == 0, result.output
        assert [cmd[-2] for cmd in captured] == ["fake-alpha"]
        assert "==== host_f ====" in result.output
        assert "skipped: daemonless scheduler host" in result.output
        assert "vq admin update host_f" in result.output

    def test_all_hosts_json_marks_scheduler_host_skipped(
        self, cli_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The JSON shape keeps scheduler skips machine-readable."""
        (cli_state / "cfg" / "config.toml").write_text(
            'default_host = "fake-alpha"\n'
            '\n'
            "[hosts.fake-alpha]\n"
            'ssh = "fake-alpha"\n'
            '\n'
            "[hosts.host_f]\n"
            'ssh = "host_f-login"\n'
            'scheduler = "pbs"\n'
            'scheduler_dialect = "torque"\n'
            'scratch_root = "/home/USER"\n'
            'scheduler_driver = "fake-alpha"\n'
            'scheduler_update_command = "/home/USER/update-vq.sh"\n'
        )
        captured: list[list[str]] = []

        def respond(cmd: list[str]):
            return subprocess.CompletedProcess(
                args=cmd,
                returncode=0,
                stdout='{"success": true, "host": "fake-alpha"}\n',
                stderr="",
            )

        from vq import transport as transport_module

        monkeypatch.setattr(
            transport_module.subprocess, "run",
            _detached_aware_ssh(captured, respond),
        )

        result = CliRunner().invoke(
            main,
            ["admin", "update", "vibeqc-queue", "--all-hosts", "--json"],
        )

        assert result.exit_code == 0, result.output
        assert [cmd[-2] for cmd in captured] == ["fake-alpha"]
        payload = json.loads(result.stdout)
        assert payload["fake-alpha"]["success"] is True
        assert payload["host_f"]["skipped"] is True
        assert payload["host_f"]["scheduler_host"] is True
        assert payload["host_f"]["next_command"] == "vq admin update host_f"

    def test_all_hosts_rejects_positional_host(self, two_host_cfg: Path) -> None:
        result = CliRunner().invoke(main, [
            "admin", "update", "vibeqc-dev", "fake-alpha", "--all-hosts",
        ])
        assert result.exit_code != 0
        assert "mutually exclusive" in result.output.lower()

    def test_all_all_hosts_rejects_positional(self, two_host_cfg: Path) -> None:
        """`--all --all-hosts` with any positional is contradictory."""
        result = CliRunner().invoke(main, [
            "admin", "update", "--all", "--all-hosts", "fake-alpha",
        ])
        assert result.exit_code != 0
        # Either of the two mutual-exclusivity messages is fine — they
        # both come from the same family of "you said too much" errors.
        assert (
            "mutually exclusive" in result.output.lower()
            or "no positional argument is allowed" in result.output.lower()
        )

    def test_one_host_failure_doesnt_abort_others_but_exits_nonzero(
        self, two_host_cfg: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Critical UX invariant for writes: a single host's failure must
        NOT prevent the other host from updating. The exit code is
        non-zero so scripts notice the partial failure, but the healthy
        host still got refreshed."""
        captured: list[list[str]] = []

        def respond(cmd: list[str]):
            # v0.6.17: ssh argv now has ConnectTimeout opts between
            # `ssh` and the host; host moved from cmd[1] to cmd[-2].
            host = cmd[-2]
            if host == "fake-beta":
                return subprocess.CompletedProcess(
                    args=cmd, returncode=1,
                    stdout="", stderr="git pull failed: divergent branches\n",
                )
            return subprocess.CompletedProcess(
                args=cmd, returncode=0,
                stdout=f"updated OK on {host}\n", stderr="",
            )

        from vq import transport as transport_module
        monkeypatch.setattr(
            transport_module.subprocess, "run",
            _detached_aware_ssh(captured, respond),
        )

        result = CliRunner().invoke(
            main, ["admin", "update", "vibeqc-dev", "--all-hosts"]
        )
        # Non-zero exit (some host failed)
        assert result.exit_code != 0
        # But fake-alpha still got updated
        assert "fake-alpha" in result.output
        assert "updated OK on fake-alpha" in result.output
        # Both delegations attempted
        assert len(captured) == 2
        # The failure summary lists fake-beta
        assert "fake-beta" in result.output

    def test_help_mentions_all_hosts(self) -> None:
        result = CliRunner().invoke(main, ["admin", "update", "--help"])
        assert result.exit_code == 0
        assert "--all-hosts" in result.output


@pytest.mark.parametrize("separator", [[], ["--"]])
def test_misplaced_idempotency_flag_is_not_a_payload_command(
    cli_state: Path, separator: list[str],
) -> None:
    source = cli_state / "payload"
    source.mkdir()
    result = CliRunner().invoke(main, [
        "submit", "localhost", "-d", str(source), *separator,
        "--idempotency-key", "wave-key", "true",
    ])
    # Before an explicit separator Click may consume the option legitimately.
    if not separator:
        assert result.exit_code == 0, result.output
        spec = JobSpec.read(paths.queue_dir() / f"{result.output.strip()}.json")
        assert spec.command == ["true"]
        assert spec.idempotency_key_hash
    else:
        assert result.exit_code != 0
        assert "command executable" in result.output
        assert "--idempotency-key" in result.output
        assert not list(paths.queue_dir().glob("*.json"))


def test_submit_receipt_reports_queue_acceptance_not_execution(cli_state: Path) -> None:
    source = cli_state / "payload.py"
    source.write_text("raise RuntimeError('not run at submit')\n")
    result = CliRunner().invoke(main, ["submit", "localhost", "--json", str(source)])
    assert result.exit_code == 0, result.output
    receipt = json.loads(result.stdout)
    assert receipt["acceptance_scope"] == "queue"
    assert receipt["execution_status"] == "not_observed"
    spec = JobSpec.read(paths.queue_dir() / f"{receipt['jobids'][0]}.json")
    assert spec.state == JobState.PENDING


@pytest.fixture
def acceptance_cli(cli_state):
    (cli_state / "cfg" / "config.toml").write_text(
        '[hosts.host_f]\nssh = "host_f"\nscheduler = "pbs"\n'
        'scheduler_dialect = "torque"\nscratch_root = "/scratch/USER"\n'
        'scheduler_driver = "localhost"\n'
    )
    payload = cli_state / "payload"
    payload.mkdir()
    return ["submit", "host_f", "-d", str(payload), "--wait-submitted", "--json"]


@pytest.mark.parametrize(
    "state,scheduler_id,code,expected_rc,outcome",
    [
        (JobState.PENDING, "321.host_f", None, 0, "accepted"),
        (JobState.FAILED, None, 255, 1, "failed"),
        (JobState.FAILED, "321.host_f", 0, 1, "failed"),
    ],
)
def test_submit_scheduler_acceptance_receipt(
    acceptance_cli,
    monkeypatch,
    state,
    scheduler_id,
    code,
    expected_rc,
    outcome,
):
    execute = submit_module._execute_submit_plan
    calls = []

    def with_daemon_result(*args, **kwargs):
        ids = execute(*args, **kwargs)
        calls.append(ids)
        for jid in ids:
            path = paths.queue_dir() / f"{jid}.json"
            spec = JobSpec.read(path)
            spec.state = state
            spec.scheduler_job_id = scheduler_id
            spec.exit_code = code
            if state == JobState.FAILED:
                spec.failure_reason = "failed to stage workspace: scp rejected after 3 attempts"
            spec.write(path)
        return ids

    monkeypatch.setattr(submit_module, "_execute_submit_plan", with_daemon_result)
    result = CliRunner().invoke(main, [*acceptance_cli, "--", "true"])
    assert result.exit_code == expected_rc, result.output
    receipt = json.loads(result.stdout)
    assert receipt["jobids"] == calls[0]
    assert len(calls) == 1
    assert receipt["scheduler_acceptance"][0]["outcome"] == outcome
    assert receipt["acceptance_scope"] == ("scheduler" if expected_rc == 0 else "queue")
    assert receipt["retry_safe"] is False
    assert all((paths.queue_dir() / f"{jid}.json").exists() for jid in calls[0])
    if expected_rc:
        assert "stage workspace" in result.stderr
        assert "do not automatically resubmit" in result.stderr


def test_submit_scheduler_acceptance_timeout_retains_identity(acceptance_cli):
    result = CliRunner().invoke(
        main, [*acceptance_cli, "--submission-timeout", "0.02", "--", "true"]
    )
    assert result.exit_code == 124, result.output
    receipt = json.loads(result.stdout)
    assert receipt["scheduler_acceptance"][0]["outcome"] == "timeout"
    assert receipt["execution_status"] == "acceptance_incomplete"
    spec = JobSpec.read(paths.queue_dir() / f"{receipt['jobids'][0]}.json")
    assert spec.state == JobState.PENDING


def test_submit_scheduler_acceptance_interrupt_retains_identity(acceptance_cli, monkeypatch):
    from vq import cli as cli_mod

    def interrupt(*args, **kwargs):
        raise KeyboardInterrupt

    monkeypatch.setattr(cli_mod, "wait_for_scheduler_acceptance", interrupt)
    result = CliRunner().invoke(main, [*acceptance_cli, "--", "true"])
    assert result.exit_code == 130, result.output
    receipt = json.loads(result.stdout)
    assert receipt["execution_status"] == "acceptance_interrupted"
    assert (paths.queue_dir() / f"{receipt['jobids'][0]}.json").exists()


@pytest.mark.parametrize("extra", [["--wait-submitted"], ["--submission-timeout", "nan"]])
def test_submit_acceptance_validation_precedes_queue_mutation(cli_state, extra):
    source = cli_state / "payload.py"
    source.write_text("pass\n")
    result = CliRunner().invoke(main, ["submit", "localhost", *extra, str(source)])
    assert result.exit_code != 0
    assert not list(paths.queue_dir().glob("*.json"))


def test_submit_enqueue_only_does_not_wait(acceptance_cli, monkeypatch):
    from vq import cli as cli_mod

    def unexpected_wait(*args, **kwargs):
        pytest.fail("enqueue-only must not wait")

    monkeypatch.setattr(cli_mod, "wait_for_scheduler_acceptance", unexpected_wait)
    result = CliRunner().invoke(main, [*acceptance_cli, "--enqueue-only", "--", "true"])
    assert result.exit_code == 0, result.output
    receipt = json.loads(result.stdout)
    assert receipt["acceptance_scope"] == "queue"
    assert "scheduler_acceptance" not in receipt


def test_submit_acceptance_plain_failure_stdout_contains_only_ids(acceptance_cli):
    args = [a for a in acceptance_cli if a != "--json"]
    result = CliRunner().invoke(main, [*args, "--submission-timeout", "0.02", "--", "true"])
    assert result.exit_code == 124, result.output
    jid = result.stdout.strip()
    assert len(jid) == 12 and int(jid, 16) >= 0
    assert (paths.queue_dir() / f"{jid}.json").exists()


def test_submit_acceptance_polls_remote_driver_not_cluster(acceptance_cli, cli_state, monkeypatch):
    from vq import cli as cli_mod
    from vq.wait import SchedulerAcceptance

    config_path = cli_state / "cfg" / "config.toml"
    config_path.write_text(
        config_path.read_text().replace(
            'scheduler_driver = "localhost"',
            'scheduler_driver = "driver"',
        )
        + '\n[hosts.driver]\nssh = "queue-driver"\n'
    )
    monkeypatch.setattr(submit_module, "_execute_submit_plan", lambda *a, **k: ["ab0000000001"])
    observed = []

    def wait(host, ids, **kwargs):
        observed.append((host, ids, kwargs))
        return [SchedulerAcceptance(ids[0], "accepted", "running", "123.host_f")]

    monkeypatch.setattr(cli_mod, "wait_for_scheduler_acceptance", wait)
    result = CliRunner().invoke(main, [*acceptance_cli, "--", "true"])
    assert result.exit_code == 0, result.output
    assert observed[0][0] == "driver"
    assert observed[0][2]["host_cfg"].ssh == "queue-driver"
    assert observed[0][2]["scheduler_target"] == "host_f"
    assert json.loads(result.stdout)["host"] == "host_f"


def test_submit_acceptance_array_keeps_mixed_outcomes(acceptance_cli, monkeypatch):
    from vq import cli as cli_mod
    from vq.wait import SchedulerAcceptance

    def wait(host, ids, **kwargs):
        assert len(ids) == 2
        return [
            SchedulerAcceptance(ids[0], "accepted", "running", "123.host_f"),
            SchedulerAcceptance(ids[1], "failed", "failed", detail="workspace staging failed"),
        ]

    monkeypatch.setattr(cli_mod, "wait_for_scheduler_acceptance", wait)
    result = CliRunner().invoke(main, [*acceptance_cli, "--array", "2", "--", "true"])
    assert result.exit_code == 1, result.output
    receipt = json.loads(result.stdout)
    assert len(receipt["jobids"]) == 2
    assert [x["outcome"] for x in receipt["scheduler_acceptance"]] == ["accepted", "failed"]
    assert receipt["cli_exit_code"] == 1
    assert len(list(paths.queue_dir().glob("*.json"))) == 2
