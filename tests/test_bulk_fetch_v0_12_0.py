"""Tests for v0.12.0 *Hollerith's Return* bulk fetch-back: vq.fetch.bulk_fetch
and the `vq fetch-all` CLI verb.

bulk_fetch is exercised directly with a fake fetch-callable (pure unit,
no disk), and the verb is exercised end-to-end on the local path via
CliRunner against an isolated state + config dir.
"""
from __future__ import annotations

import json
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from vq import config, fetch, ownership, paths, transport
from vq.cli import main
from vq.spec import JobSpec, JobState
from vq.wait import WaitResult


def _spec(jobid: str, st: JobState, name: str | None = None) -> JobSpec:
    """A minimal in-memory spec (no workspace on disk) for the bulk_fetch
    unit tests, where the fetch primitive is faked."""
    return JobSpec(
        id=jobid,
        command=["echo", "x"],
        cwd="/tmp/vq-nonexistent",
        cpus=1,
        submitter="test_user@test",
        state=st,
        job_name=name,
    )


class TestBulkFetch:
    """vq.fetch.bulk_fetch: the transport-agnostic filter+fetch loop."""

    def test_fetches_terminal_skips_active(self) -> None:
        specs = [
            _spec("aaaaaaaaaaaa", JobState.COMPLETED),
            _spec("bbbbbbbbbbbb", JobState.RUNNING),
            _spec("cccccccccccc", JobState.FAILED),
        ]
        calls: list[str] = []

        def fake(jid: str) -> Path:
            calls.append(jid)
            return Path(f"/out/{jid}")

        results = fetch.bulk_fetch(specs, states=None, fetch_one=fake)
        # The RUNNING job is dropped before any fetch, so only terminals
        # are pulled, in queue order.
        assert calls == ["aaaaaaaaaaaa", "cccccccccccc"]
        assert [r.jobid for r in results] == ["aaaaaaaaaaaa", "cccccccccccc"]
        assert all(r.outcome == "fetched" for r in results)

    def test_state_filter_selects_subset(self) -> None:
        specs = [
            _spec("aaaaaaaaaaaa", JobState.COMPLETED),
            _spec("cccccccccccc", JobState.FAILED),
        ]
        results = fetch.bulk_fetch(
            specs, states={"completed"}, fetch_one=lambda jid: Path(f"/out/{jid}")
        )
        assert [r.jobid for r in results] == ["aaaaaaaaaaaa"]

    def test_file_exists_becomes_skipped(self) -> None:
        def fake(jid: str) -> Path:
            raise FileExistsError("destination already exists")

        results = fetch.bulk_fetch(
            [_spec("aaaaaaaaaaaa", JobState.COMPLETED)], states=None, fetch_one=fake
        )
        assert results[0].outcome == "skipped"
        assert "already present" in results[0].detail

    def test_missing_workspace_isolated_as_error(self) -> None:
        def fake(jid: str) -> Path:
            if jid == "aaaaaaaaaaaa":
                raise FileNotFoundError("workspace not found")
            return Path(f"/out/{jid}")

        results = fetch.bulk_fetch(
            [
                _spec("aaaaaaaaaaaa", JobState.COMPLETED),
                _spec("bbbbbbbbbbbb", JobState.COMPLETED),
            ],
            states=None,
            fetch_one=fake,
        )
        # The first job errored, but the sweep continued to the second.
        assert results[0].outcome == "error"
        assert "workspace not found" in results[0].detail
        assert results[1].outcome == "fetched"

    def test_remote_error_isolated(self) -> None:
        def fake(jid: str) -> Path:
            raise transport.RemoteError("ssh exited 255")

        results = fetch.bulk_fetch(
            [_spec("aaaaaaaaaaaa", JobState.COMPLETED)], states=None, fetch_one=fake
        )
        assert results[0].outcome == "error"
        assert "ssh exited 255" in results[0].detail

    def test_manifest_validation_error_isolated(self) -> None:
        def fake(jid: str) -> Path:
            raise ValueError("fetch-manifest metadata is missing or unreadable")

        results = fetch.bulk_fetch(
            [_spec("aaaaaaaaaaaa", JobState.COMPLETED)], states=None, fetch_one=fake
        )

        assert results[0].outcome == "error"
        assert "fetch-manifest metadata" in results[0].detail

    def test_ownership_error_isolated_and_sweep_continues(self) -> None:
        def fake(jid: str) -> Path:
            if jid == "aaaaaaaaaaaa":
                raise ownership.OwnershipError("foreign job denied")
            return Path(f"/out/{jid}")

        results = fetch.bulk_fetch(
            [
                _spec("aaaaaaaaaaaa", JobState.COMPLETED),
                _spec("bbbbbbbbbbbb", JobState.COMPLETED),
            ],
            states=None,
            fetch_one=fake,
        )

        assert [(row.jobid, row.outcome) for row in results] == [
            ("aaaaaaaaaaaa", "error"),
            ("bbbbbbbbbbbb", "fetched"),
        ]
        assert results[0].detail == "foreign job denied"

    def test_job_name_carried_into_result(self) -> None:
        results = fetch.bulk_fetch(
            [_spec("aaaaaaaaaaaa", JobState.COMPLETED, name="scfrun")],
            states=None,
            fetch_one=lambda jid: Path(f"/out/{jid}"),
        )
        assert results[0].job_name == "scfrun"

    def test_empty_input_no_results(self) -> None:
        results = fetch.bulk_fetch([], states=None, fetch_one=lambda jid: Path("/x"))
        assert results == []

    def test_signal_exit_decoded_as_hint_when_no_tail(self) -> None:
        # A hard SIGKILL/OOM with no stderr to tail: the sweep should still
        # name the signal via the exit-code fallback (no failure_tail set).
        spec = _spec("dddddddddddd", JobState.FAILED)
        spec.exit_code = 137  # 128 + SIGKILL
        results = fetch.bulk_fetch(
            [spec], states=None, fetch_one=lambda jid: Path(f"/out/{jid}")
        )
        assert results[0].failure_hint is not None
        assert "SIGKILL" in results[0].failure_hint

    def test_failure_tail_preferred_over_signal_decode(self) -> None:
        # When stderr WAS captured, the real tail wins over the signal decode.
        spec = _spec("eeeeeeeeeeee", JobState.FAILED)
        spec.exit_code = 139  # SIGSEGV
        spec.failure_tail = "Traceback: boom\nmore detail"
        results = fetch.bulk_fetch(
            [spec], states=None, fetch_one=lambda jid: Path(f"/out/{jid}")
        )
        assert results[0].failure_hint == "Traceback: boom"

    def test_plain_nonzero_exit_gets_no_signal_hint(self) -> None:
        # A normal exit code 1 (not a signal) yields no fallback hint.
        spec = _spec("ffffffffffff", JobState.FAILED)
        spec.exit_code = 1
        results = fetch.bulk_fetch(
            [spec], states=None, fetch_one=lambda jid: Path(f"/out/{jid}")
        )
        assert results[0].failure_hint is None


# ----------------------------------------------------------------------
# `vq fetch-all` CLI verb, local path, end-to-end via CliRunner.
# ----------------------------------------------------------------------


@pytest.fixture
def cli_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Isolated state + config dir so `vq fetch-all localhost` resolves the
    local host without touching the operator's real ~/.config/vq."""
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    (tmp_path / "cfg" / "config.toml").write_text('default_host = "localhost"\n')
    return tmp_path


def _job(
    jobid: str,
    files: dict[str, str],
    *,
    st: JobState = JobState.PENDING,
    name: str | None = None,
    scheduler_target: str | None = None,
) -> None:
    """Write a spec + workspace into the live queue dir. Terminal states
    also get finished_at + exit_code so the spec is genuinely terminal."""
    queue = paths.queue_dir()
    jobs = paths.jobs_dir()
    queue.mkdir(parents=True, exist_ok=True)
    jobs.mkdir(parents=True, exist_ok=True)
    workspace = jobs / jobid
    workspace.mkdir()
    for fname, content in files.items():
        (workspace / fname).write_text(content)
    spec = JobSpec(
        id=jobid,
        command=["echo", "hi"],
        cwd=str(workspace),
        cpus=1,
        submitter="test_user@test",
        state=st,
        job_name=name,
        scheduler_target=scheduler_target,
    )
    if spec.is_terminal:
        spec.finished_at = "2026-05-02T10:00:00+00:00"
        spec.exit_code = 0 if st == JobState.COMPLETED else 1
    spec.write(queue / f"{jobid}.json")


def _write_remote_fetch_receipt(
    output_dir: Path,
    spec: JobSpec,
    *,
    source_host: str,
    diagnosis_state: str | None = None,
    diagnosis_submitted_at: str | None = None,
    stale: bool = False,
    source_kind: str = "workspace",
) -> Path:
    """Materialize one provenance-complete prior remote fetch."""
    dst = output_dir / spec.dest_dirname
    (dst / "_vq").mkdir(parents=True)
    (dst / fetch.TERMINAL_DIAGNOSIS_SIDECAR).write_text(
        json.dumps(
            {
                "schema": "vq.terminal-diagnosis.v1",
                "jobid": spec.id,
                "submitted_at": diagnosis_submitted_at or spec.submitted_at,
                "state": diagnosis_state or spec.state.value,
            }
        )
    )
    (dst / fetch.FETCH_MANIFEST_SIDECAR).write_text(
        json.dumps(
            {
                "schema": fetch.FETCH_MANIFEST_SCHEMA,
                "jobid": spec.id,
                "job_name": spec.job_name,
                "fetched_at": "2026-08-20T10:14:00+00:00",
                "refresh_attempted_at": "2026-08-20T10:14:00+00:00",
                "source_host": source_host,
                "source_kind": source_kind,
                "source_path": None,
                "transport": "ssh-stream",
                "stale": stale,
                "refresh_error": "old failure" if stale else None,
            }
        )
    )
    return dst


class TestFetchAllCli:
    def test_fetches_all_terminal_local(self, cli_state: Path) -> None:
        _job("aaaaaaaaaaaa", {"out.txt": "A"}, st=JobState.COMPLETED)
        _job("bbbbbbbbbbbb", {"out.txt": "B"}, st=JobState.COMPLETED)
        _job("cccccccccccc", {"out.txt": "C"})  # PENDING, must be skipped
        out = cli_state / "results"
        result = CliRunner().invoke(main, ["fetch-all", "localhost", "-o", str(out)])
        assert result.exit_code == 0, result.output
        assert (out / "aaaaaaaaaaaa" / "out.txt").read_text() == "A"
        assert (out / "bbbbbbbbbbbb" / "out.txt").read_text() == "B"
        assert not (out / "cccccccccccc").exists()
        assert "2 fetched" in result.output

    def test_manifestless_result_is_reported_as_error(
        self, cli_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _job("aaaaaaaaaaaa", {"out.txt": "A"}, st=JobState.COMPLETED)
        out = cli_state / "results"

        def fake_fetch_local(
            jid: str,
            output_dir: Path,
            *,
            multi_user: bool,
            idempotent: bool,
        ) -> Path:
            dst = output_dir / jid
            dst.mkdir(parents=True)
            return dst

        monkeypatch.setattr("vq.cli.fetch_local", fake_fetch_local)

        result = CliRunner().invoke(
            main, ["fetch-all", "localhost", "-o", str(out)]
        )

        assert result.exit_code != 0
        assert "ERROR   aaaaaaaaaaaa: fetch-manifest metadata" in result.output
        assert "localhost: 0 fetched, 1 failed" in result.output
        assert "fetched aaaaaaaaaaaa ->" not in result.output

    def test_idempotent_rerun_skips_present(self, cli_state: Path) -> None:
        _job("aaaaaaaaaaaa", {"out.txt": "A"}, st=JobState.COMPLETED)
        out = cli_state / "results"
        first = CliRunner().invoke(main, ["fetch-all", "localhost", "-o", str(out)])
        assert "1 fetched" in first.output, first.output
        second = CliRunner().invoke(main, ["fetch-all", "localhost", "-o", str(out)])
        assert second.exit_code == 0, second.output
        assert "0 fetched" in second.output
        assert "1 skipped" in second.output

    def test_state_filter_completed_only(self, cli_state: Path) -> None:
        _job("aaaaaaaaaaaa", {"out.txt": "A"}, st=JobState.COMPLETED)
        _job("bbbbbbbbbbbb", {"out.txt": "B"}, st=JobState.FAILED)
        out = cli_state / "results"
        result = CliRunner().invoke(
            main, ["fetch-all", "localhost", "-o", str(out), "-s", "completed"]
        )
        assert result.exit_code == 0, result.output
        assert (out / "aaaaaaaaaaaa").exists()
        assert not (out / "bbbbbbbbbbbb").exists()

    def test_named_job_in_output(self, cli_state: Path) -> None:
        _job("aaaaaaaaaaaa", {"out.txt": "A"}, st=JobState.COMPLETED, name="scfrun")
        out = cli_state / "results"
        result = CliRunner().invoke(main, ["fetch-all", "localhost", "-o", str(out)])
        assert result.exit_code == 0, result.output
        # Named jobs land under <name>-<jobid>/ and the line shows the name.
        assert "scfrun" in result.output
        assert (out / "scfrun-aaaaaaaaaaaa" / "out.txt").read_text() == "A"

    def test_invalid_state_rejected(self, cli_state: Path) -> None:
        result = CliRunner().invoke(main, ["fetch-all", "localhost", "-s", "running"])
        assert result.exit_code != 0
        assert "non-terminal or unknown" in result.output

    def test_no_jobs_reports_zero(self, cli_state: Path) -> None:
        out = cli_state / "results"
        result = CliRunner().invoke(main, ["fetch-all", "localhost", "-o", str(out)])
        assert result.exit_code == 0, result.output
        assert "0 fetched" in result.output

    def test_scheduler_host_fetch_all_filters_driver_specs(
        self, cli_state: Path
    ) -> None:
        (cli_state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
            "\n"
            "[hosts.localhost]\n"
            'ssh = "localhost"\n'
            "\n"
            "[hosts.host_f]\n"
            'ssh = "host_f.invalid"\n'
            'scheduler = "pbs"\n'
            'scheduler_dialect = "torque"\n'
            'scratch_root = "/home/USER"\n'
            'scheduler_driver = "localhost"\n'
        )
        _job(
            "twinterm0001",
            {"out.txt": "T"},
            st=JobState.COMPLETED,
            scheduler_target="host_f",
        )
        _job("localterm001", {"out.txt": "L"}, st=JobState.COMPLETED)

        out = cli_state / "results"
        result = CliRunner().invoke(main, ["fetch-all", "host_f", "-o", str(out)])

        assert result.exit_code == 0, result.output
        assert (out / "twinterm0001" / "out.txt").read_text() == "T"
        assert not (out / "localterm001").exists()
        assert "1 fetched" in result.output


class TestFetchAllRemoteMarkBack:
    @staticmethod
    def _configure_remote(cli_state: Path) -> None:
        (cli_state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
            "\n"
            "[hosts.host_b]\n"
            'ssh = "host_b.invalid"\n'
            'remote_vq = "vq"\n'
        )

    @staticmethod
    def _remote_listing(
        monkeypatch: pytest.MonkeyPatch,
        spec: JobSpec,
    ) -> None:
        monkeypatch.setattr(
            "vq.cli._delegate_to_remote",
            lambda *args, **kwargs: json.dumps([json.loads(spec.to_json())]),
        )

    def test_exact_terminal_receipt_retries_missing_remote_mark_then_skips(
        self,
        cli_state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        self._configure_remote(cli_state)
        spec = _spec("remoteack001", JobState.COMPLETED)
        out = cli_state / "results"
        dst = _write_remote_fetch_receipt(
            out,
            spec,
            source_host="host_b.invalid",
        )
        self._remote_listing(monkeypatch, spec)
        calls: list[tuple[tuple[str, ...], dict[str, object]]] = []

        def acknowledge(
            unused_host: config.HostConfig,
            *args: str,
            **kwargs: object,
        ) -> subprocess.CompletedProcess[str]:
            calls.append((args, kwargs))
            return subprocess.CompletedProcess(
                [], 0, stdout="2026-08-20T10:15:00+00:00\n", stderr=""
            )

        monkeypatch.setattr(transport, "run_remote_vq", acknowledge)
        monkeypatch.setattr(
            "vq.cli.fetch_remote",
            lambda *args, **kwargs: pytest.fail(
                "an exact prior receipt should be acknowledged then skipped"
            ),
        )

        result = CliRunner().invoke(
            main, ["fetch-all", "host_b", "-o", str(out)]
        )

        assert result.exit_code == 0, result.output
        assert "0 fetched" in result.output
        assert "1 skipped" in result.output
        assert calls == [
            (
                (
                    "mark-fetched",
                    spec.id,
                    "--submitted-at",
                    spec.submitted_at,
                    "--state",
                    "completed",
                ),
                {
                    "timeout": fetch.REMOTE_FETCH_ACK_TIMEOUT_SECONDS,
                    "retry_transient": 0,
                    "owned_process_group": True,
                    "max_stdout_bytes": fetch.REMOTE_FETCH_ACK_STDOUT_MAX_BYTES,
                    "max_stderr_bytes": fetch.REMOTE_FETCH_ACK_STDERR_MAX_BYTES,
                },
            )
        ]
        manifest = fetch.read_fetch_manifest(dst)
        assert manifest is not None
        assert manifest["stale"] is False

    @pytest.mark.parametrize(
        ("existing", "idempotent"),
        [
            ("live", True),
            ("stale", True),
            ("workdir", True),
            ("old-generation", True),
            ("malformed", True),
            ("bare", False),
            ("foreign", False),
        ],
    )
    def test_inexact_receipts_refresh_and_untrusted_collisions_do_not_backfill(
        self,
        cli_state: Path,
        monkeypatch: pytest.MonkeyPatch,
        existing: str,
        idempotent: bool,
    ) -> None:
        spec = _spec("remoteexisting", JobState.COMPLETED)
        out = cli_state / "results"
        if existing in {
            "live",
            "stale",
            "workdir",
            "old-generation",
            "malformed",
        }:
            dst = _write_remote_fetch_receipt(
                out,
                spec,
                source_host="host_b.invalid",
                diagnosis_state="running" if existing == "live" else None,
                stale=existing == "stale",
                source_kind="workdir" if existing == "workdir" else "workspace",
                diagnosis_submitted_at=(
                    "older-job-generation"
                    if existing == "old-generation"
                    else None
                ),
            )
            if existing == "malformed":
                manifest_path = dst / fetch.FETCH_MANIFEST_SIDECAR
                manifest = json.loads(manifest_path.read_text())
                manifest["refresh_error"] = "contradicts stale=false"
                manifest_path.write_text(json.dumps(manifest))
        else:
            dst = out / spec.dest_dirname
            dst.mkdir(parents=True)
        if existing == "foreign":
            (dst / "_vq").mkdir()
            (dst / fetch.TERMINAL_DIAGNOSIS_SIDECAR).write_text(
                json.dumps(
                    {
                        "schema": "vq.terminal-diagnosis.v1",
                        "jobid": "another-job",
                    }
                )
            )
        monkeypatch.setattr(
            transport,
            "run_remote_vq",
            lambda *args, **kwargs: pytest.fail(
                "an inexact or untrusted destination is not mark-back authority"
            ),
        )

        assert fetch.prepare_remote_bulk_fetch(
            config.HostConfig(ssh="host_b.invalid", remote_vq="vq"),
            spec,
            out,
        ) is idempotent


# ----------------------------------------------------------------------
# `vq submit --fetch-on-done`: imply --wait, fetch on completion.
# ----------------------------------------------------------------------


class TestSubmitFetchOnDone:
    def test_help_lists_fetch_on_done(self) -> None:
        result = CliRunner().invoke(main, ["submit", "--help"])
        assert result.exit_code == 0
        assert "--fetch-on-done" in result.output

    def test_fetch_on_done_fetches_after_implied_wait(self, cli_state: Path) -> None:
        # No in-process daemon, so mock the wait to return a terminal
        # verdict immediately and mock the fetch to record the call. This
        # exercises the wiring: --fetch-on-done implies --wait, and once
        # the wait is terminal the workspace fetch fires for the job.
        src = cli_state / "x.py"
        src.write_text("print('done')")
        fetched: list[str] = []

        def fake_fetch_local(jid: str, out: Path, *, multi_user: bool) -> Path:
            fetched.append(jid)
            return Path(out) / jid

        verdict = WaitResult(jobid="ignored", state=JobState.COMPLETED, exit_code=0)
        with (
            patch("vq.cli.wait_for_terminal", return_value=verdict),
            patch("vq.cli.fetch_local", side_effect=fake_fetch_local),
            patch(
                "vq.cli.require_fresh_fetch_manifest",
                return_value={"schema": fetch.FETCH_MANIFEST_SCHEMA},
            ) as require_manifest,
        ):
            result = CliRunner().invoke(
                main, ["submit", "localhost", str(src), "--fetch-on-done"]
            )
        assert len(fetched) == 1, result.output
        assert "fetched" in result.output
        require_manifest.assert_called_once_with(
            Path(fetched[0]),
            jobid=fetched[0],
            source_kind="workspace",
        )

    def test_fetch_on_done_reports_missing_manifest_as_failure(
        self, cli_state: Path
    ) -> None:
        src = cli_state / "x.py"
        src.write_text("print('done')")
        fetched: list[str] = []

        def fake_fetch_local(jid: str, out: Path, *, multi_user: bool) -> Path:
            fetched.append(jid)
            return Path(out) / jid

        verdict = WaitResult(jobid="ignored", state=JobState.COMPLETED, exit_code=0)
        with (
            patch("vq.cli.wait_for_terminal", return_value=verdict),
            patch("vq.cli.fetch_local", side_effect=fake_fetch_local),
        ):
            result = CliRunner().invoke(
                main, ["submit", "localhost", str(src), "--fetch-on-done"]
            )

        assert result.exit_code == 0, result.output
        assert len(fetched) == 1
        assert f"fetch of {fetched[0]} failed: fetch-manifest metadata" in result.output
        assert f"fetched {fetched[0]} ->" not in result.output

    def test_plain_submit_does_not_fetch(self, cli_state: Path) -> None:
        # Without --fetch-on-done, neither the implied wait nor the fetch
        # runs; submit just creates the spec and prints the jobid.
        src = cli_state / "x.py"
        src.write_text("print('done')")
        fetched: list[str] = []

        def fake_fetch_local(jid: str, out: Path, *, multi_user: bool) -> Path:
            fetched.append(jid)
            return Path(out) / jid

        with patch("vq.cli.fetch_local", side_effect=fake_fetch_local):
            result = CliRunner().invoke(main, ["submit", "localhost", str(src)])
        assert result.exit_code == 0, result.output
        assert fetched == []


# ----------------------------------------------------------------------
# v0.12.0 polish: crash tail in the summary + --all-hosts sweep.
# ----------------------------------------------------------------------


@pytest.fixture
def cli_state_hosts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Like cli_state but with a [hosts.localhost] block, so --all-hosts has
    a configured host to sweep (locally, no SSH)."""
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    (tmp_path / "cfg" / "config.toml").write_text(
        'default_host = "localhost"\n\n[hosts.localhost]\nssh = "localhost"\n'
    )
    return tmp_path


class TestFetchAllPolish:
    def test_failed_job_shows_crash_tail(self, cli_state: Path) -> None:
        # A FAILED job with a failure_tail: the fetched line flags it with
        # the state + the first line of the crash tail (the Hopper's Bug
        # crash-feedback field, surfaced in the bulk summary).
        _job("ffffffffffff", {"out.txt": "partial"}, st=JobState.FAILED)
        sp = paths.queue_dir() / "ffffffffffff.json"
        spec = JobSpec.read(sp)
        spec.failure_tail = "ValueError: basis set xyz not found"
        spec.write(sp)
        out = cli_state / "results"
        result = CliRunner().invoke(main, ["fetch-all", "localhost", "-o", str(out)])
        assert result.exit_code == 0, result.output
        assert "[FAILED" in result.output
        assert "ValueError" in result.output

    def test_completed_job_has_no_crash_tag(self, cli_state: Path) -> None:
        _job("aaaaaaaaaaaa", {"out.txt": "A"}, st=JobState.COMPLETED)
        out = cli_state / "results"
        result = CliRunner().invoke(main, ["fetch-all", "localhost", "-o", str(out)])
        assert result.exit_code == 0, result.output
        assert "[FAILED" not in result.output
        assert "[COMPLETED" not in result.output

    def test_all_hosts_mutually_exclusive_with_host(self, cli_state: Path) -> None:
        result = CliRunner().invoke(main, ["fetch-all", "localhost", "--all-hosts"])
        assert result.exit_code != 0
        assert "mutually exclusive" in result.output

    def test_all_hosts_no_hosts_configured(self, cli_state: Path) -> None:
        # cli_state sets default_host but writes no [hosts.*] block.
        result = CliRunner().invoke(main, ["fetch-all", "--all-hosts"])
        assert result.exit_code != 0
        assert "no hosts configured" in result.output

    def test_all_hosts_sweeps_configured_local(self, cli_state_hosts: Path) -> None:
        _job("aaaaaaaaaaaa", {"out.txt": "A"}, st=JobState.COMPLETED)
        out = cli_state_hosts / "results"
        result = CliRunner().invoke(main, ["fetch-all", "--all-hosts", "-o", str(out)])
        assert result.exit_code == 0, result.output
        assert (out / "aaaaaaaaaaaa" / "out.txt").read_text() == "A"
        assert "localhost:" in result.output
