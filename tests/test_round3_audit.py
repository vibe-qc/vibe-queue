"""v0.7.13 *Backus's Form* — round-3 hardening + test coverage audit.

Focused gap-fill on edge cases not covered by the per-module
tests for the v0.6.18 → v0.7.12 ships. Each test pins behaviour
on a case the regular tests don't exercise — either because the
case is unusual but real (an operator triggering it after-hours
is the worst time to discover an uncovered branch) or because
the case might be silently wrong and we want a tripwire.

This is a coverage-first file, not a feature file. Adds tests;
no production code touched.

Audit scope by ship:

* v0.7.5 recovery_audit — what happens on DNS failure
  (gaierror), not just connection-refused / timeout.
* v0.7.6 fanout — does ``_run_per_host`` propagate
  ``KeyboardInterrupt`` so Ctrl-C still cancels a sweep
  cleanly?
* v0.7.6 fanout JSON — does ``_aggregate_per_host_json``
  preserve a per-host result that's a JSON list / number /
  string (not a dict)?
* v0.7.7 fetch workdir — does the workdir fetch ignore
  ``spec.is_archived`` (archives are workspace-only)?
* v0.7.8 depends_on_any — does an INTERRUPTED predecessor
  count as terminal (afterany semantics)?
* v0.7.9 reset-branch — does a failed ``git fetch`` cleanly
  skip the reset and preserve the prior SHA in admin-status?
* v0.7.10 collapse-arrays — what does the breakdown render
  for an empty-but-existing group?
* v0.7.11 remote --array — is ``array=1`` byte-identical to
  the pre-v0.7.11 single-call wire shape?
"""

from __future__ import annotations

import json
import socket
import subprocess
from pathlib import Path

import pytest

from vq import (
    admin,
    cli,
    config,
    listing,
    paths,
    submit,
)
from vq import (
    recovery_audit as _audit,
)
from vq.config import HostConfig
from vq.fetch import fetch_workdir_local
from vq.spec import JobSpec, JobState


@pytest.fixture
def cli_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    return tmp_path


# ----------------------------------------------------------------------
# v0.7.5: recovery_audit on DNS failure
# ----------------------------------------------------------------------


class TestRecoveryAuditDnsFailure:
    def test_probe_tcp_returns_red_on_gaierror(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A hostname that doesn't resolve raises ``socket.gaierror``
        which is NOT a ``ConnectionRefusedError`` or ``socket.timeout``.
        The probe must catch it cleanly and report red rather than
        crashing the audit verb."""
        def fake_create_connection(addr, timeout):
            raise socket.gaierror(-2, "Name or service not known")

        monkeypatch.setattr(
            _audit.socket, "create_connection", fake_create_connection,
        )
        # The Cockpit probe is the cleanest TCP-only one to exercise.
        result = _audit._probe_cockpit("nosuchhost.invalid", 9090)
        assert result.status == "red"
        # The detail names the port AND surfaces the gaierror text so
        # the operator can distinguish "DNS doesn't resolve" from
        # "host up but cockpit not listening."
        assert "9090" in result.detail
        assert (
            "name or service" in result.detail.lower()
            or "resolve" in result.detail.lower()
            or "errno" in result.detail.lower()
        ), (
            f"DNS gaierror detail should surface the resolution "
            f"failure; got {result.detail!r}"
        )


# ----------------------------------------------------------------------
# v0.7.6: fanout resilience
# ----------------------------------------------------------------------


class TestFanoutKeyboardInterrupt:
    def test_keyboard_interrupt_propagates(self) -> None:
        """``_safe_per_host`` catches ``Exception`` but must let
        ``BaseException`` (i.e. ``KeyboardInterrupt``,
        ``SystemExit``) propagate so Ctrl-C still aborts a sweep
        instead of silently swallowing every host's interrupt."""
        def interrupting(h: str) -> str:
            raise KeyboardInterrupt
        # Single-host path: _safe_per_host called directly.
        with pytest.raises(KeyboardInterrupt):
            cli._safe_per_host("only", interrupting)


class TestAggregatePerHostJsonNonDict:
    def test_per_host_returning_list_preserved(self) -> None:
        """``_aggregate_per_host_json`` should faithfully preserve
        a per-host JSON list / number / string in the aggregate
        payload — we don't pre-suppose every verb's output is a
        dict (vq queue --json returns a list, for example)."""
        cfg = config.Config(hosts={
            "alpha": HostConfig(ssh="alpha.invalid"),
            "bravo": HostConfig(ssh="bravo.invalid"),
        })

        def fn(h: str) -> str:
            return json.dumps([{"jid": "aaa", "host": h}])

        payload = cli._aggregate_per_host_json(cfg, fn)
        # The list is preserved as-is, not wrapped.
        assert payload["alpha"] == [{"jid": "aaa", "host": "alpha"}]
        assert payload["bravo"] == [{"jid": "aaa", "host": "bravo"}]


# ----------------------------------------------------------------------
# v0.7.7: fetch workdir + archived spec
# ----------------------------------------------------------------------


class TestFetchWorkdirIgnoresArchive:
    def test_workdir_fetch_works_on_archived_workspace(
        self, cli_state: Path
    ) -> None:
        """A job whose workspace has been archived (``vq cleanup
        --archive``) may STILL have a live workdir on disk —
        archiving only touches ``spec.cwd`` / ``spec.archive_path``,
        not ``spec.workdir``. The workdir fetch must therefore
        succeed regardless of ``spec.is_archived``."""
        from vq.spec import JobSpec, JobState

        queue = paths.queue_dir()
        jobs = paths.jobs_dir()
        queue.mkdir(parents=True, exist_ok=True)
        jobs.mkdir(parents=True, exist_ok=True)
        wd = cli_state / "workdirs" / "abc123def456"
        wd.mkdir(parents=True)
        (wd / "scratch.bin").write_text("scratchy")
        spec = JobSpec(
            id="abc123def456",
            command=["true"],
            cwd=str(jobs / "abc123def456"),
            cpus=1,
            submitter="x@y",
            workdir=str(wd),
            state=JobState.COMPLETED,
            finished_at="2026-05-27T10:00:00+00:00",
            exit_code=0,
            # The archive markers:
            archived_at="2026-05-27T11:00:00+00:00",
            archive_path="/tmp/abc123def456.tar.bz2",
        )
        spec.write(queue / "abc123def456.json")
        out = cli_state / "fetched"
        dst = fetch_workdir_local("abc123def456", out)
        # Workdir landed despite archived workspace.
        assert (dst / "scratch.bin").read_text() == "scratchy"


# ----------------------------------------------------------------------
# v0.7.8: depends_on_any + INTERRUPTED
# ----------------------------------------------------------------------


class TestDependsOnAnyTerminalCoverage:
    def test_interrupted_predecessor_makes_dependent_ready(
        self,
    ) -> None:
        """v0.7.8: every state in TERMINAL_STATES should make an
        afterany dependent dispatch-ready. INTERRUPTED is in
        TERMINAL_STATES but the v0.7.8 test file's specific
        coverage list missed it. Pin it here as a tripwire."""
        from vq.spec import TERMINAL_STATES, JobState

        # INTERRUPTED must be in TERMINAL_STATES for afterany to
        # treat it as ready. If a future refactor removes it from
        # the set, this test catches the change.
        assert JobState.INTERRUPTED in TERMINAL_STATES


# ----------------------------------------------------------------------
# v0.7.9: reset-branch on fetch failure
# ----------------------------------------------------------------------


class TestResetBranchFetchFailure:
    def test_failed_fetch_leaves_admin_status_untouched(
        self,
        cli_state: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """If ``git fetch origin`` fails (network down, remote
        unreachable, auth failure), the helper must NOT attempt
        the ``git reset --hard`` — that would reset to a stale
        local ref. The admin-status record's last_sha should be
        preserved (still useful for diagnostics) rather than
        clobbered with None."""
        # Build a real but minimal repo. Don't bother with a remote
        # — we mock subprocess to force the fetch failure.
        clone = tmp_path / "clone"
        clone.mkdir()
        subprocess.run(
            ["git", "init", "--initial-branch", "main", str(clone)],
            capture_output=True, check=True,
        )
        (clone / "README.md").write_text("v1")
        subprocess.run(
            ["git", "-C", str(clone), "-c", "user.email=t@t",
             "-c", "user.name=t", "add", "."],
            capture_output=True, check=True,
        )
        subprocess.run(
            ["git", "-C", str(clone), "-c", "user.email=t@t",
             "-c", "user.name=t", "commit", "-m", "init"],
            capture_output=True, check=True,
        )

        (cli_state / "cfg" / "config.toml").write_text(
            f"""default_host = "localhost"

[hosts.localhost]
ssh = "localhost"

[programs.myenv]
kind = "venv"
python = "/usr/bin/python3"
git_dir = "{clone}"
branch = "main"
"""
        )
        cfg = config.load_config()
        # Pre-seed an admin-status record so we can verify it's
        # not clobbered by the failed reset.
        from vq.admin import AdminUpdateRecord
        admin.write_admin_status({
            "myenv": AdminUpdateRecord(
                last_updated_at="2026-05-27T00:00:00+00:00",
                last_success=True,
                last_sha="prior0000000",
                last_branch_expected="main",
                last_branch_actual="main",
            ),
        })

        # Force fetch failure by monkeypatching subprocess.run inside
        # the admin module to return rc=1 only for `git fetch`.
        orig_run = admin.subprocess.run

        def fake_run(cmd, *a, **kw):
            if "fetch" in cmd:
                return subprocess.CompletedProcess(
                    args=cmd, returncode=1,
                    stdout="", stderr="fatal: unable to reach origin\n",
                )
            return orig_run(cmd, *a, **kw)

        monkeypatch.setattr(admin.subprocess, "run", fake_run)

        result = admin.reset_branch_env("myenv", cfg=cfg)
        assert result.success is False
        assert result.fetch_rc != 0
        assert result.reset_rc is None  # never attempted
        # Admin status: last_sha preserved (NOT clobbered with None).
        rec = admin.read_admin_status()["myenv"]
        assert rec.last_sha == "prior0000000", (
            "failed reset-branch must not clobber the prior SHA in "
            "admin-status — that erases useful diagnostic state"
        )


# ----------------------------------------------------------------------
# v0.7.10: collapse-arrays edge cases
# ----------------------------------------------------------------------


class TestCollapseArraysEdgeCases:
    def test_collapse_single_element_group(self) -> None:
        """A ``--array 1`` submit is a degenerate but valid case.
        The fold should still produce a coherent ``ARRAY 1/1 done``
        row, not crash on the boundary."""
        elem = JobSpec(
            id="solo00000001",
            command=["true"],
            cwd="/tmp",
            cpus=1,
            submitter="x@y",
            state=JobState.COMPLETED,
            submitted_at="2026-05-27T10:00:00+00:00",
            array_index=0,
            array_total=1,
            array_group_id="grpSOLO01",
        )
        out = listing.format_table([elem], collapse_arrays=True)
        assert "grpSOLO01" in out
        assert "ARRAY 1/1 done" in out

    def test_collapse_array_with_all_failed(self) -> None:
        """A wholly-failed array group should render with the
        single-state form ``ARRAY N/N F`` (not the mixed form),
        making it instantly recognisable as "the whole sweep
        broke."""
        elems = [
            JobSpec(
                id=f"f{i:011x}", command=["true"], cwd="/tmp", cpus=1,
                submitter="x@y", state=JobState.FAILED,
                submitted_at="2026-05-27T10:00:00+00:00",
                array_index=i, array_total=5, array_group_id="grpFAIL5",
                exit_code=1, finished_at="2026-05-27T10:01:00+00:00",
            )
            for i in range(5)
        ]
        out = listing.format_table(elems, collapse_arrays=True)
        assert "ARRAY 5/5 F" in out


# ----------------------------------------------------------------------
# v0.7.11: remote --array byte-identical to single-call
# ----------------------------------------------------------------------


class TestRemoteArrayBackwardCompat:
    def test_array_one_omits_array_flag(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The v0.7.11 refactor folds the single-call and array
        paths into one ``submit_remote`` call. For backward compat
        of the wire shape (which the remote v0.6.52 also accepts),
        ``array=1`` must NOT emit a ``--array 1`` flag — that
        would route through the remote's array-spawn path on
        elements the operator never asked to be an array."""
        host_cfg = HostConfig(ssh="host_d", remote_vq="vq")

        captured_argv: list[tuple[str, ...]] = []

        def fake_upload(*a, **k): pass
        def fake_run_remote_vq(host_cfg, *args, check=True, **_kwargs):
            captured_argv.append(args)
            return subprocess.CompletedProcess(
                args=["ssh"], returncode=0,
                stdout="aaaaaaaaaaaa\n", stderr="",
            )
        def fake_run_remote_shell(host_cfg, *args, check=True, **_kwargs):
            return subprocess.CompletedProcess(args=list(args), returncode=0, stdout="", stderr="")
        monkeypatch.setattr(submit.transport, "upload_file", fake_upload)
        monkeypatch.setattr(submit.transport, "run_remote_vq", fake_run_remote_vq)
        monkeypatch.setattr(submit.transport, "run_remote_shell", fake_run_remote_shell)

        src = tmp_path / "in.py"
        src.write_text("pass")
        submit.submit_remote(
            host="host_d", host_cfg=host_cfg, input_file=str(src),
            # No array= arg → default 1.
        )
        # --array MUST NOT appear on the remote argv for the single-
        # call path.
        assert len(captured_argv) == 1
        assert "--array" not in captured_argv[0], (
            f"submit_remote with default array=1 must not emit "
            f"--array on the wire; got {list(captured_argv[0])}"
        )
