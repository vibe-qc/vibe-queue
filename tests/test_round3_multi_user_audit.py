"""v0.7.14 *Hamming's Code* — multi-user test coverage audit.

The general round-3 audit (v0.7.13) walked the v0.6.18 → v0.7.12
ships but stopped short of the multi-user codepaths called out
in the roadmap history's "Round-3 hardening audit + multi-user test
coverage audit" item. v0.7.14 fills the multi-user gap.

Audit scope by ship — each test pins a multi-user-specific
codepath that the per-module test files left implicit:

* **v0.7.7 fetch --workdir** — ``fetch_workdir_local(...,
  multi_user=True)`` resolves the spec via
  ``resolve_spec_path`` (cross-user search) rather than the
  single-user ``queue_dir()``.
* **v0.7.7 fetch --workdir** — workdir destination dir name
  honours the multi-user spec's job_name when set.
* **v0.7.8 depends_on_any** — the validation pass in
  ``submit_local`` checks ``--depends-on-any`` predecessors
  against the SAME multi-user queue_dir as ``--depends-on``
  (i.e. the submitter's per-user dir, not a cross-user
  lookup).
* **v0.7.9 reset-branch** — admin-status writes use the
  daemon's expected path. (We don't ship multi-user
  unification yet — v0.7.12 documents the gap — so this
  test pins the *current* behaviour explicitly so the
  future unification ship has a known starting point.)
* **v0.7.10 collapse-arrays** — ``list_jobs`` in multi-user
  mode aggregates across every user; collapse-arrays on a
  multi-user listing still groups by array_group_id
  correctly (the group can never span users, but the
  aggregation order could surprise the fold).
* **v0.7.11 remote --array** — `submit_remote` doesn't care
  about multi-user (it's a wire-level optimisation), but
  pin that the remote vq's multi-user submit endpoint is
  what processes ``--array N`` — i.e. the wire shape carries
  no multi-user info; the remote alone decides.

This is a coverage-first file, not a feature file. Adds
tests; no production code touched.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from vq import config, fetch, listing, paths, submit
from vq.config import HostConfig
from vq.spec import JobSpec, JobState


@pytest.fixture
def mu_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Path:
    """Multi-user root + config dir, both pointed at tmp_path.
    Tests get a clean ``users/`` tree per invocation."""
    monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(tmp_path))
    monkeypatch.setenv(paths.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    (tmp_path / "cfg" / "config.toml").write_text(
        "[multi_user]\nenabled = true\n"
    )
    monkeypatch.setattr(
        config, "SYSTEM_CONFIG_PATH", tmp_path / "absent-system.toml"
    )
    (tmp_path / "users").mkdir()
    return tmp_path


def _materialize_mu_job(
    mu_root: Path,
    uid: str,
    jobid: str,
    *,
    workspace_files: dict[str, str] | None = None,
    workdir_files: dict[str, str] | None = None,
    job_name: str | None = None,
    state: JobState = JobState.PENDING,
    array_index: int | None = None,
    array_total: int | None = None,
    array_group_id: str | None = None,
) -> JobSpec:
    """Build a multi-user job: spec under
    ``users/<uid>/queue/`` + workspace under
    ``users/<uid>/jobs/<jobid>/`` + optional workdir under
    ``users/<uid>/workdirs/<jobid>/``."""
    user_q = mu_root / "users" / uid / "queue"
    user_j = mu_root / "users" / uid / "jobs"
    user_w = mu_root / "users" / uid / "workdirs"
    user_q.mkdir(parents=True, exist_ok=True)
    user_j.mkdir(parents=True, exist_ok=True)
    user_w.mkdir(parents=True, exist_ok=True)
    workspace = user_j / jobid
    workspace.mkdir()
    for name, content in (workspace_files or {}).items():
        (workspace / name).write_text(content)
    workdir = user_w / jobid
    if workdir_files is not None:
        workdir.mkdir()
        for name, content in workdir_files.items():
            (workdir / name).write_text(content)
    spec = JobSpec(
        id=jobid,
        command=["true"],
        cwd=str(workspace),
        cpus=1,
        submitter=uid,
        job_name=job_name,
        state=state,
        workdir=str(workdir) if workdir_files is not None else None,
        array_index=array_index,
        array_total=array_total,
        array_group_id=array_group_id,
    )
    spec.write(user_q / f"{jobid}.json")
    return spec


# ----------------------------------------------------------------------
# v0.7.7 fetch --workdir under multi-user
# ----------------------------------------------------------------------


class TestFetchWorkdirMultiUser:
    def test_resolves_via_per_user_queue(
        self, mu_state: Path
    ) -> None:
        """In multi-user mode, ``fetch_workdir_local`` should
        resolve the spec via ``resolve_spec_path`` (which searches
        every user dir) rather than the legacy single-user
        ``queue_dir()`` (which only looks at the laptop's
        ~/.local/share/vq/queue/)."""
        _materialize_mu_job(
            mu_state, uid=str(os.geteuid()), jobid="mujob0000001",
            workspace_files={"in.py": "pass"},
            workdir_files={"scratch.bin": "raw"},
        )
        out = mu_state / "fetched"
        dst = fetch.fetch_workdir_local(
            "mujob0000001", out, multi_user=True,
        )
        assert (dst / "scratch.bin").read_text() == "raw"

    def test_workdir_dest_honours_job_name(
        self, mu_state: Path
    ) -> None:
        """The destination directory naming convention
        (``<job_name>-<jobid>-workdir/``) applies under multi-user
        the same as single-user."""
        _materialize_mu_job(
            mu_state, uid=str(os.geteuid()), jobid="mujob0000002",
            workspace_files={},
            workdir_files={"out.dat": "data"},
            job_name="experiment-7",
        )
        out = mu_state / "fetched"
        dst = fetch.fetch_workdir_local(
            "mujob0000002", out, multi_user=True,
        )
        assert dst == out / "experiment-7-mujob0000002-workdir"

    def test_unknown_jobid_in_multi_user_errors(
        self, mu_state: Path
    ) -> None:
        """A jobid that doesn't exist in any user's queue dir
        should fail with the same ``FileNotFoundError`` shape as
        single-user mode (so callers don't have to branch on
        mode)."""
        with pytest.raises(FileNotFoundError, match="no such job"):
            fetch.fetch_workdir_local(
                "neverexistsX", mu_state / "out", multi_user=True,
            )


# ----------------------------------------------------------------------
# v0.7.8 depends_on_any validation under multi-user
# ----------------------------------------------------------------------


class TestDependsOnAnyMultiUserValidation:
    def test_predecessor_validated_against_submitter_queue_dir(
        self, mu_state: Path
    ) -> None:
        """``submit_local`` validates ``--depends-on-any``
        predecessors against the caller's queue_dir, exactly as it
        does for ``--depends-on``. A predecessor in a DIFFERENT
        user's queue is invisible — the submit fails with a
        no-such-job error. Cross-user dependency mode is a future
        feature; this test pins the current single-user-scoped
        behaviour."""
        # User 1000 submits a job into their own queue.
        user_a_q = mu_state / "users" / "1000" / "queue"
        user_a_j = mu_state / "users" / "1000" / "jobs"
        user_a_q.mkdir(parents=True)
        user_a_j.mkdir(parents=True)
        pred_id = "aaaa11112222"
        pred_workspace = user_a_j / pred_id
        pred_workspace.mkdir()
        pred = JobSpec(
            id=pred_id, command=["true"], cwd=str(pred_workspace),
            cpus=1, submitter="1000",
        )
        pred.write(user_a_q / f"{pred_id}.json")

        # User 2000 tries to submit a job depending on user 1000's
        # job. We simulate this by passing user 2000's queue_dir
        # explicitly. The validation should reject.
        user_b_q = mu_state / "users" / "2000" / "queue"
        user_b_j = mu_state / "users" / "2000" / "jobs"
        user_b_q.mkdir(parents=True)
        user_b_j.mkdir(parents=True)
        f = mu_state / "input.py"
        f.write_text("pass")
        with pytest.raises(ValueError, match="no such job"):
            submit.submit_local(
                host="localhost",
                input_file=str(f),
                depends_on_any=[pred_id],
                queue_dir=user_b_q,
                jobs_dir=user_b_j,
                multi_user=True,
            )


# ----------------------------------------------------------------------
# v0.7.9 reset-branch — admin-status path pinned in multi-user
# ----------------------------------------------------------------------


class TestAdminStatusMultiUserPathBaseline:
    def test_admin_status_path_in_multi_user_is_state_root(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """v0.7.12's audit doc flagged that ``admin-status.json``
        lives at ``state_root()/admin-status.json`` in BOTH user-
        mode and daemon-mode (just with different state roots).
        This test pins the current resolution so the future
        unification ship has a known baseline to diff against —
        if someone moves admin-status.json without updating the
        audit doc, this test will catch it."""
        from vq import admin

        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        expected = tmp_path / "state" / "admin-status.json"
        assert admin.admin_status_path() == expected


# ----------------------------------------------------------------------
# v0.7.10 collapse-arrays across users
# ----------------------------------------------------------------------


class TestCollapseArraysMultiUser:
    def test_list_jobs_multi_user_aggregates_across_users(
        self, mu_state: Path
    ) -> None:
        """``list_jobs(host, multi_user=True)`` walks every user
        dir. Two array groups submitted by two different users
        should both appear in the listing — sanity check before
        the collapse-arrays interaction below."""
        _materialize_mu_job(
            mu_state, uid="1000", jobid="aa11aa11aa11",
            array_index=0, array_total=1, array_group_id="grpUSR1",
        )
        _materialize_mu_job(
            mu_state, uid="2000", jobid="bb22bb22bb22",
            array_index=0, array_total=1, array_group_id="grpUSR2",
        )
        specs = listing.list_jobs("localhost", multi_user=True)
        ids = {s.id for s in specs}
        assert "aa11aa11aa11" in ids
        assert "bb22bb22bb22" in ids

    def test_collapse_arrays_folds_each_users_group_independently(
        self, mu_state: Path
    ) -> None:
        """Array groups never span users (the array_group_id is
        minted per-submit), but ``vq queue --collapse-arrays``
        runs over the aggregated multi-user listing. The fold
        should produce ONE row per group regardless of which
        user submitted it — and the rows should carry the
        correct group ids."""
        # User 1000 submits a 3-element array.
        for idx in range(3):
            _materialize_mu_job(
                mu_state, uid="1000",
                jobid=f"aa{idx:010x}",
                state=JobState.COMPLETED,
                array_index=idx, array_total=3,
                array_group_id="grpAAA01",
            )
        # User 2000 submits a 2-element array.
        for idx in range(2):
            _materialize_mu_job(
                mu_state, uid="2000",
                jobid=f"bb{idx:010x}",
                state=JobState.PENDING,
                array_index=idx, array_total=2,
                array_group_id="grpBBB02",
            )
        specs = listing.list_jobs("localhost", multi_user=True)
        out = listing.format_table(specs, collapse_arrays=True)
        # Both group ids appear.
        assert "grpAAA01" in out
        assert "grpBBB02" in out
        # User 1000's group: all-completed → "3/3 done".
        assert "ARRAY 3/3 done" in out
        # User 2000's group: all-pending → "2/2 P".
        assert "ARRAY 2/2 P" in out
        # No per-element jobid leaks.
        for s in specs:
            assert s.id not in out


# ----------------------------------------------------------------------
# v0.7.11 remote --array under multi-user
# ----------------------------------------------------------------------


class TestRemoteArrayMultiUserWireShape:
    def test_array_kwarg_only_affects_remote_argv(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """``submit_remote`` is purely a wire-level operation —
        whether the remote vq is in multi-user mode is the
        remote's business, decided server-side from
        ``/etc/vq/config.toml``. The local side carries no
        multi-user hint on the wire (no ``--multi-user`` flag).
        Pin that: only ``--array N`` shows up on the remote
        argv; nothing else changes under multi-user."""
        host_cfg = HostConfig(ssh="host_d", remote_vq="vq")
        captured: list[tuple[str, ...]] = []

        def fake_upload(*a, **k):
            pass

        def fake_run_remote_vq(host_cfg, *args, check=True, **_kwargs):
            captured.append(args)
            jids = "\n".join(f"{i:012x}" for i in range(3))
            return subprocess.CompletedProcess(
                args=["ssh"], returncode=0,
                stdout=jids + "\n", stderr="",
            )

        def fake_run_remote_shell(host_cfg, *args, check=True, **_kwargs):
            return subprocess.CompletedProcess(
                args=list(args), returncode=0,
                stdout="", stderr="",
            )

        monkeypatch.setattr(submit.transport, "upload_file", fake_upload)
        monkeypatch.setattr(submit.transport, "run_remote_vq", fake_run_remote_vq)
        monkeypatch.setattr(submit.transport, "run_remote_shell", fake_run_remote_shell)

        src = tmp_path / "in.py"
        src.write_text("pass")
        submit.submit_remote(
            host="host_d", host_cfg=host_cfg, input_file=str(src),
            array=3,
        )
        # The wire shape carries no multi-user hint — only
        # ``--array 3``.
        assert len(captured) == 1
        argv = list(captured[0])
        assert "--array" in argv
        assert "3" in argv
        # And NO ``--multi-user`` / ``--uid`` / similar.
        for arg in argv:
            assert "--multi-user" not in arg
            assert "--uid" not in arg
