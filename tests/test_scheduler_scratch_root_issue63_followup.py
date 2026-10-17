"""#63 follow-ups, scheduler side: the workspace a job is bound to.

Two defects were demonstrated against the merged #63 change.

* The refusal to adopt a changed ``scratch_root`` while a host still carries
  a job with no recorded workspace was gated on a *cached* dispatcher, and
  the cache is empty at construction and cleared wholesale by a config
  reload. Those are exactly the two paths the operator runbook names -- set
  the key, restart the daemon -- so the guard was silent when it mattered,
  and reattachment then wrote the new root onto a pre-#63 job **durably**.
* ``vq status`` recomputed the remote workspace from the current config
  instead of reporting the recorded one. ``runtime_workdir`` is a published
  agent-contract field and ``checkpoint_qvf_path`` is derived from it, so an
  agent polled a checkpoint path nothing would ever write.

No host, scheduler, SSH or network is involved: a config file in a temporary
directory and the daemon's own bookkeeping are enough to show both.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from vq import paths
from vq.daemon import Daemon
from vq.spec import JobSpec, JobState

# ===========================================================================
# D1. The refusal has to survive the restart the runbook prescribes
# ===========================================================================


class TestTheRefusalSurvivesARestart:
    """``docs/fleet_layout.md`` phase 5 says: set the key, restart the daemon.

    The guard was gated on a cached dispatcher to compare against. The cache
    is empty at construction and cleared by a reload, so on exactly those two
    paths the daemon adopted the new root and -- worse -- ``reattach``'s
    binding wrote that new root onto a pre-#63 job's spec, permanently.
    """

    @pytest.fixture
    def cfg_dir(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
        cfg_dir = tmp_path / "sched-cfg"
        cfg_dir.mkdir()
        monkeypatch.setenv(paths.ENV_CONFIG_DIR, str(cfg_dir))
        return cfg_dir

    def _write_host(self, cfg_dir: Path, scratch_root: str) -> None:
        (cfg_dir / "config.toml").write_text(
            "[hosts.host_f]\n"
            'ssh = "host-f.invalid"\n'
            'scheduler = "pbs"\n'
            'scheduler_dialect = "torque"\n'
            'scheduler_driver = "localhost"\n'
            f'scratch_root = "{scratch_root}"\n'
            "\n[hosts.localhost]\n"
            'ssh = "localhost"\n',
            encoding="utf-8",
        )

    def _daemon(self, *, replacing: Daemon | None = None) -> Daemon:
        """One daemon at a time, as on a real host.

        ``replacing`` releases the previous daemon's lifetime queue lock
        first, which is what makes the next constructor a *restart* rather
        than a second daemon on one state root.
        """
        if replacing is not None:
            replacing._queue_lock_fd.close()
        d = Daemon(
            max_cpus=4,
            poll_interval=0.05,
            queue_dir=paths.queue_dir(),
            jobs_dir=paths.jobs_dir(),
        )
        d.queue_dir.mkdir(parents=True, exist_ok=True)
        d.jobs_dir.mkdir(parents=True, exist_ok=True)
        return d

    def _unbound_job(self, daemon: Daemon, jobid: str) -> tuple[JobSpec, Path]:
        """A job as a pre-#63 daemon left it: running, no recorded workspace."""
        workspace = daemon.jobs_dir / jobid
        workspace.mkdir(parents=True, exist_ok=True)
        spec = JobSpec(
            id=jobid,
            command=["true"],
            cwd=str(workspace),
            cpus=1,
            state=JobState.RUNNING,
            scheduler_target="host_f",
            scheduler_job_id=f"{jobid}.cluster",
        )
        spec_path = daemon._spec_path(jobid)
        spec.write(spec_path)
        assert spec.scheduler_remote_workspace is None
        return spec, spec_path

    def test_a_restart_still_serves_the_previous_root(self, cfg_dir: Path) -> None:
        self._write_host(cfg_dir, "/scratch/original")
        first = self._daemon()
        assert first._scheduler_dispatcher_for("host_f").scratch_root == (
            "/scratch/original"
        )
        self._unbound_job(first, "schedjob0101")

        # The operator edits the key and restarts: a brand-new Daemon, so the
        # dispatcher cache that used to carry the old root is empty.
        self._write_host(cfg_dir, "/scratch/moved-by-the-operator")
        restarted = self._daemon(replacing=first)

        assert restarted._scheduler_dispatcher_for("host_f").scratch_root == (
            "/scratch/original"
        ), (
            "#63: the previous root must keep serving until every unbound job "
            "binds or drains, and a restart is not amnesia"
        )

    def test_a_restart_does_not_durably_bind_a_pre_63_job_to_the_new_root(
        self, cfg_dir: Path
    ) -> None:
        """The permanent half of the defect.

        Reattachment is what binds a record that predates the binding, and
        reattachment happens at daemon start -- the same moment the cache is
        empty. So the guard was off precisely when the durable write happened.
        """
        self._write_host(cfg_dir, "/scratch/original")
        first = self._daemon()
        first._scheduler_dispatcher_for("host_f")
        self._write_host(cfg_dir, "/scratch/moved-by-the-operator")

        restarted = self._daemon(replacing=first)
        spec, spec_path = self._unbound_job(restarted, "schedjob0102")
        bound = restarted._bind_scheduler_workspace(
            spec,
            restarted._scheduler_dispatcher_for("host_f"),
            spec_path=spec_path,
        )

        expected = "/scratch/original/.vibeqc-cluster/jobs/schedjob0102"
        assert bound == expected
        assert JobSpec.read(spec_path).scheduler_remote_workspace == expected, (
            "binding a pre-#63 job to a root nothing staged it under is the "
            "stranding #63 exists to prevent, made permanent"
        )

    def test_a_config_reload_does_not_walk_past_the_refusal(
        self, cfg_dir: Path
    ) -> None:
        self._write_host(cfg_dir, "/scratch/original")
        daemon = self._daemon()
        daemon._scheduler_dispatcher_for("host_f")
        self._unbound_job(daemon, "schedjob0103")

        self._write_host(cfg_dir, "/scratch/moved-by-the-operator")
        daemon.request_config_reload()
        daemon._maybe_apply_config_reload()

        assert daemon._scheduler_dispatchers == {}, "the reload clears the cache"
        assert daemon._scheduler_dispatcher_for("host_f").scratch_root == (
            "/scratch/original"
        )

    def test_the_change_lands_once_the_last_unbound_job_binds(
        self, cfg_dir: Path
    ) -> None:
        self._write_host(cfg_dir, "/scratch/original")
        first = self._daemon()
        first._scheduler_dispatcher_for("host_f")
        self._write_host(cfg_dir, "/scratch/moved-by-the-operator")

        restarted = self._daemon(replacing=first)
        spec, spec_path = self._unbound_job(restarted, "schedjob0104")
        held = restarted._scheduler_dispatcher_for("host_f")
        assert held.scratch_root == "/scratch/original"

        restarted._bind_scheduler_workspace(spec, held, spec_path=spec_path)
        adopted = restarted._scheduler_dispatcher_for("host_f")

        assert adopted.scratch_root == "/scratch/moved-by-the-operator"
        # And the adoption is remembered, so the *next* change is guarded too.
        assert restarted._remembered_scheduler_root("host_f") == (
            "/scratch/moved-by-the-operator"
        )

    def test_a_host_with_no_memory_adopts_what_is_configured(
        self, cfg_dir: Path
    ) -> None:
        """The first start after an upgrade must not refuse its own config.

        Nothing has been remembered yet, and an unbound job there was staged
        under the root that is still configured. Refusing would strand the
        whole host on no root at all.
        """
        self._write_host(cfg_dir, "/scratch/original")
        daemon = self._daemon()
        self._unbound_job(daemon, "schedjob0105")

        assert daemon._scheduler_dispatcher_for("host_f").scratch_root == (
            "/scratch/original"
        )
        assert daemon._remembered_scheduler_root("host_f") == "/scratch/original"

    def test_the_memory_is_a_readable_daemon_owned_record(
        self, cfg_dir: Path
    ) -> None:
        self._write_host(cfg_dir, "/scratch/original")
        daemon = self._daemon()
        daemon._scheduler_dispatcher_for("host_f")

        path = paths.scheduler_scratch_roots_path()
        payload = json.loads(path.read_text(encoding="utf-8"))

        assert payload["roots"] == {"host_f": "/scratch/original"}
        assert payload["schema"].startswith("vq.scheduler-scratch-roots/")

    def test_an_unreadable_memory_is_no_memory_rather_than_a_guess(
        self, cfg_dir: Path
    ) -> None:
        self._write_host(cfg_dir, "/scratch/moved-by-the-operator")
        paths.scheduler_scratch_roots_path().write_text(
            "{not json", encoding="utf-8"
        )
        daemon = self._daemon()
        self._unbound_job(daemon, "schedjob0106")

        assert daemon._scheduler_dispatcher_for("host_f").scratch_root == (
            "/scratch/moved-by-the-operator"
        )


# ===========================================================================
# D6. `vq status` reports the workspace the job is bound to
# ===========================================================================


class TestStatusReportsTheRecordedWorkspace:
    def test_runtime_workdir_is_the_recorded_one(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """``runtime_workdir`` is a published agent-contract field.

        ``checkpoint_qvf_path`` is derived from it, so recomputing it from
        the current ``scratch_root`` sent an agent to poll a checkpoint path
        nothing would ever write.
        """
        from vq import status as status_mod

        cfg_dir = tmp_path / "cfg"
        cfg_dir.mkdir()
        (cfg_dir / "config.toml").write_text(
            "[hosts.host_f]\n"
            'ssh = "host-f.invalid"\n'
            'scheduler = "pbs"\n'
            'scheduler_dialect = "torque"\n'
            'scheduler_driver = "localhost"\n'
            'scratch_root = "/scratch/moved-by-the-operator"\n',
            encoding="utf-8",
        )
        monkeypatch.setenv(paths.ENV_CONFIG_DIR, str(cfg_dir))
        recorded = "/scratch/original/.vibeqc-cluster/jobs/schedjob0201"
        spec = JobSpec(
            id="schedjob0201",
            command=["true"],
            cwd=str(tmp_path / "workspace"),
            cpus=1,
            state=JobState.RUNNING,
            scheduler_target="host_f",
            scheduler_job_id="201.cluster",
            scheduler_remote_workspace=recorded,
        )

        payload = status_mod.monitoring_payload_for_spec(spec)

        assert payload["runtime_workdir"] == recorded
        assert payload["checkpoint_qvf_path"] == (
            f"{recorded}/{status_mod.CHECKPOINT_QVF_FILENAME}"
        )

    def test_a_job_with_no_record_still_falls_back_to_the_derivation(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        from vq import status as status_mod

        cfg_dir = tmp_path / "cfg"
        cfg_dir.mkdir()
        (cfg_dir / "config.toml").write_text(
            "[hosts.host_f]\n"
            'ssh = "host-f.invalid"\n'
            'scheduler = "pbs"\n'
            'scheduler_dialect = "torque"\n'
            'scheduler_driver = "localhost"\n'
            'scratch_root = "/scratch/original"\n',
            encoding="utf-8",
        )
        monkeypatch.setenv(paths.ENV_CONFIG_DIR, str(cfg_dir))
        spec = JobSpec(
            id="schedjob0202",
            command=["true"],
            cwd=str(tmp_path / "workspace"),
            cpus=1,
            state=JobState.RUNNING,
            scheduler_target="host_f",
        )

        payload = status_mod.monitoring_payload_for_spec(spec)

        assert payload["runtime_workdir"] == (
            "/scratch/original/.vibeqc-cluster/jobs/schedjob0202"
        )
