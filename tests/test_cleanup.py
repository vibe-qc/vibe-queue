"""Tests for vq.cleanup: archive / delete / restore + age parsing."""
from __future__ import annotations

import os
import tarfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from vq import cleanup, paths
from vq.spec import JobSpec, JobState, utcnow_iso


@pytest.fixture
def state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Isolate per-test state by pointing $VQ_STATE_DIR at tmp_path."""
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    paths.queue_dir().mkdir(parents=True)
    paths.jobs_dir().mkdir(parents=True)
    return tmp_path


def _materialize(
    jobid: str,
    *,
    state: JobState = JobState.COMPLETED,
    finished_offset_days: float = 0.0,
    files: dict[str, str] | None = None,
    archived: bool = False,
    scheduler_target: str | None = None,
    scheduler_job_id: str | None = None,
) -> JobSpec:
    workspace = paths.jobs_dir() / jobid
    workspace.mkdir(parents=True, exist_ok=True)
    files = files or {"stdout.log": "hello\n"}
    for name, content in files.items():
        (workspace / name).write_text(content)
    finished_at = (
        datetime.now(UTC) - timedelta(days=finished_offset_days)
    ).isoformat()
    spec = JobSpec(
        id=jobid,
        command=["echo", "hi"],
        cwd=str(workspace),
        cpus=1,
        state=state,
        finished_at=finished_at,
        exit_code=0,
        scheduler_target=scheduler_target,
        scheduler_job_id=scheduler_job_id,
    )
    if archived:
        # Archive on disk, stamp the spec, remove the workspace --
        # mirrors archive_workspace() but uses an explicit path so tests
        # don't need to round-trip through the public function.
        archive = paths.archive_dir() / f"{jobid}.tar.bz2"
        archive.parent.mkdir(parents=True, exist_ok=True)
        with tarfile.open(archive, mode="w:bz2") as tf:
            tf.add(workspace, arcname=jobid)
        spec.archived_at = utcnow_iso()
        spec.archive_path = str(archive)
        # Remove the original workspace once the tarball is on disk.
        for child in workspace.rglob("*"):
            if child.is_file():
                child.unlink()
        workspace.rmdir()
    spec.write(paths.spec_path(jobid))
    return spec


class TestParseAge:
    @pytest.mark.parametrize(
        "text,expected",
        [
            ("30d", timedelta(days=30)),
            ("4h", timedelta(hours=4)),
            ("1w", timedelta(weeks=1)),
            ("600s", timedelta(seconds=600)),
            ("15m", timedelta(minutes=15)),
            ("  30d  ", timedelta(days=30)),  # whitespace tolerated
        ],
    )
    def test_parses_canonical_forms(self, text: str, expected: timedelta) -> None:
        assert cleanup.parse_age(text) == expected

    @pytest.mark.parametrize(
        "text",
        [
            "",
            "30",  # no unit
            "d",  # no value
            "30 days",  # human form not supported
            "1.5d",  # fractions not supported
            "-5d",  # negative
            "0d",  # zero
            "30D",  # uppercase unit not supported (deliberate; keeps grammar tight)
            "30dx",  # trailing junk
            "1y",  # year unit not supported
        ],
    )
    def test_rejects_bad_input(self, text: str) -> None:
        with pytest.raises(ValueError):
            cleanup.parse_age(text)


class TestFindCandidates:
    def test_returns_terminal_jobs_only(self, state: Path) -> None:
        _materialize("a", state=JobState.COMPLETED)
        _materialize("b", state=JobState.RUNNING)
        _materialize("c", state=JobState.PENDING)
        _materialize("d", state=JobState.FAILED)
        _materialize("e", state=JobState.SUSPENDED)
        ids = {c.spec.id for c in cleanup.find_candidates()}
        assert ids == {"a", "d"}

    def test_age_filter_excludes_recent(self, state: Path) -> None:
        _materialize("old", finished_offset_days=40)
        _materialize("recent", finished_offset_days=5)
        ids = {
            c.spec.id
            for c in cleanup.find_candidates(older_than=timedelta(days=30))
        }
        assert ids == {"old"}

    def test_require_archived_true_filters_to_archived(self, state: Path) -> None:
        _materialize("plain", finished_offset_days=40)
        _materialize("arc", finished_offset_days=40, archived=True)
        ids = {
            c.spec.id
            for c in cleanup.find_candidates(require_archived=True)
        }
        assert ids == {"arc"}

    def test_require_archived_false_filters_to_non_archived(self, state: Path) -> None:
        _materialize("plain", finished_offset_days=40)
        _materialize("arc", finished_offset_days=40, archived=True)
        ids = {
            c.spec.id
            for c in cleanup.find_candidates(require_archived=False)
        }
        assert ids == {"plain"}

    def test_workspace_size_reflects_files(self, state: Path) -> None:
        _materialize("big", files={"a.bin": "x" * 10_000})
        candidates = cleanup.find_candidates()
        assert candidates[0].workspace_size >= 10_000

    def test_archive_size_reflects_tarball(self, state: Path) -> None:
        _materialize("arc", archived=True)
        candidates = cleanup.find_candidates(require_archived=True)
        assert candidates[0].archive_size > 0
        assert candidates[0].workspace_size == 0


class TestArchiveRoundTrip:
    def test_archive_creates_tarball_and_removes_workspace(self, state: Path) -> None:
        spec = _materialize(
            "j1", files={"out.txt": "hello world\n"}
        )
        workspace = Path(spec.cwd)
        assert workspace.is_dir()

        archive = cleanup.archive_workspace(spec)

        assert archive.is_file()
        assert not workspace.exists(), "archive should remove the workspace"
        on_disk = JobSpec.read(paths.spec_path("j1"))
        assert on_disk.is_archived
        assert on_disk.archive_path == str(archive)

    def test_archive_refuses_non_terminal(self, state: Path) -> None:
        spec = _materialize("running", state=JobState.RUNNING)
        with pytest.raises(ValueError, match="terminal"):
            cleanup.archive_workspace(spec)

    def test_archive_refuses_double_archive(self, state: Path) -> None:
        spec = _materialize("arc", archived=True)
        with pytest.raises(ValueError, match="already archived"):
            cleanup.archive_workspace(spec)

    def test_archive_refuses_missing_workspace(self, state: Path) -> None:
        spec = _materialize("gone")
        # Manually delete the workspace to simulate hand-removal.
        ws = Path(spec.cwd)
        for f in ws.iterdir():
            f.unlink()
        ws.rmdir()
        with pytest.raises(FileNotFoundError, match="workspace"):
            cleanup.archive_workspace(spec)

    def test_restore_reconstructs_workspace_and_clears_fields(
        self, state: Path
    ) -> None:
        spec = _materialize(
            "j1", files={"out.txt": "round trip\n"}
        )
        cleanup.archive_workspace(spec)
        spec_after_archive = JobSpec.read(paths.spec_path("j1"))
        assert spec_after_archive.is_archived
        archive_path = Path(spec_after_archive.archive_path)  # type: ignore[arg-type]

        workspace = cleanup.restore_workspace(spec_after_archive)

        assert workspace.is_dir()
        assert (workspace / "out.txt").read_text() == "round trip\n"
        on_disk = JobSpec.read(paths.spec_path("j1"))
        assert not on_disk.is_archived
        assert on_disk.archive_path is None
        # Archive file should be gone after a successful restore.
        assert not archive_path.exists()

    def test_restore_refuses_unarchived(self, state: Path) -> None:
        spec = _materialize("plain")
        with pytest.raises(ValueError, match="not archived"):
            cleanup.restore_workspace(spec)

    def test_restore_refuses_when_workspace_already_exists(
        self, state: Path
    ) -> None:
        spec = _materialize("arc", archived=True)
        # Recreate the workspace so the restore call sees a conflict.
        Path(spec.cwd).mkdir(parents=True, exist_ok=True)
        with pytest.raises(FileExistsError):
            cleanup.restore_workspace(spec)


class TestArchiveWithJobName:
    """v0.5.34: when a job has ``job_name`` set, the archive filename
    AND the tarball's internal top-level directory both use
    ``<name>-<jobid>``. The on-disk workspace path is unaffected — the
    name is purely a user-visible-artifact knob."""

    def test_archive_filename_uses_name_prefix(self, state: Path) -> None:
        spec = _materialize("j1", files={"out.txt": "x\n"})
        # Add the name after materialize (the _materialize helper
        # doesn't take a name; this is the minimal patch path).
        spec.job_name = "mgo-pbe"
        spec.write(paths.spec_path("j1"))

        archive = cleanup.archive_workspace(spec)
        assert archive.name == "mgo-pbe-j1.tar.bz2", (
            f"expected human-readable archive name, got {archive.name!r}"
        )

    def test_tarball_top_level_uses_name_prefix(self, state: Path) -> None:
        """The internal arcname must match the outer filename — that's
        what makes ``vq fetch`` lands at ``./mgo-pbe-j1/`` when un-tarred."""
        spec = _materialize("j1", files={"out.txt": "x\n"})
        spec.job_name = "mgo-pbe"
        spec.write(paths.spec_path("j1"))

        archive = cleanup.archive_workspace(spec)
        with tarfile.open(archive, mode="r:bz2") as tf:
            names = [m.name for m in tf.getmembers()]
        # Every member should be under "mgo-pbe-j1/"
        assert any(n == "mgo-pbe-j1" for n in names), (
            f"top-level dir 'mgo-pbe-j1' missing; got {names}"
        )
        for n in names:
            top = n.split("/", 1)[0]
            assert top == "mgo-pbe-j1", (
                f"unexpected top-level segment {top!r} in member {n!r}"
            )

    def test_archive_filename_falls_back_to_jobid_when_unnamed(
        self, state: Path
    ) -> None:
        """Pre-v0.5.34 archives (no name) keep their old shape exactly —
        no leading dash, no empty prefix."""
        spec = _materialize("j2", files={"out.txt": "x\n"})
        archive = cleanup.archive_workspace(spec)
        assert archive.name == "j2.tar.bz2"

    def test_restore_after_named_archive_lands_at_workspace_dir(
        self, state: Path
    ) -> None:
        """The on-disk workspace dir is always ``jobs/<jobid>/`` —
        even after archive+restore with a name set, restore must put
        content back at the original location (not at
        ``jobs/<name>-<jobid>/``). This is what lets the daemon find
        the workspace by jobid after a restore."""
        spec = _materialize("j3", files={"out.txt": "round\n"})
        spec.job_name = "tagged"
        spec.write(paths.spec_path("j3"))
        cleanup.archive_workspace(spec)
        spec_after = JobSpec.read(paths.spec_path("j3"))

        workspace = cleanup.restore_workspace(spec_after)
        assert workspace == paths.jobs_dir() / "j3"
        assert (workspace / "out.txt").read_text() == "round\n"
        # And there's no stray <name>-<jobid> dir leftover.
        assert not (paths.jobs_dir() / "tagged-j3").exists()


class TestDelete:
    def test_explicit_candidate_rejects_spec_filename_id_mismatch(
        self,
        state: Path,
    ) -> None:
        victim = _materialize("victim")
        victim_workspace = Path(victim.cwd)
        misbound = JobSpec(
            id="victim",
            command=["false"],
            cwd=str(victim_workspace),
            cpus=1,
            state=JobState.COMPLETED,
            finished_at=utcnow_iso(),
            exit_code=1,
        )
        misbound.write(paths.queue_dir() / "requested.json")

        candidates, errors = cleanup.find_candidates_by_jobid(["requested"])

        assert candidates == []
        assert errors == [("requested", "corrupt spec: filename/id mismatch")]
        assert victim_workspace.is_dir()
        assert paths.spec_path("victim").is_file()

    def test_delete_rejects_fresh_spec_filename_id_mismatch(
        self,
        state: Path,
    ) -> None:
        victim = _materialize("victim")
        requested = JobSpec(
            id="requested",
            command=["true"],
            cwd=str(paths.jobs_dir() / "requested"),
            cpus=1,
            state=JobState.COMPLETED,
            finished_at=utcnow_iso(),
            exit_code=0,
        )
        victim.write(paths.spec_path("requested"))

        with pytest.raises(ValueError, match="filename/id mismatch"):
            cleanup.delete_job(requested)

        assert Path(victim.cwd).is_dir()
        assert paths.spec_path("victim").is_file()
        assert paths.spec_path("requested").is_file()

    @pytest.mark.parametrize("record_kind", ["symlink", "fifo", "oversize", "malformed"])
    def test_delete_rejects_unsafe_fresh_spec_without_artifact_mutation(
        self,
        state: Path,
        record_kind: str,
    ) -> None:
        victim = _materialize("unsafe-delete")
        workspace = Path(victim.cwd)
        sentinel = workspace / "stdout.log"
        spec_path = paths.spec_path(victim.id)
        spec_path.unlink()
        if record_kind == "symlink":
            external = state / "external-spec"
            external.write_text(victim.to_json())
            spec_path.symlink_to(external)
        elif record_kind == "fifo":
            os.mkfifo(spec_path)
        elif record_kind == "oversize":
            with spec_path.open("wb") as stream:
                stream.truncate(cleanup.spec_access.SPEC_READ_MAX_BYTES + 1)
        else:
            spec_path.write_text("{malformed\n")

        with pytest.raises((OSError, ValueError)):
            cleanup.delete_job(victim)

        assert sentinel.read_text() == "hello\n"
        assert spec_path.exists() or spec_path.is_symlink()

    def test_delete_detects_post_read_spec_swap_before_artifact_mutation(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        victim = _materialize("swap-delete")
        workspace = Path(victim.cwd)
        sentinel = workspace / "stdout.log"
        spec_path = paths.spec_path(victim.id)
        original_read = cleanup.spec_access.read_bounded_regular_spec
        calls = 0

        def swap_after_read(path: Path) -> JobSpec:
            nonlocal calls
            result = original_read(path)
            calls += 1
            if calls == 1:
                replacement = result.model_copy(
                    update={"cwd": str(state / "outside")}
                )
                replacement.write(path)
            return result

        monkeypatch.setattr(
            cleanup.spec_access,
            "read_bounded_regular_spec",
            swap_after_read,
        )

        with pytest.raises(ValueError, match="changed during cleanup"):
            cleanup.delete_job(victim)

        assert sentinel.read_text() == "hello\n"
        assert spec_path.is_file()

    @pytest.mark.parametrize("replacement", ["missing", "dangling-symlink"])
    def test_delete_detects_post_read_spec_disappearance(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
        replacement: str,
    ) -> None:
        victim = _materialize(f"gone-{replacement}")
        workspace = Path(victim.cwd)
        sentinel = workspace / "stdout.log"
        original_read = cleanup.spec_access.read_bounded_regular_spec
        calls = 0

        def remove_after_read(path: Path) -> JobSpec:
            nonlocal calls
            result = original_read(path)
            calls += 1
            if calls == 1:
                path.unlink()
                if replacement == "dangling-symlink":
                    path.symlink_to(state / "does-not-exist")
            return result

        monkeypatch.setattr(
            cleanup.spec_access,
            "read_bounded_regular_spec",
            remove_after_read,
        )

        with pytest.raises((FileNotFoundError, ValueError)):
            cleanup.delete_job(victim)

        assert sentinel.read_text() == "hello\n"

    def test_delete_removes_spec_and_workspace(self, state: Path) -> None:
        spec = _materialize("j1")
        workspace = Path(spec.cwd)
        spec_file = paths.spec_path("j1")
        assert workspace.exists() and spec_file.exists()

        cleanup.delete_job(spec)

        assert not workspace.exists()
        assert not spec_file.exists()

    def test_delete_also_removes_archive(self, state: Path) -> None:
        spec = _materialize("arc", archived=True)
        archive = Path(spec.archive_path)  # type: ignore[arg-type]
        assert archive.exists()

        cleanup.delete_job(spec)

        assert not archive.exists()
        assert not paths.spec_path("arc").exists()

    def test_delete_refuses_non_terminal(self, state: Path) -> None:
        spec = _materialize("active", state=JobState.RUNNING)
        with pytest.raises(ValueError, match="terminal"):
            cleanup.delete_job(spec)

    def test_delete_idempotent_on_missing_files(self, state: Path) -> None:
        spec = _materialize("j1")
        # Hand-remove the workspace and spec to simulate partial state.
        for f in Path(spec.cwd).iterdir():
            f.unlink()
        Path(spec.cwd).rmdir()
        paths.spec_path("j1").unlink()
        cleanup.delete_job(spec)  # must not raise


class TestSchedulerRemoteWorkspaceCleanup:
    @pytest.mark.parametrize(
        ("scheduler_job_id", "expected_job_id"),
        [
            ("12345.cluster", "12345.cluster"),
            (None, "sched-projection"),
            ("", "sched-projection"),
        ],
    )
    def test_cli_reaper_preserves_cleanup_job_id_fallback(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
        scheduler_job_id: str | None,
        expected_job_id: str,
    ) -> None:
        """Cleanup alone falls back from a falsey scheduler id to the vq id."""
        from vq import cli as cli_mod

        spec = _materialize(
            "sched-projection",
            scheduler_target="host_f",
            scheduler_job_id=scheduler_job_id,
        )
        spec.array_index = 2
        spec.array_total = 5
        spec.array_group_id = "cleanup-array"
        captured: list[tuple[str, str, int | None]] = []

        class RecordingDispatcher:
            def remote_workspace(self, jobid: str) -> str:
                return f"/remote/{jobid}"

            def cleanup_remote_workspace(self, handle) -> None:  # type: ignore[no-untyped-def]
                captured.append(
                    (handle.job_id, handle.remote_workspace, handle.array_size)
                )

        dispatcher = RecordingDispatcher()
        monkeypatch.setattr(
            cli_mod,
            "scheduler_dispatcher_for",
            lambda _host_cfg: dispatcher,
        )
        cfg = SimpleNamespace(host=lambda _target: object())

        cli_mod._scheduler_workspace_reaper(cfg)(spec)

        assert captured == [
            (expected_job_id, "/remote/sched-projection", None)
        ]

    def test_cleanup_scheduler_remote_workspace_stamps_spec(self, state: Path) -> None:
        spec = _materialize(
            "sched1",
            scheduler_target="host_f",
            scheduler_job_id="12345.cluster",
        )
        calls: list[str] = []

        cleaned = cleanup.cleanup_scheduler_remote_workspace(
            spec,
            lambda s: calls.append(s.id),
        )

        assert cleaned is True
        assert calls == ["sched1"]
        recovered = JobSpec.read(paths.spec_path("sched1"))
        assert recovered.scheduler_remote_workspace_cleaned_at is not None

    def test_auto_cleanup_retries_archived_scheduler_workspace(
        self, state: Path
    ) -> None:
        spec = _materialize(
            "schedarc",
            finished_offset_days=40,
            archived=True,
            scheduler_target="host_f",
            scheduler_job_id="12345.cluster",
        )
        calls: list[str] = []
        policy = cleanup.AutoCleanupPolicy(archive_after_seconds=30 * 86400)

        counts = cleanup.run_auto_cleanup_pass(
            policy,
            scheduler_workspace_reaper=lambda s: calls.append(s.id),
        )

        assert counts["scheduler_workspaces_swept"] == 1
        assert calls == [spec.id]
        recovered = JobSpec.read(paths.spec_path(spec.id))
        assert recovered.is_archived
        assert recovered.scheduler_remote_workspace_cleaned_at is not None

    def test_delete_keeps_spec_when_scheduler_cleanup_fails(
        self, state: Path
    ) -> None:
        spec = _materialize(
            "scheddel",
            finished_offset_days=200,
            scheduler_target="host_f",
            scheduler_job_id="12345.cluster",
        )

        def fail_reaper(_: JobSpec) -> None:
            raise RuntimeError("ssh unavailable")

        policy = cleanup.AutoCleanupPolicy(delete_after_seconds=90 * 86400)
        counts = cleanup.run_auto_cleanup_pass(
            policy,
            scheduler_workspace_reaper=fail_reaper,
        )

        assert counts["deleted"] == 0
        assert counts["scheduler_workspace_errors"] >= 1
        assert paths.spec_path(spec.id).exists()


class TestFormatTable:
    def test_empty_returns_no_eligible_message(self) -> None:
        assert "no eligible" in cleanup.format_table(
            [], action="archive", dry_run=True
        )

    def test_dry_run_prefix(self, state: Path) -> None:
        _materialize("j1")
        candidates = cleanup.find_candidates()
        text = cleanup.format_table(candidates, action="archive", dry_run=True)
        assert "would archive" in text

    def test_execute_summary(self, state: Path) -> None:
        _materialize("j1")
        candidates = cleanup.find_candidates()
        text = cleanup.format_table(candidates, action="archive", dry_run=False)
        assert "archived:" in text
