"""Tests for submit_local: workspace materialization + spec generation."""
from __future__ import annotations

import errno
import io
import os
import socket
import stat
import sys
import tarfile
from pathlib import Path

import pytest

from vq import submit as submit_module
from vq.spec import JobSpec, JobState
from vq.submit import PayloadValidationError, new_jobid, submit_local


@pytest.fixture
def state_dirs(tmp_path: Path) -> tuple[Path, Path]:
    queue = tmp_path / "queue"
    jobs = tmp_path / "jobs"
    return queue, jobs


def _spec_for(queue_dir: Path, jobid: str) -> JobSpec:
    return JobSpec.read(queue_dir / f"{jobid}.json")


def _add_tar_bytes(
    archive: tarfile.TarFile,
    name: str,
    contents: bytes = b"pass\n",
) -> None:
    member = tarfile.TarInfo(name)
    member.size = len(contents)
    archive.addfile(member, io.BytesIO(contents))


class TestJobIDs:
    def test_new_jobid_returns_unique_short_hex(self) -> None:
        ids = {new_jobid() for _ in range(100)}
        assert len(ids) == 100
        for jid in ids:
            assert len(jid) == 12
            assert all(c in "0123456789abcdef" for c in jid)

    def test_existing_jobid_fails_closed_without_overwrite(
        self,
        tmp_path: Path,
        state_dirs: tuple[Path, Path],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        queue, jobs = state_dirs
        collision = "abc123def456"
        workspace = jobs / collision
        workspace.mkdir(parents=True)
        (workspace / "sentinel.txt").write_text("existing\n")
        queue.mkdir()
        existing = JobSpec(
            id=collision,
            command=["echo", "existing"],
            cwd=str(workspace),
            cpus=1,
        )
        existing.write(queue / f"{collision}.json")
        original_spec = (queue / f"{collision}.json").read_bytes()
        source = tmp_path / "input.py"
        source.write_text("new\n")
        monkeypatch.setattr(submit_module, "new_jobid", lambda: collision)

        with pytest.raises(FileExistsError, match="job ID collision"):
            submit_local(
                host="localhost",
                input_file=str(source),
                queue_dir=queue,
                jobs_dir=jobs,
            )

        assert (queue / f"{collision}.json").read_bytes() == original_spec
        assert (workspace / "sentinel.txt").read_text() == "existing\n"
        assert not (workspace / "input.py").exists()

    def test_existing_workspace_symlink_fails_closed_without_following(
        self,
        tmp_path: Path,
        state_dirs: tuple[Path, Path],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        queue, jobs = state_dirs
        collision = "abc123def456"
        outside = tmp_path / "outside"
        outside.mkdir()
        jobs.mkdir()
        (jobs / collision).symlink_to(outside, target_is_directory=True)
        source = tmp_path / "input.py"
        source.write_text("new\n")
        monkeypatch.setattr(submit_module, "new_jobid", lambda: collision)

        with pytest.raises(FileExistsError, match="job ID collision"):
            submit_local(
                host="localhost",
                input_file=str(source),
                queue_dir=queue,
                jobs_dir=jobs,
            )

        assert not (outside / "input.py").exists()
        assert not queue.exists() or not (queue / f"{collision}.json").exists()


    def test_spec_publication_race_preserves_winner_and_rolls_back_workspace(
        self,
        tmp_path: Path,
        state_dirs: tuple[Path, Path],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        queue, jobs = state_dirs
        collision = "abc123def456"
        source = tmp_path / "input.py"
        source.write_text("new\n")
        monkeypatch.setattr(submit_module, "new_jobid", lambda: collision)
        original_publish = submit_module._write_spec_exclusive

        def racing_publish(
            spec: JobSpec,
            path: Path,
            **kwargs: object,
        ) -> None:
            winner = JobSpec(
                id=collision,
                command=["echo", "winner"],
                cwd=str(tmp_path / "winner-workspace"),
                cpus=1,
            )
            original_publish(winner, path)
            original_publish(spec, path, **kwargs)  # type: ignore[arg-type]

        monkeypatch.setattr(
            submit_module,
            "_write_spec_exclusive",
            racing_publish,
        )

        with pytest.raises(FileExistsError, match="job ID collision"):
            submit_local(
                host="localhost",
                input_file=str(source),
                queue_dir=queue,
                jobs_dir=jobs,
            )

        assert JobSpec.read(queue / f"{collision}.json").command == [
            "echo",
            "winner",
        ]
        assert not (jobs / collision).exists()

    def test_copy_failure_rolls_back_only_owned_workspace_reservation(
        self,
        tmp_path: Path,
        state_dirs: tuple[Path, Path],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        queue, jobs = state_dirs
        source = tmp_path / "input.py"
        source.write_text("new\n")
        collision = "abc123def456"
        monkeypatch.setattr(submit_module, "new_jobid", lambda: collision)

        def fail_copy(*_args: object, **_kwargs: object) -> None:
            raise OSError("simulated source race")

        monkeypatch.setattr(submit_module.shutil, "copy2", fail_copy)
        with pytest.raises(OSError, match="simulated source race"):
            submit_local(
                host="localhost",
                input_file=str(source),
                queue_dir=queue,
                jobs_dir=jobs,
            )

        assert not (jobs / collision).exists()
        assert not (queue / f"{collision}.json").exists()

    def test_published_spec_survives_unsupported_directory_fsync(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        queue = tmp_path / "queue"
        queue.mkdir()
        workspace = tmp_path / "jobs" / "abc123def456"
        spec = JobSpec(
            id="abc123def456",
            command=["echo", "accepted"],
            cwd=str(workspace),
            cpus=1,
        )
        original_fsync = os.fsync

        def reject_directory_fsync(descriptor: int) -> None:
            if stat.S_ISDIR(os.fstat(descriptor).st_mode):
                raise OSError(errno.EINVAL, "directory fsync is unsupported")
            original_fsync(descriptor)

        monkeypatch.setattr(os, "fsync", reject_directory_fsync)

        submit_module._write_spec_exclusive(spec, queue / f"{spec.id}.json")

        assert JobSpec.read(queue / f"{spec.id}.json").command == [
            "echo",
            "accepted",
        ]

    def test_unkeyed_submit_returns_receipt_after_spec_parent_fsync_error(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        source = tmp_path / "input.py"
        source.write_text("pass\n")
        queue = tmp_path / "queue"
        jobs = tmp_path / "jobs"
        original_fsync = os.fsync

        def fail_queue_directory(descriptor: int) -> None:
            metadata = os.fstat(descriptor)
            if queue.is_dir() and metadata.st_ino == queue.stat().st_ino:
                raise OSError(errno.EIO, "simulated queue fsync error")
            original_fsync(descriptor)

        monkeypatch.setattr(os, "fsync", fail_queue_directory)

        jobid = submit_local(
            host="localhost",
            input_file=str(source),
            queue_dir=queue,
            jobs_dir=jobs,
        )

        assert JobSpec.read(queue / f"{jobid}.json").id == jobid
        assert (jobs / jobid / source.name).is_file()


class TestSingleFile:
    def test_workspace_contains_copied_input(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        src = tmp_path / "input.py"
        src.write_text("print('hi')")
        jid = submit_local(
            host="localhost", input_file=str(src), queue_dir=queue, jobs_dir=jobs
        )
        spec = _spec_for(queue, jid)
        assert spec.command == [sys.executable, "input.py"]
        assert spec.cpus == 1
        assert spec.state == JobState.PENDING
        assert (Path(spec.cwd) / "input.py").read_text() == "print('hi')"
        assert spec.workspace_source == str(src.resolve())

    def test_scheduler_target_recorded(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        # §17: the driver's `vq submit localhost --scheduler-target host_f` tags
        # the spec so the daemon dispatches it via SSH+qsub.
        queue, jobs = state_dirs
        src = tmp_path / "input.py"
        src.write_text("x")
        jid = submit_local(
            host="localhost",
            input_file=str(src),
            # A scheduler-target single-file submit must name a cluster-side
            # interpreter; the driver's own is rejected (see
            # TestSchedulerTargetInterpreter).
            python="/home/USER/venv/bin/python",
            queue_dir=queue,
            jobs_dir=jobs,
            scheduler_target="host_f",
        )
        assert _spec_for(queue, jid).scheduler_target == "host_f"

    def test_scheduler_tasks_recorded(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        src = tmp_path / "input.py"
        src.write_text("x")
        jid = submit_local(
            host="localhost",
            input_file=str(src),
            python="/home/USER/venv/bin/python",
            queue_dir=queue,
            jobs_dir=jobs,
            scheduler_target="host_c",
            scheduler_tasks=2,
        )
        assert _spec_for(queue, jid).scheduler_tasks == 2

    def test_scheduler_target_defaults_none(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        src = tmp_path / "input.py"
        src.write_text("x")
        jid = submit_local(
            host="localhost", input_file=str(src), queue_dir=queue, jobs_dir=jobs
        )
        assert _spec_for(queue, jid).scheduler_target is None

    def test_program_recorded(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        src = tmp_path / "input.py"
        src.write_text("x")
        jid = submit_local(
            host="localhost",
            input_file=str(src),
            queue_dir=queue,
            jobs_dir=jobs,
            program="orca",
        )
        assert _spec_for(queue, jid).program == "orca"

    def test_explicit_command_with_input_file_rejected(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        src = tmp_path / "input.py"
        src.write_text("")
        with pytest.raises(ValueError, match="single-file"):
            submit_local(
                host="localhost",
                input_file=str(src),
                command=["python", "other.py"],
                queue_dir=queue,
                jobs_dir=jobs,
            )

    def test_directory_in_single_file_slot_rejected_before_state_mutation(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        source = tmp_path / "input.py"
        source.mkdir()

        with pytest.raises(FileNotFoundError, match="input file not found"):
            submit_local(
                host="localhost",
                input_file=str(source),
                queue_dir=queue,
                jobs_dir=jobs,
            )

        assert not queue.exists()
        assert not jobs.exists()

    def test_symlink_in_single_file_slot_rejected_before_state_mutation(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        target = tmp_path / "real-input.py"
        target.write_text("pass\n")
        source = tmp_path / "input.py"
        source.symlink_to(target)

        with pytest.raises(FileNotFoundError, match="regular file"):
            submit_local(
                host="localhost",
                input_file=str(source),
                queue_dir=queue,
                jobs_dir=jobs,
            )

        assert not queue.exists()
        assert not jobs.exists()

    def test_submitter_recorded(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        src = tmp_path / "input.py"
        src.write_text("")
        jid = submit_local(
            host="localhost", input_file=str(src), queue_dir=queue, jobs_dir=jobs
        )
        spec = _spec_for(queue, jid)
        assert spec.submitter is not None
        assert socket.gethostname() in spec.submitter


class TestDirectorySubmit:
    @pytest.mark.parametrize(
        ("name", "target"),
        [("dangling", "missing"), ("loop", ".")],
    )
    def test_safe_relative_symlink_is_preserved_without_following(
        self,
        tmp_path: Path,
        state_dirs: tuple[Path, Path],
        name: str,
        target: str,
    ) -> None:
        queue, jobs = state_dirs
        source = tmp_path / "workspace_src"
        source.mkdir()
        (source / "run.py").write_text("pass\n")
        (source / name).symlink_to(target, target_is_directory=True)

        jobid = submit_local(
            host="localhost",
            directory=str(source),
            command=["python", "run.py"],
            queue_dir=queue,
            jobs_dir=jobs,
        )

        staged = jobs / jobid / name
        assert staged.is_symlink()
        assert os.readlink(staged) == target

    def test_invalid_model_value_rejected_before_state_mutation(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        source = tmp_path / "workspace_src"
        source.mkdir()
        (source / "run.py").write_text("pass\n")

        with pytest.raises(ValueError, match=r"cpus"):
            submit_local(
                host="localhost",
                directory=str(source),
                command=["python", "run.py"],
                cpus=0,
                queue_dir=queue,
                jobs_dir=jobs,
            )

        assert not queue.exists()
        assert not jobs.exists()

    def test_missing_dependency_rejected_before_state_mutation(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        source = tmp_path / "workspace_src"
        source.mkdir()
        (source / "run.py").write_text("pass\n")

        with pytest.raises(ValueError, match=r"no such job"):
            submit_local(
                host="localhost",
                directory=str(source),
                command=["python", "run.py"],
                depends_on=["0123456789ab"],
                queue_dir=queue,
                jobs_dir=jobs,
            )

        assert not queue.exists()
        assert not jobs.exists()

    def test_directory_contents_copied(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        src = tmp_path / "workspace_src"
        src.mkdir()
        (src / "run.py").write_text("print('go')")
        (src / "data").mkdir()
        (src / "data" / "x.txt").write_text("data")
        jid = submit_local(
            host="localhost",
            directory=str(src),
            command=["python", "run.py"],
            cpus=2,
            queue_dir=queue,
            jobs_dir=jobs,
        )
        spec = _spec_for(queue, jid)
        ws = Path(spec.cwd)
        assert (ws / "run.py").read_text() == "print('go')"
        assert (ws / "data" / "x.txt").read_text() == "data"
        assert spec.command == ["python", "run.py"]
        assert spec.cpus == 2

    def test_missing_python_entrypoint_rejected_before_state_mutation(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        src = tmp_path / "workspace_src"
        src.mkdir()
        (src / "batch-list.txt").write_text("case-001\n")

        with pytest.raises(
            ValueError,
            match=r"payload validation.*run_validation_batch\.py.*not present",
        ):
            submit_local(
                host="localhost",
                directory=str(src),
                command=["python", "run_validation_batch.py"],
                queue_dir=queue,
                jobs_dir=jobs,
            )

        assert not queue.exists()
        assert not jobs.exists()

    @pytest.mark.parametrize("entrypoint", ["run.py/", "run.py/.", "./run.py/."])
    @pytest.mark.parametrize("raw_head", [False, True], ids=["python", "raw-head"])
    def test_non_file_spelling_rejected_before_state_mutation(
        self,
        tmp_path: Path,
        state_dirs: tuple[Path, Path],
        entrypoint: str,
        raw_head: bool,
    ) -> None:
        queue, jobs = state_dirs
        source = tmp_path / "workspace_src"
        source.mkdir()
        (source / "run.py").write_text("pass\n")

        with pytest.raises(ValueError, match=r"payload validation.*relative path"):
            submit_local(
                host="localhost",
                directory=str(source),
                command=[entrypoint] if raw_head else ["python", entrypoint],
                queue_dir=queue,
                jobs_dir=jobs,
            )

        assert not queue.exists()
        assert not jobs.exists()

    def test_non_directory_member_with_terminal_dot_rejected_before_mutation(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        archive = tmp_path / "bundle.tar"
        with tarfile.open(archive, "w") as tf:
            _add_tar_bytes(tf, "run.py/.")

        with pytest.raises(ValueError, match=r"payload validation.*terminal"):
            submit_local(
                host="localhost",
                archive=str(archive),
                command=["python", "run.py"],
                queue_dir=queue,
                jobs_dir=jobs,
            )

        assert not queue.exists()
        assert not jobs.exists()

    @pytest.mark.parametrize(
        ("stored_name", "requested_name"),
        [
            ("run.py", "Run.py"),
            ("caf\N{LATIN SMALL LETTER E WITH ACUTE}.py", "cafe\N{COMBINING ACUTE ACCENT}.py"),
        ],
    )
    def test_entrypoint_requires_exact_staged_component_spelling(
        self,
        tmp_path: Path,
        state_dirs: tuple[Path, Path],
        stored_name: str,
        requested_name: str,
    ) -> None:
        queue, jobs = state_dirs
        source = tmp_path / "workspace_src"
        source.mkdir()
        (source / stored_name).write_text("pass\n")
        exact_stored_name = next(os.scandir(source)).name
        if exact_stored_name == requested_name:
            requested_name = stored_name

        with pytest.raises(ValueError, match=r"payload validation.*not present"):
            submit_local(
                host="localhost",
                directory=str(source),
                command=["python", requested_name],
                queue_dir=queue,
                jobs_dir=jobs,
            )

        assert not queue.exists()
        assert not jobs.exists()

    def test_symlinked_entrypoint_component_rejected_before_state_mutation(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        source = tmp_path / "workspace_src"
        source.mkdir()
        outside = tmp_path / "outside"
        outside.mkdir()
        (outside / "run.py").write_text("pass\n")
        (source / "nested").symlink_to(outside, target_is_directory=True)

        with pytest.raises(ValueError, match=r"payload validation.*symlink"):
            submit_local(
                host="localhost",
                directory=str(source),
                command=["python", "nested/run.py"],
                queue_dir=queue,
                jobs_dir=jobs,
            )

        assert not queue.exists()
        assert not jobs.exists()

    def test_unsafe_unrelated_directory_symlink_rejected_before_state_mutation(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        source = tmp_path / "workspace_src"
        source.mkdir()
        (source / "run.py").write_text("pass\n")
        (source / "outside-link").symlink_to("../../outside")

        with pytest.raises(
            PayloadValidationError,
            match=r"payload validation.*outside-link.*unsafe",
        ):
            submit_local(
                host="localhost",
                directory=str(source),
                command=["python", "run.py"],
                queue_dir=queue,
                jobs_dir=jobs,
            )

        assert not queue.exists()
        assert not jobs.exists()

    @pytest.mark.parametrize(
        "command",
        [
            ["/usr/bin/env", "python", "missing.py"],
            ["/usr/bin/env", "-uVQ_TOKEN", "python", "missing.py"],
            ["/usr/bin/env", "-iuVQ_TOKEN", "python", "missing.py"],
            ["/usr/bin/env", "-Ctmp", "python", "missing.py"],
            ["/usr/bin/env", "-P", "/usr/bin", "python", "missing.py"],
            ["/usr/bin/env", "--unset=VQ_TOKEN", "python", "missing.py"],
            ["/usr/bin/env", "--chdir=tmp", "python", "missing.py"],
            ["/usr/bin/env", "/usr/bin/env", "python", "missing.py"],
            [
                "/usr/bin/env",
                "-S",
                "/usr/bin/env python missing.py",
            ],
            ["/usr/bin/env", "=value", "python", "missing.py"],
            ["/usr/bin/env", "A/B=value", "python", "missing.py"],
            ["/usr/bin/env", "9VQ_TOKEN=value", "python", "missing.py"],
            [
                "/usr/bin/env",
                "--",
                "A.B=value",
                "python",
                "missing.py",
            ],
            [
                "/usr/bin/env",
                "--block-signal=PIPE",
                "python",
                "missing.py",
            ],
            ["python3.13t", "missing.py"],
        ],
    )
    def test_python_launcher_variants_require_staged_entrypoint(
        self,
        tmp_path: Path,
        state_dirs: tuple[Path, Path],
        command: list[str],
    ) -> None:
        queue, jobs = state_dirs
        source = tmp_path / "workspace_src"
        source.mkdir()

        with pytest.raises(
            ValueError,
            match=r"payload validation.*missing\.py.*not present",
        ):
            submit_local(
                host="localhost",
                directory=str(source),
                command=command,
                queue_dir=queue,
                jobs_dir=jobs,
            )

        assert not queue.exists()
        assert not jobs.exists()

    @pytest.mark.parametrize(
        "command",
        [
            ["/usr/bin/env", "VQ_TOKEN=value", "-i", "python", "missing.py"],
            ["/usr/bin/env", "VQ_TOKEN=value", "--", "python", "missing.py"],
            [
                "/usr/bin/env",
                "VQ_TOKEN=value",
                "-S",
                "python missing.py",
            ],
        ],
    )
    def test_env_options_after_assignment_are_command_operands(
        self,
        tmp_path: Path,
        state_dirs: tuple[Path, Path],
        command: list[str],
    ) -> None:
        queue, jobs = state_dirs
        source = tmp_path / "workspace_src"
        source.mkdir()

        jobid = submit_local(
            host="localhost",
            directory=str(source),
            command=command,
            queue_dir=queue,
            jobs_dir=jobs,
        )

        assert _spec_for(queue, jobid).command == command

    @pytest.mark.parametrize(
        "command",
        [
            ["/usr/bin/env", "-0", "python", "missing.py"],
            ["/usr/bin/env", "-i0", "python", "missing.py"],
            ["/usr/bin/env", "--null", "python", "missing.py"],
        ],
    )
    def test_env_null_output_option_cannot_launch_a_command(
        self,
        tmp_path: Path,
        state_dirs: tuple[Path, Path],
        command: list[str],
    ) -> None:
        queue, jobs = state_dirs
        source = tmp_path / "workspace_src"
        source.mkdir()

        jobid = submit_local(
            host="localhost",
            directory=str(source),
            command=command,
            queue_dir=queue,
            jobs_dir=jobs,
        )

        assert _spec_for(queue, jobid).command == command

    @pytest.mark.parametrize(
        "option",
        ["--chd=tmp", "--uns=VQ_TOKEN", "--argv=python", "--blo=PIPE"],
    )
    def test_env_abbreviated_long_option_fails_closed_before_mutation(
        self,
        tmp_path: Path,
        state_dirs: tuple[Path, Path],
        option: str,
    ) -> None:
        queue, jobs = state_dirs
        source = tmp_path / "workspace_src"
        source.mkdir()

        with pytest.raises(
            PayloadValidationError,
            match=r"unsupported /usr/bin/env option",
        ):
            submit_local(
                host="localhost",
                directory=str(source),
                command=["/usr/bin/env", option, "python", "missing.py"],
                queue_dir=queue,
                jobs_dir=jobs,
            )

        assert not queue.exists()
        assert not jobs.exists()

    @pytest.mark.parametrize(
        "command",
        [
            ["/usr/bin/env", "-S", r"python\_missing.py"],
            ["/usr/bin/env", "-iS", r"python\_missing.py"],
            [
                "/usr/bin/env",
                r"--split-string=python\_missing.py",
            ],
            ["/usr/bin/env", "-S", "VQ_TOKEN=value python missing.py"],
        ],
    )
    def test_env_split_string_python_requires_staged_entrypoint(
        self,
        tmp_path: Path,
        state_dirs: tuple[Path, Path],
        command: list[str],
    ) -> None:
        queue, jobs = state_dirs
        source = tmp_path / "workspace_src"
        source.mkdir()

        with pytest.raises(
            PayloadValidationError,
            match=r"payload validation.*missing\.py.*not present",
        ):
            submit_local(
                host="localhost",
                directory=str(source),
                command=command,
                queue_dir=queue,
                jobs_dir=jobs,
            )

        assert not queue.exists()
        assert not jobs.exists()

    @pytest.mark.parametrize("source_kind", ["directory", "archive"])
    @pytest.mark.parametrize("separator", [" ", "\t", "\n", "\r", "\v", "\f"])
    def test_env_split_string_backslash_space_requires_staged_entrypoint(
        self,
        tmp_path: Path,
        state_dirs: tuple[Path, Path],
        source_kind: str,
        separator: str,
    ) -> None:
        queue, jobs = state_dirs
        source = tmp_path / "workspace_src"
        source.mkdir()
        (source / f"python missing\\{separator}script.py").write_text("pass\n")
        kwargs: dict[str, str] = {}
        if source_kind == "directory":
            kwargs["directory"] = str(source)
        else:
            archive = tmp_path / "bundle.tar"
            with tarfile.open(archive, "w") as tf:
                tf.add(source, arcname=".")
            kwargs["archive"] = str(archive)

        with pytest.raises(
            PayloadValidationError,
            match=r"payload validation.*not present",
        ):
            submit_local(
                host="localhost",
                command=[
                    "/usr/bin/env",
                    "-S",
                    f"python missing\\{separator}script.py",
                ],
                queue_dir=queue,
                jobs_dir=jobs,
                **kwargs,
            )

        assert not queue.exists()
        assert not jobs.exists()

    @pytest.mark.parametrize("separator", ["\n", "\r", "\v", "\f"])
    def test_env_split_string_literal_ascii_whitespace_separates_arguments(
        self,
        tmp_path: Path,
        state_dirs: tuple[Path, Path],
        separator: str,
    ) -> None:
        queue, jobs = state_dirs
        source = tmp_path / "workspace_src"
        source.mkdir()
        (source / f"python{separator}missing.py").write_text("pass\n")

        with pytest.raises(
            PayloadValidationError,
            match=r"payload validation.*missing\.py.*not present",
        ):
            submit_local(
                host="localhost",
                directory=str(source),
                command=[
                    "/usr/bin/env",
                    "-S",
                    f"python{separator}missing.py",
                ],
                queue_dir=queue,
                jobs_dir=jobs,
            )

        assert not queue.exists()
        assert not jobs.exists()

    @pytest.mark.parametrize(
        ("value", "filename"),
        [
            (r"'python\nmissing.py'", r"python\nmissing.py"),
            (r"python\nmissing.py", "python\nmissing.py"),
        ],
    )
    def test_env_split_string_quoted_or_escaped_control_stays_in_one_argument(
        self,
        tmp_path: Path,
        state_dirs: tuple[Path, Path],
        value: str,
        filename: str,
    ) -> None:
        queue, jobs = state_dirs
        source = tmp_path / "workspace_src"
        source.mkdir()
        (source / filename).write_text("pass\n")
        command = ["/usr/bin/env", "-S", value]

        jobid = submit_local(
            host="localhost",
            directory=str(source),
            command=command,
            queue_dir=queue,
            jobs_dir=jobs,
        )

        assert _spec_for(queue, jobid).command == command

    def test_env_split_string_comment_does_not_invent_a_python_file(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        source = tmp_path / "workspace_src"
        source.mkdir()
        command = ["/usr/bin/env", "-S", "python # ignored.py"]

        jobid = submit_local(
            host="localhost",
            directory=str(source),
            command=command,
            queue_dir=queue,
            jobs_dir=jobs,
        )

        assert _spec_for(queue, jobid).command == command

    def test_env_split_string_target_substitution_fails_before_mutation(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        source = tmp_path / "workspace_src"
        source.mkdir()

        with pytest.raises(
            PayloadValidationError,
            match=r"env -S environment substitution.*target-dependent",
        ):
            submit_local(
                host="localhost",
                directory=str(source),
                command=[
                    "/usr/bin/env",
                    "-S",
                    "${VQ_PYTHON} missing.py",
                ],
                queue_dir=queue,
                jobs_dir=jobs,
            )

        assert not queue.exists()
        assert not jobs.exists()

    @pytest.mark.parametrize(
        "chdir_args",
        [
            ["-C", "nested"],
            ["-Cnested"],
            ["--chdir", "nested"],
            ["--chdir=nested"],
            ["-iCnested"],
            ["-S", "-C nested python run.py"],
        ],
    )
    def test_env_chdir_resolves_entrypoint_from_effective_working_directory(
        self,
        tmp_path: Path,
        state_dirs: tuple[Path, Path],
        chdir_args: list[str],
    ) -> None:
        queue, jobs = state_dirs
        source = tmp_path / "workspace_src"
        source.mkdir()
        (source / "run.py").write_text("pass\n")
        (source / "nested").mkdir()
        command = ["/usr/bin/env", *chdir_args]
        if "-S" not in chdir_args:
            command.extend(["python", "run.py"])

        with pytest.raises(
            PayloadValidationError,
            match=r"payload validation.*nested/run\.py.*not present",
        ):
            submit_local(
                host="localhost",
                directory=str(source),
                command=command,
                queue_dir=queue,
                jobs_dir=jobs,
            )

        assert not queue.exists()
        assert not jobs.exists()

    def test_env_chdir_accepts_entrypoint_in_effective_working_directory(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        source = tmp_path / "workspace_src"
        source.mkdir()
        nested = source / "nested"
        nested.mkdir()
        (nested / "run.py").write_text("pass\n")
        command = ["/usr/bin/env", "-C", "nested", "python", "run.py"]

        jobid = submit_local(
            host="localhost",
            directory=str(source),
            command=command,
            queue_dir=queue,
            jobs_dir=jobs,
        )

        assert _spec_for(queue, jobid).command == command

    def test_nested_env_chdir_composes_in_runtime_order(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        source = tmp_path / "workspace_src"
        (source / "a" / "b").mkdir(parents=True)
        (source / "a" / "b" / "run.py").write_text("pass\n")
        command = [
            "/usr/bin/env",
            "-C",
            "a",
            "/usr/bin/env",
            "-C",
            "b",
            "python",
            "run.py",
        ]

        jobid = submit_local(
            host="localhost",
            directory=str(source),
            command=command,
            queue_dir=queue,
            jobs_dir=jobs,
        )

        assert _spec_for(queue, jobid).command == command

    def test_nested_env_chdir_does_not_accept_reversed_path(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        source = tmp_path / "workspace_src"
        (source / "b" / "a").mkdir(parents=True)
        (source / "b" / "a" / "run.py").write_text("pass\n")

        with pytest.raises(
            PayloadValidationError,
            match=r"payload validation.*a/b/run\.py.*not present",
        ):
            submit_local(
                host="localhost",
                directory=str(source),
                command=[
                    "/usr/bin/env",
                    "-C",
                    "a",
                    "/usr/bin/env",
                    "-C",
                    "b",
                    "python",
                    "run.py",
                ],
                queue_dir=queue,
                jobs_dir=jobs,
            )

        assert not queue.exists()
        assert not jobs.exists()

    @pytest.mark.parametrize("working_directory", ["/tmp", "../outside"])
    def test_env_external_chdir_fails_closed_before_mutation(
        self,
        tmp_path: Path,
        state_dirs: tuple[Path, Path],
        working_directory: str,
    ) -> None:
        queue, jobs = state_dirs
        source = tmp_path / "workspace_src"
        source.mkdir()
        command = [
            "/usr/bin/env",
            "-C",
            working_directory,
            "python",
            "run.py",
        ]

        with pytest.raises(
            PayloadValidationError,
            match=r"payload validation.*working directory.*staged payload",
        ):
            submit_local(
                host="localhost",
                directory=str(source),
                command=command,
                queue_dir=queue,
                jobs_dir=jobs,
            )

        assert not queue.exists()
        assert not jobs.exists()

    def test_clustered_python_warning_option_consumes_argument(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        source = tmp_path / "workspace_src"
        source.mkdir()

        with pytest.raises(
            ValueError,
            match=r"payload validation.*missing\.py.*not present",
        ):
            submit_local(
                host="localhost",
                directory=str(source),
                command=["python", "-uW", "ignore", "missing.py"],
                queue_dir=queue,
                jobs_dir=jobs,
            )

        assert not queue.exists()
        assert not jobs.exists()

    @pytest.mark.parametrize(
        "command",
        [
            ["python", "-uc", "print('ok')", "missing.py"],
            ["python", "-um", "installed_module", "missing.py"],
            ["python", "-uV", "missing.py"],
        ],
    )
    def test_clustered_python_terminating_mode_has_no_staged_file(
        self,
        tmp_path: Path,
        state_dirs: tuple[Path, Path],
        command: list[str],
    ) -> None:
        queue, jobs = state_dirs
        source = tmp_path / "workspace_src"
        source.mkdir()

        jobid = submit_local(
            host="localhost",
            directory=str(source),
            command=command,
            queue_dir=queue,
            jobs_dir=jobs,
        )

        assert _spec_for(queue, jobid).command == command

    @pytest.mark.parametrize(
        "command",
        [
            ["python", "-"],
            ["python", "--", "-"],
            ["python", "-u", "--", "-"],
        ],
    )
    def test_python_stdin_mode_does_not_require_a_staged_file(
        self,
        tmp_path: Path,
        state_dirs: tuple[Path, Path],
        command: list[str],
    ) -> None:
        queue, jobs = state_dirs
        source = tmp_path / "workspace_src"
        source.mkdir()

        jobid = submit_local(
            host="localhost",
            directory=str(source),
            command=command,
            queue_dir=queue,
            jobs_dir=jobs,
        )

        assert _spec_for(queue, jobid).command == command

    @pytest.mark.parametrize(
        "option",
        ["-h", "--help", "-V", "-VV", "--version"],
    )
    def test_python_terminating_option_does_not_require_trailing_file(
        self,
        tmp_path: Path,
        state_dirs: tuple[Path, Path],
        option: str,
    ) -> None:
        queue, jobs = state_dirs
        source = tmp_path / "workspace_src"
        source.mkdir()
        command = ["python", option, "missing.py"]

        jobid = submit_local(
            host="localhost",
            directory=str(source),
            command=command,
            queue_dir=queue,
            jobs_dir=jobs,
        )

        assert _spec_for(queue, jobid).command == command

    def test_non_python_command_does_not_require_a_staged_entrypoint(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        src = tmp_path / "workspace_src"
        src.mkdir()
        (src / "geometry.xyz").write_text("0\n\n")

        jobid = submit_local(
            host="localhost",
            directory=str(src),
            command=["orca", "missing.inp"],
            queue_dir=queue,
            jobs_dir=jobs,
        )

        assert _spec_for(queue, jobid).command == ["orca", "missing.inp"]

    def test_orca_arguments_are_not_scanned_for_python_entrypoints(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        source = tmp_path / "workspace_src"
        source.mkdir()
        command = ["orca", "--label", "python", "missing.py"]

        jobid = submit_local(
            host="localhost",
            directory=str(source),
            command=command,
            queue_dir=queue,
            jobs_dir=jobs,
        )

        assert _spec_for(queue, jobid).command == command

    def test_python_module_mode_does_not_require_a_staged_file(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        src = tmp_path / "workspace_src"
        src.mkdir()

        jobid = submit_local(
            host="localhost",
            directory=str(src),
            command=["python", "-m", "installed_module"],
            queue_dir=queue,
            jobs_dir=jobs,
        )

        assert _spec_for(queue, jobid).command == [
            "python",
            "-m",
            "installed_module",
        ]

    def test_directory_without_command_rejected(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        src = tmp_path / "ws"
        src.mkdir()
        with pytest.raises(ValueError, match="--dir"):
            submit_local(
                host="localhost", directory=str(src), queue_dir=queue, jobs_dir=jobs
            )


class TestArchiveSubmit:
    def test_tarball_extracted(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        (src_dir / "run.sh").write_text("#!/bin/sh\necho ok\n")
        arc = tmp_path / "bundle.tar.gz"
        with tarfile.open(arc, "w:gz") as tf:
            tf.add(src_dir / "run.sh", arcname="run.sh")
        jid = submit_local(
            host="localhost",
            archive=str(arc),
            command=["bash", "run.sh"],
            queue_dir=queue,
            jobs_dir=jobs,
        )
        spec = _spec_for(queue, jid)
        assert (Path(spec.cwd) / "run.sh").read_text().startswith("#!/bin/sh")
        assert spec.command == ["bash", "run.sh"]

    def test_missing_python_entrypoint_rejected_before_state_mutation(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        arc = tmp_path / "bundle.tar.gz"
        batch_list = tmp_path / "batch-list.txt"
        batch_list.write_text("case-001\n")
        with tarfile.open(arc, "w:gz") as tf:
            tf.add(batch_list, arcname="batch-list.txt")

        with pytest.raises(
            ValueError,
            match=r"payload validation.*run_validation_batch\.py.*not present",
        ):
            submit_local(
                host="localhost",
                archive=str(arc),
                command=["python3", "-u", "run_validation_batch.py"],
                queue_dir=queue,
                jobs_dir=jobs,
            )

        assert not queue.exists()
        assert not jobs.exists()

    @pytest.mark.parametrize("entrypoint", ["run.py/", "run.py/.", "./run.py/."])
    @pytest.mark.parametrize("raw_head", [False, True], ids=["python", "raw-head"])
    def test_non_file_spelling_rejected_before_state_mutation(
        self,
        tmp_path: Path,
        state_dirs: tuple[Path, Path],
        entrypoint: str,
        raw_head: bool,
    ) -> None:
        queue, jobs = state_dirs
        archive = tmp_path / "bundle.tar"
        with tarfile.open(archive, "w") as tf:
            _add_tar_bytes(tf, "run.py")

        with pytest.raises(ValueError, match=r"payload validation.*relative path"):
            submit_local(
                host="localhost",
                archive=str(archive),
                command=[entrypoint] if raw_head else ["python", entrypoint],
                queue_dir=queue,
                jobs_dir=jobs,
            )

        assert not queue.exists()
        assert not jobs.exists()

    @pytest.mark.parametrize("member_type", [tarfile.SYMTYPE, tarfile.LNKTYPE])
    def test_link_entrypoint_rejected_before_state_mutation(
        self,
        tmp_path: Path,
        state_dirs: tuple[Path, Path],
        member_type: bytes,
    ) -> None:
        queue, jobs = state_dirs
        archive = tmp_path / "bundle.tar"
        member = tarfile.TarInfo("run.py")
        member.type = member_type
        member.linkname = "/outside/run.py"
        with tarfile.open(archive, "w") as tf:
            tf.addfile(member)

        with pytest.raises(
            ValueError,
            match=r"payload validation.*run\.py.*(?:regular file|unsafe)",
        ):
            submit_local(
                host="localhost",
                archive=str(archive),
                command=["python", "run.py"],
                queue_dir=queue,
                jobs_dir=jobs,
            )

        assert not queue.exists()
        assert not jobs.exists()

    def test_duplicate_entrypoint_rejected_before_state_mutation(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        archive = tmp_path / "bundle.tar"
        with tarfile.open(archive, "w") as tf:
            for contents in (b"print('first')\n", b"print('second')\n"):
                member = tarfile.TarInfo("run.py")
                member.size = len(contents)
                tf.addfile(member, io.BytesIO(contents))

        with pytest.raises(
            ValueError,
            match=r"payload validation.*run\.py.*ambiguous.*2 members",
        ):
            submit_local(
                host="localhost",
                archive=str(archive),
                command=["python", "run.py"],
                queue_dir=queue,
                jobs_dir=jobs,
            )

        assert not queue.exists()
        assert not jobs.exists()

    @pytest.mark.parametrize("equivalent_name", ["./run.py", "a/../run.py"])
    def test_extraction_equivalent_duplicate_entrypoint_rejected_before_mutation(
        self,
        tmp_path: Path,
        state_dirs: tuple[Path, Path],
        equivalent_name: str,
    ) -> None:
        queue, jobs = state_dirs
        archive = tmp_path / "bundle.tar"
        with tarfile.open(archive, "w") as tf:
            _add_tar_bytes(tf, "run.py", b"print('first')\n")
            _add_tar_bytes(tf, equivalent_name, b"print('second')\n")

        with pytest.raises(
            PayloadValidationError,
            match=r"payload validation.*ambiguous.*2 members.*run\.py",
        ):
            submit_local(
                host="localhost",
                archive=str(archive),
                command=["python", "run.py"],
                queue_dir=queue,
                jobs_dir=jobs,
            )

        assert not queue.exists()
        assert not jobs.exists()

    def test_unrelated_duplicate_archive_member_rejected_before_mutation(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        archive = tmp_path / "bundle.tar"
        with tarfile.open(archive, "w") as tf:
            _add_tar_bytes(tf, "run.py")
            _add_tar_bytes(tf, "data.txt", b"first\n")
            _add_tar_bytes(tf, "./data.txt", b"second\n")

        with pytest.raises(
            PayloadValidationError,
            match=r"payload validation.*data\.txt.*ambiguous.*2 members",
        ):
            submit_local(
                host="localhost",
                archive=str(archive),
                command=["python", "run.py"],
                queue_dir=queue,
                jobs_dir=jobs,
            )

        assert not queue.exists()
        assert not jobs.exists()

    @pytest.mark.parametrize(
        ("first", "second"),
        [
            ("results.txt", "RESULTS.TXT"),
            ("r\u00e9sults.txt", "re\u0301sults.txt"),
            ("Dir/first.txt", "dir/second.txt"),
        ],
    )
    def test_nonportable_archive_name_collision_rejected_before_mutation(
        self,
        tmp_path: Path,
        state_dirs: tuple[Path, Path],
        first: str,
        second: str,
    ) -> None:
        queue, jobs = state_dirs
        archive = tmp_path / "bundle.tar"
        with tarfile.open(archive, "w") as tf:
            _add_tar_bytes(tf, "run.py")
            _add_tar_bytes(tf, first, b"first\n")
            _add_tar_bytes(tf, second, b"second\n")

        with pytest.raises(
            PayloadValidationError,
            match=r"payload validation.*case/Unicode normalization",
        ):
            submit_local(
                host="localhost",
                archive=str(archive),
                command=["python", "run.py"],
                queue_dir=queue,
                jobs_dir=jobs,
            )

        assert not queue.exists()
        assert not jobs.exists()

    def test_symlinked_archive_entrypoint_ancestor_rejected_before_mutation(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        archive = tmp_path / "bundle.tar"
        with tarfile.open(archive, "w") as tf:
            real = tarfile.TarInfo("real")
            real.type = tarfile.DIRTYPE
            tf.addfile(real)
            alias = tarfile.TarInfo("alias")
            alias.type = tarfile.SYMTYPE
            alias.linkname = "real"
            tf.addfile(alias)
            _add_tar_bytes(tf, "alias/run.py")

        with pytest.raises(
            PayloadValidationError,
            match=r"payload validation.*alias/run\.py.*non-directory.*alias",
        ):
            submit_local(
                host="localhost",
                archive=str(archive),
                command=["python", "alias/run.py"],
                queue_dir=queue,
                jobs_dir=jobs,
            )

        assert not queue.exists()
        assert not jobs.exists()

    @pytest.mark.parametrize(
        ("name", "member_type", "linkname"),
        [
            ("../escape", tarfile.REGTYPE, None),
            ("results.pipe", tarfile.FIFOTYPE, None),
            ("escape-link", tarfile.SYMTYPE, "../../outside"),
            ("escape-hard", tarfile.LNKTYPE, "../outside"),
        ],
    )
    def test_unsafe_unrelated_member_rejected_before_state_mutation(
        self,
        tmp_path: Path,
        state_dirs: tuple[Path, Path],
        name: str,
        member_type: bytes,
        linkname: str | None,
    ) -> None:
        queue, jobs = state_dirs
        archive = tmp_path / "bundle.tar"
        with tarfile.open(archive, "w") as tf:
            _add_tar_bytes(tf, "run.py")
            member = tarfile.TarInfo(name)
            member.type = member_type
            if linkname is not None:
                member.linkname = linkname
            if member_type == tarfile.REGTYPE:
                contents = b"escape\n"
                member.size = len(contents)
                tf.addfile(member, io.BytesIO(contents))
            else:
                tf.addfile(member)

        with pytest.raises(
            PayloadValidationError,
            match=r"payload validation.*archive member",
        ):
            submit_local(
                host="localhost",
                archive=str(archive),
                command=["python", "run.py"],
                queue_dir=queue,
                jobs_dir=jobs,
            )

        assert not queue.exists()
        assert not jobs.exists()

    @pytest.mark.parametrize(
        "member_type",
        [tarfile.REGTYPE, tarfile.SYMTYPE, tarfile.LNKTYPE],
    )
    def test_normalized_duplicate_entrypoint_rejected_before_state_mutation(
        self,
        tmp_path: Path,
        state_dirs: tuple[Path, Path],
        member_type: bytes,
    ) -> None:
        queue, jobs = state_dirs
        archive = tmp_path / "bundle.tar"
        with tarfile.open(archive, "w") as tf:
            _add_tar_bytes(tf, "run.py", b"print('original')\n")
            member = tarfile.TarInfo("a/../run.py")
            member.type = member_type
            if member_type == tarfile.REGTYPE:
                contents = b"print('overwrite')\n"
                member.size = len(contents)
                tf.addfile(member, io.BytesIO(contents))
            else:
                member.linkname = "run.py"
                tf.addfile(member)

        with pytest.raises(
            PayloadValidationError,
            match=r"payload validation.*ambiguous.*2 members.*run\.py",
        ):
            submit_local(
                host="localhost",
                archive=str(archive),
                command=["python", "run.py"],
                queue_dir=queue,
                jobs_dir=jobs,
            )

        assert not queue.exists()
        assert not jobs.exists()

    def test_symlink_sensitive_archive_traversal_rejected_before_mutation(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        archive = tmp_path / "symlink-traversal.tar"
        with tarfile.open(archive, "w") as tf:
            alias = tarfile.TarInfo("alias")
            alias.type = tarfile.SYMTYPE
            alias.linkname = "."
            tf.addfile(alias)
            _add_tar_bytes(tf, "alias/../run.py")

        with pytest.raises(
            PayloadValidationError,
            match=r"payload validation.*archive member.*traversal",
        ):
            submit_local(
                host="localhost",
                archive=str(archive),
                command=["python", "run.py"],
                queue_dir=queue,
                jobs_dir=jobs,
            )

        assert not queue.exists()
        assert not jobs.exists()

    def test_symlink_sensitive_archive_duplicate_rejected_before_mutation(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        archive = tmp_path / "symlink-duplicate.tar"
        with tarfile.open(archive, "w") as tf:
            for name in ("b", "b/c"):
                directory = tarfile.TarInfo(name)
                directory.type = tarfile.DIRTYPE
                tf.addfile(directory)
            _add_tar_bytes(tf, "b/run.py", b"print('original')\n")
            alias = tarfile.TarInfo("alias")
            alias.type = tarfile.SYMTYPE
            alias.linkname = "b/c"
            tf.addfile(alias)
            _add_tar_bytes(tf, "alias/../run.py", b"print('overwrite')\n")

        with pytest.raises(
            PayloadValidationError,
            match=r"payload validation.*archive member.*traversal",
        ):
            submit_local(
                host="localhost",
                archive=str(archive),
                command=["python", "b/run.py"],
                queue_dir=queue,
                jobs_dir=jobs,
            )

        assert not queue.exists()
        assert not jobs.exists()

    def test_chained_relative_symlink_target_rejected_before_archive_mutation(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        archive = tmp_path / "chained-links.tar"
        with tarfile.open(archive, "w") as tf:
            directory = tarfile.TarInfo("dir")
            directory.type = tarfile.DIRTYPE
            tf.addfile(directory)
            first = tarfile.TarInfo("dir/link")
            first.type = tarfile.SYMTYPE
            first.linkname = ".."
            tf.addfile(first)
            second = tarfile.TarInfo("alias")
            second.type = tarfile.SYMTYPE
            second.linkname = "dir/link/../outside"
            tf.addfile(second)
            _add_tar_bytes(tf, "run.py")

        with pytest.raises(
            PayloadValidationError,
            match=r"payload validation.*link target.*parent traversal",
        ):
            submit_local(
                host="localhost",
                archive=str(archive),
                command=["python", "run.py"],
                queue_dir=queue,
                jobs_dir=jobs,
            )

        assert not queue.exists()
        assert not jobs.exists()

    def test_unresolved_archive_hardlink_rejected_before_state_mutation(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        archive = tmp_path / "unresolved-hardlink.tar"
        with tarfile.open(archive, "w") as tf:
            _add_tar_bytes(tf, "run.py")
            hardlink = tarfile.TarInfo("bad")
            hardlink.type = tarfile.LNKTYPE
            hardlink.linkname = "missing"
            tf.addfile(hardlink)

        with pytest.raises(
            PayloadValidationError,
            match=r"payload validation.*hardlink.*not supported",
        ):
            submit_local(
                host="localhost",
                archive=str(archive),
                command=["python", "run.py"],
                queue_dir=queue,
                jobs_dir=jobs,
            )

        assert not queue.exists()
        assert not jobs.exists()

    def test_empty_archive_symlink_rejected_before_state_mutation(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        archive = tmp_path / "empty-symlink.tar"
        with tarfile.open(archive, "w") as tf:
            _add_tar_bytes(tf, "run.py")
            symlink = tarfile.TarInfo("bad")
            symlink.type = tarfile.SYMTYPE
            symlink.linkname = ""
            tf.addfile(symlink)

        with pytest.raises(
            PayloadValidationError,
            match=r"payload validation.*link target.*empty",
        ):
            submit_local(
                host="localhost",
                archive=str(archive),
                command=["python", "run.py"],
                queue_dir=queue,
                jobs_dir=jobs,
            )

        assert not queue.exists()
        assert not jobs.exists()

    def test_chained_relative_directory_symlink_rejected_before_mutation(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        source = tmp_path / "payload"
        (source / "dir").mkdir(parents=True)
        (source / "run.py").write_text("pass\n")
        (source / "dir" / "link").symlink_to("..", target_is_directory=True)
        (source / "alias").symlink_to(
            "dir/link/../outside", target_is_directory=True
        )

        with pytest.raises(
            PayloadValidationError,
            match=r"payload validation.*link target.*parent traversal",
        ):
            submit_local(
                host="localhost",
                directory=str(source),
                command=["python", "run.py"],
                queue_dir=queue,
                jobs_dir=jobs,
            )

        assert not queue.exists()
        assert not jobs.exists()

    def test_non_python_archive_rejects_unsafe_member_before_state_mutation(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        archive = tmp_path / "orca-bundle.tar"
        with tarfile.open(archive, "w") as tf:
            _add_tar_bytes(tf, "job.inp", b"! HF\n")
            _add_tar_bytes(tf, "../escape", b"escape\n")

        with pytest.raises(PayloadValidationError, match="archive member"):
            submit_local(
                host="localhost",
                archive=str(archive),
                command=["orca", "job.inp"],
                queue_dir=queue,
                jobs_dir=jobs,
            )

        assert not queue.exists()
        assert not jobs.exists()

    def test_archive_without_command_rejected(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        arc = tmp_path / "empty.tar"
        with tarfile.open(arc, "w") as _:
            pass
        with pytest.raises(ValueError, match="--compressed"):
            submit_local(
                host="localhost", archive=str(arc), queue_dir=queue, jobs_dir=jobs
            )


class TestSourceValidation:
    def test_no_source_rejected(self, state_dirs: tuple[Path, Path]) -> None:
        queue, jobs = state_dirs
        with pytest.raises(ValueError, match="exactly one"):
            submit_local(host="localhost", queue_dir=queue, jobs_dir=jobs)

    def test_multiple_sources_rejected(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        f = tmp_path / "x.py"
        f.write_text("")
        d = tmp_path / "ws"
        d.mkdir()
        with pytest.raises(ValueError, match="exactly one"):
            submit_local(
                host="localhost",
                input_file=str(f),
                directory=str(d),
                queue_dir=queue,
                jobs_dir=jobs,
            )


class TestRemoteHost:
    def test_remote_host_rejected(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        f = tmp_path / "x.py"
        f.write_text("")
        with pytest.raises(NotImplementedError, match="remote submit"):
            submit_local(
                host="some.other.box",
                input_file=str(f),
                queue_dir=queue,
                jobs_dir=jobs,
            )


class TestPythonOverride:
    """--python overrides sys.executable for single-file submits."""

    def test_python_override_used_in_single_file_command(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        src = tmp_path / "input.py"
        src.write_text("print('hi')")
        custom_py = "/some/venv/bin/python"
        jid = submit_local(
            host="localhost",
            input_file=str(src),
            python=custom_py,
            queue_dir=queue,
            jobs_dir=jobs,
        )
        spec = _spec_for(queue, jid)
        assert spec.command == [custom_py, "input.py"]
        # Sanity: not the default
        assert spec.command[0] != sys.executable

    def test_python_none_falls_back_to_sys_executable(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        src = tmp_path / "input.py"
        src.write_text("")
        jid = submit_local(
            host="localhost",
            input_file=str(src),
            python=None,
            queue_dir=queue,
            jobs_dir=jobs,
        )
        spec = _spec_for(queue, jid)
        assert spec.command[0] == sys.executable

    def test_python_with_dir_prepends_the_interpreter(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        """Was rejected until 2026-08-01. Obeying that rejection on a scheduler
        host meant hardcoding the host's pinned wrapper path into the submit
        line, so sites used a command_wrapper hook instead -- and removing
        host_f's left its --dir submits launcherless."""
        queue, jobs = state_dirs
        d = tmp_path / "ws"
        d.mkdir()
        (d / "run.py").write_text("")

        jobid = submit_local(
            host="localhost",
            directory=str(d),
            command=["run.py"],
            python="/some/venv/bin/python",
            queue_dir=queue,
            jobs_dir=jobs,
        )

        spec = JobSpec.read(queue / f"{jobid}.json")
        assert spec.command == ["/some/venv/bin/python", "run.py"]

    def test_python_with_compressed_prepends_the_interpreter(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        ws = tmp_path / "ws"
        ws.mkdir()
        (ws / "run.py").write_text("")
        archive = tmp_path / "ws.tar"
        with tarfile.open(archive, "w") as t:
            t.add(ws / "run.py", arcname="run.py")
        jobid = submit_local(
            host="localhost",
            archive=str(archive),
            command=["run.py"],
            python="/some/venv/bin/python",
            queue_dir=queue,
            jobs_dir=jobs,
        )

        spec = JobSpec.read(queue / f"{jobid}.json")
        assert spec.command == ["/some/venv/bin/python", "run.py"]


class TestSchedulerTargetInterpreter:
    """A scheduler-target spec must never carry a driver-local program path.

    §17 scheduler dispatch ships ``spec.command`` verbatim into the generated
    qsub script (``scheduler_dispatch.build_job_script`` only *prepends* an
    optional ``scheduler_program_hooks.NAME.command_wrapper``). The driver runs
    on a different machine and often a different OS than the cluster, so the
    ``python or sys.executable`` default for a single-file submit would bake
    the driver's interpreter path into a command that can only fail remotely
    with ``FileNotFoundError`` naming a path from the driver's filesystem.
    Fail closed at submit time instead of queueing a doomed job.
    """

    def test_scheduler_target_single_file_without_python_fails_closed(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        src = tmp_path / "input.py"
        src.write_text("print('hi')")
        with pytest.raises(ValueError) as excinfo:
            submit_local(
                host="localhost",
                input_file=str(src),
                queue_dir=queue,
                jobs_dir=jobs,
                scheduler_target="host_f",
            )
        message = str(excinfo.value)
        # Actionable: names the scheduler host and the supported remedies,
        # and never leaks the driver-local interpreter as a usable value.
        assert "host_f" in message
        assert "--python" in message
        assert "scheduler_program_hooks" in message
        # Fail closed: nothing was queued.
        assert list(queue.glob("*.json")) == []

    def test_scheduler_target_single_file_with_explicit_python_allowed(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        src = tmp_path / "input.py"
        src.write_text("print('hi')")
        cluster_py = "/home/USER/.local/libexec/vq-host_f/runtimes/bin/python"
        jid = submit_local(
            host="localhost",
            input_file=str(src),
            python=cluster_py,
            queue_dir=queue,
            jobs_dir=jobs,
            scheduler_target="host_f",
        )
        spec = _spec_for(queue, jid)
        assert spec.command == [cluster_py, "input.py"]
        assert spec.scheduler_target == "host_f"

    def test_scheduler_target_directory_submit_unaffected(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        # --dir/--compressed already require an explicit (cluster-side)
        # command, so they never inject a driver-local path.
        queue, jobs = state_dirs
        d = tmp_path / "ws"
        d.mkdir()
        (d / "run.py").write_text("")
        jid = submit_local(
            host="localhost",
            directory=str(d),
            command=["/home/USER/bin/vibeqc-release-python", "run.py"],
            queue_dir=queue,
            jobs_dir=jobs,
            scheduler_target="host_f",
        )
        spec = _spec_for(queue, jid)
        assert spec.command == ["/home/USER/bin/vibeqc-release-python", "run.py"]

    def test_local_single_file_still_defaults_to_sys_executable(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        # Local/direct hosts are unchanged: the driver IS the execution host.
        queue, jobs = state_dirs
        src = tmp_path / "input.py"
        src.write_text("")
        jid = submit_local(
            host="localhost", input_file=str(src), queue_dir=queue, jobs_dir=jobs
        )
        assert _spec_for(queue, jid).command == [sys.executable, "input.py"]


class TestJobName:
    """v0.5.34: ``--job-name NAME`` flows through ``submit_local`` to
    the JobSpec field, which drives archive/fetch artifact naming."""

    def test_job_name_persisted_on_spec(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        script = tmp_path / "run.py"
        script.write_text("")
        jobid = submit_local(
            host="localhost",
            input_file=str(script),
            queue_dir=queue,
            jobs_dir=jobs,
            job_name="mgo-pbe-rev2",
        )
        spec = _spec_for(queue, jobid)
        assert spec.job_name == "mgo-pbe-rev2"
        # dest_dirname property uses it for fetch/archive paths.
        assert spec.dest_dirname == f"mgo-pbe-rev2-{jobid}"

    def test_no_job_name_keeps_field_none(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        """Pre-v0.5.34 behaviour preserved: no flag → no name → no
        change to dest_dirname (= jobid only)."""
        queue, jobs = state_dirs
        script = tmp_path / "run.py"
        script.write_text("")
        jobid = submit_local(
            host="localhost",
            input_file=str(script),
            queue_dir=queue,
            jobs_dir=jobs,
        )
        spec = _spec_for(queue, jobid)
        assert spec.job_name is None
        assert spec.dest_dirname == jobid

    def test_invalid_job_name_rejected_by_spec_validator(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        """Belt-and-braces: if a bad name gets past the CLI layer (e.g.
        a programmatic caller bypasses click), the JobSpec validator
        still catches it before the spec is written."""
        from pydantic import ValidationError
        queue, jobs = state_dirs
        script = tmp_path / "run.py"
        script.write_text("")
        with pytest.raises(ValidationError):
            submit_local(
                host="localhost",
                input_file=str(script),
                queue_dir=queue,
                jobs_dir=jobs,
                job_name="has space",
            )


class TestBranchCapture:
    """v0.5.47: submit_local writes the branch name onto spec.branch
    so vq admin update's provides_branches surgical pause can match."""

    def test_branch_kwarg_lands_on_spec(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        script = tmp_path / "run.py"
        script.write_text("print('hi')")
        jid = submit_local(
            host="localhost",
            input_file=str(script),
            queue_dir=queue,
            jobs_dir=jobs,
            branch="main",
        )
        spec = _spec_for(queue, jid)
        assert spec.branch == "main"

    def test_branch_default_is_none(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        """Submits without --branch leave spec.branch None — that's
        the explicit 'untagged' marker used by surgical-pause logic
        to skip jobs we can't classify."""
        queue, jobs = state_dirs
        script = tmp_path / "run.py"
        script.write_text("")
        jid = submit_local(
            host="localhost",
            input_file=str(script),
            queue_dir=queue,
            jobs_dir=jobs,
        )
        spec = _spec_for(queue, jid)
        assert spec.branch is None

    def test_branch_alias_preserved_verbatim(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        """The CLI does python-path resolution (alias → canonical)
        but stores whatever the user typed. provides_branches lists
        the names it accepts including aliases, so verbatim storage
        matches naturally."""
        queue, jobs = state_dirs
        script = tmp_path / "run.py"
        script.write_text("")
        jid = submit_local(
            host="localhost",
            input_file=str(script),
            queue_dir=queue,
            jobs_dir=jobs,
            branch="dev",
        )
        spec = _spec_for(queue, jid)
        assert spec.branch == "dev"


class TestTagsCapture:
    """v0.6.6: submit_local writes the tag list onto spec.tags
    (deduped + sorted at validate time)."""

    def test_tags_kwarg_lands_on_spec(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        script = tmp_path / "run.py"
        script.write_text("")
        jid = submit_local(
            host="localhost",
            input_file=str(script),
            queue_dir=queue,
            jobs_dir=jobs,
            tags=["experiment-12", "basisset-dev"],
        )
        spec = _spec_for(queue, jid)
        assert spec.tags == ["basisset-dev", "experiment-12"]

    def test_default_empty(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        script = tmp_path / "run.py"
        script.write_text("")
        jid = submit_local(
            host="localhost",
            input_file=str(script),
            queue_dir=queue,
            jobs_dir=jobs,
        )
        spec = _spec_for(queue, jid)
        assert spec.tags == []

    def test_tags_none_defaults_to_empty(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        script = tmp_path / "run.py"
        script.write_text("")
        jid = submit_local(
            host="localhost",
            input_file=str(script),
            queue_dir=queue,
            jobs_dir=jobs,
            tags=None,
        )
        spec = _spec_for(queue, jid)
        assert spec.tags == []


class TestNotBeforeCapture:
    """v0.6.12: submit_local writes the parsed ISO 8601 string onto
    spec.not_before so the daemon's existing dispatch-gate (from
    v0.5.31's retry-backoff path) holds the job until the deadline."""

    def test_not_before_kwarg_lands_on_spec(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        script = tmp_path / "run.py"
        script.write_text("")
        jid = submit_local(
            host="localhost",
            input_file=str(script),
            queue_dir=queue,
            jobs_dir=jobs,
            not_before="2026-05-20T22:00:00+00:00",
        )
        spec = _spec_for(queue, jid)
        assert spec.not_before == "2026-05-20T22:00:00+00:00"

    def test_not_before_default_is_none(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        queue, jobs = state_dirs
        script = tmp_path / "run.py"
        script.write_text("")
        jid = submit_local(
            host="localhost",
            input_file=str(script),
            queue_dir=queue,
            jobs_dir=jobs,
        )
        spec = _spec_for(queue, jid)
        assert spec.not_before is None

    def test_not_before_past_timestamp_accepted(
        self, tmp_path: Path, state_dirs: tuple[Path, Path]
    ) -> None:
        """submit_local doesn't reject past timestamps — the dispatch
        loop treats them as ready-now. This matches the v0.5.31
        retry-backoff semantics where a stale not_before is harmless."""
        queue, jobs = state_dirs
        script = tmp_path / "run.py"
        script.write_text("")
        jid = submit_local(
            host="localhost",
            input_file=str(script),
            queue_dir=queue,
            jobs_dir=jobs,
            not_before="2020-01-01T00:00:00+00:00",
        )
        spec = _spec_for(queue, jid)
        assert spec.not_before == "2020-01-01T00:00:00+00:00"


@pytest.mark.parametrize("head", ["--idempotency-key", "--cpus", "", "bad\x00name"])
def test_invalid_command_head_rejected_before_queue_creation(
    tmp_path: Path, state_dirs: tuple[Path, Path], head: str,
) -> None:
    queue, jobs = state_dirs
    source = tmp_path / "payload"
    source.mkdir()
    with pytest.raises(ValueError, match="command executable"):
        submit_local(
            host="localhost", directory=str(source),
            command=[head, "key", "bash", "-lc", "true"],
            queue_dir=queue, jobs_dir=jobs,
        )
    assert not queue.exists()
    assert not jobs.exists()
