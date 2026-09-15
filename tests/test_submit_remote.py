"""Tests for submit_remote: local tarball construction + remote vq invocation,
all with the actual SSH primitives mocked out."""
from __future__ import annotations

import contextlib
import io
import subprocess
import tarfile
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from vq import submit, transport
from vq.config import HostConfig


@dataclass
class TransportCalls:
    """Records what submit_remote asked transport to do, so tests can assert
    on the SSH/scp invocations without actually running them."""

    uploaded: list[tuple[Path, str]] = field(default_factory=list)
    """[(local_path, remote_path), ...]"""

    remote_vq: list[tuple[str, ...]] = field(default_factory=list)
    """vq_args of each run_remote_vq call."""

    remote_shell: list[tuple[str, ...]] = field(default_factory=list)
    """args of each run_remote_shell call."""


@pytest.fixture
def host_cfg() -> HostConfig:
    return HostConfig(ssh="host_d", remote_vq="vq", remote_python="/remote/py3")


@pytest.fixture
def calls(monkeypatch: pytest.MonkeyPatch) -> TransportCalls:
    """Stub out transport functions to record calls + return canned results."""
    rec = TransportCalls()

    def fake_upload(host_cfg: HostConfig, local: Path, remote: str) -> None:
        # Make sure the local tarball is a real, readable tar -- mirrors
        # what scp would care about.
        assert local.exists(), f"submit_remote uploaded missing file: {local}"
        with tarfile.open(local) as tf:
            tf.getmembers()  # raises if not a tar
        rec.uploaded.append((local, remote))

    def fake_run_remote_vq(
        host_cfg: HostConfig,
        *args: str,
        check: bool = True,
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        rec.remote_vq.append(args)
        # Canned response: a 12-hex jobid as if remote vq submit succeeded
        return subprocess.CompletedProcess(
            args=["ssh", host_cfg.ssh, host_cfg.remote_vq, *args],
            returncode=0,
            stdout="abc123def456\n",
            stderr="",
        )

    def fake_run_remote_shell(
        host_cfg: HostConfig,
        *args: str,
        check: bool = True,
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        rec.remote_shell.append(args)
        return subprocess.CompletedProcess(args=list(args), returncode=0, stdout="", stderr="")

    monkeypatch.setattr(transport, "upload_file", fake_upload)
    monkeypatch.setattr(transport, "run_remote_vq", fake_run_remote_vq)
    monkeypatch.setattr(transport, "run_remote_shell", fake_run_remote_shell)
    # Patch the bindings inside submit too (it imports the module, so this
    # is unnecessary, but explicit for safety against future refactors).
    monkeypatch.setattr(submit.transport, "upload_file", fake_upload)
    monkeypatch.setattr(submit.transport, "run_remote_vq", fake_run_remote_vq)
    monkeypatch.setattr(submit.transport, "run_remote_shell", fake_run_remote_shell)
    return rec


class TestSingleFileRemote:
    @pytest.mark.parametrize("key", [None, "remote-dependency-key"])
    @pytest.mark.parametrize("field", ["depends_on", "depends_on_any"])
    def test_invalid_dependency_is_rejected_before_temp_or_transport(
        self,
        tmp_path: Path,
        host_cfg: HostConfig,
        calls: TransportCalls,
        monkeypatch: pytest.MonkeyPatch,
        key: str | None,
        field: str,
    ) -> None:
        source = tmp_path / "input.py"
        source.write_text("pass\n")
        temp_calls: list[bool] = []
        monkeypatch.setattr(
            submit,
            "_make_temp_tar",
            lambda: temp_calls.append(True),
        )

        with pytest.raises(ValueError, match="invalid job id"):
            submit.submit_remote(
                host="host_d",
                host_cfg=host_cfg,
                input_file=str(source),
                idempotency_key=key,
                **{field: ["../outside"]},
            )

        assert temp_calls == []
        assert calls.uploaded == []
        assert calls.remote_shell == []
        assert calls.remote_vq == []

    def test_submit_receipt_capture_is_owned_and_bounded(
        self,
        tmp_path: Path,
        host_cfg: HostConfig,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        source = tmp_path / "input.py"
        source.write_text("pass\n")
        observed: dict[str, object] = {}

        monkeypatch.setattr(
            submit.transport,
            "upload_file",
            lambda *_args, **_kwargs: None,
        )
        monkeypatch.setattr(
            submit.transport,
            "run_remote_shell",
            lambda *_args, **_kwargs: subprocess.CompletedProcess(
                args=["ssh"], returncode=0, stdout="", stderr=""
            ),
        )

        def remote_vq(
            *_args: object,
            **kwargs: object,
        ) -> subprocess.CompletedProcess[str]:
            observed.update(kwargs)
            return subprocess.CompletedProcess(
                args=["ssh"],
                returncode=0,
                stdout="abc123def456\n",
                stderr="",
            )

        monkeypatch.setattr(submit.transport, "run_remote_vq", remote_vq)

        submit.submit_remote(
            host="host_d",
            host_cfg=host_cfg,
            input_file=str(source),
        )

        assert observed["owned_process_group"] is True
        assert observed["max_stdout_bytes"] == submit.REMOTE_SUBMIT_STDOUT_MAX_BYTES
        assert observed["max_stderr_bytes"] == submit.REMOTE_SUBMIT_STDERR_MAX_BYTES

    def test_invalid_model_value_precedes_temp_and_remote_mutation(
        self,
        tmp_path: Path,
        host_cfg: HostConfig,
        calls: TransportCalls,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        source = tmp_path / "input.py"
        source.write_text("pass\n")

        def no_temp_tar() -> Path:
            raise AssertionError("temporary archive allocated before validation")

        monkeypatch.setattr(submit, "_make_temp_tar", no_temp_tar)
        with pytest.raises(ValueError, match=r"cpus"):
            submit.submit_remote(
                host="host_d",
                host_cfg=host_cfg,
                input_file=str(source),
                cpus=0,
            )

        assert calls.uploaded == []
        assert calls.remote_vq == []
        assert calls.remote_shell == []

    def test_extended_hex_expected_sha_retains_receiver_authority(
        self,
        tmp_path: Path,
        host_cfg: HostConfig,
        calls: TransportCalls,
    ) -> None:
        source = tmp_path / "input.py"
        source.write_text("pass\n")
        expected_sha = "a" * 64

        submit.submit_remote(
            host="host_d",
            host_cfg=host_cfg,
            input_file=str(source),
            program="vibeqc",
            expected_sha=expected_sha,
        )

        (argv,) = calls.remote_vq
        assert argv[argv.index("--expected-sha") + 1] == expected_sha

    def test_uploads_tarball_and_invokes_remote_vq(
        self, tmp_path: Path, host_cfg: HostConfig, calls: TransportCalls
    ) -> None:
        src = tmp_path / "input.py"
        src.write_text("print('remote hi')")
        jid = submit.submit_remote(host="host_d", host_cfg=host_cfg, input_file=str(src))
        # v0.7.11: submit_remote now returns list[str] uniformly.
        assert jid == ["abc123def456"]

        # Exactly one upload + one remote vq call + one remote rm cleanup
        assert len(calls.uploaded) == 1
        local_tar, remote_tar = calls.uploaded[0]
        # 2026-07-16 host_c multi-login-node wedge: the staging tarball is
        # home-relative (shared home FS, visible from any login node), NOT under
        # node-local /tmp where a node hop between scp and the consuming ssh
        # loses it.
        assert remote_tar.startswith(".vq-upload-")
        assert remote_tar.endswith(".tar")
        assert not remote_tar.startswith("/")
        # The temp tar should have been deleted after the call
        assert not local_tar.exists()

        # The remote vq command must mirror: submit localhost -c <tar>
        # --cpus 1 -- /remote/py3 input.py
        (vq_args,) = calls.remote_vq
        assert vq_args[0:2] == ("submit", "localhost")
        assert "-c" in vq_args
        assert remote_tar in vq_args
        assert "--cpus" in vq_args
        assert "--" in vq_args
        # Command after `--` should be the resolved interp + basename
        sep_idx = vq_args.index("--")
        assert vq_args[sep_idx + 1 :] == ("/remote/py3", "input.py")

    def test_directory_in_single_file_slot_has_no_remote_mutation(
        self, tmp_path: Path, host_cfg: HostConfig, calls: TransportCalls
    ) -> None:
        source = tmp_path / "input.py"
        source.mkdir()

        with pytest.raises(FileNotFoundError, match="input file not found"):
            submit.submit_remote(
                host="host_d",
                host_cfg=host_cfg,
                input_file=str(source),
            )

        assert calls.uploaded == []
        assert calls.remote_vq == []
        assert calls.remote_shell == []

    def test_symlink_in_single_file_slot_has_no_remote_mutation(
        self, tmp_path: Path, host_cfg: HostConfig, calls: TransportCalls
    ) -> None:
        target = tmp_path / "real-input.py"
        target.write_text("pass\n")
        source = tmp_path / "input.py"
        source.symlink_to(target)

        with pytest.raises(FileNotFoundError, match="regular file"):
            submit.submit_remote(
                host="host_d",
                host_cfg=host_cfg,
                input_file=str(source),
            )

        assert calls.uploaded == []
        assert calls.remote_vq == []
        assert calls.remote_shell == []

    def test_explicit_python_overrides_host_remote_python(
        self, tmp_path: Path, host_cfg: HostConfig, calls: TransportCalls
    ) -> None:
        src = tmp_path / "input.py"
        src.write_text("")
        submit.submit_remote(
            host="host_d",
            host_cfg=host_cfg,
            input_file=str(src),
            python="/explicit/py",
        )
        (vq_args,) = calls.remote_vq
        sep_idx = vq_args.index("--")
        assert vq_args[sep_idx + 1] == "/explicit/py"

    def test_scheduler_target_forwarded(
        self, tmp_path: Path, host_cfg: HostConfig, calls: TransportCalls
    ) -> None:
        # §17: forwarding to a driver tags the remote `vq submit localhost` so
        # the driver's daemon dispatches the spec via SSH+qsub to the cluster.
        src = tmp_path / "input.py"
        src.write_text("x")
        submit.submit_remote(
            host="host_0",
            host_cfg=host_cfg,
            input_file=str(src),
            # Cluster-side interpreter: the driver's own is rejected (see
            # TestSchedulerTargetInterpreter).
            python="/cluster/bin/python",
            scheduler_target="host_f",
        )
        (vq_args,) = calls.remote_vq
        assert "--scheduler-target" in vq_args
        assert vq_args[vq_args.index("--scheduler-target") + 1] == "host_f"

    def test_scheduler_tasks_forwarded(
        self, tmp_path: Path, host_cfg: HostConfig, calls: TransportCalls
    ) -> None:
        src = tmp_path / "input.py"
        src.write_text("x")
        submit.submit_remote(
            host="host_c-driver",
            host_cfg=host_cfg,
            input_file=str(src),
            python="/cluster/bin/python",
            scheduler_target="host_c",
            scheduler_tasks=2,
        )
        (vq_args,) = calls.remote_vq
        assert "--scheduler-tasks" in vq_args
        assert vq_args[vq_args.index("--scheduler-tasks") + 1] == "2"

    def test_scheduler_target_rejects_driver_remote_python(
        self, tmp_path: Path, host_cfg: HostConfig, calls: TransportCalls
    ) -> None:
        # §17: with a scheduler_target, host_cfg is the DRIVER's config, so
        # its remote_python ("/remote/py3") is a driver-local path. Injecting
        # it would qsub a command the compute node cannot execute.
        src = tmp_path / "input.py"
        src.write_text("x")
        with pytest.raises(ValueError) as excinfo:
            submit.submit_remote(
                host="host_0",
                host_cfg=host_cfg,
                input_file=str(src),
                scheduler_target="host_f",
            )
        message = str(excinfo.value)
        assert "host_f" in message
        assert "--python" in message
        # Fail closed before anything is uploaded or run remotely.
        assert calls.uploaded == []
        assert calls.remote_vq == []

    def test_scheduler_target_allows_bare_interpreter_name(
        self, tmp_path: Path, calls: TransportCalls
    ) -> None:
        # A bare name is a PATH lookup on the cluster, not a driver-local
        # path, so it stays allowed.
        src = tmp_path / "input.py"
        src.write_text("x")
        cfg = HostConfig(ssh="host_0", remote_vq="vq", remote_python=None)
        submit.submit_remote(
            host="host_0",
            host_cfg=cfg,
            input_file=str(src),
            scheduler_target="host_f",
        )
        (vq_args,) = calls.remote_vq
        sep_idx = vq_args.index("--")
        assert vq_args[sep_idx + 1 :] == ("python", "input.py")

    def test_no_scheduler_target_still_uses_host_remote_python(
        self, tmp_path: Path, host_cfg: HostConfig, calls: TransportCalls
    ) -> None:
        # Ordinary remote-daemon hosts are unaffected: remote_python is a path
        # on the machine that actually runs the job.
        src = tmp_path / "input.py"
        src.write_text("x")
        submit.submit_remote(host="host_d", host_cfg=host_cfg, input_file=str(src))
        (vq_args,) = calls.remote_vq
        sep_idx = vq_args.index("--")
        assert vq_args[sep_idx + 1 :] == ("/remote/py3", "input.py")

    def test_no_scheduler_target_omits_flag(
        self, tmp_path: Path, host_cfg: HostConfig, calls: TransportCalls
    ) -> None:
        src = tmp_path / "input.py"
        src.write_text("x")
        submit.submit_remote(host="host_d", host_cfg=host_cfg, input_file=str(src))
        (vq_args,) = calls.remote_vq
        assert "--scheduler-target" not in vq_args

    def test_program_forwarded(
        self, tmp_path: Path, host_cfg: HostConfig, calls: TransportCalls
    ) -> None:
        src = tmp_path / "input.py"
        src.write_text("x")
        submit.submit_remote(
            host="host_d",
            host_cfg=host_cfg,
            input_file=str(src),
            program="orca",
        )
        (vq_args,) = calls.remote_vq
        assert "--program" in vq_args
        assert vq_args[vq_args.index("--program") + 1] == "orca"

    def test_expected_sha_forwarded_for_directory_payload(
        self, tmp_path: Path, host_cfg: HostConfig, calls: TransportCalls
    ) -> None:
        src_dir = tmp_path / "docs-payload"
        src_dir.mkdir()
        (src_dir / "run_docs.sh").write_text("echo docs\n")
        submit.submit_remote(
            host="host_d",
            host_cfg=host_cfg,
            directory=str(src_dir),
            command=["bash", "run_docs.sh"],
            program="vibeqc-dev",
            expected_sha="abc123def456",
        )
        (vq_args,) = calls.remote_vq
        assert "--program" in vq_args
        assert vq_args[vq_args.index("--program") + 1] == "vibeqc-dev"
        assert "--expected-sha" in vq_args
        assert vq_args[vq_args.index("--expected-sha") + 1] == "abc123def456"
        sep_idx = vq_args.index("--")
        assert vq_args[sep_idx + 1 :] == ("bash", "run_docs.sh")

    def test_not_before_forwarded_via_at_flag(
        self, tmp_path: Path, host_cfg: HostConfig, calls: TransportCalls
    ) -> None:
        """v0.6.12: parsed --at ISO 8601 string forwarded to the
        remote vq's --at flag so the remote spec carries the same
        not_before value."""
        src = tmp_path / "input.py"
        src.write_text("")
        submit.submit_remote(
            host="host_d",
            host_cfg=host_cfg,
            input_file=str(src),
            not_before="2026-05-20T22:00:00+00:00",
        )
        (vq_args,) = calls.remote_vq
        assert "--at" in vq_args
        at_idx = vq_args.index("--at")
        assert vq_args[at_idx + 1] == "2026-05-20T22:00:00+00:00"

    def test_no_not_before_omits_at_flag(
        self, tmp_path: Path, host_cfg: HostConfig, calls: TransportCalls
    ) -> None:
        src = tmp_path / "input.py"
        src.write_text("")
        submit.submit_remote(
            host="host_d", host_cfg=host_cfg, input_file=str(src)
        )
        (vq_args,) = calls.remote_vq
        assert "--at" not in vq_args

    def test_falls_back_to_python_string_when_no_remote_python_configured(
        self, tmp_path: Path, calls: TransportCalls
    ) -> None:
        host_cfg = HostConfig(ssh="x", remote_vq="vq")  # no remote_python
        src = tmp_path / "input.py"
        src.write_text("")
        submit.submit_remote(host="x", host_cfg=host_cfg, input_file=str(src))
        (vq_args,) = calls.remote_vq
        sep_idx = vq_args.index("--")
        assert vq_args[sep_idx + 1] == "python"

    def test_explicit_command_with_input_file_rejected(
        self, tmp_path: Path, host_cfg: HostConfig, calls: TransportCalls
    ) -> None:
        src = tmp_path / "input.py"
        src.write_text("")
        with pytest.raises(ValueError, match="single-file submit"):
            submit.submit_remote(
                host="host_d",
                host_cfg=host_cfg,
                input_file=str(src),
                command=["python", "input.py"],
            )

    def test_remote_tar_cleanup_called(
        self, tmp_path: Path, host_cfg: HostConfig, calls: TransportCalls
    ) -> None:
        src = tmp_path / "x.py"
        src.write_text("")
        submit.submit_remote(host="host_d", host_cfg=host_cfg, input_file=str(src))
        # rm -f .vq-upload-... was called on the home-relative staging tarball
        assert any(args[0] == "rm" and args[1] == "-f" for args in calls.remote_shell)

    def test_local_tar_cleaned_even_if_upload_fails(
        self,
        tmp_path: Path,
        host_cfg: HostConfig,
        monkeypatch: pytest.MonkeyPatch,
        calls: TransportCalls,
    ) -> None:
        # Override fake_upload to fail
        def boom(host_cfg: HostConfig, local: Path, remote: str) -> None:
            calls.uploaded.append((local, remote))
            raise transport.RemoteError("scp blew up")

        monkeypatch.setattr(submit.transport, "upload_file", boom)

        src = tmp_path / "x.py"
        src.write_text("")
        with pytest.raises(transport.RemoteError, match="scp blew up"):
            submit.submit_remote(host="host_d", host_cfg=host_cfg, input_file=str(src))
        # The temp tar should still have been deleted
        local_tar, _ = calls.uploaded[0]
        assert not local_tar.exists()


class TestDirectoryRemote:
    def test_packs_dir_contents_and_passes_explicit_command(
        self, tmp_path: Path, host_cfg: HostConfig, calls: TransportCalls
    ) -> None:
        d = tmp_path / "ws"
        d.mkdir()
        (d / "run.py").write_text("print('x')")
        (d / "data.txt").write_text("hello")

        jid = submit.submit_remote(
            host="host_d",
            host_cfg=host_cfg,
            directory=str(d),
            command=["python", "run.py"],
        )
        # v0.7.11: submit_remote now returns list[str] uniformly.
        assert jid == ["abc123def456"]

        # The uploaded tar should contain run.py + data.txt at the top level
        # (we deleted it post-upload, but we already validated it was a real
        # tar in the fake_upload fixture; here we check the remote_command).
        (vq_args,) = calls.remote_vq
        sep_idx = vq_args.index("--")
        assert vq_args[sep_idx + 1 :] == ("python", "run.py")

    def test_missing_scheduler_python_entrypoint_has_no_remote_mutation(
        self, tmp_path: Path, host_cfg: HostConfig, calls: TransportCalls
    ) -> None:
        source = tmp_path / "paper-payload"
        source.mkdir()
        (source / "batch-list.txt").write_text("case-001\n")

        with pytest.raises(
            ValueError,
            match=r"payload validation.*run_validation_batch\.py.*not present",
        ):
            submit.submit_remote(
                host="driver",
                host_cfg=host_cfg,
                directory=str(source),
                command=[
                    "/cluster/runtime/vibeqc-release-python",
                    "run_validation_batch.py",
                ],
                scheduler_target="host_f",
            )

        assert calls.uploaded == []
        assert calls.remote_vq == []
        assert calls.remote_shell == []

    def test_symlinked_entrypoint_component_has_no_remote_mutation(
        self, tmp_path: Path, host_cfg: HostConfig, calls: TransportCalls
    ) -> None:
        source = tmp_path / "paper-payload"
        source.mkdir()
        outside = tmp_path / "outside"
        outside.mkdir()
        (outside / "run.py").write_text("pass\n")
        (source / "nested").symlink_to(outside, target_is_directory=True)

        with pytest.raises(ValueError, match=r"payload validation.*symlink"):
            submit.submit_remote(
                host="driver",
                host_cfg=host_cfg,
                directory=str(source),
                command=["python", "nested/run.py"],
            )

        assert calls.uploaded == []
        assert calls.remote_vq == []
        assert calls.remote_shell == []

    def test_unsafe_unrelated_symlink_has_no_temp_or_remote_mutation(
        self,
        tmp_path: Path,
        host_cfg: HostConfig,
        calls: TransportCalls,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        source = tmp_path / "paper-payload"
        source.mkdir()
        (source / "run.py").write_text("pass\n")
        (source / "outside-link").symlink_to("/outside/payload")

        def no_temp_tar() -> Path:
            pytest.fail("unsafe directory payload reached tempfile creation")

        monkeypatch.setattr(submit, "_make_temp_tar", no_temp_tar)
        with pytest.raises(
            submit.PayloadValidationError,
            match=r"payload validation.*outside-link.*unsafe",
        ):
            submit.submit_remote(
                host="driver",
                host_cfg=host_cfg,
                directory=str(source),
                command=["python", "run.py"],
            )

        assert calls.uploaded == []
        assert calls.remote_vq == []
        assert calls.remote_shell == []

    def test_chained_relative_symlink_has_no_temp_or_remote_mutation(
        self,
        tmp_path: Path,
        host_cfg: HostConfig,
        calls: TransportCalls,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        source = tmp_path / "paper-payload"
        (source / "dir").mkdir(parents=True)
        (source / "run.py").write_text("pass\n")
        (source / "dir" / "link").symlink_to("..", target_is_directory=True)
        (source / "alias").symlink_to(
            "dir/link/../outside", target_is_directory=True
        )

        def no_temp_tar() -> Path:
            pytest.fail("unsafe directory payload reached tempfile creation")

        monkeypatch.setattr(submit, "_make_temp_tar", no_temp_tar)
        with pytest.raises(
            submit.PayloadValidationError,
            match=r"payload validation.*link target.*parent traversal",
        ):
            submit.submit_remote(
                host="driver",
                host_cfg=host_cfg,
                directory=str(source),
                command=["python", "run.py"],
            )

        assert calls.uploaded == []
        assert calls.remote_vq == []
        assert calls.remote_shell == []

    def test_python_with_dir_prepends_the_interpreter(
        self, tmp_path: Path, host_cfg: HostConfig, calls: TransportCalls
    ) -> None:
        """Was rejected until 2026-08-01; see submit._payload_command."""
        d = tmp_path / "ws"
        d.mkdir()
        (d / "run.py").write_text("")

        submit.submit_remote(
            host="host_d",
            host_cfg=host_cfg,
            directory=str(d),
            command=["run.py"],
            python="/some/py",
        )

        (vq_args,) = calls.remote_vq
        sep_idx = vq_args.index("--")
        assert vq_args[sep_idx + 1 :] == ("/some/py", "run.py")

    def test_dir_without_command_rejected(
        self, tmp_path: Path, host_cfg: HostConfig, calls: TransportCalls
    ) -> None:
        d = tmp_path / "ws"
        d.mkdir()
        with pytest.raises(ValueError, match="--dir submit requires"):
            submit.submit_remote(
                host="host_d", host_cfg=host_cfg, directory=str(d)
            )


class TestArchiveRemote:
    def test_uploads_users_tarball_unchanged_and_does_not_delete_it(
        self, tmp_path: Path, host_cfg: HostConfig, calls: TransportCalls
    ) -> None:
        ws = tmp_path / "ws"
        ws.mkdir()
        (ws / "run.py").write_text("print('x')")
        archive = tmp_path / "ws.tar"
        with tarfile.open(archive, "w") as tf:
            tf.add(ws / "run.py", arcname="run.py")

        jid = submit.submit_remote(
            host="host_d",
            host_cfg=host_cfg,
            archive=str(archive),
            command=["bash", "run.py"],
        )
        # v0.7.11: submit_remote now returns list[str] uniformly.
        assert jid == ["abc123def456"]

        local_tar, _ = calls.uploaded[0]
        # User's tarball must not have been deleted
        assert local_tar == archive
        assert archive.exists()

    def test_python_with_archive_prepends_the_interpreter(
        self, tmp_path: Path, host_cfg: HostConfig, calls: TransportCalls
    ) -> None:
        """Was rejected until 2026-08-01; see submit._payload_command."""
        archive = tmp_path / "x.tar"
        entrypoint = tmp_path / "x"
        entrypoint.write_text("pass\n")
        with tarfile.open(archive, "w") as tf:
            tf.add(entrypoint, arcname="x")

        submit.submit_remote(
            host="host_d",
            host_cfg=host_cfg,
            archive=str(archive),
            command=["x"],
            python="/some/py",
        )

        (vq_args,) = calls.remote_vq
        sep_idx = vq_args.index("--")
        assert vq_args[sep_idx + 1 :] == ("/some/py", "x")

    def test_unsafe_archive_member_has_no_remote_mutation(
        self, tmp_path: Path, host_cfg: HostConfig, calls: TransportCalls
    ) -> None:
        archive = tmp_path / "unsafe.tar"
        with tarfile.open(archive, "w") as tf:
            entrypoint = tarfile.TarInfo("run.py")
            entrypoint_contents = b"pass\n"
            entrypoint.size = len(entrypoint_contents)
            tf.addfile(entrypoint, io.BytesIO(entrypoint_contents))
            escape = tarfile.TarInfo("../escape")
            escape_contents = b"escape\n"
            escape.size = len(escape_contents)
            tf.addfile(escape, io.BytesIO(escape_contents))

        with pytest.raises(
            submit.PayloadValidationError,
            match="archive member",
        ):
            submit.submit_remote(
                host="host_d",
                host_cfg=host_cfg,
                archive=str(archive),
                command=["python", "run.py"],
            )

        assert calls.uploaded == []
        assert calls.remote_vq == []
        assert calls.remote_shell == []


class TestRemoteVQOutputValidation:
    @pytest.mark.parametrize(
        "warning_failure",
        [
            RuntimeError("broken warning parser"),
            KeyboardInterrupt("warning parsing interrupted"),
        ],
        ids=["exception", "interrupt"],
    )
    def test_valid_receipt_survives_warning_parser_failure(
        self,
        tmp_path: Path,
        host_cfg: HostConfig,
        monkeypatch: pytest.MonkeyPatch,
        warning_failure: BaseException,
    ) -> None:
        monkeypatch.setattr(
            submit.transport,
            "upload_file",
            lambda *_args, **_kwargs: None,
        )
        monkeypatch.setattr(
            submit.transport,
            "run_remote_vq",
            lambda *_args, **_kwargs: subprocess.CompletedProcess(
                ["ssh"], 0, "abc123def456\n", "advisory line\n"
            ),
        )
        monkeypatch.setattr(
            submit.transport,
            "run_remote_shell",
            lambda *_args, **_kwargs: subprocess.CompletedProcess(
                ["ssh"], 0, "", ""
            ),
        )
        monkeypatch.setattr(
            submit,
            "_remote_impossible_capacity_warning",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                warning_failure
            ),
        )
        source = tmp_path / "input.py"
        source.write_text("pass\n")

        assert submit.submit_remote(
            host="host_d",
            host_cfg=host_cfg,
            input_file=str(source),
            warning_sink=lambda _message: None,
        ) == ["abc123def456"]

    def test_valid_receipt_survives_warning_sink_failure(
        self,
        tmp_path: Path,
        host_cfg: HostConfig,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(
            submit.transport,
            "upload_file",
            lambda *_args, **_kwargs: None,
        )
        monkeypatch.setattr(
            submit.transport,
            "run_remote_vq",
            lambda *_args, **_kwargs: subprocess.CompletedProcess(
                ["ssh"],
                0,
                "abc123def456\n",
                "vq: warning: requested 32 CPUs but this daemon caps at "
                "--max-cpus 16; job abc123def456 will park PENDING until "
                "the daemon is restarted with a higher cap\n",
            ),
        )
        monkeypatch.setattr(
            submit.transport,
            "run_remote_shell",
            lambda *_args, **_kwargs: subprocess.CompletedProcess(
                ["ssh"], 0, "", ""
            ),
        )
        source = tmp_path / "input.py"
        source.write_text("pass\n")

        jobids = submit.submit_remote(
            host="host_d",
            host_cfg=host_cfg,
            input_file=str(source),
            warning_sink=lambda _message: (_ for _ in ()).throw(
                RuntimeError("broken warning consumer")
            ),
        )

        assert jobids == ["abc123def456"]

    def test_valid_receipt_survives_cleanup_interrupt(
        self,
        tmp_path: Path,
        host_cfg: HostConfig,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        uploaded: list[Path] = []
        monkeypatch.setattr(
            submit.transport,
            "upload_file",
            lambda _cfg, local, _remote: uploaded.append(local),
        )
        monkeypatch.setattr(
            submit.transport,
            "run_remote_vq",
            lambda *_args, **_kwargs: subprocess.CompletedProcess(
                ["ssh"], 0, "abc123def456\n", ""
            ),
        )
        monkeypatch.setattr(
            submit.transport,
            "run_remote_shell",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                KeyboardInterrupt("cleanup interrupted")
            ),
        )
        source = tmp_path / "input.py"
        source.write_text("pass\n")

        jobids = submit.submit_remote(
            host="host_d",
            host_cfg=host_cfg,
            input_file=str(source),
        )

        assert jobids == ["abc123def456"]
        assert not uploaded[0].exists()

    def test_broken_logger_cannot_mask_accepted_receipt(
        self,
        tmp_path: Path,
        host_cfg: HostConfig,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        uploaded: list[Path] = []
        monkeypatch.setattr(
            submit.transport,
            "upload_file",
            lambda _cfg, local, _remote: uploaded.append(local),
        )
        monkeypatch.setattr(
            submit.transport,
            "run_remote_vq",
            lambda *_args, **_kwargs: subprocess.CompletedProcess(
                ["ssh"], 0, "abc123def456\n", ""
            ),
        )
        monkeypatch.setattr(
            submit.transport,
            "run_remote_shell",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                KeyboardInterrupt("cleanup interrupted")
            ),
        )
        monkeypatch.setattr(
            submit.log,
            "warning",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                RuntimeError("logging handler failed")
            ),
        )
        source = tmp_path / "input.py"
        source.write_text("pass\n")

        assert submit.submit_remote(
            host="host_d",
            host_cfg=host_cfg,
            input_file=str(source),
        ) == ["abc123def456"]
        assert not uploaded[0].exists()

    def test_temp_tar_build_failure_removes_local_temp_without_transport(
        self,
        tmp_path: Path,
        host_cfg: HostConfig,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        allocated = tmp_path / "allocated.tar"
        transport_calls: list[str] = []
        monkeypatch.setattr(
            submit,
            "_make_temp_tar",
            lambda: allocated.touch() or allocated,
        )
        monkeypatch.setattr(
            tarfile.TarFile,
            "add",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                OSError("simulated tar construction failure")
            ),
        )
        monkeypatch.setattr(
            submit.transport,
            "upload_file",
            lambda *_args, **_kwargs: transport_calls.append("upload"),
        )
        source = tmp_path / "input.py"
        source.write_text("pass\n")

        with pytest.raises(OSError, match="tar construction failure"):
            submit.submit_remote(
                host="host_d",
                host_cfg=host_cfg,
                input_file=str(source),
            )

        assert not allocated.exists()
        assert transport_calls == []

    def test_remote_path_generation_failure_precedes_temp_allocation(
        self,
        tmp_path: Path,
        host_cfg: HostConfig,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        allocated = tmp_path / "allocated.tar"
        transport_calls: list[str] = []
        monkeypatch.setattr(
            submit,
            "_make_temp_tar",
            lambda: allocated.touch() or allocated,
        )
        monkeypatch.setattr(
            submit.transport,
            "remote_temp_tar_path",
            lambda: (_ for _ in ()).throw(
                RuntimeError("simulated remote path generation failure")
            ),
        )
        monkeypatch.setattr(
            submit.transport,
            "upload_file",
            lambda *_args, **_kwargs: transport_calls.append("upload"),
        )
        source = tmp_path / "input.py"
        source.write_text("pass\n")

        with pytest.raises(RuntimeError, match="path generation failure"):
            submit.submit_remote(
                host="host_d",
                host_cfg=host_cfg,
                input_file=str(source),
            )

        assert not allocated.exists()
        assert transport_calls == []

    def test_qvf_housekeeping_is_owned_and_bounded(
        self,
        tmp_path: Path,
        host_cfg: HostConfig,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        observed: list[dict[str, object]] = []

        def shell(
            _cfg: HostConfig,
            *_args: str,
            **kwargs: object,
        ) -> subprocess.CompletedProcess[str]:
            observed.append(kwargs)
            return subprocess.CompletedProcess(["ssh"], 0, "", "")

        monkeypatch.setattr(submit.transport, "run_remote_shell", shell)
        monkeypatch.setattr(
            submit.transport,
            "upload_file",
            lambda *_args, **_kwargs: None,
        )
        monkeypatch.setattr(
            submit.transport,
            "run_remote_vq",
            lambda *_args, **_kwargs: subprocess.CompletedProcess(
                ["ssh"], 0, "abc123def456\n", ""
            ),
        )
        source = tmp_path / "job.qvf"
        source.write_bytes(b"qvf")

        assert submit.submit_remote(
            host="host_d",
            host_cfg=host_cfg,
            input_file=str(source),
        ) == ["abc123def456"]

        assert len(observed) == 3
        assert all(item.get("owned_process_group") is True for item in observed)
        assert all(
            item.get("max_stdout_bytes")
            == submit.REMOTE_HOUSEKEEPING_STDOUT_MAX_BYTES
            for item in observed
        )
        assert all(
            item.get("max_stderr_bytes")
            == submit.REMOTE_HOUSEKEEPING_STDERR_MAX_BYTES
            for item in observed
        )

    def test_ambiguous_submit_retains_both_staged_archives(
        self,
        tmp_path: Path,
        host_cfg: HostConfig,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        uploaded: list[tuple[Path, str]] = []
        cleaned: list[tuple[str, ...]] = []

        def fake_upload(
            _host_cfg: HostConfig, local: Path, remote: str
        ) -> None:
            uploaded.append((local, remote))

        def ambiguous(*_args: object, **_kwargs: object) -> None:
            raise transport.RemoteOutcomeUnknown("submit observer lost")

        def fake_shell(
            _host_cfg: HostConfig, *args: str, **_kwargs: object
        ) -> subprocess.CompletedProcess[str]:
            cleaned.append(args)
            return subprocess.CompletedProcess(args, 0, "", "")

        monkeypatch.setattr(submit.transport, "upload_file", fake_upload)
        monkeypatch.setattr(submit.transport, "run_remote_vq", ambiguous)
        monkeypatch.setattr(submit.transport, "run_remote_shell", fake_shell)
        source = tmp_path / "ambiguous.py"
        source.write_text("pass\n")

        with pytest.raises(transport.RemoteOutcomeUnknown):
            submit.submit_remote(
                host="host_d", host_cfg=host_cfg, input_file=str(source)
            )

        [(local_tar, remote_tar)] = uploaded
        assert local_tar.exists()
        assert remote_tar.startswith(".vq-upload-")
        assert cleaned == []
        local_tar.unlink()

    def test_success_without_a_valid_jobid_is_outcome_unknown_and_retained(
        self,
        tmp_path: Path,
        host_cfg: HostConfig,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        uploaded: list[tuple[Path, str]] = []
        cleaned: list[tuple[str, ...]] = []

        monkeypatch.setattr(
            submit.transport,
            "upload_file",
            lambda _cfg, local, remote: uploaded.append((local, remote)),
        )
        monkeypatch.setattr(
            submit.transport,
            "run_remote_vq",
            lambda *_args, **_kwargs: subprocess.CompletedProcess(
                ["ssh"], 0, "", ""
            ),
        )
        monkeypatch.setattr(
            submit.transport,
            "run_remote_shell",
            lambda _cfg, *args, **_kwargs: cleaned.append(args),
        )
        source = tmp_path / "missing-receipt.py"
        source.write_text("pass\n")

        with pytest.raises(
            transport.RemoteOutcomeUnknown,
            match="returned 0 jobids",
        ):
            submit.submit_remote(
                host="host_d", host_cfg=host_cfg, input_file=str(source)
            )

        [(local_tar, _remote_tar)] = uploaded
        assert local_tar.exists()
        assert cleaned == []
        local_tar.unlink()

    def test_committed_upload_rejection_cleans_partial_remote_staging(
        self,
        tmp_path: Path,
        host_cfg: HostConfig,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        cleaned: list[tuple[str, ...]] = []

        def rejected_upload(*_args: object, **_kwargs: object) -> None:
            raise transport.RemoteError("remote filesystem rejected upload")

        monkeypatch.setattr(submit.transport, "upload_file", rejected_upload)
        monkeypatch.setattr(
            submit.transport,
            "run_remote_shell",
            lambda _cfg, *args, **_kwargs: cleaned.append(args),
        )
        source = tmp_path / "rejected.py"
        source.write_text("pass\n")

        with pytest.raises(transport.RemoteError, match="rejected upload"):
            submit.submit_remote(
                host="host_d", host_cfg=host_cfg, input_file=str(source)
            )

        assert len(cleaned) == 1
        assert cleaned[0][:2] == ("rm", "-f")

    def test_local_cleanup_failure_does_not_mask_proven_rejection(
        self,
        tmp_path: Path,
        host_cfg: HostConfig,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        allocated = tmp_path / "allocated.tar"
        source = tmp_path / "rejected.py"
        source.write_text("pass\n")
        original_unlink = Path.unlink

        monkeypatch.setattr(
            submit,
            "_make_temp_tar",
            lambda: allocated.touch() or allocated,
        )
        monkeypatch.setattr(
            submit.transport,
            "upload_file",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                transport.RemoteError("remote filesystem rejected upload")
            ),
        )
        monkeypatch.setattr(
            submit.transport,
            "run_remote_shell",
            lambda *_args, **_kwargs: subprocess.CompletedProcess(
                ["ssh"], 0, "", ""
            ),
        )

        def fail_allocated_unlink(
            path: Path,
            *args: object,
            **kwargs: object,
        ) -> None:
            if path == allocated:
                raise OSError("simulated local cleanup failure")
            original_unlink(path, *args, **kwargs)

        monkeypatch.setattr(Path, "unlink", fail_allocated_unlink)

        with pytest.raises(transport.RemoteError, match="rejected upload"):
            submit.submit_remote(
                host="host_d",
                host_cfg=host_cfg,
                input_file=str(source),
            )

        assert allocated.exists()

    def test_broken_logger_cannot_mask_proven_rejection(
        self,
        tmp_path: Path,
        host_cfg: HostConfig,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        source = tmp_path / "rejected.py"
        source.write_text("pass\n")
        monkeypatch.setattr(
            submit.transport,
            "upload_file",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                transport.RemoteError("authoritative rejection")
            ),
        )
        monkeypatch.setattr(
            submit.transport,
            "run_remote_shell",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                KeyboardInterrupt("cleanup interrupted")
            ),
        )
        monkeypatch.setattr(
            submit.log,
            "warning",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                RuntimeError("logging handler failed")
            ),
        )

        with pytest.raises(transport.RemoteError, match="authoritative rejection"):
            submit.submit_remote(
                host="host_d",
                host_cfg=host_cfg,
                input_file=str(source),
            )

    def test_duplicate_multi_job_receipt_is_outcome_unknown_and_retained(
        self,
        tmp_path: Path,
        host_cfg: HostConfig,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        cleaned: list[tuple[str, ...]] = []
        uploaded: list[Path] = []
        monkeypatch.setattr(
            submit.transport,
            "upload_file",
            lambda _cfg, local, _remote: uploaded.append(local),
        )
        monkeypatch.setattr(
            submit.transport,
            "run_remote_vq",
            lambda *_args, **_kwargs: subprocess.CompletedProcess(
                ["ssh"], 0, "abc123def456\nabc123def456\n", ""
            ),
        )
        monkeypatch.setattr(
            submit.transport,
            "run_remote_shell",
            lambda _cfg, *args, **_kwargs: cleaned.append(args),
        )
        source = tmp_path / "array.py"
        source.write_text("pass\n")

        with pytest.raises(transport.RemoteOutcomeUnknown, match="duplicate"):
            submit.submit_remote(
                host="host_d",
                host_cfg=host_cfg,
                input_file=str(source),
                array=2,
            )

        assert uploaded[0].exists()
        assert cleaned == []
        uploaded[0].unlink()

    def test_keyboard_interrupt_while_observing_submit_retains_staging(
        self,
        tmp_path: Path,
        host_cfg: HostConfig,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        uploaded: list[Path] = []
        cleaned: list[tuple[str, ...]] = []
        monkeypatch.setattr(
            submit.transport,
            "upload_file",
            lambda _cfg, local, _remote: uploaded.append(local),
        )
        monkeypatch.setattr(
            submit.transport,
            "run_remote_vq",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(KeyboardInterrupt()),
        )
        monkeypatch.setattr(
            submit.transport,
            "run_remote_shell",
            lambda _cfg, *args, **_kwargs: cleaned.append(args),
        )
        source = tmp_path / "interrupted.py"
        source.write_text("pass\n")

        with pytest.raises(KeyboardInterrupt):
            submit.submit_remote(
                host="host_d", host_cfg=host_cfg, input_file=str(source)
            )

        assert uploaded[0].exists()
        assert cleaned == []
        uploaded[0].unlink()

    def test_qvf_upload_rejection_removes_file_and_stage_directory(
        self,
        tmp_path: Path,
        host_cfg: HostConfig,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        calls: list[tuple[str, ...]] = []

        def shell(
            _cfg: HostConfig, *args: str, **_kwargs: object
        ) -> subprocess.CompletedProcess[str]:
            calls.append(args)
            return subprocess.CompletedProcess(args, 0, "", "")

        monkeypatch.setattr(submit.transport, "run_remote_shell", shell)
        monkeypatch.setattr(
            submit.transport,
            "upload_file",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                transport.RemoteError("qvf upload rejected")
            ),
        )
        source = tmp_path / "job.qvf"
        source.write_bytes(b"qvf")

        with pytest.raises(transport.RemoteError, match="qvf upload rejected"):
            submit.submit_remote(
                host="host_d", host_cfg=host_cfg, input_file=str(source)
            )

        assert calls[0][:2] == ("mkdir", "-p")
        stage_dir = calls[0][2]
        assert calls[1] == ("rm", "-f", f"{stage_dir}/job.qvf")
        assert calls[2] == ("rmdir", stage_dir)

    def test_forwards_only_remote_impossible_capacity_warning(
        self,
        tmp_path: Path,
        host_cfg: HostConfig,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def fake_upload(*_args: object, **_kwargs: object) -> None:
            return None

        def fake_run_remote_vq(
            _host_cfg: HostConfig, *_args: str, **_kwargs: object
        ) -> subprocess.CompletedProcess[str]:
            return subprocess.CompletedProcess(
                args=["ssh"],
                returncode=0,
                stdout="abc123def456\n",
                stderr=(
                    "vq: sanitized --job-name 'unsafe name' -> "
                    "'unsafe-name'\n"
                    "vq: warning: unrelated mixed-version diagnostic\n"
                    "vq: warning: requested 32 CPUs but this daemon caps at "
                    "--max-cpus 16; job abc123def456 will park PENDING until "
                    "the daemon is restarted with a higher cap\n"
                ),
            )

        def fake_run_remote_shell(
            _host_cfg: HostConfig, *_args: str, **_kwargs: object
        ) -> subprocess.CompletedProcess[str]:
            return subprocess.CompletedProcess(
                args=["ssh"], returncode=0, stdout="", stderr=""
            )

        monkeypatch.setattr(submit.transport, "upload_file", fake_upload)
        monkeypatch.setattr(
            submit.transport, "run_remote_vq", fake_run_remote_vq
        )
        monkeypatch.setattr(
            submit.transport, "run_remote_shell", fake_run_remote_shell
        )
        source = tmp_path / "wide.py"
        source.write_text("pass\n")
        warnings: list[str] = []

        jobids = submit.submit_remote(
            host="host_d",
            host_cfg=host_cfg,
            input_file=str(source),
            warning_sink=warnings.append,
        )

        assert jobids == ["abc123def456"]
        assert warnings == [
            "requested 32 CPUs but this daemon caps at --max-cpus 16; "
            "job abc123def456 will park PENDING until the daemon is "
            "restarted with a higher cap"
        ]

    @pytest.mark.parametrize("flag", ["--max-mem", "--max-mem-mb"])
    def test_forwards_undeclared_default_memory_overage_warning(
        self, flag: str
    ) -> None:
        message = (
            "undeclared memory is charged at daemon default 64000 MB but "
            f"this daemon caps at {flag} 49340 MB; job abc123def456 will "
            "park PENDING until the daemon is restarted with a higher cap"
        )

        forwarded = submit._remote_impossible_capacity_warning(
            f"{submit._SUBMIT_WARNING_PREFIX}{message}",
            jobids=["abc123def456"],
        )

        assert forwarded == message

    @pytest.mark.parametrize("flag", ["--max-mem", "--max-mem-mb"])
    def test_forwards_explicit_memory_overage_warning(self, flag: str) -> None:
        message = (
            "requested 64000 MB memory but this daemon caps at "
            f"{flag} 49340 MB; job abc123def456 will park PENDING until the "
            "daemon is restarted with a higher cap"
        )

        forwarded = submit._remote_impossible_capacity_warning(
            f"{submit._SUBMIT_WARNING_PREFIX}{message}",
            jobids=["abc123def456"],
        )

        assert forwarded == message

    def test_garbage_jobid_raises_remote_error(
        self,
        tmp_path: Path,
        host_cfg: HostConfig,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        uploaded: list[Path] = []
        cleaned: list[tuple[str, ...]] = []

        def fake_upload(_cfg, local, _remote) -> None:
            uploaded.append(local)

        def fake_run_remote_vq(host_cfg, *args, check=True, **_kwargs):
            return subprocess.CompletedProcess(
                args=["ssh"], returncode=0, stdout="not-a-jobid\n", stderr=""
            )

        def fake_run_remote_shell(host_cfg, *args, check=True, **_kwargs):
            cleaned.append(args)
            return subprocess.CompletedProcess(
                args=list(args), returncode=0, stdout="", stderr=""
            )

        monkeypatch.setattr(submit.transport, "upload_file", fake_upload)
        monkeypatch.setattr(submit.transport, "run_remote_vq", fake_run_remote_vq)
        monkeypatch.setattr(submit.transport, "run_remote_shell", fake_run_remote_shell)

        src = tmp_path / "x.py"
        src.write_text("")
        with pytest.raises(
            transport.RemoteOutcomeUnknown,
            match="unexpected output",
        ):
            submit.submit_remote(host="x", host_cfg=host_cfg, input_file=str(src))
        assert uploaded[0].exists()
        assert cleaned == []
        uploaded[0].unlink()


# ----------------------------------------------------------------------
# v0.7.11 *Stroustrup's Stencil* — remote --array single-roundtrip
# ----------------------------------------------------------------------


class TestRemoteArray:
    """v0.7.11: ``submit_remote(..., array=N)`` forwards ``--array N``
    on the remote argv and parses N jobids from the remote vq's
    multi-line stdout. Was previously a loop on the laptop side with
    N SSH roundtrips + N source tar uploads."""

    @pytest.fixture
    def array_calls(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> TransportCalls:
        """Per-test calls fixture that returns N jobids in stdout.
        Counts the --array argument and stamps that many jobids."""
        rec = TransportCalls()

        def fake_upload(host_cfg, local, remote):
            assert local.exists()
            rec.uploaded.append((local, remote))

        def fake_run_remote_vq(
            host_cfg: HostConfig,
            *args: str,
            check: bool = True,
            **_kwargs: object,
        ) -> subprocess.CompletedProcess[str]:
            rec.remote_vq.append(args)
            # Sniff --array N out of the argv so we can echo N jobids.
            n = 1
            for i, a in enumerate(args):
                if a == "--array" and i + 1 < len(args):
                    with contextlib.suppress(ValueError):
                        n = int(args[i + 1])
                    break
            # 12-hex jobids, one per line. Stable per call so callers
            # can assert on exact content.
            jids = "\n".join(f"{i:012x}" for i in range(n))
            return subprocess.CompletedProcess(
                args=["ssh", host_cfg.ssh, host_cfg.remote_vq, *args],
                returncode=0,
                stdout=jids + "\n",
                stderr="",
            )

        def fake_run_remote_shell(host_cfg, *args, check=True):
            rec.remote_shell.append(args)
            return subprocess.CompletedProcess(
                args=list(args), returncode=0, stdout="", stderr="",
            )

        monkeypatch.setattr(submit.transport, "upload_file", fake_upload)
        monkeypatch.setattr(submit.transport, "run_remote_vq", fake_run_remote_vq)
        monkeypatch.setattr(submit.transport, "run_remote_shell", fake_run_remote_shell)
        return rec

    def test_array_one_returns_single_jobid_list(
        self, tmp_path: Path, host_cfg: HostConfig,
        array_calls: TransportCalls,
    ) -> None:
        """The default (array=1) path still works: one upload, one ssh
        call, one jobid returned as a 1-element list."""
        src = tmp_path / "in.py"
        src.write_text("pass")
        jobids = submit.submit_remote(
            host="host_d", host_cfg=host_cfg, input_file=str(src),
        )
        assert jobids == ["000000000000"]
        # One upload, one remote vq call. --array NOT forwarded.
        assert len(array_calls.uploaded) == 1
        assert len(array_calls.remote_vq) == 1
        assert "--array" not in array_calls.remote_vq[0]

    def test_array_n_does_one_upload_and_one_call(
        self, tmp_path: Path, host_cfg: HostConfig,
        array_calls: TransportCalls,
    ) -> None:
        """The headline v0.7.11 invariant: --array 30 does ONE upload
        and ONE remote vq call, NOT 30 of each."""
        src = tmp_path / "in.py"
        src.write_text("pass")
        jobids = submit.submit_remote(
            host="host_d", host_cfg=host_cfg, input_file=str(src),
            array=30,
        )
        assert len(jobids) == 30
        assert len(array_calls.uploaded) == 1, (
            f"--array 30 should do exactly one source upload; got "
            f"{len(array_calls.uploaded)}"
        )
        assert len(array_calls.remote_vq) == 1, (
            f"--array 30 should do exactly one remote vq call; got "
            f"{len(array_calls.remote_vq)}"
        )
        # --array 30 forwarded on the remote argv.
        assert "--array" in array_calls.remote_vq[0]
        idx = array_calls.remote_vq[0].index("--array")
        assert array_calls.remote_vq[0][idx + 1] == "30"

    def test_returned_jobids_are_all_12_hex(
        self, tmp_path: Path, host_cfg: HostConfig,
        array_calls: TransportCalls,
    ) -> None:
        src = tmp_path / "in.py"
        src.write_text("pass")
        jobids = submit.submit_remote(
            host="host_d", host_cfg=host_cfg, input_file=str(src),
            array=4,
        )
        assert len(jobids) == 4
        for jid in jobids:
            assert len(jid) == 12
            assert all(c in "0123456789abcdef" for c in jid)

    def test_mismatched_jobid_count_raises(
        self, tmp_path: Path, host_cfg: HostConfig,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """If the remote returns fewer jobids than requested, the
        local side surfaces a RemoteError rather than silently
        returning a short list."""
        def fake_upload(*a, **k): pass
        def fake_run_remote_vq(host_cfg, *args, check=True, **_kwargs):
            return subprocess.CompletedProcess(
                args=["ssh"], returncode=0,
                # Only 2 jobids, but caller asked for 5.
                stdout="aaaaaaaaaaaa\nbbbbbbbbbbbb\n",
                stderr="",
            )
        def fake_run_remote_shell(host_cfg, *args, check=True):
            return subprocess.CompletedProcess(args=list(args), returncode=0, stdout="", stderr="")
        monkeypatch.setattr(submit.transport, "upload_file", fake_upload)
        monkeypatch.setattr(submit.transport, "run_remote_vq", fake_run_remote_vq)
        monkeypatch.setattr(submit.transport, "run_remote_shell", fake_run_remote_shell)

        src = tmp_path / "in.py"
        src.write_text("pass")
        with pytest.raises(transport.RemoteError, match="expected 5"):
            submit.submit_remote(
                host="x", host_cfg=host_cfg, input_file=str(src),
                array=5,
            )

    def test_array_zero_or_negative_rejected(
        self, tmp_path: Path, host_cfg: HostConfig,
    ) -> None:
        src = tmp_path / "in.py"
        src.write_text("pass")
        with pytest.raises(ValueError, match="array must be >= 1"):
            submit.submit_remote(
                host="x", host_cfg=host_cfg, input_file=str(src),
                array=0,
            )
        with pytest.raises(ValueError, match="array must be >= 1"):
            submit.submit_remote(
                host="x", host_cfg=host_cfg, input_file=str(src),
                array=-5,
            )

    def test_invalid_jobid_in_stream_raises(
        self, tmp_path: Path, host_cfg: HostConfig,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """If the remote's multi-line stdout contains a malformed
        jobid (not 12 hex), the local side surfaces it cleanly
        rather than passing junk through."""
        uploaded: list[Path] = []
        cleaned: list[tuple[str, ...]] = []

        def fake_upload(_cfg, local, _remote):
            uploaded.append(local)
        def fake_run_remote_vq(host_cfg, *args, check=True, **_kwargs):
            return subprocess.CompletedProcess(
                args=["ssh"], returncode=0,
                stdout="aaaaaaaaaaaa\nnot-a-jobid!\n",
                stderr="",
            )
        def fake_run_remote_shell(host_cfg, *args, check=True, **_kwargs):
            cleaned.append(args)
            return subprocess.CompletedProcess(args=list(args), returncode=0, stdout="", stderr="")
        monkeypatch.setattr(submit.transport, "upload_file", fake_upload)
        monkeypatch.setattr(submit.transport, "run_remote_vq", fake_run_remote_vq)
        monkeypatch.setattr(submit.transport, "run_remote_shell", fake_run_remote_shell)

        src = tmp_path / "in.py"
        src.write_text("pass")
        with pytest.raises(
            transport.RemoteOutcomeUnknown,
            match="unexpected output",
        ):
            submit.submit_remote(
                host="x", host_cfg=host_cfg, input_file=str(src),
                array=2,
            )
        assert uploaded[0].exists()
        assert cleaned == []
        uploaded[0].unlink()


# ----------------------------------------------------------------------
# v0.8.10 *Tarjan's Bridge* — remote --chain + --rerun-until forwarding
# ----------------------------------------------------------------------


class TestRemoteChain:
    """v0.8.10: ``submit_remote(..., chain=N)`` forwards ``--chain N``
    on the remote argv. Same single-roundtrip pattern as v0.7.11 used
    for --array — one upload, one SSH call. The remote vq mints the
    chain group id and links the depends_on chain locally."""

    @pytest.fixture
    def chain_calls(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> TransportCalls:
        """Per-test calls fixture. Sniffs --chain N out of the argv
        and stamps that many jobids."""
        rec = TransportCalls()

        def fake_upload(host_cfg, local, remote):
            assert local.exists()
            rec.uploaded.append((local, remote))

        def fake_run_remote_vq(
            host_cfg: HostConfig,
            *args: str,
            check: bool = True,
            **_kwargs: object,
        ) -> subprocess.CompletedProcess[str]:
            rec.remote_vq.append(args)
            n = 1
            for i, a in enumerate(args):
                if a == "--chain" and i + 1 < len(args):
                    with contextlib.suppress(ValueError):
                        n = int(args[i + 1])
                    break
            jids = "\n".join(f"{i:012x}" for i in range(n))
            return subprocess.CompletedProcess(
                args=["ssh", host_cfg.ssh, host_cfg.remote_vq, *args],
                returncode=0, stdout=jids + "\n", stderr="",
            )

        def fake_run_remote_shell(host_cfg, *args, check=True):
            rec.remote_shell.append(args)
            return subprocess.CompletedProcess(
                args=list(args), returncode=0, stdout="", stderr="",
            )

        monkeypatch.setattr(submit.transport, "upload_file", fake_upload)
        monkeypatch.setattr(submit.transport, "run_remote_vq", fake_run_remote_vq)
        monkeypatch.setattr(submit.transport, "run_remote_shell", fake_run_remote_shell)
        return rec

    def test_chain_one_does_not_forward_flag(
        self, tmp_path: Path, host_cfg: HostConfig,
        chain_calls: TransportCalls,
    ) -> None:
        """Default chain=1 → --chain NOT forwarded (matches v0.7.11
        array=1 behaviour: keep the wire compact in the common case)."""
        src = tmp_path / "in.py"
        src.write_text("pass")
        jobids = submit.submit_remote(
            host="host_d", host_cfg=host_cfg, input_file=str(src),
        )
        assert jobids == ["000000000000"]
        assert "--chain" not in chain_calls.remote_vq[0]

    def test_chain_n_single_upload_single_call(
        self, tmp_path: Path, host_cfg: HostConfig,
        chain_calls: TransportCalls,
    ) -> None:
        """The v0.8.10 headline: --chain 5 does ONE upload and ONE
        remote vq call (matches v0.7.11 array invariant). The remote
        vq mints the chain links locally."""
        src = tmp_path / "neb_image.py"
        src.write_text("pass")
        jobids = submit.submit_remote(
            host="host_d", host_cfg=host_cfg, input_file=str(src),
            chain=5,
        )
        assert len(jobids) == 5
        assert len(chain_calls.uploaded) == 1, (
            f"--chain 5 should do exactly one source upload; got "
            f"{len(chain_calls.uploaded)}"
        )
        assert len(chain_calls.remote_vq) == 1, (
            f"--chain 5 should do exactly one remote vq call; got "
            f"{len(chain_calls.remote_vq)}"
        )
        assert "--chain" in chain_calls.remote_vq[0]
        idx = chain_calls.remote_vq[0].index("--chain")
        assert chain_calls.remote_vq[0][idx + 1] == "5"

    def test_chain_and_array_mutually_exclusive(
        self, tmp_path: Path, host_cfg: HostConfig,
    ) -> None:
        """submit_remote rejects chain + array combo client-side so
        the operator gets a clear error before any tar is built."""
        src = tmp_path / "in.py"
        src.write_text("pass")
        with pytest.raises(ValueError, match="mutually exclusive"):
            submit.submit_remote(
                host="x", host_cfg=host_cfg, input_file=str(src),
                chain=3, array=2,
            )

    def test_chain_zero_rejected(
        self, tmp_path: Path, host_cfg: HostConfig,
    ) -> None:
        src = tmp_path / "in.py"
        src.write_text("pass")
        with pytest.raises(ValueError, match="chain must be >= 1"):
            submit.submit_remote(
                host="x", host_cfg=host_cfg, input_file=str(src),
                chain=0,
            )


class TestRemoteRerunUntil:
    """v0.8.10: --rerun-until + --rerun-max forwarded on the remote
    argv so DFT+U self-consistency loops can run on remote hosts."""

    @pytest.fixture
    def single_jobid_calls(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> TransportCalls:
        rec = TransportCalls()

        def fake_upload(host_cfg, local, remote):
            rec.uploaded.append((local, remote))

        def fake_run_remote_vq(
            host_cfg: HostConfig,
            *args: str,
            check: bool = True,
            **_kwargs: object,
        ) -> subprocess.CompletedProcess[str]:
            rec.remote_vq.append(args)
            return subprocess.CompletedProcess(
                args=["ssh", host_cfg.ssh, host_cfg.remote_vq, *args],
                returncode=0, stdout="abcdef012345\n", stderr="",
            )

        def fake_run_remote_shell(host_cfg, *args, check=True):
            rec.remote_shell.append(args)
            return subprocess.CompletedProcess(
                args=list(args), returncode=0, stdout="", stderr="",
            )

        monkeypatch.setattr(submit.transport, "upload_file", fake_upload)
        monkeypatch.setattr(submit.transport, "run_remote_vq", fake_run_remote_vq)
        monkeypatch.setattr(submit.transport, "run_remote_shell", fake_run_remote_shell)
        return rec

    def test_rerun_until_forwarded_verbatim(
        self, tmp_path: Path, host_cfg: HostConfig,
        single_jobid_calls: TransportCalls,
    ) -> None:
        """The path string passes through unchanged — the remote
        daemon substitutes $VQ_WORKDIR using its own layout."""
        src = tmp_path / "dft_u.py"
        src.write_text("pass")
        jobids = submit.submit_remote(
            host="host_d", host_cfg=host_cfg, input_file=str(src),
            rerun_until_file_exists="$VQ_WORKDIR/CONVERGED",
        )
        assert jobids == ["abcdef012345"]
        argv = single_jobid_calls.remote_vq[0]
        assert "--rerun-until" in argv
        idx = argv.index("--rerun-until")
        assert argv[idx + 1] == "$VQ_WORKDIR/CONVERGED"

    def test_default_rerun_max_not_forwarded(
        self, tmp_path: Path, host_cfg: HostConfig,
        single_jobid_calls: TransportCalls,
    ) -> None:
        """--rerun-max defaults to 10 on both sides; don't bloat the
        argv with the default value."""
        src = tmp_path / "in.py"
        src.write_text("pass")
        submit.submit_remote(
            host="host_d", host_cfg=host_cfg, input_file=str(src),
            rerun_until_file_exists="/tmp/flag",
        )
        argv = single_jobid_calls.remote_vq[0]
        assert "--rerun-until" in argv
        assert "--rerun-max" not in argv

    def test_custom_rerun_max_forwarded(
        self, tmp_path: Path, host_cfg: HostConfig,
        single_jobid_calls: TransportCalls,
    ) -> None:
        src = tmp_path / "in.py"
        src.write_text("pass")
        submit.submit_remote(
            host="host_d", host_cfg=host_cfg, input_file=str(src),
            rerun_until_file_exists="/tmp/flag",
            rerun_max=25,
        )
        argv = single_jobid_calls.remote_vq[0]
        assert "--rerun-max" in argv
        idx = argv.index("--rerun-max")
        assert argv[idx + 1] == "25"

    def test_no_rerun_no_flag_forwarded(
        self, tmp_path: Path, host_cfg: HostConfig,
        single_jobid_calls: TransportCalls,
    ) -> None:
        """Default no --rerun-until → neither flag appears in argv."""
        src = tmp_path / "in.py"
        src.write_text("pass")
        submit.submit_remote(
            host="host_d", host_cfg=host_cfg, input_file=str(src),
        )
        argv = single_jobid_calls.remote_vq[0]
        assert "--rerun-until" not in argv
        assert "--rerun-max" not in argv

    def test_chain_plus_rerun_compose(
        self, tmp_path: Path, host_cfg: HostConfig,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The NEB+DFT+U composed recipe: --chain 5 +
        --rerun-until — both forwarded, one upload, one call,
        N=5 jobids returned."""
        rec = TransportCalls()

        def fake_upload(host_cfg, local, remote):
            rec.uploaded.append((local, remote))

        def fake_run_remote_vq(
            host_cfg: HostConfig,
            *args: str,
            check: bool = True,
            **_kwargs: object,
        ) -> subprocess.CompletedProcess[str]:
            rec.remote_vq.append(args)
            n = 1
            for i, a in enumerate(args):
                if a == "--chain" and i + 1 < len(args):
                    n = int(args[i + 1])
                    break
            jids = "\n".join(f"{i:012x}" for i in range(n))
            return subprocess.CompletedProcess(
                args=["ssh"], returncode=0, stdout=jids + "\n", stderr="",
            )

        def fake_run_remote_shell(host_cfg, *args, check=True):
            return subprocess.CompletedProcess(
                args=list(args), returncode=0, stdout="", stderr="",
            )

        monkeypatch.setattr(submit.transport, "upload_file", fake_upload)
        monkeypatch.setattr(submit.transport, "run_remote_vq", fake_run_remote_vq)
        monkeypatch.setattr(submit.transport, "run_remote_shell", fake_run_remote_shell)

        src = tmp_path / "neb_pbe_u.py"
        src.write_text("pass")
        jobids = submit.submit_remote(
            host="host_d", host_cfg=host_cfg, input_file=str(src),
            chain=5,
            rerun_until_file_exists="$VQ_WORKDIR/NEB_CONVERGED",
            rerun_max=20,
        )
        assert len(jobids) == 5
        assert len(rec.uploaded) == 1
        assert len(rec.remote_vq) == 1
        argv = rec.remote_vq[0]
        # All three flags present.
        assert "--chain" in argv
        assert "--rerun-until" in argv
        assert "--rerun-max" in argv


@pytest.mark.parametrize("head", ["--idempotency-key", "--cpus", "", "bad\x00name"])
def test_invalid_command_head_rejected_before_transport(
    tmp_path: Path, host_cfg: HostConfig, calls: TransportCalls, head: str,
) -> None:
    source = tmp_path / "payload"
    source.mkdir()
    with pytest.raises(ValueError, match="command executable"):
        submit.submit_remote(
            host="host_d", host_cfg=host_cfg, directory=str(source),
            command=[head, "key", "bash", "-lc", "true"],
        )
    assert not calls.uploaded
    assert not calls.remote_shell
    assert not calls.remote_vq
