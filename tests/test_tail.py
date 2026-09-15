"""Tests for `vq tail` (v0.5.26).

The verb ``execvp``s `tail` directly (locally) or `ssh` (remotely),
which would normally terminate the test process. We patch
``os.execvp`` to capture the argv instead — the CLI's contract is
"build the right argv and hand it to execvp," so checking the argv
is the right unit-test surface.
"""
from __future__ import annotations

import json
import shlex
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import config, ownership, paths, transport
from vq.cli import main
from vq.scheduler_dispatch import SchedulerFileChunk
from vq.spec import JobSpec, JobState


@pytest.fixture
def state_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    paths.jobs_dir().mkdir(parents=True, exist_ok=True)
    return tmp_path


def _make_workspace(
    jobid: str, files: dict[str, str] | None = None,
) -> Path:
    """Create a fake workspace dir under `paths.jobs_dir()` and
    populate it with the requested files."""
    ws = paths.jobs_dir() / jobid
    ws.mkdir(parents=True, exist_ok=True)
    for name, content in (files or {}).items():
        (ws / name).write_text(content)
    return ws


class TestTailLocal:
    """``vq tail JOBID`` on the local host execs `tail` directly.
    We patch execvp to capture the argv that would have been launched."""

    def test_default_filename_is_stdout_log(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _make_workspace("aaaa00000001", {"stdout.log": "hello world\n"})
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
        )
        captured: list[tuple[str, list[str]]] = []
        monkeypatch.setattr(
            "os.execvp",
            lambda prog, argv: captured.append((prog, argv)),
        )
        result = CliRunner().invoke(main, ["tail", "aaaa00000001"])
        assert result.exit_code == 0, result.output
        assert len(captured) == 1
        prog, argv = captured[0]
        assert prog == "tail"
        # Default -n 50
        assert "-n50" in argv
        # No -f by default
        assert "-f" not in argv
        # Last positional is the absolute workspace path
        assert argv[-1].endswith("aaaa00000001/stdout.log")

    def test_follow_flag_passes_dash_f(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _make_workspace("aaaa00000002", {"stdout.log": ""})
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
        )
        captured: list[tuple[str, list[str]]] = []
        monkeypatch.setattr(
            "os.execvp",
            lambda prog, argv: captured.append((prog, argv)),
        )
        result = CliRunner().invoke(
            main, ["tail", "aaaa00000002", "-f"]
        )
        assert result.exit_code == 0
        assert "-f" in captured[0][1]

    def test_custom_name_targets_arbitrary_file(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The vibe-qc-logger use case: pass `--name vibeqc.log` and
        the verb tails that file instead of stdout.log."""
        _make_workspace(
            "aaaa00000003",
            {"stdout.log": "ignored\n", "vibeqc.log": "logger says hi\n"},
        )
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
        )
        captured: list[tuple[str, list[str]]] = []
        monkeypatch.setattr(
            "os.execvp",
            lambda prog, argv: captured.append((prog, argv)),
        )
        result = CliRunner().invoke(
            main, ["tail", "aaaa00000003", "--name", "vibeqc.log"]
        )
        assert result.exit_code == 0
        assert captured[0][1][-1].endswith("aaaa00000003/vibeqc.log")

    def test_engine_specific_output_file(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """CRYSTAL writes mgo.out, ORCA writes h2.out, etc.
        --name handles any of them."""
        _make_workspace(
            "aaaa00000004",
            {"mgo.d12": "input", "mgo.out": "SCF ENDED\n"},
        )
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
        )
        captured: list[tuple[str, list[str]]] = []
        monkeypatch.setattr(
            "os.execvp",
            lambda prog, argv: captured.append((prog, argv)),
        )
        result = CliRunner().invoke(
            main, ["tail", "aaaa00000004", "--name", "mgo.out"]
        )
        assert result.exit_code == 0
        assert captured[0][1][-1].endswith("aaaa00000004/mgo.out")

    def test_json_resolves_multi_user_spec_workspace(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Remote delegation lands here as ``tail localhost``.

        The CLI may run as the submitting user while the root daemon owns
        state under /var/lib/vq/users, so resolution must follow the
        multi-user spec's cwd instead of the legacy XDG jobs directory.
        """
        multi_root = state_dir / "multi"
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(multi_root))
        (state_dir / "cfg" / "config.toml").write_text(
            "[multi_user]\nenabled = true\n"
        )
        jobid = "aaaa00000005"
        workspace = paths.user_workdir(1000, jobid)
        workspace.mkdir(parents=True)
        (workspace / "job.out").write_text("SCF converged\n")
        spec_path = paths.user_spec_path(1000, jobid)
        spec_path.parent.mkdir(parents=True)
        JobSpec(
            id=jobid,
            command=["true"],
            cwd=str(workspace),
            cpus=1,
            state=JobState.COMPLETED,
            exit_code=0,
        ).write(spec_path)

        result = CliRunner().invoke(
            main,
            [
                "tail",
                "localhost",
                jobid,
                "--name",
                "job.out",
                "-n",
                "0",
                "--json",
            ],
        )

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["text"] == "SCF converged"
        assert payload["path"] == str(workspace / "job.out")

    @pytest.mark.parametrize("as_json", [False, True])
    def test_multi_user_tail_refuses_foreign_job_before_read_or_exec(
        self,
        state_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
        as_json: bool,
    ) -> None:
        monkeypatch.setattr(
            config, "SYSTEM_CONFIG_PATH", state_dir / "absent-system.toml"
        )
        multi_root = state_dir / "multi"
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(multi_root))
        (state_dir / "cfg" / "config.toml").write_text(
            "[multi_user]\n"
            "enabled = true\n"
            'admin_group = "nonexistent-vq-test-group"\n'
        )
        jobid = "foreign-tail-1"
        workspace = paths.user_workspace_dir("2002", jobid)
        workspace.mkdir(parents=True)
        (workspace / "stdout.log").write_text("private output\n")
        JobSpec(
            id=jobid,
            command=["true"],
            cwd=str(workspace),
            cpus=1,
            submitter="2002",
        ).write(paths.user_spec_path("2002", jobid))
        monkeypatch.setattr(ownership, "_caller_uid", lambda: 1001)
        monkeypatch.setattr(ownership, "_caller_is_admin", lambda _cfg: False)
        exec_calls: list[tuple[str, list[str]]] = []
        monkeypatch.setattr(
            "os.execvp",
            lambda prog, argv: exec_calls.append((prog, argv)),
        )
        args = ["tail", "localhost", jobid]
        if as_json:
            args.append("--json")

        result = CliRunner().invoke(main, args)

        assert isinstance(result.exception, ownership.OwnershipError)
        assert "private output" not in result.output
        assert exec_calls == []

    def test_multi_user_tail_keeps_admin_bypass(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            config, "SYSTEM_CONFIG_PATH", state_dir / "absent-system.toml"
        )
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(state_dir / "multi"))
        (state_dir / "cfg" / "config.toml").write_text(
            "[multi_user]\nenabled = true\n"
        )
        jobid = "admin-tail-1"
        workspace = paths.user_workspace_dir("2002", jobid)
        workspace.mkdir(parents=True)
        (workspace / "stdout.log").write_text("admin-visible\n")
        JobSpec(
            id=jobid,
            command=["true"],
            cwd=str(workspace),
            cpus=1,
            submitter="2002",
        ).write(paths.user_spec_path("2002", jobid))
        monkeypatch.setattr(ownership, "_caller_is_admin", lambda _cfg: True)

        result = CliRunner().invoke(
            main, ["tail", "localhost", jobid, "--json"]
        )

        assert result.exit_code == 0, result.output
        assert json.loads(result.output)["text"] == "admin-visible"

    def test_lines_flag_propagates(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _make_workspace("aaaa00000005", {"stdout.log": ""})
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
        )
        captured: list[tuple[str, list[str]]] = []
        monkeypatch.setattr(
            "os.execvp",
            lambda prog, argv: captured.append((prog, argv)),
        )
        result = CliRunner().invoke(
            main, ["tail", "aaaa00000005", "-n", "200"]
        )
        assert result.exit_code == 0
        assert "-n200" in captured[0][1]

    def test_json_outputs_machine_readable_tail_without_exec(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        ws = _make_workspace(
            "aaaa00000009",
            {"calc.out": "SCF 1\nSCF 2\nSCF 3\n"},
        )
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
        )
        JobSpec(
            id="aaaa00000009",
            command=["true"],
            cwd=str(ws),
            cpus=2,
            state=JobState.RUNNING,
            submitted_at="2026-08-02T12:34:56+00:00",
        ).write(paths.queue_dir() / "aaaa00000009.json")
        monkeypatch.setattr(
            "os.execvp",
            lambda prog, argv: (_ for _ in ()).throw(AssertionError("must not exec")),
        )

        result = CliRunner().invoke(
            main,
            ["tail", "aaaa00000009", "--name", "calc.out", "-n", "2", "--json"],
        )

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["jobid"] == "aaaa00000009"
        assert payload["host"] == "localhost"
        assert payload["state"] == "running"
        assert payload["filename"] == "calc.out"
        assert payload["tail"] == 2
        assert payload["text"] == "... (1 earlier lines)\nSCF 2\nSCF 3"
        assert payload["path"].endswith("aaaa00000009/calc.out")
        assert payload["queue_handle"] == {
            "job_id": "aaaa00000009",
            "host": "localhost",
            "submitted_at": "2026-08-02T12:34:56+00:00",
        }


class TestTailErrors:
    """Errors that we catch in Python before reaching execvp."""

    def test_missing_workspace_errors(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
        )
        # Don't create the workspace dir
        captured: list = []
        monkeypatch.setattr(
            "os.execvp", lambda prog, argv: captured.append((prog, argv)),
        )
        result = CliRunner().invoke(main, ["tail", "ghostjob9999"])
        assert result.exit_code != 0
        assert "no workspace for jobid" in result.output
        # Did NOT exec — we caught it in Python first.
        assert captured == []

    def test_missing_file_errors_with_hint(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _make_workspace(
            "aaaa00000006",
            {"stdout.log": "x", "mgo.d12": "y", "mgo.out": "z"},
        )
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
        )
        captured: list = []
        monkeypatch.setattr(
            "os.execvp", lambda prog, argv: captured.append((prog, argv)),
        )
        result = CliRunner().invoke(
            main, ["tail", "aaaa00000006", "--name", "does-not-exist.log"]
        )
        assert result.exit_code != 0
        assert "not found in workspace" in result.output
        # Hint lists what IS in the workspace
        assert "stdout.log" in result.output
        assert "mgo.out" in result.output
        assert captured == []

    def test_absolute_path_rejected(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A `--name /etc/passwd` attempt must be rejected before any
        file lookup."""
        _make_workspace("aaaa00000007", {"stdout.log": "x"})
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
        )
        captured: list = []
        monkeypatch.setattr(
            "os.execvp", lambda prog, argv: captured.append((prog, argv)),
        )
        result = CliRunner().invoke(
            main, ["tail", "aaaa00000007", "--name", "/etc/passwd"]
        )
        assert result.exit_code != 0
        assert "absolute paths" in result.output
        assert captured == []

    def test_parent_path_rejected(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A `--name ../../etc/passwd` attempt must be rejected."""
        _make_workspace("aaaa00000008", {"stdout.log": "x"})
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
        )
        captured: list = []
        monkeypatch.setattr(
            "os.execvp", lambda prog, argv: captured.append((prog, argv)),
        )
        result = CliRunner().invoke(
            main, ["tail", "aaaa00000008", "--name", "../escape"]
        )
        assert result.exit_code != 0
        assert "'..'" in result.output
        assert captured == []

    def test_json_follow_rejected_before_exec(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
        )
        captured: list = []
        monkeypatch.setattr(
            "os.execvp", lambda prog, argv: captured.append((prog, argv)),
        )

        result = CliRunner().invoke(
            main, ["tail", "aaaa00000009", "--json", "-f"]
        )

        assert result.exit_code != 0
        assert "cannot be combined with --follow" in result.output
        assert captured == []


class TestTailRemoteDelegation:
    """Remote-host case: execs hardened `ssh HOST vq tail localhost JOBID ...`."""

    def test_remote_uses_ssh_and_forwards_args(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """v0.5.32: the remote ssh argv keeps the command as one joined token
        after the host; shlex.split of the last element recovers the argv the
        remote vq tail will be invoked with. (Pre-v0.5.32 each token
        was a separate ssh argv element and got re-joined + re-parsed
        by the remote shell — that's the bug the operator's report flagged.)"""
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "compute"\n'
            '[hosts.compute]\n'
            'ssh = "compute-host"\n'
            'remote_vq = "/opt/vq/.venv/bin/vq"\n'
        )
        captured: list[tuple[str, list[str]]] = []
        monkeypatch.setattr(
            "os.execvp",
            lambda prog, argv: captured.append((prog, argv)),
        )
        monkeypatch.setattr(
            "vq.cli._queue_rows_for_authority",
            lambda *args, **kwargs: [],
        )
        result = CliRunner().invoke(
            main, ["tail", "myjob000001", "-f", "-n", "100",
                   "--name", "vibeqc.log"]
        )
        assert result.exit_code == 0
        prog, argv = captured[0]
        assert prog == "ssh"
        assert argv[0] == "ssh"
        assert argv[-2] == "compute-host"
        assert "-o" in argv
        assert f"ConnectTimeout={transport._SSH_CONNECT_TIMEOUT_SECONDS}" in argv
        assert f"ServerAliveInterval={transport._SSH_SERVER_ALIVE_INTERVAL}" in argv
        assert f"ServerAliveCountMax={transport._SSH_SERVER_ALIVE_COUNT_MAX}" in argv
        assert "BatchMode=yes" in argv
        # The remote shell will run `sh -c <argv[-1]>`; shlex.split
        # models that parsing. The recovered argv is what the remote
        # /opt/vq/.venv/bin/vq tail invocation actually receives.
        remote = shlex.split(argv[-1])
        assert remote == [
            "/opt/vq/.venv/bin/vq", "tail", "localhost", "myjob000001",
            "--name", "vibeqc.log", "-n", "100", "-f",
        ]

    def test_remote_filename_with_spaces_preserved(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """v0.5.32 regression guard: a filename containing a space
        survives the laptop → ssh → remote-shell trip. Pre-fix, the
        remote shell would have re-tokenised ``my report.log`` into
        two arguments and `vq tail --name` would have got just
        ``my``."""
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "compute"\n'
            '[hosts.compute]\n'
            'ssh = "compute-host"\n'
            'remote_vq = "/opt/vq/.venv/bin/vq"\n'
        )
        captured: list[tuple[str, list[str]]] = []
        monkeypatch.setattr(
            "os.execvp",
            lambda prog, argv: captured.append((prog, argv)),
        )
        monkeypatch.setattr(
            "vq.cli._queue_rows_for_authority",
            lambda *args, **kwargs: [],
        )
        result = CliRunner().invoke(
            main, ["tail", "myjob000001", "--name", "my report.log"]
        )
        assert result.exit_code == 0
        prog, argv = captured[0]
        assert prog == "ssh"
        remote = shlex.split(argv[-1])
        # The space-containing filename comes back as ONE token.
        assert "my report.log" in remote

    def test_remote_json_rewrites_queue_handle_host(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Remote arbitrary-file tail JSON keeps the requested host alias."""
        (state_dir / "cfg" / "config.toml").write_text(
            '[hosts.host_d]\n'
            'ssh = "host_d"\n'
            'remote_vq = "/home/USER/vq/.venv/bin/vq"\n'
        )
        stdout = json.dumps(
            {
                "jobid": "ab12cd34ef56",
                "host": "localhost",
                "state": "running",
                "queue_handle": {
                    "job_id": "ab12cd34ef56",
                    "host": "localhost",
                    "submitted_at": "2026-07-03T06:00:00+00:00",
                },
            }
        )
        captured: dict[str, object] = {}

        def fake_delegate(host, cfg, *args, stdin_data=None):
            captured["host"] = host
            captured["args"] = list(args)
            return f"{stdout}\n"

        monkeypatch.setattr("vq.cli._delegate_to_remote", fake_delegate)
        monkeypatch.setattr(
            "os.execvp",
            lambda prog, argv: (_ for _ in ()).throw(AssertionError("must not exec")),
        )

        result = CliRunner().invoke(
            main,
            [
                "tail",
                "host_d",
                "ab12cd34ef56",
                "--name",
                "calc.out",
                "--json",
            ],
        )

        assert result.exit_code == 0, result.output
        assert captured["host"] == "host_d"
        assert captured["args"] == [
            "tail",
            "localhost",
            "ab12cd34ef56",
            "--name",
            "calc.out",
            "-n",
            "50",
            "--json",
        ]
        payload = json.loads(result.output)
        assert payload["host"] == "host_d"
        assert payload["queue_handle"]["host"] == "host_d"

    def test_explicit_host_form(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """`vq tail HOST JOBID` (2-positional) explicit-host form."""
        (state_dir / "cfg" / "config.toml").write_text(
            '[hosts.host_d]\n'
            'ssh = "host_d"\n'
            'remote_vq = "/home/USER/vq/.venv/bin/vq"\n'
        )
        captured: list[tuple[str, list[str]]] = []
        monkeypatch.setattr(
            "os.execvp",
            lambda prog, argv: captured.append((prog, argv)),
        )
        result = CliRunner().invoke(
            main, ["tail", "host_d", "ab12cd34ef56"]
        )
        assert result.exit_code == 0
        prog, argv = captured[0]
        assert prog == "ssh"
        assert argv[-2] == "host_d"
        remote = shlex.split(argv[-1])
        assert "ab12cd34ef56" in remote


class TestTailSchedulerHost:
    """Scheduler-host case: live jobs read the cluster workspace via driver."""

    def _scheduler_config(self, state_dir: Path) -> None:
        (state_dir / "cfg" / "config.toml").write_text(
            "[hosts.host_f]\n"
            'ssh = "host_f-login"\n'
            'scheduler = "pbs"\n'
            'scheduler_dialect = "torque"\n'
            'scratch_root = "/home/USER"\n'
            'scheduler_driver = "localhost"\n'
            "\n"
            "[hosts.localhost]\n"
            'ssh = "localhost"\n'
        )

    def _write_scheduler_spec(
        self,
        jobid: str,
        *,
        state: JobState = JobState.RUNNING,
        scheduler_job_id: str | None = "123.cluster",
    ) -> None:
        ws = _make_workspace(jobid, {"stdout.log": "staged\n"})
        JobSpec(
            id=jobid,
            command=["true"],
            cwd=str(ws),
            cpus=1,
            state=state,
            scheduler_target="host_f",
            scheduler_job_id=scheduler_job_id,
        ).write(paths.queue_dir() / f"{jobid}.json")

    def test_live_scheduler_tail_reads_remote_workspace_file(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._scheduler_config(state_dir)
        self._write_scheduler_spec("abc123def456")
        spec_path = paths.queue_dir() / "abc123def456.json"
        spec = JobSpec.read(spec_path)
        spec.array_index = 2
        spec.array_total = 5
        spec.array_group_id = "tail-array"
        spec.write(spec_path)
        captured: dict[str, object] = {}

        class FakeDispatcher:
            def remote_workspace(self, jobid: str) -> str:
                return f"/remote/{jobid}"

            def tail_file(self, handle, *, filename: str, lines: int | None):
                captured["handle"] = handle
                captured["filename"] = filename
                captured["lines"] = lines
                return "SCF 12\n"

        monkeypatch.setattr("vq.cli.scheduler_dispatcher_for", lambda host_cfg: FakeDispatcher())
        monkeypatch.setattr(
            "os.execvp",
            lambda prog, argv: (_ for _ in ()).throw(AssertionError("must not exec")),
        )

        result = CliRunner().invoke(
            main,
            ["tail", "host_f", "abc123def456", "--name", "calc.out", "-n", "25"],
        )

        assert result.exit_code == 0, result.output
        assert result.output == "SCF 12\n"
        assert captured["filename"] == "calc.out"
        assert captured["lines"] == 25
        assert captured["handle"].job_id == "123.cluster"  # type: ignore[union-attr]
        assert captured["handle"].remote_workspace == "/remote/abc123def456"  # type: ignore[union-attr]
        assert captured["handle"].array_size is None  # type: ignore[union-attr]

    def test_scheduler_tail_refuses_foreign_multi_user_job_before_dispatch(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._scheduler_config(state_dir)
        with (state_dir / "cfg" / "config.toml").open("a") as handle:
            handle.write(
                "\n[multi_user]\n"
                "enabled = true\n"
                'admin_group = "nonexistent-vq-test-group"\n'
            )
        monkeypatch.setattr(
            config, "SYSTEM_CONFIG_PATH", state_dir / "absent-system.toml"
        )
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(state_dir / "multi"))
        jobid = "foreign-scheduler-tail"
        workspace = paths.user_workspace_dir("2002", jobid)
        workspace.mkdir(parents=True)
        (workspace / "stdout.log").write_text("private scheduler output\n")
        JobSpec(
            id=jobid,
            command=["true"],
            cwd=str(workspace),
            cpus=1,
            submitter="2002",
            state=JobState.RUNNING,
            scheduler_target="host_f",
            scheduler_job_id="123.cluster",
        ).write(paths.user_spec_path("2002", jobid))
        monkeypatch.setattr(ownership, "_caller_uid", lambda: 1001)
        monkeypatch.setattr(ownership, "_caller_is_admin", lambda _cfg: False)
        monkeypatch.setattr(
            "vq.cli.scheduler_dispatcher_for",
            lambda _host_cfg: pytest.fail("authorization must precede dispatch"),
        )

        result = CliRunner().invoke(main, ["tail", "host_f", jobid])

        assert isinstance(result.exception, ownership.OwnershipError)
        assert "private scheduler output" not in result.output

    def test_missing_scheduler_id_uses_the_staged_local_workspace(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._scheduler_config(state_dir)
        jobid = "abc123def462"
        self._write_scheduler_spec(jobid, scheduler_job_id=None)
        captured: list[tuple[str, list[str]]] = []
        monkeypatch.setattr(
            "vq.cli.scheduler_dispatcher_for",
            lambda _host_cfg: pytest.fail(
                "a missing scheduler id must not build a dispatcher"
            ),
        )
        monkeypatch.setattr(
            "os.execvp",
            lambda prog, argv: captured.append((prog, argv)),
        )

        result = CliRunner().invoke(main, ["tail", "host_f", jobid])

        assert result.exit_code == 0, result.output
        assert captured[0][0] == "tail"
        assert captured[0][1][-1].endswith(f"{jobid}/stdout.log")

    def test_empty_scheduler_id_still_uses_the_remote_workspace(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._scheduler_config(state_dir)
        jobid = "abc123def463"
        self._write_scheduler_spec(jobid, scheduler_job_id="")
        captured: list[tuple[str, str, int | None]] = []

        class FakeDispatcher:
            def remote_workspace(self, candidate: str) -> str:
                return f"/remote/{candidate}"

            def tail_file(self, handle, *, filename: str, lines: int | None):
                captured.append(
                    (handle.job_id, handle.remote_workspace, handle.array_size)
                )
                return "remote\n"

        monkeypatch.setattr(
            "vq.cli.scheduler_dispatcher_for",
            lambda _host_cfg: FakeDispatcher(),
        )
        monkeypatch.setattr(
            "os.execvp",
            lambda prog, argv: (_ for _ in ()).throw(
                AssertionError("an empty id currently takes the remote path")
            ),
        )

        result = CliRunner().invoke(main, ["tail", "host_f", jobid])

        assert result.exit_code == 0, result.output
        assert captured == [("", f"/remote/{jobid}", None)]

    def test_live_scheduler_tail_json_describes_remote_workspace(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._scheduler_config(state_dir)
        self._write_scheduler_spec("abc123def460")
        captured: dict[str, object] = {}

        class FakeDispatcher:
            def remote_workspace(self, jobid: str) -> str:
                return f"/remote/{jobid}"

            def tail_file(self, handle, *, filename: str, lines: int | None):
                captured["handle"] = handle
                captured["filename"] = filename
                captured["lines"] = lines
                return "SCF 12\n"

        monkeypatch.setattr("vq.cli.scheduler_dispatcher_for", lambda host_cfg: FakeDispatcher())
        monkeypatch.setattr(
            "os.execvp",
            lambda prog, argv: (_ for _ in ()).throw(AssertionError("must not exec")),
        )

        result = CliRunner().invoke(
            main,
            [
                "tail",
                "host_f",
                "abc123def460",
                "--name",
                "calc.out",
                "-n",
                "25",
                "--json",
            ],
        )

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["jobid"] == "abc123def460"
        assert payload["host"] == "host_f"
        assert payload["state"] == "running"
        assert payload["filename"] == "calc.out"
        assert payload["tail"] == 25
        assert payload["text"] == "SCF 12"
        assert payload["path"] == "/remote/abc123def460/calc.out"
        assert payload["remote_workspace"] == "/remote/abc123def460"
        assert payload["live_scheduler_workspace"] is True
        assert payload["scheduler_target"] == "host_f"
        assert payload["scheduler_job_id"] == "123.cluster"
        assert payload["queue_handle"]["host"] == "host_f"
        assert captured["filename"] == "calc.out"
        assert captured["lines"] == 25

    def test_live_scheduler_tail_zero_lines_requests_whole_remote_file(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._scheduler_config(state_dir)
        self._write_scheduler_spec("abc123def457")
        captured: dict[str, object] = {}

        class FakeDispatcher:
            def remote_workspace(self, jobid: str) -> str:
                return f"/remote/{jobid}"

            def tail_file(self, handle, *, filename: str, lines: int | None):
                captured["lines"] = lines
                return "all\n"

        monkeypatch.setattr("vq.cli.scheduler_dispatcher_for", lambda host_cfg: FakeDispatcher())

        result = CliRunner().invoke(
            main, ["tail", "host_f", "abc123def457", "--name", "calc.out", "-n", "0"]
        )

        assert result.exit_code == 0, result.output
        assert captured["lines"] is None

    def test_live_scheduler_follow_streams_remote_file_until_terminal(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._scheduler_config(state_dir)
        self._write_scheduler_spec("abc123def458")
        spec_path = paths.queue_dir() / "abc123def458.json"
        sleeps = 0
        offsets: list[int] = []

        class FakeDispatcher:
            def remote_workspace(self, jobid: str) -> str:
                return f"/remote/{jobid}"

            def tail_file_since(self, handle, *, filename: str, byte_offset: int):
                assert filename == "calc.out"
                offsets.append(byte_offset)
                if byte_offset == 0:
                    return SchedulerFileChunk(6, b"SCF 1\n")
                if byte_offset == 6:
                    return SchedulerFileChunk(12, b"SCF 2\n")
                return SchedulerFileChunk(12, b"")

        def fake_sleep(_seconds: float) -> None:
            nonlocal sleeps
            sleeps += 1
            if sleeps == 2:
                spec = JobSpec.read(spec_path)
                spec.state = JobState.COMPLETED
                spec.finished_at = "2026-07-03T10:00:00+00:00"
                spec.write(spec_path)

        monkeypatch.setattr("vq.cli.scheduler_dispatcher_for", lambda host_cfg: FakeDispatcher())
        monkeypatch.setattr("vq.cli.time.sleep", fake_sleep)
        monkeypatch.setattr(
            "os.execvp",
            lambda prog, argv: (_ for _ in ()).throw(AssertionError("must not exec")),
        )

        result = CliRunner().invoke(
            main,
            ["tail", "host_f", "abc123def458", "--name", "calc.out", "-f"],
        )

        assert result.exit_code == 0, result.output
        assert result.output == "SCF 1\nSCF 2\n"
        assert offsets[:3] == [0, 6, 12]
        assert sleeps >= 3

    def test_terminal_scheduler_tail_uses_staged_local_workspace(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._scheduler_config(state_dir)
        self._write_scheduler_spec(
            "abc123def459",
            state=JobState.COMPLETED,
            scheduler_job_id="123.cluster",
        )
        captured: list[tuple[str, list[str]]] = []
        monkeypatch.setattr(
            "os.execvp",
            lambda prog, argv: captured.append((prog, argv)),
        )

        result = CliRunner().invoke(main, ["tail", "host_f", "abc123def459"])

        assert result.exit_code == 0, result.output
        assert captured[0][0] == "tail"
        assert captured[0][1][-1].endswith("abc123def459/stdout.log")


class TestTailHelp:
    def test_help_mentions_follow_and_lines(self) -> None:
        result = CliRunner().invoke(main, ["tail", "--help"])
        assert result.exit_code == 0
        assert "--follow" in result.output or "-f" in result.output
        assert "--lines" in result.output or "-n" in result.output
        assert "--name" in result.output

    def test_help_mentions_vibeqc_logger_use_case(self) -> None:
        result = CliRunner().invoke(main, ["tail", "--help"])
        assert result.exit_code == 0
        # Should mention the vibe-qc logger use case so chats find it
        assert "vibe-qc" in result.output.lower() or "vibeqc" in result.output

    def test_top_level_help_lists_tail(self) -> None:
        result = CliRunner().invoke(main, ["--help"])
        assert result.exit_code == 0
        assert "tail" in result.output
