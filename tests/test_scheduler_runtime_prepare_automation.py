"""Air-gapped runtime updates feed the mirror and prepare automatically.

host_f's runbook required two manual commands before every runtime update:
push the exact pin into the login host's push-fed mirror, then run
``prepare-host_f-runtime-source``. With ``feed_source_mirror`` and
``prepare_command`` on the deployment profile, ``vq admin update PROGRAM
HOST`` performs both — feed from the driver's configured source repo,
preparation on the login host under marker heartbeats, output teed into
the transcript — after quiescence and before the build, failing closed
with the current runtime intact. This closes fleet-automation gap 2: a
release rollout needs no per-release human steps on host_f.
"""
from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from vq import admin, config, paths
from vq.scheduler_dialect import SchedulerPhase

SHA = "d" * 40
TAG = "v0.15.58"
MIRROR = ".local/share/vq-host_f/vibeqc.git"
PREPARER = "~/.local/libexec/vq-host_f/bin/prepare-host_f-runtime-source"
MIRROR_ABS = f"/home/USER/{MIRROR}"
_MIRROR_PROBE_OK = (
    f"state=present\nbare=true\nabs={MIRROR_ABS}\n"
    f"origin={MIRROR_ABS}\norigin_abs={MIRROR_ABS}\n"
)


@pytest.fixture
def state_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    paths.jobs_dir().mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(
        "vq.admin._poll_scheduler_phases",
        lambda host_cfg, specs: {
            str(s.scheduler_job_id): SchedulerPhase.RUNNING for s in specs
        },
    )
    return tmp_path


def _write_config(
    cfg_dir: Path,
    *,
    source_repo: str | None,
    program: str = "vibeqc-release",
) -> None:
    lines = [
        'default_host = "localhost"',
        "",
        "[hosts.localhost]",
        'ssh = "localhost"',
        "",
        "[hosts.host_f]",
        'ssh = "host_f-login"',
        'scheduler = "pbs"',
        'scheduler_dialect = "torque"',
        'scratch_root = "/home/USER"',
        'scheduler_driver = "localhost"',
        'fleet_role = "managed"',
        "",
        f"[hosts.host_f.scheduler_runtime_deployments.{program}]",
        'update_command = "/site/bin/deploy-host_f-runtime"',
        'verify_command = "/site/bin/verify-host_f-runtime"',
        'update_host = "host_f-login_node"',
        f'feed_source_mirror = "{MIRROR}"',
        f'prepare_command = "{PREPARER}"',
        "",
        f"[programs.{program}]",
        'kind = "binary"',
        'binary = "/opt/vibeqc/bin/vibeqc"',
        "",
    ]
    if source_repo is not None:
        lines.insert(1, f'scheduler_runtime_source_repo = "{source_repo}"')
        # Each program is staged from its own repository since the split, so
        # the viewer needs its own checkout -- feeding it from a vibe-qc clone
        # is exactly the bug this mapping prevents. The fixture points every
        # slug at one path because it only scripts git, never reads a tree.
        lines.insert(2, "[pin_source_repos]")
        for slug in ("mpei/vibe-qc", "mpei/vibe-view", "mpei/vibe-queue"):
            lines.insert(3, f'"{slug}" = "{source_repo}"')
    (cfg_dir / "config.toml").write_text("\n".join(lines))


class _Fleet:
    """Scripted git (local subprocess) + remote command sides."""

    def __init__(
        self,
        *,
        program: str = "vibeqc-release",
        tag: str | None = TAG,
        push_rc: int = 0,
        prepare_rc: int = 0,
        prepare_stderr: str = "main moved",
        mirror_probe: str | None = None,
    ) -> None:
        self.mirror_probe = (
            _MIRROR_PROBE_OK if mirror_probe is None else mirror_probe
        )
        self.git_calls: list[list[str]] = []
        self.remote_calls: list[tuple[str, tuple[str, ...]]] = []
        self.program = program
        self.tag = tag
        self.push_rc = push_rc
        self.prepare_rc = prepare_rc
        self.prepare_stderr = prepare_stderr

    def run_git(self, argv, **kwargs):  # type: ignore[no-untyped-def]
        assert argv[0] == "git", argv
        self.git_calls.append(list(argv))
        rc = self.push_rc if "push" in argv else 0
        return subprocess.CompletedProcess(
            args=list(argv), returncode=rc,
            stdout="", stderr="pushed" if "push" in argv else "",
        )

    def run_remote(self, host_cfg, *argv, **kwargs):  # type: ignore[no-untyped-def]
        self.remote_calls.append((host_cfg.ssh, argv))
        if "vq-mirror-probe" in argv:
            return subprocess.CompletedProcess(
                args=list(argv), returncode=0,
                stdout=self.mirror_probe, stderr="",
            )
        if any("prepare-host_f-runtime-source" in a for a in argv):
            return subprocess.CompletedProcess(
                args=list(argv), returncode=self.prepare_rc,
                stdout="prepared exact offline bundle\n"
                if self.prepare_rc == 0 else "",
                stderr="" if self.prepare_rc == 0 else self.prepare_stderr,
            )
        if host_cfg.ssh == "host_f-login_node":
            return subprocess.CompletedProcess(
                args=list(argv), returncode=0, stdout="built\n", stderr="",
            )
        receipt = {
            "program": self.program, "source_sha": SHA, "tag": self.tag,
            "healthy": True, "activation": "atomic", "quiescent": True,
            "updater_pid": None,
            "active_path": "/site/runtimes/vibeqc-release/current",
            "health_detail": "import ok",
        }
        return subprocess.CompletedProcess(
            args=list(argv), returncode=0,
            stdout=json.dumps(receipt), stderr="",
        )


def _update(
    fleet: _Fleet,
    monkeypatch: pytest.MonkeyPatch,
    cfg,
    *,
    program: str = "vibeqc-release",
    tag: str | None = TAG,
):  # type: ignore[no-untyped-def]
    monkeypatch.setattr(
        "vq.admin._canonical_lifecycle_checkout",
        lambda path: Path(path),
    )
    monkeypatch.setattr("vq.admin.subprocess.run", fleet.run_git)
    monkeypatch.setattr("vq.admin.transport.run_remote_shell", fleet.run_remote)
    return admin.update_scheduler_runtime(
        "host_f", program, cfg, expected_sha=SHA, expected_tag=tag,
    )


def test_feed_and_prepare_run_before_the_build(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """THE GAP-2 CONTRACT: pin fed, bundle prepared, then built — one verb."""
    _write_config(state_dir / "cfg", source_repo="/srv/vibeqc-clone")
    cfg = config.load_config()
    fleet = _Fleet()

    result = _update(fleet, monkeypatch, cfg)

    assert result.success is True, result.work_errors
    assert result.mirror_fed is True
    assert result.prepare_rc == 0
    push = next(c for c in fleet.git_calls if "push" in c)
    assert f"+{SHA}:refs/heads/main" not in push
    assert f"+{SHA}:refs/remotes/origin/main" not in push
    assert f"+{SHA}:refs/heads/release" in push
    assert f"+{SHA}:refs/remotes/origin/release" in push
    assert "+refs/tags/*:refs/tags/*" in push
    assert f"host_f-login:{MIRROR}" in push
    prepare_idx = next(
        i for i, (ssh, argv) in enumerate(fleet.remote_calls)
        if any("prepare-host_f-runtime-source" in a for a in argv)
    )
    deploy_idx = next(
        i for i, (ssh, argv) in enumerate(fleet.remote_calls)
        if ssh == "host_f-login_node"
    )
    assert prepare_idx < deploy_idx, "preparation must precede the build"
    prep_ssh, prep_argv = fleet.remote_calls[prepare_idx]
    assert prep_ssh == "host_f-login", "preparation is login-node work"
    assert tuple(
        prep_argv[prep_argv.index("--program"):][:2]
    ) == ("--program", "vibeqc-release")
    assert SHA in prep_argv
    assert TAG in prep_argv
    transcript = Path(result.run_log_path or "").read_text()
    assert "mirror feed push" in transcript
    assert "--- prepare command output (rc=0) ---" in transcript


@pytest.mark.parametrize(
    "program", ["vibeqc-dev", "vibeqc-skala-dev", "vibe-view"]
)
def test_branch_tip_feed_does_not_clobber_release_refs(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    program: str,
) -> None:
    _write_config(
        state_dir / "cfg",
        source_repo="/srv/vibeqc-clone",
        program=program,
    )
    cfg = config.load_config()
    fleet = _Fleet(program=program, tag=None)

    result = _update(
        fleet,
        monkeypatch,
        cfg,
        program=program,
        tag=None,
    )

    assert result.success is True, result.work_errors
    push = next(c for c in fleet.git_calls if "push" in c)
    assert f"+{SHA}:refs/heads/main" in push
    assert f"+{SHA}:refs/remotes/origin/main" in push
    assert f"+{SHA}:refs/heads/release" not in push
    assert f"+{SHA}:refs/remotes/origin/release" not in push
    assert "+refs/tags/*:refs/tags/*" in push


def test_prepare_failure_leaves_the_runtime_untouched(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(state_dir / "cfg", source_repo="/srv/vibeqc-clone")
    cfg = config.load_config()
    fleet = _Fleet(prepare_rc=1)

    result = _update(fleet, monkeypatch, cfg)

    assert result.success is False
    assert any("prepare command rc=1" in e for e in result.work_errors)
    assert not any(ssh == "host_f-login_node" for ssh, _ in fleet.remote_calls), (
        "a failed preparation must never reach the build host"
    )
    admin.clear_admin_update_marker()


def test_push_failure_fails_closed_before_preparing(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(state_dir / "cfg", source_repo="/srv/vibeqc-clone")
    cfg = config.load_config()
    fleet = _Fleet(push_rc=1)

    result = _update(fleet, monkeypatch, cfg)

    assert result.success is False
    assert any("git push failed" in e for e in result.work_errors)
    # The mirror precondition probe runs before the feed and is read-only;
    # what must not happen after a failed feed is preparation or a build.
    assert not any(
        ssh == "host_f-login_node"
        or any("prepare-host_f-runtime-source" in a for a in argv)
        for ssh, argv in fleet.remote_calls
    ), "no remote work may follow a failed feed"
    admin.clear_admin_update_marker()


def test_feed_without_source_repo_fails_closed(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(state_dir / "cfg", source_repo=None)
    cfg = config.load_config()
    fleet = _Fleet()

    result = _update(fleet, monkeypatch, cfg)

    assert result.success is False
    # The message now names the program and the repository it needs, since
    # "no source repo" is per-program rather than one global setting.
    assert any(
        "no local checkout is configured" in e and "mpei/" in e
        for e in result.work_errors
    )
    assert fleet.git_calls == []
    admin.clear_admin_update_marker()


class TestPrepareOutputReachesTheOperator:
    """A prepare failure must not read as a broken host.

    host_f failed twice during the 2026-09-10 migration, ``rc=2`` and
    ``rc=128``, and the whole summary was::

        -- work errors --
           prepare command rc=2

    with ``(no output)`` under deploy and verification -- correctly, since
    neither had run -- and nothing under prepare, because its output went only
    to the run log. Both causes were one line of stderr, and finding either
    meant shelling into host_f and running the preparer by hand.
    """

    def test_the_work_error_carries_what_the_preparer_said(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_config(state_dir / "cfg", source_repo="/srv/vibeqc-clone")
        cfg = config.load_config()
        fleet = _Fleet(
            prepare_rc=2, prepare_stderr="vibeqc-release requires --tag",
        )

        result = _update(fleet, monkeypatch, cfg)

        assert result.success is False
        assert result.prepare_rc == 2
        assert "vibeqc-release requires --tag" in result.prepare_output
        assert result.work_errors == [
            "prepare command rc=2: vibeqc-release requires --tag"
        ]
        admin.clear_admin_update_marker()

    def test_a_missing_mirror_origin_is_legible_without_shelling_in(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The rc=128 half: a hand-created bare mirror has no ``origin``."""
        _write_config(state_dir / "cfg", source_repo="/srv/vibeqc-clone")
        cfg = config.load_config()
        fleet = _Fleet(
            prepare_rc=128,
            prepare_stderr=(
                "fetching origin\n"
                "fatal: 'origin' does not appear to be a git repository"
            ),
        )

        result = _update(fleet, monkeypatch, cfg)

        rendered = admin.format_scheduler_runtime_update_result(result)
        assert "-- prepare command (rc=128) --" in rendered
        assert "fatal: 'origin' does not appear to be a git repository" in rendered
        # The other two sections stay empty, and that is correct: neither ran.
        # The point is that the transcript no longer stops there.
        assert "-- deploy command (rc=None) --\n(no output)" in rendered
        assert "-- verification command (rc=None) --\n(no output)" in rendered
        # The last line is the cause, not the progress line before it.
        assert any(
            e.endswith(": fatal: 'origin' does not appear to be a git repository")
            for e in result.work_errors
        )
        admin.clear_admin_update_marker()

    def test_a_successful_prepare_is_shown_too(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_config(state_dir / "cfg", source_repo="/srv/vibeqc-clone")
        cfg = config.load_config()

        result = _update(_Fleet(), monkeypatch, cfg)

        assert result.success is True, result.work_errors
        rendered = admin.format_scheduler_runtime_update_result(result)
        assert "-- prepare command (rc=0) --" in rendered
        assert "prepared exact offline bundle" in rendered

    def test_a_deployment_with_no_preparer_grows_no_empty_section(self) -> None:
        result = admin.SchedulerRuntimeUpdateResult(
            host="host_b", program="vibeqc-dev", mode="update",
            command="deploy", command_ssh="host_b", verify_command="verify",
            verify_ssh="host_b", expected_sha="a" * 40,
        )
        assert "prepare command" not in (
            admin.format_scheduler_runtime_update_result(result)
        )

    def test_prepare_output_is_in_the_json_payload(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``--json`` consumers get it without parsing the human summary."""
        _write_config(state_dir / "cfg", source_repo="/srv/vibeqc-clone")
        cfg = config.load_config()
        fleet = _Fleet(prepare_rc=2, prepare_stderr="vibeqc-release requires --tag")

        result = _update(fleet, monkeypatch, cfg)

        payload = json.loads(
            admin.format_scheduler_runtime_update_result_json(result)
        )
        assert "vibeqc-release requires --tag" in payload["prepare_output"]
        admin.clear_admin_update_marker()


class TestPushFedMirrorPrecondition:
    """`feed_source_mirror` is a convention vq neither creates nor checked.

    A mirror made with `git init --bare` has no `origin`, and the preparer
    detects a push-fed mirror by whether `origin` names the mirror's own path.
    So the feed succeeds, the preparation dies with `fatal: 'origin' does not
    appear to be a git repository`, and a setup mistake reads as a source
    problem several minutes into a deployment.
    """

    def _run(self, state_dir, monkeypatch, probe):  # type: ignore[no-untyped-def]
        _write_config(state_dir / "cfg", source_repo="/srv/vibeqc-clone")
        cfg = config.load_config()
        fleet = _Fleet(mirror_probe=probe)
        result = _update(fleet, monkeypatch, cfg)
        admin.clear_admin_update_marker()
        return fleet, result

    def test_a_bare_mirror_without_origin_is_caught_before_the_feed(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fleet, result = self._run(
            state_dir,
            monkeypatch,
            f"state=present\nbare=true\nabs={MIRROR_ABS}\norigin=\norigin_abs=\n",
        )

        assert result.success is False
        error = "\n".join(result.work_errors)
        assert "has no origin" in error
        assert "fatal: 'origin' does not appear to be a git repository" in error
        assert f"git -C {MIRROR} remote add origin {MIRROR_ABS}" in error
        assert fleet.git_calls == [], "nothing is pushed into an unusable mirror"
        assert not any(
            any("prepare-host_f-runtime-source" in a for a in argv)
            for _, argv in fleet.remote_calls
        )

    def test_the_remedy_survives_a_path_that_needs_quoting(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The remedy is meant to be pasted, so a shell must parse it back
        into the argv it describes. A command that runs and does the wrong
        thing is worse than no remedy at all."""
        import shlex

        awkward = "/srv/vq mirrors/vibeqc.git"
        _write_config(state_dir / "cfg", source_repo="/srv/vibeqc-clone")
        cfg = config.load_config()
        deployment = cfg.hosts["host_f"].scheduler_runtime_deployments[
            "vibeqc-release"
        ]
        object.__setattr__(deployment, "feed_source_mirror", awkward)
        fleet = _Fleet(mirror_probe="state=missing\n")

        result = _update(fleet, monkeypatch, cfg)
        admin.clear_admin_update_marker()

        remedy = next(
            line.strip()
            for error in result.work_errors
            for line in error.splitlines()
            if line.strip().startswith("ssh ")
        )
        argv = shlex.split(remedy)
        assert argv[0] == "ssh"
        # The whole git command is one argument to ssh, and the remote shell
        # then parses it back into a path it can use.
        assert shlex.split(argv[2])[:4] == [
            "git", "init", "--bare", awkward,
        ]

    def test_a_missing_mirror_says_how_to_create_one(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _fleet, result = self._run(state_dir, monkeypatch, "state=missing\n")

        error = "\n".join(result.work_errors)
        assert "does not exist" in error
        assert f"git init --bare {MIRROR}" in error
        assert f"git -C {MIRROR} remote add origin {MIRROR}" in error

    def test_a_fetch_fed_mirror_is_named_as_such_not_repointed(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An origin elsewhere is a different valid arrangement, not damage:
        vq reports the contradiction rather than converting it."""
        upstream = "https://gitlab.example.com/mpei/vibe-qc.git"
        _fleet, result = self._run(
            state_dir,
            monkeypatch,
            f"state=present\nbare=true\nabs={MIRROR_ABS}\n"
            f"origin={upstream}\norigin_abs={upstream}\n",
        )

        error = "\n".join(result.work_errors)
        assert upstream in error
        assert "fetch-fed" in error
        assert "Drop feed_source_mirror" in error

    def test_a_working_checkout_is_not_a_mirror(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _fleet, result = self._run(
            state_dir,
            monkeypatch,
            f"state=present\nbare=false\nabs={MIRROR_ABS}\n"
            f"origin={MIRROR_ABS}\norigin_abs={MIRROR_ABS}\n",
        )

        assert "not a bare repository" in "\n".join(result.work_errors)

    def test_a_probe_that_could_not_run_fails_closed(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_config(state_dir / "cfg", source_repo="/srv/vibeqc-clone")
        cfg = config.load_config()
        fleet = _Fleet()

        def broken_probe(host_cfg, *argv, **kwargs):  # type: ignore[no-untyped-def]
            fleet.remote_calls.append((host_cfg.ssh, argv))
            return subprocess.CompletedProcess(
                args=list(argv), returncode=255, stdout="",
                stderr="ssh: connect to host host_f-login port 22: timed out",
            )

        monkeypatch.setattr(
            "vq.admin._canonical_lifecycle_checkout", lambda path: Path(path),
        )
        monkeypatch.setattr("vq.admin.subprocess.run", fleet.run_git)
        monkeypatch.setattr("vq.admin.transport.run_remote_shell", broken_probe)
        result = admin.update_scheduler_runtime(
            "host_f", "vibeqc-release", cfg, expected_sha=SHA, expected_tag=TAG,
        )

        assert result.success is False
        assert any("could not inspect" in e for e in result.work_errors)
        assert fleet.git_calls == []
        admin.clear_admin_update_marker()

    def test_a_push_fed_mirror_proceeds_untouched(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The check reads; it never writes to the mirror."""
        fleet, result = self._run(state_dir, monkeypatch, _MIRROR_PROBE_OK)

        assert result.success is True, result.work_errors
        probes = [
            argv for _, argv in fleet.remote_calls if "vq-mirror-probe" in argv
        ]
        assert len(probes) == 1
        assert not any(
            "remote add" in a or "remote set-url" in a or "init" in a
            for a in probes[0]
        )


class TestMirrorProbeAgainstRealRepositories:
    """Run the probe shell script itself, not a canned answer.

    `TestPushFedMirrorPrecondition` feeds `_Fleet` scripted probe output, so
    it exercises the verdicts and never the script that produces them. That
    gap hid a real one: a mirror whose `origin` is `.` -- the most natural way
    to write a self-referential remote -- was resolved against the calling
    shell's cwd instead of the repository, so a working push-fed mirror
    reported as fetch-fed and the feed refused it.
    """

    def _git(self, cwd: Path, *args: str) -> None:
        subprocess.run(
            ["git", *args], cwd=cwd, check=True, capture_output=True,
        )

    def _probe(self, mirror: Path, *, cwd: Path) -> dict[str, str]:
        proc = subprocess.run(
            ["sh", "-c", admin._PUSH_FED_MIRROR_PROBE, "vq-mirror-probe", str(mirror)],
            cwd=cwd,
            check=True,
            capture_output=True,
            text=True,
        )
        return dict(
            line.split("=", 1)
            for line in proc.stdout.splitlines()
            if "=" in line
        )

    def _bare(self, root: Path, name: str, origin: str | None) -> Path:
        path = root / name
        self._git(root, "init", "-q", "--bare", name)
        if origin is not None:
            self._git(path, "remote", "add", "origin", origin)
        return path

    @pytest.mark.parametrize("spelling", [".", "{abs}", "../{name}"])
    def test_every_self_referential_spelling_reads_as_push_fed(
        self, tmp_path: Path, spelling: str,
    ) -> None:
        root = tmp_path / "mirrors"
        root.mkdir()
        name = "vibeqc.git"
        mirror = self._bare(root, name, None)
        self._git(
            mirror,
            "remote",
            "add",
            "origin",
            spelling.format(abs=str(mirror), name=name),
        )

        # From a different cwd on purpose: vq runs this over ssh, where the
        # remote shell starts in $HOME and not in the mirror.
        fields = self._probe(mirror, cwd=tmp_path)

        assert fields["state"] == "present"
        assert fields["bare"] == "true"
        assert fields["origin_abs"] == fields["abs"], (
            "a self-referential origin must resolve to the mirror itself"
        )

    def test_a_bare_repo_without_origin_reports_none(self, tmp_path: Path) -> None:
        mirror = self._bare(tmp_path, "vibeqc.git", None)

        fields = self._probe(mirror, cwd=tmp_path)

        assert fields["origin"] == ""

    def test_a_url_origin_is_not_mistaken_for_the_mirror(
        self, tmp_path: Path,
    ) -> None:
        mirror = self._bare(
            tmp_path, "vibeqc.git", "https://gitlab.example.invalid/mpei/vibe-qc.git",
        )

        fields = self._probe(mirror, cwd=tmp_path)

        assert fields["origin_abs"] != fields["abs"]
        assert fields["origin_abs"].startswith("https://")

    def test_an_origin_naming_another_repository_is_fetch_fed(
        self, tmp_path: Path,
    ) -> None:
        other = self._bare(tmp_path, "upstream.git", None)
        mirror = self._bare(tmp_path, "vibeqc.git", str(other))

        fields = self._probe(mirror, cwd=tmp_path)

        assert fields["origin_abs"] != fields["abs"]

    def test_a_working_checkout_is_reported_not_bare(self, tmp_path: Path) -> None:
        checkout = tmp_path / "checkout"
        checkout.mkdir()
        self._git(checkout, "init", "-q")

        fields = self._probe(checkout, cwd=tmp_path)

        assert fields["state"] == "present"
        assert fields["bare"] == "false"

    def test_a_missing_or_unrelated_directory_is_distinguished(
        self, tmp_path: Path,
    ) -> None:
        plain = tmp_path / "plain"
        plain.mkdir()

        assert self._probe(tmp_path / "nope", cwd=tmp_path)["state"] == "missing"
        assert self._probe(plain, cwd=tmp_path)["state"] == "not-a-repo"
