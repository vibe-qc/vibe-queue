"""The runtime source-mirror feed no longer force-pushes anything (#81).

``_feed_and_prepare_runtime_source`` used to build ``+SHA:refs/heads/release``
(or ``main``), the matching forced remote-tracking ref, and
``+refs/tags/*:refs/tags/*``, then hand all of it to ``git push``. Three
things were wrong with that. It force-pushes, which the standing rule forbids
outright, so a deployment configured to use this path could not be run at
all. It mirrors *every* tag the driver happens to carry over every tag the
mirror happens to hold, which is far broader than feeding the one release
identity a deployment is about. And a forced branch update silently resolves
the one case that most needs a human: an accepted pin that is **older** than
whatever the mirror has since drifted to.

The feed now pushes unforced and ``--atomic``: the immutable per-pin ref
``refs/vq/pins/<sha>``, this lane's branch pair, and only the tags that name
this pin in the driver's clone. Anything that would need a force is refused
before the push, naming the mirror's own values, and the mirror is left
byte-identical.

Every mirror here is a disposable bare repository created by the test. The
remote side of the feed is redirected onto those local paths, so real git
decides every fast-forward question -- these tests would not catch a
regression if they only inspected argv.
"""
from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from vq import admin, config, paths
from vq.scheduler_dialect import SchedulerPhase

# Captured before any test can monkeypatch ``subprocess.run`` out from under
# the fixtures: the fake fleet needs a way to actually run git.
_REAL_RUN = subprocess.run

SSH = "cluster-login"
BUILD_SSH = "cluster-build"
PREPARER = "/site/libexec/prepare-runtime-source"
PROGRAM = "vibeqc-release"
PIN_TAG = "v-pin"
PIN_REF_PREFIX = "refs/vq/pins/"
"""Spelled out here rather than imported, so the tests that must fail on
the parent fail on the behaviour and not on a missing attribute."""
LEGACY_TAG = "v-legacy"
ACTIVE_PATH = "/site/runtimes/vibeqc-release/current"

_GIT_ENV = {
    "GIT_AUTHOR_NAME": "vq test",
    "GIT_AUTHOR_EMAIL": "vq@example.invalid",
    "GIT_COMMITTER_NAME": "vq test",
    "GIT_COMMITTER_EMAIL": "vq@example.invalid",
    "GIT_AUTHOR_DATE": "2026-01-01T00:00:00+00:00",
    "GIT_COMMITTER_DATE": "2026-01-01T00:00:00+00:00",
    "GIT_CONFIG_NOSYSTEM": "1",
    "GIT_TERMINAL_PROMPT": "0",
}


def _git(cwd: Path, *args: str) -> str:
    """Run git for real, outside the fleet's interception."""
    import os

    env = dict(os.environ)
    env.update(_GIT_ENV)
    proc = _REAL_RUN(
        ["git", "-c", "commit.gpgsign=false", "-c", "tag.gpgsign=false", *args],
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
        stdin=subprocess.DEVNULL,
    )
    if proc.returncode != 0:
        raise AssertionError(
            f"test setup: git {args} failed rc={proc.returncode}\n"
            f"{proc.stdout}{proc.stderr}"
        )
    return proc.stdout


def _refs(bare: Path) -> str:
    """A whole bare repository's ref state, as one comparable block.

    Name, value and object type for every ref: enough that a tag replaced by
    another tag object over the same commit still reads as a change.
    """
    return _git(
        bare, "for-each-ref", "--format=%(refname) %(objectname) %(objecttype)",
    )


def _ref(bare: Path, name: str) -> str | None:
    out = _git(bare, "for-each-ref", "--format=%(objectname)", name).strip()
    return out or None


class _Forge:
    """Disposable repositories: a driver clone, and mirrors to feed.

    The commit graph is::

        c0 ── c1 ── c2 ── c3        (main)
                \\
                 side

    with an annotated ``v-pin`` at ``c2`` -- the pin every test deploys -- and
    a lightweight ``v-legacy`` at ``c0`` that no deployment ever names.
    """

    def __init__(self, root: Path) -> None:
        self.root = root / "forge"
        self.root.mkdir(parents=True)
        work = self.root / "work"
        work.mkdir()
        _git(work, "init", "-q", "-b", "main")
        self.shas: list[str] = []
        for index in range(4):
            (work / "f").write_text(f"{index}\n")
            _git(work, "add", "f")
            _git(work, "commit", "-qm", f"c{index}")
            self.shas.append(_git(work, "rev-parse", "HEAD").strip())
        self.c0, self.c1, self.c2, self.c3 = self.shas
        _git(work, "checkout", "-q", "-b", "side", self.c1)
        (work / "g").write_text("side\n")
        _git(work, "add", "g")
        _git(work, "commit", "-qm", "side")
        self.side = _git(work, "rev-parse", "HEAD").strip()
        _git(work, "checkout", "-q", "main")
        _git(work, "tag", "-a", PIN_TAG, self.c2, "-m", "the deployed identity")
        _git(work, "tag", LEGACY_TAG, self.c0)
        self.work = work

        _git(self.root, "clone", "-q", "--bare", str(work), "upstream.git")
        self.upstream = self.root / "upstream.git"
        _git(self.root, "clone", "-q", str(self.upstream), "driver")
        self.driver = self.root / "driver"
        # The tag object the driver would feed, as opposed to the commit it
        # peels to: a push compares ref values, not peeled ones.
        self.pin_tag_object = _git(
            self.driver, "rev-parse", "--verify", f"refs/tags/{PIN_TAG}",
        ).strip()

    def mirror(self, name: str, refs: dict[str, str] | None = None) -> Path:
        """A fresh bare mirror seeded with ``refs`` -- creations only.

        Nothing in the setup forces anything either: every seeded ref is new
        in a repository that was empty a moment ago.
        """
        _git(self.root, "init", "-q", "--bare", name)
        mirror = self.root / name
        # The push-fed convention vq checks for: origin names the mirror.
        _git(mirror, "remote", "add", "origin", str(mirror))
        if refs:
            _git(
                self.work,
                "push",
                "-q",
                str(mirror),
                *(f"{value}:{ref}" for ref, value in sorted(refs.items())),
            )
        return mirror

    def unrelated(self) -> dict[str, str]:
        """Refs a deployment must never name, at values it must never change.

        ``v-legacy`` sits at a *different* commit here than in the driver's
        clone on purpose: mirroring every tag would move it.
        """
        return {
            "refs/heads/experimental": self.side,
            f"refs/tags/{LEGACY_TAG}": self.c3,
            f"{PIN_REF_PREFIX}{self.c1}": self.c1,
        }


@pytest.fixture
def forge(tmp_path: Path) -> _Forge:
    return _Forge(tmp_path)


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
    cfg_dir: Path, *, driver: Path, mirror: Path, program: str = PROGRAM,
) -> None:
    lines = [
        'default_host = "localhost"',
        f'scheduler_runtime_source_repo = "{driver}"',
        "[pin_source_repos]",
        f'"example-org/vibe-qc" = "{driver}"',
        f'"example-org/vibe-view" = "{driver}"',
        f'"example-org/vibe-queue" = "{driver}"',
        "",
        "[hosts.localhost]",
        'ssh = "localhost"',
        "",
        "[hosts.cluster]",
        f'ssh = "{SSH}"',
        'scheduler = "pbs"',
        'scheduler_dialect = "torque"',
        'scratch_root = "/scratch/vq"',
        'scheduler_driver = "localhost"',
        'fleet_role = "managed"',
        "",
        f"[hosts.cluster.scheduler_runtime_deployments.{program}]",
        'update_command = "/site/bin/deploy-runtime"',
        'verify_command = "/site/bin/verify-runtime"',
        f'update_host = "{BUILD_SSH}"',
        f'feed_source_mirror = "{mirror}"',
        f'prepare_command = "{PREPARER}"',
        "",
        f"[programs.{program}]",
        'kind = "binary"',
        'binary = "/opt/vibeqc/bin/vibeqc"',
        "",
    ]
    (cfg_dir / "config.toml").write_text("\n".join(lines))


class _Fleet:
    """Real git against a disposable mirror; scripted ssh.

    The only thing faked about the feed is the transport: an argument
    spelled ``<ssh-alias>:<path>`` is rewritten to that local path, and the
    rest of the command runs for real. So the fast-forward rules, the tag
    rules and the atomicity are git's, not the test's.
    """

    def __init__(
        self,
        mirror: Path,
        *,
        program: str = PROGRAM,
        receipt_sha: str | None = None,
        receipt_tag: str | None = None,
        prepare_rc: int = 0,
    ) -> None:
        self.mirror = mirror
        self.program = program
        self.receipt_sha = receipt_sha
        self.receipt_tag = receipt_tag
        self.prepare_rc = prepare_rc
        self.git_calls: list[list[str]] = []
        self.remote_calls: list[tuple[str, tuple[str, ...]]] = []

    @property
    def push_argv(self) -> list[str]:
        pushes = [call for call in self.git_calls if "push" in call]
        assert len(pushes) == 1, f"expected exactly one push, got {pushes}"
        return pushes[0]

    def run_git(self, argv, **kwargs):  # type: ignore[no-untyped-def]
        argv = list(argv)
        self.git_calls.append(list(argv))
        if argv and argv[0] == "git":
            argv = [
                str(self.mirror) if a == f"{SSH}:{self.mirror}" else a
                for a in argv
            ]
        kwargs.pop("pass_fds", None)
        return _REAL_RUN(argv, **kwargs)

    def run_remote(self, host_cfg, *argv, **kwargs):  # type: ignore[no-untyped-def]
        self.remote_calls.append((host_cfg.ssh, argv))
        if "vq-mirror-probe" in argv:
            body = (
                f"state=present\nbare=true\nabs={self.mirror}\n"
                f"origin={self.mirror}\norigin_abs={self.mirror}\n"
            )
            return subprocess.CompletedProcess(
                args=list(argv), returncode=0, stdout=body, stderr="",
            )
        if any(PREPARER in a for a in argv):
            return subprocess.CompletedProcess(
                args=list(argv),
                returncode=self.prepare_rc,
                stdout="prepared\n" if self.prepare_rc == 0 else "",
                stderr="" if self.prepare_rc == 0 else "preparer refused",
            )
        if host_cfg.ssh == BUILD_SSH:
            return subprocess.CompletedProcess(
                args=list(argv), returncode=0, stdout="built\n", stderr="",
            )
        receipt = {
            "program": self.program,
            "source_sha": self.receipt_sha,
            "tag": self.receipt_tag,
            "healthy": True,
            "activation": "atomic",
            "quiescent": True,
            "updater_pid": None,
            "active_path": ACTIVE_PATH,
            "health_detail": "import ok",
        }
        return subprocess.CompletedProcess(
            args=list(argv), returncode=0, stdout=json.dumps(receipt), stderr="",
        )

    @property
    def reached_the_build_host(self) -> bool:
        return any(ssh == BUILD_SSH for ssh, _ in self.remote_calls)

    @property
    def prepare_argv(self) -> tuple[str, ...] | None:
        for _ssh, argv in self.remote_calls:
            if any(PREPARER in a for a in argv):
                return argv
        return None


def _deploy(
    forge: _Forge,
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    mirror: Path,
    *,
    pin: str | None = None,
    tag: str | None = PIN_TAG,
    program: str = PROGRAM,
    receipt_sha: str | None = None,
    receipt_tag: str | None = None,
    prepare_rc: int = 0,
):  # type: ignore[no-untyped-def]
    pin = pin or forge.c2
    _write_config(
        state_dir / "cfg", driver=forge.driver, mirror=mirror, program=program,
    )
    cfg = config.load_config()
    fleet = _Fleet(
        mirror,
        program=program,
        receipt_sha=pin if receipt_sha is None else receipt_sha,
        receipt_tag=tag if receipt_tag is None else receipt_tag,
        prepare_rc=prepare_rc,
    )
    monkeypatch.setattr(
        "vq.admin._canonical_lifecycle_checkout", lambda path: Path(path),
    )
    monkeypatch.setattr("vq.admin.subprocess.run", fleet.run_git)
    monkeypatch.setattr("vq.admin.transport.run_remote_shell", fleet.run_remote)
    result = admin.update_scheduler_runtime(
        "cluster", program, cfg, expected_sha=pin, expected_tag=tag,
    )
    admin.clear_admin_update_marker()
    return fleet, result


class TestTheForcedRefspecsAreGone:
    """The parent reproduction: capture the argv, push nothing by force."""

    def test_no_refspec_is_forced(
        self, forge: _Forge, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mirror = forge.mirror("m.git", {"refs/heads/release": forge.c1})

        fleet, result = _deploy(forge, state_dir, monkeypatch, mirror)

        assert result.success is True, result.work_errors
        forced = [a for a in fleet.push_argv if a.startswith("+")]
        assert forced == [], f"the feed force-pushed: {forced}"
        assert "--force" not in fleet.push_argv
        assert "-f" not in fleet.push_argv
        assert "--force-with-lease" not in fleet.push_argv
        # And the same evidence is on the result, for a --json consumer.
        assert result.mirror_feed_refspecs
        assert not any(
            spec.startswith("+") for spec in result.mirror_feed_refspecs
        )

    def test_the_feed_does_not_mirror_every_tag(
        self, forge: _Forge, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Only the tags that name this pin, never a wildcard over all of them."""
        mirror = forge.mirror("m.git", {"refs/heads/release": forge.c1})

        fleet, result = _deploy(forge, state_dir, monkeypatch, mirror)

        assert result.success is True, result.work_errors
        assert not any("refs/tags/*" in a for a in fleet.push_argv), (
            "the feed still mirrors the whole tag namespace"
        )
        tag_specs = [
            spec for spec in result.mirror_feed_refspecs if "refs/tags/" in spec
        ]
        assert tag_specs == [f"refs/tags/{PIN_TAG}:refs/tags/{PIN_TAG}"]

    def test_the_push_is_atomic(
        self, forge: _Forge, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A half-fed lane -- branch moved, tracking ref not -- is what every
        preparer fails closed on, so the feed is all-or-nothing."""
        mirror = forge.mirror("m.git", {"refs/heads/release": forge.c1})

        fleet, _result = _deploy(forge, state_dir, monkeypatch, mirror)

        assert "--atomic" in fleet.push_argv


class TestTheCorrectedFeedStillFeeds:
    def test_an_absent_ref_is_created_without_force(
        self, forge: _Forge, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mirror = forge.mirror("m.git")

        fleet, result = _deploy(forge, state_dir, monkeypatch, mirror)

        assert result.success is True, result.work_errors
        assert result.mirror_fed is True
        assert _ref(mirror, "refs/heads/release") == forge.c2
        assert _ref(mirror, "refs/remotes/origin/release") == forge.c2
        assert _ref(mirror, f"refs/tags/{PIN_TAG}") == forge.pin_tag_object
        assert not any(a.startswith("+") for a in fleet.push_argv)

    def test_a_fast_forward_ref_is_advanced_without_force(
        self, forge: _Forge, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mirror = forge.mirror(
            "m.git",
            {
                "refs/heads/release": forge.c1,
                "refs/remotes/origin/release": forge.c1,
                **forge.unrelated(),
            },
        )

        fleet, result = _deploy(forge, state_dir, monkeypatch, mirror)

        assert result.success is True, result.work_errors
        assert _ref(mirror, "refs/heads/release") == forge.c2
        assert _ref(mirror, "refs/remotes/origin/release") == forge.c2
        assert not any(a.startswith("+") for a in fleet.push_argv)

    def test_a_branch_tip_lane_feeds_main_and_leaves_release_alone(
        self, forge: _Forge, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A dev feed must not move the release lane, forced or not."""
        mirror = forge.mirror(
            "m.git",
            {"refs/heads/main": forge.c1, "refs/heads/release": forge.c0},
        )

        _fleet, result = _deploy(
            forge,
            state_dir,
            monkeypatch,
            mirror,
            program="vibeqc-dev",
            tag=None,
            receipt_tag=None,
        )

        assert result.success is True, result.work_errors
        assert _ref(mirror, "refs/heads/main") == forge.c2
        assert _ref(mirror, "refs/remotes/origin/main") == forge.c2
        assert _ref(mirror, "refs/heads/release") == forge.c0

    def test_the_immutable_pin_ref_is_written(
        self, forge: _Forge, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mirror = forge.mirror("m.git", {"refs/heads/release": forge.c1})

        _fleet, result = _deploy(forge, state_dir, monkeypatch, mirror)

        assert admin.RUNTIME_PIN_REF_PREFIX == PIN_REF_PREFIX
        pin_ref = f"{PIN_REF_PREFIX}{forge.c2}"
        assert result.mirror_pin_ref == pin_ref
        assert _ref(mirror, pin_ref) == forge.c2

    def test_feeding_the_same_pin_twice_needs_no_force(
        self, forge: _Forge, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The per-pin ref is named after its own content, so a repeat feed is
        a no-op rather than something that has to be forced over."""
        mirror = forge.mirror("m.git", {"refs/heads/release": forge.c1})
        _deploy(forge, state_dir, monkeypatch, mirror)
        after_first = _refs(mirror)

        fleet, result = _deploy(forge, state_dir, monkeypatch, mirror)

        assert result.success is True, result.work_errors
        assert _refs(mirror) == after_first
        assert not any(a.startswith("+") for a in fleet.push_argv)


class TestDivergenceIsRefusedNotForced:
    def test_a_mirror_past_the_pin_is_refused_and_retained(
        self, forge: _Forge, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The case a mechanical ``+`` removal would have got wrong in the
        other direction, and the case forcing got wrong before: a legitimate
        older accepted pin must fail visibly, never silently pick up the
        newer tree the mirror drifted to."""
        mirror = forge.mirror(
            "m.git",
            {
                "refs/heads/release": forge.c3,
                "refs/remotes/origin/release": forge.c3,
                **forge.unrelated(),
            },
        )
        before = _refs(mirror)

        fleet, result = _deploy(forge, state_dir, monkeypatch, mirror)

        assert result.success is False
        assert _refs(mirror) == before, "a refusal must not touch the mirror"
        error = "\n".join(result.work_errors)
        assert "never force-pushes" in error
        assert "moved past this pin" in error
        assert forge.c3[:12] in error and forge.c2[:12] in error
        assert fleet.git_calls, "the driver still inspected the mirror"
        assert not any("push" in call for call in fleet.git_calls)
        assert not fleet.reached_the_build_host

    def test_a_divergent_branch_is_refused_and_retained(
        self, forge: _Forge, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mirror = forge.mirror(
            "m.git",
            {
                "refs/heads/release": forge.side,
                "refs/remotes/origin/release": forge.side,
                **forge.unrelated(),
            },
        )
        before = _refs(mirror)

        _fleet, result = _deploy(forge, state_dir, monkeypatch, mirror)

        assert result.success is False
        assert _refs(mirror) == before
        assert "diverged" in "\n".join(result.work_errors)

    def test_a_tag_the_mirror_holds_elsewhere_is_refused_and_retained(
        self, forge: _Forge, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A tag is an identity. vq neither moves one nor forces one."""
        mirror = forge.mirror(
            "m.git",
            {
                "refs/heads/release": forge.c1,
                f"refs/tags/{PIN_TAG}": forge.c3,
            },
        )
        before = _refs(mirror)

        _fleet, result = _deploy(forge, state_dir, monkeypatch, mirror)

        assert result.success is False
        assert _refs(mirror) == before
        assert _ref(mirror, f"refs/tags/{PIN_TAG}") == forge.c3
        error = "\n".join(result.work_errors)
        assert "not the tag object" in error
        assert "neither moves nor forces" in error

    def test_a_tag_that_does_not_name_the_pin_is_refused_at_the_source(
        self, forge: _Forge, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Feeding tag T for commit S when the driver's own clone says T names
        something else would have published a contradiction."""
        mirror = forge.mirror("m.git")

        fleet, result = _deploy(
            forge, state_dir, monkeypatch, mirror, tag=LEGACY_TAG,
        )

        assert result.success is False
        assert _refs(mirror) == ""
        error = "\n".join(result.work_errors)
        assert f"tag '{LEGACY_TAG}' does not name" in error
        assert not any("push" in call for call in fleet.git_calls)


class TestUnrelatedRefsAreUntouched:
    def test_a_successful_feed_leaves_every_other_ref_byte_identical(
        self, forge: _Forge, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Including a tag the driver's clone holds at a different commit:
        mirroring the whole tag namespace used to move it."""
        unrelated = forge.unrelated()
        mirror = forge.mirror(
            "m.git", {"refs/heads/release": forge.c1, **unrelated},
        )

        _fleet, result = _deploy(forge, state_dir, monkeypatch, mirror)

        assert result.success is True, result.work_errors
        for ref, value in unrelated.items():
            assert _ref(mirror, ref) == value, f"{ref} was not left alone"
        # The driver's own v-legacy is elsewhere; the mirror kept its own.
        assert _ref(forge.driver, f"refs/tags/{LEGACY_TAG}") == forge.c0
        assert _ref(mirror, f"refs/tags/{LEGACY_TAG}") == forge.c3

    def test_a_refused_feed_leaves_the_whole_mirror_byte_identical(
        self, forge: _Forge, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mirror = forge.mirror(
            "m.git", {"refs/heads/release": forge.c3, **forge.unrelated()},
        )
        before = _refs(mirror)

        _fleet, result = _deploy(forge, state_dir, monkeypatch, mirror)

        assert result.success is False
        assert _refs(mirror) == before


class TestThePreparerGetsTheRequestedIdentity:
    def test_the_preparer_receives_the_requested_sha_and_tag(
        self, forge: _Forge, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mirror = forge.mirror("m.git", {"refs/heads/release": forge.c1})

        fleet, result = _deploy(forge, state_dir, monkeypatch, mirror)

        assert result.success is True, result.work_errors
        argv = fleet.prepare_argv
        assert argv is not None
        assert list(argv[argv.index("--expected-sha"):][:2]) == [
            "--expected-sha", forge.c2,
        ]
        assert list(argv[argv.index("--tag"):][:2]) == ["--tag", PIN_TAG]
        assert list(argv[argv.index("--program"):][:2]) == ["--program", PROGRAM]

    def test_preparation_happens_after_the_feed_and_before_the_build(
        self, forge: _Forge, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mirror = forge.mirror("m.git", {"refs/heads/release": forge.c1})

        fleet, result = _deploy(forge, state_dir, monkeypatch, mirror)

        assert result.success is True, result.work_errors
        prepare_at = next(
            i for i, (_s, argv) in enumerate(fleet.remote_calls)
            if any(PREPARER in a for a in argv)
        )
        build_at = next(
            i for i, (ssh, _a) in enumerate(fleet.remote_calls) if ssh == BUILD_SSH
        )
        assert prepare_at < build_at

    def test_a_receipt_for_another_commit_still_fails_the_update(
        self, forge: _Forge, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The identity check the feed change must not weaken."""
        mirror = forge.mirror("m.git", {"refs/heads/release": forge.c1})

        _fleet, result = _deploy(
            forge, state_dir, monkeypatch, mirror, receipt_sha=forge.c3,
        )

        assert result.success is False
        assert result.actual_sha == forge.c3
        assert result.expected_sha == forge.c2

    def test_a_receipt_for_another_tag_still_fails_the_update(
        self, forge: _Forge, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mirror = forge.mirror("m.git", {"refs/heads/release": forge.c1})

        _fleet, result = _deploy(
            forge, state_dir, monkeypatch, mirror, receipt_tag="v-something-else",
        )

        assert result.success is False
        assert result.actual_tag == "v-something-else"

    def test_a_non_atomic_activation_receipt_still_fails_the_update(
        self, forge: _Forge, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The activation contract the feed change must not weaken either."""
        mirror = forge.mirror("m.git", {"refs/heads/release": forge.c1})
        _write_config(state_dir / "cfg", driver=forge.driver, mirror=mirror)
        cfg = config.load_config()
        fleet = _Fleet(mirror, receipt_sha=forge.c2, receipt_tag=PIN_TAG)
        original = fleet.run_remote

        def in_place(host_cfg, *argv, **kwargs):  # type: ignore[no-untyped-def]
            proc = original(host_cfg, *argv, **kwargs)
            if proc.stdout.startswith("{"):
                payload = json.loads(proc.stdout)
                payload["activation"] = "in-place"
                proc = subprocess.CompletedProcess(
                    args=proc.args, returncode=0,
                    stdout=json.dumps(payload), stderr="",
                )
            return proc

        monkeypatch.setattr(
            "vq.admin._canonical_lifecycle_checkout", lambda path: Path(path),
        )
        monkeypatch.setattr("vq.admin.subprocess.run", fleet.run_git)
        monkeypatch.setattr("vq.admin.transport.run_remote_shell", in_place)
        result = admin.update_scheduler_runtime(
            "cluster", PROGRAM, cfg, expected_sha=forge.c2, expected_tag=PIN_TAG,
        )
        admin.clear_admin_update_marker()

        assert result.success is False
        assert result.activation == "in-place"


class TestARefusedFeedChangesNothingElse:
    def _seed_last_ok(self, forge: _Forge) -> None:
        admin.record_scheduler_runtime_outcome(
            admin.SchedulerRuntimeUpdateResult(
                host="cluster",
                program=PROGRAM,
                mode="update",
                command="deploy",
                command_ssh=BUILD_SSH,
                verify_command="verify",
                verify_ssh=SSH,
                expected_sha=forge.c0,
                expected_tag=LEGACY_TAG,
                command_rc=0,
                verify_rc=0,
                actual_sha=forge.c0,
                actual_tag=LEGACY_TAG,
                healthy=True,
                activation="atomic",
                quiescent=True,
                active_path=ACTIVE_PATH,
            )
        )

    def test_the_canonical_last_ok_record_is_left_alone(
        self, forge: _Forge, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._seed_last_ok(forge)
        before = admin.load_scheduler_runtime_status()[f"cluster:{PROGRAM}"]
        mirror = forge.mirror("m.git", {"refs/heads/release": forge.c3})

        fleet, result = _deploy(forge, state_dir, monkeypatch, mirror)

        assert result.success is False
        after = admin.load_scheduler_runtime_status()[f"cluster:{PROGRAM}"]
        assert after.last_ok_sha == before.last_ok_sha == forge.c0
        assert after.last_ok_tag == before.last_ok_tag == LEGACY_TAG
        assert after.last_ok_at == before.last_ok_at
        assert after.last_ok_active_path == before.last_ok_active_path
        # And nothing claims a new runtime identity.
        assert after.actual_sha is None
        assert after.last_success is False
        assert not fleet.reached_the_build_host

    def test_no_deployment_or_verification_command_runs(
        self, forge: _Forge, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mirror = forge.mirror("m.git", {"refs/heads/release": forge.c3})

        fleet, result = _deploy(forge, state_dir, monkeypatch, mirror)

        assert result.success is False
        assert result.command_rc is None
        assert result.verify_rc is None
        assert result.prepare_rc is None
        assert not any(
            any(PREPARER in a for a in argv) for _s, argv in fleet.remote_calls
        )

    def test_the_transcript_records_what_would_have_been_pushed(
        self, forge: _Forge, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mirror = forge.mirror("m.git", {"refs/heads/release": forge.c1})

        _fleet, result = _deploy(forge, state_dir, monkeypatch, mirror)

        transcript = Path(result.run_log_path or "").read_text()
        assert "mirror feed refspecs (unforced, atomic)" in transcript
        assert f"{forge.c2}:{PIN_REF_PREFIX}{forge.c2}" in transcript
