"""Per-SHA runtime slot layout: the flip, and what it refuses.

The point of a slot is that a running job keeps the exact runtime it started
with while a new one is built alongside. Everything here is about not breaking
that guarantee: never leave `current` unresolvable even briefly, never delete a
slot something still points at, and never let an unvalidated SHA place a
directory outside the slot root.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import venv
from pathlib import Path

import pytest

from vq import runtime_slots

SHA_A = "a" * 40
SHA_B = "b" * 40
SHA_C = "c" * 40


def _build(root: Path, sha: str, marker: str = "x") -> Path:
    slot = runtime_slots.create_slot(root, sha)
    (slot / "VERSION").write_text(marker)
    source = runtime_slots.slot_source(root, sha)
    source.mkdir()
    (source / "VERSION").write_text(marker)
    venv = runtime_slots.slot_python(root, sha).parent.parent
    (venv / "bin").mkdir(parents=True)
    python = venv / "bin" / "python"
    python.write_text("#!/bin/sh\nexit 0\n")
    python.chmod(0o755)
    (venv / runtime_slots.IMMUTABLE_RUNTIME_MARKER).write_text(
        json.dumps({"schema": 1, "kind": "vq-runtime-slot", "id": sha}) + "\n"
    )
    (slot / runtime_slots.SLOT_STATE_MARKER).write_text(
        json.dumps(
            {
                "schema": 1,
                "kind": "vq-runtime-slot",
                "id": sha,
                "transaction": "0" * 32,
                "state": "verified",
                "content_sha256": runtime_slots.venv_content_sha256(root, sha),
                "source_content_sha256": runtime_slots.source_content_sha256(
                    root, sha
                ),
            }
        )
        + "\n"
    )
    return slot


class TestRuntimePythonLauncher:
    @pytest.mark.parametrize("pointer", [None, "releases/" + SHA_A, "../outside"])
    def test_no_fallback_for_missing_unverified_or_escaping_current(
        self, tmp_path: Path, pointer: str | None, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        root = tmp_path / "runtime"
        root.mkdir()
        if pointer is not None:
            if pointer == "releases/" + SHA_A:
                runtime_slots.create_slot(root, SHA_A)
            (root / "current").symlink_to(pointer)
        calls = []
        monkeypatch.setattr(os, "execve", lambda *args: calls.append(args))
        with pytest.raises(runtime_slots.RuntimeSlotError):
            runtime_slots.exec_current_python(root, ["-c", "pass"])
        assert calls == []

    def test_expected_sha_is_checked_before_exec(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _build(tmp_path, SHA_A)
        runtime_slots.activate(tmp_path, SHA_A)
        calls = []
        monkeypatch.setattr(os, "execve", lambda *args: calls.append(args))
        with pytest.raises(runtime_slots.RuntimeSlotError, match="SHA mismatch"):
            runtime_slots.exec_current_python(tmp_path, [], expected_sha=SHA_B)
        assert calls == []

    @pytest.mark.parametrize("damage", ["missing", "not-executable", "directory"])
    def test_unusable_interpreter_is_refused(self, tmp_path: Path, damage: str) -> None:
        _build(tmp_path, SHA_A)
        runtime_slots.activate(tmp_path, SHA_A)
        python = runtime_slots.slot_python(tmp_path, SHA_A)
        if damage == "not-executable":
            python.chmod(0o644)
        else:
            python.unlink()
            if damage == "directory":
                python.mkdir()
        with pytest.raises(runtime_slots.RuntimeSlotError, match="not executable"):
            runtime_slots.exec_current_python(tmp_path, [])

    def test_flip_after_selection_does_not_retarget_exec(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _build(tmp_path, SHA_A)
        _build(tmp_path, SHA_B)
        runtime_slots.activate(tmp_path, SHA_A)
        resolve = runtime_slots.resolve_current

        def flip_after_resolve(root: Path) -> str | None:
            selected = resolve(root)
            monkeypatch.setattr(runtime_slots, "resolve_current", resolve)
            runtime_slots.activate(root, SHA_B)
            return selected

        monkeypatch.setattr(runtime_slots, "resolve_current", flip_after_resolve)
        calls = []
        monkeypatch.setattr(os, "execve", lambda *args: calls.append(args))
        runtime_slots.exec_current_python(tmp_path, ["name with spaces", "$(literal)"],
                                          expected_sha=SHA_A)
        executable, argv, env = calls[0]
        assert executable == str(runtime_slots.slot_python(tmp_path, SHA_A))
        assert argv == [executable, "name with spaces", "$(literal)"]
        assert env["VQ_RUNTIME_SLOT_SHA"] == SHA_A
        assert resolve(tmp_path) == SHA_B

    @pytest.mark.parametrize("relative", ["source", "source/.venv", "source/.venv/bin"])
    def test_redirected_runtime_directories_are_refused(
        self, tmp_path: Path, relative: str,
    ) -> None:
        slot = _build(tmp_path, SHA_A)
        runtime_slots.activate(tmp_path, SHA_A)
        target = slot / relative
        moved = target.with_name(target.name + "-moved")
        target.rename(moved)
        target.symlink_to(moved, target_is_directory=True)
        with pytest.raises(runtime_slots.RuntimeSlotError):
            runtime_slots.exec_current_python(tmp_path, [])

    def test_real_venvs_keep_late_imports_and_process_identity_across_activation(
        self, tmp_path: Path,
    ) -> None:
        repo = tmp_path / "upstream"
        repo.mkdir()

        def git(*args: str) -> str:
            return subprocess.check_output(
                ["git", "-C", str(repo), *args], text=True, stderr=subprocess.PIPE,
            ).strip()

        git("init", "-q")
        root = tmp_path / "runtimes"
        shas = []
        for version in ("A", "B"):
            (repo / "late.py").write_text(f"VERSION = {version!r}\n")
            git("add", "late.py")
            git("-c", "user.name=Runtime Test", "-c", "user.email=test@example.invalid",
                "commit", "-qm", f"runtime {version} (#577)")
            sha = git("rev-parse", "HEAD")
            shas.append(sha)
            transaction = ("1" if version == "A" else "2") * 32
            runtime_slots.begin_slot_build(root, sha, transaction_id=transaction,
                                           in_use=lambda: set())
            runtime_slots.materialize_source(repo, root, sha)
            py = runtime_slots.slot_python(root, sha)
            venv.EnvBuilder(with_pip=False, symlinks=True).create(py.parent.parent)
            site = subprocess.check_output(
                [str(py), "-c", "import sysconfig; print(sysconfig.get_path('purelib'))"],
                text=True,
            ).strip()
            (Path(site) / "runtime-source.pth").write_text(
                str(runtime_slots.slot_source(root, sha)) + "\n"
            )
            runtime_slots.seal_slot_build(root, sha, transaction_id=transaction)
        runtime_slots.activate(root, shas[0])
        env = dict(os.environ)
        env["PYTHONPATH"] = str(Path(runtime_slots.__file__).parents[1])
        base = [sys.executable, "-m", "vq", "runtime-python", "--root", str(root), "--"]
        program = (
            "import os, sys, json; print(os.getpid(), flush=True); input(); "
            "import late; print(json.dumps([late.VERSION, sys.executable, "
            "sys.prefix, os.getcwd(), sys.argv[1:]]), flush=True); sys.exit(37)"
        )
        child = subprocess.Popen(
            [*base, "-c", program, "a b", "$(literal)"], cwd=tmp_path,
            env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True,
        )
        try:
            assert child.stdout is not None
            assert int(child.stdout.readline()) == child.pid
            runtime_slots.activate(root, shas[1])
            stdout, stderr = child.communicate("continue\n", timeout=15)
            assert child.returncode == 37, stderr
            version, executable, prefix, cwd, arguments = json.loads(stdout)
            old_python = runtime_slots.slot_python(root, shas[0])
            assert version == "A"
            assert executable == str(old_python)
            assert prefix == str(old_python.parent.parent)
            assert cwd == str(tmp_path)
            assert arguments == ["a b", "$(literal)"]
            new = subprocess.run(
                [*base, "-c", "import late; print(late.VERSION)"],
                env=env, capture_output=True, text=True, timeout=15,
            )
            assert new.returncode == 0, new.stderr
            assert new.stdout == "B\n"
            signalled = subprocess.run(
                [*base, "-c", "import os, signal; os.kill(os.getpid(), signal.SIGTERM)"],
                env=env, capture_output=True, text=True, timeout=15,
            )
            assert signalled.returncode == -signal.SIGTERM
        finally:
            if child.poll() is None:
                child.kill()
            child.communicate(timeout=15)


class TestLayoutRefusesUnsafeInput:
    def test_relative_root_is_refused(self) -> None:
        """A relative root would resolve against whatever cwd the build
        inherited, which is not knowable from here."""
        with pytest.raises(runtime_slots.RuntimeSlotError, match="absolute"):
            runtime_slots.layout("relative/runtimes")

    @pytest.mark.parametrize(
        "bad",
        [
            "../../etc",
            "a" * 39,
            "A" * 40,          # upper-case hex
            "z" * 40,
            "",
            "abc/../../def",
        ],
    )
    def test_only_a_full_lowercase_hex_sha_names_a_slot(
        self, tmp_path: Path, bad: str
    ) -> None:
        """The SHA arrives from a release report and from remote command
        output; one containing a separator would place a slot anywhere."""
        with pytest.raises(runtime_slots.RuntimeSlotError):
            runtime_slots.slot_path(tmp_path, bad)

    def test_slot_stays_inside_the_root(self, tmp_path: Path) -> None:
        slot = runtime_slots.slot_path(tmp_path, SHA_A)
        assert slot.parent.parent == tmp_path
        assert str(slot).startswith(str(tmp_path))


class TestActivate:
    def test_flip_points_current_and_retains_previous(self, tmp_path: Path) -> None:
        _build(tmp_path, SHA_A, "A")
        _build(tmp_path, SHA_B, "B")

        assert runtime_slots.activate(tmp_path, SHA_A) is None
        assert runtime_slots.resolve_current(tmp_path) == SHA_A

        prior = runtime_slots.activate(tmp_path, SHA_B)

        assert prior == SHA_A
        assert runtime_slots.resolve_current(tmp_path) == SHA_B
        # The rollback target is retained, and still resolves to real content.
        previous = tmp_path / "previous"
        assert previous.is_symlink()
        assert (previous / "VERSION").read_text() == "A"

    def test_current_resolves_to_the_new_content(self, tmp_path: Path) -> None:
        _build(tmp_path, SHA_A, "A")
        _build(tmp_path, SHA_B, "B")
        runtime_slots.activate(tmp_path, SHA_A)
        runtime_slots.activate(tmp_path, SHA_B)

        assert (tmp_path / "current" / "VERSION").read_text() == "B"

    def test_a_running_job_keeps_its_own_slot_across_a_flip(
        self, tmp_path: Path
    ) -> None:
        """The whole point: resolve once, then survive an unrelated flip.

        This is what venv hosts cannot do today -- an in-place `git pull` plus
        editable install rewrites the very files a live interpreter imports
        from.
        """
        _build(tmp_path, SHA_A, "A")
        _build(tmp_path, SHA_B, "B")
        runtime_slots.activate(tmp_path, SHA_A)

        # A job resolves its interpreter at dispatch and holds that path.
        held = (tmp_path / "current").resolve()

        runtime_slots.activate(tmp_path, SHA_B)

        assert (held / "VERSION").read_text() == "A", "the running job's runtime moved"
        assert (tmp_path / "current" / "VERSION").read_text() == "B"

    def test_symlink_target_is_relative_so_the_root_relocates(
        self, tmp_path: Path
    ) -> None:
        _build(tmp_path, SHA_A)
        runtime_slots.activate(tmp_path, SHA_A)

        assert not os.path.isabs(os.readlink(tmp_path / "current"))

    def test_reactivating_the_same_sha_does_not_clobber_previous(
        self, tmp_path: Path
    ) -> None:
        """An idempotent redeploy must not destroy the rollback target by
        making `previous` point at the same slot as `current`."""
        _build(tmp_path, SHA_A)
        _build(tmp_path, SHA_B)
        runtime_slots.activate(tmp_path, SHA_A)
        runtime_slots.activate(tmp_path, SHA_B)

        assert runtime_slots.activate(tmp_path, SHA_B) == SHA_B
        assert runtime_slots._previous_sha(tmp_path) == SHA_A

    def test_activating_an_unbuilt_slot_is_refused(self, tmp_path: Path) -> None:
        """Flipping to a slot that was never built would point `current` at
        nothing -- worse than not flipping at all."""
        with pytest.raises(runtime_slots.RuntimeSlotError, match="does not exist"):
            runtime_slots.activate(tmp_path, SHA_A)

    def test_an_in_place_install_at_current_is_refused(self, tmp_path: Path) -> None:
        """A real directory at `current` means this root was never migrated;
        treating it as a slot root would let a flip discard a live runtime."""
        (tmp_path / "current").mkdir()

        with pytest.raises(runtime_slots.RuntimeSlotError, match="in-place install"):
            runtime_slots.resolve_current(tmp_path)


class TestRollback:
    def test_rollback_returns_to_the_retained_slot(self, tmp_path: Path) -> None:
        _build(tmp_path, SHA_A, "A")
        _build(tmp_path, SHA_B, "B")
        runtime_slots.activate(tmp_path, SHA_A)
        runtime_slots.activate(tmp_path, SHA_B)

        assert runtime_slots.rollback(tmp_path) == SHA_A
        assert (tmp_path / "current" / "VERSION").read_text() == "A"

    def test_rollback_with_nothing_retained_is_not_an_error(
        self, tmp_path: Path
    ) -> None:
        _build(tmp_path, SHA_A)
        runtime_slots.activate(tmp_path, SHA_A)

        assert runtime_slots.rollback(tmp_path) is None

    def test_rollback_to_a_reclaimed_slot_is_refused(self, tmp_path: Path) -> None:
        _build(tmp_path, SHA_A)
        _build(tmp_path, SHA_B)
        runtime_slots.activate(tmp_path, SHA_A)
        runtime_slots.activate(tmp_path, SHA_B)
        # Simulate retention having wrongly removed the rollback target.
        import shutil

        shutil.rmtree(tmp_path / "releases" / SHA_A)

        with pytest.raises(runtime_slots.RuntimeSlotError, match="slot is gone"):
            runtime_slots.rollback(tmp_path)


class TestReclamation:
    def test_never_reclaims_current_previous_or_in_use(self, tmp_path: Path) -> None:
        for sha in (SHA_A, SHA_B, SHA_C):
            _build(tmp_path, sha)
        runtime_slots.activate(tmp_path, SHA_A)
        runtime_slots.activate(tmp_path, SHA_B)  # A becomes previous

        # C is neither pointer, but a job is still executing from it.
        assert runtime_slots.reclaimable(tmp_path, in_use={SHA_C}) == []

    def test_reclaims_only_the_unreferenced(self, tmp_path: Path) -> None:
        for sha in (SHA_A, SHA_B, SHA_C):
            _build(tmp_path, sha)
        runtime_slots.activate(tmp_path, SHA_A)
        runtime_slots.activate(tmp_path, SHA_B)

        removed = runtime_slots.reclaim(tmp_path, in_use=set())

        assert removed == [SHA_C]
        assert (tmp_path / "releases" / SHA_A).is_dir()
        assert (tmp_path / "releases" / SHA_B).is_dir()
        assert not (tmp_path / "releases" / SHA_C).exists()
        # And the retained rollback target still works afterwards.
        assert runtime_slots.rollback(tmp_path) == SHA_A

    def test_list_slots_ignores_foreign_entries(self, tmp_path: Path) -> None:
        _build(tmp_path, SHA_A)
        (tmp_path / "releases" / "not-a-sha").mkdir()
        (tmp_path / "releases" / "README").write_text("hi")

        assert runtime_slots.list_slots(tmp_path) == [SHA_A]

    def test_empty_root_is_quiet(self, tmp_path: Path) -> None:
        assert runtime_slots.list_slots(tmp_path) == []
        assert runtime_slots.resolve_current(tmp_path) is None
        assert runtime_slots.reclaim(tmp_path, in_use=set()) == []


class TestSlotOptInConfig:
    """`runtime_slot_root` opts one venv program into per-SHA slots.

    Absent -- the default and every host today -- keeps the historical in-place
    update. The field exists so a host can be converted one at a time rather
    than the fleet changing shape at once.
    """

    def test_absolute_root_is_accepted_and_defaults_off(self) -> None:
        from vq import config

        opted_in = config.VenvProgram(
            kind="venv",
            python="/opt/rt/current/.venv/bin/python",
            git_dir="/opt/rt/current/source",
            runtime_slot_root="/opt/rt/vibeqc-release",
        )
        default = config.VenvProgram(
            kind="venv", python="/x/.venv/bin/python", git_dir="/x"
        )

        assert opted_in.runtime_slot_root == "/opt/rt/vibeqc-release"
        assert default.runtime_slot_root is None

    @pytest.mark.parametrize("bad", ["relative/runtimes", "", "   "])
    def test_a_root_that_cannot_travel_is_rejected_at_config_load(
        self, bad: str
    ) -> None:
        """The root is handed to a build running elsewhere, so a relative path
        would resolve against whatever cwd that build inherited. Better to fail
        at config load than when a release is already half-applied."""
        from pydantic import ValidationError

        from vq import config

        with pytest.raises(ValidationError):
            config.VenvProgram(
                kind="venv",
                python="/x/.venv/bin/python",
                git_dir="/x",
                runtime_slot_root=bad,
            )

    def test_program_config_still_rejects_unknown_keys(self) -> None:
        """Deliberately NOT relaxed, unlike DrainState.

        DrainState is machine-written state where a strict reader silently
        un-drained a host, so it now tolerates unknown keys. Program config is
        hand-edited, where rejecting a typo earns its keep. The consequence is a
        deployment-ordering constraint rather than a schema change: a vq that
        predates a key does not ignore it, it fails to load the config at all,
        so a new key must not be written into a config until the host reading it
        has been updated.
        """
        from pydantic import ValidationError

        from vq import config

        with pytest.raises(ValidationError):
            config.VenvProgram(
                kind="venv",
                python="/x/.venv/bin/python",
                git_dir="/x",
                some_future_key="whatever",  # type: ignore[call-arg]
            )


class TestSlotLocalProgram:
    """Deriving a program that runs inside a slot.

    This is what lets the ordinary update path operate on a tree no running job
    is importing from: repoint `git_dir` and `python` at the slot, and fetch,
    checkout, tag verification, update script and import check all work
    unchanged.
    """

    def _prog(self):  # type: ignore[no-untyped-def]
        from vq import config

        return config.VenvProgram(
            kind="venv",
            python="/opt/live/.venv/bin/python",
            git_dir="/opt/live",
            branch="release",
            update_script="scripts/update.sh",
        )

    def test_paths_are_repointed_into_the_slot(self) -> None:
        derived = runtime_slots.slot_local_program(self._prog(), "/rt", SHA_A)

        assert derived.git_dir == f"/rt/releases/{SHA_A}/source"
        assert derived.python == f"/rt/releases/{SHA_A}/source/.venv/bin/python"

    def test_everything_else_is_preserved(self) -> None:
        """Only the location changes; the update recipe must not."""
        original = self._prog()
        derived = runtime_slots.slot_local_program(original, "/rt", SHA_A)

        assert derived.branch == original.branch
        assert derived.update_script == original.update_script
        assert derived.kind == original.kind
        # The original is untouched -- callers still hold the live program.
        assert original.git_dir == "/opt/live"

    def test_paths_never_resolve_through_current(self) -> None:
        """The correctness constraint, pinned.

        A venv records an absolute path in its editable `.pth`. If a slot's
        interpreter resolved through `current`, a later flip would change a
        RUNNING job's source underneath it -- silently reintroducing exactly the
        in-place mutation slots exist to prevent. `current` is resolved only by
        the launcher at exec time.
        """
        derived = runtime_slots.slot_local_program(self._prog(), "/rt", SHA_A)

        assert "/current/" not in derived.git_dir + "/"
        assert "/current/" not in derived.python
        assert SHA_A in derived.git_dir
        assert SHA_A in derived.python

    def test_a_slot_root_named_current_is_refused(self) -> None:
        """Defence in depth: if a root were configured such that the derived
        path passed through the pointer, refuse rather than produce a runtime
        that a flip can move."""
        with pytest.raises(runtime_slots.RuntimeSlotError, match="running job"):
            runtime_slots.slot_local_program(self._prog(), "/rt/current", SHA_A)

    def test_two_slots_never_share_an_interpreter(self) -> None:
        """The property the whole design rests on."""
        a = runtime_slots.slot_local_program(self._prog(), "/rt", SHA_A)
        b = runtime_slots.slot_local_program(self._prog(), "/rt", SHA_B)

        assert a.python != b.python
        assert a.git_dir != b.git_dir


def _git(repo: Path, *args: str) -> str:
    import subprocess

    return subprocess.run(
        ["git", "-C", str(repo), *args],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


def _live_repo(tmp_path: Path) -> tuple[Path, str, str]:
    """A source checkout with two commits; returns (repo, first, second)."""
    repo = tmp_path / "live"
    repo.mkdir()
    _git(repo, "init", "--quiet", "-b", "main")
    _git(repo, "config", "user.email", "t@example.invalid")
    _git(repo, "config", "user.name", "T")
    (repo / "VERSION").write_text("one")
    _git(repo, "add", "-A")
    _git(repo, "-c", "commit.gpgsign=false", "commit", "-m", "one", "--quiet")
    first = _git(repo, "rev-parse", "HEAD")
    (repo / "VERSION").write_text("two")
    _git(repo, "add", "-A")
    _git(repo, "-c", "commit.gpgsign=false", "commit", "-m", "two", "--quiet")
    second = _git(repo, "rev-parse", "HEAD")
    return repo, first, second


class TestMaterializeSource:
    """Populating a slot with a checkout at exactly its own SHA."""

    @pytest.mark.parametrize("relative_origin", [False, True])
    def test_fetches_new_commit_only_into_unpublished_slot(
        self, tmp_path: Path, relative_origin: bool,
    ) -> None:
        repo, _first, second = _live_repo(tmp_path)
        upstream = tmp_path / "upstream"
        _git(tmp_path, "clone", "--quiet", str(repo), str(upstream))
        _git(repo, "remote", "add", "origin",
             "../upstream" if relative_origin else str(upstream))
        (upstream / "VERSION").write_text("three")
        _git(upstream, "add", "VERSION")
        _git(upstream, "-c", "user.name=T", "-c", "user.email=t@example.invalid",
             "-c", "commit.gpgsign=false", "commit", "-m", "three", "--quiet")
        target = _git(upstream, "rev-parse", "HEAD")
        refs_before = _git(repo, "show-ref")
        root = tmp_path / "rt"
        _build(root, SHA_A)
        runtime_slots.activate(root, SHA_A)

        source = runtime_slots.materialize_source(repo, root, target)

        assert _git(source, "rev-parse", "HEAD") == target
        assert (source / "VERSION").read_text() == "three"
        assert _git(source, "remote", "get-url", "origin") == str(upstream)
        assert _git(repo, "rev-parse", "HEAD") == second
        assert _git(repo, "show-ref") == refs_before
        assert (repo / "VERSION").read_text() == "two"
        assert runtime_slots.resolve_current(root) == SHA_A

    def test_missing_remote_commit_never_activates_or_fetches_into_live_repo(
        self, tmp_path: Path,
    ) -> None:
        repo, _first, second = _live_repo(tmp_path)
        _git(repo, "remote", "add", "origin", str(tmp_path / "unavailable"))
        root = tmp_path / "rt"
        _build(root, SHA_A)
        runtime_slots.activate(root, SHA_A)
        refs_before = _git(repo, "show-ref")

        with pytest.raises(runtime_slots.RuntimeSlotError):
            runtime_slots.materialize_source(repo, root, SHA_B)

        assert runtime_slots.resolve_current(root) == SHA_A
        assert _git(repo, "rev-parse", "HEAD") == second
        assert _git(repo, "show-ref") == refs_before
        with pytest.raises(runtime_slots.RuntimeSlotError):
            runtime_slots.activate(root, SHA_B)

    def test_slot_holds_the_requested_commit(self, tmp_path: Path) -> None:
        repo, first, _second = _live_repo(tmp_path)
        root = tmp_path / "rt"

        source = runtime_slots.materialize_source(repo, root, first)

        assert (source / "VERSION").read_text() == "one"
        assert _git(source, "rev-parse", "HEAD") == first

    def test_available_commit_does_not_need_origin_connectivity(self, tmp_path: Path) -> None:
        repo, first, _second = _live_repo(tmp_path)
        _git(repo, "remote", "add", "origin", str(tmp_path / "offline"))

        source = runtime_slots.materialize_source(repo, tmp_path / "rt", first)

        assert _git(source, "rev-parse", "HEAD") == first

    def test_two_slots_hold_different_commits_simultaneously(
        self, tmp_path: Path
    ) -> None:
        """The property the design rests on: an older slot keeps its own code
        while a newer one is built alongside."""
        repo, first, second = _live_repo(tmp_path)
        root = tmp_path / "rt"

        old = runtime_slots.materialize_source(repo, root, first)
        new = runtime_slots.materialize_source(repo, root, second)

        assert (old / "VERSION").read_text() == "one"
        assert (new / "VERSION").read_text() == "two"

    def test_is_idempotent_and_does_not_reclone(self, tmp_path: Path) -> None:
        repo, first, _ = _live_repo(tmp_path)
        root = tmp_path / "rt"
        runtime_slots.materialize_source(repo, root, first)
        marker = runtime_slots.slot_source(root, first) / "UNTRACKED"
        marker.write_text("kept")

        again = runtime_slots.materialize_source(repo, root, first)

        assert (again / "UNTRACKED").read_text() == "kept", "slot was re-cloned"

    def test_a_commit_the_source_lacks_fails_closed(self, tmp_path: Path) -> None:
        repo, _first, _second = _live_repo(tmp_path)
        root = tmp_path / "rt"

        with pytest.raises(runtime_slots.RuntimeSlotError, match="could not check out"):
            runtime_slots.materialize_source(repo, root, "d" * 40)

    def test_a_checkout_landing_elsewhere_is_refused(
        self, tmp_path: Path
    ) -> None:
        """Publishing a slot that does not contain its own commit is the one
        failure this design cannot tolerate, so the post-check is unconditional.
        """
        repo, first, second = _live_repo(tmp_path)
        root = tmp_path / "rt"
        real_run = __import__("subprocess").run

        def lying_runner(argv, **kw):  # type: ignore[no-untyped-def]
            # Check out the WRONG commit, as a corrupted or racing source might.
            if "checkout" in argv:
                argv = [a if a != first else second for a in argv]
            return real_run(argv, **kw)

        with pytest.raises(runtime_slots.RuntimeSlotError, match="instead"):
            runtime_slots.materialize_source(
                repo, root, first, runner=lying_runner
            )

    def test_objects_are_shared_with_the_source_not_copied(
        self, tmp_path: Path
    ) -> None:
        """`--local` hardlinks git's object store. Safe here precisely because
        git objects are immutable and content-addressed -- unlike a venv, which
        records absolute paths and cannot be shared at all."""
        repo, first, _ = _live_repo(tmp_path)
        root = tmp_path / "rt"

        source = runtime_slots.materialize_source(repo, root, first)

        assert (source / ".git").exists()
        assert _git(source, "rev-parse", "HEAD") == first


class TestDurableSlotBuild:
    def _start(
        self,
        repo: Path,
        root: Path,
        sha: str,
        transaction: str,
    ) -> None:
        assert runtime_slots.begin_slot_build(
            root,
            sha,
            transaction_id=transaction,
            in_use=lambda: set(),
        )
        runtime_slots.materialize_source(repo, root, sha)

    def _add_venv(self, root: Path, sha: str) -> None:
        python = runtime_slots.slot_python(root, sha)
        python.parent.mkdir(parents=True)
        python.symlink_to(sys.executable)

    def test_failed_exact_transaction_is_retryable_when_unused(
        self, tmp_path: Path
    ) -> None:
        repo, sha, _second = _live_repo(tmp_path)
        root = tmp_path / "rt"
        self._start(repo, root, sha, "1" * 32)
        stale = runtime_slots.slot_path(root, sha) / "failed-build"
        stale.write_text("partial\n")

        assert runtime_slots.begin_slot_build(
            root,
            sha,
            transaction_id="2" * 32,
            in_use=lambda: set(),
        )

        assert not stale.exists()
        state = json.loads(
            (
                runtime_slots.slot_path(root, sha)
                / runtime_slots.SLOT_STATE_MARKER
            ).read_text()
        )
        assert state["transaction"] == "2" * 32

    def test_failed_slot_recovery_refuses_a_live_spec_reference(
        self, tmp_path: Path
    ) -> None:
        repo, sha, _second = _live_repo(tmp_path)
        root = tmp_path / "rt"
        self._start(repo, root, sha, "3" * 32)

        with pytest.raises(runtime_slots.RuntimeSlotError, match="referenced"):
            runtime_slots.begin_slot_build(
                root,
                sha,
                transaction_id="4" * 32,
                in_use=lambda: {sha},
            )

        assert runtime_slots.slot_source(root, sha).is_dir()

    def test_receipt_survives_crash_between_mkdir_and_in_slot_marker(
        self, tmp_path: Path
    ) -> None:
        root = tmp_path / "rt"
        runtime_slots.begin_slot_build(
            root,
            SHA_A,
            transaction_id="5" * 32,
            in_use=lambda: set(),
        )
        state = (
            runtime_slots.slot_path(root, SHA_A)
            / runtime_slots.SLOT_STATE_MARKER
        )
        state.unlink()

        assert runtime_slots.begin_slot_build(
            root,
            SHA_A,
            transaction_id="6" * 32,
            in_use=lambda: set(),
        )

    def test_immutable_marker_recovers_seal_boundary_without_rebuild(
        self, tmp_path: Path
    ) -> None:
        repo, sha, _second = _live_repo(tmp_path)
        root = tmp_path / "rt"
        transaction = "7" * 32
        self._start(repo, root, sha, transaction)
        self._add_venv(root, sha)
        immutable = (
            runtime_slots.slot_python(root, sha).parent.parent
            / runtime_slots.IMMUTABLE_RUNTIME_MARKER
        )
        immutable.write_text(
            json.dumps({"schema": 1, "kind": "vq-runtime-slot", "id": sha})
            + "\n"
        )

        assert not runtime_slots.begin_slot_build(
            root,
            sha,
            transaction_id="8" * 32,
            in_use=lambda: set(),
        )
        assert runtime_slots.activate(root, sha) is None

    def test_verified_content_mutation_is_refused_on_reactivation(
        self, tmp_path: Path
    ) -> None:
        repo, sha, _second = _live_repo(tmp_path)
        root = tmp_path / "rt"
        transaction = "9" * 32
        self._start(repo, root, sha, transaction)
        self._add_venv(root, sha)
        runtime_slots.seal_slot_build(
            root,
            sha,
            transaction_id=transaction,
        )
        runtime_slots.activate(root, sha)
        (runtime_slots.slot_source(root, sha) / "VERSION").write_text("tampered")

        with pytest.raises(runtime_slots.RuntimeSlotError, match="content changed"):
            runtime_slots.activate(root, sha)

    def test_verified_interpreter_mode_mutation_is_refused_on_reactivation(
        self, tmp_path: Path
    ) -> None:
        _build(tmp_path, SHA_A)
        runtime_slots.activate(tmp_path, SHA_A)
        runtime_slots.slot_python(tmp_path, SHA_A).chmod(0o644)

        with pytest.raises(runtime_slots.RuntimeSlotError, match="not executable"):
            runtime_slots.activate(tmp_path, SHA_A)


class TestSlotPathBinding:
    def test_symlinked_root_is_refused(self, tmp_path: Path) -> None:
        real = tmp_path / "real"
        real.mkdir()
        alias = tmp_path / "alias"
        alias.symlink_to(real, target_is_directory=True)

        with pytest.raises(runtime_slots.RuntimeSlotError, match="symlinked"):
            runtime_slots.layout(alias)

    def test_lexical_aliases_share_one_canonical_root(self, tmp_path: Path) -> None:
        aliased = tmp_path / "missing" / ".." / "runtime"

        assert runtime_slots.layout(aliased).root == tmp_path / "runtime"

    def test_darwin_missing_suffix_is_case_canonicalized(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(runtime_slots.sys, "platform", "darwin")

        upper = runtime_slots.layout(tmp_path / "MiXeD" / "RuNtImE").root
        lower = runtime_slots.layout(tmp_path / "mixed" / "runtime").root

        assert upper == lower

    def test_group_writable_root_is_refused(self, tmp_path: Path) -> None:
        root = tmp_path / "runtime"
        root.mkdir(mode=0o777)
        root.chmod(0o777)

        with pytest.raises(runtime_slots.RuntimeSlotError, match="writable"):
            runtime_slots.layout(root)

    def test_symlinked_releases_root_blocks_activation_and_reclaim_without_delete(
        self, tmp_path: Path
    ) -> None:
        root = tmp_path / "runtime"
        root.mkdir()
        external_releases = tmp_path / "external-releases"
        victim = external_releases / SHA_A
        victim.mkdir(parents=True)
        sentinel = victim / "must-survive"
        sentinel.write_text("external bytes\n")
        (root / "releases").symlink_to(
            external_releases,
            target_is_directory=True,
        )

        with pytest.raises(runtime_slots.RuntimeSlotError, match="real directory"):
            runtime_slots.activate(root, SHA_A)
        with pytest.raises(runtime_slots.RuntimeSlotError, match="real directory"):
            runtime_slots.reclaim(root, in_use=set())

        assert sentinel.read_text() == "external bytes\n"

    def test_external_same_name_pointer_is_refused(self, tmp_path: Path) -> None:
        _build(tmp_path, SHA_A)
        external = tmp_path / "external" / SHA_A
        external.mkdir(parents=True)
        (tmp_path / "current").symlink_to(external)

        with pytest.raises(runtime_slots.RuntimeSlotError, match="exact relative"):
            runtime_slots.resolve_current(tmp_path)

    def test_external_same_name_previous_pointer_is_refused(
        self, tmp_path: Path
    ) -> None:
        _build(tmp_path, SHA_A)
        _build(tmp_path, SHA_B)
        runtime_slots.activate(tmp_path, SHA_A)
        runtime_slots.activate(tmp_path, SHA_B)
        external = tmp_path / "external" / SHA_A
        external.mkdir(parents=True)
        (tmp_path / "previous").unlink()
        (tmp_path / "previous").symlink_to(external)

        with pytest.raises(runtime_slots.RuntimeSlotError, match="exact relative"):
            runtime_slots.reclaimable(tmp_path, in_use=set())

    def test_symlinked_generation_is_never_activated(self, tmp_path: Path) -> None:
        external_root = tmp_path / "external-root"
        external = _build(external_root, SHA_A)
        releases = tmp_path / "releases"
        releases.mkdir()
        (releases / SHA_A).symlink_to(external, target_is_directory=True)

        with pytest.raises(runtime_slots.RuntimeSlotError, match="real directory"):
            runtime_slots.activate(tmp_path, SHA_A)

    def test_unverified_generation_is_never_activated(self, tmp_path: Path) -> None:
        runtime_slots.create_slot(tmp_path, SHA_A)

        with pytest.raises(runtime_slots.RuntimeSlotError, match="not verified"):
            runtime_slots.activate(tmp_path, SHA_A)

    def test_verified_generation_cannot_be_reopened_for_writes(
        self, tmp_path: Path
    ) -> None:
        _build(tmp_path, SHA_A)

        with pytest.raises(runtime_slots.RuntimeSlotError, match="immutable"):
            runtime_slots.create_slot(tmp_path, SHA_A)

    def test_fsync_failure_after_pointer_rename_is_retryable(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _build(tmp_path, SHA_A)
        original = runtime_slots._fsync_directory
        calls = 0

        def fail_once(path: Path) -> None:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise runtime_slots.RuntimeSlotError("injected fsync failure")
            original(path)

        monkeypatch.setattr(runtime_slots, "_fsync_directory", fail_once)
        with pytest.raises(runtime_slots.RuntimeSlotError, match="fsync"):
            runtime_slots.activate(tmp_path, SHA_A)
        assert runtime_slots.resolve_current(tmp_path) == SHA_A

        assert runtime_slots.activate(tmp_path, SHA_A) == SHA_A
        assert calls >= 2


def test_slot_local_program_is_not_itself_slot_managed() -> None:
    """A slot-local program IS the slot. Handing it back to the slot-aware
    update path would recurse forever."""
    from vq import config

    prog = config.VenvProgram(
        kind="venv",
        python="/opt/live/.venv/bin/python",
        git_dir="/opt/live",
        runtime_slot_root="/rt",
    )

    derived = runtime_slots.slot_local_program(prog, "/rt", SHA_A)

    assert derived.runtime_slot_root is None
    assert prog.runtime_slot_root == "/rt", "the caller's program is untouched"


class TestSlotsInUse:
    """Which slots a live job is actually executing from.

    Derived from each spec's command -- the path the process really exec'd --
    rather than a recorded pin, so it stays true even if a pin was written
    differently, missing, or later rewritten.
    """

    class _Spec:
        def __init__(self, command: list[str], *, terminal: bool = False) -> None:
            self.command = command
            self.is_terminal = terminal

    def _python(self, root: str, sha: str) -> str:
        return f"{root}/releases/{sha}/source/.venv/bin/python"

    def test_a_live_job_holds_its_slot(self) -> None:
        specs = [self._Spec([self._python("/rt", SHA_A), "run.py"])]

        assert runtime_slots.slots_in_use("/rt", specs) == {SHA_A}

    def test_a_terminal_job_holds_nothing(self) -> None:
        """Its process is gone, so whatever it ran from is no longer held."""
        specs = [self._Spec([self._python("/rt", SHA_A), "run.py"], terminal=True)]

        assert runtime_slots.slots_in_use("/rt", specs) == set()

    def test_several_live_slots_are_all_reported(self) -> None:
        specs = [
            self._Spec([self._python("/rt", SHA_A), "a.py"]),
            self._Spec([self._python("/rt", SHA_B), "b.py"]),
            self._Spec([self._python("/rt", SHA_A), "c.py"]),
        ]

        assert runtime_slots.slots_in_use("/rt", specs) == {SHA_A, SHA_B}

    def test_unresolved_live_commands_retain_every_generation(
        self, tmp_path: Path
    ) -> None:
        for sha in (SHA_A, SHA_B):
            _build(tmp_path, sha)
        specs = [
            self._Spec(["/usr/bin/python3", "x.py"]),
            self._Spec(["/other/root/releases/" + SHA_C + "/source/.venv/bin/python"]),
            self._Spec(
                [f"{tmp_path}/releases/not-a-sha/source/.venv/bin/python"]
            ),
            self._Spec([]),
        ]

        assert runtime_slots.slots_in_use(tmp_path, specs) == {SHA_A, SHA_B}

    def test_stable_current_command_protects_old_running_slot_on_third_flip(
        self, tmp_path: Path
    ) -> None:
        """A spec records `current`, but its process resolved A at exec time.

        Once current moves C and previous moves B, the durable spec has no
        exact-A evidence. Reclamation must retain A instead of guessing.
        """
        for sha in (SHA_A, SHA_B, SHA_C):
            _build(tmp_path, sha)
        runtime_slots.activate(tmp_path, SHA_A)
        held = (tmp_path / "current").resolve()
        specs = [
            self._Spec(
                [str(tmp_path / "current/source/.venv/bin/python"), "run.py"]
            )
        ]

        runtime_slots.activate(tmp_path, SHA_B)
        runtime_slots.activate(tmp_path, SHA_C)
        in_use = runtime_slots.slots_in_use(tmp_path, specs)
        removed = runtime_slots.reclaim(tmp_path, in_use=in_use)

        assert held.name == SHA_A
        assert in_use == {SHA_A, SHA_B, SHA_C}
        assert removed == []
        assert held.is_dir()

    def test_a_held_slot_is_never_reclaimable(self, tmp_path: Path) -> None:
        """The property that matters: deleting a slot a running job holds turns
        a silent version mix into a hard ImportError -- better, still broken."""
        for sha in (SHA_A, SHA_B, SHA_C):
            _build(tmp_path, sha)
        runtime_slots.activate(tmp_path, SHA_A)
        specs = [self._Spec([self._python(str(tmp_path), SHA_C), "run.py"])]

        in_use = runtime_slots.slots_in_use(tmp_path, specs)
        removed = runtime_slots.reclaim(tmp_path, in_use=in_use)

        assert SHA_C in in_use
        assert removed == [SHA_B], "only the genuinely unreferenced slot went"
        assert (tmp_path / "releases" / SHA_C).is_dir()
        assert (tmp_path / "releases" / SHA_A).is_dir()

    def test_reclaim_fsyncs_the_releases_directory(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        for sha in (SHA_A, SHA_B, SHA_C):
            _build(tmp_path, sha)
        runtime_slots.activate(tmp_path, SHA_A)
        runtime_slots.activate(tmp_path, SHA_B)
        synced: list[Path] = []
        original = runtime_slots._fsync_directory

        def record(path: Path) -> None:
            synced.append(path)
            original(path)

        monkeypatch.setattr(runtime_slots, "_fsync_directory", record)

        assert runtime_slots.reclaim(tmp_path, in_use=set()) == [SHA_C]
        assert synced == [tmp_path / "releases"]
