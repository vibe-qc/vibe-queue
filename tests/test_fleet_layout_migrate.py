"""contrib/fleet_layout_migrate.py: host layout migration, phases 1-3 (vibe-qc#1).

The tool runs against live hosts whose compute nodes it cannot see, so the
properties that matter are the ones that make it safe without that knowledge:
a plan never writes, an apply runs only the reviewed plan, protected trees are
refused however they are reached, an old path resolves at every instant of a
move, and an undo restores the very same inodes.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

_TOOL = Path(__file__).resolve().parent.parent / "contrib" / "fleet_layout_migrate.py"
_spec = importlib.util.spec_from_file_location("fleet_layout_migrate", _TOOL)
assert _spec and _spec.loader
flm = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(flm)


@pytest.fixture(autouse=True)
def _quiet_host(monkeypatch: pytest.MonkeyPatch) -> None:
    """The developer's own machine may really be running an update."""
    monkeypatch.setattr(flm, "list_own_cmdlines", lambda: [])


@pytest.fixture
def home(tmp_path: Path) -> Path:
    h = tmp_path / "home"
    h.mkdir()
    return h


def _snapshot(root: Path) -> list[tuple[str, int, int, str]]:
    rows = []
    for dirpath, dirnames, filenames in os.walk(root):
        for name in dirnames + filenames:
            p = Path(dirpath) / name
            st = p.lstat()
            rows.append((str(p.relative_to(root)), st.st_ino, st.st_mode,
                         os.readlink(p) if p.is_symlink() else ""))
    return sorted(rows)


def _run(home: Path, *argv: str) -> int:
    return flm.main(["--home", str(home), *argv])


def _plan_id(home: Path, *argv: str) -> str:
    args = flm.build_parser().parse_args(["--home", str(home), "plan", *argv])
    args.home = str(home)
    return flm._plan_from_args(args).plan_id()


def _apply(home: Path, *argv: str) -> int:
    return _run(home, "apply", *argv, "--approve", _plan_id(home, *argv))


def _receipt(home: Path, phase: int) -> Path:
    found = sorted((home / "vq/.layout/receipts").glob(f"*-phase{phase}-*.json"))
    assert found, "no receipt written"
    return found[-1]


def _exchange_works(directory: Path) -> bool:
    try:
        flm.probe_exchange(str(directory))
    except OSError:
        return False
    return True


# ---------------------------------------------------------------- phase 1


def test_plan_writes_nothing(home: Path) -> None:
    (home / "bin").mkdir()
    before = _snapshot(home)
    assert _run(home, "plan", "--phase", "1") == flm.EXIT_OK
    assert _run(home, "survey") == flm.EXIT_OK
    assert _snapshot(home) == before


def test_phase1_creates_the_roots_and_keeps_state_private(home: Path) -> None:
    assert _apply(home, "--phase", "1") == flm.EXIT_OK
    for name in ("vq", "vqstate", "vqscratch", "vqruns", "vq/programs"):
        assert (home / name).is_dir()
    assert (home / "vqstate").stat().st_mode & 0o777 == 0o700
    assert not (home / "vq/runtimes").exists(), "phase 2 decides what runtimes is"
    record = json.loads((home / "vq/.layout/layout.json").read_text())
    assert {r["path"] for r in record["roots"]} == {"~/vq", "~/vqstate", "~/vqscratch", "~/vqruns"}


def test_phase1_on_a_daemonless_host_makes_no_state_root(home: Path) -> None:
    """A scheduler login host has no queue state; an empty ~/vqstate would only look live."""
    assert _apply(home, "--phase", "1", "--daemonless") == flm.EXIT_OK
    assert not (home / "vqstate").exists()
    for name in ("vq", "vqscratch", "vqruns", "vq/programs"):
        assert (home / name).is_dir()
    assert _apply(home, "--phase", "1", "--daemonless") == flm.EXIT_OK  # idempotent


def test_phase1_adopts_what_exists_without_touching_it(home: Path) -> None:
    """A live state directory and another lane's files must survive untouched."""
    (home / "vqstate/jobs/abc").mkdir(parents=True, mode=0o700)
    (home / "vq/someone-elses-venv").mkdir(parents=True)
    before = _snapshot(home / "vqstate"), (home / "vqstate").stat().st_mode
    assert _apply(home, "--phase", "1") == flm.EXIT_OK
    assert (_snapshot(home / "vqstate"), (home / "vqstate").stat().st_mode) == before
    assert (home / "vq/someone-elses-venv").is_dir()
    assert (home / "vq/programs").is_dir()

    # Undo removes only what this tool made, and only while it is empty.
    assert _run(home, "undo", "--grace", "0", "--receipt", str(_receipt(home, 1)),
                "--approve", json.loads(_receipt(home, 1).read_text())["plan_id"]) == flm.EXIT_OK
    assert (home / "vqstate/jobs/abc").is_dir()
    assert (home / "vq/someone-elses-venv").is_dir()
    assert not (home / "vqruns").exists()
    assert not (home / "vq/programs").exists()


def test_phase1_is_idempotent(home: Path) -> None:
    assert _apply(home, "--phase", "1") == flm.EXIT_OK
    before = _snapshot(home)
    assert _apply(home, "--phase", "1") == flm.EXIT_OK
    assert _snapshot(home) == before


def test_a_failed_first_operation_reports_the_real_error(
    home: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Seen on a host whose home was over quota: the very first mkdir fails, so
    no receipts directory exists either. That must not mask the cause."""
    reviewed = _plan_id(home, "--phase", "1")

    def over_quota(path: str, mode: int = 0o777) -> None:
        raise OSError(122, "Disk quota exceeded", path)

    monkeypatch.setattr(flm.os, "mkdir", over_quota)
    assert _run(home, "apply", "--phase", "1", "--approve", reviewed) == flm.EXIT_FAILED
    out = capsys.readouterr().out
    assert "Disk quota exceeded" in out and "nothing was changed" in out
    monkeypatch.undo()
    assert sorted(p.name for p in home.iterdir()) == []


def test_operations_that_ran_are_never_left_without_a_record(
    home: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """If the host cannot store the receipt, the operator still gets it."""
    reviewed = _plan_id(home, "--phase", "1")

    def cannot_write(path: str, doc: object) -> None:
        raise OSError(122, "Disk quota exceeded", path)

    monkeypatch.setattr(flm, "_write_json_atomic", cannot_write)
    assert _run(home, "apply", "--phase", "1", "--approve", reviewed) == flm.EXIT_FAILED
    out = capsys.readouterr().out
    assert "NO RECEIPT COULD BE WRITTEN" in out and f'"plan_id": "{reviewed}"' in out
    assert (home / "vqruns").is_dir()


def test_apply_refuses_an_unreviewed_plan(home: Path) -> None:
    assert _run(home, "apply", "--phase", "1", "--approve", "0" * 64) == flm.EXIT_MISMATCH
    assert not (home / "vq").exists()


def test_apply_refuses_when_the_host_changed_after_review(home: Path) -> None:
    reviewed = _plan_id(home, "--phase", "1")
    (home / "vqscratch").mkdir()  # someone else acted in between
    assert _run(home, "apply", "--phase", "1", "--approve", reviewed) == flm.EXIT_MISMATCH
    assert not (home / "vq").exists()


# ------------------------------------------------------- protected trees


@pytest.mark.parametrize("protected", ["Silo", "Nextcloud", "gitlab"])
def test_a_root_that_leads_into_a_protected_tree_is_refused(home: Path, protected: str) -> None:
    (home / protected / "x").mkdir(parents=True)
    (home / "vq").symlink_to(home / protected / "x")
    before = _snapshot(home)
    assert _run(home, "plan", "--phase", "1") == flm.EXIT_BLOCKED
    assert _apply(home, "--phase", "1") == flm.EXIT_BLOCKED
    assert _snapshot(home) == before


def test_a_protected_tree_reached_through_its_mount_point_is_refused(
    home: Path, tmp_path: Path
) -> None:
    """~/Silo is a symlink onto a data disk on real hosts; both names count."""
    disk = tmp_path / "mnt/data/Silo"
    (disk / "x").mkdir(parents=True)
    (home / "Silo").symlink_to(disk)
    (home / "vqruns").symlink_to(disk / "x")
    assert _run(home, "plan", "--phase", "1") == flm.EXIT_BLOCKED


def test_scope_refuses_everything_outside_the_layout(home: Path) -> None:
    for bad in (home / ".config/vq/config.toml", home / "gitlab/vibeqc-dev/x", home.parent / "y"):
        with pytest.raises(flm.Refusal):
            flm.check_scope(str(home), str(bad))
    flm.check_scope(str(home), str(home / "vq/programs/orca-6.1.1"))


# ------------------------------------------------------ deploys in flight


def test_a_running_deploy_blocks_apply(home: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    reviewed = _plan_id(home, "--phase", "1")
    monkeypatch.setattr(flm, "list_own_cmdlines",
                        lambda: [(4242, "/opt/x/bin/vq admin update vibeqc-release compute-a")])
    assert _run(home, "apply", "--phase", "1", "--approve", reviewed) == flm.EXIT_BLOCKED
    assert not (home / "vq").exists()


def test_a_store_lock_blocks_and_a_stale_staging_entry_does_not(home: Path) -> None:
    store = home / ".local/libexec/vq-site/runtimes"
    (store / "vibeqc-dev/staging/old").mkdir(parents=True)
    stale = time.time() - 3 * 24 * 3600
    os.utime(store / "vibeqc-dev/staging/old", (stale, stale))
    blockers, notes = flm.deploy_blockers(str(home))
    assert blockers == [] and len(notes) == 1

    (store / "locks").mkdir()
    (store / "locks/vibeqc-dev.lock").touch()
    blockers, _ = flm.deploy_blockers(str(home))
    assert len(blockers) == 1 and "lock" in blockers[0]


def test_an_update_marker_blocks(home: Path) -> None:
    (home / ".local/share/vq").mkdir(parents=True)
    (home / ".local/share/vq/admin-update-in-progress").touch()
    blockers, _ = flm.deploy_blockers(str(home))
    assert len(blockers) == 1 and "marker" in blockers[0]


def test_a_retired_marker_is_a_record_not_a_blocker(home: Path) -> None:
    """A maintainer retires a marker by renaming it; two such leftovers on
    real hosts blocked every plan there for a week."""
    (home / ".local/share/vq").mkdir(parents=True)
    for name in ("admin-update-in-progress.stale-8be73cbb",
                 "admin-update-in-progress.maintainer-removed-20260830"):
        (home / ".local/share/vq" / name).touch()
    blockers, notes = flm.deploy_blockers(str(home))
    assert blockers == []
    assert len(notes) == 2 and all("ignored" in n for n in notes)


# ---------------------------------------------------------------- phase 2


def _legacy_store(home: Path, site: str = "site") -> Path:
    store = home / f".local/libexec/vq-{site}/runtimes"
    for comp, sha in (("vibeqc-dev", "a" * 40), ("vibeqc-release", "b" * 40)):
        (store / comp / "releases" / sha / "source").mkdir(parents=True)
        (store / comp / "current").symlink_to(f"releases/{sha}")
    return store


def test_phase2_needs_phase1_but_can_be_previewed(
    home: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """A reviewer should see all three phases before the first one runs."""
    _legacy_store(home)
    assert _run(home, "plan", "--phase", "2") == flm.EXIT_BLOCKED
    assert "symlink    ~/vq/runtimes ->" in capsys.readouterr().out
    assert _apply(home, "--phase", "2") == flm.EXIT_BLOCKED
    assert not (home / "vq").exists()


def test_an_approval_cannot_be_replayed_on_another_host(
    home: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two hosts with one home path and the same gaps would otherwise share an id."""
    reviewed = _plan_id(home, "--phase", "1")
    monkeypatch.setattr(flm.socket, "gethostname", lambda: "another-host")
    assert _run(home, "apply", "--phase", "1", "--approve", reviewed) == flm.EXIT_MISMATCH
    assert not (home / "vq").exists()


def test_phase2_names_the_existing_store_without_moving_it(home: Path) -> None:
    store = _legacy_store(home)
    assert _apply(home, "--phase", "1") == flm.EXIT_OK
    before = _snapshot(store)
    assert _apply(home, "--phase", "2") == flm.EXIT_OK
    assert _snapshot(store) == before, "the store itself must not change"
    assert (home / "vq/runtimes").is_symlink()
    for comp in ("vibeqc-dev", "vibeqc-release"):
        new = (home / "vq/runtimes" / comp / "current/source").resolve(strict=True)
        old = (store / comp / "current/source").resolve(strict=True)
        assert new == old
    assert _apply(home, "--phase", "2") == flm.EXIT_OK  # idempotent


def test_phase2_without_a_store_makes_an_empty_one(home: Path) -> None:
    assert _apply(home, "--phase", "1") == flm.EXIT_OK
    assert _apply(home, "--phase", "2") == flm.EXIT_OK
    assert (home / "vq/runtimes").is_dir() and not (home / "vq/runtimes").is_symlink()


def _packed_host(home: Path) -> Path:
    """A scheduler site that ships runtimes as bundles behind per-SHA wrappers."""
    libexec = home / ".local/libexec/vq-site"
    libexec.mkdir(parents=True)
    wrapper = libexec / f"vibeqc-release-0.17.1-{'c' * 40}-python"
    wrapper.write_text("#!/bin/sh\n")
    wrapper.chmod(0o700)
    return wrapper


def _packed_release(home: Path, wrapper: Path, sha: str = "c" * 40) -> Path:
    rel = home / "vq/runtimes/vibeqc-release/releases" / sha
    rel.mkdir(parents=True)
    (rel / "bundle.tar.gz").write_bytes(b"bundle")
    (rel / "SHA256SUMS").write_text(f"{hashlib.sha256(b'bundle').hexdigest()}  bundle.tar.gz\n")
    (rel / "python").symlink_to(wrapper)
    return rel


def test_phase2_will_not_invent_an_empty_store_on_a_packed_release_host(home: Path) -> None:
    """An empty ~/vq/runtimes beside runtimes it does not describe looks live and is not."""
    _packed_host(home)
    assert _apply(home, "--phase", "1", "--daemonless") == flm.EXIT_OK
    assert _run(home, "plan", "--phase", "2") == flm.EXIT_BLOCKED
    assert not (home / "vq/runtimes").exists()


def test_phase2_accepts_a_packed_release_store_and_changes_nothing(
    home: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """A release may be a bundle plus the wrapper that runs it. The host is not
    exempt from the layout; the tool checks its store and leaves it alone."""
    wrapper = _packed_host(home)
    assert _apply(home, "--phase", "1", "--daemonless") == flm.EXIT_OK
    rel = _packed_release(home, wrapper)
    (home / "vq/runtimes/vibeqc-release/current").symlink_to(f"releases/{rel.name}")
    before = _snapshot(home)
    assert _run(home, "plan", "--phase", "2") == flm.EXIT_OK
    assert "packed-release store: 1 release(s) in 1 component(s)" in capsys.readouterr().out
    assert _apply(home, "--phase", "2") == flm.EXIT_OK
    assert _snapshot(home) == before


@pytest.mark.parametrize("damage", ["bundle", "wrapper", "pointer"])
def test_phase2_reports_a_damaged_packed_release_store(home: Path, damage: str) -> None:
    wrapper = _packed_host(home)
    assert _apply(home, "--phase", "1", "--daemonless") == flm.EXIT_OK
    rel = _packed_release(home, wrapper)
    (home / "vq/runtimes/vibeqc-release/current").symlink_to(f"releases/{rel.name}")
    if damage == "bundle":
        (rel / "bundle.tar.gz").unlink()
    elif damage == "wrapper":
        wrapper.unlink()
    else:
        (home / "vq/runtimes/vibeqc-release/previous").symlink_to("releases/" + "d" * 40)
    assert _run(home, "plan", "--phase", "2") == flm.EXIT_BLOCKED


def test_phase2_will_not_choose_between_two_stores(home: Path) -> None:
    _legacy_store(home, "one")
    _legacy_store(home, "two")
    assert _apply(home, "--phase", "1") == flm.EXIT_OK
    assert _run(home, "plan", "--phase", "2") == flm.EXIT_BLOCKED
    assert not (home / "vq/runtimes").exists()


# ---------------------------------------------------------------- phase 3


def _bin_with_payloads(home: Path) -> Path:
    b = home / "bin"
    (b / "orca_6_1_1_linux").mkdir(parents=True)
    (b / "orca_6_1_1_linux/orca").write_text("#!/bin/sh\necho main\n")
    (b / "orca_6_1_1_linux/orca_scf").write_text("#!/bin/sh\necho child\n")
    for f in (b / "orca_6_1_1_linux").iterdir():
        f.chmod(0o755)
    (b / "crystal23").write_bytes(b"\x7fELF" + b"\0" * 64)
    (b / "crystal23").chmod(0o750)
    (b / "properties23").write_bytes(b"\x7fELF" + b"\0" * 64)
    (b / "crystal23_v1_0_1_dist.tar.gz").write_bytes(b"archive")
    (b / "crystal").write_text('#!/bin/sh\nexec "$HOME/bin/crystal23" "$@"\n')
    (b / "orca.bak-20260101").write_text("#!/bin/sh\n")
    (b / "mystery").write_bytes(b"\x7fELF" + b"\0" * 8)
    return b


_P3 = ("--phase", "3",
       "--program", "orca-6.1.1=orca_6_1_1_linux",
       "--program", "crystal23-1.0.1=crystal23,properties23",
       "--archive", "crystal23-1.0.1=crystal23_v1_0_1_dist.tar.gz")


def test_survey_tells_payloads_from_wrappers(home: Path) -> None:
    _bin_with_payloads(home)
    kinds = {row["name"]: row["kind"] for row in flm.survey(str(home))["bin"]}
    assert kinds == {
        "orca_6_1_1_linux": "payload-dir", "crystal23": "payload-file",
        "properties23": "payload-file", "crystal23_v1_0_1_dist.tar.gz": "archive",
        "crystal": "wrapper", "orca.bak-20260101": "backup", "mystery": "payload-file",
    }


def test_phase3_moves_payloads_and_every_old_path_still_resolves(home: Path) -> None:
    b = _bin_with_payloads(home)
    assert _apply(home, "--phase", "1") == flm.EXIT_OK
    if not _exchange_works(home / "vq/.layout"):
        pytest.skip("no atomic exchange on this filesystem")
    inode = {n: (b / n).stat().st_ino for n in ("orca_6_1_1_linux", "crystal23", "properties23")}
    mode = (b / "crystal23").stat().st_mode

    assert _apply(home, *_P3) == flm.EXIT_OK

    programs = home / "vq/programs"
    assert (b / "orca_6_1_1_linux").is_symlink()
    assert (programs / "orca-6.1.1").is_dir() and not (programs / "orca-6.1.1").is_symlink()
    assert (programs / "orca-6.1.1").stat().st_ino == inode["orca_6_1_1_linux"]
    assert subprocess.check_output([str(b / "orca_6_1_1_linux/orca_scf")]) == b"child\n"
    for name in ("crystal23", "properties23"):
        assert (b / name).is_symlink()
        assert (b / name).stat().st_ino == inode[name], "same inode: nothing was copied"
        assert (programs / "crystal23-1.0.1/bin" / name).stat().st_ino == inode[name]
    assert (b / "crystal23").stat().st_mode == mode
    assert os.readlink(programs / "orca") == "orca-6.1.1"
    assert os.readlink(programs / "crystal23") == "crystal23-1.0.1"
    # Installer archives are kept, under their vendor names, beside a manifest
    # that `sha256sum -c` can check.
    kept = home / "vq/installers/crystal23-1.0.1"
    moved = kept / "crystal23_v1_0_1_dist.tar.gz"
    assert moved.read_bytes() == b"archive" and not (b / moved.name).exists()
    assert (kept / "SHA256SUMS").read_text() == (
        f"{hashlib.sha256(b'archive').hexdigest()}  crystal23_v1_0_1_dist.tar.gz\n")
    assert sorted(p.name for p in programs.iterdir()) == [
        "crystal23", "crystal23-1.0.1", "orca", "orca-6.1.1"]
    # Wrappers, backups and the payload nobody mapped are left exactly alone.
    for name in ("crystal", "orca.bak-20260101", "mystery"):
        assert (b / name).is_file() and not (b / name).is_symlink()

    assert _apply(home, *_P3) == flm.EXIT_OK  # idempotent: already migrated


def test_phase3_undo_restores_the_same_inodes(home: Path) -> None:
    b = _bin_with_payloads(home)
    assert _apply(home, "--phase", "1") == flm.EXIT_OK
    if not _exchange_works(home / "vq/.layout"):
        pytest.skip("no atomic exchange on this filesystem")
    before = _snapshot(b)
    assert _apply(home, *_P3) == flm.EXIT_OK
    receipt = _receipt(home, 3)
    plan_id = json.loads(receipt.read_text())["plan_id"]

    shown = _snapshot(home)
    assert _run(home, "undo", "--receipt", str(receipt)) == flm.EXIT_OK
    assert _snapshot(home) == shown, "undo without --approve only shows the steps"

    assert _run(home, "undo", "--grace", "0", "--receipt", str(receipt),
                "--approve", plan_id) == flm.EXIT_OK
    assert _snapshot(b) == before
    assert sorted(p.name for p in (home / "vq/programs").iterdir()) == []
    assert not (home / "vq/installers").exists(), "manifest and its directories are undone too"


_HAMMER = """
import collections, errno, json, os, sys
stop, phase_file, paths = sys.argv[1], sys.argv[2], sys.argv[3:]
misses = collections.defaultdict(collections.Counter)
lookups = 0
while not os.path.exists(stop):
    for p in paths:
        lookups += 1
        try:
            with open(p, "rb") as fh:
                fh.read(4)
        except OSError as exc:
            with open(phase_file) as fh:
                phase = fh.read()
            misses[phase + " " + p][errno.errorcode.get(exc.errno, str(exc.errno))] += 1
print(json.dumps({"lookups": lookups, "misses": {k: dict(c) for k, c in misses.items()}}))
"""

# Darwin's name cache lets a lookup read a symlink from a vnode that has just
# stopped being one, and open() fails with a transient EINVAL. A plain
# rename(2) does it with none of this tool's code involved (see the rename
# canary below), roughly once in 1,000 to 20,000 lookups. It is never ENOENT
# or ELOOP, which is what a move with a real gap produces, so those two stay
# forbidden on every platform wherever the tool claims to have no gap.
_PLATFORM_NOISE = {"EINVAL"} if sys.platform == "darwin" else set()


def _hammer(stop: Path, phase: Path, *paths: Path) -> subprocess.Popen[str]:
    phase.write_text("idle")
    return subprocess.Popen(
        [sys.executable, "-c", _HAMMER, str(stop), str(phase), *map(str, paths)],
        stdout=subprocess.PIPE, text=True)


def _finish(proc: subprocess.Popen[str], stop: Path) -> tuple[int, dict[str, dict[str, int]]]:
    stop.touch()
    out, _ = proc.communicate(timeout=60)
    result = json.loads(out)
    return result["lookups"], result["misses"]


def _enter(phase: Path, name: str) -> None:
    """Switch phase with a margin on both sides, so a miss is attributed to the
    operation that caused it: the reader names the phase it reads just after."""
    time.sleep(0.02)
    phase.write_text(name)
    time.sleep(0.02)


def test_the_old_path_resolves_at_every_instant_of_the_move(home: Path) -> None:
    """A job on a node this tool cannot see keeps opening files by the old
    absolute path while the payload is moved, and moved back, underneath it.

    apply, both kinds:  no failed lookup. Nothing can reach the new location
                        until the operation that moves the payload creates the
                        route, so there is no gap to fall into.
    undo, file:         no failed lookup. The hard link at the new location
                        outlives the undo by a grace period.
    undo, directory:    a directory has one name, so a lookup that already read
                        the old symlink's text can arrive in the two-syscall
                        gap and see ELOOP. Held to "rare", and documented.

    A rename-then-symlink in place of the exchange fails this with dozens of
    ENOENT during apply.
    """
    b = _bin_with_payloads(home)
    assert _apply(home, "--phase", "1") == flm.EXIT_OK
    if not _exchange_works(home / "vq/.layout"):
        pytest.skip("no atomic exchange on this filesystem")
    in_dir, a_file = b / "orca_6_1_1_linux/orca_scf", b / "crystal23"
    stop, phase = home / "stop", home / "phase"
    proc = _hammer(stop, phase, in_dir, a_file)
    try:
        for _ in range(25):
            _enter(phase, "apply")
            assert _apply(home, *_P3) == flm.EXIT_OK
            receipt = _receipt(home, 3)
            plan_id = json.loads(receipt.read_text())["plan_id"]
            _enter(phase, "undo")
            assert _run(home, "undo", "--grace", "0.05", "--receipt", str(receipt),
                        "--approve", plan_id) == flm.EXIT_OK
            _enter(phase, "idle")
            receipt.unlink()
    finally:
        lookups, misses = _finish(proc, stop)
    assert lookups > 1000, "the reader never really ran against the moves"
    for key, seen in misses.items():
        where = f"{key}: {seen} in {lookups} lookups"
        if key == f"undo {in_dir}":
            assert set(seen) <= _PLATFORM_NOISE | {"ELOOP"}, where
        else:
            assert set(seen) <= _PLATFORM_NOISE, where
        assert sum(seen.values()) <= lookups // 500, where


def test_undoing_a_file_move_keeps_both_names_until_the_grace_period_ends(home: Path) -> None:
    b = _bin_with_payloads(home)
    assert _apply(home, "--phase", "1") == flm.EXIT_OK
    argv = ("--phase", "3", "--program", "crystal23-1.0.1=crystal23")
    assert _apply(home, *argv) == flm.EXIT_OK
    receipt = json.loads(_receipt(home, 3).read_text())
    moved = home / "vq/programs/crystal23-1.0.1/bin/crystal23"
    relocate = next(op for op in receipt["ops"] if op["op"] == "relocate_file")

    deferred: list[str] = []
    assert flm.undo_op(str(home), relocate, deferred) is None
    assert not (b / "crystal23").is_symlink()
    assert moved.samefile(b / "crystal23"), "a lookup already heading for the new name still lands"
    assert deferred == [str(moved)]


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="the fleet's guarantee is Linux's")
def test_plain_rename_over_a_name_is_lookup_atomic_on_linux(tmp_path: Path) -> None:
    """The file move is only as good as rename(2). No tool code here: if this
    ever fails on a fleet filesystem, the file move must not be used on it."""
    real, name, tmp = tmp_path / "real", tmp_path / "name", tmp_path / ".tmp"
    real.write_bytes(b"data")
    os.link(real, name)
    stop = tmp_path / "stop"
    proc = _hammer(stop, tmp_path / "phase", name)
    try:
        for _ in range(3000):
            os.symlink(real, tmp)
            os.replace(tmp, name)  # file -> symlink: the forward step
            os.link(real, tmp)
            os.replace(tmp, name)  # symlink -> file: the undo step
    finally:
        lookups, misses = _finish(proc, stop)
    assert lookups > 1000 and misses == {}, (lookups, misses)


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="the fleet's guarantee is Linux's")
def test_a_bare_exchange_is_lookup_atomic_on_linux(tmp_path: Path) -> None:
    """The directory move is only as good as renameat2(RENAME_EXCHANGE).

    Only the forward exchange is judged. Swapping back is the undo direction,
    whose in-flight lookups can land on the self-referential placeholder by
    construction; counting those would blame the kernel for this test's reset.
    """
    if not _exchange_works(tmp_path):
        pytest.skip("no atomic exchange on this filesystem")
    old, new = tmp_path / "old", tmp_path / "new"
    old.mkdir()
    (old / "f").write_bytes(b"data")
    os.symlink(new, new)
    stop, phase = tmp_path / "stop", tmp_path / "phase"
    proc = _hammer(stop, phase, old / "f")
    try:
        for _ in range(200):
            _enter(phase, "forward")
            flm.exchange(str(old), str(new))  # old becomes the symlink
            _enter(phase, "reset")
            flm.exchange(str(old), str(new))
    finally:
        lookups, misses = _finish(proc, stop)
    forward = {k: v for k, v in misses.items() if k.startswith("forward ")}
    assert lookups > 1000 and forward == {}, (lookups, misses)


def test_a_failed_exchange_changes_nothing(home: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    b = _bin_with_payloads(home)
    assert _apply(home, "--phase", "1") == flm.EXIT_OK
    before = _snapshot(b)

    def broken(a: str, b_: str) -> None:
        raise OSError(22, "exchange is not supported here")

    monkeypatch.setattr(flm, "exchange", broken)
    monkeypatch.setattr(flm, "probe_exchange", lambda d: None)
    argv = ("--phase", "3", "--program", "orca-6.1.1=orca_6_1_1_linux")
    assert _apply(home, *argv) == flm.EXIT_FAILED
    assert _snapshot(b) == before
    assert not (home / "vq/programs/orca-6.1.1").exists()
    assert json.loads(_receipt(home, 3).read_text())["status"] == "failed"


def test_an_unsupported_filesystem_is_found_before_anything_moves(
    home: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    b = _bin_with_payloads(home)
    assert _apply(home, "--phase", "1") == flm.EXIT_OK
    before = _snapshot(home)

    def broken(a: str, b_: str) -> None:
        raise OSError(22, "exchange is not supported here")

    monkeypatch.setattr(flm, "exchange", broken)
    with pytest.raises(OSError):
        _apply(home, *_P3)
    assert _snapshot(b) == _snapshot(home / "bin")
    assert [r for r in _snapshot(home) if "exchange-probe" not in r[0]] == before


def test_phase3_refuses_bad_mappings(home: Path) -> None:
    _bin_with_payloads(home)
    assert _apply(home, "--phase", "1") == flm.EXIT_OK
    assert _run(home, "plan", "--phase", "3", "--program", "orca=orca_6_1_1_linux") \
        == flm.EXIT_USAGE  # no version
    assert _run(home, "plan", "--phase", "3", "--program", "orca-6.1.1=../bin/orca") \
        == flm.EXIT_USAGE  # not a plain name
    assert _run(home, "plan", "--phase", "3", "--program", "orca-6.1.1=absent") \
        == flm.EXIT_BLOCKED
    assert _run(home, "plan", "--phase", "3",
                "--program", "orca-6.1.1=orca_6_1_1_linux,crystal23") == flm.EXIT_BLOCKED
    assert _run(home, "plan", "--phase", "3", "--archive", "crystal23-1.0.1=crystal23") \
        == flm.EXIT_BLOCKED  # a binary is not an archive


def test_phase3_never_overwrites_an_existing_program(home: Path) -> None:
    _bin_with_payloads(home)
    assert _apply(home, "--phase", "1") == flm.EXIT_OK
    (home / "vq/programs/orca-6.1.1").mkdir()
    assert _run(home, "plan", "--phase", "3",
                "--program", "orca-6.1.1=orca_6_1_1_linux") == flm.EXIT_BLOCKED


def test_the_tool_is_piped_over_ssh_so_it_must_run_from_stdin(home: Path) -> None:
    done = subprocess.run(
        [sys.executable, "-", "--home", str(home), "plan", "--phase", "1"],
        input=_TOOL.read_text(), capture_output=True, text=True, check=False,
    )
    # This process list is the real one: a build running on the developer's
    # machine legitimately reports a blocker, and the plan is still printed.
    assert done.returncode in (flm.EXIT_OK, flm.EXIT_BLOCKED), done.stderr
    assert "plan id:" in done.stdout and "read-only" in done.stdout
    assert not (home / "vq").exists()


# ---------------------------------------------------------------- phase 4


def _state_root(home: Path, rel: str = ".local/share/vq") -> Path:
    root = home / rel
    (root / "queue").mkdir(parents=True)
    (root / "jobs/abc123").mkdir(parents=True)
    (root / "workdirs/abc123").mkdir(parents=True)
    spec = {"workdir": str(root / "workdirs/abc123")}
    (root / "jobs/abc123/spec.json").write_text(json.dumps(spec))
    return root


def test_phase4_names_the_state_root_and_never_moves_it(
    home: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """vq's managed update requires its state root to be a REAL directory (an
    lstat check on the update lock directory). Moving the root and leaving a
    symlink where vq computes it made every `vq admin update` on that host
    refuse. So the root stays put, the daemon keeps running, and ~/vqstate is
    only a name for it."""
    root = _state_root(home)
    assert _apply(home, "--phase", "1") == flm.EXIT_OK
    monkeypatch.setattr(flm, "vq_daemons", lambda h: [(4242, str(root))])  # daemon is UP
    before = _snapshot(root)
    argv = ("--phase", "4", "--state-root", str(root))

    assert _apply(home, *argv) == flm.EXIT_OK

    assert _snapshot(root) == before, "the state root itself must not change"
    assert root.is_dir() and not root.is_symlink(), "vq must still find a real directory"
    vqstate = home / "vqstate"
    assert vqstate.is_symlink() and os.readlink(vqstate) == str(root)
    assert (vqstate / "jobs/abc123/spec.json").samefile(root / "jobs/abc123/spec.json")
    assert _apply(home, *argv) == flm.EXIT_OK  # idempotent

    receipt = _receipt(home, 4)
    assert _run(home, "undo", "--grace", "0", "--receipt", str(receipt), "--approve",
                json.loads(receipt.read_text())["plan_id"]) == flm.EXIT_OK
    assert vqstate.is_dir() and not vqstate.is_symlink() and list(vqstate.iterdir()) == []
    assert vqstate.stat().st_mode & 0o777 == 0o700, "the empty root comes back as phase 1 made it"
    assert _snapshot(root) == before


def test_phase4_flags_a_state_root_that_is_already_a_symlink(
    home: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The state this tool once left a host in: managed updates are refused there."""
    real = _state_root(home, "elsewhere/vq")
    (home / ".local/share").mkdir(parents=True)
    (home / ".local/share/vq").symlink_to(real)
    assert _apply(home, "--phase", "1") == flm.EXIT_OK
    monkeypatch.setattr(flm, "vq_daemons", lambda h: [])
    before = _snapshot(home)
    assert _apply(home, "--phase", "4", "--state-root", str(home / ".local/share/vq")) \
        == flm.EXIT_BLOCKED
    assert _snapshot(home) == before


def test_phase4_refuses_a_root_the_running_daemon_does_not_use(
    home: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A host can carry a stale, state-shaped directory beside the live one."""
    stale = _state_root(home, "vq-state-root")
    live = _state_root(home, ".local/share/vq")
    assert _apply(home, "--phase", "1") == flm.EXIT_OK
    monkeypatch.setattr(flm, "vq_daemons", lambda h: [(4242, str(live))])
    before = _snapshot(home)
    assert _apply(home, "--phase", "4", "--state-root", str(stale)) == flm.EXIT_BLOCKED
    assert _snapshot(home) == before


def test_phase4_names_a_root_on_another_disk(
    home: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    disk_root = _state_root(tmp_path, "mnt/data/vq-state")
    assert _apply(home, "--phase", "1") == flm.EXIT_OK
    monkeypatch.setattr(flm, "vq_daemons", lambda h: [(4242, str(disk_root))])
    before = _snapshot(disk_root)
    assert _apply(home, "--phase", "4", "--state-root", str(disk_root)) == flm.EXIT_OK
    assert _snapshot(disk_root) == before
    assert (home / "vqstate").is_symlink()
    assert (home / "vqstate/jobs/abc123/spec.json").samefile(disk_root / "jobs/abc123/spec.json")


def test_phase4_never_merges_two_stores(home: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = _state_root(home)
    (home / "vqstate/jobs/other").mkdir(parents=True)
    assert _apply(home, "--phase", "1") == flm.EXIT_OK
    monkeypatch.setattr(flm, "vq_daemons", lambda h: [])
    before = _snapshot(home)
    assert _apply(home, "--phase", "4", "--state-root", str(root)) == flm.EXIT_BLOCKED
    assert _snapshot(home) == before


def test_phase4_refuses_what_is_not_a_state_root(
    home: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert _apply(home, "--phase", "1") == flm.EXIT_OK
    monkeypatch.setattr(flm, "vq_daemons", lambda h: [])
    (home / "notstate").mkdir()
    assert _run(home, "plan", "--phase", "4") == flm.EXIT_BLOCKED  # no root named
    assert _run(home, "plan", "--phase", "4", "--state-root", str(home / "absent")) \
        == flm.EXIT_BLOCKED
    assert _run(home, "plan", "--phase", "4", "--state-root", str(home / "notstate")) \
        == flm.EXIT_BLOCKED
    in_silo = _state_root(home, "Silo/vq-state")  # the name must not lead into a sync target
    assert _run(home, "plan", "--phase", "4", "--state-root", str(in_silo)) == flm.EXIT_BLOCKED


def test_a_daemon_command_line_is_recognised() -> None:
    for cmd in ("/home/user/x/.venv/bin/python /home/user/.local/bin/vq daemon run",
                "/opt/x/bin/vq daemon run --max-jobs 1", "vq --debug daemon run"):
        assert flm.DAEMON_RE.search(cmd), cmd
    for cmd in ("vq daemon ping", "vq web run", "python -m vq.resource_receipt -- x daemon run.py"):
        assert not flm.DAEMON_RE.search(cmd), cmd
