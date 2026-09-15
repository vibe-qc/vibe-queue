"""No absolute home paths, employer names or private addresses in tracked content.

vibe-queue is destined to be public, and it is the member of the toolset most
exposed to this class of leak: vq is the fleet's control plane, so real host
names, real addresses and real account names are the natural vocabulary of its
docs, docstrings and fixtures. They arrive by default rather than by accident.

`.githooks/pre-commit` blocks new ones at commit time, but a hook is opt-in
(`git config --local core.hooksPath .githooks`) and git skips a missing hooks
directory silently — so this test is the half that always runs.

What this cannot check: hostnames. `docs/hosts.md` explains where the real
inventory lives and why it does not live here; keeping it out is a review
responsibility, not a mechanical one.

Keep the patterns and the allowlists in sync with the hook.
"""

from __future__ import annotations

import os
import re
import runpy
import shutil
import subprocess
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent

# Placeholders and system usernames that are legitimate references: CI and system accounts,
# the documented "user" placeholder, and the fleet's service account, which is
# a role rather than a person and which the multi-user deployment docs have to
# name. Mirrors ALLOWED_USERS in .githooks/pre-commit. Add sparingly; each
# entry weakens the gate.
_ALLOWED_USERS = ("USER", "Shared", "runner", "root", "user", "vqadmin")

_HOME_PATH = re.compile(r"/(?:Users|home)/([A-Za-z][A-Za-z0-9_.-]*)")
# Private names are configured externally; never publish the denylist itself.
_PRIVATE_TERMS = runpy.run_path(str(_ROOT / ".githooks/private_terms.py"))["load_terms"](_ROOT)


def _private_match(line: str) -> bool:
    return any(term in line.casefold() for term in _PRIVATE_TERMS)

_PRIVATE_IP = re.compile(
    r"\b(?:10\.[0-9]{1,3}|192\.168|172\.(?:1[6-9]|2[0-9]|3[01]))"
    r"\.[0-9]{1,3}\.[0-9]{1,3}\b"
)

# Meta-files where the patterns legitimately appear.
_EXEMPT = {
    ".githooks/pre-commit",
    ".mailmap",
    "tests/test_no_maintainer_paths.py",
}


def _tracked_text_files() -> list[str]:
    out = subprocess.run(
        ["git", "-C", str(_ROOT), "ls-files", "-z"],
        capture_output=True,
        text=True,
        check=True,
    )
    return [p for p in out.stdout.split("\0") if p and p not in _EXEMPT]


def _scan(match_line) -> list[str]:
    offenders: list[str] = []
    for rel in _tracked_text_files():
        try:
            text = (_ROOT / rel).read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue  # binary or unreadable: nothing to leak in review terms
        for lineno, line in enumerate(text.splitlines(), start=1):
            hit = match_line(line)
            if hit:
                offenders.append(f"{rel}:{lineno}: {hit}")
    return offenders


def test_no_absolute_home_paths_in_tracked_content():
    def match(line: str) -> str | None:
        for m in _HOME_PATH.finditer(line):
            if m.group(1) not in _ALLOWED_USERS:
                return "[redacted home path]"
        return None

    offenders = _scan(match)
    assert not offenders, (
        "absolute home paths in tracked content (use ~/, /home/USER/ or "
        "<vibe-queue-checkout> instead):\n  " + "\n  ".join(offenders)
    )


def test_no_private_terms_in_tracked_content():
    offenders = _scan(lambda line: "private-policy match" if _private_match(line) else None)
    assert not offenders, "employer name in tracked content:\n  " + "\n  ".join(offenders)


def test_no_private_addresses_in_tracked_content():
    """A real LAN address in a doc or a fixture is a topology leak.

    `docs/hosts.md` used to carry both compute hosts' static addresses and the
    fail2ban ignoreip range; it now describes the shape of a host record and
    the real inventory lives in the private operations repository. This keeps it
    that way.
    """

    def match(line: str) -> str | None:
        for _match in _PRIVATE_IP.finditer(line):
            return "[redacted private IPv4]"
        return None

    offenders = _scan(match)
    assert not offenders, (
        "private IPv4 addresses in tracked content (use a documented hostname "
        "alias or <host>; for examples use RFC 5737 documentation ranges"
        "):\n  " + "\n  ".join(offenders)
    )


def test_the_hook_exists_and_is_executable():
    """CONTRIBUTING.md tells contributors to enable it; it has to be there."""
    hook = _ROOT / ".githooks" / "pre-commit"
    assert hook.is_file(), "CONTRIBUTING.md documents .githooks/pre-commit"
    assert os.access(hook, os.X_OK), f"{hook} is not executable; git will skip it"


def test_the_hook_and_this_test_share_their_allowlists():
    """Two copies of a security allowlist that can drift is worse than one."""
    hook = (_ROOT / ".githooks" / "pre-commit").read_text(encoding="utf-8")

    users = re.search(r"^ALLOWED_USERS='([^']*)'", hook, re.MULTILINE)
    assert users, "could not find ALLOWED_USERS in .githooks/pre-commit"
    assert tuple(users.group(1).split("|")) == _ALLOWED_USERS


@pytest.mark.skipif(shutil.which("git") is None, reason="git is not installed")
@pytest.mark.parametrize(
    ("leak", "clean"),
    [
        # Synthetic negative controls must not become allowed placeholders
        # when a public snapshot sanitizes ordinary documentation examples.
        ('PATH = "' + "/" + "Users" + "/privacy_fixture_person/secret" + '"', 'PATH = "~/secret"'),
        ('LAN = "' + ".".join(("172", "31", "4", "9")) + '"', 'LAN = "<host>"'),
    ],
    ids=["home-path", "private-ip"],
)
def test_the_hook_actually_fires(tmp_path, leak: str, clean: str):
    """The guard has to fire, not merely exist."""
    repo = tmp_path / "repo"
    subprocess.run(["git", "init", "-q", str(repo)], check=True)

    def run(*args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["git", "-C", str(repo), *args],
            capture_output=True,
            text=True,
            check=False,
        )

    run("config", "user.email", "t@example.invalid")
    run("config", "user.name", "T")
    hooks = repo / ".githooks"
    hooks.mkdir()
    (hooks / "pre-commit").write_text(
        (_ROOT / ".githooks" / "pre-commit").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (hooks / "pre-commit").chmod(0o755)
    run("config", "core.hooksPath", ".githooks")

    (repo / "leak.py").write_text(leak + "\n", encoding="utf-8")
    run("add", "leak.py")
    blocked = run("commit", "-m", "leak")
    assert blocked.returncode != 0, f"the hook let {leak!r} through"
    assert "ERROR: staged content contains" in blocked.stderr

    (repo / "leak.py").write_text(clean + "\n", encoding="utf-8")
    run("add", "leak.py")
    allowed = run("commit", "-m", "no leak")
    assert allowed.returncode == 0, (
        f"the hook blocked a clean commit:\n{allowed.stdout}{allowed.stderr}"
    )


def test_privacy_hook_regressions():
    """Run isolated staged-diff regressions without any runtime dependencies."""
    import subprocess
    import sys

    root = Path(__file__).resolve().parents[1]
    completed = subprocess.run(
        [sys.executable, str(root / ".githooks" / "test_privacy_hook.py"), "-q"],
        capture_output=True, text=True, timeout=60, check=False,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
