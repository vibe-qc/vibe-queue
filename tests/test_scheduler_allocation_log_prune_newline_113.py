"""#113: the deploy-log prune never reaches outside the program's log dir.

The prune loop reads ``ls -1td "$log_dir"/deploy.*`` line by line. An entry
whose name holds a newline is printed as several lines; before the fix a
continuation line was taken as a path of its own (relative to the wrapper's
cwd, the login shell's ``$HOME`` on a real host) and ``rm -rf``'d once the
cap was passed. These cases run the real generated wrapper, as the #105 cap
tests do, with a victim directory outside ``log_dir``.
"""
from __future__ import annotations

import os
import time
from pathlib import Path

import pytest

from tests.test_scheduler_allocation_log_cap_v0_26_17 import _old_attempts, wrapper  # noqa: F401


def _crafted(log_dir: Path, name: str) -> Path:
    crafted = log_dir / name
    crafted.mkdir()
    stamp = time.time() - 7 * 3600  # the oldest entry: reached in removal mode
    os.utime(crafted, (stamp, stamp))
    return crafted


# A name holds no `/`, so each continuation line is a path relative to the
# wrapper's cwd: a sibling there, or `..`, the directory above it. `rm`
# itself refuses `..`, so that case is a control; `victim` was reachable.
@pytest.mark.parametrize("line", ["victim", ".."])
def test_a_newline_name_does_not_reach_outside_the_log_dir(
    wrapper, monkeypatch: pytest.MonkeyPatch, line: str,  # noqa: F811
) -> None:
    outer = wrapper.tmp / "outer"
    home = outer / "home"
    (home / "victim").mkdir(parents=True)
    (home / "victim" / "keep.txt").write_text("foreign data")
    (outer / "keep.txt").write_text("foreign data")
    monkeypatch.chdir(home)  # the wrapper's cwd, as ssh's $HOME
    old = _old_attempts(wrapper.log_dir, [200])  # passes the cap: removal mode
    crafted = _crafted(wrapper.log_dir, f"deploy.zz\n{line}")

    proc = wrapper.run(cap_kib=100)

    assert proc.returncode == 0
    assert (home / "victim" / "keep.txt").read_text() == "foreign data"
    assert (outer / "keep.txt").read_text() == "foreign data"
    assert not old[0].exists()  # the cap itself still applies
    assert crafted.exists()
