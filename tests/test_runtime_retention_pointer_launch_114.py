"""#114: a process reached through ``current`` or ``previous`` proves nothing.

A job launched as ``<root>/current/venv/bin/python job.py`` (not through the
stable launcher) has an exe that resolves to the base interpreter, no
``VQ_RUNTIME_SLOT_SHA`` and, when pure Python, no slot library mapped. Before
the fix its complete group named no generation and so "proved" it held
nothing, making its old generation a removal candidate.
"""
from __future__ import annotations

import pytest

from tests.test_runtime_retention_processes_74 import _kept, _spec, proc, store  # noqa: F401
from vq import runtime_retention, runtime_slots
from vq.spec import JobState

pytestmark = [pytest.mark.no_autopatch_lifecycle_lock]


@pytest.mark.parametrize("pointer", ["current", "previous"])
@pytest.mark.parametrize(
    ("via", "kwargs"),
    [
        ("cmdline", lambda path: {"cmdline": (f"{path}/venv/bin/python", "job.py")}),
        ("cwd", lambda path: {"cwd": str(path)}),
        ("open fd", lambda path: {"fds": (f"{path}/src/run.sh",)}),
    ],
)
def test_a_job_group_naming_a_pointer_is_unresolved_and_keeps_everything(
    store, proc, pointer, via, kwargs,  # noqa: F811
) -> None:
    root, (a, b, _c, _d), cfg, specs, _services = store
    specs.append(_spec("job", JobState.RUNNING, pid=500, pgid=500))
    proc.add(500, pgid=500, **kwargs(root / pointer))
    proc.add(501, pgid=500, cmdline=("sh", "-c", "sleep 1"))

    value = runtime_retention.plan("demo", cfg, keep=0)

    assert value["census"]["process_proven_jobs"] == [], via
    assert value["census"]["unresolved_live_jobs"] == ["job"], via
    assert value["candidates"] == [], via


def test_a_stray_process_through_current_keeps_every_generation(store, proc) -> None:  # noqa: F811
    root, (a, b, _c, _d), cfg, _specs, _services = store
    proc.add(700, pgid=700, cmdline=(str(root / "current" / "venv" / "bin" / "python"), "nb.py"))

    value = runtime_retention.plan("demo", cfg, keep=0)

    assert value["candidates"] == []
    assert _kept(value)[a] == ["a live process group runs through current or previous"]


def test_a_look_alike_name_is_not_a_pointer(store, proc) -> None:  # noqa: F811
    root, (a, b, _c, _d), cfg, specs, _services = store
    specs.append(_spec("job", JobState.RUNNING, pid=500, pgid=500))
    proc.add(500, pgid=500, cmdline=(f"{root}/current.bak/run", f"{root}/previously"))

    value = runtime_retention.plan("demo", cfg, keep=0)

    assert value["census"]["process_proven_jobs"] == ["job"]
    assert value["candidates"] == [b, a]


def test_a_pointer_named_by_the_stable_launcher_path_still_proves_its_sha(store, proc) -> None:  # noqa: F811
    # The stable launcher execs releases/<sha>/... directly, never via current.
    root, (a, b, _c, _d), cfg, specs, _services = store
    specs.append(_spec("job", JobState.RUNNING, pid=500, pgid=500))
    proc.add(500, pgid=500, cmdline=(str(runtime_slots.slot_python(root, b)), "job.py"))

    value = runtime_retention.plan("demo", cfg, keep=0)

    assert value["census"]["process_proven_jobs"] == ["job"]
    assert value["candidates"] == [a]
