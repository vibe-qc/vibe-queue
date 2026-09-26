"""#66 review: the public snapshot writer refuses a value outside the allowlist.

``check_public_document`` used to compare key sets only, so a document
whose keys were right and whose values were not -- a host's error string
in its ``load``, an address in a calculation's ``host``, a job id in an
``alias`` -- was written to the public directory. The value constraints
the schema states (types, allowed values, the alias pattern, the display
name bound, the timestamp format) are now re-checked before anything is
written, and a projection failure is a clean refusal rather than a
traceback.
"""
from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

from tests.test_public_fleet_snapshot_v0_26_15 import (
    LEAK,
    _marked_fleet,
    _public_config,
    _run,
    writer_env,  # noqa: F401 -- fixture
)
from vq.web import public_snapshot as ps


def _doc() -> dict:
    return ps.project_public_snapshot(_marked_fleet(), _public_config())


VALUE_MUTANTS = {
    # The P2 mutant that only the marker scan used to catch.
    "load carries the down reason": (
        lambda d: d["hosts"][0].__setitem__("load", f"{LEAK} ssh: connect refused"), "load"),
    "state outside the enum": (lambda d: d["hosts"][0].__setitem__("state", LEAK), "state"),
    "name beyond the bound": (
        lambda d: d["hosts"][0].__setitem__("name", f"{LEAK}-key" * 40), "name"),
    "name not a string": (lambda d: d["hosts"][0].__setitem__("name", [LEAK]), "name"),
    "name empty": (lambda d: d["hosts"][0].__setitem__("name", ""), "name"),
    "alias with a path": (
        lambda d: d["calculations"][0].__setitem__("alias", f"{LEAK} /srv/path"), "alias"),
    "alias too long": (
        lambda d: d["calculations"][0].__setitem__(
            "alias", "a" * (ps.PUBLIC_ALIAS_MAX_LEN + 1)), "alias"),
    "calculation state nested": (
        lambda d: d["calculations"][0].__setitem__("state", {"nested": LEAK}), "state"),
    "calculation host is an address": (
        lambda d: d["calculations"][0].__setitem__("host", f"{LEAK}-192.0.2.1" * 8), "host"),
    "calculation host not a string": (
        lambda d: d["calculations"][0].__setitem__("host", 7), "host"),
    "title nested": (lambda d: d.__setitem__("title", {"x": LEAK}), "title"),
    "title beyond the bound": (lambda d: d.__setitem__("title", "t" * 65), "title"),
    "generated_at garbage": (
        lambda d: d.__setitem__("generated_at", f"{LEAK} not a time"), "generated_at"),
    "stale_after naive": (
        lambda d: d.__setitem__("stale_after", "2026-09-25T12:15:00"), "stale_after"),
    "schema wrong": (lambda d: d.__setitem__("schema", "vq.public-fleet/2"), "schema"),
    "hosts not a list": (lambda d: d.__setitem__("hosts", {}), "hosts"),
}


class TestValueGuard:
    def test_the_projection_passes_its_own_guard(self) -> None:
        ps.check_public_document(_doc())  # positive control: no exception

    @pytest.mark.parametrize("name", sorted(VALUE_MUTANTS))
    def test_a_value_outside_the_allowlist_is_refused(self, name: str) -> None:
        mutate, where = VALUE_MUTANTS[name]
        doc = _doc()
        mutate(doc)
        with pytest.raises(ValueError, match=where) as info:
            ps.check_public_document(doc)
        # The error names the path, never the value.
        assert LEAK not in str(info.value)

    @pytest.mark.parametrize("name", sorted(VALUE_MUTANTS))
    def test_nothing_is_written_for_a_refused_document(self, name: str, tmp_path: Path) -> None:
        mutate, _ = VALUE_MUTANTS[name]
        out = tmp_path / "out"
        out.mkdir()
        (out / ps.PUBLIC_JSON_NAME).write_text("old json")
        (out / ps.PUBLIC_HTML_NAME).write_text("old page")
        doc = _doc()
        mutate(doc)
        with pytest.raises(ValueError):
            ps.write_public_snapshot(doc, out)
        assert sorted(p.name for p in out.iterdir()) == ["fleet.json", "index.html"]
        assert (out / ps.PUBLIC_JSON_NAME).read_text() == "old json"
        assert (out / ps.PUBLIC_HTML_NAME).read_text() == "old page"


class TestWriterFailsClosed:
    def test_a_projection_failure_writes_nothing_and_is_not_a_traceback(
        self, writer_env: Path, monkeypatch: pytest.MonkeyPatch,  # noqa: F811
    ) -> None:
        from vq.web import fleet

        # A first run so there is something to keep.
        assert _run("--out", str(writer_env)).exit_code == 0
        before = {p.name: p.read_bytes() for p in writer_env.iterdir()}

        snapshot = _marked_fleet()
        snapshot.gathered_at = f"{LEAK}-garbage"  # a malformed sweep result
        monkeypatch.setattr(fleet, "gather_fleet_snapshot", lambda cfg=None, **_: snapshot)
        result = _run("--out", str(writer_env))
        assert result.exit_code != 0
        assert result.exception is None or isinstance(result.exception, SystemExit)
        assert "nothing written" in result.output
        assert "Traceback" not in result.output
        assert {p.name: p.read_bytes() for p in writer_env.iterdir()} == before

    def test_the_writer_runs_with_the_web_stack_blocked(
        self, writer_env: Path, monkeypatch: pytest.MonkeyPatch,  # noqa: F811
    ) -> None:
        # The module docstring promises the writer needs no ``[web]`` extra.
        blocked = ("fastapi", "starlette", "uvicorn", "jinja2")
        for name in list(sys.modules):
            if name.split(".")[0] in blocked:
                monkeypatch.delitem(sys.modules, name)
        for name in blocked:
            monkeypatch.setitem(sys.modules, name, None)  # import raises ImportError
        monkeypatch.delitem(sys.modules, "vq.web.public_snapshot", raising=False)
        module = importlib.import_module("vq.web.public_snapshot")
        assert module.SCHEMA_ID == ps.SCHEMA_ID
        with pytest.raises(ImportError):
            importlib.import_module("fastapi")
        result = _run("--out", str(writer_env))
        assert result.exit_code == 0, result.output
        assert sorted(p.name for p in writer_env.iterdir()) == ["fleet.json", "index.html"]
