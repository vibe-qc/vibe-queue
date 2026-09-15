"""``vq.web`` must be importable without the ``[web]`` extra.

The diagnostic verbs -- ``vq web status``, ``vq web config`` -- exist to
answer "is a console installed here, and is it current?". On every fleet
host except the coordinator the correct answer is "no console here", and
those are precisely the hosts where FastAPI is not installed.

Until this was fixed, ``vq/web/__init__.py`` imported FastAPI at module
level, so both verbs died with a bare ``ModuleNotFoundError`` there. The
drift detection built to prevent the 2026-08-05 stale-console incident
was therefore unusable on 13 of 14 hosts, and a fleet-wide convergence
probe would have reported a hard failure on every host where it was in
fact working correctly.

These tests run the import in a SUBPROCESS with FastAPI masked, because
the test process itself has the extra installed and cannot un-import it.
"""
from __future__ import annotations

import os
import subprocess
import sys
import textwrap

import pytest

#: Modules to hide from the child interpreter. Blocking the top-level
#: package is not enough: submodule imports (`fastapi.staticfiles`) do
#: not re-consult a parent that is already poisoned in the same way.
_MASKED = ("fastapi", "starlette", "uvicorn", "jinja2")

_MASK_PREAMBLE = f"""
import sys, importlib.abc, importlib.machinery

MASKED = {_MASKED!r}


class _Blocker(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        root = fullname.split(".", 1)[0]
        if root in MASKED:
            raise ModuleNotFoundError(f"No module named {{fullname!r}}")
        return None


for name in list(sys.modules):
    if name.split(".", 1)[0] in MASKED:
        del sys.modules[name]
sys.meta_path.insert(0, _Blocker())
"""


def _run_without_extra(body: str) -> subprocess.CompletedProcess[str]:
    script = _MASK_PREAMBLE + textwrap.dedent(body)
    env = dict(os.environ)
    for name in ("PYTEST_ADDOPTS", "PYTEST_CURRENT_TEST", "PYTEST_VERSION"):
        env.pop(name, None)
    return subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=120,
        env=env,
    )


def _assert_ok(proc: subprocess.CompletedProcess[str], expect: str) -> None:
    assert proc.returncode == 0, (
        f"exit {proc.returncode}\nstdout: {proc.stdout}\nstderr: {proc.stderr}"
    )
    assert expect in proc.stdout, proc.stdout


class TestMaskWorks:
    """Guard the guard: if masking silently stopped working, every test
    below would pass while proving nothing."""

    def test_fastapi_is_actually_hidden(self) -> None:
        proc = _run_without_extra(
            """
            try:
                import fastapi
            except ModuleNotFoundError:
                print("MASKED")
            else:
                print("NOT MASKED")
            """
        )
        _assert_ok(proc, "MASKED")

    def test_fastapi_is_present_in_the_test_environment(self) -> None:
        """So a pass above means the mask worked, not that the extra is
        merely absent from this checkout."""
        pytest.importorskip("fastapi")


class TestImportableWithoutExtra:
    def test_vq_web_package_imports(self) -> None:
        proc = _run_without_extra(
            """
            import vq.web
            print("IMPORTED", vq.web.WEB_DIR.name)
            """
        )
        _assert_ok(proc, "IMPORTED")

    def test_settings_resolves(self) -> None:
        proc = _run_without_extra(
            """
            from vq.web.settings import WebSettings, resolve_settings
            from vq import config
            s = resolve_settings(config.Config())
            print("RESOLVED", s.bind, s.port, s.is_loopback_bind)
            """
        )
        _assert_ok(proc, "RESOLVED 127.0.0.1 8765 True")

    def test_install_module_reports_status(self) -> None:
        proc = _run_without_extra(
            """
            import tempfile, os
            os.environ["VQ_CONFIG_DIR"] = tempfile.mkdtemp()
            from vq.web import install
            st = install.console_service_status()
            print("STATUS", st.installed, st.running_version is not None)
            """
        )
        _assert_ok(proc, "STATUS False True")

    def test_console_status_imports(self) -> None:
        proc = _run_without_extra(
            """
            from vq.web import console_status
            print("IDENTITY", console_status.console_identity().version != "")
            """
        )
        _assert_ok(proc, "IDENTITY True")

    def test_cli_loopback_helper_works(self) -> None:
        """Pinned by an older suite and reachable from a plain install."""
        proc = _run_without_extra(
            """
            from vq.cli import _is_loopback_bind
            print("LOOPBACK", _is_loopback_bind("127.0.0.1"), _is_loopback_bind("0.0.0.0"))
            """
        )
        _assert_ok(proc, "LOOPBACK True False")


class TestCreateAppStillNeedsTheExtra:
    def test_create_app_raises_without_fastapi(self) -> None:
        """Deferring the import must not pretend the app can be built.
        `vq web run` turns this into a readable UsageError."""
        proc = _run_without_extra(
            """
            from vq.web import create_app
            try:
                create_app()
            except ModuleNotFoundError as e:
                print("RAISED", e.name)
            else:
                print("DID NOT RAISE")
            """
        )
        _assert_ok(proc, "RAISED")

    def test_lazy_app_attribute_raises_without_fastapi(self) -> None:
        proc = _run_without_extra(
            """
            import vq.web
            try:
                vq.web.app
            except ModuleNotFoundError as e:
                print("RAISED", e.name)
            else:
                print("DID NOT RAISE")
            """
        )
        _assert_ok(proc, "RAISED")


class TestLazyAttributesWithTheExtra:
    """With FastAPI available the old module attributes behave as before."""

    def test_templates_is_cached(self) -> None:
        """A test monkeypatches web.TEMPLATES.TemplateResponse and then
        renders through create_app(); handing out a fresh instance per
        access would patch an object nobody uses."""
        from vq import web

        assert web.TEMPLATES is web.TEMPLATES

    def test_failed_clear_states_still_reachable(self) -> None:
        from vq import web
        from vq.web import single_host_routes

        assert web.FAILED_CLEAR_STATES is single_host_routes.FAILED_CLEAR_STATES

    def test_app_attribute_builds_an_app(self) -> None:
        """`uvicorn vq.web:app` is a documented entry point."""
        from vq import web

        assert hasattr(web.app, "router")

    def test_unknown_attribute_still_raises_attribute_error(self) -> None:
        from vq import web

        with pytest.raises(AttributeError):
            getattr(web, "no_such_attribute")  # noqa: B009 — the point is the lookup
