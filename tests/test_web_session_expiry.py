"""Tests for what a browser sees when its console session runs out.

The fleet grid and the jobs table refresh themselves every 10 s
(``hx-trigger="every 10s, refresh"``). When the 12 h session cookie
expires those polls answer 401 — and htmx 2.x does not swap a non-2xx
response, so the last good fragment stays on screen and the page keeps
polling and animating. It looks completely alive while showing
arbitrarily old data: the same failure class as a stale snapshot, and
invisible for exactly the same reason.

The recovery lives in ``static/dashboard.js``, which no test process can
drive (no browser here). So this file pins the two halves the server
owns and the browser cannot fake:

* the **contract** the handler is written against — fragments answer
  401, never a redirect and never a 200, while an HTML navigation gets
  the login form with a correctly encoded ``next=``;
* the **delivery** of the handler — ``dashboard.js`` is actually served,
  is actually pulled in by the page, and still contains the 401 path.

The last one is a file-was-dropped guard, not a substitute for a browser
test. It is here because a silently missing script is precisely how this
failure mode gets reintroduced.
"""
from __future__ import annotations

from pathlib import Path
from urllib.parse import quote

import pytest
from fastapi.testclient import TestClient

from vq import auth as vq_auth
from vq import config, paths
from vq.web import WEB_DIR, authn, create_app

#: What htmx puts on the wire for a polled fragment: its own marker plus
#: the XHR default Accept. Not ``text/html`` — that distinction is what
#: lets the server answer a fetch and a navigation differently.
HTMX_HEADERS = {"HX-Request": "true", "accept": "*/*"}

#: Fragment routes that poll on a timer behind the login. Every one of
#: them is a surface where a 401 would otherwise freeze silently.
POLLED_FRAGMENTS = (
    "/fleet/_grid",
    "/fleet/jobs/_table",
    "/fleet/doctor/_board",
    "/queue/_table",
    "/jobs/somejob00001/_log",
)


@pytest.fixture
def web_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    monkeypatch.delenv(vq_auth.ENV_WEB_TOKEN_FILE, raising=False)
    monkeypatch.setenv("VQ_WEB_FLEET", "1")
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    paths.jobs_dir().mkdir(parents=True, exist_ok=True)
    return tmp_path


@pytest.fixture
def client(web_state: Path) -> TestClient:
    return TestClient(create_app())


def _expire_session(client: TestClient, user: str, role: str) -> None:
    """Put a genuinely expired — correctly signed — session on the client.

    A missing cookie and an expired one are different states, and only
    the second is the one operators hit. The signature is valid; the
    ``exp`` claim is in the past.
    """
    client.cookies.set(
        authn.SESSION_COOKIE,
        authn.issue_session(user, role, ttl_seconds=-5),
    )


def _login(client: TestClient, user: str, password: str) -> None:
    r = client.post(
        "/fleet/login",
        data={"user": user, "password": password, "next": "/fleet"},
        follow_redirects=False,
    )
    assert r.status_code == 303, r.text


def _dashboard_js() -> str:
    return (WEB_DIR / "static" / "dashboard.js").read_text(encoding="utf-8")


class TestFragmentsFailLoudly:
    """A polled fragment must answer 401 and nothing else.

    Not a 302: htmx follows redirects transparently, so the login page
    would be swapped into the grid. Not a 200: that is the freeze.
    """

    @pytest.mark.parametrize("path", POLLED_FRAGMENTS)
    def test_expired_session_gets_401(
        self, client: TestClient, path: str
    ) -> None:
        authn.add_user("example_user", "pw", "viewer")
        _expire_session(client, "example_user", "viewer")
        r = client.get(path, headers=HTMX_HEADERS, follow_redirects=False)
        assert r.status_code == 401, f"{path} answered {r.status_code}"

    @pytest.mark.parametrize("path", POLLED_FRAGMENTS)
    def test_missing_session_gets_401(
        self, client: TestClient, path: str
    ) -> None:
        authn.add_user("example_user", "pw", "viewer")
        r = client.get(path, headers=HTMX_HEADERS, follow_redirects=False)
        assert r.status_code == 401, f"{path} answered {r.status_code}"

    @pytest.mark.parametrize("path", POLLED_FRAGMENTS)
    def test_401_body_is_not_a_login_form(
        self, client: TestClient, path: str
    ) -> None:
        """Whatever the fragment answers with, it must not be a page a
        careless swap could paint into the table as if it were data."""
        authn.add_user("example_user", "pw", "viewer")
        _expire_session(client, "example_user", "viewer")
        body = client.get(
            path, headers=HTMX_HEADERS, follow_redirects=False
        ).text
        assert 'type="password"' not in body
        assert "<form" not in body

    def test_live_session_still_serves_the_fragments(
        self, client: TestClient
    ) -> None:
        """The 401 has to mean something. If these were 401 regardless,
        the tests above would pass with the console permanently broken."""
        authn.add_user("example_user", "pw", "viewer")
        _login(client, "example_user", "pw")
        for path in ("/fleet/_grid", "/fleet/jobs/_table", "/queue/_table"):
            assert client.get(path, headers=HTMX_HEADERS).status_code == 200

    def test_fragments_stay_open_before_any_account_exists(
        self, client: TestClient
    ) -> None:
        """No accounts = the documented open tunnel posture. Nothing here
        should teach the console to 401 in that mode."""
        for path in ("/fleet/_grid", "/fleet/jobs/_table", "/queue/_table"):
            assert client.get(path, headers=HTMX_HEADERS).status_code == 200


class TestNavigationRedirects:
    """A browser navigation is the other half: it gets the login form,
    with enough of the URL preserved to come back to."""

    @pytest.mark.parametrize(
        "path",
        [
            "/fleet",
            "/fleet/jobs?state=running&host=planet-x",
            "/queue?rows=200",
            "/jobs/somejob00001",
        ],
    )
    def test_html_navigation_redirects_with_encoded_next(
        self, client: TestClient, path: str
    ) -> None:
        authn.add_user("example_user", "pw", "viewer")
        _expire_session(client, "example_user", "viewer")
        r = client.get(
            path,
            headers={"accept": "text/html"},
            follow_redirects=False,
        )
        assert r.status_code == 302
        # The whole path+query is one encoded component, so the query of
        # the page being returned to cannot be mistaken for a query of
        # the login URL itself.
        assert r.headers["location"] == (
            f"/fleet/login?next={quote(path, safe='')}"
        )

    def test_next_round_trips_back_to_the_polled_page(
        self, client: TestClient
    ) -> None:
        """What dashboard.js builds must survive the login form.

        The handler redirects to ``/fleet/login?next=<encodeURIComponent
        (pathname + search)>``; ``encodeURIComponent`` and
        ``quote(safe="")`` agree on this alphabet.
        """
        authn.add_user("example_user", "pw", "viewer")
        here = "/fleet/jobs?state=running&host=planet-x"
        form = client.get(f"/fleet/login?next={quote(here, safe='')}")
        assert form.status_code == 200
        # Jinja escapes the & in the round-tripped value; the browser
        # unescapes it back on submit.
        assert "/fleet/jobs?state=running&amp;host=planet-x" in form.text

        landed = client.post(
            "/fleet/login",
            data={"user": "example_user", "password": "pw", "next": here},
            follow_redirects=False,
        )
        assert landed.status_code == 303
        assert landed.headers["location"] == here

    def test_offsite_next_is_still_neutralized(
        self, client: TestClient
    ) -> None:
        """The handler feeds ``next=`` from the address bar, which is
        attacker-reachable via a crafted link. The server keeps the final
        say."""
        authn.add_user("example_user", "pw", "viewer")
        r = client.post(
            "/fleet/login",
            data={
                "user": "example_user",
                "password": "pw",
                "next": "//evil.example.com/",
            },
            follow_redirects=False,
        )
        assert r.status_code == 303
        assert r.headers["location"] == "/fleet"


class TestHandlerIsDelivered:
    """Guards against the recovery being present in the repo but absent
    from the browser."""

    def test_dashboard_js_is_served_verbatim(self, client: TestClient) -> None:
        authn.add_user("example_user", "pw", "viewer")
        r = client.get("/static/dashboard.js")
        # Served without a session on purpose: a script that needs a live
        # session to load is a script that is missing exactly when the
        # session-expiry banner is needed.
        assert r.status_code == 200
        assert "javascript" in r.headers["content-type"]
        assert r.text == _dashboard_js()

    @pytest.mark.parametrize(
        "path", ["/fleet", "/fleet/jobs", "/queue", "/fleet/login"]
    )
    def test_pages_pull_the_script_in(
        self, client: TestClient, path: str
    ) -> None:
        """Checked on the pages that actually freeze, not just one.

        ``base.html`` carries the tag today and every page extends it, so
        any single page would pass -- which is the trap. The pages listed
        here are the ones hosting the timed fragments (and the login page,
        which must keep loading it too, since that is where the handler
        sends people and its loop guard has to be live when they land).
        A template that stops extending the base, or grows its own, has
        to fail here rather than ship a page that polls with no recovery.
        """
        authn.add_user("example_user", "pw", "viewer")
        _login(client, "example_user", "pw")
        page = client.get(path)
        assert page.status_code == 200
        assert "/static/dashboard.js" in page.text

    def test_handler_covers_the_401_path(self) -> None:
        """The pieces the handler cannot work without. If a refactor
        renames them, re-point this test — do not drop it."""
        source = _dashboard_js()
        for needle in (
            "htmx:responseError",  # htmx 2.x's non-2xx signal
            "htmx:beforeSwap",  # and the hook that could re-enable a swap
            "401",
            "/fleet/login",
            "encodeURIComponent",  # next= must be one encoded component
        ):
            assert needle in source, f"dashboard.js lost {needle!r}"

    def test_handler_guards_against_a_redirect_loop(self) -> None:
        """A 401 raised on the login page must not send the browser to
        the login page."""
        assert "location.pathname.startsWith" in _dashboard_js()

    def test_handler_acts_on_401_and_only_401(self) -> None:
        """403 means the session is valid and the role is not (an
        operator opening /fleet/audit). Re-issuing the same credentials
        at the login form would loop, so 403 must stay untouched — it
        may be discussed in a comment, never branched on."""
        source = _dashboard_js()
        assert "=== 401" in source
        for line in source.splitlines():
            if "403" in line:
                assert line.strip().startswith("//"), line

    def test_handler_adds_no_dependency(self) -> None:
        """docs/SPEC.md § 15: htmx + vanilla JS, no bundler, no build
        step. A CDN tag or an import would also break the console the
        moment it is served somewhere without egress."""
        source = _dashboard_js()
        assert "<script" not in source
        assert "import " not in source
        assert "require(" not in source
        assert "//cdn" not in source
