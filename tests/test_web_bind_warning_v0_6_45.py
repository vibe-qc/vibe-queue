"""v0.6.45: ``vq web run`` warns on non-loopback bind.

Audit finding (security review #2 in the 2026-05-24 pass) flagged
that the read-only HTML pages + OpenAPI `/docs` have no auth, so a
non-loopback bind without a fronting TLS reverse proxy leaks job
names / cwds / stdout-stderr tails / host metadata / queue state to
anyone who can reach the port. The default localhost bind is silent;
any non-loopback bind triggers a loud stderr warning unless the
operator passes the ``--i-understand-public-bind`` acknowledgement
flag.

These tests cover the loopback classifier directly (no uvicorn spin-
up required, so they run fast in CI) plus the click-level integration
where the CLI invokes uvicorn through a patched stub.
"""
from __future__ import annotations

from unittest.mock import patch

from click.testing import CliRunner

from vq.cli import _is_loopback_bind, main


class TestIsLoopbackBind:
    """Classification — what counts as a safe-by-default bind."""

    def test_ipv4_loopback_127_0_0_1(self) -> None:
        assert _is_loopback_bind("127.0.0.1") is True

    def test_ipv4_loopback_anywhere_in_127_0_0_0_8(self) -> None:
        # The whole 127.0.0.0/8 block is loopback per RFC 1122 §
        # 3.2.1.3; ipaddress.is_loopback honours that.
        assert _is_loopback_bind("127.1.2.3") is True
        assert _is_loopback_bind("127.255.255.254") is True

    def test_ipv6_loopback(self) -> None:
        assert _is_loopback_bind("::1") is True

    def test_literal_localhost(self) -> None:
        # Not an IP — but the convention the operator expects to work.
        assert _is_loopback_bind("localhost") is True

    def test_ipv4_wildcard_is_not_loopback(self) -> None:
        # 0.0.0.0 binds every interface — the classic audit footgun.
        assert _is_loopback_bind("0.0.0.0") is False

    def test_ipv6_wildcard_is_not_loopback(self) -> None:
        assert _is_loopback_bind("::") is False

    def test_lan_ip_is_not_loopback(self) -> None:
        assert _is_loopback_bind("192.0.2.15") is False
        assert _is_loopback_bind("192.0.2.13") is False

    def test_public_ip_is_not_loopback(self) -> None:
        assert _is_loopback_bind("203.0.113.8") is False

    def test_unresolved_hostname_is_not_loopback(self) -> None:
        # We don't do DNS lookups — anything that isn't a parseable
        # IP and isn't literally "localhost" is treated as non-loop.
        # Operator can silence with --i-understand-public-bind.
        assert _is_loopback_bind("compute.example.com") is False


class TestWebRunWarning:
    """CLI-level: warning text on stderr when bind is non-loopback."""

    def _invoke(self, *extra_args: str):
        """Invoke ``vq web run`` with uvicorn.run stubbed out."""
        # Patch uvicorn.run via the module-level import path used in
        # cli.web_run (it does `import uvicorn` inside the function).
        with patch("uvicorn.run") as run, patch("vq.web.app", new=object()):
            # click 8.2 split stderr by default; older clicks
            # needed mix_stderr=False. Use whichever the installed
            # version supports.
            try:
                runner = CliRunner(mix_stderr=False)  # type: ignore[call-arg]
            except TypeError:
                runner = CliRunner()
            result = runner.invoke(main, ["web", "run", *extra_args])
        return result, run

    def test_loopback_bind_no_warning(self) -> None:
        result, _run = self._invoke("--host", "127.0.0.1")
        assert result.exit_code == 0, (result.stdout, result.stderr)
        assert "non-loopback" not in result.stderr.lower()
        assert "unauthenticated" not in result.stderr.lower()

    def test_localhost_literal_no_warning(self) -> None:
        result, _run = self._invoke("--host", "localhost")
        assert result.exit_code == 0, (result.stdout, result.stderr)
        assert "non-loopback" not in result.stderr.lower()

    def test_wildcard_bind_warns(self) -> None:
        result, _run = self._invoke("--host", "0.0.0.0")
        assert result.exit_code == 0, (result.stdout, result.stderr)
        # Substring checks — exact wording may evolve; the contract is
        # that the operator gets told the metadata-exposure risk and is
        # pointed at the silence flag.
        assert "non-loopback" in result.stderr.lower()
        assert "unauthenticated" in result.stderr.lower()
        assert "--i-understand-public-bind" in result.stderr

    def test_lan_ip_bind_warns(self) -> None:
        result, _run = self._invoke("--host", "192.0.2.15")
        assert result.exit_code == 0, (result.stdout, result.stderr)
        assert "non-loopback" in result.stderr.lower()

    def test_ack_flag_silences_warning_on_non_loopback(self) -> None:
        result, _run = self._invoke(
            "--host", "0.0.0.0", "--i-understand-public-bind"
        )
        assert result.exit_code == 0, (result.stdout, result.stderr)
        assert "non-loopback" not in result.stderr.lower()

    def test_ack_flag_is_noop_for_loopback(self) -> None:
        # No double-warning, no spurious output.
        result, _run = self._invoke(
            "--host", "127.0.0.1", "--i-understand-public-bind"
        )
        assert result.exit_code == 0, (result.stdout, result.stderr)
        assert result.stderr == ""

    def test_uvicorn_invoked_with_chosen_host(self) -> None:
        # Sanity: the bind decision plumbs through to uvicorn unchanged.
        _result, run = self._invoke("--host", "0.0.0.0", "--port", "9000")
        assert run.call_count == 1
        kwargs = run.call_args.kwargs
        assert kwargs["host"] == "0.0.0.0"
        assert kwargs["port"] == 9000
