"""#163: the gateway client names a TLS certificate it does not trust.

A certificate failure is not transient: the client gives up at once instead
of retrying, and says what failed verification and which trust store it
used. Other transport failures keep their retries and name their reason. A
real local TLS server with throwaway certificates; no gateway app is needed.
"""
from __future__ import annotations

import shutil
import socket
import ssl
import subprocess
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest

from vq.gateway import client as gw_client


def _certificate(directory: Path, name: str) -> tuple[Path, Path]:
    openssl = shutil.which("openssl")
    if openssl is None:
        pytest.skip("OpenSSL is required for the local TLS fixture")
    certificate, key = directory / f"{name}.pem", directory / f"{name}-key.pem"
    subprocess.run([
        openssl, "req", "-x509", "-newkey", "rsa:2048", "-sha256", "-nodes",
        "-keyout", str(key), "-out", str(certificate), "-days", "1",
        "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost",
    ], check=True, capture_output=True)
    return certificate, key


@pytest.fixture
def server(tmp_path: Path) -> Iterator[dict[str, Any]]:
    certificate, key = _certificate(tmp_path, "gateway")
    other, _ = _certificate(tmp_path, "other")
    served: list[str] = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            served.append(self.path)
            body = b'{"job_id": "j1", "state": "running"}'
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args: Any) -> None:
            return None

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certificate, key)
    httpd.socket = context.wrap_socket(httpd.socket, server_side=True)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        yield {"port": httpd.server_address[1], "ca": certificate, "other": other,
               "served": served}
    finally:
        httpd.shutdown()
        httpd.server_close()


def _client(url: str, ca_file: Path | None = None) -> tuple[gw_client.GatewayClient, list[float]]:
    sleeps: list[float] = []
    credentials = gw_client.Credentials(url=url, key="vqk_test",
                                        ca_file=str(ca_file) if ca_file else None)
    return gw_client.GatewayClient(credentials, sleep=sleeps.append), sleeps


def test_a_trusted_certificate_is_used(server: dict[str, Any]) -> None:
    client, sleeps = _client(f"https://localhost:{server['port']}", server["ca"])
    assert client.job("j1")["state"] == "running"
    assert server["served"] == ["/v1/jobs/j1"] and sleeps == []


@pytest.mark.parametrize(("host", "ca", "detail", "store"), [
    ("localhost", None, "self-signed certificate",
     "checked against the system trust store; for a private CA, set ca_file"),
    ("localhost", "other", "self-signed certificate", "checked against ca_file"),
    ("127.0.0.1", "ca", "IP address mismatch", "checked against ca_file"),
])
def test_an_untrusted_certificate_is_named_and_not_retried(
    server: dict[str, Any], host: str, ca: str | None, detail: str, store: str,
) -> None:
    client, sleeps = _client(f"https://{host}:{server['port']}", server[ca] if ca else None)
    with pytest.raises(gw_client.ClientError) as caught:
        client.job("j1")
    message = str(caught.value)
    assert message.startswith(
        "the gateway's TLS certificate is not trusted for GET /v1/jobs/j1: ")
    assert detail in message and store in message
    if ca:
        assert str(server[ca]) in message
    assert sleeps == [] and server["served"] == []


def test_an_unreachable_gateway_is_retried_and_names_its_reason() -> None:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]  # closed again before the client connects
    client, sleeps = _client(f"https://127.0.0.1:{port}")
    with pytest.raises(gw_client.ClientError,
                       match=r"cannot reach the gateway for GET /v1/jobs/j1: .*refused"):
        client.job("j1")
    assert sleeps == [1.0, 2.0, 4.0, 8.0]  # every attempt but the last backs off


def test_an_unusable_ca_file_is_a_credentials_error(tmp_path: Path) -> None:
    with pytest.raises(gw_client.ClientError, match="cannot use ca_file .*missing.pem"):
        _client("https://localhost:1", tmp_path / "missing.pem")
    garbage = tmp_path / "garbage.pem"
    garbage.write_text("not a certificate\n")
    with pytest.raises(gw_client.ClientError, match="cannot use ca_file .*garbage.pem"):
        _client("https://localhost:1", garbage)
