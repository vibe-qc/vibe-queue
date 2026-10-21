"""Gateway slice 1 (#163): API keys, project policy, authentication and audit."""
from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner
from fastapi.testclient import TestClient
from pydantic import ValidationError

from vq import config, paths
from vq.cli import main as cli
from vq.gateway import keys as gw_keys
from vq.gateway.app import AUDIT_BASENAME, create_gateway_app, require_scope

PROJECT = config.GatewayProjectConfig(
    programs=["vibeqc-release"], max_cpus=8, max_mem_mb=8192, max_wall_time_seconds=3600,
)


def _cfg(*, enabled: bool = True, broker: bool = True, **gateway: Any) -> config.Config:
    return config.Config(
        hosts={"target": config.HostConfig(ssh="target.example")},
        broker=config.BrokerConfig(enabled=broker, targets=["target"]) if broker
        else config.BrokerConfig(),
        gateway=config.GatewayConfig(enabled=enabled, projects={"group-a": PROJECT},
                                     **gateway),
    )


@pytest.fixture
def store(tmp_path: Path) -> gw_keys.KeyStore:
    return gw_keys.KeyStore(tmp_path / "gateway")


def _issue(store: gw_keys.KeyStore, **kw: Any) -> tuple[gw_keys.GatewayKey, str]:
    args = dict(principal="alice", project="group-a", scopes=["submit", "read"],
                expires_days=30, projects=["group-a"])
    args.update(kw)
    return store.create(**args)


def test_a_key_is_shown_once_and_stored_only_as_a_keyed_digest(store) -> None:
    key, secret = _issue(store)
    assert gw_keys.KEY_RE.fullmatch(secret)
    stored = (store.root / "keys.json").read_text()
    assert secret[len("vqk_") + 17:] not in stored
    assert json.loads(stored)["keys"][key.key_id]["digest"]
    for name in ("keys.json", "pepper"):
        assert (store.root / name).stat().st_mode & 0o777 == 0o600
    assert store.root.stat().st_mode & 0o777 == 0o700
    assert store.authenticate(secret, projects=["group-a"]) .key_id == key.key_id


@pytest.mark.parametrize("presented", [
    "", "vqk_short", "Bearer x", "vqk_" + "0" * 16 + "_" + "A" * 43,
])
def test_malformed_and_unknown_keys_are_refused(store, presented) -> None:
    _issue(store)
    with pytest.raises(gw_keys.GatewayAuthError, match="invalid gateway API key"):
        store.authenticate(presented, projects=["group-a"])


def test_a_wrong_secret_for_a_real_key_id_is_refused(store) -> None:
    key, secret = _issue(store)
    forged = f"vqk_{key.key_id}_{'B' * 43}"
    assert forged != secret
    with pytest.raises(gw_keys.GatewayAuthError, match="invalid"):
        store.authenticate(forged, projects=["group-a"])


def test_revocation_and_expiry_apply_on_the_next_request(store, monkeypatch) -> None:
    key, secret = _issue(store)
    store.revoke(key.key_id)
    with pytest.raises(gw_keys.GatewayAuthError, match="revoked"):
        store.authenticate(secret, projects=["group-a"])
    _key2, secret2 = _issue(store, expires_days=1)
    later = gw_keys._now() + timedelta(days=2)
    monkeypatch.setattr(gw_keys, "_now", lambda: later)
    with pytest.raises(gw_keys.GatewayAuthError, match="expired"):
        store.authenticate(secret2, projects=["group-a"])


def test_a_key_whose_project_was_removed_no_longer_works(store) -> None:
    _key, secret = _issue(store)
    with pytest.raises(gw_keys.GatewayAuthError, match="no longer configured"):
        store.authenticate(secret, projects=["group-b"])


@pytest.mark.parametrize("kw,match", [
    ({"project": "nope"}, "not configured"),
    ({"scopes": []}, "subset"),
    ({"scopes": ["admin"]}, "subset"),
    ({"principal": "bad name"}, "principal"),
    ({"expires_days": 0}, "expiry"),
])
def test_invalid_grants_are_refused(store, kw, match) -> None:
    with pytest.raises(gw_keys.GatewayKeyError, match=match):
        _issue(store, **kw)


def test_rotation_keeps_the_grant_and_revokes_the_old_key(store) -> None:
    old, old_secret = _issue(store, scopes=["read"])
    new, new_secret = store.rotate(old.key_id, projects=["group-a"])
    assert (new.principal, new.project, new.scopes) == (old.principal, old.project, ("read",))
    assert new.key_id != old.key_id
    assert store.authenticate(new_secret, projects=["group-a"]).key_id == new.key_id
    with pytest.raises(gw_keys.GatewayAuthError, match="revoked"):
        store.authenticate(old_secret, projects=["group-a"])
    with pytest.raises(gw_keys.GatewayKeyError, match="already revoked"):
        store.rotate(old.key_id, projects=["group-a"])


def test_last_used_is_recorded(store) -> None:
    key, secret = _issue(store)
    assert key.last_used_at is None
    store.authenticate(secret, projects=["group-a"])
    (listed,) = store.list()
    assert listed.last_used_at is not None


def _client(tmp_path: Path, cfg: config.Config) -> tuple[TestClient, gw_keys.KeyStore]:
    root = tmp_path / "gateway"
    app = create_gateway_app(lambda: cfg, root=root)

    @app.get("/v1/test-cancel")
    def needs_cancel(key=require_scope_dependency(app)):  # noqa: B008
        return {"ok": True}

    return TestClient(app), gw_keys.KeyStore(root)


def require_scope_dependency(app):
    from fastapi import Depends

    return Depends(require_scope(app, "cancel"))


def test_whoami_needs_a_key_and_never_takes_the_admin_bearer(tmp_path) -> None:
    client, store = _client(tmp_path, _cfg())
    key, secret = _issue(store)
    assert client.get("/v1/healthz").json() == {"ok": True}
    assert client.get("/v1/whoami").status_code == 401
    assert client.get("/v1/whoami", headers={"Authorization": "Bearer admin-token"}
                      ).status_code == 401
    assert client.get(f"/v1/whoami?key={secret}").status_code == 401
    ok = client.get("/v1/whoami", headers={"Authorization": f"Bearer {secret}"})
    assert ok.status_code == 200, ok.text
    assert ok.json()["principal"] == "alice" and ok.json()["key_id"] == key.key_id


def test_a_missing_scope_is_forbidden(tmp_path) -> None:
    client, store = _client(tmp_path, _cfg())
    _key, secret = _issue(store, scopes=["read"])
    assert client.get("/v1/test-cancel", headers={"Authorization": f"Bearer {secret}"}
                      ).status_code == 403
    _key, both = _issue(store, scopes=["read", "cancel"])
    assert client.get("/v1/test-cancel", headers={"Authorization": f"Bearer {both}"}
                      ).status_code == 200


def test_a_disabled_gateway_answers_nothing_authenticated(tmp_path) -> None:
    client, store = _client(tmp_path, _cfg(enabled=False))
    _key, secret = _issue(store)
    assert client.get("/v1/whoami", headers={"Authorization": f"Bearer {secret}"}
                      ).status_code == 503


def test_the_audit_trail_names_the_key_but_never_the_secret(tmp_path) -> None:
    client, store = _client(tmp_path, _cfg())
    key, secret = _issue(store)
    client.get("/v1/whoami", headers={"Authorization": f"Bearer {secret}"})
    client.get("/v1/whoami", headers={"Authorization": "Bearer vqk_bogus"})
    text = (tmp_path / "gateway" / AUDIT_BASENAME).read_text()
    assert secret not in text and secret[len("vqk_") + 17:] not in text
    lines = [json.loads(line) for line in text.splitlines()]
    assert [line["status"] for line in lines] == [200, 401]
    assert lines[0]["key_id"] == key.key_id and lines[1]["key_id"] is None


@pytest.mark.parametrize("gateway,match", [
    ({"tls_cert": "/c"}, "set together"),
    ({"tls_cert": "c", "tls_key": "k"}, "absolute"),
])
def test_transport_config_is_validated(gateway, match) -> None:
    with pytest.raises(ValidationError, match=match):
        config.GatewayConfig(**gateway)
    with pytest.raises(ValidationError, match="invalid project name"):
        config.GatewayConfig(projects={"bad name": PROJECT})


@pytest.fixture
def serve_env(tmp_path, monkeypatch):
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    started: list[dict[str, Any]] = []
    import uvicorn

    monkeypatch.setattr(uvicorn, "run", lambda app, **kw: started.append(kw))
    return started


@pytest.mark.parametrize("cfg,args,match", [
    (_cfg(enabled=False), [], "enabled = true is required"),
    (_cfg(broker=False), [], "submits through the broker"),
    (_cfg(), ["--bind", "0.0.0.0"], "refusing a plaintext gateway"),
])
def test_serve_refuses_unsafe_or_incomplete_setups(serve_env, monkeypatch, cfg, args, match):
    monkeypatch.setattr(config, "load_config", lambda: cfg)
    result = CliRunner().invoke(cli, ["gateway", "serve", *args])
    assert result.exit_code != 0 and match in result.output
    assert serve_env == []


def test_serve_starts_on_loopback_or_with_tls(serve_env, monkeypatch) -> None:
    monkeypatch.setattr(config, "load_config", lambda: _cfg())
    assert CliRunner().invoke(cli, ["gateway", "serve"]).exit_code == 0
    tls = _cfg(bind="0.0.0.0", tls_cert="/etc/c.pem", tls_key="/etc/k.pem")
    monkeypatch.setattr(config, "load_config", lambda: tls)
    assert CliRunner().invoke(cli, ["gateway", "serve"]).exit_code == 0
    assert serve_env[0]["host"] == "127.0.0.1" and serve_env[0]["ssl_certfile"] is None
    assert serve_env[1]["host"] == "0.0.0.0" and serve_env[1]["ssl_certfile"] == "/etc/c.pem"


def test_key_cli_create_list_revoke(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setattr(config, "load_config", lambda: _cfg())
    runner = CliRunner()
    created = runner.invoke(cli, ["gateway", "key", "create", "--principal", "alice",
                                  "--project", "group-a", "--scope", "read", "--json"])
    assert created.exit_code == 0, created.output
    payload = json.loads(created.output)
    assert gw_keys.KEY_RE.fullmatch(payload["api_key"])
    listed = runner.invoke(cli, ["gateway", "key", "list", "--json"])
    assert payload["api_key"] not in listed.output
    assert json.loads(listed.output)[0]["key_id"] == payload["key_id"]
    revoked = runner.invoke(cli, ["gateway", "key", "revoke", payload["key_id"]])
    assert revoked.exit_code == 0 and "revoked" in revoked.output
    bad = runner.invoke(cli, ["gateway", "key", "create", "--principal", "bob",
                              "--project", "nope", "--scope", "read"])
    assert bad.exit_code != 0 and "not configured" in bad.output
