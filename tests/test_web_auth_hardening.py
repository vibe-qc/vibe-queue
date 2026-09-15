"""Session replay, concurrent login admission and authenticated write audit regressions."""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from vq import auth, config, paths
from vq.web import authn, create_app, fleet_audit


@pytest.fixture
def web_state(tmp_path, monkeypatch):
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / 'state'))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / 'config'))
    monkeypatch.delenv(auth.ENV_WEB_TOKEN_FILE, raising=False)
    monkeypatch.setenv('VQ_WEB_FLEET', '1')
    paths.queue_dir().mkdir(parents=True)
    paths.jobs_dir().mkdir(parents=True)
    authn.add_user('alice', 'correct', 'operator')
    return tmp_path


def login(client, password='correct', **kwargs):
    return client.post('/fleet/login', data={'user': 'alice', 'password': password},
                       follow_redirects=False, **kwargs)


def test_logout_revokes_copied_cookie_across_app_instances(web_state):
    client = TestClient(create_app())
    assert login(client).status_code == 303
    token = client.cookies.get(authn.SESSION_COOKIE).strip('"')
    second = authn.issue_session('alice', 'operator')
    assert authn.verify_session(token)
    assert client.post('/fleet/logout', follow_redirects=False).status_code == 303
    restarted = TestClient(create_app())
    restarted.cookies.set(authn.SESSION_COOKIE, token)
    assert restarted.get('/api/v1/fleet').status_code == 401
    assert restarted.get('/api/v1/queue').status_code == 401
    assert authn.verify_session(second)  # Other browser remains logged in.


@pytest.mark.parametrize('change', ['password', 'role', 'remove'])
def test_account_change_invalidates_existing_session(web_state, change):
    token = authn.issue_session('alice', 'operator')
    if change == 'remove':
        authn.remove_user('alice')
    else:
        authn.add_user('alice', 'new-password' if change == 'password' else 'correct',
                       'viewer' if change == 'role' else 'operator', force=True)
    assert authn.verify_session(token) is None


def test_unregistered_old_signed_cookie_is_not_a_session(web_state):
    payload = {'user': 'alice', 'role': 'operator', 'exp': 9999999999, 'nonce': 'legacy'}
    body = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode()
    signature = hmac.new(authn.session_secret(), body.encode(), hashlib.sha256).hexdigest()
    assert authn.verify_session(f'{body}.{signature}') is None


@pytest.mark.parametrize('damage', ['corrupt', 'wide', 'symlink', 'missing'])
def test_lost_or_unsafe_session_authority_fails_closed(web_state, damage):
    token = authn.issue_session('alice', 'operator')
    db = config.config_dir() / 'web-auth' / 'sessions.sqlite3'
    if damage == 'corrupt':
        db.write_bytes(b'not a database')
    elif damage == 'wide':
        db.chmod(0o644)
    else:
        db.unlink()
        if damage == 'symlink':
            other = web_state / 'other'
            other.write_text('do not change')
            db.symlink_to(other)
    assert authn.verify_session(token) is None
    if damage == 'symlink':
        assert other.read_text() == 'do not change'


def test_session_store_is_private_and_contains_no_cookie_or_password(web_state):
    token = authn.issue_session('alice', 'operator')
    directory = config.config_dir() / 'web-auth'
    db = directory / 'sessions.sqlite3'
    assert directory.stat().st_mode & 0o777 == 0o700
    assert db.stat().st_mode & 0o777 == 0o600
    assert token.encode() not in db.read_bytes()
    assert b'correct' not in db.read_bytes()


def test_login_windows_are_atomic_across_concurrent_callers(web_state, monkeypatch):
    monkeypatch.setattr(authn.time, 'time', lambda: 1000)

    def attempt(_):
        try:
            authn.reserve_login_attempt('alice', 'peer')
            return True
        except authn.LoginRateLimited:
            return False

    with ThreadPoolExecutor(max_workers=16) as pool:
        accepted = list(pool.map(attempt, range(30)))
    assert sum(accepted) == authn.LOGIN_ACCOUNT_LIMIT
    with pytest.raises(authn.LoginRateLimited) as error:
        authn.reserve_login_attempt('alice', 'different-peer')
    assert error.value.retry_after == 60
    monkeypatch.setattr(authn.time, 'time', lambda: 1060)
    authn.reserve_login_attempt('alice', 'peer')


def test_peer_limit_bounds_random_account_names(web_state):
    for i in range(authn.LOGIN_PEER_LIMIT):
        authn.reserve_login_attempt(f'user-{i}', 'same-peer')
    with pytest.raises(authn.LoginRateLimited):
        authn.reserve_login_attempt('yet-another-user', 'same-peer')
    authn.reserve_login_attempt('different-user', 'different-peer')


def test_global_limit_bounds_random_peers_and_accounts(web_state):
    for i in range(authn.LOGIN_GLOBAL_LIMIT):
        authn.reserve_login_attempt(f'user-{i}', f'peer-{i}')
    with pytest.raises(authn.LoginRateLimited):
        authn.reserve_login_attempt('random', 'random')


def test_http_limit_persists_and_rejects_before_password_work(web_state, monkeypatch):
    client = TestClient(create_app())
    for _ in range(authn.LOGIN_ACCOUNT_LIMIT):
        assert login(client, 'wrong').status_code == 401
    monkeypatch.setattr(authn, 'verify_password', lambda *a: pytest.fail('hashed after limit'))
    restarted = TestClient(create_app())
    response = login(restarted, headers={'X-Forwarded-For': 'new-peer'})
    assert response.status_code == 429
    assert 1 <= int(response.headers['Retry-After']) <= 60
    assert 'set-cookie' not in response.headers
    assert restarted.get('/health/live').status_code == 200


@pytest.mark.parametrize('scheme', ['http', 'https'])
def test_cookie_secure_uses_trusted_scheme_not_raw_forwarded_header(web_state, scheme):
    client = TestClient(create_app(), base_url=f'{scheme}://testserver')
    response = login(client, headers={'X-Forwarded-Proto': 'https' if scheme == 'http' else 'http'})
    assert response.status_code == 303
    assert ('; Secure' in response.headers['set-cookie']) == (scheme == 'https')
    assert authn.verify_session(client.cookies.get(authn.SESSION_COOKIE).strip('"'))
    logout = client.post('/fleet/logout', follow_redirects=False)
    assert ('; Secure' in logout.headers['set-cookie']) == (scheme == 'https')


@pytest.mark.parametrize('path,service', [
    ('/api/v1/jobs/test-job/kill', 'kill_job'),
    ('/api/v1/jobs/test-job/pause', 'pause_job'),
    ('/api/v1/jobs/test-job/resume', 'resume_job'),
    ('/api/v1/queue/pause', 'pause_all'),
    ('/api/v1/queue/resume', 'resume_all'),
    ('/api/v1/queue/clear-failed', 'list_jobs'),
])
def test_bearer_write_records_start_and_outcome(web_state, monkeypatch, path, service):
    import vq.web as web

    token = auth.generate_token()
    auth.write_token(token)
    monkeypatch.setattr(web, service, lambda *a, **k: [] if service == 'list_jobs' else 'done')
    client = TestClient(create_app())
    response = client.post(path, headers={'Authorization': f'Bearer {token}'})
    assert response.status_code == 200, response.text
    records = fleet_audit.read_audit()
    assert [r['outcome'] for r in records] == ['ok', 'started']
    assert all(r['user'] == 'bearer-token' and r['host'] == 'localhost' for r in records)
    assert records[0]['request_id'] == records[1]['request_id']
    assert token not in fleet_audit.audit_path().read_text()


def test_write_refuses_before_mutation_when_audit_unavailable(web_state, monkeypatch):
    import vq.web as web

    token = auth.generate_token()
    auth.write_token(token)
    monkeypatch.setattr(web, 'kill_job', lambda *a, **k: pytest.fail('unaudited mutation'))
    def unavailable(**kwargs):
        raise OSError('disk unavailable')
    monkeypatch.setattr(fleet_audit, 'append_audit', unavailable)
    response = TestClient(create_app()).post('/api/v1/jobs/test-job/kill',
                                           headers={'Authorization': f'Bearer {token}'})
    assert response.status_code == 503


def test_failed_write_is_audited_without_exception_text(web_state, monkeypatch):
    import vq.web as web
    from vq.ownership import OwnershipError

    token = auth.generate_token()
    auth.write_token(token)
    def denied(*args, **kwargs):
        raise OwnershipError('private details')
    monkeypatch.setattr(web, 'pause_job', denied)
    response = TestClient(create_app()).post('/api/v1/jobs/test-job/pause',
                                           headers={'Authorization': f'Bearer {token}'})
    assert response.status_code == 403
    records = fleet_audit.read_audit()
    assert records[0]['outcome'] == 'http-403'
    assert 'private details' not in fleet_audit.audit_path().read_text()


def test_concurrent_first_sessions_share_one_complete_secret(web_state):
    with ThreadPoolExecutor(max_workers=8) as pool:
        tokens = list(pool.map(lambda _: authn.issue_session('alice', 'operator'), range(16)))
    assert len(set(tokens)) == 16
    assert all(authn.verify_session(token) for token in tokens)


def test_password_change_during_login_cannot_issue_a_new_session(web_state, monkeypatch):
    original = authn.verify_password
    def racing_verify(user, password):
        role = original(user, password)
        authn.add_user(user, 'replacement', 'operator', force=True)
        return role
    monkeypatch.setattr(authn, 'verify_password', racing_verify)
    response = login(TestClient(create_app()))
    assert response.status_code == 503
    assert 'set-cookie' not in response.headers


def test_logout_does_not_claim_success_when_revocation_cannot_persist(web_state, monkeypatch):
    client = TestClient(create_app())
    assert login(client).status_code == 303
    def unavailable(token):
        raise OSError('unavailable')
    monkeypatch.setattr(authn, 'revoke_session', unavailable)
    assert client.post('/fleet/logout', follow_redirects=False).status_code == 503


def test_session_count_is_bounded_per_account(web_state, monkeypatch):
    monkeypatch.setattr(authn, 'MAX_SESSIONS_PER_USER', 2)
    first = authn.issue_session('alice', 'operator')
    second = authn.issue_session('alice', 'operator')
    third = authn.issue_session('alice', 'operator')
    assert authn.verify_session(first) is None
    assert authn.verify_session(second) and authn.verify_session(third)


def test_completed_write_audit_failure_preserves_outcome_and_logs_receipt_id(web_state,
                                                                          monkeypatch, caplog):
    import vq.web as web

    token = auth.generate_token()
    auth.write_token(token)
    monkeypatch.setattr(web, 'kill_job', lambda *a, **k: 'done')
    original = fleet_audit.append_audit
    def fail_outcome(**kwargs):
        if kwargs['outcome'] == 'ok':
            raise OSError('disk unavailable')
        original(**kwargs)
    monkeypatch.setattr(fleet_audit, 'append_audit', fail_outcome)
    response = TestClient(create_app()).post('/api/v1/jobs/test-job/kill',
                                           headers={'Authorization': f'Bearer {token}'})
    assert response.status_code == 200 and response.text == 'done'
    records = fleet_audit.read_audit()
    assert len(records) == 1 and records[0]['outcome'] == 'started'
    assert records[0]['request_id'] in caplog.text
    assert token not in caplog.text


@pytest.mark.parametrize('peer,trusted', [('127.0.0.1', True), ('192.0.2.1', False)])
def test_uvicorn_proxy_boundary_controls_cookie_scheme_and_login_peer(web_state, monkeypatch,
                                                                      peer, trusted):
    from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware

    seen = []
    original = authn.reserve_login_attempt
    def observe(user, client_peer):
        seen.append(client_peer)
        original(user, client_peer)
    monkeypatch.setattr(authn, 'reserve_login_attempt', observe)
    app = ProxyHeadersMiddleware(create_app(), trusted_hosts=['127.0.0.1'])
    client = TestClient(app, client=(peer, 12345))
    response = login(client, headers={'X-Forwarded-Proto': 'https',
                                     'X-Forwarded-For': '198.51.100.1'})
    assert response.status_code == 303
    assert ('; Secure' in response.headers['set-cookie']) == trusted
    assert seen == ['198.51.100.1' if trusted else peer]


@pytest.mark.parametrize('size,status', [(1025, 400), (17000, 413)])
def test_oversized_login_is_rejected_before_password_work(web_state, monkeypatch, size, status):
    monkeypatch.setattr(authn, 'verify_password', lambda *a: pytest.fail('unbounded password work'))
    response = login(TestClient(create_app()), password='x' * size)
    assert response.status_code == status


def test_kill_then_failed_resubmit_keeps_failure_receipt_without_query_values(
    web_state, monkeypatch,
):
    import vq.web as web

    token = auth.generate_token()
    auth.write_token(token)
    killed = []
    monkeypatch.setattr(web, 'kill_job', lambda *a, **k: killed.append(a[1]) or 'killed')
    def failed_resubmit(*a, **k):
        raise ValueError('cannot resubmit')
    monkeypatch.setattr(web, 'resubmit_local', failed_resubmit)
    response = TestClient(create_app()).post(
        '/api/v1/jobs/test-job/kill?resubmit=true&reason=private-reason',
        headers={'Authorization': f'Bearer {token}'},
    )
    assert response.status_code == 409 and killed == ['test-job']
    records = fleet_audit.read_audit()
    assert [r['outcome'] for r in records] == ['http-409', 'started']
    assert all(r['action'] == 'kill-resubmit' for r in records)
    assert 'private-reason' not in fleet_audit.audit_path().read_text()


@pytest.mark.parametrize('user,password', [('x' * 129, 'pw'), ('bob', 'x' * 1025)])
def test_account_creation_cannot_exceed_login_field_limits(web_state, user, password):
    before = authn.users_path().read_bytes()
    with pytest.raises(ValueError, match='limited'):
        authn.add_user(user, password, 'viewer')
    assert authn.users_path().read_bytes() == before
