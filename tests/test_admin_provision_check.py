"""Contract for ``vq admin provision <host> [--check]``.

Every check here exists because the condition it names cost hours of a real
fleet sweep while ``vq doctor`` reported the host green throughout. The verb is
read-only by construction: repairing any of these needs root on the target, so
it reports the exact fix rather than running it.

SSH is faked at the transport boundary, which is where the real seam is -- the
probes are shell snippets whose text is part of the contract, so the tests
assert the recorded command list rather than only the verdicts.
"""

from __future__ import annotations

import subprocess
from typing import Any

import pytest
from click.testing import CliRunner

from vq import config, provision
from vq.cli import main


def _cfg(
    *,
    multi_user: bool = True,
    remote_vq: str = "/home/USER/bin/vq-fleet-admin",
    admin_token_file: str | None = "/home/USER/.config/vq/admin-token",
    admin_group: str = "vq-admins",
    programs: bool = True,
) -> config.Config:
    return config.Config(
        hosts={
            "host_d": config.HostConfig(
                ssh="host_d",
                remote_vq=remote_vq,
                admin_token_file=admin_token_file,
                fleet_role="managed",
            ),
        },
        programs=(
            {
                "vibeqc-queue": config.VenvProgram(
                    kind="venv",
                    python="/home/USER/gitlab/vibeqc-queue/vibe-queue/.venv/bin/python",
                    git_dir="/home/USER/gitlab/vibeqc-queue",
                    branch="main",
                )
            }
            if programs
            else {}
        ),
        multi_user=config.MultiUserConfig(
            enabled=multi_user,
            admin_group=admin_group,
        ),
    )


# Ordered most-specific first: the probes are shell snippets and several share
# substrings (every `command -v ...` line contains "command -v"), so a looser
# key placed earlier would answer a question it was not asked.
HEALTHY = {
    "multi_user": "enabled",
    "systemd-run": "yes",
    "systemctl is-active": "active",
    "vq-multi-user-refresh": "755 root:root",
    "--version": "vq, version 0.24.0",
    "admin-token": "600",
    "/var/lib/vq": "2775 root:vq-admins",
    "command -v": "/home/USER/bin/vq-fleet-admin",
    "cat ": (
        "#!/usr/bin/env bash\n"
        "export VQ_CONFIG_DIR=/etc/vq\n"
        "export VQ_STATE_DIR=/var/lib/vq\n"
        "exec /opt/vq/venv/bin/vq \"$@\"\n"
    ),
}


def _fake_shell(
    overrides: dict[str, str] | None = None,
    *,
    multi_user: bool = True,
):
    """Return (runner, recorded) where runner answers by snippet substring."""
    table = dict(HEALTHY)
    if not multi_user:
        # The target's own /etc/vq/config.toml explicitly selects the
        # single-user path and its root-only system unit is inactive.
        table["multi_user"] = "disabled"
        table["systemctl is-active"] = "inactive"
    table.update(overrides or {})
    recorded: list[str] = []

    def run_remote_shell(host_cfg, *args, **kwargs):  # type: ignore[no-untyped-def]
        script = args[-1]
        recorded.append(script)
        for needle, answer in table.items():
            if needle in script:
                return subprocess.CompletedProcess(
                    list(args), 0, stdout=answer + "\n", stderr=""
                )
        return subprocess.CompletedProcess(list(args), 0, stdout="", stderr="")

    return run_remote_shell, recorded


def _fake_programs(entries: list[dict[str, Any]] | None = None):
    """Stand in for `vq programs --json` on the TARGET host."""
    import json

    body = json.dumps(
        entries
        if entries is not None
        else [{"name": "vibeqc-queue"}, {"name": "vibeqc-dev"}]
    )

    def run_remote_vq(host_cfg, *args, **kwargs):  # type: ignore[no-untyped-def]
        return subprocess.CompletedProcess(list(args), 0, stdout=body, stderr="")

    return run_remote_vq


def _diagnose(
    monkeypatch: pytest.MonkeyPatch,
    *,
    cfg: config.Config | None = None,
    overrides: dict[str, str] | None = None,
    programs: list[dict[str, Any]] | None = None,
) -> tuple[dict[str, Any], list[str]]:
    target_cfg = cfg or _cfg()
    runner, recorded = _fake_shell(
        overrides,
        multi_user=target_cfg.multi_user.enabled,
    )
    monkeypatch.setattr(provision.transport, "run_remote_shell", runner)
    monkeypatch.setattr(
        provision.transport, "run_remote_vq", _fake_programs(programs)
    )
    monkeypatch.setattr(
        provision,
        "local_ssh_checks",
        lambda host_cfg, *, probe_cache: ([], None, None, False),
    )
    payload = provision.diagnose_host(target_cfg, "host_d")
    return payload, recorded


def _named(payload: dict[str, Any], name: str) -> dict[str, Any]:
    return next(item for item in payload["checks"] if item["name"] == name)


# --- the payload is deliberately not the doctor payload --------------------


def test_the_payload_carries_its_own_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Provision verdicts must never enter the doctor payload.

    ``fleet_rollout._host_hold_reason`` turns any failed doctor check other
    than ``scheduler_remote_vq`` into a rollout *defer*, so a new failing check
    landing in doctor's payload would defer every host on the next rollout.
    """
    payload, _ = _diagnose(monkeypatch)

    assert payload["schema"] == "vq.admin.provision_check/1"
    assert payload["schema"] != "vq.doctor/1"


def test_a_fully_provisioned_host_passes_every_check(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload, _ = _diagnose(monkeypatch)

    assert payload["ok"] is True, [
        item for item in payload["checks"] if not item["ok"]
    ]


def test_check_records_share_the_doctor_record_shape(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload, _ = _diagnose(monkeypatch)

    for item in payload["checks"]:
        assert {"name", "ok", "message"} <= set(item)
        assert isinstance(item["ok"], bool)


# --- the wrapper check: the split-store failure ----------------------------


def test_a_remote_vq_pointing_straight_at_the_venv_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The 2026-07-26 failure. Doctor passes it; this must not.

    Without the wrapper the CLI reads the per-user store while canonical writes
    go to the system one, so updates report ``success: True`` with no work
    errors and LAST OK never advances.
    """
    payload, _ = _diagnose(
        monkeypatch,
        cfg=_cfg(remote_vq="/opt/vq/venv/bin/vq"),
        overrides={"command -v": "/opt/vq/venv/bin/vq"},
    )

    verdict = _named(payload, "remote_vq_wrapper")
    assert verdict["ok"] is False
    assert "not to a wrapper" in verdict["message"]
    assert "VQ_STATE_DIR" in verdict["message"]


def test_a_wrapper_missing_one_export_fails_and_names_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload, _ = _diagnose(
        monkeypatch,
        overrides={
            "cat ": "#!/bin/sh\nexport VQ_CONFIG_DIR=/etc/vq\nexec /opt/vq/venv/bin/vq \"$@\"\n",
        },
    )

    verdict = _named(payload, "remote_vq_wrapper")
    assert verdict["ok"] is False
    assert "VQ_STATE_DIR" in verdict["message"]
    assert verdict["exports"] == ["VQ_CONFIG_DIR"]


def test_a_wrapper_exporting_both_roots_passes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload, _ = _diagnose(monkeypatch)

    assert _named(payload, "remote_vq_wrapper")["ok"] is True


# --- the admin token -------------------------------------------------------


def test_an_unset_admin_token_file_fails_with_the_reason(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload, _ = _diagnose(monkeypatch, cfg=_cfg(admin_token_file=None))

    verdict = _named(payload, "admin_token_file")
    assert verdict["ok"] is False
    assert "admin_token_file is unset" in verdict["message"]


def test_a_world_readable_token_file_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The loader refuses any group or other bits, so loose is as bad as absent."""
    payload, _ = _diagnose(
        monkeypatch,
        overrides={"admin-token": "644"},
    )

    verdict = _named(payload, "admin_token_file")
    assert verdict["ok"] is False
    assert "chmod 600" in verdict["message"]


def test_a_missing_token_file_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    payload, _ = _diagnose(monkeypatch, overrides={"admin-token": "missing"})

    verdict = _named(payload, "admin_token_file")
    assert verdict["ok"] is False
    assert "does not exist" in verdict["message"]


# --- the state root --------------------------------------------------------


def test_a_root_root_755_state_root_fails_with_the_chgrp_command(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The v0.15.106 bug's shape: provisioned, running, and unable to do
    anything, because the marker lives directly under the state root."""
    payload, _ = _diagnose(
        monkeypatch,
        overrides={"/var/lib/vq": "755 root:root"},
    )

    verdict = _named(payload, "state_root_perms")
    assert verdict["ok"] is False
    assert "sudo chgrp vq-admins /var/lib/vq" in verdict["message"]
    assert "sudo chmod 2775 /var/lib/vq" in verdict["message"]


def test_the_expected_state_root_group_follows_the_configured_admin_group(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload, _ = _diagnose(monkeypatch, cfg=_cfg(admin_group="vq-ops"))

    verdict = _named(payload, "state_root_perms")
    assert verdict["ok"] is False
    assert "root:vq-ops" in verdict["message"]


# --- the root-owned install and its refresh helper -------------------------


def test_a_missing_refresh_helper_is_reported_with_its_consequence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload, _ = _diagnose(
        monkeypatch,
        overrides={"vq-multi-user-refresh": "missing"},
    )

    verdict = _named(payload, "refresh_helper")
    assert verdict["ok"] is False
    assert "stamps a new commit onto old code" in verdict["message"]


def test_a_user_owned_refresh_helper_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A NOPASSWD helper the group can rewrite is a root grant."""
    payload, _ = _diagnose(
        monkeypatch,
        overrides={"vq-multi-user-refresh": "755 USER:USER"},
    )

    assert _named(payload, "refresh_helper")["ok"] is False


def test_a_missing_root_owned_install_names_the_deploy_script(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload, _ = _diagnose(monkeypatch, overrides={"--version": "missing"})

    verdict = _named(payload, "root_owned_install")
    assert verdict["ok"] is False
    assert "deploy-multi-user.sh" in verdict["message"]


# --- delegation, unit, programs -------------------------------------------


def test_missing_systemd_run_fails_delegation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload, _ = _diagnose(monkeypatch, overrides={"systemd-run": "no"})

    verdict = _named(payload, "delegation")
    assert verdict["ok"] is False
    assert "cannot drop job privileges" in verdict["message"]


def test_an_inactive_daemon_unit_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    payload, _ = _diagnose(
        monkeypatch,
        overrides={"systemctl is-active": "inactive"},
    )

    verdict = _named(payload, "daemon_unit")
    assert verdict["ok"] is False
    assert verdict["state"] == "inactive"


def test_a_host_with_no_registered_programs_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A host with no managed program is planned as 'no managed lane' and
    silently skipped by rollout-latest -- indistinguishable from converged."""
    payload, _ = _diagnose(monkeypatch, programs=[])

    verdict = _named(payload, "programs_registered")
    assert verdict["ok"] is False
    assert "silently skipped" in verdict["message"]


def test_the_program_registry_is_read_from_the_target_not_the_driver(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`vq admin update <env> <host>` resolves the env against the TARGET's
    registry, so the driver's own [programs.*] says nothing about the host."""
    seen: list[tuple[str, ...]] = []

    def record(host_cfg, *args, **kwargs):  # type: ignore[no-untyped-def]
        seen.append(args)
        return subprocess.CompletedProcess(
            list(args), 0, stdout='[{"name": "vibeqc-dev"}]', stderr=""
        )

    runner, _recorded = _fake_shell()
    monkeypatch.setattr(provision.transport, "run_remote_shell", runner)
    monkeypatch.setattr(provision.transport, "run_remote_vq", record)
    monkeypatch.setattr(
        provision,
        "local_ssh_checks",
        lambda host_cfg, *, probe_cache: ([], None, None, False),
    )

    # Driver config has vibeqc-queue registered; the target reports only
    # vibeqc-dev. The verdict must describe the target.
    payload = provision.diagnose_host(_cfg(programs=True), "host_d")

    assert seen == [("programs", "--json")]
    assert _named(payload, "programs_registered")["programs"] == ["vibeqc-dev"]


def test_a_non_json_programs_reply_fails_rather_than_reading_as_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, _recorded = _fake_shell()
    monkeypatch.setattr(provision.transport, "run_remote_shell", runner)
    monkeypatch.setattr(
        provision.transport,
        "run_remote_vq",
        lambda host_cfg, *a, **k: subprocess.CompletedProcess(
            list(a), 1, stdout="", stderr="vq: command not found\n"
        ),
    )
    monkeypatch.setattr(
        provision,
        "local_ssh_checks",
        lambda host_cfg, *, probe_cache: ([], None, None, False),
    )

    payload = provision.diagnose_host(_cfg(), "host_d")

    verdict = _named(payload, "programs_registered")
    assert verdict["ok"] is False
    assert "command not found" in verdict["message"]


# --- single-user hosts -----------------------------------------------------


def test_a_single_user_host_skips_the_multi_user_checks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The multi-user preconditions are not faults on a single-user host."""
    payload, _ = _diagnose(monkeypatch, cfg=_cfg(multi_user=False))

    for name in (
        "remote_vq_wrapper",
        "admin_token_file",
        "state_root_perms",
        "root_owned_install",
        "delegation",
    ):
        assert _named(payload, name)["ok"] is True
        assert "not applicable" in _named(payload, name)["message"]

    unit = _named(payload, "daemon_unit")
    assert unit["ok"] is True
    assert unit["state"] == "inactive"
    assert "consistent" in unit["message"]


def test_active_root_unit_contradicts_disabled_multi_user_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Issue #136: disabled policy must not hide a serving root daemon."""
    payload, recorded = _diagnose(
        monkeypatch,
        cfg=_cfg(multi_user=False),
        overrides={"systemctl is-active": "active"},
    )

    assert _named(payload, "multi_user_mode")["config_state"] == "disabled"
    verdict = _named(payload, "daemon_unit")
    assert verdict["ok"] is False
    assert verdict["state"] == "active"
    assert verdict["config_multi_user"] is False
    assert "unsafe deployment contradiction" in verdict["message"]
    assert "run submitted payloads as root" in verdict["message"]
    assert sum("systemctl is-active" in script for script in recorded) == 1


def test_active_root_unit_contradicts_absent_multi_user_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A readable config without the table is still definite single-user."""
    payload, _ = _diagnose(
        monkeypatch,
        cfg=_cfg(multi_user=False),
        overrides={"multi_user": "absent", "systemctl is-active": "active"},
    )

    mode = _named(payload, "multi_user_mode")
    assert mode["ok"] is True
    assert mode["multi_user"] is False
    assert mode["config_state"] == "absent"
    assert _named(payload, "daemon_unit")["ok"] is False


def test_active_root_unit_contradicts_missing_target_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload, recorded = _diagnose(
        monkeypatch,
        cfg=_cfg(multi_user=False),
        overrides={"multi_user": "missing", "systemctl is-active": "active"},
    )

    mode = _named(payload, "multi_user_mode")
    assert mode["ok"] is True
    assert mode["multi_user"] is False
    assert mode["config_state"] == "missing"
    assert _named(payload, "daemon_unit")["ok"] is False
    probe = next(script for script in recorded if "Config.model_validate" in script)
    assert "except FileNotFoundError:" in probe
    assert 'print("missing")' in probe


def test_target_config_probe_uses_the_root_install_schema_when_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _payload, recorded = _diagnose(monkeypatch)

    probe = next(script for script in recorded if "Config.model_validate" in script)
    assert "/opt/vq/venv/bin/python" in probe
    assert 'Path("/etc/vq/config.toml")' in probe


def test_failed_root_unit_is_not_a_green_single_user_absence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload, _ = _diagnose(
        monkeypatch,
        cfg=_cfg(multi_user=False),
        overrides={"systemctl is-active": "failed"},
    )

    verdict = _named(payload, "daemon_unit")
    assert verdict["ok"] is False
    assert verdict["state"] == "failed"
    assert "could not prove" in verdict["message"]


def test_systemd_is_not_required_for_a_single_user_host(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload, recorded = _diagnose(
        monkeypatch,
        cfg=_cfg(multi_user=False),
        overrides={"systemctl is-active": "unavailable"},
    )

    verdict = _named(payload, "daemon_unit")
    assert verdict["ok"] is True
    assert verdict["state"] == "unavailable"
    assert "not applicable" in verdict["message"]
    probe = next(script for script in recorded if "systemctl is-active" in script)
    assert "command -v systemctl" in probe


def test_systemd_is_required_for_a_multi_user_host(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload, _ = _diagnose(
        monkeypatch,
        overrides={"systemctl is-active": "unavailable"},
    )

    verdict = _named(payload, "daemon_unit")
    assert verdict["ok"] is False
    assert verdict["state"] == "unavailable"


def test_missing_systemd_cannot_excuse_unknown_target_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload, _ = _diagnose(
        monkeypatch,
        overrides={
            "multi_user": "multi_user.enabled must be a boolean",
            "systemctl is-active": "unavailable",
        },
    )

    verdict = _named(payload, "daemon_unit")
    assert verdict["ok"] is False
    assert verdict["state"] == "unavailable"
    assert verdict["config_multi_user"] is None


# --- read-only by construction --------------------------------------------


def test_no_probe_mutates_anything(monkeypatch: pytest.MonkeyPatch) -> None:
    """The contract that makes --check safe to run against a live fleet."""
    _payload, recorded = _diagnose(monkeypatch)

    assert recorded
    for script in recorded:
        for verb in (
            "chgrp",
            "chmod",
            "chown",
            "pip install",
            "systemctl restart",
            "systemctl enable",
            "rm ",
            "mv ",
            "> /",
            "install -",
        ):
            assert verb not in script, f"{verb!r} in {script!r}"


def test_an_unreachable_first_hop_reports_not_attempted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One connection failure must not become N distinct provisioning faults."""
    from vq.doctor import check as doctor_check

    monkeypatch.setattr(
        provision,
        "local_ssh_checks",
        lambda host_cfg, *, probe_cache: (
            [doctor_check("ssh_first_hop", False, "connection refused")],
            None,
            None,
            True,
        ),
    )
    called: list[str] = []
    monkeypatch.setattr(
        provision.transport,
        "run_remote_shell",
        lambda *a, **k: called.append("probed"),
    )

    payload = provision.diagnose_host(_cfg(), "host_d")

    assert payload["ok"] is False
    assert called == []
    assert _named(payload, "provisioning")["message"].startswith("not attempted")


# --- CLI wiring ------------------------------------------------------------


def test_all_and_host_are_mutually_exclusive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("vq.cli.config.load_config", lambda: _cfg())

    result = CliRunner().invoke(main, ["admin", "provision", "--all", "host_d"])

    assert result.exit_code == 2
    assert "mutually exclusive" in result.output


def test_a_failing_host_exits_1_and_prints_the_remediation_plan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("vq.cli.config.load_config", lambda: _cfg())
    runner, _recorded = _fake_shell({"/var/lib/vq": "755 root:root"})
    monkeypatch.setattr(provision.transport, "run_remote_shell", runner)
    monkeypatch.setattr(provision.transport, "run_remote_vq", _fake_programs())
    monkeypatch.setattr(
        provision,
        "local_ssh_checks",
        lambda host_cfg, *, probe_cache: ([], None, None, False),
    )

    result = CliRunner().invoke(main, ["admin", "provision", "host_d"])

    assert result.exit_code == 1
    assert "FAIL state_root_perms" in result.output
    assert "remediation plan for host_d" in result.output


def test_check_suppresses_the_remediation_plan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """--check is the machine-readable rehearsal form, not the fix-it form."""
    monkeypatch.setattr("vq.cli.config.load_config", lambda: _cfg())
    runner, _recorded = _fake_shell({"/var/lib/vq": "755 root:root"})
    monkeypatch.setattr(provision.transport, "run_remote_shell", runner)
    monkeypatch.setattr(provision.transport, "run_remote_vq", _fake_programs())
    monkeypatch.setattr(
        provision,
        "local_ssh_checks",
        lambda host_cfg, *, probe_cache: ([], None, None, False),
    )

    result = CliRunner().invoke(main, ["admin", "provision", "host_d", "--check"])

    assert result.exit_code == 1
    assert "FAIL state_root_perms" in result.output
    assert "remediation plan" not in result.output


def test_json_emits_the_versioned_envelope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import json

    monkeypatch.setattr("vq.cli.config.load_config", lambda: _cfg())
    runner, _recorded = _fake_shell()
    monkeypatch.setattr(provision.transport, "run_remote_shell", runner)
    monkeypatch.setattr(provision.transport, "run_remote_vq", _fake_programs())
    monkeypatch.setattr(
        provision,
        "local_ssh_checks",
        lambda host_cfg, *, probe_cache: ([], None, None, False),
    )

    result = CliRunner().invoke(
        main, ["admin", "provision", "host_d", "--json"]
    )

    payload = json.loads(result.output)
    assert payload["schema"] == "vq.admin.provision_check/1"
    assert payload["host"] == "host_d"
    assert isinstance(payload["checks"], list)


def test_provision_and_provision_user_remain_distinct_verbs() -> None:
    """One hyphen apart, opposite argument types, opposite locality."""
    result = CliRunner().invoke(main, ["admin", "--help"])

    assert "provision " in result.output
    assert "provision-user" in result.output


# --- regressions found in review -------------------------------------------


def test_multi_user_mode_is_read_from_the_target_not_the_driver(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A mixed fleet is the normal case, so the driver's flag is not the answer.

    Taking the driver's `[multi_user] enabled` as the target's would report a
    correctly provisioned single-user host as missing /opt/vq, /var/lib/vq and
    a root daemon it is not supposed to have -- six confident failures for a
    healthy machine -- and, reversed, would skip every multi-user check on the
    hosts that need them most.
    """
    # Driver says single-user; the target's own config says otherwise.
    runner, _rec = _fake_shell(multi_user=True)
    monkeypatch.setattr(provision.transport, "run_remote_shell", runner)
    monkeypatch.setattr(provision.transport, "run_remote_vq", _fake_programs())
    monkeypatch.setattr(
        provision,
        "local_ssh_checks",
        lambda host_cfg, *, probe_cache: ([], None, None, False),
    )

    payload = provision.diagnose_host(_cfg(multi_user=False), "host_d")

    verdict = _named(payload, "multi_user_mode")
    assert verdict["multi_user"] is True
    assert verdict["source"] == "target /etc/vq/config.toml"
    assert "not applicable" not in _named(payload, "state_root_perms")["message"]


def test_an_unreadable_target_config_is_unknown_without_driver_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A different host's policy cannot authorize or excuse this root unit."""
    def failing(host_cfg, *args, **kwargs):  # type: ignore[no-untyped-def]
        if "multi_user" in args[-1]:
            return subprocess.CompletedProcess(
                list(args), 1, stdout="", stderr="permission denied"
            )
        return subprocess.CompletedProcess(list(args), 0, stdout="", stderr="")

    monkeypatch.setattr(provision.transport, "run_remote_shell", failing)
    monkeypatch.setattr(provision.transport, "run_remote_vq", _fake_programs())
    monkeypatch.setattr(
        provision,
        "local_ssh_checks",
        lambda host_cfg, *, probe_cache: ([], None, None, False),
    )

    payload = provision.diagnose_host(_cfg(), "host_d")

    verdict = _named(payload, "multi_user_mode")
    assert verdict["ok"] is False
    assert verdict["multi_user"] is None
    assert verdict["config_state"] == "unknown"
    assert verdict["source"] == "target /etc/vq/config.toml"
    assert "not used as fallback authority" in verdict["message"]
    unit = _named(payload, "daemon_unit")
    assert unit["ok"] is False
    assert unit["config_multi_user"] is None
    assert "applicability is unknown" in unit["message"]


def test_a_malformed_target_config_is_unknown_without_driver_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unexpected parser output is not equivalent to disabled policy."""
    payload, _ = _diagnose(
        monkeypatch,
        overrides={"multi_user": "multi_user.enabled must be a boolean"},
    )

    verdict = _named(payload, "multi_user_mode")
    assert verdict["ok"] is False
    assert verdict["multi_user"] is None
    assert verdict["config_state"] == "unknown"
    assert "not used as fallback authority" in verdict["message"]
    assert _named(payload, "daemon_unit")["ok"] is False


def test_an_ssh_banner_on_stderr_does_not_turn_a_missing_install_green(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Probes parse stdout positionally, so stderr must never be glued in.

    A login banner, or the `stat -c` complaint from the first arm of the
    portability fallback, would otherwise sit in front of the sentinel.
    """
    def noisy(host_cfg, *args, **kwargs):  # type: ignore[no-untyped-def]
        script = args[-1]
        answer = ""
        for needle, value in HEALTHY.items():
            if needle in script:
                answer = value
                break
        if "--version" in script:
            answer = "missing"
        return subprocess.CompletedProcess(
            list(args),
            0,
            stdout=answer + "\n",
            stderr="Welcome to host_d! Last login: never\n",
        )

    monkeypatch.setattr(provision.transport, "run_remote_shell", noisy)
    monkeypatch.setattr(provision.transport, "run_remote_vq", _fake_programs())
    monkeypatch.setattr(
        provision,
        "local_ssh_checks",
        lambda host_cfg, *, probe_cache: ([], None, None, False),
    )

    payload = provision.diagnose_host(_cfg(), "host_d")

    assert _named(payload, "root_owned_install")["ok"] is False


def test_a_garbled_token_mode_fails_instead_of_raising(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`int(mode, 8)` on a stat error message is a ValueError, not a verdict."""
    payload, _ = _diagnose(
        monkeypatch,
        overrides={"admin-token": "stat: cannot stat: No such file"},
    )

    verdict = _named(payload, "admin_token_file")
    assert verdict["ok"] is False
    assert "could not be inspected" in verdict["message"]


def test_a_wrapper_that_only_mentions_the_vars_in_a_comment_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A substring search would pass this and miss the split-store failure."""
    payload, _ = _diagnose(
        monkeypatch,
        overrides={
            "cat ": (
                "#!/bin/sh\n"
                "# TODO: export VQ_CONFIG_DIR and VQ_STATE_DIR here\n"
                'exec /opt/vq/venv/bin/vq "$@"\n'
            ),
        },
    )

    verdict = _named(payload, "remote_vq_wrapper")
    assert verdict["ok"] is False
    assert verdict["exports"] == []


def test_a_remote_vq_carrying_arguments_resolves_its_program(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`command -v "vq --config /etc/vq"` finds nothing; the program does."""
    seen: list[str] = []

    def record(host_cfg, *args, **kwargs):  # type: ignore[no-untyped-def]
        seen.append(args[-1])
        answer = ""
        for needle, value in HEALTHY.items():
            if needle in args[-1]:
                answer = value
                break
        return subprocess.CompletedProcess(
            list(args), 0, stdout=answer + "\n", stderr=""
        )

    monkeypatch.setattr(provision.transport, "run_remote_shell", record)
    monkeypatch.setattr(provision.transport, "run_remote_vq", _fake_programs())
    monkeypatch.setattr(
        provision,
        "local_ssh_checks",
        lambda host_cfg, *, probe_cache: ([], None, None, False),
    )

    provision.diagnose_host(
        _cfg(remote_vq="/home/USER/bin/vq-fleet-admin --config /etc/vq"),
        "host_d",
    )

    resolution = next(s for s in seen if s.startswith("command -v"))
    assert resolution == "command -v /home/USER/bin/vq-fleet-admin 2>/dev/null || true"


def test_the_all_json_envelope_shape_follows_the_form_not_the_host_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A --all consumer must not break the day a second host is configured."""
    import json

    monkeypatch.setattr("vq.cli.config.load_config", lambda: _cfg())
    runner, _rec = _fake_shell()
    monkeypatch.setattr(provision.transport, "run_remote_shell", runner)
    monkeypatch.setattr(provision.transport, "run_remote_vq", _fake_programs())
    monkeypatch.setattr(
        provision,
        "local_ssh_checks",
        lambda host_cfg, *, probe_cache: ([], None, None, False),
    )

    result = CliRunner().invoke(main, ["admin", "provision", "--all", "--json"])

    payload = json.loads(result.output)
    assert payload["schema"] == "vq.admin.provision_check_fleet/1"
    assert set(payload["hosts"]) == {"host_d"}
    assert payload["ok"] is True


def test_all_against_an_empty_fleet_refuses_rather_than_reporting_ok(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`all([])` is True, so the empty case must be caught explicitly."""
    monkeypatch.setattr(
        "vq.cli.config.load_config",
        lambda: config.Config(hosts={}),
    )

    result = CliRunner().invoke(main, ["admin", "provision", "--all"])

    assert result.exit_code == 2
    assert "no [hosts.X] configured" in result.output


def test_the_report_header_does_not_claim_check_when_check_was_not_passed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("vq.cli.config.load_config", lambda: _cfg())
    runner, _rec = _fake_shell()
    monkeypatch.setattr(provision.transport, "run_remote_shell", runner)
    monkeypatch.setattr(provision.transport, "run_remote_vq", _fake_programs())
    monkeypatch.setattr(
        provision,
        "local_ssh_checks",
        lambda host_cfg, *, probe_cache: ([], None, None, False),
    )

    result = CliRunner().invoke(main, ["admin", "provision", "host_d"])

    assert result.output.splitlines()[0] == "== vq admin provision: host_d =="
