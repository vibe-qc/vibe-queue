"""`vq doctor` says whether a declared extras floor has a route (#84).

A venv program declares `extras` to say what its environment is *for*, and
that floor reaches pip only through vq's own `scripts/install.sh` or
`scripts/update.sh`, resolved inside the checkout the program points at.
Nothing told an operator when they were outside that: config load accepts the
key as long as one of the two scripts is configured and cannot check what
those scripts *point at*, because at load time there may be no checkout to
read; the provision refuses where the evidence is, but only once someone runs
it. So a program whose installer is a site command loaded clean and produced
the symptom the key exists to prevent -- a console venv declaring
`extras = ["web"]`, built without uvicorn, whose unit failed on every start
while `vq doctor` reported nothing unusual.

`vq programs --json` already carries `install_script` and `extras`, which is
enough for a fleet audit to find the affected programs (option 1 of the issue,
pinned in `test_programs.py`). This is the check that says it where an
operator is already looking.

Two boundaries these tests hold:

* the answer needs the checkout, so it is asked only on the host that has one.
  `cfg.programs` describes *this* machine, so `vq doctor REMOTE` must not
  answer about a remote host from it;
* a question that cannot be reached is not a failure. A host part-way through
  a provision has no checkout yet, and a doctor that goes red on that is a
  doctor operators learn to ignore.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import config, doctor, paths
from vq.cli import main


@pytest.fixture
def cfg_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    d = tmp_path / "cfg"
    d.mkdir()
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(d))
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    return d


def _script(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("#!/usr/bin/env bash\necho stub\n", encoding="utf-8")
    path.chmod(0o755)


def _vq_checkout(root: Path) -> Path:
    """What :func:`vq.admin.vq_project_root` recognises, with vq's scripts."""
    (root / "src" / "vq").mkdir(parents=True)
    (root / "pyproject.toml").write_text("", encoding="utf-8")
    _script(root / "scripts" / "install.sh")
    _script(root / "scripts" / "update.sh")
    (root / ".venv" / "bin").mkdir(parents=True)
    return root


def _foreign_checkout(root: Path) -> Path:
    """A program with its own installer, which has never heard of --extras."""
    (root / "bin").mkdir(parents=True)
    _script(root / "bin" / "setup.sh")
    _script(root / "bin" / "build.sh")
    (root / ".venv" / "bin").mkdir(parents=True)
    return root


def _write(
    cfg_dir: Path,
    checkout: Path,
    *,
    extras: str = 'extras = ["web"]',
    install_script: str | None = None,
    update_script: str | None = None,
    name: str = "console",
) -> config.Config:
    """One program, carrying exactly the scripts this test names.

    Nothing is filled in by default: a helper that writes both scripts
    regardless of the shape under test is how a whole lane of #47 passed
    without ever exercising the shape it was named for.
    """
    lines = [
        'default_host = "localhost"',
        "",
        "[hosts.localhost]",
        'ssh = "localhost"',
        "",
        f"[programs.{name}]",
        'kind = "venv"',
        f'python = "{checkout / ".venv" / "bin" / "python"}"',
        f'git_dir = "{checkout}"',
    ]
    if install_script is not None:
        lines.append(f'install_script = "{install_script}"')
    if update_script is not None:
        lines.append(f'update_script = "{update_script}"')
    if extras:
        lines.append(extras)
    (cfg_dir / "config.toml").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return config.load_config()


def _only_check(cfg: config.Config) -> dict[str, object]:
    checks = doctor.declared_extras_route_checks(cfg)
    assert len(checks) == 1, checks
    return checks[0]


class TestWhatTheCheckReports:
    def test_an_updater_that_is_vqs_own_re_applies_the_floor(
        self, cfg_dir: Path, tmp_path: Path,
    ) -> None:
        checkout = _vq_checkout(tmp_path / "vibe-queue")
        cfg = _write(cfg_dir, checkout, update_script="scripts/update.sh")

        verdict = _only_check(cfg)

        assert verdict["name"] == "declared_extras_routes"
        assert verdict["ok"] is True
        assert "re-applied by every" in str(verdict["message"])
        assert str(checkout / "scripts" / "update.sh") in str(verdict["message"])

    def test_an_install_only_route_says_nothing_re_applies_it(
        self, cfg_dir: Path, tmp_path: Path,
    ) -> None:
        """The shape the model admits: built once, and never rebuilt.

        Not a failure -- the provision really does apply the declaration --
        but the thing an operator needs to know before the next rebuild.
        """
        checkout = _vq_checkout(tmp_path / "vibe-queue")
        cfg = _write(cfg_dir, checkout, install_script="scripts/install.sh")

        verdict = _only_check(cfg)

        assert verdict["ok"] is True
        message = str(verdict["message"])
        assert "applied once, by the provision" in message
        assert "update_script is not configured" in message

    def test_a_declaration_with_no_route_at_all_fails(
        self, cfg_dir: Path, tmp_path: Path,
    ) -> None:
        """The situation nothing announced: a key that cannot reach pip."""
        checkout = _foreign_checkout(tmp_path / "vibeqc")
        cfg = _write(
            cfg_dir,
            checkout,
            install_script="bin/setup.sh",
            update_script="bin/build.sh",
        )

        verdict = _only_check(cfg)

        assert verdict["ok"] is False
        message = str(verdict["message"])
        assert "no route into the venv" in message
        assert "'bin/setup.sh'" in message
        assert "'bin/build.sh'" in message
        assert "console" in message

    def test_a_checkout_that_is_not_there_cannot_answer_and_stays_ok(
        self, cfg_dir: Path, tmp_path: Path,
    ) -> None:
        """A host part-way through a provision is not a misconfigured host."""
        cfg = _write(
            cfg_dir,
            tmp_path / "not-cloned-yet",
            install_script="scripts/install.sh",
            update_script="scripts/update.sh",
        )

        verdict = _only_check(cfg)

        assert verdict["ok"] is True
        message = str(verdict["message"])
        assert "cannot tell" in message
        assert "is not a readable directory" in message

    def test_a_script_that_is_missing_from_a_vq_checkout_is_answered(
        self, cfg_dir: Path, tmp_path: Path,
    ) -> None:
        """A checkout that is there and a script that is not is an answer.

        Nothing of vq's is going to run, so this is the no-route verdict and
        not the cannot-answer one -- the distinction the two carry.
        """
        checkout = _vq_checkout(tmp_path / "vibe-queue")
        cfg = _write(
            cfg_dir, checkout, update_script="scripts/does-not-exist.sh",
        )

        verdict = _only_check(cfg)

        assert verdict["ok"] is False
        assert "no route into the venv" in str(verdict["message"])

    def test_a_host_where_nothing_declares_extras_says_nothing(
        self, cfg_dir: Path, tmp_path: Path,
    ) -> None:
        """Which is every host that only runs the daemon."""
        checkout = _vq_checkout(tmp_path / "vibe-queue")
        cfg = _write(
            cfg_dir, checkout, extras="", update_script="scripts/update.sh",
        )

        assert doctor.declared_extras_route_checks(cfg) == []

    def test_the_record_carries_the_evidence_for_a_fleet_audit(
        self, cfg_dir: Path, tmp_path: Path,
    ) -> None:
        checkout = _vq_checkout(tmp_path / "vibe-queue")
        cfg = _write(
            cfg_dir,
            checkout,
            install_script="scripts/install.sh",
            update_script="scripts/update.sh",
        )

        programs = _only_check(cfg)["programs"]

        assert isinstance(programs, list)
        assert programs == [
            {
                "program": "console",
                "extras": ["web"],
                "install_script": "scripts/install.sh",
                "update_script": "scripts/update.sh",
                "install_route": "vq",
                "update_route": "vq",
                "install_detail": str(checkout / "scripts" / "install.sh"),
                "update_detail": str(checkout / "scripts" / "update.sh"),
                "applied_at_install": True,
                "applied_on_update": True,
            }
        ]


class TestWhereTheCheckRuns:
    def test_it_rides_the_local_doctor_payload_and_can_fail_it(
        self, cfg_dir: Path, tmp_path: Path,
    ) -> None:
        checkout = _foreign_checkout(tmp_path / "vibeqc")
        cfg = _write(
            cfg_dir,
            checkout,
            install_script="bin/setup.sh",
            update_script="bin/build.sh",
        )

        payload = doctor.diagnose_host(cfg, "localhost", timeout=1, check_timeout=1)

        named = [
            c for c in payload["checks"]  # type: ignore[union-attr]
            if c["name"] == "declared_extras_routes"
        ]
        assert named and named[0]["ok"] is False
        assert payload["ok"] is False

    def test_a_remote_host_is_not_answered_from_the_local_config(
        self, cfg_dir: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """`cfg.programs` is this machine's registry, not the remote's.

        The ssh and daemon probes are stubbed out: what is under test is which
        host the question is asked on, and a real probe would reach the
        network to answer a different question.
        """
        checkout = _foreign_checkout(tmp_path / "vibeqc")
        _write(
            cfg_dir,
            checkout,
            install_script="bin/setup.sh",
            update_script="bin/build.sh",
        )
        (cfg_dir / "config.toml").write_text(
            (cfg_dir / "config.toml").read_text(encoding="utf-8")
            + '\n[hosts.remote]\nssh = "remote.example.invalid"\n',
            encoding="utf-8",
        )
        cfg = config.load_config()
        monkeypatch.setattr(
            doctor, "local_ssh_checks", lambda *a, **k: ([], None, None, False),
        )
        monkeypatch.setattr(doctor, "daemon_checks", lambda *a, **k: ([], False))

        payload = doctor.diagnose_host(cfg, "remote", timeout=1, check_timeout=1)

        assert [
            c["name"] for c in payload["checks"]  # type: ignore[union-attr]
        ] == ["config"]

    def test_the_json_envelope_carries_the_verdict(
        self, cfg_dir: Path, tmp_path: Path,
    ) -> None:
        """Including the route verdicts, which a monitor reads as strings."""
        checkout = _vq_checkout(tmp_path / "vibe-queue")
        _write(cfg_dir, checkout, update_script="scripts/update.sh")

        result = CliRunner().invoke(
            main, ["doctor", "localhost", "--json", "--timeout", "1"],
        )

        payload = json.loads(result.output)
        verdict = next(
            c for c in payload["checks"] if c["name"] == "declared_extras_routes"
        )
        assert verdict["ok"] is True
        assert verdict["programs"][0]["update_route"] == "vq"
        assert verdict["programs"][0]["install_route"] == "absent"
