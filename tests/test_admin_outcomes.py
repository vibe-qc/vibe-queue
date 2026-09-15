"""The machine-readable contract an orchestration drives vq by.

Every chain written during the 2026-09 fleet migration ended up matching on
prose to decide between wait, skip, acknowledge and stop::

    grep -q "local checkout mutation lock" "$log" && { sleep 90; continue; }

That is load-bearing infrastructure spelled as a substring match on a
sentence. One assertion in ``tests/test_self_update.py`` did pin that
sentence, so vq's CI would have caught a reword -- but that protection ends
at this repository, and an orchestration greping a log gets no signal and
simply stops matching.

These tests pin the classification instead. **The messages below are
deliberately not asserted on** -- they are free to change, and one of them is
reworded in this same release to prove the guarantee is real.
"""
from __future__ import annotations

import contextlib
import json
import subprocess
import sys
import threading
from collections.abc import Iterator
from pathlib import Path

import pytest
from click.testing import CliRunner, Result

from vq import admin, config, paths, transport
from vq.cli import main

REPO = Path(__file__).resolve().parents[1]
SWEEP_SCRIPT = REPO / "contrib" / "fleet-sweep.sh"
ORCHESTRATION_DOC = REPO / "docs" / "orchestration.md"


def _git(cwd: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=cwd, check=True, capture_output=True, text=True,
    ).stdout.strip()


@pytest.fixture
def host(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    repo = tmp_path / "repo"
    (repo / "scripts").mkdir(parents=True)
    _git(tmp_path, "init", "-q", str(repo))
    _git(repo, "config", "user.email", "test@example.invalid")
    _git(repo, "config", "user.name", "Test User")
    (repo / "scripts" / "update.sh").write_text(
        "#!/usr/bin/env bash\necho built\n", encoding="utf-8",
    )
    (repo / "scripts" / "update.sh").chmod(0o755)
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "first")
    (tmp_path / "cfg" / "config.toml").write_text(
        'default_host = "localhost"\n'
        "\n"
        "[hosts.localhost]\n"
        'ssh = "localhost"\n'
        "\n"
        "[programs.demo]\n"
        'kind = "venv"\n'
        f'python = "{sys.executable}"\n'
        f'git_dir = "{repo}"\n'
        'update_script = "scripts/update.sh"\n',
        encoding="utf-8",
    )
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    paths.jobs_dir().mkdir(parents=True, exist_ok=True)
    return tmp_path


class TestTheContractItself:
    def test_the_outcome_set_is_closed_and_exact(self) -> None:
        """Adding a value is a contract change and must be a deliberate one."""
        assert admin.ADMIN_OUTCOMES == (
            "ok",
            "already-current",
            "locked",
            "marker-present",
            "precondition-failed",
            "failed",
        )

    def test_every_outcome_has_an_exit_code(self) -> None:
        assert set(admin.ADMIN_OUTCOME_EXIT_CODES) == set(admin.ADMIN_OUTCOMES)

    def test_the_exit_codes_are_exact(self) -> None:
        assert admin.ADMIN_OUTCOME_EXIT_CODES == {
            "ok": 0,
            "already-current": 0,
            "locked": 75,
            "marker-present": 76,
            "precondition-failed": 77,
            "failed": 1,
        }

    def test_continue_outcomes_share_zero_and_the_rest_do_not(self) -> None:
        """`ok` and `already-current` both mean continue, and every `set -e`
        wrapper treats non-zero as stop. A caller that must tell them apart
        reads `outcome`. Everything that is not "continue" is non-zero, and
        each of those is distinct."""
        codes = admin.ADMIN_OUTCOME_EXIT_CODES
        assert codes["ok"] == codes["already-current"] == 0
        stop = [codes[name] for name in admin.ADMIN_OUTCOMES if codes[name] != 0]
        assert len(set(stop)) == len(stop)

    @pytest.mark.parametrize(
        ("error", "outcome"),
        [
            (admin.AdminError, "failed"),
            (admin.AdminUpdateInProgress, "locked"),
            (admin.AdminMarkerPresent, "marker-present"),
            (admin.AdminPreconditionFailed, "precondition-failed"),
        ],
    )
    def test_each_error_class_classifies_itself(
        self, error: type[admin.AdminError], outcome: str,
    ) -> None:
        """The specific mapping, written out because this *is* the pin."""
        assert error("some message").outcome == outcome

    def test_no_error_class_escapes_the_closed_set(self) -> None:
        """Derived, because the list above is a pin and not a census.

        The parametrised mapping states what the known classes mean; it
        cannot state anything about a class added later. A fifth
        `AdminError` subclass carrying `outcome = "marker_present"` -- an
        underscore where the contract has a hyphen -- would satisfy every
        assertion above and then raise `ValueError` out of
        `AdminOutcomeError` at the moment a real operator hit that gate.

        Walked recursively rather than one level, because a subclass of a
        subclass is exactly as capable of being wrong.
        """

        def descendants(cls: type) -> set[type]:
            found: set[type] = set()
            for sub in cls.__subclasses__():
                found.add(sub)
                found |= descendants(sub)
            return found

        classes = {admin.AdminError} | descendants(admin.AdminError)
        assert len(classes) >= 4, "expected the classified admin errors"
        for cls in classes:
            assert cls.outcome in admin.ADMIN_OUTCOMES, (
                f"{cls.__name__}.outcome = {cls.outcome!r} is not one of "
                f"{admin.ADMIN_OUTCOMES}"
            )

    def test_the_new_classes_stay_catchable_as_the_old_ones(self) -> None:
        """Every existing `except AdminUpdateInProgress` must keep working."""
        assert issubclass(admin.AdminMarkerPresent, admin.AdminUpdateInProgress)
        assert issubclass(admin.AdminPreconditionFailed, admin.AdminError)
        assert issubclass(admin.AdminUpdateInProgress, admin.AdminError)


class TestUpdateOutcomesThroughTheCLI:
    def _sha(self, host: Path) -> str:
        return _git(host / "repo", "rev-parse", "HEAD")

    def test_a_conflicting_failed_marker_is_marker_present(
        self, host: Path,
    ) -> None:
        """host_c2's five hours: neither a lock error nor a build error,
        so an orchestration that could only read prose stopped."""
        admin.acquire_admin_update_marker(envs=["demo"], host="localhost")
        admin.transition_admin_update_state(
            admin.ADMIN_UPDATE_STATE_FAILED, failure_reason="boom",
        )

        result = CliRunner().invoke(main, ["admin", "update", "demo", "--json"])

        assert result.exit_code == 76
        assert json.loads(result.output)["outcome"] == "marker-present"

    def test_a_held_lock_is_locked(
        self, host: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(
            "vq.cli.admin_module.update_env",
            lambda *a, **k: (_ for _ in ()).throw(
                admin.AdminUpdateInProgress("another admin update owns the lock")
            ),
        )

        result = CliRunner().invoke(main, ["admin", "update", "demo", "--json"])

        assert result.exit_code == 75
        assert json.loads(result.output)["outcome"] == "locked"

    def test_a_refused_safety_gate_is_precondition_failed(
        self, host: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Distinct from `failed` because retrying or forcing past a correct
        refusal is dangerous, and both used to be exit 1 plus prose."""
        monkeypatch.setattr(
            "vq.cli.admin_module.update_env",
            lambda *a, **k: (_ for _ in ()).throw(
                admin.AdminPreconditionFailed("host is not converged")
            ),
        )

        result = CliRunner().invoke(main, ["admin", "update", "demo", "--json"])

        assert result.exit_code == 77
        assert json.loads(result.output)["outcome"] == "precondition-failed"

    def test_an_already_deployed_target_is_already_current_and_does_no_work(
        self, host: Path,
    ) -> None:
        result = CliRunner().invoke(
            main,
            ["admin", "update", "demo", "--expected-sha", self._sha(host), "--json"],
        )

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload["outcome"] == "already-current"
        assert payload["already_current"] is True
        assert payload["success"] is True
        # Nothing ran: no pull, no build.
        assert payload["git_pull_rc"] is None
        assert payload["update_script_rc"] is None

    def test_a_wrong_argv_is_still_a_usage_error(self, host: Path) -> None:
        """Unknown env is not one of the outcomes: the operation never
        started and no host state is implied. Exit 2 is the universal
        spelling and callers already know it."""
        result = CliRunner().invoke(main, ["admin", "update", "nosuchenv"])

        assert result.exit_code == 2

    def test_the_text_form_names_the_outcome_too(
        self, host: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(
            "vq.cli.admin_module.update_env",
            lambda *a, **k: (_ for _ in ()).throw(
                admin.AdminMarkerPresent("a marker is present")
            ),
        )

        result = CliRunner().invoke(main, ["admin", "update", "demo"])

        assert result.exit_code == 76
        assert "[marker-present]" in result.output


class TestAlreadyCurrentIsNotJustASHAComparison:
    """host_e sat at the right commit with a venv that could not import.

    A check that compared SHAs alone would call that converged and skip it
    forever, which is the failure this codebase keeps re-learning.
    """

    def _prog(self, host: Path, **overrides: object) -> config.VenvProgram:
        cfg = config.load_config()
        prog = cfg.programs["demo"]
        for key, value in overrides.items():
            object.__setattr__(prog, key, value)
        return prog

    def test_the_right_commit_with_a_broken_runtime_is_not_current(
        self, host: Path,
    ) -> None:
        prog = self._prog(host, python=str(host / "gone" / "bin" / "python"))

        current, why = admin.already_current(
            prog, expected_sha=_git(host / "repo", "rev-parse", "HEAD"),
            expected_tag=None,
        )

        assert current is False
        assert "not healthy" in why

    def test_the_right_commit_and_a_healthy_runtime_is_current(
        self, host: Path,
    ) -> None:
        current, _why = admin.already_current(
            self._prog(host),
            expected_sha=_git(host / "repo", "rev-parse", "HEAD"),
            expected_tag=None,
        )

        assert current is True

    def test_healthy_fixture_does_not_require_a_checkout_venv(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        request: pytest.FixtureRequest,
    ) -> None:
        # CI installs into the image interpreter, not <checkout>/.venv. Model
        # that layout without moving or damaging the developer's real venv.
        checkout = tmp_path / "checkout-without-venv"
        monkeypatch.setitem(globals(), "__file__", str(checkout / "tests" / Path(__file__).name))
        host = request.getfixturevalue("host")
        current, why = admin.already_current(
            self._prog(host), expected_sha=_git(host / "repo", "rev-parse", "HEAD"),
            expected_tag=None,
        )
        assert not (checkout / ".venv").exists()
        assert current is True, why

    def test_a_different_commit_is_not_current(self, host: Path) -> None:
        current, why = admin.already_current(
            self._prog(host), expected_sha="b" * 40, expected_tag=None,
        )

        assert current is False
        assert "not " + "b" * 40 in why

    def test_a_dirty_checkout_is_not_current(self, host: Path) -> None:
        (host / "repo" / "scratch.txt").write_text("wip", encoding="utf-8")

        current, why = admin.already_current(
            self._prog(host),
            expected_sha=_git(host / "repo", "rev-parse", "HEAD"),
            expected_tag=None,
        )

        assert current is False
        assert "dirty" in why

    def test_a_tag_that_does_not_match_is_not_current(self, host: Path) -> None:
        current, why = admin.already_current(
            self._prog(host),
            expected_sha=_git(host / "repo", "rev-parse", "HEAD"),
            expected_tag="v9.9.9",
        )

        assert current is False
        assert "v9.9.9" in why

    def test_without_a_target_the_answer_is_always_no(self, host: Path) -> None:
        """An untargeted update means "bring this to the tip", and only the
        update can know whether it already is."""
        current, why = admin.already_current(
            self._prog(host), expected_sha=None, expected_tag=None,
        )

        assert current is False
        assert "no target" in why


class TestAnInconsistentServingPairIsNeverARollbackBaseline:
    """#44: an atomic update must not arm on a checkout vq never installed.

    host_e, 2026-09-12: an update killed by its ssh session (#37) had already
    moved the checkout to v0.17.1 while the installed runtime stayed older. The
    next attempt snapshotted that pair as its "pre-update" state and, when the
    build was reaped, "rolled back" to a checkout the core was never built
    from. The refusal must come before any marker, pause, checkout or build.
    """

    @pytest.fixture
    def armed(self, host: Path) -> dict[str, str]:
        """Commits A, B and C; the checkout parked at B; an import check that
        arms the atomic path. Which commit was *installed* is per test."""
        repo = host / "repo"
        sentinel = host / "update-script-ran"
        (repo / "scripts" / "update.sh").write_text(
            f"#!/usr/bin/env bash\ntouch '{sentinel}'\n", encoding="utf-8",
        )
        _git(repo, "commit", "-qam", "A")
        shas = {"A": _git(repo, "rev-parse", "HEAD")}
        for name in ("B", "C"):
            (repo / name).write_text(name, encoding="utf-8")
            _git(repo, "add", name)
            _git(repo, "commit", "-qm", name)
            shas[name] = _git(repo, "rev-parse", "HEAD")
        _git(repo, "checkout", "-q", "--detach", shas["B"])
        self._set_import_check(host, "json")
        return shas

    @staticmethod
    def _set_import_check(host: Path, module: str | None) -> None:
        cfg = host / "cfg" / "config.toml"
        lines = [
            line
            for line in cfg.read_text(encoding="utf-8").splitlines()
            if not line.startswith("import_check")
        ]
        if module is not None:
            lines.append(f'import_check = "{module}"')
        cfg.write_text("\n".join(lines) + "\n", encoding="utf-8")

    @staticmethod
    def _installed(sha: str) -> None:
        admin.write_admin_status({
            "demo": admin.AdminUpdateRecord(
                last_updated_at="2026-09-11T17:41:00+00:00",
                last_success=True,
                last_sha=sha,
                last_installed_sha=sha,
            ),
        })

    @staticmethod
    def _nothing_was_touched(host: Path, head: str) -> None:
        assert _git(host / "repo", "rev-parse", "HEAD") == head
        assert not (host / "update-script-ran").exists()
        assert list(admin._admin_update_marker_entries()) == []

    def test_an_update_to_another_commit_refuses_before_touching_anything(
        self, host: Path, armed: dict[str, str],
    ) -> None:
        self._installed(armed["A"])

        result = CliRunner().invoke(
            main,
            ["admin", "update", "demo", "--expected-sha", armed["C"], "--json"],
        )

        assert result.exit_code == 77, result.output
        assert json.loads(result.stdout)["outcome"] == "precondition-failed"
        self._nothing_was_touched(host, armed["B"])

    def test_an_untargeted_update_refuses_too(
        self, host: Path, armed: dict[str, str],
    ) -> None:
        self._installed(armed["A"])

        result = CliRunner().invoke(main, ["admin", "update", "demo", "--json"])

        assert result.exit_code == 77, result.output
        self._nothing_was_touched(host, armed["B"])

    def test_an_all_batch_refuses_before_touching_anything(
        self, host: Path, armed: dict[str, str],
    ) -> None:
        self._installed(armed["A"])

        with pytest.raises(admin.AdminPreconditionFailed):
            admin.update_all(config.load_config(), host="localhost")

        self._nothing_was_touched(host, armed["B"])

    @staticmethod
    def _was_not_refused(result: Result) -> None:
        """The gate let it through. What happens next is not this gate's
        business: this fixture repository has no upstream, so the update
        itself goes on to fail at its pull, as it did before #44."""
        assert result.exit_code != 77, result.output
        payload = json.loads(result.stdout)
        assert payload["outcome"] != "precondition-failed"

    def test_a_rebuild_at_the_checkout_s_own_commit_is_the_remedy(
        self, host: Path, armed: dict[str, str],
    ) -> None:
        """Refusing this too would strand the lane: it is the recovery the
        refusal names, and a failure restores exactly the state it started
        from. The runtime is broken so the request is not already-current."""
        self._installed(armed["A"])
        self._set_import_check(host, "vq_no_such_module_44")

        result = CliRunner().invoke(
            main,
            ["admin", "update", "demo", "--expected-sha", armed["B"], "--json"],
        )

        self._was_not_refused(result)

    @pytest.mark.parametrize(
        "case", ["consistent-pair", "never-installed-by-vq", "not-atomic"],
    )
    def test_the_gate_stays_out_of_the_way_otherwise(
        self, host: Path, armed: dict[str, str], case: str,
    ) -> None:
        if case == "consistent-pair":
            self._installed(armed["B"])
        elif case == "not-atomic":
            self._installed(armed["A"])
            self._set_import_check(host, None)

        result = CliRunner().invoke(
            main,
            ["admin", "update", "demo", "--expected-sha", armed["C"], "--json"],
        )

        self._was_not_refused(result)


class TestAcknowledgeFailedMarker:
    """The unattended half of `vq admin clear-update-marker`.

    Recovery was: read the marker, clear it (interactively), re-run. Three
    steps and a TTY, and an orchestration has none of them -- host_c2's
    marker sat about five hours behind exactly that.
    """

    def _failed_marker(self, envs: list[str]) -> None:
        admin.acquire_admin_update_marker(envs=envs, host="localhost")
        admin.transition_admin_update_state(
            admin.ADMIN_UPDATE_STATE_FAILED, failure_reason="boom",
        )

    def test_it_acknowledges_and_proceeds(self, host: Path) -> None:
        self._failed_marker(["demo"])

        result = CliRunner().invoke(
            main,
            [
                "admin", "update", "demo", "--acknowledge-failed-marker",
                "--expected-sha", _git(host / "repo", "rev-parse", "HEAD"),
                "--json",
            ],
        )

        assert result.exit_code == 0
        assert json.loads(result.output)["outcome"] == "already-current"
        assert admin.read_admin_update_marker() is None

    def test_without_the_flag_the_marker_still_blocks(self, host: Path) -> None:
        self._failed_marker(["demo"])

        result = CliRunner().invoke(main, ["admin", "update", "demo", "--json"])

        assert result.exit_code == 76
        assert json.loads(result.output)["outcome"] == "marker-present"

    def test_it_refuses_a_marker_that_did_not_fail(self, host: Path) -> None:
        """A live or stale marker is not something to discard unattended."""
        admin.acquire_admin_update_marker(envs=["demo"], host="localhost")

        result = CliRunner().invoke(
            main,
            ["admin", "update", "demo", "--acknowledge-failed-marker", "--json"],
        )

        assert result.exit_code == 77
        assert json.loads(result.output)["outcome"] == "precondition-failed"
        assert admin.read_admin_update_marker() is not None, (
            "a marker it refused to acknowledge must still be there"
        )

    def test_it_never_acknowledges_another_program_s_marker(
        self, host: Path,
    ) -> None:
        """Scope, not blast radius: acknowledging one program must not clear
        a marker that belongs to a different one."""
        self._failed_marker(["someone-else"])

        acknowledged = admin.acknowledge_failed_markers(["demo"], "localhost")

        assert acknowledged == []
        assert admin.read_admin_update_marker() is not None

    def test_it_reports_what_it_cleared(self, host: Path) -> None:
        self._failed_marker(["demo"])

        acknowledged = admin.acknowledge_failed_markers(["demo"], "localhost")

        assert len(acknowledged) == 1
        assert admin.read_admin_update_marker() is None

    def test_nothing_to_acknowledge_is_a_quiet_no_op(self, host: Path) -> None:
        assert admin.acknowledge_failed_markers(["demo"], "localhost") == []


class TestConfirmationWithoutATerminal:
    """A delegated `clear-update-marker HOST` has no TTY on the far side."""

    def test_a_piped_answer_still_works(self, host: Path) -> None:
        """The fix must not break confirmation that is merely not a TTY --
        an answer on stdin is still an answer."""
        admin.acquire_admin_update_marker(envs=["demo"], host="localhost")
        admin.transition_admin_update_state(
            admin.ADMIN_UPDATE_STATE_FAILED, failure_reason="boom",
        )

        result = CliRunner().invoke(
            main, ["admin", "clear-update-marker", "localhost"], input="y\n",
        )

        assert result.exit_code == 0, result.output
        assert admin.read_admin_update_marker() is None

    def test_no_answer_and_no_terminal_names_the_flag(self, host: Path) -> None:
        """Rather than a bare "Aborted!" naming neither cause nor remedy."""
        admin.acquire_admin_update_marker(envs=["demo"], host="localhost")
        admin.transition_admin_update_state(
            admin.ADMIN_UPDATE_STATE_FAILED, failure_reason="boom",
        )

        result = CliRunner().invoke(
            main, ["admin", "clear-update-marker", "localhost"], input="",
        )

        assert result.exit_code == 2
        assert "--yes" in result.output
        assert admin.read_admin_update_marker() is not None


class TestStatusAnswersTheSequencingQuestions:
    """`in_flight` and `last_outcome`, so nobody writes `ps | grep` again.

    The migration's waiter was::

        while [ "$(ssh host 'ps -eo command | grep -c "[n]inja"' || echo 1)" != "0" ]

    `grep -c` exits 1 on a zero count, so the `|| echo 1` fallback fired on
    *success* and produced "0\\n1", which never equals "0". The build
    finished; the loop ran for six hours. That bug belongs to whoever wrote
    it -- and nobody should have to write it, which is what these fields are
    for.
    """

    def _status(self) -> dict:
        cfg = config.load_config()
        return json.loads(admin.format_admin_status_json(cfg))

    def test_a_quiet_host_reports_nothing_in_flight(self, host: Path) -> None:
        payload = self._status()

        assert payload["in_flight"] is False
        assert payload["last_outcome"] is None

    def test_a_live_marker_is_in_flight(self, host: Path) -> None:
        admin.acquire_admin_update_marker(envs=["demo"], host="localhost")

        assert self._status()["in_flight"] is True

    def test_a_failed_marker_is_not_in_flight(self, host: Path) -> None:
        """It is something to acknowledge, not something to wait for.
        Conflating the two is exactly what sends a caller to `ps`."""
        admin.acquire_admin_update_marker(envs=["demo"], host="localhost")
        admin.transition_admin_update_state(
            admin.ADMIN_UPDATE_STATE_FAILED, failure_reason="boom",
        )

        assert self._status()["in_flight"] is False

    def test_last_outcome_reports_the_recorded_verdict(self, host: Path) -> None:
        admin.write_admin_status({
            "demo": admin.AdminUpdateRecord(
                last_updated_at="2026-09-10T12:00:00+00:00",
                last_success=True,
            )
        })

        payload = self._status()

        assert payload["last_outcome"] == "ok"
        assert payload["last_outcome_at"] == "2026-09-10T12:00:00+00:00"
        assert payload["last_outcome"] in admin.ADMIN_OUTCOMES

    def test_a_recorded_failure_reports_failed(self, host: Path) -> None:
        admin.write_admin_status({
            "demo": admin.AdminUpdateRecord(
                last_updated_at="2026-09-10T12:00:00+00:00",
                last_success=False,
            )
        })

        assert self._status()["last_outcome"] == "failed"


class TestTheWorkedExampleHonoursTheContract:
    """`contrib/fleet-sweep.sh` is the check that the contract is finished.

    The brief's test for this work was: write the orchestration the migration
    wished it had -- retry `locked`, acknowledge `marker-present`, skip
    `already-current`, stop on `precondition-failed` -- using only exit codes
    and `--json`, with no `grep` of any message anywhere. If it cannot be
    written cleanly, the contract is not done.
    """

    SCRIPT = SWEEP_SCRIPT

    def _code_lines(self) -> list[str]:
        return [
            line
            for line in self.SCRIPT.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.strip().startswith("#")
        ]

    def test_it_greps_no_vq_output(self) -> None:
        """Comments may name the anti-pattern; code may not perform it."""
        offenders = [line for line in self._code_lines() if "grep" in line]

        assert offenders == []

    def test_it_shells_out_to_no_process_table(self) -> None:
        """The six-hour waiter was `ps -eo command | grep -c "[n]inja"`."""
        offenders = [
            line for line in self._code_lines()
            if "ps -eo" in line or "pgrep" in line
        ]

        assert offenders == []

    def test_it_branches_on_every_non_continue_outcome(self) -> None:
        """Each outcome that is not "continue" needs its own handling.

        Derived from `ADMIN_OUTCOME_EXIT_CODES`, not from a list written out
        here. This assertion used to read `for code in (75, 76, 77)` -- a
        partial enumeration presenting itself as complete, which passes
        forever once a seventh outcome is added while the script quietly
        routes it to its `*)` fallback. The vq documentation chat hit the
        same shape in a tutorial that listed seven of eight terminal states
        and had been proofread against the enum once already; deriving the
        list is what makes it grow with the contract.
        """
        body = self.SCRIPT.read_text(encoding="utf-8")

        stop_codes = {
            code for code in admin.ADMIN_OUTCOME_EXIT_CODES.values() if code != 0
        }
        assert stop_codes, "the contract must have at least one stop outcome"
        for code in sorted(stop_codes):
            assert str(code) in body, (
                f"exit {code} has no branch in the worked example"
            )

    def test_the_codes_it_hardcodes_are_the_ones_vq_emits(self) -> None:
        """A worked example that drifts from the contract is worse than none."""
        body = self.SCRIPT.read_text(encoding="utf-8")

        assert f"EX_LOCKED={admin.ADMIN_OUTCOME_EXIT_CODES['locked']}" in body
        assert (
            f"EX_MARKER={admin.ADMIN_OUTCOME_EXIT_CODES['marker-present']}"
            in body
        )
        assert (
            "EX_PRECONDITION="
            f"{admin.ADMIN_OUTCOME_EXIT_CODES['precondition-failed']}" in body
        )

    def test_it_is_executable_and_parses(self) -> None:
        import os

        assert os.access(self.SCRIPT, os.X_OK)
        subprocess.run(
            ["bash", "-n", str(self.SCRIPT)], check=True, capture_output=True,
        )


class TestAFailedMarkerBlocksOnlyItsOwnScope:
    """The blast-radius claim, checked rather than assumed.

    The brief reported that one program's failed marker blocked unrelated
    programs on the same host, and asked for the marker to be scoped to what
    failed. It already is: `_admin_update_resources` keys a plain env on its
    own name, so `vibeview-dev` and `vibeqc-release` are different resources.
    These tests pin that, because a regression here would recreate the report.

    What the failed marker *does* hold host-wide is **dispatch** -- see
    `test_a_failed_marker_still_holds_local_dispatch`. That is a different
    mechanism with a different rationale, and it is what actually sat on
    host_c2 and host_e.
    """

    def _failed_marker_for(self, env: str) -> None:
        admin.acquire_admin_update_marker(envs=[env], host="localhost")
        admin.transition_admin_update_state(
            admin.ADMIN_UPDATE_STATE_FAILED, failure_reason="boom",
        )

    @pytest.mark.parametrize(
        "other", ["vibeqc-release", "vibeqc-dev", "vibe-view"],
    )
    def test_an_unrelated_program_is_not_blocked(
        self, host: Path, other: str,
    ) -> None:
        self._failed_marker_for("vibeview-dev")

        # Does not raise: a different program is a different resource.
        admin._guard_admin_update_marker(
            force=False, envs=[other], host="localhost",
        )

    def test_the_program_that_failed_is_blocked(self, host: Path) -> None:
        self._failed_marker_for("vibeview-dev")

        with pytest.raises(admin.AdminMarkerPresent):
            admin._guard_admin_update_marker(
                force=False, envs=["vibeview-dev"], host="localhost",
            )

    def test_an_all_request_is_blocked_because_it_includes_the_failure(
        self, host: Path,
    ) -> None:
        """`--all` genuinely overlaps, and refusing it is correct -- this is
        the shape most likely to be read as over-blocking."""
        self._failed_marker_for("vibeview-dev")

        with pytest.raises(admin.AdminMarkerPresent):
            admin._guard_admin_update_marker(
                force=False,
                envs=["vibeqc-release", "vibeqc-dev", "vibeview-dev"],
                host="localhost",
            )

    def test_a_failed_marker_still_holds_local_dispatch(
        self, host: Path,
    ) -> None:
        """The real host-wide hold, and it is on jobs rather than updates.

        Correct where the marker was born -- a venv being rebuilt must not be
        dispatched into -- and it does not narrow once the update has stopped,
        which is why host_e's stale marker was "still scoping host_e's
        dispatch" hours later. Pinned here so the behaviour is visible rather
        than surprising; narrowing it is a separate design decision.
        """
        self._failed_marker_for("vibeview-dev")

        scope = admin.admin_update_marker_scope(admin.read_admin_update_marker())

        assert scope == frozenset({admin.LOCAL_DISPATCH_SCOPE})


class TestJsonStdoutStaysOneDocument:
    """`--json` must emit exactly one JSON value, whatever happened.

    The first cut of the outcome error printed its own
    `{"outcome": ..., "error": ...}` object even when the verb had already
    written a result payload to stdout, leaving two JSON documents on one
    stream. A caller doing `json.load(stdout)` gets "Extra data" -- which is
    precisely the class of breakage this whole contract exists to remove.
    """

    def test_a_failure_before_any_payload_emits_the_outcome_object(
        self, host: Path,
    ) -> None:
        admin.acquire_admin_update_marker(envs=["demo"], host="localhost")
        admin.transition_admin_update_state(
            admin.ADMIN_UPDATE_STATE_FAILED, failure_reason="boom",
        )

        result = CliRunner().invoke(main, ["admin", "update", "demo", "--json"])

        payload = json.loads(result.output)  # exactly one document
        assert payload["outcome"] == "marker-present"

    def test_a_failure_after_a_payload_does_not_add_a_second(
        self, host: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The result payload already carries `outcome`; the exit code
        classifies. A second document would add nothing and break parsing."""
        failed = admin.UpdateResult(
            env="demo",
            git_dir=str(host / "repo"),
            branch=None,
            update_script="scripts/update.sh",
            git_pull_rc=0,
            update_script_rc=1,
            update_script_output="boom",
        )
        monkeypatch.setattr(
            "vq.cli.admin_module.update_env", lambda *a, **k: failed,
        )

        result = CliRunner().invoke(main, ["admin", "update", "demo", "--json"])

        assert result.exit_code == 1
        payload = json.loads(result.stdout)  # would raise "Extra data" before
        assert payload["outcome"] == "failed"
        assert payload["success"] is False


class TestTheDocumentedTableMatchesTheCode:
    """`docs/orchestration.md`'s outcome table is the contract, published.

    A worked example that drifts is worse than none, and the same is true of
    the table someone reads before writing their orchestration: a stale row
    misleads the person *writing* a caller, where a stale quote elsewhere in
    the docs merely misinforms someone reading one. The exit codes are
    already pinned against `ADMIN_OUTCOME_EXIT_CODES`; this closes the doc
    half.
    """

    DOC = ORCHESTRATION_DOC

    def _table_rows(self) -> dict[str, int]:
        rows: dict[str, int] = {}
        for line in self.DOC.read_text(encoding="utf-8").splitlines():
            if not line.startswith("| `"):
                continue
            cells = [cell.strip() for cell in line.strip("|").split("|")]
            name = cells[0].strip("`")
            if name not in admin.ADMIN_OUTCOMES:
                continue
            rows[name] = int(cells[1])
        return rows

    def test_every_outcome_is_documented(self) -> None:
        assert set(self._table_rows()) == set(admin.ADMIN_OUTCOMES)

    def test_every_documented_exit_code_is_the_one_vq_emits(self) -> None:
        assert self._table_rows() == admin.ADMIN_OUTCOME_EXIT_CODES

    def test_the_doc_and_the_worked_example_still_name_each_other(self) -> None:
        """Each points at the other, and the paths are derived, not typed.

        This assertion used to spell both paths out. That reads like a pin of
        a cross-reference and is a contract check wearing a pin's clothes:
        move the doc, update `ORCHESTRATION_DOC`, and the literal
        `"docs/orchestration.md"` still matches the script's now-stale
        pointer -- green while the script names a file that no longer exists,
        which is the exact failure the docstring claimed to prevent.

        Derived from the Path objects, a move makes the assertion demand the
        *new* path. The vq documentation chat hit the identical shape in a
        guard asserting a hand-written `vq.spec.JobState` appeared in a note.
        """
        script = SWEEP_SCRIPT.read_text(encoding="utf-8")
        doc = ORCHESTRATION_DOC.read_text(encoding="utf-8")

        assert str(ORCHESTRATION_DOC.relative_to(REPO)) in script
        assert str(SWEEP_SCRIPT.relative_to(REPO)) in doc


# --------------------------------------------------------------------------
# The scheduler lanes: `vq admin update HOST` and `vq admin update PROGRAM HOST`
# --------------------------------------------------------------------------

PIN = "f" * 40

HELPER_LANE = ["admin", "update", "host_f", "--expected-sha", PIN]
RUNTIME_LANE = ["admin", "update", "vibeqc-release", "host_f", "--expected-sha", PIN]
SCHEDULER_LANES = [
    pytest.param(HELPER_LANE, "update_scheduler_host", id="helper"),
    pytest.param(RUNTIME_LANE, "update_scheduler_runtime", id="runtime"),
]


def _write_scheduler_config(tmp_path: Path, *, driver: str = "localhost") -> None:
    """A managed PBS host whose helper and runtime lanes route to ``driver``."""
    driver_ssh = "localhost" if driver == "localhost" else "driver.example.invalid"
    (tmp_path / "cfg" / "config.toml").write_text(
        f'default_host = "{driver}"\n'
        "\n"
        f"[hosts.{driver}]\n"
        f'ssh = "{driver_ssh}"\n'
        "\n"
        "[hosts.host_f]\n"
        'ssh = "host_f-login.example.invalid"\n'
        'scheduler = "pbs"\n'
        'scheduler_dialect = "torque"\n'
        'scratch_root = "/cluster/scratch"\n'
        f'scheduler_driver = "{driver}"\n'
        'fleet_role = "managed"\n'
        'scheduler_update_command = "/site/bin/update-helper"\n'
        "\n"
        "[hosts.host_f.scheduler_runtime_deployments.vibeqc-release]\n"
        'update_command = "/site/bin/deploy-runtime"\n'
        'install_command = "/site/bin/install-runtime"\n'
        'update_host = "cluster-build"\n'
        'verify_command = "/site/bin/verify-runtime"\n'
        "timeout_seconds = 321\n",
        encoding="utf-8",
    )


@pytest.fixture
def scheduler_host(host: Path) -> Path:
    _write_scheduler_config(host)
    return host


@contextlib.contextmanager
def _ownership_held_by_another_operation() -> Iterator[None]:
    """Hold the checkout-mutation lock the way a concurrent update does.

    Not in this thread: the lock is reentrant for its owner, so holding it
    here would let the verb straight through. Another thread is what a second
    update inside one process looks like, and it refuses at the same
    process-level check a second process fails at the flock -- with the
    same message the 2026-09-11 helper lanes printed.
    """
    acquired = threading.Event()
    release = threading.Event()
    failure: list[BaseException] = []

    def hold() -> None:
        try:
            with admin.admin_update_ownership():
                acquired.set()
                release.wait(timeout=60)
        except BaseException as exc:  # noqa: BLE001 -- re-raised in the test
            failure.append(exc)
            acquired.set()

    other = threading.Thread(target=hold, name="another-admin-operation")
    other.start()
    try:
        assert acquired.wait(timeout=60), "the other operation never took the lock"
        if failure:
            raise failure[0]
        yield
    finally:
        release.set()
        other.join(timeout=60)


def _raising(error: type[admin.AdminError], message: str):  # type: ignore[no-untyped-def]
    def raise_it(*args: object, **kwargs: object) -> None:
        raise error(message)

    return raise_it


class TestTheSchedulerLanesClassifyTheSameWay:
    """The host_f and host_c helper lanes of 2026-09-11 13:30 UTC.

    Launched alongside a host_c2 `vibeqc-release` build, both returned
    exit 1 with "this checkout is being mutated by another admin operation;
    wait for it to finish, then retry". The env lane had classified exactly
    that as `locked` / 75 for a release already; the helper and runtime lanes
    still rendered it as a plain click error, so a `contrib/fleet-sweep.sh`
    chain stopped where the message told it to wait.
    """

    @pytest.mark.parametrize(("argv", "_lane"), SCHEDULER_LANES)
    def test_a_held_ownership_lock_is_locked(
        self, scheduler_host: Path, argv: list[str], _lane: str,
    ) -> None:
        """The real lock, held by a real other operation: exit 75."""
        with _ownership_held_by_another_operation():
            result = CliRunner().invoke(main, [*argv, "--json"])

        assert result.exit_code == 75, result.output
        assert json.loads(result.stdout)["outcome"] == "locked"

    @pytest.mark.parametrize(("argv", "lane"), SCHEDULER_LANES)
    @pytest.mark.parametrize(
        ("error", "outcome", "code"),
        [
            (admin.AdminUpdateInProgress, "locked", 75),
            (admin.AdminMarkerPresent, "marker-present", 76),
            (admin.AdminPreconditionFailed, "precondition-failed", 77),
        ],
    )
    def test_every_operational_refusal_carries_its_outcome(
        self,
        scheduler_host: Path,
        monkeypatch: pytest.MonkeyPatch,
        argv: list[str],
        lane: str,
        error: type[admin.AdminError],
        outcome: str,
        code: int,
    ) -> None:
        monkeypatch.setattr(f"vq.cli.admin_module.{lane}", _raising(error, "refused"))

        result = CliRunner().invoke(main, [*argv, "--json"])

        assert result.exit_code == code, result.output
        assert json.loads(result.stdout)["outcome"] == outcome

    @pytest.mark.parametrize(("argv", "lane"), SCHEDULER_LANES)
    def test_the_text_form_names_the_outcome_too(
        self,
        scheduler_host: Path,
        monkeypatch: pytest.MonkeyPatch,
        argv: list[str],
        lane: str,
    ) -> None:
        monkeypatch.setattr(
            f"vq.cli.admin_module.{lane}",
            _raising(admin.AdminUpdateInProgress, "another operation owns the lock"),
        )

        result = CliRunner().invoke(main, argv)

        assert result.exit_code == 75, result.output
        assert "[locked]" in result.output

    @pytest.mark.parametrize(("argv", "lane"), SCHEDULER_LANES)
    def test_a_wrong_argv_is_still_a_usage_error(
        self,
        scheduler_host: Path,
        monkeypatch: pytest.MonkeyPatch,
        argv: list[str],
        lane: str,
    ) -> None:
        """A plain AdminError is an input error, as it is on the env lane."""
        monkeypatch.setattr(
            f"vq.cli.admin_module.{lane}", _raising(admin.AdminError, "no such thing"),
        )

        result = CliRunner().invoke(main, argv)

        assert result.exit_code == 2, result.output

    def _helper_result(self, rc: int) -> admin.SchedulerHostUpdateResult:
        return admin.SchedulerHostUpdateResult(
            host="host_f",
            ssh="host_f-login.example.invalid",
            scheduler="pbs",
            mode="update",
            command="/site/bin/update-helper",
            command_rc=rc,
        )

    def _runtime_result(self, rc: int) -> admin.SchedulerRuntimeUpdateResult:
        return admin.SchedulerRuntimeUpdateResult(
            host="host_f",
            program="vibeqc-release",
            mode="update",
            command="/site/bin/deploy-runtime",
            command_ssh="ssh cluster-build /site/bin/deploy-runtime",
            verify_command="/site/bin/verify-runtime",
            verify_ssh="ssh host_f-login.example.invalid /site/bin/verify-runtime",
            expected_sha=PIN,
            command_rc=rc,
            verify_rc=0,
            actual_sha=PIN,
            healthy=True,
            activation="atomic",
            quiescent=True,
        )

    @pytest.mark.parametrize(("argv", "lane"), SCHEDULER_LANES)
    def test_a_lane_that_ran_reports_ok_on_its_payload(
        self,
        scheduler_host: Path,
        monkeypatch: pytest.MonkeyPatch,
        argv: list[str],
        lane: str,
    ) -> None:
        """`--json` carries `outcome` on success too, as the env lane does."""
        made = self._helper_result(0) if lane == "update_scheduler_host" else (
            self._runtime_result(0)
        )
        monkeypatch.setattr(f"vq.cli.admin_module.{lane}", lambda *a, **k: made)

        result = CliRunner().invoke(main, [*argv, "--json"])

        assert result.exit_code == 0, result.output
        payload = json.loads(result.stdout)
        assert payload["outcome"] == "ok"
        assert payload["success"] is True

    @pytest.mark.parametrize(("argv", "lane"), SCHEDULER_LANES)
    def test_a_lane_that_failed_reports_failed_and_no_second_object(
        self,
        scheduler_host: Path,
        monkeypatch: pytest.MonkeyPatch,
        argv: list[str],
        lane: str,
    ) -> None:
        made = self._helper_result(1) if lane == "update_scheduler_host" else (
            self._runtime_result(1)
        )
        monkeypatch.setattr(f"vq.cli.admin_module.{lane}", lambda *a, **k: made)

        result = CliRunner().invoke(main, [*argv, "--json"])

        assert result.exit_code == 1, result.output
        payload = json.loads(result.stdout)  # one document, not two
        assert payload["outcome"] == "failed"
        assert payload["success"] is False


class TestAForwardedUpdateRelaysTheDriversOutcome:
    """`vq admin update host_f` off the driver delegates to the driver over SSH.

    The driver classified its refusal when it chose exit 75. Folding that into
    "remote vq failed (exit 75)" and exit 1 here would leave a laptop-driven
    sweep exactly where the helper lanes were: reading a retry as a stop.
    """

    def _driver_answers(
        self,
        monkeypatch: pytest.MonkeyPatch,
        *,
        returncode: int,
        stdout: str = "",
        stderr: str = "",
    ) -> list[list[str]]:
        calls: list[list[str]] = []

        def fake_run(cmd: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
            calls.append(cmd)
            return subprocess.CompletedProcess(cmd, returncode, stdout=stdout, stderr=stderr)

        monkeypatch.setattr(transport.subprocess, "run", fake_run)
        return calls

    @pytest.mark.parametrize(("argv", "_lane"), SCHEDULER_LANES)
    def test_json_relays_the_code_and_the_drivers_error(
        self,
        host: Path,
        monkeypatch: pytest.MonkeyPatch,
        argv: list[str],
        _lane: str,
    ) -> None:
        _write_scheduler_config(host, driver="driver")
        calls = self._driver_answers(
            monkeypatch,
            returncode=75,
            stdout=json.dumps({"outcome": "locked", "error": "the driver is busy"}) + "\n",
        )

        result = CliRunner().invoke(main, [*argv, "--json"])

        assert len(calls) == 1
        assert result.exit_code == 75, result.output
        payload = json.loads(result.stdout)
        assert payload["outcome"] == "locked"
        assert payload["error"] == "the driver is busy"

    def test_the_text_form_relays_the_code(
        self, host: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _write_scheduler_config(host, driver="driver")
        self._driver_answers(
            monkeypatch,
            returncode=76,
            stderr="Error [marker-present]: a previous run left its marker\n",
        )

        result = CliRunner().invoke(main, HELPER_LANE)

        assert result.exit_code == 76, result.output
        assert "[marker-present]" in result.output

    def test_the_env_lane_relays_too(
        self, host: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Every lane forwards through the same helper; the fix is not
        scheduler-specific."""
        (host / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
            "\n"
            "[hosts.localhost]\n"
            'ssh = "localhost"\n'
            "\n"
            "[hosts.remote]\n"
            'ssh = "remote.example.invalid"\n'
            "\n"
            "[programs.demo]\n"
            'kind = "venv"\n'
            'python = "/fake/python"\n'
            'git_dir = "/fake/repo"\n',
            encoding="utf-8",
        )
        self._driver_answers(
            monkeypatch,
            returncode=77,
            stdout=json.dumps(
                {"outcome": "precondition-failed", "error": "remote is not converged"}
            ),
        )

        result = CliRunner().invoke(main, ["admin", "update", "demo", "remote", "--json"])

        assert result.exit_code == 77, result.output
        assert json.loads(result.stdout)["outcome"] == "precondition-failed"

    def test_an_unclassified_remote_failure_is_unchanged(
        self, host: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Exit 2 on the driver is a usage error there. It is not an outcome
        and stays what it was here: exit 1 and the transport's message."""
        _write_scheduler_config(host, driver="driver")
        self._driver_answers(
            monkeypatch, returncode=2, stderr="remote validation rejected the request\n",
        )

        result = CliRunner().invoke(main, HELPER_LANE)

        assert result.exit_code == 1, result.output
        assert "remote vq failed (exit 2)" in result.output
        assert "remote validation rejected the request" in result.output

    def test_a_non_admin_verb_is_not_relayed(
        self, host: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The relay is the admin verbs' contract, not every delegated one.

        Nothing else in vq exits 75/76/77 today, so this asserts a boundary
        rather than a behaviour change: a verb that starts using one of those
        codes for its own reasons must not inherit an admin meaning.
        """
        _write_scheduler_config(host, driver="driver")
        self._driver_answers(monkeypatch, returncode=75, stderr="busy\n")

        result = CliRunner().invoke(main, ["queue", "driver"])

        assert result.exit_code == 1, result.output
        assert "remote vq failed (exit 75)" in result.output

    def test_only_the_unambiguous_codes_are_relayed(self) -> None:
        """0 is not a failure and 1 is click's exit for every unclassified
        error, so neither names an outcome. The rest map back exactly."""
        assert admin.admin_outcome_for_exit_code(0) is None
        assert admin.admin_outcome_for_exit_code(1) is None
        assert admin.admin_outcome_for_exit_code(2) is None
        relayed = {
            code: admin.admin_outcome_for_exit_code(code)
            for outcome, code in admin.ADMIN_OUTCOME_EXIT_CODES.items()
            if code not in (0, 1)
        }
        assert relayed == {75: "locked", 76: "marker-present", 77: "precondition-failed"}


class TestClearUpdateMarkerClassifiesALockToo:
    """The manual recovery step the contract points at takes the same lock."""

    def test_a_held_lock_is_locked(
        self, host: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        admin.acquire_admin_update_marker(envs=["demo"], host="localhost")
        admin.transition_admin_update_state(
            admin.ADMIN_UPDATE_STATE_FAILED, failure_reason="boom",
        )
        busy = _raising(
            admin.AdminUpdateInProgress, "another admin operation owns the checkout",
        )
        monkeypatch.setattr("vq.cli.admin_module.clear_admin_update_marker", busy)
        monkeypatch.setattr(
            "vq.cli.admin_module.recover_pause_scope_and_clear_marker", busy,
        )

        result = CliRunner().invoke(
            main, ["admin", "clear-update-marker", "localhost", "--json"],
        )

        assert result.exit_code == 75, result.output
        assert json.loads(result.stdout)["outcome"] == "locked"
