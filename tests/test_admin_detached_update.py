"""A dropped SSH session must cost a poll, never a build.

The 2026-09-11 release lane (`--tag v0.17.1 --expected-sha b4f6035e...`) lost
builds on host_b, host_e and host_d to dropped ssh sessions (#37). The remote
updater ran inside the session, and those hosts run systemd-logind with
`KillUserProcesses=yes`, which stops a session's scope -- and everything in it
-- when the last ssh session ends. The build was killed between the checkout
and the native rebuild, the atomic rollback never ran, and each host was left
at the new tag with a `building` marker, a dead pid, and a venv whose
`import vibeqc` failed on a half-built `libint2.so`.

The fix starts the updater outside the session and has it publish its own
progress and outcome. These tests hold that line from three ends:

* the scope-kill end -- where a systemd user manager answers, the updater is a
  transient user service unit (the remedy validated on host_e), started with
  the state and watchdogs it needs and never with a credential on argv or in
  its environment. That systemd really keeps it alive is checked on the real
  hosts, per #37: there is no logind where these tests run;
* the session end -- where no user manager answers, killing the process group
  that launched the build leaves it running, the marker honest, and
  `in_flight` true;
* the driver end -- a poll that fails is retried against the same run id, and
  only a run that dies without a receipt is still called unknown.
"""
from __future__ import annotations

import base64
import contextlib
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import admin, admin_detached, cli, config, paths, transport
from vq.cli import main

_ACTIVATION_TIMEOUT = 60.0

# The detached updater, standing in for a real rebuild. It runs through the
# production wrapper -- so it activates, ignores SIGHUP, takes the marker and
# publishes a terminal receipt exactly as `vq admin update` does -- but its
# "build" is a wait on a file, so the test controls when it finishes instead
# of waiting on a compiler.
_UPDATER_SOURCE = """
import os, sys, time
from vq import admin

run_id, ready, hold = sys.argv[1], sys.argv[2], sys.argv[3]


def work():
    admin.acquire_admin_update_marker(envs=["vibeqc-dev"], host="localhost")
    admin.transition_admin_update_state(admin.ADMIN_UPDATE_STATE_BUILDING)
    open(ready, "w").close()
    deadline = time.monotonic() + 120
    while not os.path.exists(hold) and time.monotonic() < deadline:
        time.sleep(0.02)
    print("BUILD COMPLETE")


raise SystemExit(admin.run_detached_update_child(run_id, work))
"""

# The sshd-attached half: it launches the updater and then keeps sitting
# there, which is what an ssh session does while a build runs. The test kills
# its process group to reproduce the transport drop.
_LAUNCHER_SOURCE = """
import json, sys, time
from vq import admin

run_id, ready, hold, updater = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
receipt = admin.launch_detached_update(
    run_id=run_id,
    target="vibeqc-dev",
    child_argv=[sys.executable, "-c", updater, run_id, ready, hold],
    token=None,
    mechanism="session",
)
sys.stdout.write(json.dumps(receipt) + "\\n")
sys.stdout.flush()
time.sleep(300)
"""

# The same pair for `vq admin auto-update`, driven through the verb itself
# rather than the admin helpers. The launcher runs the real CLI `--detach`
# branch; the only thing it swaps is the child's interpreter entry, so the
# child still receives the verb's own `--detach-child` argv and runs its real
# local path -- with the drift probe and the rebuild it would apply stood in
# by a marker, a BUILDING transition, and a wait on a file.
_AUTO_UPDATE_CHILD_SOURCE = """
import os, sys, time
from vq import admin, auto_update
from vq.cli import main

ready, hold, argv = sys.argv[1], sys.argv[2], sys.argv[3:]


def rebuild(env, cfg, *, host, dry_run=False, admin_token=None):
    admin.acquire_admin_update_marker(envs=[env], host="localhost")
    admin.transition_admin_update_state(admin.ADMIN_UPDATE_STATE_BUILDING)
    open(ready, "w").close()
    deadline = time.monotonic() + 120
    while not os.path.exists(hold) and time.monotonic() < deadline:
        time.sleep(0.02)
    return auto_update.AutoUpdateOutcome(
        decision=auto_update.AutoUpdateDecision(
            env_name=env,
            action="update",
            reason="newer tag available",
            current_tag="v0.17.0",
            target_tag="v0.17.1",
        ),
        update_result=admin.UpdateResult(
            env=env,
            git_dir="/fake/repo",
            branch="main",
            update_script=None,
            git_pull_rc=0,
        ),
    )


auto_update.auto_update_env = rebuild
main(argv, prog_name="vq")
"""

_AUTO_UPDATE_LAUNCHER_SOURCE = """
import contextlib, io, json, sys, time
from vq import admin, cli

# The session mechanism, deterministically: on a Linux runner with a user
# manager the verb would otherwise ask systemd-run for a real unit.
admin._detached_spawn_mechanism = lambda: admin.DETACHED_MECHANISM_SESSION

run_id, ready, hold, child = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
real_child_argv = cli._detached_auto_update_child_argv


def child_argv(*args, **kwargs):
    argv = real_child_argv(*args, **kwargs)
    assert argv[1:5] == ["-m", "vq", "admin", "auto-update"], argv
    return [sys.executable, "-c", child, ready, hold, *argv[3:]]


cli._detached_auto_update_child_argv = child_argv
captured = io.StringIO()
with contextlib.redirect_stdout(captured):
    cli.main(
        ["admin", "auto-update", "vibeqc-dev", "--detach",
         "--detach-run-id", run_id, "localhost"],
        prog_name="vq",
        standalone_mode=False,
    )
sys.stdout.write(json.dumps(json.loads(captured.getvalue())) + "\\n")
sys.stdout.flush()
time.sleep(300)
"""


def _wait_for(predicate, *, timeout: float = _ACTIVATION_TIMEOUT, what: str) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError(f"timed out waiting for {what}")


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


@pytest.fixture
def state_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A config with one venv program, enough for marker and status work."""
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    monkeypatch.delenv(cli._ADMIN_NO_DETACH_ENV, raising=False)
    (tmp_path / "cfg").mkdir()
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    paths.jobs_dir().mkdir(parents=True, exist_ok=True)
    repo = tmp_path / "repo"
    (repo / ".git").mkdir(parents=True)
    (tmp_path / "cfg" / "config.toml").write_text(
        'default_host = "localhost"\n'
        "\n"
        "[hosts.localhost]\n"
        'ssh = "localhost"\n'
        "\n"
        "[hosts.host_b]\n"
        'ssh = "host_b"\n'
        'remote_vq = "/opt/vq/bin/vq"\n'
        "\n"
        "[programs.vibeqc-dev]\n"
        'kind = "venv"\n'
        'python = "/fake/python"\n'
        f'git_dir = "{repo}"\n'
        'branch = "main"\n'
    )
    return tmp_path


# ----------------------------------------------------------------------
# The host end: the build outlives the session that started it.
# ----------------------------------------------------------------------


class TestTheBuildOutlivesItsSession:
    def test_killing_the_launching_process_group_leaves_the_build_running(
        self, state_dir: Path, tmp_path: Path
    ) -> None:
        """Killing the launching process group leaves the build running.

        This is the session mechanism: the launcher stands in for an ssh
        session's process group and is killed outright. It is *not* the
        2026-09-11 kill, which was logind stopping a whole session scope; a new
        session does not escape that, and the user-unit tests below cover it.
        This is the guarantee on every host where no user manager answers.
        """
        run_id = admin_detached.new_run_id()
        ready = tmp_path / "building"
        hold = tmp_path / "finish"
        launcher = subprocess.Popen(
            [
                sys.executable,
                "-c",
                _LAUNCHER_SOURCE,
                run_id,
                str(ready),
                str(hold),
                _UPDATER_SOURCE,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
            text=True,
        )
        updater_pid: int | None = None
        try:
            assert launcher.stdout is not None
            receipt_line = launcher.stdout.readline()
            assert receipt_line, (
                "launcher produced no activation receipt: "
                f"{(launcher.stderr.read() if launcher.stderr else '')}"
            )
            receipt = json.loads(receipt_line)
            updater_pid = receipt["pid"]
            assert isinstance(updater_pid, int)
            assert receipt["state"] == admin_detached.STATE_RUNNING

            _wait_for(ready.exists, what="the updater to reach BUILDING")

            # The transport drops: sshd tears down the session's process
            # group. Before the fix this signal reached the build.
            os.killpg(os.getpgid(launcher.pid), signal.SIGKILL)
            launcher.wait(timeout=30)

            # The build is untouched, and every surface agrees it is running.
            assert _pid_alive(updater_pid), (
                "the detached updater died with the session that launched it"
            )
            marker = admin.read_admin_update_marker()
            assert marker is not None
            assert marker.state == admin.ADMIN_UPDATE_STATE_BUILDING
            assert marker.pid == updater_pid
            assert marker.detached_run_id == run_id
            assert admin.admin_update_in_flight() is True
            observed = admin_detached.observe(run_id)
            assert observed.state == admin_detached.STATE_RUNNING
            assert observed.pid == updater_pid

            # And it still finishes, publishing the outcome the driver reads.
            hold.write_text("go\n")
            _wait_for(
                lambda: admin_detached.observe(run_id).state
                == admin_detached.STATE_COMPLETED,
                what="the detached updater to publish its receipt",
            )
            final = admin_detached.observe(run_id)
            assert final.outcome == admin.OUTCOME_OK
            assert final.exit_code == 0
            assert "BUILD COMPLETE" in (final.payload or "")
        finally:
            for pid in (launcher.pid, updater_pid):
                if pid is None:
                    continue
                with contextlib.suppress(ProcessLookupError, PermissionError):
                    os.kill(pid, signal.SIGKILL)
            launcher.poll()

    def test_killing_the_session_that_launched_an_auto_update_leaves_it_running(
        self, state_dir: Path, tmp_path: Path
    ) -> None:
        """THE POINT, for `vq admin auto-update`.

        A delegated auto-update applies the same rebuild through its own verb,
        so the 2026-09-11 kill reached it just the same. Here the launcher is
        that verb's `--detach` branch and the updater its `--detach-child`
        branch; killing the launcher's process group must leave the rebuild
        running, the marker honest, and the receipt still published.
        """
        run_id = admin_detached.new_run_id()
        ready = tmp_path / "building"
        hold = tmp_path / "finish"
        launcher = subprocess.Popen(
            [
                sys.executable,
                "-c",
                _AUTO_UPDATE_LAUNCHER_SOURCE,
                run_id,
                str(ready),
                str(hold),
                _AUTO_UPDATE_CHILD_SOURCE,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
            text=True,
        )
        updater_pid: int | None = None
        try:
            assert launcher.stdout is not None
            receipt_line = launcher.stdout.readline()
            assert receipt_line, (
                "auto-update launcher produced no activation receipt: "
                f"{(launcher.stderr.read() if launcher.stderr else '')}"
            )
            receipt = json.loads(receipt_line)
            assert receipt["run_id"] == run_id
            assert receipt["target"] == "vibeqc-dev"
            assert receipt["state"] == admin_detached.STATE_RUNNING
            updater_pid = receipt["pid"]
            assert isinstance(updater_pid, int)

            _wait_for(ready.exists, what="the auto-update to reach BUILDING")

            os.killpg(os.getpgid(launcher.pid), signal.SIGKILL)
            launcher.wait(timeout=30)

            assert _pid_alive(updater_pid), (
                "the detached auto-update died with the session that launched it"
            )
            marker = admin.read_admin_update_marker()
            assert marker is not None
            assert marker.state == admin.ADMIN_UPDATE_STATE_BUILDING
            assert marker.pid == updater_pid
            assert marker.detached_run_id == run_id
            assert admin.admin_update_in_flight() is True
            assert admin_detached.observe(run_id).state == admin_detached.STATE_RUNNING
            launch = json.loads(
                (admin_detached.detached_run_dir(run_id) / "launch.json").read_text()
            )
            assert "auto-update" in launch["argv"]
            assert "--detach-child" in launch["argv"]

            hold.write_text("go\n")
            _wait_for(
                lambda: admin_detached.observe(run_id).state
                == admin_detached.STATE_COMPLETED,
                what="the detached auto-update to publish its receipt",
            )
            final = admin_detached.observe(run_id)
            assert final.outcome == admin.OUTCOME_OK
            assert final.exit_code == 0
            # The receipt carries the report the attached verb would print.
            assert "action:       update" in (final.payload or "")
            assert "apply:        OK" in (final.payload or "")
        finally:
            for pid in (launcher.pid, updater_pid):
                if pid is None:
                    continue
                with contextlib.suppress(ProcessLookupError, PermissionError):
                    os.kill(pid, signal.SIGKILL)
            launcher.poll()

    def test_admin_status_json_reports_a_detached_build_as_in_flight(
        self, state_dir: Path, tmp_path: Path
    ) -> None:
        """`in_flight` must be true for a build whose launcher is long gone."""
        run_id = admin_detached.new_run_id()
        ready = tmp_path / "building"
        hold = tmp_path / "finish"
        updater = subprocess.Popen(
            [sys.executable, "-c", _UPDATER_SOURCE, run_id, str(ready), str(hold)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        try:
            _wait_for(ready.exists, what="the updater to reach BUILDING")
            result = CliRunner().invoke(main, ["admin", "status", "--json"])
            assert result.exit_code == 0, result.output
            payload = json.loads(result.stdout)
            assert payload["in_flight"] is True
            assert payload["marker"]["detached_run_id"] == run_id
            assert payload["marker"]["marker_status"] == "running"
        finally:
            hold.write_text("go\n")
            try:
                updater.wait(timeout=30)
            except subprocess.TimeoutExpired:
                updater.kill()

    def test_observe_update_reads_back_the_run_on_the_host(
        self, state_dir: Path, tmp_path: Path
    ) -> None:
        run_id = admin_detached.new_run_id()
        admin_detached.write_launch(run_id, target="vibeqc-dev", argv=["x"])
        admin_detached.write_activation(
            run_id, pid=os.getpid(), pid_start_time=admin._pid_start_time(os.getpid()) or 0
        )
        result = CliRunner().invoke(main, ["admin", "observe-update", run_id])
        assert result.exit_code == 0, result.output
        assert run_id in result.stdout
        assert admin_detached.STATE_RUNNING in result.stdout


# ----------------------------------------------------------------------
# The run store: states are exact, and a lost run says so.
# ----------------------------------------------------------------------


class TestRunStore:
    def test_states_walk_launch_to_activation_to_result(
        self, state_dir: Path
    ) -> None:
        run_id = admin_detached.new_run_id()
        assert admin_detached.observe(run_id).state == admin_detached.STATE_MISSING

        admin_detached.write_launch(run_id, target="vibeqc-dev", argv=["vq"])
        assert admin_detached.observe(run_id).state == admin_detached.STATE_LAUNCHING

        admin_detached.write_activation(
            run_id,
            pid=os.getpid(),
            pid_start_time=admin._pid_start_time(os.getpid()) or 0,
        )
        assert admin_detached.observe(run_id).state == admin_detached.STATE_RUNNING

        admin_detached.write_result(
            run_id, outcome=admin.OUTCOME_OK, exit_code=0, payload="done\n"
        )
        final = admin_detached.observe(run_id)
        assert final.state == admin_detached.STATE_COMPLETED
        assert final.payload == "done\n"

    def test_an_activated_run_whose_process_is_gone_is_lost(
        self, state_dir: Path
    ) -> None:
        """The one case that is still genuinely unknown, named as such."""
        run_id = admin_detached.new_run_id()
        admin_detached.write_launch(run_id, target="vibeqc-dev", argv=["vq"])
        dead = subprocess.Popen([sys.executable, "-c", "raise SystemExit(0)"])
        dead.wait(timeout=30)
        admin_detached.write_activation(run_id, pid=dead.pid, pid_start_time=0)
        assert admin_detached.observe(run_id).state == admin_detached.STATE_LOST

    def test_what_the_host_emits_is_what_the_driver_parses(
        self, state_dir: Path
    ) -> None:
        """The two halves of the protocol meet here, so pin the round trip.

        The host serialises an observation and the driver reconstructs it over
        SSH. A field renamed on one side alone would otherwise surface as a
        driver that quietly stops seeing terminal receipts.
        """
        run_id = admin_detached.new_run_id()
        admin_detached.write_launch(run_id, target="vibeqc-dev", argv=["vq"])
        admin_detached.write_activation(
            run_id,
            pid=os.getpid(),
            pid_start_time=admin._pid_start_time(os.getpid()) or 0,
        )
        admin_detached.write_result(
            run_id,
            outcome=admin.OUTCOME_PRECONDITION_FAILED,
            exit_code=77,
            payload='{"outcome": "precondition-failed"}\n',
            error="not converged",
        )
        emitted = admin_detached.observe(run_id)
        # Exactly the trip the wire makes: dict -> JSON text -> dict -> object.
        parsed = admin_detached.parse_observation(json.loads(json.dumps(emitted.to_json())))
        assert parsed == emitted
        assert parsed.terminal
        assert parsed.exit_code == 77
        assert parsed.outcome == admin.OUTCOME_PRECONDITION_FAILED

    def test_an_observation_of_an_unknown_schema_is_refused(
        self, state_dir: Path
    ) -> None:
        """A driver that accepted a loose shape could read junk as terminal."""
        run_id = admin_detached.new_run_id()
        admin_detached.write_launch(run_id, target="vibeqc-dev", argv=["vq"])
        payload = admin_detached.observe(run_id).to_json()
        for mutate in (
            lambda p: p.update(schema="vq.admin.something_else/9"),
            lambda p: p.update(state="halfway"),
            lambda p: p.pop("run_id"),
            lambda p: p.update(state=admin_detached.STATE_COMPLETED, exit_code=None),
        ):
            broken = dict(payload)
            mutate(broken)
            with pytest.raises(admin_detached.DetachedRunError):
                admin_detached.parse_observation(broken)

    def test_a_run_id_that_is_not_32_hex_is_refused(self, state_dir: Path) -> None:
        """The run id selects a directory, so it is a containment check."""
        for bad in ("../../etc", "", "XYZ", "a" * 31, "a" * 33):
            with pytest.raises(admin_detached.DetachedRunError):
                admin_detached.observe(bad)

    def test_a_transcript_outside_the_log_dir_is_not_served(
        self, state_dir: Path, tmp_path: Path
    ) -> None:
        secret = tmp_path / "secret.txt"
        secret.write_text("not a transcript\n")
        run_id = admin_detached.new_run_id()
        admin_detached.write_launch(run_id, target="vibeqc-dev", argv=["vq"])
        admin_detached.write_activation(
            run_id,
            pid=os.getpid(),
            pid_start_time=admin._pid_start_time(os.getpid()) or 0,
        )
        admin_detached.publish_transcript(run_id, secret)
        observed = admin_detached.observe(run_id)
        assert observed.transcript_base64 == ""
        assert observed.transcript_size == 0

    def test_opening_the_run_log_publishes_its_path_to_the_run(
        self, state_dir: Path
    ) -> None:
        """The driver cannot follow a build whose transcript it cannot name."""
        run_id = admin_detached.new_run_id()
        admin_detached.write_launch(run_id, target="vibeqc-dev", argv=["vq"])
        admin_detached.write_activation(
            run_id,
            pid=os.getpid(),
            pid_start_time=admin._pid_start_time(os.getpid()) or 0,
        )
        assert admin_detached.observe(run_id).transcript is None

        admin.set_detached_run_id(run_id)
        try:
            with admin.admin_run_log("vibeqc-dev", what="admin update") as run_log:
                run_log.stamp("[building]")
                published = admin_detached.observe(run_id).transcript
                assert published == str(run_log.path)
        finally:
            admin.set_detached_run_id(None)

        observed = admin_detached.observe(run_id)
        assert base64.b64decode(observed.transcript_base64).decode().count(
            "[building]"
        ) == 1

    def test_an_attached_update_publishes_no_run_record(
        self, state_dir: Path
    ) -> None:
        """Detaching is opt-in; a local update must not grow a run store."""
        with admin.admin_run_log("vibeqc-dev", what="admin update") as run_log:
            run_log.stamp("[building]")
        assert not admin_detached.detached_run_root().exists()

    def test_a_child_that_crashes_still_publishes_a_receipt(
        self, state_dir: Path
    ) -> None:
        """A run without a receipt is the ambiguity this replaces."""
        run_id = admin_detached.new_run_id()
        admin_detached.write_launch(run_id, target="vibeqc-dev", argv=["vq"])

        def work() -> None:
            print("partial output")
            raise RuntimeError("the build helper exploded")

        try:
            exit_code = admin.run_detached_update_child(run_id, work)
        finally:
            admin.set_detached_run_id(None)

        assert exit_code == 1
        observed = admin_detached.observe(run_id)
        assert observed.state == admin_detached.STATE_COMPLETED
        assert observed.outcome == admin.OUTCOME_FAILED
        assert observed.exit_code == 1
        assert "the build helper exploded" in (observed.error or "")
        assert "partial output" in (observed.payload or "")

    def test_a_live_run_is_never_pruned(self, state_dir: Path) -> None:
        live = admin_detached.new_run_id()
        admin_detached.write_launch(live, target="vibeqc-dev", argv=["vq"])
        admin_detached.write_activation(
            live,
            pid=os.getpid(),
            pid_start_time=admin._pid_start_time(os.getpid()) or 0,
        )
        finished = []
        for _ in range(3):
            run_id = admin_detached.new_run_id()
            admin_detached.write_launch(run_id, target="vibeqc-dev", argv=["vq"])
            admin_detached.write_result(
                run_id, outcome=admin.OUTCOME_OK, exit_code=0, payload=""
            )
            finished.append(run_id)
        admin_detached.prune_detached_runs(keep=1)
        assert admin_detached.observe(live).state == admin_detached.STATE_RUNNING


# ----------------------------------------------------------------------
# The driver end: a dropped poll costs a poll.
# ----------------------------------------------------------------------


def _observation_proc(payload: dict[str, object]) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(
        args=["ssh"], returncode=0, stdout=json.dumps(payload), stderr=""
    )


def _observation(
    state: str,
    *,
    run_id: str,
    transcript: bytes = b"",
    offset: int = 0,
    outcome: str | None = None,
    exit_code: int | None = None,
    payload: str | None = None,
    error: str | None = None,
) -> dict[str, object]:
    return {
        "schema": admin_detached.DETACHED_OBSERVATION_SCHEMA,
        "run_id": run_id,
        "state": state,
        "detail": f"stub {state}",
        "target": "vibeqc-dev",
        "pid": 4242,
        "transcript": "/state/admin-updates/vibeqc-dev/x.log",
        "transcript_offset": offset,
        "transcript_next_offset": offset + len(transcript),
        "transcript_size": offset + len(transcript),
        "transcript_base64": base64.b64encode(transcript).decode("ascii"),
        "outcome": outcome,
        "exit_code": exit_code,
        "payload": payload,
        "error": error,
    }


@pytest.fixture
def no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli.time, "sleep", lambda _seconds: None)


class TestDriverPolling:
    def test_a_dropped_poll_is_retried_against_the_same_run(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch, no_sleep: None
    ) -> None:
        """THE POINT, from the driver's side. A drop costs one poll."""
        run_id = "b" * 32
        polls: list[object] = [
            transport.RemoteOutcomeUnknown("kex_exchange_identification: reset"),
            _observation_proc(
                _observation(
                    admin_detached.STATE_RUNNING, run_id=run_id, transcript=b"x" * 4
                )
            ),
            transport.RemoteError("ssh: connect to host host_b port 22: timed out"),
            _observation_proc(
                _observation(
                    admin_detached.STATE_COMPLETED,
                    run_id=run_id,
                    offset=4,
                    outcome=admin.OUTCOME_OK,
                    exit_code=0,
                    payload="REMOTE-OK\n",
                )
            ),
        ]
        calls: list[tuple[str, ...]] = []

        def fake(host_cfg, *vq_args, **kwargs):  # type: ignore[no-untyped-def]
            calls.append(tuple(vq_args))
            nxt = polls.pop(0)
            if isinstance(nxt, BaseException):
                raise nxt
            return nxt

        monkeypatch.setattr(transport, "run_remote_vq", fake)
        runner = CliRunner()
        with runner.isolation() as (stdout, _stderr, _):
            payload = cli._poll_detached_admin_update(
                config.HostConfig(ssh="host_b", remote_vq="/opt/vq/bin/vq"),
                run_id=run_id,
                target="host_b",
                adopted=False,
            )
        # Returned, not printed: the per-host fan-out has to compose these
        # into one document.
        assert payload == "REMOTE-OK\n"
        assert stdout.getvalue().decode() == ""
        assert not polls, "every stubbed poll should have been consumed"
        # Every poll names the run AND the host it lives on: a target whose own
        # default_host is elsewhere must not forward the observation there.
        for call in calls:
            assert call[:3] == ("admin", "observe-update", run_id)
            assert call[call.index("--host") + 1] == "localhost"

    def test_an_outage_past_the_grace_window_is_reported_unknown(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Both windows, or the poll spins out the one that was left alone:
        # an unconfirmed run uses the short grace, a confirmed one the long.
        monkeypatch.setattr(cli, "_DETACHED_OBSERVATION_GRACE_SECONDS", 0.0)
        monkeypatch.setattr(cli, "_DETACHED_UNCONFIRMED_GRACE_SECONDS", 0.0)
        monkeypatch.setattr(cli.time, "sleep", lambda _seconds: None)
        monkeypatch.setattr(
            transport,
            "run_remote_vq",
            lambda *a, **k: (_ for _ in ()).throw(
                transport.RemoteError("host unreachable")
            ),
        )
        with pytest.raises(cli.click.ClickException) as excinfo:
            cli._poll_detached_admin_update(
                config.HostConfig(ssh="host_b", remote_vq="/opt/vq/bin/vq"),
                run_id="c" * 32,
                target="host_b",
                adopted=False,
            )
        message = excinfo.value.format_message()
        assert "Remote admin outcome is unknown" in message
        assert "Do not retry it yet" in message
        assert "vq admin status host_b --json" in message

    def test_a_run_that_died_without_a_receipt_is_reported_unknown(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch, no_sleep: None
    ) -> None:
        """Detaching narrows the unknown case; it does not pretend it is gone."""
        run_id = "d" * 32
        monkeypatch.setattr(
            transport,
            "run_remote_vq",
            lambda *a, **k: _observation_proc(
                _observation(admin_detached.STATE_LOST, run_id=run_id)
            ),
        )
        with pytest.raises(cli.click.ClickException) as excinfo:
            cli._poll_detached_admin_update(
                config.HostConfig(ssh="host_b", remote_vq="/opt/vq/bin/vq"),
                run_id=run_id,
                target="host_b",
                adopted=False,
            )
        message = excinfo.value.format_message()
        assert "Remote admin outcome is unknown" in message
        assert "Do not retry it yet" in message
        assert "/state/admin-updates/vibeqc-dev/x.log" in message

    def test_a_run_that_vanishes_while_followed_ends_rather_than_hangs(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch, no_sleep: None
    ) -> None:
        """A host answering "no such run" is terminal, however we got here.

        Every other state either advances or is reported. A `missing` that
        matched no branch would leave the driver polling forever on a build
        that may well have finished -- the opposite of the problem detaching
        was introduced to solve.
        """
        run_id = "0" * 32
        polls = [
            _observation_proc(_observation(admin_detached.STATE_RUNNING, run_id=run_id)),
            _observation_proc(_observation(admin_detached.STATE_MISSING, run_id=run_id)),
        ]
        monkeypatch.setattr(
            transport, "run_remote_vq", lambda *a, **k: polls.pop(0)
        )
        with pytest.raises(cli.click.ClickException) as excinfo:
            cli._poll_detached_admin_update(
                config.HostConfig(ssh="host_b", remote_vq="/opt/vq/bin/vq"),
                run_id=run_id,
                target="host_b",
                adopted=False,
            )
        message = excinfo.value.format_message()
        assert "disappeared" in message
        assert "Remote admin outcome is unknown" in message
        assert not polls

    def test_a_launch_that_recorded_no_run_is_reported_not_retried(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch, no_sleep: None
    ) -> None:
        monkeypatch.setattr(cli, "_DETACHED_UNCONFIRMED_GRACE_SECONDS", 0.0)
        run_id = "1" * 32
        monkeypatch.setattr(
            transport,
            "run_remote_vq",
            lambda *a, **k: _observation_proc(
                _observation(admin_detached.STATE_MISSING, run_id=run_id)
            ),
        )
        with pytest.raises(cli.click.ClickException) as excinfo:
            cli._poll_detached_admin_update(
                config.HostConfig(ssh="host_b", remote_vq="/opt/vq/bin/vq"),
                run_id=run_id,
                target="host_b",
                adopted=True,
            )
        message = excinfo.value.format_message()
        assert "never recorded a run" in message
        assert "Do not retry it yet" in message

    def test_a_failed_run_keeps_its_exact_outcome_and_payload(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch, no_sleep: None
    ) -> None:
        run_id = "e" * 32
        monkeypatch.setattr(
            transport,
            "run_remote_vq",
            lambda *a, **k: _observation_proc(
                _observation(
                    admin_detached.STATE_COMPLETED,
                    run_id=run_id,
                    outcome=admin.OUTCOME_PRECONDITION_FAILED,
                    exit_code=77,
                    payload='{"outcome": "precondition-failed"}\n',
                    error="host is not converged",
                )
            ),
        )
        runner = CliRunner()
        with runner.isolation() as (stdout, _stderr, _), pytest.raises(
            cli.AdminOutcomeError
        ) as excinfo:
            cli._poll_detached_admin_update(
                config.HostConfig(ssh="host_b", remote_vq="/opt/vq/bin/vq"),
                run_id=run_id,
                target="host_b",
                adopted=False,
            )
        assert excinfo.value.outcome == admin.OUTCOME_PRECONDITION_FAILED
        assert excinfo.value.exit_code == 77
        # Carried, not printed: only the caller that owns stdout may write it.
        assert isinstance(excinfo.value, cli._DetachedRunFailed)
        assert excinfo.value.payload == '{"outcome": "precondition-failed"}\n'
        assert stdout.getvalue().decode() == ""

    def test_only_narration_reaches_the_terminal_not_the_build_stream(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch, no_sleep: None
    ) -> None:
        """The attached command showed phases, never raw compiler output."""
        run_id = "f" * 32
        transcript = (
            b"2026-09-12T10:11:12+00:00  [building]\n"
            b"[1/900] Building CXX object libint2.cc.o\n"
            b"2026-09-12T10:19:00+00:00  build-env repo: still running\n"
        )
        polls = [
            _observation_proc(
                _observation(
                    admin_detached.STATE_RUNNING, run_id=run_id, transcript=transcript
                )
            ),
            _observation_proc(
                _observation(
                    admin_detached.STATE_COMPLETED,
                    run_id=run_id,
                    offset=len(transcript),
                    outcome=admin.OUTCOME_OK,
                    exit_code=0,
                    payload="",
                )
            ),
        ]
        monkeypatch.setattr(
            transport, "run_remote_vq", lambda *a, **k: polls.pop(0)
        )
        runner = CliRunner()
        with runner.isolation() as (_stdout, stderr, _):
            cli._poll_detached_admin_update(
                config.HostConfig(ssh="host_b", remote_vq="/opt/vq/bin/vq"),
                run_id=run_id,
                target="host_b",
                adopted=False,
            )
        narrated = stderr.getvalue().decode()
        assert "[building]" in narrated
        assert "still running" in narrated
        assert "Building CXX object" not in narrated


# ----------------------------------------------------------------------
# The delegated command: shape, adoption, and the old-remote fallback.
# ----------------------------------------------------------------------


class TestDelegation:
    def _capture(
        self, monkeypatch: pytest.MonkeyPatch, run_id_box: list[str]
    ) -> list[tuple[str, ...]]:
        calls: list[tuple[str, ...]] = []

        def fake(host_cfg, *vq_args, **kwargs):  # type: ignore[no-untyped-def]
            calls.append(tuple(vq_args))
            if vq_args[:2] == ("admin", "update"):
                run_id_box.append(vq_args[vq_args.index("--detach-run-id") + 1])
                return subprocess.CompletedProcess(
                    args=["ssh"], returncode=0, stdout="{}", stderr=""
                )
            return _observation_proc(
                _observation(
                    admin_detached.STATE_COMPLETED,
                    run_id=run_id_box[-1],
                    outcome=admin.OUTCOME_OK,
                    exit_code=0,
                    payload="REMOTE-VENV\n",
                )
            )

        monkeypatch.setattr(transport, "run_remote_vq", fake)
        return calls

    def test_a_delegated_update_launches_detached_and_polls(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch, no_sleep: None
    ) -> None:
        run_id_box: list[str] = []
        calls = self._capture(monkeypatch, run_id_box)
        result = CliRunner().invoke(
            main, ["admin", "update", "vibeqc-dev", "host_b", "--json"]
        )
        assert result.exit_code == 0, result.output
        assert result.stdout == "REMOTE-VENV\n"
        launch = calls[0]
        assert launch[:3] == ("admin", "update", "vibeqc-dev")
        assert "--detach" in launch
        assert launch[-1] == "localhost"
        assert admin_detached.validate_run_id(run_id_box[0])
        assert calls[1][:3] == ("admin", "observe-update", run_id_box[0])

    def test_observing_a_remote_run_names_the_host_it_lives_on(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """`--host H` observes H, whatever H's own `default_host` says (#57).

        The first hop honours the caller. Without an explicit destination the
        target's own CLI resolves *its* `default_host` and delegates a second
        time, so a run that completed on H reads back as `missing` from a
        machine that never heard of it -- a wrong-host read wearing the mask
        of a lost receipt. The driver's own poller already says `localhost`
        for this reason; the command has to say it too.
        """
        calls: list[tuple[str, ...]] = []

        def fake(host_cfg, *vq_args, **kwargs):  # type: ignore[no-untyped-def]
            calls.append(tuple(vq_args))
            return subprocess.CompletedProcess(
                args=["ssh"], returncode=0, stdout="{}\n", stderr=""
            )

        monkeypatch.setattr(transport, "run_remote_vq", fake)
        run_id = admin_detached.new_run_id()
        result = CliRunner().invoke(
            main,
            [
                "admin",
                "observe-update",
                run_id,
                "--host",
                "host_b",
                "--offset",
                "4096",
                "--max-bytes",
                "8192",
                "--json",
            ],
        )

        assert result.exit_code == 0, result.output
        assert len(calls) == 1, calls
        forwarded = calls[0]
        assert forwarded[:3] == ("admin", "observe-update", run_id)
        assert forwarded[forwarded.index("--host") + 1] == "localhost"
        # The rest of the read travels unchanged: a follower's offset window
        # and its JSON shape are the caller's, not the target default's.
        assert forwarded[forwarded.index("--offset") + 1] == "4096"
        assert forwarded[forwarded.index("--max-bytes") + 1] == "8192"
        assert "--json" in forwarded

    def test_a_lost_launch_response_is_adopted_by_run_id(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch, no_sleep: None
    ) -> None:
        """The run id is chosen here, so a missing response is not unknown."""
        seen: list[str] = []

        def fake(host_cfg, *vq_args, **kwargs):  # type: ignore[no-untyped-def]
            if vq_args[:2] == ("admin", "update"):
                seen.append(vq_args[vq_args.index("--detach-run-id") + 1])
                raise transport.RemoteOutcomeUnknown("remote vq failed (exit 255)")
            assert vq_args[2] == seen[0]
            return _observation_proc(
                _observation(
                    admin_detached.STATE_COMPLETED,
                    run_id=seen[0],
                    outcome=admin.OUTCOME_OK,
                    exit_code=0,
                    payload="ADOPTED\n",
                )
            )

        monkeypatch.setattr(transport, "run_remote_vq", fake)
        result = CliRunner().invoke(main, ["admin", "update", "vibeqc-dev", "host_b"])
        assert result.exit_code == 0, result.output
        assert result.stdout == "ADOPTED\n"
        assert "adopting run" in result.stderr

    def test_a_remote_too_old_to_detach_falls_back_to_the_attached_path(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch, no_sleep: None
    ) -> None:
        """The tool that performs updates cannot require the update first."""
        calls: list[tuple[str, ...]] = []

        def fake(host_cfg, *vq_args, **kwargs):  # type: ignore[no-untyped-def]
            calls.append(tuple(vq_args))
            if "--detach" in vq_args:
                raise transport.RemoteCommandError(
                    "remote vq failed (exit 2)",
                    returncode=2,
                    stderr="Error: No such option: --detach",
                )
            return subprocess.CompletedProcess(
                args=["ssh"], returncode=0, stdout="LEGACY-OK\n", stderr=""
            )

        monkeypatch.setattr(transport, "run_remote_vq", fake)
        result = CliRunner().invoke(main, ["admin", "update", "vibeqc-dev", "host_b"])
        assert result.exit_code == 0, result.output
        assert result.stdout == "LEGACY-OK\n"
        assert "does not support detached updates" in result.stderr
        assert "--detach" not in calls[1]

    def test_a_real_remote_usage_error_is_not_mistaken_for_an_old_remote(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch, no_sleep: None
    ) -> None:
        """Only "no such option: --detach" may be retried as something else."""
        attempts: list[tuple[str, ...]] = []

        def fake(host_cfg, *vq_args, **kwargs):  # type: ignore[no-untyped-def]
            attempts.append(tuple(vq_args))
            raise transport.RemoteCommandError(
                "remote vq failed (exit 2) on host_b:\n"
                "  stderr: Error: unknown env 'vibeqc-dev'",
                returncode=2,
                stderr="Error: unknown env 'vibeqc-dev'",
            )

        monkeypatch.setattr(transport, "run_remote_vq", fake)
        result = CliRunner().invoke(main, ["admin", "update", "vibeqc-dev", "host_b"])
        assert result.exit_code == 1
        assert len(attempts) == 1, "a real answer must not be retried"
        assert "unknown env" in result.stderr
        assert "Remote admin outcome is unknown" not in result.stderr

    def test_a_single_host_failure_prints_its_result_then_classifies(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch, no_sleep: None
    ) -> None:
        """One host owns stdout: the result document, then the exit code."""
        payload = '{"outcome": "precondition-failed"}\n'
        seen: list[str] = []

        def fake(host_cfg, *vq_args, **kwargs):  # type: ignore[no-untyped-def]
            if vq_args[:2] == ("admin", "update"):
                seen.append(vq_args[vq_args.index("--detach-run-id") + 1])
                return subprocess.CompletedProcess(
                    args=["ssh"], returncode=0, stdout="{}", stderr=""
                )
            return _observation_proc(
                _observation(
                    admin_detached.STATE_COMPLETED,
                    run_id=seen[0],
                    outcome=admin.OUTCOME_PRECONDITION_FAILED,
                    exit_code=77,
                    payload=payload,
                    error="host is not converged",
                )
            )

        monkeypatch.setattr(transport, "run_remote_vq", fake)
        result = CliRunner().invoke(
            main, ["admin", "update", "vibeqc-dev", "host_b", "--json"]
        )
        assert result.exit_code == 77, result.output
        assert result.stdout == payload

    def test_an_all_hosts_json_failure_stays_inside_the_one_document(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch, no_sleep: None
    ) -> None:
        """A failing host must not print its result outside the aggregate."""
        (state_dir / "cfg" / "config.toml").write_text(
            "[hosts.alpha]\n"
            'ssh = "alpha"\n'
            "\n"
            "[hosts.beta]\n"
            'ssh = "beta"\n'
            "\n"
            "[programs.vibeqc-dev]\n"
            'kind = "venv"\n'
            'python = "/fake/python"\n'
            'git_dir = "/fake/repo"\n'
            'branch = "main"\n'
        )

        def fake(host_cfg, *vq_args, **kwargs):  # type: ignore[no-untyped-def]
            if vq_args[:2] == ("admin", "update"):
                return subprocess.CompletedProcess(
                    args=["ssh"], returncode=0, stdout="{}", stderr=""
                )
            failing = host_cfg.ssh == "beta"
            return _observation_proc(
                _observation(
                    admin_detached.STATE_COMPLETED,
                    run_id=vq_args[2],
                    outcome=(
                        admin.OUTCOME_PRECONDITION_FAILED if failing else admin.OUTCOME_OK
                    ),
                    exit_code=77 if failing else 0,
                    payload=json.dumps({"host": host_cfg.ssh, "ok": not failing}) + "\n",
                    error="host is not converged" if failing else None,
                )
            )

        monkeypatch.setattr(transport, "run_remote_vq", fake)
        result = CliRunner().invoke(
            main,
            ["admin", "update", "vibeqc-dev", "--all-hosts", "--serial", "--json"],
        )
        assert result.exit_code != 0
        document = json.loads(result.stdout)
        assert document["alpha"] == {"host": "alpha", "ok": True}
        assert "not converged" in document["beta"]["error"]

    def test_the_escape_hatch_restores_the_attached_delegation(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(cli._ADMIN_NO_DETACH_ENV, "1")
        calls: list[tuple[str, ...]] = []

        def fake(host_cfg, *vq_args, **kwargs):  # type: ignore[no-untyped-def]
            calls.append(tuple(vq_args))
            return subprocess.CompletedProcess(
                args=["ssh"], returncode=0, stdout="ATTACHED\n", stderr=""
            )

        monkeypatch.setattr(transport, "run_remote_vq", fake)
        result = CliRunner().invoke(main, ["admin", "update", "vibeqc-dev", "host_b"])
        assert result.exit_code == 0, result.output
        assert calls == [("admin", "update", "vibeqc-dev", "localhost")]

    def test_detach_refuses_to_name_a_remote_host(self, state_dir: Path) -> None:
        result = CliRunner().invoke(
            main, ["admin", "update", "vibeqc-dev", "host_b", "--detach"]
        )
        assert result.exit_code == 2
        assert "runs the update on the host it is invoked on" in result.output

    def test_the_child_argv_never_carries_the_bearer_token(self) -> None:
        """A token on argv is readable by every user on the host."""
        argv = cli._detached_update_child_argv(
            "a" * 32,
            env="vibeqc-dev",
            all_envs=False,
            expected_tag="v0.17.1",
            expected_sha="b" * 40,
            no_restart_daemon=False,
            force=False,
            as_json=True,
            acknowledge_failed_marker=False,
            update_script_args=("--recreate-venv",),
            show_output=True,
        )
        # No credential option at all: the launcher adds whichever one its
        # spawn mechanism can deliver (stdin for a session, an owner-only file
        # for a user unit), and never a value on argv.
        assert not any(element.startswith("--token") for element in argv)
        assert argv[:5] == [sys.executable, "-m", "vq", "admin", "update"]
        assert argv[-1] == "localhost"
        assert "--detach-child" in argv
        assert argv[argv.index("--detach-run-id") + 1] == "a" * 32
        assert argv[argv.index("--update-script-arg") + 1] == "--recreate-venv"


# ----------------------------------------------------------------------
# The logind case: a scope kill, which a new session does not escape.
# ----------------------------------------------------------------------


class TestTheBuildLeavesTheSessionScope:
    """host_b, host_e and host_d run logind with ``KillUserProcesses=yes``.

    There the end of an ssh session stops the session's *scope*, and a new
    session or process group is still inside it -- a ``setsid nohup`` build died
    that way ten minutes in (``203184e``). What survived was a transient
    systemd user unit. This development machine has no logind, so these tests
    pin what vq asks systemd for; whether systemd then keeps the build alive is
    the real-host validation requested on the tracking issue.
    """

    RUN_ID = "a" * 32
    CHILD_ARGV = (
        "/venv/bin/python", "-m", "vq", "admin", "update", "vibeqc-dev", "localhost",
    )

    def test_the_unit_is_a_collected_user_service_with_the_real_argv(
        self, tmp_path: Path
    ) -> None:
        child_log = tmp_path / "child.log"
        argv = admin._detached_systemd_run_argv(
            self.RUN_ID,
            ["/venv/bin/python", "-m", "vq", "admin", "update", "vibeqc-dev", "localhost"],
            child_log=child_log,
            environ={"PATH": "/usr/bin", "VQ_STATE_DIR": "/state"},
            cwd="/work",
        )
        assert argv[:4] == ["systemd-run", "--user", "--collect", "--quiet"]
        assert f"--unit=vq-admin-update-{self.RUN_ID}" in argv
        # A service unit, never --scope: a scope is still started from, and
        # accounted to, the caller -- the validated remedy is a service unit.
        assert "--scope" not in argv
        assert "--working-directory=/work" in argv
        assert f"--property=StandardOutput=append:{child_log}" in argv
        assert f"--property=StandardError=append:{child_log}" in argv
        separator = argv.index("--")
        assert argv[separator + 1 :] == [
            "/venv/bin/python", "-m", "vq", "admin", "update", "vibeqc-dev", "localhost",
        ]
        assert "--setenv=VQ_STATE_DIR=/state" in argv[:separator]

    def test_the_unit_gets_state_and_watchdogs_but_never_a_secret(self) -> None:
        """A unit starts from the manager's environment, and `--setenv` is public."""
        kept = admin._detached_systemd_environment(
            {
                "PATH": "/usr/bin",
                "HOME": "/home/USER",
                "VQ_STATE_DIR": "/state",
                "VQ_UPDATE_SCRIPT_TIMEOUT": "14400.0",
                "VQ_BUILD_STALL_TIMEOUT": "3600.0",
                "VQ_WEB_TOKEN_FILE": "/etc/vq/web-token",
                "XDG_RUNTIME_DIR": "/run/user/1000",
                "LC_ALL": "C.UTF-8",
                "CMAKE_BUILD_PARALLEL_LEVEL": "6",
                "VQ_TOKEN": "bearer-secret",
                "GITLAB_TOKEN": "glpat-secret",
                "AWS_SECRET_ACCESS_KEY": "aws-secret",
                "VQ_FLEET_OPERATION_ID": "operation-context",
                "VQ_ODD": "line one\nline two",
                "UNRELATED": "x",
            }
        )
        assert kept == {
            "PATH": "/usr/bin",
            "HOME": "/home/USER",
            "VQ_STATE_DIR": "/state",
            "VQ_UPDATE_SCRIPT_TIMEOUT": "14400.0",
            "VQ_BUILD_STALL_TIMEOUT": "3600.0",
            "VQ_WEB_TOKEN_FILE": "/etc/vq/web-token",
            "XDG_RUNTIME_DIR": "/run/user/1000",
            "LC_ALL": "C.UTF-8",
            "CMAKE_BUILD_PARALLEL_LEVEL": "6",
        }

    @pytest.mark.parametrize(
        ("platform", "systemd_run", "user_manager", "expected"),
        [
            ("linux", "/usr/bin/systemd-run", True, admin.DETACHED_MECHANISM_SYSTEMD),
            ("linux", None, True, admin.DETACHED_MECHANISM_SESSION),
            ("linux", "/usr/bin/systemd-run", False, admin.DETACHED_MECHANISM_SESSION),
            ("darwin", "/usr/bin/systemd-run", True, admin.DETACHED_MECHANISM_SESSION),
        ],
    )
    def test_a_user_unit_is_chosen_wherever_a_user_manager_answers(
        self,
        monkeypatch: pytest.MonkeyPatch,
        platform: str,
        systemd_run: str | None,
        user_manager: bool,
        expected: str,
    ) -> None:
        monkeypatch.setattr(admin.sys, "platform", platform)
        monkeypatch.setattr(
            admin.shutil,
            "which",
            lambda name: systemd_run if name == "systemd-run" else None,
        )
        monkeypatch.setattr(admin, "_systemctl_user_available", lambda: user_manager)
        assert admin._detached_spawn_mechanism() == expected

    def _fake_systemd(
        self,
        monkeypatch: pytest.MonkeyPatch,
        *,
        returncode: int = 0,
        activate: bool = True,
        stderr: str = "",
    ) -> list[list[str]]:
        """Stand in for systemd-run: record the argv, maybe activate the run."""
        calls: list[list[str]] = []
        real_run = subprocess.run

        def fake_run(argv, *args, **kwargs):  # type: ignore[no-untyped-def]
            if not argv or argv[0] != "systemd-run":
                return real_run(argv, *args, **kwargs)
            calls.append(list(argv))
            child = argv[argv.index("--") + 1 :]
            if "--token-file" in child:
                token_file = Path(child[child.index("--token-file") + 1])
                # What the updater reads before it activates.
                assert (token_file.stat().st_mode & 0o777) == 0o600
                assert token_file.read_text() == "bearer-secret\n"
            if returncode == 0 and activate:
                admin_detached.write_activation(
                    self.RUN_ID, pid=os.getpid(), pid_start_time=0
                )
            return subprocess.CompletedProcess(argv, returncode, stdout="", stderr=stderr)

        monkeypatch.setattr(admin.subprocess, "run", fake_run)
        monkeypatch.setattr(admin, "_user_lingers", lambda: True)
        return calls

    def test_the_token_crosses_by_owner_only_file_and_is_removed(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A unit has no stdin, and a token on argv is readable by every user."""
        calls = self._fake_systemd(monkeypatch)
        receipt = admin.launch_detached_update(
            run_id=self.RUN_ID,
            target="vibeqc-dev",
            child_argv=list(self.CHILD_ARGV),
            token="bearer-secret",
            mechanism=admin.DETACHED_MECHANISM_SYSTEMD,
        )
        assert receipt["mechanism"] == admin.DETACHED_MECHANISM_SYSTEMD
        assert receipt["unit"] == f"vq-admin-update-{self.RUN_ID}"
        assert "warning" not in receipt
        (argv,) = calls
        assert all("bearer-secret" not in element for element in argv)
        child = argv[argv.index("--") + 1 :]
        assert child[-1] == "localhost"
        token_file = Path(child[child.index("--token-file") + 1])
        assert not token_file.exists(), "the token file must not outlive activation"

    def test_a_unit_that_systemd_refuses_is_a_clean_refusal(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._fake_systemd(
            monkeypatch, returncode=1, stderr="Failed to connect to bus"
        )
        with pytest.raises(admin.AdminError) as excinfo:
            admin.launch_detached_update(
                run_id=self.RUN_ID,
                target="vibeqc-dev",
                child_argv=list(self.CHILD_ARGV),
                token="bearer-secret",
                mechanism=admin.DETACHED_MECHANISM_SYSTEMD,
            )
        message = str(excinfo.value)
        assert "no update was attempted" in message
        assert "Failed to connect to bus" in message
        run_dir = admin_detached.detached_run_dir(self.RUN_ID)
        assert not (run_dir / "token").exists()

    def test_a_unit_that_dies_before_activating_is_reported_with_its_log(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._fake_systemd(monkeypatch, activate=False)
        monkeypatch.setattr(admin, "_detached_unit_active", lambda unit: False)
        run_dir = admin_detached.detached_run_dir(self.RUN_ID)

        with pytest.raises(admin.AdminError) as excinfo:
            admin.launch_detached_update(
                run_id=self.RUN_ID,
                target="vibeqc-dev",
                child_argv=list(self.CHILD_ARGV),
                token=None,
                mechanism=admin.DETACHED_MECHANISM_SYSTEMD,
                activation_timeout=30.0,
            )
        assert "is no longer active" in str(excinfo.value)
        assert "no update was attempted" in str(excinfo.value)
        assert run_dir.is_dir()

    def test_disabled_lingering_is_said_out_loud(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Without lingering the user manager, and the unit, stop at logout too."""
        self._fake_systemd(monkeypatch)
        monkeypatch.setattr(admin, "_user_lingers", lambda: False)
        receipt = admin.launch_detached_update(
            run_id=self.RUN_ID,
            target="vibeqc-dev",
            child_argv=list(self.CHILD_ARGV),
            token=None,
            mechanism=admin.DETACHED_MECHANISM_SYSTEMD,
        )
        assert "loginctl enable-linger" in str(receipt["warning"])

    def test_a_linux_host_without_a_user_manager_is_warned_about_logind(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(admin.sys, "platform", "linux")
        warning = admin._detached_mechanism_warning(admin.DETACHED_MECHANISM_SESSION)
        assert warning is not None and "KillUserProcesses" in warning

    def test_the_driver_relays_a_launch_warning(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch, no_sleep: None
    ) -> None:
        """A warning that stays on the target host protects nobody."""
        seen: list[str] = []

        def fake(host_cfg, *vq_args, **kwargs):  # type: ignore[no-untyped-def]
            if vq_args[:2] == ("admin", "update"):
                seen.append(vq_args[vq_args.index("--detach-run-id") + 1])
                return subprocess.CompletedProcess(
                    args=["ssh"],
                    returncode=0,
                    stdout=json.dumps(
                        {
                            "schema": admin_detached.DETACHED_ACTIVATION_SCHEMA,
                            "run_id": seen[0],
                            "state": "running",
                            "mechanism": admin.DETACHED_MECHANISM_SYSTEMD,
                            "unit": f"vq-admin-update-{seen[0]}",
                            "warning": "user lingering is disabled for this account",
                        }
                    ),
                    stderr="",
                )
            return _observation_proc(
                _observation(
                    admin_detached.STATE_COMPLETED,
                    run_id=seen[0],
                    outcome=admin.OUTCOME_OK,
                    exit_code=0,
                    payload="OK\n",
                )
            )

        monkeypatch.setattr(transport, "run_remote_vq", fake)
        result = CliRunner().invoke(main, ["admin", "update", "vibeqc-dev", "host_b"])
        assert result.exit_code == 0, result.output
        assert "user lingering is disabled" in result.stderr
        assert "systemd-run" in result.stderr


class TestDetachedHousekeeping:
    def test_a_finished_launched_run_is_actually_pruned(self, state_dir: Path) -> None:
        """Every launch leaves a child.log, so pruning must remove that too."""
        finished = admin_detached.new_run_id()
        run_dir = admin_detached.write_launch(finished, target="vibeqc-dev", argv=["vq"])
        (run_dir / "child.log").write_text("diagnostics\n")
        admin_detached.write_result(
            finished, outcome=admin.OUTCOME_OK, exit_code=0, payload=""
        )
        admin_detached.prune_detached_runs(keep=0)
        assert not run_dir.exists()

    def test_the_launcher_adds_the_credential_option_ahead_of_the_host(self) -> None:
        argv = ["python", "-m", "vq", "admin", "update", "vibeqc-dev", "localhost"]
        assert admin._insert_before_host(argv, ["--token-stdin"]) == [
            "python", "-m", "vq", "admin", "update", "vibeqc-dev",
            "--token-stdin", "localhost",
        ]
        assert admin._insert_before_host(["python", "x"], ["--token-stdin"]) == [
            "python", "x", "--token-stdin",
        ]


# ----------------------------------------------------------------------
# `vq admin auto-update ENV HOST`: the same handshake through its own verb.
# ----------------------------------------------------------------------


class TestAutoUpdateDelegation:
    """A delegated drift apply is a real rebuild, so it detaches too."""

    @staticmethod
    def _launch_ok(
        calls: list[tuple[str, ...]], observation: dict[str, object] | None = None
    ):  # type: ignore[no-untyped-def]
        def fake(host_cfg, *vq_args, **kwargs):  # type: ignore[no-untyped-def]
            calls.append(tuple(vq_args))
            if vq_args[:2] == ("admin", "auto-update"):
                return subprocess.CompletedProcess(
                    args=["ssh"], returncode=0, stdout="{}", stderr=""
                )
            return _observation_proc(
                observation
                if observation is not None
                else _observation(
                    admin_detached.STATE_COMPLETED,
                    run_id=vq_args[2],
                    outcome=admin.OUTCOME_OK,
                    exit_code=0,
                    payload="AUTO-APPLIED\n",
                )
            )

        return fake

    def test_a_delegated_auto_update_launches_detached_and_polls(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch, no_sleep: None
    ) -> None:
        calls: list[tuple[str, ...]] = []
        monkeypatch.setattr(transport, "run_remote_vq", self._launch_ok(calls))
        result = CliRunner().invoke(
            main, ["admin", "auto-update", "vibeqc-dev", "host_b", "--json"]
        )
        assert result.exit_code == 0, result.output
        assert result.stdout == "AUTO-APPLIED\n"
        launches = [c for c in calls if c[:2] == ("admin", "auto-update")]
        assert len(launches) == 1, "the mutating launch crosses the wire once"
        launch = launches[0]
        assert launch[:3] == ("admin", "auto-update", "vibeqc-dev")
        assert "--json" in launch
        assert "--detach" in launch
        assert launch[-1] == "localhost"
        run_id = launch[launch.index("--detach-run-id") + 1]
        assert admin_detached.validate_run_id(run_id)
        assert calls[1][:3] == ("admin", "observe-update", run_id)

    def test_a_dry_run_auto_update_stays_attached(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A probe mutates nothing, so there is nothing for a drop to kill."""
        calls: list[tuple[str, ...]] = []

        def fake(host_cfg, *vq_args, **kwargs):  # type: ignore[no-untyped-def]
            calls.append(tuple(vq_args))
            return subprocess.CompletedProcess(
                args=["ssh"], returncode=0, stdout="PROBED\n", stderr=""
            )

        monkeypatch.setattr(transport, "run_remote_vq", fake)
        result = CliRunner().invoke(
            main, ["admin", "auto-update", "vibeqc-dev", "host_b", "--dry-run"]
        )
        assert result.exit_code == 0, result.output
        assert result.stdout == "PROBED\n"
        assert calls == [("admin", "auto-update", "vibeqc-dev", "--dry-run", "localhost")]

    def test_a_lost_auto_update_launch_response_is_adopted_by_run_id(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch, no_sleep: None
    ) -> None:
        seen: list[str] = []

        def fake(host_cfg, *vq_args, **kwargs):  # type: ignore[no-untyped-def]
            if vq_args[:2] == ("admin", "auto-update"):
                seen.append(vq_args[vq_args.index("--detach-run-id") + 1])
                raise transport.RemoteOutcomeUnknown("remote vq failed (exit 255)")
            assert vq_args[2] == seen[0]
            return _observation_proc(
                _observation(
                    admin_detached.STATE_COMPLETED,
                    run_id=seen[0],
                    outcome=admin.OUTCOME_OK,
                    exit_code=0,
                    payload="ADOPTED\n",
                )
            )

        monkeypatch.setattr(transport, "run_remote_vq", fake)
        result = CliRunner().invoke(
            main, ["admin", "auto-update", "vibeqc-dev", "host_b"]
        )
        assert result.exit_code == 0, result.output
        assert result.stdout == "ADOPTED\n"
        assert "adopting run" in result.stderr
        assert len(seen) == 1

    def test_an_auto_update_run_that_died_without_a_receipt_is_reported_unknown(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch, no_sleep: None
    ) -> None:
        """The genuinely unknown case keeps the do-not-retry advice."""
        calls: list[tuple[str, ...]] = []
        monkeypatch.setattr(
            transport,
            "run_remote_vq",
            self._launch_ok(
                calls, _observation(admin_detached.STATE_LOST, run_id="0" * 32)
            ),
        )
        result = CliRunner().invoke(
            main, ["admin", "auto-update", "vibeqc-dev", "host_b"]
        )
        assert result.exit_code == 1
        assert "Remote admin outcome is unknown" in result.stderr
        assert "Do not retry it yet" in result.stderr
        assert "vq admin status host_b --json" in result.stderr
        assert len([c for c in calls if c[:2] == ("admin", "auto-update")]) == 1

    def test_a_failed_auto_update_keeps_its_report_and_is_not_called_unknown(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch, no_sleep: None
    ) -> None:
        calls: list[tuple[str, ...]] = []
        monkeypatch.setattr(
            transport,
            "run_remote_vq",
            self._launch_ok(
                calls,
                _observation(
                    admin_detached.STATE_COMPLETED,
                    run_id="0" * 32,
                    outcome=admin.OUTCOME_FAILED,
                    exit_code=1,
                    payload="action:       update\napply:        FAILED\n",
                    error="auto-update apply step failed",
                ),
            ),
        )
        result = CliRunner().invoke(
            main, ["admin", "auto-update", "vibeqc-dev", "host_b"]
        )
        assert result.exit_code == admin.ADMIN_OUTCOME_EXIT_CODES[admin.OUTCOME_FAILED]
        assert "apply:        FAILED" in result.stdout
        assert "auto-update apply step failed" in result.stderr
        assert "Remote admin outcome is unknown" not in result.stderr

    def test_a_remote_too_old_to_detach_auto_update_falls_back_to_attached(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch, no_sleep: None
    ) -> None:
        calls: list[tuple[str, ...]] = []

        def fake(host_cfg, *vq_args, **kwargs):  # type: ignore[no-untyped-def]
            calls.append(tuple(vq_args))
            if "--detach" in vq_args:
                raise transport.RemoteCommandError(
                    "remote vq failed (exit 2)",
                    returncode=2,
                    stderr="Error: No such option: --detach",
                )
            return subprocess.CompletedProcess(
                args=["ssh"], returncode=0, stdout="LEGACY-AUTO\n", stderr=""
            )

        monkeypatch.setattr(transport, "run_remote_vq", fake)
        result = CliRunner().invoke(
            main, ["admin", "auto-update", "vibeqc-dev", "host_b"]
        )
        assert result.exit_code == 0, result.output
        assert result.stdout == "LEGACY-AUTO\n"
        assert "does not support detached updates" in result.stderr
        assert calls[1] == ("admin", "auto-update", "vibeqc-dev", "localhost")

    def test_the_escape_hatch_restores_the_attached_auto_update(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(cli._ADMIN_NO_DETACH_ENV, "1")
        calls: list[tuple[str, ...]] = []

        def fake(host_cfg, *vq_args, **kwargs):  # type: ignore[no-untyped-def]
            calls.append(tuple(vq_args))
            return subprocess.CompletedProcess(
                args=["ssh"], returncode=0, stdout="ATTACHED\n", stderr=""
            )

        monkeypatch.setattr(transport, "run_remote_vq", fake)
        result = CliRunner().invoke(
            main, ["admin", "auto-update", "vibeqc-dev", "host_b"]
        )
        assert result.exit_code == 0, result.output
        assert calls == [("admin", "auto-update", "vibeqc-dev", "localhost")]

    @pytest.mark.parametrize(
        ("argv", "message"),
        [
            (
                ["admin", "auto-update", "vibeqc-dev", "host_b", "--detach"],
                "runs the auto-update on the host it is invoked on",
            ),
            (
                ["admin", "auto-update", "vibeqc-dev", "--all-hosts", "--detach"],
                "--all-hosts walks every configured host",
            ),
        ],
    )
    def test_detach_refuses_what_it_cannot_protect(
        self, state_dir: Path, argv: list[str], message: str
    ) -> None:
        result = CliRunner().invoke(main, argv)
        assert result.exit_code == 2
        assert message in result.output

    def test_the_auto_update_child_argv_never_carries_the_bearer_token(self) -> None:
        argv = cli._detached_auto_update_child_argv(
            "a" * 32,
            env=None,
            all_envs=True,
            dry_run=False,
            as_json=True,
        )
        assert argv[:5] == [sys.executable, "-m", "vq", "admin", "auto-update"]
        # The launcher adds --token-stdin or an owner-only --token-file,
        # whichever its mechanism can deliver; the argv carries neither.
        assert not any(element.startswith("--token") for element in argv)
        assert "--all" in argv
        assert "--json" in argv
        assert "--dry-run" not in argv
        assert "--detach-child" in argv
        assert argv[argv.index("--detach-run-id") + 1] == "a" * 32
        assert argv[-1] == "localhost"

    def test_the_detached_child_publishes_a_failed_apply_as_its_receipt(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The report and the failure both reach the driver, not a SIGHUP."""
        from vq import auto_update

        def failed_apply(env, cfg, *, host, dry_run=False, admin_token=None):  # type: ignore[no-untyped-def]
            return auto_update.AutoUpdateOutcome(
                decision=auto_update.AutoUpdateDecision(
                    env_name=env,
                    action="update",
                    reason="newer tag available",
                    current_tag="v0.17.0",
                    target_tag="v0.17.1",
                ),
                update_result=admin.UpdateResult(
                    env=env,
                    git_dir="/fake/repo",
                    branch="main",
                    update_script=None,
                    git_pull_rc=1,
                    work_errors=["git pull failed"],
                ),
            )

        monkeypatch.setattr(auto_update, "auto_update_env", failed_apply)
        run_id = admin_detached.new_run_id()
        admin_detached.write_launch(run_id, target="vibeqc-dev", argv=["vq"])
        try:
            result = CliRunner().invoke(
                main,
                [
                    "admin",
                    "auto-update",
                    "vibeqc-dev",
                    "--detach-child",
                    "--detach-run-id",
                    run_id,
                    "localhost",
                ],
            )
        finally:
            admin.set_detached_run_id(None)

        assert result.exit_code == 1, result.output
        observed = admin_detached.observe(run_id)
        assert observed.state == admin_detached.STATE_COMPLETED
        assert observed.outcome == admin.OUTCOME_FAILED
        assert observed.exit_code == 1
        assert observed.error == "auto-update apply step failed"
        assert "apply:        FAILED" in (observed.payload or "")
        assert "git pull failed" in (observed.payload or "")

    def test_a_detached_auto_update_uses_the_update_launcher(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """One launcher for both verbs, so auto-update gets whatever it learned:
        a transient user unit where a user manager answers, and a credential
        that never rides on argv."""
        captured: list[dict[str, object]] = []

        def fake_launch(**kwargs: object) -> dict[str, object]:
            captured.append(kwargs)
            return {"run_id": kwargs["run_id"], "state": "running"}

        monkeypatch.setattr(admin, "launch_detached_update", fake_launch)
        run_id = "b" * 32

        result = CliRunner().invoke(
            main,
            [
                "admin", "auto-update", "vibeqc-dev",
                "--detach", "--detach-run-id", run_id, "localhost",
            ],
        )

        assert result.exit_code == 0, result.output
        (call,) = captured
        assert call["run_id"] == run_id
        child = list(call["child_argv"])  # type: ignore[call-overload]
        assert child[3:5] == ["admin", "auto-update"]
        assert "--detach-child" in child
        assert child[-1] == "localhost"
        assert not any(str(element).startswith("--token") for element in child)
        assert json.loads(result.stdout)["run_id"] == run_id

    def test_an_all_hosts_json_run_with_a_failing_host_stays_one_document(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch, no_sleep: None
    ) -> None:
        """A failing detached host's recorded report is raised, not echoed:
        the fan-out composes the only document on stdout."""
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "host_b"\n'
            "\n"
            "[hosts.host_b]\n"
            'ssh = "host_b"\n'
            "\n"
            "[hosts.host_e]\n"
            'ssh = "host_e"\n'
            "\n"
            "[programs.vibeqc-dev]\n"
            'kind = "venv"\n'
            'python = "/fake/python"\n'
            f'git_dir = "{state_dir / "repo"}"\n'
            'branch = "main"\n'
        )
        runs: dict[str, str] = {}

        def fake(host_cfg, *vq_args, **kwargs):  # type: ignore[no-untyped-def]
            if vq_args[:2] == ("admin", "auto-update"):
                runs[vq_args[vq_args.index("--detach-run-id") + 1]] = host_cfg.ssh
                return subprocess.CompletedProcess(
                    args=["ssh"], returncode=0, stdout="{}", stderr=""
                )
            run_id = vq_args[2]
            failing = runs.get(run_id) == "host_e"
            return _observation_proc(
                _observation(
                    admin_detached.STATE_COMPLETED,
                    run_id=run_id,
                    outcome=admin.OUTCOME_FAILED if failing else admin.OUTCOME_OK,
                    exit_code=1 if failing else 0,
                    payload=(
                        '{"apply": "FAILED"}\n' if failing else '{"apply": "ok"}\n'
                    ),
                    error="auto-update apply step failed" if failing else None,
                )
            )

        monkeypatch.setattr(transport, "run_remote_vq", fake)

        result = CliRunner().invoke(
            main,
            ["admin", "auto-update", "vibeqc-dev", "--all-hosts", "--json"],
        )

        assert result.exit_code != 0, result.output
        assert sorted(runs.values()) == ["host_b", "host_e"]
        json.loads(result.stdout)  # exactly one document, or this raises
        assert "host_e" in result.stdout
