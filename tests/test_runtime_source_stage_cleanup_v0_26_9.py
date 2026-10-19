"""A runtime-source upload stage belongs to one deploy, and is reclaimed (#61).

A scheduler build host that cannot reach the source repository gets the exact
commit uploaded to it, about 110 MB per deploy, under
``<scratch_root>/.vq-admin/runtime-source/<program>/<sha>-<uuid>/``. Nothing
removed those. On the SLURM host 235 of them had accumulated since July, 26 GB,
and the home they share went over quota: every write failed, down to ``mkdir``.

This is not the helper-generation rule and must not become it. A helper staging
generation is retained deliberately, because another deployment -- possibly on
another host -- may still be using an older one. A runtime-source stage is
upload staging for exactly one deploy: the driver re-archives the same SHA from
git on demand, and nothing reads a stage once the build has consumed it.

The remote script runs here for real, against real directories, because
``scratch_root`` is a temporary directory and the fake transport executes the
script it is handed with ``sh``.
"""
from __future__ import annotations

import json
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

from vq import admin, config, paths

_SHA = "b" * 40
_PROGRAM = "vibeqc-release"


@pytest.fixture
def state_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    paths.jobs_dir().mkdir(parents=True, exist_ok=True)
    return tmp_path


def _write_config(cfg_dir: Path, *, scratch_root: Path) -> None:
    cfg_dir.joinpath("config.toml").write_text(
        "\n".join(
            [
                'scheduler_runtime_source_repo = "/repo"',
                "",
                "[hosts.localhost]",
                'ssh = "localhost"',
                "",
                "[hosts.host_c]",
                'ssh = "host_c-login"',
                'scheduler = "slurm"',
                'scheduler_dialect = "slurm"',
                f'scratch_root = "{scratch_root}"',
                'scheduler_driver = "localhost"',
                'fleet_role = "managed"',
                'submit_extra = ["--account", "grp", "--partition", "p"]',
                "",
                f"[hosts.host_c.scheduler_runtime_deployments.{_PROGRAM}]",
                'update_command = "/site/bin/deploy-host_c-runtime"',
                'verify_command = "srun /site/bin/verify-host_c-runtime"',
                "stage_source = true",
                "verify_timeout_seconds = 900",
                "timeout_seconds = 5400",
                "",
                f"[programs.{_PROGRAM}]",
                'kind = "binary"',
                'binary = "/opt/vibeqc/python"',
                "",
            ]
        ),
        encoding="utf-8",
    )


def _stage_root(scratch_root: Path) -> Path:
    return scratch_root / ".vq-admin" / "runtime-source" / _PROGRAM


def _make_stage(scratch_root: Path, token: str, *, sha: str = _SHA) -> Path:
    """One stage directory, as ``_stage_scheduler_runtime_source`` leaves it."""
    stage = _stage_root(scratch_root) / f"{sha}-{token}"
    stage.mkdir(parents=True)
    (stage / f"vibeqc-{sha[:12]}-source.tar.gz").write_bytes(b"archive" * 64)
    (stage / "SOURCE-SHA").write_text(sha + "\n", encoding="utf-8")
    (stage / "ARCHIVE-SHA256").write_text("0" * 64 + "  a\n", encoding="utf-8")
    return stage


def _stages(scratch_root: Path) -> list[str]:
    root = _stage_root(scratch_root)
    return sorted(p.name for p in root.iterdir()) if root.is_dir() else []


def _deploy(
    scratch_root: Path,
    *,
    token: str,
    verify_ok: bool = True,
    force: bool = False,
    record: list[tuple[str, ...]] | None = None,
) -> admin.SchedulerRuntimeUpdateResult:
    """Run one deployment whose staging really lands on disk."""
    cfg = config.load_config()

    def fake_stage(host, command_host_cfg, prog, expected_sha, cfg_, result):  # type: ignore[no-untyped-def]
        stage = _make_stage(scratch_root, token, sha=expected_sha)
        result.staged_source_stage = str(stage)
        archive = f"{stage}/vibeqc-{expected_sha[:12]}-source.tar.gz"
        result.staged_source_archive = archive
        return archive

    def fake_run_remote_shell(host_cfg, *argv, **kwargs):  # type: ignore[no-untyped-def]
        if record is not None:
            record.append(argv)
        if argv[:2] == ("sh", "-c"):
            # The reclaim script, run for real against the temporary tree.
            proc = subprocess.run(
                ["sh", "-c", *argv[2:]], capture_output=True, text=True,
            )
            return subprocess.CompletedProcess(
                args=list(argv),
                returncode=proc.returncode,
                stdout=proc.stdout,
                stderr=proc.stderr,
            )
        if any("deploy-host_c-runtime" in tok for tok in argv):
            return subprocess.CompletedProcess(
                args=list(argv), returncode=0, stdout="ok\n", stderr="",
            )
        receipt = {
            "program": _PROGRAM,
            "source_sha": _SHA if verify_ok else "c" * 40,
            "tag": None,
            "healthy": True,
            "activation": "atomic",
            "active_path": "/rt/current",
            "health_detail": "ok",
            "quiescent": True,
            "updater_pid": None,
        }
        return subprocess.CompletedProcess(
            args=list(argv),
            returncode=0 if verify_ok else 1,
            stdout=json.dumps(receipt),
            stderr="",
        )

    with (
        patch("vq.admin._stage_scheduler_runtime_source", side_effect=fake_stage),
        patch(
            "vq.admin._canonical_lifecycle_checkout",
            side_effect=lambda _path: Path("/repo"),
        ),
        patch(
            "vq.admin.transport.run_remote_shell",
            side_effect=fake_run_remote_shell,
        ),
    ):
        return admin.update_scheduler_runtime(
            "host_c", _PROGRAM, cfg, expected_sha=_SHA, force=force,
        )


class TestADeployReclaimsItsOwnStaging:
    def test_a_successful_deploy_leaves_no_stage_behind(
        self, state_dir: Path,
    ) -> None:
        scratch = state_dir / "scratch"
        _write_config(state_dir / "cfg", scratch_root=scratch)

        result = _deploy(scratch, token="a" * 32)

        # The disk first: on unfixed vq this is the 110 MB the deploy leaves.
        assert _stages(scratch) == []
        assert result.success is True
        assert result.staged_source_stage_reclaimed is True
        assert result.staged_source_reclaim_error is None

    def test_two_successful_deploys_leave_at_most_the_retention(
        self, state_dir: Path,
    ) -> None:
        """The closure check this issue asks for."""
        scratch = state_dir / "scratch"
        _write_config(state_dir / "cfg", scratch_root=scratch)

        first = _deploy(scratch, token="a" * 32)
        second = _deploy(scratch, token="d" * 32)

        assert _stages(scratch) == []
        assert first.success and second.success
        assert len(_stages(scratch)) <= admin.RUNTIME_SOURCE_STAGES_TO_KEEP

    def test_many_successful_deploys_never_accumulate(
        self, state_dir: Path,
    ) -> None:
        """The reported failure was 235 stages from 235 deploys."""
        scratch = state_dir / "scratch"
        _write_config(state_dir / "cfg", scratch_root=scratch)

        for index in range(8):
            assert _deploy(scratch, token=f"{index:032x}").success

        assert _stages(scratch) == []

    def test_a_failed_deploy_keeps_its_stage_for_forensics(
        self, state_dir: Path,
    ) -> None:
        scratch = state_dir / "scratch"
        _write_config(state_dir / "cfg", scratch_root=scratch)

        result = _deploy(scratch, token="a" * 32, verify_ok=False)

        assert result.success is False
        assert result.staged_source_stage_reclaimed is False
        assert _stages(scratch) == [f"{_SHA}-{'a' * 32}"]

    def test_failed_deploys_are_bounded_by_the_retention(
        self, state_dir: Path,
    ) -> None:
        """Forensics are kept, but not without limit."""
        scratch = state_dir / "scratch"
        _write_config(state_dir / "cfg", scratch_root=scratch)

        # A failed update leaves its marker for an operator to acknowledge, so
        # every deploy after the first is the `--force` re-run they would make.
        for index in range(7):
            assert not _deploy(
                scratch,
                token=f"{index:032x}",
                verify_ok=False,
                force=index > 0,
            ).success

        assert len(_stages(scratch)) == admin.RUNTIME_SOURCE_STAGES_TO_KEEP

    def test_a_successful_deploy_also_reclaims_what_older_vq_left(
        self, state_dir: Path,
    ) -> None:
        scratch = state_dir / "scratch"
        _write_config(state_dir / "cfg", scratch_root=scratch)
        for index in range(6):
            _make_stage(scratch, f"{index:032x}")
        assert len(_stages(scratch)) == 6

        result = _deploy(scratch, token="f" * 32)

        assert result.success is True
        assert len(_stages(scratch)) == admin.RUNTIME_SOURCE_STAGES_TO_KEEP
        assert f"{_SHA}-{'f' * 32}" not in _stages(scratch)


class TestTheReclaimStaysInItsLane:
    def test_it_never_calls_the_prune_verb_on_the_host(
        self, state_dir: Path,
    ) -> None:
        """Helper generations are pruned by an operator, never by a deploy.

        See docs/operations.md, "Managed helper updates retain staging
        generations". The reclaim removes the one directory this deploy made,
        by name, and that program's own older stages -- it does not reach for
        the remote verb.
        """
        scratch = state_dir / "scratch"
        _write_config(state_dir / "cfg", scratch_root=scratch)
        calls: list[tuple[str, ...]] = []

        _deploy(scratch, token="a" * 32, record=calls)

        assert not any("source-stage-prune" in tok for a in calls for tok in a)

    def test_it_leaves_helper_generations_and_foreign_files_alone(
        self, state_dir: Path,
    ) -> None:
        scratch = state_dir / "scratch"
        _write_config(state_dir / "cfg", scratch_root=scratch)
        generations = scratch / ".vq-admin" / "host_c" / "generations"
        helper = generations / f"{'e' * 40}-{'9' * 32}"
        helper.mkdir(parents=True)
        (helper / "payload").write_text("helper", encoding="utf-8")
        root = _stage_root(scratch)
        root.mkdir(parents=True, exist_ok=True)
        (root / "operator-notes.txt").write_text("keep", encoding="utf-8")
        (root / "not-a-stage").mkdir()

        assert _deploy(scratch, token="a" * 32).success is True

        assert helper.is_dir() and (helper / "payload").exists()
        assert (root / "operator-notes.txt").read_text() == "keep"
        assert (root / "not-a-stage").is_dir()

    def test_an_unrecognized_stage_path_is_refused_rather_than_removed(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """``scratch_root`` is operator config and the reclaim is ``rm -rf``."""
        victim = state_dir / "not-staging"
        victim.mkdir()
        (victim / "precious").write_text("data", encoding="utf-8")
        result = admin.SchedulerRuntimeUpdateResult(
            host="host_c", program=_PROGRAM, mode="deploy", command="",
            command_ssh="", verify_command="", verify_ssh="", expected_sha=_SHA,
        )
        result.staged_source_stage = str(victim)

        def unexpected(*_args, **_kwargs):  # type: ignore[no-untyped-def]
            raise AssertionError("the reclaim ran on an unrecognized path")

        monkeypatch.setattr("vq.admin.transport.run_remote_shell", unexpected)

        admin._reclaim_runtime_source_stage(
            "host_c", config.HostConfig(ssh="host_c"), result,
            remove_current=True,
        )

        assert result.staged_source_reclaim_error is not None
        assert "refusing to reclaim" in result.staged_source_reclaim_error
        assert (victim / "precious").exists()

    def test_a_reclaim_failure_never_fails_a_good_deploy(
        self, state_dir: Path,
    ) -> None:
        """Disk left behind is a worse outcome than a deploy reported failed."""
        scratch = state_dir / "scratch"
        _write_config(state_dir / "cfg", scratch_root=scratch)
        cfg = config.load_config()

        def fake_stage(host, command_host_cfg, prog, expected_sha, cfg_, result):  # type: ignore[no-untyped-def]
            stage = _make_stage(scratch, "a" * 32, sha=expected_sha)
            result.staged_source_stage = str(stage)
            archive = f"{stage}/vibeqc-{expected_sha[:12]}-source.tar.gz"
            result.staged_source_archive = archive
            return archive

        def fake_run_remote_shell(host_cfg, *argv, **kwargs):  # type: ignore[no-untyped-def]
            if argv[:2] == ("sh", "-c"):
                return subprocess.CompletedProcess(
                    args=list(argv), returncode=1, stdout="", stderr="rm: denied",
                )
            if any("deploy-host_c-runtime" in tok for tok in argv):
                return subprocess.CompletedProcess(
                    args=list(argv), returncode=0, stdout="ok\n", stderr="",
                )
            receipt = {
                "program": _PROGRAM, "source_sha": _SHA, "tag": None,
                "healthy": True, "activation": "atomic",
                "active_path": "/rt/current", "health_detail": "ok",
                "quiescent": True, "updater_pid": None,
            }
            return subprocess.CompletedProcess(
                args=list(argv), returncode=0, stdout=json.dumps(receipt), stderr="",
            )

        with (
            patch("vq.admin._stage_scheduler_runtime_source", side_effect=fake_stage),
            patch(
                "vq.admin._canonical_lifecycle_checkout",
                side_effect=lambda _path: Path("/repo"),
            ),
            patch(
                "vq.admin.transport.run_remote_shell",
                side_effect=fake_run_remote_shell,
            ),
        ):
            result = admin.update_scheduler_runtime(
                "host_c", _PROGRAM, cfg, expected_sha=_SHA,
            )

        assert result.success is True
        assert result.work_errors == []
        assert result.staged_source_reclaim_error is not None
        assert "rm: denied" in result.staged_source_reclaim_error


class TestTheMaintenanceVerbCanSeeTheseStages:
    def test_the_generations_prune_still_cannot_see_them(
        self, tmp_path: Path,
    ) -> None:
        """The symptom in the report: ``removed=0`` and the disk stays full."""
        root = tmp_path / ".vq-admin" / "runtime-source"
        for index in range(5):
            (root / _PROGRAM / f"{_SHA}-{index:032x}").mkdir(parents=True)

        result = admin.prune_scheduler_stage_generations(root / _PROGRAM, keep=1)

        assert result.removed == []
        assert len(list((root / _PROGRAM).iterdir())) == 5

    def test_the_runtime_source_prune_keeps_the_newest_per_program(
        self, tmp_path: Path,
    ) -> None:
        root = tmp_path / ".vq-admin" / "runtime-source"
        for program in (_PROGRAM, "vibeqc-dev"):
            for index in range(5):
                stage = root / program / f"{_SHA}-{index:032x}"
                stage.mkdir(parents=True)

        result = admin.prune_runtime_source_stages(root, keep=2)

        assert len(result.removed) == 6
        for program in (_PROGRAM, "vibeqc-dev"):
            assert len(list((root / program).iterdir())) == 2

    def test_it_leaves_unrecognized_entries_alone(self, tmp_path: Path) -> None:
        root = tmp_path / ".vq-admin" / "runtime-source"
        (root / _PROGRAM).mkdir(parents=True)
        for index in range(4):
            (root / _PROGRAM / f"{_SHA}-{index:032x}").mkdir()
        (root / _PROGRAM / "README").write_text("notes", encoding="utf-8")
        (root / "loose-file").write_text("x", encoding="utf-8")

        result = admin.prune_runtime_source_stages(root, keep=1)

        assert (root / _PROGRAM / "README").exists()
        assert (root / "loose-file").exists()
        assert len(result.removed) == 3

    def test_preserve_keeps_a_named_stage(self, tmp_path: Path) -> None:
        root = tmp_path / ".vq-admin" / "runtime-source"
        stages = []
        for index in range(4):
            stage = root / _PROGRAM / f"{_SHA}-{index:032x}"
            stage.mkdir(parents=True)
            stages.append(stage)

        result = admin.prune_runtime_source_stages(
            root, keep=1, preserve=stages[0],
        )

        assert stages[0].is_dir()
        assert str(stages[0]) in result.retained

    @pytest.mark.parametrize(
        ("kwargs", "message"),
        [
            ({"keep": 0}, "at least 1"),
            ({"keep": 2}, "must be the 'runtime-source' directory"),
        ],
    )
    def test_it_refuses_an_unsafe_request(
        self, tmp_path: Path, kwargs: dict, message: str,
    ) -> None:
        root = tmp_path / ("runtime-source" if "keep" in kwargs and
                           kwargs["keep"] == 0 else "somewhere-else")
        root.mkdir()
        with pytest.raises(admin.AdminError, match=message):
            admin.prune_runtime_source_stages(root, **kwargs)

    def test_it_refuses_a_relative_root(self) -> None:
        with pytest.raises(admin.AdminError, match="absolute path"):
            admin.prune_runtime_source_stages(Path("runtime-source"))
