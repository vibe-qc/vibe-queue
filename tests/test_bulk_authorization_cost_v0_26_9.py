"""A bulk verb's authorization cost must not grow with retained history (#22).

An ownership decision has two halves. The half that depends on the job row is
a uid comparison. The half that does not is the whole expense: reading and
validating the personal and system configs, and in multi-user mode resolving
the caller's group and passwd entries through NSS.

Before this change every row in a bulk verb paid both halves. A queue retains
its terminal jobs, so `vq pause --all` on a driver that had run 16,000 jobs
parsed and validated its config 16,000 times and, on a multi-user host, made
32,000 NSS lookups -- to reach the same verdict every time.

These tests pin the split: the policy is resolved once per operation, every
row is still checked against it, and a row that must be denied still is.
"""
from __future__ import annotations

import contextlib
from collections.abc import Iterator
from pathlib import Path

import pytest

from vq import config, ownership, paths, pause_resume
from vq.spec import JobSpec, JobState

RETAINED = 12


@pytest.fixture
def state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    paths.jobs_dir().mkdir(parents=True, exist_ok=True)
    return tmp_path


def _terminal_spec(jobid: str, *, submitter: str | None = None) -> JobSpec:
    """One retained, fully settled row -- the bulk of a long-lived queue."""
    spec = JobSpec(
        id=jobid,
        command=["true"],
        cwd=str(paths.jobs_dir() / jobid),
        cpus=1,
        state=JobState.COMPLETED,
        exit_code=0,
        submitter=submitter,
    )
    spec.write(paths.spec_path(jobid))
    return spec


def _count_policy_resolutions(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Record every read of the half of the decision that is not per-row."""
    resolutions: list[str] = []
    real_config = ownership._authorization_config
    real_admin = ownership._caller_is_admin

    def counting_config(cfg: object, **kwargs: object) -> object:
        resolutions.append("config")
        return real_config(cfg, **kwargs)  # type: ignore[arg-type]

    def counting_admin(cfg: config.Config) -> bool:
        resolutions.append("nss")
        return real_admin(cfg)

    monkeypatch.setattr(ownership, "_authorization_config", counting_config)
    monkeypatch.setattr(ownership, "_caller_is_admin", counting_admin)
    return resolutions


def _count_authorized_rows(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Record every row the verb actually puts through an ownership check."""
    checked: list[str] = []
    real_check = ownership.check_owner

    def counting_check(spec: JobSpec, **kwargs: object) -> None:
        checked.append(spec.id)
        real_check(spec, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(pause_resume.ownership, "check_owner", counting_check)
    return checked


class TestTheCostIsBounded:
    """The expensive half is paid once per operation, not once per row."""

    def test_pause_all_resolves_the_authorization_policy_once(
        self, state: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        for index in range(RETAINED):
            _terminal_spec(f"retained{index:08d}")
        resolutions = _count_policy_resolutions(monkeypatch)

        pause_resume.pause_all("localhost")

        assert resolutions.count("config") == 1
        # Single-user mode decides on `enabled` alone and never reaches NSS.
        assert resolutions.count("nss") == 0

    def test_resume_all_resolves_the_authorization_policy_once(
        self, state: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        for index in range(RETAINED):
            _terminal_spec(f"retained{index:08d}")
        resolutions = _count_policy_resolutions(monkeypatch)

        pause_resume.resume_all("localhost")

        assert resolutions.count("config") == 1
        assert resolutions.count("nss") == 0

    def test_the_cost_does_not_grow_with_the_number_of_retained_rows(
        self, state: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The property the issue asks for, stated as a comparison."""
        for index in range(3):
            _terminal_spec(f"small{index:08d}")
        small = _count_policy_resolutions(monkeypatch)
        pause_resume.pause_all("localhost")
        small_total = len(small)

        for index in range(RETAINED * 4):
            _terminal_spec(f"large{index:08d}")
        large = _count_policy_resolutions(monkeypatch)
        pause_resume.pause_all("localhost")

        assert len(large) == small_total

    def test_a_multi_user_policy_resolves_its_nss_lookups_once(
        self, state: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The half that costs two NSS lookups per row is the worst of it."""
        (state / "cfg" / "config.toml").write_text(
            '[multi_user]\nenabled = true\nadmin_group = "vqadmin"\n',
            encoding="utf-8",
        )
        for index in range(RETAINED):
            _terminal_spec(f"retained{index:08d}")
        resolutions = _count_policy_resolutions(monkeypatch)

        pause_resume.pause_all("localhost")

        assert resolutions.count("config") == 1
        assert resolutions.count("nss") == 1

    def test_an_empty_queue_still_reads_no_policy_at_all(
        self, state: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Resolving up front must not add work a queue with no rows avoided."""
        resolutions = _count_policy_resolutions(monkeypatch)

        pause_resume.pause_all("localhost")

        assert resolutions == []


class TestEveryRowIsStillAuthorized:
    """Bounding the cost must not skip, weaken or reorder a decision."""

    def test_every_retained_row_still_goes_through_an_ownership_check(
        self, state: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        expected = {f"retained{index:08d}" for index in range(RETAINED)}
        for jobid in sorted(expected):
            _terminal_spec(jobid)
        checked = _count_authorized_rows(monkeypatch)

        pause_resume.pause_all("localhost")

        assert set(checked) == expected
        assert len(checked) == len(expected)

    def test_a_denied_row_is_still_reported_as_an_error(
        self, state: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The resolved policy is consulted, not bypassed."""
        _terminal_spec("deniedrow1")
        denied: list[str] = []

        def deny(spec: JobSpec, **kwargs: object) -> None:
            denied.append(spec.id)
            raise ownership.OwnershipError(f"job {spec.id} is foreign")

        monkeypatch.setattr(pause_resume.ownership, "check_owner", deny)

        summary = pause_resume.resume_all("localhost")

        assert denied == ["deniedrow1", "deniedrow1"]
        assert "1 error(s)" in summary

    def test_the_decision_still_happens_inside_the_spec_lock(
        self, state: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Resolving the policy outside the lock must not move the verdict."""
        spec = _terminal_spec("lockedrow1")
        spec.state = JobState.SUSPENDED
        spec.paused_by = "operator"
        spec.write(paths.spec_path(spec.id))
        depth = 0
        depths: list[int] = []
        real_lock = paths.spec_lock
        real_check = ownership.check_owner

        @contextlib.contextmanager
        def tracked_lock(spec_path: Path, **kwargs: object) -> Iterator[None]:
            nonlocal depth
            with real_lock(spec_path, **kwargs):  # type: ignore[arg-type]
                depth += 1
                try:
                    yield
                finally:
                    depth -= 1

        def tracked_check(job: JobSpec, **kwargs: object) -> None:
            depths.append(depth)
            real_check(job, **kwargs)  # type: ignore[arg-type]

        monkeypatch.setattr(pause_resume.paths, "spec_lock", tracked_lock)
        monkeypatch.setattr(pause_resume.ownership, "check_owner", tracked_check)

        pause_resume.resume_all("localhost")

        assert depths, "the row was never authorized"
        assert depths[-1] > 0, "the final verdict must be taken under the lock"

    def test_an_unreadable_policy_still_stops_the_verb(
        self, state: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _terminal_spec("badpolicy1")

        def invalid(*args: object, **kwargs: object) -> object:
            raise config.ConfigError("policy unavailable")

        monkeypatch.setattr(ownership, "_authorization_config", invalid)

        with pytest.raises(config.ConfigError, match="policy unavailable"):
            pause_resume.resume_all("localhost")


class TestThePolicyCarriesTheSameVerdict:
    """A resolved policy must decide exactly what a fresh load decides."""

    def test_a_resolved_policy_denies_what_a_fresh_load_denies(
        self, state: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        (state / "cfg" / "config.toml").write_text(
            '[multi_user]\nenabled = true\nadmin_group = "vqadmin"\n',
            encoding="utf-8",
        )
        monkeypatch.setattr(ownership, "_caller_is_admin", lambda _cfg: False)
        foreign = JobSpec(
            id="foreignrow1",
            command=["true"],
            cwd=str(paths.jobs_dir() / "foreignrow1"),
            cpus=1,
            state=JobState.COMPLETED,
            submitter=str(ownership._caller_uid() + 1),
        )

        with pytest.raises(ownership.OwnershipError) as fresh:
            ownership.check_owner(foreign)

        policy = ownership.authorization_policy()
        with pytest.raises(ownership.OwnershipError) as resolved:
            ownership.check_owner(foreign, policy=policy)

        assert str(resolved.value) == str(fresh.value)

    def test_a_resolved_policy_admits_what_a_fresh_load_admits(
        self, state: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        (state / "cfg" / "config.toml").write_text(
            '[multi_user]\nenabled = true\nadmin_group = "vqadmin"\n',
            encoding="utf-8",
        )
        monkeypatch.setattr(ownership, "_caller_is_admin", lambda _cfg: False)
        own = JobSpec(
            id="ownrow1",
            command=["true"],
            cwd=str(paths.jobs_dir() / "ownrow1"),
            cpus=1,
            state=JobState.COMPLETED,
            submitter=str(ownership._caller_uid()),
        )

        ownership.check_owner(own)
        ownership.check_owner(own, policy=ownership.authorization_policy())

    def test_omitting_the_policy_still_resolves_one_per_check(
        self, state: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The single-job path is unchanged: no caller has to pass a policy."""
        spec = _terminal_spec("singlerow1")
        resolutions = _count_policy_resolutions(monkeypatch)

        ownership.check_owner(spec)
        ownership.check_owner(spec)

        assert resolutions.count("config") == 2
