"""Tests for the JobSpec on-disk schema."""
from __future__ import annotations

import json
import math
from pathlib import Path

import pytest
from pydantic import ValidationError

from vq.spec import (
    SPEC_VERSION,
    TERMINAL_STATES,
    WATCHDOG_TERMINAL_STATES,
    JobSpec,
    JobState,
)


def _minimal_spec(**overrides: object) -> JobSpec:
    base = {
        "id": "abc123",
        "command": ["python", "run.py"],
        "cwd": "/tmp/job-abc123",
        "cpus": 1,
    }
    base.update(overrides)
    return JobSpec(**base)


class TestRoundTrip:
    def test_dump_then_parse_recovers_all_fields(self) -> None:
        spec = _minimal_spec(submitter="user@example.org", workspace_source="run.tar.gz")
        recovered = JobSpec.from_json(spec.to_json())
        assert recovered == spec

    def test_write_then_read_via_path(self, tmp_path: Path) -> None:
        spec = _minimal_spec()
        path = tmp_path / "queue" / "abc123.json"
        spec.write(path)
        assert path.exists()
        assert JobSpec.read(path) == spec

    def test_write_creates_parent_dirs(self, tmp_path: Path) -> None:
        spec = _minimal_spec()
        path = tmp_path / "deep" / "nested" / "abc123.json"
        spec.write(path)
        assert path.exists()


class TestDefaults:
    def test_state_defaults_to_pending(self) -> None:
        assert _minimal_spec().state == JobState.PENDING

    def test_submitted_at_is_iso8601_with_tz(self) -> None:
        spec = _minimal_spec()
        assert "T" in spec.submitted_at
        assert spec.submitted_at.endswith(("+00:00", "Z"))

    def test_log_paths_default_to_relative(self) -> None:
        spec = _minimal_spec()
        assert spec.stdout_path == "stdout.log"
        assert spec.stderr_path == "stderr.log"

    def test_runtime_fields_start_unset(self) -> None:
        spec = _minimal_spec()
        assert spec.pid is None
        assert spec.started_at is None
        assert spec.finished_at is None
        assert spec.exit_code is None


class TestValidation:
    def test_cpus_must_be_at_least_one(self) -> None:
        with pytest.raises(ValidationError):
            _minimal_spec(cpus=0)

    def test_command_cannot_be_empty(self) -> None:
        with pytest.raises(ValidationError):
            _minimal_spec(command=[])

    def test_unknown_state_rejected(self) -> None:
        with pytest.raises(ValidationError):
            JobSpec.model_validate({**_minimal_spec().model_dump(), "state": "fnord"})

    def test_newer_spec_version_rejected(self) -> None:
        payload = _minimal_spec().model_dump()
        payload["spec_version"] = SPEC_VERSION + 1
        with pytest.raises(ValidationError):
            JobSpec.model_validate(payload)

    @pytest.mark.parametrize("spec_version", [0, -1])
    def test_pre_v1_spec_version_rejected(self, spec_version: int) -> None:
        payload = _minimal_spec().model_dump()
        payload["spec_version"] = spec_version
        with pytest.raises(
            ValidationError,
            match=r"spec version .* is older than the oldest supported version \(1\)",
        ):
            JobSpec.model_validate(payload)

    def test_unknown_field_is_ignored_for_forward_compat(self) -> None:
        payload = _minimal_spec().model_dump()
        payload["future_field_added_in_v2"] = "whatever"
        spec = JobSpec.model_validate(payload)
        assert spec.id == "abc123"

    @pytest.mark.parametrize("field", ["pid", "pgid", "pause_intent_pgid"])
    @pytest.mark.parametrize("value", [0, -1, True, "2", 2.0])
    def test_process_identities_reject_invalid_durable_values(
        self,
        field: str,
        value: object,
    ) -> None:
        with pytest.raises(ValidationError, match=field):
            _minimal_spec(**{field: value})

        payload = _minimal_spec().model_dump(mode="json")
        payload[field] = value
        with pytest.raises(ValidationError, match=field):
            JobSpec.from_json(json.dumps(payload))

        spec = _minimal_spec()
        with pytest.raises(ValidationError, match=field):
            setattr(spec, field, value)

    @pytest.mark.parametrize("field", ["pid", "pgid", "pause_intent_pgid"])
    @pytest.mark.parametrize("value", [None, 1, 12345])
    def test_process_identities_accept_none_or_positive_strict_integers(
        self,
        field: str,
        value: int | None,
    ) -> None:
        spec = _minimal_spec(**{field: value})
        recovered = JobSpec.from_json(spec.to_json())
        assert getattr(recovered, field) == value

        setattr(spec, field, value)
        assert getattr(spec, field) == value

    @pytest.mark.parametrize(
        "value",
        [math.nan, math.inf, -math.inf, -1.0],
        ids=["nan", "positive-infinity", "negative-infinity", "negative"],
    )
    def test_paused_seconds_total_rejects_invalid_durable_values(
        self,
        value: float,
    ) -> None:
        with pytest.raises(ValidationError, match="paused_seconds_total"):
            _minimal_spec(paused_seconds_total=value)

        payload = _minimal_spec().model_dump(mode="json")
        payload["paused_seconds_total"] = value
        with pytest.raises(ValidationError, match="paused_seconds_total"):
            JobSpec.from_json(json.dumps(payload))

        spec = _minimal_spec()
        with pytest.raises(ValidationError, match="paused_seconds_total"):
            spec.paused_seconds_total = value
        assert spec.paused_seconds_total == 0.0

    @pytest.mark.parametrize("value", [0.0, 2.5])
    def test_paused_seconds_total_finite_nonnegative_round_trip(
        self,
        value: float,
    ) -> None:
        spec = _minimal_spec(paused_seconds_total=value)
        spec.paused_seconds_total = value

        recovered = JobSpec.from_json(spec.to_json())

        assert recovered.paused_seconds_total == value

    @pytest.mark.parametrize(
        "field",
        ["paused_monotonic_at", "pause_intent_monotonic_at"],
    )
    @pytest.mark.parametrize(
        "value",
        [math.nan, math.inf, -math.inf, -1.0],
        ids=["nan", "positive-infinity", "negative-infinity", "negative"],
    )
    def test_pause_monotonic_anchors_reject_invalid_durable_values(
        self,
        field: str,
        value: float,
    ) -> None:
        with pytest.raises(ValidationError, match=field):
            _minimal_spec(**{field: value})

        payload = _minimal_spec().model_dump(mode="json")
        payload[field] = value
        with pytest.raises(ValidationError, match=field):
            JobSpec.from_json(json.dumps(payload))

        spec = _minimal_spec()
        with pytest.raises(ValidationError, match=field):
            setattr(spec, field, value)
        assert getattr(spec, field) is None

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("paused_monotonic_at", None),
            ("paused_monotonic_at", 0.0),
            ("paused_monotonic_at", 2.5),
            ("pause_intent_monotonic_at", None),
            ("pause_intent_monotonic_at", 0.0),
            ("pause_intent_monotonic_at", 2.5),
        ],
    )
    def test_pause_monotonic_anchors_round_trip_valid_values(
        self,
        field: str,
        value: float | None,
    ) -> None:
        spec = _minimal_spec(**{field: value})
        setattr(spec, field, value)

        recovered = JobSpec.from_json(spec.to_json())

        assert getattr(recovered, field) == value

    def test_program_uses_safe_identifier_charset(self) -> None:
        assert _minimal_spec(program="orca-6.1").program == "orca-6.1"
        with pytest.raises(ValidationError):
            _minimal_spec(program="orca 6")

    @pytest.mark.parametrize(
        "jobid",
        [
            "a",
            "abc123def456",
            "legacy-job_1.2",
            "a" * 50,
            "a" * 51,
            "a" * 160,
        ],
    )
    def test_job_id_accepts_generated_and_legacy_safe_components(
        self, jobid: str
    ) -> None:
        assert _minimal_spec(id=jobid).id == jobid

    @pytest.mark.parametrize(
        "jobid",
        [
            "",
            ".",
            "..",
            "../escape",
            "/tmp/escape",
            "slash/in/id",
            "back\\slash",
            "has space",
            "line\nbreak",
            "shell$var",
            "unicode-é",
            "a" * 161,
        ],
    )
    def test_job_id_rejects_path_and_command_unsafe_values(
        self, jobid: str
    ) -> None:
        with pytest.raises(ValidationError):
            _minimal_spec(id=jobid)

    @pytest.mark.parametrize("field", ["depends_on", "depends_on_any"])
    @pytest.mark.parametrize(
        "jobid",
        [
            "",
            ".",
            "..",
            "../escape",
            "/tmp/escape",
            "slash/in/id",
            "back\\slash",
            "has space",
            "line\nbreak",
            "shell$var",
            "unicode-é",
            "a" * 161,
        ],
    )
    def test_dependency_ids_reject_path_and_command_unsafe_durable_values(
        self,
        field: str,
        jobid: str,
    ) -> None:
        with pytest.raises(ValidationError, match=field):
            _minimal_spec(**{field: [jobid]})

        payload = _minimal_spec().model_dump(mode="json")
        payload[field] = [jobid]
        with pytest.raises(ValidationError, match=field):
            JobSpec.from_json(json.dumps(payload))

        spec = _minimal_spec()
        with pytest.raises(ValidationError, match=field):
            setattr(spec, field, [jobid])
        assert getattr(spec, field) == []

    @pytest.mark.parametrize("field", ["depends_on", "depends_on_any"])
    def test_dependency_ids_preserve_safe_legacy_values_order_and_duplicates(
        self,
        field: str,
    ) -> None:
        values = ["pred1", "legacy-job_1.2", "pred1", "a" * 160]
        spec = _minimal_spec(**{field: values})

        recovered = JobSpec.from_json(spec.to_json())

        assert getattr(spec, field) == values
        assert getattr(recovered, field) == values

        assigned = ["next.2", "next_1", "next.2"]
        setattr(spec, field, assigned)
        assert getattr(spec, field) == assigned


class TestTerminalState:
    @pytest.mark.parametrize("state", list(TERMINAL_STATES))
    def test_terminal_states_report_terminal(self, state: JobState) -> None:
        assert _minimal_spec(state=state).is_terminal

    @pytest.mark.parametrize("state", [JobState.PENDING, JobState.RUNNING])
    def test_active_states_not_terminal(self, state: JobState) -> None:
        assert not _minimal_spec(state=state).is_terminal


class TestJSONShape:
    def test_json_has_stable_top_level_keys(self) -> None:
        text = _minimal_spec().to_json()
        data = json.loads(text)
        assert data["spec_version"] == SPEC_VERSION
        assert data["state"] == "pending"
        assert data["command"] == ["python", "run.py"]

    def test_assignment_is_validated(self) -> None:
        spec = _minimal_spec()
        with pytest.raises(ValidationError):
            spec.cpus = -1


class TestSpecV0_3Fields:
    """v0.3 added mem_mb, wall_time_seconds, pgid, last_heartbeat_at,
    plus three watchdog terminal states. All five fields are optional in
    v2 (will become required for resource-budget fields in v0.4 per
    SPEC.md), so old v1 specs read into v2 cleanly."""

    def test_new_fields_default_to_none(self) -> None:
        spec = _minimal_spec()
        assert spec.mem_mb is None
        assert spec.wall_time_seconds is None
        assert spec.pgid is None
        assert spec.last_heartbeat_at is None

    def test_new_fields_round_trip(self) -> None:
        spec = _minimal_spec(
            mem_mb=16000,
            wall_time_seconds=7200,
            pgid=12345,
            last_heartbeat_at="2026-05-09T15:30:00+00:00",
        )
        recovered = JobSpec.from_json(spec.to_json())
        assert recovered.mem_mb == 16000
        assert recovered.wall_time_seconds == 7200
        assert recovered.pgid == 12345
        assert recovered.last_heartbeat_at == "2026-05-09T15:30:00+00:00"

    def test_mem_mb_must_be_positive(self) -> None:
        with pytest.raises(ValidationError):
            _minimal_spec(mem_mb=0)

    def test_wall_time_seconds_must_be_positive(self) -> None:
        with pytest.raises(ValidationError):
            _minimal_spec(wall_time_seconds=0)


class TestWatchdogTerminalStates:
    @pytest.mark.parametrize(
        "state",
        [JobState.OOM_KILLED, JobState.STARVED, JobState.TIME_EXCEEDED],
    )
    def test_watchdog_states_are_terminal(self, state: JobState) -> None:
        spec = _minimal_spec(state=state)
        assert spec.is_terminal
        assert spec.is_watchdog_killed

    def test_manual_killed_is_terminal_but_not_watchdog(self) -> None:
        spec = _minimal_spec(state=JobState.KILLED)
        assert spec.is_terminal
        assert not spec.is_watchdog_killed

    def test_completed_is_terminal_but_not_watchdog(self) -> None:
        spec = _minimal_spec(state=JobState.COMPLETED)
        assert spec.is_terminal
        assert not spec.is_watchdog_killed

    def test_running_is_neither(self) -> None:
        spec = _minimal_spec(state=JobState.RUNNING)
        assert not spec.is_terminal
        assert not spec.is_watchdog_killed

    def test_watchdog_states_subset_of_terminal_states(self) -> None:
        # Catch a future bug where someone adds a watchdog state without
        # also adding it to TERMINAL_STATES.
        assert WATCHDOG_TERMINAL_STATES <= TERMINAL_STATES


class TestV1ToV2Migration:
    """v1 specs on disk lack mem_mb / wall_time_seconds / pgid /
    last_heartbeat_at. Loading them into the v2 model must yield a clean
    spec with those fields defaulted to None and spec_version preserved."""

    def test_v1_spec_loads_into_v2_with_defaults(self) -> None:
        v1_payload = {
            "spec_version": 1,
            "id": "old-job-001",
            "command": ["python", "old_run.py"],
            "cwd": "/tmp/old",
            "cpus": 4,
            "state": "completed",
            "submitted_at": "2026-04-01T12:00:00+00:00",
            "submitter": "test_user@old-host",
            "pid": 99,
            "started_at": "2026-04-01T12:00:01+00:00",
            "finished_at": "2026-04-01T12:00:50+00:00",
            "exit_code": 0,
            "stdout_path": "stdout.log",
            "stderr_path": "stderr.log",
        }
        spec = JobSpec.model_validate(v1_payload)
        assert spec.spec_version == 1  # preserved, NOT silently bumped
        assert spec.mem_mb is None
        assert spec.wall_time_seconds is None
        assert spec.pgid is None
        assert spec.last_heartbeat_at is None
        assert spec.state == JobState.COMPLETED
        assert spec.cpus == 4

    def test_v1_spec_preserves_version_through_round_trip(self) -> None:
        v1_payload = {
            "spec_version": 1,
            "id": "old-job-002",
            "command": ["true"],
            "cwd": "/tmp/old2",
            "cpus": 1,
        }
        spec = JobSpec.model_validate(v1_payload)
        # Re-serialise: spec_version stays 1 because the loaded value wins
        # over the class default. Old specs do NOT auto-upgrade just by
        # being read.
        assert spec.spec_version == 1
        recovered = JobSpec.from_json(spec.to_json())
        assert recovered.spec_version == 1

    def test_v2_spec_default_carries_v2_version(self) -> None:
        # Newly created specs (no version override) get the current version.
        spec = _minimal_spec()
        assert spec.spec_version == SPEC_VERSION == 2


class TestAbortedByQueueState:
    """v0.4.1 added ABORTED_BY_QUEUE for orphan-loss / queue-side
    terminations. Distinguished from KILLED (user) and watchdog states
    (OOM_KILLED / STARVED / TIME_EXCEEDED) and from INTERRUPTED (legacy
    v0.3 catchall, kept for backward compatibility with old specs)."""

    def test_aborted_by_queue_is_terminal(self) -> None:
        spec = _minimal_spec(state=JobState.ABORTED_BY_QUEUE)
        assert spec.is_terminal

    def test_aborted_by_queue_is_not_watchdog_killed(self) -> None:
        # The watchdog distinction matters for retry policies that
        # target only watchdog-attributed deaths.
        spec = _minimal_spec(state=JobState.ABORTED_BY_QUEUE)
        assert not spec.is_watchdog_killed

    def test_aborted_by_queue_round_trips(self) -> None:
        spec = _minimal_spec(state=JobState.ABORTED_BY_QUEUE)
        recovered = JobSpec.from_json(spec.to_json())
        assert recovered.state == JobState.ABORTED_BY_QUEUE
        assert recovered.is_terminal

    def test_aborted_by_queue_in_terminal_states_constant(self) -> None:
        assert JobState.ABORTED_BY_QUEUE in TERMINAL_STATES


class TestJobName:
    """v0.5.34: optional human-readable label. Charset is strict so the
    name flows into archive filenames and ssh-shipped argv without
    needing quoting; jobid stays the canonical addressing key.
    """

    def test_default_is_none(self) -> None:
        spec = _minimal_spec()
        assert spec.job_name is None

    def test_accepts_valid_name(self) -> None:
        spec = _minimal_spec(job_name="mgo-pbe-rev2")
        assert spec.job_name == "mgo-pbe-rev2"

    @pytest.mark.parametrize(
        "name",
        [
            "x",                              # single char minimum
            "a-b_c.d",                        # all three special chars
            "MgO_PBE-rev2.run3",              # mixed case
            "ABC123",                         # alnum only
            "a" * 50,                         # max length
        ],
    )
    def test_accepts_valid_charset(self, name: str) -> None:
        spec = _minimal_spec(job_name=name)
        assert spec.job_name == name

    @pytest.mark.parametrize(
        "bad",
        [
            "",                               # empty
            "a" * 51,                         # too long
            "has space",                      # whitespace
            "slash/in/name",                  # path separator
            "back\\slash",                    # backslash
            "shell$var",                      # shell metachar
            "name;rm",                        # shell separator
            "name>out",                       # redirect
            "name|pipe",                      # pipe
            "unicode-é",                      # non-ASCII
            "tab\there",                      # tab
        ],
    )
    def test_rejects_invalid_charset(self, bad: str) -> None:
        with pytest.raises(ValidationError):
            _minimal_spec(job_name=bad)

    def test_round_trip(self) -> None:
        spec = _minimal_spec(job_name="mgo-pbe-rev2")
        recovered = JobSpec.from_json(spec.to_json())
        assert recovered.job_name == "mgo-pbe-rev2"

    def test_old_spec_without_job_name_reads_clean(self) -> None:
        """Pre-v0.5.34 specs lack the field; pydantic default makes it
        None on load — additive field, no migration needed."""
        old_payload = {
            "spec_version": 2,
            "id": "old001",
            "command": ["true"],
            "cwd": "/tmp/old",
            "cpus": 1,
        }
        spec = JobSpec.model_validate(old_payload)
        assert spec.job_name is None

    def test_dest_dirname_unnamed_uses_jobid_only(self) -> None:
        """Pre-v0.5.34 shape preserved when job_name is None."""
        spec = _minimal_spec()
        assert spec.dest_dirname == spec.id

    def test_dest_dirname_named_prefixes_jobid(self) -> None:
        spec = _minimal_spec(job_name="mgo-run")
        assert spec.dest_dirname == f"mgo-run-{spec.id}"

    def test_dest_dirname_is_filesystem_safe(self) -> None:
        """The dest_dirname is what ends up in archive filenames + fetch
        destination directories. The strict charset on job_name ensures
        the result has no slashes / spaces / shell metacharacters —
        catch a future regression of either the charset or the
        formatting."""
        import re
        spec = _minimal_spec(job_name="a-b_c.d")
        # Same charset as job_name itself, no surprises from formatting.
        assert re.fullmatch(r"[A-Za-z0-9._-]+", spec.dest_dirname)


class TestBranchField:
    """v0.5.47: JobSpec.branch optional field — additive, must round-
    trip through JSON, must default None on old specs."""

    def test_default_is_none(self) -> None:
        spec = _minimal_spec()
        assert spec.branch is None

    def test_explicit_branch_round_trips(self) -> None:
        spec = _minimal_spec(branch="release")
        recovered = JobSpec.from_json(spec.to_json())
        assert recovered.branch == "release"

    def test_pre_v0_5_47_spec_loads_clean(self) -> None:
        """A spec on disk that predates the branch field has no
        `branch` key; reading it must default to None rather than
        raising."""
        import json
        spec = _minimal_spec()
        data = json.loads(spec.to_json())
        data.pop("branch", None)
        recovered = JobSpec.from_json(json.dumps(data))
        assert recovered.branch is None


class TestTagsField:
    """v0.6.6: JobSpec.tags optional list[str] — additive, round-
    trips through JSON, deduped + sorted at validate, charset enforced,
    pre-v0.6.6 specs read clean."""

    def test_default_is_empty_list(self) -> None:
        spec = _minimal_spec()
        assert spec.tags == []

    def test_tags_deduped_and_sorted(self) -> None:
        spec = _minimal_spec(tags=["zebra", "apple", "zebra", "mango"])
        # Validator dedupes + sorts.
        assert spec.tags == ["apple", "mango", "zebra"]

    def test_invalid_tag_charset_rejected(self) -> None:
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            _minimal_spec(tags=["has space"])
        with pytest.raises(ValidationError):
            _minimal_spec(tags=["foo/bar"])

    def test_tags_round_trip_through_json(self) -> None:
        spec = _minimal_spec(tags=["experiment-12", "basisset-dev"])
        recovered = JobSpec.from_json(spec.to_json())
        # Sorted at validate, so order is alphabetical.
        assert recovered.tags == ["basisset-dev", "experiment-12"]

    def test_pre_v0_6_6_spec_loads_clean(self) -> None:
        """A spec on disk that predates the tags field has no
        `tags` key; reading it must default to [] rather than
        raising."""
        import json
        spec = _minimal_spec()
        data = json.loads(spec.to_json())
        data.pop("tags", None)
        recovered = JobSpec.from_json(json.dumps(data))
        assert recovered.tags == []
