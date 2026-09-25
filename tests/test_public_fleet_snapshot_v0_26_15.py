"""#66: the public read-only fleet snapshot, schema ``vq.public-fleet/1``.

P0 pins the contract: the schema's allowlist, the ``public.<alias>`` tag
spelling and the ``[public_snapshot]`` operator config.
"""
from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from vq import config
from vq.spec import JobSpec
from vq.web import public_snapshot as ps


def _objects(schema: object) -> list[dict]:
    """Every JSON-Schema node of ``type: object`` in ``schema``."""
    found: list[dict] = []
    if isinstance(schema, dict):
        if schema.get("type") == "object":
            found.append(schema)
        for value in schema.values():
            found.extend(_objects(value))
    elif isinstance(schema, list):
        for value in schema:
            found.extend(_objects(value))
    return found


class TestSchema:
    def test_every_object_forbids_additional_properties(self) -> None:
        objects = _objects(ps.PUBLIC_SNAPSHOT_JSON_SCHEMA)
        assert len(objects) == 3  # the snapshot, a host card, a calculation
        for node in objects:
            assert node["additionalProperties"] is False
            assert set(node["required"]) == set(node["properties"])

    def test_the_allowlist_is_exactly_the_decided_fields(self) -> None:
        props = ps.PUBLIC_SNAPSHOT_JSON_SCHEMA["properties"]
        assert set(props) == {
            "schema", "generated_at", "stale_after", "title", "hosts", "calculations",
        }
        assert set(props["hosts"]["items"]["properties"]) == {"name", "state", "load"}
        assert set(props["calculations"]["items"]["properties"]) == {
            "alias", "state", "host",
        }
        assert props["schema"] == {"const": "vq.public-fleet/1"}

    def test_schema_is_a_valid_draft_2020_12_schema(self) -> None:
        jsonschema = pytest.importorskip("jsonschema")
        jsonschema.Draft202012Validator.check_schema(ps.PUBLIC_SNAPSHOT_JSON_SCHEMA)

    def test_a_smuggled_field_fails_validation(self) -> None:
        jsonschema = pytest.importorskip("jsonschema")
        doc = {
            "schema": ps.SCHEMA_ID,
            "generated_at": "2026-09-25T12:00:00+00:00",
            "stale_after": "2026-09-25T12:15:00+00:00",
            "title": None,
            "hosts": [{"name": "Cluster A", "state": "up", "load": "idle"}],
            "calculations": [{"alias": "a", "state": "running", "host": None}],
        }
        validator = jsonschema.Draft202012Validator(ps.PUBLIC_SNAPSHOT_JSON_SCHEMA)
        validator.validate(doc)
        doc["hosts"][0]["error"] = "ssh: connect refused"
        with pytest.raises(jsonschema.ValidationError):
            validator.validate(doc)


def _spec(tags: list[str]) -> JobSpec:
    return JobSpec(id="0123456789ab", host="localhost", command=["true"],
                   cwd="/tmp/example", cpus=1, tags=tags)


class TestTagSpelling:
    def test_the_documented_tag_passes_the_spec_validator(self) -> None:
        # The whole point of the spelling: it must be a legal JobSpec tag.
        spec = _spec(["public.water-dimer-scan"])
        assert ps.public_alias(spec.tags) == "water-dimer-scan"

    def test_the_equals_form_is_not_a_legal_tag(self) -> None:
        _spec(["public.water-dimer-scan"])  # the same spec is otherwise valid
        with pytest.raises(ValidationError) as info:
            _spec(["public=water-dimer-scan"])
        assert [err["loc"] for err in info.value.errors()] == [("tags",)]

    def test_the_longest_legal_tag_yields_the_longest_alias(self) -> None:
        tag = ps.PUBLIC_TAG_PREFIX + "a" * ps.PUBLIC_ALIAS_MAX_LEN
        spec = _spec([tag])
        assert ps.public_alias(spec.tags) == "a" * ps.PUBLIC_ALIAS_MAX_LEN
        with pytest.raises(ValidationError):
            _spec([tag + "a"])
        assert ps.public_alias([tag + "a"]) is None

    @pytest.mark.parametrize("tags", [
        [], ["science"], ["public"], ["public."], ["Public.x"], ["public-x"],
        ["public.a", "public.b"], ["public.a b"], "public.x", None, 7,
    ])
    def test_no_opt_in(self, tags: object) -> None:
        assert ps.public_alias(tags) is None

    def test_other_tags_do_not_disturb_the_opt_in(self) -> None:
        assert ps.public_alias(["pr-request", "public.x", "public.x"]) == "x"


class TestOperatorConfig:
    def _load(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, body: str) -> config.Config:
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path))
        (tmp_path / "config.toml").write_text(body)
        return config.load_config()

    def test_absent_section_names_no_host(self, tmp_path: Path,
                                          monkeypatch: pytest.MonkeyPatch) -> None:
        cfg = self._load(tmp_path, monkeypatch, "")
        assert cfg.public_snapshot.hosts == {}
        assert cfg.public_snapshot.title is None
        assert cfg.public_snapshot.stale_after_seconds == 900

    def test_display_names_load(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        cfg = self._load(tmp_path, monkeypatch, (
            '[public_snapshot]\ntitle = " Example fleet "\nstale_after_seconds = 600\n'
            '[public_snapshot.hosts]\ncluster-a = "Cluster A"\n'
        ))
        assert cfg.public_snapshot.hosts == {"cluster-a": "Cluster A"}
        assert cfg.public_snapshot.title == "Example fleet"
        assert cfg.public_snapshot.stale_after_seconds == 600

    @pytest.mark.parametrize("body", [
        '[public_snapshot.hosts]\na = "Same"\nb = "same"\n',
        '[public_snapshot.hosts]\na = "  "\n',
        '[public_snapshot.hosts]\na = "' + "x" * 65 + '"\n',
        '[public_snapshot.hosts]\na = "bad\\u0007name"\n',
        '[public_snapshot]\nstale_after_seconds = 5\n',
        '[public_snapshot]\ndisplay_names = {}\n',
    ])
    def test_invalid_config_is_a_load_error(self, tmp_path: Path,
                                            monkeypatch: pytest.MonkeyPatch,
                                            body: str) -> None:
        with pytest.raises(config.ConfigError):
            self._load(tmp_path, monkeypatch, body)


# --------------------------------------------------------------------------
# P1: the projection. Every value the public page must never show carries
# the marker below, so one substring check covers every field at once --
# including fields nobody thought to name in an assertion.
# --------------------------------------------------------------------------

LEAK = "ZZLEAK"


def _marked_spec(job_id: str, *, tags: list[str], state: str = "running",
                 scheduler_target: str | None = None) -> JobSpec:
    return JobSpec(
        id=job_id,
        command=[f"/opt/{LEAK}-bin/vibe-qc", "run", f"{LEAK}-argv.py"],
        cwd=f"/srv/{LEAK}-workspace/{job_id}",
        cpus=4,
        workdir=f"/scratch/{LEAK}-workdir",
        state=state,
        submitter=f"{LEAK}-submitter",
        job_name=f"{LEAK}-job-name",
        program=f"{LEAK}-program",
        failure_reason=f"{LEAK}-failure",
        failure_tail=f"{LEAK}-log-tail E(RHF) = -76.0",
        stdout_path=f"/srv/{LEAK}-stdout.log",
        stderr_path=f"/srv/{LEAK}-stderr.log",
        branch=f"{LEAK}-branch",
        scheduler_target=scheduler_target,
        scheduler_job_id=f"{LEAK}-pbs-id" if scheduler_target else None,
        scheduler_exec_host=f"{LEAK}-node-192.0.2.61" if scheduler_target else None,
        tags=tags,
    )


def _marked_row(spec: JobSpec, queue_host: str) -> dict:
    from vq.web import fleet

    row = fleet._job_row(spec, queue_host)
    row["queue_host"] = queue_host
    # Fields a newer remote vq, or a preview, might add to a row.
    row["stdout_tail"] = f"{LEAK}-stdout-line"
    row["energy_hartree"] = f"{LEAK}-energy -76.026"
    row["structure_xyz"] = f"{LEAK}-structure O 0 0 0"
    row["ssh_alias"] = f"{LEAK}-ssh-alias"
    return row


def _marked_overview(host: str, **fields: object):
    from vq import overview

    base: dict = dict(
        host=host,
        vq_version=f"{LEAK}-version",
        reported_hostname=f"{LEAK}-reported-hostname.invalid",
        helper_source_sha=f"{LEAK}-sha",
        running_cpus=0,
        pending_cpus=0,
        max_cpus=16,
    )
    base.update(fields)
    return overview.HostOverview(**base)


# Host keys. Each is forbidden in the output; only display names may appear.
KEY_BUSY = f"{LEAK}-key-cluster-a"
KEY_DOWN = f"{LEAK}-key-workstation"
KEY_UNREACHABLE = f"{LEAK}-key-192.0.2.17"
KEY_UNNAMED = f"{LEAK}-key-2001:db8::5"
KEY_SCHED = f"{LEAK}-key-pbs.example.com"
KEY_MISSING = f"{LEAK}-key-retired"


def _marked_fleet():
    from vq.web.fleet import FleetSnapshot, HostSnapshot

    busy_jobs = [
        _marked_row(_marked_spec(f"{LEAK}-job-public-run", tags=["public.water-dimer"]),
                    KEY_BUSY),
        _marked_row(_marked_spec(f"{LEAK}-job-private", tags=[f"{LEAK}-private-tag"]),
                    KEY_BUSY),
        _marked_row(_marked_spec(f"{LEAK}-job-done", tags=["public.benzene-opt"],
                                 state="completed"), KEY_BUSY),
        _marked_row(_marked_spec(f"{LEAK}-job-ambiguous",
                                 tags=["public.one", "public.two"]), KEY_BUSY),
    ]
    unnamed_jobs = [
        _marked_row(_marked_spec(f"{LEAK}-job-on-unnamed", tags=["public.hidden-host-run"]),
                    KEY_UNNAMED),
        _marked_row(_marked_spec(f"{LEAK}-job-unnamed-private", tags=[]), KEY_UNNAMED),
    ]
    sched_jobs = [
        _marked_row(_marked_spec(f"{LEAK}-job-sched", tags=["public.cluster-scan"],
                                 scheduler_target=KEY_SCHED), KEY_SCHED),
    ]
    return FleetSnapshot(
        gathered_at="2026-09-25T12:00:00+00:00",
        duration_seconds=3.5,
        hosts=[
            HostSnapshot(host=KEY_BUSY, overview=_marked_overview(
                KEY_BUSY, running_cpus=12, pending_cpus=4), jobs=busy_jobs),
            HostSnapshot(host=KEY_DOWN, overview=_marked_overview(
                KEY_DOWN, reachable=False, admin_down=f"{LEAK}-admin-down-reason",
                error=f"{LEAK}-error")),
            HostSnapshot(host=KEY_UNREACHABLE, overview=_marked_overview(
                KEY_UNREACHABLE, reachable=False,
                error=f"ssh: connect to host {LEAK}-192.0.2.17 port 22: refused"),
                jobs_error=f"{LEAK}-jobs-error"),
            HostSnapshot(host=KEY_UNNAMED, overview=_marked_overview(
                KEY_UNNAMED, running_cpus=8), jobs=unnamed_jobs),
            HostSnapshot(host=KEY_SCHED, overview=_marked_overview(
                KEY_SCHED, is_scheduler_host=True, max_cpus=None, running_cpus=4),
                jobs=sched_jobs),
        ],
    )


def _public_config() -> config.PublicSnapshotConfig:
    return config.PublicSnapshotConfig(
        title="Example fleet",
        hosts={
            KEY_BUSY: "Cluster A",
            KEY_DOWN: "Workstation",
            KEY_UNREACHABLE: "Laptop",
            KEY_SCHED: "Batch cluster",
            KEY_MISSING: "Retired box",
        },
    )


def _serialized(doc: dict) -> str:
    import json

    return json.dumps(doc, sort_keys=True)


class TestProjectionPrivacy:
    def test_the_fixture_really_carries_the_markers(self) -> None:
        # Guards the guard: if the fixture lost its markers, the leak test
        # below would pass vacuously.
        snapshot = _marked_fleet()
        dumped = _serialized({"s": snapshot.to_json(), "jobs": snapshot.jobs})
        for probe in ("key-", "job-", "argv", "workspace", "submitter", "log-tail",
                      "energy", "structure", "ssh-alias", "reported-hostname",
                      "admin-down-reason", "error", "192.0.2.17", "2001:db8::5",
                      "private-tag", "version", "node-192.0.2.61"):
            assert f"{LEAK}-" in dumped and probe in dumped, probe

    def test_no_forbidden_value_reaches_the_output(self) -> None:
        doc = ps.project_public_snapshot(_marked_fleet(), _public_config())
        text = _serialized(doc)
        assert LEAK.lower() not in text.lower(), text
        # Values without the marker that are forbidden all the same.
        for forbidden in ("192.0.2.", "2001:db8", "vibe-qc", "/srv/", "/scratch/",
                          "-76.0", "E(RHF)", "public.", "invalid"):
            assert forbidden not in text, forbidden

    def test_the_output_is_exactly_the_expected_public_view(self) -> None:
        # The positive half: the leak test must not pass on an empty page.
        doc = ps.project_public_snapshot(_marked_fleet(), _public_config())
        assert doc == {
            "schema": "vq.public-fleet/1",
            "generated_at": "2026-09-25T12:00:00+00:00",
            "stale_after": "2026-09-25T12:15:00+00:00",
            "title": "Example fleet",
            "hosts": [
                {"name": "Cluster A", "state": "up", "load": "busy"},
                {"name": "Workstation", "state": "down", "load": "unknown"},
                {"name": "Laptop", "state": "down", "load": "unknown"},
                {"name": "Batch cluster", "state": "up", "load": "active"},
                {"name": "Retired box", "state": "unknown", "load": "unknown"},
            ],
            "calculations": [
                {"alias": "benzene-opt", "state": "finished", "host": "Cluster A"},
                # Handed to the scheduler, never polled: ``unpolled``, not running.
                {"alias": "cluster-scan", "state": "queued", "host": "Batch cluster"},
                {"alias": "hidden-host-run", "state": "running", "host": None},
                {"alias": "water-dimer", "state": "running", "host": "Cluster A"},
            ],
        }

    def test_the_output_validates_against_the_schema(self) -> None:
        jsonschema = pytest.importorskip("jsonschema")
        doc = ps.project_public_snapshot(_marked_fleet(), _public_config())
        jsonschema.Draft202012Validator(
            ps.PUBLIC_SNAPSHOT_JSON_SCHEMA,
            format_checker=jsonschema.Draft202012Validator.FORMAT_CHECKER,
        ).validate(doc)

    def test_only_opted_in_jobs_appear(self) -> None:
        # Seven jobs in the fixture; four carry exactly one valid public tag.
        snapshot = _marked_fleet()
        assert len(snapshot.jobs) == 7
        doc = ps.project_public_snapshot(snapshot, _public_config())
        assert sorted(c["alias"] for c in doc["calculations"]) == [
            "benzene-opt", "cluster-scan", "hidden-host-run", "water-dimer",
        ]

    def test_no_display_names_means_no_hosts(self) -> None:
        doc = ps.project_public_snapshot(_marked_fleet(), config.PublicSnapshotConfig())
        assert doc["hosts"] == []
        assert {c["host"] for c in doc["calculations"]} == {None}
        assert LEAK.lower() not in _serialized(doc).lower()

    def test_a_job_listed_under_two_keys_appears_once(self) -> None:
        from vq.web.fleet import FleetSnapshot, HostSnapshot

        row = _marked_row(_marked_spec(f"{LEAK}-job-dup", tags=["public.dup"]), KEY_BUSY)
        snapshot = FleetSnapshot(gathered_at="2026-09-25T12:00:00+00:00", duration_seconds=1,
                                 hosts=[HostSnapshot(KEY_BUSY, _marked_overview(KEY_BUSY),
                                                     jobs=[row]),
                                        HostSnapshot(KEY_DOWN, _marked_overview(KEY_DOWN),
                                                     jobs=[dict(row)])])
        doc = ps.project_public_snapshot(snapshot, _public_config())
        assert doc["calculations"] == [{"alias": "dup", "state": "running",
                                        "host": "Cluster A"}]


class TestProjectionBuckets:
    @pytest.mark.parametrize(("running", "pending", "max_cpus", "load"), [
        (0, 0, 16, "idle"),
        (0, 0, None, "idle"),
        (0, 4, 16, "light"),
        (7, 0, 16, "light"),
        (8, 0, 16, "busy"),
        (14, 0, 16, "busy"),
        (15, 0, 16, "full"),
        (32, 0, 16, "full"),
        (3, 0, None, "active"),
        (0, 2, 0, "active"),
    ])
    def test_load(self, running: int, pending: int, max_cpus: int | None, load: str) -> None:
        from vq.web.fleet import FleetSnapshot, HostSnapshot

        ov = _marked_overview("k", running_cpus=running, pending_cpus=pending,
                              max_cpus=max_cpus)
        doc = ps.project_public_snapshot(
            FleetSnapshot("2026-09-25T12:00:00+00:00", 0.1, [HostSnapshot("k", ov)]),
            config.PublicSnapshotConfig(hosts={"k": "K"}),
        )
        assert doc["hosts"] == [{"name": "K", "state": "up", "load": load}]

    @pytest.mark.parametrize(("raw", "public"), [
        ("pending", "queued"), ("queued", "queued"), ("held", "queued"),
        ("running", "running"), ("suspended", "paused"), ("completed", "finished"),
        ("oom_killed", "failed"), ("aborted_by_queue", "failed"), ("killed", "cancelled"),
        ("poll_failed", "unknown"), ("scheduler_unknown", "unknown"),
        ("a-state-from-the-future", "unknown"), (None, "unknown"), (7, "unknown"),
    ])
    def test_lifecycle_state(self, raw: object, public: str) -> None:
        from vq.web.fleet import FleetSnapshot, HostSnapshot

        row = {"id": "x", "tags": ["public.a"], "effective_state": raw, "state": raw}
        doc = ps.project_public_snapshot(
            FleetSnapshot("2026-09-25T12:00:00+00:00", 0.1,
                          [HostSnapshot("k", _marked_overview("k"), jobs=[row])]),
            config.PublicSnapshotConfig(),
        )
        assert doc["calculations"] == [{"alias": "a", "state": public, "host": None}]
        assert public in ps.CALCULATION_STATES

    def test_stale_after_follows_the_config(self) -> None:
        from vq.web.fleet import FleetSnapshot

        doc = ps.project_public_snapshot(
            FleetSnapshot("2026-09-25T12:00:00+00:00", 0.1, []),
            config.PublicSnapshotConfig(stale_after_seconds=3600),
        )
        assert doc["stale_after"] == "2026-09-25T13:00:00+00:00"
