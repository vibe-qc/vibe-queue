"""v0.6.14 JobSpec additions for vibe-qc output integration.

Pins the contract for the three additive fields:

  * ``expected_outputs`` — list of workspace-relative artefact paths
    the job is declared to produce.
  * ``output_stem`` — bare basename of the output file family
    (e.g. ``"output-h2o"``).
  * ``last_output_status`` — most-recently-observed value of
    ``[outputs].status`` from the job's ``{stem}.system`` manifest.

All three are additive — pre-v0.6.14 specs read clean (defaults
empty list / None). No SPEC_VERSION bump needed.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from vq.spec import JobSpec


def _minimal_spec(**overrides: object) -> JobSpec:
    base = {
        "id": "abc123",
        "command": ["python", "input.py"],
        "cwd": "/tmp/job-abc123",
        "cpus": 1,
    }
    base.update(overrides)
    return JobSpec(**base)


# ---------------------------------------------------------------------- #
# Defaults — additive, backward-compat
# ---------------------------------------------------------------------- #

def test_expected_outputs_defaults_to_empty_list() -> None:
    spec = _minimal_spec()
    assert spec.expected_outputs == []


def test_output_stem_defaults_to_none() -> None:
    spec = _minimal_spec()
    assert spec.output_stem is None


def test_last_output_status_defaults_to_none() -> None:
    spec = _minimal_spec()
    assert spec.last_output_status is None


def test_pre_v0_6_14_spec_reads_clean() -> None:
    """A spec dict from before v0.6.14 (no expected_outputs / output_stem
    / last_output_status keys at all) must load without error."""
    payload = {
        "id": "abc123",
        "command": ["python", "x.py"],
        "cwd": "/tmp/job-abc123",
        "cpus": 1,
    }
    spec = JobSpec.model_validate(payload)
    assert spec.expected_outputs == []
    assert spec.output_stem is None


# ---------------------------------------------------------------------- #
# Round-trip
# ---------------------------------------------------------------------- #

def test_expected_outputs_round_trips_via_json() -> None:
    spec = _minimal_spec(
        expected_outputs=["output-h2o.out", "output-h2o.molden",
                          "output-h2o.system", "output-h2o.xyz",
                          "output-h2o.bibtex"],
        output_stem="output-h2o",
        last_output_status="complete",
    )
    text = spec.to_json()
    recovered = JobSpec.from_json(text)
    assert recovered.expected_outputs == spec.expected_outputs
    assert recovered.output_stem == "output-h2o"
    assert recovered.last_output_status == "complete"


def test_expected_outputs_round_trips_via_file(tmp_path: Path) -> None:
    spec = _minimal_spec(
        expected_outputs=["a.out", "a.molden"],
        output_stem="a",
        last_output_status="running",
    )
    path = tmp_path / "abc123.json"
    spec.write(path)
    re = JobSpec.read(path)
    assert re.expected_outputs == ["a.out", "a.molden"]
    assert re.output_stem == "a"
    assert re.last_output_status == "running"


def test_json_carries_new_keys() -> None:
    spec = _minimal_spec(
        expected_outputs=["x.out"],
        output_stem="x",
        last_output_status="dry_run",
    )
    data = json.loads(spec.to_json())
    assert data["expected_outputs"] == ["x.out"]
    assert data["output_stem"] == "x"
    assert data["last_output_status"] == "dry_run"


# ---------------------------------------------------------------------- #
# Status values
# ---------------------------------------------------------------------- #

@pytest.mark.parametrize(
    "status", ["running", "complete", "crashed", "dry_run"],
)
def test_last_output_status_accepts_documented_values(
    status: str,
) -> None:
    """The four states the vibe-qc output module emits must all
    round-trip cleanly. No enum constraint by design — the field is
    a free string so vibe-qc can extend its status vocabulary
    without bumping the vq SPEC_VERSION."""
    spec = _minimal_spec(last_output_status=status)
    assert spec.last_output_status == status
