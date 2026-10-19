"""Where campaign controllers run, so the console can draw them (#165).

``vq campaign run`` records each run directory it drives under the
controller user's vq state, as ``campaigns/<key>.json``. The fleet console
served by that user on that machine reads these registrations to show each
campaign's graph. A registration holds the campaign's id and digest and the
absolute paths of its campaign file and run directory, nothing else: no
campaign content and no credentials. Deleting one only hides its campaign
from the console.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Final, Literal

from pydantic import BaseModel, ConfigDict, ValidationError, field_validator

from vq import paths
from vq.campaign import Campaign, Digest, Label, campaign_digest

REGISTRATION_SCHEMA: Final = "vq.campaign-registration/1"
#: The console shows at most this many, newest first.
MAX_REGISTRATIONS = 100
_MAX_BYTES = 64 * 1024


class Registration(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    schema_name: Literal["vq.campaign-registration/1"]
    campaign_id: Label
    campaign_sha256: Digest
    campaign_file: str
    run_dir: str
    registered_at: str

    @field_validator("campaign_file", "run_dir")
    @classmethod
    def _absolute(cls, value: str) -> str:
        if not value.startswith("/") or "\0" in value or len(value) > 4096:
            raise ValueError("must be an absolute path")
        return value


def registry_dir() -> Path:
    return paths.state_root() / "campaigns"


def register(campaign: Campaign, campaign_file: Path, run_dir: Path) -> Path:
    """Record that a controller drives ``run_dir``; rewriting it is harmless."""
    from vq.campaign_controller import _write_atomic

    run_dir = run_dir.resolve()
    registration = Registration(
        schema_name=REGISTRATION_SCHEMA, campaign_id=campaign.campaign_id,
        campaign_sha256=campaign_digest(campaign),
        campaign_file=str(campaign_file.resolve()), run_dir=str(run_dir),
        registered_at=datetime.now(UTC).isoformat(timespec="seconds"),
    )
    directory = registry_dir()
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{hashlib.sha256(str(run_dir).encode()).hexdigest()[:24]}.json"
    _write_atomic(path, json.dumps(registration.model_dump(), indent=2, sort_keys=True) + "\n")
    return path


def registrations() -> tuple[list[Registration], list[str]]:
    """Every readable registration, newest first, and one line per unreadable one."""
    directory = registry_dir()
    found: list[Registration] = []
    problems: list[str] = []
    try:
        entries = sorted(directory.glob("*.json"))
    except OSError as exc:
        return [], [f"cannot list {directory.name}/: {exc.strerror or exc}"]
    for path in entries:
        try:
            if path.is_symlink() or not path.is_file():
                raise ValueError("not a regular file")
            with path.open("rb") as handle:
                data = handle.read(_MAX_BYTES + 1)
            if len(data) > _MAX_BYTES:
                raise ValueError(f"larger than {_MAX_BYTES} bytes")
            found.append(Registration.model_validate_json(data))
        except (OSError, ValueError, ValidationError) as exc:
            reason = exc.errors()[0]["msg"] if isinstance(exc, ValidationError) else str(exc)
            problems.append(f"registration {path.name}: {reason}")
    found.sort(key=lambda r: r.registered_at, reverse=True)
    return found[:MAX_REGISTRATIONS], problems
