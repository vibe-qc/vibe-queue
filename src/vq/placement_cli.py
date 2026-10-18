"""`vq placement`: explain completion-time placement over recorded target capabilities."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import click

_FILE = click.Path(exists=True, dir_okay=False, path_type=Path)


def _parse_now(_ctx: click.Context, _param: click.Parameter, value: str | None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        raise click.BadParameter(f"not an ISO 8601 time: {value!r}") from None
    if parsed.tzinfo is None:
        raise click.BadParameter("give a zone offset, e.g. 2026-10-12T12:00:00+00:00")
    return parsed


@click.group("placement")
def placement() -> None:
    """Explain where a job would run under the completion-time policy (#160).

    Read-only and local: reads a fleet capability file and a request file,
    and prints the decision with every rejection and score component. It
    submits nothing, reserves nothing and contacts no host. Broker
    placement and `vq submit auto` are unchanged. See
    docs/placement_policy.md.
    """


@placement.command("explain")
@click.argument("fleet_file", type=_FILE)
@click.argument("request_file", type=_FILE)
@click.option("--json", "as_json", is_flag=True, help="Print the full explanation as JSON.")
@click.option("--now", callback=_parse_now,
              help="Evaluate data age at this ISO 8601 time instead of the current time.")
@click.option("--max-age", "max_age_seconds", default=300.0, show_default=True,
              type=click.FloatRange(min=0, min_open=True, max=86400),
              help="Seconds after which a target's observation is stale and it is rejected.")
def explain_cmd(fleet_file: Path, request_file: Path, as_json: bool, now: datetime,
                max_age_seconds: float) -> None:
    """Explain the placement of REQUEST_FILE over FLEET_FILE.

    Hard requirements are checked first; a target that fails any is
    rejected with every reason. The rest are ranked by predicted
    completion time: staging + queue wait + execution + result transfer.
    Unknown components are named and ranked after complete predictions.
    With no eligible target the job is reported held.
    """
    from vq.placement_policy import (
        explain,
        explanation_json,
        explanation_text,
        load_fleet,
        load_request,
    )

    try:
        result = explain(load_fleet(fleet_file), load_request(request_file),
                         now=now, max_age_seconds=max_age_seconds)
    except ValueError as exc:
        raise click.ClickException(str(exc)) from None
    click.echo(explanation_json(result) if as_json else explanation_text(result), nl=False)
