"""`vq campaign`: validate campaigns, track their progression and verify artifacts."""

from __future__ import annotations

import json
from pathlib import Path

import click

_FILE = click.Path(exists=True, dir_okay=False, path_type=Path)
_DIRECTORY = click.Path(exists=True, file_okay=False, path_type=Path)


def _fail(exc: Exception) -> click.ClickException:
    return click.ClickException(str(exc))


@click.group("campaign")
def campaign() -> None:
    """Artifact-aware calculation campaigns (#165).

    Local and read-only: validate a campaign file, report each node's
    progression from a controller journal, and publish or verify artifact
    manifests. Nothing here submits a job, transfers a file or deletes
    anything. See docs/campaigns.md.
    """


@campaign.command("validate")
@click.argument("campaign_file", type=_FILE)
@click.option("--json", "as_json", is_flag=True, help="Print the result as JSON.")
def validate_cmd(campaign_file: Path, as_json: bool) -> None:
    """Check CAMPAIGN_FILE and print its digest, order and edges.

    Refuses unknown references, cycles, unsafe or overlapping paths, a
    completion edge on a node whose output is consumed, and a render node
    without a validate node upstream along success edges.
    """
    from vq.campaign import campaign_digest, load_campaign

    try:
        parsed = load_campaign(campaign_file)
    except ValueError as exc:
        raise _fail(exc) from None
    result = {
        "campaign_id": parsed.campaign_id,
        "campaign_sha256": campaign_digest(parsed),
        "order": parsed.order(),
        "edges": {n.id: n.edges() for n in parsed.nodes},
    }
    if as_json:
        click.echo(json.dumps(result, indent=2, sort_keys=True))
        return
    click.echo(f"campaign {parsed.campaign_id}  campaign_sha256 {result['campaign_sha256']}")
    for node_id in parsed.order():
        edges = parsed.node(node_id).edges()
        after = ", ".join(f"{dep} ({on})" for dep, on in sorted(edges.items())) or "-"
        click.echo(f"  {node_id}  after: {after}")


@campaign.command("status")
@click.argument("campaign_file", type=_FILE)
@click.argument("journal_file", type=_FILE)
@click.option("--json", "as_json", is_flag=True, help="Print the full status as JSON.")
def status_cmd(campaign_file: Path, journal_file: Path, as_json: bool) -> None:
    """Report each node's status, reasons and next action from JOURNAL_FILE.

    An attempt that may have reached its target is reconciled by its
    idempotency key, never submitted again. A failed node is retried only
    as an explicit new attempt. Artifacts still referenced by unfinished
    consumers are listed for retention.
    """
    from vq.campaign import campaign_status, load_campaign, load_journal, status_text, to_json

    try:
        report = campaign_status(load_campaign(campaign_file), load_journal(journal_file))
    except ValueError as exc:
        raise _fail(exc) from None
    click.echo(to_json(report) if as_json else status_text(report), nl=False)


@campaign.command("artifact-publish")
@click.argument("campaign_file", type=_FILE)
@click.argument("workspace", type=_DIRECTORY)
@click.option("--node", "node_id", required=True, help="The producing node.")
@click.option("--attempt", required=True, type=click.IntRange(min=1),
              help="The producing attempt.")
@click.option("--output", "output_name", required=True, help="The node's declared output.")
@click.option("--runtime-digest", help="SHA256 of the runtime that produced it.")
def publish_cmd(campaign_file: Path, workspace: Path, node_id: str, attempt: int,
                output_name: str, runtime_digest: str | None) -> None:
    """Print the manifest of a node's output found under WORKSPACE.

    Hashes every file. Symbolic links and special files anywhere in the
    output are refused.
    """
    from vq.campaign import load_campaign, publish_artifact, to_json

    try:
        manifest = publish_artifact(workspace, load_campaign(campaign_file), node_id, attempt,
                                    output_name, runtime_digest=runtime_digest)
    except (OSError, ValueError) as exc:
        raise _fail(exc) from None
    click.echo(to_json(manifest.model_dump(mode="json")), nl=False)


@campaign.command("artifact-verify")
@click.argument("manifest_file", type=_FILE)
@click.argument("staged", type=click.Path(path_type=Path))
@click.option("--json", "as_json", is_flag=True, help="Print the verification as JSON.")
def verify_cmd(manifest_file: Path, staged: Path, as_json: bool) -> None:
    """Verify a staged copy STAGED against MANIFEST_FILE.

    Exits 0 only when the copy is exactly the manifest: no missing, partial,
    different, unexpected or unsafe entry. Otherwise exits 1 and lists them.
    """
    from vq.campaign import load_manifest, to_json, verify_artifact

    try:
        result = verify_artifact(staged, load_manifest(manifest_file))
    except (OSError, ValueError) as exc:
        raise _fail(exc) from None
    if as_json:
        click.echo(to_json(result), nl=False)
    else:
        click.echo(f"{result['node']}.{result['output']} attempt {result['attempt']}: "
                   f"{result['status']}")
        for item in result["problems"]:  # type: ignore[attr-defined]
            click.echo(f"  {item['category']}: {item['path']} ({item['detail']})")
    if result["status"] != "verified":
        raise SystemExit(1)
