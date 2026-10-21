"""`vq campaign`: validate campaigns, track their progression and verify artifacts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

import click

if TYPE_CHECKING:
    from vq.campaign_controller import Controller

_FILE = click.Path(exists=True, dir_okay=False, path_type=Path)
_DIRECTORY = click.Path(exists=True, file_okay=False, path_type=Path)


def _fail(exc: Exception) -> click.ClickException:
    return click.ClickException(str(exc))


@click.group("campaign")
def campaign() -> None:
    """Artifact-aware calculation campaigns (#165).

    `run` is the controller: it submits ready nodes through the ordinary
    queue, stages their verified inputs and advances the campaign until
    nothing more can run, and can be stopped and started again at any
    point. `retry` authorizes the next attempt of a failed node. The other
    commands are read-only: validate a campaign file, report each node's
    progression from a journal, and publish or verify artifact manifests.
    Nothing here transfers files between hosts or deletes anything. See
    docs/campaigns.md.
    """


@campaign.command("validate")
@click.argument("campaign_file", type=_FILE)
@click.option("--json", "as_json", is_flag=True, help="Print the result as JSON.")
def validate_cmd(campaign_file: Path, as_json: bool) -> None:
    """Check CAMPAIGN_FILE and print its digest, order and edges.

    Refuses unknown references, cycles, unsafe or overlapping paths, a
    completion edge on a node whose output is consumed, and a render node
    without a validate node upstream along success edges. Each node's
    `sources` directory, relative to the campaign file, is hashed and
    compared with its pin; a mismatch prints the actual digest and exits 1.
    """
    from vq.campaign import campaign_digest, check_sources, load_campaign

    try:
        parsed = load_campaign(campaign_file)
    except ValueError as exc:
        raise _fail(exc) from None
    sources = check_sources(parsed, campaign_file.resolve().parent)
    result = {
        "campaign_id": parsed.campaign_id,
        "campaign_sha256": campaign_digest(parsed),
        "order": parsed.order(),
        "edges": {n.id: n.edges() for n in parsed.nodes},
        "sources": sources,
    }
    if as_json:
        click.echo(json.dumps(result, indent=2, sort_keys=True))
    else:
        click.echo(f"campaign {parsed.campaign_id}  campaign_sha256 {result['campaign_sha256']}")
        for node_id in parsed.order():
            edges = parsed.node(node_id).edges()
            after = ", ".join(f"{dep} ({on})" for dep, on in sorted(edges.items())) or "-"
            click.echo(f"  {node_id}  after: {after}")
            if node_id in sources:
                item = sources[node_id]
                verdict = item["problem"] or f"sources {item['path']} match their pin"
                click.echo(f"    {verdict}")
    if any(item["problem"] for item in sources.values()):
        raise SystemExit(1)


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


def _controller(campaign_file: Path, run_dir: Path, *, log: bool,
                check: bool = True) -> Controller:
    """The controller for CAMPAIGN_FILE over RUN_DIR; ``check`` refuses a campaign
    that could not run here (unknown host, sources off their pins)."""
    from vq import config
    from vq.campaign import load_campaign
    from vq.campaign_controller import CliQueue, Controller, preflight

    parsed = load_campaign(campaign_file)
    sources_root = campaign_file.resolve().parent
    if not check:
        return Controller(parsed, run_dir, CliQueue(), sources_root=sources_root)
    try:
        cfg = config.load_config()
    except config.ConfigError as exc:
        raise click.ClickException(f"cannot read the vq configuration: {exc}") from None
    hosts = frozenset(cfg.hosts)
    # The hosts `vq submit` forwards to a scheduler driver: a node's
    # runtime_root on one of them is a path on the cluster.
    scheduler_hosts = frozenset(name for name, host in cfg.hosts.items()
                                if host.scheduler != "local")
    problems = preflight(parsed, sources_root, hosts, scheduler_hosts)
    if problems:
        raise click.ClickException(
            "this controller cannot run the campaign:\n  " + "\n  ".join(problems))
    return Controller(parsed, run_dir, CliQueue(), sources_root=sources_root,
                      log=(lambda message: click.echo(message)) if log else (lambda _m: None))


@campaign.command("run")
@click.argument("campaign_file", type=_FILE)
@click.argument("run_dir", type=click.Path(file_okay=False, path_type=Path))
@click.option("--once", is_flag=True,
              help="Make one pass over the nodes and exit instead of waiting for jobs.")
@click.option("--interval", type=click.FloatRange(min=1.0), default=30.0, show_default=True,
              help="Seconds between passes while jobs run.")
@click.option("--json", "as_json", is_flag=True,
              help="Print the final status, and any problems, as JSON.")
def run_cmd(campaign_file: Path, run_dir: Path, once: bool, interval: float,
            as_json: bool) -> None:
    """Run CAMPAIGN_FILE to its end, keeping state in RUN_DIR.

    \b
    Each pass, in campaign order:
      * records how every submitted job ended (`vq status HOST`);
      * fetches each output of a succeeded attempt (`vq fetch HOST JOB
        --name`) into RUN_DIR, publishes it into RUN_DIR/artifacts and
        verifies the copy against its manifest;
      * stages a ready node's inputs from verified artifacts into
        RUN_DIR/payloads, verifies them, records the attempt, and submits
        it with `vq submit --idempotency-key`;
      * resends an attempt left unanswered by a crash under the same key,
        so the queue returns the job it already accepted instead of a
        second one.

    It passes again until the campaign succeeds or nothing more can run,
    sleeping --interval seconds while jobs run. RUN_DIR/journal.json is
    rewritten atomically after every change and RUN_DIR is locked, so the
    controller can be stopped at any time and started again with the same
    command. It never retries a failed attempt; see `vq campaign retry`.

    The run directory is registered in your vq state, so the fleet console
    on this machine lists the campaign at /fleet/campaigns.

    A node runs on the host it names (this machine without one), which must
    be this machine or a configured host. Every artifact is relayed through
    RUN_DIR, so only this machine needs to reach each node's host. A node
    with an unknown host, or with a runtime_root that `vq submit` would
    refuse there, is refused before anything is submitted. A node's
    runtime_digest and runtime_root bind its job to that runtime-bundle
    generation, on a daemon host or on a scheduler target, where the job's
    node verifies it before the command starts. The generation counts as
    run on a daemon host once the job started, and on a scheduler target
    once it completed with exit code 0; a job that ran anything else is
    never published.

    Exits 0 when the campaign succeeded, or with --once while it is still
    in progress without a problem; otherwise 1.
    """
    from vq import campaign_registry
    from vq.campaign import CampaignError, status_text, to_json
    from vq.campaign_controller import locked

    try:
        with locked(run_dir):
            controller = _controller(campaign_file, run_dir, log=not as_json)
            try:
                campaign_registry.register(controller.campaign, campaign_file, run_dir)
            except OSError as exc:
                # Only the console's view depends on it; the run does not.
                click.echo(f"warning: the console will not list this campaign: "
                           f"{exc.strerror or exc}", err=True)
            report = controller.run(once=once, interval=interval)
    except (CampaignError, OSError, ValueError) as exc:
        raise _fail(exc) from None
    problems = {**controller.transient, **controller.problems}
    if as_json:
        click.echo(to_json({"status": report, "problems": problems}), nl=False)
    else:
        click.echo(status_text(report), nl=False)
        if problems:
            click.echo("\nNeeds attention:")
            for node_id, problem in sorted(problems.items()):
                click.echo(f"  {node_id}: {problem}")
    ok = report["status"] == "succeeded" or (
        once and report["status"] == "in_progress" and not problems)
    if not ok:
        raise SystemExit(1)


@campaign.command("retry")
@click.argument("campaign_file", type=_FILE)
@click.argument("run_dir", type=click.Path(file_okay=False, path_type=Path))
@click.argument("node_id")
def retry_cmd(campaign_file: Path, run_dir: Path, node_id: str) -> None:
    """Authorize the next attempt of NODE_ID, whose last attempt failed.

    Stages its inputs from the producers' verified artifacts and records
    the attempt as retrying the failed one, under its own idempotency key.
    It submits nothing: the next `vq campaign run` sends it.
    """
    from vq.campaign import CampaignError
    from vq.campaign_controller import locked

    try:
        with locked(run_dir):
            controller = _controller(campaign_file, run_dir, log=False)
            attempt = controller.authorize_retry(node_id)
    except (CampaignError, OSError, ValueError) as exc:
        raise _fail(exc) from None
    click.echo(f"{node_id}: attempt {attempt.attempt} authorized (retries attempt "
               f"{attempt.retry_of}); `vq campaign run` submits it")


@campaign.command("cleanup")
@click.argument("campaign_file", type=_FILE)
@click.argument("run_dir", type=click.Path(file_okay=False, path_type=Path))
@click.option("--intermediate", is_flag=True,
              help="Also remove consumed outputs once their producer attempt is off "
                   "the retention list.")
@click.option("--apply", "apply_", is_flag=True,
              help="Remove what is listed. Without it, only list it.")
@click.option("--json", "as_json", is_flag=True, help="Print the plan as JSON.")
def cleanup_cmd(campaign_file: Path, run_dir: Path, intermediate: bool, apply_: bool,
                as_json: bool) -> None:
    """List, and with --apply remove, what RUN_DIR no longer needs.

    \b
    By default only the controller's own copies:
      * the payload of an attempt the queue accepted (only an attempt still
        `submitting` is ever resent, so its payload is kept);
      * leftovers of an interrupted stage or publish.
    With --intermediate, also each consumed output once every node that
    consumes its producer attempt has succeeded. Never an output still on
    the retention list, an output no node consumes (a campaign result), a
    manifest, or anything outside RUN_DIR: job workspaces on the hosts are
    `vq cleanup`'s business.
    """
    from vq.campaign import CampaignError
    from vq.campaign_controller import apply_cleanup, locked

    try:
        with locked(run_dir):
            # Cleanup reads only the journal and the run directory, so a host
            # or script that changed since the run does not block it.
            controller = _controller(campaign_file, run_dir, log=False, check=False)
            plan = controller.cleanup_plan(intermediate=intermediate)
            freed = apply_cleanup(plan) if apply_ else 0
    except (CampaignError, OSError, ValueError) as exc:
        raise _fail(exc) from None
    total = sum(int(item["bytes"]) for item in plan)
    if as_json:
        click.echo(json.dumps({"applied": apply_, "bytes": total, "freed": freed,
                               "paths": plan}, indent=2, sort_keys=True))
        return
    for item in plan:
        click.echo(f"{item['bytes']:>12}  {item['path']}  ({item['reason']})")
    summary = f"{len(plan)} path{'' if len(plan) == 1 else 's'}, {total} bytes"
    click.echo(f"removed {summary}" if apply_
               else f"would remove {summary} (rerun with --apply)")
