"""`vq placement`: explain completion-time placement over recorded target capabilities."""

from __future__ import annotations

import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

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
    submits nothing, reserves nothing and contacts no host (`fleet
    --collect` asks only the named daemons). The broker uses this policy
    only with `[broker] policy = "completion-time"`; `vq submit auto` is
    unchanged. See docs/placement_policy.md.
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
@click.option("--estimate", "estimate_file", type=_FILE, default=None, metavar="CALIBRATED",
              help="Fill the request's unset runtime_seconds and mem_mb_per_node from "
              "a `vq estimate --calibration ... --json` document (#159).")
def explain_cmd(fleet_file: Path, request_file: Path, as_json: bool, now: datetime,
                max_age_seconds: float, estimate_file: Path | None) -> None:
    """Explain the placement of REQUEST_FILE over FLEET_FILE.

    Hard requirements are checked first; a target that fails any is
    rejected with every reason. The rest are ranked by predicted
    completion time: staging + queue wait + execution + result transfer.
    Unknown components are named and ranked after complete predictions.
    With no eligible target the job is reported held. With --estimate, a
    calibrated estimate fills the request's unset runtime and per-node memory;
    explicit request values win, and the explanation says what was filled.
    """
    from vq.placement_policy import (
        _read_json,
        explain,
        explanation_json,
        explanation_text,
        load_fleet,
        load_request,
        request_with_estimate,
    )

    try:
        request = load_request(request_file)
        estimate = None
        if estimate_file is not None:
            request, estimate = request_with_estimate(request, _read_json(estimate_file))
        result = explain(load_fleet(fleet_file), request,
                         now=now, max_age_seconds=max_age_seconds, estimate=estimate)
    except ValueError as exc:
        raise click.ClickException(str(exc)) from None
    click.echo(explanation_json(result) if as_json else explanation_text(result), nl=False)


def _capacity_option(_ctx: click.Context, _param: click.Parameter,
                     values: tuple[str, ...]) -> dict[str, Path]:
    pairs: dict[str, Path] = {}
    for value in values:
        name, sep, path = value.partition("=")
        if not sep or not name or not path:
            raise click.BadParameter(f"expected NAME=ANSWER.json, got {value!r}")
        if name in pairs:
            raise click.BadParameter(f"two capacity answers for {name}")
        pairs[name] = Path(path)
    return pairs


@placement.command("fleet")
@click.argument("base_file", type=_FILE)
@click.option("--capacity", "capacity", multiple=True, metavar="NAME=ANSWER.json",
              callback=_capacity_option,
              help="A daemon target's `vq broker capacity --json` answer (repeatable).")
@click.option("--collect", "collect", multiple=True, metavar="NAME",
              help="Ask this daemon target, a configured [hosts.NAME], for its "
              "capacity over SSH now (repeatable).")
@click.option("--timeout", default=60.0, show_default=True,
              type=click.FloatRange(min=0, min_open=True, max=600),
              help="Seconds each --collect call may take.")
@click.option("--queue-history", "queue_history", multiple=True, metavar="NAME[=TARGET]",
              help="Set this scheduler lane's queue wait from the waits this host's "
              "jobs on scheduler target TARGET (default NAME) recorded (repeatable).")
@click.option("--history-days", default=14.0, show_default=True,
              type=click.FloatRange(min=0, min_open=True, max=366),
              help="Only jobs that started within this many days count.")
@click.option("--history-jobs", default=50, show_default=True, type=click.IntRange(min=1),
              help="At most this many of the most recent such jobs count.")
@click.option("--history-min", default=5, show_default=True, type=click.IntRange(min=1),
              help="Fewer recorded waits than this keep the base record.")
@click.option("--scheduler-shape", "scheduler_shape", multiple=True, metavar="NAME[=HOST]",
              help="Set this scheduler lane's node shape and limits from scheduler host "
              "HOST (default NAME), for the lane's queue: `sinfo` on Slurm, `qstat -Qf` "
              "and `pbsnodes -a` on Torque/PBS (repeatable).")
@click.option("--runtime-root", "runtime_root", multiple=True, metavar="NAME=ROOT",
              help="Set daemon target NAME's runtimes from the verified generations of "
              "its #161 deployment root ROOT, read over SSH (repeatable).")
@click.option("-o", "--output", type=click.Path(dir_okay=False, path_type=Path),
              help="Write the fleet here instead of printing it; never BASE_FILE.")
def fleet_cmd(base_file: Path, capacity: dict[str, Path], collect: tuple[str, ...],
              timeout: float, queue_history: tuple[str, ...], history_days: float,
              history_jobs: int, history_min: int, scheduler_shape: tuple[str, ...],
              runtime_root: tuple[str, ...], output: Path | None) -> None:
    """Build a fleet file from BASE_FILE and live daemon capacity answers.

    BASE_FILE is the operator's vq.placement-fleet/1 record: node shapes,
    runtime digests, accounts, speeds and rates. Each --capacity answer,
    collected on that daemon host with `vq broker capacity --json`, sets
    its target's observation time, reachability, free-now CPUs, memory and
    GPUs (only when its admission is open and its queue ready), its GPUs
    per node, and removes programs it reports unavailable. Each --collect
    target is asked over SSH now, as the broker asks: its figures are dated by
    this host's clock less the daemon's snapshot age, and a target that gives
    no answer is recorded unreachable now. Scheduler lanes and daemons without
    an answer keep their base record. Only --collect hosts are contacted.

    Each --queue-history lane gets the nearest-rank 10th-90th percentile of
    its recorded queue waits (scheduler acceptance to first seen running),
    from this host's queue. Each --scheduler-shape lane gets its queue's
    smallest node shape, GPUs, walltime limit and job-size limit from its
    Slurm (`sinfo`) or Torque/PBS (`qstat -Qf`, `pbsnodes -a`) host, which is
    contacted read-only.
    """
    import json
    import time

    from vq import config
    from vq._storage import atomic_write_text
    from vq.broker_capacity import fetch_payload
    from vq.placement_policy import (
        PlacementError,
        _read_json,
        capacity_target,
        fleet_with_capacity,
    )

    try:
        base = _read_json(base_file)
        if not isinstance(base, dict):
            raise PlacementError(f"{base_file}: not a fleet document")
        answers = {name: _read_json(path) for name, path in capacity.items()}
        unreachable: dict[str, float] = {}
        if collect:
            for name in sorted(set(collect)):
                if name in answers:
                    raise PlacementError(f"{name} is given both --capacity and --collect")
                capacity_target(base, name)  # before any host is contacted
            cfg = config.load_config()
            for name in sorted(set(collect)):
                if name not in cfg.hosts:
                    raise PlacementError(f"--collect {name}: not a configured [hosts.{name}]")
            for name in sorted(set(collect)):
                started = time.time()
                try:
                    answer = fetch_payload(cfg, name, timeout=timeout, programs=sorted(
                        capacity_target(base, name).get("programs", {})))
                except ValueError as exc:
                    click.echo(f"{name}: unreachable ({exc})", err=True)
                    unreachable[name] = started
                    continue
                if isinstance(answer, dict):
                    # Dated by this host's clock, as the broker dates it.
                    answer = {**answer, "sampled_at": started}
                answers[name] = answer
        fleet = fleet_with_capacity(base, answers, unreachable=unreachable)
        lanes: dict[str, str] = {}
        for value in queue_history:
            name, _sep, scheduler_target = value.partition("=")
            if name in lanes:
                raise PlacementError(f"--queue-history {name} is given twice")
            lane = next((t for t in fleet["targets"] if t["name"] == name), None)
            if lane is None or lane["kind"] != "scheduler":
                raise PlacementError(f"--queue-history {name}: not a scheduler lane of the fleet")
            lanes[name] = scheduler_target or name
        if lanes:
            from datetime import UTC, datetime

            from vq.listing import list_jobs
            from vq.placement_policy import queue_wait_history

            cfg = config.load_config()
            specs = list_jobs("localhost", multi_user=bool(cfg.multi_user.enabled)
                              or config.system_multi_user_enabled())
            for name, scheduler_target in sorted(lanes.items()):
                wait, count = queue_wait_history(
                    specs, scheduler_target, now=datetime.now(UTC), days=history_days,
                    max_jobs=history_jobs, min_jobs=history_min)
                if wait is None:
                    click.echo(f"{name}: kept the base queue wait; {count} recorded "
                               f"wait(s), fewer than {history_min}", err=True)
                    continue
                next(t for t in fleet["targets"] if t["name"] == name)[
                    "queue_wait_seconds"] = wait
                click.echo(f"{name}: queue wait {wait['low']:g}-{wait['high']:g} s "
                           f"from {count} job(s)", err=True)
        if scheduler_shape:
            _apply_scheduler_shapes(fleet, scheduler_shape, timeout=timeout)
        if runtime_root:
            _apply_runtime_roots(fleet, runtime_root, timeout=timeout)
    except (ValueError, config.ConfigError) as exc:
        raise click.ClickException(str(exc)) from None
    text = json.dumps(fleet, indent=2, sort_keys=True) + "\n"
    if output is None:
        click.echo(text, nl=False)
        return
    if output.resolve() == base_file.resolve():
        raise click.ClickException("the output must not replace BASE_FILE")
    atomic_write_text(output, text)
    click.echo(f"wrote {output} ({len(fleet['targets'])} targets, "
               f"{len(answers)} with live capacity, {len(unreachable)} unreachable)")


@placement.command("compare")
@click.argument("fleet_file", type=_FILE)
@click.option("--jobs", "limit", default=50, show_default=True, type=click.IntRange(min=1),
              help="Compare on this many of this host's most recent finished local jobs.")
@click.option("--now", callback=_parse_now,
              help="Evaluate data age at this ISO 8601 time instead of the current time.")
@click.option("--max-age", "max_age_seconds", default=300.0, show_default=True,
              type=click.FloatRange(min=0, min_open=True, max=86400),
              help="Seconds after which a target's observation is stale.")
@click.option("--json", "as_json", is_flag=True, help="Print the full comparison as JSON.")
def compare_cmd(fleet_file: Path, limit: int, now: datetime, max_age_seconds: float,
                as_json: bool) -> None:
    """Compare the broker's two orderings on this host's recorded jobs.

    For each recent finished local job (its CPUs, declared memory, walltime
    and program), shows where the broker's default `capacity` ordering and
    `[broker] policy = "completion-time"` would each place it, over capacity
    snapshots built from FLEET_FILE's daemon targets, and how often they
    agree. Jobs without a declared --mem-mb are skipped, since a broker
    needs one. Read-only: places nothing and contacts no host.
    """
    import json

    from vq import config
    from vq.listing import list_jobs
    from vq.placement_compare import compare, compare_text
    from vq.placement_policy import load_fleet

    try:
        fleet = load_fleet(fleet_file)
        cfg = config.load_config()
        specs = list_jobs("localhost", multi_user=bool(cfg.multi_user.enabled)
                          or config.system_multi_user_enabled())
        report = compare(fleet, specs, now=now, max_age_seconds=max_age_seconds,
                         limit=limit)
    except (ValueError, config.ConfigError) as exc:
        raise click.ClickException(str(exc)) from None
    click.echo(json.dumps(report, indent=2, sort_keys=True) + "\n" if as_json
               else compare_text(report), nl=False)


def _apply_scheduler_shapes(fleet: dict[str, Any], values: tuple[str, ...], *,
                            timeout: float) -> None:
    """#160: each named lane's shape from its Slurm or Torque/PBS host, read-only."""
    import time

    from vq import config, transport
    from vq.placement_policy import (
        SINFO_FORMAT,
        PlacementError,
        lane_shape_from_pbs,
        lane_shape_from_sinfo,
    )

    lanes: dict[str, tuple[dict[str, Any], str]] = {}
    cfg = config.load_config()
    for value in values:  # every refusal before any host is contacted
        name, _sep, host = value.partition("=")
        host = host or name
        if name in lanes:
            raise PlacementError(f"--scheduler-shape {name} is given twice")
        lane = next((t for t in fleet["targets"] if t["name"] == name), None)
        if lane is None or lane["kind"] != "scheduler":
            raise PlacementError(f"--scheduler-shape {name}: not a scheduler lane of the fleet")
        if not lane.get("queue"):
            raise PlacementError(f"--scheduler-shape {name}: the lane records no queue "
                                 "(its Slurm partition)")
        host_cfg = cfg.hosts.get(host)
        if host_cfg is None or host_cfg.scheduler not in ("slurm", "pbs"):
            raise PlacementError(f"--scheduler-shape {name}: {host} is not a configured "
                                 "Slurm or Torque/PBS host")
        lanes[name] = (lane, host)
    def ask(host: str, *argv: str) -> str:
        return transport.run_remote_shell(
            cfg.hosts[host], *argv, check=True, timeout=timeout, retry_transient=1,
            owned_process_group=True, max_stdout_bytes=16 * 1024 * 1024,
            max_stderr_bytes=4096).stdout

    for name, (lane, host) in sorted(lanes.items()):
        started = time.time()
        note: str | None = None
        try:
            if cfg.hosts[host].scheduler == "slurm":
                shape = lane_shape_from_sinfo(ask(
                    host, "sinfo", "--noheader", "--partition", lane["queue"],
                    "--format", SINFO_FORMAT))
            else:
                qstat = ask(host, "qstat", "-Qf", lane["queue"])
                try:
                    nodes: str | None = ask(host, "pbsnodes", "-a")
                except (OSError, subprocess.SubprocessError,
                        transport.RemoteOutcomeUnknown):
                    nodes = None
                shape, note = lane_shape_from_pbs(lane["queue"], qstat, nodes)
        except (OSError, ValueError, subprocess.SubprocessError,
                transport.RemoteOutcomeUnknown) as exc:
            text = " ".join(str(exc).split())[:300] or type(exc).__name__
            click.echo(f"{name}: unreachable ({host} gave no partition shape: {text})",
                       err=True)
            lane["reachable"] = False
        else:
            node = shape.pop("node", None)
            if node is not None:
                lane["node"] = {**lane["node"], **node}
            lane.update(shape)
            lane["reachable"] = True
            click.echo(f"{name}: " + (
                f"{node['cpus']} CPUs, {node['mem_mb']} MB, {node['gpus']} GPUs per node"
                if node is not None else f"node shape kept ({note})") + f" from {host}",
                err=True)
        lane["observed_at"] = datetime.fromtimestamp(started, tz=UTC).isoformat()


def _apply_runtime_roots(fleet: dict[str, Any], values: tuple[str, ...], *,
                         timeout: float) -> None:
    """#160/#161: each daemon target's verified generations as its runtimes."""
    import json

    from vq import config, transport
    from vq.placement_policy import PlacementError, runtimes_from_generations

    roots: dict[str, tuple[dict[str, Any], str]] = {}
    cfg = config.load_config()
    for value in values:  # every refusal before any host is contacted
        name, sep, root = value.partition("=")
        if not sep or not root.startswith("/"):
            raise PlacementError(f"--runtime-root {value}: expected NAME=/absolute/root")
        if name in roots:
            raise PlacementError(f"--runtime-root {name} is given twice")
        target = next((t for t in fleet["targets"] if t["name"] == name), None)
        if target is None or target["kind"] != "daemon":
            raise PlacementError(f"--runtime-root {name}: not a daemon target of the fleet")
        if name not in cfg.hosts:
            raise PlacementError(f"--runtime-root {name}: not a configured [hosts.{name}]")
        roots[name] = (target, root)
    for name, (target, root) in sorted(roots.items()):
        try:
            proc = transport.run_remote_vq(
                cfg.hosts[name], "runtime-bundle", "generations", "--root", root, "--json",
                timeout=timeout, owned_process_group=True,
                max_stdout_bytes=16 * 1024 * 1024, max_stderr_bytes=1024 * 1024)
            runtimes, skipped = runtimes_from_generations(json.loads(proc.stdout))
        except Exception as exc:  # noqa: BLE001 - any failure is no runtime evidence
            text = " ".join(str(exc).split())[:300] or type(exc).__name__
            click.echo(f"{name}: runtimes kept ({name} gave no generation listing: {text})",
                       err=True)
            continue
        target["programs"] = {**target["programs"], **runtimes}
        for reason in skipped:
            click.echo(f"{name}: skipped {reason}", err=True)
        click.echo(f"{name}: " + (", ".join(
            f"{program} {len(ids)} runtime(s)" for program, ids in sorted(runtimes.items()))
            or "no verified generation") + f" at {root}", err=True)
