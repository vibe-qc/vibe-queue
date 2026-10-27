"""`vq benchmark`: plan and analyze generic-versus-optimized runtime comparisons."""

from __future__ import annotations

import json
from pathlib import Path

import click

_FILE = click.Path(exists=True, dir_okay=False, path_type=Path)


@click.group("benchmark")
def benchmark() -> None:
    """Plan, run and analyze generic-versus-optimized runtime comparisons (#162).

    `plan` and `analyze` are local and read-only. `run` submits a plan's
    runs through the queue, one at a time per node class, and collects
    their results; it starts only when an operator runs it. See
    docs/benchmark_campaigns.md.
    """


@benchmark.command("plan")
@click.argument("plan_file", type=_FILE)
@click.option("--json", "as_json", is_flag=True,
              help="Print the plan digest and run order as JSON.")
def plan_cmd(plan_file: Path, as_json: bool) -> None:
    """Validate PLAN_FILE and print its digest and run order.

    Within each block every workload/variant pair runs once, in an order
    shuffled by the plan's seed; the first `warmup` blocks are warm-ups.
    Record the printed plan_sha256 in the results file.
    """
    from vq.benchmark import load_plan, plan_digest, run_order

    try:
        plan = load_plan(plan_file)
    except ValueError as exc:
        raise click.ClickException(str(exc)) from None
    order = run_order(plan)
    digest = plan_digest(plan)
    if as_json:
        click.echo(json.dumps({"plan_id": plan.plan_id, "plan_sha256": digest,
                               "runs": order}, indent=2, sort_keys=True))
        return
    click.echo(f"plan {plan.plan_id}  plan_sha256 {digest}")
    click.echo(f"{len(order)} runs: {plan.warmup} warm-up and {plan.repeats} measured "
               f"block(s) per node class; layout {plan.layout.label()}")
    for entry in order:
        click.echo(
            f"{entry['sequence']:>5}  {entry['node_class']}  block {entry['block']}"
            f"{' (warm-up)' if entry['warmup'] else ''}  {entry['workload']}  {entry['variant']}"
        )


@benchmark.command("variant")
@click.argument("bundle", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.option("--name", required=True, help="The variant's name in the plan.")
@click.option("--library", "libraries", multiple=True, metavar="NAME=VALUE",
              help="Another loaded numerical library and its version; repeatable.")
@click.option("--build-cost-core-seconds", type=click.FloatRange(min=0), default=0.0,
              show_default=True)
@click.option("--deployment-cost-core-seconds", type=click.FloatRange(min=0), default=0.0,
              show_default=True)
def variant_cmd(bundle: Path, name: str, libraries: tuple[str, ...],
                build_cost_core_seconds: float, deployment_cost_core_seconds: float) -> None:
    """Print a plan variant identified by the portable runtime BUNDLE.

    BUNDLE is a `vq runtime-bundle pack` output or a published artifact. Its
    archive is checked against its manifest. The variant's runtime_digest is
    the artifact ID, which `vq benchmark measure --runtime-root` reports for a
    deployment of it. Build flags, toolchain, dependency digest and archive
    size come from the bundle. Paste the printed object into the plan's
    variants.
    """
    from vq.benchmark import variant_from_bundle

    extra: dict[str, str] = {}
    for item in libraries:
        library, sep, value = item.partition("=")
        if not sep or not library or not value:
            raise click.BadParameter(f"expected NAME=VALUE, got {item!r}", param_hint="--library")
        if library in extra:
            raise click.BadParameter(f"{library} is given twice", param_hint="--library")
        extra[library] = value
    try:
        variant, build = variant_from_bundle(
            bundle, name, libraries=extra, build_cost_core_seconds=build_cost_core_seconds,
            deployment_cost_core_seconds=deployment_cost_core_seconds)
    except (OSError, ValueError) as exc:  # a BundleError is a ValueError
        raise click.ClickException(str(exc)) from None
    click.echo(f"{build.program} {build.variant} build {build.build_id[:12]}, "
               f"artifact {variant.runtime_digest[:12]}", err=True)
    click.echo(json.dumps(variant.model_dump(mode="json"), indent=2, sort_keys=True))


@benchmark.command("analyze")
@click.argument("plan_file", type=_FILE)
@click.argument("results_file", type=_FILE)
@click.option("--json", "as_json", is_flag=True, help="Print the full report as JSON.")
@click.option("--csv", "csv_path", type=click.Path(dir_okay=False, path_type=Path),
              help="Also write one CSV row per node class, workload and variant to this file.")
@click.option("--svg", "svg_path", type=click.Path(dir_okay=False, path_type=Path),
              help="Also write a standalone speedup plot with intervals and sample counts.")
def analyze_cmd(plan_file: Path, results_file: Path, as_json: bool,
                csv_path: Path | None, svg_path: Path | None) -> None:
    """Analyze RESULTS_FILE recorded under PLAN_FILE.

    Pairs each variant's run with the baseline run of the same block and
    reports T_baseline / T_variant: geometric mean, Student-t interval,
    sample count, verdict and break-even runs. Failed, unconverged and
    numerically mismatched runs, other runtimes or layouts, and pairs on
    different CPU models are excluded with their reason. The same inputs
    always produce the same output.
    """
    from vq.benchmark import (
        analyze,
        load_plan,
        load_results,
        report_csv,
        report_json,
        report_svg,
        report_text,
    )

    try:
        protected = [plan_file, results_file]
        for output in (csv_path, svg_path):
            if output is None:
                continue
            if any(output.resolve() == path.resolve()
                   or (output.exists() and path.exists() and output.samefile(path))
                   for path in protected):
                raise ValueError("report outputs must be distinct from input files and each other")
            protected.append(output)
        report = analyze(load_plan(plan_file), load_results(results_file))
        if csv_path is not None:
            csv_path.write_text(report_csv(report), encoding="utf-8")
        if svg_path is not None:
            svg_path.write_text(report_svg(report), encoding="utf-8")
    except (OSError, ValueError) as exc:
        raise click.ClickException(str(exc)) from None
    click.echo(report_json(report) if as_json else report_text(report), nl=False)


@benchmark.command("run")
@click.argument("plan_file", type=_FILE)
@click.argument("binding_file", type=_FILE)
@click.argument("run_dir", type=click.Path(file_okay=False, path_type=Path))
@click.option("--once", is_flag=True,
              help="Make one pass (record what finished, start what is next) and exit.")
@click.option("--interval", type=click.FloatRange(min=1.0), default=30.0, show_default=True,
              help="Seconds between passes while runs execute.")
def run_cmd(plan_file: Path, binding_file: Path, run_dir: Path, once: bool,
            interval: float) -> None:
    """Run PLAN_FILE through the queue as BINDING_FILE says, results in RUN_DIR.

    \b
    BINDING_FILE (vq.benchmark-binding/1) maps each node class to a configured
    host (or this machine), each variant to its command, and each workload
    to a directory, relative to the binding file, whose content must match
    the plan's input_digest. Runs follow the plan's seeded order, one at a
    time per node class, each submitted with `vq submit HOST
    --idempotency-key` so a restart never submits a run twice.

    \b
    A finished run's timings, check values, runtime digest and CPU model
    come from the vq-benchmark-run.json (vq.benchmark-run-report/1) its job
    wrote; peak RSS and CPU time from its measured resource usage. A
    completed job without a valid report is left missing. Failed, OOM- or
    time-killed and killed runs are recorded with that status.

    RUN_DIR/results.json (vq.benchmark-results/1) is rewritten after every
    run, ready for `vq benchmark analyze`. Exits 0 once every run is
    recorded (or, with --once, while runs remain without a problem); a
    run left missing or a problem exits 1.
    """
    from vq import config
    from vq.benchmark_runner import RunnerError, locked, open_runner
    from vq.campaign import CampaignError

    try:
        hosts = frozenset(config.load_config().hosts)
        with locked(run_dir):
            runner = open_runner(plan_file, binding_file, run_dir, hosts=hosts,
                                 log=lambda message: click.echo(message))
            runner.run(once=once, interval=interval)
    except (RunnerError, CampaignError, config.ConfigError, OSError, ValueError) as exc:
        raise click.ClickException(str(exc)) from None
    missing = [e for e in runner.journal.runs if e.state == "missing"]
    pending = runner.pending()
    click.echo(f"{len(runner.results.runs)} recorded, {len(missing)} missing, "
               f"{pending} not yet run; results in {run_dir / 'results.json'}")
    for entry in missing:
        click.echo(f"  run {entry.sequence} missing: {entry.detail}")
    for sequence, problem in sorted({**runner.transient, **runner.problems}.items()):
        click.echo(f"  run {sequence}: {problem}")
    if runner.problems or missing or (pending and not once):
        raise SystemExit(1)


@benchmark.command("measure", context_settings={"ignore_unknown_options": True})
@click.option("--program", required=True, metavar="NAME",
              help="The program whose output to read: vibe-qc or orca.")
@click.option("--runtime-root", type=click.Path(file_okay=False, path_type=Path),
              help="A portable runtime deployment: report its verified current selection "
              "and expand {runtime} in COMMAND to that generation's payload.")
@click.option("--runtime-digest", metavar="SHA256",
              help="Instead of --runtime-root: the runtime identity the operator declares.")
@click.option("--report", "report_path", type=click.Path(dir_okay=False, path_type=Path),
              default=Path("vq-benchmark-run.json"), show_default=True,
              help="Where to write the run report.")
@click.argument("command", nargs=-1, required=True, type=click.UNPROCESSED)
def measure_cmd(program: str, runtime_root: Path | None, runtime_digest: str | None,
                report_path: Path, command: tuple[str, ...]) -> None:
    """Run COMMAND as one benchmark run and write its report.

    \b
    For use as a variant's command in a `vq benchmark run` binding:
      vq benchmark measure --program vibe-qc --runtime-root /opt/rt/tuned \
        -- {runtime}/bin/python run.py

    The program's output passes through unchanged and its exit code is
    kept. Startup is the time to the first SCF iteration line, steady time
    the rest (all of it when no such line appears). The total energy, gap,
    SCF iterations and convergence are read from the output. With
    --runtime-root the reported runtime is the deployment's verified
    current selection, and a selection that changes during the run is
    refused; --runtime-digest records a declared identity instead.
    """
    from vq.benchmark_measure import MeasureError, measure

    try:
        returncode = measure(program, list(command), runtime_root=runtime_root,
                             runtime_digest=runtime_digest, report=report_path)
    except (MeasureError, OSError, ValueError) as exc:
        raise click.ClickException(str(exc)) from None
    raise SystemExit(returncode)
