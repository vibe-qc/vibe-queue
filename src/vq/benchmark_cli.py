"""`vq benchmark`: plan and analyze generic-versus-optimized runtime comparisons."""

from __future__ import annotations

import json
from pathlib import Path

import click

_FILE = click.Path(exists=True, dir_okay=False, path_type=Path)


@click.group("benchmark")
def benchmark() -> None:
    """Plan and analyze generic-versus-optimized runtime comparisons (#162).

    Local and read-only: these commands validate a plan, print its run
    order and analyze recorded results. They submit nothing, measure
    nothing and contact no host. See docs/benchmark_campaigns.md.
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


@benchmark.command("analyze")
@click.argument("plan_file", type=_FILE)
@click.argument("results_file", type=_FILE)
@click.option("--json", "as_json", is_flag=True, help="Print the full report as JSON.")
@click.option("--csv", "csv_path", type=click.Path(dir_okay=False, path_type=Path),
              help="Also write one CSV row per node class, workload and variant to this file.")
def analyze_cmd(plan_file: Path, results_file: Path, as_json: bool,
                csv_path: Path | None) -> None:
    """Analyze RESULTS_FILE recorded under PLAN_FILE.

    Pairs each variant's run with the baseline run of the same block and
    reports T_baseline / T_variant: geometric mean, Student-t interval,
    sample count, verdict and break-even runs. Failed, unconverged and
    numerically mismatched runs, other runtimes or layouts, and pairs on
    different CPU models are excluded with their reason. The same inputs
    always produce the same output.
    """
    from vq.benchmark import analyze, load_plan, load_results, report_csv, report_json, report_text

    try:
        report = analyze(load_plan(plan_file), load_results(results_file))
        if csv_path is not None:
            csv_path.write_text(report_csv(report), encoding="utf-8")
    except (OSError, ValueError) as exc:
        raise click.ClickException(str(exc)) from None
    click.echo(report_json(report) if as_json else report_text(report), nl=False)
