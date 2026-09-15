# The agent contract

**Audience: you are a program, or you are writing one.** You need shapes that
do not move, exit codes that mean something, and a statement of what vq
promises across versions.

vq was built to be driven by agents as well as people, and the interfaces
below are maintained as contracts rather than as conveniences. That is the
difference between this section and the other two: here, a change in output
format is a breaking change.

## What to rely on

* [Agent interaction protocol](../agent_interaction.md) — the primary
  document. Stable JSON shapes, the verbs worth calling, and the patterns that
  survive a daemon restart.
* [Design invariants (SPEC)](../SPEC.md) — what vq guarantees and, as
  importantly, what it deliberately does not.
* [Version compatibility](../version_compatibility.md) — vq to vibe-qc
  version mapping, and the state-format contract between daemon generations.
* [Chat onboarding](../chat-onboarding.md) — the orientation an agent needs
  before its first submission.
* [Prompt fragments](../agent_prompts.md) — reusable text for agents that
  drive vq.

## The short version

Almost every read verb takes `--json`. Prefer it to parsing text output, which
is formatted for humans and changes without ceremony:

```sh
vq status <jobid> --json
vq queue --all --json
vq overview --json
```

`vq wait` blocks until a job reaches a terminal state, so an agent does not
have to poll. When you must poll, poll `status --json` rather than scraping
`queue`.

Two distinctions that cause most agent bugs:

**Terminal is not the same as successful.** A job can end COMPLETED, FAILED,
INTERRUPTED, or killed. `vq wait` returning is not evidence the work
succeeded; check the state.

**A scheduler-owned job's "running" is a reservation, not a confirmation.**
On PBS and SLURM hosts, vq distinguishes durable ownership from
last-confirmed compute execution. `scheduler_running_confirmed` is tri-state
for exactly this reason: absent evidence is not negative evidence.

```{toctree}
:hidden:

../agent_interaction
../SPEC
../chat-onboarding
../agent_prompts
```
