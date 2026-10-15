# Calculation campaigns

A campaign is a small, reviewable chain of ordinary vq jobs whose outputs
feed each other, for example:

```text
calculate (cluster)  ->  validate (workstation)  ->  render (workstation)
          \->  cleanup, whatever the calculation's outcome
```

This page describes #165 as built so far: the campaign file, artifact
manifests, the progression they follow, and the controller that runs a
campaign across the hosts it names. `vq campaign run` submits ready nodes
through the ordinary queue, relays their verified artifacts between hosts
and advances the campaign until nothing more can run; it can be stopped and
started again at any point. The other commands are read-only. The console
graph is not built yet. Single-job submission is unchanged.

```sh
vq campaign validate campaign.json
vq campaign run campaign.json RUN_DIR            # until done; safe to stop and rerun
vq campaign status campaign.json RUN_DIR/journal.json
vq campaign retry campaign.json RUN_DIR NODE     # authorize the next attempt of a failed node
vq campaign artifact-publish campaign.json WORKSPACE --node calculate --attempt 1 --output result > manifest.json
vq campaign artifact-verify manifest.json STAGED_COPY
```

## The campaign file: `vq.campaign/1`

```json
{
  "schema_name": "vq.campaign/1",
  "campaign_id": "h2o-pipeline",
  "nodes": [
    {"id": "calculate", "kind": "compute", "host": "cluster-a", "program": "vibe-qc",
     "runtime_digest": "<sha256>", "command": ["python", "run.py"], "cpus": 16, "mem_mb": 64000,
     "outputs": [{"name": "result", "path": "out", "kind": "directory"}]},
    {"id": "validate", "kind": "validate", "host": "workstation", "program": "vibe-qc",
     "command": ["python", "check_scf.py", "in"], "cpus": 1,
     "sources": {"path": "validate", "sha256": "<sha256 of the validate/ directory>"},
     "inputs": [{"name": "result", "source": "calculate.result", "path": "in"}],
     "outputs": [{"name": "verdict", "path": "verdict.json", "kind": "file"}]},
    {"id": "render", "kind": "render", "host": "workstation", "program": "vibe-view",
     "runtime_digest": "<sha256>", "command": ["vibe-view", "render", "in"], "cpus": 1,
     "inputs": [{"name": "result", "source": "calculate.result", "path": "in"}],
     "after": [{"node": "validate", "on": "success"}],
     "outputs": [{"name": "figure", "path": "figure.png", "kind": "file"}]},
    {"id": "cleanup", "kind": "other", "command": ["true"], "cpus": 1,
     "after": [{"node": "calculate", "on": "completion"}]}
  ]
}
```

* A node is an ordinary vq job: command, resources, optional program,
  runtime digest and target host. At most 64 nodes.
* **Sources** are the files a node's job needs, such as its input script:
  a directory next to the campaign file (`validate/` above, holding
  `check_scf.py`), copied to the root of the job's workspace before its
  inputs are staged. `sha256` pins the directory's contents (every entry's
  path, type, size and SHA256; symbolic links and special files are
  refused), so the campaign digest covers the exact scripts a node runs and
  an edited script is a changed campaign. `vq campaign validate` prints the
  actual digest when a pin does not match, which is how you pin a new or
  edited directory. A source file may not sit where the node stages an
  input or expects an output.
* **Outputs** are files or directories the job leaves in its workspace.
  **Inputs** name another node's output (`node.output`) and the path to
  stage it at. All paths are relative, without `.` or `..`. Within a node no
  two input or output paths may overlap.
* An input is a **success** edge: an artifact exists only after its producer
  succeeded. `after` adds ordering explicitly: `success` (the predecessor must
  succeed) or `completion` (it must finish, whatever the outcome). A
  completion edge on a node whose output is also consumed is refused as a
  contradiction.
* **Validation is a node**, not an exit code. `kind: render` publishes a
  result, so a render node must have a `validate` node upstream along success
  edges. A successful exit is not a validated result.
* The file has no field for environment variables or credentials, and
  unknown fields are refused.

`vq campaign validate` refuses unknown references, cycles (it names one),
unsafe or overlapping paths and the cases above. It prints the
`campaign_sha256` digest, a stable order and every node's edges. The digest
covers content, not formatting. It also hashes each node's sources and
exits 1 if one is missing, contains a link, or no longer matches its pin.

## Artifacts: `vq.campaign-artifact/1`

After its producer succeeds, an output is published as a manifest: the
campaign digest, node, attempt, output, the producing runtime digest, and,
for a directory, every entry's relative path, type, size and SHA256 (for a
file, its own size and SHA256). Publishing refuses symbolic links and
special files anywhere in the output, including a linked path component. A
consumer receives exactly the listed bytes, inside its own workspace.

`vq campaign artifact-verify` compares a staged copy with its manifest and
exits 0 only when they are identical. Otherwise it exits 1 and names each
problem; the worst one is the status:

| Status | Meaning |
|---|---|
| `unsafe` | a symbolic link, special file, or a file where a directory belongs |
| `missing` | a listed entry, or the whole copy, is absent |
| `partial` | a file is shorter than listed |
| `digest_mismatch` | a file's bytes differ |
| `unexpected` | an entry is not in the manifest |

A dependency having completed does not establish that its files arrived:
only a `verified` artifact unblocks its consumers.

## Progression: `vq.campaign-journal/1`

The controller records every attempt in a journal: node, attempt number,
state (`submitting`, `accepted`, `running`, `succeeded`, `failed`,
`cancelled`), the job's host and ID once accepted, which producer attempt
and manifest each input was staged from, and each output's verification
status. `vq campaign status` reads it and reports, per node, a status, the
reasons, and the next action:

| Status | Next action |
|---|---|
| `ready` | `submit` attempt 1, with its idempotency key and the exact producer attempts and manifests to stage |
| `waiting` | none: a predecessor has not finished yet |
| `blocked` | none: a success predecessor failed or is itself blocked, a completion predecessor will never run, or an input artifact is unpublished or not verified |
| `uncertain` | `reconcile`: a submission may have reached its target. Resend the same payload under the attempt's idempotency key, which returns the job already accepted; never send it under another key |
| `running` | `wait` |
| `succeeded` | none, or `publish` while an output is not yet verified |
| `failed` | none: a retry is an explicit new attempt |

* **Idempotency keys** derive from the campaign digest, node and attempt
  number, so a restarted controller computes the same key and never
  duplicates an accepted job.
* **Retries** are explicit. Attempt `n + 1` records `retry_of: n`, and only a
  failed or cancelled attempt is retried. A consumer is staged from its
  producer's succeeded attempt and verified manifest; one bound to any other
  attempt, such as a failed one, makes the journal invalid.
* **Retention**: the report lists each succeeded producer attempt whose
  artifacts an unfinished consumer still needs. `vq campaign cleanup` never
  removes them.
* The campaign is `succeeded` only when every node succeeded with every
  output verified. A failed node makes it `failed`. A failed validation
  therefore blocks rendering and can never end in a succeeded campaign.

The journal is checked before anything is reported. A journal for another
campaign digest, a key that does not derive, gaps in attempt numbers, a
retry of a succeeded attempt, an accepted attempt without its job, or an
attempt started before its dependencies were met is refused.

## Running a campaign: `vq campaign run`

```sh
vq campaign run campaign.json ~/campaigns/h2o
```

The controller keeps everything in the run directory you name:

| Path | Contents |
|---|---|
| `journal.json` | the `vq.campaign-journal/1` journal; `vq campaign status` reads it |
| `controller.lock` | held while a controller runs; a second one is refused |
| `payloads/<node>/<attempt>/` | the node's sources and staged inputs an attempt was submitted with (its `vq submit --dir`) |
| `incoming/<node>/<attempt>/` | an output while it is fetched and hashed; removed once it is published, and left behind only by an interrupted publish |
| `artifacts/<node>/<attempt>/<output>` | the verified copy of an output, with `<output>.manifest.json` beside it |

Each pass goes through the nodes in campaign order:

1. **Observe** every submitted job with `vq status HOST`. `completed` makes the
   attempt `succeeded`, `killed` makes it `cancelled`, and any other
   terminal state (`failed`, `oom_killed`, `time_exceeded`, ...) makes it
   `failed`. A job is never moved backwards: one seen running and later
   pending again (a resume) stays running.
2. **Publish** each declared output of a succeeded attempt: bring the
   top-level entry holding it back with
   `vq fetch HOST JOB --name ENTRY -o RUN_DIR/incoming/...`, hash the
   output there, copy it into `artifacts/`, and verify the copy against
   that manifest. Only a `verified` copy is kept and unblocks consumers. If
   the copy differs, the record says so and the next pass publishes it
   again. An output the job did not leave is reported and blocks its
   consumers; a fetch that fails (a host unreachable for a moment) is
   retried on the next pass.
3. **Stage and submit** each ready node: copy its sources and then its
   inputs from `artifacts/` into a fresh payload directory, verify the
   sources against their pin and every input against its manifest,
   record the attempt as `submitting` with the producer attempt and
   manifest it was staged from, and then run
   `vq submit HOST --dir PAYLOAD --idempotency-key KEY --job-name CAMPAIGN.NODE.aN --json -- COMMAND`
   with the node's `cpus`, `mem_mb`, `wall_time_seconds` and `program`.
   `HOST` is the node's `host`, or `localhost` without one; for a remote
   host `vq submit` uploads the payload as it does for any `--dir` job.
4. **Reconcile** an attempt still `submitting`: re-verify its payload and
   send it again under the same key. The queue's keyed submission returns
   the job it already accepted under that key, or accepts the request once
   if the first one never arrived. A payload whose inputs no longer match
   their manifests is reported and never sent.

The controller passes again until the campaign succeeds or nothing more can
run, waiting `--interval` seconds (default 30) between passes while jobs
run. It stops when no job is in flight, no queue call is waiting to be
retried, and a pass changed nothing: the remaining nodes have failed, are
blocked, or need someone to act. The output ends with the status table and
a *Needs attention* list. The exit status is 0 only when the campaign
succeeded (or, with `--once`, while it is still progressing without a
problem).

**Stopping and restarting is safe at every point.** The journal is
rewritten atomically after every change, and an attempt is recorded before
its request is sent. A controller killed mid-submission leaves an attempt
`submitting`, and the next run reconciles it under its key, so a node is
never submitted twice. Run it under `nohup`, `tmux` or a service manager to
outlive your session; rerunning the same command picks up where it left
off.

**What the controller will not decide for you:**

* **Retries.** A failed attempt stays failed. `vq campaign retry
  campaign.json RUN_DIR NODE` authorizes attempt `n + 1`: it stages the
  inputs from the producers' verified artifacts and records the attempt
  with `retry_of: n` and its own key. It submits nothing; the next
  `vq campaign run` sends it.
* **A refused submission** (for example a key reused for a different
  payload, or an option the queue rejects) is reported and left
  `submitting`. It is not marked failed, which would invite a retry under a
  new key, and it is resent under its own key after you fix the cause and
  rerun.
* **Science.** A node succeeds by its job's exit. Whether a result is right
  is decided by the validate nodes the campaign declares, and a render node
  needs one upstream.

**Hosts and storage.** A node runs on the host it names: this machine, a
daemon host or a scheduler lane from your configuration; a node without
`host` runs here. The storage policy is **controller relay**: every
artifact comes back to the run directory with `vq fetch`, is hashed and
verified there, and goes out again inside the consumer's payload with
`vq submit`. A producer and its consumer therefore never need to reach
each other; the controller's machine needs to reach every node's host, as
for any `vq submit` and `vq fetch` from it. The manifest of an output is
computed on the controller's copy, after a transfer over the host's
existing SSH connection.

**Limits of this increment.** A node whose `host` is neither this machine
nor a configured host, or that declares a `runtime_digest` (which the queue
cannot yet bind to a submission), is refused before anything is submitted.
Pin a runtime through the node's `program` instead. Artifacts are relayed
whole, so a very large output costs a round trip through the controller's
disk.

A node's workspace holds its sources and its staged inputs. Every pin is
checked before anything is submitted, so an edited script stops the run
until the campaign is pinned again, and that is a new campaign with a new
journal. The sources are copied into the payload and the copy is verified
before the attempt is recorded; before a resend the payload is checked
again, sources and inputs alike, and a changed one is never sent.

## Cleaning up: `vq campaign cleanup`

```sh
vq campaign cleanup campaign.json ~/campaigns/h2o                  # list
vq campaign cleanup campaign.json ~/campaigns/h2o --intermediate --apply
```

Cleanup lists what the run directory no longer needs, with each path's
size and the reason. Nothing is removed without `--apply`. It holds the
run directory's lock, so it never runs beside a controller, and it reads
only the journal and the run directory: it works for a campaign whose host
has since left the configuration or whose scripts have changed.

By default it lists only the controller's own copies:

* the payload of every attempt the queue accepted. Only an attempt still
  `submitting` is ever resent, so its payload is kept;
* a fetched copy in `incoming/`, or a `.staging` or `.copy` directory, left
  by an interrupted publish or stage. The next pass fetches or stages again.

`--intermediate` also lists each verified output that one or more nodes
consume, once its producer attempt is off the retention list, that is,
once every node that consumes any of that attempt's outputs has succeeded.
It never lists:

* an output nothing consumes: that is a result of the campaign;
* an output on the retention list;
* a manifest (`<output>.manifest.json` stays, so the record of what was
  produced survives its data);
* the journal, or anything outside the run directory. Job workspaces on
  the hosts are `vq cleanup`'s business.

The campaign status does not change: a succeeded node stays succeeded
after its intermediate output is gone, and a succeeded attempt is never
retried, so nothing will stage from that output again. `--json` prints the
plan with `applied`, the total `bytes` listed and the bytes `freed`.

## Watching a campaign in the console

`vq campaign run` registers its run directory in your vq state, and the
fleet console on the same machine, served as the same user, draws each
registered campaign at `/fleet/campaigns`. It shows each stage's status,
blockers and next step, the success and completion edges, which outputs
are published and verified, and a link to each stage's latest job. The
page is read-only and uses the same evaluation as `vq campaign status`.
See [Campaigns in the console](fleet_console.md#campaigns-in-the-console).

## Not built yet

* Storage policies other than controller relay, such as a direct
  host-to-host transfer or a shared filesystem reference.
* Binding a declared `runtime_digest` to a submission.
* Integration with resource estimates (#159) and placement (#160).
* The live acceptance: a three-node campaign across two hosts with a
  directory/QVF artifact.
