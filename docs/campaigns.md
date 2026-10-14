# Calculation campaigns

A campaign is a small, reviewable chain of ordinary vq jobs whose outputs
feed each other, for example:

```text
calculate (cluster)  ->  validate (workstation)  ->  render (workstation)
          \->  cleanup, whatever the calculation's outcome
```

This page describes the first increment of #165: the campaign file, artifact
manifests, and the progression a controller follows. `vq campaign` is local
and read-only. It validates a campaign, reports where each node stands
according to a controller journal, and publishes or verifies artifact
manifests. It does not yet submit jobs, move files or show a console graph;
those come with the controller. Single-job submission is unchanged.

```sh
vq campaign validate campaign.json
vq campaign status campaign.json journal.json
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
covers content, not formatting.

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
| `uncertain` | `reconcile`: a submission may have reached its target. Look it up there by its idempotency key; never submit the attempt again |
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
  artifacts an unfinished consumer still needs. Do not clean those up.
* The campaign is `succeeded` only when every node succeeded with every
  output verified. A failed node makes it `failed`. A failed validation
  therefore blocks rendering and can never end in a succeeded campaign.

The journal is checked before anything is reported. A journal for another
campaign digest, a key that does not derive, gaps in attempt numbers, a
retry of a succeeded attempt, an accepted attempt without its job, or an
attempt started before its dependencies were met is refused.

## Not in this increment

* The controller that submits nodes, stages and transfers verified inputs
  between hosts, and writes the journal.
* Integration with resource estimates (#159) and placement (#160).
* The console's campaign graph.
* The live acceptance: a three-node campaign across two hosts with a
  directory/QVF artifact.
