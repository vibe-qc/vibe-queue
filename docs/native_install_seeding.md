# Native install seeding for runtime slots

An optional product adapter can qualify native dependency installs from the
current verified generation before installing a new source-SHA generation.
Queue copies the qualified files into the candidate's final prefix. It does
not copy a virtualenv or build tree, rebuild dependencies, or infer their
provenance from version strings. The ordinary Git materializer stays unchanged.

```toml
[programs.example]
kind = "venv"
git_dir = "/opt/example/source"
python = "/opt/example/source/.venv/bin/python"
runtime_slot_root = "/opt/example/runtime"
install_script = "scripts/install.sh"
update_script = "scripts/update.sh"
runtime_slot_seed_adapter = "scripts/native-seed"
runtime_slot_seed_timeout_seconds = 60
```

The adapter is an executable tracked at the candidate commit, specified as one
canonical relative path, with no shell command or arguments. Update every
configuration reader before adding the setting. Its absence preserves existing
installation behavior. Its presence means **reuse is required** for fresh slots:
missing donor, provenance mismatch, adapter failure or failed relocation stops
before the installer. There is no automatic cold-build fallback. A separately
authorized cold build uses a registration without this opt-in. Reusing an
already verified exact-SHA generation does not rerun seeding or installation.

This first increment supports regular files with modes 0444, 0644, 0555 or 0755,
at most 4096 files per plan, and nonoverlapping, absent destination prefixes.
Symlinks and hardlinks are refused. The donor and candidate must use the same
source-local virtualenv directory. Only the current retained generation is
selected; a mutable build checkout is never an artifact donor.

## Adapter protocol

Queue invokes the adapter twice, with a JSON request on standard input and a
single JSON response on standard output. Each phase has a 1–600 second wall
limit and a 1 MiB output limit per stream. Nonzero exit, malformed response or
`{"decision":"hold","reason":"..."}` stops the operation. Descendants are
terminated when the adapter finishes or times out. The source/store/venv locks
remain held and their descriptors are inherited by the adapter. Ambient Python
and dynamic-loader overrides are removed. The executable and all code it uses
are trusted product code, not sandboxed untrusted plugins.

Both requests have this shape (values below are placeholders):

```json
{
  "schema": "vq.native-seed-request/1",
  "phase": "plan",
  "transaction": "32 lowercase hex digits",
  "donor": {
    "sha": "40 lowercase hex digits",
    "source": "/opt/example/runtime/releases/DONOR/source",
    "source_content_sha256": "64 lowercase hex digits",
    "venv_content_sha256": "64 lowercase hex digits"
  },
  "candidate": {
    "sha": "40 lowercase hex digits",
    "source": "/opt/example/runtime/releases/CANDIDATE/source"
  }
}
```

The `plan` phase must be read-only. Authenticate a real build-time receipt under
the donor source and compare its complete build inputs with those requested by
the candidate. Return `vq.native-seed-plan/1` with these fields:

| Field | Meaning |
| --- | --- |
| `schema` | `vq.native-seed-plan/1` |
| `decision` | `qualified` |
| `receipt` | Canonical donor-source-relative receipt file |
| `receipt_sha256` | SHA-256 of that receipt's bytes |
| `donor_inputs`, `candidate_inputs` | Objects with exactly `sources`, `patches`, `recipe`, `toolchain`, `options`, `abi`, each a SHA-256 |
| `installs` | Mapping of source-relative install prefix to a complete file manifest |

Each manifest maps an install-relative file name to `{"sha256":"...","mode":420}`
(420 is decimal 0644). Source and destination use identical relative paths.
The adapter defines a versioned, complete semantic identity for each build-input
category, including applied patches, compiler/toolchain, effective options and
ABI. Normalize only path differences that the product has proved immaterial.
Hash an explicit empty category when it genuinely has no inputs. Never use an
unknown value as an empty category or create retrospective build attestations.

Queue checks both input objects are identical, the authenticated donor receipt
matches its digest, every install manifest is complete, and every destination
is absent before the first copy. It checks containment, rejects links, copies
files without sharing writable inodes and verifies the resulting manifests.
Queue does not parse product-specific receipts or independently establish that
the adapter's semantic hashes cover every compiler dependency. That is the
product adapter's qualification responsibility.

The `verify` request adds the accepted plan under `plan` and changes `phase` to
`verify`. The adapter must execute a native operation and data lookup using the
**final candidate copy**, with reads from the donor prefix denied by a supported
isolation mechanism. If isolation is unavailable, return a hold. Report:

```json
{
  "schema": "vq.native-seed-verification/1",
  "decision": "qualified",
  "isolation": "denied-donor-prefix",
  "operation": "native data lookup",
  "result": 42,
  "libraries": [{"path": "/absolute/candidate/install/library", "sha256": "..."}],
  "data": [{"path": "/absolute/candidate/install/data", "sha256": "..."}]
}
```

The paths must be absolute observed loaded-library and consulted-data paths,
with SHA-256 values, not requested paths inferred from command arguments. Both
lists must be nonempty and every entry must match an exact candidate install
file. The adapter owns actual loader observation, native/data behavior and
donor-read denial; Queue checks the structured claims against copied bytes.
It rechecks the donor content seals and candidate immutability after probing.
No venv, mutable object tree, credentials or historical provenance fabrication
belongs in a seed plan.

Only then does the ordinary installer run. It must consume the seeded installs
without compiling their native dependencies. Queue verifies their complete
manifests again after installation and refuses activation if they changed.
This content check cannot detect a rebuild producing byte-identical outputs;
the product installer must separately enforce its no-recompile contract.
Existing import, seal and atomic activation checks still apply.

## Decisions and acceptance limits

The generation's `.vq-native-seed.json` sidecar records the transaction, donor
and candidate identities, adapter hash, plan, probe result and qualification or
hold. It describes preparation, not successful subsequent installation. A
successful update also reports `native_seed=qualified` and the receipt path in
its metrics. Failed unpublished slots retain the decision until the existing
transaction recovery removes that failed generation for a retry. Current and
queued/running generation bindings stay unchanged on failure.

There is no bundled chemistry adapter or default enablement. The tiny native
test demonstrates orchestration and macOS donor-read denial, not qualification
of a chemistry dependency. Real dependency receipts, product-native/data probes,
supported compute-platform isolation and an actual deployment receipt remain
necessary before claiming deployment reuse. Missing product evidence must keep
that reuse request held; it does not stop unrelated calculations.
