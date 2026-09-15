# `toolset_lifecycle.md`: the vq edits

> **Superseded; kept as a record.** vibe-qc rewrote `toolset_lifecycle.md` for
> the split under vibe-qc#189 (`665b352d`, `a8938c7c`), which covers every
> edit below, including the `--with-vq` question in section 6. Sections 7 and 8
> were fixed on vibe-qc separately (checked at `2e7179e`). Nothing here needs
> doing.

`docs/toolset_lifecycle.md` in the vibe-qc repository documents four
co-located tools at once. Three of them are still subprojects of that
checkout; vq is not, so this page cannot simply be replaced, and rewriting
the vibe-view and vibe-basis material is not vq's call.

What follows is the exact set of vq touchpoints, with replacement text. Line
numbers are against the file as of vibe-qc `5f95d0870`; search for the quoted
text rather than trusting the number.

Every replacement below is free of em and en dashes, per that repository's
house style.

---

## 1. The "At a glance" table, line 27

The vq row points into a directory that no longer exists in the checkout.

**Before**

```markdown
| vq | {{vq_version}} | 3.12+ | `vibe-queue/.venv` | `./vibe-queue/scripts/install.sh` | `vibe-queue/.venv/bin/vq --version` |
```

**After**

```markdown
| vq | {{vq_version}} | 3.12+ | its own checkout | [separate repository](https://github.com/vibe-qc/vibe-queue) | `.venv/bin/vq --version` |
```

```{warning}
`{{vq_version}}` is now broken and fails quietly. See § 7.
```

Add a note under the table:

```markdown
vq is the exception in this table. It used to live in `vibe-queue/` inside
this checkout and it does not any more: it is a separate project, with its
own release line and its own documentation at
<https://vibe-qc.com/vibe-queue/docs/>. Install it from its own repository;
nothing in this checkout installs or updates it.
```

## 2. The native-libraries sentence, line 34

**Before**

```markdown
Only vibe-qc builds the native chemistry libraries. vibe-view, vq, and
vibe-basis are standalone Python installs unless you explicitly request a
combined environment.
```

**After**

```markdown
Only vibe-qc builds the native chemistry libraries. vibe-view and vibe-basis
are standalone Python installs unless you explicitly request a combined
environment. vq is installed from its own repository entirely.
```

## 3. The `### vq` install section, lines 182 to 202

Replace the whole section.

**After**

````markdown
### vq

vq is **not part of this checkout**. Install it from its own repository:

```sh
git clone https://github.com/vibe-qc/vibe-queue.git
cd vibe-queue
./scripts/install.sh
.venv/bin/vq --version
```

The default `core` profile installs the CLI and daemon. `--extras web` adds
the dashboard; `--editable --extras dev` is the development form. Copied
installation is the default and is the safer choice for a stable daemon.

A remote daemon also needs a supervised systemd-user service on Linux, or a
launchd user agent on macOS.

* [Running vibe-qc through the vq queue](user_guide/queue.md) for the
  vibe-qc side: pointing vq at your vibeqc-dev and vibeqc-release
  environments, submitting calculations, and fetching results.
* [vq's operator documentation](https://vibe-qc.com/vibe-queue/docs/operator/index.html)
  for installation, supervision, resource caps and updates.
````

## 4. The "subprojects in one Git repository" admonition, line 238

**Before**

```markdown
vibe-view, vq, and vibe-basis are subprojects in one Git repository. A
Git-aware companion `update.sh` fetches or switches the entire vibe-qc checkout,
not only the component directory.
```

**After**

```markdown
vibe-view and vibe-basis are subprojects in one Git repository. A Git-aware
companion `update.sh` fetches or switches the entire vibe-qc checkout, not
only the component directory. vq is a separate repository and is updated on
its own.
```

## 5. The Git-default difference, line 258

**Before**

```markdown
- vibe-view, vq, and vibe-basis update the currently checked-out branch unless
  you select `--dev`, `--release`, or `--branch NAME`.
```

**After**

```markdown
- vibe-view and vibe-basis update the currently checked-out branch unless you
  select `--dev`, `--release`, or `--branch NAME`.
```

Also drop `./vibe-queue/scripts/uninstall.sh --dry-run` from the preview
block just above it, and the `./vibe-queue/.venv/bin/vq self-update` line
from the update block at line 271. Both name paths that no longer exist.

The surrounding guidance about `vq self-update` and `vq admin update` owning
a serving daemon **stays correct** and is worth keeping; only the paths
change. In a vq checkout the command is `.venv/bin/vq self-update`.

## 6. The remaining path references

Same treatment, all of them replacing a `vibe-queue/`-relative path with
either a vq-checkout-relative one or a link:

| Line | What is there now |
| --- | --- |
| 219 | `./vibe-basis/scripts/install.sh --with-vq`. **Ask the vibe-basis owner**: whether this still works is a vibe-basis question, not a vq one. If it vendored vq from the sibling directory, it is broken. |
| 316 to 335 | The vq direct-script examples and the `--update-script-arg` form. The behaviour is unchanged; the paths are not. |
| 351 | The uninstall table's vq row. Contents are still right: queue state, job history, workspaces, logs, config, service units and `/opt/vq` installs are all preserved. |
| 402 | "The root vibe-qc and vq scripts use the same option roles." Still true, but they are now in two repositories. |
| 430 | The Python floor. Still correct: vibe-qc, vibe-view and vibe-basis want 3.11+, vq wants 3.12+. |
| 454 to 457 | "vq refuses to update while the daemon is running." Still correct. |
| 472, 482 | Links to `user_guide/queue.md`. The target survives; see the replacement page. |

## 7. `{{vq_version}}` is now silently wrong

`docs/conf.py` builds the substitution from `_SIBLING_PYPROJECTS`:

```python
_SIBLING_PYPROJECTS = {
    "vibeview_version": "vibe-view/pyproject.toml",
    "vq_version": "vibe-queue/pyproject.toml",      # gone after the split
    "vibebasis_version": "vibe-basis/pyproject.toml",
}
```

`_read_sibling_version` catches everything and returns `"(unknown)"`, so the
docs build does not fail. It renders `vq | (unknown) |` in the table on line
27 and anywhere else the substitution appears, including `docs/index.md`
line 80.

Failing soft was the right call for a missing sibling. It is the wrong
outcome for a sibling that has permanently moved, because nothing announces
it. Two options, and the choice is the vibe-qc side's:

1. **Hard-code the supported vq version** in `conf.py`, and treat bumping it
   as part of accepting a new vq release. Honest, and it makes the coupling
   visible.
2. **Drop the vq version from the vibe-qc docs** and link to vq's own site,
   which always states its own version. Removes the coupling instead of
   maintaining it.

The second matches how the two sites are now split, and is what the
replacement pages assume: neither of them states a vq version number.

## 8. The website build breaks outright, and that one is not soft

This is outside `docs/`, so it belongs to whoever owns `website/`, but it
falls out of the same split and it fails harder than § 7.

`website/src/data/components.mjs`:

```js
const REPO_ROOT = new URL('../../../', import.meta.url);

function versionOf(relativePath) {
  const path = fileURLToPath(new URL(relativePath, REPO_ROOT));
  const version = readProjectVersion(readFileSync(path, 'utf8'));
  ...
}
```

with `vq` declared as `versionOf('vibe-queue/pyproject.toml')`.

`readFileSync` on a missing path throws, and there is no `try`. Once
`vibe-queue/` is gone from the checkout, `npm run build` fails, which means
**`website-deploy` fails**, which means the marketing site stops shipping.
`website/tests/components.test.mjs` line 31 asserts the same path and fails
with it.

The site presents four components and vq is genuinely one of them, so the
fix is not to drop the card. It is to source vq's version from something
that still exists: a pinned constant in `components.mjs`, or the version vq
publishes on its own site.

Worth doing before the next `website/` change lands, not after: the job runs
automatically on every `main` push that touches `website/`, so the first
person to edit an unrelated `.astro` file is the one who finds this.
