# Changelog fragments

One file per change a user or an operator would notice, named
`<issues>-<topic>.<kind>.md`: the tracker issue number (several joined with
`-`), a short kebab-case topic, and the Keep a Changelog section in lower
case (`added`, `changed`, `deprecated`, `removed`, `fixed`, `security`). The
file holds **one** entry as it will read under `[Unreleased]` in
`CHANGELOG.md`: a bold lead sentence, the body, and the issue number at the
end. Write it as prose, with no list marker and no heading. The fold adds the
`- ` and indents the rest, so a nested list inside the entry is written flush
left and comes out nested. A change that needs an entry in two sections
writes two files, which is what makes each file's name true of its contents.

Do not edit `CHANGELOG.md` for a change; write a fragment. The release cut
folds the fragments with `python scripts/fold_changelog.py`, and
`python scripts/fold_changelog.py --check` (run by the test suite) validates
the names. This file stays so the directory survives a fold that empties it.
See `CONTRIBUTING.md`, "Changelog fragments".
