# Self-contained macOS package

The Apple Silicon package includes Python 3.13, the `vq` command, the local
daemon and the complete `web` extra. No Python, pip or Homebrew installation
is needed. It requires macOS 14 or newer; qualify each supported OS before
publishing a release. Opening the app starts a Terminal session with `vq`
available. This preserves Queue's command interface.

Drag `vibe-queue.app` to Applications, then open it. Type `vq --help`.
For the web console, run `vq web run --host 127.0.0.1`, then visit
<http://127.0.0.1:8765>. Ctrl-C stops that foreground server. Existing
configuration and queue data remain in their usual user locations outside
the app. Nothing starts automatically or changes remote jobs on opening.

From another terminal, run:

```sh
"/Applications/vibe-queue.app/Contents/MacOS/vq" --help
```

Remote connections use macOS's built-in SSH tools and the user's own
credentials. Remote hosts still need their configured scheduler and engines.
QC engines, source compilers and Linux service managers are not Queue runtime
dependencies and are not included. Source-build/update operations retain their
documented developer prerequisites. Use the complete app when updating;
do not run pip inside its signed resources.

## Build

On an Apple Silicon build machine with Python 3.11+ and Apple's command-line
developer tools:

```sh
python3 packaging/macos/build.py
python3 packaging/macos/build.py --verify dist/macos/vibe-queue.app
python3 packaging/macos/smoke.py dist/macos/vibe-queue.app
```

The builder exports only tracked wheel inputs from the selected Git revision
(`--source-revision`, default HEAD), resolves nothing at package time, verifies
every upstream hash, installs offline and rejects external native library
references. `runtime.lock.json` records the reviewed Python and wheel inputs.
Update that lock deliberately, including native notices when dependencies
change. `--cache` can reuse upstream downloads. The build interpreter and its
site-packages never enter the app.

Outputs in `dist/macos` are an app, DMG, ZIP and checksum file. These local
previews are ad-hoc signed. Developer ID signing, notarization, clean minimum-OS
acceptance and a separately designed Mac App Store workflow remain release
work. Do not advertise these previews as notarized or Store-ready.

## Notices and source

`Contents/Resources/notices` includes the MPL license, exact application source
archive, Python's native notices and the runtime lock. Wheel license files
remain in their distribution metadata. `runtime.json` records source, wheel
and lock hashes and the complete package inventory. Python's standalone build
comes from Astral's python-build-standalone project; its zlib-ng notice is
included separately. Queue's bundled HTMX retains its 0BSD notice.

Git builds remain free. Official website packages may require a supporter
membership; downloaded versions keep working without a subscription check.
Recipients retain all rights granted by the included open-source licenses.
This builder does not implement a payment service or publish any downloads.
