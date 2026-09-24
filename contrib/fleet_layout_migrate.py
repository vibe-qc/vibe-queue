#!/usr/bin/env python3
"""Move one host onto the shared directory layout, phases 1-4 (vibe-qc#1).

The target layout sorts a host's home by lifecycle, not by content type::

    ~/vq          everything vq runs: runtimes/ and programs/   (versioned)
    ~/vqstate     queue database, leases, audit trail           (durable)
    ~/vqscratch   submitted work, results not yet judged        (disposable)
    ~/vqruns      staging before promotion into a paper repo    (durable)
    ~/bin         wrappers and symlinks only; the PATHS are provenance

This tool covers the three phases that never stop a daemon and never delete:

    1  create the roots (adopting any that already exist, untouched)
    2  make ~/vq/runtimes resolve: a symlink onto the host's existing runtime
       store when there is one, a new directory when there is none. A host
       that runs PACKED releases (bundles unpacked per job by per-SHA
       wrappers) is checked, not changed: its releases hold a bundle
    3  move external-program payloads out of ~/bin into
       ~/vq/programs/<name>-<version>/, leaving a symlink at every old path,
       and installer archives into ~/vq/installers/<name>-<version>/ under
       their vendor file names, with a SHA256SUMS manifest beside them

and a fourth that only ever adds a name:

    4  give the live queue state root the NAME ~/vqstate: a symlink to wherever
       the running daemon keeps its state. The state is never moved. vq's
       managed update requires its state root to be an owner-controlled REAL
       directory (an lstat check on the update lock directory), so putting a
       symlink where vq computes its root makes every `vq admin update` on
       that host refuse. That was learned by doing it once. You name the root
       with --state-root after reading it from the running daemon; the tool
       refuses if the daemon disagrees or if the directory does not look like
       a vq state root. The daemon is not touched and need not stop.

Converging scratch, retiring legacy trees and deleting anything are
deliberately out of scope; see docs/fleet_layout.md.

How a change reaches a host
---------------------------
``plan`` is the default and is strictly read-only. It prints every operation
and a plan id, a SHA-256 over the operations *and the host state they were
computed from*. ``apply`` recomputes the plan and refuses unless ``--approve``
carries that exact id, so what runs is what was reviewed; if the host changed
in between, the id changes and the apply is refused. Every apply writes a
receipt, and ``undo --receipt`` reverses exactly the operations it records.

Why the moves are safe under running jobs
-----------------------------------------
A scheduler host's compute nodes are invisible from its login node, so "no job
is using this" cannot be established. The tool does not need it to be:

* a directory payload and its replacement symlink trade places in ONE atomic
  ``renameat2(RENAME_EXCHANGE)`` call, so the old path resolves at every
  instant and an open handle keeps its inode;
* a file payload is hard-linked to its new home, then a symlink is renamed
  over the old name (atomic ``rename``), so the old path never disappears.

Nothing is copied. A payload on another filesystem than ~/vq is refused rather
than copied, because a copy is neither atomic nor cheap to undo.

Undo is weaker than apply, and says so
--------------------------------------
Resolving a symlink takes two steps: read its text, then walk that text. A
lookup that has read the old path's text is already on its way to the new
location when an undo takes that location away. Undoing a FILE move stays
gap-free, because the hard link at the new location is kept through a grace
period and both names are valid throughout. Undoing a DIRECTORY move cannot be:
a directory has one name, so between the exchange that restores it and the
rename that leaves a back-link behind (two adjacent system calls) an in-flight
lookup can fail with ELOOP. Apply has no such gap, since nothing can reach the
new location until the exchange itself creates the route. Prefer to undo a
directory move while its program is idle.

Hard refusals: anything under ~/Nextcloud, ~/Silo or ~/gitlab (lexically or
after resolving symlinks); any write outside the four roots and ~/bin; any
apply or undo while a deploy looks in flight; overwriting anything that exists.

Runs on any Python >= 3.6 with the standard library only, because a login node
may have nothing newer, and reads nothing from stdin, so it can be piped::

    ssh HOST python3 - plan --phase 1 < contrib/fleet_layout_migrate.py
"""

import argparse
import ctypes
import ctypes.util
import errno
import fcntl
import glob
import hashlib
import json
import os
import platform
import re
import socket
import stat
import subprocess
import sys
import tempfile
import time

SCHEMA = 1
ISSUE = "vibe-qc#1"

# name, mode, lifecycle, prunable
ROOTS = (
    ("vq", 0o755, "versioned releases: runtimes/ and programs/",
     "yes: keep current + previous + N"),
    ("vqstate", 0o700, "durable: queue database, leases, audit trail, run records",
     "never"),
    ("vqscratch", 0o755, "disposable: submitted work, results not yet judged",
     "yes: by age, once vqruns exists"),
    ("vqruns", 0o755, "durable staging before promotion into a calculations repository",
     "no: back up"),
)
VQ_SUBDIRS = (".layout", ".layout/receipts", "programs")

# Sync targets and managed checkouts. A local delete under the first two
# propagates off-machine; the third is `vq admin update` territory.
FORBIDDEN_NAMES = ("Nextcloud", "Silo", "gitlab")

DEPLOY_PATTERNS = (
    r"\bvq\s+admin\s+\S*(update|rollout)",
    r"(^|[/\s])(update|install|reinstall)\.sh(\s|$)",
    r"deploy-[\w-]*runtime",
    r"finalize-[\w-]*runtime",
    r"prepare-[\w-]*runtime-source",
    r"update-scheduler-vq",
)
STATE_DIR_CANDIDATES = (".local/share/vq", ".vq-admin", "vq-state-root", "vqstate")
LEGACY_STORE_GLOB = ".local/libexec/vq-*/runtimes"
FRESH_STAGING_SECONDS = 24 * 3600

ARCHIVE_SUFFIXES = (".tar.gz", ".tgz", ".tar.bz2", ".tar.xz", ".tar", ".zip")
BACKUP_MARKERS = (".bak", ".before-", ".tmp-", ".pre-", ".orig")
PAYLOAD_FILE_BYTES = 1024 * 1024
PER_SHA_WRAPPER_RE = re.compile(r"-[0-9a-f]{40}-python$")
PROGRAM_RE = re.compile(r"^([A-Za-z][A-Za-z0-9_+]*?)-([0-9][A-Za-z0-9_.+-]*)$")

EXIT_OK, EXIT_USAGE, EXIT_BLOCKED, EXIT_MISMATCH, EXIT_FAILED = 0, 2, 3, 4, 5


class Refusal(Exception):
    """A request this tool will not carry out."""


# --------------------------------------------------------------------------
# scope: where this tool may write
# --------------------------------------------------------------------------

def _is_under(path, root):
    root = root.rstrip("/")
    return path == root or path.startswith(root + "/")


def _real_of(path):
    """Resolve everything but the final component, which may not exist yet or
    may be the very symlink being written."""
    parent = os.path.realpath(os.path.dirname(path))
    return os.path.join(parent, os.path.basename(path))


def forbidden_roots(home):
    out = []
    for name in FORBIDDEN_NAMES:
        lexical = os.path.join(home, name)
        out.append(lexical)
        if os.path.lexists(lexical):
            out.append(os.path.realpath(lexical))
    return out


def check_scope(home, path):
    """Raise Refusal unless `path` is somewhere this tool may write."""
    lexical = os.path.normpath(path)
    allowed = [os.path.join(home, name) for name, _, _, _ in ROOTS]
    allowed.append(os.path.join(home, "bin"))
    if not any(_is_under(lexical, a) for a in allowed):
        raise Refusal("outside the layout roots and ~/bin: %s" % lexical)
    real = _real_of(lexical)
    for bad in forbidden_roots(home):
        if _is_under(lexical, bad) or _is_under(real, bad):
            raise Refusal("protected tree (%s): %s" % (bad, lexical))


def check_op_scope(home, op):
    for key in ("path", "src", "dst"):
        if key not in op:
            continue
        check_scope(home, op[key])
    check_link_target(home, op)


def check_link_target(home, op):
    """A symlink this tool makes, or a root it adopts through a symlink, must
    not lead into a protected tree either."""
    if op["op"] == "symlink":
        target = op["target"]
        if not os.path.isabs(target):
            target = os.path.join(os.path.dirname(op["path"]), target)
    elif op["op"] == "adopt":
        target = op["path"]
    else:
        return
    real = os.path.realpath(target)
    for bad in forbidden_roots(home):
        if _is_under(real, bad):
            raise Refusal("leads into a protected tree (%s): %s" % (bad, op["path"]))


# --------------------------------------------------------------------------
# atomic exchange of two directory entries
# --------------------------------------------------------------------------

_SYS_RENAMEAT2 = {"x86_64": 316, "aarch64": 276, "ppc64le": 357, "s390x": 347}


def exchange(a, b):
    """Atomically swap the directory entries `a` and `b`."""
    libc = ctypes.CDLL(ctypes.util.find_library("c") or "libc.so.6", use_errno=True)
    a_b, b_b = os.fsencode(a), os.fsencode(b)
    if sys.platform.startswith("linux"):
        at_fdcwd, rename_exchange = -100, 2
        fn = getattr(libc, "renameat2", None)
        if fn is not None:
            rc = fn(at_fdcwd, a_b, at_fdcwd, b_b, ctypes.c_uint(rename_exchange))
        else:
            number = _SYS_RENAMEAT2.get(platform.machine())
            if number is None:
                raise OSError(errno.ENOSYS, "renameat2 is unavailable on this platform")
            rc = libc.syscall(number, at_fdcwd, a_b, at_fdcwd, b_b,
                              ctypes.c_uint(rename_exchange))
    elif sys.platform == "darwin":
        rc = libc.renamex_np(a_b, b_b, ctypes.c_uint(2))  # RENAME_SWAP
    else:
        raise OSError(errno.ENOSYS, "no atomic exchange on %s" % sys.platform)
    if rc != 0:
        err = ctypes.get_errno()
        raise OSError(err, "atomic exchange failed: %s" % os.strerror(err), a)


def probe_exchange(directory):
    """Prove a directory and a symlink can trade places on this filesystem."""
    tmp = tempfile.mkdtemp(prefix=".exchange-probe-", dir=directory)
    d, link = os.path.join(tmp, "d"), os.path.join(tmp, "l")
    try:
        os.mkdir(d)
        os.symlink(d, link)
        exchange(d, link)
        if not (os.path.islink(d) and os.path.isdir(link) and not os.path.islink(link)):
            raise OSError(errno.EIO, "exchange returned success but did not swap")
        exchange(d, link)
    finally:
        for p in (link, d):
            try:
                if os.path.islink(p):
                    os.unlink(p)
                elif os.path.isdir(p):
                    os.rmdir(p)
            except OSError:
                pass
        try:
            os.rmdir(tmp)
        except OSError:
            pass


# --------------------------------------------------------------------------
# in-flight deploy detection
# --------------------------------------------------------------------------

def _ancestors(pid):
    seen = set()
    while pid > 1 and pid not in seen:
        seen.add(pid)
        try:
            with open("/proc/%d/stat" % pid) as fh:
                pid = int(fh.read().rsplit(")", 1)[1].split()[1])
        except (OSError, ValueError, IndexError):
            break
    return seen


def list_own_cmdlines():
    """[(pid, cmdline)] for this user's other processes."""
    uid, out = os.getuid(), []
    skip = _ancestors(os.getpid()) | {os.getpid()}
    if os.path.isdir("/proc/self"):
        for name in os.listdir("/proc"):
            if not name.isdigit() or int(name) in skip:
                continue
            try:
                if os.stat("/proc/" + name).st_uid != uid:
                    continue
                with open("/proc/%s/cmdline" % name, "rb") as fh:
                    raw = fh.read()
            except OSError:
                continue
            if raw:
                out.append((int(name), raw.replace(b"\0", b" ").decode("utf-8", "replace").strip()))
        return out
    proc = subprocess.Popen(["ps", "-U", str(uid), "-o", "pid=,command="],
                            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                            universal_newlines=True)
    text, _ = proc.communicate()
    for line in text.splitlines():
        parts = line.strip().split(None, 1)
        if len(parts) == 2 and parts[0].isdigit() and int(parts[0]) not in skip:
            out.append((int(parts[0]), parts[1]))
    return out


def deploy_blockers(home, extra_patterns=()):
    """Reasons to believe a deploy is in flight. Any one blocks apply and undo:
    a deploy writes a tree and flips a pointer, so a layout change taken
    mid-roll acts on a target that is about to move."""
    blockers, notes = [], []
    patterns = [re.compile(p) for p in tuple(DEPLOY_PATTERNS) + tuple(extra_patterns)]
    for pid, cmd in list_own_cmdlines():
        if any(p.search(cmd) for p in patterns):
            blockers.append("deploy process running: pid %d: %s" % (pid, cmd[:160]))
    stores = glob.glob(os.path.join(home, LEGACY_STORE_GLOB))
    stores.append(os.path.join(home, "vq", "runtimes"))
    seen = set()
    for store in stores:
        real = os.path.realpath(store)
        if real in seen or not os.path.isdir(real):
            continue
        seen.add(real)
        for lock in sorted(glob.glob(os.path.join(real, "locks", "*"))):
            blockers.append("runtime store lock present: %s" % lock)
        for entry in sorted(glob.glob(os.path.join(real, "*", "staging", "*"))):
            try:
                age = time.time() - os.lstat(entry).st_mtime
            except OSError:
                continue
            if age < FRESH_STAGING_SECONDS:
                blockers.append("staging entry younger than 24 h: %s" % entry)
            else:
                notes.append("stale staging entry (%.0f h old), left alone: %s"
                             % (age / 3600.0, entry))
    for rel in STATE_DIR_CANDIDATES:
        base = os.path.join(home, rel)
        for pattern in ("admin-update-in-progress*", os.path.join("*", "admin-update-in-progress*")):
            for marker in sorted(glob.glob(os.path.join(base, pattern))):
                blockers.append("admin update marker present: %s" % marker)
    return blockers, notes


DAEMON_RE = re.compile(r"(^|[/\s])vq(\s+|\s.*\s)daemon\s+run(\s|$)")


def vq_daemons(home):
    """[(pid, state_root)] for this user's running vq daemons, the root read
    from each daemon's own environment: that, not a directory that merely looks
    like state, says where the live store is."""
    out = []
    for pid, cmd in list_own_cmdlines():
        if not DAEMON_RE.search(cmd):
            continue
        env = {}
        try:
            with open("/proc/%d/environ" % pid, "rb") as fh:
                for item in fh.read().split(b"\0"):
                    key, _, value = item.partition(b"=")
                    env[key.decode("utf-8", "replace")] = value.decode("utf-8", "replace")
        except OSError:
            pass
        root = env.get("VQ_STATE_DIR") or os.path.join(
            env.get("XDG_DATA_HOME") or os.path.join(home, ".local", "share"), "vq")
        out.append((pid, os.path.expanduser(root)))
    return out


# --------------------------------------------------------------------------
# survey
# --------------------------------------------------------------------------

def classify_bin_entry(path):
    st = os.lstat(path)
    name = os.path.basename(path)
    if stat.S_ISLNK(st.st_mode):
        return "symlink"
    if stat.S_ISDIR(st.st_mode):
        return "payload-dir"
    if not stat.S_ISREG(st.st_mode):
        return "other"
    if any(name.endswith(s) for s in ARCHIVE_SUFFIXES):
        return "archive"
    if any(m in name for m in BACKUP_MARKERS):
        return "backup"
    if st.st_size >= PAYLOAD_FILE_BYTES:
        return "payload-file"
    try:
        with open(path, "rb") as fh:
            if fh.read(4) == b"\x7fELF":
                return "payload-file"
    except OSError:
        pass
    return "wrapper"


def survey(home):
    out = {"schema": SCHEMA, "home": home, "roots": {}, "bin": None, "legacy_stores": []}
    for name, _, _, _ in ROOTS:
        p = os.path.join(home, name)
        if os.path.islink(p):
            out["roots"][name] = "symlink -> %s" % os.readlink(p)
        elif os.path.isdir(p):
            out["roots"][name] = "directory (%d entries)" % len(os.listdir(p))
        elif os.path.lexists(p):
            out["roots"][name] = "NOT A DIRECTORY"
        else:
            out["roots"][name] = "absent"
    bindir = os.path.join(home, "bin")
    if os.path.isdir(bindir):
        entries = []
        for name in sorted(os.listdir(bindir)):
            p = os.path.join(bindir, name)
            kind = classify_bin_entry(p)
            row = {"name": name, "kind": kind}
            if kind == "symlink":
                row["target"] = os.readlink(p)
                row["resolves"] = os.path.exists(p)
            elif kind != "payload-dir":
                row["bytes"] = os.lstat(p).st_size
            entries.append(row)
        out["bin"] = entries
    for store in sorted(glob.glob(os.path.join(home, LEGACY_STORE_GLOB))):
        if os.path.isdir(store) and not os.path.islink(store):
            comps = {}
            for comp in sorted(os.listdir(store)):
                cur = os.path.join(store, comp, "current")
                if os.path.islink(cur):
                    comps[comp] = os.readlink(cur)
            out["legacy_stores"].append({"path": store, "current": comps})
    blockers, notes = deploy_blockers(home)
    out["deploy_blockers"], out["deploy_notes"] = blockers, notes
    return out


# --------------------------------------------------------------------------
# planning (read-only)
# --------------------------------------------------------------------------

def layout_record():
    doc = {
        "schema": SCHEMA,
        "issue": ISSUE,
        "principle": "sort by lifecycle: can this be regenerated, and if not, is it backed up?",
        "roots": [{"path": "~/" + n, "lifecycle": life, "prunable": prune}
                  for n, _, life, prune in ROOTS],
        "bin": "~/bin holds wrappers and symlinks only; its paths are provenance and must "
               "keep resolving",
        "never": ["~/Nextcloud", "~/Silo"],
    }
    return json.dumps(doc, indent=2, sort_keys=True) + "\n"


def _observe(path):
    st = os.lstat(path)
    seen = {"ino": st.st_ino, "dev": st.st_dev, "mode": "%o" % stat.S_IMODE(st.st_mode)}
    if stat.S_ISREG(st.st_mode):
        seen["bytes"] = st.st_size
        seen["nlink"] = st.st_nlink
    return seen


class Plan(object):
    def __init__(self, home, phase):
        self.home, self.phase = home, phase
        self.ops, self.blockers, self.notes = [], [], []
        self._planned_dirs = set()

    def add(self, op):
        try:
            check_op_scope(self.home, op)
        except Refusal as exc:
            self.blockers.append(str(exc))
            return
        self.ops.append(op)

    def want_dir(self, path, mode=0o755):
        """Plan a directory: adopt an existing one, create an absent one."""
        if path in self._planned_dirs:
            return
        self._planned_dirs.add(path)
        if os.path.islink(path):
            if os.path.isdir(path):
                self.add({"op": "adopt", "path": path,
                          "as": "symlink -> %s" % os.readlink(path)})
            else:
                self.blockers.append("dangling symlink where a directory is needed: %s" % path)
        elif os.path.isdir(path):
            self.add({"op": "adopt", "path": path, "as": "directory"})
        elif os.path.lexists(path):
            self.blockers.append("not a directory: %s" % path)
        else:
            self.add({"op": "mkdir", "path": path, "mode": "%o" % mode})

    def plan_id(self):
        # The host name is part of the identity: two hosts with the same home
        # path and the same gaps would otherwise share an id, and an approval
        # given for one could be replayed on the other.
        body = {"schema": SCHEMA, "phase": self.phase, "host": socket.gethostname(),
                "home": self.home, "ops": self.ops}
        return hashlib.sha256(json.dumps(body, sort_keys=True).encode("utf-8")).hexdigest()


def plan_phase1(plan, daemonless=False):
    home = plan.home
    for name, mode, _, _ in ROOTS:
        if daemonless and name == "vqstate":
            # A scheduler login host runs no daemon and so has no queue state.
            # An empty, durable-looking ~/vqstate there would be one more layer
            # that looks live and is not.
            plan.notes.append("daemonless host: no ~/vqstate is created")
            continue
        plan.want_dir(os.path.join(home, name), mode)
    for sub in VQ_SUBDIRS:
        plan.want_dir(os.path.join(home, "vq", sub))
    record = os.path.join(home, "vq", ".layout", "layout.json")
    content = layout_record()
    digest = hashlib.sha256(content.encode("utf-8")).hexdigest()
    if os.path.lexists(record):
        with open(record, "rb") as fh:
            if hashlib.sha256(fh.read()).hexdigest() != digest:
                plan.notes.append("existing %s differs from this version; left alone" % record)
    else:
        plan.add({"op": "write_new", "path": record, "sha256": digest})


def plan_phase2(plan):
    home = plan.home
    vq = os.path.join(home, "vq")
    target = os.path.join(vq, "runtimes")
    if not os.path.isdir(vq):
        plan.blockers.append("~/vq does not exist: apply phase 1 first "
                             "(what follows is a preview)")
    stores = [s for s in sorted(glob.glob(os.path.join(home, LEGACY_STORE_GLOB)))
              if os.path.isdir(s) and not os.path.islink(s)]
    if len(stores) > 1:
        plan.blockers.append("more than one legacy runtime store; a person must choose: %s"
                             % ", ".join(stores))
        return
    if not stores:
        wrappers = [w for w in glob.glob(os.path.join(home, ".local/libexec/vq-*/*-python"))
                    if PER_SHA_WRAPPER_RE.search(os.path.basename(w))]
        packed = glob.glob(os.path.join(target, "*", "releases", "*", "SHA256SUMS"))
        if wrappers or packed:
            # Per-SHA wrappers (or bundles already published) with no unpacked
            # releases/ tree: this host runs PACKED releases, a bundle unpacked
            # per job by its wrapper. It is not exempt from the layout; its
            # releases just hold a bundle.
            plan_packed_store(plan, target, len(wrappers))
            return
        plan.want_dir(target)
        plan.notes.append("no legacy runtime store on this host: ~/vq/runtimes is a new, empty "
                          "store. Nothing deploys into it until a program's runtime_slot_root "
                          "(or the site's deploy script) is pointed there; that is a config "
                          "change and is not made by this tool.")
        return
    store = stores[0]
    if os.path.islink(target):
        if os.path.realpath(target) == os.path.realpath(store):
            plan.notes.append("~/vq/runtimes already resolves to %s" % store)
        else:
            plan.blockers.append("~/vq/runtimes points somewhere else: %s" % os.readlink(target))
        return
    if os.path.lexists(target):
        plan.blockers.append("~/vq/runtimes exists and a legacy store exists too (%s); "
                             "a person must reconcile them" % store)
        return
    plan.add({"op": "symlink", "path": target, "target": store,
              "observed": {"store_ino": os.stat(store).st_ino}})
    plan.notes.append("the store stays where it is; ~/vq/runtimes becomes a second name for it, "
                      "so in-flight jobs, venv shebangs and old receipts keep resolving. "
                      "Deploy scripts that compare a resolved path against an unresolved root "
                      "must be checked before they are pointed at the new name.")


def plan_packed_store(plan, target, wrapper_count):
    """Phase 2 on a host whose releases are packed bundles.

    The contract is releases/<sha>, current, previous. It does not say a
    release must be an unpacked tree: here it holds the bundle, a SHA256SUMS,
    and `python`, a link to the per-SHA wrapper that verifies the bundle,
    unpacks it on the compute node and execs it. Publishing bundles is the
    site deploy tooling's job, so this only checks the store (without hashing:
    a plan stays cheap) and never builds or repairs it.
    """
    releases = sorted(glob.glob(os.path.join(target, "*", "releases", "*")))
    if not releases:
        plan.blockers.append(
            "%d per-SHA runtime wrappers and no unpacked runtimes/ store: this host runs packed "
            "releases, and ~/vq/runtimes has none yet. It is filled by the site's own tooling "
            "(one directory per release: the bundle, SHA256SUMS, and `python` -> the per-SHA "
            "wrapper). An empty directory here would be a layer that looks live and is not"
            % wrapper_count)
        return
    problems = 0
    for rel in releases:
        sums = os.path.join(rel, "SHA256SUMS")
        names = []
        if os.path.isfile(sums):
            with open(sums) as fh:
                fields = [line.split(None, 1) for line in fh]
                names = [f[1].strip() for f in fields if len(f) == 2]
        missing = [n for n in names if not os.path.isfile(os.path.join(rel, n))]
        if not names or missing:
            plan.blockers.append("packed release without its bundle or SHA256SUMS: %s" % rel)
            problems += 1
        elif not os.path.exists(os.path.join(rel, "python")):
            plan.blockers.append("packed release whose `python` wrapper link does not resolve: %s"
                                 % rel)
            problems += 1
    components = sorted(set(os.path.basename(os.path.dirname(os.path.dirname(r)))
                            for r in releases))
    for comp in components:
        for pointer in ("current", "previous"):
            link = os.path.join(target, comp, pointer)
            if os.path.islink(link) and not os.path.isdir(link):
                plan.blockers.append("dangling %s pointer: %s" % (pointer, link))
                problems += 1
        if not os.path.islink(os.path.join(target, comp, "current")):
            plan.notes.append("%s has releases but no `current` pointer" % comp)
    if not problems:
        plan.notes.append("packed-release store: %d release(s) in %d component(s), every bundle "
                          "and wrapper link present (bundles are not hashed by a plan; run "
                          "`sha256sum -c SHA256SUMS` in a release to verify one)"
                          % (len(releases), len(components)))


def parse_mapping(values, what):
    out = []
    for value in values or ():
        if "=" not in value:
            raise Refusal("%s needs NAME-VERSION=ENTRY[,ENTRY...]: %r" % (what, value))
        label, entries = value.split("=", 1)
        match = PROGRAM_RE.match(label)
        if not match:
            raise Refusal("%s label must look like name-1.2.3: %r" % (what, label))
        names = [e for e in entries.split(",") if e]
        for entry in names:
            if "/" in entry or entry in (".", ".."):
                raise Refusal("%s entries are plain names inside ~/bin: %r" % (what, entry))
        if not names:
            raise Refusal("%s names no entries: %r" % (what, value))
        out.append((label, match.group(1), names))
    return out


def plan_phase3(plan, programs, archives):
    home = plan.home
    bindir = os.path.join(home, "bin")
    progroot = os.path.join(home, "vq", "programs")
    if not os.path.isdir(progroot):
        plan.blockers.append("~/vq/programs does not exist: apply phase 1 first "
                             "(what follows is a preview)")
    if os.path.islink(bindir):
        plan.blockers.append("~/bin is itself a symlink; a person must look at this host")
        return
    if not os.path.isdir(bindir):
        plan.notes.append("no ~/bin on this host: nothing to move")
        return
    prog_dev = os.stat(progroot if os.path.isdir(progroot) else home).st_dev
    mapped = set()

    def source(entry, label):
        path = os.path.join(bindir, entry)
        if not os.path.lexists(path):
            plan.blockers.append("%s: ~/bin/%s does not exist" % (label, entry))
            return None, None
        kind = classify_bin_entry(path)
        if kind == "symlink":
            if _is_under(os.path.realpath(path), os.path.realpath(progroot)):
                plan.notes.append("%s: ~/bin/%s already resolves into ~/vq/programs"
                                  % (label, entry))
            else:
                plan.blockers.append("%s: ~/bin/%s is a symlink, not a payload" % (label, entry))
            return None, None
        if os.lstat(path).st_dev != prog_dev:
            plan.blockers.append("%s: ~/bin/%s is on another filesystem than ~/vq; this tool "
                                 "renames and never copies" % (label, entry))
            return None, None
        return path, kind

    for label, name, entries in programs:
        dst_dir = os.path.join(progroot, label)
        found = []
        for entry in entries:
            mapped.add(entry)
            path, kind = source(entry, label)
            if path:
                found.append((entry, path, kind))
        if not found:
            continue
        kinds = set(k for _, _, k in found)
        if "payload-dir" in kinds:
            if len(found) != 1:
                plan.blockers.append("%s: a directory payload must be the program's only entry"
                                     % label)
                continue
            if os.path.lexists(dst_dir):
                plan.blockers.append("%s: %s already exists" % (label, dst_dir))
                continue
            plan.add({"op": "relocate_dir", "src": found[0][1], "dst": dst_dir,
                      "observed": _observe(found[0][1])})
        else:
            plan.want_dir(dst_dir)
            plan.want_dir(os.path.join(dst_dir, "bin"))
            for entry, path, _ in found:
                dst = os.path.join(dst_dir, "bin", entry)
                if os.path.lexists(dst):
                    plan.blockers.append("%s: %s already exists" % (label, dst))
                    continue
                plan.add({"op": "relocate_file", "src": path, "dst": dst,
                          "observed": _observe(path)})
        inuse = os.path.join(progroot, name)
        if os.path.islink(inuse):
            if os.readlink(inuse) != label:
                plan.notes.append("~/vq/programs/%s already selects %s; left alone"
                                  % (name, os.readlink(inuse)))
        elif os.path.lexists(inuse):
            plan.notes.append("~/vq/programs/%s exists and is not a symlink; left alone" % name)
        else:
            plan.add({"op": "symlink", "path": inuse, "target": label})

    inst_root = os.path.join(home, "vq", "installers")
    for label, _, entries in archives:
        dist = os.path.join(inst_root, label)
        moving = []
        for entry in entries:
            mapped.add(entry)
            if (not os.path.lexists(os.path.join(bindir, entry))
                    and os.path.isfile(os.path.join(dist, entry))):
                plan.notes.append("%s: %s is already in ~/vq/installers" % (label, entry))
                continue
            path, kind = source(entry, label)
            if not path:
                continue
            if kind != "archive":
                plan.blockers.append("%s: ~/bin/%s is %s, not an archive" % (label, entry, kind))
                continue
            plan.want_dir(inst_root)
            plan.want_dir(dist)
            dst = os.path.join(dist, entry)
            if os.path.lexists(dst):
                plan.blockers.append("%s: %s already exists" % (label, dst))
                continue
            plan.add({"op": "move", "src": path, "dst": dst, "observed": _observe(path)})
            moving.append(entry)
        if moving:
            manifest = os.path.join(dist, "SHA256SUMS")
            if os.path.lexists(manifest):
                plan.notes.append("%s: %s exists; left alone" % (label, manifest))
            else:
                plan.add({"op": "manifest", "path": manifest, "files": sorted(moving)})
            plan.notes.append("%s: %d installer archive(s) are KEPT, under their vendor names. "
                              "Their ~/bin paths stop resolving: an archive is moved, not "
                              "linked, because no job receipt names an installer."
                              % (label, len(moving)))

    for entry in sorted(os.listdir(bindir)):
        if entry in mapped:
            continue
        kind = classify_bin_entry(os.path.join(bindir, entry))
        if kind in ("payload-dir", "payload-file", "archive"):
            plan.notes.append("unmapped %s stays in ~/bin: %s" % (kind, entry))


def plan_phase4(plan, state_root):
    home = plan.home
    target = os.path.join(home, "vqstate")
    if not state_root:
        plan.blockers.append("--state-root is required: read it from the running daemon "
                             "(VQ_STATE_DIR in its environment, else the XDG default)")
        return
    named = os.path.abspath(os.path.expanduser(state_root))
    root = os.path.realpath(named)
    if not os.path.isdir(root):
        plan.blockers.append("no such state root: %s" % named)
        return
    if not (os.path.isdir(os.path.join(root, "queue")) and os.path.isdir(os.path.join(root, "jobs"))):
        plan.blockers.append("%s has no queue/ and jobs/: it does not look like a vq state root"
                             % named)
        return
    if named != root:
        plan.blockers.append("%s is a symlink to %s. vq's managed update refuses a symlinked "
                             "state root (it needs an owner-controlled real directory), so this "
                             "host cannot take `vq admin update` until the real directory is "
                             "back at that path" % (named, root))
        return
    daemons = vq_daemons(home)
    live = [(pid, r) for pid, r in daemons if os.path.realpath(r) == root]
    if daemons and not live:
        plan.blockers.append("the running daemon uses %s, not the root you named (%s)"
                             % (", ".join(sorted(set(r for _, r in daemons))), named))
        return
    if os.path.realpath(target) == root:
        plan.notes.append("~/vqstate already is the live state root")
        return
    if os.path.islink(target):
        plan.blockers.append("~/vqstate is a symlink to something else: %s" % os.readlink(target))
        return
    if os.path.lexists(target):
        if not os.path.isdir(target) or os.listdir(target):
            plan.blockers.append("~/vqstate is not an empty directory while the live root is %s; "
                                 "two stores must be reconciled by a person, never merged by "
                                 "a tool" % named)
            return
        plan.add({"op": "rmdir_empty", "path": target, "observed": _observe(target)})

    plan.add({"op": "symlink", "path": target, "target": root,
              "observed": {"root_ino": os.stat(root).st_ino}})
    plan.notes.append("the state root stays exactly where the daemon computes it; ~/vqstate "
                      "becomes its name. vq's managed update demands that its state root be a "
                      "real directory, so the root is never replaced by a symlink. The daemon "
                      "is not touched and need not stop.")


def build_plan(home, phase, programs=(), archives=(), extra_patterns=(), state_root=None,
               daemonless=False):
    plan = Plan(home, phase)
    if phase == 1:
        plan_phase1(plan, daemonless)
    elif phase == 2:
        plan_phase2(plan)
    elif phase == 3:
        plan_phase3(plan, programs, archives)
    elif phase == 4:
        plan_phase4(plan, state_root)
    else:
        raise Refusal("this tool covers phases 1 to 4 only")
    blockers, notes = deploy_blockers(home, extra_patterns)
    plan.blockers.extend(blockers)
    plan.notes.extend(notes)
    return plan


# --------------------------------------------------------------------------
# operations
# --------------------------------------------------------------------------

def _require_unchanged(op):
    seen = op.get("observed")
    if seen and "ino" in seen and _observe(op["src"]) != seen:
        raise Refusal("source changed since the plan was made: %s" % op["src"])


def do_op(home, op):
    check_op_scope(home, op)
    kind = op["op"]
    if kind == "adopt":
        return
    if kind == "mkdir":
        os.mkdir(op["path"], int(op["mode"], 8))
        os.chmod(op["path"], int(op["mode"], 8))
    elif kind == "rmdir_empty":
        path = op["path"]
        if os.path.islink(path) or not os.path.isdir(path) or os.listdir(path):
            raise Refusal("not an empty directory any more: %s" % path)
        if _observe(path) != op["observed"]:
            raise Refusal("directory changed since the plan was made: %s" % path)
        os.rmdir(path)
    elif kind == "write_new":
        content = layout_record().encode("utf-8")
        if hashlib.sha256(content).hexdigest() != op["sha256"]:
            raise Refusal("layout record no longer matches the plan")
        fd = os.open(op["path"], os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        with os.fdopen(fd, "wb") as fh:
            fh.write(content)
    elif kind == "symlink":
        os.symlink(op["target"], op["path"])
        if not os.path.exists(op["path"]):
            os.unlink(op["path"])
            raise Refusal("new symlink would dangle: %s -> %s" % (op["path"], op["target"]))
    elif kind == "relocate_dir":
        src, dst = op["src"], op["dst"]
        _require_unchanged(op)
        os.symlink(dst, dst)  # placeholder; names its own final target
        try:
            exchange(src, dst)
        except OSError:
            os.unlink(dst)
            raise
        if not (os.path.islink(src) and os.readlink(src) == dst
                and os.path.isdir(dst) and not os.path.islink(dst)):
            raise Refusal("exchange did not leave the expected state at %s" % src)
    elif kind == "relocate_file":
        src, dst = op["src"], op["dst"]
        _require_unchanged(op)
        os.link(src, dst)
        tmp = os.path.join(os.path.dirname(src), ".%s.vq-layout-tmp" % os.path.basename(src))
        try:
            os.symlink(dst, tmp)
            os.replace(tmp, src)
        except OSError:
            for leftover in (tmp, dst):
                if os.path.lexists(leftover):
                    os.unlink(leftover)
            raise
        if not os.path.samefile(src, dst):
            raise Refusal("old path does not resolve to the moved file: %s" % src)
    elif kind == "move":
        _require_unchanged(op)
        if os.path.lexists(op["dst"]):
            raise Refusal("destination exists: %s" % op["dst"])
        os.rename(op["src"], op["dst"])
    elif kind == "manifest":
        # `sha256sum -c SHA256SUMS` format. Written at apply time: hashing
        # gigabytes belongs to the run that was approved, not to every plan.
        directory, lines = os.path.dirname(op["path"]), []
        for name in op["files"]:
            digest = hashlib.sha256()
            with open(os.path.join(directory, name), "rb") as fh:
                for block in iter(lambda: fh.read(1 << 20), b""):
                    digest.update(block)
            lines.append("%s  %s\n" % (digest.hexdigest(), name))
        content = "".join(lines).encode("utf-8")
        fd = os.open(op["path"], os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        with os.fdopen(fd, "wb") as fh:
            fh.write(content)
        op["sha256"] = hashlib.sha256(content).hexdigest()
    else:
        raise Refusal("unknown operation %r" % kind)


def undo_op(home, op, deferred):
    """Reverse one recorded operation, or explain why it is left alone.

    Names that lookups already in flight may still be walking towards are not
    removed here; they are appended to `deferred` for removal after a grace
    period.
    """
    check_op_scope(home, op)
    kind = op["op"]
    if kind == "adopt":
        return "adopted, never ours to remove"
    if kind == "mkdir":
        path = op["path"]
        if not os.path.isdir(path) or os.path.islink(path):
            return "no longer a directory"
        if os.listdir(path):
            return "not empty, left in place"
        os.rmdir(path)
    elif kind == "rmdir_empty":
        path = op["path"]
        if os.path.lexists(path):
            return "something is there again, left in place"
        os.mkdir(path, int(op["observed"]["mode"], 8))
        os.chmod(path, int(op["observed"]["mode"], 8))
    elif kind in ("write_new", "manifest"):
        path = op["path"]
        if not os.path.isfile(path):
            return "already gone"
        with open(path, "rb") as fh:
            if hashlib.sha256(fh.read()).hexdigest() != op["sha256"]:
                return "content changed, left in place"
        os.unlink(path)
    elif kind == "symlink":
        path = op["path"]
        if not os.path.islink(path) or os.readlink(path) != op["target"]:
            return "no longer the symlink this tool made"
        os.unlink(path)
    elif kind == "relocate_dir":
        src, dst = op["src"], op["dst"]
        if not (os.path.islink(src) and os.readlink(src) == dst):
            return "old path is no longer this tool's symlink"
        if not os.path.isdir(dst) or os.path.islink(dst):
            return "moved directory is missing"
        back = dst + ".vq-layout-back"
        os.symlink(src, back)
        try:
            exchange(src, dst)      # src is the directory again; dst loops onto itself
        except OSError:
            os.unlink(back)
            raise
        os.replace(back, dst)       # dst now leads back to src
        deferred.append(dst)
    elif kind == "relocate_file":
        src, dst = op["src"], op["dst"]
        if not (os.path.islink(src) and os.readlink(src) == dst):
            return "old path is no longer this tool's symlink"
        if not os.path.isfile(dst):
            return "moved file is missing"
        tmp = os.path.join(os.path.dirname(src), ".%s.vq-layout-tmp" % os.path.basename(src))
        os.link(dst, tmp)
        os.replace(tmp, src)
        deferred.append(dst)        # still a valid name for the same inode
    elif kind == "move":
        if os.path.lexists(op["src"]):
            return "something else now holds the old name"
        if not os.path.lexists(op["dst"]):
            return "moved file is missing"
        os.rename(op["dst"], op["src"])
    return None


# --------------------------------------------------------------------------
# receipts
# --------------------------------------------------------------------------

def _write_json_atomic(path, doc):
    tmp = "%s.tmp-%d" % (path, os.getpid())
    with open(tmp, "w") as fh:
        json.dump(doc, fh, indent=2, sort_keys=True)
        fh.write("\n")
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def save_receipt(home, receipt):
    """Write the receipt if it can be written; None if it cannot.

    It cannot while phase 1 has not yet made the receipts directory, and it
    cannot on a host that is over quota, which is exactly when the first
    operation fails too. The caller must not let that hide the real error.
    """
    directory = os.path.join(home, "vq", ".layout", "receipts")
    if not os.path.isdir(directory):
        return None
    path = os.path.join(directory, receipt["name"])
    try:
        _write_json_atomic(path, receipt)
    except OSError as exc:
        print("  warning: receipt not written (%s)" % exc)
        return None
    return path


class _Lock(object):
    def __init__(self):
        self.path = os.path.join(tempfile.gettempdir(), "vq-layout-%d.lock" % os.getuid())
        self.fd = None

    def __enter__(self):
        self.fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            os.close(self.fd)
            raise Refusal("another layout migration is running on this host")
        return self

    def __exit__(self, *exc):
        fcntl.flock(self.fd, fcntl.LOCK_UN)
        os.close(self.fd)


# --------------------------------------------------------------------------
# commands
# --------------------------------------------------------------------------

def _short(home, path):
    return "~" + path[len(home):] if _is_under(path, home) else path


def describe(home, op):
    kind = op["op"]
    if kind == "adopt":
        return "adopt      %s  (exists as %s; untouched)" % (_short(home, op["path"]), op["as"])
    if kind == "mkdir":
        return "mkdir      %s  (mode %s)" % (_short(home, op["path"]), op["mode"])
    if kind == "rmdir_empty":
        return "rmdir      %s  (empty; makes way for the name)" % _short(home, op["path"])
    if kind == "write_new":
        return "write      %s  (layout record, sha256 %s...)" % (_short(home, op["path"]),
                                                                 op["sha256"][:12])
    if kind == "symlink":
        return "symlink    %s -> %s" % (_short(home, op["path"]), op["target"])
    if kind == "relocate_dir":
        return ("relocate   %s => %s  (atomic exchange; old path becomes a symlink)"
                % (_short(home, op["src"]), _short(home, op["dst"])))
    if kind == "relocate_file":
        return ("relocate   %s => %s  (hard link, then symlink renamed over the old name)"
                % (_short(home, op["src"]), _short(home, op["dst"])))
    if kind == "move":
        return "move       %s => %s  (kept; no link left behind)" % (_short(home, op["src"]),
                                                                     _short(home, op["dst"]))
    if kind == "manifest":
        return "manifest   %s  (sha256 of %d archive(s))" % (_short(home, op["path"]),
                                                            len(op["files"]))
    return json.dumps(op, sort_keys=True)


def print_plan(plan, stream=None):
    stream = stream or sys.stdout
    changing = [op for op in plan.ops if op["op"] != "adopt"]
    print("phase %d plan for %s:%s" % (plan.phase, socket.gethostname(), plan.home),
          file=stream)
    if not plan.ops:
        print("  (no operations)", file=stream)
    for index, op in enumerate(plan.ops, 1):
        print("  %2d. %s" % (index, describe(plan.home, op)), file=stream)
    for note in plan.notes:
        print("  note: %s" % note, file=stream)
    for blocker in plan.blockers:
        print("  BLOCKER: %s" % blocker, file=stream)
    print("changes: %d   blockers: %d" % (len(changing), len(plan.blockers)), file=stream)
    print("plan id: %s" % plan.plan_id(), file=stream)


def _plan_from_args(args):
    return build_plan(args.home, args.phase,
                      parse_mapping(getattr(args, "program", None), "--program"),
                      parse_mapping(getattr(args, "archive", None), "--archive"),
                      args.deploy_pattern or (),
                      getattr(args, "state_root", None),
                      getattr(args, "daemonless", False))


def cmd_survey(args):
    print(json.dumps(survey(args.home), indent=2, sort_keys=True))
    return EXIT_OK


def cmd_plan(args):
    plan = _plan_from_args(args)
    if args.json:
        print(json.dumps({"plan_id": plan.plan_id(), "phase": plan.phase, "ops": plan.ops,
                          "blockers": plan.blockers, "notes": plan.notes},
                         indent=2, sort_keys=True))
    else:
        print_plan(plan)
        print("read-only: nothing was changed. To run exactly this plan:\n"
              "  apply --phase %d ... --approve %s" % (plan.phase, plan.plan_id()))
    return EXIT_BLOCKED if plan.blockers else EXIT_OK


def cmd_apply(args):
    with _Lock():
        plan = _plan_from_args(args)
        print_plan(plan)
        if plan.plan_id() != args.approve:
            print("REFUSED: --approve does not match this host's current plan. Review the plan "
                  "above; the host may have changed since the reviewed plan was made.")
            return EXIT_MISMATCH
        if plan.blockers:
            print("REFUSED: blockers present; nothing was changed.")
            return EXIT_BLOCKED
        changing = [op for op in plan.ops if op["op"] != "adopt"]
        if not changing:
            print("nothing to do.")
            return EXIT_OK
        if any(op["op"] == "relocate_dir" for op in plan.ops):
            probe_exchange(os.path.join(args.home, "vq", ".layout"))
        stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        receipt = {"schema": SCHEMA, "issue": ISSUE, "host": socket.gethostname(),
                   "home": args.home, "phase": plan.phase,
                   "plan_id": plan.plan_id(), "started": stamp, "status": "running",
                   "name": "%s-phase%d-%s.json" % (stamp, plan.phase, plan.plan_id()[:12]),
                   "ops": []}
        failed = None
        for op in plan.ops:
            record = dict(op)
            record["status"] = "adopted" if op["op"] == "adopt" else "pending"
            receipt["ops"].append(record)
            if op["op"] == "adopt":
                continue
            try:
                do_op(args.home, record)
                record["status"] = "done"
                print("  ok    %s" % describe(args.home, op))
            except (OSError, Refusal) as exc:
                record["status"] = "failed"
                record["error"] = str(exc)
                failed = exc
                print("  FAIL  %s\n        %s" % (describe(args.home, op), exc))
                break
            finally:
                save_receipt(args.home, receipt)
        receipt["status"] = "failed" if failed else "done"
        receipt["finished"] = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        path = save_receipt(args.home, receipt)
        changed = any(r["status"] == "done" for r in receipt["ops"])
        if path:
            print("receipt: %s" % path)
        elif changed:
            # Operations ran and the record could not be stored on the host.
            # Losing it would make them impossible to undo by receipt.
            print("NO RECEIPT COULD BE WRITTEN ON THIS HOST. Keep this record:")
            print(json.dumps(receipt, indent=2, sort_keys=True))
        else:
            print("nothing was changed, so there is no receipt.")
        if failed:
            if changed:
                print("stopped at the first failure; completed operations stay in place and "
                      "can be reversed with: undo --receipt FILE")
            return EXIT_FAILED
        return EXIT_OK if path else EXIT_FAILED


def cmd_undo(args):
    with open(args.receipt) as fh:
        receipt = json.load(fh)
    if (receipt.get("schema") != SCHEMA or receipt.get("home") != args.home
            or receipt.get("host") != socket.gethostname()):
        raise Refusal("receipt is for another schema, host or home directory")
    done = [op for op in reversed(receipt["ops"]) if op.get("status") == "done"]
    print("undo of phase %s, plan %s" % (receipt.get("phase"), receipt.get("plan_id")))
    for index, op in enumerate(done, 1):
        print("  %2d. reverse: %s" % (index, describe(args.home, op)))
    if args.approve != receipt.get("plan_id"):
        print("read-only: nothing was changed. To run this undo:\n"
              "  undo --receipt %s --approve %s" % (args.receipt, receipt.get("plan_id")))
        return EXIT_OK
    with _Lock():
        blockers, _ = deploy_blockers(args.home, args.deploy_pattern or ())
        if blockers:
            for blocker in blockers:
                print("  BLOCKER: %s" % blocker)
            print("REFUSED: blockers present; nothing was changed.")
            return EXIT_BLOCKED
        # Containers last: a directory this tool made cannot be removed until
        # the names inside it have outlived the grace period.
        containers = ("mkdir", "write_new", "manifest", "rmdir_empty")
        deferred = []
        for batch in ([op for op in done if op["op"] not in containers],
                      [op for op in done if op["op"] in containers]):
            for op in batch:
                try:
                    skipped = undo_op(args.home, op, deferred)
                except (OSError, Refusal) as exc:
                    print("  FAIL  %s\n        %s" % (describe(args.home, op), exc))
                    return EXIT_FAILED
                if skipped:
                    print("  kept  %s\n        %s" % (describe(args.home, op), skipped))
                else:
                    print("  ok    reversed: %s" % describe(args.home, op))
            if deferred:
                time.sleep(args.grace)
                for path in deferred:
                    check_scope(args.home, path)
                    if os.path.islink(path) or os.path.isfile(path):
                        os.unlink(path)
                print("  ok    removed %d transitional name(s) after %.1f s grace"
                      % (len(deferred), args.grace))
                deferred = []
        return EXIT_OK


def build_parser():
    parser = argparse.ArgumentParser(
        prog="fleet_layout_migrate.py",
        description="Host directory layout migration, phases 1-4 (%s). "
                    "`plan` is read-only; `apply` runs only a reviewed plan id." % ISSUE)
    parser.add_argument("--home", default=os.path.expanduser("~"),
                        help="home directory to act on (default: yours)")
    parser.add_argument("--deploy-pattern", action="append", metavar="REGEX",
                        help="extra command-line pattern that means a deploy is in flight")
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("survey", help="describe this host against the target layout (read-only)")
    for name in ("plan", "apply"):
        p = sub.add_parser(name, help="show the operations (read-only)" if name == "plan"
                           else "run a reviewed plan")
        p.add_argument("--phase", type=int, required=True, choices=(1, 2, 3, 4))
        p.add_argument("--daemonless", action="store_true",
                       help="phase 1: a scheduler host with no vq daemon gets no ~/vqstate")
        p.add_argument("--state-root", metavar="DIR",
                       help="phase 4: the live state root, as read from the running daemon")
        p.add_argument("--program", action="append", metavar="NAME-VERSION=ENTRY[,ENTRY...]",
                       help="phase 3: ~/bin entries that make up one external program")
        p.add_argument("--archive", action="append", metavar="NAME-VERSION=ENTRY[,ENTRY...]",
                       help="phase 3: installer archives in ~/bin belonging to a program")
        if name == "plan":
            p.add_argument("--json", action="store_true")
        else:
            p.add_argument("--approve", required=True, metavar="PLAN_ID",
                           help="the plan id printed by a reviewed `plan`")
    u = sub.add_parser("undo", help="reverse the operations a receipt records")
    u.add_argument("--receipt", required=True)
    u.add_argument("--approve", metavar="PLAN_ID",
                   help="the receipt's plan id; without it the undo is only shown")
    u.add_argument("--grace", type=float, default=5.0, metavar="SECONDS",
                   help="how long transitional names outlive the undo, for lookups already "
                        "in flight (default: 5)")
    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.command:
        parser.print_help()
        return EXIT_USAGE
    if os.getuid() == 0:
        print("refusing to run as root: this acts on one unprivileged home", file=sys.stderr)
        return EXIT_USAGE
    args.home = os.path.abspath(args.home)
    if not os.path.isdir(args.home):
        print("no such home directory: %s" % args.home, file=sys.stderr)
        return EXIT_USAGE
    handler = {"survey": cmd_survey, "plan": cmd_plan, "apply": cmd_apply, "undo": cmd_undo}
    try:
        return handler[args.command](args)
    except Refusal as exc:
        print("REFUSED: %s" % exc, file=sys.stderr)
        return EXIT_USAGE


if __name__ == "__main__":
    sys.exit(main())
