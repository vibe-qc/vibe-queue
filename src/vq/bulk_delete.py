"""Remote tree removal that is cheap on Lustre metadata servers.

``rm -r`` stats every entry before unlinking it. On a Lustre filesystem that
is one metadata-server round trip per file, which makes deleting a large job
workspace slow and loads the MDS for every other user of the filesystem. The
Lustre-recommended alternative is to find files and symlinks without
following links, unlink them with ``munlink`` (which skips the stat), and then
remove the emptied directories with ``find -delete``.

:data:`RMTREE_SHELL` defines one POSIX shell function, ``__vq_rmtree``, used by
every remote bulk deletion. It takes the ``munlink`` path only when the tree is
a real directory on a filesystem that ``stat -f`` reports as ``lustre`` and
``munlink`` is installed, so it needs no per-host configuration. It always
finishes with ``rm -rf --``, which removes whatever the fast path left (for
example special files, or entries ``munlink`` could not remove) and supplies
the exit status. Callers keep their own path validation: this function
deletes exactly the paths it is given.
"""
from __future__ import annotations

RMTREE_SHELL = r"""__vq_rmtree() {
    for __vq_rmtree_path in "$@"; do
        if [ -d "$__vq_rmtree_path" ] && [ ! -L "$__vq_rmtree_path" ] \
            && command -v munlink >/dev/null 2>&1 \
            && [ "$(stat -f -c %T -- "$__vq_rmtree_path" 2>/dev/null)" = lustre ]
        then
            find -P "$__vq_rmtree_path" \( -type f -o -type l \) -print0 \
                | xargs -0 -r munlink || :
            find -P "$__vq_rmtree_path" -depth -type d -empty -delete || :
        fi
        rm -rf -- "$__vq_rmtree_path" || return $?
    done
}
"""
"""Shell source defining ``__vq_rmtree PATH...``; see the module docstring."""


def rmtree_argv(*paths: str) -> list[str]:
    """``sh -c`` argv that removes exactly ``paths`` with :data:`RMTREE_SHELL`."""
    return ["sh", "-c", RMTREE_SHELL + '__vq_rmtree "$@"\n', "vq-rmtree", *paths]
