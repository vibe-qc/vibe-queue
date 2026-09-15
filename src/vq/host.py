"""Host identification helpers."""
from __future__ import annotations

import socket

LOCAL_HOST_ALIASES = frozenset({"localhost", "127.0.0.1", "::1"})


def is_local_host(host: str) -> bool:
    """Return True if `host` refers to the local machine.

    Accepts the loopback aliases, the FQDN, and the short hostname.
    """
    if host in LOCAL_HOST_ALIASES:
        return True
    fqdn = socket.gethostname()
    if host == fqdn:
        return True
    short = fqdn.split(".", 1)[0]
    return host == short
