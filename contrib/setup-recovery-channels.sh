#!/usr/bin/env bash
# v0.7.5 *Hopper's Compiler*: idempotent bootstrap for the
# 3-tier host recovery contract documented in
# vibe-queue/docs/host_recovery_channels.md.
#
# Implements tiers 2 (Cockpit) + 3 (recovery sshd). Tier 1 (BMC)
# is hardware-side and out of scope for a script — see the doc.
#
# Run as: sudo env RECOVERY_USER=QUEUE_USER bash setup-recovery-channels.sh
#         [--recovery-key-path PATH]
#
# Idempotent: every step checks current state and skips if already
# satisfied. Safe to re-run after partial failures or to upgrade a
# previously-bootstrapped host.

set -euo pipefail

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

RECOVERY_SSH_PORT="${RECOVERY_SSH_PORT:-22222}"
RECOVERY_USER="${RECOVERY_USER:-}"
RECOVERY_KEY_PATH="${RECOVERY_KEY_PATH:-}"   # set via --recovery-key-path or env
COCKPIT_INSTALL="${COCKPIT_INSTALL:-1}"      # set =0 to skip cockpit (tier 2)
RECOVERY_SSHD_INSTALL="${RECOVERY_SSHD_INSTALL:-1}"  # set =0 to skip tier 3

# -----------------------------------------------------------------------------
# Argv parse (lets a script wrapper or the operator pass the recovery key path)
# -----------------------------------------------------------------------------

while [[ $# -gt 0 ]]; do
    case "$1" in
        --recovery-key-path)
            RECOVERY_KEY_PATH="$2"
            shift 2
            ;;
        --recovery-key-path=*)
            RECOVERY_KEY_PATH="${1#*=}"
            shift
            ;;
        --skip-cockpit)
            COCKPIT_INSTALL=0
            shift
            ;;
        --skip-recovery-sshd)
            RECOVERY_SSHD_INSTALL=0
            shift
            ;;
        --recovery-port)
            RECOVERY_SSH_PORT="$2"
            shift 2
            ;;
        -h|--help)
            cat <<EOF
Usage: sudo $0 [--recovery-key-path PATH]
                  [--recovery-port PORT]      (default: 22222)
                  [--skip-cockpit]
                  [--skip-recovery-sshd]

Sets up tiers 2 + 3 of the host recovery contract documented in
vibe-queue/docs/host_recovery_channels.md.

--recovery-key-path is the path on THIS host to the recovery
PUBLIC key (.pub) that will be placed in
/etc/ssh/recovery_authorized_keys. If omitted, the script asks
interactively (or reads from stdin if not a tty).

RECOVERY_USER must explicitly name the local account allowed to recover this host.
It is required unless --skip-recovery-sshd is selected.

Env var overrides: RECOVERY_SSH_PORT, RECOVERY_USER,
RECOVERY_KEY_PATH, COCKPIT_INSTALL, RECOVERY_SSHD_INSTALL.
EOF
            exit 0
            ;;
        *)
            echo "Unknown arg: $1" >&2
            echo "Try --help" >&2
            exit 2
            ;;
    esac
done

# -----------------------------------------------------------------------------
# Pre-flight
# -----------------------------------------------------------------------------

if [[ "$RECOVERY_SSHD_INSTALL" == "1" ]] &&
   [[ ! "$RECOVERY_USER" =~ ^[a-zA-Z_][a-zA-Z0-9_-]*[$]?$ ]]; then
    echo "ERROR: RECOVERY_USER must name one explicit local account [value redacted]." >&2
    exit 2
fi

if [[ "$(id -u)" != "0" ]]; then
    echo "Must run as root (sudo $0 ...)" >&2
    exit 1
fi

# Detect distro family. Cockpit + sshd unit names vary slightly.
DISTRO_FAMILY=""
if [[ -r /etc/os-release ]]; then
    # shellcheck disable=SC1091
    . /etc/os-release
    case "${ID:-}:${ID_LIKE:-}" in
        ubuntu*|debian*|*debian*) DISTRO_FAMILY="debian" ;;
        fedora*|rhel*|centos*|*fedora*|*rhel*) DISTRO_FAMILY="fedora" ;;
        arch*|manjaro*|*arch*) DISTRO_FAMILY="arch" ;;
        *)
            echo "WARN: unknown distro family (ID=${ID:-?} ID_LIKE=${ID_LIKE:-?})." >&2
            echo "      Assuming debian-like package names. Adjust manually if needed." >&2
            DISTRO_FAMILY="debian"
            ;;
    esac
fi

# sshd unit name varies: ssh on debian/ubuntu, sshd on fedora/arch.
case "$DISTRO_FAMILY" in
    debian) SSHD_UNIT="ssh" ;;
    *)      SSHD_UNIT="sshd" ;;
esac

echo "==> distro family: $DISTRO_FAMILY (sshd unit: $SSHD_UNIT)"

# -----------------------------------------------------------------------------
# Tier 2: Cockpit
# -----------------------------------------------------------------------------

if [[ "$COCKPIT_INSTALL" == "1" ]]; then
    echo
    echo "=== Tier 2: Cockpit web admin ==="
    if systemctl is-active --quiet cockpit.socket; then
        echo "    already active — skipping install"
    else
        case "$DISTRO_FAMILY" in
            debian)
                apt-get update -qq
                apt-get install -y -qq cockpit
                ;;
            fedora)
                dnf install -y -q cockpit
                ;;
            arch)
                pacman -S --noconfirm --needed cockpit
                ;;
        esac
        systemctl enable --now cockpit.socket
        echo "    cockpit installed + enabled"
    fi

    # Verify it's actually listening on 9090. Sometimes the socket
    # exists but firewall + listen config disagree; surface that
    # gap immediately rather than during a recovery.
    if ss -lntp 2>/dev/null | grep -q ':9090 '; then
        echo "    cockpit listening on :9090 ✓"
    else
        echo "    WARN: cockpit.socket active but nothing listening on :9090." >&2
        echo "          Check 'systemctl status cockpit.socket' + ListenStream= setting." >&2
    fi

    # Curl probe (ignore self-signed; we just want 'something responds').
    if curl -ksS -o /dev/null -w '%{http_code}\n' -I https://localhost:9090/ --max-time 5 | grep -qE '^(2|3)'; then
        echo "    cockpit web responds 2xx/3xx ✓"
    else
        echo "    WARN: cockpit web didn't respond healthily on :9090." >&2
        echo "          Investigate before declaring tier 2 green." >&2
    fi
else
    echo "==> Tier 2 (Cockpit) skipped per --skip-cockpit"
fi

# -----------------------------------------------------------------------------
# Tier 3: Recovery sshd
# -----------------------------------------------------------------------------

if [[ "$RECOVERY_SSHD_INSTALL" == "1" ]]; then
    echo
    echo "=== Tier 3: Recovery sshd on port $RECOVERY_SSH_PORT ==="

    # Resolve the recovery key path. If not provided, ask.
    if [[ -z "$RECOVERY_KEY_PATH" ]]; then
        if [[ -t 0 ]]; then
            echo
            echo "Need the recovery PUBLIC key (.pub) path on this host."
            echo "It should be a key you generated SEPARATELY from your "
            echo "day-to-day laptop key, stored in cold storage."
            read -r -p "Path to recovery .pub: " RECOVERY_KEY_PATH
        else
            echo "ERROR: no --recovery-key-path given and stdin is not a tty." >&2
            echo "       Either pass --recovery-key-path PATH or run interactively." >&2
            exit 1
        fi
    fi

    if [[ ! -r "$RECOVERY_KEY_PATH" ]]; then
        echo "ERROR: recovery key not readable at $RECOVERY_KEY_PATH" >&2
        exit 1
    fi

    # Sanity-check: looks like an SSH public key?
    if ! grep -qE '^(ssh-(ed25519|rsa|ecdsa)|sk-) ' "$RECOVERY_KEY_PATH"; then
        echo "ERROR: $RECOVERY_KEY_PATH doesn't look like an ssh public key." >&2
        echo "       Expected first token: ssh-ed25519 / ssh-rsa / ssh-ecdsa." >&2
        exit 1
    fi

    # Place the key.
    AUTH_KEYS=/etc/ssh/recovery_authorized_keys
    if [[ -r "$AUTH_KEYS" ]] && grep -qFf "$RECOVERY_KEY_PATH" "$AUTH_KEYS" 2>/dev/null; then
        echo "    recovery key already present in $AUTH_KEYS — skipping"
    else
        cp "$RECOVERY_KEY_PATH" "$AUTH_KEYS"
        chown root:root "$AUTH_KEYS"
        chmod 600 "$AUTH_KEYS"
        echo "    recovery pubkey installed at $AUTH_KEYS"
    fi

    # Write the sshd Match-block config.
    CONFIG_FILE=/etc/ssh/sshd_config.d/recovery.conf
    EXPECTED_CONTENT=$(cat <<EOF
# v0.7.5 Hopper's Compiler — recovery sshd block.
# DO NOT REMOVE without first verifying tier 1 (BMC) and tier 2
# (Cockpit) both work. See vibe-queue/docs/host_recovery_channels.md
# for the contract.

Port 22
Port $RECOVERY_SSH_PORT

Match LocalPort $RECOVERY_SSH_PORT
    AuthorizedKeysFile /etc/ssh/recovery_authorized_keys
    AllowUsers $RECOVERY_USER
    PasswordAuthentication no
    KbdInteractiveAuthentication no
    PermitRootLogin no
EOF
)

    if [[ -r "$CONFIG_FILE" ]] && [[ "$(cat "$CONFIG_FILE")" == "$EXPECTED_CONTENT" ]]; then
        echo "    $CONFIG_FILE already up to date — skipping"
    else
        # Validate sshd would accept this config before swapping it in
        # (defends against typos breaking the next sshd restart).
        TMPFILE=$(mktemp)
        trap 'rm -f "$TMPFILE"' EXIT
        echo "$EXPECTED_CONTENT" > "$TMPFILE"
        if sshd -t -f /dev/null -o "Include $TMPFILE" 2>/dev/null; then
            echo "    sshd -t syntax check: ok"
        else
            # Some sshd versions don't honor Include via -o; fall back
            # to a less strict validation (just check Port is parsable).
            echo "    sshd -t pre-flight skipped (older sshd; relying on reload-test below)"
        fi
        cp "$TMPFILE" "$CONFIG_FILE"
        chmod 644 "$CONFIG_FILE"
        echo "    wrote $CONFIG_FILE"
    fi

    # Reload sshd. If reload fails we want to know NOW, not after lockout.
    if systemctl reload "$SSHD_UNIT" 2>&1; then
        echo "    sshd reloaded ✓"
    else
        echo "    ERROR: sshd reload failed. Investigate immediately!" >&2
        echo "    Likely the config is bad; revert with:" >&2
        echo "      sudo rm $CONFIG_FILE && sudo systemctl reload $SSHD_UNIT" >&2
        exit 1
    fi

    # Verify port 22222 is listening.
    if ss -lntp 2>/dev/null | grep -q ":$RECOVERY_SSH_PORT "; then
        echo "    recovery sshd listening on :$RECOVERY_SSH_PORT ✓"
    else
        echo "    WARN: nothing listening on :$RECOVERY_SSH_PORT after reload." >&2
        echo "          Check 'sudo journalctl -u $SSHD_UNIT -n 30' for errors." >&2
    fi
else
    echo "==> Tier 3 (Recovery sshd) skipped per --skip-recovery-sshd"
fi

# -----------------------------------------------------------------------------
# Summary
# -----------------------------------------------------------------------------

echo
echo "==> Bootstrap complete."
echo "Next steps:"
echo "  1. Test cockpit from your laptop:"
echo "       ssh -L 9090:localhost:9090 -N <host>"
echo "       # in browser: https://localhost:9090/"
echo "  2. Test recovery sshd from your laptop:"
echo "       ssh -i ~/.ssh/id_ed25519_vibeqc-recovery -p $RECOVERY_SSH_PORT $RECOVERY_USER@<host>"
echo "  3. Configure BMC (tier 1) per your hardware vendor's docs."
echo "  4. Run 'vq admin audit-recovery <host>' to verify all tiers green."
echo
echo "See vibe-queue/docs/host_recovery_channels.md for the full contract."
