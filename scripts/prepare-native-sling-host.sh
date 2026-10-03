#!/usr/bin/env bash
# Source this file, then call prepare_native_sling_host. Add
# cleanup_native_sling_host to the caller's existing EXIT cleanup; no traps are
# installed here. Preparation supplies source, never an admission decision.
# Bound: one private temporary root <=128 KiB, two pinned source files and two
# empty package markers, removed by caller cleanup. -B prevents policy caches.

cleanup_native_sling_host() {
    if [[ -n "${_GC_NATIVE_SLING_SQL_PID:-}" ]]; then
        kill "$_GC_NATIVE_SLING_SQL_PID" 2>/dev/null || true
        wait "$_GC_NATIVE_SLING_SQL_PID" 2>/dev/null || true
        unset _GC_NATIVE_SLING_SQL_PID
    fi
    if [[ -n "${_GC_NATIVE_SLING_SQL_ROOT:-}" ]]; then
        rm -rf -- "$_GC_NATIVE_SLING_SQL_ROOT" || return
        unset _GC_NATIVE_SLING_SQL_ROOT GC_SLING_TEST_SCOPE
    fi
    if [[ -n "${_GC_NATIVE_SLING_HOST_ROOT:-}" ]]; then
        rm -rf -- "$_GC_NATIVE_SLING_HOST_ROOT" || return
        unset _GC_NATIVE_SLING_HOST_ROOT
    fi
}

prepare_native_sling_host() {
    # An operator-supplied authority takes precedence, including its environment.
    if [[ -n "${GC_SLING_ADMISSION_COMMAND:-}" ]]; then
        export GC_SLING_ADMISSION_COMMAND
        return 0
    fi

    _GC_NATIVE_SLING_HOST_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/gc-native-sling-host.XXXXXX")" || return
    if ! python3 - "$_GC_NATIVE_SLING_HOST_ROOT" <<'PY'
import hashlib
import sys
import subprocess
import threading
from pathlib import Path

# SHA256 and byte counts are from these exact pinned Git blobs,
# not a manifest generated from the download being checked.
commit = "adfc2064ce8d110a33533ba31fbcfff1818c3240"
base = "repos/tudorsaitoc/saitoc/contents/"
sources = (
    (
        "scripts/nerve/dispatch_admission.py",
        62295,
        "1034ef8a11c209c28799ca8ab01441e04be558ff679ffd1d36e038b55b5e697b",
    ),
    (
        "scripts/nerve/andon.py",
        11368,
        "12be97843eff36dcbe7c0195ebadd07b74659f5c6e771638b429c2157d4d9bab",
    ),
)
root = Path(sys.argv[1]).resolve()
try:
    for relative, size, expected in sources:
        # The private source uses the caller's existing gh authentication.
        # No token is created, stored, copied or added to the command line.
        endpoint = f"{base}{relative}?ref={commit}"
        process = subprocess.Popen(
            ["gh", "api", "-H", "Accept: application/vnd.github.raw+json", endpoint],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        deadline = threading.Timer(30, process.kill)
        deadline.start()
        try:
            content = process.stdout.read(size + 1)
            if len(content) > size:
                process.kill()
            error = process.stderr.read(4097)
            code = process.wait()
        finally:
            deadline.cancel()
        if code:
            raise RuntimeError(
                "existing gh authentication must read the pinned Saitoc source, "
                "or configure a genuine GC_SLING_ADMISSION_COMMAND: "
                + error[:4096].decode(errors="replace")
            )
        if len(content) != size or hashlib.sha256(content).hexdigest() != expected:
            raise ValueError(f"pinned source mismatch: {endpoint}")
        destination = root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(content)
    # Empty package markers bind both imports to these verified policy files.
    (root / "scripts/__init__.py").write_bytes(b"")
    (root / "scripts/nerve/__init__.py").write_bytes(b"")
except Exception as exc:
    print(f"native sling host preparation failed: {exc}", file=sys.stderr)
    sys.exit(1)
PY
    then
        cleanup_native_sling_host
        return 1
    fi

    export PYTHONPATH="$_GC_NATIVE_SLING_HOST_ROOT"
    GC_SLING_ADMISSION_COMMAND="$(python3 - "$_GC_NATIVE_SLING_HOST_ROOT" <<'PY'
import shlex
import sys
from pathlib import Path

print(shlex.join([
    sys.executable, "-B",
    str(Path(sys.argv[1]) / "scripts/nerve/dispatch_admission.py"),
    "native-admission", "--json",
]))
PY
    )" || return
    export GC_SLING_ADMISSION_COMMAND
}

# The runner owns one real loopback SQL server for this package invocation;
# individual Go cases create private schemas and close both native handles.
prepare_native_sling_test_scope() {
    if [[ -n "${GC_SLING_TEST_SCOPE:-}" ]]; then
        return 0
    fi
    _GC_NATIVE_SLING_SQL_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/gc-native-sling-sql.XXXXXX")" || return
    mkdir -m 700 "$_GC_NATIVE_SLING_SQL_ROOT/dolt" "$_GC_NATIVE_SLING_SQL_ROOT/home" || return
    local port
    port="$(python3 - "$_GC_NATIVE_SLING_SQL_ROOT" <<'PY'
import json
import socket
import sys
from pathlib import Path

root = Path(sys.argv[1])
with socket.socket() as listener:
    listener.bind(("127.0.0.1", 0))
    port = listener.getsockname()[1]
env = {
    "BEADS_DOLT_SERVER_HOST": "127.0.0.1",
    "BEADS_DOLT_SERVER_PORT": str(port),
    "BEADS_DOLT_PORT": str(port),
    "BEADS_DOLT_SERVER_SOCKET": "",
    "BEADS_DOLT_PASSWORD": "",
    "DOLT_ROOT_PATH": str(root / "dolt"),
}
(root / "client-env.json").write_text(json.dumps(env))
print(port)
PY
    )" || return
    HOME="$_GC_NATIVE_SLING_SQL_ROOT/home" DOLT_ROOT_PATH="$_GC_NATIVE_SLING_SQL_ROOT/dolt" \
        dolt sql-server --host 127.0.0.1 --port "$port" --data-dir "$_GC_NATIVE_SLING_SQL_ROOT/dolt" \
        >"$_GC_NATIVE_SLING_SQL_ROOT/server.log" 2>&1 &
    _GC_NATIVE_SLING_SQL_PID=$!
    if ! python3 - "$port" "$_GC_NATIVE_SLING_SQL_PID" <<'PY'
import os
import socket
import sys
import time

port, pid = map(int, sys.argv[1:])
deadline = time.monotonic() + 30
while time.monotonic() < deadline:
    os.kill(pid, 0)
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=1) as connection:
            # A MySQL protocol greeting proves more than an open listener.
            packet = connection.recv(256)
            if len(packet) >= 5 and packet[4] == 10:
                sys.exit(0)
    except OSError:
        pass
    time.sleep(0.1)
raise RuntimeError("owned Dolt SQL server did not become ready")
PY
    then
        cleanup_native_sling_host
        return 1
    fi
    export GC_SLING_TEST_SCOPE="$_GC_NATIVE_SLING_SQL_ROOT"
}
