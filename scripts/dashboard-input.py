#!/usr/bin/env python3
"""Build or admit the one source-matched SPA input; never create fallback assets."""
import hashlib
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SPA = ROOT / "internal/api/dashboardspa"
WEB = SPA / "web"
DIST = SPA / "dist"
MANIFEST = SPA / "dashboard-input.json"
# One replaceable bundle, capped at 16 MiB; CI retains it for one day.
LIMIT = 16 * 1024 * 1024
EXCLUDED = {"node_modules", "dist", "test-results", "playwright-report", "blob-report", ".playwright"}


def digest(files, base):
    result = hashlib.sha256()
    for path in sorted(files):
        if path.is_symlink() or not path.is_file():
            raise ValueError(f"not a regular input file: {path}")
        name = path.relative_to(base).as_posix().encode()
        data = path.read_bytes()
        result.update(len(name).to_bytes(8, "big"))
        result.update(name)
        result.update(len(data).to_bytes(8, "big"))
        result.update(data)
    return result.hexdigest()


def source():
    if WEB.is_symlink():
        raise ValueError("symlinked source directory is not admitted")
    files = []
    for directory, dirs, names in os.walk(WEB):
        dirs[:] = [name for name in dirs if name not in EXCLUDED]
        if any((Path(directory) / name).is_symlink() for name in dirs):
            raise ValueError("symlinked source directory is not admitted")
        files.extend(Path(directory) / name for name in names if not name.endswith(".tsbuildinfo"))
    files += [ROOT / "internal/api/openapi.json", Path(__file__).resolve()]
    return digest(files, ROOT)


def assets():
    if not (DIST / "index.html").is_file():
        raise ValueError("missing real SPA index; run make dashboard-build")
    files = [p for p in DIST.rglob("*") if p.is_file()]
    if any(p.is_symlink() for p in DIST.rglob("*")):
        raise ValueError("symlinks are not admitted in the SPA input")
    size = sum(p.stat().st_size for p in files)
    if size > LIMIT:
        raise ValueError(f"SPA input exceeds {LIMIT} bytes")
    return digest(files, DIST), size


def identity():
    return {"head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(), "source_sha256": source()}


def verify():
    manifest = json.loads(MANIFEST.read_text())
    expected = identity()
    expected["assets_sha256"], expected["bytes"] = assets()
    if manifest != expected:
        raise ValueError("SPA input does not match checkout HEAD, source, or asset digest")
    print(f"Admitted SPA input: {expected['assets_sha256']} ({expected['bytes']} bytes)")


def build():
    before = identity()
    MANIFEST.unlink(missing_ok=True)
    subprocess.run(["npm", "ci", "--silent"], cwd=WEB, check=True)
    subprocess.run(["npm", "run", "build"], cwd=WEB, check=True)
    if identity() != before:
        raise ValueError("dashboard source changed during build")
    output = WEB / "frontend/dist"
    if not (output / "index.html").is_file():
        raise ValueError("Vite did not produce the SPA index")
    files = [p for p in output.rglob("*") if p.is_file()]
    if any(p.is_symlink() for p in output.rglob("*")) or sum(p.stat().st_size for p in files) > LIMIT:
        raise ValueError("Vite output is not an admitted bounded regular-file bundle")
    if DIST.exists():
        shutil.rmtree(DIST)
    shutil.copytree(output, DIST)
    before["assets_sha256"], before["bytes"] = assets()
    MANIFEST.write_text(json.dumps(before, sort_keys=True) + "\n")
    verify()


def main():
    if len(sys.argv) != 2 or sys.argv[1] not in {"build", "prepare", "verify"}:
        raise ValueError("usage: dashboard-input.py build|prepare|verify")
    if sys.argv[1] == "verify":
        verify()
    elif sys.argv[1] == "build":
        build()
    else:
        try:
            verify()
        except (OSError, ValueError):
            build()


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print(f"dashboard input admission failed: {error}", file=sys.stderr)
        sys.exit(1)
