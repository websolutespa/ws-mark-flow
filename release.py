#!/usr/bin/env python3
"""
Prepare a release: make sure the version in setup.py is not on PyPI yet
(bumping the patch number if it is), rebuild dist/ and run ``twine check``.

Uploading is deliberately a separate step:  python -m twine upload --verbose dist/*
"""

import argparse
import json
import re
import shutil
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).parent.resolve()
SETUP_PY = ROOT / "setup.py"
PACKAGE = "ws-mark-flow"

_VERSION_RE = re.compile(r"""(\bversion\s*=\s*["'])([^"']+)(["'])""")


def read_version(setup_text: str) -> str:
    match = _VERSION_RE.search(setup_text)
    if not match:
        raise ValueError("version=... not found in setup.py")
    return match.group(2)


def bump_patch(version: str) -> str:
    """``0.0.17`` -> ``0.0.18``. Pre/post-release suffixes are not supported."""
    parts = version.split(".")
    if not all(part.isdigit() for part in parts):
        raise ValueError(f"cannot bump non-numeric version {version!r}")
    parts[-1] = str(int(parts[-1]) + 1)
    return ".".join(parts)


def published_versions(package: str = PACKAGE, timeout: float = 10) -> set[str]:
    """Versions released on PyPI; empty if the project was never published."""
    url = f"https://pypi.org/pypi/{package}/json"
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return set(json.loads(response.read().decode())["releases"])
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return set()
        raise


def next_unpublished(version: str, published: set[str]) -> str:
    while version in published:
        version = bump_patch(version)
    return version


def update_setup_version(setup_path: Path, published: set[str], dry_run: bool = False) -> tuple[str, str]:
    """Returns ``(current, target)``; rewrites setup.py when they differ."""
    text = setup_path.read_text(encoding="utf-8")
    current = read_version(text)
    target = next_unpublished(current, published)
    if target != current and not dry_run:
        setup_path.write_text(
            _VERSION_RE.sub(lambda m: f"{m.group(1)}{target}{m.group(3)}", text, count=1),
            encoding="utf-8",
        )
    return current, target


def clean(root: Path = ROOT) -> None:
    for path in [root / "dist", root / "build", *root.glob("*.egg-info"), *root.glob("src/*.egg-info")]:
        shutil.rmtree(path, ignore_errors=True)


def _run(*args: str) -> None:
    print(f"\n$ python {' '.join(args)}", flush=True)
    subprocess.run([sys.executable, *args], cwd=ROOT, check=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true", help="only report the version that would be built")
    args = parser.parse_args(argv)

    published = published_versions()
    current, target = update_setup_version(SETUP_PY, published, dry_run=args.dry_run)
    if target == current:
        print(f"version {current} is not on PyPI yet: keeping it")
    else:
        print(f"version {current} is already on PyPI: {'would bump' if args.dry_run else 'bumped'} to {target}")
    if args.dry_run:
        return 0

    clean()
    _run("-m", "build")
    _run("-m", "twine", "check", "--strict", *(str(p) for p in sorted((ROOT / "dist").iterdir())))
    print(f"\n{PACKAGE} {target} is ready: python -m twine upload --verbose dist/*")
    return 0


if __name__ == "__main__":
    sys.exit(main())
