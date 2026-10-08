#!/usr/bin/env python3
"""Fail if an upload would leave PyPI and the GitHub release disagree.

The publish job uploads with ``skip-existing``: extensions keep their version
across Kindling releases, and an unchanged one is already on the index. Builds
are reproducible, so an unchanged package rebuilds to byte-identical files.
This check compares each file about to be uploaded against the same file
already on the index (by SHA-256) and fails when they differ -- a package
whose code changed without a version bump, which ``skip-existing`` would
otherwise skip silently, leaving the index on the old code.

Usage: check_pypi_artifacts.py <upload-dir> [<index-base-url>]
       (default index: https://pypi.org)
"""

import hashlib
import json
import re
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Dict, List, Optional

_FILENAME_RE = re.compile(r"^(?P<name>[A-Za-z0-9_]+)-(?P<version>[^-]+?)(?:-|\.tar\.gz$)")


def _index_digests(base_url: str, name: str, version: str) -> Optional[Dict[str, str]]:
    """{filename: sha256} already on the index for name==version, or None."""
    url = f"{base_url.rstrip('/')}/pypi/{name}/{version}/json"
    try:
        with urllib.request.urlopen(url, timeout=30) as response:
            data = json.load(response)
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return None
        raise
    return {f["filename"]: f["digests"]["sha256"] for f in data.get("urls", [])}


def find_mismatches(upload_dir: Path, base_url: str) -> List[str]:
    problems: List[str] = []
    for path in sorted(upload_dir.iterdir()):
        match = _FILENAME_RE.match(path.name)
        if not match:
            continue
        name = match.group("name").replace("_", "-")
        existing = _index_digests(base_url, name, match.group("version"))
        if existing is None or path.name not in existing:
            continue
        local = hashlib.sha256(path.read_bytes()).hexdigest()
        if local != existing[path.name]:
            problems.append(
                f"{path.name}: already on the index with different contents. "
                f"{name} {match.group('version')} changed without a version bump; "
                "bump its version in its pyproject.toml and release again."
            )
    return problems


def main() -> None:
    upload_dir = Path(sys.argv[1])
    base_url = sys.argv[2] if len(sys.argv) > 2 else "https://pypi.org"
    problems = find_mismatches(upload_dir, base_url)
    for problem in problems:
        print(f"❌ {problem}")
    if problems:
        sys.exit(1)
    print(f"✅ Nothing in {upload_dir} conflicts with {base_url}.")


if __name__ == "__main__":
    main()
