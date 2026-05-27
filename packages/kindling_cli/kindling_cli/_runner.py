"""Standalone app runner.

Invoked by `kindling app run` for local/standalone execution:
    python -m kindling_cli._runner --env local --config settings.yaml [--config ...] app.py

Initializes the Kindling framework then execs the app entrypoint in-process,
mirroring how DataAppManager._execute_app() works for remote runs.
"""

import argparse
import builtins
import json
import os
import sys
from pathlib import Path
from typing import Callable, Optional, Set


def _normalize_distribution_name(name: str) -> str:
    return (name or "").strip().replace("-", "_").lower()


def _discover_package_roots_from_pythonpath() -> Set[str]:
    """Best-effort package root discovery from PYTHONPATH entries."""
    raw_pythonpath = (os.getenv("PYTHONPATH") or "").strip()
    if not raw_pythonpath:
        return set()

    discovered = set()
    for entry in raw_pythonpath.split(os.pathsep):
        if not entry:
            continue
        path = Path(entry).expanduser()
        if not path.is_dir():
            continue

        try:
            candidates = list(path.iterdir())
        except OSError:
            continue

        for candidate in candidates:
            if candidate.is_dir() and not candidate.name.startswith((".", "_")):
                discovered.add(_normalize_distribution_name(candidate.name))

    return discovered


def _load_local_package_modules() -> Set[str]:
    raw_modules = (os.getenv("KINDLING_LOCAL_PACKAGE_MODULES") or "").strip()
    discovered = _discover_package_roots_from_pythonpath()
    if not raw_modules:
        return discovered

    try:
        parsed = json.loads(raw_modules)
    except json.JSONDecodeError:
        return discovered

    if not isinstance(parsed, list):
        return discovered

    normalized_modules = set()
    for module_name in parsed:
        if isinstance(module_name, str):
            normalized = _normalize_distribution_name(module_name)
            if normalized:
                normalized_modules.add(normalized)
    return normalized_modules | discovered


def _install_local_package_version_shim() -> Optional[Callable[[], None]]:
    """Return a restore callback when local packages should bypass metadata version lookup."""
    local_modules = _load_local_package_modules()
    if not local_modules:
        return None

    from importlib import metadata as importlib_metadata

    original_version = importlib_metadata.version
    local_version = os.getenv("KINDLING_LOCAL_PACKAGE_VERSION", "0.0.0+local")

    def _version_with_local_fallback(package_name: str) -> str:
        if _normalize_distribution_name(package_name) in local_modules:
            return local_version
        return original_version(package_name)

    importlib_metadata.version = _version_with_local_fallback

    def _restore() -> None:
        importlib_metadata.version = original_version

    return _restore


def main() -> None:
    parser = argparse.ArgumentParser(prog="kindling_cli._runner", add_help=False)
    parser.add_argument("app_path")
    parser.add_argument("--env", default="local")
    parser.add_argument("--config", action="append", dest="config_files", default=[])
    args = parser.parse_args()

    app_path = Path(args.app_path).resolve()
    config_files = [f for f in args.config_files if Path(f).exists()]

    restore_version_shim = _install_local_package_version_shim()

    try:
        from kindling.bootstrap import initialize_framework

        initialize_framework(
            {
                "platform": "standalone",
                "environment": args.env,
                "config_files": config_files,
            }
        )

        code = app_path.read_text(encoding="utf-8")
        exec_globals = {
            "__name__": "__main__",
            "__file__": str(app_path),
            "__spec__": None,
            "__builtins__": builtins,
        }
        # Ensure imports inside the app resolve relative to its directory
        app_dir = str(app_path.parent)
        if app_dir not in sys.path:
            sys.path.insert(0, app_dir)

        exec(compile(code, str(app_path), "exec"), exec_globals)  # noqa: S102
    finally:
        if restore_version_shim is not None:
            restore_version_shim()


if __name__ == "__main__":
    main()
