"""Standalone app runner.

Invoked by `kindling app run` for local/standalone execution:
    python -m kindling_cli._runner --env local --config settings.yaml [--config ...] app.py

Initializes the Kindling framework then execs the app entrypoint in-process,
mirroring how DataAppManager._execute_app() works for remote runs.
"""

import argparse
import builtins
import json
import logging
import os
import sys
from pathlib import Path
from typing import Callable, List, Optional, Set

_runner_logger = logging.getLogger("kindling._runner")


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


def _dist_name_from_spec(spec: str) -> str:
    """Extract the bare distribution name from a requirement spec (e.g. 'pkg==1.0' → 'pkg')."""
    for sep in ("~=", ">=", "==", "<=", "!=", ">", "<"):
        if sep in spec:
            return spec.split(sep)[0].strip()
    return spec.strip()


def _is_editable_install(dist_name: str) -> bool:
    """Return True if dist_name is installed as an editable (development) install."""
    import importlib.metadata as im

    try:
        dist = im.distribution(dist_name)
        raw = dist.read_text("direct_url.json")
        if raw:
            import json as _json

            info = _json.loads(raw)
            return bool(info.get("dir_info", {}).get("editable", False))
    except Exception:
        pass
    return False


def _is_available_in_env(dist_name: str) -> bool:
    """Return True if the distribution is resolvable in the current environment.

    This covers editable installs, regular pip installs, devcontainer pre-installs,
    and packages published to a registry and installed — anything importlib.metadata
    can find, regardless of how it got there.
    """
    import importlib.metadata as im

    try:
        im.distribution(dist_name)
        return True
    except im.PackageNotFoundError:
        return False


def _filter_editable_packages(lake_requirements: List[str]) -> List[str]:
    """Return only the lake requirements whose distributions are NOT editable installs.

    Editable installs are skipped because:
    - pip install over an editable install removes the editable link, breaking local dev.
    - The package is already imported in the running process; reinstalling it mid-run
      has no effect on the current execution.
    """
    filtered = []
    for spec in lake_requirements:
        dist_name = _dist_name_from_spec(spec)
        if _is_editable_install(dist_name):
            _runner_logger.debug(
                "Skipping lake install of %r — editable install active (local source takes precedence)",
                dist_name,
            )
        else:
            filtered.append(spec)
    return filtered


def _read_lake_requirements(app_dir: Path) -> List[str]:
    """Read lake-reqs.txt from the app directory, returning non-comment lines."""
    lake_reqs_path = app_dir / "lake-reqs.txt"
    if not lake_reqs_path.exists():
        return []
    return [
        line.strip()
        for line in lake_reqs_path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.startswith("#")
    ]


def _install_lake_requirements(
    lake_requirements: List[str], app_name: str, load_lake: bool = False
) -> None:
    """Install lake packages listed in lake-reqs.txt.

    By default (load_lake=False), packages already installed locally are used as-is and
    the lake is not contacted.  Pass load_lake=True to force downloading from the lake
    regardless of local install state (mirrors DataAppManager.run_app() on Synapse/Databricks).
    """
    if not lake_requirements:
        return

    installable = _filter_editable_packages(lake_requirements)
    skipped = len(lake_requirements) - len(installable)
    if skipped:
        _runner_logger.debug(
            "Skipped %d lake package(s) that are active editable installs", skipped
        )
    if not installable:
        return

    if not load_lake:
        available = [s for s in installable if _is_available_in_env(_dist_name_from_spec(s))]
        missing = [s for s in installable if s not in available]
        if available:
            _runner_logger.debug(
                "Using %d locally installed lake package(s): %s",
                len(available),
                available,
            )
        if missing:
            _runner_logger.warning(
                "%d lake package(s) in lake-reqs.txt are not installed locally and will be "
                "skipped — pass --load-lake to fetch them from the lake: %s",
                len(missing),
                missing,
            )
        return

    try:
        from kindling.data_apps import DataAppManager
        from kindling.injection import get_kindling_service

        manager = get_kindling_service(DataAppManager)
        if not manager.artifacts_path:
            _runner_logger.warning(
                "lake-reqs.txt found but artifacts_storage_path is not configured — "
                "lake packages cannot be fetched for local runs without it"
            )
            return
        _runner_logger.info(
            "Installing %d lake package(s) from lake-reqs.txt: %s",
            len(installable),
            installable,
        )
        manager._install_app_dependencies(app_name, [], installable)
    except Exception as exc:
        _runner_logger.warning("Could not install lake packages from lake-reqs.txt: %s", exc)


def main() -> None:
    parser = argparse.ArgumentParser(prog="kindling_cli._runner", add_help=False)
    parser.add_argument("app_path")
    parser.add_argument("--env", default="local")
    parser.add_argument("--config", action="append", dest="config_files", default=[])
    parser.add_argument(
        "--load-lake",
        action="store_true",
        default=False,
        help="Download lake-reqs packages from the lake even if already installed locally.",
    )
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

        lake_requirements = _read_lake_requirements(app_path.parent)
        _install_lake_requirements(lake_requirements, app_path.stem, load_lake=args.load_lake)

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
