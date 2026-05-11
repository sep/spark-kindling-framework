"""Reusable test command helpers for Kindling projects."""

from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import dataclass
from importlib.util import find_spec
from pathlib import Path
from typing import Iterable, List, Optional, Sequence

SUPPORTED_SUITES = ("unit", "component", "integration", "system", "extension", "all")


@dataclass(frozen=True)
class TestRunOptions:
    suite: str
    paths: Sequence[Path]
    platform: Optional[str] = None
    test_filter: Optional[str] = None
    marker: Optional[str] = None
    ci: bool = False
    results_dir: Path = Path("test-results")
    workers: Optional[str] = None
    coverage: Sequence[str] = ()
    no_cov: bool = False
    preflight: str = "none"
    dotenv_paths: Sequence[Path] = (Path(".env"),)
    pytest_args: Sequence[str] = ()


def load_dotenv(path: Path) -> None:
    """Best-effort loader for simple KEY=value dotenv files."""
    if os.environ.get("KINDLING_SKIP_DOTENV", "").strip().lower() in {"1", "true", "yes"}:
        return

    if not path.exists():
        return

    try:
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[len("export ") :].strip()
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            if not key or key in os.environ:
                continue
            if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
                value = value[1:-1]
            os.environ[key] = value
    except (OSError, UnicodeDecodeError):
        return


def default_paths_for_suite(suite: str) -> List[Path]:
    """Return conventional test paths for a suite when the caller omits --path."""
    if suite == "all":
        candidates = [Path("tests")]
    elif suite == "extension":
        candidates = [Path("tests/system/extensions")]
    else:
        candidates = [Path("tests") / suite]

    existing = [path for path in candidates if path.exists()]
    return existing or candidates


def normalize_paths(suite: str, paths: Sequence[Path]) -> List[Path]:
    return list(paths) if paths else default_paths_for_suite(suite)


def resolve_workers(platform: Optional[str], explicit_workers: Optional[str], ci: bool) -> str:
    """Resolve pytest-xdist worker count for local and CI runs."""
    if explicit_workers:
        return explicit_workers.strip()

    if ci:
        default_workers_by_platform = {
            "synapse": "2",
            "fabric": "4",
            "databricks": "4",
        }
        return (
            os.getenv("KINDLING_SYSTEM_TEST_CI_WORKERS")
            or default_workers_by_platform.get(platform or "", "")
        ).strip()

    return (os.getenv("KINDLING_SYSTEM_TEST_WORKERS") or "").strip()


def ensure_xdist_available(workers: str) -> None:
    """Fail fast when multiple xdist workers are requested but unavailable."""
    if not workers or workers in {"0", "1"}:
        return
    if find_spec("xdist") is not None:
        return
    raise RuntimeError(
        "pytest-xdist is required for distributed test execution "
        f"(requested workers={workers}). Install dev dependencies first."
    )


def set_system_coverage_file(platform: Optional[str]) -> None:
    """Avoid coverage sqlite collisions across concurrent system-test runs."""
    if os.environ.get("COVERAGE_FILE"):
        return
    suffix = platform or "all"
    os.environ["COVERAGE_FILE"] = f".coverage.system.{suffix}.{os.getpid()}"


def build_pytest_args(options: TestRunOptions) -> List[str]:
    """Build pytest arguments from explicit CLI options."""
    paths = normalize_paths(options.suite, options.paths)
    args = ["pytest", *[str(path) for path in paths], "-v", "-s"]

    if options.platform:
        args.extend(["--platform", options.platform])
    if options.test_filter:
        args.extend(["-k", options.test_filter])
    if options.marker:
        args.extend(["-m", options.marker])

    if options.ci:
        junit_name, json_name = _report_file_names(options.suite, options.platform)
        args.extend(
            [
                f"--junit-xml={options.results_dir / junit_name}",
                "--json-report",
                f"--json-report-file={options.results_dir / json_name}",
                "--maxfail=1",
            ]
        )

    if options.no_cov:
        args.append("--no-cov")
    else:
        for target in options.coverage:
            args.append(f"--cov={target}")

    workers = resolve_workers(options.platform, options.workers, options.ci)
    if workers and workers not in {"0", "1"}:
        ensure_xdist_available(workers)
        args.extend(["-n", workers])

    args.extend(options.pytest_args)
    return args


def _report_file_names(suite: str, platform: Optional[str]) -> tuple[str, str]:
    """Return CI report filenames compatible with existing Kindling workflows."""
    if suite == "unit":
        return "unit-test-results.xml", "unit-test-report.json"
    if suite == "integration":
        return "integration-test-results.xml", "integration-test-report.json"
    if suite == "system":
        suffix = platform or "all"
        return f"system-test-results-{suffix}.xml", f"system-test-report-{suffix}.json"

    suffix = platform or suite
    return f"{suite}-test-results-{suffix}.xml", f"{suite}-test-report-{suffix}.json"


def run_preflight(mode: str, platform: Optional[str]) -> int:
    """Run optional preflight checks before pytest."""
    if mode == "none":
        return 0

    if mode == "local":
        cmd = ["kindling", "env", "check", "--local"]
        return subprocess.run(cmd).returncode

    if mode == "system":
        auth_check = Path("tests/system/auth_check.py")
        if not auth_check.exists():
            print(f"Skipping system preflight; {auth_check} was not found.")
            return 0
        cmd = [sys.executable, str(auth_check)]
        if platform:
            cmd.extend(["--platform", platform])
        return subprocess.run(cmd).returncode

    raise ValueError(f"Unknown preflight mode: {mode}")


def run_tests(options: TestRunOptions) -> int:
    """Run pytest using Kindling's shared test conventions."""
    for dotenv_path in options.dotenv_paths:
        load_dotenv(dotenv_path)

    if options.ci:
        options.results_dir.mkdir(parents=True, exist_ok=True)

    if options.suite == "system":
        set_system_coverage_file(options.platform)

    preflight_rc = run_preflight(options.preflight, options.platform)
    if preflight_rc != 0:
        return preflight_rc

    args = build_pytest_args(options)
    print(f"Running: {' '.join(args)}", flush=True)
    print(flush=True)
    return subprocess.run(args).returncode


def run_cleanup(
    platform: Optional[str],
    *,
    all_platforms: bool = False,
    skip_packages: bool = False,
) -> int:
    """Delete orphaned system-test jobs and data-apps from configured platforms."""
    storage_account = os.environ.get("AZURE_STORAGE_ACCOUNT", "").strip()
    if not storage_account:
        print("Error: AZURE_STORAGE_ACCOUNT environment variable not set")
        return 1

    container = os.environ.get("AZURE_CONTAINER", "artifacts")
    base_path = os.environ.get("AZURE_BASE_PATH", "")

    platforms: List[str]
    if all_platforms:
        platforms = ["fabric", "synapse", "databricks"]
    elif platform:
        platforms = [platform]
    else:
        print("Must specify either --platform or --all")
        return 1

    print("Kindling Test Resource Cleanup")
    print("=" * 50)
    print(f"Storage: {storage_account}/{container}")
    print()

    total_jobs = 0
    total_apps = 0
    for p in platforms:
        jobs, apps = _cleanup_platform(p, storage_account, container, base_path)
        total_jobs += jobs
        total_apps += apps
        print()

    print("=" * 50)
    print("Cleanup complete!")
    print(f"  Jobs deleted: {total_jobs}")
    print(f"  Data-apps deleted: {total_apps}")
    print(f"  Total: {total_jobs + total_apps}")

    if skip_packages:
        return 0
    return _cleanup_old_packages()


def _cleanup_platform(platform: str, storage_account: str, container: str, base_path: str) -> tuple:
    print(f"Cleaning up {platform} test resources...")
    jobs_deleted = 0
    try:
        from kindling_sdk.platform_provider import create_platform_api_from_env

        client, _ = create_platform_api_from_env(platform)
        list_fn = getattr(client, "list_jobs", None) or getattr(client, "list_spark_jobs")
        jobs = list_fn()
        for job in jobs:
            name = job.get("settings", {}).get("name", "") or job.get("displayName", "")
            if not name.startswith("systest-"):
                continue
            job_id = job.get("job_id") or job.get("id")
            try:
                if client.delete_job(job_id):
                    print(f"  Deleted job: {name}")
                    jobs_deleted += 1
            except Exception as exc:
                print(f"  Failed to delete job {name}: {exc}")
    except ImportError:
        print(f"  Warning: spark-kindling-sdk not installed; skipping {platform} job cleanup")
    except Exception as exc:
        print(f"  Error listing {platform} jobs: {exc}")

    apps_deleted = _cleanup_storage_apps(storage_account, container, base_path)
    return jobs_deleted, apps_deleted


def _cleanup_storage_apps(storage_account: str, container: str, base_path: str) -> int:
    apps_deleted = 0
    try:
        from azure.storage.filedatalake import DataLakeServiceClient
        from kindling_sdk.platform_provider import (
            azure_storage_account_url,
            create_azure_credential,
        )
    except ImportError:
        print("  Warning: azure-storage-file-datalake not installed; skipping storage cleanup")
        return 0

    try:
        account_url = azure_storage_account_url(storage_account)
        fs_client = DataLakeServiceClient(
            account_url, credential=create_azure_credential()
        ).get_file_system_client(container)
        data_apps_path = f"{base_path}/data-apps" if base_path else "data-apps"
        for path in fs_client.get_paths(path=data_apps_path):
            if not path.is_directory:
                continue
            app_name = path.name.split("/")[-1]
            if not (app_name.startswith("universal-test-app-") or "test" in app_name.lower()):
                continue
            try:
                fs_client.get_directory_client(path.name).delete_directory()
                print(f"  Deleted data-app: {app_name}")
                apps_deleted += 1
            except Exception as exc:
                print(f"  Failed to delete data-app {app_name}: {exc}")
    except Exception as exc:
        print(f"  Error cleaning up storage apps: {exc}")

    return apps_deleted


def _cleanup_old_packages() -> int:
    package_cleanup = Path("scripts/cleanup_old_packages.py")
    if not package_cleanup.exists():
        return 0
    cmd = [sys.executable, str(package_cleanup)]
    print(f"Running: {' '.join(cmd)}", flush=True)
    print(flush=True)
    return subprocess.run(cmd).returncode


def flatten_pytest_args(values: Iterable[str]) -> List[str]:
    """Normalize repeated --pytest-arg values into a plain list."""
    return [value for value in values if value]
