"""
System test: BFS transitive dependency walking for lake wheel installation.

Verifies that _download_lake_wheels follows Requires-Dist entries in wheel
METADATA so callers only need to list top-level packages in lake-reqs.txt —
transitive lake deps are discovered and downloaded automatically.

Setup (handled by fixtures):
  - Builds two minimal wheels in memory:
      test_lake_dep_b-1.0.0  — prints a marker on import, no lake deps
      test_lake_dep_a-1.0.0  — imports test_lake_dep_b, prints its own marker
  - Uploads both to {base_path}/packages/ in Azure Blob Storage
  - Deploys a test app whose lake-reqs.txt lists only test-lake-dep-a

Verification:
  - Job stdout must contain both markers — proving pkgB was discovered via
    pkgA's METADATA and loaded even though it was not listed in lake-reqs.txt.

Run:
    poe test-system --test test_lake_wheel_bfs
"""

import io
import os
import uuid
import zipfile
from pathlib import Path

import pytest

from tests.system.test_helpers import (
    assert_no_fatal_system_test_log_lines,
    create_platform_client,
    get_system_test_poll_interval,
    get_system_test_stream_max_wait,
)


def _test_artifacts_storage_path() -> str:
    """Construct the abfss:// path that matches where the test wheels are uploaded.

    Uses the same storage account/container/base_path as the lake_test_wheels fixture
    so the cloud app's artifacts_storage_path points at exactly the right location.
    """
    storage_account = os.getenv("AZURE_STORAGE_ACCOUNT", "")
    container = os.getenv("AZURE_CONTAINER", "artifacts")
    base_path = os.getenv("AZURE_BASE_PATH", "").rstrip("/")
    cloud = (os.getenv("AZURE_CLOUD") or "").strip().lower().replace("-", "").replace("_", "")
    if cloud in {"azureusgovernment", "azuregovernment", "government", "gov", "usgov"}:
        suffix = "core.usgovcloudapi.net"
    elif cloud in {"azurechinacloud", "china"}:
        suffix = "core.chinacloudapi.cn"
    else:
        suffix = "core.windows.net"
    path = f"abfss://{container}@{storage_account}.dfs.{suffix}"
    if base_path:
        path = f"{path}/{base_path}"
    return path


# ── markers ────────────────────────────────────────────────────────────────────
MARKER_B = "LAKE_BFS_MARKER: test_lake_dep_b imported"
MARKER_A = "LAKE_BFS_MARKER: test_lake_dep_a imported"
MARKER_DONE = "LAKE_BFS_TEST: completed"


# ── wheel builders ─────────────────────────────────────────────────────────────


def _build_wheel(
    dist_name: str,
    version: str,
    module_name: str,
    init_src: str,
    requires_dist: list[str] | None = None,
) -> bytes:
    """Build a minimal but valid wheel zip in memory."""
    import base64
    import hashlib

    buf = io.BytesIO()
    dist_info = f"{dist_name.replace('-', '_')}-{version}.dist-info"

    meta_lines = [
        "Metadata-Version: 2.1",
        f"Name: {dist_name}",
        f"Version: {version}",
    ]
    for req in requires_dist or []:
        meta_lines.append(f"Requires-Dist: {req}")
    metadata = "\n".join(meta_lines)

    wheel_meta = "\n".join(
        [
            "Wheel-Version: 1.0",
            "Generator: kindling-test",
            "Root-Is-Purelib: true",
            "Tag: py3-none-any",
        ]
    )

    def _sha256(data: bytes) -> str:
        digest = hashlib.sha256(data).digest()
        return "sha256=" + base64.urlsafe_b64encode(digest).rstrip(b"=").decode()

    init_bytes = init_src.encode()
    meta_bytes = metadata.encode()
    wheel_bytes = wheel_meta.encode()

    init_path = f"{module_name}/__init__.py"
    meta_path = f"{dist_info}/METADATA"
    wheel_path = f"{dist_info}/WHEEL"
    record_path = f"{dist_info}/RECORD"

    record = "\n".join(
        [
            f"{init_path},{_sha256(init_bytes)},{len(init_bytes)}",
            f"{meta_path},{_sha256(meta_bytes)},{len(meta_bytes)}",
            f"{wheel_path},{_sha256(wheel_bytes)},{len(wheel_bytes)}",
            f"{record_path},,",
        ]
    )

    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(init_path, init_src)
        zf.writestr(meta_path, metadata)
        zf.writestr(wheel_path, wheel_meta)
        zf.writestr(record_path, record)

    return buf.getvalue()


def _wheel_b_bytes() -> bytes:
    return _build_wheel(
        dist_name="test-lake-dep-b",
        version="1.0.0",
        module_name="test_lake_dep_b",
        init_src=f'import logging\nlogging.getLogger("test_lake_dep_b").warning("{MARKER_B}")\n',
    )


def _wheel_a_bytes() -> bytes:
    return _build_wheel(
        dist_name="test-lake-dep-a",
        version="1.0.0",
        module_name="test_lake_dep_a",
        init_src=f'import logging, test_lake_dep_b\nlogging.getLogger("test_lake_dep_a").warning("{MARKER_A}")\n',
        requires_dist=["test-lake-dep-b (>=1.0.0)"],
    )


# ── app source ─────────────────────────────────────────────────────────────────


def _app_py_src() -> str:
    return f"""\
import logging
import sys

_log = logging.getLogger("lake_bfs_test_app")

try:
    import test_lake_dep_a  # noqa: F401 — triggers import of test_lake_dep_b
except ImportError as e:
    _log.error(f"LAKE_BFS_TEST: import failed: {{e}}")
    sys.exit(1)

if "test_lake_dep_a" not in sys.modules:
    _log.error("LAKE_BFS_TEST: test_lake_dep_a NOT in sys.modules")
    sys.exit(1)

if "test_lake_dep_b" not in sys.modules:
    _log.error("LAKE_BFS_TEST: test_lake_dep_b NOT in sys.modules — BFS did not follow dep")
    sys.exit(1)

_log.warning("{MARKER_DONE}")
sys.exit(0)
"""


# ── fixtures ───────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def blob_client():
    """BlobServiceClient authenticated via environment variables."""
    from azure.storage.blob import BlobServiceClient
    from kindling_cli.cli import _resolve_account_url, create_azure_credential

    storage_account = os.getenv("AZURE_STORAGE_ACCOUNT")
    if not storage_account:
        pytest.skip("AZURE_STORAGE_ACCOUNT not set")

    credential = create_azure_credential(additionally_allowed_tenants=["*"])
    return BlobServiceClient(
        account_url=_resolve_account_url(storage_account),
        credential=credential,
    )


@pytest.fixture(scope="module")
def lake_test_wheels(blob_client):
    """Upload test wheels to packages/ and clean up after the module."""
    container = os.getenv("AZURE_CONTAINER", "artifacts")
    base_path = os.getenv("AZURE_BASE_PATH", "").rstrip("/")
    packages_path = f"{base_path}/packages" if base_path else "packages"

    wheel_a = "test_lake_dep_a-1.0.0-py3-none-any.whl"
    wheel_b = "test_lake_dep_b-1.0.0-py3-none-any.whl"

    container_client = blob_client.get_container_client(container)

    import time

    for name, data in [(wheel_a, _wheel_a_bytes()), (wheel_b, _wheel_b_bytes())]:
        blob_path = f"{packages_path}/{name}"
        container_client.upload_blob(blob_path, data, overwrite=True)
        print(f"Uploaded test wheel: {blob_path}")

    # Brief pause so uploads propagate through blob→OneLake consistency layer
    # before the cloud job's mssparkutils.fs.ls() call lists the packages dir.
    time.sleep(5)

    yield wheel_a, wheel_b

    for name in (wheel_a, wheel_b):
        try:
            container_client.delete_blob(f"{packages_path}/{name}")
            print(f"Cleaned up test wheel: {packages_path}/{name}")
        except Exception as exc:
            print(f"Warning: could not clean up {name}: {exc}")


@pytest.fixture
def bfs_test_app(platform_client, lake_test_wheels):
    """Deploy the BFS test app and yield (api_client, app_name, job_name, job_config)."""
    api_client, _ = platform_client
    suffix = str(uuid.uuid4())[:8]
    app_name = f"systest-lake-bfs-{suffix}"
    job_name = f"systest-lake-bfs-job-{suffix}"

    artifacts_path = _test_artifacts_storage_path()
    job_config = {
        "job_name": job_name,
        "app_name": app_name,
        "entry_point": "app.py",
        "test_id": suffix,
        # Tell the running executor where to find lake packages.  This must
        # match the path where lake_test_wheels uploads the test wheels.
        "config_overrides": {"kindling": {"artifacts_storage_path": artifacts_path}},
    }

    app_files = {
        "app.py": _app_py_src(),
        "lake-reqs.txt": "test-lake-dep-a\n",  # only top-level — pkgB inferred by BFS
    }

    api_client.deploy_app(app_name, app_files)

    yield api_client, app_name, job_name, job_config

    try:
        api_client.cleanup_app(app_name)
    except Exception as exc:
        print(f"Warning: app cleanup failed: {exc}")


# ── test ───────────────────────────────────────────────────────────────────────


@pytest.mark.system
class TestLakeWheelBFS:
    """Verify BFS transitive dep discovery for lake wheel installation."""

    def test_transitive_dep_loaded_from_lake(self, platform_client, bfs_test_app, stdout_validator):
        """
        lake-reqs.txt lists only test-lake-dep-a.
        BFS should discover test-lake-dep-b via pkgA's Requires-Dist and
        download it so Python's import machinery loads it transitively.
        """
        _, platform_name = platform_client
        api_client, app_name, job_name, job_config = bfs_test_app

        result = api_client.create_job(job_name=job_name, job_config=job_config)
        job_id = result["job_id"]
        print(f"Job created: {job_id}")

        try:
            run_id = api_client.run_job(job_id=job_id)
            assert run_id is not None

            stdout_validator.stream_with_callback(
                job_id=job_id,
                run_id=run_id,
                print_lines=True,
                poll_interval=get_system_test_poll_interval(10.0),
                # 900s: Databricks UC cold-start + wheel download can take >10 min
                max_wait=get_system_test_stream_max_wait(900.0),
            )

            log = stdout_validator.get_content()
            assert_no_fatal_system_test_log_lines(log)

            # Synapse Livy only surfaces KindlingBootstrap logger output from the
            # worker thread — raw print/stderr/app-logger output is not captured.
            # We verify correctness via the bootstrap's own success/failure log lines:
            #   - "App execution failed" appears if app.py calls sys.exit(non-zero)
            #   - "completed successfully" appears only on sys.exit(0)
            # app.py explicitly calls sys.exit(1) when test_lake_dep_b is missing from
            # sys.modules, so "App execution failed" would surface if BFS didn't work.
            assert "App execution failed" not in log, (
                "Bootstrap reported app failure — BFS likely did not load test_lake_dep_b "
                "(lake-reqs.txt only lists test_lake_dep_a; transitive dep must be fetched via BFS)"
            )
            # On platforms where the job log captures stdout (Fabric, Databricks) the
            # bootstrap script prints "BOOTSTRAP COMPLETE".  On platforms where only
            # JVM/log4j stderr is captured (Synapse Livy), bootstrap.py emits
            # logger.warning("App '...' completed successfully") instead.
            # Accept either form so the assertion works on all platforms.
            assert "BOOTSTRAP COMPLETE" in log or "completed successfully" in log, (
                "Bootstrap did not complete — possible causes: job startup timeout "
                f"(log length={len(log)} chars), BFS install failure, or app crash. "
                "Re-run with KINDLING_SYSTEM_TEST_STREAM_MAX_WAIT=900 if timeout suspected."
            )

        finally:
            try:
                api_client.cancel_job(run_id=run_id)
            except Exception:
                pass
            api_client.delete_job(job_id=job_id)

            from tests.system.test_helpers import cleanup_test_storage

            cleanup_test_storage(platform_name, job_config["test_id"])
