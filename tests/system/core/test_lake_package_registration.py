"""
System test: a deployed app's lake-reqs.txt package registers its declarations.

A domain package scaffolded by ``kindling package init`` keeps its entities
and pipes in ``<pkg>/entities`` and ``<pkg>/pipes`` with an empty
``<pkg>/__init__.py``. Locally, the runner imports those subpackages; remotely
``DataAppManager`` used to import only ``<pkg>`` itself, so the package's
entities and pipes never registered and a batch app ran nothing.

Setup (handled by fixtures):
  - Builds a wheel in that scaffolded layout: empty ``__init__.py``,
    ``entities/orders.py`` declaring ``lakereg.orders`` and
    ``pipes/orders_copy.py`` declaring the pipe ``lakereg.orders_copy``
  - Uploads it to {base_path}/packages/ and deploys an app whose
    lake-reqs.txt lists it

Verification:
  - app.py exits non-zero unless both declarations are in the registries,
    and logs a completion marker when they are.

Run:
    poe test-system --test test_lake_package_registration
"""

import base64
import hashlib
import io
import os
import time
import uuid
import zipfile

import pytest

from tests.system.core.test_lake_wheel_bfs import (  # noqa: F401 (fixture)
    _test_artifacts_storage_path,
    blob_client,
)
from tests.system.test_helpers import (
    apply_env_config_overrides,
    assert_no_fatal_system_test_log_lines,
    get_system_test_poll_interval,
    get_system_test_stream_max_wait,
    wait_for_job_terminal_teardown,
)

DIST_NAME = "test-lake-reg-domain"
MODULE = "test_lake_reg_domain"
VERSION = "1.0.0"
WHEEL_NAME = f"{MODULE}-{VERSION}-py3-none-any.whl"
ENTITY_ID = "lakereg.orders"
PIPE_ID = "lakereg.orders_copy"
MARKER_DONE = "LAKE_REG_TEST: entities and pipes registered"

_ENTITIES_SRC = f"""\
from kindling.data_entities import DataEntities
from pyspark.sql.types import StringType, StructField, StructType

DataEntities.entity(
    entityid="{ENTITY_ID}",
    name="lakereg_orders",
    partition_columns=[],
    merge_columns=["id"],
    tags={{"provider_type": "memory"}},
    schema=StructType([StructField("id", StringType(), False)]),
)
"""

_PIPES_SRC = f"""\
from kindling.data_pipes import DataPipes


@DataPipes.pipe(
    pipeid="{PIPE_ID}",
    name="LakeregOrdersCopy",
    tags={{}},
    input_entity_ids=["{ENTITY_ID}"],
    output_entity_id="{ENTITY_ID}",
    output_type="table",
)
def lakereg_orders_copy(lakereg_orders):
    return lakereg_orders
"""

_APP_SRC = f"""\
import logging
import sys

from kindling.data_entities import DataEntityRegistry
from kindling.data_pipes import DataPipesRegistry
from kindling.injection import GlobalInjector

_log = logging.getLogger("lake_reg_test_app")
entities = set(GlobalInjector.get(DataEntityRegistry).get_entity_ids())
pipes = set(GlobalInjector.get(DataPipesRegistry).get_pipe_ids())
if "{ENTITY_ID}" not in entities or "{PIPE_ID}" not in pipes:
    _log.error(
        "LAKE_REG_TEST: declarations missing -- entities=%s pipes=%s",
        sorted(entities),
        sorted(pipes),
    )
    sys.exit(1)
_log.warning("{MARKER_DONE}")
sys.exit(0)
"""


def _wheel_bytes() -> bytes:
    """A wheel laid out like a scaffolded domain package."""
    files = {
        f"{MODULE}/__init__.py": "",
        f"{MODULE}/entities/__init__.py": "",
        f"{MODULE}/entities/orders.py": _ENTITIES_SRC,
        f"{MODULE}/pipes/__init__.py": "",
        f"{MODULE}/pipes/orders_copy.py": _PIPES_SRC,
    }
    dist_info = f"{MODULE}-{VERSION}.dist-info"
    files[f"{dist_info}/METADATA"] = (
        f"Metadata-Version: 2.1\nName: {DIST_NAME}\nVersion: {VERSION}\n"
    )
    files[f"{dist_info}/WHEEL"] = (
        "Wheel-Version: 1.0\nGenerator: kindling-test\nRoot-Is-Purelib: true\nTag: py3-none-any\n"
    )

    def _sha256(data: bytes) -> str:
        digest = hashlib.sha256(data).digest()
        return "sha256=" + base64.urlsafe_b64encode(digest).rstrip(b"=").decode()

    record_path = f"{dist_info}/RECORD"
    record = [f"{path},{_sha256(src.encode())},{len(src.encode())}" for path, src in files.items()]
    record.append(f"{record_path},,")

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for path, src in files.items():
            zf.writestr(path, src)
        zf.writestr(record_path, "\n".join(record))
    return buf.getvalue()


@pytest.fixture(scope="module")
def lake_registration_wheel(blob_client):
    """Upload the package wheel to packages/ (left in place under a per-run
    base path, as test_lake_wheel_bfs explains; removed for ad-hoc runs)."""
    container = os.getenv("AZURE_CONTAINER", "artifacts")
    base_path = os.getenv("AZURE_BASE_PATH", "").rstrip("/")
    packages_path = f"{base_path}/packages" if base_path else "packages"
    blob_path = f"{packages_path}/{WHEEL_NAME}"

    container_client = blob_client.get_container_client(container)
    container_client.upload_blob(blob_path, _wheel_bytes(), overwrite=True)
    deadline = time.time() + 120.0
    while True:
        try:
            container_client.get_blob_client(blob_path).get_blob_properties()
            break
        except Exception:
            if time.time() >= deadline:
                raise RuntimeError(f"Blob not readable after 120s: {blob_path}")
            time.sleep(5)

    yield WHEEL_NAME
    if not base_path:
        try:
            container_client.delete_blob(blob_path)
        except Exception as exc:
            print(f"Warning: could not clean up {blob_path}: {exc}")


@pytest.fixture
def registration_app(platform_client, lake_registration_wheel):
    api_client, platform_name = platform_client
    suffix = str(uuid.uuid4())[:8]
    app_name = f"systest-lake-reg-{suffix}"
    job_name = f"systest-lake-reg-job-{suffix}"
    artifacts_path = _test_artifacts_storage_path()
    job_config = apply_env_config_overrides(
        {
            "job_name": job_name,
            "app_name": app_name,
            "entry_point": "app.py",
            "test_id": suffix,
            "artifacts_storage_path": artifacts_path,
            "config_overrides": {"kindling": {"artifacts_storage_path": artifacts_path}},
        },
        platform_name,
    )
    api_client.deploy_app(app_name, {"app.py": _APP_SRC, "lake-reqs.txt": f"{DIST_NAME}\n"})

    yield api_client, app_name, job_name, job_config

    try:
        api_client.cleanup_app(app_name)
    except Exception as exc:
        print(f"Warning: app cleanup failed: {exc}")


@pytest.mark.system
class TestLakePackageRegistration:
    def test_scaffolded_package_declarations_register_remotely(
        self, platform_client, registration_app, stdout_validator
    ):
        _, platform_name = platform_client
        api_client, _app_name, job_name, job_config = registration_app

        job_id = api_client.create_job(job_name=job_name, job_config=job_config)["job_id"]
        run_id = None
        try:
            run_id = api_client.run_job(job_id=job_id)
            assert run_id is not None

            stdout_validator.stream_with_callback(
                job_id=job_id,
                run_id=run_id,
                print_lines=True,
                poll_interval=get_system_test_poll_interval(10.0),
                max_wait=get_system_test_stream_max_wait(900.0, platform_name),
            )
            log = stdout_validator.get_content()
            assert_no_fatal_system_test_log_lines(log)

            status_info = api_client.get_job_status(run_id=run_id)
            job_result = (status_info.get("result_state") or "").upper()
            job_status = (status_info.get("status") or "").upper()
            infra_error = job_status in ("INTERNAL_ERROR", "SKIPPED")
            job_succeeded = job_result in ("SUCCESS", "SUCCEEDED") or (
                not job_result and job_status in ("COMPLETED", "SUCCESS", "SUCCEEDED")
            )

            # app.py exits 1 (-> "App execution failed") when the package's
            # entities or pipes are not registered.
            assert (
                "LAKE_REG_TEST: declarations missing" not in log
            ), "The lake package's entities/pipes were not registered remotely"
            assert "App execution failed" not in log, "Bootstrap reported app failure"
            # Registration must be positively verified: the app's marker, or a
            # successful job (app.py exits 1 when declarations are missing). An
            # infrastructure error proves nothing, so it fails as inconclusive
            # rather than passing.
            if MARKER_DONE not in log and not job_succeeded:
                if infra_error:
                    pytest.fail(
                        f"Inconclusive: job ended {job_status} before registration was "
                        "verified (infrastructure error); rerun the test"
                    )
                pytest.fail(
                    f"No completion marker and job not successful: status={job_status} "
                    f"result={job_result} log_length={len(log)}"
                )
        finally:
            try:
                api_client.cancel_job(run_id=run_id)
            except Exception:
                pass
            if run_id is not None:
                wait_for_job_terminal_teardown(api_client, run_id, platform_name)
            api_client.delete_job(job_id=job_id)

            from tests.system.test_helpers import cleanup_test_storage

            cleanup_test_storage(platform_name, job_config["test_id"])
