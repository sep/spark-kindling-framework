#!/usr/bin/env python3
"""
Deploy the wheels a Lakeflow/DLT pipeline notebook %pip installs to a Unity
Catalog volume.

Two system tests spin up a real serverless Lakeflow pipeline whose generated
notebook does a ``%pip install`` of exact-pinned wheel versions from a UC
Volume path (see ``PACKAGES_VOLUME`` in both test files):

  tests/system/extensions/databricks/test_lakeflow_scd_platform.py
  tests/system/extensions/databricks/test_temporal_lakeflow.py

Serverless Lakeflow pipelines cannot install from the abfss:// staging path
the rest of CI uses (scripts/deploy.py) — the notebook's %pip install only
resolves PyPI, Workspace Files, or Unity Catalog Volume paths — so these
specific wheels need a separate upload through the Databricks Files API
(kindling_sdk.artifact_store.VolumesArtifactStore).

This script:
  1. Builds the two Lakeflow test-app wheels under tests/data-apps/. They
     are test fixtures, not shipped packages, so scripts/build.py's
     DESIGN_TIME_PACKAGE_DIRS does not build them.
  2. Uploads the exact wheels the two tests reference — the framework
     wheel plus the sdp/databricks/temporal extension wheels already built
     to dist/ by scripts/build_platform_wheels.sh, plus the two freshly
     built test-app wheels — to packages/ under the UC volume root.

Requires DATABRICKS_HOST plus Databricks credentials (DATABRICKS_TOKEN, or
the AZURE_TENANT_ID/AZURE_CLIENT_ID/AZURE_CLIENT_SECRET service principal,
or an Azure CLI login — the same chain kindling_sdk uses elsewhere) and
KINDLING_DATABRICKS_RUNTIME_VOLUME_CATALOG / _SCHEMA / _TEMP_VOLUME
(defaults: kindling / kindling / artifacts, matching
scripts/databricks_uc_preflight.py's defaults).

Usage:
    python scripts/deploy_lakeflow_uc_wheels.py
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import List

REPO_ROOT = Path(__file__).resolve().parent.parent
DIST_DIR = REPO_ROOT / "dist"

# Repo-relative package dirs whose already-built dist/ wheel this script
# deploys. Matches the wheels tests/system/extensions/databricks/
# test_lakeflow_scd_platform.py and test_temporal_lakeflow.py bake into
# their generated pipeline notebook's %pip install line.
FRAMEWORK_WHEEL_GLOB = "spark_kindling-*.whl"
EXTENSION_WHEEL_GLOBS = [
    "spark_kindling_ext_sdp-*.whl",
    "spark_kindling_ext_databricks-*.whl",
    "spark_kindling_ext_temporal-*.whl",
]

# Test-fixture data apps, built here since they aren't part of
# scripts/build.py's DESIGN_TIME_PACKAGE_DIRS.
TEST_APP_DIRS = [
    REPO_ROOT / "tests" / "data-apps" / "lakeflow-scd-test-app",
    REPO_ROOT / "tests" / "data-apps" / "lakeflow-temporal-test-app",
]

# Make kindling_sdk importable even when it isn't installed on the active
# interpreter (mirrors the PYTHONPATH the poe tasks set explicitly).
sys.path.insert(0, str(REPO_ROOT / "packages" / "kindling_sdk"))

from kindling_sdk.artifact_store import artifact_store_for  # noqa: E402


def _build_test_app_wheel(app_dir: Path) -> Path:
    dist = app_dir / "dist"
    if dist.exists():
        for stale in dist.glob("*.whl"):
            stale.unlink()
    result = subprocess.run(
        ["poetry", "build", "--format", "wheel"],
        cwd=app_dir,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Failed building {app_dir.name} wheel:\n{result.stderr}")
    wheels = sorted(dist.glob("*.whl"))
    if not wheels:
        raise FileNotFoundError(f"poetry build produced no wheel for {app_dir.name}")
    return wheels[-1]


def _resolve_dist_wheel(glob: str) -> Path:
    matches = sorted(DIST_DIR.glob(glob))
    if not matches:
        raise FileNotFoundError(
            f"No wheel matching `{glob}` in {DIST_DIR}. Run "
            "scripts/build_platform_wheels.sh first."
        )
    return matches[-1]


def _volume_root() -> str:
    catalog = (os.getenv("KINDLING_DATABRICKS_RUNTIME_VOLUME_CATALOG") or "kindling").strip()
    schema = (os.getenv("KINDLING_DATABRICKS_RUNTIME_VOLUME_SCHEMA") or "kindling").strip()
    volume = (os.getenv("KINDLING_DATABRICKS_RUNTIME_TEMP_VOLUME") or "artifacts").strip()
    return f"/Volumes/{catalog}/{schema}/{volume}"


def main() -> int:
    if not DIST_DIR.exists():
        raise SystemExit(
            f"{DIST_DIR} not found. Run scripts/build_platform_wheels.sh before this script."
        )

    wheels: List[Path] = [_resolve_dist_wheel(FRAMEWORK_WHEEL_GLOB)]
    wheels.extend(_resolve_dist_wheel(glob) for glob in EXTENSION_WHEEL_GLOBS)
    wheels.extend(_build_test_app_wheel(app_dir) for app_dir in TEST_APP_DIRS)

    root = _volume_root()
    print(f"Deploying {len(wheels)} wheel(s) to {root}/packages/")
    store = artifact_store_for(root)
    for wheel in wheels:
        store.upload_file(f"packages/{wheel.name}", wheel.read_bytes(), overwrite=True)
        print(f"  ✓ {wheel.name}")

    print(f"\n✅ Deployed {len(wheels)} wheel(s) to {store.describe()}/packages/")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
