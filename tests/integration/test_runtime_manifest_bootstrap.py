"""Runtime manifest through a real standalone initialize_framework.

Fresh Python process (as test_di_wiring_standalone.py), so the manifest,
the injector and the signal wiring are the ones bootstrap builds.
"""

import subprocess
import sys
import textwrap

import pytest


def _run_fresh_python(code: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        capture_output=True,
        text=True,
        timeout=120,
    )


@pytest.mark.requires_spark
def test_initialize_builds_the_manifest_and_lists_executor_runs() -> None:
    result = _run_fresh_python("""
        from kindling.bootstrap import initialize_framework
        from kindling.data_pipes import DataPipesExecution
        from kindling.injection import GlobalInjector
        from kindling.runtime_manifest import get_runtime_manifest

        initialize_framework({"platform": "standalone", "environment": "local"})
        manifest = get_runtime_manifest()
        assert manifest is not None and manifest.run_id
        assert manifest.platform == "standalone", manifest.platform
        assert manifest.pyspark_version and manifest.spark_version, manifest
        assert manifest.kindling_version, manifest
        assert manifest.python_version.count(".") == 2
        assert len(manifest.runtime_fingerprint) == 64
        assert len(manifest.package_fingerprint) == 64
        assert "pyspark" in manifest.key_packages

        GlobalInjector.get(DataPipesExecution).run_datapipes([])
        assert len(manifest.execution_ids) == 1, manifest.execution_ids
        print("MANIFEST_OK", manifest.summary())
        """)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "MANIFEST_OK Runtime manifest: run_id=" in result.stdout
