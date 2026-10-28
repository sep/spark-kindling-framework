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


@pytest.mark.requires_spark
def test_runs_enabled_records_one_row_per_initialization(tmp_path) -> None:
    settings = tmp_path / "settings.yaml"
    settings.write_text(
        "kindling:\n"
        "  runs:\n"
        "    enabled: true\n"
        "  delta:\n"
        "    access_mode: storage\n"
        "  storage:\n"
        f"    table_root: {str(tmp_path / 'tables')!r}\n",
        encoding="utf-8",
    )
    runs_path = tmp_path / "tables" / "system" / "runs"
    result = _run_fresh_python(f"""
        from kindling.bootstrap import _record_run, initialize_framework
        from kindling.injection import GlobalInjector
        from kindling.runtime_manifest import get_runtime_manifest
        from kindling.spark_config import ConfigService
        from kindling.spark_session import get_or_create_spark_session

        initialize_framework({{
            "platform": "standalone",
            "environment": "local",
            "config_files": [{str(settings)!r}],
        }})
        spark = get_or_create_spark_session()
        manifest = get_runtime_manifest()

        def rows():
            return spark.read.format("delta").load({str(runs_path)!r}).collect()

        (start,) = rows()
        assert start["run_id"] == manifest.run_id, start
        assert (start["status"], start["ended_at"]) == ("running", None), start
        assert start["platform"] == "standalone" and start["runtime_match"] == "unchecked"

        class _Logger:
            def warning(self, message):
                raise AssertionError(message)

        _record_run(GlobalInjector.get(ConfigService), _Logger(), "succeeded")
        (end,) = rows()
        assert end["run_id"] == manifest.run_id
        assert end["status"] == "succeeded" and end["ended_at"] is not None, end
        assert end["started_at"] == start["started_at"]
        print("RUNS_OK")
        """)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "RUNS_OK" in result.stdout
