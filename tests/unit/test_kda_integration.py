"""
Unit tests for KDA framework integration in system test runners

Tests that system test runners correctly use the DataAppManager to package
test applications as KDAs with platform-specific configurations.
"""

import tempfile
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest


class TestKDAIntegration:
    """Test KDA framework integration with system test runners"""

    def test_data_app_manager_import(self):
        """Verify DataAppManager can be imported"""
        from kindling.data_apps import DataAppManager

        assert DataAppManager is not None

    def test_kda_package_structure(self):
        """Test that KDA packages have correct structure"""
        from kindling.data_apps import DataAppPackage

        # Create test app directory
        with tempfile.TemporaryDirectory() as temp_dir:
            app_dir = Path(temp_dir) / "test-app"
            app_dir.mkdir()

            # Create app.yaml
            (app_dir / "app.yaml").write_text(
                """name: test-app
version: "1.0.0"
entry_point: main.py
"""
            )

            # Create main.py
            (app_dir / "main.py").write_text(
                """print("Hello from test app")
"""
            )

            # Package as KDA using the new utility
            kda_path = DataAppPackage.create(
                app_directory=str(app_dir),
                target_platform="synapse",
                merge_platform_config=True,
            )

            # Verify KDA exists and is a zip file
            assert Path(kda_path).exists()
            assert Path(kda_path).suffix == ".kda"

            # Verify KDA contents
            with zipfile.ZipFile(kda_path, "r") as zf:
                names = zf.namelist()

                # Should contain manifest
                assert "kda-manifest.json" in names, "KDA should contain kda-manifest.json"

                # Should contain app files
                assert "main.py" in names, "KDA should contain main.py"
                assert "app.yaml" in names, "KDA should contain app.yaml"

    def test_kda_platform_config_merge(self):
        """Test that platform-specific configs are merged correctly"""
        import json

        import yaml
        from kindling.data_apps import DataAppPackage

        with tempfile.TemporaryDirectory() as temp_dir:
            app_dir = Path(temp_dir) / "test-app"
            app_dir.mkdir()

            # Create base config
            (app_dir / "app.yaml").write_text(
                """name: test-app
version: "1.0.0"
entry_point: main.py
spark_config:
  spark.sql.shuffle.partitions: "10"
"""
            )

            # Create Synapse-specific config
            (app_dir / "app.synapse.yaml").write_text(
                """spark_config:
  spark.synapse.linkedService.useDefaultCredential: "true"
environment_vars:
  PLATFORM: synapse
"""
            )

            (app_dir / "main.py").write_text("print('test')")

            # Package for Synapse using the new utility
            kda_path = DataAppPackage.create(
                app_directory=str(app_dir),
                target_platform="synapse",
                merge_platform_config=True,
            )

            # Extract and verify merged config
            with zipfile.ZipFile(kda_path, "r") as zf:
                # Read app.yaml from KDA
                app_yaml_content = zf.read("app.yaml").decode()
                app_config = yaml.safe_load(app_yaml_content)

                # Should have base config
                assert "spark_config" in app_config
                assert app_config["spark_config"]["spark.sql.shuffle.partitions"] == "10"

                # Should have merged Synapse config
                assert (
                    app_config["spark_config"]["spark.synapse.linkedService.useDefaultCredential"]
                    == "true"
                )

                # Should have environment vars from Synapse config
                if "environment_vars" in app_config:
                    assert app_config["environment_vars"]["PLATFORM"] == "synapse"


class TestKDAAppStructure:
    """Test proper KDA app structure for system tests"""

    def test_universal_test_app_structure(self):
        """Verify universal-test-app has proper structure"""
        app_dir = Path(__file__).parent.parent / "data-apps" / "universal-test-app"

        if app_dir.exists():
            # Verify required files
            assert (app_dir / "main.py").exists(), "Missing main.py"

            # Universal test app uses pure Python, no app.yaml required
            # It's designed to work across all platforms via framework abstraction


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
