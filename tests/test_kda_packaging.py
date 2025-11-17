#!/usr/bin/env python3
"""
Test script for KDA packaging functionality
"""

import json
import os
import sys
import tempfile
import zipfile
from pathlib import Path

from kindling.data_apps import DataAppConstants, DataAppManager

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


def test_kda_packaging():
    """Test packaging and deployment of KDA files"""

    print("🧪 Testing KDA packaging functionality...")

    # Mock components for standalone testing
    class MockLogger:
        def info(self, msg):
            print(f"INFO: {msg}")

        def debug(self, msg):
            print(f"DEBUG: {msg}")

        def error(self, msg):
            print(f"ERROR: {msg}")

        def warning(self, msg):
            print(f"WARNING: {msg}")

    class MockLoggerProvider:
        def get_logger(self, name):
            return MockLogger()

    class MockTraceProvider:
        def span(self, **kwargs):
            class MockSpan:
                def __enter__(self):
                    return self

                def __exit__(self, *args):
                    pass

            return MockSpan()

    class MockConfigService:
        def get(self, key, default=None):
            if key == "artifacts_storage_path":
                return "/tmp/kindling-test-artifacts"
            return default

    class MockNotebookManager:
        pass

    class MockPlatformService:
        def list(self, path):
            return []

        def read(self, path):
            return ""

        def write(self, path, content):
            pass

        def delete(self, path):
            pass

    class MockPlatformServiceProvider:
        def get_service(self):
            return MockPlatformService()

    # Create data app manager with mocks
    app_manager = DataAppManager.__new__(DataAppManager)
    app_manager.config = MockConfigService()
    app_manager.psp = MockPlatformServiceProvider()
    app_manager.es = None
    app_manager.tp = MockTraceProvider()
    app_manager.framework = MockNotebookManager()
    app_manager.logger = MockLogger()
    app_manager.artifacts_path = "/tmp/kindling-test-artifacts"

    # Test 1: Package the kda-test-app app
    test_app_dir = Path(__file__).parent / "data-apps" / "kda-test-app"

    if not test_app_dir.exists():
        print(f"❌ Test app directory not found: {test_app_dir}")
        return False

    print(f"📦 Packaging test app from: {test_app_dir}")

    with tempfile.TemporaryDirectory() as temp_dir:
        # Package the app
        output_path = Path(temp_dir) / "kda-test-app-v1.0.kda"

        try:
            # Test 1a: Package with platform merging (default)
            result_path = app_manager.package_app(
                app_directory=str(test_app_dir),
                output_path=str(output_path),
                version="1.0.0",
                target_platform="synapse",
                merge_platform_config=True,
            )

            print(f"✅ Successfully packaged KDA: {result_path}")

            # Test 2: Validate KDA contents
            print("🔍 Validating KDA contents...")

            with zipfile.ZipFile(result_path, "r") as kda_file:
                file_list = kda_file.namelist()
                print(f"📁 KDA contains {len(file_list)} files:")
                for filename in sorted(file_list):
                    print(f"   - {filename}")

                # Check for required files
                required_files = ["app.yaml", "main.py", DataAppConstants.KDA_MANIFEST_FILE]

                missing_files = [f for f in required_files if f not in file_list]
                if missing_files:
                    print(f"❌ Missing required files: {missing_files}")
                    return False

                # Validate manifest
                manifest_content = kda_file.read(DataAppConstants.KDA_MANIFEST_FILE)
                manifest_data = json.loads(manifest_content)

                print(f"📋 Manifest data:")
                print(f"   - Name: {manifest_data['name']}")
                print(f"   - Version: {manifest_data['version']}")
                print(f"   - Entry Point: {manifest_data['entry_point']}")
                print(f"   - Dependencies: {len(manifest_data['dependencies'])}")

                if manifest_data["name"] != "kda-test-app":
                    print(f"❌ Unexpected app name: {manifest_data['name']}")
                    return False

                if manifest_data["version"] != "1.0.0":
                    print(f"❌ Unexpected version: {manifest_data['version']}")
                    return False

            print("✅ KDA validation passed!")

            # Test 3: Multi-platform KDA (includes all platform configs)
            print("\n🔄 Testing multi-platform KDA...")
            multi_platform_path = Path(temp_dir) / "kda-test-app-multi-v1.0.kda"

            multi_result_path = app_manager.package_app(
                app_directory=str(test_app_dir),
                output_path=str(multi_platform_path),
                version="1.0.0",
                target_platform=None,
                merge_platform_config=False,
            )

            with zipfile.ZipFile(multi_result_path, "r") as multi_kda:
                multi_files = multi_kda.namelist()
                print(f"📁 Multi-platform KDA contains {len(multi_files)} files:")
                for filename in sorted(multi_files):
                    print(f"   - {filename}")

                # Should include platform-specific configs
                platform_configs = [
                    f
                    for f in multi_files
                    if f.startswith("app.") and f.endswith(".yaml") and f != "app.yaml"
                ]
                if platform_configs:
                    print(f"✅ Multi-platform KDA includes platform configs: {platform_configs}")
                else:
                    print("⚠️ No platform-specific configs found in multi-platform KDA")

            print("✅ All tests passed! 🎉")
            return True

        except Exception as e:
            print(f"❌ Test failed with error: {str(e)}")
            import traceback

            traceback.print_exc()
            return False


if __name__ == "__main__":
    success = test_kda_packaging()
    sys.exit(0 if success else 1)
