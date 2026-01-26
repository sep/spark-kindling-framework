"""
Unit tests for data_apps module

Tests for the refactored DataAppManager class, focusing on:
- Config loading with environment overrides
- Dependency resolution and wheel selection
- Installation orchestration
- App context preparation
"""

from typing import Any, Dict, List
from unittest.mock import MagicMock, Mock, patch

import pytest
from kindling.data_apps import (
    DataAppConfig,
    DataAppConstants,
    DataAppContext,
    DataAppManager,
    WheelCandidate,
)


class TestAppConstants:
    """Test data app framework constants"""

    def test_app_constants_values(self):
        """Test that DataAppConstants has expected values"""
        assert DataAppConstants.REQUIREMENTS_FILE == "requirements.txt"
        assert DataAppConstants.LAKE_REQUIREMENTS_FILE == "lake-reqs.txt"
        assert DataAppConstants.BASE_CONFIG_FILE == "app.yaml"
        assert DataAppConstants.DEFAULT_ENTRY_POINT == "main.py"

    def test_app_constants_priorities(self):
        """Test wheel priority constants"""
        assert DataAppConstants.WHEEL_PRIORITY_PLATFORM_SPECIFIC == 1
        assert DataAppConstants.WHEEL_PRIORITY_GENERIC == 2
        assert DataAppConstants.WHEEL_PRIORITY_FALLBACK == 3

    def test_app_constants_pip_args(self):
        """Test pip common arguments"""
        assert "--disable-pip-version-check" in DataAppConstants.PIP_COMMON_ARGS
        assert "--no-warn-conflicts" in DataAppConstants.PIP_COMMON_ARGS


class TestWheelCandidate:
    """Test WheelCandidate dataclass"""

    def test_creation(self):
        """Test WheelCandidate creation"""
        candidate = WheelCandidate(
            file_path="/path/to/wheel.whl",
            file_name="package-1.0.0-py3-none-any.whl",
            priority=2,
            version="1.0.0",
        )

        assert candidate.file_path == "/path/to/wheel.whl"
        assert candidate.file_name == "package-1.0.0-py3-none-any.whl"
        assert candidate.priority == 2
        assert candidate.version == "1.0.0"

    def test_sort_key(self):
        """Test sort key property returns Version objects for proper semantic versioning"""
        from packaging.version import Version

        candidate1 = WheelCandidate("", "", 1, "1.0.0")
        candidate2 = WheelCandidate("", "", 2, "1.0.0")

        # Verify sort_key returns (priority, Version)
        assert candidate1.sort_key == (1, Version("1.0.0"))
        assert candidate2.sort_key == (2, Version("1.0.0"))
        # Lower priority wins (1 < 2)
        assert candidate1.sort_key < candidate2.sort_key

        # Test pre-release versions are handled correctly
        candidate_stable = WheelCandidate("", "", 2, "1.0.0")
        candidate_alpha = WheelCandidate("", "", 2, "1.0.0-alpha")
        # Alpha < stable (pre-release comes before stable)
        assert candidate_alpha.sort_key < candidate_stable.sort_key

        # Test malformed version handling
        candidate_bad = WheelCandidate("", "", 2, None)
        assert candidate_bad.sort_key == (2, Version("0.0.0"))


class TestAppManagerHelpers:
    """Test helper methods in DataAppManager"""

    @pytest.fixture
    def mock_app_manager(self):
        """Create mock DataAppManager for testing"""
        manager = Mock(spec=DataAppManager)
        manager.artifacts_path = "/artifacts"
        manager.logger = Mock()

        # Mock config service
        mock_config = Mock()
        mock_config.get.return_value = "data-apps"  # Default apps directory
        manager.config = mock_config

        # Mock the methods we need to access from the real class
        # This allows us to test the real method logic on a mock instance
        manager._get_app_dir = DataAppManager._get_app_dir.__get__(manager)
        manager._get_packages_dir = DataAppManager._get_packages_dir.__get__(manager)
        manager._extract_package_name = DataAppManager._extract_package_name.__get__(manager)
        manager._parse_package_spec = DataAppManager._parse_package_spec.__get__(manager)

        return manager

    def test_get_app_dir(self, mock_app_manager):
        """Test _get_app_dir helper"""
        result = mock_app_manager._get_app_dir("my_app")
        assert result == "/artifacts/data-apps/my_app/"

    def test_extract_package_name_simple(self, mock_app_manager):
        """Test package name extraction"""
        result = mock_app_manager._extract_package_name("pandas")
        assert result == "pandas"

    def test_extract_package_name_with_version(self, mock_app_manager):
        """Test package name extraction with version"""
        result = mock_app_manager._extract_package_name("pandas==1.5.0")
        assert result == "pandas"

    def test_parse_package_spec_with_version(self, mock_app_manager):
        """Test parsing package spec with version"""
        name, version = mock_app_manager._parse_package_spec("pandas==1.5.0")
        assert name == "pandas"
        assert version == "1.5.0"

    def test_parse_package_spec_without_version(self, mock_app_manager):
        """Test parsing package spec without version"""
        name, version = mock_app_manager._parse_package_spec("pandas")
        assert name == "pandas"
        assert version is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
