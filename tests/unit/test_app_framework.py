"""
Unit tests for app_framework module

Tests for the refactored AppManager class, focusing on:
- Config loading with environment overrides
- Dependency resolution and wheel selection
- Installation orchestration
- App context preparation
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from typing import List, Dict, Any

from kindling.app_framework import (
    AppManager,
    AppConfig,
    AppContext,
    AppConstants,
    WheelCandidate
)


class TestAppConstants:
    """Test app framework constants"""

    def test_constants_defined(self):
        """Verify all required constants are defined"""
        assert AppConstants.REQUIREMENTS_FILE == "requirements.txt"
        assert AppConstants.LAKE_REQUIREMENTS_FILE == "lake-reqs.txt"
        assert AppConstants.BASE_CONFIG_FILE == "app.yaml"
        assert AppConstants.DEFAULT_ENTRY_POINT == "main.py"

    def test_wheel_priorities(self):
        """Verify wheel priority ordering"""
        assert AppConstants.WHEEL_PRIORITY_PLATFORM_SPECIFIC == 1
        assert AppConstants.WHEEL_PRIORITY_GENERIC == 2
        assert AppConstants.WHEEL_PRIORITY_FALLBACK == 3

    def test_pip_common_args(self):
        """Verify pip common arguments"""
        assert "--disable-pip-version-check" in AppConstants.PIP_COMMON_ARGS
        assert "--no-warn-conflicts" in AppConstants.PIP_COMMON_ARGS


class TestWheelCandidate:
    """Test WheelCandidate dataclass"""

    def test_creation(self):
        """Test WheelCandidate creation"""
        candidate = WheelCandidate(
            file_path="/path/to/wheel.whl",
            file_name="package-1.0.0-py3-none-any.whl",
            priority=2,
            version="1.0.0"
        )

        assert candidate.file_path == "/path/to/wheel.whl"
        assert candidate.file_name == "package-1.0.0-py3-none-any.whl"
        assert candidate.priority == 2
        assert candidate.version == "1.0.0"

    def test_sort_key(self):
        """Test sort key property"""
        candidate1 = WheelCandidate("", "", 1, "1.0.0")
        candidate2 = WheelCandidate("", "", 2, "1.0.0")

        assert candidate1.sort_key == (1, "1.0.0")
        assert candidate2.sort_key == (2, "1.0.0")
        assert candidate1.sort_key < candidate2.sort_key


class TestAppManagerHelpers:
    """Test helper methods in AppManager"""

    @pytest.fixture
    def mock_app_manager(self):
        """Create mock AppManager for testing"""
        manager = Mock(spec=AppManager)
        manager.artifacts_path = "/artifacts"
        manager.logger = Mock()

        manager._get_app_dir = AppManager._get_app_dir.__get__(manager)
        manager._get_packages_dir = AppManager._get_packages_dir.__get__(
            manager)
        manager._extract_package_name = AppManager._extract_package_name.__get__(
            manager)
        manager._parse_package_spec = AppManager._parse_package_spec.__get__(
            manager)

        return manager

    def test_get_app_dir(self, mock_app_manager):
        """Test _get_app_dir helper"""
        result = mock_app_manager._get_app_dir("my_app")
        assert result == "/artifacts/apps/my_app/"

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
