"""
Unit tests for data_apps module

Tests for the refactored DataAppManager class, focusing on:
- Config loading with environment overrides
- Dependency resolution and wheel selection
- Installation orchestration
- App context preparation
"""

import io
import zipfile
from pathlib import Path
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


def _make_wheel_zip(*requires_dist: str) -> bytes:
    """Build a minimal in-memory wheel zip with the given Requires-Dist entries."""
    buf = io.BytesIO()
    lines = ["Metadata-Version: 2.1", "Name: testpkg", "Version: 1.0.0"]
    for req in requires_dist:
        lines.append(f"Requires-Dist: {req}")
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("testpkg-1.0.0.dist-info/METADATA", "\n".join(lines).encode())
    return buf.getvalue()


class TestAppConstants:
    """Test data app framework constants"""

    def test_app_constants_values(self):
        """Test that DataAppConstants has expected values"""
        assert DataAppConstants.REQUIREMENTS_FILE == "requirements.txt"
        assert DataAppConstants.LAKE_REQUIREMENTS_FILE == "lake-reqs.txt"
        assert DataAppConstants.BASE_CONFIG_FILE == "app.yaml"
        assert DataAppConstants.DEFAULT_ENTRY_POINT == "app.py"

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
        manager.framework = "framework-value"

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
        manager._load_app_code = DataAppManager._load_app_code.__get__(manager)
        manager._execute_app = DataAppManager._execute_app.__get__(manager)

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

    def test_execute_app_preserves_import_builtins(self, mock_app_manager, monkeypatch):
        """App execution should not inherit broken import machinery from __main__."""
        import __main__

        monkeypatch.setitem(__main__.__dict__, "__builtins__", None)

        result = mock_app_manager._execute_app(
            "testapp",
            "from datetime import datetime\n"
            "assert datetime is not None\n"
            "assert framework == 'framework-value'\n"
            "assert logger is not None\n"
            "result = 'ok'\n",
        )

        assert result == "ok"

    def test_load_app_code_uses_default_app_py(self, mock_app_manager):
        platform = Mock()
        platform.read.return_value = "result = 'ok'\n"
        mock_app_manager.get_platform_service.return_value = platform

        code, entry_point = mock_app_manager._load_app_code("testapp", "app.py")

        assert code == "result = 'ok'\n"
        assert entry_point == "app.py"
        platform.read.assert_called_once_with("/artifacts/data-apps/testapp/app.py")

    def test_load_app_code_raises_when_entry_point_missing(self, mock_app_manager):
        platform = Mock()
        platform.read.side_effect = FileNotFoundError("missing")
        mock_app_manager.get_platform_service.return_value = platform

        with pytest.raises(Exception):
            mock_app_manager._load_app_code("testapp", "app.py")


class TestReadWheelRequires:
    """Unit tests for _read_wheel_requires."""

    @pytest.fixture
    def manager(self):
        m = Mock(spec=DataAppManager)
        m.logger = Mock()
        m._read_wheel_requires = DataAppManager._read_wheel_requires.__get__(m)
        return m

    def test_returns_requires_dist_entries(self, manager, tmp_path):
        wheel = tmp_path / "pkg-1.0-py3-none-any.whl"
        wheel.write_bytes(_make_wheel_zip("requests (>=2.28)", "click"))
        assert manager._read_wheel_requires(wheel) == ["requests (>=2.28)", "click"]

    def test_returns_empty_for_no_requires(self, manager, tmp_path):
        wheel = tmp_path / "pkg-1.0-py3-none-any.whl"
        wheel.write_bytes(_make_wheel_zip())
        assert manager._read_wheel_requires(wheel) == []

    def test_returns_empty_for_missing_metadata(self, manager, tmp_path):
        wheel = tmp_path / "pkg-1.0-py3-none-any.whl"
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("something_else.txt", "hello")
        wheel.write_bytes(buf.getvalue())
        assert manager._read_wheel_requires(wheel) == []

    def test_returns_empty_for_corrupt_zip(self, manager, tmp_path):
        wheel = tmp_path / "bad.whl"
        wheel.write_bytes(b"not a zip")
        assert manager._read_wheel_requires(wheel) == []

    def test_handles_marker_and_extras_in_entry(self, manager, tmp_path):
        wheel = tmp_path / "pkg-1.0-py3-none-any.whl"
        wheel.write_bytes(
            _make_wheel_zip(
                'pywin32 ; sys_platform == "win32"', "spark-kindling[standalone] (>=0.10.0)"
            )
        )
        reqs = manager._read_wheel_requires(wheel)
        assert len(reqs) == 2
        assert any("pywin32" in r for r in reqs)
        assert any("spark-kindling" in r for r in reqs)


class TestNormalizePkgName:
    def test_lowercases(self):
        assert DataAppManager._normalize_pkg_name("Pandas") == "pandas"

    def test_replaces_dashes_with_underscores(self):
        assert DataAppManager._normalize_pkg_name("spark-kindling") == "spark_kindling"

    def test_combined(self):
        assert DataAppManager._normalize_pkg_name("My-Package") == "my_package"


class TestDownloadLakeWheelsBFS:
    """Unit tests for BFS dependency walking in _download_lake_wheels."""

    @pytest.fixture
    def manager(self):
        m = Mock(spec=DataAppManager)
        m.logger = Mock()
        m.artifacts_path = "/artifacts"
        m._extract_package_name = DataAppManager._extract_package_name.__get__(m)
        m._normalize_pkg_name = DataAppManager._normalize_pkg_name
        m._read_wheel_requires = DataAppManager._read_wheel_requires.__get__(m)
        m._find_best_wheel = DataAppManager._find_best_wheel.__get__(m)
        m._parse_package_spec = DataAppManager._parse_package_spec.__get__(m)
        m._filter_matching_wheels = DataAppManager._filter_matching_wheels.__get__(m)
        m._get_env_name = Mock(return_value="synapse")
        m._parse_wheel_filename = DataAppManager._parse_wheel_filename.__get__(m)
        m._select_best_wheel = DataAppManager._select_best_wheel.__get__(m)
        m._download_lake_wheels = DataAppManager._download_lake_wheels.__get__(m)
        return m

    def _make_platform_copy(self, wheel_bytes_map: dict):
        """Return a copy() side_effect that writes wheel bytes to the local path."""

        def _copy(remote_path, local_url, **kwargs):
            local_path = local_url.replace("file://", "")
            wheel_name = remote_path.split("/")[-1]
            data = wheel_bytes_map.get(wheel_name, _make_wheel_zip())
            Path(local_path).write_bytes(data)

        return _copy

    def test_downloads_only_top_level_when_no_lake_deps(self, manager, tmp_path):
        pkgA_bytes = _make_wheel_zip("requests (>=2.28)")  # requests not in lake
        manager._list_available_wheels.return_value = [
            "/artifacts/packages/pkg_a-1.0-py3-none-any.whl"
        ]
        platform = Mock()
        platform.copy.side_effect = self._make_platform_copy(
            {"pkg_a-1.0-py3-none-any.whl": pkgA_bytes}
        )
        manager.get_platform_service.return_value = platform

        wheels_dir = manager._download_lake_wheels("myapp", ["pkg-a"], str(tmp_path))

        downloaded = list(Path(wheels_dir).glob("*.whl"))
        assert len(downloaded) == 1
        assert downloaded[0].name == "pkg_a-1.0-py3-none-any.whl"

    def test_follows_transitive_lake_dep(self, manager, tmp_path):
        pkgA_bytes = _make_wheel_zip("pkg-b")
        pkgB_bytes = _make_wheel_zip()
        manager._list_available_wheels.return_value = [
            "/artifacts/packages/pkg_a-1.0-py3-none-any.whl",
            "/artifacts/packages/pkg_b-1.0-py3-none-any.whl",
        ]
        platform = Mock()
        platform.copy.side_effect = self._make_platform_copy(
            {
                "pkg_a-1.0-py3-none-any.whl": pkgA_bytes,
                "pkg_b-1.0-py3-none-any.whl": pkgB_bytes,
            }
        )
        manager.get_platform_service.return_value = platform

        wheels_dir = manager._download_lake_wheels("myapp", ["pkg-a"], str(tmp_path))

        names = {p.name for p in Path(wheels_dir).glob("*.whl")}
        assert "pkg_a-1.0-py3-none-any.whl" in names
        assert "pkg_b-1.0-py3-none-any.whl" in names

    def test_does_not_follow_non_lake_deps(self, manager, tmp_path):
        pkgA_bytes = _make_wheel_zip("requests", "click")
        manager._list_available_wheels.return_value = [
            "/artifacts/packages/pkg_a-1.0-py3-none-any.whl",
        ]
        platform = Mock()
        platform.copy.side_effect = self._make_platform_copy(
            {"pkg_a-1.0-py3-none-any.whl": pkgA_bytes}
        )
        manager.get_platform_service.return_value = platform

        wheels_dir = manager._download_lake_wheels("myapp", ["pkg-a"], str(tmp_path))

        names = {p.name for p in Path(wheels_dir).glob("*.whl")}
        assert names == {"pkg_a-1.0-py3-none-any.whl"}

    def test_handles_cycles_without_infinite_loop(self, manager, tmp_path):
        pkgA_bytes = _make_wheel_zip("pkg-b")
        pkgB_bytes = _make_wheel_zip("pkg-a")  # circular dep back to pkgA
        manager._list_available_wheels.return_value = [
            "/artifacts/packages/pkg_a-1.0-py3-none-any.whl",
            "/artifacts/packages/pkg_b-1.0-py3-none-any.whl",
        ]
        platform = Mock()
        platform.copy.side_effect = self._make_platform_copy(
            {
                "pkg_a-1.0-py3-none-any.whl": pkgA_bytes,
                "pkg_b-1.0-py3-none-any.whl": pkgB_bytes,
            }
        )
        manager.get_platform_service.return_value = platform

        wheels_dir = manager._download_lake_wheels("myapp", ["pkg-a"], str(tmp_path))

        names = {p.name for p in Path(wheels_dir).glob("*.whl")}
        assert names == {
            "pkg_a-1.0-py3-none-any.whl",
            "pkg_b-1.0-py3-none-any.whl",
        }
        assert platform.copy.call_count == 2  # each downloaded exactly once

    def test_returns_empty_string_for_empty_requirements(self, manager, tmp_path):
        result = manager._download_lake_wheels("myapp", [], str(tmp_path))
        assert result == ""

    def test_skips_missing_top_level_wheel_gracefully(self, manager, tmp_path):
        manager._list_available_wheels.return_value = []
        manager.get_platform_service.return_value = Mock()

        wheels_dir = manager._download_lake_wheels("myapp", ["missing-pkg"], str(tmp_path))

        assert Path(wheels_dir).exists()
        assert list(Path(wheels_dir).glob("*.whl")) == []

    def test_list_available_wheels_called_once(self, manager, tmp_path):
        pkgA_bytes = _make_wheel_zip("pkg-b")
        pkgB_bytes = _make_wheel_zip("pkg-c")
        pkgC_bytes = _make_wheel_zip()
        manager._list_available_wheels.return_value = [
            "/artifacts/packages/pkg_a-1.0-py3-none-any.whl",
            "/artifacts/packages/pkg_b-1.0-py3-none-any.whl",
            "/artifacts/packages/pkg_c-1.0-py3-none-any.whl",
        ]
        platform = Mock()
        platform.copy.side_effect = self._make_platform_copy(
            {
                "pkg_a-1.0-py3-none-any.whl": pkgA_bytes,
                "pkg_b-1.0-py3-none-any.whl": pkgB_bytes,
                "pkg_c-1.0-py3-none-any.whl": pkgC_bytes,
            }
        )
        manager.get_platform_service.return_value = platform

        manager._download_lake_wheels("myapp", ["pkg-a"], str(tmp_path))

        manager._list_available_wheels.assert_called_once()


class TestDownloadLakeWheelsBFSSystem:
    """System-level tests: real wheel files, real temp dirs, mocked lake I/O only."""

    @pytest.fixture
    def manager(self, tmp_path):
        """Manager with real method bindings and a local 'lake' directory."""
        m = Mock(spec=DataAppManager)
        m.logger = Mock()
        m.artifacts_path = str(tmp_path / "lake")
        (tmp_path / "lake" / "packages").mkdir(parents=True)
        m._extract_package_name = DataAppManager._extract_package_name.__get__(m)
        m._normalize_pkg_name = DataAppManager._normalize_pkg_name
        m._read_wheel_requires = DataAppManager._read_wheel_requires.__get__(m)
        m._find_best_wheel = DataAppManager._find_best_wheel.__get__(m)
        m._parse_package_spec = DataAppManager._parse_package_spec.__get__(m)
        m._filter_matching_wheels = DataAppManager._filter_matching_wheels.__get__(m)
        m._get_env_name = Mock(return_value="synapse")
        m._parse_wheel_filename = DataAppManager._parse_wheel_filename.__get__(m)
        m._select_best_wheel = DataAppManager._select_best_wheel.__get__(m)
        m._download_lake_wheels = DataAppManager._download_lake_wheels.__get__(m)
        return m, tmp_path

    def _seed_lake(self, tmp_path, wheel_name: str, *requires_dist: str):
        """Write a wheel file into the fake lake packages directory."""
        wheel_bytes = _make_wheel_zip(*requires_dist)
        (tmp_path / "lake" / "packages" / wheel_name).write_bytes(wheel_bytes)

    def test_three_level_chain_all_downloaded(self, manager):
        m, tmp_path = manager
        self._seed_lake(tmp_path, "app-1.0-py3-none-any.whl", "domain-lib")
        self._seed_lake(tmp_path, "domain_lib-1.0-py3-none-any.whl", "shared-core")
        self._seed_lake(tmp_path, "shared_core-1.0-py3-none-any.whl")

        lake_dir = tmp_path / "lake" / "packages"
        available = [str(lake_dir / p.name) for p in lake_dir.glob("*.whl")]
        m._list_available_wheels.return_value = available

        def _copy_from_lake(remote_path, local_url, **kwargs):
            src = Path(remote_path)
            dst = Path(local_url.replace("file://", ""))
            dst.write_bytes(src.read_bytes())

        platform = Mock()
        platform.copy.side_effect = _copy_from_lake
        m.get_platform_service.return_value = platform

        install_dir = tmp_path / "install"
        install_dir.mkdir()
        wheels_dir = m._download_lake_wheels("myapp", ["app"], str(install_dir))

        names = {p.name for p in Path(wheels_dir).glob("*.whl")}
        assert "app-1.0-py3-none-any.whl" in names
        assert "domain_lib-1.0-py3-none-any.whl" in names
        assert "shared_core-1.0-py3-none-any.whl" in names
        assert platform.copy.call_count == 3

    def test_dep_with_version_constraint_and_extras_resolved(self, manager):
        m, tmp_path = manager
        self._seed_lake(tmp_path, "app-1.0-py3-none-any.whl", "my-lib[extra] (>=0.5.0)")
        self._seed_lake(tmp_path, "my_lib-0.9-py3-none-any.whl")

        lake_dir = tmp_path / "lake" / "packages"
        available = [str(lake_dir / p.name) for p in lake_dir.glob("*.whl")]
        m._list_available_wheels.return_value = available

        def _copy(remote_path, local_url, **kwargs):
            Path(local_url.replace("file://", "")).write_bytes(Path(remote_path).read_bytes())

        m.get_platform_service.return_value = Mock(copy=_copy)

        install_dir = tmp_path / "install"
        install_dir.mkdir()
        wheels_dir = m._download_lake_wheels("myapp", ["app"], str(install_dir))

        names = {p.name for p in Path(wheels_dir).glob("*.whl")}
        assert "my_lib-0.9-py3-none-any.whl" in names

    def test_only_top_level_listed_in_lake_reqs(self, manager):
        """Verify lake-reqs.txt only needs top-level entry."""
        m, tmp_path = manager
        self._seed_lake(tmp_path, "my_app-1.0-py3-none-any.whl", "pkg-b")
        self._seed_lake(tmp_path, "pkg_b-1.0-py3-none-any.whl", "pkg-c")
        self._seed_lake(tmp_path, "pkg_c-1.0-py3-none-any.whl")

        lake_dir = tmp_path / "lake" / "packages"
        available = [str(lake_dir / p.name) for p in lake_dir.glob("*.whl")]
        m._list_available_wheels.return_value = available

        def _copy(remote_path, local_url, **kwargs):
            Path(local_url.replace("file://", "")).write_bytes(Path(remote_path).read_bytes())

        m.get_platform_service.return_value = Mock(copy=_copy)

        install_dir = tmp_path / "install"
        install_dir.mkdir()
        # Only "my-app" listed — pkgB and pkgC should still be discovered and downloaded
        wheels_dir = m._download_lake_wheels("myapp", ["my-app"], str(install_dir))

        names = {p.name for p in Path(wheels_dir).glob("*.whl")}
        assert names == {
            "my_app-1.0-py3-none-any.whl",
            "pkg_b-1.0-py3-none-any.whl",
            "pkg_c-1.0-py3-none-any.whl",
        }


class TestParsePackageSpecWheelStem:
    """Regression tests: _parse_package_spec must handle wheel stems and filenames.

    Bug: lake-reqs.txt often contains wheel stems (e.g. 'name-1.0-py3-none-any')
    rather than PEP 508 specs.  The old implementation returned the full stem as
    the 'name', causing downstream import and matching to fail.
    """

    @pytest.fixture
    def m(self):
        obj = Mock(spec=DataAppManager)
        obj._parse_package_spec = DataAppManager._parse_package_spec.__get__(obj)
        obj._extract_package_name = DataAppManager._extract_package_name.__get__(obj)
        return obj

    def test_plain_name(self, m):
        assert m._parse_package_spec("pandas") == ("pandas", None)

    def test_pep508_equals(self, m):
        assert m._parse_package_spec("pandas==1.5.0") == ("pandas", "1.5.0")

    def test_pep508_gte(self, m):
        name, ver = m._parse_package_spec("pandas>=1.5.0")
        assert name == "pandas"
        assert ver is None

    def test_wheel_filename(self, m):
        name, ver = m._parse_package_spec("sample_engine-0.1.0-py3-none-any.whl")
        assert name == "sample_engine"
        assert ver == "0.1.0"

    def test_wheel_stem(self, m):
        """Regression: 'sample_engine-0.1.0-py3-none-any' must parse to ('sample_engine', '0.1.0')."""
        name, ver = m._parse_package_spec("sample_engine-0.1.0-py3-none-any")
        assert name == "sample_engine"
        assert ver == "0.1.0"

    def test_wheel_stem_hyphenated_name(self, m):
        """Package names with hyphens: first digit-starting part is the version boundary."""
        name, ver = m._parse_package_spec("my-package-1.0.0-py3-none-any")
        assert name == "my-package"
        assert ver == "1.0.0"

    def test_extract_package_name_wheel_stem(self, m):
        """Regression: _extract_package_name must not return the full wheel stem."""
        assert m._extract_package_name("sample_engine-0.1.0-py3-none-any") == "sample_engine"

    def test_extract_package_name_wheel_filename(self, m):
        assert m._extract_package_name("sample_engine-0.1.0-py3-none-any.whl") == "sample_engine"


class TestFilterMatchingWheelsBareFilename:
    """Regression tests: bare wheel files (no version in name) must match versioned specs.

    Bug 1: _parse_wheel_filename returned None for 'name.whl' (single-part stem).
    Bug 2: version check 'candidate.version != spec_version' rejected bare wheels
           (candidate.version is None) even when the spec had a version.
    """

    @pytest.fixture
    def m(self):
        obj = Mock(spec=DataAppManager)
        obj._parse_package_spec = DataAppManager._parse_package_spec.__get__(obj)
        obj._parse_wheel_filename = DataAppManager._parse_wheel_filename.__get__(obj)
        obj._filter_matching_wheels = DataAppManager._filter_matching_wheels.__get__(obj)
        obj._select_best_wheel = DataAppManager._select_best_wheel.__get__(obj)
        obj._get_env_name = Mock(return_value="standalone")
        return obj

    def test_bare_wheel_matches_plain_spec(self, m):
        """'sample_engine.whl' matches spec 'sample_engine'."""
        candidates = m._filter_matching_wheels(["sample_engine.whl"], "sample_engine", None)
        assert len(candidates) == 1

    def test_bare_wheel_matches_versioned_spec(self, m):
        """'sample_engine.whl' (no version in filename) matches spec version '0.1.0'."""
        candidates = m._filter_matching_wheels(["sample_engine.whl"], "sample_engine", "0.1.0")
        assert len(candidates) == 1, "bare wheel must match any version spec"

    def test_versioned_wheel_still_filtered_by_version(self, m):
        """A wheel with an explicit version is still rejected when version mismatches."""
        candidates = m._filter_matching_wheels(
            ["sample_engine-0.2.0-py3-none-any.whl"], "sample_engine", "0.1.0"
        )
        assert len(candidates) == 0

    def test_wheel_stem_spec_matches_bare_wheel(self, m):
        """Wheel stem in lake-reqs.txt ('sample_engine-0.1.0-py3-none-any') matches bare 'sample_engine.whl'."""
        spec = "sample_engine-0.1.0-py3-none-any"
        obj = Mock(spec=DataAppManager)
        obj._parse_package_spec = DataAppManager._parse_package_spec.__get__(obj)
        obj._parse_wheel_filename = DataAppManager._parse_wheel_filename.__get__(obj)
        obj._filter_matching_wheels = DataAppManager._filter_matching_wheels.__get__(obj)
        obj._get_env_name = Mock(return_value="standalone")

        pkg_name, version = obj._parse_package_spec(spec)
        candidates = obj._filter_matching_wheels(["sample_engine.whl"], pkg_name, version)
        assert len(candidates) == 1


class TestStandaloneServiceBlobHttps:
    """Regression tests: StandaloneService must handle https://...blob.core... URLs.

    Bug: KINDLING_ARTIFACTS_STORAGE_PATH configured as an HTTPS blob URL
    (common in Azure Gov Cloud) fell through to local filesystem in list()
    and copy(), silently returning empty results.
    """

    @pytest.fixture
    def service(self, tmp_path):
        import types

        from kindling.platform_standalone import StandaloneService

        cfg = types.SimpleNamespace(local_workspace_path=str(tmp_path))
        logger = Mock()
        svc = StandaloneService(cfg, logger)
        return svc

    def test_parse_blob_https_uri_standard(self, service):
        account_url, container, remote_path = service._parse_blob_https_uri(
            "https://myaccount.blob.core.windows.net/mycontainer/some/path"
        )
        assert account_url == "https://myaccount.dfs.core.windows.net"
        assert container == "mycontainer"
        assert remote_path == "some/path"

    def test_parse_blob_https_uri_gov_cloud(self, service):
        account_url, container, remote_path = service._parse_blob_https_uri(
            "https://myaccount.blob.core.usgovcloudapi.net/artifacts/packages/"
        )
        assert account_url == "https://myaccount.dfs.core.usgovcloudapi.net"
        assert container == "artifacts"
        assert remote_path == "packages/"

    def test_is_azure_storage_path_abfss(self, service):
        assert service._is_azure_storage_path("abfss://container@account.dfs.core.windows.net/path")

    def test_is_azure_storage_path_blob_https(self, service):
        assert service._is_azure_storage_path(
            "https://account.blob.core.windows.net/container/path"
        )

    def test_is_azure_storage_path_local(self, service):
        assert not service._is_azure_storage_path("/local/path")

    def test_is_azure_storage_path_plain_https(self, service):
        assert not service._is_azure_storage_path("https://example.com/some/path")

    def test_list_calls_adls_for_blob_https(self, service):
        """list() must route blob HTTPS URLs through _adls_list, not local FS."""
        with patch.object(service, "_adls_list", return_value=["file.whl"]) as mock_list:
            result = service.list("https://account.blob.core.windows.net/container/packages/")
        mock_list.assert_called_once()
        assert result == ["file.whl"]

    def test_copy_calls_adls_download_for_blob_https(self, service):
        """copy() must route blob HTTPS sources through _adls_download."""
        with patch.object(service, "_adls_download") as mock_dl:
            service.copy(
                "https://account.blob.core.windows.net/container/packages/pkg.whl",
                "file:///tmp/pkg.whl",
                overwrite=True,
            )
        mock_dl.assert_called_once()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
