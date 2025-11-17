"""
Unit tests for notebook utility classes

Tests NotebookUtilities, NotebookWheelBuilder, NotebookAppBuilder, and DataAppPackage
"""

import os
import tempfile
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, Mock

import pytest


class MockNotebookCell:
    """Mock notebook cell for testing"""

    def __init__(self, cell_type="code", source=""):
        self.cell_type = cell_type
        self.source = source if isinstance(source, list) else [source]


class MockNotebookProperties:
    """Mock notebook properties"""

    def __init__(self, cells):
        self.cells = cells


class MockNotebook:
    """Mock notebook resource"""

    def __init__(self, name, cells):
        self.name = name
        self.properties = MockNotebookProperties(cells)


class MockPlatformService:
    """Mock platform service for testing"""

    def __init__(self):
        self.notebooks = []

    def get_notebook(self, name):
        for nb in self.notebooks:
            if nb.name == name:
                return nb
        raise FileNotFoundError(f"Notebook not found: {name}")

    def get_notebooks(self):
        return self.notebooks

    def copy(self, source, destination, overwrite=False):
        pass


class TestNotebookUtilities:
    """Test NotebookUtilities class"""

    def test_extract_code_from_notebook(self):
        """Test extracting code from a notebook"""
        from kindling.notebook_framework import NotebookUtilities

        # Create mock notebook
        cells = [
            MockNotebookCell("code", "print('Hello')"),
            MockNotebookCell("code", "x = 42"),
            MockNotebookCell("markdown", "# Comment"),
            MockNotebookCell("code", "print(x)"),
        ]
        notebook = MockNotebook("test_notebook", cells)

        # Create mock platform service
        platform_service = MockPlatformService()
        platform_service.notebooks.append(notebook)

        # Create utilities instance
        utilities = NotebookUtilities(platform_service)

        # Extract code
        code, imports = utilities.extract_code_from_notebook("test_notebook")

        # Verify
        assert "print('Hello')" in code
        assert "x = 42" in code
        assert "print(x)" in code
        assert "# Comment" not in code  # Markdown should be excluded
        assert isinstance(imports, list)

    def test_extract_code_with_notebook_imports(self):
        """Test processing notebook_import() calls"""
        from kindling.notebook_framework import NotebookUtilities

        cells = [MockNotebookCell("code", 'notebook_import("mypackage.mymodule")')]
        notebook = MockNotebook("test_notebook", cells)

        platform_service = MockPlatformService()
        platform_service.notebooks.append(notebook)

        utilities = NotebookUtilities(platform_service)
        code, imports = utilities.extract_code_from_notebook("test_notebook")

        # Should convert to standard import
        assert "from mypackage.mymodule import *" in code
        assert "mymodule" in imports

    def test_get_notebooks_in_folder(self):
        """Test finding notebooks in a folder"""
        from kindling.notebook_framework import NotebookUtilities

        # Create mock notebooks
        notebooks = [
            MockNotebook("folder1/notebook1", []),
            MockNotebook("folder1/notebook2", []),
            MockNotebook("folder1/subfolder/notebook3", []),
            MockNotebook("folder2/notebook4", []),
        ]

        platform_service = MockPlatformService()
        platform_service.notebooks = notebooks

        utilities = NotebookUtilities(platform_service)
        folder1_notebooks = utilities.get_notebooks_in_folder("folder1")

        # Should only return direct children, not subfolder
        assert len(folder1_notebooks) == 2
        assert any(nb.name == "folder1/notebook1" for nb in folder1_notebooks)
        assert any(nb.name == "folder1/notebook2" for nb in folder1_notebooks)
        assert not any(nb.name == "folder1/subfolder/notebook3" for nb in folder1_notebooks)

    def test_scan_for_notebook_packages(self):
        """Test finding notebook packages"""
        from kindling.notebook_framework import NotebookUtilities

        notebooks = [
            MockNotebook("package1/module1", []),
            MockNotebook("package1_init", []),
            MockNotebook("package2/module2", []),
            MockNotebook("package2_init", []),
            MockNotebook("not_a_package/module3", []),
        ]

        platform_service = MockPlatformService()
        platform_service.notebooks = notebooks

        utilities = NotebookUtilities(platform_service)
        packages = utilities.scan_for_notebook_packages()

        assert "package1" in packages
        assert "package2" in packages
        assert "not_a_package" not in packages


class TestNotebookWheelBuilder:
    """Test NotebookWheelBuilder class"""

    def test_wheel_builder_initialization(self):
        """Test wheel builder can be initialized"""
        from kindling.notebook_framework import NotebookWheelBuilder

        platform_service = MockPlatformService()
        builder = NotebookWheelBuilder(platform_service)

        assert builder is not None
        assert builder.utilities is not None
        assert builder.platform_service is platform_service

    def test_generate_pyproject_toml(self):
        """Test pyproject.toml generation"""
        from kindling.notebook_framework import NotebookWheelBuilder

        platform_service = MockPlatformService()
        builder = NotebookWheelBuilder(platform_service)

        pyproject = builder._generate_pyproject_toml(
            name="test-package",
            version="1.0.0",
            description="Test package",
            author="Test Author",
            deps='    "numpy",\n    "pandas",',
        )

        assert "test-package" in pyproject
        assert "1.0.0" in pyproject
        assert "Test package" in pyproject
        assert "Test Author" in pyproject
        assert "numpy" in pyproject
        assert "pandas" in pyproject

    def test_format_dependencies(self):
        """Test dependency formatting"""
        from kindling.notebook_framework import NotebookWheelBuilder

        platform_service = MockPlatformService()
        builder = NotebookWheelBuilder(platform_service)

        deps = builder._format_dependencies(["numpy>=1.0", "pandas>=2.0"])

        assert '"numpy>=1.0"' in deps
        assert '"pandas>=2.0"' in deps


class TestNotebookAppBuilder:
    """Test NotebookAppBuilder class"""

    def test_app_builder_initialization(self):
        """Test app builder can be initialized"""
        from kindling.notebook_framework import NotebookAppBuilder

        platform_service = MockPlatformService()
        builder = NotebookAppBuilder(platform_service)

        assert builder is not None
        assert builder.utilities is not None


class TestDataAppPackage:
    """Test DataAppPackage utility class"""

    def test_create_kda_package(self):
        """Test creating a .kda package"""
        from kindling.data_apps import DataAppPackage

        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test app
            app_dir = Path(temp_dir) / "test-app"
            app_dir.mkdir()

            (app_dir / "app.yaml").write_text(
                """name: test-app
version: 1.0.0
entry_point: main.py
description: Test application
"""
            )

            (app_dir / "main.py").write_text("print('Hello from test app')")

            # Create KDA
            output_path = str(Path(temp_dir) / "test-app.kda")
            result_path = DataAppPackage.create(str(app_dir), output_path)

            # Verify
            assert Path(result_path).exists()
            assert Path(result_path).suffix == ".kda"

            # Check contents
            with zipfile.ZipFile(result_path, "r") as zf:
                names = zf.namelist()
                assert "app.yaml" in names
                assert "main.py" in names
                assert "kda-manifest.json" in names

    def test_create_with_platform_config_merge(self):
        """Test creating KDA with platform-specific config merging"""
        from kindling.data_apps import DataAppPackage

        with tempfile.TemporaryDirectory() as temp_dir:
            app_dir = Path(temp_dir) / "test-app"
            app_dir.mkdir()

            (app_dir / "app.yaml").write_text(
                """name: test-app
version: 1.0.0
entry_point: main.py
"""
            )

            (app_dir / "app.synapse.yaml").write_text(
                """spark_config:
  spark.synapse.setting: "true"
"""
            )

            (app_dir / "main.py").write_text("print('test')")

            # Create with merge
            output_path = str(Path(temp_dir) / "test-app-synapse.kda")
            result_path = DataAppPackage.create(
                str(app_dir), output_path, target_platform="synapse", merge_platform_config=True
            )

            # Verify platform config was merged
            with zipfile.ZipFile(result_path, "r") as zf:
                names = zf.namelist()
                # Platform-specific config should NOT be in merged package
                assert "app.synapse.yaml" not in names
                # But merged app.yaml should be present
                assert "app.yaml" in names

    def test_extract_kda_package(self):
        """Test extracting a .kda package"""
        from kindling.data_apps import DataAppPackage

        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test app
            app_dir = Path(temp_dir) / "test-app"
            app_dir.mkdir()

            (app_dir / "app.yaml").write_text(
                """name: test-app
version: 1.0.0
entry_point: main.py
"""
            )

            (app_dir / "main.py").write_text("print('Hello')")

            # Create KDA
            kda_path = str(Path(temp_dir) / "test.kda")
            DataAppPackage.create(str(app_dir), kda_path)

            # Extract KDA
            extract_dir = Path(temp_dir) / "extracted"
            manifest = DataAppPackage.extract(kda_path, str(extract_dir))

            # Verify extraction
            assert (extract_dir / "app.yaml").exists()
            assert (extract_dir / "main.py").exists()
            assert manifest.name == "test-app"
            assert manifest.version == "1.0.0"

    def test_create_without_app_yaml_raises_error(self):
        """Test that creating KDA without app.yaml raises error"""
        from kindling.data_apps import DataAppPackage

        with tempfile.TemporaryDirectory() as temp_dir:
            app_dir = Path(temp_dir) / "invalid-app"
            app_dir.mkdir()
            (app_dir / "main.py").write_text("print('test')")

            # Should raise error
            with pytest.raises(FileNotFoundError):
                DataAppPackage.create(str(app_dir))

    def test_extract_invalid_kda_raises_error(self):
        """Test that extracting invalid KDA raises error"""
        from kindling.data_apps import DataAppPackage

        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a zip file without manifest
            kda_path = Path(temp_dir) / "invalid.kda"
            with zipfile.ZipFile(kda_path, "w") as zf:
                zf.writestr("dummy.txt", "not a valid KDA")

            extract_dir = Path(temp_dir) / "extracted"

            # Should raise error
            with pytest.raises(ValueError, match="missing manifest"):
                DataAppPackage.extract(str(kda_path), str(extract_dir))


class TestUtilitiesIntegration:
    """Integration tests for utilities working together"""

    def test_utilities_with_logger(self):
        """Test utilities work with logger"""
        from kindling.notebook_framework import NotebookUtilities

        # Create mock logger
        mock_logger = Mock()

        platform_service = MockPlatformService()
        utilities = NotebookUtilities(platform_service, logger=mock_logger)

        # Should accept logger
        assert utilities.logger is mock_logger


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
