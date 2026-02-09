"""
Unit tests for job deployment module

Tests the DataAppDeployer orchestrator with mocked platform services.
"""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, Mock

import pytest


class TestDataAppDeployer:
    """Test DataAppDeployer orchestration logic"""

    def test_deploy_as_job_validates_config(self):
        """Test that deploy_as_job validates required config parameters"""
        from kindling.job_deployment import (
            AppPackager,
            DataAppDeployer,
            JobConfigValidator,
        )

        # Mock dependencies
        mock_platform = Mock()
        mock_platform.get_platform_name.return_value = "test"

        mock_logger = Mock()

        # Create deployer
        deployer = DataAppDeployer.__new__(DataAppDeployer)
        deployer.platform = mock_platform
        deployer.logger = mock_logger

        # Initialize utilities
        deployer.packager = AppPackager()
        deployer.validator = JobConfigValidator()

        # Test missing job_name - use a real temp dir so app path validation passes
        mock_platform.deploy_app.return_value = "/storage/path"
        with tempfile.TemporaryDirectory() as tmpdir:
            Path(tmpdir, "main.py").write_text("# test")
            with pytest.raises(ValueError, match="Missing required job config parameters"):
                deployer.deploy_as_job(tmpdir, {})

    def test_deploy_app_uploads_files_to_storage(self):
        """Test that deploy_app uploads files via platform API"""
        from kindling.job_deployment import (
            AppPackager,
            DataAppDeployer,
            JobConfigValidator,
        )

        # Create temp directory with test files
        with tempfile.TemporaryDirectory() as tmpdir:
            app_path = Path(tmpdir)
            (app_path / "main.py").write_text("# Main file")
            (app_path / "config.py").write_text("# Config file")
            (app_path / "app.yaml").write_text("name: test")

            # Mock dependencies
            mock_platform = Mock()
            mock_platform.get_platform_name.return_value = "test"
            mock_platform.deploy_app.return_value = "data-apps/test-app"

            mock_logger = Mock()

            deployer = DataAppDeployer.__new__(DataAppDeployer)
            deployer.platform = mock_platform
            deployer.logger = mock_logger
            deployer.packager = AppPackager()
            deployer.validator = JobConfigValidator()

            # Deploy app
            storage_path = deployer.deploy_app(str(app_path), "test-app")

            # Verify platform deploy_app was called
            assert mock_platform.deploy_app.called
            call_args = mock_platform.deploy_app.call_args
            assert call_args[0][0] == "test-app"  # app_name
            app_files = call_args[0][1]
            assert "main.py" in app_files
            assert "config.py" in app_files
            assert "app.yaml" in app_files
            assert storage_path == "data-apps/test-app"

    def test_create_job_creates_definition(self):
        """Test that create_job calls platform create_job"""
        from kindling.job_deployment import DataAppDeployer, JobConfigValidator

        mock_platform = Mock()
        mock_platform.get_platform_name.return_value = "test"
        mock_platform.create_job.return_value = {"job_id": "test-job-123"}

        mock_logger = Mock()

        deployer = DataAppDeployer.__new__(DataAppDeployer)
        deployer.platform = mock_platform
        deployer.logger = mock_logger
        deployer.validator = JobConfigValidator()

        result = deployer.create_job({"job_name": "test-job", "app_name": "my-app"})

        assert result["job_id"] == "test-job-123"
        mock_platform.create_job.assert_called_once_with(
            "test-job", {"job_name": "test-job", "app_name": "my-app"}
        )

    def test_deploy_as_job_calls_deploy_app_then_create_job(self):
        """Test that deploy_as_job does both deploy_app and create_job"""
        from kindling.job_deployment import (
            AppPackager,
            DataAppDeployer,
            JobConfigValidator,
        )

        # Create temp directory with test files
        with tempfile.TemporaryDirectory() as tmpdir:
            app_path = Path(tmpdir)
            (app_path / "main.py").write_text("# Main file")

            # Mock dependencies
            mock_platform = Mock()
            mock_platform.get_platform_name.return_value = "fabric"
            mock_platform.deploy_app.return_value = "data-apps/my-data-app"
            mock_platform.create_job.return_value = {"job_id": "test-job-123"}

            mock_logger = Mock()

            deployer = DataAppDeployer.__new__(DataAppDeployer)
            deployer.platform = mock_platform
            deployer.logger = mock_logger
            deployer.packager = AppPackager()
            deployer.validator = JobConfigValidator()

            # Deploy with app_name and artifacts_path
            result = deployer.deploy_as_job(
                str(app_path),
                {
                    "job_name": "test-job",
                    "app_name": "my-data-app",
                    "artifacts_path": "Files/artifacts",
                },
            )

            # Verify deploy_app was called
            assert mock_platform.deploy_app.called
            deploy_call = mock_platform.deploy_app.call_args
            assert deploy_call[0][0] == "my-data-app"

            # Verify create_job was called
            assert mock_platform.create_job.called
            create_call = mock_platform.create_job.call_args
            assert create_call[0][0] == "test-job"
            assert create_call[0][1]["app_name"] == "my-data-app"
            assert create_call[0][1]["artifacts_path"] == "Files/artifacts"

            assert result["job_id"] == "test-job-123"

    def test_run_job_calls_platform_service(self):
        """Test that run_job delegates to platform service"""
        from kindling.job_deployment import DataAppDeployer

        # Mock dependencies
        mock_platform = Mock()
        mock_platform.run_spark_job.return_value = "run-123"

        mock_logger = Mock()

        deployer = DataAppDeployer.__new__(DataAppDeployer)
        deployer.platform = mock_platform
        deployer.logger = mock_logger

        # Run job
        run_id = deployer.run_job("job-123", {"param1": "value1"})

        # Verify platform service was called
        assert run_id == "run-123"
        mock_platform.run_spark_job.assert_called_once_with("job-123", {"param1": "value1"})

    def test_get_job_status_delegates_to_platform(self):
        """Test that get_job_status delegates to platform service"""
        from kindling.job_deployment import DataAppDeployer

        # Mock dependencies
        mock_platform = Mock()
        mock_platform.get_job_status.return_value = {
            "status": "Running",
            "start_time": "2025-10-22T10:00:00",
        }

        deployer = DataAppDeployer.__new__(DataAppDeployer)
        deployer.platform = mock_platform

        # Get status
        status = deployer.get_job_status("run-123")

        # Verify
        assert status["status"] == "Running"
        mock_platform.get_job_status.assert_called_once_with("run-123")

    def test_cancel_job_delegates_to_platform(self):
        """Test that cancel_job delegates to platform service"""
        from kindling.job_deployment import DataAppDeployer

        # Mock dependencies
        mock_platform = Mock()
        mock_platform.cancel_job.return_value = True

        mock_logger = Mock()

        deployer = DataAppDeployer.__new__(DataAppDeployer)
        deployer.platform = mock_platform
        deployer.logger = mock_logger

        # Cancel job
        result = deployer.cancel_job("run-123")

        # Verify
        assert result is True
        mock_platform.cancel_job.assert_called_once_with("run-123")

    def test_cleanup_app_delegates_to_platform(self):
        """Test that cleanup_app delegates to platform service"""
        from kindling.job_deployment import DataAppDeployer

        mock_platform = Mock()
        mock_platform.cleanup_app.return_value = True

        mock_logger = Mock()

        deployer = DataAppDeployer.__new__(DataAppDeployer)
        deployer.platform = mock_platform
        deployer.logger = mock_logger

        result = deployer.cleanup_app("my-app")

        assert result is True
        mock_platform.cleanup_app.assert_called_once_with("my-app")


class TestAppPackager:
    """Test app packaging utility"""

    def test_prepare_app_files_from_directory(self):
        """Test packaging app files from a directory"""
        from kindling.job_deployment import AppPackager

        with tempfile.TemporaryDirectory() as tmpdir:
            app_path = Path(tmpdir)
            (app_path / "main.py").write_text("print('hello')")
            (app_path / "utils.py").write_text("def helper(): pass")
            (app_path / "config.yaml").write_text("setting: value")

            packager = AppPackager()
            app_files = packager.prepare_app_files(str(app_path))

            # Verify files were packaged
            assert "main.py" in app_files
            assert "utils.py" in app_files
            assert "config.yaml" in app_files
            assert "print('hello')" in app_files["main.py"]


class TestJobPackagerBackwardCompat:
    """Test backward compatibility alias"""

    def test_job_packager_is_app_packager(self):
        """Test that JobPackager is an alias for AppPackager"""
        from kindling.job_deployment import AppPackager, JobPackager

        assert JobPackager is AppPackager


class TestJobConfigValidator:
    """Test job config validation utility"""

    def test_validates_required_fields(self):
        """Test that validator checks for required fields"""
        from kindling.job_deployment import JobConfigValidator

        validator = JobConfigValidator()

        # Should raise on missing job_name
        with pytest.raises(ValueError, match="Missing required job config parameters"):
            validator.validate({})

        # Should pass with job_name
        validator.validate({"job_name": "test-job"})  # Should not raise
