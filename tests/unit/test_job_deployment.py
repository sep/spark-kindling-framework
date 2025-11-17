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
            DataAppDeployer,
            JobConfigValidator,
            JobPackager,
        )

        # Mock dependencies
        mock_platform = Mock()
        mock_platform.get_platform_name.return_value = "test"

        mock_psp = Mock()
        mock_psp.get_service.return_value = mock_platform

        mock_logger = Mock()
        mock_plp = Mock()
        mock_plp.get_logger.return_value = mock_logger

        # Create deployer
        deployer = DataAppDeployer.__new__(DataAppDeployer)
        deployer.platform = mock_platform
        deployer.logger = mock_logger

        # Initialize utilities
        deployer.packager = JobPackager()
        deployer.validator = JobConfigValidator()

        # Test missing job_name
        with pytest.raises(ValueError, match="Missing required job config parameters"):
            deployer.deploy_as_job("/path/to/app", {})

    def test_deploy_as_job_prepares_files_from_directory(self):
        """Test that deploy_as_job can prepare files from a directory"""
        from kindling.job_deployment import (
            DataAppDeployer,
            JobConfigValidator,
            JobPackager,
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
            mock_platform.deploy_spark_job.return_value = {
                "job_id": "test-job-123",
                "deployment_path": "/test/path",
            }

            mock_logger = Mock()

            deployer = DataAppDeployer.__new__(DataAppDeployer)
            deployer.platform = mock_platform
            deployer.logger = mock_logger

            # Initialize utilities
            deployer.packager = JobPackager()
            deployer.validator = JobConfigValidator()

            # Deploy
            result = deployer.deploy_as_job(
                str(app_path), {"job_name": "test-job", "app_name": "test-app"}
            )

            # Verify platform service was called with app files
            assert mock_platform.deploy_spark_job.called
            call_args = mock_platform.deploy_spark_job.call_args
            app_files = call_args[0][0]

            # Check that Python files were included
            assert "main.py" in app_files
            assert "config.py" in app_files
            assert "app.yaml" in app_files

    def test_deploy_as_job_with_app_name(self):
        """Test that deploy_as_job passes app_name to platform service"""
        from kindling.job_deployment import (
            DataAppDeployer,
            JobConfigValidator,
            JobPackager,
        )

        # Create temp directory
        with tempfile.TemporaryDirectory() as tmpdir:
            app_path = Path(tmpdir)
            (app_path / "main.py").write_text("# Main file")

            # Mock dependencies
            mock_platform = Mock()
            mock_platform.get_platform_name.return_value = "fabric"
            mock_platform.deploy_spark_job.return_value = {
                "job_id": "test-job-123",
            }

            mock_logger = Mock()

            deployer = DataAppDeployer.__new__(DataAppDeployer)
            deployer.platform = mock_platform
            deployer.logger = mock_logger

            # Initialize utilities
            deployer.packager = JobPackager()
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

            # Verify platform service was called with correct config
            call_args = mock_platform.deploy_spark_job.call_args
            job_config = call_args[0][1]
            assert job_config["app_name"] == "my-data-app"
            assert job_config["artifacts_path"] == "Files/artifacts"

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


class TestJobPackager:
    """Test job packaging utility"""

    def test_prepare_app_files_from_directory(self):
        """Test packaging app files from a directory"""
        from kindling.job_deployment import JobPackager

        with tempfile.TemporaryDirectory() as tmpdir:
            app_path = Path(tmpdir)
            (app_path / "main.py").write_text("print('hello')")
            (app_path / "utils.py").write_text("def helper(): pass")
            (app_path / "config.yaml").write_text("setting: value")

            packager = JobPackager()
            app_files = packager.prepare_app_files(str(app_path))

            # Verify files were packaged
            assert "main.py" in app_files
            assert "utils.py" in app_files
            assert "config.yaml" in app_files
            assert "print('hello')" in app_files["main.py"]


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
