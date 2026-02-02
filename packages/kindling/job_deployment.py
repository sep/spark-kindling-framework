"""
Job Deployment Module

Provides platform-agnostic job deployment capabilities using dependency injection
and platform services pattern. Enables deploying data apps as Spark jobs across
Fabric, Databricks, and Synapse platforms.

This module contains both utilities (JobPackager, JobConfigValidator) and services
(DataAppDeployer) as they are tightly coupled and used together.
"""

import time
import uuid
import zipfile
from pathlib import Path
from typing import Any, Dict, Optional

from injector import inject

from .platform_provider import PlatformServiceProvider
from .signaling import SignalEmitter, SignalProvider
from .spark_log_provider import PythonLoggerProvider

# ============================================================================
# UTILITIES - Pure functions for mechanical operations
# ============================================================================


class JobPackager:
    """Utility for preparing app files for job deployment"""

    @staticmethod
    def prepare_app_files(app_path: str) -> Dict[str, str]:
        """Prepare app files for deployment

        Args:
            app_path: Path to app directory or .kda file

        Returns:
            Dictionary of {filename: file_content}
        """
        path = Path(app_path)

        if path.is_file() and path.suffix == ".kda":
            return JobPackager.extract_kda_files(path)
        elif path.is_dir():
            return JobPackager.scan_directory_files(path)
        else:
            raise ValueError(f"Invalid app path: {app_path}")

    @staticmethod
    def extract_kda_files(kda_path: Path) -> Dict[str, str]:
        """Extract files from KDA package

        Args:
            kda_path: Path to .kda file

        Returns:
            Dictionary of {filename: file_content}
        """
        app_files = {}

        with zipfile.ZipFile(kda_path, "r") as zf:
            for file_info in zf.filelist:
                if file_info.filename.endswith(".py") or file_info.filename.endswith(".yaml"):
                    content = zf.read(file_info.filename).decode("utf-8")
                    app_files[file_info.filename] = content

        return app_files

    @staticmethod
    def scan_directory_files(dir_path: Path) -> Dict[str, str]:
        """Scan directory for Python and config files

        Args:
            dir_path: Path to app directory

        Returns:
            Dictionary of {filename: file_content}
        """
        app_files = {}

        # Include Python files
        for file_path in dir_path.rglob("*.py"):
            rel_path = file_path.relative_to(dir_path)
            with open(file_path, "r") as f:
                app_files[str(rel_path)] = f.read()

        # Include YAML config files
        for file_path in dir_path.glob("*.yaml"):
            rel_path = file_path.relative_to(dir_path)
            with open(file_path, "r") as f:
                app_files[str(rel_path)] = f.read()

        return app_files


class JobConfigValidator:
    """Utility for validating job configuration"""

    @staticmethod
    def validate(config: Dict[str, Any]) -> None:
        """Validate required job configuration parameters

        Args:
            config: Job configuration dictionary

        Raises:
            ValueError: If required parameters are missing
        """
        required = ["job_name"]
        missing = [key for key in required if key not in config]
        if missing:
            raise ValueError(f"Missing required job config parameters: {missing}")


# ============================================================================
# SERVICE - High-level orchestration with dependency injection
# ============================================================================


class DataAppDeployer(SignalEmitter):
    """
    Service for deploying data apps as Spark jobs.

    Uses dependency injection for platform service and logger.
    Delegates mechanical operations to utility classes.

    Signals emitted:
        - job.before_deploy: Before deploying an app as a job
        - job.after_deploy: After successful deployment
        - job.deploy_failed: When deployment fails
        - job.before_run: Before running a job
        - job.after_run: After job starts (returns run_id)
        - job.run_failed: When job start fails
        - job.before_cancel: Before cancelling a job
        - job.after_cancel: After job is cancelled
        - job.cancel_failed: When cancellation fails
        - job.status_checked: When job status is retrieved
    """

    EMITS = [
        "job.before_deploy",
        "job.after_deploy",
        "job.deploy_failed",
        "job.before_run",
        "job.after_run",
        "job.run_failed",
        "job.before_cancel",
        "job.after_cancel",
        "job.cancel_failed",
        "job.status_checked",
    ]

    @inject
    def __init__(
        self,
        platform_provider: PlatformServiceProvider,
        logger_provider: PythonLoggerProvider,
        signal_provider: Optional[SignalProvider] = None,
    ):
        """Initialize deployer with injected dependencies

        Args:
            platform_provider: Provider for platform service
            logger_provider: Provider for logger
            signal_provider: Optional signal provider for event emissions
        """
        self.platform = platform_provider.get()
        self.logger = logger_provider.get()
        self._init_signal_emitter(signal_provider)

        # Initialize utilities (no DI needed - pure utilities)
        self.packager = JobPackager()
        self.validator = JobConfigValidator()

    def deploy_as_job(self, app_path: str, job_config: Dict[str, Any]) -> Dict[str, Any]:
        """Deploy a data app as a Spark job

        Args:
            app_path: Path to app directory or .kda file
            job_config: Job configuration including:
                - job_name: Name for the job
                - app_name: Application name for Kindling bootstrap
                - artifacts_path: Where framework wheels are stored (default: Files/artifacts)
                - lakehouse_id or workspace_id: Platform-specific IDs
                - additional parameters as needed by platform

        Returns:
            Dictionary with deployment info from platform service

        Note:
            Job will use kindling_bootstrap.py from lakehouse Files/scripts/
            Bootstrap config is passed via command line arguments (config:key=value)
        """
        deployment_id = str(uuid.uuid4())
        start_time = time.time()
        job_name = job_config.get("job_name", "unknown")
        platform_name = self.platform.get_platform_name()

        self.logger.info(f"Deploying app from: {app_path}")

        self.emit(
            "job.before_deploy",
            app_path=app_path,
            job_name=job_name,
            platform=platform_name,
            deployment_id=deployment_id,
        )

        try:
            # Validate config using utility
            self.validator.validate(job_config)

            # Prepare app files using utility
            app_files = self.packager.prepare_app_files(app_path)
            self.logger.debug(f"Prepared {len(app_files)} app files")

            # Delegate to platform service
            # Platform service (e.g., FabricAPI) will:
            # 1. Create job definition pointing to Files/scripts/kindling_bootstrap.py
            # 2. Pass bootstrap config via command line args (config:key=value)
            # 3. Upload app files to appropriate location
            self.logger.info(f"Deploying to {platform_name} platform")
            result = self.platform.deploy_spark_job(app_files, job_config)

            duration = time.time() - start_time
            self.logger.info(f"✅ Deployment complete: {result.get('job_id')}")

            self.emit(
                "job.after_deploy",
                app_path=app_path,
                job_name=job_name,
                job_id=result.get("job_id"),
                platform=platform_name,
                duration_seconds=duration,
                deployment_id=deployment_id,
            )

            return result

        except Exception as e:
            duration = time.time() - start_time
            self.emit(
                "job.deploy_failed",
                app_path=app_path,
                job_name=job_name,
                platform=platform_name,
                error=str(e),
                error_type=type(e).__name__,
                duration_seconds=duration,
                deployment_id=deployment_id,
            )
            raise

    def run_job(self, job_id: str, parameters: Dict[str, Any] = None) -> str:
        """Run a deployed job

        Args:
            job_id: ID of the deployed job
            parameters: Optional runtime parameters

        Returns:
            Run ID for monitoring
        """
        self.logger.info(f"Running job: {job_id}")

        self.emit("job.before_run", job_id=job_id, parameters=parameters)

        try:
            run_id = self.platform.run_spark_job(job_id, parameters)
            self.logger.info(f"Job started with run_id: {run_id}")

            self.emit("job.after_run", job_id=job_id, run_id=run_id, parameters=parameters)

            return run_id

        except Exception as e:
            self.emit(
                "job.run_failed",
                job_id=job_id,
                parameters=parameters,
                error=str(e),
                error_type=type(e).__name__,
            )
            raise

    def get_job_status(self, run_id: str) -> Dict[str, Any]:
        """Get status of a running job

        Args:
            run_id: Run ID to check

        Returns:
            Status dictionary from platform service
        """
        status = self.platform.get_job_status(run_id)

        self.emit(
            "job.status_checked",
            run_id=run_id,
            status=status.get("status"),
            is_complete=status.get("status") in ("COMPLETED", "FAILED", "CANCELLED"),
        )

        return status

    def cancel_job(self, run_id: str) -> bool:
        """Cancel a running job

        Args:
            run_id: Run ID to cancel

        Returns:
            True if cancelled successfully
        """
        self.logger.info(f"Cancelling job: {run_id}")

        self.emit("job.before_cancel", run_id=run_id)

        try:
            result = self.platform.cancel_job(run_id)

            if result:
                self.logger.info(f"✅ Job cancelled: {run_id}")
                self.emit("job.after_cancel", run_id=run_id, success=True)
            else:
                self.emit(
                    "job.cancel_failed",
                    run_id=run_id,
                    error="Platform returned False",
                    error_type="CancelFailed",
                )

            return result

        except Exception as e:
            self.emit("job.cancel_failed", run_id=run_id, error=str(e), error_type=type(e).__name__)
            raise


# Expose utilities and service in module
__all__ = ["JobPackager", "JobConfigValidator", "DataAppDeployer"]
