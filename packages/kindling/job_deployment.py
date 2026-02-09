"""
Job Deployment Module

Provides platform-agnostic job deployment capabilities using dependency injection
and platform services pattern. Enables deploying data apps and managing Spark jobs
across Fabric, Databricks, and Synapse platforms.

Architecture:
    App deployment and job deployment are SEPARATE concerns:
    - App deployment: Package files → upload to storage (deploy_app)
    - Job deployment: Create a job definition → run → monitor (create_job, run_job)
    - A job references an app by name in its config, but they are independent activities

This module contains:
    - AppPackager: Utility for preparing app files from directories or .kda packages
    - JobConfigValidator: Utility for validating job configuration
    - DataAppDeployer: Service orchestrating the full lifecycle (app deploy + job create + run)
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


class AppPackager:
    """Utility for preparing app files for deployment.

    Handles both directory-based apps and .kda packages.
    This is an app concern, not a job concern — apps are deployed
    independently from jobs.
    """

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
            return AppPackager.extract_kda_files(path)
        elif path.is_dir():
            return AppPackager.scan_directory_files(path)
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


# Backward compatibility alias
JobPackager = AppPackager


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
    Service for deploying data apps and managing Spark jobs.

    App deployment and job deployment are SEPARATE concerns:
    - deploy_app(): Package files and upload to storage
    - create_job(): Create a Spark job definition
    - run_job(): Execute a job (passing app_name as config)
    - deploy_and_create_job(): Convenience method that does both

    A single job definition can run different apps by passing different
    app_name values at execution time.

    Uses dependency injection for platform service and logger.
    Delegates mechanical operations to utility classes.

    Signals emitted:
        - app.before_deploy: Before deploying app files
        - app.after_deploy: After successful app deployment
        - app.deploy_failed: When app deployment fails
        - job.before_create: Before creating a job definition
        - job.after_create: After successful job creation
        - job.create_failed: When job creation fails
        - job.before_run: Before running a job
        - job.after_run: After job starts (returns run_id)
        - job.run_failed: When job start fails
        - job.before_cancel: Before cancelling a job
        - job.after_cancel: After job is cancelled
        - job.cancel_failed: When cancellation fails
        - job.status_checked: When job status is retrieved
    """

    EMITS = [
        "app.before_deploy",
        "app.after_deploy",
        "app.deploy_failed",
        "job.before_create",
        "job.after_create",
        "job.create_failed",
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
        self.packager = AppPackager()
        self.validator = JobConfigValidator()

    def deploy_app(self, app_path: str, app_name: str) -> str:
        """Deploy app files to platform storage.

        This is INDEPENDENT of job creation. Apps are deployed to storage
        and can be referenced by any number of jobs.

        Args:
            app_path: Path to app directory or .kda file
            app_name: Name for the deployed app (used as storage key)

        Returns:
            Storage path where app was deployed
        """
        platform_name = self.platform.get_platform_name()
        self.logger.info(f"Deploying app '{app_name}' from: {app_path}")

        self.emit(
            "app.before_deploy",
            app_path=app_path,
            app_name=app_name,
            platform=platform_name,
        )

        try:
            # Prepare app files using utility
            app_files = self.packager.prepare_app_files(app_path)
            self.logger.debug(f"Prepared {len(app_files)} app files")

            # Upload to platform storage
            storage_path = self.platform.deploy_app(app_name, app_files)
            self.logger.info(f"✅ App deployed to: {storage_path}")

            self.emit(
                "app.after_deploy",
                app_path=app_path,
                app_name=app_name,
                storage_path=storage_path,
                platform=platform_name,
            )

            return storage_path

        except Exception as e:
            self.emit(
                "app.deploy_failed",
                app_path=app_path,
                app_name=app_name,
                platform=platform_name,
                error=str(e),
                error_type=type(e).__name__,
            )
            raise

    def create_job(self, job_config: Dict[str, Any]) -> Dict[str, Any]:
        """Create a Spark job definition.

        This is INDEPENDENT of app deployment. A job references an app
        by name in its config (app_name), but the job definition itself
        is a separate resource.

        Args:
            job_config: Job configuration including:
                - job_name: Name for the job definition
                - app_name: Which app to run (referenced by bootstrap)
                - Additional platform-specific parameters

        Returns:
            Dictionary with job info including 'job_id'
        """
        platform_name = self.platform.get_platform_name()
        job_name = job_config.get("job_name", "unknown")

        self.logger.info(f"Creating job '{job_name}' on {platform_name}")

        self.emit(
            "job.before_create",
            job_name=job_name,
            platform=platform_name,
        )

        try:
            self.validator.validate(job_config)
            result = self.platform.create_job(job_name, job_config)

            self.logger.info(f"✅ Job created: {result.get('job_id')}")

            self.emit(
                "job.after_create",
                job_name=job_name,
                job_id=result.get("job_id"),
                platform=platform_name,
            )

            return result

        except Exception as e:
            self.emit(
                "job.create_failed",
                job_name=job_name,
                platform=platform_name,
                error=str(e),
                error_type=type(e).__name__,
            )
            raise

    def deploy_as_job(self, app_path: str, job_config: Dict[str, Any]) -> Dict[str, Any]:
        """Convenience method: deploy app + create job in one call.

        Equivalent to calling deploy_app() then create_job() separately.
        Maintained for backward compatibility.

        Args:
            app_path: Path to app directory or .kda file
            job_config: Job configuration including job_name, app_name, etc.

        Returns:
            Dictionary with deployment info from platform service
        """
        deployment_id = str(uuid.uuid4())
        start_time = time.time()
        job_name = job_config.get("job_name", "unknown")
        app_name = job_config.get("app_name", job_name)
        platform_name = self.platform.get_platform_name()

        self.logger.info(f"Deploying app and creating job from: {app_path}")

        try:
            # Step 1: Deploy app files to storage
            self.deploy_app(app_path, app_name)

            # Step 2: Create job definition
            result = self.create_job(job_config)

            duration = time.time() - start_time
            self.logger.info(
                f"✅ App deployed and job created: {result.get('job_id')} ({duration:.1f}s)"
            )

            return result

        except Exception as e:
            duration = time.time() - start_time
            self.logger.error(f"Deploy failed after {duration:.1f}s: {e}", exc_info=True)
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

    def cleanup_app(self, app_name: str) -> bool:
        """Clean up deployed app files from storage.

        Args:
            app_name: Name of the app to clean up

        Returns:
            True if cleanup succeeded
        """
        self.logger.info(f"Cleaning up app: {app_name}")
        try:
            result = self.platform.cleanup_app(app_name)
            if result:
                self.logger.info(f"✅ App cleaned up: {app_name}")
            return result
        except Exception as e:
            self.logger.warning(f"App cleanup failed for {app_name}: {e}")
            return False


# Expose utilities and service in module
__all__ = ["AppPackager", "JobPackager", "JobConfigValidator", "DataAppDeployer"]
