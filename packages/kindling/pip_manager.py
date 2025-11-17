"""
Pip Manager - Centralized pip operations for the framework

Provides a consistent interface for all pip operations with:
- Standardized error handling
- Comprehensive logging
- Easy mocking for tests
- Timeout protection
"""

import subprocess
import sys
import time
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import List, Optional

from injector import inject
from kindling.injection import GlobalInjector


class PipCommand(Enum):
    """Pip command types"""

    INSTALL = "install"
    WHEEL = "wheel"
    UNINSTALL = "uninstall"
    LIST = "list"
    SHOW = "show"


@dataclass
class PipResult:
    """Result of a pip operation"""

    success: bool
    return_code: int
    stdout: str
    stderr: str
    command: List[str]
    duration_seconds: float

    @property
    def output(self) -> str:
        """Combined output"""
        return self.stdout + self.stderr

    def raise_on_error(self, message: str = None):
        """Raise exception if operation failed"""
        if not self.success:
            error_msg = message or f"Pip command failed: {' '.join(self.command)}"
            raise PipExecutionError(error_msg, self)


class PipExecutionError(Exception):
    """Exception for pip execution failures"""

    def __init__(self, message: str, result: PipResult):
        super().__init__(message)
        self.result = result


class PipManager:
    """
    Centralized manager for all pip operations in the framework.

    Features:
    - Consistent error handling
    - Comprehensive logging
    - Easy mocking for tests
    - Timeout protection
    """

    DEFAULT_COMMON_ARGS = ["--disable-pip-version-check", "--no-warn-conflicts"]

    DEFAULT_TIMEOUT_SECONDS = 300  # 5 minutes

    def __init__(
        self, logger=None, common_args: List[str] = None, timeout: int = DEFAULT_TIMEOUT_SECONDS
    ):
        """
        Initialize PipManager

        Args:
            logger: Logger instance (optional)
            common_args: Common arguments for all pip commands
            timeout: Timeout for pip operations in seconds
        """
        self.logger = logger
        self.common_args = common_args or self.DEFAULT_COMMON_ARGS
        self.timeout = timeout

    def install_packages(
        self,
        packages: List[str],
        find_links: Optional[str] = None,
        upgrade: bool = False,
        no_deps: bool = False,
        extra_args: List[str] = None,
    ) -> PipResult:
        """
        Install packages via pip

        Args:
            packages: List of package specs (e.g., ['pandas==1.5.0', 'numpy'])
            find_links: Additional location to search for packages
            upgrade: Whether to upgrade packages
            no_deps: Install without dependencies
            extra_args: Additional pip arguments

        Returns:
            PipResult with operation details
        """
        if not packages:
            return self._empty_result("No packages to install")

        args = ["install", *packages]

        if find_links:
            args.extend(["--find-links", find_links])

        if upgrade:
            args.append("--upgrade")

        if no_deps:
            args.append("--no-deps")

        if extra_args:
            args.extend(extra_args)

        self._log_info(
            f"Installing {len(packages)} packages: {', '.join(packages[:3])}{'...' if len(packages) > 3 else ''}"
        )

        return self._execute_pip(args)

    def install_wheels(self, wheel_paths: List[Path], force_reinstall: bool = False) -> PipResult:
        """
        Install wheel files

        Args:
            wheel_paths: List of paths to .whl files
            force_reinstall: Force reinstallation

        Returns:
            PipResult with operation details
        """
        if not wheel_paths:
            return self._empty_result("No wheels to install")

        wheel_strs = [str(p) for p in wheel_paths]
        args = ["install", *wheel_strs]

        if force_reinstall:
            args.append("--force-reinstall")

        self._log_info(f"Installing {len(wheel_paths)} wheel files")

        return self._execute_pip(args)

    def build_wheel(self, source_dir: str, output_dir: str, no_deps: bool = True) -> PipResult:
        """
        Build a wheel from source

        Args:
            source_dir: Source directory containing setup.py or pyproject.toml
            output_dir: Output directory for built wheel
            no_deps: Build without building dependencies

        Returns:
            PipResult with operation details
        """
        args = ["wheel", source_dir, "-w", output_dir]

        if no_deps:
            args.append("--no-deps")

        self._log_info(f"Building wheel from {source_dir}")

        return self._execute_pip(args, use_common_args=False)

    def uninstall_packages(self, packages: List[str], yes: bool = True) -> PipResult:
        """
        Uninstall packages

        Args:
            packages: List of package names
            yes: Auto-confirm uninstallation

        Returns:
            PipResult with operation details
        """
        if not packages:
            return self._empty_result("No packages to uninstall")

        args = ["uninstall", *packages]

        if yes:
            args.append("-y")

        self._log_info(f"Uninstalling {len(packages)} packages")

        return self._execute_pip(args, use_common_args=False)

    def list_installed(self) -> PipResult:
        """List installed packages"""
        return self._execute_pip(["list"], use_common_args=False)

    def show_package(self, package_name: str) -> PipResult:
        """Show package details"""
        return self._execute_pip(["show", package_name], use_common_args=False)

    def _execute_pip(self, args: List[str], use_common_args: bool = True) -> PipResult:
        """
        Execute pip command

        Args:
            args: Pip arguments (without 'python -m pip')
            use_common_args: Whether to include common args

        Returns:
            PipResult with execution details
        """
        # Build full command
        cmd = [sys.executable, "-m", "pip"] + args

        if use_common_args:
            cmd.extend(self.common_args)

        start_time = time.time()

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=self.timeout)

            duration = time.time() - start_time
            success = result.returncode == 0

            pip_result = PipResult(
                success=success,
                return_code=result.returncode,
                stdout=result.stdout,
                stderr=result.stderr,
                command=cmd,
                duration_seconds=duration,
            )

            if success:
                self._log_info(f"Pip command completed successfully in {duration:.1f}s")
            else:
                self._log_error(f"Pip command failed: {result.stderr}")

            return pip_result

        except subprocess.TimeoutExpired:
            duration = time.time() - start_time
            self._log_error(f"Pip command timed out after {duration:.1f}s")
            return PipResult(
                success=False,
                return_code=-1,
                stdout="",
                stderr=f"Command timed out after {self.timeout}s",
                command=cmd,
                duration_seconds=duration,
            )

        except Exception as e:
            duration = time.time() - start_time
            self._log_error(f"Pip command exception: {e}")
            return PipResult(
                success=False,
                return_code=-1,
                stdout="",
                stderr=str(e),
                command=cmd,
                duration_seconds=duration,
            )

    def _empty_result(self, message: str) -> PipResult:
        """Create empty result for no-op operations"""
        return PipResult(
            success=True, return_code=0, stdout=message, stderr="", command=[], duration_seconds=0.0
        )

    def _log_info(self, message: str):
        """Log info message"""
        if self.logger:
            self.logger.info(message)

    def _log_warning(self, message: str):
        """Log warning message"""
        if self.logger:
            self.logger.warning(message)

    def _log_error(self, message: str):
        """Log error message"""
        if self.logger:
            self.logger.error(message)


@GlobalInjector.singleton_autobind()
class PipManagerProvider:
    """
    Provider for PipManager instances.

    Similar to SparkLogProvider, this allows services to get a PipManager
    instance that can be easily mocked for testing.

    This is a singleton that gets injected into services that need pip operations.
    """

    @inject
    def __init__(self, lp):
        """
        Initialize provider with logger from SparkLogProvider

        Args:
            lp: SparkLogProvider instance (injected)
        """
        from kindling.spark_log_provider import PythonLoggerProvider

        # Import to ensure type is available, but use injected instance
        self.log_provider = lp
        self.logger = lp.get_logger("PipManager")

    def get_pip_manager(self) -> PipManager:
        """
        Get a PipManager instance

        Returns:
            PipManager configured with logger
        """
        return PipManager(logger=self.logger)
