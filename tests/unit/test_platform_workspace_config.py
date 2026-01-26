"""
Unit tests for platform and workspace configuration loading
"""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


class TestPlatformWorkspaceConfig:
    """Test platform and workspace-specific configuration loading"""

    def test_download_config_files_basic(self):
        """Test basic config file download with only settings and environment"""
        from kindling.bootstrap import download_config_files

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create mock config files
            config_dir = Path(tmpdir) / "config"
            config_dir.mkdir()

            settings_file = config_dir / "settings.yaml"
            settings_file.write_text("kindling:\n  version: '0.2.0'\n")

            dev_file = config_dir / "env_dev.yaml"
            dev_file.write_text("kindling:\n  TELEMETRY:\n    logging:\n      level: DEBUG\n")

            # Mock storage utils
            mock_storage = MagicMock()

            def mock_cp(remote, local):
                # Simulate copying files
                local_path = local.replace("file://", "")
                filename = remote.split("/")[-1]
                source = config_dir / filename
                if source.exists():
                    Path(local_path).write_text(source.read_text())
                else:
                    raise FileNotFoundError(f"Config file not found: {filename}")

            mock_storage.fs.cp = mock_cp

            with patch("kindling.bootstrap._get_storage_utils", return_value=mock_storage):
                config_files = download_config_files(
                    artifacts_storage_path=tmpdir,
                    environment="dev",
                    platform=None,
                    workspace_id=None,
                )

                # Should have downloaded 2 files: settings.yaml and env_dev.yaml
                assert len(config_files) == 2
                assert "settings.yaml" in config_files[0]
                assert "env_dev.yaml" in config_files[1]

    def test_download_config_files_with_platform(self):
        """Test config file download with platform-specific config"""
        from kindling.bootstrap import download_config_files

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create mock config files
            config_dir = Path(tmpdir) / "config"
            config_dir.mkdir()

            settings_file = config_dir / "settings.yaml"
            settings_file.write_text("kindling:\n  version: '0.2.0'\n")

            platform_file = config_dir / "platform_fabric.yaml"
            platform_file.write_text(
                "kindling:\n  platform:\n    name: fabric\n  TELEMETRY:\n    logging:\n      level: DEBUG\n"
            )

            dev_file = config_dir / "env_dev.yaml"
            dev_file.write_text("kindling:\n  TELEMETRY:\n    logging:\n      level: INFO\n")

            # Mock storage utils
            mock_storage = MagicMock()

            def mock_cp(remote, local):
                local_path = local.replace("file://", "")
                filename = remote.split("/")[-1]
                source = config_dir / filename
                if source.exists():
                    Path(local_path).write_text(source.read_text())
                else:
                    raise FileNotFoundError(f"Config file not found: {filename}")

            mock_storage.fs.cp = mock_cp

            with patch("kindling.bootstrap._get_storage_utils", return_value=mock_storage):
                config_files = download_config_files(
                    artifacts_storage_path=tmpdir,
                    environment="dev",
                    platform="fabric",
                    workspace_id=None,
                )

                # Should have 3 files: settings, platform_fabric, env_dev
                assert len(config_files) == 3
                assert "settings.yaml" in config_files[0]
                assert "platform_fabric.yaml" in config_files[1]
                assert "env_dev.yaml" in config_files[2]

    def test_download_config_files_with_workspace(self):
        """Test config file download with workspace-specific config"""
        from kindling.bootstrap import download_config_files

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create mock config files
            config_dir = Path(tmpdir) / "config"
            config_dir.mkdir()

            settings_file = config_dir / "settings.yaml"
            settings_file.write_text("kindling:\n  version: '0.2.0'\n")

            platform_file = config_dir / "platform_fabric.yaml"
            platform_file.write_text("kindling:\n  platform:\n    name: fabric\n")

            workspace_file = config_dir / "workspace_abc123.yaml"
            workspace_file.write_text("kindling:\n  workspace:\n    team: 'team-a'\n")

            dev_file = config_dir / "env_dev.yaml"
            dev_file.write_text("kindling:\n  TELEMETRY:\n    logging:\n      level: INFO\n")

            # Mock storage utils
            mock_storage = MagicMock()

            def mock_cp(remote, local):
                local_path = local.replace("file://", "")
                filename = remote.split("/")[-1]
                source = config_dir / filename
                if source.exists():
                    Path(local_path).write_text(source.read_text())
                else:
                    raise FileNotFoundError(f"Config file not found: {filename}")

            mock_storage.fs.cp = mock_cp

            with patch("kindling.bootstrap._get_storage_utils", return_value=mock_storage):
                config_files = download_config_files(
                    artifacts_storage_path=tmpdir,
                    environment="dev",
                    platform="fabric",
                    workspace_id="abc123",
                )

                # Should have 4 files: settings, platform_fabric, workspace_abc123, env_dev
                assert len(config_files) == 4
                assert "settings.yaml" in config_files[0]
                assert "platform_fabric.yaml" in config_files[1]
                assert "workspace_abc123.yaml" in config_files[2]
                assert "env_dev.yaml" in config_files[3]

    def test_download_config_files_missing_optional(self):
        """Test that missing optional config files are handled gracefully"""
        from kindling.bootstrap import download_config_files

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create only settings file
            config_dir = Path(tmpdir) / "config"
            config_dir.mkdir()

            settings_file = config_dir / "settings.yaml"
            settings_file.write_text("kindling:\n  version: '0.2.0'\n")

            # Mock storage utils
            mock_storage = MagicMock()

            def mock_cp(remote, local):
                local_path = local.replace("file://", "")
                filename = remote.split("/")[-1]
                source = config_dir / filename
                if source.exists():
                    Path(local_path).write_text(source.read_text())
                else:
                    raise FileNotFoundError(f"Config file not found: {filename}")

            mock_storage.fs.cp = mock_cp

            with patch("kindling.bootstrap._get_storage_utils", return_value=mock_storage):
                config_files = download_config_files(
                    artifacts_storage_path=tmpdir,
                    environment="prod",
                    platform="fabric",
                    workspace_id="xyz789",
                )

                # Should only have settings.yaml since others are missing (but that's OK)
                assert len(config_files) == 1
                assert "settings.yaml" in config_files[0]

    def test_download_config_files_no_files_uses_default(self):
        """Test that when no config files exist, a default is created"""
        from kindling.bootstrap import download_config_files

        with tempfile.TemporaryDirectory() as tmpdir:
            # No config files created - empty directory

            # Mock storage utils
            mock_storage = MagicMock()
            mock_storage.fs.cp = MagicMock(side_effect=FileNotFoundError("No config"))

            with patch("kindling.bootstrap._get_storage_utils", return_value=mock_storage):
                config_files = download_config_files(
                    artifacts_storage_path=tmpdir,
                    environment="dev",
                    platform="fabric",
                    workspace_id="test",
                )

                # Should have created a default config
                assert len(config_files) == 1
                assert "default_settings.yaml" in config_files[0]

                # Verify default config was created
                default_config = Path(config_files[0])
                assert default_config.exists()
                content = default_config.read_text()
                assert "kindling:" in content

    def test_get_workspace_id_for_platform_fabric(self):
        """Test workspace ID detection for Fabric"""
        from kindling.bootstrap import _get_workspace_id_for_platform

        # Mock notebookutils
        mock_notebookutils = MagicMock()
        mock_notebookutils.runtime.context.get.return_value = "fabric-workspace-123"

        with patch.dict("sys.modules", {"notebookutils": mock_notebookutils}):
            workspace_id = _get_workspace_id_for_platform("fabric")
            assert workspace_id == "fabric-workspace-123"

    def test_get_workspace_id_for_platform_synapse(self):
        """Test workspace ID detection for Synapse (uses workspace name)"""
        from kindling.bootstrap import _get_workspace_id_for_platform

        # Mock Spark session
        mock_spark = MagicMock()
        mock_spark.conf.get.return_value = "synapse-workspace-name"

        with patch("kindling.bootstrap.get_or_create_spark_session", return_value=mock_spark):
            workspace_id = _get_workspace_id_for_platform("synapse")
            assert workspace_id == "synapse-workspace-name"

    def test_get_workspace_id_for_platform_databricks(self):
        """Test workspace ID detection for Databricks"""
        from kindling.bootstrap import _get_workspace_id_for_platform

        # Mock Spark session
        mock_spark = MagicMock()
        mock_spark.conf.get.return_value = "adb-123456789.azuredatabricks.net"

        with patch("kindling.bootstrap.get_or_create_spark_session", return_value=mock_spark):
            workspace_id = _get_workspace_id_for_platform("databricks")
            # Should sanitize dots to underscores
            assert workspace_id == "adb-123456789_azuredatabricks_net"

    def test_get_workspace_id_for_platform_none(self):
        """Test workspace ID returns None for unknown platform"""
        from kindling.bootstrap import _get_workspace_id_for_platform

        workspace_id = _get_workspace_id_for_platform(None)
        assert workspace_id is None

        workspace_id = _get_workspace_id_for_platform("unknown")
        assert workspace_id is None
