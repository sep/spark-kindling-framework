"""
Unit tests for LocalService platform implementation.

Tests the local platform service which provides file operations,
notebook management, and Spark integration for local development.
"""

import pytest
from pathlib import Path
from kindling.platform_local import LocalService


class TestLocalServiceInitialization:
    """Test LocalService initialization and configuration"""

    def test_initialization_with_default_config(self, mock_logger):
        """Test service initializes with default configuration"""
        config = {}
        service = LocalService(config, mock_logger)

        assert service.get_platform_name() == "local", "Platform name should be 'local'"
        assert service.workspace_id is None, "Workspace ID should be None for local platform"
        assert (
            service.workspace_url == "http://localhost"
        ), "Workspace URL should be 'http://localhost'"
        assert (
            service.local_workspace_path.name == "kindling_local_workspace"
        ), "Default workspace directory name should be 'kindling_local_workspace'"
        assert (
            service.local_workspace_path.exists()
        ), "Default workspace directory should be created"

    def test_initialization_with_custom_workspace(self, temp_workspace, mock_logger):
        """Test service initializes with custom workspace path"""
        config = {"local_workspace_path": str(temp_workspace)}
        service = LocalService(config, mock_logger)

        assert (
            service.local_workspace_path == temp_workspace
        ), f"Workspace path should be set to {temp_workspace}"
        assert service.local_workspace_path.exists(), "Custom workspace path should exist"

    def test_creates_workspace_directory(self, temp_workspace, mock_logger):
        """Test service creates workspace directory if it doesn't exist"""
        new_workspace = temp_workspace / "new_workspace"
        assert not new_workspace.exists(), "New workspace directory should not exist initially"

        config = {"local_workspace_path": str(new_workspace)}
        service = LocalService(config, mock_logger)

        assert (
            new_workspace.exists()
        ), "LocalService should create workspace directory if it doesn't exist"
        assert (
            service.local_workspace_path == new_workspace
        ), f"Workspace path should be set to {new_workspace}"


class TestLocalServiceFileOperations:
    """Test file operations (read, write, delete, exists, etc.)"""

    def test_write_and_read_text_file(self, temp_workspace, mock_logger):
        """Test writing and reading text files"""
        config = {"local_workspace_path": str(temp_workspace)}
        service = LocalService(config, mock_logger)

        test_content = "Hello, Kindling!"
        service.write("test.txt", test_content)

        content = service.read("test.txt")
        assert (
            content == test_content
        ), f"Read content should match written content: expected '{test_content}', got '{content}'"

    def test_write_and_read_binary_file(self, temp_workspace, mock_logger):
        """Test writing and reading binary files"""
        config = {"local_workspace_path": str(temp_workspace)}
        service = LocalService(config, mock_logger)

        test_content = b"Binary content \x00\x01\x02"
        service.write("test.bin", test_content)

        content = service.read("test.bin", encoding=None)
        assert (
            content == test_content
        ), f"Binary content should match: expected {test_content}, got {content}"

    def test_file_exists(self, temp_workspace, mock_logger):
        """Test checking file existence"""
        config = {"local_workspace_path": str(temp_workspace)}
        service = LocalService(config, mock_logger)

        # File doesn't exist
        assert not service.exists(
            "nonexistent.txt"
        ), "exists() should return False for nonexistent file"

        # Create file
        service.write("exists.txt", "content")
        assert service.exists("exists.txt"), "exists() should return True after file is created"

    def test_delete_file(self, temp_workspace, mock_logger):
        """Test deleting files"""
        config = {"local_workspace_path": str(temp_workspace)}
        service = LocalService(config, mock_logger)

        # Create file
        service.write("to_delete.txt", "content")
        assert service.exists("to_delete.txt"), "File should exist after creation"

        # Delete file
        service.delete("to_delete.txt")
        assert not service.exists("to_delete.txt"), "File should not exist after deletion"

    def test_copy_file(self, temp_workspace, mock_logger):
        """Test copying files"""
        config = {"local_workspace_path": str(temp_workspace)}
        service = LocalService(config, mock_logger)

        # Create source file
        source_content = "Source content"
        service.write("source.txt", source_content)

        # Copy file
        service.copy("source.txt", "destination.txt")

        # Both files should exist
        assert service.exists("source.txt"), "Source file should still exist after copy"
        assert service.exists("destination.txt"), "Destination file should exist after copy"

        # Content should match
        assert (
            service.read("destination.txt") == source_content
        ), "Copied file content should match source"

    def test_move_file(self, temp_workspace, mock_logger):
        """Test moving files"""
        config = {"local_workspace_path": str(temp_workspace)}
        service = LocalService(config, mock_logger)

        # Create source file
        source_content = "Source content"
        service.write("source.txt", source_content)

        # Move file
        service.move("source.txt", "moved.txt")

        # Source should not exist, destination should
        assert not service.exists("source.txt"), "Source file should not exist after move"
        assert service.exists("moved.txt"), "Destination file should exist after move"
        assert (
            service.read("moved.txt") == source_content
        ), "Moved file content should match original"

    def test_list_files(self, temp_workspace, mock_logger):
        """Test listing files in directory"""
        config = {"local_workspace_path": str(temp_workspace)}
        service = LocalService(config, mock_logger)

        # Create some files
        service.write("file1.txt", "content1")
        service.write("file2.txt", "content2")
        service.write("file3.txt", "content3")

        # List files
        files = service.list(".")

        assert "file1.txt" in files, "file1.txt should be in list of files"
        assert "file2.txt" in files, "file2.txt should be in list of files"
        assert "file3.txt" in files, "file3.txt should be in list of files"

    def test_write_with_subdirectories(self, temp_workspace, mock_logger):
        """Test writing files creates subdirectories automatically"""
        config = {"local_workspace_path": str(temp_workspace)}
        service = LocalService(config, mock_logger)

        # Write to nested path
        service.write("subdir1/subdir2/file.txt", "content")

        assert service.exists(
            "subdir1/subdir2/file.txt"
        ), "File should exist in nested subdirectories"
        assert (
            service.read("subdir1/subdir2/file.txt") == "content"
        ), "Content should be readable from nested path"


class TestLocalServiceNotebooks:
    """Test notebook-related operations"""

    def test_list_notebooks_empty(self, temp_workspace, mock_logger):
        """Test listing notebooks in empty workspace"""
        config = {"local_workspace_path": str(temp_workspace)}
        service = LocalService(config, mock_logger)

        notebooks = service.list_notebooks()
        assert notebooks == [], "Empty workspace should return empty list of notebooks"

    def test_list_notebooks(self, temp_workspace, mock_logger, sample_notebook_files):
        """Test listing notebooks finds .py files"""
        config = {"local_workspace_path": str(temp_workspace)}
        service = LocalService(config, mock_logger)

        # Re-initialize cache to pick up new files
        service._initialize_cache()

        notebooks = service.list_notebooks()
        notebook_names = [nb["displayName"] for nb in notebooks]

        assert "notebook1" in notebook_names, "notebook1 should be in list of notebooks"
        assert "notebook2" in notebook_names, "notebook2 should be in list of notebooks"
        assert "notebook3" in notebook_names, "notebook3 should be in list of notebooks"

    def test_get_notebook_by_name(self, temp_workspace, mock_logger, sample_notebook_files):
        """Test retrieving specific notebook"""
        config = {"local_workspace_path": str(temp_workspace)}
        service = LocalService(config, mock_logger)
        service._initialize_cache()

        notebook = service.get_notebook("notebook1")

        assert notebook is not None, "get_notebook should return a notebook object"
        assert notebook.name == "notebook1", "Notebook name should be 'notebook1'"

    def test_get_notebook_with_content(self, temp_workspace, mock_logger, sample_notebook_files):
        """Test retrieving notebook includes content"""
        config = {"local_workspace_path": str(temp_workspace)}
        service = LocalService(config, mock_logger)
        service._initialize_cache()

        notebook = service.get_notebook("notebook2", include_content=True)

        assert notebook is not None, "get_notebook should return a notebook object"
        assert hasattr(
            notebook.properties, "cells"
        ), "Notebook properties should have 'cells' attribute when include_content=True"
        assert len(notebook.properties.cells) > 0, "Notebook should have at least one cell"


class TestLocalServicePaths:
    """Test path handling (absolute vs relative)"""

    def test_relative_path_handling(self, temp_workspace, mock_logger):
        """Test operations with relative paths"""
        config = {"local_workspace_path": str(temp_workspace)}
        service = LocalService(config, mock_logger)

        # Relative path
        service.write("relative.txt", "content")
        assert service.exists("relative.txt"), "File should exist using relative path"
        assert (
            service.read("relative.txt") == "content"
        ), "Content should be readable using relative path"

    def test_absolute_path_handling(self, temp_workspace, mock_logger):
        """Test operations with absolute paths"""
        config = {"local_workspace_path": str(temp_workspace)}
        service = LocalService(config, mock_logger)

        # Absolute path
        abs_path = temp_workspace / "absolute.txt"
        service.write(str(abs_path), "content")
        assert service.exists(str(abs_path)), "File should exist using absolute path"
        assert (
            service.read(str(abs_path)) == "content"
        ), "Content should be readable using absolute path"


class TestLocalServiceInfo:
    """Test workspace and cluster info methods"""

    def test_get_workspace_info(self, temp_workspace, mock_logger):
        """Test retrieving workspace information"""
        config = {"local_workspace_path": str(temp_workspace)}
        service = LocalService(config, mock_logger)

        info = service.get_workspace_info()

        assert info["workspace_id"] is None, "workspace_id should be None for local platform"
        assert (
            info["workspace_url"] == "http://localhost"
        ), "workspace_url should be 'http://localhost'"
        assert info["platform"] == "local", "platform should be 'local'"
        assert "workspace_path" in info, "workspace_path should be in workspace info"

    def test_get_cluster_info_no_spark(self, temp_workspace, mock_logger):
        """Test cluster info when Spark not available"""
        config = {"local_workspace_path": str(temp_workspace)}
        service = LocalService(config, mock_logger)

        info = service.get_cluster_info()

        assert "environment" in info, "cluster info should contain 'environment' key"
        assert info["environment"] == "local", "environment should be 'local'"

    def test_get_platform_name(self, temp_workspace, mock_logger):
        """Test getting platform name"""
        config = {"local_workspace_path": str(temp_workspace)}
        service = LocalService(config, mock_logger)

        assert service.get_platform_name() == "local", "get_platform_name() should return 'local'"

    def test_get_token(self, temp_workspace, mock_logger):
        """Test getting token returns dummy value"""
        config = {"local_workspace_path": str(temp_workspace)}
        service = LocalService(config, mock_logger)

        token = service.get_token("some-audience")
        assert (
            token == "local-dummy-token"
        ), "get_token() should return 'local-dummy-token' for local platform"


class TestLocalServiceEdgeCases:
    """Test edge cases and error handling"""

    def test_read_nonexistent_file_raises_error(self, temp_workspace, mock_logger):
        """Test reading nonexistent file raises error"""
        config = {"local_workspace_path": str(temp_workspace)}
        service = LocalService(config, mock_logger)

        with pytest.raises(FileNotFoundError, match=".*nonexistent.txt.*"):
            service.read("nonexistent.txt")

    def test_write_overwrite_protection(self, temp_workspace, mock_logger):
        """Test overwrite=False protects existing files"""
        config = {"local_workspace_path": str(temp_workspace)}
        service = LocalService(config, mock_logger)

        # Create initial file
        service.write("protected.txt", "original")

        # Try to overwrite without flag
        with pytest.raises(FileExistsError, match=".*protected.txt.*"):
            service.write("protected.txt", "new content", overwrite=False)

        # Content should be unchanged
        assert (
            service.read("protected.txt") == "original"
        ), "File content should remain unchanged when overwrite=False raises error"

    def test_write_with_overwrite(self, temp_workspace, mock_logger):
        """Test overwrite=True replaces existing files"""
        config = {"local_workspace_path": str(temp_workspace)}
        service = LocalService(config, mock_logger)

        # Create initial file
        service.write("replaceable.txt", "original")

        # Overwrite
        service.write("replaceable.txt", "new content", overwrite=True)

        # Content should be updated
        assert (
            service.read("replaceable.txt") == "new content"
        ), "File content should be updated when overwrite=True"

    def test_delete_nonexistent_file_no_error(self, temp_workspace, mock_logger):
        """Test deleting nonexistent file doesn't raise error"""
        config = {"local_workspace_path": str(temp_workspace)}
        service = LocalService(config, mock_logger)

        # Should not raise error
        service.delete("nonexistent.txt")


if __name__ == "__main__":
    # Allow running tests directly
    pytest.main([__file__, "-v"])
