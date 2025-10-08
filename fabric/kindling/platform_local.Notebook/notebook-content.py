# Databricks notebook source
# MAGIC %md
# MAGIC # Local Platform Service
# MAGIC 
# MAGIC This notebook provides a local platform implementation for development and testing purposes.

# COMMAND ----------

import os
import sys
import types
import time
import json
from typing import Dict, Any, List, Optional, Union
from pathlib import Path

# Import the base PlatformService
notebook_import(".notebook_framework")

class LocalService(PlatformService):
    """
    Local platform implementation for development and testing.
    
    This service provides sensible defaults for local/non-cloud execution:
    - workspace_id = None (no workspace concept locally)
    - workspace_url = "http://localhost"
    - environment from os.environ
    - File operations use local filesystem
    """
    
    def __init__(self, config, logger):
        self.config = types.SimpleNamespace(**config)
        self.logger = logger
        self.workspace_id = None
        self.workspace_url = "http://localhost"
        
        # Set up local paths
        self.local_workspace_path = Path(config.get('local_workspace_path', './local_workspace'))
        self.local_workspace_path.mkdir(exist_ok=True)
        
        # Cache system for consistency with other platforms
        self._notebooks_cache = []
        self._folders_cache = {}
        self._items_cache = []
        self._cache_initialized = False
        
        self._initialize_cache()

    def _initialize_cache(self):
        """Initialize cache with local filesystem content"""
        try:
            self.logger.debug("Initializing local workspace cache...")
            
            # Scan local workspace for notebook files
            self._notebooks_cache = []
            if self.local_workspace_path.exists():
                for notebook_file in self.local_workspace_path.rglob("*.py"):
                    self._notebooks_cache.append({
                        'id': notebook_file.stem,
                        'displayName': notebook_file.stem,
                        'type': 'notebook',
                        'path': str(notebook_file),
                        'etag': None,
                        'properties': {
                            'folder': {'name': notebook_file.parent.name if notebook_file.parent != self.local_workspace_path else None}
                        }
                    })
            
            # Build folders cache
            self._folders_cache = {}
            for notebook in self._notebooks_cache:
                folder_info = notebook.get('properties', {}).get('folder', {})
                if folder_info and folder_info.get('name'):
                    folder_name = folder_info['name']
                    self._folders_cache[folder_name] = folder_name
            
            self._items_cache = self._notebooks_cache.copy()
            self._cache_initialized = True
            
            self.logger.debug(f"Local cache initialized with {len(self._items_cache)} items")
            self.logger.debug(f"Found {len(self._notebooks_cache)} notebooks")
            self.logger.debug(f"Found {len(self._folders_cache)} folders")
            
        except Exception as e:
            self.logger.warning(f"Failed to initialize local cache: {e}")
            self._cache_initialized = False

    def initialize(self):
        """Initialize the service"""
        if not self._cache_initialized:
            self._initialize_cache()
        return True

    def get_platform_name(self):
        """Return platform name"""
        return "local"

    def get_workspace_id(self):
        """Get workspace ID (None for local)"""
        return None

    def get_workspace_url(self):
        """Get workspace URL (localhost for local)"""
        return "http://localhost"

    def get_environment(self):
        """Get environment from environment variables"""
        return os.environ.get("PROJECT_ENV", "local")

    def get_token(self, audience: str = None) -> str:
        """Get access token (not needed for local)"""
        return "local-token"

    def exists(self, path: str) -> bool:
        """Check if a file exists locally"""
        try:
            local_path = self.local_workspace_path / path
            return local_path.exists()
        except:
            return False

    def copy(self, source: str, destination: str, overwrite: bool = False) -> None:
        """Copy a file locally"""
        import shutil
        source_path = Path(source)
        dest_path = Path(destination)
        
        if not overwrite and dest_path.exists():
            raise FileExistsError(f"Destination {destination} already exists")
            
        shutil.copy2(source_path, dest_path)

    def delete(self, path: str, recurse: bool = False) -> None:
        """Delete a file or directory locally"""
        target_path = Path(path)
        if target_path.is_file():
            target_path.unlink()
        elif target_path.is_dir() and recurse:
            import shutil
            shutil.rmtree(target_path)
        elif target_path.is_dir():
            target_path.rmdir()

    def read(self, path: str, encoding: str = 'utf-8') -> Union[str, bytes]:
        """Read file content locally"""
        local_path = self.local_workspace_path / path
        mode = 'r' if encoding else 'rb'
        with open(local_path, mode, encoding=encoding if encoding else None) as f:
            return f.read()

    def write(self, path: str, content: Union[str, bytes], overwrite: bool = False) -> None:
        """Write content to local file"""
        local_path = self.local_workspace_path / path
        
        if not overwrite and local_path.exists():
            raise FileExistsError(f"File {path} already exists")
            
        local_path.parent.mkdir(parents=True, exist_ok=True)
        mode = 'w' if isinstance(content, str) else 'wb'
        with open(local_path, mode) as f:
            f.write(content)

    def move(self, source: str, destination: str) -> None:
        """Move a file locally"""
        import shutil
        shutil.move(source, destination)

    def list(self, path: str) -> List[str]:
        """List files in local directory"""
        local_path = self.local_workspace_path / path
        if local_path.is_dir():
            return [item.name for item in local_path.iterdir()]
        return []

    def list_notebooks(self) -> List[Dict[str, Any]]:
        """List all notebooks in local workspace"""
        return self._notebooks_cache.copy()

    def get_notebook_id_by_name(self, notebook_name: str) -> Optional[str]:
        """Get notebook ID by name"""
        for notebook in self._notebooks_cache:
            if notebook['displayName'] == notebook_name:
                return notebook['id']
        return None

    def get_notebooks(self):
        """Get all notebooks as NotebookResource objects"""
        return [self._convert_to_notebook_resource(notebook) for notebook in self._notebooks_cache]

    def get_notebook(self, notebook_name: str, include_content: bool = True):
        """Get a specific notebook"""
        try:
            for notebook in self._notebooks_cache:
                if notebook['displayName'] == notebook_name:
                    return self._convert_to_notebook_resource(notebook, include_content)
            return None
        except Exception as e:
            self.logger.error(f"Error getting notebook {notebook_name}: {e}")
            return None

    def _convert_to_notebook_resource(self, local_data: Dict[str, Any], include_content: bool = True):
        """Convert local notebook data to NotebookResource format"""
        notebook_data = {
            'id': local_data['id'],
            'displayName': local_data['displayName'],
            'type': 'notebook',
            'etag': local_data.get('etag'),
            'properties': {
                'folder': local_data.get('properties', {}).get('folder'),
                'metadata': {},
                'nbformat': 4,
                'nbformat_minor': 2
            }
        }
        
        if include_content:
            # Read content from local file
            try:
                file_path = local_data.get('path')
                if file_path and Path(file_path).exists():
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                    
                    # Convert Python file to notebook format
                    notebook_data['properties']['cells'] = self._convert_python_to_cells(content)
                else:
                    notebook_data['properties']['cells'] = []
            except Exception as e:
                self.logger.warning(f"Could not read notebook content: {e}")
                notebook_data['properties']['cells'] = []
        
        return NotebookResource(**notebook_data)

    def _convert_python_to_cells(self, python_content: str) -> List[Dict[str, Any]]:
        """Convert Python file content to notebook cells"""
        cells = []
        lines = python_content.split('\n')
        current_cell = []
        cell_type = 'code'
        
        for line in lines:
            if line.startswith('# MAGIC %md'):
                # Save current cell if it has content
                if current_cell:
                    cells.append({
                        'cell_type': cell_type,
                        'source': '\n'.join(current_cell),
                        'metadata': {},
                        'outputs': [] if cell_type == 'code' else None,
                        'execution_count': None if cell_type == 'code' else None
                    })
                    current_cell = []
                cell_type = 'markdown'
            elif line.startswith('# COMMAND ----------'):
                # Save current cell and start new code cell
                if current_cell:
                    cells.append({
                        'cell_type': cell_type,
                        'source': '\n'.join(current_cell),
                        'metadata': {},
                        'outputs': [] if cell_type == 'code' else None,
                        'execution_count': None if cell_type == 'code' else None
                    })
                    current_cell = []
                cell_type = 'code'
            else:
                # Remove MAGIC prefix for markdown
                if cell_type == 'markdown' and line.startswith('# MAGIC '):
                    line = line[8:]  # Remove '# MAGIC '
                current_cell.append(line)
        
        # Save final cell
        if current_cell:
            cells.append({
                'cell_type': cell_type,
                'source': '\n'.join(current_cell),
                'metadata': {},
                'outputs': [] if cell_type == 'code' else None,
                'execution_count': None if cell_type == 'code' else None
            })
        
        return cells

    def create_notebook(self, notebook_name: str, content: str = None) -> bool:
        """Create a new notebook locally"""
        try:
            notebook_path = self.local_workspace_path / f"{notebook_name}.py"
            
            if notebook_path.exists():
                return False  # Already exists
            
            # Create default content if none provided
            if content is None:
                content = f"""# Databricks notebook source
# MAGIC %md
# MAGIC # {notebook_name}
# MAGIC 
# MAGIC This is a local notebook.

# COMMAND ----------

print("Hello from {notebook_name}!")
"""
            
            with open(notebook_path, 'w', encoding='utf-8') as f:
                f.write(content)
            
            # Update cache
            self._initialize_cache()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create notebook {notebook_name}: {e}")
            return False

    def update_notebook(self, notebook_name: str, content: str) -> bool:
        """Update an existing notebook locally"""
        try:
            notebook_path = self.local_workspace_path / f"{notebook_name}.py"
            
            with open(notebook_path, 'w', encoding='utf-8') as f:
                f.write(content)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to update notebook {notebook_name}: {e}")
            return False

    def delete_notebook(self, notebook_name: str) -> bool:
        """Delete a notebook locally"""
        try:
            notebook_path = self.local_workspace_path / f"{notebook_name}.py"
            
            if notebook_path.exists():
                notebook_path.unlink()
                
                # Update cache
                self._initialize_cache()
                
                return True
            return False
            
        except Exception as e:
            self.logger.error(f"Failed to delete notebook {notebook_name}: {e}")
            return False
