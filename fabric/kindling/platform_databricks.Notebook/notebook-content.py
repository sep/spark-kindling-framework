# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import time
import subprocess
import sys
from typing import Dict, List, Optional, Any, Union
import types
import requests 
import json
import re
from datetime import datetime, timedelta
from urllib.parse import quote

notebook_import(".notebook_framework")

class DatabricksService(PlatformService):
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self._base_url = self._build_base_url()

        # Cache system from original code
        self._token_cache = {}
        self._items_cache = []
        self._folders_cache = {}
        self._notebooks_cache = []
        self.credential = self

        self._initialize_cache()

    def _initialize_cache(self):
        """Initialize the cache with all workspace items and folders"""
        try:
            self.logger.debug("Initializing workspace cache...")
            
            # Get all items in the workspace using the workspace list API
            url = f"{self._base_url}/api/2.0/workspace/list"
            params = {"path": "/", "fmt": "SOURCE"}
            self.logger.debug(f"Fetching items from: {url}")
            response = requests.get(url, headers=self._get_headers(), params=params)
            
            if response.status_code == 200:
                data = self._handle_response(response)
                
                if isinstance(data, dict):
                    self._items_cache = data.get('objects', [])
                elif isinstance(data, list):
                    self._items_cache = data
                else:
                    self._items_cache = []
                
                # Debug: Print all item types found
                item_types = {}
                for item in self._items_cache:
                    item_type = item.get('object_type', 'Unknown')
                    if item_type not in item_types:
                        item_types[item_type] = 0
                    item_types[item_type] += 1
                
                self.logger.debug(f"Item types found: {item_types}")
                
                # Separate items by type 
                self._notebooks_cache = []
                self._folders_cache = {}
                
                for item in self._items_cache:
                    item_type = item.get('object_type', '').upper()
                    
                    if item_type == 'NOTEBOOK':
                        self._notebooks_cache.append(item)
                    elif item_type == 'DIRECTORY':
                        folder_path = item.get('path')
                        folder_name = folder_path.split('/')[-1] if folder_path else ''
                        self.logger.debug(f"Found folder - Path: {folder_path}, Name: {folder_name}")
                        if folder_path:
                            self._folders_cache[folder_path] = folder_name
                
                # Recursively get items from subdirectories
                self._populate_subdirectories()
                
                self._cache_initialized = True
                self.logger.debug(f"Cache initialized with {len(self._items_cache)} total items")
                self.logger.debug(f"Found {len(self._notebooks_cache)} notebooks")
                self.logger.debug(f"Found {len(self._folders_cache)} folders")
                
                # Debug: Print folder cache contents
                if self._folders_cache:
                    self.logger.debug(f"Folder cache: {self._folders_cache}")
                
            else:
                self.logger.warning(f"Failed to initialize cache. Status: {response.status_code}")
                self.logger.debug(f"Response text: {response.text}")
                self._cache_initialized = False
                
        except Exception as e:
            self.logger.warning(f"Failed to initialize cache: {e}")
            import traceback
            traceback.print_exc()
            self._cache_initialized = False

    def _populate_subdirectories(self):
        """Recursively populate cache with items from subdirectories"""
        processed_directories = set()
        directories_to_process = [item['path'] for item in self._items_cache if item.get('object_type') == 'DIRECTORY']
        
        while directories_to_process:
            directory_path = directories_to_process.pop(0)
            
            # Skip if already processed (avoid infinite loops)
            if directory_path in processed_directories:
                continue
                
            processed_directories.add(directory_path)
            
            try:
                url = f"{self._base_url}/api/2.0/workspace/list"
                params = {"path": directory_path, "fmt": "SOURCE"}
                response = requests.get(url, headers=self._get_headers(), params=params)
                
                if response.status_code == 200:
                    data = self._handle_response(response)
                    sub_items = data.get('objects', [])
                    
                    for item in sub_items:
                        item_type = item.get('object_type', '').upper()
                        
                        if item_type == 'NOTEBOOK':
                            self._notebooks_cache.append(item)
                        elif item_type == 'DIRECTORY':
                            folder_path = item.get('path')
                            folder_name = folder_path.split('/')[-1] if folder_path else ''
                            self.logger.debug(f"Found nested folder - Path: {folder_path}, Name: {folder_name}")
                            if folder_path:
                                self._folders_cache[folder_path] = folder_name
                                # Add newly discovered directory to processing queue
                                if folder_path not in processed_directories:
                                    directories_to_process.append(folder_path)
                    
                    # Add sub-items to main cache
                    self._items_cache.extend(sub_items)
                    
                else:
                    self.logger.debug(f"Failed to access directory {directory_path}: HTTP {response.status_code}")
                    
            except Exception as e:
                self.logger.debug(f"Failed to process directory {directory_path}: {e}")

    def get_platform_name(self):
        return "databricks"

    def get_token(self, audience: str = None) -> str:
        """Get Databricks authentication token using dbutils"""
        try:
            # Try to get dbutils from the global namespace
            import __main__
            dbutils = getattr(__main__, 'dbutils', None)
            if dbutils:
                # Get the current notebook context token
                return dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
            else:
                # Fallback: try to access dbutils directly
                try:
                    from pyspark.dbutils import DBUtils
                    from pyspark.sql import SparkSession
                    spark = SparkSession.getActiveSession()
                    if spark:
                        dbutils = DBUtils(spark)
                        return dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
                except ImportError:
                    pass
                
                raise Exception("dbutils not available - not running in Databricks environment")
        except Exception as e:
            # If dbutils token fails, try environment variable
            import os
            token = os.getenv('DATABRICKS_TOKEN')
            if token:
                return token
            raise Exception(f"Failed to get Databricks token: {e}")

    def exists(self, path: str) -> bool:
        """Check if file/path exists using dbutils"""
        import __main__
        dbutils = getattr(__main__, 'dbutils', None)
        if not dbutils:
            raise Exception("dbutils not available")
        
        try:
            dbutils.fs.ls(path)
            return True
        except:
            return False

    def copy(self, source: str, destination: str, overwrite: bool = False) -> None:
        """Copy file using dbutils"""
        import __main__
        dbutils = getattr(__main__, 'dbutils', None)
        if not dbutils:
            raise Exception("dbutils not available")
        
        dbutils.fs.cp(source, destination, overwrite)

    def read(self, path: str, encoding: str = 'utf-8') -> Union[str, bytes]:
        """Read file content"""
        if path.startswith('/dbfs/') or path.startswith('dbfs:/'):
            # Use dbutils for DBFS paths
            import __main__
            dbutils = getattr(__main__, 'dbutils', None)
            if dbutils:
                return dbutils.fs.head(path)
        
        # For local paths, use standard file operations
        with open(path, 'r' if encoding else 'rb') as f:
            return f.read()

    def write(self, path: str, content: Union[str, bytes], overwrite: bool = False) -> None:
        """Write file content"""
        if path.startswith('/dbfs/') or path.startswith('dbfs:/'):
            # Use dbutils for DBFS paths
            import __main__
            dbutils = getattr(__main__, 'dbutils', None)
            if dbutils:
                # For DBFS, we need to write to a temp file first then copy
                temp_path = f"/tmp/{path.split('/')[-1]}"
                mode = 'w' if isinstance(content, str) else 'wb'
                with open(temp_path, mode) as f:
                    f.write(content)
                dbutils.fs.cp(f"file://{temp_path}", path, overwrite)
                return
        
        # For local paths, use standard file operations
        mode = 'w' if isinstance(content, str) else 'wb'
        with open(path, mode) as f:
            f.write(content)

    def list(self, path: str) -> List[str]:
        """List files in directory"""
        import __main__
        dbutils = getattr(__main__, 'dbutils', None)
        if not dbutils:
            raise Exception("dbutils not available")
        
        files = dbutils.fs.ls(path)
        return [f.name for f in files]

    def _build_base_url(self) -> str:
        """Build Databricks API base URL from workspace_id"""
        workspace_id = getattr(self.config, 'workspace_id', None)
        
        # Parse workspace_id which could be:
        # 1. Full URL: https://adb-123456789.4.azuredatabricks.net
        # 2. Just hostname: adb-123456789.4.azuredatabricks.net
        # 3. Custom domain URL for gov/private clouds
        if workspace_id:
            # If it's already a URL, use it directly
            if workspace_id.startswith('https://') or workspace_id.startswith('http://'):
                return workspace_id.rstrip('/')
            else:
                # Assume it's just the hostname
                return f"https://{workspace_id}"
        
        # Auto-detect from environment as fallback
        try:
            import __main__
            dbutils = getattr(__main__, 'dbutils', None)
            if dbutils:
                # Get workspace URL from notebook context
                context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
                browser_host_name = context.browserHostName().get()
                return f"https://{browser_host_name}"
        except:
            pass
        
        raise Exception("No workspace_id provided and unable to detect from environment")

    def _get_token(self) -> str:
        """Get access token for Databricks API with caching"""
        current_time = time.time()
        
        # Check if we have a valid cached token
        if ('token' in self._token_cache and 
            'expires_at' in self._token_cache and 
            current_time < self._token_cache['expires_at']):
            return self._token_cache['token']
        
        # Get new token
        token = self.get_token()
        
        # Cache the token (Databricks tokens typically last 24 hours)
        self._token_cache = {
            'token': token,
            'expires_at': (datetime.now() + timedelta(hours=23)).timestamp()  # Refresh an hour early
        }
        
        return token
    
    def _get_headers(self) -> Dict[str, str]:
        """Get request headers with authorization"""
        return {
            'Authorization': f'Bearer {self._get_token()}',
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'User-Agent': 'databricks-notebooks/1.0.0'
        }
    
    def _handle_response(self, response: requests.Response) -> Any:
        """Handle HTTP response and convert to appropriate exceptions"""
        if 200 <= response.status_code < 300:
            try:
                return response.json() if response.content else None
            except json.JSONDecodeError:
                return response.text if response.content else None
        
        # Convert HTTP errors to appropriate exceptions
        error_map = {
            401: Exception,  # ClientAuthenticationError
            404: Exception,  # ResourceNotFoundError
            409: Exception   # ResourceExistsError
        }
        
        exception_class = error_map.get(response.status_code, Exception)
        
        try:
            error_data = response.json()
            message = error_data.get('error_code', error_data.get('message', response.text))
        except:
            message = response.text or f"HTTP {response.status_code}"
        
        raise exception_class(message)
    
    def list_notebooks(self) -> List[Dict[str, Any]]:
        """List all notebooks in the workspace"""
        if hasattr(self, '_notebooks_cache'):
            return self._notebooks_cache
        
        # If cache not available, fetch directly
        url = f"{self._base_url}/api/2.0/workspace/list"
        params = {"path": "/", "fmt": "SOURCE"}
        
        response = requests.get(url, headers=self._get_headers(), params=params)
        data = self._handle_response(response)
        
        notebooks = []
        if isinstance(data, dict):
            objects = data.get('objects', [])
            notebooks = [obj for obj in objects if obj.get('object_type') == 'NOTEBOOK']
        
        #nbnames = [obj.name for obj in objects if obj.get('object_type') == 'NOTEBOOK']
        #print("Notebooks before rename {nbnames}")

        #for nb in notebooks:
        #    nb.name = nb.name.split('/')[-1]

        #nbnames =  [nb.name for nb in notebooks]
        #print("Notebooks post rename {nbnames}")

        return notebooks
    
    def get_notebook_by_path(self, notebook_path: str) -> Optional[Dict[str, Any]]:
        """Get notebook by its workspace path"""
        notebooks = self.list_notebooks()
        for notebook in notebooks:
            if notebook.get('path') == notebook_path:
                return notebook
        return None
    
    def get_notebooks(self):
        """Get all notebooks as NotebookResource objects"""
        return [self._convert_to_notebook_resource(d) for d in self.list_notebooks()] 

    def get_notebook(self, notebook_identifier: str, include_content: bool = True):
        """Get a specific notebook by name or path"""
        # Determine if this is already a full path or just a name
        if notebook_identifier.startswith('/'):
            # Already a full path
            notebook_path = notebook_identifier
        else:
            # Need to resolve the name using cache
            notebook_path = self._resolve_notebook_path(notebook_identifier)
            if not notebook_path:
                raise Exception(f"Notebook '{notebook_identifier}' not found in workspace")
        
        self.logger.debug(f"Resolved notebook identifier '{notebook_identifier}' to path '{notebook_path}'")
        
        # Get notebook info
        url = f"{self._base_url}/api/2.0/workspace/get-status"
        params = {"path": notebook_path}
        response = requests.get(url, headers=self._get_headers(), params=params)
        
        if response.status_code == 404:
            raise Exception(f"Notebook '{notebook_path}' not found")
        elif response.status_code != 200:
            self.logger.error(f"Failed to get notebook status: {response.status_code} - {response.text}")
            raise Exception(f"Failed to get notebook '{notebook_path}': HTTP {response.status_code}")
        
        notebook_data = self._handle_response(response)
        
        if include_content:
            # Get notebook content
            content_url = f"{self._base_url}/api/2.0/workspace/export"
            content_params = {"path": notebook_path, "format": "SOURCE"}
            
            self.logger.debug(f"Getting notebook content: {content_url}")
            content_response = requests.get(content_url, headers=self._get_headers(), params=content_params)
            
            if content_response.status_code == 200:
                content_data = content_response.json()
                
                # Handle different response formats
                if 'content' in content_data:
                    import base64
                    try:
                        decoded_content = base64.b64decode(content_data['content']).decode('utf-8')
                        # For SOURCE format, content is raw Python/SQL code, not JSON
                        notebook_data['content'] = {
                            'nbformat': 4,
                            'nbformat_minor': 2,
                            'metadata': {},
                            'cells': [{
                                'cell_type': 'code',
                                'source': decoded_content.split('\n'),
                                'metadata': {},
                                'outputs': [],
                                'execution_count': None
                            }]
                        }
                        self.logger.debug("Successfully got notebook content")
                    except Exception as e:
                        self.logger.debug(f"Failed to decode content: {e}")
                        notebook_data['content'] = {'cells': []}
                else:
                    self.logger.debug("No content field in response")
                    notebook_data['content'] = {'cells': []}
            else:
                self.logger.debug(f"Failed to get notebook content: Status {content_response.status_code}, Response: {content_response.text}")
                notebook_data['content'] = {'cells': []}
        
        return self._convert_to_notebook_resource(notebook_data)

    def _resolve_notebook_path(self, notebook_name: str) -> Optional[str]:
        """Resolve a notebook name to its full workspace path using the cache"""
        if not hasattr(self, '_notebooks_cache') or not self._notebooks_cache:
            self.logger.debug("Cache not available, trying to initialize...")
            self._initialize_cache()
        
        # Search through cached notebooks
        for notebook in self._notebooks_cache:
            notebook_path = notebook.get('path', '')
            if not notebook_path:
                continue
                
            # Extract the base name from the path
            base_name = notebook_path.split('/')[-1] if '/' in notebook_path else notebook_path
            
            # Check for exact match
            if base_name == notebook_name:
                self.logger.debug(f"Found exact match: '{notebook_name}' -> '{notebook_path}'")
                return notebook_path
            
            # Check for match without extension
            base_name_no_ext = base_name.replace('.ipynb', '').replace('.py', '')
            notebook_name_no_ext = notebook_name.replace('.ipynb', '').replace('.py', '')
            if base_name_no_ext == notebook_name_no_ext:
                self.logger.debug(f"Found match without extension: '{notebook_name}' -> '{notebook_path}'")
                return notebook_path
        
        # Log available notebooks for debugging
        available_notebooks = [nb.get('path', 'no-path') for nb in self._notebooks_cache[:10]]
        self.logger.debug(f"Could not resolve '{notebook_name}'. Available notebooks (first 10): {available_notebooks}")
        
        return None

    def get_notebook_by_path(self, notebook_path: str) -> Optional[Dict[str, Any]]:
        """Get notebook by its workspace path"""
        if not hasattr(self, '_notebooks_cache') or not self._notebooks_cache:
            self._initialize_cache()
        
        for notebook in self._notebooks_cache:
            if notebook.get('path') == notebook_path:
                return notebook
        return None

    def get_notebook_by_name(self, notebook_name: str) -> Optional[Dict[str, Any]]:
        """Get notebook by its base name (searches cache)"""
        resolved_path = self._resolve_notebook_path(notebook_name)
        if resolved_path:
            return self.get_notebook_by_path(resolved_path)
        return None

    def create_or_update_notebook(self, notebook_path: str, notebook_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create or update a notebook"""
        url = f"{self._base_url}/api/2.0/workspace/import"
        
        # Convert notebook content to base64
        import base64
        if isinstance(notebook_data.get('content'), dict):
            content_str = json.dumps(notebook_data['content'])
        else:
            content_str = str(notebook_data.get('content', ''))
        
        content_base64 = base64.b64encode(content_str.encode('utf-8')).decode('utf-8')
        
        payload = {
            "path": notebook_path,
            "format": "JUPYTER",
            "content": content_base64,
            "overwrite": True
        }
        
        try:
            self.logger.debug(f"Creating/updating notebook - URL: {url}")
            self.logger.debug(f"Notebook path: {notebook_path}")
            
            response = requests.post(url, headers=self._get_headers(), json=payload)
            self.logger.debug(f"Response Status: {response.status_code}")
            
            if response.status_code not in [200, 201]:
                self.logger.debug(f"Response Text: {response.text}")
            
            return self._handle_response(response)
        except Exception as e:
            self.logger.debug(f"Exception in create_or_update_notebook: {e}")
            raise
    
    def delete_notebook(self, notebook_path: str) -> None:
        """Delete a notebook"""
        url = f"{self._base_url}/api/2.0/workspace/delete"
        payload = {"path": notebook_path}
        
        response = requests.post(url, headers=self._get_headers(), json=payload)
        self._handle_response(response)
    
    def get_folder_name(self, folder_path: str) -> str:
        """Get folder name from folder path"""
        if not folder_path:
            return ""
        
        # Extract folder name from path
        return folder_path.split('/')[-1] if folder_path else ""
    
    def _convert_to_notebook_resource(self, databricks_data: Dict[str, Any], include_content: bool = True) -> 'NotebookResource':
        """Convert Databricks workspace object to NotebookResource"""
        
        notebook_path = databricks_data.get('path', '')
        notebook_name = notebook_path.split('/')[-1] if notebook_path else ''
        
        notebook_data = {
            'id': databricks_data.get('object_id', notebook_path),
            'name': notebook_name,
            'type': 'Microsoft.Synapse/workspaces/notebooks',  # Keep compatible format
            'etag': str(databricks_data.get('modified_at', '')),
            'properties': {}
        }
        
        # Extract folder information from path
        folder_path = '/'.join(notebook_path.split('/')[:-1]) if '/' in notebook_path else ''
        folder_info = {
            'name': self.get_folder_name(folder_path),
            'path': folder_path
        }
        
        if include_content and 'content' in databricks_data:
            # Parse notebook content
            content = databricks_data['content']
            if isinstance(content, dict):
                notebook_content = content
            else:
                # If content is string, try to parse as JSON
                try:
                    notebook_content = json.loads(content) if isinstance(content, str) else {}
                except json.JSONDecodeError:
                    # Create basic notebook structure
                    notebook_content = {
                        'nbformat': 4,
                        'nbformat_minor': 2,
                        'metadata': {},
                        'cells': [{
                            'cell_type': 'code',
                            'source': content.split('\n') if isinstance(content, str) else [],
                            'metadata': {},
                            'outputs': [],
                            'execution_count': Noneinfo
                        }]
                    }
            
            # Add folder information
            notebook_content['folder'] = folder_info
            notebook_data['properties'] = notebook_content
        else:
            # Even without content, always add folder info
            notebook_data['properties'] = {'folder': folder_info}
        
        return NotebookResource(**notebook_data)

    def _convert_from_notebook_resource(self, notebook: 'NotebookResource') -> Dict[str, Any]:
        """Convert NotebookResource to Databricks format"""
        return {
            "name": notebook.name,
            "content": {
                "cells": [
                    {
                        "cell_type": getattr(cell, 'cell_type', 'code'),
                        "source": getattr(cell, 'source', [])
                    } for cell in getattr(notebook.properties, 'cells', [])
                ]
            }
        }

    def is_interactive_session(self) -> bool:
        """Check if running in interactive Databricks session"""
        try:
            # Check for Databricks environment
            import __main__
            dbutils = getattr(__main__, 'dbutils', None)
            if dbutils:
                return True
                
            # Check for IPython/Jupyter
            if 'get_ipython' not in globals():
                try:
                    from IPython import get_ipython
                except ImportError:
                    return False
            else:
                get_ipython = globals()['get_ipython']
                
            ipython = get_ipython()
            return ipython is not None
        except:
            return False

    def get_workspace_info(self) -> Dict[str, Any]:
        """Get Databricks workspace information"""
        try:
            import __main__
            dbutils = getattr(__main__, 'dbutils', None)
            if dbutils:
                context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
                return {
                    'workspace_id': context.workspaceId().get(),
                    'workspace_url': context.browserHostName().get(),
                    'cluster_id': context.clusterId().get(),
                    'environment': 'databricks'
                }
        except Exception:
            pass
        return {'environment': 'databricks'}

    def get_cluster_info(self) -> Dict[str, Any]:
        """Get Databricks cluster information"""
        try:
            import __main__
            spark = getattr(__main__, 'spark', None)
            if spark:
                cluster_info = {
                    'app_name': spark.sparkContext.appName,
                    'spark_version': spark.version,
                    'master': spark.sparkContext.master
                }
                
                # Try to get cluster ID from dbutils
                dbutils = getattr(__main__, 'dbutils', None)
                if dbutils:
                    try:
                        context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
                        cluster_info['cluster_id'] = context.clusterId().get()
                        cluster_info['cluster_name'] = context.clusterName().get()
                    except:
                        pass
                
                return cluster_info
        except Exception:
            pass
        return {}

        
@PlatformServices.register(
    name="databricks", 
    description="Databricks platform service")
def create_platform_service(config, logger):
    return DatabricksService(config, logger)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
