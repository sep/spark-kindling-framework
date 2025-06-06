# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

"""
Azure Synapse Artifacts SDK
Implementation of the official azure-synapse-artifacts SDK using Fabric REST APIs as backend

This is the actual SDK implementation that provides the official Azure Synapse Artifacts
interface while using Microsoft Fabric APIs for the underlying operations.
"""

import json
import time
import re
import base64
from datetime import datetime
from typing import Dict, List, Optional, Union, Any
from urllib.parse import quote

import requests
from msal import ConfidentialClientApplication

# Core SDK imports
from azure.core.credentials import TokenCredential
from azure.core.exceptions import (
    HttpResponseError, 
    ResourceNotFoundError, 
    ResourceExistsError,
    ClientAuthenticationError
)

# ============================================================================
# MODELS (azure.synapse.artifacts.models)
# ============================================================================

class NotebookMetadata:
    """Metadata for a notebook."""
    
    def __init__(self, **kwargs):
        self.kernelspec = kwargs.get('kernelspec')
        self.language_info = kwargs.get('language_info')
        self.additional_properties = kwargs.get('additional_properties', {})


class NotebookCellMetadata:
    """Metadata for a notebook cell."""
    
    def __init__(self, **kwargs):
        self.additional_properties = kwargs.get('additional_properties', {})


class NotebookCell:
    """Represents a notebook cell."""
    
    def __init__(self, **kwargs):
        self.cell_type = kwargs.get('cell_type', 'code')
        self.source = kwargs.get('source', [])
        self.metadata = NotebookCellMetadata(**kwargs.get('metadata', {}))
        self.outputs = kwargs.get('outputs', [])
        self.execution_count = kwargs.get('execution_count')
        self.additional_properties = kwargs.get('additional_properties', {})


class BigDataPoolReference:
    """Reference to a big data pool."""
    
    def __init__(self, **kwargs):
        self.type = kwargs.get('type', 'BigDataPoolReference')
        self.reference_name = kwargs.get('reference_name')


class NotebookSessionProperties:
    """Session properties for a notebook."""
    
    def __init__(self, **kwargs):
        self.driver_memory = kwargs.get('driver_memory')
        self.driver_cores = kwargs.get('driver_cores')
        self.executor_memory = kwargs.get('executor_memory')
        self.executor_cores = kwargs.get('executor_cores')
        self.num_executors = kwargs.get('num_executors')


class NotebookFolder:
    """Represents a notebook folder."""
    
    def __init__(self, **kwargs):
        self.name = kwargs.get('name', '')
        self.path = kwargs.get('path', '')


class Notebook:
    """Notebook definition."""
    
    def __init__(self, **kwargs):
        self.description = kwargs.get('description')
        self.big_data_pool = kwargs.get('big_data_pool')
        if self.big_data_pool and isinstance(self.big_data_pool, dict):
            self.big_data_pool = BigDataPoolReference(**self.big_data_pool)
        
        self.session_properties = kwargs.get('session_properties')
        if self.session_properties and isinstance(self.session_properties, dict):
            self.session_properties = NotebookSessionProperties(**self.session_properties)
        
        self.metadata = NotebookMetadata(**kwargs.get('metadata', {}))
        self.nbformat = kwargs.get('nbformat', 4)
        self.nbformat_minor = kwargs.get('nbformat_minor', 2)
        
        # Add folder support
        folder_data = kwargs.get('folder', {})
        if isinstance(folder_data, dict):
            self.folder = NotebookFolder(**folder_data)
        else:
            # Default folder if not specified
            self.folder = NotebookFolder(name='', path='')
        
        cells_data = kwargs.get('cells', [])
        self.cells = []
        for cell_data in cells_data:
            if isinstance(cell_data, dict):
                self.cells.append(NotebookCell(**cell_data))
            else:
                self.cells.append(cell_data)
        
        self.additional_properties = kwargs.get('additional_properties', {})


class NotebookResource:
    """Represents a notebook resource."""
    
    def __init__(self, **kwargs):
        self.id = kwargs.get('id')
        self.name = kwargs.get('name')
        self.type = kwargs.get('type', 'Microsoft.Synapse/workspaces/notebooks')
        self.etag = kwargs.get('etag')
        
        properties_data = kwargs.get('properties', {})
        if isinstance(properties_data, dict):
            self.properties = Notebook(**properties_data)
        else:
            self.properties = properties_data
        
        self.additional_properties = kwargs.get('additional_properties', {})


class ArtifactRenameRequest:
    """Request object for renaming artifacts"""
    
    def __init__(self, new_name: str):
        self.new_name = new_name


# ============================================================================
# FABRIC BACKEND IMPLEMENTATION
# ============================================================================

class FabricBackend:
    """Backend implementation using Fabric REST APIs"""
    
    def __init__(self, workspace_id: str, credential: TokenCredential):
        self.workspace_id = self._validate_workspace_id(workspace_id)
        self.credential = credential
        self.base_url = "https://api.fabric.microsoft.com/v1"
        self._token_cache = {}
    
    def _validate_workspace_id(self, workspace_id: str) -> str:
        """Validate and clean workspace ID"""
        # Remove common URL patterns
        if workspace_id.startswith('https://'):
            # Extract from URL like https://app.fabric.microsoft.com/groups/{id}
            if '/groups/' in workspace_id:
                workspace_id = workspace_id.split('/groups/')[-1].split('/')[0]
            else:
                # Remove https:// prefix
                workspace_id = workspace_id.replace('https://', '')
        
        # Clean up workspace ID
        workspace_id = workspace_id.strip()
        
        # Validate GUID format (Fabric workspace IDs are GUIDs)
        guid_pattern = r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
        
        if not re.match(guid_pattern, workspace_id):
            print(f"WARNING: Workspace ID '{workspace_id}' doesn't appear to be a valid GUID format")
            print("Expected format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx")
        
        return workspace_id
    
    def _get_token(self) -> str:
        """Get access token for Fabric API"""
        current_time = time.time()
        
        # Check if we have a valid cached token
        if ('token' in self._token_cache and 
            'expires_at' in self._token_cache and 
            current_time < self._token_cache['expires_at']):
            return self._token_cache['token']
        
        # Get new token
        token_response = self.credential.get_token("https://api.fabric.microsoft.com/.default")
        
        # Cache the token
        self._token_cache = {
            'token': token_response.token,
            'expires_at': token_response.expires_on
        }
        
        return token_response.token
    
    def _get_headers(self) -> Dict[str, str]:
        """Get request headers with authorization"""
        return {
            'Authorization': f'Bearer {self._get_token()}',
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'User-Agent': 'azure-synapse-artifacts/1.0.0'
        }
    
    def _handle_response(self, response: requests.Response) -> Any:
        """Handle HTTP response and convert to appropriate exceptions"""
        if 200 <= response.status_code < 300:
            try:
                return response.json() if response.content else None
            except json.JSONDecodeError:
                return response.text if response.content else None
        
        # Convert HTTP errors to appropriate Azure exceptions
        error_map = {
            401: ClientAuthenticationError,
            404: ResourceNotFoundError,
            409: ResourceExistsError
        }
        
        exception_class = error_map.get(response.status_code, HttpResponseError)
        
        try:
            error_data = response.json()
            message = error_data.get('error', {}).get('message', response.text)
        except:
            message = response.text or f"HTTP {response.status_code}"
        
        raise exception_class(message)
    
    def test_connection(self) -> Dict[str, Any]:
        """Test connection and permissions"""
        url = f"{self.base_url}/workspaces/{self.workspace_id}"
        response = requests.get(url, headers=self._get_headers())
        
        if response.status_code == 200:
            return response.json()
        else:
            return None
    
    def list_notebooks(self) -> List[Dict[str, Any]]:
        """List all notebooks in the workspace"""
        url = f"{self.base_url}/workspaces/{self.workspace_id}/notebooks"
        
        response = requests.get(url, headers=self._get_headers())
        data = self._handle_response(response)
        
        if isinstance(data, dict):
            return data.get('value', [])
        elif isinstance(data, list):
            return data
        else:
            return []
    
    def get_notebook_id_by_name(self, notebook_name: str) -> Optional[str]:
        """Get notebook ID by display name"""
        notebooks = self.list_notebooks()
        for notebook in notebooks:
            if notebook.get('displayName') == notebook_name:
                return notebook.get('id')
        return None
    
    def get_notebook(self, notebook_name: str, include_content: bool = True) -> Dict[str, Any]:
        """Get a specific notebook"""
        # Get notebook ID by name
        notebook_id = self.get_notebook_id_by_name(notebook_name)
        
        if not notebook_id:
            raise ResourceNotFoundError(f"Notebook '{notebook_name}' not found")
        
        # Get basic notebook info first
        url = f"{self.base_url}/workspaces/{self.workspace_id}/notebooks/{notebook_id}"
        response = requests.get(url, headers=self._get_headers())
        notebook_data = self._handle_response(response)
        
        if include_content:
            # Use the correct Fabric API endpoint for getting definition
            definition_url = f"{self.base_url}/workspaces/{self.workspace_id}/items/{notebook_id}/getDefinition"
            definition_url += "?format=ipynb"
            
            print(f"DEBUG: Getting definition via POST: {definition_url}")
            
            # Use POST request as per Fabric API documentation
            definition_response = requests.post(definition_url, headers=self._get_headers())
            print(f"DEBUG: Definition response status: {definition_response.status_code}")
            
            if definition_response.status_code == 200:
                definition_data = definition_response.json()
                notebook_data['definition'] = definition_data.get('definition', definition_data)
                print("DEBUG: Successfully got notebook definition via POST")
            elif definition_response.status_code == 202:
                # Async operation - get the operation ID and poll
                operation_id = definition_response.headers.get('x-ms-operation-id')
                if not operation_id:
                    # Extract from Location header if x-ms-operation-id not present
                    location = definition_response.headers.get('Location')
                    if location:
                        operation_id = location.split('/')[-1]
                
                if operation_id:
                    # Poll until operation completes using existing method signature
                    operation_url = f"{self.base_url}/operations/{operation_id}"
                    
                    # The existing _poll_operation method expects (operation_url, notebook_name=None)
                    # and returns the result data, not just boolean
                    try:
                        print(f"DEBUG: Polling operation: {operation_url}")
                        completed_successfully = self._wait_for_operation(operation_id)
                        if completed_successfully:
                            # Get the operation result using the documented API
                            result_url = f"{self.base_url}/operations/{operation_id}/result"
                            print(f"DEBUG: Getting operation result: {result_url}")
                            
                            result_response = requests.get(result_url, headers=self._get_headers())
                            print(f"DEBUG: Result response status: {result_response.status_code}")
                            
                            if result_response.status_code == 200:
                                definition_data = result_response.json()
                                notebook_data['definition'] = definition_data.get('definition', definition_data)
                                print("DEBUG: Successfully got notebook definition from operation result")
                            else:
                                print(f"DEBUG: Operation result request failed: {result_response.text}")
                        else:
                            print("DEBUG: Operation failed or timed out")
                    except AttributeError as e:
                        print(f"DEBUG: Method not found: {e}")
            else:
                print(f"DEBUG: Definition POST failed: {definition_response.text}")
        
        return notebook_data
    
    def _wait_for_operation(self, operation_id: str) -> bool:
        """Wait for operation to complete"""
        max_attempts = 30
        delay = 1
        operation_url = f"{self.base_url}/operations/{operation_id}"
        
        for attempt in range(max_attempts):
            response = requests.get(operation_url, headers=self._get_headers())
            
            if response.status_code == 200:
                data = response.json()
                status = data.get('status', '').lower()
                
                if status == 'succeeded':
                    return True
                elif status == 'failed':
                    error = data.get('error', {})
                    print(f"DEBUG: Operation failed: {error}")
                    return False
                elif status in ['inprogress', 'running', 'notstarted']:
                    time.sleep(delay)
                    continue
            
            time.sleep(delay)
        
        return False
    
    def _poll_operation_completion(self, operation_url: str) -> bool:
        """Poll operation until completion, return True if succeeded"""
        max_attempts = 30
        delay = 1
        
        for attempt in range(max_attempts):
            response = requests.get(operation_url, headers=self._get_headers())
            
            if response.status_code == 200:
                data = response.json()
                status = data.get('status', '').lower()
                
                if status == 'succeeded':
                    return True
                elif status == 'failed':
                    error = data.get('error', {})
                    print(f"DEBUG: Operation failed: {error}")
                    return False  
                elif status in ['inprogress', 'running', 'notstarted']:
                    time.sleep(delay)
                    continue
                    
            time.sleep(delay)
        
        print("DEBUG: Operation polling timeout")
        return False
    
    def create_or_update_notebook(self, notebook_name: str, notebook_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create or update a notebook"""
        encoded_name = quote(notebook_name, safe='')
        
        url = f"{self.base_url}/workspaces/{self.workspace_id}/notebooks/{encoded_name}"
        
        try:
            print(f"DEBUG: Creating/updating notebook - URL: {url}")
            print(f"DEBUG: Payload keys: {list(notebook_data.keys())}")
            
            # Validate the payload structure
            if 'displayName' not in notebook_data:
                notebook_data['displayName'] = notebook_name
            
            response = requests.post(url, headers=self._get_headers(), json=notebook_data)
            print(f"DEBUG: Response Status: {response.status_code}")
            
            if response.status_code not in [200, 201, 202]:
                print(f"DEBUG: Response Text: {response.text}")
            
            # Handle async operation
            if response.status_code == 202:
                operation_url = response.headers.get('Location')
                if operation_url:
                    print(f"DEBUG: Polling operation at: {operation_url}")
                    return self._poll_operation(operation_url, notebook_name)
            
            return self._handle_response(response)
        except Exception as e:
            print(f"DEBUG: Exception in create_or_update_notebook: {e}")
            raise
    
    def delete_notebook(self, notebook_name: str) -> None:
        """Delete a notebook"""
        encoded_name = quote(notebook_name, safe='')
        url = f"{self.base_url}/workspaces/{self.workspace_id}/notebooks/{encoded_name}"
        response = requests.delete(url, headers=self._get_headers())
        
        if response.status_code == 202:
            operation_url = response.headers.get('Location')
            if operation_url:
                self._poll_operation(operation_url)
                return
        
        self._handle_response(response)
    
    def get_folder_name(self, folder_id: str) -> str:
        """Get folder name from folder ID"""
        if not folder_id:
            return ""
        
        try:
            # Try the folders endpoint first
            url = f"{self.base_url}/workspaces/{self.workspace_id}/folders/{folder_id}"
            response = requests.get(url, headers=self._get_headers())
            
            if response.status_code == 200:
                folder_data = response.json()
                return folder_data.get('displayName', '')
            
            return ""
                
        except Exception as e:
            return ""
    
    def _poll_operation(self, operation_url: str, notebook_name: str = None) -> Any:
        """Poll async operation until completion"""
        max_attempts = 60
        delay = 2
        
        for attempt in range(max_attempts):
            response = requests.get(operation_url, headers=self._get_headers())
            
            if response.status_code == 200:
                data = response.json()
                status = data.get('status', '').lower()
                
                if status == 'succeeded':
                    if notebook_name:
                        return self.get_notebook(notebook_name)
                    return data
                elif status == 'failed':
                    error = data.get('error', {})
                    raise HttpResponseError(f"Operation failed: {error.get('message', 'Unknown error')}")
                elif status in ['inprogress', 'running']:
                    time.sleep(delay)
                    continue
                else:
                    # Unknown status, continue polling
                    time.sleep(delay)
                    continue
            elif response.status_code == 404:
                # Operation completed and cleaned up
                if notebook_name:
                    return self.get_notebook(notebook_name)
                return None
            else:
                # Unexpected response, continue polling for a bit
                if attempt < 5:
                    time.sleep(delay)
                    continue
                else:
                    raise HttpResponseError(f"Operation polling failed: HTTP {response.status_code}")
        
        raise HttpResponseError("Operation polling timeout after 2 minutes")


# ============================================================================
# SYNAPSE NOTEBOOK CLIENT
# ============================================================================

class NotebookOperations:
    """Operations for managing notebooks"""
    
    def __init__(self, client, config, serializer, deserializer):
        self._client = client
        self._config = config
        self._serialize = serializer
        self._deserialize = deserializer
        
        # Extract workspace ID from endpoint
        endpoint = getattr(config, 'endpoint', '')
        if '/workspaces/' in endpoint:
            self.workspace_id = endpoint.split('/workspaces/')[-1].split('/')[0]
        else:
            # Assume the endpoint is the workspace ID
            self.workspace_id = endpoint.replace('https://', '').replace('http://', '')
        
        self.backend = FabricBackend(self.workspace_id, client._credential)
        
        # Add caching for workspace items
        self._items_cache = []
        self._folders_cache = {}
        self._notebooks_cache = []
        self._cache_initialized = False
        
        # Initialize the cache
        self._initialize_cache()
    
    def _initialize_cache(self):
        """Initialize the cache with all workspace items and folders"""
        try:
            print("DEBUG: Initializing workspace cache...")
            
            # Get all items in the workspace using the items endpoint
            url = f"{self.backend.base_url}/workspaces/{self.workspace_id}/items"
            print(f"DEBUG: Fetching items from: {url}")
            response = requests.get(url, headers=self.backend._get_headers())
            
            if response.status_code == 200:
                data = self.backend._handle_response(response)
                
                if isinstance(data, dict):
                    self._items_cache = data.get('value', [])
                elif isinstance(data, list):
                    self._items_cache = data
                else:
                    self._items_cache = []
                
                # Debug: Print all item types found
                item_types = {}
                for item in self._items_cache:
                    item_type = item.get('type', 'Unknown')
                    if item_type not in item_types:
                        item_types[item_type] = 0
                    item_types[item_type] += 1
                
                print(f"DEBUG: Item types found: {item_types}")
                
                # Separate items by type 
                self._notebooks_cache = []
                self._folders_cache = {}
                
                for item in self._items_cache:
                    item_type = item.get('type', '').lower()
                    
                    if item_type == 'notebook':
                        self._notebooks_cache.append(item)
                    elif item_type == 'folder':
                        folder_id = item.get('id')
                        folder_name = item.get('displayName', '')
                        print(f"DEBUG: Found folder from items - ID: {folder_id}, Name: {folder_name}")
                        if folder_id:
                            self._folders_cache[folder_id] = folder_name
                
                # Since /items doesn't return folders, fetch them separately
                print("DEBUG: Items endpoint didn't return folders, fetching from /folders endpoint...")
                folders_url = f"{self.backend.base_url}/workspaces/{self.workspace_id}/folders"
                print(f"DEBUG: Fetching folders from: {folders_url}")
                
                folders_response = requests.get(folders_url, headers=self.backend._get_headers())
                
                if folders_response.status_code == 200:
                    folders_data = self.backend._handle_response(folders_response)
                    
                    if isinstance(folders_data, dict):
                        folders_list = folders_data.get('value', [])
                    elif isinstance(folders_data, list):
                        folders_list = folders_data
                    else:
                        folders_list = []
                    
                    print(f"DEBUG: Found {len(folders_list)} folders from /folders endpoint")
                    
                    for folder in folders_list:
                        folder_id = folder.get('id')
                        folder_name = folder.get('displayName', '')
                        print(f"DEBUG: Found folder from /folders - ID: {folder_id}, Name: {folder_name}")
                        if folder_id:
                            self._folders_cache[folder_id] = folder_name
                else:
                    print(f"DEBUG: Failed to fetch folders. Status: {folders_response.status_code}")
                    print(f"DEBUG: Folders response: {folders_response.text}")
                
                self._cache_initialized = True
                print(f"DEBUG: Cache initialized with {len(self._items_cache)} total items")
                print(f"DEBUG: Found {len(self._notebooks_cache)} notebooks")
                print(f"DEBUG: Found {len(self._folders_cache)} folders")
                
                # Debug: Print folder cache contents
                if self._folders_cache:
                    print(f"DEBUG: Folder cache: {self._folders_cache}")
                
            else:
                print(f"WARNING: Failed to initialize cache. Status: {response.status_code}")
                print(f"DEBUG: Response text: {response.text}")
                self._cache_initialized = False
                
        except Exception as e:
            print(f"WARNING: Failed to initialize cache: {e}")
            import traceback
            traceback.print_exc()
            self._cache_initialized = False
    
    def _refresh_cache(self):
        """Refresh the cache after create/update/delete operations"""
        self._cache_initialized = False
        self._initialize_cache()
    
    def _get_cached_folder_name(self, folder_id: str) -> str:
        """Get folder name from folder ID using cache"""
        if not folder_id:
            return ""
        
        if not self._cache_initialized:
            self._initialize_cache()
        
        return self._folders_cache.get(folder_id, "")
    
    def _get_cached_notebook_id_by_name(self, notebook_name: str) -> Optional[str]:
        """Get notebook ID by display name using cache"""
        if not self._cache_initialized:
            self._initialize_cache()
        
        for notebook in self._notebooks_cache:
            if notebook.get('displayName') == notebook_name:
                return notebook.get('id')
        return None
    
    def _get_cached_notebooks_list(self) -> List[Dict[str, Any]]:
        """Get list of notebooks from cache"""
        if not self._cache_initialized:
            self._initialize_cache()
        
        return self._notebooks_cache.copy()
    
    def diagnose_connection(self) -> Dict[str, Any]:
        """Diagnose connection issues"""
        workspace_info = self.backend.test_connection()
        
        if workspace_info:
            try:
                notebooks = self.backend.list_notebooks()
                return {'status': 'success', 'notebook_count': len(notebooks)}
            except Exception as e:
                return {'status': 'error', 'error': str(e)}
        else:
            return {'status': 'error', 'error': 'Workspace access failed'}
    
    def get_notebook_summary_by_work_space(self, **kwargs) -> List[NotebookResource]:
        """Get notebook summary by workspace"""
        try:
            notebooks_data = self.backend.list_notebooks()
            if notebooks_data is None:
                print("WARNING: list_notebooks returned None")
                return []
            
            result = []
            for nb in notebooks_data:
                try:
                    synapse_resource = self._fabric_to_synapse_resource(nb, include_content=False)
                    if synapse_resource:
                        result.append(synapse_resource)
                except Exception as e:
                    print(f"WARNING: Failed to convert notebook {nb.get('displayName', 'Unknown')}: {e}")
                    continue
            
            return result
        except Exception as e:
            print(f"ERROR in get_notebook_summary_by_work_space: {e}")
            return []
    
    def get_notebooks_by_workspace(self, **kwargs) -> List[NotebookResource]:
        """Get notebooks by workspace"""
        notebooks_data = self.backend.list_notebooks()
        
        result = []
        for nb in notebooks_data:
            try:
                synapse_resource = self._fabric_to_synapse_resource(nb, include_content=False)
                if synapse_resource:
                    result.append(synapse_resource)
            except Exception:
                continue
        
        return result
    
    def list_notebooks_by_workspace(self, **kwargs) -> List[NotebookResource]:
        """List notebooks by workspace"""
        try:
            notebooks_data = self.backend.list_notebooks()
            if notebooks_data is None:
                print("WARNING: list_notebooks returned None")
                return []
            
            result = []
            for nb in notebooks_data:
                try:
                    synapse_resource = self._fabric_to_synapse_resource(nb, include_content=True)
                    if synapse_resource:
                        result.append(synapse_resource)
                except Exception as e:
                    print(f"WARNING: Failed to convert notebook {nb.get('displayName', 'Unknown')}: {e}")
                    continue
            
            return result
        except Exception as e:
            print(f"ERROR in list_notebooks_by_workspace: {e}")
            return []
    
    def get_notebook(self, notebook_name: str, if_none_match: Optional[str] = None, **kwargs) -> NotebookResource:
        """Get a notebook by name"""
        notebook_data = self.backend.get_notebook(notebook_name, include_content=True)
        return self._fabric_to_synapse_resource(notebook_data, include_content=True)
    
    def create_or_update_notebook(self, 
                                 notebook_name: str, 
                                 notebook: NotebookResource,
                                 if_match: Optional[str] = None,
                                 **kwargs) -> NotebookResource:
        """Create or update a notebook"""
        fabric_data = self._synapse_to_fabric_data(notebook)
        result_data = self.backend.create_or_update_notebook(notebook_name, fabric_data)
        # Refresh cache after modification
        self._refresh_cache()
        return self._fabric_to_synapse_resource(result_data, include_content=True)
    
    def delete_notebook(self, notebook_name: str, **kwargs) -> None:
        """Delete a notebook"""
        self.backend.delete_notebook(notebook_name)
        # Refresh cache after modification
        self._refresh_cache()
    
    def begin_create_or_update_notebook(self, 
                                        notebook_name: str, 
                                        notebook: NotebookResource,
                                        if_match: Optional[str] = None,
                                        **kwargs):
        """Begin create or update notebook (async operation)"""
        result = self.create_or_update_notebook(notebook_name, notebook, if_match, **kwargs)
        # Cache is already refreshed in create_or_update_notebook
        return result
    
    def begin_delete_notebook(self, notebook_name: str, **kwargs):
        """Begin delete notebook (async operation)"""
        result = self.delete_notebook(notebook_name, **kwargs)
        # Cache is already refreshed in delete_notebook
        return result
    
    def begin_rename_notebook(self, notebook_name: str, request, **kwargs):
        """Begin rename notebook (async operation)"""
        return self.rename_notebook(notebook_name, request, **kwargs)
    
    def rename_notebook(self, notebook_name: str, request, **kwargs) -> None:
        """Rename a notebook"""
        new_name = getattr(request, 'new_name', None)
        if not new_name:
            raise ValueError("new_name is required for rename operation")
        
        # Get current notebook
        current_notebook = self.get_notebook(notebook_name)
        
        # Create with new name
        current_notebook.name = new_name
        self.create_or_update_notebook(new_name, current_notebook)
        
        # Delete old notebook
        self.delete_notebook(notebook_name)
        # Cache is refreshed in both create_or_update and delete methods
    
    def _fabric_to_synapse_resource(self, fabric_data: Dict[str, Any], include_content: bool = True) -> NotebookResource:
        """Convert Fabric notebook data to Synapse NotebookResource"""
        
        # print(f"fabric_data = {fabric_data}")

        synapse_data = {
            'id': fabric_data.get('id'),
            'name': fabric_data.get('displayName', fabric_data.get('name')),
            'type': 'Microsoft.Synapse/workspaces/notebooks',
            'etag': fabric_data.get('etag'),
            'properties': {}
        }
        
        # Use cached folder lookup instead of API call
        folder_id = fabric_data.get('folderId')
        if folder_id:
            folder_name = self._get_cached_folder_name(folder_id)  # Use cache instead of backend.get_folder_name
            folder_info = {
                'name': folder_name,
                'path': folder_name  # For now, use folder name as path
            }
        else:
            # No folder - notebook is at root level
            folder_info = {
                'name': '',
                'path': ''
            }
        
        if include_content and 'definition' in fabric_data:
            # Parse notebook content from Fabric definition
            definition = fabric_data['definition']
            notebook_content = self._extract_notebook_content_from_fabric(definition)
            # Add folder information
            notebook_content['folder'] = folder_info
            synapse_data['properties'] = notebook_content
        else:
            # Even without content, always add folder info
            synapse_data['properties'] = {'folder': folder_info}
        
        return NotebookResource(**synapse_data)
    
    def _extract_notebook_content_from_fabric(self, definition: Dict[str, Any]) -> Dict[str, Any]:
        """Extract notebook content from Fabric definition"""
        content = {
            'nbformat': 4,
            'nbformat_minor': 2,
            'metadata': {},
            'cells': []
        }
        
        definition_keys = list(definition.keys()) if definition else 'None'
        print(f"DEBUG: Definition keys: {definition_keys}")
        
        if 'parts' in definition:
            parts_count = len(definition['parts'])
            print(f"DEBUG: Found {parts_count} parts")
            
            for i, part in enumerate(definition['parts']):
                part_path = part.get('path')
                part_payload_type = part.get('payloadType')
                part_payload_length = len(part.get('payload', ''))
                print(f"DEBUG: Part {i}: path='{part_path}', payloadType='{part_payload_type}', payload_length={part_payload_length}")
                
                if part.get('path') in ['notebook-content.ipynb', 'notebook-content.py']:
                    payload = part.get('payload', '')
                    payload_type = part.get('payloadType', '')
                    
                    print(f"DEBUG: Processing payload of type '{payload_type}', length {len(payload)}")
                    payload_preview = payload[:200]
                    #print(f"DEBUG: Payload preview: {payload_preview}...")
                    
                    try:
                        # Handle base64 encoded payloads
                        if payload_type == 'InlineBase64':
                            import base64
                            decoded_payload = base64.b64decode(payload).decode('utf-8')
                            decoded_length = len(decoded_payload)
                            print(f"DEBUG: Decoded payload length: {decoded_length}")
                            decoded_preview = decoded_payload[:200]
                            #print(f"DEBUG: Decoded preview: {decoded_preview}...")
                        else:
                            decoded_payload = payload
                        
                        # Try to parse as JSON (Jupyter format)
                        if decoded_payload.strip():
                            parsed_content = json.loads(decoded_payload)
                            if isinstance(parsed_content, dict):
                                content.update(parsed_content)
                                cells_count = len(content.get('cells', []))
                                print(f"DEBUG: Successfully parsed notebook with {cells_count} cells")
                        
                    except (json.JSONDecodeError, Exception) as e:
                        print(f"DEBUG: Failed to parse as JSON: {e}")
                        # Handle as plain text/Python code
                        payload_text = decoded_payload if payload_type == 'InlineBase64' else payload
                        lines_count = len(payload_text.split('\n') if payload_text else [])
                        content['cells'] = [{
                            'cell_type': 'code',
                            'source': payload_text.split('\n') if payload_text else [],
                            'metadata': {},
                            'outputs': [],
                            'execution_count': None
                        }]
                        print(f"DEBUG: Created single code cell with {lines_count} lines")
        else:
            print("DEBUG: No 'parts' found in definition")
        
        return content
    
    def _synapse_to_fabric_data(self, notebook: NotebookResource) -> Dict[str, Any]:
        """Convert Synapse NotebookResource to Fabric data format"""
        import base64
        
        # Build Jupyter notebook content
        notebook_content = {
            'nbformat': getattr(notebook.properties, 'nbformat', 4),
            'nbformat_minor': getattr(notebook.properties, 'nbformat_minor', 2),
            'metadata': self._metadata_to_dict(getattr(notebook.properties, 'metadata', None)),
            'cells': []
        }
        
        # Convert cells
        cells = getattr(notebook.properties, 'cells', [])
        for cell in cells:
            cell_dict = {
                'cell_type': getattr(cell, 'cell_type', 'code'),
                'source': getattr(cell, 'source', []),
                'metadata': self._metadata_to_dict(getattr(cell, 'metadata', None))
            }
            
            if cell_dict['cell_type'] == 'code':
                cell_dict['outputs'] = getattr(cell, 'outputs', [])
                cell_dict['execution_count'] = getattr(cell, 'execution_count', None)
            
            notebook_content['cells'].append(cell_dict)
        
        # Convert to JSON string and base64 encode
        json_content = json.dumps(notebook_content)
        base64_content = base64.b64encode(json_content.encode('utf-8')).decode('utf-8')
        
        # Create Fabric definition
        fabric_data = {
            'displayName': notebook.name,
            'type': 'Notebook',
            'definition': {
                'format': 'ipynb',
                'parts': [
                    {
                        'path': 'notebook-content.ipynb',
                        'payload': base64_content,
                        'payloadType': 'InlineBase64'
                    }
                ]
            }
        }
        
        # Add description if present
        description = getattr(notebook.properties, 'description', None)
        if description:
            fabric_data['description'] = description
        
        return fabric_data
    
    def _metadata_to_dict(self, metadata) -> Dict[str, Any]:
        """Convert metadata object to dictionary"""
        if metadata is None:
            return {}
        elif isinstance(metadata, dict):
            return metadata
        elif hasattr(metadata, '__dict__'):
            result = {}
            for key, value in metadata.__dict__.items():
                if not key.startswith('_'):
                    result[key] = value
            return result
        else:
            return {}


# ============================================================================
# MAIN ARTIFACTS CLIENT
# ============================================================================

class ArtifactsClientConfiguration:
    """Configuration for ArtifactsClient"""
    
    def __init__(self, endpoint: str, **kwargs):
        self.endpoint = endpoint
        self.api_version = kwargs.get('api_version', '2020-12-01')


class ArtifactsClient:
    """
    Azure Synapse Analytics Artifacts Client
    
    This is the main client for Azure Synapse Analytics artifacts, implemented
    using Microsoft Fabric REST APIs as the backend.
    """
    
    def __init__(self, endpoint: str, credential: TokenCredential, **kwargs):
        """
        Initialize the Artifacts Client
        
        Args:
            endpoint: The Synapse workspace endpoint URL or Fabric workspace ID
            credential: Azure credential for authentication
            **kwargs: Additional configuration options
        """
        self._credential = credential
        self._config = ArtifactsClientConfiguration(endpoint, **kwargs)
        
        # Initialize operations
        self.notebook = NotebookOperations(
            client=self,
            config=self._config,
            serializer=self._serialize,
            deserializer=self._deserialize
        )
    
    def _serialize(self, obj, data_type):
        """Serialize object (placeholder for actual serialization)"""
        return obj
    
    def _deserialize(self, data, data_type):
        """Deserialize data (placeholder for actual deserialization)"""  
        return data
    
    def close(self):
        """Close the client"""
        pass
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        self.close()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
