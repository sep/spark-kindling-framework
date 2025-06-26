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
from datetime import datetime, timedelta
from azure.core.exceptions import *

notebook_import(".notebook_framework")
notebook_import(".injection")

@GlobalInjector.singleton_autobind()
class FabricService(EnvironmentService):
    def __init__(self, logger, config):
        self.config = types.SimpleNamespace(**config)
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
            
            # Get all items in the workspace using the items endpoint
            url = f"{self._base_url}/workspaces/{self.config.workspace_id}/items"
            self.logger.debug(f"Fetching items from: {url}")
            response = requests.get(url, headers=self._get_headers())
            
            if response.status_code == 200:
                data = self._handle_response(response)
                
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
                
                self.logger.debug(f"Item types found: {item_types}")
                
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
                        self.logger.debug(f"Found folder from items - ID: {folder_id}, Name: {folder_name}")
                        if folder_id:
                            self._folders_cache[folder_id] = folder_name
                
                # Since /items doesn't return folders, fetch them separately
                self.logger.debug("Items endpoint didn't return folders, fetching from /folders endpoint...")
                folders_url = f"{self._base_url}/workspaces/{self.config.workspace_id}/folders"
                self.logger.debug(f"Fetching folders from: {folders_url}")
                
                folders_response = requests.get(folders_url, headers=self._get_headers())
                
                if folders_response.status_code == 200:
                    folders_data = self._handle_response(folders_response)
                    
                    if isinstance(folders_data, dict):
                        folders_list = folders_data.get('value', [])
                    elif isinstance(folders_data, list):
                        folders_list = folders_data
                    else:
                        folders_list = []
                    
                    self.logger.debug(f"Found {len(folders_list)} folders from /folders endpoint")
                    
                    for folder in folders_list:
                        folder_id = folder.get('id')
                        folder_name = folder.get('displayName', '')
                        self.logger.debug(f"Found folder from /folders - ID: {folder_id}, Name: {folder_name}")
                        if folder_id:
                            self._folders_cache[folder_id] = folder_name
                else:
                    self.logger.debug(f"Failed to fetch folders. Status: {folders_response.status_code}")
                    self.logger.debug(f"Folders response: {folders_response.text}")
                
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
    

    def get_token(self, audience: str) -> str:
        import __main__
        mssparkutils = getattr(__main__, 'mssparkutils', None)
        if not mssparkutils:
            raise Exception("mssparkutils not available in global scope")
        
        return mssparkutils.credentials.getToken(audience)

    def exists(self, path: str) -> bool:
        import __main__
        mssparkutils = getattr(__main__, 'mssparkutils', None)
        return mssparkutils.fs.exists(path)

    def copy(self, source: str, destination: str, overwrite: bool = False) -> None:
        import __main__
        mssparkutils = getattr(__main__, 'mssparkutils', None)
        mssparkutils.fs.cp(source, destination, overwrite)

    def read(self, path: str, encoding: str = 'utf-8') -> Union[str, bytes]:
        with open(path, 'r' if encoding else 'rb') as f:
            return f.read()

    def write(self, path: str, content: Union[str, bytes], overwrite: bool = False) -> None:
        mode = 'w' if isinstance(content, str) else 'wb'
        with open(path, mode) as f:
            f.write(content)

    def list(self, path: str) -> List[str]:
        import __main__
        mssparkutils = getattr(__main__, 'mssparkutils', None)
        files = mssparkutils.fs.ls(path)
        return [f.name for f in files]

    def _build_base_url(self) -> str:
        if self.config.workspace_id:
            return f"https://api.fabric.microsoft.com/v1/"
        elif self.config.endpoint:
            return self.config.endpoint
        else:
            raise Exception("No workspace_id or endpoint provided")

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
            self.logger.warning(f"Workspace ID '{workspace_id}' doesn't appear to be a valid GUID format")
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
            'token': token_response,
            'expires_at': (datetime.now() + timedelta(hours=24)).timestamp()
        }
        
        return token_response
    
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
    
    def list_notebooks(self) -> List[Dict[str, Any]]:
        """List all notebooks in the workspace"""
        url = f"{self._base_url}/workspaces/{self.config.workspace_id}/notebooks"
        
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
    
    def get_notebooks(self):
        return [self._convert_to_notebook_resource(d) for d in self.list_notebooks()] 

    def get_notebook(self, notebook_name: str, include_content: bool = True):
        """Get a specific notebook"""
        # Get notebook ID by name
        notebook_id = self.get_notebook_id_by_name(notebook_name)
        
        if not notebook_id:
            raise ResourceNotFoundError(f"Notebook '{notebook_name}' not found")
        
        # Get basic notebook info first
        url = f"{self._base_url}/workspaces/{self.config.workspace_id}/notebooks/{notebook_id}"
        response = requests.get(url, headers=self._get_headers())
        notebook_data = self._handle_response(response)
        
        if include_content:
            # Use the correct Fabric API endpoint for getting definition
            definition_url = f"{self._base_url}/workspaces/{self.config.workspace_id}/items/{notebook_id}/getDefinition"
            definition_url += "?format=ipynb"
            
            self.logger.debug(f"Getting definition via POST: {definition_url}")
            
            # Use POST request as per Fabric API documentation
            definition_response = requests.post(definition_url, headers=self._get_headers())
            self.logger.debug(f"Definition response status: {definition_response.status_code}")
            
            if definition_response.status_code == 200:
                definition_data = definition_response.json()
                notebook_data['definition'] = definition_data.get('definition', definition_data)
                self.logger.debug("Successfully got notebook definition via POST")
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
                    operation_url = f"{self._base_url}/operations/{operation_id}"
                    
                    # The existing _poll_operation method expects (operation_url, notebook_name=None)
                    # and returns the result data, not just boolean
                    try:
                        self.logger.debug(f"Polling operation: {operation_url}")
                        completed_successfully = self._wait_for_operation(operation_id)
                        if completed_successfully:
                            # Get the operation result using the documented API
                            result_url = f"{self._base_url}/operations/{operation_id}/result"
                            self.logger.debug(f"Getting operation result: {result_url}")
                            
                            result_response = requests.get(result_url, headers=self._get_headers())
                            self.logger.debug(f"Result response status: {result_response.status_code}")
                            
                            if result_response.status_code == 200:
                                definition_data = result_response.json()
                                notebook_data['definition'] = definition_data.get('definition', definition_data)
                                self.logger.debug(f"Successfully got notebook definition from operation result")

                            else:
                                self.logger.debug(f"Operation result request failed: {result_response.text}")
                        else:
                            self.logger.debug("Operation failed or timed out")
                    except AttributeError as e:
                        self.logger.debug(f"Method not found: {e}")
            else:
                self.logger.debug(f"Definition POST failed: {definition_response.text}")
        
        return self._convert_to_notebook_resource(notebook_data)
    
    def _wait_for_operation(self, operation_id: str) -> bool:
        """Wait for operation to complete"""
        max_attempts = 30
        delay = 1
        operation_url = f"{self._base_url}/operations/{operation_id}"
        
        for attempt in range(max_attempts):
            response = requests.get(operation_url, headers=self._get_headers())
            
            if response.status_code == 200:
                data = response.json()
                status = data.get('status', '').lower()
                
                if status == 'succeeded':
                    return True
                elif status == 'failed':
                    error = data.get('error', {})
                    self.logger.debug(f"Operation failed: {error}")
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
                    self.logger.debug(f"Operation failed: {error}")
                    return False  
                elif status in ['inprogress', 'running', 'notstarted']:
                    time.sleep(delay)
                    continue
                    
            time.sleep(delay)
        
        self.logger.debug("Operation polling timeout")
        return False
    
    def create_or_update_notebook(self, notebook_name: str, notebook_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create or update a notebook"""
        encoded_name = quote(notebook_name, safe='')
        
        url = f"{self._base_url}/workspaces/{self.config.workspace_id}/notebooks/{encoded_name}"
        
        try:
            self.logger.debug(f"Creating/updating notebook - URL: {url}")
            self.logger.debug(f"Payload keys: {list(notebook_data.keys())}")
            
            # Validate the payload structure
            if 'displayName' not in notebook_data:
                notebook_data['displayName'] = notebook_name
            
            response = requests.post(url, headers=self._get_headers(), json=notebook_data)
            self.logger.debug(f"Response Status: {response.status_code}")
            
            if response.status_code not in [200, 201, 202]:
                self.logger.debug(f"Response Text: {response.text}")
            
            # Handle async operation
            if response.status_code == 202:
                operation_url = response.headers.get('Location')
                if operation_url:
                    self.logger.debug(f"Polling operation at: {operation_url}")
                    return self._poll_operation(operation_url, notebook_name)
            
            return self._handle_response(response)
        except Exception as e:
            self.logger.debug(f"Exception in create_or_update_notebook: {e}")
            raise
    
    def delete_notebook(self, notebook_name: str) -> None:
        """Delete a notebook"""
        encoded_name = quote(notebook_name, safe='')
        url = f"{self._base_url}/workspaces/{self.config.workspace_id}/notebooks/{encoded_name}"
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
            url = f"{self._base_url}/workspaces/{self.config.workspace_id}/folders/{folder_id}"
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


    def _convert_to_notebook_resource(self, fabric_data: Dict[str, Any], include_content: bool = True) -> NotebookResource:
        """Convert Fabric item to NotebookResource with folder path logic from original"""
        
        notebook_data = {
            'id': fabric_data.get('id'),
            'name': fabric_data.get('displayName', fabric_data.get('name')),
            'type': 'Microsoft.Synapse/workspaces/notebooks',
            'etag': fabric_data.get('etag'),
            'properties': {}
        }
        
        # Use cached folder lookup instead of API call
        folder_id = fabric_data.get('folderId')
        if folder_id:
            folder_name = self._folders_cache[folder_id]  # Use cache instead of backend.get_folder_name
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
            notebook_data['properties'] = notebook_content
        else:
            # Even without content, always add folder info
            notebook_data['properties'] = {'folder': folder_info}
        
        return NotebookResource(**notebook_data)

    def _extract_notebook_content_from_fabric(self, definition: Dict[str, Any]) -> Dict[str, Any]:
        """Extract notebook content from Fabric definition"""
        content = {
            'nbformat': 4,
            'nbformat_minor': 2,
            'metadata': {},
            'cells': []
        }
        
        definition_keys = list(definition.keys()) if definition else 'None'
        self.logger.debug(f"Definition keys: {definition_keys}")
        
        if 'parts' in definition:
            parts_count = len(definition['parts'])
            self.logger.debug(f"Found {parts_count} parts")
            
            for i, part in enumerate(definition['parts']):
                part_path = part.get('path')
                part_payload_type = part.get('payloadType')
                part_payload_length = len(part.get('payload', ''))
                self.logger.debug(f"Part {i}: path='{part_path}', payloadType='{part_payload_type}', payload_length={part_payload_length}")
                
                if part.get('path') in ['notebook-content.ipynb', 'notebook-content.py']:
                    payload = part.get('payload', '')
                    payload_type = part.get('payloadType', '')
                    
                    self.logger.debug(f"Processing payload of type '{payload_type}', length {len(payload)}")
                    payload_preview = payload[:200]
                    #logger.debug(f"Payload preview: {payload_preview}...")
                    
                    try:
                        # Handle base64 encoded payloads
                        if payload_type == 'InlineBase64':
                            import base64
                            decoded_payload = base64.b64decode(payload).decode('utf-8')
                            decoded_length = len(decoded_payload)
                            self.logger.debug(f"Decoded payload length: {decoded_length}")
                            decoded_preview = decoded_payload[:200]
                            #logger.debug(f"Decoded preview: {decoded_preview}...")
                        else:
                            decoded_payload = payload
                        
                        # Try to parse as JSON (Jupyter format)
                        if decoded_payload.strip():
                            parsed_content = json.loads(decoded_payload)
                            if isinstance(parsed_content, dict):
                                content.update(parsed_content)
                                cells_count = len(content.get('cells', []))
                                self.logger.debug(f"Successfully parsed notebook with {cells_count} cells")
                        
                    except (json.JSONDecodeError, Exception) as e:
                        self.logger.debug(f"Failed to parse as JSON: {e}")
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
                        self.logger.debug(f"Created single code cell with {lines_count} lines")
        else:
            self.logger.debug("No 'parts' found in definition")
        
        return content    

    def _convert_from_notebook_resource(self, notebook: NotebookResource) -> Dict[str, Any]:
        # Extract just the display name (remove folder path)
        display_name = notebook.name
        if "/" in display_name:
            display_name = display_name.split("/")[-1]
        
        return {
            "displayName": display_name,
            "type": "Notebook",
            "definition": {
                "parts": [{
                    "path": "notebook-content.py",
                    "payload": {
                        "cells": [
                            {
                                "cell_type": cell.cell_type,
                                "source": cell.source
                            } for cell in notebook.properties.cells
                        ]
                    }
                }]
            }
        }

    def is_interactive_session(self) -> bool:
        if 'get_ipython' not in globals():
            try:
                from IPython import get_ipython
            except ImportError:
                return False
        else:
            get_ipython = globals()['get_ipython']
            
        ipython = get_ipython()
        if ipython is None:
            return False
            
        try:
            connection_file = ipython.config.get('IPKernelApp', {}).get('connection_file')
            return connection_file is not None
        except:
            return False

    def get_workspace_info(self) -> Dict[str, Any]:
        try:
            import __main__
            mssparkutils = getattr(__main__, 'mssparkutils', None)
            if mssparkutils:
                return {
                    'workspace_id': getattr(mssparkutils.env, 'getWorkspaceId', lambda: 'unknown')(),
                    'environment': 'fabric'
                }
        except Exception:
            pass
        return {'environment': 'fabric'}

    def get_cluster_info(self) -> Dict[str, Any]:
        try:
            import __main__
            spark = getattr(__main__, 'spark', None)
            if spark:
                return {
                    'app_name': spark.sparkContext.appName,
                    'spark_version': spark.version,
                    'master': spark.sparkContext.master
                }
        except Exception:
            pass
        return {}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
