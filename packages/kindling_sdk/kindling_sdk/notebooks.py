"""Workspace notebook operations for Databricks, Synapse, and Fabric.

Design-time REST clients behind the `kindling notebook` CLI commands, also
usable programmatically. All clients speak Jupyter-shaped cell dicts and
whole Jupyter documents; pair with ``kindling.notebook_source`` to convert
cells to and from Databricks-style python source files.

Operations raise :class:`NotebookOperationError` on failure so callers can
present errors without depending on ``requests`` response objects.
"""

from __future__ import annotations

import base64
import json
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

import requests

from .platform_provider import (
    azure_synapse_dev_endpoint_suffix,
    azure_token_scope,
    create_azure_credential,
    fabric_api_base_url,
)

#: AAD resource ID for Azure Databricks (fixed across tenants).
DATABRICKS_TOKEN_SCOPE = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default"

DEFAULT_DATABRICKS_FOLDER = "/Shared/kindling"


class NotebookOperationError(RuntimeError):
    """A workspace notebook operation failed."""


class WorkspaceNotebookClient(ABC):
    """Platform-neutral notebook operations against one workspace."""

    @abstractmethod
    def list_notebooks(self) -> List[str]:
        """Return notebook names, sorted."""

    @abstractmethod
    def get_notebook_cells(self, name: str) -> List[Dict[str, Any]]:
        """Return one notebook's cells in Jupyter shape."""

    @abstractmethod
    def import_notebook(self, name: str, notebook_data: Dict[str, Any]) -> None:
        """Create or replace a notebook from a Jupyter document dict."""

    @abstractmethod
    def delete_notebook(self, name: str) -> None:
        """Delete a notebook. Missing notebooks are not an error."""


class DatabricksNotebookClient(WorkspaceNotebookClient):
    def __init__(
        self,
        workspace_url: str,
        folder: str = DEFAULT_DATABRICKS_FOLDER,
        credential: Optional[Any] = None,
    ):
        self.workspace_url = workspace_url.rstrip("/")
        self.folder = (folder or DEFAULT_DATABRICKS_FOLDER).rstrip("/")
        self._credential = create_azure_credential(
            credential=credential, additionally_allowed_tenants=["*"]
        )

    def _headers(self) -> Dict[str, str]:
        token = self._credential.get_token(DATABRICKS_TOKEN_SCOPE).token
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    def _notebook_path(self, name: str) -> str:
        return f"{self.folder}/{name}"

    def list_notebooks(self) -> List[str]:
        resp = requests.get(
            f"{self.workspace_url}/api/2.0/workspace/list",
            headers=self._headers(),
            params={"path": self.folder},
        )
        if resp.status_code == 404:
            return []
        if resp.status_code != 200:
            raise NotebookOperationError(
                f"Databricks workspace list failed ({resp.status_code}): {resp.text}"
            )
        objects = resp.json().get("objects", [])
        return sorted(
            obj["path"].rsplit("/", 1)[-1]
            for obj in objects
            if obj.get("object_type") == "NOTEBOOK"
        )

    def get_notebook_cells(self, name: str) -> List[Dict[str, Any]]:
        nb_path = self._notebook_path(name)
        resp = requests.get(
            f"{self.workspace_url}/api/2.0/workspace/export",
            headers=self._headers(),
            params={"path": nb_path, "format": "JUPYTER"},
        )
        if resp.status_code != 200:
            raise NotebookOperationError(
                f"Databricks export failed for {nb_path} ({resp.status_code}): {resp.text}"
            )
        content = base64.b64decode(resp.json()["content"]).decode("utf-8")
        return json.loads(content).get("cells", [])

    def import_notebook(self, name: str, notebook_data: Dict[str, Any]) -> None:
        headers = self._headers()

        # Import does not create parent folders; mkdirs is idempotent.
        mkdirs_resp = requests.post(
            f"{self.workspace_url}/api/2.0/workspace/mkdirs",
            headers=headers,
            json={"path": self.folder},
        )
        if mkdirs_resp.status_code != 200:
            raise NotebookOperationError(
                f"Databricks mkdirs failed for {self.folder} "
                f"({mkdirs_resp.status_code}): {mkdirs_resp.text}"
            )

        content_b64 = base64.b64encode(json.dumps(notebook_data).encode()).decode()
        resp = requests.post(
            f"{self.workspace_url}/api/2.0/workspace/import",
            headers=headers,
            json={
                "path": self._notebook_path(name),
                "format": "JUPYTER",
                "content": content_b64,
                "overwrite": True,
            },
        )
        if resp.status_code not in (200, 201):
            raise NotebookOperationError(
                f"Databricks import failed ({resp.status_code}): {resp.text}"
            )

    def delete_notebook(self, name: str) -> None:
        resp = requests.post(
            f"{self.workspace_url}/api/2.0/workspace/delete",
            headers=self._headers(),
            json={"path": self._notebook_path(name)},
        )
        if resp.status_code not in (200, 404):
            raise NotebookOperationError(
                f"Databricks delete failed ({resp.status_code}): {resp.text}"
            )


class SynapseNotebookClient(WorkspaceNotebookClient):
    _API_VERSION = "2020-12-01"

    def __init__(self, workspace_name: str, credential: Optional[Any] = None):
        self.workspace_name = workspace_name
        synapse_suffix = azure_synapse_dev_endpoint_suffix()
        self.base_url = f"https://{workspace_name}.{synapse_suffix.strip().lstrip('.')}"
        self._credential = create_azure_credential(
            credential=credential, additionally_allowed_tenants=["*"]
        )

    def _headers(self) -> Dict[str, str]:
        scope = azure_token_scope(
            "AZURE_SYNAPSE_TOKEN_SCOPE", "https://dev.azuresynapse.net/.default"
        )
        token = self._credential.get_token(scope).token
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    def list_notebooks(self) -> List[str]:
        headers = self._headers()
        names: List[str] = []
        url = f"{self.base_url}/notebooks?api-version={self._API_VERSION}"
        while url:
            resp = requests.get(url, headers=headers)
            if resp.status_code != 200:
                raise NotebookOperationError(
                    f"Synapse notebook list failed ({resp.status_code}): {resp.text}"
                )
            payload = resp.json()
            names.extend(item["name"] for item in payload.get("value", []))
            url = payload.get("nextLink")
        return sorted(names)

    def get_notebook_cells(self, name: str) -> List[Dict[str, Any]]:
        resp = requests.get(
            f"{self.base_url}/notebooks/{name}?api-version={self._API_VERSION}",
            headers=self._headers(),
        )
        if resp.status_code != 200:
            raise NotebookOperationError(
                f"Synapse notebook get failed for '{name}' ({resp.status_code}): {resp.text}"
            )
        return resp.json().get("properties", {}).get("cells", [])

    def import_notebook(self, name: str, notebook_data: Dict[str, Any]) -> None:
        headers = self._headers()
        nb_resource = {
            "name": name,
            "properties": {
                "nbformat": notebook_data.get("nbformat", 4),
                "nbformat_minor": notebook_data.get("nbformat_minor", 5),
                "metadata": notebook_data.get("metadata", {}),
                "cells": notebook_data.get("cells", []),
            },
        }
        resp = requests.put(
            f"{self.base_url}/notebooks/{name}?api-version={self._API_VERSION}",
            headers=headers,
            json=nb_resource,
        )
        if resp.status_code not in (200, 201, 202):
            raise NotebookOperationError(
                f"Synapse notebook create failed ({resp.status_code}): {resp.text}"
            )
        if resp.status_code == 202:
            location = resp.headers.get("Location", "")
            for _ in range(30):
                time.sleep(2)
                poll = requests.get(location, headers=headers)
                if poll.status_code == 200:
                    return
            raise NotebookOperationError(
                f"Synapse notebook '{name}' was accepted but did not finish provisioning."
            )

    def delete_notebook(self, name: str) -> None:
        resp = requests.delete(
            f"{self.base_url}/notebooks/{name}?api-version={self._API_VERSION}",
            headers=self._headers(),
        )
        if resp.status_code not in (200, 202, 204, 404):
            raise NotebookOperationError(f"Synapse delete failed ({resp.status_code}): {resp.text}")


class FabricNotebookClient(WorkspaceNotebookClient):
    def __init__(self, workspace_id: str, credential: Optional[Any] = None):
        self.workspace_id = workspace_id
        self.base_url = fabric_api_base_url()
        self._credential = create_azure_credential(
            credential=credential, additionally_allowed_tenants=["*"]
        )

    def _headers(self) -> Dict[str, str]:
        scope = azure_token_scope("FABRIC_TOKEN_SCOPE", "https://api.fabric.microsoft.com/.default")
        token = self._credential.get_token(scope).token
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    def _items(self, headers: Dict[str, str]) -> List[Dict[str, Any]]:
        """List notebook items (id + displayName), following pagination."""
        items: List[Dict[str, Any]] = []
        url = f"{self.base_url}/workspaces/{self.workspace_id}/notebooks"
        while url:
            resp = requests.get(url, headers=headers)
            if resp.status_code != 200:
                raise NotebookOperationError(
                    f"Fabric notebook list failed ({resp.status_code}): {resp.text}"
                )
            payload = resp.json()
            items.extend(payload.get("value", []))
            url = payload.get("continuationUri")
        return items

    def _find_item(self, name: str, headers: Dict[str, str]) -> Optional[Dict[str, Any]]:
        return next(
            (item for item in self._items(headers) if item.get("displayName") == name),
            None,
        )

    def _poll_operation(self, headers: Dict[str, str], location: str, what: str) -> None:
        """Poll a Fabric long-running operation to its terminal status.

        A 200 poll response is not success: the body carries Succeeded /
        Failed / Running. Treating the first 200 as done reports
        asynchronously failed operations as successful.
        """
        if not location:
            raise NotebookOperationError(f"Fabric accepted {what} but returned no operation URL.")
        for _ in range(60):
            time.sleep(2)
            poll = requests.get(location, headers=headers)
            if poll.status_code == 404:
                return  # operation completed and was cleaned up
            if poll.status_code != 200:
                continue
            status = str((poll.json() or {}).get("status", "")).lower()
            if status == "succeeded":
                return
            if status == "failed":
                error = (poll.json() or {}).get("error", {})
                raise NotebookOperationError(f"Fabric operation failed for {what}: {error}")
        raise NotebookOperationError(f"Fabric operation timed out for {what}.")

    @staticmethod
    def _ipynb_definition(notebook_data: Dict[str, Any]) -> Dict[str, Any]:
        notebook_json = json.dumps(notebook_data).encode("utf-8")
        return {
            # Without an explicit format Fabric assumes its native source
            # part layout and the create operation fails asynchronously.
            "format": "ipynb",
            "parts": [
                {
                    "path": "notebook-content.ipynb",
                    "payload": base64.b64encode(notebook_json).decode("utf-8"),
                    "payloadType": "InlineBase64",
                }
            ],
        }

    def list_notebooks(self) -> List[str]:
        return sorted(
            item.get("displayName", "")
            for item in self._items(self._headers())
            if item.get("displayName")
        )

    def get_notebook_cells(self, name: str) -> List[Dict[str, Any]]:
        headers = self._headers()

        # The item listing is eventually consistent — a just-pushed notebook
        # can take a few seconds to appear, so retry the lookup briefly.
        match = None
        for attempt in range(4):
            if attempt:
                time.sleep(3)
            match = self._find_item(name, headers)
            if match is not None:
                break
        if match is None:
            raise NotebookOperationError(
                f"Notebook '{name}' not found in workspace {self.workspace_id}."
            )

        resp = requests.post(
            f"{self.base_url}/workspaces/{self.workspace_id}/notebooks/{match['id']}"
            "/getDefinition?format=ipynb",
            headers=headers,
        )
        if resp.status_code == 202:
            location = resp.headers.get("Location", "")
            self._poll_operation(headers, location, f"definition export of '{name}'")
            resp = requests.get(f"{location.rstrip('/')}/result", headers=headers)
            if resp.status_code != 200:
                raise NotebookOperationError(
                    f"Fabric definition result fetch failed for '{name}' "
                    f"({resp.status_code}): {resp.text}"
                )
        elif resp.status_code != 200:
            raise NotebookOperationError(
                f"Fabric definition export failed for '{name}' ({resp.status_code}): {resp.text}"
            )

        parts = resp.json().get("definition", {}).get("parts", [])
        for part in parts:
            if part.get("path", "").endswith(".ipynb"):
                content = base64.b64decode(part["payload"]).decode("utf-8")
                return json.loads(content).get("cells", [])
        raise NotebookOperationError(f"Fabric notebook '{name}' has no ipynb definition part.")

    def import_notebook(self, name: str, notebook_data: Dict[str, Any]) -> None:
        headers = self._headers()
        definition = self._ipynb_definition(notebook_data)

        def _update(item: Dict[str, Any]):
            return requests.post(
                f"{self.base_url}/workspaces/{self.workspace_id}/notebooks/{item['id']}"
                "/updateDefinition",
                headers=headers,
                json={"definition": definition},
            )

        # Fabric has no upsert-by-name: create posts to /notebooks with the
        # name in the body; replacing an existing notebook needs its item id
        # and the updateDefinition endpoint.
        existing = self._find_item(name, headers)
        if existing:
            resp = _update(existing)
        else:
            resp = requests.post(
                f"{self.base_url}/workspaces/{self.workspace_id}/notebooks",
                headers=headers,
                json={"displayName": name, "definition": definition},
            )
            if resp.status_code == 409:
                # Item listing is eventually consistent: a just-created
                # notebook reserves its name (409) before it appears in the
                # list. Wait for the item id, then update instead.
                for _ in range(15):
                    time.sleep(2)
                    existing = self._find_item(name, headers)
                    if existing:
                        break
                if existing is None:
                    raise NotebookOperationError(
                        f"Fabric notebook '{name}' conflicted on create but never "
                        f"appeared in the item list: {resp.text}"
                    )
                resp = _update(existing)

        if resp.status_code not in (200, 201, 202):
            raise NotebookOperationError(
                f"Fabric notebook create failed ({resp.status_code}): {resp.text}"
            )
        if resp.status_code == 202:
            self._poll_operation(headers, resp.headers.get("Location", ""), f"notebook '{name}'")

    def delete_notebook(self, name: str) -> None:
        headers = self._headers()
        item = self._find_item(name, headers)
        if item is None:
            return
        resp = requests.delete(
            f"{self.base_url}/workspaces/{self.workspace_id}/items/{item['id']}",
            headers=headers,
        )
        if resp.status_code not in (200, 202, 404):
            raise NotebookOperationError(f"Fabric delete failed ({resp.status_code}): {resp.text}")


def create_notebook_client(
    platform: str,
    workspace: str,
    databricks_folder: str = DEFAULT_DATABRICKS_FOLDER,
    credential: Optional[Any] = None,
) -> WorkspaceNotebookClient:
    """Build the notebook client for a platform.

    ``workspace`` is the workspace URL for Databricks, the workspace name for
    Synapse, and the workspace id for Fabric.
    """
    platform = (platform or "").strip().lower()
    if platform == "databricks":
        return DatabricksNotebookClient(workspace, folder=databricks_folder, credential=credential)
    if platform == "synapse":
        return SynapseNotebookClient(workspace, credential=credential)
    if platform == "fabric":
        return FabricNotebookClient(workspace, credential=credential)
    raise NotebookOperationError(f"Unsupported platform for notebook operations: {platform}")
