"""Design-time artifact store abstraction (gh#207).

The artifacts contract ({root}/config, {root}/packages, {root}/scripts,
{root}/data-apps/<app>) is read platform-agnostically at runtime — the
cluster's storage utils interpret whatever string ``artifacts_storage_path``
holds. This module gives the design-time side (CLI/SDK uploads from a dev
machine) the same property: the path string declares the backend, and one
store interface covers every write the tooling performs.

Supported artifacts roots:

- ``abfss://container@account.dfs.<suffix>/base`` — Azure Blob/ADLS Gen2,
  authenticated via the shared Azure credential chain (service principal
  env triple, then ``DefaultAzureCredential``). Sovereign-cloud endpoints
  resolve exactly as before via ``AZURE_STORAGE_BLOB_ENDPOINT_SUFFIX`` /
  ``AZURE_CLOUD``.
- ``/Volumes/<catalog>/<schema>/<volume>[/base]`` — Unity Catalog volume,
  written through the Databricks Files API. Auth follows the same chain as
  the SDK's ``DatabricksAPI``: ``DATABRICKS_TOKEN``, then the
  ``AZURE_TENANT_ID``/``AZURE_CLIENT_ID``/``AZURE_CLIENT_SECRET`` service
  principal, then Azure CLI login (databricks-sdk natively reads ``ARM_*``,
  not ``AZURE_*``, so the explicit mapping here is deliberate).
- ``file:///path`` or a local directory — filesystem store for tests and
  local development.

``dbfs:/`` roots are rejected deliberately: DBFS root storage is deprecated
by Databricks; use a Unity Catalog volume instead.

All store methods take root-relative paths (``config/settings.yaml``,
``packages/foo.whl``), matching the artifacts folder contract. Third-party
imports are lazy with actionable install hints.
"""

import io
import os
import posixpath
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, List, Optional, Tuple

from kindling_sdk.platform_provider import (
    azure_abfss_uri,
    azure_cloud_config,
    create_azure_credential,
)

_SUPPORTED_ROOT_FORMS = (
    "abfss://container@account.dfs.<endpoint-suffix>/path, "
    "/Volumes/<catalog>/<schema>/<volume>[/path], "
    "file:///path, or an existing local directory"
)


def parse_abfss_uri(uri: str) -> Tuple[str, str, str]:
    """Parse an abfss:// URI into (container, account, path).

    Expected format: abfss://container@account.dfs.core.windows.net/path
    The storage account name is the part before the first '.' in the host.

    Returns (container, storage_account_name, blob_path_prefix).
    Raises ValueError on malformed URIs.
    """
    if not uri.startswith("abfss://"):
        raise ValueError(
            f"Invalid destination URI `{uri}`. "
            "Expected abfss://container@account.dfs.core.windows.net/path"
        )
    rest = uri[len("abfss://") :]
    if "@" not in rest:
        raise ValueError(
            f"Invalid abfss URI `{uri}`. Missing '@' separator between container and account."
        )
    container, host_and_path = rest.split("@", 1)
    if not container:
        raise ValueError(f"Invalid abfss URI `{uri}`. Container name is empty.")

    if "/" in host_and_path:
        host, path = host_and_path.split("/", 1)
    else:
        host = host_and_path
        path = ""

    if "." not in host:
        raise ValueError(
            f"Invalid abfss URI `{uri}`. Host `{host}` does not contain a domain suffix."
        )
    account_name = host.split(".", 1)[0]
    if not account_name:
        raise ValueError(f"Invalid abfss URI `{uri}`. Storage account name is empty.")

    return container, account_name, path.lstrip("/")


def resolve_blob_account_url(storage_account: str) -> str:
    """Resolve a storage account name or URL to a full blob endpoint URL.

    Accepts:
      - Just the account name: "mystorageacct" -> configured cloud blob endpoint
      - Name with custom domain: "mystorageacct.blob.core.usgovcloudapi.net" -> "https://..."
      - Full URL: "https://mystorageacct.blob.core.usgovcloudapi.net" -> passed through
    """
    if storage_account.startswith("https://"):
        return storage_account
    if "." in storage_account:
        return f"https://{storage_account}"
    endpoint_suffix = os.getenv("AZURE_STORAGE_BLOB_ENDPOINT_SUFFIX")
    if endpoint_suffix:
        endpoint_suffix = endpoint_suffix.strip().rstrip("/").lstrip(".")
        return f"https://{storage_account}.{endpoint_suffix.lstrip('.')}"
    endpoint_suffix = azure_cloud_config()["storage_suffix"]
    return f"https://{storage_account}.blob.{endpoint_suffix.lstrip('.')}"


def create_blob_service_client(storage_account: str):
    """Create a BlobServiceClient using the shared Azure credential chain."""
    try:
        from azure.storage.blob import BlobServiceClient
    except ImportError as exc:
        raise ImportError(
            "azure-identity and azure-storage-blob are required for abfss:// "
            "artifact stores.\n"
            "Install with: pip install 'spark-kindling-cli[deploy]' "
            "(or: pip install azure-identity azure-storage-blob)"
        ) from exc

    account_url = resolve_blob_account_url(storage_account)
    credential = create_azure_credential(additionally_allowed_tenants=["*"])
    return BlobServiceClient(account_url=account_url, credential=credential)


def create_databricks_workspace_client(host: Optional[str] = None):
    """Create a databricks-sdk WorkspaceClient with the SDK's auth chain.

    Token first, then the AZURE_* service principal triple, then Azure CLI
    login — the same order as DatabricksAPI. Kept as a module function so
    stores, the CLI, and (Phase 2) the platform APIs resolve credentials
    identically.
    """
    try:
        from databricks.sdk import WorkspaceClient
    except ImportError as exc:
        raise ImportError(
            "databricks-sdk is required for /Volumes artifact stores.\n"
            "Install with: pip install databricks-sdk"
        ) from exc

    resolved_host = (host or os.getenv("DATABRICKS_HOST") or "").strip()
    if not resolved_host:
        raise ValueError(
            "DATABRICKS_HOST is required for /Volumes artifact paths — volume "
            "artifact roots are written through the Databricks Files API. Set "
            "DATABRICKS_HOST plus DATABRICKS_TOKEN, the AZURE_TENANT_ID/"
            "AZURE_CLIENT_ID/AZURE_CLIENT_SECRET service principal, or an "
            "Azure CLI login."
        )

    token = (os.getenv("DATABRICKS_TOKEN") or "").strip()
    if token:
        return WorkspaceClient(host=resolved_host, token=token)

    tenant_id = (os.getenv("AZURE_TENANT_ID") or "").strip()
    client_id = (os.getenv("AZURE_CLIENT_ID") or "").strip()
    client_secret = (os.getenv("AZURE_CLIENT_SECRET") or "").strip()
    if tenant_id and client_id and client_secret:
        return WorkspaceClient(
            host=resolved_host,
            azure_tenant_id=tenant_id,
            azure_client_id=client_id,
            azure_client_secret=client_secret,
            auth_type="azure-client-secret",
        )

    return WorkspaceClient(host=resolved_host, auth_type="azure-cli")


class ArtifactStore(ABC):
    """One artifacts root and the writes/reads the design-time tooling needs.

    All paths are root-relative and forward-slashed, matching the artifacts
    folder contract (``config/``, ``packages/``, ``scripts/``,
    ``data-apps/<app>/``).
    """

    root: str

    def describe(self) -> str:
        """Human-readable destination for CLI display lines."""
        return self.root

    @abstractmethod
    def upload_file(self, rel_path: str, data: bytes, overwrite: bool = True) -> bool:
        """Upload bytes to ``rel_path``. Returns False when the file already
        exists and ``overwrite`` is False (skip semantics), True otherwise."""

    @abstractmethod
    def download_file(self, rel_path: str) -> bytes:
        """Return the contents of ``rel_path``."""

    @abstractmethod
    def list_files(self, prefix: str = "") -> List[str]:
        """Recursively list files under ``prefix``, as root-relative paths."""

    @abstractmethod
    def exists(self, rel_path: str) -> bool:
        """True when ``rel_path`` exists as a file."""

    @abstractmethod
    def delete_prefix(self, prefix: str) -> int:
        """Recursively delete everything under ``prefix``; returns the number
        of files removed."""


class LocalArtifactStore(ArtifactStore):
    """Filesystem-backed store (tests and local development)."""

    def __init__(self, root: str):
        raw = root[len("file://") :] if root.startswith("file://") else root
        self._root_path = Path(raw).expanduser()
        self.root = str(self._root_path)

    def _full(self, rel_path: str) -> Path:
        return self._root_path / rel_path

    def upload_file(self, rel_path: str, data: bytes, overwrite: bool = True) -> bool:
        target = self._full(rel_path)
        if not overwrite and target.exists():
            return False
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
        return True

    def download_file(self, rel_path: str) -> bytes:
        return self._full(rel_path).read_bytes()

    def list_files(self, prefix: str = "") -> List[str]:
        base = self._full(prefix) if prefix else self._root_path
        if not base.exists():
            return []
        return sorted(
            str(path.relative_to(self._root_path)).replace(os.sep, "/")
            for path in base.rglob("*")
            if path.is_file()
        )

    def exists(self, rel_path: str) -> bool:
        return self._full(rel_path).is_file()

    def delete_prefix(self, prefix: str) -> int:
        import shutil

        base = self._full(prefix) if prefix else self._root_path
        if not base.exists():
            return 0
        count = sum(1 for path in base.rglob("*") if path.is_file())
        shutil.rmtree(base)
        return count


class AbfssArtifactStore(ArtifactStore):
    """Azure Blob/ADLS Gen2 store for abfss:// artifact roots.

    The blob endpoint is rebuilt from the account name via the configured
    Azure environment (``AZURE_STORAGE_BLOB_ENDPOINT_SUFFIX`` /
    ``AZURE_CLOUD``), exactly as the CLI resolved it before; the dfs host in
    the URI only contributes the account name. A pre-built client may be
    injected (Phase 2: the platform APIs pass their own).
    """

    def __init__(self, uri: str, *, blob_service_client: Any = None):
        self.container, self.account, self.base_path = parse_abfss_uri(uri)
        self.root = uri.rstrip("/")
        self._client = blob_service_client

    @property
    def client(self) -> Any:
        if self._client is None:
            self._client = create_blob_service_client(self.account)
        return self._client

    def _blob(self, rel_path: str) -> str:
        return f"{self.base_path}/{rel_path}" if self.base_path else rel_path

    def _rel(self, blob_name: str) -> str:
        if self.base_path and blob_name.startswith(f"{self.base_path}/"):
            return blob_name[len(self.base_path) + 1 :]
        return blob_name

    def upload_file(self, rel_path: str, data: bytes, overwrite: bool = True) -> bool:
        blob_client = self.client.get_blob_client(
            container=self.container, blob=self._blob(rel_path)
        )
        if not overwrite:
            try:
                blob_client.get_blob_properties()
                return False  # exists, skip
            except Exception:
                pass  # doesn't exist, proceed
        blob_client.upload_blob(data, overwrite=True)
        return True

    def download_file(self, rel_path: str) -> bytes:
        blob_client = self.client.get_blob_client(
            container=self.container, blob=self._blob(rel_path)
        )
        return blob_client.download_blob().readall()

    def list_files(self, prefix: str = "") -> List[str]:
        full_prefix = self._blob(prefix) if prefix else self.base_path
        list_prefix = (
            f"{full_prefix}/"
            if full_prefix and not full_prefix.endswith("/")
            else (full_prefix or "")
        )
        container_client = self.client.get_container_client(self.container)
        return sorted(
            self._rel(blob.name)
            for blob in container_client.list_blobs(name_starts_with=list_prefix)
        )

    def exists(self, rel_path: str) -> bool:
        blob_client = self.client.get_blob_client(
            container=self.container, blob=self._blob(rel_path)
        )
        try:
            blob_client.get_blob_properties()
            return True
        except Exception:
            return False

    def delete_prefix(self, prefix: str) -> int:
        count = 0
        for rel_path in self.list_files(prefix):
            blob_client = self.client.get_blob_client(
                container=self.container, blob=self._blob(rel_path)
            )
            blob_client.delete_blob()
            count += 1
        return count


class VolumesArtifactStore(ArtifactStore):
    """Unity Catalog volume store, written through the Databricks Files API.

    Requires Databricks credentials (see create_databricks_workspace_client);
    the workspace client may be injected. Files API directory deletes are
    non-recursive, so delete_prefix walks files first, then removes the
    emptied directories deepest-first.
    """

    def __init__(self, root: str, *, workspace_client: Any = None):
        segments = [segment for segment in root.strip("/").split("/") if segment]
        if len(segments) < 4 or segments[0] != "Volumes":
            raise ValueError(
                f"Invalid volume artifacts path `{root}`. Expected "
                "/Volumes/<catalog>/<schema>/<volume>[/path]."
            )
        self.root = "/" + "/".join(segments)
        self._workspace_client = workspace_client
        if self._workspace_client is None:
            self._workspace_client = create_databricks_workspace_client()
        from databricks.sdk.errors import NotFound

        self._not_found = NotFound

    @property
    def _files(self) -> Any:
        return self._workspace_client.files

    def _full(self, rel_path: str) -> str:
        return posixpath.join(self.root, rel_path) if rel_path else self.root

    def _rel(self, full_path: str) -> str:
        return (
            full_path[len(self.root) + 1 :]
            if full_path.startswith(f"{self.root}/")
            else (full_path)
        )

    def upload_file(self, rel_path: str, data: bytes, overwrite: bool = True) -> bool:
        full = self._full(rel_path)
        if not overwrite and self.exists(rel_path):
            return False
        try:
            self._files.upload(full, io.BytesIO(data), overwrite=True)
        except self._not_found:
            # Some workspace/API versions do not auto-create parent
            # directories on upload.
            self._files.create_directory(posixpath.dirname(full))
            self._files.upload(full, io.BytesIO(data), overwrite=True)
        return True

    def download_file(self, rel_path: str) -> bytes:
        response = self._files.download(self._full(rel_path))
        return response.contents.read()

    def list_files(self, prefix: str = "") -> List[str]:
        base = self._full(prefix)
        results: List[str] = []
        stack = [base]
        while stack:
            directory = stack.pop()
            try:
                entries = list(self._files.list_directory_contents(directory))
            except self._not_found:
                continue
            for entry in entries:
                if entry.is_directory:
                    stack.append(entry.path)
                else:
                    results.append(self._rel(entry.path))
        return sorted(results)

    def exists(self, rel_path: str) -> bool:
        try:
            self._files.get_metadata(self._full(rel_path))
            return True
        except self._not_found:
            return False

    def delete_prefix(self, prefix: str) -> int:
        base = self._full(prefix)
        files: List[str] = []
        directories: List[str] = []
        stack = [base]
        while stack:
            directory = stack.pop()
            try:
                entries = list(self._files.list_directory_contents(directory))
            except self._not_found:
                continue
            directories.append(directory)
            for entry in entries:
                if entry.is_directory:
                    stack.append(entry.path)
                else:
                    files.append(entry.path)
        for file_path in files:
            self._files.delete(file_path)
        for directory in sorted(directories, key=lambda path: path.count("/"), reverse=True):
            try:
                self._files.delete_directory(directory)
            except Exception:
                pass  # base may be the volume root itself, which is not deletable
        return len(files)


def artifact_store_for(path: str, *, workspace_client: Any = None) -> ArtifactStore:
    """Return the store implementation declared by ``path``'s shape.

    The path string is the single source of truth — the same
    ``artifacts_storage_path`` value the runtime interprets on-cluster.
    """
    candidate = (path or "").strip()
    if not candidate:
        raise ValueError(f"Artifacts path is empty. Expected {_SUPPORTED_ROOT_FORMS}.")
    if candidate.startswith("abfss://"):
        return AbfssArtifactStore(candidate)
    if candidate == "/Volumes" or candidate.startswith("/Volumes/"):
        return VolumesArtifactStore(candidate, workspace_client=workspace_client)
    if candidate.startswith("dbfs:/"):
        raise ValueError(
            f"Unsupported artifacts path `{candidate}`: DBFS root storage is "
            "deprecated by Databricks — use a Unity Catalog volume "
            "(/Volumes/<catalog>/<schema>/<volume>/...) instead."
        )
    if candidate.startswith("file://"):
        return LocalArtifactStore(candidate)
    if "://" in candidate:
        raise ValueError(
            f"Unsupported artifacts path `{candidate}`. Expected {_SUPPORTED_ROOT_FORMS}."
        )
    local_candidate = Path(candidate).expanduser()
    if not local_candidate.is_absolute() and not local_candidate.is_dir():
        raise ValueError(
            f"Unsupported artifacts path `{candidate}`. Expected {_SUPPORTED_ROOT_FORMS}."
        )
    return LocalArtifactStore(candidate)


def resolve_artifacts_path(
    explicit: Optional[str] = None,
    *,
    storage_account: Optional[str] = None,
    container: Optional[str] = None,
    base_path: Optional[str] = None,
) -> str:
    """Resolve the artifacts destination to a single path string.

    Precedence: explicit path argument, then explicit legacy storage flags,
    then the ``KINDLING_ARTIFACTS_STORAGE_PATH`` env var (the Dynaconf env
    spelling of the runtime's ``artifacts_storage_path`` config key — one
    value configures both sides), then the legacy ``AZURE_*`` env triple
    synthesized to an abfss:// URI. Raises ValueError when nothing is
    configured.
    """
    if explicit and explicit.strip():
        return explicit.strip()

    legacy_flags_given = bool(storage_account or container) or base_path is not None
    if legacy_flags_given:
        account = (storage_account or os.getenv("AZURE_STORAGE_ACCOUNT", "")).strip()
        if not account:
            raise ValueError(
                "Storage account is required. Use --storage-account or set "
                "AZURE_STORAGE_ACCOUNT."
            )
        resolved_container = container or os.getenv("AZURE_CONTAINER", "artifacts")
        resolved_base = base_path if base_path is not None else os.getenv("AZURE_BASE_PATH", "")
        return azure_abfss_uri(resolved_container, account, resolved_base)

    env_path = os.getenv("KINDLING_ARTIFACTS_STORAGE_PATH", "").strip()
    if env_path:
        return env_path

    account = os.getenv("AZURE_STORAGE_ACCOUNT", "").strip()
    if account:
        return azure_abfss_uri(
            os.getenv("AZURE_CONTAINER", "artifacts"), account, os.getenv("AZURE_BASE_PATH", "")
        )

    raise ValueError(
        "Artifacts location is not configured. Provide --artifacts-path, set "
        "KINDLING_ARTIFACTS_STORAGE_PATH (e.g. "
        "abfss://artifacts@acct.dfs.core.windows.net/kindling or "
        "/Volumes/<catalog>/<schema>/<volume>/kindling), or set the legacy "
        "AZURE_STORAGE_ACCOUNT (+ optional AZURE_CONTAINER, AZURE_BASE_PATH)."
    )
