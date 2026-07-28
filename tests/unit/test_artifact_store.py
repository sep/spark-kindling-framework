"""Unit tests for kindling_sdk.artifact_store (gh#207).

Covers scheme dispatch, destination resolution precedence, and the three
store backends: Local against a real tmp_path, Volumes against a stubbed
Files API, and Abfss against a mocked BlobServiceClient.
"""

import io
import posixpath
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from kindling_sdk.artifact_store import (
    AbfssArtifactStore,
    LocalArtifactStore,
    VolumesArtifactStore,
    artifact_store_for,
    parse_abfss_uri,
    resolve_artifacts_path,
)

_ENV_VARS = (
    "KINDLING_ARTIFACTS_STORAGE_PATH",
    "AZURE_STORAGE_ACCOUNT",
    "AZURE_CONTAINER",
    "AZURE_BASE_PATH",
    "AZURE_STORAGE_DFS_ENDPOINT_SUFFIX",
    "AZURE_STORAGE_BLOB_ENDPOINT_SUFFIX",
    "AZURE_CLOUD",
)


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for var in _ENV_VARS:
        monkeypatch.delenv(var, raising=False)


class TestDispatch:
    def test_abfss_dispatches_to_abfss_store(self):
        store = artifact_store_for("abfss://artifacts@acct.dfs.core.windows.net/kindling")
        assert isinstance(store, AbfssArtifactStore)
        assert store.container == "artifacts"
        assert store.account == "acct"
        assert store.base_path == "kindling"

    def test_volumes_dispatches_to_volumes_store(self):
        store = artifact_store_for("/Volumes/main/kindling/artifacts", workspace_client=MagicMock())
        assert isinstance(store, VolumesArtifactStore)
        assert store.root == "/Volumes/main/kindling/artifacts"

    def test_file_uri_and_existing_dir_dispatch_to_local_store(self, tmp_path):
        assert isinstance(artifact_store_for(f"file://{tmp_path}"), LocalArtifactStore)
        assert isinstance(artifact_store_for(str(tmp_path)), LocalArtifactStore)

    def test_dbfs_rejected_with_volume_hint(self):
        with pytest.raises(ValueError, match="deprecated.*Unity Catalog volume"):
            artifact_store_for("dbfs:/FileStore/kindling")

    def test_unknown_scheme_rejected(self):
        with pytest.raises(ValueError, match="Unsupported artifacts path"):
            artifact_store_for("ftp://host/path")

    def test_relative_nonexistent_path_rejected(self):
        with pytest.raises(ValueError, match="Unsupported artifacts path"):
            artifact_store_for("not-an-abfss-uri")

    def test_empty_path_rejected(self):
        with pytest.raises(ValueError, match="empty"):
            artifact_store_for("")

    def test_truncated_volume_path_rejected(self):
        with pytest.raises(ValueError, match="/Volumes/<catalog>/<schema>/<volume>"):
            artifact_store_for("/Volumes/main/kindling", workspace_client=MagicMock())


class TestResolveArtifactsPath:
    def test_explicit_path_wins(self, monkeypatch):
        monkeypatch.setenv("KINDLING_ARTIFACTS_STORAGE_PATH", "/Volumes/a/b/c")
        assert resolve_artifacts_path("/Volumes/x/y/z") == "/Volumes/x/y/z"

    def test_legacy_flags_win_over_env_path(self, monkeypatch):
        monkeypatch.setenv("KINDLING_ARTIFACTS_STORAGE_PATH", "/Volumes/a/b/c")
        resolved = resolve_artifacts_path(None, storage_account="acct")
        assert resolved == "abfss://artifacts@acct.dfs.core.windows.net"

    def test_env_path_wins_over_legacy_env_triple(self, monkeypatch):
        monkeypatch.setenv("KINDLING_ARTIFACTS_STORAGE_PATH", "/Volumes/a/b/c")
        monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "acct")
        assert resolve_artifacts_path() == "/Volumes/a/b/c"

    def test_legacy_env_triple_synthesizes_abfss(self, monkeypatch):
        monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "acct")
        monkeypatch.setenv("AZURE_CONTAINER", "deploy")
        monkeypatch.setenv("AZURE_BASE_PATH", "kindling")
        assert resolve_artifacts_path() == "abfss://deploy@acct.dfs.core.windows.net/kindling"

    def test_synthesis_honors_dfs_endpoint_suffix(self, monkeypatch):
        monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "acct")
        monkeypatch.setenv("AZURE_STORAGE_DFS_ENDPOINT_SUFFIX", "dfs.core.usgovcloudapi.net")
        assert resolve_artifacts_path() == "abfss://artifacts@acct.dfs.core.usgovcloudapi.net"

    def test_synthesis_honors_azure_cloud(self, monkeypatch):
        monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "govacct")
        monkeypatch.setenv("AZURE_CLOUD", "AzureUSGovernment")
        assert resolve_artifacts_path() == "abfss://artifacts@govacct.dfs.core.usgovcloudapi.net"

    def test_legacy_flags_without_account_raise_current_message(self):
        with pytest.raises(ValueError, match="Storage account is required"):
            resolve_artifacts_path(None, container="artifacts")

    def test_nothing_configured_raises_actionable_error(self):
        with pytest.raises(ValueError) as exc_info:
            resolve_artifacts_path()
        message = str(exc_info.value)
        assert "KINDLING_ARTIFACTS_STORAGE_PATH" in message
        assert "AZURE_STORAGE_ACCOUNT" in message
        assert "--artifacts-path" in message


class TestLocalArtifactStore:
    def test_round_trip(self, tmp_path):
        store = LocalArtifactStore(str(tmp_path))

        assert store.upload_file("config/settings.yaml", b"kindling: {}") is True
        assert store.upload_file("packages/a.whl", b"wheel") is True
        assert store.download_file("config/settings.yaml") == b"kindling: {}"
        assert store.exists("packages/a.whl") is True
        assert store.exists("packages/missing.whl") is False
        assert store.list_files() == ["config/settings.yaml", "packages/a.whl"]
        assert store.list_files("packages") == ["packages/a.whl"]
        assert store.delete_prefix("packages") == 1
        assert store.list_files("packages") == []

    def test_overwrite_false_skips_existing(self, tmp_path):
        store = LocalArtifactStore(str(tmp_path))
        store.upload_file("scripts/kindling_bootstrap.py", b"one")

        assert store.upload_file("scripts/kindling_bootstrap.py", b"two", overwrite=False) is (
            False
        )
        assert store.download_file("scripts/kindling_bootstrap.py") == b"one"

    def test_file_uri_root(self, tmp_path):
        store = LocalArtifactStore(f"file://{tmp_path}")
        store.upload_file("a.txt", b"x")
        assert (tmp_path / "a.txt").read_bytes() == b"x"


def _make_fake_blob_service_client(existing=None):
    """Fake BlobServiceClient with dict-backed blobs and prefix listing."""
    blobs = dict(existing or {})

    def get_blob_client(container, blob):
        client = MagicMock()

        def get_properties():
            if blob not in blobs:
                raise Exception("not found")
            return {}

        client.get_blob_properties.side_effect = get_properties
        client.upload_blob.side_effect = lambda data, overwrite=True: blobs.__setitem__(blob, data)
        client.download_blob.side_effect = lambda: SimpleNamespace(readall=lambda: blobs[blob])
        client.delete_blob.side_effect = lambda: blobs.pop(blob)
        return client

    def get_container_client(container):
        container_client = MagicMock()
        container_client.list_blobs.side_effect = lambda name_starts_with="": [
            SimpleNamespace(name=name)
            for name in sorted(blobs)
            if name.startswith(name_starts_with)
        ]
        return container_client

    service_client = MagicMock()
    service_client.get_blob_client.side_effect = get_blob_client
    service_client.get_container_client.side_effect = get_container_client
    service_client._blobs = blobs
    return service_client


class TestAbfssArtifactStore:
    URI = "abfss://artifacts@acct.dfs.core.windows.net/kindling"

    def _store(self, existing=None):
        client = _make_fake_blob_service_client(existing)
        return AbfssArtifactStore(self.URI, blob_service_client=client), client

    def test_upload_prefixes_base_path(self):
        store, client = self._store()
        assert store.upload_file("packages/a.whl", b"wheel") is True
        assert client._blobs == {"kindling/packages/a.whl": b"wheel"}

    def test_overwrite_false_skips_existing_blob(self):
        store, client = self._store(existing={"kindling/scripts/boot.py": b"one"})
        assert store.upload_file("scripts/boot.py", b"two", overwrite=False) is False
        assert client._blobs["kindling/scripts/boot.py"] == b"one"

    def test_list_files_returns_root_relative_paths(self):
        store, _ = self._store(
            existing={
                "kindling/packages/a.whl": b"1",
                "kindling/packages/nested/b.whl": b"2",
                "kindling/scripts/boot.py": b"3",
                "unrelated/other.txt": b"4",
            }
        )
        assert store.list_files("packages") == ["packages/a.whl", "packages/nested/b.whl"]
        assert store.list_files() == [
            "packages/a.whl",
            "packages/nested/b.whl",
            "scripts/boot.py",
        ]

    def test_download_exists_delete(self):
        store, client = self._store(existing={"kindling/packages/a.whl": b"wheel"})
        assert store.download_file("packages/a.whl") == b"wheel"
        assert store.exists("packages/a.whl") is True
        assert store.exists("packages/b.whl") is False
        assert store.delete_prefix("packages") == 1
        assert client._blobs == {}

    def test_no_base_path_uses_container_root(self):
        client = _make_fake_blob_service_client()
        store = AbfssArtifactStore(
            "abfss://artifacts@acct.dfs.core.windows.net", blob_service_client=client
        )
        store.upload_file("packages/a.whl", b"wheel")
        assert client._blobs == {"packages/a.whl": b"wheel"}

    @pytest.mark.parametrize(
        ("uri", "match"),
        [
            ("https://acct.blob.core.windows.net/c", "Invalid destination URI"),
            ("abfss://artifacts.dfs.core.windows.net/p", "Missing '@' separator"),
            ("abfss://@acct.dfs.core.windows.net/p", "Container name is empty"),
            ("abfss://c@accountnodot/p", "does not contain a domain suffix"),
        ],
    )
    def test_malformed_uris_raise_current_messages(self, uri, match):
        with pytest.raises(ValueError, match=match):
            parse_abfss_uri(uri)


class _FakeFilesAPI:
    """Dict-backed stub of WorkspaceClient.files raising real NotFound."""

    def __init__(self, require_parent_dirs=False):
        from databricks.sdk.errors import NotFound

        self._not_found = NotFound
        self.require_parent_dirs = require_parent_dirs
        self.files = {}
        self.dirs = set()
        self.deleted_directories = []

    def upload(self, path, contents, overwrite=None):
        if self.require_parent_dirs and posixpath.dirname(path) not in self.dirs:
            raise self._not_found(f"parent of {path} does not exist")
        self.files[path] = contents.read()

    def download(self, path):
        if path not in self.files:
            raise self._not_found(path)
        return SimpleNamespace(contents=io.BytesIO(self.files[path]))

    def get_metadata(self, path):
        if path not in self.files:
            raise self._not_found(path)
        return {}

    def create_directory(self, path):
        self.dirs.add(path)

    def list_directory_contents(self, directory):
        directory = directory.rstrip("/")
        children = {}
        for path in self.files:
            if path.startswith(f"{directory}/"):
                rest = path[len(directory) + 1 :]
                head = rest.split("/", 1)[0]
                children[f"{directory}/{head}"] = "/" in rest
        if not children:
            raise self._not_found(directory)
        return [
            SimpleNamespace(path=path, is_directory=is_dir) for path, is_dir in children.items()
        ]

    def delete(self, path):
        del self.files[path]

    def delete_directory(self, path):
        self.deleted_directories.append(path)


def _make_volumes_store(require_parent_dirs=False):
    files_api = _FakeFilesAPI(require_parent_dirs=require_parent_dirs)
    workspace_client = SimpleNamespace(files=files_api)
    store = VolumesArtifactStore(
        "/Volumes/main/kindling/artifacts", workspace_client=workspace_client
    )
    return store, files_api


class TestVolumesArtifactStore:
    def test_upload_maps_to_full_volume_path(self):
        store, files_api = _make_volumes_store()
        assert store.upload_file("packages/a.whl", b"wheel") is True
        assert files_api.files == {"/Volumes/main/kindling/artifacts/packages/a.whl": b"wheel"}

    def test_upload_ensures_parent_directory_on_not_found(self):
        store, files_api = _make_volumes_store(require_parent_dirs=True)
        assert store.upload_file("packages/a.whl", b"wheel") is True
        assert "/Volumes/main/kindling/artifacts/packages" in files_api.dirs
        assert files_api.files == {"/Volumes/main/kindling/artifacts/packages/a.whl": b"wheel"}

    def test_overwrite_false_skips_existing_file(self):
        store, files_api = _make_volumes_store()
        store.upload_file("scripts/boot.py", b"one")
        assert store.upload_file("scripts/boot.py", b"two", overwrite=False) is False
        assert files_api.files["/Volumes/main/kindling/artifacts/scripts/boot.py"] == b"one"

    def test_exists_via_get_metadata_not_found(self):
        store, _ = _make_volumes_store()
        store.upload_file("packages/a.whl", b"wheel")
        assert store.exists("packages/a.whl") is True
        assert store.exists("packages/missing.whl") is False

    def test_list_files_walks_recursively(self):
        store, _ = _make_volumes_store()
        store.upload_file("packages/a.whl", b"1")
        store.upload_file("packages/nested/b.whl", b"2")
        store.upload_file("scripts/boot.py", b"3")
        assert store.list_files() == [
            "packages/a.whl",
            "packages/nested/b.whl",
            "scripts/boot.py",
        ]
        assert store.list_files("packages") == ["packages/a.whl", "packages/nested/b.whl"]
        assert store.list_files("empty") == []

    def test_download_round_trip(self):
        store, _ = _make_volumes_store()
        store.upload_file("config/settings.yaml", b"kindling: {}")
        assert store.download_file("config/settings.yaml") == b"kindling: {}"

    def test_delete_prefix_removes_files_then_directories(self):
        store, files_api = _make_volumes_store()
        store.upload_file("packages/a.whl", b"1")
        store.upload_file("packages/nested/b.whl", b"2")
        store.upload_file("scripts/boot.py", b"3")

        assert store.delete_prefix("packages") == 2

        assert list(files_api.files) == ["/Volumes/main/kindling/artifacts/scripts/boot.py"]
        # Directories removed deepest-first, after their files.
        assert files_api.deleted_directories == [
            "/Volumes/main/kindling/artifacts/packages/nested",
            "/Volumes/main/kindling/artifacts/packages",
        ]

    def test_missing_databricks_host_raises_credentials_hint(self, monkeypatch):
        monkeypatch.delenv("DATABRICKS_HOST", raising=False)
        with pytest.raises(ValueError, match="DATABRICKS_HOST.*Files API"):
            VolumesArtifactStore("/Volumes/main/kindling/artifacts")
