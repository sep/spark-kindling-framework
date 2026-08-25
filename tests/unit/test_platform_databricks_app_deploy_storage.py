"""Unit tests for DatabricksAPI app file upload/cleanup destination resolution.

Regression coverage for: _upload_files/deploy_app/cleanup_app only ever
understood ABFSS (storage_account/container), constructing the Azure Data
Lake SDK client directly. A Unity Catalog volume artifacts root
(`artifacts_path`/`KINDLING_ARTIFACTS_STORAGE_PATH`) -- already resolved
correctly everywhere else in this class (job submission, cluster logs) --
was silently ignored here: `kindling app deploy --platform databricks` on a
Volumes-only setup printed "Storage account/container not configured. Files
not uploaded." and reported success without uploading anything. Both
methods now go through the same `_resolve_artifacts_storage_path` +
`artifact_store_for` path the rest of the class already uses.
"""

from unittest.mock import MagicMock

from kindling_sdk.platform_databricks import DatabricksAPI


def _make_api(**attrs) -> DatabricksAPI:
    api = DatabricksAPI.__new__(DatabricksAPI)
    api.storage_account = attrs.get("storage_account")
    api.container = attrs.get("container")
    api.base_path = attrs.get("base_path")
    api.artifacts_path = attrs.get("artifacts_path")
    api._client = attrs.get("client", MagicMock())
    return api


def _fake_store(root):
    store = MagicMock()
    store.root = root
    store.upload_file.return_value = True
    store.delete_prefix.return_value = 2
    return store


def test_upload_files_uses_volumes_store_when_artifacts_path_set(monkeypatch):
    api = _make_api(artifacts_path="/Volumes/main/kindling/artifacts")
    store = _fake_store("/Volumes/main/kindling/artifacts")
    factory = MagicMock(return_value=store)
    monkeypatch.setattr("kindling_sdk.platform_databricks.artifact_store_for", factory)

    result = api._upload_files({"app.py": "print('hi')"}, "data-apps/my-app")

    factory.assert_called_once_with(
        "/Volumes/main/kindling/artifacts", workspace_client=api._client
    )
    store.upload_file.assert_called_once_with(
        "data-apps/my-app/app.py", b"print('hi')", overwrite=True
    )
    assert result == "/Volumes/main/kindling/artifacts/data-apps/my-app"


def test_upload_files_falls_back_to_abfss_when_no_artifacts_path(monkeypatch):
    api = _make_api(storage_account="myacct", container="artifacts")
    store = _fake_store("abfss://artifacts@myacct.dfs.core.windows.net")
    factory = MagicMock(return_value=store)
    monkeypatch.setattr("kindling_sdk.platform_databricks.artifact_store_for", factory)

    result = api._upload_files({"app.py": "print('hi')"}, "data-apps/my-app")

    assert factory.call_args[0][0] == "abfss://artifacts@myacct.dfs.core.windows.net"
    store.upload_file.assert_called_once()
    assert result == "abfss://artifacts@myacct.dfs.core.windows.net/data-apps/my-app"


def test_upload_files_warns_and_skips_when_nothing_configured():
    api = _make_api()

    result = api._upload_files({"app.py": "print('hi')"}, "data-apps/my-app")

    assert result == "data-apps/my-app"


def test_upload_files_raises_when_some_uploads_fail(monkeypatch):
    api = _make_api(artifacts_path="/Volumes/main/kindling/artifacts")
    store = _fake_store("/Volumes/main/kindling/artifacts")
    store.upload_file.side_effect = [None, RuntimeError("boom")]
    monkeypatch.setattr(
        "kindling_sdk.platform_databricks.artifact_store_for", lambda *a, **k: store
    )

    try:
        api._upload_files({"a.py": "1", "b.py": "2"}, "data-apps/my-app")
        assert False, "expected RuntimeError"
    except RuntimeError as exc:
        assert "uploaded 1/2 files" in str(exc)


def test_deploy_app_targets_data_apps_prefix(monkeypatch):
    api = _make_api(artifacts_path="/Volumes/main/kindling/artifacts")
    store = _fake_store("/Volumes/main/kindling/artifacts")
    monkeypatch.setattr(
        "kindling_sdk.platform_databricks.artifact_store_for", lambda *a, **k: store
    )

    api.deploy_app("my-app", {"app.py": "print('hi')"})

    store.upload_file.assert_called_once_with(
        "data-apps/my-app/app.py", b"print('hi')", overwrite=True
    )


def test_cleanup_app_uses_volumes_store_when_artifacts_path_set(monkeypatch):
    api = _make_api(artifacts_path="/Volumes/main/kindling/artifacts")
    store = _fake_store("/Volumes/main/kindling/artifacts")
    monkeypatch.setattr(
        "kindling_sdk.platform_databricks.artifact_store_for", lambda *a, **k: store
    )

    result = api.cleanup_app("my-app")

    assert result is True
    store.delete_prefix.assert_called_once_with("data-apps/my-app")


def test_cleanup_app_skips_when_nothing_configured():
    api = _make_api()

    result = api.cleanup_app("my-app")

    assert result is False
