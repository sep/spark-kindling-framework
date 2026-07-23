"""Unit tests for kindling_sdk.notebooks — workspace notebook REST clients."""

import base64
import json
import types

import pytest
from kindling_sdk.notebooks import (
    DatabricksNotebookClient,
    FabricNotebookClient,
    NotebookOperationError,
    SynapseNotebookClient,
    create_notebook_client,
)


class _FakeCredential:
    def get_token(self, *scopes):
        return types.SimpleNamespace(token="fake-token")


def _response(status_code, payload=None, headers=None):
    return types.SimpleNamespace(
        status_code=status_code,
        json=lambda: payload or {},
        text=str(payload),
        headers=headers or {},
    )


@pytest.fixture
def rest(monkeypatch):
    """Install a fake requests module into kindling_sdk.notebooks.

    Returns a holder whose .handler is called as handler(method, url, params, json).
    """
    holder = types.SimpleNamespace(handler=None)

    fake = types.ModuleType("requests")
    fake.get = lambda url, headers=None, params=None: holder.handler("GET", url, params, None)
    fake.post = lambda url, headers=None, params=None, json=None: holder.handler(
        "POST", url, params, json
    )
    fake.put = lambda url, headers=None, params=None, json=None: holder.handler(
        "PUT", url, params, json
    )
    fake.delete = lambda url, headers=None, params=None: holder.handler("DELETE", url, params, None)
    monkeypatch.setattr("kindling_sdk.notebooks.requests", fake)
    monkeypatch.setattr(
        "kindling_sdk.notebooks.create_azure_credential", lambda **kw: _FakeCredential()
    )
    monkeypatch.setattr("time.sleep", lambda seconds: None)
    return holder


class TestFactory:
    def test_dispatches_by_platform(self, rest):
        assert isinstance(
            create_notebook_client("databricks", "https://db"), DatabricksNotebookClient
        )
        assert isinstance(create_notebook_client("synapse", "ws"), SynapseNotebookClient)
        assert isinstance(create_notebook_client("fabric", "ws-id"), FabricNotebookClient)

    def test_unsupported_platform_raises(self, rest):
        with pytest.raises(NotebookOperationError, match="Unsupported platform"):
            create_notebook_client("standalone", "x")


class TestDatabricks:
    def _client(self):
        return DatabricksNotebookClient("https://db.example.net")

    def test_list_filters_notebooks(self, rest):
        def handler(method, url, params, body):
            assert url.endswith("/api/2.0/workspace/list")
            assert params == {"path": "/Shared/kindling"}
            return _response(
                200,
                {
                    "objects": [
                        {"object_type": "NOTEBOOK", "path": "/Shared/kindling/etl"},
                        {"object_type": "DIRECTORY", "path": "/Shared/kindling/sub"},
                    ]
                },
            )

        rest.handler = handler
        assert self._client().list_notebooks() == ["etl"]

    def test_list_missing_folder_is_empty(self, rest):
        rest.handler = lambda method, url, params, body: _response(404)
        assert self._client().list_notebooks() == []

    def test_export_decodes_jupyter_payload(self, rest):
        nb = {"cells": [{"cell_type": "code", "source": ["x = 1"], "metadata": {}}]}
        payload = base64.b64encode(json.dumps(nb).encode()).decode()

        def handler(method, url, params, body):
            assert url.endswith("/api/2.0/workspace/export")
            assert params == {"path": "/Shared/kindling/etl", "format": "JUPYTER"}
            return _response(200, {"content": payload})

        rest.handler = handler
        assert self._client().get_notebook_cells("etl") == nb["cells"]

    def test_import_creates_parent_folder_first(self, rest):
        calls = []

        def handler(method, url, params, body):
            calls.append((url.rsplit("/", 1)[-1], body))
            return _response(200)

        rest.handler = handler
        self._client().import_notebook("etl", {"cells": []})

        assert calls[0] == ("mkdirs", {"path": "/Shared/kindling"})
        assert calls[1][0] == "import"
        assert calls[1][1]["path"] == "/Shared/kindling/etl"
        assert calls[1][1]["overwrite"] is True

    def test_delete_tolerates_missing(self, rest):
        rest.handler = lambda method, url, params, body: _response(404)
        self._client().delete_notebook("gone")


class TestSynapse:
    def _client(self):
        return SynapseNotebookClient("my-ws")

    def test_list_follows_pagination(self, rest):
        def handler(method, url, params, body):
            if "page2" not in url:
                return _response(200, {"value": [{"name": "b"}], "nextLink": url + "&page2=1"})
            return _response(200, {"value": [{"name": "a"}]})

        rest.handler = handler
        assert self._client().list_notebooks() == ["a", "b"]

    def test_get_returns_cells(self, rest):
        cells = [{"cell_type": "code", "source": ["x"], "metadata": {}}]
        rest.handler = lambda method, url, params, body: _response(
            200, {"properties": {"cells": cells}}
        )
        assert self._client().get_notebook_cells("etl") == cells

    def test_import_puts_notebook_resource(self, rest):
        puts = []

        def handler(method, url, params, body):
            assert method == "PUT"
            puts.append(body)
            return _response(200)

        rest.handler = handler
        self._client().import_notebook("etl", {"cells": [{"cell_type": "code", "source": []}]})

        (body,) = puts
        assert body["name"] == "etl"
        assert body["properties"]["cells"]


class TestFabric:
    def _client(self):
        return FabricNotebookClient("ws-id")

    @staticmethod
    def _ipynb_part(nb):
        return {
            "path": "notebook-content.ipynb",
            "payload": base64.b64encode(json.dumps(nb).encode()).decode(),
        }

    def test_list_follows_pagination(self, rest):
        def handler(method, url, params, body):
            if url.endswith("/notebooks"):
                return _response(
                    200,
                    {"value": [{"displayName": "b", "id": "2"}], "continuationUri": url + "?c=1"},
                )
            return _response(200, {"value": [{"displayName": "a", "id": "1"}]})

        rest.handler = handler
        assert self._client().list_notebooks() == ["a", "b"]

    def test_fetch_resolves_id_and_decodes_definition(self, rest):
        nb = {"cells": [{"cell_type": "code", "source": ["x = 1"], "metadata": {}}]}

        def handler(method, url, params, body):
            if method == "GET" and url.endswith("/notebooks"):
                return _response(200, {"value": [{"id": "item-1", "displayName": "etl"}]})
            if method == "POST" and "item-1/getDefinition" in url:
                return _response(200, {"definition": {"parts": [self._ipynb_part(nb)]}})
            raise AssertionError(f"Unexpected request: {method} {url}")

        rest.handler = handler
        assert self._client().get_notebook_cells("etl") == nb["cells"]

    def test_fetch_async_definition_uses_operation_result(self, rest):
        nb = {"cells": [{"cell_type": "code", "source": ["x = 1"], "metadata": {}}]}

        def handler(method, url, params, body):
            if method == "GET" and url.endswith("/notebooks"):
                return _response(200, {"value": [{"id": "item-1", "displayName": "etl"}]})
            if method == "POST" and "item-1/getDefinition" in url:
                return _response(202, headers={"Location": "https://ops/op-1"})
            if method == "GET" and url == "https://ops/op-1":
                return _response(200, {"status": "Succeeded"})
            if method == "GET" and url == "https://ops/op-1/result":
                return _response(200, {"definition": {"parts": [self._ipynb_part(nb)]}})
            raise AssertionError(f"Unexpected request: {method} {url}")

        rest.handler = handler
        assert self._client().get_notebook_cells("etl") == nb["cells"]

    def test_import_creates_when_absent(self, rest):
        created = []

        def handler(method, url, params, body):
            if method == "GET" and url.endswith("/notebooks"):
                return _response(200, {"value": []})
            if method == "POST" and url.endswith("/notebooks"):
                created.append(body)
                return _response(201)
            raise AssertionError(f"Unexpected request: {method} {url}")

        rest.handler = handler
        self._client().import_notebook("etl", {"cells": []})

        (body,) = created
        assert body["displayName"] == "etl"
        assert body["definition"]["format"] == "ipynb"

    def test_import_updates_existing_by_item_id(self, rest):
        updated = []

        def handler(method, url, params, body):
            if method == "GET" and url.endswith("/notebooks"):
                return _response(200, {"value": [{"id": "item-1", "displayName": "etl"}]})
            if method == "POST" and url.endswith("item-1/updateDefinition"):
                updated.append(body)
                return _response(200)
            raise AssertionError(f"Unexpected request: {method} {url}")

        rest.handler = handler
        self._client().import_notebook("etl", {"cells": []})

        (body,) = updated
        assert body["definition"]["format"] == "ipynb"

    def test_import_handles_name_reservation_409(self, rest):
        """A create 409 (name reserved, item not listed yet) falls back to
        updateDefinition once the item appears."""
        state = {"listed": 0}
        updated = []

        def handler(method, url, params, body):
            if method == "GET" and url.endswith("/notebooks"):
                state["listed"] += 1
                if state["listed"] < 3:
                    return _response(200, {"value": []})
                return _response(200, {"value": [{"id": "item-1", "displayName": "etl"}]})
            if method == "POST" and url.endswith("/notebooks"):
                return _response(409, {"errorCode": "ItemDisplayNameNotAvailableYet"})
            if method == "POST" and url.endswith("item-1/updateDefinition"):
                updated.append(body)
                return _response(200)
            raise AssertionError(f"Unexpected request: {method} {url}")

        rest.handler = handler
        self._client().import_notebook("etl", {"cells": []})
        assert len(updated) == 1

    def test_async_failure_is_reported(self, rest):
        """A 202 whose operation ends Failed must not report success."""

        def handler(method, url, params, body):
            if method == "GET" and url.endswith("/notebooks"):
                return _response(200, {"value": []})
            if method == "POST" and url.endswith("/notebooks"):
                return _response(202, headers={"Location": "https://ops/op-1"})
            if method == "GET" and url == "https://ops/op-1":
                return _response(200, {"status": "Failed", "error": {"message": "bad definition"}})
            raise AssertionError(f"Unexpected request: {method} {url}")

        rest.handler = handler
        with pytest.raises(NotebookOperationError, match="bad definition"):
            self._client().import_notebook("etl", {"cells": []})

    def test_async_success_completes(self, rest):
        def handler(method, url, params, body):
            if method == "GET" and url.endswith("/notebooks"):
                return _response(200, {"value": []})
            if method == "POST" and url.endswith("/notebooks"):
                return _response(202, headers={"Location": "https://ops/op-1"})
            if method == "GET" and url == "https://ops/op-1":
                return _response(200, {"status": "Succeeded"})
            raise AssertionError(f"Unexpected request: {method} {url}")

        rest.handler = handler
        self._client().import_notebook("etl", {"cells": []})
