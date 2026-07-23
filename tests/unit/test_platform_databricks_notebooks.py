"""Unit tests for DatabricksService notebook methods.

Regressions pinned from live verification:
- get_notebook wrapped the whole SOURCE export in one blob code cell and,
  worse, split source on newlines without keepends — re-joining the cell
  lost every newline, silently corrupting code extracted from workspace
  notebooks.
- create_or_update_notebook silently imported an *empty* notebook when
  passed a bare Jupyter document, never created parent folders, and
  required absolute paths unlike its NotebookManager siblings.
"""

import base64
import json
import types
from unittest.mock import MagicMock

import pytest

from kindling.platform_databricks import DatabricksService


def _response(status_code, payload=None):
    return types.SimpleNamespace(
        status_code=status_code,
        json=lambda: payload or {},
        text=str(payload),
        content=b"x" if payload is not None else b"",
    )


def _service():
    svc = DatabricksService.__new__(DatabricksService)
    svc.logger = MagicMock()
    svc._base_url = "https://db.example.net"
    svc._get_headers = lambda: {"Authorization": "t"}
    svc._items_cache, svc._folders_cache, svc._notebooks_cache = [], {}, []
    return svc


def _jupyter_doc():
    return {
        "nbformat": 4,
        "nbformat_minor": 2,
        "metadata": {},
        "cells": [{"cell_type": "code", "source": ["x = 1"], "metadata": {}}],
    }


@pytest.fixture
def rest(monkeypatch):
    holder = types.SimpleNamespace(handler=None)

    fake = types.ModuleType("requests")
    fake.get = lambda url, headers=None, params=None, timeout=None: holder.handler(
        "GET", url, params, None
    )
    fake.post = lambda url, headers=None, params=None, json=None, timeout=None: holder.handler(
        "POST", url, params, json
    )
    monkeypatch.setattr("kindling.platform_databricks.requests", fake)
    return holder


class TestCreateOrUpdateNotebook:
    def test_accepts_bare_jupyter_document(self, rest):
        imports = []

        def handler(method, url, params, body):
            if url.endswith("/mkdirs"):
                return _response(200, {})
            imports.append(body)
            return _response(200, {})

        rest.handler = handler
        doc = _jupyter_doc()
        _service().create_or_update_notebook("/Shared/kindling/etl", doc)

        (body,) = imports
        decoded = json.loads(base64.b64decode(body["content"]).decode("utf-8"))
        assert decoded == doc
        assert body["format"] == "JUPYTER"
        assert body["overwrite"] is True

    def test_creates_parent_folder_first(self, rest):
        calls = []

        def handler(method, url, params, body):
            calls.append((url.rsplit("/", 1)[-1], body))
            return _response(200, {})

        rest.handler = handler
        _service().create_or_update_notebook("/Shared/fresh/etl", {"content": _jupyter_doc()})

        assert calls[0] == ("mkdirs", {"path": "/Shared/fresh"})
        assert calls[1][0] == "import"

    def test_bare_name_is_normalized(self, rest):
        imports = []

        def handler(method, url, params, body):
            if url.endswith("/mkdirs"):
                return _response(200, {})
            imports.append(body)
            return _response(200, {})

        rest.handler = handler
        _service().create_or_update_notebook("etl", _jupyter_doc())

        assert imports[0]["path"] == "/Shared/kindling/etl"

    def test_missing_content_raises_instead_of_importing_empty(self, rest):
        rest.handler = lambda method, url, params, body: _response(200, {})
        with pytest.raises(ValueError, match="No notebook content"):
            _service().create_or_update_notebook("etl", {"something_else": 1})


class TestGetNotebook:
    SOURCE = (
        "# Databricks notebook source\n"
        "# MAGIC %md\n"
        "# MAGIC # Title\n"
        "\n"
        "# COMMAND ----------\n"
        "\n"
        "x = 1\n"
        "print(x)\n"
    )

    def _handler(self):
        payload = base64.b64encode(self.SOURCE.encode()).decode()

        def handler(method, url, params, body):
            if url.endswith("/get-status"):
                return _response(200, {"path": params["path"], "object_id": 1})
            if url.endswith("/export"):
                assert params["format"] == "SOURCE"
                return _response(200, {"content": payload})
            raise AssertionError(f"Unexpected request: {method} {url}")

        return handler

    def test_parses_cells_with_structure_and_newlines(self, rest):
        rest.handler = self._handler()
        nb = _service().get_notebook("/Shared/kindling/etl")

        cells = nb.properties.cells
        assert [c.cell_type for c in cells] == ["markdown", "code"]
        assert cells[1].get_source_as_string() == "x = 1\nprint(x)"
        assert cells[0].get_source_as_string() == "# Title"


class TestLazyWorkspaceCache:
    """The workspace scan (one REST call per directory — minutes on large
    workspaces) must not run at service construction; deployments with
    load_workspace_packages=False never need it."""

    def _construct(self, monkeypatch, rest):
        calls = []

        def handler(method, url, params, body):
            calls.append((url, dict(params or {})))
            return _response(200, {"objects": []})

        rest.handler = handler
        monkeypatch.setattr(
            DatabricksService, "_build_base_url", lambda self: "https://db.example.net"
        )
        svc = DatabricksService(MagicMock(), MagicMock())
        svc._get_headers = lambda: {"Authorization": "t"}
        return svc, calls

    def test_construction_makes_no_workspace_calls(self, monkeypatch, rest):
        _, calls = self._construct(monkeypatch, rest)
        assert calls == []

    def test_first_notebook_lookup_triggers_scan_once(self, monkeypatch, rest):
        svc, calls = self._construct(monkeypatch, rest)

        svc._resolve_notebook_path("some_notebook")
        assert len(calls) == 1  # root listing (no subdirectories in fake)
        assert calls[0][1].get("path") == "/"

        svc._resolve_notebook_path("another")
        assert len(calls) == 1  # cache initialized once, not per lookup
