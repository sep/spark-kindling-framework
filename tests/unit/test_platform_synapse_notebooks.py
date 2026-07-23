"""Unit tests for SynapseService notebook conversion.

Regression: _convert_to_synapse_notebook must build azure.synapse.artifacts
model types — kindling.notebook_framework's same-named classes shadow them via
star import, and the ArtifactsClient serializer only understands its own
generated models. The old code built kindling's classes and crashed before
reaching the API (NotebookMetadata(**NotebookMetadata) TypeError).
"""

from unittest.mock import MagicMock

import pytest

synapse_models = pytest.importorskip("azure.synapse.artifacts").models

from kindling.platform_synapse import SynapseService  # noqa: E402


def _service():
    svc = SynapseService.__new__(SynapseService)
    svc.logger = MagicMock()
    return svc


def _jupyter_doc():
    return {
        "nbformat": 4,
        "nbformat_minor": 2,
        "metadata": {"kernelspec": {"name": "python3"}},
        "cells": [
            {"cell_type": "markdown", "source": ["# Title"], "metadata": {}},
            {"cell_type": "code", "source": ["x = 1\n", "print(x)"], "metadata": {}},
        ],
    }


class TestConvertToSynapseNotebook:
    def test_builds_azure_sdk_models(self):
        resource = _service()._convert_to_synapse_notebook("nb", {"properties": _jupyter_doc()})

        assert isinstance(resource, synapse_models.NotebookResource)
        assert isinstance(resource.properties, synapse_models.Notebook)
        assert all(
            isinstance(cell, synapse_models.NotebookCell) for cell in resource.properties.cells
        )
        assert isinstance(resource.properties.metadata, synapse_models.NotebookMetadata)

    def test_accepts_bare_jupyter_document(self):
        resource = _service()._convert_to_synapse_notebook("nb", _jupyter_doc())

        assert resource.name == "nb"
        assert [c.cell_type for c in resource.properties.cells] == ["markdown", "code"]
        assert resource.properties.nbformat == 4

    def test_serializes_through_sdk(self):
        """The produced resource must survive the SDK's own serializer —
        the contract the old kindling-class version violated."""
        from azure.synapse.artifacts._serialization import Serializer

        resource = _service()._convert_to_synapse_notebook("nb", _jupyter_doc())
        body = Serializer(
            {k: v for k, v in vars(synapse_models).items() if isinstance(v, type)}
        ).body(resource, "NotebookResource")

        assert body["name"] == "nb"
        assert len(body["properties"]["cells"]) == 2

    def test_string_source_is_split_into_lines(self):
        doc = {"cells": [{"cell_type": "code", "source": "a = 1\nb = 2", "metadata": {}}]}
        resource = _service()._convert_to_synapse_notebook("nb", doc)

        assert resource.properties.cells[0].source == ["a = 1\n", "b = 2"]

    def test_markdown_cells_have_no_outputs(self):
        resource = _service()._convert_to_synapse_notebook("nb", _jupyter_doc())
        markdown, code = resource.properties.cells

        assert markdown.outputs is None
        assert code.outputs == []

    def test_folder_only_when_named(self):
        doc = _jupyter_doc()
        resource = _service()._convert_to_synapse_notebook("nb", {"properties": doc})
        assert resource.properties.folder is None

        doc["folder"] = {"name": "apps/kindling"}
        resource = _service()._convert_to_synapse_notebook("nb", {"properties": doc})
        assert resource.properties.folder.name == "apps/kindling"
