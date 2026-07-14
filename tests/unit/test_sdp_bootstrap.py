"""Unit tests for the engine-extension seam and SDP's implementation of it.

Kindling core knows only the generic contract — ``engine="<name>"`` →
import ``kindling_<name>`` → ``engine_extension()`` factory → object with
``activate()`` / ``owns_incrementality`` / optional ``declare_pipeline``.
These tests exercise that seam and kindling_sdp's implementation of it;
full ``initialize_framework`` runs are integration territory.
"""

import kindling
import pytest
from kindling_sdp.bootstrap import resolve_engine_config
from kindling_sdp.engine_extension import SdpEngineExtension


class FakeConfigService:
    def __init__(self, values):
        self.values = values

    def get(self, key, default=None):
        return self.values.get(key, default)


class TestEngineExtensionSeam:
    """Core's generic loader — no engine names hardcoded anywhere in core."""

    def test_unknown_engine_fails_naming_the_extension_module(self):
        with pytest.raises(ImportError, match="kindling_flink"):
            kindling._load_engine_extension("flink")

    def test_module_without_factory_is_rejected(self, monkeypatch):
        import sys
        from types import ModuleType

        monkeypatch.setitem(sys.modules, "kindling_notanengine", ModuleType("kindling_notanengine"))

        with pytest.raises(TypeError, match="engine_extension"):
            kindling._load_engine_extension("notanengine")

    def test_loading_is_side_effect_free_activation_is_deferred(self, monkeypatch):
        import kindling_sdp.bootstrap as sdp_bootstrap

        calls = []
        monkeypatch.setattr(sdp_bootstrap, "activate_sdp_mode", lambda: calls.append(True))

        extension = kindling._load_engine_extension("sdp")

        assert calls == [], "loading must not activate"
        extension.activate()
        assert calls == [True]

    def test_declare_pipeline_requires_an_active_engine(self, monkeypatch):
        monkeypatch.setattr(kindling, "_active_engine_extension", None)

        with pytest.raises(RuntimeError, match="initialize\\(engine="):
            kindling.declare_pipeline()

    def test_declare_pipeline_delegates_to_the_active_extension(self, monkeypatch):
        class FakeExtension:
            def declare_pipeline(self, pipe_ids=None):
                return ("declared", pipe_ids)

        monkeypatch.setattr(kindling, "_active_engine_extension", FakeExtension())

        assert kindling.declare_pipeline(["a.pipe"]) == ("declared", ["a.pipe"])


class TestSdpEngineExtension:
    """kindling_sdp's side of the contract."""

    def test_resolves_via_the_naming_convention(self):
        extension = kindling._load_engine_extension("sdp")

        assert isinstance(extension, SdpEngineExtension)

    def test_owns_incrementality_so_core_skips_the_watermark_aspect(self):
        assert SdpEngineExtension().owns_incrementality is True

    def test_declare_pipeline_delegates_to_sdp_bootstrap(self, monkeypatch):
        import kindling_sdp.bootstrap as sdp_bootstrap

        received = {}

        def fake_declare(pipe_ids=None):
            received["pipe_ids"] = pipe_ids
            return "plan"

        monkeypatch.setattr(sdp_bootstrap, "declare_pipeline", fake_declare)

        assert SdpEngineExtension().declare_pipeline(["p1"]) == "plan"
        assert received == {"pipe_ids": ["p1"]}


class TestEngineConfigResolution:
    def test_resolves_only_pipes_with_engine_blocks(self):
        config = FakeConfigService(
            {
                "datapipes.silver.orders.engine": {
                    "sdp": {"dataset_type": "materialized_view"},
                    "databricks_sdp": {"refresh_policy": "incremental"},
                }
            }
        )

        resolved = resolve_engine_config(config, ["silver.orders", "gold.orders"])

        assert set(resolved) == {"silver.orders"}
        assert resolved["silver.orders"]["sdp"] == {"dataset_type": "materialized_view"}
