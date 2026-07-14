"""Unit tests for the SDP-mode bootstrap surface (Phase 2).

Covers the engine-selection gate in ``kindling.initialize`` internals and
per-pipe engine-config resolution. Full ``initialize_framework`` runs are
integration territory; these tests exercise the seams directly.
"""

import kindling
import pytest
from kindling_sdp.bootstrap import resolve_engine_config


class FakeConfigService:
    def __init__(self, values):
        self.values = values

    def get(self, key, default=None):
        return self.values.get(key, default)


class TestEngineSelection:
    def test_unknown_sdp_engine_fails_with_supported_list(self):
        with pytest.raises(ValueError, match="databricks_sdp.*not available"):
            kindling._activate_sdp_mode("databricks_sdp")

    def test_sdp_engine_activates_guard(self, monkeypatch):
        import kindling_sdp.bootstrap as sdp_bootstrap

        calls = []
        monkeypatch.setattr(sdp_bootstrap, "activate_sdp_mode", lambda: calls.append(True))

        kindling._activate_sdp_mode("sdp")

        assert calls == [True]


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
