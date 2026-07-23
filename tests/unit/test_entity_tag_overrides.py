"""Run-scoped entity tag overrides (JIT parameters).

run_datapipes(..., entity_tags=...) applies per-entity tag overrides for one
run and restores them — the parameter channel for per-run values like a
provider read window in a backfill loop, replacing durable
ConfigService.set_entity_tags mutation inside loops.
"""

from unittest.mock import MagicMock, patch

import pytest

from kindling.data_entities import DataEntityManager
from kindling.data_pipes import DataPipesExecuter


def _manager(config_service=None):
    return DataEntityManager(signal_provider=None, config_service=config_service)


def _register(manager, entityid="adx.telemetry", tags=None):
    manager.register_entity(
        entityid,
        name=entityid.split(".")[-1],
        merge_columns=[],
        tags=tags or {"provider_type": "adx-api", "provider.database": "db"},
        schema=None,
    )


class TestRegistryTagOverrides:
    def test_overlay_wins_and_restores(self):
        manager = _manager()
        _register(manager)

        with manager.tag_overrides({"adx.telemetry": {"provider.query": "T | take 1"}}):
            inside = manager.get_entity_definition("adx.telemetry")
            assert inside.tags["provider.query"] == "T | take 1"
            assert inside.tags["provider.database"] == "db"  # declared tags kept

        after = manager.get_entity_definition("adx.telemetry")
        assert "provider.query" not in after.tags

    def test_overlay_wins_over_config_overrides(self):
        config = MagicMock()
        config.get_entity_tags.return_value = {"provider.query": "from-config"}
        manager = _manager(config_service=config)
        _register(manager)

        with manager.tag_overrides({"adx.telemetry": {"provider.query": "from-run"}}):
            assert (
                manager.get_entity_definition("adx.telemetry").tags["provider.query"] == "from-run"
            )
        assert (
            manager.get_entity_definition("adx.telemetry").tags["provider.query"] == "from-config"
        )

    def test_none_and_empty_are_noops(self):
        manager = _manager()
        _register(manager)
        with manager.tag_overrides(None):
            assert manager.get_entity_definition("adx.telemetry").tags["provider.database"] == "db"
        with manager.tag_overrides({}):
            pass

    def test_nested_overrides_merge_and_unwind(self):
        manager = _manager()
        _register(manager)

        with manager.tag_overrides({"adx.telemetry": {"a": "1"}}):
            with manager.tag_overrides({"adx.telemetry": {"b": "2"}}):
                tags = manager.get_entity_definition("adx.telemetry").tags
                assert (tags["a"], tags["b"]) == ("1", "2")
            tags = manager.get_entity_definition("adx.telemetry").tags
            assert tags["a"] == "1" and "b" not in tags

    def test_registered_entity_object_never_mutated(self):
        manager = _manager()
        _register(manager)
        with manager.tag_overrides({"adx.telemetry": {"x": "y"}}):
            manager.get_entity_definition("adx.telemetry")
        assert "x" not in manager.registry["adx.telemetry"].tags


class TestRunDatapipesEntityTags:
    def _executer(self, manager):
        lp = MagicMock()
        lp.get_logger.return_value = MagicMock()
        erps = MagicMock()
        dpr = MagicMock()
        return DataPipesExecuter(
            lp=lp, dpe=manager, dpr=dpr, erps=erps, tp=MagicMock(), signal_provider=None
        )

    def test_overrides_visible_during_run_and_restored(self):
        manager = _manager()
        _register(manager)
        executer = self._executer(manager)

        seen = {}

        def _fake_pipe_execution(pipes, no_watermark=False):
            seen["tags"] = dict(manager.get_entity_definition("adx.telemetry").tags)

        with patch.object(executer, "_run_datapipes_sequential", _fake_pipe_execution):
            executer.run_datapipes(
                ["some.pipe"],
                entity_tags={"adx.telemetry": {"provider.start": "2026-07-01T00:00:00"}},
            )

        assert seen["tags"]["provider.start"] == "2026-07-01T00:00:00"
        assert "provider.start" not in manager.get_entity_definition("adx.telemetry").tags

    def test_no_overrides_short_circuits(self):
        manager = _manager()
        _register(manager)
        executer = self._executer(manager)
        with patch.object(executer, "_run_datapipes_sequential", lambda *a, **kw: "ran") as _:
            assert executer.run_datapipes(["p"]) == "ran"
