"""Unit tests for the engine-extension seam and SDP's implementation of it.

Kindling core knows only the generic contract — ``engine="<name>"`` →
    import ``kindling_ext_<name>`` → ``engine_extension()`` factory → object with
``activate()`` / ``owns_incrementality`` / optional ``declare_pipeline``.
These tests exercise that seam and kindling_ext_sdp's implementation of it;
full ``initialize_framework`` runs are integration territory.
"""

import kindling
import pytest
from kindling_ext_sdp.bootstrap import resolve_engine_config
from kindling_ext_sdp.engine_extension import SdpEngineExtension


class FakeConfigService:
    def __init__(self, values):
        self.values = values

    def get(self, key, default=None):
        return self.values.get(key, default)


class TestEngineExtensionSeam:
    """Core's extension loader and its Databricks umbrella mapping."""

    def test_unknown_engine_fails_naming_the_extension_module(self):
        with pytest.raises(ImportError, match="kindling_ext_flink"):
            kindling._load_engine_extension("flink")

    def test_module_without_factory_is_rejected(self, monkeypatch):
        import sys
        from types import ModuleType

        monkeypatch.setitem(
            sys.modules, "kindling_ext_notanengine", ModuleType("kindling_ext_notanengine")
        )

        with pytest.raises(TypeError, match="engine_extension"):
            kindling._load_engine_extension("notanengine")

    def test_loading_is_side_effect_free_activation_is_deferred(self, monkeypatch):
        import kindling_ext_sdp.bootstrap as sdp_bootstrap

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
    """kindling_ext_sdp's side of the contract."""

    def test_resolves_via_the_naming_convention(self):
        extension = kindling._load_engine_extension("sdp")

        assert isinstance(extension, SdpEngineExtension)

    def test_owns_incrementality_so_core_skips_the_watermark_aspect(self):
        assert SdpEngineExtension().owns_incrementality is True

    def test_declare_pipeline_delegates_to_sdp_bootstrap(self, monkeypatch):
        import kindling_ext_sdp.bootstrap as sdp_bootstrap

        received = {}

        def fake_declare(pipe_ids=None):
            received["pipe_ids"] = pipe_ids
            return "plan"

        monkeypatch.setattr(sdp_bootstrap, "declare_pipeline", fake_declare)

        assert SdpEngineExtension().declare_pipeline(["p1"]) == "plan"
        assert received == {"pipe_ids": ["p1"]}

    def test_activate_sdp_mode_installs_write_guard(self, monkeypatch):
        from unittest.mock import MagicMock

        from kindling.injection import GlobalInjector
        from kindling_ext_sdp.bootstrap import activate_sdp_mode
        from kindling_ext_sdp.guard_provider import SdpWriteGuardProvider

        registry = MagicMock()
        monkeypatch.setattr(GlobalInjector, "get", lambda _iface: registry)

        activate_sdp_mode()

        registry.set_provider_decorator.assert_called_once_with(SdpWriteGuardProvider)


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


@pytest.mark.parametrize("engine_name", ["sdp", "databricks_sdp"])
@pytest.mark.parametrize(
    "configured_mode, expected_mode",
    [
        ("absent", "normalized"),
        (None, "normalized"),
        ("leaf", "leaf"),
        (" Leaf ", "leaf"),
        ("NORMALIZED", "normalized"),
    ],
)
def test_bootstrap_resolves_dataset_naming_from_config(
    monkeypatch, engine_name, configured_mode, expected_mode
):
    from unittest.mock import MagicMock

    from kindling.data_entities import DataEntityRegistry
    from kindling.data_pipes import DataPipesRegistry
    from kindling.injection import GlobalInjector
    from kindling.spark_config import ConfigService
    from kindling_ext_databricks import DatabricksSdpEngine
    from kindling_ext_sdp.bootstrap import declare_pipeline
    from kindling_ext_sdp.oss_engine import OssSdpEngine

    values = {} if configured_mode == "absent" else {"kindling.sdp.dataset_naming": configured_mode}
    pipes = MagicMock()
    pipes.get_pipe_ids.return_value = []
    services = {
        DataEntityRegistry: MagicMock(),
        DataPipesRegistry: pipes,
        ConfigService: FakeConfigService(values),
    }
    monkeypatch.setattr(GlobalInjector, "get", services.__getitem__)
    engine_class = OssSdpEngine if engine_name == "sdp" else DatabricksSdpEngine
    engines = []

    def factory(*args, **kwargs):
        engine = engine_class(*args, **kwargs)
        engines.append(engine)
        return engine

    declare_pipeline(engine_factory=factory, dp_module=MagicMock())
    assert engines[0].dataset_name.mode == expected_mode
    expected_name = "device_telemetry" if expected_mode == "leaf" else "silver_device_telemetry"
    assert engines[0].dataset_name("silver.device_telemetry") == expected_name


def test_invalid_dataset_naming_reports_existing_declaration_issue(monkeypatch):
    from unittest.mock import MagicMock

    from kindling.data_entities import DataEntityRegistry
    from kindling.data_pipes import DataPipesRegistry
    from kindling.injection import GlobalInjector
    from kindling.spark_config import ConfigService
    from kindling_ext_sdp.bootstrap import declare_pipeline
    from kindling_ext_sdp.declaration_plan import DeclarationValidationError

    pipes = MagicMock()
    pipes.get_pipe_ids.return_value = []
    services = {
        DataEntityRegistry: MagicMock(),
        DataPipesRegistry: pipes,
        ConfigService: FakeConfigService({"kindling.sdp.dataset_naming": "catalog"}),
    }
    monkeypatch.setattr(GlobalInjector, "get", services.__getitem__)

    with pytest.raises(DeclarationValidationError) as exc_info:
        declare_pipeline(dp_module=MagicMock())

    assert [(issue.pipe_id, issue.code) for issue in exc_info.value.issues] == [
        ("<pipeline>", "invalid_dataset_naming")
    ]


def test_sdp_dataset_naming_is_independent_of_core_entity_name_mapper():
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    from kindling.entity_resolution import ConfigDrivenEntityNameMapper
    from kindling_ext_sdp.declaration_plan import DatasetNameMapper

    logger_provider = MagicMock()
    logger_provider.get_logger.return_value = MagicMock()
    core_mapper = ConfigDrivenEntityNameMapper(
        FakeConfigService({"kindling.storage.table_schema": "published"}),
        logger_provider,
    )

    assert DatasetNameMapper("leaf")("silver.device_telemetry") == "device_telemetry"
    assert (
        core_mapper.get_table_name(SimpleNamespace(entityid="silver.device_telemetry", tags={}))
        == "published.silver_device_telemetry"
    )
