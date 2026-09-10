"""External table resolution must use runtime metadata, not pipeline names."""

from unittest.mock import MagicMock

import pytest
from kindling.data_entities import EntityNameMapper
from kindling.entity_resolution import ConfigDrivenEntityNameMapper
from kindling.injection import GlobalInjector
from kindling_ext_databricks import DatabricksSdpEngine
from kindling_ext_sdp import OssSdpEngine

from tests.unit.test_sdp_oss_engine import (
    FakeDpModule,
    FakeEntityRegistry,
    FakePipeRegistry,
    make_entity,
    make_pipe,
)


@pytest.mark.parametrize("engine_class", [OssSdpEngine, DatabricksSdpEngine])
@pytest.mark.parametrize(
    "extra_tags, expected_table",
    [
        ({}, "raw_catalog.iot.bronze_device_telemetry"),
        ({"provider.table_name_strategy": "leaf"}, "raw_catalog.iot.device_telemetry"),
        (
            {
                "provider.table_name_strategy": "leaf",
                "provider.table_name": "archive_catalog.history.telemetry_v2",
            },
            "archive_catalog.history.telemetry_v2",
        ),
    ],
)
def test_default_external_delta_read_uses_registered_mapper(
    engine_class, extra_tags, expected_table, monkeypatch
):
    entity = make_entity(
        "bronze.device_telemetry",
        tags={
            "provider_type": "delta",
            "provider.table_catalog": "raw_catalog",
            "provider.table_schema": "iot",
            **extra_tags,
        },
    )
    entities = FakeEntityRegistry([entity, make_entity("silver.telemetry")])
    captured = {}
    pipes = FakePipeRegistry(
        [
            make_pipe(
                "clean.telemetry",
                [entity.entityid],
                "silver.telemetry",
                execute=lambda **frames: captured.update(frames) or "result",
            )
        ]
    )
    config = MagicMock()
    config.get.side_effect = {
        "kindling.storage.table_catalog": "pipeline_catalog",
        "kindling.storage.table_schema": "curated",
    }.get
    mapper = ConfigDrivenEntityNameMapper(config, MagicMock())
    mapper.get_table_name = MagicMock(wraps=mapper.get_table_name)
    requests = []

    def get_service(cls):
        assert cls is EntityNameMapper
        requests.append(cls)
        return mapper

    monkeypatch.setattr(GlobalInjector, "get", get_service)
    spark = MagicMock()
    spark.catalog.currentCatalog.return_value = "pipeline_catalog"
    spark.table.return_value = "external-dataframe"
    dp = FakeDpModule()
    engine = engine_class(
        entities,
        pipes,
        dp_module=dp,
        session_provider=lambda: spark,
        provider_resolver=lambda entity: None,
        dataset_naming="leaf",
    )
    plan = engine.build_plan()
    engine.declare_pipeline(plan)

    # Resolution is deferred until evaluation and does not mutate the plan API.
    assert requests == []
    assert plan.datasets[0].inputs[0].entity_id == "bronze.device_telemetry"
    assert dp.declared["telemetry"]() == "result"
    mapper.get_table_name.assert_called_once_with(entity)
    spark.table.assert_called_once_with(expected_table)
    spark.readStream.table.assert_not_called()
    assert captured == {"bronze_device_telemetry": "external-dataframe"}
    assert entity.entityid == "bronze.device_telemetry"


def test_missing_external_metadata_fails_without_reading_logical_id():
    entity = make_entity("bronze.telemetry")
    entities = FakeEntityRegistry([entity, make_entity("silver.telemetry")])
    pipes = FakePipeRegistry([make_pipe("clean", [entity.entityid], "silver.telemetry")])
    spark = MagicMock()
    engine = OssSdpEngine(
        entities, pipes, session_provider=lambda: spark, provider_resolver=lambda entity: None
    )
    dataset_function = engine._build_dataset_function(engine.build_plan().datasets[0])
    del entities.registry[entity.entityid]

    with pytest.raises(RuntimeError, match="External entity 'bronze.telemetry' is not registered"):
        dataset_function()
    spark.table.assert_not_called()
