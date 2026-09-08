import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

pytestmark = [pytest.mark.integration]


class FakeConf:
    def __init__(self, values=None):
        self.values = dict(values or {})

    def get(self, key, default=None):
        return self.values.get(key, default)

    def getAll(self):
        return dict(self.values)


class FakeSpark:
    def __init__(self, values=None):
        self.conf = FakeConf(values)


def _repo_root() -> Path:
    return Path(__file__).parents[2]


def _example_root() -> Path:
    return _repo_root() / "examples" / "lakeflow-telemetry"


def _load_pipeline_configuration(name: str = "telemetry_silver") -> dict:
    with (_example_root() / "databricks.yml").open(encoding="utf-8") as config_file:
        bundle = yaml.safe_load(config_file)
    return bundle["resources"]["pipelines"][name]["configuration"]


def _localize_config_files(configuration: dict) -> dict:
    from kindling_ext_databricks import lakeflow_app_selector as selector

    workspace_prefix = "/Workspace/Shared/kindling/lakeflow-telemetry/dev/"
    localized = dict(configuration)
    configured_files = json.loads(localized[selector.CANONICAL_CONFIG_FILES_CONFIG_KEY])
    localized_files = []
    for configured_file in configured_files:
        assert configured_file.startswith(workspace_prefix)
        localized_files.append(str(_example_root() / configured_file[len(workspace_prefix) :]))
    localized[selector.CANONICAL_CONFIG_FILES_CONFIG_KEY] = json.dumps(localized_files)
    return localized


def _initialize_config(initial_config: dict):
    from kindling.spark_config import DynaconfConfig

    service = DynaconfConfig()
    empty_spark = FakeSpark()
    with patch("kindling.spark_config.get_or_create_spark_session", return_value=empty_spark):
        service.initialize(
            config_files=initial_config["config_files"],
            initial_config=initial_config,
            environment=initial_config["environment"],
        )
    return service


def _resolved_values(initial_config: dict) -> dict:
    service = _initialize_config(initial_config)
    dataentities = service.get("dataentities")
    return {
        "platform": service.get("kindling.platform.environment"),
        "dataset_naming": service.get("kindling.sdp.dataset_naming"),
        "logging_level": service.get("kindling.telemetry.logging.level"),
        "table_schema": service.get("kindling.storage.table_schema"),
        "events_table": dataentities["silver.events"]["tags"]["provider.table_name"],
    }


def _example_lakeflow_initial_config(extra: dict | None = None) -> dict:
    from kindling_ext_databricks import lakeflow_app_selector as selector

    spark_config = _localize_config_files(_load_pipeline_configuration())
    if extra:
        spark_config.update(extra)
    return selector._pipeline_config_for_kindling(FakeSpark(spark_config), "telemetry")


def _example_core_initial_config(extra: dict | None = None) -> dict:
    from kindling.bootstrap import read_spark_kindling_config

    spark_config = _localize_config_files(_load_pipeline_configuration())
    if extra:
        spark_config.update(extra)
    config = read_spark_kindling_config(FakeSpark(spark_config))
    config["kindling.data_app"] = "telemetry"
    return config


def test_example_layers_resolve_in_documented_order():
    config = _example_lakeflow_initial_config()

    values = _resolved_values(config)

    assert values == {
        "platform": "databricks",
        "dataset_naming": "leaf",
        "logging_level": "CRITICAL",
        "table_schema": "cwmdp",
        "events_table": "dev_silver.cwmdp.events",
    }


def test_lakeflow_and_core_spark_kindling_precedence_match():
    spark_override = {"spark.kindling.telemetry.logging.level": '"SESSION"'}
    lakeflow = _example_lakeflow_initial_config(spark_override)
    core = _example_core_initial_config(spark_override)

    assert _resolved_values(lakeflow)["logging_level"] == "SESSION"
    assert _resolved_values(core)["logging_level"] == "SESSION"

    lakeflow["kindling.telemetry.logging.level"] = "BOOTSTRAP"
    core["kindling.telemetry.logging.level"] = "BOOTSTRAP"

    assert _resolved_values(lakeflow) == _resolved_values(core)
    assert _resolved_values(lakeflow)["logging_level"] == "BOOTSTRAP"


def test_example_dataentities_resolve_external_table_names():
    from kindling.data_entities import DataEntityManager
    from kindling.entity_resolution import ConfigDrivenEntityNameMapper

    config_service = _initialize_config(_example_lakeflow_initial_config())
    signal_provider = MagicMock()
    signal_provider.create_signal.return_value = MagicMock()
    manager = DataEntityManager(signal_provider=signal_provider, config_service=config_service)
    manager.apply_config_overrides(config_service)

    for entity_id, tier in (
        ("bronze.device_telemetry", "bronze"),
        ("silver.device_telemetry", "silver"),
        ("silver.events", "silver"),
        ("silver.episodes", "silver"),
    ):
        manager.register_entity(
            entityid=entity_id,
            name=entity_id.rsplit(".", 1)[-1],
            merge_columns=[],
            tags={"provider_type": "delta", "tier": tier},
            schema=None,
        )

    logger_provider = MagicMock()
    logger_provider.get_logger.return_value = MagicMock()
    mapper = ConfigDrivenEntityNameMapper(config_service, logger_provider)

    expected_names = {
        "bronze.device_telemetry": "dev_bronze.cwmdp.device_telemetry",
        "silver.device_telemetry": "dev_silver.cwmdp.device_telemetry",
        "silver.events": "dev_silver.cwmdp.events",
        "silver.episodes": "dev_silver.cwmdp.episodes",
    }
    for entity_id, expected in expected_names.items():
        entity = manager.get_entity_definition(entity_id)
        assert mapper.get_table_name(entity) == expected
        assert entity.tags["placement.source"] == "app"

    assert manager.get_entity_definition("silver.events").tags["retention.days"] == "14"
