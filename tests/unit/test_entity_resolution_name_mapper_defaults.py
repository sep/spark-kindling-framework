from unittest.mock import MagicMock, patch

from kindling.entity_resolution import ConfigDrivenEntityNameMapper
from kindling.spark_config import ConfigService


def _make_mapper_with_no_config():
    config = MagicMock(spec=ConfigService)
    config.get.return_value = None

    logger_provider = MagicMock()
    logger_provider.get_logger.return_value = MagicMock()
    return ConfigDrivenEntityNameMapper(config, logger_provider)


def test_name_mapper_without_config_treats_three_part_entityid_as_qualified():
    mapper = _make_mapper_with_no_config()

    entity = MagicMock()
    entity.tags = {}
    entity.entityid = "iot_telemetry.event_hub_raw.raw_telemetry_events"

    assert mapper.get_table_name(entity) == "iot_telemetry.event_hub_raw.raw_telemetry_events"


def test_name_mapper_without_config_treats_two_part_entityid_as_schema_table_with_default_catalog():
    mapper = _make_mapper_with_no_config()

    entity = MagicMock()
    entity.tags = {}
    entity.entityid = "event_hub_raw.raw_telemetry_events"

    with patch(
        "kindling.entity_resolution._get_current_namespace", return_value=("main", "default")
    ):
        assert mapper.get_table_name(entity) == "main.event_hub_raw.raw_telemetry_events"


def test_name_mapper_without_config_does_not_require_default_catalog_for_two_part_names():
    mapper = _make_mapper_with_no_config()

    entity = MagicMock()
    entity.tags = {}
    entity.entityid = "event_hub_raw.raw_telemetry_events"

    with patch("kindling.entity_resolution._get_current_namespace", return_value=(None, None)):
        assert mapper.get_table_name(entity) == "event_hub_raw.raw_telemetry_events"
