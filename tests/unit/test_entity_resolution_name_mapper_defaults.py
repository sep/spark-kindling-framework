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


def test_name_mapper_without_config_does_not_prefix_spark_catalog_for_two_part_names():
    mapper = _make_mapper_with_no_config()

    entity = MagicMock()
    entity.tags = {}
    entity.entityid = "event_hub_raw.raw_telemetry_events"

    # spark_catalog is the built-in Hive catalog — not a real UC catalog.
    # Entity IDs must stay as schema.table, not spark_catalog.schema.table.
    with patch(
        "kindling.entity_resolution._get_current_namespace",
        return_value=("spark_catalog", "default"),
    ):
        assert mapper.get_table_name(entity) == "event_hub_raw.raw_telemetry_events"


def _make_mapper_with_volume_root(volume_root: str):
    config = MagicMock(spec=ConfigService)

    def _cfg_get(key, default=None):
        if key == "kindling.storage.table_root":
            return volume_root
        return None

    config.get.side_effect = _cfg_get

    logger_provider = MagicMock()
    logger_provider.get_logger.return_value = MagicMock()
    return ConfigDrivenEntityNameMapper(config, logger_provider)


def test_name_mapper_qualifies_one_part_name_when_table_root_is_volume_path():
    """1-part entity IDs must get a 3-part name when table_root is a Volume path.

    Databricks UC resolves unqualified names against the session's current catalog,
    which can differ between saveAsTable (write) and spark.read.table (read).
    A fully-qualified name avoids the mismatch.
    """
    mapper = _make_mapper_with_volume_root(
        "/Volumes/kindling/kindling/artifacts/ci-tests/abc123/tables"
    )

    entity = MagicMock()
    entity.tags = {}
    entity.entityid = "test_static_entity_c6eb1567"

    assert mapper.get_table_name(entity) == "kindling.kindling.test_static_entity_c6eb1567"


def test_name_mapper_normalises_hyphens_in_leaf_when_table_root_is_volume_path():
    mapper = _make_mapper_with_volume_root("/Volumes/mycatalog/myschema/data")

    entity = MagicMock()
    entity.tags = {}
    entity.entityid = "my-entity-001"

    assert mapper.get_table_name(entity) == "mycatalog.myschema.my_entity_001"


def test_name_mapper_does_not_qualify_one_part_name_when_table_root_is_not_volume():
    """Non-Volume table_root must leave 1-part names unchanged (backward compat)."""
    mapper = _make_mapper_with_volume_root("Tables/myschema")

    entity = MagicMock()
    entity.tags = {}
    entity.entityid = "plain_entity"

    assert mapper.get_table_name(entity) == "plain_entity"


def test_name_mapper_volume_inference_does_not_affect_three_part_names():
    """Explicit 3-part names must pass through even when table_root is a Volume path."""
    mapper = _make_mapper_with_volume_root("/Volumes/kindling/kindling/data")

    entity = MagicMock()
    entity.tags = {}
    entity.entityid = "other.catalog.my_entity"

    assert mapper.get_table_name(entity) == "other.catalog.my_entity"
