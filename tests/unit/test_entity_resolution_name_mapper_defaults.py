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


def _make_mapper_with_config(values: dict):
    config = MagicMock(spec=ConfigService)
    config.get.side_effect = lambda key, default=None: values.get(key, default)

    logger_provider = MagicMock()
    logger_provider.get_logger.return_value = MagicMock()
    return ConfigDrivenEntityNameMapper(config, logger_provider)


def _entity(entity_id: str):
    entity = MagicMock()
    entity.tags = {}
    entity.entityid = entity_id
    return entity


def _entity_with_tags(entity_id: str, tags: dict):
    entity = MagicMock()
    entity.tags = tags
    entity.entityid = entity_id
    return entity


class TestCatalogOnlyConfig:
    """Regression tests for the catalog-dropped-silently bug.

    Previously, configuring `kindling.storage.table_catalog` without also
    setting `kindling.storage.table_schema` silently dropped the catalog
    entirely: `if catalog and schema` was False and `if schema` was also
    False, so the code fell through to a bare, unqualified leaf name. There
    was no way to configure "just a catalog" and have it take effect.
    """

    def test_catalog_only_qualifies_one_part_entity_id(self):
        mapper = _make_mapper_with_config({"kindling.storage.table_catalog": "maincat"})

        assert mapper.get_table_name(_entity("orders")) == "maincat.orders"

    def test_catalog_only_preserves_two_part_entity_id_structure(self):
        """A 2-part entity id already reads as schema.table; the configured
        catalog should be layered on top rather than flattening the whole id
        into a single leaf under the catalog."""
        mapper = _make_mapper_with_config({"kindling.storage.table_catalog": "maincat"})

        assert (
            mapper.get_table_name(_entity("staging.device_telemetry"))
            == "maincat.staging.device_telemetry"
        )

    def test_catalog_only_overrides_leading_segment_of_three_part_entity_id(self):
        """A 3-part entity id is already fully-qualified; the configured
        catalog overrides its own leading segment while the trailing
        schema.table is preserved."""
        mapper = _make_mapper_with_config({"kindling.storage.table_catalog": "maincat"})

        assert (
            mapper.get_table_name(_entity("other_cat.staging.device_telemetry"))
            == "maincat.staging.device_telemetry"
        )

    def test_catalog_only_normalises_hyphens_in_trailing_table_segment(self):
        mapper = _make_mapper_with_config({"kindling.storage.table_catalog": "maincat"})

        assert (
            mapper.get_table_name(_entity("staging.device-telemetry-abc"))
            == "maincat.staging.device_telemetry_abc"
        )

    def test_catalog_only_applies_table_name_prefix_to_trailing_table_segment(self):
        mapper = _make_mapper_with_config(
            {
                "kindling.storage.table_catalog": "maincat",
                "kindling.storage.table_name_prefix": "pfx_",
            }
        )

        assert (
            mapper.get_table_name(_entity("staging.device_telemetry"))
            == "maincat.staging.pfx_device_telemetry"
        )

    def test_catalog_only_applies_table_name_prefix_to_one_part_entity_id(self):
        mapper = _make_mapper_with_config(
            {
                "kindling.storage.table_catalog": "maincat",
                "kindling.storage.table_name_prefix": "pfx_",
            }
        )

        assert mapper.get_table_name(_entity("orders")) == "maincat.pfx_orders"


class TestCatalogAndSchemaFlattenRegression:
    """Locks in the existing, documented, and tested flattening behavior for
    the case where `table_schema` is configured (with or without
    `table_catalog`). This is deliberately NOT changed by the catalog-only
    fix above, since existing deployments/tests rely on the entity id being
    flattened into a single leaf underneath the configured namespace."""

    def test_catalog_and_schema_both_set_flattens_dotted_entity_id(self):
        mapper = _make_mapper_with_config(
            {
                "kindling.storage.table_catalog": "maincat",
                "kindling.storage.table_schema": "analytics",
            }
        )

        assert (
            mapper.get_table_name(_entity("staging.device_telemetry"))
            == "maincat.analytics.staging_device_telemetry"
        )

    def test_schema_only_set_flattens_dotted_entity_id(self):
        mapper = _make_mapper_with_config({"kindling.storage.table_schema": "analytics"})

        assert (
            mapper.get_table_name(_entity("staging.device_telemetry"))
            == "analytics.staging_device_telemetry"
        )

    def test_catalog_and_schema_both_set_applies_prefix_to_flattened_leaf(self):
        mapper = _make_mapper_with_config(
            {
                "kindling.storage.table_catalog": "maincat",
                "kindling.storage.table_schema": "analytics",
                "kindling.storage.table_name_prefix": "pfx_",
            }
        )

        assert (
            mapper.get_table_name(_entity("staging.device_telemetry"))
            == "maincat.analytics.pfx_staging_device_telemetry"
        )


class TestPerEntityTagOverride:
    """`provider.table_catalog` / `provider.table_schema` entity tags override
    (and, if no other namespace config is present, alone trigger) namespace
    resolution -- the per-entity counterpart to global config, same
    precedence tier as the existing `provider.table_name` tag."""

    def test_provider_table_catalog_tag_applies_with_no_other_config(self):
        mapper = _make_mapper_with_config({})

        entity = _entity_with_tags("staging.device_telemetry", {"provider.table_catalog": "dev"})

        assert mapper.get_table_name(entity) == "dev.staging.device_telemetry"

    def test_provider_table_catalog_tag_overrides_global_config(self):
        mapper = _make_mapper_with_config({"kindling.storage.table_catalog": "globalcat"})

        entity = _entity_with_tags(
            "staging.device_telemetry", {"provider.table_catalog": "entitycat"}
        )

        assert mapper.get_table_name(entity) == "entitycat.staging.device_telemetry"

    def test_provider_table_schema_tag_flattens_like_global_schema_config(self):
        mapper = _make_mapper_with_config({})

        entity = _entity_with_tags(
            "staging.device_telemetry", {"provider.table_schema": "analytics"}
        )

        assert mapper.get_table_name(entity) == "analytics.staging_device_telemetry"

    def test_provider_table_name_tag_still_wins_over_catalog_tag(self):
        mapper = _make_mapper_with_config({})

        entity = _entity_with_tags(
            "staging.device_telemetry",
            {"provider.table_catalog": "dev", "provider.table_name": "explicit.table"},
        )

        assert mapper.get_table_name(entity) == "explicit.table"
