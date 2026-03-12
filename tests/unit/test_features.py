from unittest.mock import MagicMock

from kindling.features import discover_runtime_features


class _ConfigStub:
    def __init__(self):
        self.values = {}

    def get(self, key, default=None):
        return self.values.get(key, default)

    def set(self, key, value):
        self.values[key] = value


class _Collectable:
    def __init__(self, rows):
        self._rows = rows

    def collect(self):
        return self._rows


def _make_spark(version: str, current_catalog: str, catalogs: list[str]) -> MagicMock:
    spark = MagicMock()
    spark.version = "3.5.0"

    def _sql(query: str):
        if query == "SELECT current_catalog() AS catalog":
            return _Collectable([[current_catalog]])
        if query == "SHOW CATALOGS":
            return _Collectable([[catalog] for catalog in catalogs])
        if query == "EXPLAIN ALTER TABLE `__kindling__nonexistent__` CLUSTER BY (`x`)":
            return _Collectable([])
        if query == "EXPLAIN ALTER TABLE `__kindling__nonexistent__` CLUSTER BY AUTO":
            return _Collectable([])
        raise AssertionError(f"Unexpected SQL: {query}")

    spark.sql.side_effect = _sql
    spark.conf.get.return_value = version
    return spark


def test_discover_runtime_features_sets_databricks_uc_and_volume_flags(monkeypatch):
    config = _ConfigStub()
    spark = _make_spark(
        version="15.4.x-scala2.12",
        current_catalog="main",
        catalogs=["main", "system"],
    )

    monkeypatch.setattr("kindling.features.get_or_create_spark_session", lambda: spark)
    monkeypatch.setattr("kindling.features._supports_databricks_volumes", lambda spark: True)

    discover_runtime_features(config)

    assert config.get("kindling.runtime.features.databricks.runtime_version") == "15.4.x-scala2.12"
    assert config.get("kindling.runtime.features.databricks.uc_enabled") is True
    assert config.get("kindling.runtime.features.databricks.volumes_enabled") is True
    assert (
        config.get("kindling.runtime.features.databricks.any_file_required_for_bootstrap") is False
    )
    assert config.get("kindling.runtime.features.databricks.name_mode_catalog_qualified") is True


def test_discover_runtime_features_sets_databricks_classic_flags(monkeypatch):
    config = _ConfigStub()
    spark = _make_spark(
        version="13.3.x-scala2.12",
        current_catalog="spark_catalog",
        catalogs=["spark_catalog"],
    )

    monkeypatch.setattr("kindling.features.get_or_create_spark_session", lambda: spark)
    monkeypatch.setattr("kindling.features._supports_databricks_volumes", lambda spark: False)

    discover_runtime_features(config)

    assert config.get("kindling.runtime.features.databricks.runtime_version") == "13.3.x-scala2.12"
    assert config.get("kindling.runtime.features.databricks.uc_enabled") is False
    assert config.get("kindling.runtime.features.databricks.volumes_enabled") is False
    assert (
        config.get("kindling.runtime.features.databricks.any_file_required_for_bootstrap") is True
    )
    assert config.get("kindling.runtime.features.databricks.name_mode_catalog_qualified") is False
