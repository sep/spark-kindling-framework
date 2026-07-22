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


def test_discovery_false_skips_probes_and_assumes_modern_databricks(monkeypatch):
    config = _ConfigStub()
    config.set("kindling.features.discovery", "false")

    def _no_session():
        raise AssertionError("discovery disabled must not touch Spark")

    monkeypatch.setattr("kindling.features.get_or_create_spark_session", _no_session)

    discover_runtime_features(config)

    assert config.get("kindling.runtime.features.databricks.uc_enabled") is True
    assert config.get("kindling.runtime.features.databricks.volumes_enabled") is True
    assert (
        config.get("kindling.runtime.features.databricks.any_file_required_for_bootstrap") is False
    )
    assert config.get("kindling.runtime.features.delta.cluster_by") is True
    assert config.get("kindling.runtime.features.delta.auto_clustering") is True


def test_discovery_false_defaults_yield_to_static_overrides(monkeypatch):
    from kindling.features import get_feature_bool

    config = _ConfigStub()
    config.set("kindling.features.discovery", "false")
    config.set("kindling.features.databricks.uc_enabled", "false")  # static wins

    monkeypatch.setattr(
        "kindling.features.get_or_create_spark_session",
        lambda: (_ for _ in ()).throw(AssertionError("no spark")),
    )
    discover_runtime_features(config)

    assert get_feature_bool(config, "databricks.uc_enabled") is False


def test_uc_detection_short_circuits_show_catalogs():
    """A conclusive current_catalog() must not trigger the expensive
    SHOW CATALOGS metastore enumeration."""
    from kindling.features import _detect_databricks_uc_enabled

    spark = MagicMock()

    def _sql(query: str):
        if query == "SELECT current_catalog() AS catalog":
            return _Collectable([["main"]])
        raise AssertionError(f"Unexpected SQL after short-circuit: {query}")

    spark.sql.side_effect = _sql

    assert _detect_databricks_uc_enabled(spark) is True
    assert spark.sql.call_count == 1


def test_uc_detection_falls_back_to_show_catalogs_on_legacy_current():
    from kindling.features import _detect_databricks_uc_enabled

    spark = MagicMock()

    def _sql(query: str):
        if query == "SELECT current_catalog() AS catalog":
            return _Collectable([["spark_catalog"]])
        if query == "SHOW CATALOGS":
            return _Collectable([["spark_catalog"], ["main"]])
        raise AssertionError(f"Unexpected SQL: {query}")

    spark.sql.side_effect = _sql

    assert _detect_databricks_uc_enabled(spark) is True
    assert spark.sql.call_count == 2


def test_runtime_version_regex_parses_major_minor(monkeypatch):
    """Regression: the version regex previously used escaped backslashes and
    never matched, so runtime_major/minor were silently unset."""
    config = _ConfigStub()
    spark = _make_spark(
        version="15.4.x-scala2.12",
        current_catalog="main",
        catalogs=["main"],
    )
    monkeypatch.setattr("kindling.features.get_or_create_spark_session", lambda: spark)
    monkeypatch.setattr("kindling.features._supports_databricks_volumes", lambda spark: True)

    discover_runtime_features(config)

    assert config.get("kindling.runtime.features.databricks.runtime_major") == 15
    assert config.get("kindling.runtime.features.databricks.runtime_minor") == 4
    # >= 15.2 with a parsing engine: AUTO clustering now detectable
    assert config.get("kindling.runtime.features.delta.auto_clustering") is True
