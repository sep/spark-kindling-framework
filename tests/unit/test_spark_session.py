import sys
import types
from unittest.mock import MagicMock


def test_create_session_uses_plain_builder_by_default(monkeypatch):
    import kindling.spark_session as spark_session

    builder = MagicMock()
    builder.getOrCreate.return_value = "plain-spark"
    monkeypatch.setattr(spark_session, "SparkSession", MagicMock(builder=builder))
    monkeypatch.setattr(spark_session, "_abfss_az_cli_jar", lambda: None)
    monkeypatch.delenv("KINDLING_SPARK_ENABLE_DELTA", raising=False)

    result = spark_session.create_session()

    assert result == "plain-spark"
    builder.getOrCreate.assert_called_once_with()


def test_create_session_configures_delta_when_requested(monkeypatch):
    import kindling.spark_session as spark_session

    builder = MagicMock()
    builder.config.return_value = builder
    configured_builder = MagicMock()
    configured_builder.getOrCreate.return_value = "delta-spark"
    configure_spark = MagicMock(return_value=configured_builder)
    delta_module = types.SimpleNamespace(configure_spark_with_delta_pip=configure_spark)

    monkeypatch.setitem(sys.modules, "delta", delta_module)
    monkeypatch.setattr(spark_session, "SparkSession", MagicMock(builder=builder))
    monkeypatch.setattr(spark_session, "_abfss_az_cli_jar", lambda: None)
    monkeypatch.setenv("KINDLING_SPARK_ENABLE_DELTA", "true")

    result = spark_session.create_session()

    assert result == "delta-spark"
    builder.config.assert_any_call(
        "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
    )
    builder.config.assert_any_call(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    configure_spark.assert_called_once_with(builder)
    configured_builder.getOrCreate.assert_called_once_with()


def test_create_session_delta_request_explains_missing_dependency(monkeypatch):
    import kindling.spark_session as spark_session

    monkeypatch.delitem(sys.modules, "delta", raising=False)
    monkeypatch.setenv("KINDLING_SPARK_ENABLE_DELTA", "true")

    class MissingDeltaImporter:
        def find_spec(self, fullname, path=None, target=None):
            if fullname == "delta":
                return None
            return None

    monkeypatch.setattr(sys, "meta_path", [MissingDeltaImporter()])

    try:
        spark_session.create_session()
    except RuntimeError as exc:
        assert "delta-spark is not installed" in str(exc)
    else:
        raise AssertionError("Expected RuntimeError when delta-spark is missing")
