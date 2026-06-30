import sys
import types
from unittest.mock import MagicMock


def test_create_session_uses_plain_builder_by_default(monkeypatch):
    import kindling.spark_session as spark_session

    builder = MagicMock()
    builder._options = {}
    builder.getOrCreate.return_value = "plain-spark"
    monkeypatch.setattr(spark_session, "SparkSession", MagicMock(builder=builder))
    monkeypatch.setattr(spark_session, "_available_abfss_jars", lambda: [])
    monkeypatch.setattr(spark_session, "_abfss_az_cli_jar", lambda: None)
    monkeypatch.delenv("KINDLING_SPARK_ENABLE_DELTA", raising=False)

    result = spark_session.create_session()

    assert result == "plain-spark"
    builder.getOrCreate.assert_called_once_with()


def test_create_session_adds_available_abfss_jars(monkeypatch):
    import kindling.spark_session as spark_session

    builder = MagicMock()
    builder._options = {}
    builder.config.return_value = builder
    builder.getOrCreate.return_value = "plain-spark"
    monkeypatch.setattr(spark_session, "SparkSession", MagicMock(builder=builder))
    monkeypatch.setattr(
        spark_session,
        "_available_abfss_jars",
        lambda: [
            "/tmp/hadoop-jars/hadoop-azure-3.3.4.jar",
            "/tmp/hadoop-jars/hadoop-azure-datalake-3.3.4.jar",
        ],
    )
    monkeypatch.setattr(spark_session, "_abfss_az_cli_jar", lambda: None)
    monkeypatch.delenv("KINDLING_SPARK_ENABLE_DELTA", raising=False)

    result = spark_session.create_session()

    assert result == "plain-spark"
    builder.config.assert_any_call(
        "spark.jars",
        "/tmp/hadoop-jars/hadoop-azure-3.3.4.jar,"
        "/tmp/hadoop-jars/hadoop-azure-datalake-3.3.4.jar",
    )


def test_create_session_sets_auth_config_at_builder_time(monkeypatch):
    import kindling.spark_session as spark_session

    builder = MagicMock()
    builder._options = {}
    builder.config.return_value = builder
    builder.getOrCreate.return_value = "plain-spark"
    monkeypatch.setattr(spark_session, "SparkSession", MagicMock(builder=builder))
    monkeypatch.setattr(
        spark_session,
        "_available_abfss_jars",
        lambda: ["/tmp/hadoop-jars/kindling-abfss-local-auth.jar"],
    )
    monkeypatch.setattr(
        spark_session, "_abfss_az_cli_jar", lambda: "/tmp/hadoop-jars/kindling-abfss-local-auth.jar"
    )
    monkeypatch.delenv("KINDLING_SPARK_ENABLE_DELTA", raising=False)
    monkeypatch.delenv("KINDLING_ABFSS_AZ_CLI_AUTH", raising=False)

    spark_session.create_session()

    builder.config.assert_any_call("spark.hadoop.fs.azure.account.auth.type", "Custom")
    builder.config.assert_any_call(
        "spark.hadoop.fs.azure.account.oauth.provider.type",
        "io.kindling.abfss.AzureCliTokenProvider",
    )


def test_create_session_preserves_existing_spark_jars(monkeypatch):
    import kindling.spark_session as spark_session

    builder = MagicMock()
    builder._options = {"spark.jars": "/opt/existing.jar"}
    builder.config.return_value = builder
    builder.getOrCreate.return_value = "plain-spark"
    monkeypatch.setattr(spark_session, "SparkSession", MagicMock(builder=builder))
    monkeypatch.setattr(
        spark_session,
        "_available_abfss_jars",
        lambda: ["/tmp/hadoop-jars/hadoop-azure-3.3.4.jar"],
    )
    monkeypatch.setattr(spark_session, "_abfss_az_cli_jar", lambda: None)
    monkeypatch.delenv("KINDLING_SPARK_ENABLE_DELTA", raising=False)

    spark_session.create_session()

    builder.config.assert_any_call(
        "spark.jars",
        "/opt/existing.jar,/tmp/hadoop-jars/hadoop-azure-3.3.4.jar",
    )


def test_create_session_configures_delta_when_requested(monkeypatch):
    import kindling.spark_session as spark_session

    builder = MagicMock()
    builder._options = {}
    builder.config.return_value = builder
    configured_builder = MagicMock()
    configured_builder.getOrCreate.return_value = "delta-spark"
    configure_spark = MagicMock(return_value=configured_builder)
    delta_module = types.SimpleNamespace(configure_spark_with_delta_pip=configure_spark)

    monkeypatch.setitem(sys.modules, "delta", delta_module)
    monkeypatch.setattr(spark_session, "SparkSession", MagicMock(builder=builder))
    monkeypatch.setattr(spark_session, "_available_abfss_jars", lambda: [])
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


def test_abfss_auth_enabled_returns_true_by_default(monkeypatch):
    import kindling.spark_session as spark_session

    monkeypatch.delenv("KINDLING_ABFSS_AZ_CLI_AUTH", raising=False)
    assert spark_session._abfss_auth_enabled() is True


def test_abfss_auth_enabled_returns_false_when_env_var_is_false(monkeypatch):
    import kindling.spark_session as spark_session

    monkeypatch.setenv("KINDLING_ABFSS_AZ_CLI_AUTH", "false")
    assert spark_session._abfss_auth_enabled() is False


def test_abfss_auth_enabled_returns_true_when_env_var_is_true(monkeypatch):
    import kindling.spark_session as spark_session

    monkeypatch.setenv("KINDLING_ABFSS_AZ_CLI_AUTH", "true")
    assert spark_session._abfss_auth_enabled() is True


def test_create_session_respects_abfss_auth_opt_out(monkeypatch):
    import kindling.spark_session as spark_session

    builder = MagicMock()
    builder._options = {}
    builder.config.return_value = builder
    builder.getOrCreate.return_value = "plain-spark"
    monkeypatch.setattr(spark_session, "SparkSession", MagicMock(builder=builder))
    monkeypatch.setattr(
        spark_session,
        "_available_abfss_jars",
        lambda: ["/tmp/hadoop-jars/kindling-abfss-local-auth.jar"],
    )
    monkeypatch.setattr(
        spark_session, "_abfss_az_cli_jar", lambda: "/tmp/hadoop-jars/kindling-abfss-local-auth.jar"
    )
    monkeypatch.delenv("KINDLING_SPARK_ENABLE_DELTA", raising=False)
    monkeypatch.setenv("KINDLING_ABFSS_AZ_CLI_AUTH", "false")

    spark_session.create_session()

    # JARs are still injected — only auth config is suppressed
    builder.config.assert_any_call("spark.jars", "/tmp/hadoop-jars/kindling-abfss-local-auth.jar")
    auth_calls = [
        c
        for c in builder.config.call_args_list
        if c.args and "fs.azure.account.auth.type" in str(c.args[0])
    ]
    assert auth_calls == [], "Auth config should not be set when opt-out env var is false"


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
