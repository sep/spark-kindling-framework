from unittest.mock import MagicMock, patch


def test_get_spark_kindling_config_maps_bootstrap_and_kindling_keys():
    from kindling.bootstrap import _get_spark_kindling_config

    mock_spark = MagicMock()
    mock_spark.conf.getAll.return_value = [
        ("spark.kindling.bootstrap.use_lake_packages", "false"),
        ("spark.kindling.bootstrap.load_local", "true"),
        ("spark.kindling.bootstrap.artifacts_storage_path", "abfss://artifacts@acct/path"),
        ("spark.kindling.extensions", '["kindling-otel-azure>=0.3.0"]'),
        ("spark.kindling.telemetry.logging.level", '"DEBUG"'),
        ("spark.executor.memory", "8g"),
    ]

    with patch("kindling.bootstrap.get_or_create_spark_session", return_value=mock_spark):
        result = _get_spark_kindling_config()

    assert result["use_lake_packages"] is False
    assert result["load_local_packages"] is True
    assert result["artifacts_storage_path"] == "abfss://artifacts@acct/path"
    assert result["kindling.extensions"] == ["kindling-otel-azure>=0.3.0"]
    assert result["kindling.telemetry.logging.level"] == "DEBUG"
    assert "spark.executor.memory" not in result


def test_merge_with_spark_kindling_config_explicit_values_win():
    from kindling.bootstrap import _merge_with_spark_kindling_config

    mock_spark = MagicMock()
    mock_spark.conf.getAll.return_value = [
        ("spark.kindling.bootstrap.use_lake_packages", "true"),
        ("spark.kindling.bootstrap.environment", '"production"'),
        ("spark.kindling.extensions", '["ext-from-spark"]'),
    ]

    explicit_config = {
        "use_lake_packages": False,
        "environment": "dev",
        "kindling.extensions": ["ext-from-explicit"],
    }

    with patch("kindling.bootstrap.get_or_create_spark_session", return_value=mock_spark):
        merged = _merge_with_spark_kindling_config(explicit_config)

    assert merged["use_lake_packages"] is False
    assert merged["environment"] == "dev"
    assert merged["kindling.extensions"] == ["ext-from-explicit"]


def test_get_spark_kindling_config_handles_dict_getall_shape():
    from kindling.bootstrap import _get_spark_kindling_config

    mock_spark = MagicMock()
    mock_spark.conf.getAll.return_value = {
        "spark.kindling.bootstrap.log_level": '"INFO"',
        "spark.kindling.bootstrap.load_lake": "true",
    }

    with patch("kindling.bootstrap.get_or_create_spark_session", return_value=mock_spark):
        result = _get_spark_kindling_config()

    assert result["log_level"] == "INFO"
    assert result["use_lake_packages"] is True
