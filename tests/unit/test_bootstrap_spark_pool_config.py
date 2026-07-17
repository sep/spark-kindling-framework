from unittest.mock import MagicMock, patch


def test_get_spark_kindling_config_maps_bootstrap_and_kindling_keys():
    from kindling.bootstrap import _get_spark_kindling_config

    mock_spark = MagicMock()
    mock_spark.conf.getAll.return_value = [
        ("spark.kindling.bootstrap.use_lake_packages", "false"),
        ("spark.kindling.bootstrap.load_local", "true"),
        ("spark.kindling.bootstrap.artifacts_storage_path", "abfss://artifacts@acct/path"),
        ("spark.kindling.extensions", '["kindling-ext-otel-azure>=0.3.0"]'),
        ("spark.kindling.telemetry.logging.level", '"DEBUG"'),
        ("spark.executor.memory", "8g"),
    ]

    with patch("kindling.bootstrap.get_or_create_spark_session", return_value=mock_spark):
        result = _get_spark_kindling_config()

    assert result["use_lake_packages"] is False
    assert result["load_workspace_packages"] is True
    assert result["artifacts_storage_path"] == "abfss://artifacts@acct/path"
    assert result["kindling.extensions"] == ["kindling-ext-otel-azure>=0.3.0"]
    assert result["kindling.telemetry.logging.level"] == "DEBUG"
    assert "spark.executor.memory" not in result


def test_get_spark_kindling_config_maps_canonical_load_workspace_packages_key():
    """The new spark.kindling.bootstrap.load_workspace_packages key passes through unaliased."""
    from kindling.bootstrap import _get_spark_kindling_config

    mock_spark = MagicMock()
    mock_spark.conf.getAll.return_value = [
        ("spark.kindling.bootstrap.load_workspace_packages", "true"),
    ]

    with patch("kindling.bootstrap.get_or_create_spark_session", return_value=mock_spark):
        result = _get_spark_kindling_config()

    assert result["load_workspace_packages"] is True
    assert "load_local_packages" not in result


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


# ── _apply_spark_configs ────────────────────────────────────────────────────


def _make_config_service(spark_configs_value):
    """Return a minimal mock ConfigService that returns the given value for spark_configs."""
    config = MagicMock()
    config.get.side_effect = lambda key, *_: (
        spark_configs_value if key in ("spark_configs", "SPARK_CONFIGS") else None
    )
    return config


def test_apply_spark_configs_sets_each_key_on_spark_session():
    from kindling.bootstrap import _apply_spark_configs

    mock_spark = MagicMock()
    logger = MagicMock()
    cfg = _make_config_service(
        {
            "spark.databricks.delta.schema.autoMerge.enabled": "true",
            "spark.sql.shuffle.partitions": "200",
        }
    )

    with patch("kindling.spark_session.get_or_create_spark_session", return_value=mock_spark):
        _apply_spark_configs(cfg, logger)

    mock_spark.conf.set.assert_any_call("spark.databricks.delta.schema.autoMerge.enabled", "true")
    mock_spark.conf.set.assert_any_call("spark.sql.shuffle.partitions", "200")
    assert mock_spark.conf.set.call_count == 2
    logger.info.assert_called_once()  # "Applied N spark_config(s)"


def test_apply_spark_configs_coerces_non_string_values():
    from kindling.bootstrap import _apply_spark_configs

    mock_spark = MagicMock()
    cfg = _make_config_service(
        {
            "spark.sql.shuffle.partitions": 400,  # int
            "spark.executor.memory": 8,  # int, not a string
        }
    )

    with patch("kindling.spark_session.get_or_create_spark_session", return_value=mock_spark):
        _apply_spark_configs(cfg, MagicMock())

    mock_spark.conf.set.assert_any_call("spark.sql.shuffle.partitions", "400")
    mock_spark.conf.set.assert_any_call("spark.executor.memory", "8")


def test_apply_spark_configs_skips_when_empty():
    from kindling.bootstrap import _apply_spark_configs

    mock_spark = MagicMock()
    cfg = _make_config_service({})  # empty dict

    with patch("kindling.spark_session.get_or_create_spark_session", return_value=mock_spark):
        _apply_spark_configs(cfg, MagicMock())

    mock_spark.conf.set.assert_not_called()


def test_apply_spark_configs_skips_when_none():
    from kindling.bootstrap import _apply_spark_configs

    mock_spark = MagicMock()
    cfg = _make_config_service(None)

    with patch("kindling.spark_session.get_or_create_spark_session", return_value=mock_spark):
        _apply_spark_configs(cfg, MagicMock())

    mock_spark.conf.set.assert_not_called()


def test_apply_spark_configs_skips_when_no_spark_session():
    from kindling.bootstrap import _apply_spark_configs

    cfg = _make_config_service({"spark.some.key": "value"})

    with patch("kindling.spark_session.get_or_create_spark_session", return_value=None):
        # should not raise
        _apply_spark_configs(cfg, MagicMock())


def test_apply_spark_configs_warns_on_individual_key_failure():
    from kindling.bootstrap import _apply_spark_configs

    mock_spark = MagicMock()
    mock_spark.conf.set.side_effect = Exception("read-only key")
    logger = MagicMock()
    cfg = _make_config_service({"spark.bad.key": "value"})

    with patch("kindling.spark_session.get_or_create_spark_session", return_value=mock_spark):
        _apply_spark_configs(cfg, logger)  # should not propagate the exception

    logger.warning.assert_called_once()
    assert "spark.bad.key" in logger.warning.call_args[0][0]


def test_apply_spark_configs_skips_when_not_a_dict():
    """Guard against spark_configs accidentally being set to a non-dict value."""
    from kindling.bootstrap import _apply_spark_configs

    mock_spark = MagicMock()
    cfg = _make_config_service("not-a-dict")

    with patch("kindling.spark_session.get_or_create_spark_session", return_value=mock_spark):
        _apply_spark_configs(cfg, MagicMock())

    mock_spark.conf.set.assert_not_called()


def test_apply_spark_configs_reads_spark_configs_key_first():
    """Verify 'spark_configs' is tried before the uppercased 'SPARK_CONFIGS' fallback."""
    from kindling.bootstrap import _apply_spark_configs

    mock_spark = MagicMock()
    call_order = []

    def _get(key, *_):
        call_order.append(key)
        return {"spark.key": "val"} if key == "spark_configs" else None

    cfg = MagicMock()
    cfg.get.side_effect = _get

    with patch("kindling.spark_session.get_or_create_spark_session", return_value=mock_spark):
        _apply_spark_configs(cfg, MagicMock())

    assert call_order[0] == "spark_configs"
    mock_spark.conf.set.assert_called_once_with("spark.key", "val")
