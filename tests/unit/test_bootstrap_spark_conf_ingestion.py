import logging
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest


class FakeConf:
    def __init__(self, values=None, *, get_all=None, get_raises=False):
        self.values = dict(values or {})
        self._get_all = get_all
        self._get_raises = get_raises

    def get(self, key, default=None):
        if self._get_raises:
            raise RuntimeError("point lookup blocked")
        return self.values.get(key, default)

    def getAll(self):
        if isinstance(self._get_all, Exception):
            raise self._get_all
        return self.values if self._get_all is None else self._get_all


class FakeConfWithoutGetAll:
    def __init__(self, values=None, *, get_raises=False):
        self.values = dict(values or {})
        self._get_raises = get_raises

    def get(self, key, default=None):
        if self._get_raises:
            raise RuntimeError("point lookup blocked")
        return self.values.get(key, default)


class FakeSparkContext:
    def __init__(self, values=None, *, raises=False):
        self.values = dict(values or {})
        self.raises = raises

    def getConf(self):
        if self.raises:
            raise RuntimeError("spark context blocked")
        return self

    def getAll(self):
        return tuple(self.values.items())


def test_map_spark_kindling_items_maps_bootstrap_aliases_and_coerces_values():
    from kindling.bootstrap import map_spark_kindling_items

    result = map_spark_kindling_items(
        [
            ("spark.kindling.bootstrap.load_lake", "false"),
            ("spark.kindling.bootstrap.load_local", "true"),
            ("spark.kindling.bootstrap.config_files", '["settings.yaml"]'),
            ("spark.kindling.pipeline.max_workers", "5"),
            ("spark.kindling.pipeline.sample_ratio", "1.5"),
            ("spark.kindling.telemetry.logging.level", '"DEBUG"'),
            ("spark.executor.memory", "8g"),
        ]
    )

    assert result == {
        "use_lake_packages": False,
        "load_workspace_packages": True,
        "config_files": ["settings.yaml"],
        "kindling.pipeline.max_workers": 5,
        "kindling.pipeline.sample_ratio": 1.5,
        "kindling.telemetry.logging.level": "DEBUG",
    }


def test_config_files_comma_string_from_spark_conf_is_split_with_warning(caplog):
    from kindling.bootstrap import map_spark_kindling_items

    with caplog.at_level(logging.WARNING, logger="kindling.bootstrap"):
        result = map_spark_kindling_items(
            [("spark.kindling.bootstrap.config_files", "first.yaml, second.yaml")]
        )

    assert result == {"config_files": ["first.yaml", "second.yaml"]}
    assert "spark.kindling.bootstrap.config_files" in caplog.text
    assert "JSON array string" in caplog.text


def test_dynaconf_config_warns_for_missing_spark_conf_config_files(caplog, tmp_path):
    from kindling.spark_config import DynaconfConfig

    missing = tmp_path / "missing.yaml"

    with patch("kindling.spark_config.get_or_create_spark_session", return_value=MagicMock()):
        config = DynaconfConfig()
        with caplog.at_level(logging.WARNING, logger="kindling.config"):
            config.initialize(
                config_files=[str(missing)],
                initial_config={
                    "config_files": [str(missing)],
                    "_kindling_config_files_source_key": ("spark.kindling.bootstrap.config_files"),
                },
            )

    assert "spark.kindling.bootstrap.config_files" in caplog.text
    assert str(missing) in caplog.text


def test_dynaconf_config_raises_for_malformed_spark_conf_config_file(tmp_path):
    from kindling.spark_config import DynaconfConfig

    malformed = tmp_path / "settings.yaml"
    malformed.write_text("kindling:\n  platform: [\n", encoding="utf-8")

    with patch("kindling.spark_config.get_or_create_spark_session", return_value=MagicMock()):
        config = DynaconfConfig()
        with pytest.raises(Exception, match="while parsing"):
            config.initialize(
                config_files=[str(malformed)],
                initial_config={
                    "config_files": [str(malformed)],
                    "_kindling_config_files_source_key": ("spark.kindling.bootstrap.config_files"),
                },
            )


def test_iter_spark_conf_items_uses_spark_context_after_runtime_config_failure():
    from kindling.bootstrap import iter_spark_conf_items

    spark = SimpleNamespace(
        conf=FakeConf(get_all=RuntimeError("restricted")),
        sparkContext=FakeSparkContext({"spark.kindling.bootstrap.environment": "prod"}),
    )

    assert dict(iter_spark_conf_items(spark)) == {"spark.kindling.bootstrap.environment": "prod"}


def test_iter_spark_conf_items_uses_sql_set_after_conf_objects_fail():
    from kindling.bootstrap import iter_spark_conf_items

    class SparkWithSqlOnly:
        conf = FakeConfWithoutGetAll()
        sparkContext = FakeSparkContext(raises=True)

        def sql(self, statement):
            assert statement == "SET"
            return SimpleNamespace(
                collect=lambda: [
                    ("spark.kindling.bootstrap.declaration_only", "true"),
                    ("spark.sql.shuffle.partitions", "10"),
                ]
            )

    assert dict(iter_spark_conf_items(SparkWithSqlOnly())) == {
        "spark.kindling.bootstrap.declaration_only": "true",
        "spark.sql.shuffle.partitions": "10",
    }


def test_read_spark_kindling_config_uses_explicit_point_lookup_when_enumeration_is_blocked():
    from kindling.bootstrap import read_spark_kindling_config

    spark = SimpleNamespace(
        conf=FakeConfWithoutGetAll(
            {
                "spark.kindling.bootstrap.environment": "qa",
                "spark.kindling.bootstrap.load_local": "true",
                "kindling.data_app": "orders",
            }
        )
    )

    assert read_spark_kindling_config(
        spark,
        extra_keys=(
            "spark.kindling.bootstrap.environment",
            "spark.kindling.bootstrap.load_local",
            "kindling.data_app",
        ),
    ) == {
        "environment": "qa",
        "load_workspace_packages": True,
    }


def test_iter_spark_conf_items_cascade_cannot_raise():
    from kindling.bootstrap import iter_spark_conf_items

    class BrokenSpark:
        conf = FakeConf(get_all=RuntimeError("getAll blocked"), get_raises=True)
        sparkContext = FakeSparkContext(raises=True)

        def sql(self, _statement):
            raise RuntimeError("sql blocked")

    assert (
        tuple(
            iter_spark_conf_items(
                BrokenSpark(),
                extra_keys=("spark.kindling.bootstrap.environment", "kindling.data_app"),
            )
        )
        == ()
    )
