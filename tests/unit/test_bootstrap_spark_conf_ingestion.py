from types import SimpleNamespace


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
            ("spark.kindling.telemetry.logging.level", '"DEBUG"'),
            ("spark.executor.memory", "8g"),
        ]
    )

    assert result == {
        "use_lake_packages": False,
        "load_workspace_packages": True,
        "config_files": ["settings.yaml"],
        "kindling.telemetry.logging.level": "DEBUG",
    }


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
