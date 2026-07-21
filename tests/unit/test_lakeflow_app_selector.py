"""Hermetic tests for evaluation-time Lakeflow Kindling app selection."""

from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from kindling_ext_databricks import lakeflow_app_selector as selector


class FakeConf:
    def __init__(self, values=None):
        self.values = dict(values or {})

    def get(self, key, default=None):
        return self.values.get(key, default)

    def getAll(self):
        return dict(self.values)


class FakeSpark:
    def __init__(self, values=None):
        self.conf = FakeConf(values)


class FakeSparkConfWithoutGetAll:
    """The PySpark 3.x RuntimeConfig surface: get(), but no getAll()."""

    def __init__(self, values=None):
        self.values = dict(values or {})

    def get(self, key, default=None):
        return self.values.get(key, default)


class FakeSparkContext:
    def __init__(self, values=None):
        self.values = dict(values or {})

    def getConf(self):
        return self

    def getAll(self):
        return tuple(self.values.items())


class FakeSpark3:
    def __init__(self, values=None):
        self.conf = FakeSparkConfWithoutGetAll(values)
        self.sparkContext = FakeSparkContext(values)


class FakeEntryPoint:
    def __init__(self, name, value):
        self.name = name
        self.value = value


def _module(name, register_all):
    module = ModuleType(name)
    module.register_all = register_all
    return module


def test_data_app_entry_point_group_is_discovered_without_loading_modules(monkeypatch):
    entry_point = FakeEntryPoint("orders", "orders_app")
    observed = {}

    def entry_points(**kwargs):
        observed.update(kwargs)
        return [entry_point]

    monkeypatch.setattr("importlib.metadata.entry_points", entry_points)

    assert selector._registered_data_app_entry_points() == {"orders": entry_point}
    assert observed == {"group": "spark_kindling.data_apps"}


def test_selected_module_must_expose_register_all(monkeypatch):
    monkeypatch.setattr(
        selector,
        "_registered_data_app_entry_points",
        lambda: {"orders": FakeEntryPoint("orders", "orders_app")},
    )
    monkeypatch.setattr(selector, "_registry_snapshot", lambda: {"entity": {}, "pipe": {}})
    monkeypatch.setattr(
        selector,
        "importlib",
        SimpleNamespace(import_module=lambda _: ModuleType("orders_app")),
    )
    monkeypatch.setattr("kindling.initialize", lambda **kwargs: None)

    with pytest.raises(selector.LakeflowAppDeclarationError, match="register_all"):
        selector.declare_from_pipeline_config(FakeSpark({"kindling.data_app": "orders"}))


def test_two_app_names_select_their_declaration_graphs(monkeypatch):
    declared = []
    initialized = []

    for app_name, graph in (("orders", "orders-graph"), ("customers", "customers-graph")):

        def register(graph=graph):
            declared.append(graph)

        spark = FakeSpark({"kindling.data_app": app_name})
        monkeypatch.setattr(
            selector,
            "_registered_data_app_entry_points",
            lambda app_name=app_name: {app_name: FakeEntryPoint(app_name, f"app_{app_name}")},
        )
        monkeypatch.setattr(selector, "_registry_snapshot", lambda: {"entity": {}, "pipe": {}})
        monkeypatch.setattr(
            selector,
            "importlib",
            SimpleNamespace(
                import_module=lambda _, register=register, app_name=app_name: _module(
                    f"app_{app_name}", register
                )
            ),
        )
        monkeypatch.setattr("kindling.initialize", lambda **kwargs: initialized.append(kwargs))
        monkeypatch.setattr("kindling.declare_pipeline", lambda: declared[-1])

        assert selector.declare_from_pipeline_config(spark) == graph

    assert declared == ["orders-graph", "customers-graph"]
    assert [call["engine"] for call in initialized] == ["databricks_sdp", "databricks_sdp"]


@pytest.mark.parametrize(
    ("values", "error", "message"),
    [
        ({}, selector.LakeflowAppSelectionError, "kindling.data_app"),
        (
            {"kindling.data_app": "missing"},
            selector.LakeflowAppNotFoundError,
            "Discovered apps: orders",
        ),
        (
            {
                "kindling.data_app": "orders",
                "kindling.lakeflow.allowed_apps": "customers",
            },
            selector.LakeflowAppNotAuthorizedError,
            "kindling.lakeflow.allowed_apps",
        ),
    ],
)
def test_selection_errors_are_distinct_and_actionable(monkeypatch, values, error, message):
    spark = FakeSpark(values)
    monkeypatch.setattr(
        selector,
        "_registered_data_app_entry_points",
        lambda: {"orders": FakeEntryPoint("orders", "orders_app")},
    )

    with pytest.raises(error, match=message):
        selector.declare_from_pipeline_config(spark)


def test_initialize_completes_before_app_import(monkeypatch):
    events = []

    def initialize(**kwargs):
        events.append("initialize")

    def register_all():
        events.append("register_all")

    spark = FakeSpark({"kindling.data_app": "probe"})
    monkeypatch.setattr(
        selector,
        "_registered_data_app_entry_points",
        lambda: {"probe": FakeEntryPoint("probe", "probe_app")},
    )
    monkeypatch.setattr(selector, "_registry_snapshot", lambda: {"entity": {}, "pipe": {}})
    monkeypatch.setattr("kindling.initialize", initialize)
    monkeypatch.setattr("kindling.declare_pipeline", lambda: events.append("declare"))

    def import_module(_):
        events.append("import")
        return _module("probe_app", register_all)

    monkeypatch.setattr(
        selector,
        "importlib",
        SimpleNamespace(import_module=import_module),
    )

    selector.declare_from_pipeline_config(spark)
    assert events == ["initialize", "import", "register_all", "declare"]


def test_pipeline_configuration_is_bridged_to_kindling(monkeypatch):
    captured = {}
    spark = FakeSpark(
        {
            "kindling.data_app": "orders",
            "kindling.lakeflow.allowed_apps": "orders",
            "datapipes.silver.orders.engine": '{"dataset_type": "materialized_view"}',
            "spark.sql.shuffle.partitions": "10",
        }
    )
    monkeypatch.setattr(
        selector,
        "_registered_data_app_entry_points",
        lambda: {"orders": FakeEntryPoint("orders", "orders_app")},
    )
    monkeypatch.setattr(selector, "_registry_snapshot", lambda: {"entity": {}, "pipe": {}})
    monkeypatch.setattr("kindling.initialize", lambda **kwargs: captured.update(kwargs))
    monkeypatch.setattr(
        selector,
        "importlib",
        SimpleNamespace(import_module=lambda _: _module("orders_app", lambda: None)),
    )
    monkeypatch.setattr("kindling.declare_pipeline", lambda: "plan")

    assert selector.declare_from_pipeline_config(spark) == "plan"
    assert captured["engine"] == "databricks_sdp"
    assert captured["config"]["kindling.data_app"] == "orders"
    assert captured["config"]["kindling.lakeflow.allowed_apps"] == "orders"
    assert "datapipes.silver.orders.engine" in captured["config"]
    assert "spark.sql.shuffle.partitions" not in captured["config"]


def test_pipeline_configuration_falls_back_to_spark_context_conf(monkeypatch):
    captured = {}
    spark = FakeSpark3(
        {
            "kindling.data_app": "orders",
            "kindling.lakeflow.allowed_apps": "orders",
            "datapipes.silver.orders.engine": '{"dataset_type": "materialized_view"}',
            "spark.sql.shuffle.partitions": "10",
        }
    )
    monkeypatch.setattr(
        selector,
        "_registered_data_app_entry_points",
        lambda: {"orders": FakeEntryPoint("orders", "orders_app")},
    )
    monkeypatch.setattr(selector, "_registry_snapshot", lambda: {"entity": {}, "pipe": {}})
    monkeypatch.setattr("kindling.initialize", lambda **kwargs: captured.update(kwargs))
    monkeypatch.setattr(
        selector,
        "importlib",
        SimpleNamespace(import_module=lambda _: _module("orders_app", lambda: None)),
    )
    monkeypatch.setattr("kindling.declare_pipeline", lambda: "plan")

    assert selector.declare_from_pipeline_config(spark) == "plan"
    assert captured["config"]["kindling.data_app"] == "orders"
    assert captured["config"]["kindling.lakeflow.allowed_apps"] == "orders"
    assert "datapipes.silver.orders.engine" in captured["config"]
    assert "spark.sql.shuffle.partitions" not in captured["config"]


def test_pipeline_configuration_warns_when_only_explicit_keys_are_available(caplog):
    class SparkWithoutConfigEnumeration:
        def __init__(self):
            self.conf = FakeSparkConfWithoutGetAll(
                {
                    "kindling.data_app": "orders",
                    "kindling.lakeflow.allowed_apps": "orders",
                }
            )

    with caplog.at_level("WARNING", logger=selector.__name__):
        items = dict(selector._spark_conf_items(SparkWithoutConfigEnumeration()))

    assert items == {
        "kindling.data_app": "orders",
        "kindling.lakeflow.allowed_apps": "orders",
    }
    assert "RuntimeConfig.getAll()" in caplog.text
    assert "kindling.data_app" in caplog.text


def test_double_evaluation_is_idempotent(monkeypatch):
    register_calls = []
    snapshot = {"entity": {"orders": ("same",)}, "pipe": {"orders": ("same",)}}
    monkeypatch.setattr(
        selector,
        "_registered_data_app_entry_points",
        lambda: {"orders": FakeEntryPoint("orders", "orders_app")},
    )
    monkeypatch.setattr(selector, "_registry_snapshot", lambda: snapshot)
    monkeypatch.setattr(
        selector,
        "importlib",
        SimpleNamespace(
            import_module=lambda _: _module(
                "orders_app", lambda: register_calls.append("registered")
            )
        ),
    )
    monkeypatch.setattr("kindling.initialize", lambda **kwargs: None)
    monkeypatch.setattr("kindling.declare_pipeline", lambda: "plan")
    spark = FakeSpark({"kindling.data_app": "orders"})

    assert selector.declare_from_pipeline_config(spark) == "plan"
    assert selector.declare_from_pipeline_config(spark) == "plan"
    assert register_calls == ["registered", "registered"]


def test_double_evaluation_calls_real_kindling_initialize_twice(monkeypatch):
    import kindling

    extension = SimpleNamespace(owns_incrementality=True, activate=MagicMock())
    monkeypatch.setattr(kindling, "_active_engine_extension", None)
    monkeypatch.setattr(kindling, "_load_engine_extension", lambda _: extension)
    monkeypatch.setattr(kindling, "initialize_framework", MagicMock())
    monkeypatch.setattr(kindling, "declare_pipeline", lambda: "plan")
    monkeypatch.setattr(
        selector,
        "_registered_data_app_entry_points",
        lambda: {"orders": FakeEntryPoint("orders", "orders_app")},
    )
    monkeypatch.setattr(selector, "_registry_snapshot", lambda: {"entity": {}, "pipe": {}})
    monkeypatch.setattr(
        selector,
        "importlib",
        SimpleNamespace(import_module=lambda _: _module("orders_app", lambda: None)),
    )
    spark = FakeSpark({"kindling.data_app": "orders"})

    assert selector.declare_from_pipeline_config(spark) == "plan"
    assert selector.declare_from_pipeline_config(spark) == "plan"
    assert extension.activate.call_count == 2


def test_real_registry_snapshot_round_trip_is_stable(monkeypatch):
    from kindling.data_entities import DataEntityManager, DataEntityRegistry
    from kindling.data_pipes import DataPipesManager, DataPipesRegistry
    from kindling.injection import GlobalInjector
    from pyspark.sql.types import StringType, StructField, StructType

    entity_registry = DataEntityManager()
    logger_provider = MagicMock()
    logger_provider.get_logger.return_value = MagicMock()
    pipe_registry = DataPipesManager(logger_provider)
    entity_registry.register_entity(
        "orders",
        name="orders",
        merge_columns=["id"],
        tags={},
        schema=StructType([StructField("id", StringType(), nullable=False)]),
    )

    def execute(**dataframes):
        def identity(value):
            return value

        return identity(dataframes)

    pipe_registry.register_pipe(
        "orders.pipe",
        name="orders.pipe",
        execute=execute,
        tags={},
        input_entity_ids=["orders"],
        output_entity_id="orders",
        output_type="delta",
    )

    def get_registry(interface):
        if interface is DataEntityRegistry:
            return entity_registry
        if interface is DataPipesRegistry:
            return pipe_registry
        raise AssertionError(f"Unexpected registry request: {interface}")

    monkeypatch.setattr(GlobalInjector, "get", get_registry)
    before = selector._registry_snapshot()
    after = selector._registry_snapshot()

    selector._raise_on_conflicts(before, after, "orders")


def test_structural_callable_signatures_ignore_fresh_nested_code_objects():
    source = """
def transform(value):
    def nested(item):
        return item + 1
    return nested(value)
"""
    first_namespace = {}
    second_namespace = {}
    exec(source, first_namespace)
    exec(source, second_namespace)

    assert selector._stable_signature(first_namespace["transform"]) == selector._stable_signature(
        second_namespace["transform"]
    )


def test_conflicting_reregistration_names_the_id(monkeypatch):
    snapshots = iter(
        [
            {"entity": {"orders": ("old",)}, "pipe": {}},
            {"entity": {"orders": ("new",)}, "pipe": {}},
        ]
    )
    monkeypatch.setattr(
        selector,
        "_registered_data_app_entry_points",
        lambda: {"orders": FakeEntryPoint("orders", "orders_app")},
    )
    monkeypatch.setattr(selector, "_registry_snapshot", lambda: next(snapshots))
    monkeypatch.setattr(
        selector,
        "importlib",
        SimpleNamespace(import_module=lambda _: _module("orders_app", lambda: None)),
    )
    monkeypatch.setattr("kindling.initialize", lambda **kwargs: None)

    with pytest.raises(selector.LakeflowAppConflictError, match="orders"):
        selector.declare_from_pipeline_config(FakeSpark({"kindling.data_app": "orders"}))


def test_data_app_manager_is_not_invoked(monkeypatch):
    from kindling.data_apps import DataAppManager

    with patch.object(
        DataAppManager, "run_app", side_effect=AssertionError("must not run")
    ) as run_app:
        monkeypatch.setattr(
            selector,
            "_registered_data_app_entry_points",
            lambda: {"orders": FakeEntryPoint("orders", "orders_app")},
        )
        monkeypatch.setattr(selector, "_registry_snapshot", lambda: {"entity": {}, "pipe": {}})
        monkeypatch.setattr(
            selector,
            "importlib",
            SimpleNamespace(import_module=lambda _: _module("orders_app", lambda: None)),
        )
        monkeypatch.setattr("kindling.initialize", lambda **kwargs: None)
        monkeypatch.setattr("kindling.declare_pipeline", lambda: "plan")

        assert (
            selector.declare_from_pipeline_config(FakeSpark({"kindling.data_app": "orders"}))
            == "plan"
        )
        run_app.assert_not_called()


def test_pipeline_config_defaults_platform_to_standalone():
    config = selector._pipeline_config_for_kindling(
        FakeSpark({"kindling.data_app": "orders"}), "orders"
    )
    # Declaration-time pipelines get no platform machinery by default: the
    # Databricks platform service cannot construct inside Lakeflow.
    assert config["platform"] == "standalone"


def test_pipeline_config_explicit_platform_wins():
    config = selector._pipeline_config_for_kindling(
        FakeSpark(
            {
                "kindling.data_app": "orders",
                "kindling.platform.environment": "databricks",
            }
        ),
        "orders",
    )
    assert "platform" not in config
    assert config["kindling.platform.environment"] == "databricks"


def test_restricted_runtime_bridges_named_config_keys():
    """Serverless/shared runtimes allow point lookups but no enumeration."""

    class SparkPointLookupOnly:
        def __init__(self, values):
            self.conf = FakeSparkConfWithoutGetAll(values)
            # No sparkContext attribute at all, no SQL: enumeration is dead.

    config = selector._pipeline_config_for_kindling(
        SparkPointLookupOnly(
            {
                "kindling.data_app": "orders",
                "kindling.lakeflow.config_keys": (
                    "kindling.storage.table_catalog, datapipes.orders.engine.sdp.dataset_type"
                ),
                "kindling.storage.table_catalog": "main",
                "datapipes.orders.engine.sdp.dataset_type": "streaming_table",
                "kindling.unrelated": "not-bridged-unless-named",
            }
        ),
        "orders",
    )
    assert config["kindling.storage.table_catalog"] == "main"
    assert config["datapipes.orders.engine.sdp.dataset_type"] == "streaming_table"
    assert "kindling.unrelated" not in config


def test_enumeration_falls_back_to_sql_set():
    class SparkWithSqlOnly:
        def __init__(self, values):
            self.conf = FakeSparkConfWithoutGetAll(values)
            self._values = values

        def sql(self, statement):
            assert statement == "SET"
            rows = [(k, v) for k, v in self._values.items()]
            return SimpleNamespace(collect=lambda: rows)

    config = selector._pipeline_config_for_kindling(
        SparkWithSqlOnly(
            {
                "kindling.data_app": "orders",
                "kindling.storage.table_schema": "default",
            }
        ),
        "orders",
    )
    assert config["kindling.storage.table_schema"] == "default"


def test_declared_pipes_are_filtered_by_config(monkeypatch):
    monkeypatch.setattr(
        selector,
        "_registered_data_app_entry_points",
        lambda: {"orders": FakeEntryPoint("orders", "orders_app")},
    )
    monkeypatch.setattr(selector, "_registry_snapshot", lambda: {"entity": {}, "pipe": {}})
    monkeypatch.setattr(
        selector,
        "importlib",
        SimpleNamespace(import_module=lambda _: _module("orders_app", lambda: None)),
    )
    monkeypatch.setattr("kindling.initialize", lambda **kwargs: None)
    declared = {}
    monkeypatch.setattr(
        "kindling.declare_pipeline",
        lambda pipe_ids=None: declared.setdefault("pipe_ids", pipe_ids),
    )

    selector.declare_from_pipeline_config(
        FakeSpark(
            {
                "kindling.data_app": "orders",
                "kindling.lakeflow.pipes": " temporal.chain.events.default, temporal.chain.episodes.default ",
            }
        )
    )
    assert declared["pipe_ids"] == [
        "temporal.chain.events.default",
        "temporal.chain.episodes.default",
    ]
