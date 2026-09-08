"""Hermetic tests for evaluation-time Lakeflow Kindling app selection."""

import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path
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


class SparkPointLookupOnly:
    def __init__(self, values):
        self.conf = FakeSparkConfWithoutGetAll(values)
        # No sparkContext attribute at all, no SQL: enumeration is dead.


class FakeEntryPoint:
    def __init__(self, name, value):
        self.name = name
        self.value = value


def _module(name, register_all):
    module = ModuleType(name)
    module.register_all = register_all
    return module


def _run_repro(script: str, tmp_path: Path) -> dict:
    result_path = tmp_path / "result.json"
    repo_root = Path(__file__).parents[2]
    pythonpath = os.pathsep.join(
        [
            str(repo_root / "packages"),
            str(repo_root / "packages" / "kindling_cli"),
            str(repo_root / "packages" / "kindling_sdk"),
            str(repo_root / "packages" / "extensions" / "kindling_ext_sdp"),
            str(repo_root / "packages" / "extensions" / "kindling_ext_databricks"),
            os.environ.get("PYTHONPATH", ""),
        ]
    )
    env = {**os.environ, "PYTHONPATH": pythonpath}
    proc = subprocess.run(
        [sys.executable, "-c", script, str(tmp_path), str(result_path)],
        capture_output=True,
        text=True,
        timeout=180,
        cwd=repo_root,
        env=env,
    )
    assert proc.returncode == 0, (
        f"repro subprocess failed (exit {proc.returncode})\n"
        f"--- stdout ---\n{proc.stdout}\n--- stderr ---\n{proc.stderr}"
    )
    return json.loads(result_path.read_text(encoding="utf-8"))


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
        monkeypatch.setattr("kindling.declare_pipeline", lambda pipe_ids=None: declared[-1])

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
    monkeypatch.setattr("kindling.declare_pipeline", lambda pipe_ids=None: events.append("declare"))

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
    monkeypatch.setattr("kindling.declare_pipeline", lambda pipe_ids=None: "plan")

    assert selector.declare_from_pipeline_config(spark) == "plan"
    assert captured["engine"] == "databricks_sdp"
    assert captured["config"]["kindling.data_app"] == "orders"
    assert captured["config"]["kindling.lakeflow.allowed_apps"] == "orders"
    assert "datapipes.silver.orders.engine" in captured["config"]
    assert "spark.sql.shuffle.partitions" not in captured["config"]
    assert "config_files" not in captured["config"]


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
    monkeypatch.setattr("kindling.declare_pipeline", lambda pipe_ids=None: "plan")

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
    monkeypatch.setattr("kindling.declare_pipeline", lambda pipe_ids=None: "plan")
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
    monkeypatch.setattr(kindling, "declare_pipeline", lambda pipe_ids=None: "plan")
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
        monkeypatch.setattr("kindling.declare_pipeline", lambda pipe_ids=None: "plan")

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


def test_config_files_key_is_point_looked_up_without_config_keys(tmp_path):
    settings = tmp_path / "settings.yaml"
    settings.write_text("dataentities: {}\n", encoding="utf-8")

    config_keys = "kindling.storage.table_catalog"
    config = selector._pipeline_config_for_kindling(
        SparkPointLookupOnly(
            {
                "kindling.data_app": "orders",
                "kindling.lakeflow.config_keys": config_keys,
                "kindling.storage.table_catalog": "main",
                selector.CONFIG_FILES_CONFIG_KEY: str(settings),
            }
        ),
        "orders",
    )

    assert selector.CONFIG_FILES_CONFIG_KEY not in config_keys
    assert config["config_files"] == [os.path.abspath(settings)]
    assert config["kindling.storage.table_catalog"] == "main"


def test_config_files_key_empty_string_is_noop():
    config = selector._pipeline_config_for_kindling(
        FakeSpark({"kindling.data_app": "orders", selector.CONFIG_FILES_CONFIG_KEY: ""}),
        "orders",
    )

    assert "config_files" not in config


def test_config_files_are_split_normalized_and_ordered(tmp_path):
    first = tmp_path / "first.yaml"
    second = tmp_path / "second.yml"
    first.write_text("dataentities: {}\n", encoding="utf-8")
    second.write_text("datapipes: {}\n", encoding="utf-8")

    config = selector._pipeline_config_for_kindling(
        FakeSpark(
            {
                "kindling.data_app": "orders",
                selector.CONFIG_FILES_CONFIG_KEY: f" {first}, , {second} ",
            }
        ),
        "orders",
    )

    assert config["config_files"] == [os.path.abspath(first), os.path.abspath(second)]


@pytest.mark.parametrize("raw_value", [",", " , "])
def test_config_files_path_free_value_raises(raw_value):
    with pytest.raises(selector.LakeflowConfigSourceError) as exc_info:
        selector._pipeline_config_for_kindling(
            FakeSpark({"kindling.data_app": "orders", selector.CONFIG_FILES_CONFIG_KEY: raw_value}),
            "orders",
        )

    message = str(exc_info.value)
    assert selector.CONFIG_FILES_CONFIG_KEY in message
    assert raw_value in message


def test_config_files_missing_path_raises_config_source_error(tmp_path):
    missing = tmp_path / "missing.yaml"

    with pytest.raises(selector.LakeflowConfigSourceError) as exc_info:
        selector._pipeline_config_for_kindling(
            FakeSpark(
                {"kindling.data_app": "orders", selector.CONFIG_FILES_CONFIG_KEY: str(missing)}
            ),
            "orders",
        )

    message = str(exc_info.value)
    assert selector.CONFIG_FILES_CONFIG_KEY in message
    assert str(missing) in message


def test_config_files_unsupported_suffix_raises_config_source_error(tmp_path):
    settings = tmp_path / "settings.json"
    settings.write_text('{"dataentities": {}}\n', encoding="utf-8")

    with pytest.raises(selector.LakeflowConfigSourceError) as exc_info:
        selector._pipeline_config_for_kindling(
            FakeSpark(
                {"kindling.data_app": "orders", selector.CONFIG_FILES_CONFIG_KEY: str(settings)}
            ),
            "orders",
        )

    message = str(exc_info.value)
    assert selector.CONFIG_FILES_CONFIG_KEY in message
    assert str(settings) in message
    assert ".yaml" in message
    assert ".yml" in message


@pytest.mark.parametrize(
    ("content", "expected"),
    [
        ("dataentities:\n  bronze.device_telemetry: [\n", "could not parse YAML"),
        ("- not-a-mapping\n", "must contain a mapping"),
        ("dataentities: 42\n", "section 'dataentities' must be a mapping"),
        (
            "dataentities:\n  bronze.device_telemetry: scalar\n",
            "section 'dataentities' entry 'bronze.device_telemetry' must be a mapping",
        ),
        ("datapipes-bytag: 42\n", "section 'datapipes-bytag' must be a mapping"),
    ],
)
def test_config_files_invalid_yaml_shapes_raise_config_source_error(tmp_path, content, expected):
    settings = tmp_path / "settings.yaml"
    settings.write_text(content, encoding="utf-8")

    with pytest.raises(selector.LakeflowConfigSourceError) as exc_info:
        selector._pipeline_config_for_kindling(
            FakeSpark(
                {"kindling.data_app": "orders", selector.CONFIG_FILES_CONFIG_KEY: str(settings)}
            ),
            "orders",
        )

    message = str(exc_info.value)
    assert selector.CONFIG_FILES_CONFIG_KEY in message
    assert str(settings) in message
    assert expected in message


def test_config_source_error_is_exported():
    import kindling_ext_databricks as databricks_ext

    assert databricks_ext.LakeflowConfigSourceError is selector.LakeflowConfigSourceError


_REAL_SELECTOR_REPRO = textwrap.dedent("""
    import json
    import sys
    from types import ModuleType, SimpleNamespace

    import kindling
    from kindling.data_entities import DataEntities, DataEntityRegistry, EntityNameMapper
    from kindling.data_pipes import DataPipes, DataPipesRegistry
    from kindling.injection import GlobalInjector, get_kindling_service
    from kindling.spark_config import ConfigService
    from kindling_ext_databricks import lakeflow_app_selector as selector
    from pyspark.sql.types import StringType, StructField, StructType

    tmp_path, result_path = sys.argv[1], sys.argv[2]
    settings_path = f"{tmp_path}/settings.yaml"
    with open(settings_path, "w", encoding="utf-8") as config_file:
        config_file.write(
            "kindling:\\n"
            "  data_app: yaml_app\\n"
            "  lakeflow:\\n"
            "    allowed_apps: yaml_app\\n"
            "  sdp:\\n"
            "    dataset_naming: leaf\\n"
            "dataentities-bytag:\\n"
            "  tier:\\n"
            "    bronze:\\n"
            "      tags:\\n"
            "        provider.table_catalog: dev_bronze\\n"
            "        from_bytag: 'yes'\\n"
            "dataentities:\\n"
            "  bronze.device_telemetry:\\n"
            "    tags:\\n"
            "      provider.table_name: dev_bronze.cwmdp.device_telemetry\\n"
            "      from_dataentities: exact\\n"
            "    partition_columns:\\n"
            "      - event_date\\n"
            "datapipes:\\n"
            "  bronze.ingest_telemetry:\\n"
            "    tags:\\n"
            "      from_datapipes: yaml\\n"
            "    output_type: memory\\n"
        )

    class FakeConf:
        def __init__(self, values):
            self.values = dict(values)

        def get(self, key, default=None):
            return self.values.get(key, default)

        def getAll(self):
            return dict(self.values)

    class FakeSpark:
        def __init__(self, values):
            self.conf = FakeConf(values)

    class FakeEntryPoint:
        name = "orders"
        value = "orders_app"

    schema = StructType([StructField("id", StringType(), nullable=False)])
    extension = SimpleNamespace(owns_incrementality=True, activate=lambda: None)
    kindling._active_engine_extension = None
    kindling._load_engine_extension = lambda _: extension
    selector._registered_data_app_entry_points = lambda: {"orders": FakeEntryPoint()}

    def register_import_time_entity():
        DataEntities.entity(
            entityid="bronze.device_telemetry",
            name="device_telemetry",
            merge_columns=["id"],
            tags={"tier": "bronze"},
            schema=schema,
        )

    def register_all():
        DataEntities.entity(
            entityid="bronze.registered_later",
            name="registered_later",
            merge_columns=["id"],
            tags={"tier": "bronze"},
            schema=schema,
        )

        @DataPipes.pipe(
            pipeid="bronze.ingest_telemetry",
            name="Ingest Telemetry",
            input_entity_ids=["bronze.device_telemetry"],
            output_entity_id="bronze.registered_later",
            output_type="delta",
            tags={"source": "app"},
        )
        def ingest_telemetry(**dataframes):
            return next(iter(dataframes.values()), None)

    def import_module(_):
        register_import_time_entity()
        module = ModuleType("orders_app")
        module.register_all = register_all
        return module

    selector.importlib = SimpleNamespace(import_module=import_module)
    kindling.declare_pipeline = lambda pipe_ids=None: sorted(
        get_kindling_service(DataPipesRegistry).get_pipe_ids()
        if pipe_ids is None
        else pipe_ids
    )

    spark = FakeSpark(
        {
            "kindling.data_app": "orders",
            "kindling.lakeflow.allowed_apps": "orders",
            selector.CONFIG_FILES_CONFIG_KEY: settings_path,
            "kindling.sdp.dataset_naming": "normalized",
            "datapipes.bronze.ingest_telemetry.engine.sdp.dataset_type": "streaming_table",
        }
    )
    first_plan = selector.declare_from_pipeline_config(spark)
    second_plan = selector.declare_from_pipeline_config(spark)

    entity_registry = get_kindling_service(DataEntityRegistry)
    pipe_registry = get_kindling_service(DataPipesRegistry)
    config_service = get_kindling_service(ConfigService)
    name_mapper = GlobalInjector.get(EntityNameMapper)
    import_time_entity = entity_registry.get_entity_definition("bronze.device_telemetry")
    register_all_entity = entity_registry.get_entity_definition("bronze.registered_later")
    pipe = pipe_registry.get_pipe_definition("bronze.ingest_telemetry")

    with open(result_path, "w", encoding="utf-8") as result_file:
        json.dump(
            {
                "first_plan": first_plan,
                "second_plan": second_plan,
                "config_files": config_service.initial_config.get("config_files"),
                "data_app": config_service.get("kindling.data_app"),
                "allowlist": config_service.get("kindling.lakeflow.allowed_apps"),
                "dataset_naming": config_service.get("kindling.sdp.dataset_naming"),
                "engine_dataset_type": config_service.get(
                    "datapipes.bronze.ingest_telemetry.engine.sdp.dataset_type"
                ),
                "import_time_tags": import_time_entity.tags,
                "import_time_partitions": import_time_entity.partition_columns,
                "register_all_tags": register_all_entity.tags,
                "pipe_tags": pipe.tags,
                "pipe_output_type": pipe.output_type,
                "physical_name": name_mapper.get_table_name(import_time_entity),
            },
            result_file,
        )
    """)


def test_config_files_reach_real_initialize_registry_overlays_and_reentry(tmp_path):
    result = _run_repro(_REAL_SELECTOR_REPRO, tmp_path)

    assert result["first_plan"] == ["bronze.ingest_telemetry"]
    assert result["second_plan"] == ["bronze.ingest_telemetry"]
    assert result["config_files"] == [str(tmp_path / "settings.yaml")]
    assert result["data_app"] == "orders"
    assert result["allowlist"] == "orders"
    assert result["dataset_naming"] == "normalized"
    assert result["engine_dataset_type"] == "streaming_table"
    assert result["import_time_tags"] == {
        "tier": "bronze",
        "provider.table_catalog": "dev_bronze",
        "from_bytag": "yes",
        "provider.table_name": "dev_bronze.cwmdp.device_telemetry",
        "from_dataentities": "exact",
    }
    assert result["import_time_partitions"] == ["event_date"]
    assert result["register_all_tags"] == {
        "tier": "bronze",
        "provider.table_catalog": "dev_bronze",
        "from_bytag": "yes",
    }
    assert result["pipe_tags"] == {"source": "app", "from_datapipes": "yaml"}
    assert result["pipe_output_type"] == "memory"
    assert result["physical_name"] == "dev_bronze.cwmdp.device_telemetry"


def test_structured_config_cannot_authorize_non_allowlisted_app(monkeypatch, tmp_path):
    settings = tmp_path / "settings.yaml"
    settings.write_text(
        "kindling:\n" "  data_app: customers\n" "  lakeflow:\n" "    allowed_apps: orders\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        selector,
        "_registered_data_app_entry_points",
        lambda: {"orders": FakeEntryPoint("orders", "orders_app")},
    )

    with pytest.raises(selector.LakeflowAppNotAuthorizedError, match="allowed_apps"):
        selector.declare_from_pipeline_config(
            FakeSpark(
                {
                    "kindling.data_app": "orders",
                    "kindling.lakeflow.allowed_apps": "customers",
                    selector.CONFIG_FILES_CONFIG_KEY: str(settings),
                }
            )
        )


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
