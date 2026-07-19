"""Hermetic tests for evaluation-time Lakeflow Kindling app selection."""

import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import patch

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
