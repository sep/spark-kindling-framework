"""Component tests — Kindling DI wiring and registry correctness.

Strategy: entity/pipe registration is a module-level side effect that runs
once per process (Python caches imports). Tests in this file share a single
injector session and verify the state of the registries after importing the
sales_ops modules. No DataFrames are read or written.
"""

import pytest


def _entity_registry():
    from kindling.data_entities import DataEntityRegistry
    from kindling.injection import GlobalInjector

    return GlobalInjector.get(DataEntityRegistry)


def _pipe_registry():
    from kindling.data_pipes import DataPipesRegistry
    from kindling.injection import GlobalInjector

    return GlobalInjector.get(DataPipesRegistry)


@pytest.fixture(scope="module", autouse=True)
def initialize_sales_ops():
    """Bootstrap the Kindling framework and register sales_ops modules.

    DataPipes.pipe() decorator calls GlobalInjector.get(DataPipesRegistry),
    which triggers DataPipesManager instantiation, which needs PythonLoggerProvider,
    which needs an initialized ConfigService. A minimal standalone bootstrap
    satisfies the full DI chain without any cloud infrastructure.

    @secret: references in env.local.yaml fail silently (no ABFSS creds needed
    here), so entity tags will carry unresolved strings — that's fine for the
    metadata assertions below.
    """
    from sales_ops.app import initialize

    initialize(env="local")


@pytest.mark.component
class TestEntityRegistration:
    def test_bronze_orders_is_registered(self):
        entity = _entity_registry().get_entity_definition("bronze.orders")

        assert entity is not None
        assert entity.name == "bronze_orders"
        assert entity.merge_columns == ["order_id"]
        assert entity.partition_columns == ["order_date"]
        assert entity.tags.get("layer") == "bronze"

    def test_silver_orders_is_registered(self):
        entity = _entity_registry().get_entity_definition("silver.orders")

        assert entity is not None
        assert entity.name == "silver_orders"
        assert entity.tags.get("layer") == "silver"

    def test_both_entities_appear_in_registry(self):
        ids = set(_entity_registry().get_entity_ids())

        assert "bronze.orders" in ids
        assert "silver.orders" in ids


@pytest.mark.component
class TestPipeRegistration:
    def test_bronze_to_silver_pipe_is_registered(self):
        pipe = _pipe_registry().get_pipe_definition("bronze_to_silver_orders")

        assert pipe is not None
        assert pipe.name == "Bronze to Silver Orders"
        assert pipe.input_entity_ids == ["bronze.orders"]
        assert pipe.output_entity_id == "silver.orders"
        assert pipe.output_type == "table"

    def test_pipe_execute_is_callable(self):
        pipe = _pipe_registry().get_pipe_definition("bronze_to_silver_orders")

        assert callable(pipe.execute)


@pytest.mark.component
class TestAppRegisterAll:
    def test_entity_ids_populated(self):
        ids = set(_entity_registry().get_entity_ids())
        assert "bronze.orders" in ids
        assert "silver.orders" in ids

    def test_pipe_ids_populated(self):
        ids = set(_pipe_registry().get_pipe_ids())
        assert "bronze_to_silver_orders" in ids
