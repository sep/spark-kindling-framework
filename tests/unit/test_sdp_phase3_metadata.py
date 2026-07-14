"""Unit tests for Phase 3: entity-metadata emission and the Databricks
adapter (expectations, engine extension).

The FakeDpModule records full decorator kwargs, mirroring the real Spark
4.1 ``dp.materialized_view`` keyword surface (verified against pyspark
4.1.2: name, comment, spark_conf, table_properties, partition_cols,
cluster_by, schema, format). Adapter tests give the fake the Lakeflow
expectation decorators the OSS module lacks.
"""

from types import SimpleNamespace

import kindling
import pytest
from kindling.data_entities import EntityMetadata
from kindling.data_pipes import PipeMetadata
from kindling_databricks_sdp import DatabricksSdpEngine, engine_extension
from kindling_sdp import OssSdpEngine

# --------------------------------------------------------------------- #
# Fixtures                                                               #
# --------------------------------------------------------------------- #


class FakeRegistry(dict):
    def get_entity_ids(self):
        return self.keys()

    get_entity_definition = dict.get

    def get_pipe_ids(self):
        return self.keys()

    get_pipe_definition = dict.get


def make_entity(entityid, tags=None, **overrides):
    params = dict(
        entityid=entityid,
        name=entityid.split(".")[-1],
        merge_columns=["id"],
        tags=tags or {},
        schema=None,
    )
    params.update(overrides)
    return EntityMetadata(**params)


def make_pipe(pipeid, input_entity_ids, output_entity_id, **overrides):
    params = dict(
        pipeid=pipeid,
        name=pipeid,
        execute=lambda **dfs: "df",
        tags={},
        input_entity_ids=input_entity_ids,
        output_entity_id=output_entity_id,
        output_type="delta",
    )
    params.update(overrides)
    return PipeMetadata(**params)


class FakeDpModule:
    """Records declarations with their full kwargs; optionally exposes the
    Lakeflow expectation decorators."""

    def __init__(self, lakeflow=False):
        self.declared = {}  # name -> (kwargs, fn)
        self.expectations = []  # (decorator_name, dict) in application order
        if lakeflow:
            for decorator_name in ("expect_all", "expect_all_or_drop", "expect_all_or_fail"):
                setattr(self, decorator_name, self._make_expectation(decorator_name))

    def materialized_view(self, **kwargs):
        def decorator(fn):
            self.declared[kwargs["name"]] = (kwargs, fn)
            return fn

        return decorator

    def _make_expectation(self, decorator_name):
        def factory(expectations):
            def decorator(fn):
                self.expectations.append((decorator_name, dict(expectations)))
                return fn

            return decorator

        return factory


def single_pipe_graph(entity_tags=None, entity_overrides=None, engine_config=None):
    entities = FakeRegistry(
        {
            e.entityid: e
            for e in [
                make_entity("src.orders"),
                make_entity("silver.orders", tags=entity_tags, **(entity_overrides or {})),
            ]
        }
    )
    pipes = FakeRegistry({"p": make_pipe("p", ["src.orders"], "silver.orders")})
    return entities, pipes


# --------------------------------------------------------------------- #
# OSS emission: entity metadata -> decorator kwargs                      #
# --------------------------------------------------------------------- #


class TestMetadataEmission:
    def test_full_metadata_reaches_the_decorator(self):
        schema = SimpleNamespace(kind="struct")  # opaque passthrough
        entities, pipes = single_pipe_graph(
            entity_tags={
                "comment": "Cleaned orders",
                "sdp.table_properties.delta.enableChangeDataFeed": "true",
                "sdp.table_properties.quality": "silver",
            },
            entity_overrides={
                "partition_columns": ["order_date"],
                "cluster_columns": ["customer_id"],
                "schema": schema,
            },
        )
        dp = FakeDpModule()
        engine = OssSdpEngine(entities, pipes, dp_module=dp)

        engine.declare_pipeline(engine.build_plan())

        kwargs, _ = dp.declared["silver.orders"]
        assert kwargs["comment"] == "Cleaned orders"
        assert kwargs["table_properties"] == {
            "delta.enableChangeDataFeed": "true",
            "quality": "silver",
        }
        assert kwargs["partition_cols"] == ["order_date"]
        assert kwargs["cluster_by"] == ["customer_id"]
        assert kwargs["schema"] is schema

    def test_empty_metadata_is_omitted_not_passed_as_empties(self):
        entities, pipes = single_pipe_graph()
        dp = FakeDpModule()
        engine = OssSdpEngine(entities, pipes, dp_module=dp)

        engine.declare_pipeline(engine.build_plan())

        kwargs, _ = dp.declared["silver.orders"]
        assert set(kwargs) == {"name"}

    def test_cdf_is_not_forced_unlike_the_runner_engine(self):
        """Documented dual-engine divergence: the runner's Delta provider
        force-sets delta.enableChangeDataFeed; SDP declares only what the
        entity asks for."""
        entities, pipes = single_pipe_graph()
        dp = FakeDpModule()
        engine = OssSdpEngine(entities, pipes, dp_module=dp)

        engine.declare_pipeline(engine.build_plan())

        kwargs, _ = dp.declared["silver.orders"]
        assert "table_properties" not in kwargs

    def test_engine_config_table_properties_with_entity_tag_precedence(self):
        entities, pipes = single_pipe_graph(
            entity_tags={"sdp.table_properties.owner": "entity-team"}
        )
        engine_config = {
            "p": {"sdp": {"table_properties": {"owner": "config-team", " layer ": " silver "}}}
        }
        dp = FakeDpModule()
        engine = OssSdpEngine(entities, pipes, engine_config=engine_config, dp_module=dp)

        engine.declare_pipeline(engine.build_plan())

        kwargs, _ = dp.declared["silver.orders"]
        assert kwargs["table_properties"] == {"owner": "entity-team", "layer": "silver"}


# --------------------------------------------------------------------- #
# Databricks adapter                                                     #
# --------------------------------------------------------------------- #


EXPECTATION_CONFIG = {
    "p": {
        "databricks_sdp": {
            "expectations": {" valid_id ": " id IS NOT NULL "},
            "expectations_drop": {"positive_qty": "qty > 0"},
            "expectations_fail": {"no_future": "d <= current_date()"},
        }
    }
}


class TestDatabricksAdapter:
    def test_expectation_blocks_map_to_lakeflow_decorators(self):
        entities, pipes = single_pipe_graph()
        dp = FakeDpModule(lakeflow=True)
        engine = DatabricksSdpEngine(
            entities, pipes, engine_config=EXPECTATION_CONFIG, dp_module=dp
        )

        engine.declare_pipeline(engine.build_plan())

        assert ("expect_all", {"valid_id": "id IS NOT NULL"}) in dp.expectations
        assert ("expect_all_or_drop", {"positive_qty": "qty > 0"}) in dp.expectations
        assert ("expect_all_or_fail", {"no_future": "d <= current_date()"}) in dp.expectations
        assert "silver.orders" in dp.declared, "the MV must still be declared"

    def test_expectations_against_oss_runtime_fail_actionably(self):
        """A Databricks engine pointed at an OSS dp module (no expect_all)
        must fail at declaration, not silently drop data-quality rules."""
        entities, pipes = single_pipe_graph()
        engine = DatabricksSdpEngine(
            entities, pipes, engine_config=EXPECTATION_CONFIG, dp_module=FakeDpModule()
        )

        with pytest.raises(RuntimeError, match="Lakeflow"):
            engine.declare_pipeline(engine.build_plan())

    def test_no_expectations_means_no_decorators(self):
        entities, pipes = single_pipe_graph()
        dp = FakeDpModule(lakeflow=True)
        engine = DatabricksSdpEngine(entities, pipes, dp_module=dp)

        engine.declare_pipeline(engine.build_plan())

        assert dp.expectations == []

    def test_inherits_oss_metadata_emission(self):
        entities, pipes = single_pipe_graph(
            entity_tags={"comment": "x"}, entity_overrides={"partition_columns": ["d"]}
        )
        dp = FakeDpModule(lakeflow=True)
        engine = DatabricksSdpEngine(entities, pipes, dp_module=dp)

        engine.declare_pipeline(engine.build_plan())

        kwargs, _ = dp.declared["silver.orders"]
        assert kwargs["comment"] == "x" and kwargs["partition_cols"] == ["d"]


class TestDatabricksEngineExtension:
    def test_resolves_via_the_core_naming_convention(self):
        extension = kindling._load_engine_extension("databricks_sdp")

        assert extension.name == "databricks_sdp"
        assert extension.owns_incrementality is True

    def test_declare_pipeline_passes_the_adapter_engine_factory(self, monkeypatch):
        import kindling_sdp.bootstrap as sdp_bootstrap

        received = {}

        def fake_declare(pipe_ids=None, engine_factory=None):
            received.update(pipe_ids=pipe_ids, engine_factory=engine_factory)
            return "plan"

        monkeypatch.setattr(sdp_bootstrap, "declare_pipeline", fake_declare)

        assert engine_extension().declare_pipeline(["p1"]) == "plan"
        assert received["engine_factory"] is DatabricksSdpEngine
        assert received["pipe_ids"] == ["p1"]
