"""Unit tests for the kindling_sdp Phase-1 declaration engine.

Covers internal/external input classification, dataset-type selection
precedence, fail-fast validation (all errors accumulated), and
OSS-vs-Databricks capability gating. The plan is pure metadata — no Spark
session is needed.
"""

import sys

import pytest
from kindling.data_entities import EntityMetadata
from kindling.data_pipes import PipeMetadata

# The extension package root is added to sys.path by tests/conftest.py,
# matching the other extension packages (kindling_visualization, ...).
from kindling_sdp import (
    DATABRICKS_SDP,
    OSS_SDP,
    DatasetType,
    DeclarationEngine,
    DeclarationValidationError,
    InputClassification,
    supports_auto_cdc,
    supports_expectations,
    supports_incremental_mv_refresh,
)

# --------------------------------------------------------------------- #
# Fixtures: fake registries over a small bronze -> silver -> gold graph #
# --------------------------------------------------------------------- #


class FakeEntityRegistry:
    """Dict-backed stand-in for kindling.data_entities.DataEntityRegistry."""

    def __init__(self, entities):
        self.registry = {entity.entityid: entity for entity in entities}

    def get_entity_ids(self):
        return self.registry.keys()

    def get_entity_definition(self, name):
        return self.registry.get(name)


class FakePipeRegistry:
    """Dict-backed stand-in for kindling.data_pipes.DataPipesRegistry."""

    def __init__(self, pipes):
        self.registry = {pipe.pipeid: pipe for pipe in pipes}

    def get_pipe_ids(self):
        return self.registry.keys()

    def get_pipe_definition(self, name):
        return self.registry.get(name)


class PlanOnlyEngine(DeclarationEngine):
    """Concrete engine for tests: records the declared plan, emits nothing."""

    declared_plan = None

    def declare_pipeline(self, plan):
        self.declared_plan = plan


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


def make_pipe(pipeid, input_entity_ids, output_entity_id, tags=None, **overrides):
    params = dict(
        pipeid=pipeid,
        name=pipeid,
        execute=lambda **dfs: None,
        tags=tags or {},
        input_entity_ids=input_entity_ids,
        output_entity_id=output_entity_id,
        output_type="delta",
    )
    params.update(overrides)
    return PipeMetadata(**params)


@pytest.fixture
def entities():
    """landing/ref are external sources; bronze/silver/gold are produced."""
    return [
        make_entity("landing.orders", tags={"read_only": "true"}),
        make_entity("ref.customers"),
        make_entity("bronze.orders"),
        make_entity(
            "silver.orders",
            tags={"comment": "Cleaned orders", "domain": "sales"},
            partition_columns=["order_date"],
            cluster_columns=["customer_id"],
        ),
        make_entity("gold.orders_summary"),
    ]


@pytest.fixture
def pipes():
    return [
        make_pipe("ingest.orders", ["landing.orders"], "bronze.orders"),
        make_pipe(
            "bronze_to_silver.orders",
            ["bronze.orders", "ref.customers"],
            "silver.orders",
        ),
        make_pipe("silver_to_gold.orders", ["silver.orders"], "gold.orders_summary"),
    ]


@pytest.fixture
def entity_registry(entities):
    return FakeEntityRegistry(entities)


@pytest.fixture
def pipe_registry(pipes):
    return FakePipeRegistry(pipes)


def make_engine(entity_registry, pipe_registry, capabilities=OSS_SDP, engine_config=None):
    return PlanOnlyEngine(
        entity_registry=entity_registry,
        pipe_registry=pipe_registry,
        capabilities=capabilities,
        engine_config=engine_config,
    )


def issue_codes(issues):
    return [issue.code for issue in issues]


# --------------------------------------------------------------------- #
# Phase-1 invariant: no pyspark.pipelines dependency                     #
# --------------------------------------------------------------------- #


class TestPhase1Invariants:
    def test_importing_kindling_sdp_does_not_import_pyspark_pipelines(self):
        assert "kindling_sdp" in sys.modules
        assert "pyspark.pipelines" not in sys.modules


# --------------------------------------------------------------------- #
# Input classification                                                   #
# --------------------------------------------------------------------- #


class TestInputClassification:
    def test_input_produced_by_another_pipe_is_internal(self, entity_registry, pipe_registry):
        engine = make_engine(entity_registry, pipe_registry)

        inputs = engine.classify_inputs("bronze_to_silver.orders")
        by_id = {classified.entity_id: classified for classified in inputs}

        assert by_id["bronze.orders"].classification is InputClassification.INTERNAL
        assert by_id["bronze.orders"].produced_by == "ingest.orders"

    def test_input_with_no_producer_is_external(self, entity_registry, pipe_registry):
        engine = make_engine(entity_registry, pipe_registry)

        inputs = engine.classify_inputs("bronze_to_silver.orders")
        by_id = {classified.entity_id: classified for classified in inputs}

        assert by_id["ref.customers"].classification is InputClassification.EXTERNAL
        assert by_id["ref.customers"].produced_by is None

    def test_landing_source_is_external(self, entity_registry, pipe_registry):
        engine = make_engine(entity_registry, pipe_registry)

        (landing,) = engine.classify_inputs("ingest.orders")

        assert landing.entity_id == "landing.orders"
        assert landing.classification is InputClassification.EXTERNAL

    def test_classification_is_scoped_to_selected_pipes(self, entity_registry, pipe_registry):
        """silver.orders is external when its producer is not in the selection."""
        engine = make_engine(entity_registry, pipe_registry)

        (silver,) = engine.classify_inputs(
            "silver_to_gold.orders", pipe_ids=["silver_to_gold.orders"]
        )

        assert silver.classification is InputClassification.EXTERNAL

    def test_unregistered_pipe_id_raises_actionable_error(self, entity_registry, pipe_registry):
        engine = make_engine(entity_registry, pipe_registry)

        with pytest.raises(KeyError, match="'no_such.pipe' is not registered"):
            engine.classify_inputs("no_such.pipe")

    def test_plan_records_classified_inputs(self, entity_registry, pipe_registry):
        engine = make_engine(entity_registry, pipe_registry)

        plan = engine.build_plan()
        gold = plan.get_dataset("gold.orders_summary")

        assert [i.classification for i in gold.inputs] == [InputClassification.INTERNAL]
        assert plan.internal_entity_ids == {
            "bronze.orders",
            "silver.orders",
            "gold.orders_summary",
        }


# --------------------------------------------------------------------- #
# Dataset-type selection precedence                                      #
# --------------------------------------------------------------------- #


class TestDatasetTypeSelection:
    def test_defaults_to_materialized_view(self, entity_registry, pipe_registry):
        engine = make_engine(entity_registry, pipe_registry)

        plan = engine.build_plan()

        assert plan.get_dataset("silver.orders").dataset_type is DatasetType.MATERIALIZED_VIEW

    def test_engine_config_overrides_default(self, entity_registry, pipe_registry):
        engine = make_engine(
            entity_registry,
            pipe_registry,
            engine_config={"ingest.orders": {"sdp": {"dataset_type": "streaming_table"}}},
        )

        plan = engine.build_plan()

        assert plan.get_dataset("bronze.orders").dataset_type is DatasetType.STREAMING_TABLE
        # Other pipes keep the default.
        assert plan.get_dataset("silver.orders").dataset_type is DatasetType.MATERIALIZED_VIEW

    def test_entity_tag_overrides_engine_config(self, entities, pipe_registry):
        """Entity tag sdp.dataset_type is highest precedence."""
        tagged = [
            (
                make_entity("bronze.orders", tags={"sdp.dataset_type": "streaming_table"})
                if entity.entityid == "bronze.orders"
                else entity
            )
            for entity in entities
        ]
        engine = make_engine(
            FakeEntityRegistry(tagged),
            pipe_registry,
            engine_config={"ingest.orders": {"sdp": {"dataset_type": "materialized_view"}}},
        )

        plan = engine.build_plan()

        assert plan.get_dataset("bronze.orders").dataset_type is DatasetType.STREAMING_TABLE

    def test_databricks_target_falls_back_to_common_sdp_block(self, entity_registry, pipe_registry):
        engine = make_engine(
            entity_registry,
            pipe_registry,
            capabilities=DATABRICKS_SDP,
            engine_config={"ingest.orders": {"sdp": {"dataset_type": "streaming_table"}}},
        )

        plan = engine.build_plan()

        assert plan.engine_name == "databricks_sdp"
        assert plan.get_dataset("bronze.orders").dataset_type is DatasetType.STREAMING_TABLE

    def test_invalid_dataset_type_is_a_validation_error(self, entities, pipe_registry):
        tagged = [
            (
                make_entity("bronze.orders", tags={"sdp.dataset_type": "delta_live_table"})
                if entity.entityid == "bronze.orders"
                else entity
            )
            for entity in entities
        ]
        engine = make_engine(FakeEntityRegistry(tagged), pipe_registry)

        issues = engine.validate()

        assert issue_codes(issues) == ["invalid_dataset_type"]
        assert issues[0].pipe_id == "ingest.orders"
        assert "delta_live_table" in issues[0].reason
        assert "materialized_view" in issues[0].reason


# --------------------------------------------------------------------- #
# Fail-fast validation                                                   #
# --------------------------------------------------------------------- #


class TestValidation:
    def test_valid_graph_has_no_issues(self, entity_registry, pipe_registry):
        engine = make_engine(entity_registry, pipe_registry)

        assert engine.validate() == []

    def test_missing_output_entity_id(self, entity_registry):
        pipes = FakePipeRegistry([make_pipe("broken.pipe", ["landing.orders"], "")])
        engine = make_engine(entity_registry, pipes)

        issues = engine.validate()

        assert issue_codes(issues) == ["missing_output_entity"]
        assert issues[0].pipe_id == "broken.pipe"

    def test_unregistered_output_entity(self, entity_registry):
        pipes = FakePipeRegistry([make_pipe("broken.pipe", ["landing.orders"], "bronze.unknown")])
        engine = make_engine(entity_registry, pipes)

        issues = engine.validate()

        assert issue_codes(issues) == ["output_entity_not_registered"]
        assert "bronze.unknown" in issues[0].reason

    def test_non_table_backed_output_provider(self, entities):
        entities = entities + [make_entity("mem.scratch", tags={"provider_type": "memory"})]
        pipes = FakePipeRegistry([make_pipe("broken.pipe", ["landing.orders"], "mem.scratch")])
        engine = make_engine(FakeEntityRegistry(entities), pipes)

        issues = engine.validate()

        assert issue_codes(issues) == ["output_entity_not_table_backed"]
        assert "memory" in issues[0].reason

    def test_sql_view_pipe_fails_fast(self, entity_registry):
        pipes = FakePipeRegistry(
            [
                make_pipe(
                    "view.enriched",
                    ["landing.orders"],
                    "view.enriched",
                    tags={"pipe_type": "view", "provider_type": "memory"},
                    output_type="memory",
                )
            ]
        )
        engine = make_engine(entity_registry, pipes)

        issues = engine.validate()

        assert "sql_view_pipe" in issue_codes(issues)

    def test_unregistered_external_input(self, entity_registry):
        pipes = FakePipeRegistry([make_pipe("broken.pipe", ["nowhere.data"], "bronze.orders")])
        engine = make_engine(entity_registry, pipes)

        issues = engine.validate()

        assert issue_codes(issues) == ["input_entity_not_registered"]
        assert "nowhere.data" in issues[0].reason

    def test_local_only_external_input_provider(self, entities):
        entities = entities + [make_entity("mem.lookup", tags={"provider_type": "memory"})]
        pipes = FakePipeRegistry(
            [make_pipe("broken.pipe", ["landing.orders", "mem.lookup"], "bronze.orders")]
        )
        engine = make_engine(FakeEntityRegistry(entities), pipes)

        issues = engine.validate()

        assert issue_codes(issues) == ["external_input_not_declarable"]
        assert "mem.lookup" in issues[0].reason

    def test_internal_inputs_do_not_require_declarable_providers(
        self, entity_registry, pipe_registry
    ):
        """Internal inputs are validated as their producer's output, not as reads."""
        engine = make_engine(entity_registry, pipe_registry)

        issues = [issue for issue in engine.validate() if issue.code.startswith("external")]

        assert issues == []

    def test_self_referencing_pipe(self, entity_registry):
        pipes = FakePipeRegistry([make_pipe("merge.self", ["bronze.orders"], "bronze.orders")])
        engine = make_engine(entity_registry, pipes)

        issues = engine.validate()

        assert "self_referencing_pipe" in issue_codes(issues)

    def test_duplicate_output_entity(self, entity_registry):
        pipes = FakePipeRegistry(
            [
                make_pipe("ingest.orders.a", ["landing.orders"], "bronze.orders"),
                make_pipe("ingest.orders.b", ["ref.customers"], "bronze.orders"),
            ]
        )
        engine = make_engine(entity_registry, pipes)

        issues = engine.validate()

        duplicates = [i for i in issues if i.code == "duplicate_output_entity"]
        assert len(duplicates) == 1
        assert duplicates[0].pipe_id == "ingest.orders.b"
        assert "ingest.orders.a" in duplicates[0].reason

    def test_unknown_pipe_id_in_selection(self, entity_registry, pipe_registry):
        engine = make_engine(entity_registry, pipe_registry)

        issues = engine.validate(pipe_ids=["no.such.pipe"])

        assert issue_codes(issues) == ["unknown_pipe"]

    def test_all_errors_are_accumulated_not_first_only(self, entity_registry):
        pipes = FakePipeRegistry(
            [
                make_pipe("broken.no_output", ["landing.orders"], ""),
                make_pipe("broken.bad_input", ["nowhere.data"], "bronze.orders"),
                make_pipe("broken.self_ref", ["silver.orders"], "silver.orders"),
            ]
        )
        engine = make_engine(entity_registry, pipes)

        issues = engine.validate()

        codes = issue_codes(issues)
        assert "missing_output_entity" in codes
        assert "input_entity_not_registered" in codes
        assert "self_referencing_pipe" in codes
        assert len(issues) >= 3

    def test_use_watermark_pipes_are_declarable(self, entity_registry, pipes):
        """Watermarking is a signal aspect SDP mode never registers — not an error."""
        watermarked = [
            (
                make_pipe(
                    "bronze_to_silver.orders",
                    ["bronze.orders", "ref.customers"],
                    "silver.orders",
                    use_watermark=True,
                )
                if pipe.pipeid == "bronze_to_silver.orders"
                else pipe
            )
            for pipe in pipes
        ]
        engine = make_engine(entity_registry, FakePipeRegistry(watermarked))

        assert engine.validate() == []


# --------------------------------------------------------------------- #
# Capability gating: OSS vs Databricks                                    #
# --------------------------------------------------------------------- #


class TestCapabilityGating:
    ADAPTER_CONFIG = {
        "bronze_to_silver.orders": {
            "sdp": {
                "expectations": {"valid_order_id": "order_id IS NOT NULL"},
                "refresh_policy": "incremental",
            }
        }
    }

    def test_capability_sets(self):
        assert not supports_expectations(OSS_SDP)
        assert not supports_auto_cdc(OSS_SDP)
        assert not supports_incremental_mv_refresh(OSS_SDP)
        assert supports_expectations(DATABRICKS_SDP)
        assert supports_auto_cdc(DATABRICKS_SDP)
        assert supports_incremental_mv_refresh(DATABRICKS_SDP)
        assert OSS_SDP.engine_name == "sdp"
        assert DATABRICKS_SDP.engine_name == "databricks_sdp"

    def test_adapter_tier_config_fails_on_oss(self, entity_registry, pipe_registry):
        engine = make_engine(
            entity_registry, pipe_registry, capabilities=OSS_SDP, engine_config=self.ADAPTER_CONFIG
        )

        issues = engine.validate()

        gated = [i for i in issues if i.code == "capability_not_supported"]
        assert len(gated) == 2  # expectations AND refresh_policy, accumulated
        assert all(i.pipe_id == "bronze_to_silver.orders" for i in gated)
        reasons = " ".join(i.reason for i in gated)
        assert "expectations" in reasons
        assert "refresh_policy" in reasons

    def test_adapter_tier_config_allowed_on_databricks(self, entity_registry, pipe_registry):
        config = {
            "bronze_to_silver.orders": {
                "databricks_sdp": {
                    "expectations": {"valid_order_id": "order_id IS NOT NULL"},
                    "refresh_policy": "incremental",
                }
            }
        }
        engine = make_engine(
            entity_registry, pipe_registry, capabilities=DATABRICKS_SDP, engine_config=config
        )

        assert engine.validate() == []

    def test_other_engines_config_block_is_ignored(self, entity_registry, pipe_registry):
        """A databricks_sdp block is declared intent for another target; OSS ignores it."""
        config = {
            "bronze_to_silver.orders": {
                "databricks_sdp": {"expectations": {"valid": "order_id IS NOT NULL"}}
            }
        }
        engine = make_engine(
            entity_registry, pipe_registry, capabilities=OSS_SDP, engine_config=config
        )

        assert engine.validate() == []

    def test_scd_tags_fail_on_oss(self, entities, pipe_registry):
        tagged = [
            (
                make_entity(
                    "silver.orders",
                    tags={"scd.type": "2", "scd.tracked": "status"},
                )
                if entity.entityid == "silver.orders"
                else entity
            )
            for entity in entities
        ]
        engine = make_engine(FakeEntityRegistry(tagged), pipe_registry, capabilities=OSS_SDP)

        issues = engine.validate()

        assert issue_codes(issues) == ["capability_not_supported"]
        assert issues[0].pipe_id == "bronze_to_silver.orders"
        assert "AUTO CDC" in issues[0].reason

    def test_scd_tags_pass_gating_on_databricks(self, entities, pipe_registry):
        tagged = [
            (
                make_entity("silver.orders", tags={"scd.type": "2"})
                if entity.entityid == "silver.orders"
                else entity
            )
            for entity in entities
        ]
        engine = make_engine(FakeEntityRegistry(tagged), pipe_registry, capabilities=DATABRICKS_SDP)

        assert engine.validate() == []


# --------------------------------------------------------------------- #
# Plan building                                                          #
# --------------------------------------------------------------------- #


class TestBuildPlan:
    def test_plan_contains_one_dataset_per_pipe(self, entity_registry, pipe_registry):
        engine = make_engine(entity_registry, pipe_registry)

        plan = engine.build_plan()

        assert plan.engine_name == "sdp"
        assert {dataset.name for dataset in plan.datasets} == {
            "bronze.orders",
            "silver.orders",
            "gold.orders_summary",
        }

    def test_entity_metadata_passthrough(self, entity_registry, pipe_registry):
        engine = make_engine(entity_registry, pipe_registry)

        silver = engine.build_plan().get_dataset("silver.orders")

        assert silver.partition_columns == ("order_date",)
        assert silver.cluster_columns == ("customer_id",)
        assert silver.tags["domain"] == "sales"
        assert silver.comment == "Cleaned orders"
        assert silver.pipe_id == "bronze_to_silver.orders"

    def test_plan_carries_the_pipe_execute_callable_unchanged(self, entity_registry, pipe_registry):
        engine = make_engine(entity_registry, pipe_registry)

        plan = engine.build_plan()

        for dataset in plan.datasets:
            registered = pipe_registry.get_pipe_definition(dataset.pipe_id)
            assert dataset.execute is registered.execute

    def test_build_plan_raises_with_all_issues(self, entity_registry):
        pipes = FakePipeRegistry(
            [
                make_pipe("broken.no_output", ["landing.orders"], ""),
                make_pipe("broken.bad_input", ["nowhere.data"], "bronze.orders"),
            ]
        )
        engine = make_engine(entity_registry, pipes)

        with pytest.raises(DeclarationValidationError) as exc_info:
            engine.build_plan()

        error = exc_info.value
        assert len(error.issues) == 2
        assert "broken.no_output" in str(error)
        assert "broken.bad_input" in str(error)
        assert "2 error(s)" in str(error)

    def test_build_plan_with_pipe_subset(self, entity_registry, pipe_registry):
        engine = make_engine(entity_registry, pipe_registry)

        plan = engine.build_plan(pipe_ids=["ingest.orders"])

        assert [dataset.name for dataset in plan.datasets] == ["bronze.orders"]

    def test_declaration_engine_is_abstract(self, entity_registry, pipe_registry):
        with pytest.raises(TypeError):
            DeclarationEngine(
                entity_registry=entity_registry,
                pipe_registry=pipe_registry,
                capabilities=OSS_SDP,
            )
