"""Unit tests for the Phase-5 SCD -> AUTO CDC mapping in the Databricks
adapter.

The FakeLakeflowDp mirrors the documented Lakeflow surface (temporary_view,
create_streaming_table, create_auto_cdc_flow, create_auto_cdc_from_
snapshot_flow) — AUTO CDC is explicitly unsupported on OSS Spark SDP, so a
recording fake is the only local test seam, mirroring the current Azure
Databricks CDC reference.
"""

import pytest
from kindling.data_entities import EntityMetadata
from kindling.data_pipes import PipeMetadata
from kindling_ext_databricks import (
    DatabricksSdpEngine,
    scd_spec_from_tags,
    validate_scd_spec,
)
from kindling_ext_sdp import DeclarationValidationError
from kindling_ext_sdp.declaration_plan import DatasetDeclaration, DatasetType

# --------------------------------------------------------------------- #
# Fixtures                                                               #
# --------------------------------------------------------------------- #


@pytest.fixture(autouse=True)
def hermetic_expr(monkeypatch):
    """pyspark's expr() needs a live JVM; without this stub the delete_when
    tests pass only when an earlier suite member happens to have started
    Spark. Unit tests must not depend on suite order."""
    monkeypatch.setattr("pyspark.sql.functions.expr", lambda predicate: f"expr({predicate})")


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
        merge_columns=["customer_id"],
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


class FakeLakeflowDp:
    """Records the Lakeflow declaration surface AUTO CDC needs."""

    def __init__(self):
        self.views = {}  # name -> fn
        self.streaming_tables = {}  # name -> kwargs
        self.cdc_flows = []  # kwargs of create_auto_cdc_flow
        self.snapshot_flows = []  # kwargs of create_auto_cdc_from_snapshot_flow
        self.declared_mvs = {}

    def materialized_view(self, **kwargs):
        def decorator(fn):
            self.declared_mvs[kwargs["name"]] = (kwargs, fn)
            return fn

        return decorator

    def temporary_view(self, name=None):
        def decorator(fn):
            self.views[name or fn.__name__] = fn
            return fn

        return decorator

    def create_streaming_table(self, name, **kwargs):
        self.streaming_tables[name] = kwargs

    def create_auto_cdc_flow(self, **kwargs):
        self.cdc_flows.append(kwargs)

    def create_auto_cdc_from_snapshot_flow(self, **kwargs):
        self.snapshot_flows.append(kwargs)


CHANGE_FEED_TAGS = {
    "scd.type": "2",
    "scd.sequence_by": "updated_at",
    "scd.source_kind": "change_feed",
    "scd.delete_when": "status = 'DELETED'",
}

SNAPSHOT_TAGS = {
    "scd.type": "2",
    "scd.source_kind": "snapshot",
}


def scd_graph(tags, **entity_overrides):
    entities = FakeRegistry(
        {
            e.entityid: e
            for e in [
                make_entity("bronze.customers"),
                make_entity("silver.customers", tags=tags, **entity_overrides),
            ]
        }
    )
    pipes = FakeRegistry(
        {"scd.customers": make_pipe("scd.customers", ["bronze.customers"], "silver.customers")}
    )
    return entities, pipes


def declare(tags, dp=None, engine_config=None, **entity_overrides):
    entities, pipes = scd_graph(tags, **entity_overrides)
    dp = dp or FakeLakeflowDp()
    engine = DatabricksSdpEngine(entities, pipes, engine_config=engine_config, dp_module=dp)
    engine.declare_pipeline(engine.build_plan())
    return dp


# --------------------------------------------------------------------- #
# Change-feed mapping                                                    #
# --------------------------------------------------------------------- #


class TestChangeFeedMapping:
    def test_emits_view_streaming_table_and_cdc_flow(self):
        dp = declare(CHANGE_FEED_TAGS)

        assert "silver.customers__scd_source" in dp.views
        assert "silver.customers" in dp.streaming_tables
        (flow,) = dp.cdc_flows
        assert flow["target"] == "silver.customers"
        assert flow["source"] == "silver.customers__scd_source"
        assert flow["keys"] == ["customer_id"]
        assert flow["sequence_by"] == "updated_at"
        assert flow["stored_as_scd_type"] == 2

    def test_delete_when_maps_to_apply_as_deletes(self):
        dp = declare(CHANGE_FEED_TAGS)

        (flow,) = dp.cdc_flows
        assert flow["apply_as_deletes"] == "expr(status = 'DELETED')"

    def test_no_delete_when_omits_apply_as_deletes(self):
        tags = {k: v for k, v in CHANGE_FEED_TAGS.items() if k != "scd.delete_when"}
        dp = declare(tags)

        assert "apply_as_deletes" not in dp.cdc_flows[0]

    def test_sequence_column_excluded_from_history_tracking(self):
        """Parity with #159: ordering authority is not content — a newer
        sequence value alone must not create a history version."""
        dp = declare(CHANGE_FEED_TAGS)

        assert dp.cdc_flows[0]["track_history_except_column_list"] == ["updated_at"]

    def test_explicit_tracked_columns_map_to_track_history_column_list(self):
        dp = declare({**CHANGE_FEED_TAGS, "scd.tracked": "status, tier"})

        flow = dp.cdc_flows[0]
        assert flow["track_history_column_list"] == ["status", "tier"]
        assert "track_history_except_column_list" not in flow

    def test_scd_type_1_fails_fast_matching_core_registration(self):
        """Core registration only accepts scd.type='2'; the adapter must
        not encode reachable behavior for types kindling cannot declare."""
        entities, pipes = scd_graph({**CHANGE_FEED_TAGS, "scd.type": "1"})
        engine = DatabricksSdpEngine(entities, pipes, dp_module=FakeLakeflowDp())

        with pytest.raises(DeclarationValidationError, match="scd_type_unsupported"):
            engine.build_plan()

    def test_runner_schema_is_not_passed_to_the_streaming_table(self):
        """Documented divergence: AUTO CDC emits __START_AT/__END_AT, not
        the runner's effective-date columns the entity schema describes."""
        dp = declare(CHANGE_FEED_TAGS, schema="not-none-sentinel")

        assert "schema" not in dp.streaming_tables["silver.customers"]

    def test_entity_metadata_still_reaches_the_streaming_table(self):
        dp = declare(
            {**CHANGE_FEED_TAGS, "comment": "Customer history"},
            partition_columns=["region"],
        )

        st = dp.streaming_tables["silver.customers"]
        assert st["comment"] == "Customer history"
        assert st["partition_cols"] == ["region"]

    def test_expectations_attach_to_the_source_view(self):
        class LakeflowWithExpectations(FakeLakeflowDp):
            def __init__(self):
                super().__init__()
                self.expectations = []

            def expect_all(self, expectations):
                def decorator(fn):
                    self.expectations.append(dict(expectations))
                    return fn

                return decorator

        dp = LakeflowWithExpectations()
        declare(
            CHANGE_FEED_TAGS,
            dp=dp,
            engine_config={
                "scd.customers": {
                    "databricks_sdp": {"expectations": {"valid_key": "customer_id IS NOT NULL"}}
                }
            },
        )

        assert dp.expectations == [{"valid_key": "customer_id IS NOT NULL"}]
        assert "silver.customers__scd_source" in dp.views


class FakeStreamingSession:
    """Records batch vs streaming table reads."""

    class _ReadStream:
        def __init__(self, outer):
            self.outer = outer

        def table(self, name):
            self.outer.stream_reads.append(name)
            return f"stream:{name}"

    def __init__(self):
        self.batch_reads = []
        self.stream_reads = []
        self.readStream = FakeStreamingSession._ReadStream(self)

    def table(self, name):
        self.batch_reads.append(name)
        return f"df:{name}"


class TestChangeFeedStreamingSource:
    """The change-feed source view must consume the driving input as a
    stream — AUTO CDC replaces foreachBatch+MERGE, and that only holds if
    the feed is incremental, not a full batch re-read per update."""

    def _declare_with_session(self, tags, input_ids=("bronze.customers", "ref.regions")):
        captured = {}

        def execute(**dfs):
            captured.update(dfs)
            return "df:out"

        entities = FakeRegistry(
            {
                e.entityid: e
                for e in [
                    *(make_entity(i) for i in input_ids),
                    make_entity("silver.customers", tags=tags),
                ]
            }
        )
        pipes = FakeRegistry(
            {
                "scd.customers": make_pipe(
                    "scd.customers", list(input_ids), "silver.customers", execute=execute
                )
            }
        )
        dp = FakeLakeflowDp()
        session = FakeStreamingSession()
        engine = DatabricksSdpEngine(
            entities, pipes, dp_module=dp, session_provider=lambda: session
        )
        engine.declare_pipeline(engine.build_plan())
        dp.views["silver.customers__scd_source"]()
        return session, captured

    def test_driving_input_streams_remaining_inputs_stay_batch(self):
        session, captured = self._declare_with_session(CHANGE_FEED_TAGS)

        assert session.stream_reads == ["bronze.customers"]
        assert session.batch_reads == ["ref.regions"]
        assert captured["bronze_customers"] == "stream:bronze.customers"
        assert captured["ref_regions"] == "df:ref.regions"

    def test_external_driving_input_uses_stream_resolver(self):
        captured = {}

        def execute(**dfs):
            captured.update(dfs)
            return "df:out"

        entities = FakeRegistry(
            {
                e.entityid: e
                for e in [
                    make_entity("bronze.customers"),
                    make_entity("ref.regions"),
                    make_entity("silver.customers", tags=CHANGE_FEED_TAGS),
                ]
            }
        )
        pipes = FakeRegistry(
            {
                "scd.customers": make_pipe(
                    "scd.customers",
                    ["bronze.customers", "ref.regions"],
                    "silver.customers",
                    execute=execute,
                )
            }
        )
        dp = FakeLakeflowDp()
        session = FakeStreamingSession()

        def batch_resolver(spark, entity_id):
            return spark.table(f"catalog.{entity_id}")

        def stream_resolver(spark, entity_id):
            return spark.readStream.table(f"catalog.{entity_id}")

        engine = DatabricksSdpEngine(
            entities,
            pipes,
            dp_module=dp,
            session_provider=lambda: session,
            external_read_resolver=batch_resolver,
            external_stream_read_resolver=stream_resolver,
        )
        engine.declare_pipeline(engine.build_plan())
        dp.views["silver.customers__scd_source"]()

        assert session.stream_reads == ["catalog.bronze.customers"]
        assert session.batch_reads == ["catalog.ref.regions"]
        assert captured["bronze_customers"] == "stream:catalog.bronze.customers"
        assert captured["ref_regions"] == "df:catalog.ref.regions"

    def test_snapshot_source_keeps_batch_reads(self):
        """The snapshot API diffs whole snapshots per update — a streaming
        read would be wrong there."""
        session, captured = self._declare_with_session(
            SNAPSHOT_TAGS, input_ids=("bronze.customers",)
        )

        assert session.stream_reads == []
        assert session.batch_reads == ["bronze.customers"]


# --------------------------------------------------------------------- #
# Snapshot mapping                                                       #
# --------------------------------------------------------------------- #


class TestSnapshotMapping:
    def test_snapshot_maps_to_from_snapshot_flow(self):
        dp = declare(SNAPSHOT_TAGS)

        (flow,) = dp.snapshot_flows
        assert flow["target"] == "silver.customers"
        assert flow["source"] == "silver.customers__scd_source"
        assert flow["keys"] == ["customer_id"]
        assert flow["stored_as_scd_type"] == 2
        assert dp.cdc_flows == []

    def test_close_on_missing_sugar_also_maps_to_snapshot_flow(self):
        dp = declare({"scd.type": "2", "scd.close_on_missing": "true"})

        assert len(dp.snapshot_flows) == 1

    def test_snapshot_flow_takes_no_sequence_by(self):
        """Documented divergence: snapshot ordering is ingestion order —
        the API has no sequencing column."""
        dp = declare({**SNAPSHOT_TAGS, "scd.sequence_by": "updated_at"})

        assert "sequence_by" not in dp.snapshot_flows[0]

    def test_snapshot_delete_when_fails_fast(self):
        entities, pipes = scd_graph({**SNAPSHOT_TAGS, "scd.delete_when": "deleted = true"})
        engine = DatabricksSdpEngine(entities, pipes, dp_module=FakeLakeflowDp())

        with pytest.raises(
            DeclarationValidationError,
            match="scd_delete_when_snapshot_unsupported",
        ):
            engine.build_plan()


# --------------------------------------------------------------------- #
# Validation                                                             #
# --------------------------------------------------------------------- #


class TestAutoCdcValidation:
    def test_change_feed_without_sequence_by_fails_fast(self):
        entities, pipes = scd_graph({"scd.type": "2", "scd.source_kind": "change_feed"})
        engine = DatabricksSdpEngine(entities, pipes, dp_module=FakeLakeflowDp())

        with pytest.raises(DeclarationValidationError, match="scd_sequence_by_required"):
            engine.build_plan()

    def test_snapshot_without_sequence_by_is_fine(self):
        entities, pipes = scd_graph(SNAPSHOT_TAGS)
        engine = DatabricksSdpEngine(entities, pipes, dp_module=FakeLakeflowDp())

        assert engine.validate() == []

    def test_bitemporal_scd_type_fails_fast(self):
        entities, pipes = scd_graph({**CHANGE_FEED_TAGS, "scd.type": "bitemporal"})
        engine = DatabricksSdpEngine(entities, pipes, dp_module=FakeLakeflowDp())

        with pytest.raises(DeclarationValidationError, match="scd_type_unsupported"):
            engine.build_plan()

    def test_missing_merge_columns_fails_fast(self):
        entities, pipes = scd_graph(CHANGE_FEED_TAGS, merge_columns=[])
        engine = DatabricksSdpEngine(entities, pipes, dp_module=FakeLakeflowDp())

        with pytest.raises(DeclarationValidationError, match="scd_keys_required"):
            engine.build_plan()

    def test_missing_scd_entity_fails_with_dataset_name(self):
        dataset = DatasetDeclaration(
            name="silver.missing",
            dataset_type=DatasetType.MATERIALIZED_VIEW,
            execute=lambda **dfs: "df",
            pipe_id="scd.missing",
            inputs=(),
        )
        engine = DatabricksSdpEngine(FakeRegistry(), FakeRegistry(), dp_module=FakeLakeflowDp())

        with pytest.raises(RuntimeError, match="silver.missing"):
            engine._declare_scd_dataset(
                FakeLakeflowDp(),
                dataset,
                scd_spec_from_tags(CHANGE_FEED_TAGS),
            )

    def test_duplicate_scd_writers_fail_once_per_entity_issue(self):
        entities, pipes = scd_graph({"scd.type": "2", "scd.source_kind": "change_feed"})
        pipes["scd.customers.second"] = make_pipe(
            "scd.customers.second",
            ["bronze.customers"],
            "silver.customers",
        )
        engine = DatabricksSdpEngine(entities, pipes, dp_module=FakeLakeflowDp())

        issues = engine.validate()

        assert sum(issue.code == "duplicate_output_entity" for issue in issues) == 1
        assert sum(issue.code == "scd_sequence_by_required" for issue in issues) == 1

    def test_non_scd_datasets_still_declare_as_materialized_views(self):
        entities, pipes = scd_graph({})  # no scd tags on silver
        dp = FakeLakeflowDp()
        engine = DatabricksSdpEngine(entities, pipes, dp_module=dp)

        engine.declare_pipeline(engine.build_plan())

        assert "silver.customers" in dp.declared_mvs
        assert dp.cdc_flows == [] and dp.snapshot_flows == []


# --------------------------------------------------------------------- #
# Spec parsing                                                           #
# --------------------------------------------------------------------- #


class TestScdSpecParsing:
    def test_non_scd_tags_parse_to_none(self):
        assert scd_spec_from_tags({"provider_type": "delta"}) is None
        assert scd_spec_from_tags(None) is None

    def test_defaults_mirror_kindling_scd_config(self):
        spec = scd_spec_from_tags({"scd.type": "2"})

        assert spec.source_kind == "change_feed"
        assert spec.sequence_by is None
        assert spec.tracked_columns == ()

    def test_validate_reports_all_issues_at_once(self):
        spec = scd_spec_from_tags({"scd.type": "9", "scd.source_kind": "change_feed"})

        codes = [code for code, _ in validate_scd_spec(spec, [])]
        assert codes == ["scd_type_unsupported", "scd_sequence_by_required", "scd_keys_required"]
