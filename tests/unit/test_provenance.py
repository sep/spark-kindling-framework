"""Provenance stamping: stamp values, schema augmentation, secret exclusion
and the execution-id capture. No Spark session; the column expression and
the write paths are covered in tests/integration."""

from unittest.mock import MagicMock

import pytest
from kindling import config_loaders
from kindling.data_entities import EntityMetadata
from kindling.data_pipes import PipeMetadata
from kindling.provenance import (
    MODE_BATCH,
    ProvenanceRunCapture,
    augment_schema_for_provenance,
    build_provenance_stamp,
    provenance_struct_type,
    selected_pipe_tags,
)
from kindling.signaling import BlinkerSignalProvider
from pyspark.sql.types import StringType, StructField, StructType

SCHEMA = StructType([StructField("id", StringType(), False)])


def _entity(tags=None):
    return EntityMetadata("silver.orders", "orders", ["id"], dict(tags or {}), SCHEMA)


def _pipe(tags=None):
    return PipeMetadata(
        pipeid="silver.build_orders",
        name="Build orders",
        execute=lambda df: df,
        tags=dict(tags or {}),
        input_entity_ids=["bronze.orders", "ref.customers"],
        output_entity_id="silver.orders",
        output_type="delta",
    )


@pytest.fixture(autouse=True)
def _clear_secret_digests(monkeypatch):
    monkeypatch.setattr(config_loaders, "_RESOLVED_SECRET_DIGESTS", set())


class TestStructType:
    def test_fields_in_contract_order_all_nullable(self):
        struct = provenance_struct_type()
        assert struct.fieldNames() == [
            "pipe_id",
            "pipe_name",
            "run_id",
            "execution_id",
            "persist_id",
            "processed_at",
            "mode",
            "app_name",
            "environment",
            "platform",
            "kindling_version",
            "input_entity_ids",
            "input_versions",
            "tags",
            "schema_version",
        ]
        assert all(field.nullable for field in struct.fields)


class TestAugmentSchema:
    def test_adds_column_when_enabled(self):
        schema = augment_schema_for_provenance(SCHEMA, _entity({"provenance.enabled": "true"}))
        assert schema.fieldNames() == ["id", "__provenance"]
        assert schema["__provenance"].dataType == provenance_struct_type()
        assert schema["__provenance"].nullable

    def test_renamed_column(self):
        entity = _entity({"provenance.enabled": "true", "provenance.column": "_lineage"})
        assert augment_schema_for_provenance(SCHEMA, entity).fieldNames() == ["id", "_lineage"]

    def test_disabled_or_inferred_schema_unchanged(self):
        assert augment_schema_for_provenance(SCHEMA, _entity()) is SCHEMA
        assert augment_schema_for_provenance(None, _entity({"provenance.enabled": "true"})) is None

    def test_idempotent(self):
        entity = _entity({"provenance.enabled": "true"})
        once = augment_schema_for_provenance(SCHEMA, entity)
        assert augment_schema_for_provenance(once, entity).fieldNames() == ["id", "__provenance"]


class TestSelectedPipeTags:
    def test_allow_list_with_globs_sorted(self):
        entity = _entity({"provenance.enabled": "true", "provenance.pipe_tags": "owner,layer.*"})
        pipe = _pipe({"owner": "sales", "layer.tier": "gold", "layer.zone": "eu", "other": "x"})
        assert selected_pipe_tags(pipe, entity) == (
            ("layer.tier", "gold"),
            ("layer.zone", "eu"),
            ("owner", "sales"),
        )

    def test_no_allow_list_selects_nothing(self):
        assert (
            selected_pipe_tags(_pipe({"owner": "x"}), _entity({"provenance.enabled": "true"})) == ()
        )

    def test_resolved_secret_values_are_excluded(self):
        provider = MagicMock()
        provider.get_secret.return_value = "s3cr3t"
        resolved = config_loaders.resolve_secret_value("@secret:scope:key", provider)
        assert resolved == "s3cr3t"
        entity = _entity({"provenance.enabled": "true", "provenance.pipe_tags": "token,owner"})
        pipe = _pipe({"token": resolved, "owner": "sales"})
        assert selected_pipe_tags(pipe, entity) == (("owner", "sales"),)

    def test_unresolved_secret_references_are_excluded(self):
        entity = _entity({"provenance.enabled": "true", "provenance.pipe_tags": "token"})
        assert selected_pipe_tags(_pipe({"token": "@secret:scope:key"}), entity) == ()


class TestBuildStamp:
    def test_values_from_pipe_and_context(self):
        config_service = MagicMock()
        config_service.get.side_effect = lambda key, default=None: {
            "kindling.platform.name": "standalone"
        }.get(key, default)
        config_service.initial_config = {"app_name": "orders-app", "environment": "prod"}
        entity = _entity({"provenance.enabled": "true", "provenance.pipe_tags": "owner"})
        stamp = build_provenance_stamp(
            _pipe({"owner": "sales"}),
            entity,
            mode=MODE_BATCH,
            persist_id="p-1",
            execution_id="e-1",
            config_service=config_service,
        )
        assert stamp.pipe_id == "silver.build_orders"
        assert stamp.pipe_name == "Build orders"
        assert stamp.run_id is None  # until the runtime manifest lands
        assert stamp.execution_id == "e-1"
        assert stamp.persist_id == "p-1"
        assert stamp.mode == "batch"
        assert stamp.app_name == "orders-app"
        assert stamp.environment == "prod"
        assert stamp.platform == "standalone"
        assert stamp.input_entity_ids == ("bronze.orders", "ref.customers")
        assert stamp.tags == (("owner", "sales"),)
        assert stamp.processed_at is None

    def test_missing_context_is_null(self):
        stamp = build_provenance_stamp(
            _pipe(),
            _entity({"provenance.enabled": "true"}),
            mode=MODE_BATCH,
            config_service=MagicMock(),
        )
        assert stamp.app_name is None
        assert stamp.environment is None
        assert stamp.platform is None
        assert stamp.execution_id is None


class TestRunCapture:
    def _capture(self):
        signals = BlinkerSignalProvider()
        capture = ProvenanceRunCapture(signals)
        capture.register()
        capture.register()  # idempotent
        return signals, capture

    @pytest.mark.parametrize(
        "started, finished",
        [
            ("orchestrator.pipe_started", "orchestrator.pipe_completed"),
            ("orchestrator.pipe_started", "orchestrator.pipe_failed"),
            ("orchestrator.pipe_started", "orchestrator.pipe_skipped"),
            ("datapipes.before_pipe", "datapipes.after_pipe"),
            ("datapipes.before_pipe", "datapipes.pipe_failed"),
            ("datapipes.before_pipe", "datapipes.pipe_skipped"),
        ],
    )
    def test_captured_until_the_pipe_ends(self, started, finished):
        signals, capture = self._capture()
        signals.get_signal(started).send(None, pipe_id="p1", run_id="run-1")
        assert capture.execution_id("p1") == "run-1"
        assert capture.execution_id("p2") is None
        signals.get_signal(finished).send(None, pipe_id="p1")
        assert capture.execution_id("p1") is None

    def test_retry_keeps_the_id(self):
        signals, capture = self._capture()
        signals.get_signal("orchestrator.pipe_started").send(None, pipe_id="p1", run_id="run-1")
        signals.create_signal("orchestrator.pipe_retrying").send(None, pipe_id="p1")
        assert capture.execution_id("p1") == "run-1"

    def test_streaming_start_signal(self):
        signals, capture = self._capture()
        signals.get_signal("streaming.pipe_starting").send(None, pipe_id="s1", run_id="run-9")
        assert capture.execution_id("s1") == "run-9"

    def test_unregistered_capture_has_nothing(self):
        assert ProvenanceRunCapture(BlinkerSignalProvider()).execution_id("p1") is None


def test_stamp_values_are_text_or_null_for_test_doubles():
    """Only str values reach the stamp: a Mock pipe (as many unit tests use)
    gives nulls, never a Mock literal."""
    pipe = MagicMock(input_entity_ids="not-a-list")
    stamp = build_provenance_stamp(
        pipe, _entity({"provenance.enabled": "true"}), mode=MODE_BATCH, config_service=MagicMock()
    )
    assert stamp.pipe_id is None and stamp.pipe_name is None
    assert stamp.input_entity_ids == ()
    assert stamp.tags == ()


def test_augment_ddl_schema_string():
    entity = _entity({"provenance.enabled": "true"})
    ddl = augment_schema_for_provenance("id STRING, status STRING", entity)
    assert ddl.startswith("id STRING, status STRING, `__provenance` struct<pipe_id:string,")
    assert augment_schema_for_provenance("", entity) == ""
    assert augment_schema_for_provenance("id STRING", _entity()) == "id STRING"
