"""Integration tests for streaming merge (merge_as_stream) against real Delta.

A streaming source (Delta table read as a stream) is merged into a target
entity via ``DeltaEntityProvider.merge_as_stream``. Each micro-batch runs
through the batch ``merge_to_entity`` path, so:

- plain entities get SCD1 upsert semantics (no duplicate keys across
  micro-batches), and
- ``scd.type: "2"`` entities chain versions exactly as batch SCD2 merges do.

Checkpointing makes re-runs incremental: a second availableNow pass over the
same checkpoint only processes source data added since the first pass.
"""

import shutil
import tempfile
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from delta import configure_spark_with_delta_pip
from kindling.data_entities import EntityNameMapper, EntityPathLocator
from kindling.entity_provider_delta import DeltaEntityProvider
from kindling.spark_config import ConfigService
from kindling.spark_log_provider import PythonLoggerProvider
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, TimestampType


def _teardown_existing_spark_jvm():
    """See test_watermark_incremental_correctness — Delta jars are only on
    the classpath if this module's session launches the JVM."""
    from pyspark import SparkContext

    active = SparkSession.getActiveSession()
    if active is not None:
        active.stop()
    if SparkContext._gateway is not None:
        try:
            SparkContext._gateway.shutdown()
        except Exception:
            pass
        SparkContext._gateway = None
        SparkContext._jvm = None


@pytest.fixture(scope="module")
def spark():
    _teardown_existing_spark_jvm()
    builder = (
        SparkSession.builder.appName("StreamMerge")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


@pytest.fixture
def temp_dir():
    temp_path = tempfile.mkdtemp(prefix="kindling-stream-merge-test-")
    yield Path(temp_path)
    shutil.rmtree(temp_path, ignore_errors=True)


@pytest.fixture
def delta_provider(spark, temp_dir, monkeypatch):
    monkeypatch.setattr("kindling.entity_provider_delta.get_or_create_spark_session", lambda: spark)
    config = MagicMock(spec=ConfigService)
    config.get.side_effect = lambda key, default=None: (
        "storage" if key == "kindling.delta.access_mode" else default
    )
    name_mapper = MagicMock(spec=EntityNameMapper)
    name_mapper.get_table_name.side_effect = lambda entity: entity.entityid.replace(".", "_")
    path_locator = MagicMock(spec=EntityPathLocator)
    path_locator.get_table_path.side_effect = lambda entity: str(
        Path(str(temp_dir)) / entity.entityid
    )
    logger_provider = MagicMock(spec=PythonLoggerProvider)
    logger_provider.get_logger.return_value = MagicMock()
    return DeltaEntityProvider(
        config=config,
        entity_name_mapper=name_mapper,
        path_locator=path_locator,
        tp=logger_provider,
        signal_provider=None,
    )


SOURCE_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("status", StringType(), True),
    ]
)


def _make_entity(entityid, tags=None):
    return SimpleNamespace(
        entityid=entityid,
        name=entityid.split(".")[-1],
        partition_columns=[],
        cluster_columns=None,
        merge_columns=["customer_id"],
        tags={"provider_type": "delta", **(tags or {})},
        schema=SOURCE_SCHEMA,
    )


def _append_source_batch(spark, source_path, rows):
    df = spark.createDataFrame(rows, SOURCE_SCHEMA)
    df.write.format("delta").mode("append").save(source_path)


def _run_stream_merge(spark, provider, entity, source_path, checkpoint):
    stream_df = spark.readStream.format("delta").load(source_path)
    query = provider.merge_as_stream(
        stream_df, entity, checkpoint, options={"trigger": {"availableNow": True}}
    )
    query.awaitTermination()


class TestStreamMergeSCD1:
    def test_micro_batches_upsert_by_business_key(self, spark, delta_provider, temp_dir):
        entity = _make_entity("silver.customers_stream")
        source_path = str(temp_dir / "source_scd1")
        checkpoint = str(temp_dir / "chk_scd1")

        _append_source_batch(spark, source_path, [("c1", "bronze"), ("c2", "silver")])
        _run_stream_merge(spark, delta_provider, entity, source_path, checkpoint)

        _append_source_batch(spark, source_path, [("c1", "gold")])
        _run_stream_merge(spark, delta_provider, entity, source_path, checkpoint)

        rows = {r["customer_id"]: r["status"] for r in delta_provider.read_entity(entity).collect()}
        assert rows == {"c1": "gold", "c2": "silver"}

    def test_checkpoint_makes_reruns_incremental(self, spark, delta_provider, temp_dir):
        entity = _make_entity("silver.customers_stream_idem")
        source_path = str(temp_dir / "source_idem")
        checkpoint = str(temp_dir / "chk_idem")

        _append_source_batch(spark, source_path, [("c1", "bronze")])
        _run_stream_merge(spark, delta_provider, entity, source_path, checkpoint)
        # No new source data: rerun processes nothing and changes nothing.
        version_before = delta_provider.get_entity_version(entity)
        _run_stream_merge(spark, delta_provider, entity, source_path, checkpoint)

        assert delta_provider.get_entity_version(entity) == version_before
        assert delta_provider.read_entity(entity).count() == 1


class TestStreamMergeSCD2:
    def test_micro_batches_chain_scd2_versions(self, spark, delta_provider, temp_dir):
        entity = _make_entity("silver.customers_stream_scd2", {"scd.type": "2"})
        source_path = str(temp_dir / "source_scd2")
        checkpoint = str(temp_dir / "chk_scd2")

        # Mirror SimplePipeStreamStarter: the destination is ensured before
        # the stream starts, so the table exists with the augmented SCD2
        # schema and the first micro-batch takes the merge path.
        delta_provider.ensure_destination(entity)

        _append_source_batch(spark, source_path, [("c1", "bronze")])
        _run_stream_merge(spark, delta_provider, entity, source_path, checkpoint)

        _append_source_batch(spark, source_path, [("c1", "gold")])
        _run_stream_merge(spark, delta_provider, entity, source_path, checkpoint)

        rows = delta_provider.read_entity(entity).collect()
        by_state = {(r["status"], r["__is_current"]): r for r in rows}

        assert len(rows) == 2
        closed = by_state[("bronze", False)]
        current = by_state[("gold", True)]
        assert closed["__effective_to"] is not None
        assert current["__effective_to"] is None
        assert current["__effective_from"] >= closed["__effective_from"]

    def test_replayed_micro_batch_does_not_chain_new_scd2_version(
        self, spark, delta_provider, temp_dir
    ):
        """foreachBatch delivery is at-least-once: after a failure between
        the merge and the checkpoint commit, Spark re-delivers the last
        micro-batch. Re-merging the same rows through the batch path that
        ``_merge_batch`` drives must be a no-op — no new SCD2 version, no
        re-versioned current row (the docstring's replay-safety claim,
        actually exercised)."""
        entity = _make_entity("silver.customers_stream_replay", {"scd.type": "2"})
        source_path = str(temp_dir / "source_replay")
        checkpoint = str(temp_dir / "chk_replay")
        delta_provider.ensure_destination(entity)

        _append_source_batch(spark, source_path, [("c1", "bronze")])
        _run_stream_merge(spark, delta_provider, entity, source_path, checkpoint)
        last_batch_rows = [("c1", "gold")]
        _append_source_batch(spark, source_path, last_batch_rows)
        _run_stream_merge(spark, delta_provider, entity, source_path, checkpoint)

        def _versions():
            return {
                (r["status"], r["__is_current"], r["__effective_from"], r["__effective_to"])
                for r in delta_provider.read_entity(entity).collect()
            }

        versions_before = _versions()

        # Replay: the last micro-batch's DataFrame goes through the same
        # merge the foreachBatch handler runs.
        replay_df = spark.createDataFrame(last_batch_rows, SOURCE_SCHEMA)
        delta_provider.merge_to_entity(replay_df, entity)

        assert _versions() == versions_before
        assert len(versions_before) == 2  # bronze (closed) + gold (current)

    def test_multi_event_micro_batch_collapses_to_latest_by_sequence(
        self, spark, delta_provider, temp_dir
    ):
        """A single micro-batch carrying several change events for one key
        is collapsed to the latest by scd.sequence_by before the merge —
        the merge contract is one row per business key per merge."""
        seq_schema = StructType(
            [
                StructField("customer_id", StringType(), False),
                StructField("status", StringType(), True),
                StructField("updated_at", TimestampType(), True),
            ]
        )
        entity = SimpleNamespace(
            entityid="silver.customers_stream_seq",
            name="customers_stream_seq",
            partition_columns=[],
            cluster_columns=None,
            merge_columns=["customer_id"],
            tags={
                "provider_type": "delta",
                "scd.type": "2",
                "scd.sequence_by": "updated_at",
            },
            schema=seq_schema,
        )
        source_path = str(temp_dir / "source_seq")
        checkpoint = str(temp_dir / "chk_seq")
        delta_provider.ensure_destination(entity)

        # One source commit = one micro-batch with three events for c1.
        df = spark.createDataFrame(
            [
                ("c1", "bronze", datetime(2026, 7, 1)),
                ("c1", "silver", datetime(2026, 7, 2)),
                ("c1", "gold", datetime(2026, 7, 3)),
            ],
            seq_schema,
        )
        df.coalesce(1).write.format("delta").mode("append").save(source_path)
        _run_stream_merge(spark, delta_provider, entity, source_path, checkpoint)

        rows = delta_provider.read_entity(entity).collect()
        assert len(rows) == 1
        assert rows[0]["status"] == "gold"
        assert rows[0]["__is_current"] is True
        assert rows[0]["__effective_from"] == datetime(2026, 7, 3)
