"""Provenance stamping through the batch persist step and the pipe read.

SimpleReadPersistStrategy straight to the memory provider (the merge
semantics are covered by test_provenance_parity.py on both providers):

- the persist step stamps a stamped entity's output after every
  ``persist.before_persist`` handler and after the surrogate key step;
- the execution id comes from the executor's lifecycle signal;
- a downstream read drops an input's stamp unless the pipe keeps it.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import pytest
from kindling.data_pipes import PipeMetadata
from kindling.entity_provider_memory import MemoryEntityProvider
from kindling.provenance import ProvenanceRunCapture
from kindling.signaling import BlinkerSignalProvider
from kindling.simple_read_persist_strategy import SimpleReadPersistStrategy
from kindling.spark_log_provider import PythonLoggerProvider
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("status", StringType(), True),
    ]
)


class _Registry:
    def __init__(self, entities):
        self._entities = {entity.entityid: entity for entity in entities}

    def get_entity_definition(self, entity_id):
        return self._entities.get(entity_id)


def _entity(entityid, tags, merge_columns=("customer_id",)):
    return SimpleNamespace(
        entityid=entityid,
        name=entityid.split(".")[-1],
        partition_columns=[],
        cluster_columns=None,
        merge_columns=list(merge_columns),
        tags={"provider_type": "memory", **tags},
        schema=None,
    )


def _pipe(output_id, tags=None, inputs=("bronze.src",)):
    return PipeMetadata(
        pipeid=f"build_{output_id.replace('.', '_')}",
        name="Build output",
        execute=lambda df: df,
        tags=dict(tags or {}),
        input_entity_ids=list(inputs),
        output_entity_id=output_id,
        output_type="memory",
    )


@pytest.fixture
def provider(spark_session, monkeypatch):
    monkeypatch.setattr(
        "kindling.entity_provider_memory.get_or_create_spark_session", lambda: MagicMock()
    )
    lp = MagicMock(spec=PythonLoggerProvider)
    lp.get_logger.return_value = MagicMock()
    memory = MemoryEntityProvider(lp)
    memory.spark = spark_session
    return memory


@pytest.fixture
def signals():
    return BlinkerSignalProvider()


def _strategy(provider, signals, entities):
    lp = MagicMock(spec=PythonLoggerProvider)
    lp.get_logger.return_value = MagicMock()
    registry = Mock()
    registry.get_provider_for_entity.return_value = provider
    return SimpleReadPersistStrategy(
        ep=Mock(),
        der=_Registry(entities),
        tp=MagicMock(),
        lp=lp,
        provider_registry=registry,
        signal_provider=signals,
    )


def _df(spark, rows):
    return spark.createDataFrame(rows, SCHEMA)


def test_persist_stamps_the_output(spark_session, provider, signals, request, monkeypatch):
    source = _entity("bronze.src", {})
    output = _entity(
        f"silver.out_{request.node.name}",
        {"provenance.enabled": "true", "provenance.pipe_tags": "owner"},
    )
    pipe = _pipe(output.entityid, tags={"owner": "sales", "secret_ish": "x"})
    strategy = _strategy(provider, signals, [source, output])

    capture = ProvenanceRunCapture(signals)
    capture.register()
    signals.create_signal("orchestrator.pipe_started").send(
        None, pipe_id=pipe.pipeid, run_id="exec-1"
    )
    monkeypatch.setattr(
        "kindling.simple_read_persist_strategy.current_execution_id", capture.execution_id
    )
    strategy.create_pipe_persist_activator(pipe)(_df(spark_session, [("c1", "bronze")]))

    (row,) = provider.read_entity(output).collect()
    stamp = row["__provenance"]
    assert stamp["pipe_id"] == pipe.pipeid
    assert stamp["pipe_name"] == "Build output"
    assert stamp["execution_id"] == "exec-1"
    assert stamp["persist_id"]
    assert stamp["mode"] == "batch"
    assert stamp["run_id"] is None
    assert stamp["input_entity_ids"] == ["bronze.src"]
    assert stamp["tags"] == {"owner": "sales"}
    assert stamp["processed_at"] is not None
    assert stamp["schema_version"] == 1


def test_unstamped_output_is_unchanged(spark_session, provider, signals, request):
    source = _entity("bronze.src", {})
    output = _entity(f"silver.out_{request.node.name}", {})
    pipe = _pipe(output.entityid)
    _strategy(provider, signals, [source, output]).create_pipe_persist_activator(pipe)(
        _df(spark_session, [("c1", "bronze")])
    )
    assert provider.read_entity(output).columns == ["customer_id", "status"]


def test_before_persist_handlers_cannot_strip_the_stamp(spark_session, provider, signals, request):
    source = _entity("bronze.src", {})
    output = _entity(f"silver.out_{request.node.name}", {"provenance.enabled": "true"})
    pipe = _pipe(output.entityid)

    def strip(sender, df, **kwargs):
        return df.drop("__provenance").withColumn("audit", F.lit("x"))

    signals.create_signal("persist.before_persist").connect(strip, weak=False)
    _strategy(provider, signals, [source, output]).create_pipe_persist_activator(pipe)(
        _df(spark_session, [("c1", "bronze")])
    )
    (row,) = provider.read_entity(output).collect()
    assert row["audit"] == "x"
    assert row["__provenance"]["pipe_id"] == pipe.pipeid


def test_execution_id_is_null_outside_an_executor(spark_session, provider, signals, request):
    source = _entity("bronze.src", {})
    output = _entity(f"silver.out_{request.node.name}", {"provenance.enabled": "true"})
    pipe = _pipe(output.entityid)
    _strategy(provider, signals, [source, output]).create_pipe_persist_activator(pipe)(
        _df(spark_session, [("c1", "bronze")])
    )
    (row,) = provider.read_entity(output).collect()
    assert row["__provenance"]["execution_id"] is None


def test_scd2_merge_through_persist_keeps_the_stamp_on_no_change(
    spark_session, provider, signals, request
):
    source = _entity("bronze.src", {})
    output = _entity(
        f"silver.out_{request.node.name}",
        {"provenance.enabled": "true", "scd.type": "2"},
    )
    pipe = _pipe(output.entityid)
    persist = _strategy(provider, signals, [source, output]).create_pipe_persist_activator(pipe)
    persist(_df(spark_session, [("c1", "bronze")]))
    (first,) = provider.read_entity(output).collect()
    persist(_df(spark_session, [("c1", "bronze")]))

    rows = provider.read_entity(output).collect()
    assert len(rows) == 1, "a provenance-only difference opens no version"
    assert rows[0]["__provenance"]["persist_id"] == first["__provenance"]["persist_id"]


def test_downstream_read_drops_the_input_stamp(spark_session, provider, signals, request):
    source = _entity("bronze.src", {})
    middle = _entity(f"silver.mid_{request.node.name}", {"provenance.enabled": "true"})
    strategy = _strategy(provider, signals, [source, middle])
    strategy.create_pipe_persist_activator(_pipe(middle.entityid))(
        _df(spark_session, [("c1", "bronze")])
    )

    reader = strategy.create_pipe_entity_reader(_pipe("gold.out", inputs=(middle.entityid,)))
    assert reader(middle, False).columns == ["customer_id", "status"]

    keeping = _pipe(
        "gold.out",
        tags={"provenance.keep_inputs": middle.entityid},
        inputs=(middle.entityid,),
    )
    kept = strategy.create_pipe_entity_reader(keeping)(middle, False)
    assert kept.columns == ["customer_id", "status", "__provenance"]


def test_a_kept_input_stamp_collides_with_the_output_stamp(
    spark_session, provider, signals, request
):
    """Keeping an input's stamp and writing it to a stamped output would
    replace the output's stamp: the persist step refuses instead."""
    source = _entity("bronze.src", {})
    middle = _entity(f"silver.mid_{request.node.name}", {"provenance.enabled": "true"})
    output = _entity(f"gold.out_{request.node.name}", {"provenance.enabled": "true"})
    strategy = _strategy(provider, signals, [source, middle, output])
    strategy.create_pipe_persist_activator(_pipe(middle.entityid))(
        _df(spark_session, [("c1", "bronze")])
    )
    keeping = _pipe(
        output.entityid,
        tags={"provenance.keep_inputs": middle.entityid},
        inputs=(middle.entityid,),
    )
    kept = strategy.create_pipe_entity_reader(keeping)(middle, False)
    with pytest.raises(ValueError, match="already has a column '__provenance'"):
        strategy.create_pipe_persist_activator(keeping)(kept)
