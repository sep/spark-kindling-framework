"""Integration tests for the schema-evolution opt-in on Auto Loader entries.

cloudFiles itself (and its schemaEvolutionMode option) only exists on
Databricks, so its own evolve-vs-fail decision cannot run in a local
(no-cloud) test. What these tests verify instead is the contract our own
code owns at that boundary:

- when cloudFiles evolves and delivers a later micro-batch with a new
  column, ParallelizingFileIngestionProcessor's own enrichment/write path
  must carry that column through to EntityProvider.append_to_entity
  untouched, rather than silently truncating to the first batch's schema.
- when cloudFiles instead fails the stream (e.g. under failOnNewColumns),
  that failure must propagate out of process_path() -- not be swallowed --
  and must surface as file_ingestion.process_failed rather than a normal
  after_process.

A scripted fake AutoLoaderFileIngestionRunner stands in for the real
cloudFiles stream, replaying real local-file-backed DataFrames (so
enrich_file_dataframe / _write_table_group run against genuine Spark
DataFrames, not mocks) through the exact same write_batch callback
production code wires up.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from kindling.file_ingestion import (
    FileIngestionMetadata,
    ParallelizingFileIngestionProcessor,
)
from kindling.trace_ops import TracingGates
from pyspark.sql import SparkSession

from tests.conftest import _sockets_permitted

pytestmark = [pytest.mark.integration]


@pytest.fixture(scope="module")
def spark_session():
    """Plain (non-Delta) SparkSession, module-scoped and self-contained.

    This deliberately shadows conftest.py's shared, session-scoped
    ``spark_session`` fixture rather than requesting it: these tests only
    need genuine Spark DataFrames for the CSV read/enrichment path, no Delta
    capability at all, and the shared fixture stays alive for the rest of
    the pytest process once created. Other integration modules (e.g.
    test_scd2_provider_parity.py) tear down and relaunch the JVM for their
    own Delta-configured session, which is fatal to a still-referenced
    shared session created here -- so this file, like
    test_entity_provider_memory_scd2.py, gets its own local, disposable one.
    """
    if not _sockets_permitted():
        pytest.skip(
            "Sockets are not permitted in this environment; cannot start a real SparkSession."
        )
    spark = (
        SparkSession.builder.appName("AutoloaderSchemaEvolutionTests")
        .master("local[2]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


def _make_entry(schema_evolution_mode):
    return FileIngestionMetadata(
        entry_id="autoloader_entry",
        name="autoloader schema evolution entry",
        patterns=[r".*\.csv"],
        dest_entity_id="target_entity",
        tags={},
        filetype="csv",
        discovery="autoloader",
        source_glob="*.csv",
        schema_evolution_mode=schema_evolution_mode,
    )


def _make_processor(entry, spark):
    proc = object.__new__(ParallelizingFileIngestionProcessor)
    proc.logger = MagicMock()
    proc.tp = MagicMock()
    proc._trace_gates = TracingGates(False, "standard")
    proc.emit = MagicMock()
    proc.spark = spark
    proc.config = MagicMock()
    proc.config.get.side_effect = lambda key, default=None: (
        "/chk" if key == "kindling.storage.checkpoint_root" else default
    )
    proc.env = MagicMock()
    proc.env.list.return_value = []
    proc.fir = MagicMock()
    proc.fir.get_entry_ids.return_value = [entry.entry_id]
    proc.fir.get_entry_definition.side_effect = {entry.entry_id: entry}.get
    proc.der = MagicMock()
    proc.der.get_entity_definition.return_value = SimpleNamespace(entityid=entry.dest_entity_id)
    proc.ep = MagicMock()
    return proc


class _ScriptedRunner:
    """Stands in for a real cloudFiles stream: replays scripted micro-batches
    (and optionally raises, modeling a rejected stream) through the same
    write_batch callback production code passes to a real runner."""

    def __init__(self, batches, failure=None):
        self.batches = batches
        self.failure = failure

    def run_entry(self, entry, path, checkpoint_location, schema_location, write_batch):
        for batch_df, micro_batch_id in self.batches:
            write_batch(batch_df, micro_batch_id)
        if self.failure:
            raise self.failure


def test_new_column_in_later_microbatch_flows_through_to_write(spark_session, temp_workspace):
    entry = _make_entry(schema_evolution_mode="addNewColumns")
    proc = _make_processor(entry, spark_session)

    file1 = temp_workspace / "batch1.csv"
    file1.write_text("id,name\n1,alpha\n")
    file2 = temp_workspace / "batch2.csv"
    file2.write_text("id,name,extra_col\n2,beta,newval\n")

    df1 = spark_session.read.format("csv").option("header", "true").load(str(file1))
    df2 = spark_session.read.format("csv").option("header", "true").load(str(file2))

    proc._get_autoloader_runner = lambda: _ScriptedRunner([(df1, "b0"), (df2, "b1")])

    proc.process_path(str(temp_workspace))

    written = [c.args[0] for c in proc.ep.append_to_entity.call_args_list]
    assert len(written) == 2

    first_row = written[0].collect()[0].asDict()
    second_row = written[1].collect()[0].asDict()

    assert set(first_row.keys()) == {"id", "name", "ingestion_timestamp"}
    assert first_row["id"] == "1"
    assert first_row["name"] == "alpha"

    assert set(second_row.keys()) == {"id", "name", "extra_col", "ingestion_timestamp"}
    assert second_row["id"] == "2"
    assert second_row["extra_col"] == "newval"


def test_runner_failure_propagates_and_emits_process_failed_not_after_process(
    spark_session, temp_workspace
):
    entry = _make_entry(schema_evolution_mode="failOnNewColumns")
    proc = _make_processor(entry, spark_session)

    file1 = temp_workspace / "batch1.csv"
    file1.write_text("id,name\n1,alpha\n")
    df1 = spark_session.read.format("csv").option("header", "true").load(str(file1))

    boom = RuntimeError("cloudFiles: stream failed -- new column rejected under failOnNewColumns")
    proc._get_autoloader_runner = lambda: _ScriptedRunner([(df1, "b0")], failure=boom)

    with pytest.raises(RuntimeError, match="failOnNewColumns"):
        proc.process_path(str(temp_workspace))

    emitted_signals = [c.args[0] for c in proc.emit.call_args_list]
    assert "file_ingestion.process_failed" in emitted_signals
    assert "file_ingestion.after_process" not in emitted_signals
