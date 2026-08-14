"""Integration tests for Auto Loader discovery across repeated process_path()
calls, and for unmatched-file skip behavior within a delivered microbatch.

cloudFiles itself owns two guarantees these tests cannot exercise directly on
Databricks-less infrastructure:

- checkpoint-based dedup: an already-processed file is never redelivered on a
  later run.
- pathGlobFilter listing-time scoping: a file that doesn't match an entry's
  source_glob is never listed or read by that entry's stream.

What these tests verify instead is the contract our own code owns at the
boundary immediately around those cloudFiles-native behaviors:

- the checkpoint_location/schema_location passed to the Auto Loader runner
  for a given entry is stable (derived only from entry_id, not from any
  per-call state) across repeated process_path() calls -- a location that
  moved between calls would break cloudFiles' own checkpointing regardless of
  what cloudFiles itself does.
- when a scripted runner (standing in for a real cloudFiles stream that has
  already checkpointed an earlier file) delivers only a new file on a second
  process_path() call, ParallelizingFileIngestionProcessor writes only that
  new file's rows -- it does not re-list or re-write anything from the first
  call.
- once cloudFiles has delivered a microbatch (i.e. a file already passed the
  stream's own source_glob), _process_autoloader_batch still re-matches each
  file's name against the entry's own patterns[0] regex (mirroring the batch
  path's per-file matching in _build_df_plan) before writing it -- a file
  that does not match the regex must be skipped (not written, no exception),
  with its file_ingestion.after_file signal reporting matched=False. This
  matters because source_glob and patterns[0] are different languages (glob
  vs. regex): a file can satisfy one and not the other.
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

    Shadows conftest.py's shared, session-scoped ``spark_session`` fixture
    rather than requesting it -- see test_autoloader_schema_evolution.py's
    identical fixture for why: these tests only need genuine Spark
    DataFrames for a CSV read path, and the shared fixture stays alive for
    the rest of the pytest process once instantiated, which corrupts later
    plain-session fixtures elsewhere in the suite.
    """
    if not _sockets_permitted():
        pytest.skip(
            "Sockets are not permitted in this environment; cannot start a real SparkSession."
        )
    spark = (
        SparkSession.builder.appName("AutoloaderIncrementalDiscoveryTests")
        .master("local[2]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


def _make_entry(patterns=None):
    return FileIngestionMetadata(
        entry_id="autoloader_entry",
        name="autoloader incremental-discovery entry",
        patterns=patterns or [r".*\.csv"],
        dest_entity_id="target_entity",
        tags={},
        filetype="csv",
        discovery="autoloader",
        source_glob="*.csv",
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


class _RecordingRunner:
    """Stands in for a real cloudFiles stream across repeated process_path()
    calls: replays exactly the microbatches scripted for THIS call (modeling
    cloudFiles' own checkpoint state having already drained earlier files),
    and records every (checkpoint_location, schema_location) it is invoked
    with so the test can assert path stability across calls."""

    def __init__(self):
        self.calls = []
        self._next_batches = []

    def script(self, batches):
        self._next_batches = batches

    def run_entry(self, entry, path, checkpoint_location, schema_location, write_batch):
        self.calls.append((checkpoint_location, schema_location))
        for batch_df, micro_batch_id in self._next_batches:
            write_batch(batch_df, micro_batch_id)


class _ScriptedRunner:
    """Replays a fixed list of microbatches for a single process_path() call."""

    def __init__(self, batches):
        self.batches = batches

    def run_entry(self, entry, path, checkpoint_location, schema_location, write_batch):
        for batch_df, micro_batch_id in self.batches:
            write_batch(batch_df, micro_batch_id)


def test_repeated_calls_use_stable_checkpoint_path_and_write_only_new_files(
    spark_session, temp_workspace
):
    entry = _make_entry()
    proc = _make_processor(entry, spark_session)

    runner = _RecordingRunner()
    proc._get_autoloader_runner = lambda: runner

    file1 = temp_workspace / "batch1.csv"
    file1.write_text("id,name\n1,alpha\n")
    df1 = spark_session.read.format("csv").option("header", "true").load(str(file1))

    runner.script([(df1, "b0")])
    proc.process_path(str(temp_workspace))

    # A real cloudFiles stream would not redeliver batch1.csv here -- it is
    # already checkpointed. The scripted runner models exactly that: this
    # second call only ever hands the processor the new file.
    file2 = temp_workspace / "batch2.csv"
    file2.write_text("id,name\n2,beta\n")
    df2 = spark_session.read.format("csv").option("header", "true").load(str(file2))

    runner.script([(df2, "b1")])
    proc.process_path(str(temp_workspace))

    assert len(runner.calls) == 2
    assert runner.calls[0] == runner.calls[1], (
        "checkpoint/schema location must be stable across calls -- a moving "
        "path would break cloudFiles' own checkpoint-based dedup regardless "
        "of what cloudFiles itself does"
    )

    written = [c.args[0] for c in proc.ep.append_to_entity.call_args_list]
    assert len(written) == 2

    first_call_rows = written[0].collect()
    second_call_rows = written[1].collect()

    assert [r["id"] for r in first_call_rows] == ["1"]
    assert [r["id"] for r in second_call_rows] == ["2"], (
        "the second process_path() call must write only the newly-delivered "
        "file's rows, not re-write the first call's file"
    )


def test_unmatched_file_in_microbatch_is_skipped_not_written(spark_session, temp_workspace):
    entry = _make_entry(patterns=[r"sales_(?P<region>\w+)\.csv"])
    proc = _make_processor(entry, spark_session)

    matching_file = temp_workspace / "sales_east.csv"
    matching_file.write_text("id,amount\n1,100\n")
    stray_file = temp_workspace / "notes.csv"
    stray_file.write_text("id,amount\n2,200\n")

    matched_df = spark_session.read.format("csv").option("header", "true").load(str(matching_file))
    stray_df = spark_session.read.format("csv").option("header", "true").load(str(stray_file))

    # Two separate microbatches (rather than one unioned batch) so each
    # keeps its own file's _metadata.file_path untouched by any transform.
    proc._get_autoloader_runner = lambda: _ScriptedRunner([(stray_df, "b0"), (matched_df, "b1")])

    proc.process_path(str(temp_workspace))

    written = [c.args[0] for c in proc.ep.append_to_entity.call_args_list]
    assert len(written) == 1, "only the matching file's microbatch should ever be written"

    rows = written[0].collect()
    assert len(rows) == 1
    assert rows[0]["id"] == "1"
    assert rows[0]["region"] == "east"

    after_file_events = [
        c.kwargs
        for c in proc.emit.call_args_list
        if c.args and c.args[0] == "file_ingestion.after_file"
    ]
    unmatched = [e for e in after_file_events if e.get("filename") == "notes.csv"]
    assert len(unmatched) == 1
    assert unmatched[0]["matched"] is False
    assert unmatched[0]["dest_entity_id"] is None

    matched = [e for e in after_file_events if e.get("filename") == "sales_east.csv"]
    assert len(matched) == 1
    assert matched[0]["dest_entity_id"] == "target_entity"

    failed_events = [
        c.kwargs
        for c in proc.emit.call_args_list
        if c.args and c.args[0] == "file_ingestion.file_failed"
    ]
    assert failed_events == [], "an unmatched file is a skip, not a failure"
