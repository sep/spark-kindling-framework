"""Unit tests for the Databricks Auto Loader (cloudFiles) runner.

Focused on the cloudFiles.schemaEvolutionMode option wiring: the runner must
pass FileIngestionMetadata.schema_evolution_mode straight through to cloudFiles
when an entry has opted in, and must not set the option at all when the entry
has left it unset (so cloudFiles applies its own default).
"""

from unittest.mock import MagicMock, call, patch

from kindling.file_ingestion import FileIngestionMetadata
from kindling_ext_databricks_autoloader.autoloader_file_ingestion import (
    DatabricksAutoLoaderFileIngestionRunner,
)


def _make_entry(schema_evolution_mode=None):
    return FileIngestionMetadata(
        entry_id="e1",
        name="autoloader entry",
        patterns=[r".*\.csv"],
        dest_entity_id="target_entity",
        tags={},
        filetype="csv",
        discovery="autoloader",
        source_glob="*.csv",
        schema_evolution_mode=schema_evolution_mode,
    )


def _make_mock_spark():
    """Build a MagicMock spark session whose readStream/writeStream chains
    return themselves, so every .option()/.trigger()/... call in the chain
    is captured on a single mock for inspection."""
    spark = MagicMock()

    reader = MagicMock(name="reader")
    reader.option.return_value = reader
    stream = MagicMock(name="stream")
    reader.load.return_value = stream
    spark.readStream.format.return_value = reader

    write_stream = MagicMock(name="write_stream")
    write_stream.option.return_value = write_stream
    write_stream.trigger.return_value = write_stream
    stream.writeStream.foreachBatch.return_value = write_stream

    return spark, reader


def test_run_entry_sets_schema_evolution_mode_option_when_configured():
    entry = _make_entry(schema_evolution_mode="addNewColumns")
    spark, reader = _make_mock_spark()

    with patch(
        "kindling_ext_databricks_autoloader.autoloader_file_ingestion.get_or_create_spark_session",
        return_value=spark,
    ):
        runner = DatabricksAutoLoaderFileIngestionRunner()
        runner.run_entry(entry, "/data", "/chk", "/schema", MagicMock())

    assert call("cloudFiles.schemaEvolutionMode", "addNewColumns") in reader.option.call_args_list


def test_run_entry_omits_schema_evolution_mode_option_when_not_configured():
    entry = _make_entry(schema_evolution_mode=None)
    spark, reader = _make_mock_spark()

    with patch(
        "kindling_ext_databricks_autoloader.autoloader_file_ingestion.get_or_create_spark_session",
        return_value=spark,
    ):
        runner = DatabricksAutoLoaderFileIngestionRunner()
        runner.run_entry(entry, "/data", "/chk", "/schema", MagicMock())

    schema_evolution_calls = [
        c
        for c in reader.option.call_args_list
        if c.args and c.args[0] == "cloudFiles.schemaEvolutionMode"
    ]
    assert schema_evolution_calls == []
