"""Auto Loader (cloudFiles) discovery runner for Kindling file ingestion.

Binds an ``AutoLoaderFileIngestionRunner`` implementation so
``ParallelizingFileIngestionProcessor`` can start a per-entry ``cloudFiles``
stream for ``discovery="autoloader"`` ``FileIngestionEntry`` registrations,
without core ``kindling`` importing anything Databricks-specific. See
``plans/autoloader-file-ingestion/implementation-plan.md``.
"""

from typing import Any, Callable

from kindling.file_ingestion import AutoLoaderFileIngestionRunner, FileIngestionMetadata
from kindling.injection import GlobalInjector
from kindling.spark_session import get_or_create_spark_session


class DatabricksAutoLoaderFileIngestionRunner(AutoLoaderFileIngestionRunner):
    """Runs one ``Trigger.AvailableNow`` cloudFiles stream per Auto Loader entry.

    All enrichment, entity-group writing, and signal emission stays owned by
    ``ParallelizingFileIngestionProcessor`` (via `write_batch`); this class
    only wires the Databricks-only ``cloudFiles`` source/options and drives
    the stream to completion so ``process_path()`` keeps its synchronous
    run-now-drain-what's-new-stop contract.
    """

    def run_entry(
        self,
        entry: FileIngestionMetadata,
        path: str,
        checkpoint_location: str,
        schema_location: str,
        write_batch: Callable[[Any, str], None],
    ) -> None:
        spark = get_or_create_spark_session()

        reader = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", entry.filetype)
            .option("cloudFiles.schemaLocation", schema_location)
            .option("pathGlobFilter", entry.source_glob)
        )
        if entry.schema_evolution_mode:
            reader = reader.option("cloudFiles.schemaEvolutionMode", entry.schema_evolution_mode)
        stream = reader.load(path)

        query = (
            stream.writeStream.foreachBatch(
                lambda batch_df, micro_batch_id: write_batch(batch_df, str(micro_batch_id))
            )
            .option("checkpointLocation", checkpoint_location)
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination()


def register_runner() -> None:
    """Bind DatabricksAutoLoaderFileIngestionRunner as the Auto Loader runner."""
    GlobalInjector.bind(AutoLoaderFileIngestionRunner, DatabricksAutoLoaderFileIngestionRunner)
