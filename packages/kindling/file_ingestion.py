import logging
import re
import time
import uuid
from abc import ABC, abstractmethod
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import nullcontext
from dataclasses import dataclass, fields
from functools import reduce
from typing import Any, Callable, Dict, List, Optional, Tuple

from delta.tables import DeltaTable
from injector import Binder, Injector, inject, singleton
from kindling.data_entities import *
from kindling.file_ingestion import *
from kindling.injection import *
from kindling.platform_provider import *
from kindling.signaling import SignalEmitter, SignalProvider
from kindling.spark_config import *
from kindling.spark_log_provider import *
from kindling.spark_session import *
from kindling.spark_trace import *
from kindling.trace_ops import COMPONENT_INGESTION, tracing_gates
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, lit


@dataclass
class FileIngestionMetadata:
    entry_id: str
    name: str
    patterns: List[str]
    dest_entity_id: str
    tags: Dict[str, str]
    infer_schema: bool = True
    filetype: str = "csv"
    static_values: Optional[Dict[str, Any]] = None
    discovery: str = "batch"
    source_glob: Optional[str] = None
    schema_evolution_mode: Optional[str] = None


class FileIngestionEntries:
    deregistry = None

    @classmethod
    def entry(cls, **decorator_params):
        if cls.deregistry is None:
            cls.deregistry = GlobalInjector.get(FileIngestionRegistry)
        # Check all required fields are provided
        required_fields = {field.name for field in fields(FileIngestionMetadata)}

        decorator_params["infer_schema"] = (
            decorator_params["infer_schema"]
            if ("infer_schema" in decorator_params.keys())
            else True
        )

        decorator_params.setdefault("static_values", None)
        decorator_params.setdefault("discovery", "batch")
        decorator_params.setdefault("source_glob", None)
        decorator_params.setdefault("schema_evolution_mode", None)

        missing_fields = required_fields - decorator_params.keys()

        if missing_fields:
            raise ValueError(
                f"Missing required fields in file ingestion decorator: {missing_fields}"
            )

        if decorator_params["discovery"] not in ("batch", "autoloader"):
            raise ValueError(
                f"File ingestion entry '{decorator_params.get('entry_id')}': invalid "
                f"discovery '{decorator_params['discovery']}' (expected 'batch' or "
                "'autoloader')"
            )

        if decorator_params["discovery"] == "autoloader" and not decorator_params["source_glob"]:
            raise ValueError(
                f"File ingestion entry '{decorator_params.get('entry_id')}': "
                'discovery="autoloader" requires an explicit source_glob to scope '
                "per-entry Auto Loader discovery (passed as cloudFiles' pathGlobFilter)"
            )

        destEntityId = decorator_params["entry_id"]

        del decorator_params["entry_id"]

        cls.deregistry.register_entry(destEntityId, **decorator_params)

        return None


class FileIngestionRegistry(ABC):
    @abstractmethod
    def register_entry(self, entryId, **decorator_params):
        pass

    @abstractmethod
    def get_entry_ids(self) -> List[str]:
        """Every registered entry id, as a plain list.

        Implementations must return a real ``list``, never a live view
        (e.g. ``dict.keys()``) over internal state -- see
        ``DataPipesRegistry.get_pipe_ids()`` for why this matters even
        when nothing currently mutates the result in place.
        """
        pass

    @abstractmethod
    def get_entry_definition(self, entryId):
        pass


@GlobalInjector.singleton_autobind()
class FileIngestionManager(FileIngestionRegistry):
    @inject
    def __init__(self, lp: PythonLoggerProvider):
        self.logger = lp.get_logger("FileIngestionManager")
        self.logger.debug("File ingestion manager initialized ...")
        self.registry = {}

    def register_entry(self, entryId, **decorator_params):
        self.registry[entryId] = FileIngestionMetadata(entryId, **decorator_params)

    def get_entry_ids(self) -> List[str]:
        return list(self.registry.keys())

    def get_entry_definition(self, entryId):
        return self.registry.get(entryId)


class FileIngestionProcessor(ABC):
    """Abstract base for file ingestion processing.

    Implementations MUST emit these signals:
        - file_ingestion.before_process: Before batch processing starts
        - file_ingestion.after_process: After batch completes
        - file_ingestion.process_failed: Batch processing fails
        - file_ingestion.before_file: Before individual file processing
        - file_ingestion.after_file: After file processed
        - file_ingestion.file_failed: File processing fails
        - file_ingestion.file_moved: File moved after ingestion
        - file_ingestion.batch_written: Batch written to table
    """

    EMITS = [
        "file_ingestion.before_process",
        "file_ingestion.after_process",
        "file_ingestion.process_failed",
        "file_ingestion.before_file",
        "file_ingestion.after_file",
        "file_ingestion.file_failed",
        "file_ingestion.file_moved",
        "file_ingestion.batch_written",
    ]

    @abstractmethod
    def process_path(self, path: str):
        pass


class FileIngestionProcessorProvider(ABC):
    @abstractmethod
    def get_file_processor(self, path: str):
        pass


class AutoLoaderFileIngestionRunner(ABC):
    """Extension point for per-entry Databricks Auto Loader (cloudFiles) discovery.

    Core stays engine-neutral: this module never calls ``cloudFiles`` itself.
    An optional extension (``kindling_ext_databricks_autoloader``) binds an
    implementation at import time; ``ParallelizingFileIngestionProcessor``
    resolves it lazily -- only when a ``discovery="autoloader"`` entry is
    actually encountered -- so batch-only usage on any engine is unaffected.
    """

    @abstractmethod
    def run_entry(
        self,
        entry: "FileIngestionMetadata",
        path: str,
        checkpoint_location: str,
        schema_location: str,
        write_batch: Callable[[Any, str], None],
    ) -> None:
        """Run one ``Trigger.AvailableNow`` cloudFiles stream for `entry` against `path`.

        Must call ``write_batch(batch_df, micro_batch_id)`` once per
        delivered microbatch, and block until the stream has drained
        everything currently available (i.e. call ``awaitTermination()``
        before returning) so the caller's synchronous
        run-now-drain-what's-new-stop contract holds.
        """
        pass


def enrich_file_dataframe(
    df: DataFrame,
    named_groups: Dict[str, str],
    static_values: Optional[Dict[str, Any]] = None,
) -> DataFrame:
    """Add regex named-group, static-value, and ingestion-timestamp columns.

    Pure DataFrame transform with no processor state, so it is usable from
    both the batch _build_df_plan path and a future foreachBatch callback.
    """
    for group_name, group_value in named_groups.items():
        df = df.withColumn(group_name, lit(group_value))

    if static_values:
        for col_name, col_value in static_values.items():
            df = df.withColumn(col_name, lit(col_value))

    return df.withColumn("ingestion_timestamp", current_timestamp())


@GlobalInjector.singleton_autobind()
class ParallelizingFileIngestionProcessor(FileIngestionProcessor, SignalEmitter):
    """Advanced file ingestion processor with batching, parallelism, and signal support.

    Features:
    - Lazy DataFrame building - builds execution plans without triggering Spark actions
    - Batching - groups files by destination table and writes in bulk
    - Parallel execution - processes multiple tables concurrently
    - File management - moves files after successful ingestion
    - Transform support - applies custom transformations to DataFrames
    - Enrichment - adds named regex groups and ingestion timestamp as columns
    - Signal emissions - emits signals for monitoring and orchestration
    """

    @inject
    def __init__(
        self,
        config: ConfigService,
        fir: FileIngestionRegistry,
        ep: EntityProvider,
        der: DataEntityRegistry,
        tp: SparkTraceProvider,
        lp: PythonLoggerProvider,
        pep: PlatformServiceProvider,
        signal_provider: SignalProvider = None,
    ):
        self._init_signal_emitter(signal_provider)
        self.config = config
        self.fir = fir
        self.ep = ep
        self.der = der
        self.tp = tp
        self.logger = lp.get_logger("SimpleFileIngestionProcessor")
        self.spark = get_or_create_spark_session()
        self.env = pep.get_service()
        self._trace_gates = tracing_gates(config)

    def _build_df_plan(self, fn: str, path: str, transform: Optional[Callable] = None):
        """Build DataFrame plan without executing - keep it lazy.

        Args:
            fn: Filename to process
            path: Base path containing the file
            transform: Optional transformation function to apply to DataFrame

        Returns:
            Tuple of (dest_entity_id, dataframe, file_info) or None if no pattern match
        """
        fis = self.fir.get_entry_ids()

        for fi in fis:
            fe = self.fir.get_entry_definition(fi)
            if fe.discovery == "autoloader":
                # Discovered via that entry's own cloudFiles stream
                # (_process_autoloader_entries), not by matching listed
                # filenames here.
                continue
            pattern = re.compile(fe.patterns[0])
            match = re.match(pattern, fn)

            if match:
                named_groups = match.groupdict()
                dest_entity_id = fe.dest_entity_id.format(**named_groups)
                self.logger.debug(f"Matched {fn} to {dest_entity_id}")

                filetype = named_groups.get("filetype", "csv")

                # Build lazy DataFrame plan - NO execution!
                df = (
                    self.spark.read.format(filetype)
                    .option("header", "true")
                    .option("inferSchema", "false")
                    .load(f"{path}/{fn}")
                )

                # Add named groups, static values, and ingestion timestamp (still lazy)
                df = enrich_file_dataframe(df, named_groups, fe.static_values)

                # Apply custom transformation if provided
                if transform:
                    df = transform(df)

                file_info = {"source_path": f"{path}/{fn}", "filename": fn}

                return (dest_entity_id, df, file_info)

        self.logger.debug(f"No pattern matched for {fn}")
        return None

    def _write_table_group(
        self, dest_entity_id: str, df_list: List, movepath: Optional[str] = None
    ):
        """Union and write all DataFrames for a single destination table.

        Args:
            dest_entity_id: Destination entity ID
            df_list: List of (dataframe, file_info) tuples
            movepath: Optional path to move files after successful write
        """
        de = self.der.get_entity_definition(dest_entity_id)

        if not de:
            raise ValueError(
                f"File ingestor references unknown destination entity '{dest_entity_id}'. "
                "Register the entity with @DataEntities.entity() before running ingestion."
            )

        # Union all DataFrames for this table
        dfs = [df for df, _ in df_list]
        if len(dfs) == 1:
            combined_df = dfs[0]
        else:
            # Union all DataFrames, allowing missing columns
            combined_df = dfs[0]
            for df in dfs[1:]:
                combined_df = combined_df.unionByName(df, allowMissingColumns=True)

        # Spark reads all files for THIS table in parallel during write.
        # No dedicated span here: the provider-op tracer (trace_ops) supplies
        # the append_to_entity child span.
        try:
            self.ep.append_to_entity(combined_df, de)

            self.logger.info(f"Successfully wrote {len(df_list)} files to {dest_entity_id}")

            # Emit batch_written signal
            self.emit(
                "file_ingestion.batch_written",
                dest_entity_id=dest_entity_id,
                file_count=len(df_list),
            )

            # Clean up after successful write
            if movepath:
                for _, file_info in df_list:
                    self.env.copy(file_info["source_path"], movepath)
                    self.env.delete(file_info["source_path"])
                    self.logger.debug(f"Moved {file_info['filename']} to {movepath}")

                    # Emit file_moved signal
                    self.emit(
                        "file_ingestion.file_moved",
                        filename=file_info["filename"],
                        source_path=file_info["source_path"],
                        dest_path=movepath,
                    )

        except Exception as e:
            self.logger.error(f"Failed to write {dest_entity_id}: {e}")
            raise
            raise

    def _has_batch_entries(self) -> bool:
        """Whether any registered entry still relies on discovery="batch" listing.

        process_path() only needs env.list(path) -- the per-run directory
        listing discovery="autoloader" entries exist to avoid (gh#228) --
        when at least one entry hasn't opted into Auto Loader discovery.
        """
        return any(
            self.fir.get_entry_definition(fi).discovery == "batch"
            for fi in self.fir.get_entry_ids()
        )

    def process_path(
        self, path: str, movepath: Optional[str] = None, transform: Optional[Callable] = None
    ):
        """Process all files in path, grouping by destination table.

        Args:
            path: Path containing files to ingest
            movepath: Optional path to move files after successful ingestion
            transform: Optional function to transform DataFrames before writing
        """
        batch_id = str(uuid.uuid4())
        start_time = time.time()
        success_files = 0
        failed_files = 0

        # Component previously named a nonexistent class
        # ("SimpleFileIngestionProcessor"); normalized to the naming
        # convention (gh#210).
        process_span = (
            self.tp.span(
                component=COMPONENT_INGESTION,
                operation="process",
                details={"path": path, "batch_id": batch_id},
                reraise=True,
            )
            if self._trace_gates.minimal
            else nullcontext()
        )
        with process_span:
            if self._has_batch_entries():
                filenames = self.env.list(path)
                self.logger.info(f"Found {len(filenames)} files in {path}")
            else:
                # No discovery="batch" entries are registered, so there is
                # nothing for a listing to match against -- skip env.list()
                # entirely. Auto Loader entries discover files via their own
                # cloudFiles stream (_process_autoloader_entries), so an
                # autoloader-only registry actually avoids the per-run
                # directory listing cost that motivated the feature (gh#228).
                filenames = []
                self.logger.debug(
                    f'No discovery="batch" entries registered -- skipping '
                    f"directory listing for {path}"
                )

            # Emit before_process signal
            self.emit(
                "file_ingestion.before_process",
                path=path,
                file_count=len(filenames),
                batch_id=batch_id,
            )

            try:
                # Phase 1: Build DataFrame plans and group by destination (fast, no execution)
                df_plans = defaultdict(list)

                for fn in filenames:
                    # Emit before_file signal
                    self.emit("file_ingestion.before_file", filename=fn, batch_id=batch_id)

                    # Per-file spans only at verbose level: paths can hold
                    # thousands of files and standard runs must stay lean.
                    file_span = (
                        self.tp.span(
                            operation="file",
                            component=COMPONENT_INGESTION,
                            details={"filename": fn, "batch_id": batch_id},
                            reraise=True,
                        )
                        if self._trace_gates.verbose
                        else nullcontext()
                    )
                    try:
                        with file_span:
                            result = self._build_df_plan(fn, path, transform)
                        if result:
                            dest_entity_id, df, file_info = result
                            df_plans[dest_entity_id].append((df, file_info))
                            success_files += 1

                            # Emit after_file signal
                            self.emit(
                                "file_ingestion.after_file",
                                filename=fn,
                                dest_entity_id=dest_entity_id,
                                batch_id=batch_id,
                            )
                        else:
                            # No pattern matched
                            self.emit(
                                "file_ingestion.after_file",
                                filename=fn,
                                dest_entity_id=None,
                                matched=False,
                                batch_id=batch_id,
                            )
                    except Exception as e:
                        failed_files += 1
                        self.emit(
                            "file_ingestion.file_failed",
                            filename=fn,
                            error=str(e),
                            error_type=type(e).__name__,
                            batch_id=batch_id,
                        )
                        raise

                tables_written = 0
                if not df_plans:
                    self.logger.info("No files matched any patterns")
                else:
                    self.logger.info(f"Grouped files into {len(df_plans)} destination tables")

                    # Phase 2: Process each destination table (optionally in parallel)
                    max_workers = self.config.get("ingestion.max_parallel_tables", 3)

                    if max_workers <= 1 or len(df_plans) == 1:
                        # Sequential processing
                        for dest_entity_id, df_list in df_plans.items():
                            self._write_table_group(dest_entity_id, df_list, movepath)
                    else:
                        # Parallel processing
                        self.logger.info(
                            f"Processing {len(df_plans)} tables in parallel (max_workers={max_workers})"
                        )
                        with ThreadPoolExecutor(max_workers=max_workers) as executor:
                            futures = {
                                executor.submit(
                                    self._write_table_group, dest_entity_id, df_list, movepath
                                ): dest_entity_id
                                for dest_entity_id, df_list in df_plans.items()
                            }

                            for future in as_completed(futures):
                                dest_entity_id = futures[future]
                                try:
                                    future.result()
                                except Exception as e:
                                    self.logger.error(f"Failed to write {dest_entity_id}: {e}")

                    tables_written = len(df_plans)

                # Phase 3: Auto Loader entries -- one cloudFiles stream per
                # discovery="autoloader" entry, scoped to this same path.
                # Independent of whether any batch file matched above, and a
                # fast no-op when no entry has opted in, so
                # before_process/after_process keep wrapping one
                # process_path() call end-to-end exactly as before for
                # batch-only registries (regression-safe default).
                al_success, al_failed, al_tables = self._process_autoloader_entries(
                    path, movepath, transform
                )
                success_files += al_success
                failed_files += al_failed
                tables_written += al_tables

                duration = time.time() - start_time
                self.emit(
                    "file_ingestion.after_process",
                    path=path,
                    success_files=success_files,
                    failed_files=failed_files,
                    tables_written=tables_written,
                    duration_seconds=duration,
                    batch_id=batch_id,
                )

            except Exception as e:
                duration = time.time() - start_time
                self.emit(
                    "file_ingestion.process_failed",
                    path=path,
                    error=str(e),
                    error_type=type(e).__name__,
                    success_files=success_files,
                    failed_files=failed_files,
                    duration_seconds=duration,
                    batch_id=batch_id,
                )
                raise

    def _process_autoloader_entries(
        self, path: str, movepath: Optional[str], transform: Optional[Callable]
    ) -> Tuple[int, int, int]:
        """Run every discovery="autoloader" entry's cloudFiles stream against `path`.

        Returns (success_files, failed_files, tables_written) totals across
        every entry's Trigger.AvailableNow run, for process_path() to fold
        into its single before_process/after_process pair. A fast no-op
        when no entry has opted into discovery="autoloader" -- never
        touches DI resolution or Spark, so batch-only/non-Databricks usage
        is unaffected.
        """
        autoloader_entries = [
            fe
            for fe in (self.fir.get_entry_definition(fi) for fi in self.fir.get_entry_ids())
            if fe.discovery == "autoloader"
        ]
        if not autoloader_entries:
            return 0, 0, 0

        runner = self._get_autoloader_runner()
        checkpoint_root = self.config.get("kindling.storage.checkpoint_root")
        if not checkpoint_root:
            raise ValueError(
                "Missing kindling.storage.checkpoint_root config -- required to "
                "derive per-entry Auto Loader checkpoint/schema locations."
            )

        totals = {"success": 0, "failed": 0, "tables": 0}

        for entry in autoloader_entries:
            checkpoint_location = f"{checkpoint_root}/file_ingestion/{entry.entry_id}/checkpoint"
            schema_location = f"{checkpoint_root}/file_ingestion/{entry.entry_id}/schema"

            def _write_batch(batch_df, micro_batch_id, entry=entry):
                s, f, t = self._process_autoloader_batch(
                    entry, batch_df, str(micro_batch_id), movepath, transform
                )
                totals["success"] += s
                totals["failed"] += f
                totals["tables"] += t

            runner.run_entry(entry, path, checkpoint_location, schema_location, _write_batch)

        return totals["success"], totals["failed"], totals["tables"]

    def _get_autoloader_runner(self) -> "AutoLoaderFileIngestionRunner":
        """Resolve the bound Auto Loader runner, lazily.

        Resolved only when a discovery="autoloader" entry is actually
        encountered -- constructing this processor never requires the
        extension, so batch-only pipelines on any engine are unaffected.
        """
        try:
            return GlobalInjector.get(AutoLoaderFileIngestionRunner)
        except Exception as e:
            raise RuntimeError(
                'No Auto Loader runner is bound for discovery="autoloader" file '
                "ingestion entries. Install and import "
                "kindling_ext_databricks_autoloader to enable Auto Loader "
                "(cloudFiles) discovery."
            ) from e

    def _process_autoloader_batch(
        self,
        entry: FileIngestionMetadata,
        batch_df,
        micro_batch_id: str,
        movepath: Optional[str],
        transform: Optional[Callable],
    ) -> Tuple[int, int, int]:
        """Enrich and write one Auto Loader microbatch for a single entry.

        Mirrors _build_df_plan's per-file matching against
        `entry.patterns[0]`, but files are enumerated via the standard
        Spark ``_metadata.file_path`` column (already delivered by
        cloudFiles) instead of a fresh spark.read.load() per filename --
        the microbatch has already been read.
        """
        success_files = 0
        failed_files = 0
        pattern = re.compile(entry.patterns[0])

        file_paths = [
            row["file_path"]
            for row in (
                batch_df.select(col("_metadata.file_path").alias("file_path")).distinct().collect()
            )
        ]

        df_plans = defaultdict(list)
        for file_path in file_paths:
            fn = file_path.rsplit("/", 1)[-1]
            self.emit("file_ingestion.before_file", filename=fn, batch_id=micro_batch_id)
            try:
                match = re.match(pattern, fn)
                if not match:
                    # source_glob scopes discovery per entry, but glob and
                    # regex are different languages -- a file can pass the
                    # entry's own glob and still miss its own regex.
                    self.emit(
                        "file_ingestion.after_file",
                        filename=fn,
                        dest_entity_id=None,
                        matched=False,
                        batch_id=micro_batch_id,
                    )
                    continue

                named_groups = match.groupdict()
                dest_entity_id = entry.dest_entity_id.format(**named_groups)

                file_df = batch_df.filter(col("_metadata.file_path") == file_path)
                file_df = enrich_file_dataframe(file_df, named_groups, entry.static_values)
                if transform:
                    file_df = transform(file_df)

                df_plans[dest_entity_id].append(
                    (file_df, {"source_path": file_path, "filename": fn})
                )
                success_files += 1
                self.emit(
                    "file_ingestion.after_file",
                    filename=fn,
                    dest_entity_id=dest_entity_id,
                    batch_id=micro_batch_id,
                )
            except Exception as e:
                failed_files += 1
                self.emit(
                    "file_ingestion.file_failed",
                    filename=fn,
                    error=str(e),
                    error_type=type(e).__name__,
                    batch_id=micro_batch_id,
                )
                raise

        for dest_entity_id, df_list in df_plans.items():
            self._write_table_group(dest_entity_id, df_list, movepath)

        return success_files, failed_files, len(df_plans)
