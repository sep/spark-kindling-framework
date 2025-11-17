import logging
import re
import time
from abc import ABC, abstractmethod
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, fields
from functools import reduce
from typing import Any, Callable, Dict, List, Optional

from delta.tables import DeltaTable
from injector import Binder, Injector, inject, singleton
from kindling.data_entities import *
from kindling.file_ingestion import *
from kindling.injection import *
from kindling.platform_provider import *
from kindling.spark_config import *
from kindling.spark_log_provider import *
from kindling.spark_session import *
from kindling.spark_trace import *
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, lit


@dataclass
class FileIngestionMetadata:
    entry_id: str
    name: str
    patterns: List[str]
    dest_entity_id: str
    tags: Dict[str, str]
    infer_schema: bool = True
    filetype: str = "csv"


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

        missing_fields = required_fields - decorator_params.keys()

        if missing_fields:
            raise ValueError(
                f"Missing required fields in file ingestion decorator: {missing_fields}"
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
    def get_entry_ids(self):
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

    def get_entry_ids(self):
        return self.registry.keys()

    def get_entry_definition(self, entryId):
        return self.registry.get(entryId)


class FileIngestionProcessor(ABC):
    @abstractmethod
    def process_path(self, path: str):
        pass


class FileIngestionProcessorProvider(ABC):
    @abstractmethod
    def get_file_processor(self, path: str):
        pass


@GlobalInjector.singleton_autobind()
class ParallelizingFileIngestionProcessor(FileIngestionProcessor):
    """Advanced file ingestion processor with batching, parallelism, and signal support.

    Features:
    - Lazy DataFrame building - builds execution plans without triggering Spark actions
    - Batching - groups files by destination table and writes in bulk
    - Parallel execution - processes multiple tables concurrently
    - File management - moves files after successful ingestion
    - Transform support - applies custom transformations to DataFrames
    - Enrichment - adds named regex groups and ingestion timestamp as columns
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
    ):
        self.config = config
        self.fir = fir
        self.ep = ep
        self.der = der
        self.tp = tp
        self.logger = lp.get_logger("SimpleFileIngestionProcessor")
        self.spark = get_or_create_spark_session()
        self.env = pep.get_service()

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

                # Add named groups as columns (still lazy)
                for group_name, group_value in named_groups.items():
                    df = df.withColumn(group_name, lit(group_value))

                # Add ingestion timestamp
                df = df.withColumn("ingestion_timestamp", current_timestamp())

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
            self.logger.error(f"Entity definition not found: {dest_entity_id}")
            return

        # Ensure table exists
        self.ep.ensure_entity_table(de)

        # Union all DataFrames for this table
        dfs = [df for df, _ in df_list]
        if len(dfs) == 1:
            combined_df = dfs[0]
        else:
            # Union all DataFrames, allowing missing columns
            combined_df = dfs[0]
            for df in dfs[1:]:
                combined_df = combined_df.unionByName(df, allowMissingColumns=True)

        # Spark reads all files for THIS table in parallel during write
        try:
            with self.tp.span(operation="append_to_entity"):
                self.ep.append_to_entity(combined_df, de)

            self.logger.info(f"Successfully wrote {len(df_list)} files to {dest_entity_id}")

            # Clean up after successful write
            if movepath:
                for _, file_info in df_list:
                    self.env.copy(file_info["source_path"], movepath)
                    self.env.delete(file_info["source_path"])
                    self.logger.debug(f"Moved {file_info['filename']} to {movepath}")

        except Exception as e:
            self.logger.error(f"Failed to write {dest_entity_id}: {e}")
            raise

    def process_path(
        self, path: str, movepath: Optional[str] = None, transform: Optional[Callable] = None
    ):
        """Process all files in path, grouping by destination table.

        Args:
            path: Path containing files to ingest
            movepath: Optional path to move files after successful ingestion
            transform: Optional function to transform DataFrames before writing
        """
        with self.tp.span(
            component="SimpleFileIngestionProcessor",
            operation="process_path",
            details={"path": path},
            reraise=True,
        ):
            filenames = self.env.list(path)
            self.logger.info(f"Found {len(filenames)} files in {path}")

            # Phase 1: Build DataFrame plans and group by destination (fast, no execution)
            df_plans = defaultdict(list)

            for fn in filenames:
                result = self._build_df_plan(fn, path, transform)
                if result:
                    dest_entity_id, df, file_info = result
                    df_plans[dest_entity_id].append((df, file_info))

            if not df_plans:
                self.logger.info("No files matched any patterns")
                return

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
