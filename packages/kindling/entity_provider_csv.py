"""
CSV Entity Provider

Read-only entity provider for CSV files using Spark's CSV reader.
Includes FixtureCSVEntityProvider for auto-discovered tests/entities/ fixtures.
"""

import logging
from pathlib import Path
from typing import Optional

from injector import inject
from pyspark.sql import DataFrame

from .data_entities import EntityMetadata
from .entity_provider import BaseEntityProvider, WritableEntityProvider
from .injection import GlobalInjector
from .spark_config import get_or_create_spark_session
from .spark_log_provider import PythonLoggerProvider

_logger = logging.getLogger(__name__)


def _entity_id_to_fixture_path(entity_id: str, base_dir: Path) -> Path:
    """
    Map an entity ID to its expected fixture CSV path under base_dir.

    Dotted entity IDs map to nested directories:
        bronze.orders  -> <base_dir>/bronze/orders.csv
        orders         -> <base_dir>/orders.csv

    Args:
        entity_id: Entity identifier (may contain dots)
        base_dir: Root directory to search under

    Returns:
        Expected Path for the fixture CSV file
    """
    parts = [p.strip() for p in entity_id.split(".") if p.strip()]
    if not parts:
        parts = [entity_id]
    if len(parts) > 1:
        relative = Path(*parts[:-1], f"{parts[-1]}.csv")
    else:
        relative = Path(f"{parts[0]}.csv")
    return base_dir / relative


def resolve_fixture_csv_path(entity_id: str, cwd: Path) -> Optional[Path]:
    """
    Return the fixture CSV path if it exists under tests/entities/ relative to cwd.

    Returns None when the convention does not apply (file absent).

    Args:
        entity_id: Entity identifier (may contain dots)
        cwd: Working directory from which kindling was invoked

    Returns:
        Absolute Path to the CSV fixture, or None if not found
    """
    base_dir = cwd / "tests" / "entities"
    candidate = _entity_id_to_fixture_path(entity_id, base_dir)
    if candidate.is_file():
        return candidate
    return None


class FixtureCSVEntityProvider(BaseEntityProvider):
    """
    Entity provider that reads a single fixture CSV file from tests/entities/.

    This provider is instantiated per-entity during local execution when the
    auto-discovery convention applies.  It is NOT registered as a singleton in
    the DI container — callers construct it directly.

    Raises:
        ValueError: If the CSV exists but contains no data rows (headers only).
    """

    def __init__(self, csv_path: Path) -> None:
        """
        Args:
            csv_path: Absolute path to the fixture CSV file.
        """
        self._csv_path = csv_path

    def read_entity(self, entity_metadata: EntityMetadata) -> DataFrame:
        """
        Read the fixture CSV as a batch DataFrame.

        Args:
            entity_metadata: Entity metadata (used for error messages only).

        Returns:
            DataFrame containing the fixture data.

        Raises:
            ValueError: If the CSV has no data rows (headers only).
        """
        path_str = str(self._csv_path)
        _logger.info(
            "Reading fixture CSV for entity '%s' from %s",
            entity_metadata.entityid,
            path_str,
        )

        spark = get_or_create_spark_session()
        df = (
            spark.read.format("csv")
            .option("header", True)
            .option("inferSchema", True)
            .load(path_str)
        )

        # Guard against headers-only fixtures to give a clear error early.
        if df.count() == 0:
            raise ValueError(
                f"Entity '{entity_metadata.entityid}' fixture at {path_str} has no data rows. "
                "Populate it before running."
            )

        return df

    def check_entity_exists(self, entity_metadata: EntityMetadata) -> bool:
        """Return True if the fixture file exists on disk."""
        return self._csv_path.is_file()


@GlobalInjector.singleton_autobind()
class CSVEntityProvider(BaseEntityProvider, WritableEntityProvider):
    """
    CSV file entity provider (batch read and write operations).

    Implements BaseEntityProvider and WritableEntityProvider for reading and
    writing CSV files. Write operations use Spark's CSV writer; existing files
    are overwritten in full-write mode and appended in append mode.

    Provider configuration options (via entity tags with 'provider.' prefix):
    - provider.path: CSV file path (required)
    - provider.header: Whether first row is header (default: "true")
    - provider.inferSchema: Infer column types (default: "true")
    - provider.delimiter: Column delimiter (default: ",")
    - provider.encoding: File encoding (default: "UTF-8")
    - provider.quote: Quote character (default: '"')
    - provider.escape: Escape character (default: '\\')
    - provider.multiLine: Support multi-line records (default: "false")

    Example entity definition:
    ```python
    @DataEntities.entity(
        entityid="ref.product_categories",
        name="product_categories",
        partition_columns=[],
        merge_columns=["category_id"],
        tags={
            "provider_type": "csv",
            "provider.path": "Files/reference/categories.csv",
            "provider.header": "true",
            "provider.inferSchema": "true"
        },
        schema=None
    )
    ```
    """

    @inject
    def __init__(self, logger_provider: PythonLoggerProvider):
        self.logger = logger_provider.get_logger("CSVEntityProvider")

    def read_entity(self, entity_metadata: EntityMetadata) -> DataFrame:
        """
        Read CSV file as batch DataFrame.

        Args:
            entity_metadata: Entity metadata with tags containing CSV options

        Returns:
            DataFrame containing CSV data

        Raises:
            ValueError: If path is not specified in tags
            Exception: If CSV read fails
        """
        config = self._get_provider_config(entity_metadata)

        # Validate required config
        path = config.get("path")
        if not path:
            raise ValueError(
                f"CSV provider requires 'path' in tags (provider.path) for entity "
                f"'{entity_metadata.entityid}'"
            )

        # Extract CSV options with defaults
        header = config.get("header", True)
        infer_schema = config.get("inferSchema", True)
        delimiter = config.get("delimiter", ",")
        encoding = config.get("encoding", "UTF-8")
        quote = config.get("quote", '"')
        escape = config.get("escape", "\\")
        multi_line = config.get("multiLine", False)

        self.logger.info(f"Reading CSV entity '{entity_metadata.entityid}' from path: {path}")
        self.logger.debug(
            f"CSV options: header={header}, inferSchema={infer_schema}, delimiter={delimiter}"
        )

        try:
            # Get current Spark session
            spark = get_or_create_spark_session()

            # Build CSV reader with options
            reader = (
                spark.read.format("csv")
                .option("header", header)
                .option("inferSchema", infer_schema)
                .option("delimiter", delimiter)
                .option("encoding", encoding)
                .option("quote", quote)
                .option("escape", escape)
                .option("multiLine", multi_line)
            )

            # Add any additional options from config
            additional_options = {
                k: v
                for k, v in config.items()
                if k
                not in [
                    "path",
                    "header",
                    "inferSchema",
                    "delimiter",
                    "encoding",
                    "quote",
                    "escape",
                    "multiLine",
                ]
            }

            if additional_options:
                self.logger.debug(f"Additional CSV options: {additional_options}")
                reader = reader.options(**additional_options)

            # Load CSV
            df = reader.load(path)

            row_count = df.count()
            col_count = len(df.columns)
            self.logger.info(
                f"Successfully read CSV entity '{entity_metadata.entityid}': "
                f"{row_count} rows, {col_count} columns"
            )

            return df

        except Exception as e:
            self.logger.error(
                f"Failed to read CSV entity '{entity_metadata.entityid}' from {path}: {e}",
                include_traceback=True,
            )
            raise

    def _write_csv(self, df: DataFrame, entity_metadata: EntityMetadata, mode: str) -> None:
        config = self._get_provider_config(entity_metadata)
        path = config.get("path")
        if not path:
            raise ValueError(
                f"CSV provider requires 'path' in tags (provider.path) for entity "
                f"'{entity_metadata.entityid}'"
            )
        header = config.get("header", True)
        delimiter = config.get("delimiter", ",")
        encoding = config.get("encoding", "UTF-8")
        quote = config.get("quote", '"')
        escape = config.get("escape", "\\")

        self.logger.info(
            f"Writing CSV entity '{entity_metadata.entityid}' to path: {path} (mode={mode})"
        )
        try:
            (
                df.write.format("csv")
                .mode(mode)
                .option("header", header)
                .option("delimiter", delimiter)
                .option("encoding", encoding)
                .option("quote", quote)
                .option("escape", escape)
                .save(path)
            )
            self.logger.info(f"Successfully wrote CSV entity '{entity_metadata.entityid}'")
        except Exception as e:
            self.logger.error(
                f"Failed to write CSV entity '{entity_metadata.entityid}' to {path}: {e}",
                include_traceback=True,
            )
            raise

    def write_to_entity(self, df: DataFrame, entity_metadata: EntityMetadata) -> None:
        """Write DataFrame to CSV (overwrite mode)."""
        self._write_csv(df, entity_metadata, "overwrite")

    def append_to_entity(self, df: DataFrame, entity_metadata: EntityMetadata) -> None:
        """Append DataFrame to CSV (append mode)."""
        self._write_csv(df, entity_metadata, "append")

    def check_entity_exists(self, entity_metadata: EntityMetadata) -> bool:
        """
        Check if CSV file exists.

        Args:
            entity_metadata: Entity metadata with tags containing path

        Returns:
            True if CSV file exists and is readable, False otherwise
        """
        config = self._get_provider_config(entity_metadata)
        path = config.get("path")

        if not path:
            self.logger.warning(
                f"Cannot check existence for entity '{entity_metadata.entityid}': "
                "no path in tags (provider.path)"
            )
            return False

        try:
            # Get current Spark session
            spark = get_or_create_spark_session()
            # Try to read just the schema (lightweight check)
            spark.read.format("csv").option("header", True).load(path).schema
            self.logger.debug(f"CSV entity '{entity_metadata.entityid}' exists at: {path}")
            return True

        except Exception as e:
            self.logger.debug(f"CSV entity '{entity_metadata.entityid}' not found at {path}: {e}")
            return False
