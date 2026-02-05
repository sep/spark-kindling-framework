"""
CSV Entity Provider

Read-only entity provider for CSV files using Spark's CSV reader.
"""

from typing import Any, Dict

from injector import inject
from pyspark.sql import DataFrame

from .data_entities import EntityMetadata
from .entity_provider import BaseEntityProvider
from .injection import GlobalInjector
from .spark_config import get_or_create_spark_session
from .spark_log_provider import PythonLoggerProvider


@GlobalInjector.singleton_autobind()
class CSVEntityProvider(BaseEntityProvider):
    """
    CSV file entity provider (read-only batch operations).

    Implements only BaseEntityProvider interface for reading CSV files.
    Does not support write operations (CSV is typically used for reference/lookup data).

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

    def _get_provider_config(self, entity_metadata: EntityMetadata) -> Dict[str, Any]:
        """
        Extract provider configuration from entity tags.

        Looks for tags with 'provider.' prefix and converts them to a config dict.

        Args:
            entity_metadata: Entity metadata

        Returns:
            Dict with provider configuration (keys without 'provider.' prefix)
        """
        config = {}
        for key, value in entity_metadata.tags.items():
            if key.startswith("provider."):
                config_key = key[9:]  # Remove 'provider.' prefix
                # Convert string values to appropriate types
                if value.lower() in ("true", "false"):
                    config[config_key] = value.lower() == "true"
                else:
                    config[config_key] = value
        return config

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
                f"CSV provider requires 'path' in tags (provider.path) for entity '{entity_metadata.entityid}'"
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

            self.logger.info(
                f"Successfully read CSV entity '{entity_metadata.entityid}': {df.count()} rows, {len(df.columns)} columns"
            )

            return df

        except Exception as e:
            self.logger.error(
                f"Failed to read CSV entity '{entity_metadata.entityid}' from {path}: {e}",
                include_traceback=True,
            )
            raise

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
                f"Cannot check existence for entity '{entity_metadata.entityid}': no path in tags (provider.path)"
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
