"""
SQL entity provider — manages permanent Spark catalog views.

SQL entities are registered via ``@DataEntities.sql_entity(...)`` and are
read-only.  The provider creates/replaces the catalog view on ``ensure_destination``
and reads it like any other catalog table.

No write operations are supported; attempting them raises ``NotImplementedError``.
"""

from injector import inject
from kindling.data_entities import EntityMetadata
from kindling.entity_provider import BaseEntityProvider, DestinationEnsuringProvider
from kindling.injection import GlobalInjector
from kindling.spark_log_provider import PythonLoggerProvider
from kindling.spark_session import get_or_create_spark_session
from pyspark.sql import DataFrame


@GlobalInjector.singleton_autobind()
class SqlEntityProvider(BaseEntityProvider, DestinationEnsuringProvider):
    """
    Read-only entity provider backed by a permanent Spark catalog view.

    ``ensure_destination`` issues ``CREATE OR REPLACE VIEW name AS <sql>``.
    ``read_entity`` reads the view as a batch DataFrame via ``spark.read.table``.
    """

    @inject
    def __init__(self, tp: PythonLoggerProvider):
        self._logger = tp.get_logger("SqlEntityProvider")

    # ------------------------------------------------------------------
    # BaseEntityProvider
    # ------------------------------------------------------------------

    def read_entity(self, entity_metadata: EntityMetadata) -> DataFrame:
        if not entity_metadata.is_sql_entity:
            raise ValueError(
                f"SqlEntityProvider cannot read non-SQL entity '{entity_metadata.entityid}'. "
                "Use DeltaEntityProvider for Delta entities."
            )
        spark = get_or_create_spark_session()
        view_name = self._view_name(entity_metadata)
        self._logger.debug(
            f"Reading SQL entity '{entity_metadata.entityid}' from view '{view_name}'"
        )
        return spark.read.table(view_name)

    def check_entity_exists(self, entity_metadata: EntityMetadata) -> bool:
        spark = get_or_create_spark_session()
        view_name = self._view_name(entity_metadata)
        try:
            spark.sql(f"DESCRIBE {view_name}")
            return True
        except Exception:
            return False

    # ------------------------------------------------------------------
    # DestinationEnsuringProvider
    # ------------------------------------------------------------------

    def ensure_destination(self, entity_metadata: EntityMetadata) -> None:
        """Create or replace the catalog view from the entity's SQL definition."""
        if not entity_metadata.is_sql_entity:
            raise ValueError(
                f"SqlEntityProvider.ensure_destination called on non-SQL entity "
                f"'{entity_metadata.entityid}'."
            )
        spark = get_or_create_spark_session()
        view_name = self._view_name(entity_metadata)
        self._logger.info(f"Ensuring view '{view_name}' for entity '{entity_metadata.entityid}'")
        namespace = self._view_namespace(view_name)
        if namespace:
            try:
                spark.sql(f"CREATE SCHEMA IF NOT EXISTS {namespace}")
            except Exception as e:
                self._logger.warning(
                    f"Unable to ensure schema '{namespace}' exists for view '{view_name}': {e}"
                )
        spark.sql(f"CREATE OR REPLACE VIEW {view_name} AS {entity_metadata.sql}")

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _view_name(self, entity_metadata: EntityMetadata) -> str:
        """Resolve the catalog view name from entity tags or entity name."""
        return entity_metadata.tags.get("provider.table_name") or entity_metadata.name

    def _view_namespace(self, view_name: str) -> str:
        """Return the namespace portion of a qualified view name, or empty string if unqualified."""
        parts = []
        current = []
        in_backticks = False
        for char in view_name:
            if char == "`":
                in_backticks = not in_backticks
                current.append(char)
            elif char == "." and not in_backticks:
                parts.append("".join(current).strip())
                current = []
            else:
                current.append(char)
        if current:
            parts.append("".join(current).strip())
        return ".".join(parts[:-1]) if len(parts) >= 2 else ""
