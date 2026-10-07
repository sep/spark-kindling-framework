"""
SQL entity provider: reads SQL-defined entities by evaluating their SQL.

SQL entities are registered via ``@DataEntities.sql_entity(...)`` and tagged
``provider_type: "view"``. The ``EntityProviderRegistry`` maps that type to
this provider, so a SQL entity can be a pipe input in the core runner.

Reads evaluate the entity's declared SQL (``spark.sql(entity.sql)``) instead
of reading the published catalog view. That keeps a read independent of
deployment DDL: the result matches the SQL the running code declares, and it
works where ``kindling migrate apply`` has not run (or cannot persist a view,
e.g. a standalone session catalog, or SQL over session temp views). Creating
and replacing the permanent catalog view stays the job of
``kindling migrate apply``.

The provider is read-only. It implements no write or destination-ensuring
interface, so capability checks (``is_writable``, ``can_ensure_destination``)
report it as read-only, and the runner rejects a pipe that writes to a SQL
entity before any DDL or write is issued.
"""

from injector import inject
from kindling.data_entities import EntityMetadata
from kindling.entity_provider import BaseEntityProvider
from kindling.injection import GlobalInjector
from kindling.spark_log_provider import PythonLoggerProvider
from kindling.spark_session import get_or_create_spark_session
from pyspark.sql import DataFrame


@GlobalInjector.singleton_autobind()
class SqlEntityProvider(BaseEntityProvider):
    """
    Read-only provider for SQL-defined entities (``provider_type: "view"``).

    ``read_entity`` returns ``spark.sql(entity.sql)`` as a batch DataFrame.
    ``check_entity_exists`` reports whether that SQL resolves (every table
    or view it references exists).
    """

    @inject
    def __init__(self, tp: PythonLoggerProvider):
        self._logger = tp.get_logger("SqlEntityProvider")

    def read_entity(self, entity_metadata: EntityMetadata) -> DataFrame:
        self._require_sql_entity(entity_metadata)
        self._logger.debug(f"Reading SQL entity '{entity_metadata.entityid}' by evaluating its SQL")
        return get_or_create_spark_session().sql(entity_metadata.sql)

    def check_entity_exists(self, entity_metadata: EntityMetadata) -> bool:
        """True when the entity's SQL resolves. Spark analyzes the query
        eagerly in ``spark.sql`` without running a job, so a missing
        table or view surfaces here as an analysis error."""
        self._require_sql_entity(entity_metadata)
        try:
            get_or_create_spark_session().sql(entity_metadata.sql)
            return True
        except Exception as e:
            self._logger.debug(f"SQL entity '{entity_metadata.entityid}' does not resolve: {e}")
            return False

    @staticmethod
    def _require_sql_entity(entity_metadata: EntityMetadata) -> None:
        if not entity_metadata.is_sql_entity:
            raise ValueError(
                f"SqlEntityProvider cannot read non-SQL entity '{entity_metadata.entityid}': "
                "provider_type 'view' is reserved for entities declared with "
                "DataEntities.sql_entity()."
            )
