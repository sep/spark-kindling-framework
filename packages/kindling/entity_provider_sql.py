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

import re

from injector import inject
from kindling.data_entities import EntityMetadata
from kindling.entity_provider import BaseEntityProvider
from kindling.injection import GlobalInjector
from kindling.spark_log_provider import PythonLoggerProvider
from kindling.spark_session import get_or_create_spark_session
from pyspark.sql import DataFrame

_QUERY_START = ("select", "with", "values", "table", "from")
_NON_QUERY_KEYWORDS = re.compile(
    r"\b(insert|update|delete|merge|drop|create|alter|truncate|replace|grant|revoke"
    r"|refresh|optimize|vacuum|msck|load|cache|uncache|set|reset|use|call)\b",
    re.IGNORECASE,
)


def _strip_sql_comments_and_literals(sql: str) -> str:
    """The SQL with comments removed and quoted text blanked, so keyword and
    statement checks see only the statement's own tokens."""
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.S)
    sql = re.sub(r"--[^\n]*", " ", sql)
    return re.sub(r"'(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\"|`[^`]*`", "''", sql)


def require_query_sql(sql: str, entity_id: str) -> None:
    """Raise ValueError unless sql is a single read-only query.

    A SQL entity is read by running its SQL, so anything other than a query
    (DDL, DML, session commands, several statements) must be rejected before
    it reaches Spark.
    """
    code = _strip_sql_comments_and_literals(sql).strip().rstrip(";").strip()
    first_word = re.match(r"\(*\s*([A-Za-z]+)", code)
    problem = None
    if not code:
        problem = "it is empty"
    elif ";" in code:
        problem = "it contains more than one statement"
    elif not first_word or first_word.group(1).lower() not in _QUERY_START:
        problem = "it does not start with SELECT, WITH, VALUES, TABLE or FROM"
    else:
        match = _NON_QUERY_KEYWORDS.search(code)
        if match:
            problem = f"it contains the non-query keyword {match.group(1).upper()}"
    if problem:
        raise ValueError(
            f"SQL entity '{entity_id}' must be a single read-only query, but {problem}."
        )


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
        require_query_sql(entity_metadata.sql, entity_metadata.entityid)
