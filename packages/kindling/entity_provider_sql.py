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

_QUERY_START = {"select", "with", "values", "table", "from"}


def _sql_tokens(sql: str):
    """Tokenize SQL into ("word", w), ("str", ""), ("(", ""), (")", ""),
    (";", "") and ("sym", c), skipping comments.

    One pass that tracks quoting, so comment markers inside strings stay part
    of the string and quotes inside comments stay part of the comment.
    """
    tokens = []
    i, n = 0, len(sql)
    while i < n:
        c = sql[i]
        if c.isspace():
            i += 1
        elif sql.startswith("--", i):
            # Spark ends a line comment at a carriage return or a line feed.
            ends = [e for e in (sql.find("\n", i), sql.find("\r", i)) if e != -1]
            i = min(ends) + 1 if ends else n
        elif sql.startswith("/*", i):
            # Spark block comments nest: /* a /* b */ c */ is one comment.
            depth, i = 1, i + 2
            while depth:
                if i >= n:
                    raise ValueError("unterminated block comment")
                if sql.startswith("/*", i):
                    depth, i = depth + 1, i + 2
                elif sql.startswith("*/", i):
                    depth, i = depth - 1, i + 2
                else:
                    i += 1
        elif c in "'\"`":
            j = i + 1
            while True:
                if j >= n:
                    raise ValueError("unterminated quoted text")
                if sql[j] == "\\" and c != "`":
                    j += 2
                    continue
                if sql[j] == c:
                    if j + 1 < n and sql[j + 1] == c:  # doubled quote
                        j += 2
                        continue
                    break
                j += 1
            tokens.append(("str", "") if c != "`" else ("word", "`"))
            i = j + 1
        elif c in "rR" and i + 1 < n and sql[i + 1] in "'\"":
            # Raw literal (r'...', R"..."): no escapes, ends at the next quote.
            end = sql.find(sql[i + 1], i + 2)
            if end == -1:
                raise ValueError("unterminated quoted text")
            tokens.append(("str", ""))
            i = end + 1
        elif c.isalpha() or c == "_":
            j = i
            while j < n and (sql[j].isalnum() or sql[j] == "_"):
                j += 1
            tokens.append(("word", sql[i:j].lower()))
            i = j
        elif c in "();":
            tokens.append((c, ""))
            i += 1
        else:
            tokens.append(("sym", c))
            i += 1
    return tokens


def _main_statement_index(tokens) -> int:
    """Index of the statement keyword after a WITH clause's definitions."""
    i, depth = 1, 0
    if i < len(tokens) and tokens[i] == ("word", "recursive"):
        i += 1
    while i < len(tokens):
        kind, value = tokens[i]
        if kind == "(" and depth == 0 and tokens[i - 1][0] == ")":
            # A parenthesized main query: WITH x AS (...) (SELECT ...).
            # Its statement keyword is the first token past the parens.
            while i < len(tokens) and tokens[i][0] == "(":
                i += 1
            return i if i < len(tokens) else -1
        if kind == "(":
            depth += 1
        elif kind == ")":
            depth -= 1
        elif depth == 0 and kind == "word" and value not in ("as", "not", "materialized"):
            # Skip the CTE name: the statement starts at the first depth-0
            # word that follows a closed definition and no comma.
            previous = tokens[i - 1][0]
            if previous == ")":
                return i
        i += 1
    return -1


def require_query_sql(sql: str, entity_id: str) -> None:
    """Raise ValueError unless sql is a single read-only query.

    A SQL entity is read by running its SQL, so DDL, DML, session commands or
    several statements must be rejected before reaching Spark. Statement
    keywords are checked only where a statement starts (and Spark's
    FROM-first INSERT), so functions and columns named like keywords
    (``replace(...)``, ``update_time``) are fine.
    """
    problem = None
    try:
        tokens = _sql_tokens(sql or "")
    except ValueError as exc:
        tokens, problem = [], str(exc)
    while tokens and tokens[-1][0] == ";":
        tokens.pop()
    first = next((i for i, tok in enumerate(tokens) if tok[0] != "("), None)
    if problem:
        pass
    elif first is None:
        problem = "it is empty"
    elif any(kind == ";" for kind, _ in tokens):
        problem = "it contains more than one statement"
    elif tokens[first][0] != "word" or tokens[first][1] not in _QUERY_START:
        problem = "it does not start with SELECT, WITH, VALUES, TABLE or FROM"
    else:
        if tokens[first][1] == "with":
            main = _main_statement_index(tokens[first:])
            keyword = tokens[first + main][1] if main >= 0 else None
            if keyword not in _QUERY_START - {"with"}:
                problem = (
                    f"the statement after its WITH clause is {str(keyword).upper()}, not a query"
                )
        if problem is None:
            depth = 0
            for index, (kind, value) in enumerate(tokens):
                depth += kind == "("
                depth -= kind == ")"
                following = tokens[index + 1] if index + 1 < len(tokens) else ("", "")
                if (
                    depth == 0
                    and kind == "word"
                    and value == "insert"
                    and following in (("word", "into"), ("word", "overwrite"))
                ):
                    problem = "it contains an INSERT statement"
                    break
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
