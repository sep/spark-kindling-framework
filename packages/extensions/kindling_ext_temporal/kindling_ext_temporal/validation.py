"""Validation for rules-as-data temporal Conditions."""

import logging
from collections import Counter
from dataclasses import dataclass, field
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Protocol,
    Sequence,
    Tuple,
)

from kindling.data_pipes import DataPipesRegistry
from kindling.pipe_graph import (
    GraphCycleError,
    PipeEdge,
    PipeGraph,
    PipeGraphBuilder,
    PipeNode,
)


class ConditionValidationError(ValueError):
    """Raised when callers request a valid-only condition set and rows are invalid."""


class SparkSqlExpressionParser(Protocol):
    """Parser interface used to validate Spark SQL expression strings."""

    def parse(self, expression: str) -> None:
        """Raise an exception when the expression cannot be parsed."""


def _empty_events_frame(spark):
    from .entities import events_schema

    try:
        return spark.createDataFrame([], events_schema())
    except Exception:  # noqa: BLE001 -- any frame works for the parse check
        return spark.range(0)


def _error_condition(exc: BaseException):
    for accessor in ("getCondition", "getErrorClass"):
        get = getattr(exc, accessor, None)
        if callable(get):
            try:
                condition = get()
            except Exception:  # noqa: BLE001
                condition = None
            if isinstance(condition, str):
                return condition
    return None


def _is_analysis_error(exc: BaseException) -> bool:
    """An analysis error (unresolved column/function, type mismatch): the
    expression parsed, it just isn't resolved against real events yet."""
    try:
        from pyspark.errors import AnalysisException
    except ImportError:  # pyspark < 3.4
        AnalysisException = ()  # type: ignore[assignment]
    return bool(AnalysisException) and isinstance(exc, AnalysisException)


def _is_parse_error(exc: BaseException) -> bool:
    """A SQL syntax error, whichever way the session reports it."""
    try:
        from pyspark.errors import ParseException
    except ImportError:  # pyspark < 3.4
        ParseException = ()  # type: ignore[assignment]
    if ParseException and isinstance(exc, ParseException):
        return True
    condition = _error_condition(exc)
    return isinstance(condition, str) and condition.startswith("PARSE_")


class ActiveSparkSqlExpressionParser:
    """Validate expression syntax with Spark's SQL parser.

    Column references are intentionally not resolved here. Condition rows are
    validated before they are scoped to a concrete events dataframe, so the check
    should reject malformed SQL without requiring every referenced column to be
    present in a local test dataframe.
    """

    def __init__(self, spark=None):
        self.spark = spark
        self._probe_frames = {}

    def _probe_frame(self, spark):
        frame = self._probe_frames.get(id(spark))
        if frame is None:
            frame = self._probe_frames[id(spark)] = _empty_events_frame(spark)
        return frame

    def parse(self, expression: str) -> None:
        if expression is None or not str(expression).strip():
            raise ValueError("expression is required")

        spark = self.spark
        if spark is None:
            from pyspark.sql import SparkSession

            spark = SparkSession.getActiveSession()

        if spark is None:
            raise RuntimeError("No active SparkSession available for expression parsing")

        # The direct route (spark._jsparkSession.sessionState().sqlParser())
        # is blocked by py4j security whitelisting on Databricks shared-access
        # clusters, so parse through the public API. functions.expr parses
        # eagerly only on classic Spark 3.x; Spark 4 (classic and Connect)
        # defers parsing to analysis, so force analysis of a one-column
        # projection (.schema issues the AnalyzePlan request on Connect). Only
        # a parse error makes the expression invalid: unresolved columns and
        # functions are expected, because rules are validated before they are
        # scoped to an events dataframe. Projecting over an empty frame with
        # the events schema resolves the usual columns (payload, subject_id,
        # ...), so valid rules don't make Spark log analysis errors.
        from pyspark.sql import functions as F

        try:
            self._probe_frame(spark).select(F.expr(expression)).schema
        except Exception as exc:  # noqa: BLE001 -- only analysis errors are accepted
            # Unknown columns/functions surface as analysis errors and are
            # fine at this stage. Anything else -- a parse error, or a failure
            # that means no check ran (stopped session, Connect RPC error) --
            # propagates, and the caller marks the condition invalid.
            if _is_parse_error(exc) or not _is_analysis_error(exc):
                raise


@dataclass(frozen=True)
class ConditionRule:
    """Normalized temporal Condition row."""

    condition_id: str
    consumes_event_type: List[str]
    subject_type: str
    parameters: Mapping[str, Any]
    enabled: bool = True
    valid_from: Optional[Any] = None
    valid_to: Optional[Any] = None

    @classmethod
    def from_row(cls, row: Any) -> "ConditionRule":
        data = _row_to_dict(row)
        return cls(
            condition_id=str(data.get("condition_id", "")).strip(),
            consumes_event_type=_normalize_event_types(data.get("consumes_event_type")),
            subject_type=str(data.get("subject_type", "")).strip(),
            parameters=data.get("parameters") or {},
            enabled=bool(data.get("enabled", True)),
            valid_from=data.get("valid_from"),
            valid_to=data.get("valid_to"),
        )

    @property
    def entered_event_type(self) -> str:
        return f"{self.condition_id}.entered"

    @property
    def exited_event_type(self) -> str:
        return f"{self.condition_id}.exited"

    @property
    def produced_event_types(self) -> List[str]:
        return [self.entered_event_type, self.exited_event_type]


@dataclass(frozen=True)
class InvalidCondition:
    condition_id: str
    errors: List[str]
    row: Any


@dataclass(frozen=True)
class ConditionValidationReport:
    valid_rules: List[ConditionRule] = field(default_factory=list)
    invalid_conditions: List[InvalidCondition] = field(default_factory=list)
    generations: List[List[str]] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        return not self.invalid_conditions


class TemporalConditionValidator:
    """Validate condition rows and compute event-type graph generations."""

    def __init__(
        self,
        expression_parser: Optional[SparkSqlExpressionParser] = None,
        graph_builder: Optional[PipeGraphBuilder] = None,
    ):
        self.expression_parser = expression_parser or ActiveSparkSqlExpressionParser()
        self.graph_builder = graph_builder or PipeGraphBuilder(
            registry=_NullPipeRegistry(),
            logger_provider=_LoggerProvider(),
        )

    def validate(self, rows: Iterable[Any]) -> ConditionValidationReport:
        valid_rules: List[ConditionRule] = []
        invalid_conditions: List[InvalidCondition] = []

        for row in rows:
            try:
                rule = ConditionRule.from_row(row)
            except Exception as exc:
                invalid_conditions.append(
                    InvalidCondition(condition_id="", errors=[str(exc)], row=row)
                )
                continue

            errors = self._validate_rule(rule)
            if errors:
                invalid_conditions.append(
                    InvalidCondition(
                        condition_id=rule.condition_id,
                        errors=errors,
                        row=row,
                    )
                )
            elif rule.enabled:
                valid_rules.append(rule)

        graph_generations: List[List[str]] = []
        if valid_rules:
            graph = self.build_event_type_graph(valid_rules)
            cycles = self.graph_builder.detect_cycles(graph)
            if cycles:
                cycle_error = GraphCycleError(cycles[0])
                invalid_conditions.append(
                    InvalidCondition(
                        condition_id="",
                        errors=[str(cycle_error)],
                        row=None,
                    )
                )
            else:
                graph_generations = self.graph_builder.get_generations(graph)

        return ConditionValidationReport(
            valid_rules=valid_rules,
            invalid_conditions=invalid_conditions,
            generations=graph_generations,
        )

    def validate_or_raise(self, rows: Iterable[Any]) -> ConditionValidationReport:
        report = self.validate(rows)
        if not report.is_valid:
            messages = [
                f"{invalid.condition_id or '<graph>'}: {'; '.join(invalid.errors)}"
                for invalid in report.invalid_conditions
            ]
            raise ConditionValidationError("Invalid temporal conditions:\n" + "\n".join(messages))
        return report

    def build_event_type_graph(self, rules: Iterable[ConditionRule]) -> PipeGraph:
        graph = PipeGraph()

        for rule in rules:
            if not rule.enabled:
                continue

            for consumed in rule.consumes_event_type:
                _ensure_event_type_node(graph, consumed)

            for produced in rule.produced_event_types:
                _ensure_event_type_node(graph, produced)
                for consumed in rule.consumes_event_type:
                    graph.add_edge(
                        PipeEdge(
                            from_pipe=consumed,
                            to_pipe=produced,
                            entity=rule.condition_id,
                        )
                    )

        return graph

    def _validate_rule(self, rule: ConditionRule) -> List[str]:
        errors: List[str] = []

        if not rule.condition_id:
            errors.append("condition_id is required")
        if not rule.subject_type:
            errors.append("subject_type is required")
        if not rule.consumes_event_type:
            errors.append("consumes_event_type must contain at least one event type")
        if not isinstance(rule.parameters, Mapping):
            errors.append("parameters must be a mapping")
            return errors

        for key in ("enter_when", "exit_when"):
            expression = rule.parameters.get(key)
            if expression is None or not str(expression).strip():
                errors.append(f"parameters.{key} is required")
                continue
            try:
                self.expression_parser.parse(str(expression))
            except Exception as exc:
                errors.append(f"parameters.{key} is invalid: {exc}")

        return errors


def combine_condition_rules(
    table_rules: Iterable[ConditionRule],
    registry_rules: Iterable[ConditionRule],
    *,
    has_table_engine: bool,
    has_registry_engine: bool,
) -> List[ConditionRule]:
    """Merge the table- and registry-sourced rule sets for one chain.

    Each source validates its own rules in isolation -- table rows through
    ``validate_or_raise``, registry rules at ``condition_engine()``
    declaration time (``registry.py``). Neither ever checks a rule from ONE
    source consuming a produced event type from the OTHER, nor a
    ``condition_id`` collision between the two sources: those are the
    cross-source interactions, and they are checked here so that every
    lowering merging the two sources checks them identically. A lowering
    that merged the sources on its own would silently diverge -- which is
    exactly how the Lakeflow lowering came to drop registry rules entirely.

    Disabled rules are dropped here too. ``validate()`` already excludes
    them from a table-sourced set, but ``get_all_conditions()`` returns
    every registration regardless, and ``execute_rules`` runs whatever it
    is handed -- so without this filter a registry rule declared
    ``enabled=False`` would still emit boundary events and still occupy a
    generation.
    """
    combined = [rule for rule in (list(table_rules) + list(registry_rules)) if rule.enabled]
    if not (has_table_engine and has_registry_engine and combined):
        return combined

    id_counts = Counter(rule.condition_id for rule in combined)
    duplicate_ids = sorted(condition_id for condition_id, count in id_counts.items() if count > 1)
    if duplicate_ids:
        raise ConditionValidationError(
            "Conditions set is not ingestible: condition_id(s) "
            f"{', '.join(duplicate_ids)} are declared in both the table and "
            "registry sources -- a condition_id drives its own "
            "'<condition_id>.entered'/'.exited' boundary event types, so a "
            "collision would produce ambiguous, duplicated events"
        )

    cross_validator = TemporalConditionValidator()
    graph = cross_validator.build_event_type_graph(combined)
    cycles = cross_validator.graph_builder.detect_cycles(graph)
    if cycles:
        raise ConditionValidationError(f"Conditions set is not ingestible:\n{cycles[0]}")

    return combined


def layer_rules_by_generation(
    rules: Iterable[ConditionRule],
) -> Tuple[Dict[int, List[ConditionRule]], int]:
    """Bucket rules by the generation of the boundary events they produce.

    The event-type graph is built over the rules *as given*, so a caller
    holding rules from more than one source must pass the combined set:
    layering a subset assigns any rule consuming an omitted rule's output
    the default generation 1, which in a statically stratified lowering
    misplaces it into the wrong stratum.

    Returns ``(rules_by_generation, max_generation)``; an empty rule set
    yields ``({}, 0)``.
    """
    rules = list(rules)
    if not rules:
        return {}, 0

    validator = TemporalConditionValidator()
    graph = validator.build_event_type_graph(rules)
    layer_by_type = {
        event_type: layer_index
        for layer_index, layer in enumerate(validator.graph_builder.get_generations(graph))
        for event_type in layer
    }

    by_generation: Dict[int, List[ConditionRule]] = {}
    for rule in rules:
        generation = max(
            (layer_by_type.get(produced, 1) for produced in rule.produced_event_types),
            default=1,
        )
        by_generation.setdefault(generation, []).append(rule)

    return by_generation, max(by_generation, default=0)


class _LoggerProvider:
    def get_logger(self, name: str):
        return logging.getLogger(name)


class _NullPipeRegistry(DataPipesRegistry):
    def register_pipe(self, pipeid, **decorator_params):
        raise NotImplementedError("Temporal condition validation builds event-type graphs directly")

    def unregister_pipe(self, pipeid):
        raise NotImplementedError("Temporal condition validation builds event-type graphs directly")

    def get_pipe_ids(self):
        return []

    def get_pipe_definition(self, name):
        return None


def _ensure_event_type_node(graph: PipeGraph, event_type: str) -> None:
    if event_type in graph.nodes:
        return

    graph.add_node(PipeNode(pipe_id=event_type, input_entities=[], output_entity=event_type))


def _row_to_dict(row: Any) -> Dict[str, Any]:
    if isinstance(row, Mapping):
        return dict(row)
    if hasattr(row, "asDict"):
        return row.asDict(recursive=True)
    if hasattr(row, "__dict__"):
        return dict(row.__dict__)
    raise TypeError(f"Unsupported condition row type: {type(row).__name__}")


def _normalize_event_types(value: Any) -> List[str]:
    if value is None:
        return []

    if isinstance(value, str):
        raw_values: Sequence[Any] = [value]
    elif isinstance(value, Sequence):
        raw_values = value
    else:
        raise TypeError("consumes_event_type must be a string or sequence of strings")

    event_types = [str(item).strip() for item in raw_values if str(item).strip()]
    return list(dict.fromkeys(event_types))
