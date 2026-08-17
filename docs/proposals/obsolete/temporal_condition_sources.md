# Temporal Condition Sources

## Status

Implemented by PR #224 and archived.

## Context

The temporal extension currently uses one concept, Condition, for two
different kinds of content:

1. A condition engine is declared in Python through
   `DataEvents.condition_engine()`.
2. The condition rules evaluated by that engine are loaded from the current
   view of the `silver.conditions` table.

The second path is intentionally rules-as-data. It supports high-cardinality
and frequently changing rules, SCD2 history, ingestion validation, and
quarantine. It is not the right representation for a small set of static
application rules, especially when those rules should use native PySpark
expressions rather than serialized SQL strings.

## Decision

Support two explicit condition sources:

- `table`: rules are loaded from the configured current conditions entity.
- `registry`: rules are declared in application code and held in the temporal
  condition registry.

The source is part of condition-engine metadata. An engine has one source; it
does not silently union registry rules with table rows.

The existing table source remains the default so current applications retain
their behavior:

```python
DataEvents.condition_engine(engineid="default")
# Equivalent to condition_source="table"
```

The explicit form is:

```python
DataEvents.condition_engine(
    engineid="dynamic_conditions",
    condition_source="table",
)
```

## Registry-declared conditions

Condition content should have its own declaration namespace rather than
making `DataEvents.condition()` look like an event normalizer. A proposed API
is:

```python
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from kindling_ext_temporal import DataConditions, DataEvents


def temperature(events: DataFrame) -> Column:
    return events["payload"].getItem("temperature").cast("double")


def overheat_enter(events: DataFrame) -> Column:
    return temperature(events) > F.lit(90)


def overheat_exit(events: DataFrame) -> Column:
    return temperature(events) <= F.lit(90)


DataConditions.register(
    condition_id="condition.system_overheat",
    consumes_event_type=["telemetry.observed"],
    subject_type="machine",
    enter_when=overheat_enter,
    exit_when=overheat_exit,
)

DataEvents.condition_engine(
    engineid="static_conditions",
    condition_source="registry",
)
```

`enter_when` and `exit_when` are predicate builders with the shape
`Callable[[DataFrame], Column]`. The builder is called with the scoped events
DataFrame at execution time. The registry stores the callable, not a
DataFrame-bound `Column`, so declarations do not capture a Spark session or a
particular execution plan.

The registry path is intended for static, application-owned rules. A registry
condition is not ingested, versioned, or quarantined as a row in
`silver.conditions`.

## Table-backed conditions

The table path remains the current rules-as-data interface:

```python
from kindling_ext_temporal import conditions_schema, ingest_conditions


conditions_df = spark.createDataFrame(
    [
        (
            "condition.machine_threshold",
            ["telemetry.observed"],
            "machine",
            {
                "enter_when": "cast(payload['temperature'] as double) > 75",
                "exit_when": "cast(payload['temperature'] as double) <= 75",
            },
            True,
            None,
            None,
        )
    ],
    conditions_schema(),
)

ingest_conditions(conditions_df)

DataEvents.condition_engine(
    engineid="table_conditions",
    condition_source="table",
)
```

The lowered table-backed pipe continues to depend on both the events entity
and the configured current conditions entity. `ingest_conditions()` keeps its
existing validation, SCD2, duplicate detection, graph-cycle detection, and
quarantine behavior.

## Shared execution model

Both sources normalize to the same internal condition representation:

- condition ID;
- consumed event types;
- subject type;
- enabled and business-validity state where applicable;
- an enter predicate;
- an exit predicate.

The condition engine continues to scope events by subject and consumed event
type, evaluate enter and exit predicates, and emit the same canonical boundary
events:

```text
{condition_id}.entered
{condition_id}.exited
```

For table rows, the predicates remain serialized Spark SQL expressions. For
registry definitions, the engine invokes the PySpark predicate builders and
passes their returned `Column` objects to `DataFrame.filter()`.

The registry path must preserve Spark laziness: builders may construct
columns and expressions, but must not call actions, collect data, or mutate
the DataFrame. A builder must return a boolean `Column`; arbitrary joins or
DataFrame-to-DataFrame transformations are outside this predicate API and
need a separate condition-evaluation path.

## Validation and failure behavior

Registry declarations are code and should fail fast rather than be
quarantined:

- required metadata and duplicate condition IDs are validated during
  registration;
- event-type graph cycles are rejected when the registry is validated for an
  engine;
- predicate-builder errors, or a non-`Column` return value, fail the registry
  engine execution clearly.

Table rows retain their current behavior: malformed rows can be quarantined
by ingestion, and invalid table SQL is rejected by the existing validation
pass.

## Mixed-source policy

An engine must specify one source. Applications that need both use two engine
declarations, normally with distinct condition IDs:

```python
DataEvents.condition_engine(
    engineid="static_conditions",
    condition_source="registry",
)
DataEvents.condition_engine(
    engineid="dynamic_conditions",
    condition_source="table",
)
```

Condition IDs must be unique across both engines because they determine the
boundary event types and event IDs. A future `combined` mode could define
explicit precedence and duplicate handling, but implicit merging is unsafe
and is not part of this proposal.

## Compatibility and migration

- Existing calls to `DataEvents.condition_engine()` continue to mean
  table-backed conditions.
- Existing condition entities, schemas, ingestion functions, and current
  views remain unchanged.
- New registry declarations do not require a conditions table or a synthetic
  placeholder row.
- Chain lowering must carry the selected source: registry-backed chains read
  only events, while table-backed chains retain the conditions-current input.
- Episode declarations and downstream consumers continue to consume the same
  boundary event contract.

## Non-goals

This proposal does not add:

- dynamic Python code stored in a table;
- automatic merging of static and dynamic rules;
- arbitrary DataFrame transforms inside a condition predicate;
- a replacement for the existing table-backed rules-as-data model.

## Tracking

Implementation tracking: `kind-ec5.6`.
