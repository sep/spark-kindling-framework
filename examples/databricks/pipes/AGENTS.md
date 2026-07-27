# Kindling pipe and validation instructions

Use this file when creating transformations, watermarked/streaming behavior,
SQL views, or signal-based validation. The contracts below are self-contained.

## Pipe declaration and execution

"DataPipes.pipe(...)" is a decorator. Required keyword arguments are "pipeid",
"name", "tags", "input_entity_ids", "output_entity_id", and "output_type".
"use_watermark" is optional and defaults to "False". Its function returns one
DataFrame.

Kindling calls the function with keyword DataFrame arguments whose names are
input IDs with dots changed to underscores. For example, input "bronze.orders"
becomes "bronze_orders". New pipes must use those keyword arguments; only legacy
single-input pipes may use positional fallback.

~~~python
from pyspark.sql import DataFrame
from kindling.data_pipes import DataPipes

@DataPipes.pipe(
    pipeid="silver.clean_orders",
    name="Clean orders",
    input_entity_ids=["bronze.orders"],
    output_entity_id="silver.orders",
    output_type="delta",
    tags={"layer": "silver"},
)
def clean_orders(bronze_orders: DataFrame) -> DataFrame:
    return bronze_orders.filter("order_id IS NOT NULL")
~~~

"output_type" is required metadata and normally "delta" for a Delta output; it
does not replace the output entity's "provider_type". Every input and output
must be registered entities. Let Kindling persist the returned DataFrame; do not
write "save", "writeStream", "MERGE", or Spark actions in an ordinary pipe
unless a documented local extension point requires it.

## Incrementality and streaming

The first input is the driving input. It alone is read incrementally when
"use_watermark=True"; later inputs are full reference reads. Put the
incremental/event input first and make joins deterministic.

Enable watermarks only after defining the event timestamp, late-data tolerance,
replay behavior, and idempotent target semantics. One pipe has one output
entity; split unrelated outputs into separate pipes. Do not compensate for an
unclear entity contract with ad hoc pipe logic.

## Views

"DataPipes.view()" is a session-memory SQL transformation, not a general
virtual-table abstraction. Its required arguments are "pipeid",
"input_entity_ids", "output_entity_id", and one of "sql" or "sql_file"; "name"
and "tags" are optional. "sql_file" is relative to the declaring Python module.

Inputs become Spark temporary views with dots changed to underscores. The helper
sets "pipe_type: view", "provider_type: memory", and "output_type: memory" as
defaults. Use it only for short-lived memory behavior. Use
"DataEntities.sql_entity()" for a persistent catalog view and a normal pure
Python helper for a reusable DataFrame modifier.

## Signals and data quality

Signals are lifecycle hooks, not the quality contract itself. Use
"read.after_read" for input quality and synchronous "persist.before_persist" for
an output gate or deterministic DataFrame repair.

~~~python
from kindling.signaling import DataSignals

@DataSignals.handler("persist.before_persist", priority=10)
def reject_negative_amounts(sender, df, pipe_id, **kwargs):
    if df.filter("amount_cents < 0").limit(1).count():
        raise ValueError(f"{pipe_id}: negative amount")
    return df
~~~

"DataSignals.handler(signal_name, mode='sync', priority=50,
on_error='raise', pipe_id=None)" declares a handler. Synchronous handlers can
return a replacement DataFrame or raise to stop persistence; lower priority
runs earlier. "mode='async'" is fire-and-forget: return values are ignored and
errors are logged, so it cannot veto a write. Returning "None" from a sync
handler leaves the DataFrame unchanged.

For Great Expectations or another quality system, declare and version the suite
and severity explicitly. Run blocking output checks in "persist.before_persist",
input checks in "read.after_read", and record failures, metrics, run IDs, and
quarantine outputs. There is no first-class Great Expectations integration to
assume exists.
