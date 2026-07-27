# Kindling signal instructions

Use this file when creating lifecycle hooks for validation, enrichment,
observability, watermark-related behavior, or failure handling. Signals extend
the execution lifecycle; they do not replace entity contracts, pipe
transformations, or explicit quality declarations.

## Signal declaration contract

Declare a handler with:

~~~python
from kindling.signaling import DataSignals

@DataSignals.handler(
    "persist.before_persist",
    mode="sync",
    priority=10,
    on_error="raise",
    pipe_id=None,
)
def validate(sender, df, pipe_id, **kwargs):
    return df
~~~

| Argument | Meaning |
| --- | --- |
| Signal name | The lifecycle event to receive. |
| "mode" | "sync" (default) waits for the handler; "async" is fire-and-forget. |
| "priority" | Synchronous order; lower numbers run first. Default is 50. |
| "on_error" | "raise" (default) stops synchronous execution; "log" records and continues. |
| "pipe_id" | Optional exact pipe-ID scope; "None" applies to every pipe. |

A synchronous handler may return a replacement DataFrame or raise an exception
to stop the operation. Returning "None" leaves the current DataFrame unchanged.
Async handlers ignore return values and log exceptions; they cannot veto a
write.

## Relevant lifecycle stages

| Signal | Use |
| --- | --- |
| "read.after_read" | Inspect, validate, or deterministically enrich an input after it is read. Receives "df", "entity_id", "pipe_id", and "used_watermark". |
| "persist.before_persist" | Last synchronous output gate before the normal write. Receives "df", "pipe_id", "source_entity_id", "output_entity_id", and "persist_id". |
| "persist.after_persist" | Post-write metrics, notifications, and bookkeeping. Do not treat it as a transaction veto point. |
| "persist.persist_failed" | Failure telemetry, cleanup, and operational alerting. |

Use "persist.before_persist" for a blocking output validation. A synchronous
handler that raises prevents the normal persistence path from running; one that
returns a DataFrame changes what is persisted.

## Quality and safety rules

1. Keep expectations explicit and versioned. A signal is an execution hook, not
   the sole record of a business-quality policy.
2. Scope a rule to a pipe when it is output-specific. Use a shared handler only
   for genuinely universal checks.
3. Do not perform accidental full-table actions in a handler. Use a bounded
   existence check such as "filter(...).limit(1).count()" only when a blocking
   validation needs it, and record the cost.
4. A repair/filter handler must be deterministic, declared by policy, and
   observable. Never silently discard production rows.
5. Emit metrics, validation run IDs, failing-rule identifiers, and quarantine
   destinations where policy requires them.
6. Order dependent synchronous handlers deliberately with "priority"; keep
   validation before enrichment only when that is the intended contract.
7. Put slow notifications, telemetry, or noncritical side effects in an async
   handler with failure logging. Do not let a notification receiver decide
   whether a pipe write occurs.

## Great Expectations and other validation engines

There is no first-class Great Expectations integration to assume exists. When
using it or another validation engine:

- declare the suite/configuration and severity as versioned application data;
- use "read.after_read" for input checks and "persist.before_persist" for a
  blocking output gate;
- explicitly classify failures as fail, quarantine, warn, or repair;
- keep persistence control in the synchronous pre-persist path; and
- test pass, warning, quarantine, and blocking-failure behavior.

Do not rely on an asynchronous signal or an after-persist hook to prevent an
invalid output from being written.
