# Kindling temporal-model instructions

Use this file when modelling or processing events, conditions, episodes,
thresholds, or temporal scope. The temporal ontology is independent of normal
Kindling entity/pipe mechanics: use ordinary entities and pipes to carry and
process these domain concepts.

## Ontology

- An event is an immutable, time-stamped fact: something happened at a point in
  time. It carries an event type, event time, stable identity when available,
  and business scope keys.
- A condition is an evaluated state or predicate over data. It has a scope, an
  evaluation time/interval, and a boolean or classified result. It is not an
  episode.
- An episode is a derived, bounded or open time interval representing a business
  situation. It has a stable identity, scope keys, start/end semantics, status,
  and provenance back to the events/conditions that formed it.

Events, conditions, and episodes are entities. A temporal processor is a pipe
or a small connected set of pipes. Do not merge ontology concepts with the
processor that creates them.

## Two ways to form episodes

### Event-delimited episode

Use this when start and end events are first-class business delimiters—not when
a predicate happens to remain true long enough.

~~~text
start event -- opens an episode for its scope
end event   -- closes the matching open episode for its scope
~~~

Define the matching rule explicitly: scope key, event type(s), correlation or
business key, start/end timestamps, tie-break sequence, unmatched-start policy,
unmatched-end policy, duplicate/replay policy, and late/out-of-order policy.
An unclosed episode may be represented as open; do not manufacture an end time.

### Condition-duration episode

Use this when an episode exists because a condition remains true across time.
Define the condition evaluator, scope key, time grain/continuity rule, start
threshold, end/reset rule, allowed gaps, and late-data/recomputation behavior.
For example, "temperature exceeded a threshold continuously for 20 minutes" is
condition-duration logic, not a start/end-event episode.

## Scope and parameter values

Scope belongs in the condition contract: it answers "for which independent
business subject is this state evaluated?" Evaluate a condition per "machine_id",
asset, tenant, patient, account, or another declared key. Every input event
must carry that key; every output condition/episode must retain it.

Do not create one entity per machine merely because thresholds differ. Keep the
condition definition stable and supply business parameter values as data:

~~~text
parameter entity: parameter_name, scope_type, scope_key, effective_from,
                  effective_to, value, unit, version
condition pipe:  events + applicable parameters -> condition results
~~~

This permits per-machine, per-tenant, or global defaults with effective dating,
versioning, and auditability. Define precedence, for example:
machine-specific value -> site value -> global default. Separate entities only
when the contract, security, ownership, retention, or lifecycle differs.

## Temporal data contracts

For every temporal entity, declare fields for:

- scope keys, source/event identity, and correlation key where applicable;
- event/evaluation/start/end timestamps and a deterministic sequence;
- condition value/status or episode status, including "open" and "closed";
- parameter version/value provenance where thresholds apply; and
- source lineage, processing/revision version, and quality state if required.

Use stable keys. An open episode can close later or be revised by late data, so
episode lifecycle commonly needs merge/upsert semantics. State the late-data and
reprocessing policy before choosing append-only versus merge persistence.

## Implementation rules

1. Build a static, acyclic entity/pipe graph. Do not hide temporal state in a
   driver-side loop or import-time action.
2. Keep condition evaluation and episode construction separate when they have
   different contracts; this makes quality, thresholds, and replay testable.
3. Use event time and deterministic sequencing, not processing time, for
   business boundaries unless processing-time semantics are explicit.
4. Test normal sequences, duplicates, missing delimiters, simultaneous events,
   late/out-of-order events, scope isolation, threshold changes, and reruns.
5. Treat revisions, backfills, and parameter changes as controlled operations;
   state whether episodes are recomputed, superseded, or preserved.

Do not assume a temporal helper API exists unless it is installed and already
used by the target application. The ontology is the contract; implement it with
the available Kindling entities, pipes, tags, and tests.
