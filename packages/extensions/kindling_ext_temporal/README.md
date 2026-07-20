# kindling-ext-temporal

Temporal event, condition, and episode primitives for Kindling.

This package implements the first executable slice of the extension described
in `docs/proposals/temporal_event_segmentation.md`. It supports the core
Event -> Condition boundary Events -> closed Episode path, but it is not yet the
complete temporal-processing system described in the white paper and proposal.

Operator docs: the execution/streaming contract is specified in
`docs/guide/temporal_streaming_contract.md`; a worked telemetry-to-gold
walkthrough is in `docs/guide/temporal_end_to_end.md`.

## Implemented

- canonical `silver.events`, `silver.conditions`, and `silver.episodes` entity
  metadata through a DI-overridable `TemporalEntityResolver`;
- `DataEvents` registration for base event normalizers and the generic
  condition engine;
- `DataEpisodes` registration for event-pair episode definitions;
- base-event lowering into the canonical event envelope;
- `TemporalConditionValidator` for per-row `enter_when`/`exit_when` validation
  and event-type graph generation/cycle checks;
- `ConditionEngineRunner` execution that emits `{condition_id}.entered` and
  `{condition_id}.exited` events with incremented generation numbers;
- `EpisodeRunner` execution that pairs start/end events into closed episodes and
  materializes open episodes when no end event has arrived;
- batch expiration of open episodes using `expires_after_seconds` and an
  explicit evaluation time (execution parameter or
  `kindling.temporal.evaluation_time` config) or bounded input horizon;
- bounded batch correction where a visible real end event wins over synthetic
  expiration while preserving the same `episode_id`;
- episode invalidation for configured `min_duration_seconds` and
  `max_duration_seconds` bounds, including synthetic invalidation of open
  episodes that pass their maximum duration without a real end event; when
  both expiration and max duration are configured, the earliest synthetic
  boundary is terminal;
- episode-determination events emitted back into the canonical event envelope
  with `correlation_id = episode_id` and incremented generation numbers,
  including expiration events for expired episodes and invalidation events for
  invalidated episodes;
- stateful late real-end revision of persisted episodes: the episode engine
  resolves its own prior state at execution time (on by default, disabled via
  `kindling.temporal.revise_persisted`), reconstructs start boundaries for
  persisted open, expired, and synthetically-invalidated episodes, and
  re-emits them with the same `episode_id` when a late real end event
  arrives — the entity's `merge_columns=["episode_id"]` upsert turns the
  re-emit into an in-place revision; a batch with no new events and no
  evaluation time emits nothing for reconstructed episodes so persisted state
  never regresses;
- validated conditions ingestion: `ingest_conditions` validates rule rows per
  row (Spark SQL expression parsing, event-type graph cycle rejection,
  duplicate `condition_id`s within a batch), quarantines rejects (returned,
  and appended to
  `kindling.temporal.conditions.quarantine_entity_id` when configured), and
  upserts the well-formed rows — including disabled ones — through the
  conditions entity's SCD2 merge; `validated_conditions_transform` gates a
  `FileIngestion` file-drop entry with the same validation, rejecting a file
  whole on any invalid row;
- unit, integration, and system coverage for the first executable slice,
  including a Databricks system test (`tests/system/extensions/temporal/`,
  `tests/data-apps/temporal-test-app`) that validates the end-to-end flow,
  validated conditions ingestion with per-row quarantine, and cross-run
  late-end revision (two separate job executions over shared catalog
  storage) on a real workspace.

## Configuration

- `kindling.temporal.evaluation_time` — optional explicit evaluation time for
  synthetic episode boundaries (expiration and max-duration invalidation) in
  batch views. A per-execution `temporal_evaluation_time` keyword argument to
  a temporal pipe's `execute` overrides it. When neither is set, the bounded
  input horizon (the batch's maximum `event_ts`) is used.
- `kindling.temporal.revise_persisted` — set to `false` to disable the prior
  episode-state read entirely; episode pipes then compute a pure batch view.
  Defaults to enabled.
- `kindling.temporal.conditions.quarantine_entity_id` — optional entity id for
  rejected condition rows; when set, `ingest_conditions` appends quarantined
  rows (condition id, errors, raw row, timestamp) there. Unset: rejects are
  only returned in the ingestion result.

## Revision semantics

Episodes ended by a real end event are terminal: revision reconsiders only
persisted episodes whose end is synthetic (expired, invalidated past max
duration) or absent (open). A late-arriving end event that is earlier than an
already-accepted real end does not reopen or re-pair a closed episode. Prior
determination events (for example an expiration event later superseded by a
real close) remain in `silver.events` as history; the corrective event is
emitted alongside them with its own deterministic `event_id`.

Prior state is an execution-strategy concern this extension owns, not a
declared dataflow dependency: episode pipes declare only the events entity as
input, and the engine resolves persisted episode state itself at execution
time — a keyword argument named after the episodes entity (tests, manual
runs) wins, the `kindling.temporal.revise_persisted` config key can disable
the read, and otherwise the episodes entity is read through its provider when
it exists. A first run therefore proceeds with no prior state instead of
failing on a read of the table it is about to create. The pipes carry a
`temporal.reads_prior_state` tag so lineage tooling can see the feedback
loop the pipe graph deliberately does not schedule around. (A pipe that does
declare its own output as an input — the determination pipe writes and reads
`silver.events` — is prior-state feedback too; `PipeGraphBuilder` skips such
self-edges instead of reporting a cycle.)

Known limitations:

- episode rows persisted before the schema carried `start_generation` are
  reconstructed at generation 1; determination events for that legacy cohort
  can understate generation, and revision writes the fallback back to the
  row;
- revision re-emits refresh `created_at` (the upsert updates all columns), so
  a revised episode does not retain its original creation timestamp.

## Lifecycle identity

`episode_id` is deterministic from the episode definition and start boundary,
not from the eventual end boundary. That lets a materialized open episode keep
the same identity when a bounded batch view later marks it expired or closes it
with a real end event. End-boundary changes update lifecycle fields such as
`end_event_id`, `end_time`, `status`, `close_reason`, and `duration_ms`; they
do not create a different episode identity.

## Not yet implemented

The remaining proposal work is tracked here so the current package is not
mistaken for a full implementation:

- revision of episodes closed by a real end event (a later-arriving end
  earlier than the accepted one never re-pairs a closed episode);
- late-event grace windows, watermarks, replay/backfill semantics, and
  stateful streaming execution;
- multi-generation orchestration beyond one condition-engine pass;
- interval hierarchy and temporal-relation reasoning;
- aggregation, correlation, and inference-derived event paths;
- cloud platform coverage beyond Databricks (the Fabric/Synapse legs of the
  platform system test);
- a `kindling conditions set/remove` CLI on top of `ingest_conditions`;
- a runnable example data app packaging the end-to-end guide walkthrough.
