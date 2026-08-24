# Episode-Scoped Analytical Facts from Heterogeneous Event Data

**Status:** Proposed. No framework or application code changes made yet;
this is the design-decision record requested before touching either.
**Created:** 2026-08-20
**Related:** `temporal_event_segmentation.md` (the base episode/event system
this proposal extends — episode membership is sketched there as an
optional, unimplemented section; the typed-payload gap is named there as
an open question), `fan_in_upsert_pipes.md` (evaluates and rejects
multi-writer partial-column fan-in for this exact shape of problem —
referenced below rather than re-derived), `surrogate_keys.md` (the
hash-based deterministic-key pattern this proposal generalizes to the
event/episode/membership grains).

## Problem

Deriving episode/session-level analytical facts from heterogeneous IoT
event data, where:

- raw events arrive from a live message stream, and must also support
  historical backfills from sources that may lack stream offsets or
  other transport metadata;
- events conform to several domain schemas, each with a shared envelope
  (source identity, subject/device identity, event time, component
  identity) plus schema-specific typed attributes;
- a temporal engine derives events and episodes from the source data,
  and a single source event may legitimately participate in multiple
  episodes or episode types;
- consumer-facing facts summarize measures over episode windows:
  averages, counts, durations, first/last values, state transitions;
- the design must avoid repeated time-range joins and JSON parsing in
  every aggregate, and must be replay-safe, backfill-safe, and suitable
  for streaming/declarative execution.

Entity/column names below (`events_typed`, `episode_membership`,
`episode_summary`, `episode_id`, `event_id`, `subject_id`) are
illustrative grain labels, not real application table names —
substitute an app's own naming.

## Recommendation

Adopt three layers as a **pipeline**, not a menu of alternatives — they
are sequential stages, not competing options:

1. **Typed domain projections** on top of the temporal extension's
   existing shared envelope, replacing today's flat, unschematized
   payload map with a per-event-type typed schema.
2. **A persisted episode-membership dataset** applying episode
   boundaries to raw events, keyed many-to-many `(episode_id,
   event_id)` — genuinely net-new; today's engine only pairs boundary
   events, one-to-one, and never touches interior events.
3. **A single aggregate pipe** that reads the membership dataset (joined
   to typed projections) and writes the complete summary row in one
   merge — not a fan-in of partial upserts, not an intermediate
   contribution-table-plus-assembly design.

The reason (3) can be single-writer rather than needing partial-column
fan-in: once (2) exists, every heterogeneous domain's contribution has
already converged into one shared, keyed dataset upstream. Fan-in only
earns its complexity when there is no shared upstream convergence point;
here there is one, so reaching for fan-in would solve a problem this
design doesn't have, while inheriting the problems documented in
`fan_in_upsert_pipes.md` (unsupported at the core-validation level, and
no Lakeflow primitive has both the right semantics and the right
cardinality).

## Evidence: current state

### The shared envelope already exists; typed payload does not

`events_schema()` (`packages/extensions/kindling_ext_temporal/kindling_ext_temporal/entities.py:21-39`)
declares a fixed envelope — `event_id, event_type, generation,
event_class, subject_type, subject_id, event_ts, source_system,
correlation_id, payload, attributes, ingested_at` — where `payload`/
`attributes` are `MapType(StringType(), StringType())`. `BaseEventMetadata`
(`registry.py:31-47`) declares `payload_columns` as a flat `List[str]`,
folded into that one string map by `TemporalPipeTranslator
.select_event_envelope` (`translation.py:415-450`). There is no typed,
per-`event_type` payload schema today. The base proposal names this gap
explicitly: *"The payload has no schema, so semantic drift is silent...
the missing piece is a payload contract per `event_type`"*
(`temporal_event_segmentation.md:396-403`, open question at `:1741-1747`).

### Episode pairing is one-to-one by construction; membership is unimplemented design

`EpisodeRunner._paired_boundaries` (`engine.py:264-443`) joins every
start-event row to candidate end-event rows sharing `subject_type`/
`subject_id` with `end.event_ts >= start.event_ts`, then keeps only the
earliest via `row_number()==1` partitioned by `start.event_id`
(`engine.py:301-315`) — each start pairs to exactly one end. Nothing
filters interior events at all. No persisted membership table exists
anywhere in the extension (`entities.py`, `engine.py`, `chain.py`,
`translation.py`, `registry.py` — zero hits for "membership"). The base
proposal's "Episode Membership" section (`temporal_event_segmentation.md:911-928`)
sketches exactly this schema (`episode_id, event_id, event_ts, offset_ms,
ordinal_in_episode, membership_reason`) but calls it optional and lists
*"Should episode membership be opt-in per episode definition?"* as an
open question (`:1757`) — design-only, not code.

Across distinct episode-type declarations, the same raw event **can**
already be consumed by more than one episode declaration: each
`DataEpisodes.episode()` call independently filters the events entity by
its own `start_event`/`end_event` type strings (`engine.py:274-278`) with
no exclusivity enforced — this is the natural entry point for "one event,
multiple episode types."

### The deterministic-hash identity pattern is already validated, just not shared

`surrogate_keys.md` recommends `sha2(to_json(struct(from_columns)), 256)`
over `uuid()` specifically because "retries, re-pulls, and backfills
produce the same key" (`surrogate_keys.md:33-37, 73-78`) — nondeterminism
"breaks every idempotency property the framework leans on." The temporal
extension already hand-rolls this same pattern three times, independently:
event ID (`translation.py:424-433`), condition boundary event ID
(`engine.py:112-120`), and episode ID (`engine.py:425-432`) — each a
one-off `sha2(concat_ws("||", ...), 256)` expression, not a shared
utility. `packages/kindling/entity_resolution.py` does **not** provide
identity/dedup utilities despite the name — it resolves table names and
storage paths, not row identity.

What the hash pattern doesn't yet solve: "same input → same ID" is
covered, but "corrected input → recognized revision of the same
conceptual event" is not — flagged as open in
`temporal_event_segmentation.md:1732-1740`.

### Watermarking is cursor-based and replay-safe by construction; backfill ingestion is not

`WatermarkManager`/`WatermarkAspect` (`packages/kindling/watermarking.py`)
use an opaque, provider-defined cursor per `(source_entity_id, reader_id)`
(`:53-77`) with no time-based grace period — replay safety comes from
capturing the cursor at read time and only advancing it after a durable
write succeeds (`:139-142, 473-478`); a failed run simply re-reads the
same slice next time. This makes the *runner engine's* incremental reads
at-least-once-safe, provided the destination write is merge-key
idempotent.

File-based backfill ingestion does not inherit this: `discovery="batch"`
(`packages/kindling/file_ingestion.py`) dedups by **moving the source
file after a successful write** (`:352-365`) and writes via plain
`append_to_entity` (`:341`) — not a merge. A re-run against an un-moved
file duplicates rows. `discovery="autoloader"`
(`packages/extensions/kindling_ext_databricks_autoloader/kindling_ext_databricks_autoloader/autoloader_file_ingestion.py:37-55`)
gets real once-only file-identity tracking for free from Databricks Auto
Loader's own checkpoint — a materially stronger backfill-safety guarantee
than the batch path.

### Streaming transport identity is not uniform even across live sources

Event Hub exposes a composite `(partition, offset, sequenceNumber,
enqueuedTime, ...)` transport identity (`packages/kindling/entity_provider_eventhub.py:495-506`),
but the Kafka transport path explicitly nulls out `sequenceNumber,
publisher, partitionKey, properties, systemProperties`
(`entity_provider_eventhub.py:700-712`) — even live-stream identity
richness varies by transport. Backfill/file sources have only
`source_path`/`filename` and an ingestion timestamp
(`file_ingestion.py:200-217, 302, 689`). Canonical event identity
therefore cannot depend on transport metadata being present or uniform;
it must be derived from business-stable fields regardless of source.

### SDP/Lakeflow has no join primitive today, and forbids the revision pattern

No windowed join, stream-stream join, or stateful-processing support
exists anywhere in `kindling_ext_sdp` (`declaration_engine.py`,
`declaration_plan.py`, `oss_engine.py` — zero hits for interval/windowed
joins, `flatMapGroupsWithState`, `withWatermark`). `oss_engine.py`
currently only implements `DatasetType.MATERIALIZED_VIEW`
(`:116-121`); `streaming_table`/append-flow dataset types raise
`NotImplementedError`, deferred to "Phase 4." Separately,
`kindling_ext_sdp` explicitly forbids self-referencing pipes — a pipe
reading its own prior output — via the `self_referencing_pipe` validation
rule (`declaration_engine.py:332-343`, reasoning: "SDP owns
persistence"). `pipe_graph.py:227-230` names exactly this pattern —
*"prior-state feedback (e.g. temporal episode revision)"* — as something
the classic runner engine supports and SDP does not.

**Platform knowledge (not repo evidence — verify against current
Databricks docs before relying on it):** Structured Streaming
stream-stream joins require `withWatermark` on both sides plus an
explicit, bounded time constraint in the join condition; arbitrary
open-ended `BETWEEN` predicates without watermarks are rejected or force
unbounded state retention. Lakeflow materialized-view incrementalization
is not documented as handling arbitrary interval/range joins efficiently
— it's suited to equality-based, monotonically-appending patterns, not
open time ranges. Re-expressing the same interval join independently in
every downstream aggregate multiplies the raw-event scan/shuffle cost by
the number of consumers.

## Comparison: summary-construction options

| | A. Single aggregate pipe | B. Multiple partial upserts (fan-in) | C. Intermediate contributions + assembly |
|---|---|---|---|
| Supported today (core) | Yes — ordinary SCD1 merge, one writer | No — no owned-columns concept; `pipe_graph.py` tracks one producer per entity (`:89`); memory provider destructively replaces full rows on match | Yes per-table, but adds a join layer nobody queries directly |
| Lakeflow/SDP compatible | In principle, but still blocked on the membership join itself (see gaps below) | No — `append_flow` is insert-only; `create_auto_cdc_flow` is restricted to one flow per target by Databricks' own docs | Per-table yes; same join-primitive gap downstream |
| New framework capability required | None for the merge itself; the real gap is the membership dataset (net-new either way) | A full owned-columns/patch-upsert subsystem — largest lift, Delta/memory/Lakeflow parity risk | None new, but an extra table + join layer |
| Replay/backfill safety | Strong — single merge-by-key write | Weak until conflict-detection and concurrency-retry logic is built | Strong per-table, but the assembly join re-pays the range-join cost every rebuild |
| Team independence | Domains stay isolated upstream (typed projection + membership); aggregation only reads | Genuine independent deploy per domain team — the one real advantage over A | Same independence as B, no incremental benefit over A once membership exists |
| Recommendation | **Adopt** | Reject — unsupported and unnecessary given the membership dataset | Reject — extra hop, no benefit |

## Identity/key contract

| Grain | Key strategy | Rationale |
|---|---|---|
| Raw | Preserve source-native identity verbatim as `source_native_id`/`source_offset` — never discard it, but it is **not** the canonical identity | Transport identity varies even across live sources (Event Hub vs. Kafka nulls different fields) and is absent for backfill sources; canonical identity can't depend on it. |
| Typed/canonical event | `event_id = deterministic_hash(event_type, subject_id, event_ts, <domain-stable fields>)` | Already the validated pattern from `surrogate_keys.md` and already used ad hoc in `translation.py`; deterministic hash reproduces the same ID across retries/backfills. |
| Temporal (episode) | `episode_id = deterministic_hash(episode_type, start_event_id)`, anchored on the start boundary, stable across boundary revisions | Already the implemented pattern (`engine.py:425-432`); a revised end boundary must update the same row via merge-by-key, not mint a new episode — needs to become an explicit, tested guarantee. |
| Membership (new) | Natural composite key `(episode_id, event_id)` — no hash needed, both halves already deterministic | Net-new grain; the same `event_id` legitimately recurs under multiple `episode_id`s since episode declarations aren't mutually exclusive over the events entity. |
| Summary | `merge_columns=["episode_id"]`, plain SCD1, one writer | No new identity concept — built from the already-deduplicated membership grain. |

**Revision/supersession:** the hash pattern solves "same input → same ID"
but not "corrected input → recognized revision of the same conceptual
event" (open in `temporal_event_segmentation.md:1732-1740`). Default:
keep hashing on business-identity fields (subject + type + time), so a
correction to a backfilled row naturally lands on the same `event_id`;
treat two genuinely distinct events at an identical subject+type+timestamp
as an accepted, rare collision rather than building a general
revision-pointer model up front.

## Framework gaps / required enhancements

1. **Typed payload contract per `event_type`** — extend
   `BaseEventMetadata`/`payload_columns` from a flat list folded into one
   string map to a real per-`event_type` schema.
2. **Persisted episode-membership entity + engine stage** — net-new
   schema and a new `EpisodeRunner` stage that, per closed episode,
   selects all subject events in `[start, end]` (not just the two
   boundary rows) and writes membership rows.
3. **Shared deterministic-ID helper** — extract the three existing
   ad hoc hash expressions into one reusable utility used consistently
   across raw/typed/temporal/membership/summary grains.
4. **Backfill ingestion must merge, not append** — `discovery="batch"`
   file ingestion dedups by moving the source file, not by key; route
   backfill writes through the same merge-by-`event_id` path live events
   use.
5. **Episode revision requires prior-state read, which SDP forbids** —
   any stage that revises an episode's own prior row must run on the
   classic runner engine, not declaratively.
6. **SDP/Lakeflow has no join primitive today** — `oss_engine.py` only
   supports `MATERIALIZED_VIEW`; streaming_table/append-flow are
   deferred. Run event ingestion → episode pairing → membership
   materialization on the classic runner engine; reserve SDP/Lakeflow for
   simpler downstream consumers, if any.
7. **Fan-in/patch-upsert remains unsupported** (carried over from
   `fan_in_upsert_pipes.md`) — not required by this design, but worth
   stating plainly so it isn't reached for later out of habit.

## Implementation plan

**Framework**

1. Add typed per-`event_type` payload schema declaration to
   `BaseEventMetadata`, replacing the flat string-map fold.
2. Extract the shared `deterministic_id(*cols)` helper; migrate the
   three existing ad hoc call sites onto it.
3. Add the episode-membership schema and the `EpisodeRunner` stage that
   materializes it (many-to-many, keyed `(episode_id, event_id)`).
4. Add explicit test coverage that episode boundary revision updates the
   existing `episode_id` row rather than minting a new one.
5. Fix batch-mode file ingestion to write backfills through merge-by-key
   rather than append+move.

**Application**

6. Declare each domain schema as a typed base-event projection against
   the shared envelope.
7. Declare episode types against the (now-typed) events entity,
   unchanged from today's API.
8. Declare the single aggregate pipe reading the membership dataset and
   typed projections, computing measures (averages, counts, durations,
   first/last values, state transitions) in one pass, merging into the
   summary entity by `episode_id`.

## Tests

- **Identity stability:** the same raw event, replayed (live) or
  re-ingested (backfill, no transport offset), resolves to the same
  `event_id`; a corrected backfill row with the same business-identity
  fields lands on the same ID rather than duplicating.
- **Many-to-many membership:** one event correctly appears in membership
  rows for two different episode types simultaneously, and (if the
  domain allows it) two overlapping instances of the same episode type.
- **Revision behavior:** an episode whose provisional end is later
  superseded by a real end event keeps the same `episode_id`, and
  downstream membership/summary rows reflect the revised boundary
  without duplication.
- **Aggregate correctness:** the summary row matches a hand-computed
  reference over the membership-joined typed events, for each measure
  kind.
- **Backfill/live convergence:** an episode assembled partly from
  live-stream events and partly from backfill events with no natural
  transport ID produces an identical summary row to an all-live
  equivalent.

## Open questions

- Whether episode membership should be opt-in per episode declaration
  (as `temporal_event_segmentation.md:1757` already asks) or the default
  once this capability exists.
- Whether `membership_reason` needs a controlled vocabulary or is a free
  string, given it's meant to explain *why* an event was included (e.g.
  time-range containment vs. an explicit correlation rule).
- Where the `deterministic_id` helper should live — `packages/kindling`
  core (usable by any extension) vs. staying scoped to
  `kindling_ext_temporal` until a second consumer emerges.

## References

- Databricks — platform behavior around Structured Streaming
  stream-stream joins (watermark + bounded time constraint requirement)
  and Lakeflow materialized-view incrementalization is cited above as
  **platform knowledge, not repo evidence** — verify against current
  Databricks documentation before relying on it for implementation
  decisions.
