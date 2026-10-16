# Complete-Set Processing

**Status:** Preliminary. Captures the concept, the contract to settle, and a
recommended first slice from design findings against the code; decisions are
open. No framework code changes.
**Created:** 2026-10-15
**Related:** `episode_analytical_facts.md` and `temporal_event_segmentation.md`
(sets whose boundaries are *derived*; this one's members are *known*),
`declarable_streaming_sources.md` and `event_hub_kafka_transport.md`
(per-item arrival), `obsolete/stage_processor_signal_inversion.md` (signals
a release could emit), `docs/guide/data_entities.md` (watermark-based
incremental reads, which set-scoped reads would parallel).

## Problem

Kindling pipes run on two cadences today, batch (on a schedule or when
something arrives) and streaming:

| Cadence | Runs when | Unit of work |
|---|---|---|
| Batch (time-based) | the schedule says so | whatever arrived since the last run |
| Batch (event-based) | something new arrives | whatever arrived since the last run |
| Streaming | continuously | each micro-batch |

Neither fits data that arrives as a **known, bounded set**. Example: a feed
delivers 200 forecasts every 15 minutes, one per site. Each set should be
processed together, as one bounded dataset, and as soon as all 200 are in.
Not on a timer (too early, the set may be incomplete; or too late, it
waited for the next tick), and not per arrival (each run sees a partial
set).

The trigger here is **completeness**: run because a known set is now
whole. Most real sets are easy to bound: the expected members are known
in advance (a site list, a fixed count, a manifest), so completeness is a
simple membership check rather than an inference.

## Shape

Stream as far as possible; gate only where set semantics start to matter.

1. **Ingest per item, on arrival (bronze).** Land, validate and
   deduplicate each member as it comes in. Nothing is gained by waiting,
   and bad members surface early.
2. **A gate tracks each set and releases it when complete.** Per set it
   records which members have arrived and whether the set is complete,
   released and processed.
3. **Process per set (silver and beyond).** The downstream pipe receives a
   bounded DataFrame holding exactly the released set's rows, processes
   them together, and writes its output keyed by set, replaceable as a
   unit, so reprocessing a set is safe.

## Concepts to define

- **Set key.** What identifies a set, e.g. the forecast issue time (one
  set per 15-minute issue).
- **Member key.** What identifies a member within a set, e.g. `site_id`.
  Completeness counts distinct `(set, member)` pairs, never rows, so a
  re-delivered member does not count twice.
- **Expected membership.** How the gate knows a set is whole. In rough
  order of robustness:
  - an expected-member list (a reference entity such as the site list, or
    a manifest delivered with the set);
  - an end-of-set marker from the source;
  - an expected count (simplest; fragile if membership changes or
    duplicates slip through).
- **Release.** The transition from complete to available downstream.
  Happens exactly once per set revision, including across restarts and
  concurrent checks, so the gate's per-set state must be persisted. How a
  *revised* set reaches consumers that already processed an earlier release
  (after a partial release or late members) is open until the reprocessing
  mechanism is chosen; one option, sketched under later slices, is that a
  reopened set gets a new revision and release number, so set-scoped reads
  return it again.
- **Set-scoped read.** What a downstream pipe consumes: "the set(s)
  released since my last run", analogous to the incremental read that
  watermarks provide today ("changes since version N").
- **Set-keyed write.** Downstream output replaced per set, so a set can be
  reprocessed without duplicating or mixing results.

## Policies (defaults TBD)

The common case needs none of these beyond sensible defaults, but each has
to have a declared answer:

- **Deadline.** If a set is still incomplete after a period: release it
  partial (flagged), keep holding it, or quarantine it. Never wait forever
  silently.
- **Late or revised members after release.** Reopen and reprocess the set
  (safe, given set-keyed writes), or quarantine the late member. This
  decides whether downstream results can change after the fact.
- **Ordering.** Whether a later set may be processed before an earlier
  one that is still incomplete. Independent processing is the likely
  default.
- **Catch-up.** Whether several released sets can be processed in one run
  (after an outage) or strictly one set per run.

## Illustrative declaration

Not a committed syntax; it shows what an entity would need to say:

```yaml
completeness:
  set_key: forecast_issued_at
  members:
    key: site_id
    expected_from: ref.forecast_sites   # or expected_count: 200, or end_marker: ...
  deadline: 10m
  on_deadline: release_partial          # | hold | quarantine
  late_members: reprocess               # | quarantine
```

## How: recommended first slice

Design findings against the current code (2026-10-16). Not final; the
decisions below are open.

### Driver: in-run gate evaluation, not a streaming gate

The gate runs as a **read-time aspect inside an ordinary batch pipe run**,
built like `WatermarkAspect`. The run is started by a schedule or a platform
trigger (e.g. a Databricks file-arrival or table-update trigger on bronze).
Each run evaluates the gate itself, so no set id has to be passed in, which
matters because the SDK deliberately passes no runtime parameters to job runs
(`kindling_sdk/platform_databricks.py:859-876`). A run in which no set is
complete skips cheaply through the executor's existing skip-when-empty.

A streaming gate (running the set-level pipe inside `foreachBatch`) is **not
recommended**: under Spark Connect (Databricks standard/shared access mode and
serverless) the callback runs without dependency injection
(`entity_provider_delta.py:1878-1943`), so a full Kindling pipe can't run
there. If very low latency is needed, a long-running app looping the set pipe
every few seconds gets close with the same code, and also serves as the timer
for deadlines.

Bronze ingest runs separately (streaming ingest, or a batch ingest earlier in
the same run): a plan with any streaming pipe switches entirely to streaming
(`execution_strategy.py:508-552`).

### Gate state: `system.set_gates`

One system entity, defined like the watermarks entity and replaceable through
DI, with one row per (gate, set): `gate_row_id`, `gate_id`, `source_entity_id`,
`set_id`, `status` (`open`, `released`; later `overdue`, `released_partial`,
`quarantined`, `reopened`), `expected_count`, `arrived_count`,
`first_seen_at`, `last_member_at`, `released_at`, `release_seq` (increases per
gate; the consumers' cursor), `release_reason`, `revision`,
`last_execution_id`. Consumer progress reuses `system.watermarks`
(`cursor` = the last `release_seq` consumed).

### Declaration

Completeness describes the data, so it goes on the bronze entity; the
consuming pipe opts in:

```yaml
# bronze entity
set.key: forecast_issued_at
set.member_key: site_id
set.expected_count: "200"        # or set.expected_from: ref.forecast_sites

# set-level pipe
set.read: released
set.max_per_run: "1"             # optional; default: all released sets
```

Registration checks that the driving input carries `set.*` tags and doesn't
also use `use_watermark` (two read handlers would clash: the last one wins
silently, `simple_read_persist_strategy.py:159-168`), that the columns exist,
and that the output entity is derived with `derived.replace_keys` equal to
`set.key`.

### Set-scoped read

For a `set.read: released` pipe, the aspect:
1. finds the sets touched since its last check (`read_changes` on bronze with a
   gate-owned reader);
2. recounts distinct members over all rows of those sets, so duplicates don't
   count twice;
3. compares with the expected count, or anti-joins with the reference entity
   (which also yields the missing members);
4. releases complete sets with a conditional MERGE into `system.set_gates`
   (`WHEN MATCHED AND status = 'open' AND <complete>`), which guarantees each
   set is released once;
5. hands the pipe the rows of released sets past its cursor (or no data, so the
   pipe skips), saving the cursor only after a successful persist and dropping
   it on failure or skip, as the watermark aspect does.

Release is exactly once; consumption is at least once, which the replace
write makes harmless.

### Set-keyed write

Reuse the derived-entity keyed replace (`replaceWhere` on the key,
`entity_provider_delta.py:2213-2294`), with one change: the replace scope must
come from the **released set ids**, not from the output rows. Today the scope
is taken from the output, and an empty output is a no-op, so a reprocessed set
that now yields no rows would keep its old rows. Add an optional replace
scope to `replace_entity`, supplied by the aspect for that persist. Only Delta
implements `replace_entity` today.

### Lakeflow / SDP

The aspect is never registered under SDP, so in the first slice the
declaration engine **rejects** `set.read` pipes rather than letting them read
all of bronze. A later lowering could be a materialized view filtered to
complete sets, without the release log, deadlines or signals.

### Observability

Signals `setgate.evaluated`, `setgate.set_released` (set id, reason, counts,
latency from first and last member) and later `setgate.set_overdue` /
`setgate.set_reopened`; spans gated like the watermark spans. The state table
itself answers "open sets", "past deadline" and "release latency".

### Later slices

- **Deadlines and late members:** `set.deadline`, `set.on_deadline`,
  `set.late_members`, with the transitions above. Arrival triggers never fire
  when nothing arrives, so deadlines need a scheduled run (cron, or the
  polling loop). A set where nothing arrived at all is only detectable with a
  declared cadence (`set.cadence: 15m`) or an expected-sets reference. Partial
  releases add a flag column to the delivered data.
- **Then:** membership that changes over time, composite set keys, strict
  ordering, a CLI view of gate state, and an SDP lowering.

### Sizes

First slice **M** (tags and validation, state entity, the aspect, the
replace-scope change, the SDP rejection, signals, docs, memory-provider unit
tests and one Databricks system test on both classic and Spark Connect
compute). Deadlines and late members **M**. Later items **S–M** each.

## Relationship to existing features

- **Watermarks:** set-scoped reads are the set-granular counterpart of
  version-based incremental reads.
- **Temporal episodes:** also sets, but their boundaries are derived from
  the events. Complete sets are simpler: members are known in advance.
- **Streaming pipes:** bronze ingest can stay a streaming pipe; the gate
  is what turns a stream into bounded units.

## Decisions needed

1. Completeness on the entity (recommended), with an optional pipe-level
   `set.gate_id` when one bronze entity has several gates.
2. Whether the aspect also runs on standalone (unlike watermarks), so local
   tests exercise it.
3. Whether set-scoped reads require a derived output entity, or also allow
   merge mode (which can't remove rows that disappeared from a set).
4. Default catch-up: every released set in one run, or `set.max_per_run: 1`.
5. Whether triggers stay outside Kindling (set up per platform by hand) or job
   deployment learns schedule and file-arrival trigger blocks.
6. Whether SDP rejects `set.read` pipes for now (recommended).
7. How expected membership is declared when it changes over time (a site
   added mid-day).

Unverified: platform trigger behaviour (Databricks file-arrival and
table-update triggers, Fabric and Synapse event triggers) is from general
knowledge, not this repository; check current platform docs. Delta write
conflicts are not retried anywhere in the provider today.

## Non-goals (for now)

- Inferring set boundaries from the data (that is the Temporal episode
  work).
- Cross-entity sets (one set spanning several bronze entities).
