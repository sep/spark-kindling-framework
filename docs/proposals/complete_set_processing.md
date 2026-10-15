# Complete-Set Processing

**Status:** Preliminary. Captures the concept and the contract to settle;
how it is built is deliberately left open. No framework code changes.
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
  Happens exactly once per set, including across restarts and concurrent
  checks, so the gate's per-set state must be persisted.
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

## How (TBD)

Two candidate shapes, to be chosen when this is designed in detail:

- **Stream into the gate, batch out of it.** A streaming query over the
  bronze entity uses `foreachBatch` to update the per-set state and, for
  each set that just became complete, read that set's rows as an ordinary
  DataFrame and run the set-level pipe. Lowest latency for continuous
  sources such as Event Hubs. Spark's windows and watermarks close sets on
  time, not completeness, so they are not the mechanism.
- **Completeness check per arrival, batch run per set.** Each arrival (or
  file drop) triggers a cheap check; a set that becomes complete launches
  a normal batch run with the set id as a parameter, through the existing
  execution path. Simpler; fits file-based arrival.

Either way, the gate's state, set-scoped reads and set-keyed writes are
the same contract; only what drives the gate differs. A `set.released`
signal is a natural extension point for anything else that should react.

## Relationship to existing features

- **Watermarks:** set-scoped reads are the set-granular counterpart of
  version-based incremental reads.
- **Temporal episodes:** also sets, but their boundaries are derived from
  the events. Complete sets are simpler: members are known in advance.
- **Streaming pipes:** bronze ingest can stay a streaming pipe; the gate
  is what turns a stream into bounded units.

## Open questions

1. Where the gate's state lives (a system entity alongside watermarks, or
   per-entity state).
2. Whether the gate is an entity property (declared on bronze) or a pipe
   property (declared on the set-level consumer).
3. How expected membership is declared when it changes over time (a site
   added mid-day).
4. Behaviour on Lakeflow / SDP-executed apps, if any.
5. What observability is needed: open sets, sets past deadline, release
   latency.

## Non-goals (for now)

- Inferring set boundaries from the data (that is the Temporal episode
  work).
- Cross-entity sets (one set spanning several bronze entities).
