# Temporal Execution Contract

The contract for how the temporal extension (`kindling_ext_temporal`)
processes events into conditions and episodes: ordering, incrementality,
open-episode state, late events, synthetic boundaries, and closure. This
documents the behavior of the implemented bounded engine and states, where
relevant, what the planned streaming driver adds — streaming *execution* is
not yet implemented; per the proposal it is a `foreachBatch` driver around
this same engine, not a second implementation.

## One engine, two drivers

All episode determination is computed by one bounded engine
(`ConditionEngineRunner`, `EpisodeRunner`): a bounded slice of events plus
persisted episode state in, revised episode rows and determination events
out. Today a scheduler drives it in batch mode. The streaming mode in the
proposal drives the identical engine from a Structured Streaming
`foreachBatch` trigger body; nothing in this contract changes between the
two drivers except who supplies the slice.

## Two lowerings, one contract

The declarations lower two ways; both are executed by the ordinary Kindling
engines and both honor this contract:

- **Per-declaration pipes** (one pipe per primitive): generations advance
  one scheduled run per hop, with the events table as the conveyor belt
  between them.
- **Chained pipes** (`declare_temporal_chain()`): one events pipe computes
  all generation strata in memory (feeding determination events back into
  further condition passes until quiescence, capped by
  `kindling.temporal.max_generations`) and persists once; one episodes pipe
  pairs from the persisted events. Higher-order conditions converge in a
  single run; the events and episodes tables each have exactly one writer
  and the graph has no self-reads — the shape declarative engines
  (Lakeflow/SDP) require. Determination events and episode rows derive from
  the same pre-revision prior state, so revision ordering cannot skew them.

## Incrementality ownership — one owner per hop

- The **events driving input** of every temporal pipe is watermarked with
  Kindling's per-reader cursor (pipe input 0, per the runner's
  driving-source convention). Routine runs see only new events. Under the
  chained lowering, only the true boundary inputs carry cursors: the
  chain's driving entity (events pipe) and the events entity (episodes
  pipe); intermediate generations are in-memory strata, never
  persisted-and-re-read.
- **Prior episode state is not a pipe input.** The episode engine resolves
  its own persisted state at execution time (JIT `silver_episodes` keyword →
  `kindling.temporal.revise_persisted` config → provider read guarded by an
  existence check). A first run proceeds with no prior state. The pipes
  carry a `temporal.reads_prior_state` tag so lineage tooling can see the
  feedback loop the pipe graph deliberately does not schedule around.
- Under the planned streaming driver, the stream's checkpoint owns the
  source→events hop and the same per-reader cursors own each downstream
  hop — no double bookkeeping.

## Ordering

- Condition boundary events (`{condition_id}.entered` / `.exited`) are
  emitted at `generation = source generation + 1`. Generations are computed,
  never hand-specified; the event-type graph is validated (cycles rejected)
  before execution.
- Episode pairing is per subject: a start event pairs with the **earliest**
  end event whose `event_ts` is not before the start (ties broken by
  `event_id`).
- `episode_id` is deterministic from the episode definition and the start
  boundary only. End-boundary changes revise lifecycle fields; they never
  change identity.

## Open episode state

A start with no visible end materializes an **open** episode row
(`status = open`, null end fields). Open, expired, and
synthetically-invalidated episodes form the *revisable set*: the engine
reconstructs their start boundaries from the episodes table on later runs so
an incremental batch containing only a late end event can still pair it.

## Synthetic boundaries

With no real end visible, two configured boundaries can close an episode
synthetically, evaluated against the **evaluation time** (resolution order:
per-execution `temporal_evaluation_time` argument →
`kindling.temporal.evaluation_time` config → the bounded input horizon, the
batch's max `event_ts`):

- `expires_after_seconds` → `status = expired`, `close_reason = expiration`.
- `max_duration_seconds` (open past it) → `status = invalidated`,
  `close_reason = max_duration`.

When both are configured, the **earliest synthetic boundary is terminal**:
an episode that expired before its max-duration horizon stays expired in
every later batch view, and vice versa. Closed episodes are also validated
against `min_duration_seconds`/`max_duration_seconds` (real-end
invalidation, `end_event_synthetic = false`).

## Late events

- A late **real end** supersedes a synthetic end: the episode is re-emitted
  with the same `episode_id` as `closed` (or invalidated by duration
  bounds), and the entity's `merge_columns=["episode_id"]` upsert makes the
  re-emit an in-place revision.
- Episodes ended by a **real** end event are terminal. A later-arriving end
  earlier than the accepted one does not re-pair a closed episode
  (documented limitation; grace-window semantics are future work).
- A batch with no new events and no evaluation time emits **nothing** for
  reconstructed episodes: persisted lifecycle state never regresses.
- Duplicate replays of a start event deduplicate against the reconstructed
  state by `event_id`.

## Closure and determination events

Every determination (`closed`, `expired`, `invalidated`) emits an event back
into the canonical events entity: deterministic `event_id`
(type + episode id), `event_class = episode`,
`correlation_id = episode_id`, `event_ts` = the end boundary,
`generation = max(start, end) + 1`. Corrective events (a `.closed` after an
earlier `.expired`) are **appended alongside** the historical ones — the
event log records what was determined and when; the episodes table holds
current lifecycle state.

## Residuals the batch driver does not solve

- **Expiration with zero arrivals**: synthetic boundaries only fire when
  something evaluates the engine. With no new events, run the episode pipes
  on a scheduler tick with an explicit evaluation time (config or
  parameter). The planned streaming driver has the same need
  (empty-trigger ticks).
- **Sub-second latency**: out of contract; the proposal tiers it as an
  opt-in stateful-streaming optimization for specific conditions, not the
  default model.
- **Replay/backfill** (business-time condition filtering, cursor resets) is
  specified in the proposal and not yet implemented.
