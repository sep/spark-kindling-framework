# kindling-temporal

Temporal event, condition, and episode primitives for Kindling.

This package implements the first executable slice of the extension described
in `docs/proposals/temporal_event_segmentation.md`. It supports the core
Event -> Condition boundary Events -> closed Episode path, but it is not yet the
complete temporal-processing system described in the white paper and proposal.

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
  explicit evaluation time or bounded input horizon;
- episode-determination events emitted back into the canonical event envelope
  with `correlation_id = episode_id` and incremented generation numbers,
  including expiration events for expired episodes;
- unit, integration, and system coverage for the first executable slice.

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

- invalidated episode lifecycle handling and late real-end revision of expired
  episodes;
- late-event grace windows, watermarks, replay/backfill semantics, and
  stateful streaming execution;
- multi-generation orchestration beyond one condition-engine pass;
- interval hierarchy and temporal-relation reasoning;
- aggregation, correlation, and inference-derived event paths;
- cloud platform persistence/orchestration coverage beyond the local system
  test path;
- production examples and operator documentation.
