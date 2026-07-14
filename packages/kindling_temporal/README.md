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
- `EpisodeRunner` execution that pairs start/end events into closed episodes;
- unit, integration, and system coverage for the first executable slice.

## Not yet implemented

The remaining proposal work is tracked here so the current package is not
mistaken for a full implementation:

- episode-determination events emitted back into `silver.events`;
- open, expired, and invalidated episode lifecycle handling;
- late-event grace windows, watermarks, replay/backfill semantics, and
  stateful streaming execution;
- multi-generation orchestration beyond one condition-engine pass;
- interval hierarchy and temporal-relation reasoning;
- aggregation, correlation, and inference-derived event paths;
- cloud platform persistence/orchestration coverage beyond the local system
  test path;
- production examples and operator documentation.
