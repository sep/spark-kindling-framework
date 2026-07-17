# kindling-ext-temporal

Temporal event, condition, and episode primitives for Kindling.

This package starts the extension described in
`docs/proposals/temporal_event_segmentation.md`. The first slice provides:

- canonical `silver.events`, `silver.conditions`, and `silver.episodes` entity
  metadata through a DI-overridable `TemporalEntityResolver`;
- `DataEvents` registration for base event normalizers and the generic
  condition engine;
- `DataEpisodes` registration for event-pair episode definitions.
- `TemporalConditionValidator` for per-row `enter_when`/`exit_when` validation
  and event-type graph generation/cycle checks.

Runtime execution and episode materialization will land in later slices.
