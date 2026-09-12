# Cosmos DB Change-Feed Streaming Reads and Config-First Throughput Controls

**Status:** Proposed
**Created:** 2026-08-27
**Scope:** `packages/extensions/kindling_ext_cosmos`
**Related:** `docs/proposals/obsolete/entity_provider_roadmap.md` (Cosmos DB
follow-ups), `docs/proposals/obsolete/config_driven_execution_options.md`
("config dictates, parameters override" principle), `declarable_streaming_sources.md`

## Recommendation

Add two related capabilities to the Cosmos extension in one PR-sized unit,
since both touch the same file and the same new `ConfigService` injection
point:

1. **Change-feed streaming reads** — implement `StreamableEntityProvider` on
   `CosmosEntityProvider`, backed by the connector's `spark.cosmos.changeFeed.*`
   options, so Cosmos becomes a third streaming source alongside Delta (CDF)
   and EventHub.
2. **Config-first throughput controls** — a `kindling.cosmos.*` hierarchical
   config layer that sets safe run-level defaults for the connector's
   built-in Throughput Control feature and for read-side RU exposure
   (partitioning strategy, page size), applied before per-entity
   `provider.option.*` overrides. This is the "anything else needed to stop
   a bulk read from saturating the server" answer: the mechanism already
   exists in the connector, it's just unreachable except as an undocumented,
   opt-in passthrough today.

Both follow the "config dictates, parameters/tags override" principle
already established for `kindling.delta.access_mode` and
`kindling.execution.*` — see Constraints below.

## Motivation

`docs/proposals/obsolete/entity_provider_roadmap.md` marks Cosmos DB as
shipped (PR #171) and calls out change-feed streaming reads as "the
highest-value Cosmos follow-up" and config-first throughput controls as a
named gap. Neither exists today:

- `CosmosEntityProvider` implements `BaseEntityProvider`,
  `WritableEntityProvider`, and `StreamWritableEntityProvider` — not
  `StreamableEntityProvider`. Today only Delta and EventHub can be the
  driving input of a streaming pipe (`pipe_streaming.py`'s
  `is_streamable()` gate).
- Every Cosmos throughput/partitioning knob is reachable only through the
  generic `provider.option.` passthrough in `_extra_connector_options()` —
  undocumented, opt-in per entity, and with no safe defaults. A batch read
  of a whole container with the connector's default partitioning strategy
  and unset page size can draw a large share of provisioned/autoscale RU/s,
  starving other consumers of the same container. This is the concrete
  "does bulk read saturate the server" risk.

## Constraints (from code and existing decisions)

- `StreamableEntityProvider` (`packages/kindling/entity_provider.py`) is a
  single abstract method: `read_entity_as_stream(entity_metadata,
  format=None, options=None) -> DataFrame`. `EventHubEntityProvider` is the
  reference implementation.
- `pipe_streaming.py`'s `SimplePipeStreamStarter.start_pipe_stream()` calls
  `input_provider.read_entity_as_stream(input_entity)` with **no checkpoint
  or options passed on the read side** — checkpointing is entirely a
  write-side concern (`base_checkpoint_path` / `kindling.storage.checkpoint_root`,
  resolved once per pipe run). Source offset/continuation tracking (Kafka
  offsets, Cosmos change-feed continuation tokens) is Spark Structured
  Streaming's own responsibility, persisted in that same checkpoint
  directory's offset log. **This means adding Cosmos as a stream source
  requires zero changes to `pipe_streaming.py`** — implementing the
  interface is sufficient, `is_streamable()` gates on the interface, not a
  hardcoded provider list.
- `kindling.delta.access_mode` pattern (`entity_provider_delta.py.__init__`):
  `self.access_mode = self.config.get("kindling.delta.access_mode") or
  "catalog"` — config resolved once at provider construction via an
  injected `ConfigService`, with platform modules
  (`platform_databricks.py`/`platform_fabric.py`/`platform_synapse.py`)
  setting platform-appropriate defaults before user config overrides.
  `CosmosEntityProvider` has no `ConfigService` injected today — that's the
  one piece of new DI wiring this proposal needs.
- Cosmos connector capabilities not yet surfaced by Kindling
  (`azure-cosmos-spark_3-5_2-12:4.37.2`, per
  `COSMOS_SPARK_CONNECTOR_MAVEN_COORDINATE`):
  - Change feed source options: `spark.cosmos.changeFeed.mode`
    (incremental/latest-version vs. all-versions-and-deletes — exact
    constant names must be confirmed against the pinned connector version
    before implementation, see Open Questions), `spark.cosmos.changeFeed.startFrom`
    (`Beginning` | `Now` | ISO-8601 timestamp | continuation token),
    `spark.cosmos.changeFeed.itemCountPerTriggerHint`.
  - All-versions-and-deletes mode requires the container to have been
    provisioned for it (change feed policy / continuous backup, a
    data-plane setting) — a Kindling config toggle cannot retrofit
    eligibility onto a container that wasn't set up for it, and there is no
    reliable client-side way to check eligibility without duplicating the
    connector's own ARM resolution.
  - Throughput Control: `spark.cosmos.throughputControl.enabled`, `.name`,
    `.targetThroughput` (absolute RU/s) or `.targetThroughputThreshold`
    (fraction of provisioned/autoscale max, e.g. `0.9`), optionally
    `.globalControl.database`/`.container` for cross-job coordination via a
    shared control container.
  - Read tuning: `spark.cosmos.read.partitioning.strategy` (`Default` |
    `Custom` | `Restrictive` | `Aggressive` — `Aggressive` maximizes
    Spark-side parallelism, which maximizes concurrent RU draw),
    `spark.cosmos.read.maxItemCount` (page size per request; connector
    default is already conservative but currently left implicit rather
    than pinned).

## Options considered

### A. Change-feed mode support

1. **`LatestVersion` only** (connector default). Works on any existing
   container with no reconfiguration; matches what "streaming" already
   means for EventHub (no delete visibility there either). Con: silently
   drops deletes, with no error to a consumer that expected CDC-style
   delete propagation.
2. **`AllVersionsAndDeletes` only**. Full fidelity by default. Con: hard
   requirement on container configuration that most existing containers
   won't have; the first read fails with a connector error unless Kindling
   pre-validates (extra surface, and pre-validation would just re-implement
   the connector's own eligibility check).
3. **Both, selected via `provider.changefeed.mode`, default
   `latest_version`** — recommended. Mirrors the write-side pattern already
   in this provider (`ItemOverwrite` default vs. explicit
   `ItemAppend`/`ItemDelete`): safe default, explicit opt-in for the
   sharper edge. Kindling wraps the connector's rejection of `full_fidelity`
   on an ineligible container into a `ValueError` naming the container
   setting to check, rather than surfacing a raw JVM stack trace.

### B. Config-first throughput layer: default posture

1. **Status quo** (per-entity `provider.option.*` only). Rejected — leaves
   the actual problem (undocumented, opt-in, easy to forget) unsolved.
2. **`kindling.cosmos.*` run-level defaults, applied before
   `provider.option.*`, throughput control off by default; partitioning
   strategy and page size get conservative explicit defaults even with no
   config at all** — recommended. Mirrors `kindling.delta.access_mode`:
   framework picks a sane default, config can override per environment,
   per-entity tags win when one entity needs to deviate.
3. **Throughput control enabled by default at some fraction of provisioned
   RU (e.g. 0.9)**. Rejected for v1 — Kindling doesn't currently make an
   ARM call anywhere in this provider to learn the container's actual
   provisioned/autoscale RU/s, and guessing a default threshold risks
   throttling a low-RU dev container to uselessness. Ship the mechanism and
   document it; let ops opt in with a real number.

## Recommended design

1. Add `ConfigService` to `CosmosEntityProvider.__init__` (matches the
   Delta/EventHub pattern).
2. Implement `read_entity_as_stream()`, reusing `_connection_options()` /
   `_extra_connector_options()` / `_stringify_options()` unchanged — only
   the changefeed option block and `spark.readStream` vs. `spark.read`
   differ from the existing `read_entity()`:
   - `provider.changefeed.mode`: `latest_version` (default) |
     `full_fidelity`.
   - `provider.changefeed.start_from`: `Beginning` (default) | `Now` |
     ISO-8601 timestamp.
   - `provider.changefeed.items_per_trigger`: optional.
3. Add a `_throughput_defaults()` block sourced from `kindling.cosmos.*`,
   merged into `_connection_options()` at **lower** precedence than
   `provider.option.*` (which already wins today since it's applied last):

   ```yaml
   kindling:
     cosmos:
       read:
         partitioning_strategy: Default   # Default | Custom | Restrictive | Aggressive
         max_item_count: 100              # explicit; matches connector default, no longer implicit
       throughput_control:
         enabled: false
         target_threshold: null           # e.g. 0.9 of provisioned/autoscale max RU/s
         target_throughput: null          # absolute RU/s, alternative to threshold
         group_name: null                 # spark.cosmos.throughputControl.name
         global_control:
           database: null
           container: null
   ```

4. Document both in the extension README (mirroring its existing
   Configuration/Reads sections) and in `docs/reference/config_reference.md`
   (which already lists `kindling.delta.access_mode`).
5. Mark the roadmap doc's Cosmos follow-up items shipped once merged.

## Implementation sketch

```python
class CosmosEntityProvider(
    BaseEntityProvider, WritableEntityProvider,
    StreamWritableEntityProvider, StreamableEntityProvider,
):
    @inject
    def __init__(self, logger_provider: PythonLoggerProvider, config_service: ConfigService):
        self.logger = logger_provider.get_logger("CosmosEntityProvider")
        self.config_service = config_service

    def read_entity_as_stream(self, entity_metadata, format=None, options=None) -> DataFrame:
        config = self._get_provider_config(entity_metadata)
        if options:
            config = {**config, **options}
        opts = self._build_read_options(entity_metadata, config)
        opts.update(self._changefeed_options(config))
        spark = get_or_create_spark_session()
        reader = spark.readStream.format(format or COSMOS_FORMAT)
        for key, value in opts.items():
            reader = reader.option(key, value)
        return reader.load()

    def _changefeed_options(self, config):
        mode = str(config.get("changefeed.mode", "latest_version")).lower()
        mode_value = {"latest_version": "Incremental", "full_fidelity": "FullFidelity"}.get(mode)
        if mode_value is None:
            raise ValueError(f"Unsupported provider.changefeed.mode '{mode}'")
        opts = {"spark.cosmos.changeFeed.mode": mode_value}
        if config.get("changefeed.start_from"):
            opts["spark.cosmos.changeFeed.startFrom"] = config["changefeed.start_from"]
        if config.get("changefeed.items_per_trigger"):
            opts["spark.cosmos.changeFeed.itemCountPerTriggerHint"] = config["changefeed.items_per_trigger"]
        return self._stringify_options(opts)

    def _connection_options(self, entity_metadata, config):
        options = {...}  # unchanged: endpoint, database, container
        options.update(self._throughput_defaults())        # NEW, lowest precedence
        options.update(self._auth_options(entity_metadata, config))
        options.update(self._extra_connector_options(config))  # unchanged, still wins
        return options

    def _throughput_defaults(self):
        cs = self.config_service
        defaults = {
            "spark.cosmos.read.partitioning.strategy": cs.get(
                "kindling.cosmos.read.partitioning_strategy", "Default"
            ),
            "spark.cosmos.read.maxItemCount": cs.get("kindling.cosmos.read.max_item_count", 100),
        }
        if cs.get("kindling.cosmos.throughput_control.enabled", False):
            defaults["spark.cosmos.throughputControl.enabled"] = True
            defaults["spark.cosmos.throughputControl.name"] = cs.get(
                "kindling.cosmos.throughput_control.group_name"
            )
            threshold = cs.get("kindling.cosmos.throughput_control.target_threshold")
            target = cs.get("kindling.cosmos.throughput_control.target_throughput")
            if threshold:
                defaults["spark.cosmos.throughputControl.targetThroughputThreshold"] = threshold
            if target:
                defaults["spark.cosmos.throughputControl.targetThroughput"] = target
            db = cs.get("kindling.cosmos.throughput_control.global_control.database")
            container = cs.get("kindling.cosmos.throughput_control.global_control.container")
            if db and container:
                defaults["spark.cosmos.throughputControl.globalControl.database"] = db
                defaults["spark.cosmos.throughputControl.globalControl.container"] = container
        return defaults
```

## Files to touch

| File | Change | Reason |
|---|---|---|
| `packages/extensions/kindling_ext_cosmos/kindling_ext_cosmos/entity_provider_cosmos.py` | Add `StreamableEntityProvider`, `ConfigService` injection, `read_entity_as_stream`, `_changefeed_options`, `_throughput_defaults` | Core feature |
| `packages/extensions/kindling_ext_cosmos/README.md` | Document `changefeed.*` tags, `kindling.cosmos.*` config, throughput control | User-facing docs |
| `docs/reference/config_reference.md` | Add `kindling.cosmos.*` entries | Central config reference already lists `kindling.delta.access_mode` |
| `docs/proposals/obsolete/entity_provider_roadmap.md` | Move the Cosmos "Change-feed stream read" and throughput-control follow-ups from open to shipped | Roadmap doc is the decision record for this area |
| `tests/unit/test_cosmos_entity_provider_extension.py` | Unit tests: option-building for each `changefeed.mode`/`start_from`/`items_per_trigger` combination; `_throughput_defaults()` precedence vs. `provider.option.*`; invalid mode raises `ValueError` | Existing suite already tests option-building this way |
| `tests/system/extensions/cosmos/test_cosmos_provider_system.py` | Live change-feed round-trip (write rows, stream-read via change feed, assert); if a full-fidelity-eligible container is available, assert deletes are visible only in that mode | Existing system test already does a live round-trip |
| `CHANGELOG.md` | `## Unreleased` entry | Repo convention |

## Test strategy

- **Unit:** option-dict assertions per `changefeed.mode`/`start_from`/
  `items_per_trigger` combination; `_throughput_defaults()` precedence
  (`kindling.cosmos.*` loses to `provider.option.*`); invalid mode raises
  `ValueError`.
- **System** (live, `poe test-extension --extension cosmos`): start a
  streaming read off the existing dev container, write a batch, assert the
  streamed micro-batch contains it. Cover `full_fidelity` delete-visibility
  live only if `sep-cosmos-dataint-dev` (or a dedicated test container) is
  eligible; otherwise cover it at the unit/option level only and note the
  gap rather than skip silently.

## Open questions

1. **Connector constant names.** `Incremental`/`LatestVersion` vs.
   `FullFidelity`/`AllVersionsAndDeletes` naming has shifted across
   `azure-cosmos-spark` connector versions. Confirm exact values against
   `4.37.2` (the pinned `COSMOS_SPARK_CONNECTOR_MAVEN_COORDINATE`) docs or
   source before implementation — names above are illustrative.
2. **Full-fidelity test coverage.** Does `sep-cosmos-dataint-dev` support
   enabling all-versions-and-deletes without recreating the container? If
   not, that path ships unit-tested only, with live coverage as a tracked
   follow-up once a suitable container exists.
3. **Opinionated throughput-control default.** Should
   `kindling.cosmos.throughput_control.target_threshold` gain a non-null
   default (e.g. `0.9`) once this ships and ops has a real RU number to
   reason about? Left `null` (disabled) for v1 per Option B.3 above;
   revisit after the first environment configures it manually.
