# Temporal End-to-End: Telemetry to Gold

One worked example through the temporal extension: raw telemetry becomes
canonical events, a rules-as-data condition segments them, an episode pairs
the boundaries, determination events feed back for higher-order conditions,
and an ordinary gold pipe curates the business mart. Every block uses the
shipped API; the execution semantics are specified in
[temporal_streaming_contract.md](temporal_streaming_contract.md).

The scenario: machines report temperature; we want one gold row per
*thermal excursion* — a contiguous period a machine ran hot — with its
duration and disposition.

## 1. Normalize telemetry into canonical events

Base events lower raw rows into the canonical `silver.events` envelope. The
output entity is always the resolver's events entity; the declaration lowers
to a normal Kindling pipe (`temporal.event.telemetry.base`).

```python
from kindling_ext_temporal import DataEvents

@DataEvents.base_event(
    eventid="telemetry.base",
    input_entity_id="bronze.telemetry",
    subject_type="machine",
    subject_keys=["machine_id"],
    time_column="reading_ts",
    event_type="telemetry.observed",
    payload_columns=["temperature"],
    use_watermark=True,
)
def normalize(df):
    return df  # already one reading per row
```

## 2. Author the condition as data and ingest it validated

Conditions are rows, not code. `enter_when`/`exit_when` are Spark SQL
expressions over the event envelope — bounded by Catalyst's grammar, not
`eval()`. Ingestion validates per row and quarantines rejects; well-formed
rows (including disabled ones) upsert through the entity's SCD2 merge.

```python
from kindling_ext_temporal import ingest_conditions

result = ingest_conditions(conditions_df)  # schema: conditions_schema()
# result.ingested_count, result.quarantined, result.quarantine_entity_id
```

A condition row for the running example:

```text
condition_id:        condition.temperature_high
consumes_event_type: [telemetry.observed]
subject_type:        machine
parameters:          enter_when = cast(payload['temperature'] as double) > 90
                     exit_when  = cast(payload['temperature'] as double) <= 90
enabled:             true
```

Set `kindling.temporal.conditions.quarantine_entity_id` to persist rejects;
duplicate `condition_id`s within a batch are quarantined too. For file-drop
ingestion, gate the `FileIngestion` entry with
`validated_conditions_transform` (whole-file rejection).

Register the generic engine once — it reads all current rules from the
conditions current view and emits `condition.temperature_high.entered` /
`.exited` boundary events at generation N+1:

```python
DataEvents.condition_engine(engineid="default")
```

## 3. Declare the episode

```python
from kindling_ext_temporal import DataEpisodes

DataEpisodes.episode(
    episodeid="episode.temperature_high_active",
    start_event="condition.temperature_high.entered",
    end_event="condition.temperature_high.exited",
    subject_type="machine",
    expires_after_seconds=8 * 3600,   # synthetic expiration for stale opens
    min_duration_seconds=60,          # blips invalidate, not close
)
```

This registers two pipes: `temporal.episode.episode.temperature_high_active`
(pairs boundaries into `silver.episodes` — open, closed, expired, or
invalidated, upserted by deterministic `episode_id`) and
`temporal.episode_event.episode.temperature_high_active` (emits
`episode.temperature_high_active.closed` / `.expired` / `.invalidated`
determination events back into `silver.events`, `correlation_id =
episode_id`). Late real ends revise persisted episodes in place — see the
execution contract.

## 4. Optional: conditions over determination events

Determination events are ordinary events at the next generation, so a
higher-order condition can consume them — no joins, no special casing:

```text
condition_id:        condition.thermal_excursion_confirmed
consumes_event_type: [episode.temperature_high_active.closed]
parameters:          enter_when = payload['status'] = 'closed'
                     exit_when  = false
```

## 5. Curate gold with an ordinary pipe

The silver/gold boundary is mechanical: gold is business-specific curation
over the generic episodes table, written as a plain Kindling pipe.

```python
from kindling.data_pipes import DataPipes
from pyspark.sql import functions as F

@DataPipes.pipe(
    pipeid="gold.thermal_excursions",
    name="Thermal excursions",
    input_entity_ids=["silver.episodes"],
    output_entity_id="gold.thermal_excursions",
    output_type="delta",
    tags={},
    use_watermark=True,
)
def thermal_excursions(silver_episodes):
    return (
        silver_episodes.filter(
            (F.col("episode_type") == "episode.temperature_high_active")
            & F.col("status").isin("closed", "expired")
        )
        .select(
            F.col("episode_id"),
            F.col("subject_id").alias("machine_id"),
            F.col("start_time"),
            F.col("end_time"),
            (F.col("duration_ms") / 60000).alias("duration_minutes"),
            F.col("close_reason"),
        )
    )
```

## 6. Run it

The declarations above lower to normal pipes. The recommended execution
path is the chained lowering — after all declarations, lower them into two
composite pipes and run those:

```python
from kindling_ext_temporal import declare_temporal_chain

chain_pipes = declare_temporal_chain()   # events chain + episodes
run_datapipes(chain_pipes + ["gold.thermal_excursions"])
```

One run then covers every generation: base events → condition boundaries →
episodes → determination events (fed back into higher-order conditions
within the same run) → gold. Alternatively execute the per-declaration
pipes individually (`run_datapipes`/DAG orchestration) — generation
layering then advances one scheduled run per hop. Two config keys matter
operationally: `kindling.temporal.evaluation_time` (explicit batch
evaluation time for synthetic boundaries — supply it on scheduler ticks so
stale opens expire even with no arrivals) and
`kindling.temporal.revise_persisted` (prior-state read switch, on by
default).
