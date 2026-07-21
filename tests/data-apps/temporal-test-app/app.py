#!/usr/bin/env python3
"""
Temporal Extension Test App

Validates the kindling-ext-temporal batch contract on a real platform
(Delta storage, real providers, real watermark cursors) across TWO separate
job executions sharing one test_id-scoped storage root:

  Scenario run1 — end-to-end flow + open/expired seed (Experiment 1)
    1. ingest_conditions: one valid rule merges, one malformed rule is
       quarantined per row (real quarantine table write)
    2. telemetry -> base events -> condition engine -> episodes ->
       determination events, executed through the framework pipe runner
    3. machine-hot closes with a real end event; machine-late has a start
       only and expires synthetically at the configured
       kindling.temporal.evaluation_time
    4. first-run tolerance: the episode engine's prior-state read must not
       fail when the episodes table does not exist yet

  Scenario run2 — cross-run late-end revision (Experiment 2) + gold
    1. only the late real end telemetry for machine-late is ingested
    2. the engine reconstructs the persisted expired episode (engine-owned
       prior-state read across processes) and revises it in place to closed
       with end_event_synthetic=false, same episode_id
    3. the corrective .closed determination event is appended alongside the
       historical .expired event
    4. gold interval-join aggregation (Experiment 3): avg temperature per
       closed episode window

The scenario is selected by the `temporal_test_scenario` config value the
system test passes per job; the evaluation time comes from
`kindling.temporal.evaluation_time` config, exercising the documented
config path (not the per-execution kwarg).

Each step emits a TEST_ID= marker so the system test harness can validate
individual outcomes from streamed stdout.
"""

import sys
from datetime import datetime

from kindling.data_entities import DataEntities, DataEntityRegistry, EntityMetadata
from kindling.data_pipes import DataPipes, DataPipesExecution
from kindling.entity_provider_registry import EntityProviderRegistry
from kindling.injection import GlobalInjector, get_kindling_service
from kindling.platform_provider import PlatformServiceProvider
from kindling.spark_config import ConfigService
from kindling.spark_log_provider import SparkLoggerProvider
from kindling.spark_session import get_or_create_spark_session
from kindling_ext_temporal import (
    DataEpisodes,
    DataEvents,
    condition_quarantine_schema,
    conditions_schema,
    declare_temporal_chain,
    ingest_conditions,
)
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------

logger = get_kindling_service(SparkLoggerProvider).get_logger("temporal-test-app")
config_service = get_kindling_service(ConfigService)

test_id = str(config_service.get("test_id") or "unknown").replace("-", "_")
scenario = str(config_service.get("temporal_test_scenario") or "run1").strip().lower()

platform_service = get_kindling_service(PlatformServiceProvider).get_service()
platform_name = platform_service.get_platform_name() if platform_service else "unknown"

msg = f"TEST_ID={test_id} status=STARTED platform={platform_name} scenario={scenario}"
logger.info(msg)
print(msg, flush=True)

spark = get_or_create_spark_session()

table_root = config_service.get("kindling.storage.table_root") or "Tables"

# ---------------------------------------------------------------------------
# Entities — the shipped SimpleTemporalEntityResolver supplies the canonical
# temporal entities. All tables are catalog-managed: the system test passes
# kindling.storage.table_catalog / table_schema / table_name_prefix so every
# entity (including system.watermarks) lands in a UC-supported, test-scoped
# namespace. Delta tables at /Volumes paths are not durable on UC shared
# clusters, so no provider.path tags here.
# ---------------------------------------------------------------------------

TELEMETRY_SCHEMA = StructType(
    [
        StructField("machine_id", StringType(), False),
        StructField("reading_ts", TimestampType(), False),
        StructField("temperature", DoubleType(), False),
    ]
)

QUARANTINE_SCHEMA = StructType(
    list(condition_quarantine_schema().fields)
    + [StructField("quarantined_at", TimestampType(), True)]
)

DataEntities.entity(
    entityid="bronze.telemetry",
    name="telemetry",
    merge_columns=["machine_id", "reading_ts"],
    tags={"provider_type": "delta"},
    schema=TELEMETRY_SCHEMA,
    partition_columns=[],
)

DataEntities.entity(
    entityid="silver.conditions.quarantine",
    name="conditions_quarantine",
    merge_columns=[],
    tags={"provider_type": "delta"},
    schema=QUARANTINE_SCHEMA,
    partition_columns=[],
)

GOLD_SCHEMA = StructType(
    [
        StructField("episode_id", StringType(), False),
        StructField("machine_id", StringType(), False),
        StructField("start_time", TimestampType(), False),
        StructField("end_time", TimestampType(), True),
        StructField("avg_temperature", DoubleType(), True),
        StructField("reading_count", StringType(), True),
    ]
)

DataEntities.entity(
    entityid="gold.thermal_excursions",
    name="thermal_excursions",
    merge_columns=["episode_id"],
    tags={"provider_type": "delta"},
    schema=GOLD_SCHEMA,
    partition_columns=[],
)

# ---------------------------------------------------------------------------
# Temporal declarations — telemetry -> events -> condition -> episode
# ---------------------------------------------------------------------------


@DataEvents.base_event(
    eventid="telemetry.base",
    input_entity_id="bronze.telemetry",
    subject_type="machine",
    subject_keys=["machine_id"],
    time_column="reading_ts",
    event_type="telemetry.observed",
    payload_columns=["temperature"],
    source_system="system-test",
    use_watermark=True,
)
def normalize_telemetry(df):
    return df


DataEvents.condition_engine(engineid="default")

EPISODE_ID = "episode.temperature_high_active"

DataEpisodes.episode(
    episodeid=EPISODE_ID,
    start_event="condition.temperature_high.entered",
    end_event="condition.temperature_high.exited",
    subject_type="machine",
    expires_after_seconds=300,
)


@DataPipes.pipe(
    pipeid="gold.thermal_excursions",
    name="Thermal excursions",
    input_entity_ids=["silver.episodes", "bronze.telemetry"],
    output_entity_id="gold.thermal_excursions",
    output_type="delta",
    tags={},
    use_watermark=False,
)
def thermal_excursions(silver_episodes, bronze_telemetry):
    episodes = silver_episodes.filter(
        (F.col("episode_type") == EPISODE_ID) & (F.col("status") == "closed")
    ).select("episode_id", "subject_id", "start_time", "end_time")
    return (
        episodes.join(
            bronze_telemetry,
            (episodes.subject_id == bronze_telemetry.machine_id)
            & (bronze_telemetry.reading_ts >= episodes.start_time)
            & (bronze_telemetry.reading_ts <= episodes.end_time),
        )
        .groupBy("episode_id", "machine_id", "start_time", "end_time")
        .agg(
            F.avg("temperature").alias("avg_temperature"),
            F.count("*").cast("string").alias("reading_count"),
        )
    )


TEMPORAL_PIPES = [
    "temporal.event.telemetry.base",
    "temporal.condition.default",
    f"temporal.episode.{EPISODE_ID}",
    f"temporal.episode_event.{EPISODE_ID}",
]

# The chained lowering: the same declarations as two composite pipes
# (events chain + episodes), selected per run by temporal_execution_mode.
CHAIN_PIPES = declare_temporal_chain()
execution_mode = str(config_service.get("temporal_execution_mode", "pipes")).strip().lower()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

T0 = datetime(2026, 7, 14, 12, 0, 0)
T_END = datetime(2026, 7, 14, 12, 10, 0)
EXPIRED_END = datetime(2026, 7, 14, 12, 5, 0)  # T0 + expires_after_seconds

test_results = {}


def _pass(test_name: str) -> None:
    test_results[test_name] = True
    line = f"TEST_ID={test_id} test={test_name} status=PASSED"
    logger.info(line)
    print(line, flush=True)


def _fail(test_name: str, reason: str) -> None:
    test_results[test_name] = False
    line = f"TEST_ID={test_id} test={test_name} status=FAILED reason={reason}"
    logger.error(line)
    print(line, flush=True)


def _check(test_name: str, condition: bool, reason: str = "") -> None:
    if condition:
        _pass(test_name)
    else:
        _fail(test_name, (reason or "condition was False").replace("\n", " | "))


def _step(test_name: str, fn) -> None:
    """Run one assertion step; an exception fails this step only."""
    try:
        fn()
    except Exception as exc:  # noqa: BLE001 - keep later steps observable
        import traceback

        traceback.print_exc()
        _fail(test_name, f"exception: {exc}".replace("\n", " | "))


def _diag(label: str, value) -> None:
    print(f"DIAG {label}: {value}", flush=True)


def _entity(entity_id: str) -> EntityMetadata:
    return GlobalInjector.get(DataEntityRegistry).get_entity_definition(entity_id)


def _read(entity_id: str):
    entity = _entity(entity_id)
    return (
        GlobalInjector.get(EntityProviderRegistry)
        .get_provider_for_entity(entity)
        .read_entity(entity)
    )


def _append(entity_id: str, df) -> None:
    entity = _entity(entity_id)
    GlobalInjector.get(EntityProviderRegistry).get_provider_for_entity(entity).append_to_entity(
        df, entity
    )


def _run_temporal_pipes() -> None:
    pipes = CHAIN_PIPES if execution_mode == "chain" else TEMPORAL_PIPES
    _diag("execution mode", {"mode": execution_mode, "pipes": pipes})
    get_kindling_service(DataPipesExecution).run_datapipes(pipes)


def _ensure_tables() -> None:
    """Pre-create Delta tables empty (idempotent), mirroring the migration
    pattern core apps use. Watermarked first reads treat a table whose
    creation commit already contains data (version 0) as having no new
    data, so seed/pipe writes must land at version >= 1.

    The conditions entity is deliberately excluded: its table is created by
    the SCD2 merge inside ingest_conditions, which owns its bookkeeping
    columns.
    """
    for entity_id in [
        "bronze.telemetry",
        "silver.events",
        "silver.episodes",
        "silver.conditions.quarantine",
        "gold.thermal_excursions",
    ]:
        entity = _entity(entity_id)
        provider = GlobalInjector.get(EntityProviderRegistry).get_provider_for_entity(entity)
        provider.ensure_entity_table(entity)


def _episode_rows(subject_id: str):
    return (
        _read("silver.episodes")
        .filter((F.col("episode_type") == EPISODE_ID) & (F.col("subject_id") == subject_id))
        .collect()
    )


def _determination_events(subject_id: str):
    return (
        _read("silver.events")
        .filter((F.col("event_class") == "episode") & (F.col("subject_id") == subject_id))
        .collect()
    )


def _diag_tables() -> None:
    def dump(entity_id, columns):
        try:
            rows = _read(entity_id).select(*columns).collect()
            _diag(entity_id, [tuple(row) for row in rows])
        except Exception as exc:  # noqa: BLE001 - diagnostics only
            _diag(entity_id, f"unreadable: {exc}")

    dump("silver.events", ["event_type", "generation", "event_class", "subject_id", "event_ts"])
    dump(
        "silver.episodes",
        ["episode_type", "subject_id", "status", "close_reason", "end_event_synthetic"],
    )


# ---------------------------------------------------------------------------
# Scenario run1 — Experiment 1: end-to-end flow on real storage
# ---------------------------------------------------------------------------


def scenario_run1():
    # Rules-as-data ingestion: one valid rule, one malformed rule.
    conditions_df = spark.createDataFrame(
        [
            (
                "condition.temperature_high",
                ["telemetry.observed"],
                "machine",
                {
                    "enter_when": "cast(payload['temperature'] as double) > 90",
                    "exit_when": "cast(payload['temperature'] as double) <= 90",
                },
                True,
                datetime(2026, 7, 14, 11, 0, 0),
                None,
            ),
            (
                "condition.bad_rule",
                ["telemetry.observed"],
                "machine",
                {
                    "enter_when": "this is ! not (valid sql",
                    "exit_when": "false",
                },
                True,
                datetime(2026, 7, 14, 11, 0, 0),
                None,
            ),
        ],
        conditions_schema(),
    )
    result = ingest_conditions(conditions_df)
    _diag(
        "ingestion result",
        {
            "ingested_count": result.ingested_count,
            "quarantined": [(q.condition_id, q.errors) for q in result.quarantined],
            "quarantine_entity_id": result.quarantine_entity_id,
        },
    )
    _check(
        "run1_conditions_ingested",
        result.ingested_count == 1,
        f"expected 1 ingested, got {result.ingested_count}",
    )
    _check(
        "run1_conditions_quarantined",
        len(result.quarantined) == 1 and result.quarantined[0].condition_id == "condition.bad_rule",
        f"expected condition.bad_rule quarantined, got "
        f"{[(q.condition_id, q.errors) for q in result.quarantined]}",
    )

    def check_quarantine_persisted():
        quarantine_rows = _read("silver.conditions.quarantine").collect()
        _diag("quarantine rows", [(r.condition_id, r.errors) for r in quarantine_rows])
        _check(
            "run1_quarantine_persisted",
            len(quarantine_rows) == 1 and quarantine_rows[0].condition_id == "condition.bad_rule",
            f"expected 1 persisted quarantine row, got {len(quarantine_rows)}",
        )

    _step("run1_quarantine_persisted", check_quarantine_persisted)

    def check_current_view():
        current_rows = _read("silver.conditions.current").collect()
        _diag("conditions current rows", [r.condition_id for r in current_rows])
        _check(
            "run1_conditions_current_view",
            len(current_rows) == 1 and current_rows[0].condition_id == "condition.temperature_high",
            f"expected temperature_high current, got {[r.condition_id for r in current_rows]}",
        )

    _step("run1_conditions_current_view", check_current_view)

    # Telemetry: machine-hot has start+end in-batch; machine-late start only.
    _append(
        "bronze.telemetry",
        spark.createDataFrame(
            [
                ("machine-hot", T0, 95.0),
                ("machine-hot", T_END, 80.0),
                ("machine-late", T0, 95.0),
            ],
            TELEMETRY_SCHEMA,
        ),
    )

    # First-run tolerance: episodes table does not exist yet; the engine's
    # prior-state read must proceed with no prior state instead of failing.
    def run_pipes_first_time():
        _run_temporal_pipes()
        _pass("run1_first_run_pipes_executed")

    _step("run1_first_run_pipes_executed", run_pipes_first_time)

    _diag_tables()

    def check_hot_closed():
        hot = _episode_rows("machine-hot")
        _check(
            "run1_hot_episode_closed",
            len(hot) == 1
            and hot[0].status == "closed"
            and hot[0].close_reason == "end_event"
            and hot[0].end_event_synthetic is False
            and hot[0].duration_ms == 600000,
            f"machine-hot rows: {[(r.status, r.close_reason, r.duration_ms) for r in hot]}",
        )

    _step("run1_hot_episode_closed", check_hot_closed)

    def check_late_expired():
        late = _episode_rows("machine-late")
        _check(
            "run1_late_episode_expired",
            len(late) == 1
            and late[0].status == "expired"
            and late[0].close_reason == "expiration"
            and late[0].end_event_synthetic is True
            and late[0].end_time == EXPIRED_END,
            f"machine-late rows: {[(r.status, r.close_reason, r.end_time) for r in late]}",
        )

    _step("run1_late_episode_expired", check_late_expired)

    def check_hot_determination():
        hot = _episode_rows("machine-hot")
        hot_events = _determination_events("machine-hot")
        _check(
            "run1_hot_determination_event",
            len(hot_events) == 1
            and hot_events[0].event_type == f"{EPISODE_ID}.closed"
            and hot_events[0].generation == 2
            and len(hot) == 1
            and hot_events[0].correlation_id == hot[0].episode_id,
            f"machine-hot determination events: "
            f"{[(e.event_type, e.generation) for e in hot_events]}",
        )

    _step("run1_hot_determination_event", check_hot_determination)

    def check_late_expiration_event():
        late = _episode_rows("machine-late")
        late_events = _determination_events("machine-late")
        _check(
            "run1_late_expiration_event",
            len(late_events) == 1
            and late_events[0].event_type == f"{EPISODE_ID}.expired"
            and len(late) == 1
            and late_events[0].correlation_id == late[0].episode_id,
            f"machine-late determination events: {[e.event_type for e in late_events]}",
        )

    _step("run1_late_expiration_event", check_late_expiration_event)


# ---------------------------------------------------------------------------
# Scenario run2 — Experiment 2: cross-run late-end revision, Experiment 3: gold
# ---------------------------------------------------------------------------


def scenario_run2():
    # Only the late real end arrives in this process; everything else is
    # persisted state from the run1 job execution.
    _append(
        "bronze.telemetry",
        spark.createDataFrame([("machine-late", T_END, 80.0)], TELEMETRY_SCHEMA),
    )

    expired_before = _episode_rows("machine-late")
    _check(
        "run2_sees_persisted_expired_episode",
        len(expired_before) == 1 and expired_before[0].status == "expired",
        f"expected persisted expired episode, got {[(r.status,) for r in expired_before]}",
    )

    def run_pipes():
        _run_temporal_pipes()
        _pass("run2_pipes_executed")

    _step("run2_pipes_executed", run_pipes)

    _diag_tables()

    def check_revised_in_place():
        late = _episode_rows("machine-late")
        _check(
            "run2_late_episode_revised_in_place",
            len(late) == 1
            and late[0].status == "closed"
            and late[0].close_reason == "end_event"
            and late[0].end_event_synthetic is False
            and late[0].end_time == T_END
            and late[0].duration_ms == 600000,
            f"machine-late rows: "
            f"{[(r.status, r.close_reason, r.end_time, r.duration_ms) for r in late]}",
        )
        _check(
            "run2_same_episode_id",
            len(late) == 1
            and len(expired_before) == 1
            and late[0].episode_id == expired_before[0].episode_id
            and late[0].start_event_id == expired_before[0].start_event_id,
            "revised episode did not keep its identity",
        )

    _step("run2_late_episode_revised_in_place", check_revised_in_place)

    def check_corrective_event():
        late = _episode_rows("machine-late")
        late_events = _determination_events("machine-late")
        event_types = sorted(e.event_type for e in late_events)
        _check(
            "run2_corrective_event_appended",
            event_types == [f"{EPISODE_ID}.closed", f"{EPISODE_ID}.expired"]
            and len(late) == 1
            and all(e.correlation_id == late[0].episode_id for e in late_events),
            f"expected historical expired + corrective closed, got {event_types}",
        )

    _step("run2_corrective_event_appended", check_corrective_event)

    def check_hot_untouched():
        hot = _episode_rows("machine-hot")
        _check(
            "run2_hot_episode_untouched",
            len(hot) == 1 and hot[0].status == "closed" and hot[0].end_event_synthetic is False,
            f"machine-hot rows: {[(r.status,) for r in hot]}",
        )

    _step("run2_hot_episode_untouched", check_hot_untouched)

    # Experiment 3: gold interval-join aggregation after revision.
    def check_gold():
        get_kindling_service(DataPipesExecution).run_datapipes(["gold.thermal_excursions"])
        gold = {row.machine_id: row for row in _read("gold.thermal_excursions").collect()}
        _diag(
            "gold rows",
            [(m, r.avg_temperature, r.reading_count) for m, r in gold.items()],
        )
        _check(
            "run2_gold_aggregation",
            set(gold) == {"machine-hot", "machine-late"}
            and all(abs(row.avg_temperature - 87.5) < 1e-9 for row in gold.values())
            and all(row.reading_count == "2" for row in gold.values()),
            f"gold rows: {[(m, r.avg_temperature, r.reading_count) for m, r in gold.items()]}",
        )

    _step("run2_gold_aggregation", check_gold)


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

_diag(
    "config",
    {
        "temporal_test_scenario": config_service.get("temporal_test_scenario", None),
        "kindling.temporal.evaluation_time": config_service.get(
            "kindling.temporal.evaluation_time", None
        ),
        "kindling.temporal.conditions.quarantine_entity_id": config_service.get(
            "kindling.temporal.conditions.quarantine_entity_id", None
        ),
        "kindling.storage.table_root": table_root,
        "kindling.storage.table_catalog": config_service.get(
            "kindling.storage.table_catalog", None
        ),
        "kindling.storage.table_schema": config_service.get("kindling.storage.table_schema", None),
        "kindling.storage.table_name_prefix": config_service.get(
            "kindling.storage.table_name_prefix", None
        ),
    },
)

try:
    _ensure_tables()
    if scenario == "run2":
        scenario_run2()
    else:
        scenario_run1()
except Exception as exc:  # noqa: BLE001 - report and exit nonzero
    import traceback

    traceback.print_exc()
    _fail(f"{scenario}_overall", str(exc))

overall = all(test_results.values()) if test_results else False
overall_status = "PASSED" if overall else "FAILED"
failed = [name for name, ok in test_results.items() if not ok]

if scenario == "run2" and overall:
    # The tables are prefix-scoped catalog tables the storage cleanup path
    # does not cover; drop them once the full lifecycle has been asserted.
    # A failed run keeps them for debugging.
    from kindling.data_entities import EntityNameMapper

    name_mapper = GlobalInjector.get(EntityNameMapper)
    for entity_id in [
        "bronze.telemetry",
        "silver.events",
        "silver.conditions",
        "silver.episodes",
        "silver.conditions.quarantine",
        "gold.thermal_excursions",
        "system.watermarks",
    ]:
        try:
            entity = _entity(entity_id)
            table_name = name_mapper.get_table_name(
                entity if entity is not None else type("E", (), {"entityid": entity_id})()
            )
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
            _diag("dropped", table_name)
        except Exception as exc:  # noqa: BLE001 - cleanup is best-effort
            _diag("drop failed", f"{entity_id}: {exc}")

msg = f"TEST_ID={test_id} status=COMPLETED result={overall_status}"
if failed:
    msg += f" failed_tests={','.join(failed)}"
logger.info(msg)
print(msg, flush=True)

sys.exit(0 if overall else 1)
