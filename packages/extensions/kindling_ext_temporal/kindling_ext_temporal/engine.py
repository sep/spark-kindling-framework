"""Runtime execution for temporal primitives."""

from functools import reduce
from typing import Optional

from .entities import episodes_schema, events_schema
from .validation import ActiveSparkSqlExpressionParser, TemporalConditionValidator


class ConditionEngineRunner:
    """Evaluate rules-as-data Conditions against temporal events."""

    def __init__(self, validator: Optional[TemporalConditionValidator] = None):
        self.validator = validator

    def execute(self, events_df, conditions_df):
        condition_rows = conditions_df.collect()
        validator = self.validator or TemporalConditionValidator(
            expression_parser=ActiveSparkSqlExpressionParser(events_df.sparkSession)
        )
        validation_report = validator.validate_or_raise(condition_rows)
        if not validation_report.valid_rules:
            return events_df.sparkSession.createDataFrame([], events_schema())

        outputs = []
        for rule in validation_report.valid_rules:
            scoped = self._scope_events(events_df, rule)
            outputs.append(
                self._boundary_events(
                    scoped.filter(rule.parameters["enter_when"]),
                    rule,
                    boundary="entered",
                    event_type=rule.entered_event_type,
                )
            )
            outputs.append(
                self._boundary_events(
                    scoped.filter(rule.parameters["exit_when"]),
                    rule,
                    boundary="exited",
                    event_type=rule.exited_event_type,
                )
            )

        return reduce(lambda left, right: left.unionByName(right), outputs)

    @staticmethod
    def _scope_events(events_df, rule):
        from pyspark.sql import functions as F

        scoped = events_df.filter(F.col("subject_type") == F.lit(rule.subject_type)).filter(
            F.col("event_type").isin(rule.consumes_event_type)
        )
        if rule.valid_from is not None:
            scoped = scoped.filter(F.col("event_ts") >= F.lit(rule.valid_from).cast("timestamp"))
        if rule.valid_to is not None:
            scoped = scoped.filter(F.col("event_ts") < F.lit(rule.valid_to).cast("timestamp"))
        return scoped

    @staticmethod
    def _boundary_events(df, rule, *, boundary: str, event_type: str):
        from pyspark.sql import functions as F

        attributes = F.create_map(
            F.lit("condition_id"),
            F.lit(rule.condition_id),
            F.lit("boundary"),
            F.lit(boundary),
            F.lit("source_event_id"),
            F.col("event_id").cast("string"),
        )
        event_id = F.sha2(
            F.concat_ws(
                "||",
                F.lit(rule.condition_id),
                F.lit(boundary),
                F.col("event_id").cast("string"),
            ),
            256,
        )

        return df.select(
            event_id.alias("event_id"),
            F.lit(event_type).alias("event_type"),
            (F.col("generation") + F.lit(1)).cast("int").alias("generation"),
            F.lit("condition").alias("event_class"),
            F.col("subject_type"),
            F.col("subject_id"),
            F.col("event_ts"),
            F.lit("kindling-ext-temporal").alias("source_system"),
            F.lit(rule.condition_id).alias("correlation_id"),
            F.col("payload"),
            attributes.alias("attributes"),
            F.current_timestamp().alias("ingested_at"),
        )


class EpisodeRunner:
    """Form Episodes by pairing start Events with the earliest matching end Event."""

    def execute(self, events_df, episode, evaluation_time=None):
        from pyspark.sql import functions as F

        paired = self._paired_boundaries(events_df, episode, evaluation_time=evaluation_time)

        return paired.select(
            F.col("episode_id"),
            F.lit(episode.episodeid).alias("episode_type"),
            F.lit(episode.condition_id).alias("condition_id"),
            F.lit(None).cast("string").alias("parent_episode_id"),
            F.col("start.subject_type").alias("subject_type"),
            F.col("start.subject_id").alias("subject_id"),
            F.col("start.event_id").alias("start_event_id"),
            F.col("end_event_id"),
            F.col("start.event_ts").alias("start_time"),
            F.col("end_time"),
            F.col("status"),
            F.col("close_reason"),
            F.col("end_event_synthetic"),
            F.col("duration_ms"),
            F.lit(None).cast("long").alias("event_count"),
            F.current_timestamp().alias("created_at"),
            F.current_timestamp().alias("updated_at"),
            self._episode_attributes(episode).alias("attributes"),
        )

    def execute_determination_events(self, events_df, episode, evaluation_time=None):
        from pyspark.sql import functions as F

        paired = self._paired_boundaries(
            events_df, episode, evaluation_time=evaluation_time
        ).filter(F.col("status").isin("closed", "expired", "invalidated"))
        determination_event = episode.determination_event or f"{episode.episodeid}.closed"
        expiration_event = episode.expiration_event or f"{episode.episodeid}.expired"
        invalidation_event = episode.invalidation_event or f"{episode.episodeid}.invalidated"
        emitted_event_type = (
            F.when(F.col("status") == F.lit("expired"), F.lit(expiration_event))
            .when(F.col("status") == F.lit("invalidated"), F.lit(invalidation_event))
            .otherwise(F.lit(determination_event))
        )
        event_id = F.sha2(
            F.concat_ws(
                "||",
                emitted_event_type,
                F.col("episode_id").cast("string"),
            ),
            256,
        )
        payload = F.create_map(
            F.lit("episode_id"),
            F.col("episode_id").cast("string"),
            F.lit("episode_type"),
            F.lit(episode.episodeid),
            F.lit("condition_id"),
            F.lit(episode.condition_id).cast("string"),
            F.lit("status"),
            F.col("status").cast("string"),
            F.lit("close_reason"),
            F.col("close_reason").cast("string"),
            F.lit("start_event_id"),
            F.col("start.event_id").cast("string"),
            F.lit("end_event_id"),
            F.col("end_event_id").cast("string"),
            F.lit("start_time"),
            F.col("start.event_ts").cast("string"),
            F.lit("end_time"),
            F.col("end_time").cast("string"),
            F.lit("duration_ms"),
            F.col("duration_ms").cast("string"),
        )
        attributes = F.create_map(
            F.lit("start_event_type"),
            F.lit(episode.start_event),
            F.lit("end_event_type"),
            F.lit(episode.end_event),
            F.lit("start_generation"),
            F.col("start.generation").cast("string"),
            F.lit("end_generation"),
            F.col("end.generation").cast("string"),
            F.lit("end_event_synthetic"),
            F.col("end_event_synthetic").cast("string"),
        )

        return paired.select(
            event_id.alias("event_id"),
            emitted_event_type.alias("event_type"),
            (
                F.greatest(
                    F.col("start.generation"),
                    F.coalesce(F.col("end.generation"), F.col("start.generation")),
                )
                + F.lit(1)
            )
            .cast("int")
            .alias("generation"),
            F.lit("episode").alias("event_class"),
            F.col("start.subject_type").alias("subject_type"),
            F.col("start.subject_id").alias("subject_id"),
            F.col("end_time").alias("event_ts"),
            F.lit("kindling-temporal").alias("source_system"),
            F.col("episode_id").alias("correlation_id"),
            payload.alias("payload"),
            attributes.alias("attributes"),
            F.current_timestamp().alias("ingested_at"),
        )

    @staticmethod
    def _paired_boundaries(events_df, episode, evaluation_time=None):
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        starts = events_df.filter(F.col("event_type") == F.lit(episode.start_event))
        ends = events_df.filter(F.col("event_type") == F.lit(episode.end_event))
        if episode.subject_type:
            starts = starts.filter(F.col("subject_type") == F.lit(episode.subject_type))
            ends = ends.filter(F.col("subject_type") == F.lit(episode.subject_type))

        starts = starts.alias("start")
        ends = ends.alias("end")
        candidates = starts.join(
            ends,
            on=[
                F.col("start.subject_type") == F.col("end.subject_type"),
                F.col("start.subject_id") == F.col("end.subject_id"),
                F.col("end.event_ts") >= F.col("start.event_ts"),
            ],
            how="left",
        )
        window = Window.partitionBy(F.col("start.event_id")).orderBy(
            F.col("end.event_ts"), F.col("end.event_id")
        )
        paired = candidates.withColumn("__episode_pair_rank", F.row_number().over(window)).filter(
            F.col("__episode_pair_rank") == F.lit(1)
        )
        if evaluation_time is None:
            horizon = events_df.agg(F.max("event_ts").alias("__temporal_evaluation_time"))
            paired = paired.crossJoin(horizon)
        else:
            paired = paired.withColumn(
                "__temporal_evaluation_time",
                F.lit(evaluation_time).cast("timestamp"),
            )

        expiration_time = F.lit(None).cast("timestamp")
        if episode.expires_after_seconds is not None:
            expiration_time = F.from_unixtime(
                F.col("start.event_ts").cast("long") + F.lit(episode.expires_after_seconds)
            ).cast("timestamp")
        max_duration_time = F.lit(None).cast("timestamp")
        if episode.max_duration_seconds is not None:
            max_duration_time = F.from_unixtime(
                F.col("start.event_ts").cast("long") + F.lit(episode.max_duration_seconds)
            ).cast("timestamp")
        is_closed = F.col("end.event_id").isNotNull()
        is_expired = (
            F.lit(episode.expires_after_seconds is not None)
            & F.col("end.event_id").isNull()
            & F.col("__temporal_evaluation_time").isNotNull()
            & (F.col("__temporal_evaluation_time") >= expiration_time)
        )
        is_open_past_max_duration = (
            F.lit(episode.max_duration_seconds is not None)
            & F.col("end.event_id").isNull()
            & F.col("__temporal_evaluation_time").isNotNull()
            & (F.col("__temporal_evaluation_time") >= max_duration_time)
        )
        expiration_end_event_id = F.sha2(
            F.concat_ws(
                "||",
                F.lit(episode.episodeid),
                F.lit("expired"),
                F.col("start.event_id").cast("string"),
                expiration_time.cast("string"),
            ),
            256,
        )
        max_duration_end_event_id = F.sha2(
            F.concat_ws(
                "||",
                F.lit(episode.episodeid),
                F.lit("max_duration"),
                F.col("start.event_id").cast("string"),
                max_duration_time.cast("string"),
            ),
            256,
        )
        end_event_id = (
            F.when(is_closed, F.col("end.event_id"))
            .when(is_open_past_max_duration, max_duration_end_event_id)
            .when(is_expired, expiration_end_event_id)
            .otherwise(F.lit(None).cast("string"))
        )
        end_time = (
            F.when(is_closed, F.col("end.event_ts"))
            .when(is_open_past_max_duration, max_duration_time)
            .when(is_expired, expiration_time)
            .otherwise(F.lit(None).cast("timestamp"))
        )
        duration_ms = F.when(
            end_time.isNotNull(),
            (end_time.cast("long") - F.col("start.event_ts").cast("long")) * F.lit(1000),
        ).otherwise(F.lit(None).cast("long"))
        is_too_short = F.lit(False)
        if episode.min_duration_seconds is not None:
            is_too_short = is_closed & (duration_ms < F.lit(episode.min_duration_seconds * 1000))
        is_too_long = F.lit(False)
        if episode.max_duration_seconds is not None:
            is_too_long = is_closed & (duration_ms > F.lit(episode.max_duration_seconds * 1000))
        is_max_duration_invalidated = is_too_long | is_open_past_max_duration
        is_invalidated = is_too_short | is_max_duration_invalidated
        status = (
            F.when(is_invalidated, F.lit("invalidated"))
            .when(is_closed, F.lit("closed"))
            .when(is_expired, F.lit("expired"))
            .otherwise(F.lit("open"))
        )
        close_reason = (
            F.when(is_too_short, F.lit("min_duration"))
            .when(is_max_duration_invalidated, F.lit("max_duration"))
            .when(is_closed, F.lit("end_event"))
            .when(is_expired, F.lit("expiration"))
            .otherwise(F.lit(None).cast("string"))
        )
        episode_id = F.sha2(
            F.concat_ws(
                "||",
                F.lit(episode.episodeid),
                F.col("start.event_id").cast("string"),
            ),
            256,
        )

        return (
            paired.withColumn("episode_id", episode_id)
            .withColumn("end_event_id", end_event_id)
            .withColumn("end_time", end_time)
            .withColumn("status", status)
            .withColumn("close_reason", close_reason)
            .withColumn("end_event_synthetic", is_expired | is_open_past_max_duration)
            .withColumn("duration_ms", duration_ms.cast("long"))
        )

    @staticmethod
    def _episode_attributes(episode):
        from pyspark.sql import functions as F

        return F.create_map(
            F.lit("start_event_type"),
            F.lit(episode.start_event),
            F.lit("end_event_type"),
            F.lit(episode.end_event),
        )

    @staticmethod
    def empty(spark):
        return spark.createDataFrame([], episodes_schema())
