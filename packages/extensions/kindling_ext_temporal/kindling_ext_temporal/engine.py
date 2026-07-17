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
    """Form closed Episodes by pairing start and end Events."""

    def execute(self, events_df, episode):
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
            how="inner",
        )
        window = Window.partitionBy(F.col("start.event_id")).orderBy(
            F.col("end.event_ts"), F.col("end.event_id")
        )
        paired = candidates.withColumn("__episode_pair_rank", F.row_number().over(window)).filter(
            F.col("__episode_pair_rank") == F.lit(1)
        )

        duration_ms = (
            F.col("end.event_ts").cast("long") - F.col("start.event_ts").cast("long")
        ) * F.lit(1000)
        episode_id = F.sha2(
            F.concat_ws(
                "||",
                F.lit(episode.episodeid),
                F.col("start.event_id").cast("string"),
                F.col("end.event_id").cast("string"),
            ),
            256,
        )
        attributes = F.create_map(
            F.lit("start_event_type"),
            F.lit(episode.start_event),
            F.lit("end_event_type"),
            F.lit(episode.end_event),
        )

        return paired.select(
            episode_id.alias("episode_id"),
            F.lit(episode.episodeid).alias("episode_type"),
            F.lit(episode.condition_id).alias("condition_id"),
            F.lit(None).cast("string").alias("parent_episode_id"),
            F.col("start.subject_type").alias("subject_type"),
            F.col("start.subject_id").alias("subject_id"),
            F.col("start.event_id").alias("start_event_id"),
            F.col("end.event_id").alias("end_event_id"),
            F.col("start.event_ts").alias("start_time"),
            F.col("end.event_ts").alias("end_time"),
            F.lit("closed").alias("status"),
            F.lit("end_event").alias("close_reason"),
            F.lit(False).alias("end_event_synthetic"),
            duration_ms.cast("long").alias("duration_ms"),
            F.lit(None).cast("long").alias("event_count"),
            F.current_timestamp().alias("created_at"),
            F.current_timestamp().alias("updated_at"),
            attributes.alias("attributes"),
        )

    @staticmethod
    def empty(spark):
        return spark.createDataFrame([], episodes_schema())
