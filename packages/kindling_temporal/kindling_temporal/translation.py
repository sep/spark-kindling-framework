"""Lower temporal declarations into ordinary Kindling pipe registrations."""

from typing import TYPE_CHECKING, Any, Dict, Optional

from kindling.data_entities import DataEntityRegistry, EntityMetadata
from kindling.data_pipes import DataPipesRegistry

if TYPE_CHECKING:
    from .registry import BaseEventMetadata


class TemporalPipeTranslator:
    """Translate temporal declarations to native Kindling execution metadata."""

    BASE_EVENT_PIPE_PREFIX = "temporal.event."

    @classmethod
    def base_event_pipe_id(cls, eventid: str) -> str:
        return f"{cls.BASE_EVENT_PIPE_PREFIX}{eventid}"

    @classmethod
    def register_base_event(
        cls,
        metadata: "BaseEventMetadata",
        pipe_registry: DataPipesRegistry,
        entity_registry: Optional[DataEntityRegistry] = None,
        output_entity: Optional[EntityMetadata] = None,
    ) -> str:
        """Register a base event declaration as a normal Kindling pipe."""
        if output_entity is not None and entity_registry is not None:
            cls.ensure_entity(entity_registry, output_entity)

        pipeid = metadata.pipeid or cls.base_event_pipe_id(metadata.eventid)
        pipe_registry.register_pipe(pipeid, **cls.base_event_pipe_params(metadata))
        return pipeid

    @classmethod
    def base_event_pipe_params(cls, metadata: "BaseEventMetadata") -> Dict[str, Any]:
        if not metadata.subject_keys:
            raise ValueError(
                f"Temporal event '{metadata.eventid}' requires at least one subject key"
            )

        return {
            "name": metadata.name or f"Temporal event: {metadata.eventid}",
            "execute": cls.base_event_execute(metadata),
            "tags": {
                **metadata.tags,
                "pipe_type": "temporal.base_event",
                "temporal.kind": "base_event",
                "temporal.event_id": metadata.eventid,
                "temporal.event_type": metadata.event_type,
                "temporal.subject_type": metadata.subject_type,
            },
            "input_entity_ids": [metadata.input_entity_id],
            "output_entity_id": metadata.output_entity_id,
            "output_type": metadata.output_type,
            "use_watermark": metadata.use_watermark,
        }

    @classmethod
    def base_event_execute(cls, metadata: "BaseEventMetadata"):
        """Build the executable function for a lowered base event pipe."""

        def execute(**entity_dfs):
            if not entity_dfs:
                raise ValueError(
                    f"Temporal event pipe '{metadata.eventid}' received no input entity frames"
                )

            source_df = next(iter(entity_dfs.values()))
            event_df = metadata.transform(source_df) if metadata.transform else source_df
            return cls.select_event_envelope(event_df, metadata)

        return execute

    @staticmethod
    def ensure_entity(registry: DataEntityRegistry, entity: EntityMetadata) -> None:
        """Register an entity only when the target registry does not already know it."""
        if registry.get_entity_definition(entity.entityid) is not None:
            return

        registry.register_entity(
            entity.entityid,
            name=entity.name,
            merge_columns=list(entity.merge_columns),
            tags=dict(entity.tags),
            schema=entity.schema,
            partition_columns=list(entity.partition_columns),
            cluster_columns=list(entity.cluster_columns),
            sql=entity.sql,
        )

    @staticmethod
    def select_event_envelope(df, metadata: "BaseEventMetadata"):
        """Project a source dataframe into the canonical event envelope."""
        from pyspark.sql import functions as F

        subject_columns = [
            F.coalesce(F.col(column_name).cast("string"), F.lit(""))
            for column_name in metadata.subject_keys
        ]
        subject_id = F.concat_ws("|", *subject_columns)
        event_id = F.sha2(
            F.concat_ws(
                "||",
                F.lit(metadata.eventid),
                F.lit(metadata.event_type),
                subject_id,
                F.coalesce(F.col(metadata.time_column).cast("string"), F.lit("")),
            ),
            256,
        )

        return df.select(
            event_id.alias("event_id"),
            F.lit(metadata.event_type).alias("event_type"),
            F.lit(0).alias("generation"),
            F.lit("base").alias("event_class"),
            F.lit(metadata.subject_type).alias("subject_type"),
            subject_id.alias("subject_id"),
            F.col(metadata.time_column).cast("timestamp").alias("event_ts"),
            F.lit(metadata.source_system).cast("string").alias("source_system"),
            F.lit(None).cast("string").alias("correlation_id"),
            TemporalPipeTranslator._string_map_expr(metadata.payload_columns).alias("payload"),
            F.lit(None).cast("map<string,string>").alias("attributes"),
            F.current_timestamp().alias("ingested_at"),
        )

    @staticmethod
    def _string_map_expr(column_names):
        from pyspark.sql import functions as F

        if not column_names:
            return F.lit(None).cast("map<string,string>")

        entries = []
        for column_name in column_names:
            entries.extend([F.lit(column_name), F.col(column_name).cast("string")])
        return F.create_map(*entries)
