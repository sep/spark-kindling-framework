"""Lower temporal declarations into ordinary Kindling pipe registrations."""

from typing import TYPE_CHECKING, Any, Dict, Optional

from kindling.data_entities import DataEntityRegistry, EntityMetadata
from kindling.data_pipes import DataPipesRegistry

if TYPE_CHECKING:
    from .registry import BaseEventMetadata, ConditionEngineMetadata, EpisodeMetadata


class TemporalPipeTranslator:
    """Translate temporal declarations to native Kindling execution metadata."""

    BASE_EVENT_PIPE_PREFIX = "temporal.event."
    CONDITION_ENGINE_PIPE_PREFIX = "temporal.condition."
    EPISODE_PIPE_PREFIX = "temporal.episode."
    EPISODE_DETERMINATION_EVENT_PIPE_PREFIX = "temporal.episode_event."
    EVALUATION_TIME_CONFIG_KEY = "kindling.temporal.evaluation_time"
    PRIOR_STATE_CONFIG_KEY = "kindling.temporal.revise_persisted"

    @classmethod
    def base_event_pipe_id(cls, eventid: str) -> str:
        return f"{cls.BASE_EVENT_PIPE_PREFIX}{eventid}"

    @classmethod
    def condition_engine_pipe_id(cls, engineid: str) -> str:
        return f"{cls.CONDITION_ENGINE_PIPE_PREFIX}{engineid}"

    @classmethod
    def episode_pipe_id(cls, episodeid: str) -> str:
        return f"{cls.EPISODE_PIPE_PREFIX}{episodeid}"

    @classmethod
    def episode_determination_event_pipe_id(cls, episodeid: str) -> str:
        return f"{cls.EPISODE_DETERMINATION_EVENT_PIPE_PREFIX}{episodeid}"

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
    def register_condition_engine(
        cls,
        metadata: "ConditionEngineMetadata",
        pipe_registry: DataPipesRegistry,
        entity_registry: Optional[DataEntityRegistry] = None,
        events_entity: Optional[EntityMetadata] = None,
        conditions_entity: Optional[EntityMetadata] = None,
    ) -> str:
        """Register the generic condition engine as a normal Kindling pipe."""
        if entity_registry is not None:
            if events_entity is not None:
                cls.ensure_entity(entity_registry, events_entity)
            if conditions_entity is not None:
                cls.ensure_entity(entity_registry, conditions_entity)

        pipeid = metadata.pipeid or cls.condition_engine_pipe_id(metadata.engineid)
        pipe_registry.register_pipe(pipeid, **cls.condition_engine_pipe_params(metadata))
        return pipeid

    @classmethod
    def register_episode(
        cls,
        metadata: "EpisodeMetadata",
        pipe_registry: DataPipesRegistry,
        entity_registry: Optional[DataEntityRegistry] = None,
        output_entity: Optional[EntityMetadata] = None,
        events_entity: Optional[EntityMetadata] = None,
    ) -> str:
        """Register an episode declaration as a normal Kindling pipe."""
        if entity_registry is not None:
            if output_entity is not None:
                cls.ensure_entity(entity_registry, output_entity)
            if events_entity is not None:
                cls.ensure_entity(entity_registry, events_entity)

        pipeid = metadata.pipeid or cls.episode_pipe_id(metadata.episodeid)
        pipe_registry.register_pipe(pipeid, **cls.episode_pipe_params(metadata))
        return pipeid

    @classmethod
    def register_episode_determination_event(
        cls,
        metadata: "EpisodeMetadata",
        pipe_registry: DataPipesRegistry,
        entity_registry: Optional[DataEntityRegistry] = None,
        events_entity: Optional[EntityMetadata] = None,
    ) -> str:
        """Register the episode-determination event output as a normal pipe."""
        if events_entity is not None and entity_registry is not None:
            cls.ensure_entity(entity_registry, events_entity)

        pipeid = metadata.determination_pipeid or cls.episode_determination_event_pipe_id(
            metadata.episodeid
        )
        pipe_registry.register_pipe(pipeid, **cls.episode_determination_event_pipe_params(metadata))
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
                **(metadata.tags or {}),
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
    def condition_engine_pipe_params(cls, metadata: "ConditionEngineMetadata") -> Dict[str, Any]:
        return {
            "name": metadata.name or f"Temporal condition engine: {metadata.engineid}",
            "execute": cls.condition_engine_execute(metadata),
            "tags": {
                **(metadata.tags or {}),
                "pipe_type": "temporal.condition_engine",
                "temporal.kind": "condition_engine",
                "temporal.engine_id": metadata.engineid,
            },
            "input_entity_ids": [
                metadata.events_entity_id,
                metadata.conditions_current_entity_id,
            ],
            "output_entity_id": metadata.events_entity_id,
            "output_type": metadata.output_type,
            "use_watermark": metadata.use_watermark,
        }

    @classmethod
    def episode_pipe_params(cls, metadata: "EpisodeMetadata") -> Dict[str, Any]:
        return {
            "name": metadata.name or f"Temporal episode: {metadata.episodeid}",
            "execute": cls.episode_execute(metadata),
            "tags": {
                **(metadata.tags or {}),
                "pipe_type": "temporal.episode",
                "temporal.kind": "episode",
                "temporal.episode_id": metadata.episodeid,
                "temporal.start_event": metadata.start_event,
                "temporal.end_event": metadata.end_event,
                "temporal.reads_prior_state": "true",
            },
            "input_entity_ids": [metadata.events_entity_id],
            "output_entity_id": metadata.output_entity_id,
            "output_type": metadata.output_type,
            "use_watermark": metadata.use_watermark,
        }

    @classmethod
    def episode_determination_event_pipe_params(cls, metadata: "EpisodeMetadata") -> Dict[str, Any]:
        return {
            "name": f"Temporal episode event: {metadata.episodeid}",
            "execute": cls.episode_determination_event_execute(metadata),
            "tags": {
                **(metadata.tags or {}),
                "pipe_type": "temporal.episode_event",
                "temporal.kind": "episode_event",
                "temporal.episode_id": metadata.episodeid,
                "temporal.event_type": metadata.determination_event,
                "temporal.expiration_event_type": metadata.expiration_event,
                "temporal.invalidation_event_type": metadata.invalidation_event,
                "temporal.start_event": metadata.start_event,
                "temporal.end_event": metadata.end_event,
                "temporal.reads_prior_state": "true",
            },
            "input_entity_ids": [metadata.events_entity_id],
            "output_entity_id": metadata.events_entity_id,
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

    @classmethod
    def episode_execute(cls, metadata: "EpisodeMetadata"):
        """Build the executable function for a lowered episode pipe."""

        def execute(**entity_dfs):
            events_key = metadata.events_entity_id.replace(".", "_")
            try:
                events_df = entity_dfs[events_key]
            except KeyError as exc:
                available = ", ".join(sorted(entity_dfs.keys()))
                raise ValueError(
                    f"Temporal episode '{metadata.episodeid}' expected input "
                    f"'{events_key}', got: {available}"
                ) from exc

            from .engine import EpisodeRunner

            return EpisodeRunner().execute(
                events_df,
                metadata,
                evaluation_time=cls.resolve_evaluation_time(entity_dfs),
                existing_episodes_df=cls.resolve_prior_episodes(entity_dfs, metadata),
            )

        return execute

    @classmethod
    def resolve_prior_episodes(cls, entity_dfs: Dict[str, Any], metadata: "EpisodeMetadata") -> Any:
        """Resolve the persisted episode state an episode-family pipe revises.

        Prior state is an execution-strategy concern owned by this extension,
        not a declared dataflow dependency — the pipe graph never schedules a
        pipe ahead of its own previous run. Resolution order: a JIT keyword
        argument named after the episodes entity (tests, manual runs) wins;
        the `kindling.temporal.revise_persisted` config key can disable the
        read; otherwise the episodes entity is read through its provider when
        it exists. Returns None (no prior state) when disabled, unresolvable,
        or not yet created — the runner treats None as a pure batch view.
        """
        episodes_key = metadata.output_entity_id.replace(".", "_")
        if episodes_key in entity_dfs:
            return entity_dfs[episodes_key]
        try:
            from kindling.injection import GlobalInjector
            from kindling.spark_config import ConfigService

            enabled = GlobalInjector.get(ConfigService).get(cls.PRIOR_STATE_CONFIG_KEY, True)
            if str(enabled).strip().lower() in ("false", "0", "no", "off"):
                return None
        except Exception:
            pass
        try:
            from kindling.data_entities import DataEntityRegistry
            from kindling.entity_provider_registry import EntityProviderRegistry
            from kindling.injection import GlobalInjector

            entity = GlobalInjector.get(DataEntityRegistry).get_entity_definition(
                metadata.output_entity_id
            )
            if entity is None:
                return None
            provider = GlobalInjector.get(EntityProviderRegistry).get_provider_for_entity(entity)
            if provider is None or not provider.check_entity_exists(entity):
                return None
            return provider.read_entity(entity)
        except Exception:
            return None

    @classmethod
    def resolve_evaluation_time(cls, entity_dfs: Dict[str, Any]) -> Any:
        """Resolve the batch evaluation time for synthetic episode boundaries.

        A per-execution `temporal_evaluation_time` keyword argument wins;
        otherwise the `kindling.temporal.evaluation_time` config key is
        consulted. Returns None (bounded input horizon) when neither is set or
        no ConfigService is bound.
        """
        if "temporal_evaluation_time" in entity_dfs:
            return entity_dfs["temporal_evaluation_time"]
        try:
            from kindling.injection import GlobalInjector
            from kindling.spark_config import ConfigService

            return GlobalInjector.get(ConfigService).get(cls.EVALUATION_TIME_CONFIG_KEY, None)
        except Exception:
            return None

    @classmethod
    def episode_determination_event_execute(cls, metadata: "EpisodeMetadata"):
        """Build the executable function for episode-determination event output."""

        def execute(**entity_dfs):
            events_key = metadata.events_entity_id.replace(".", "_")
            try:
                events_df = entity_dfs[events_key]
            except KeyError as exc:
                available = ", ".join(sorted(entity_dfs.keys()))
                raise ValueError(
                    f"Temporal episode event '{metadata.episodeid}' expected input "
                    f"'{events_key}', got: {available}"
                ) from exc

            from .engine import EpisodeRunner

            return EpisodeRunner().execute_determination_events(
                events_df,
                metadata,
                evaluation_time=cls.resolve_evaluation_time(entity_dfs),
                existing_episodes_df=cls.resolve_prior_episodes(entity_dfs, metadata),
            )

        return execute

    @classmethod
    def condition_engine_execute(cls, metadata: "ConditionEngineMetadata"):
        """Build the executable function for the lowered condition engine."""

        def execute(**entity_dfs):
            events_key = metadata.events_entity_id.replace(".", "_")
            conditions_key = metadata.conditions_current_entity_id.replace(".", "_")
            try:
                events_df = entity_dfs[events_key]
                conditions_df = entity_dfs[conditions_key]
            except KeyError as exc:
                available = ", ".join(sorted(entity_dfs.keys()))
                raise ValueError(
                    f"Temporal condition engine '{metadata.engineid}' expected "
                    f"inputs '{events_key}' and '{conditions_key}', got: {available}"
                ) from exc

            from .engine import ConditionEngineRunner

            return ConditionEngineRunner().execute(events_df, conditions_df)

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
            tags=dict(entity.tags or {}),
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
