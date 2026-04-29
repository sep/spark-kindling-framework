"""Read-only provider for SCD2 current-row companion entities."""

from typing import Tuple

from injector import inject
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit

from .data_entities import EntityMetadata, scd_config_from_tags
from .entity_provider import BaseEntityProvider
from .injection import GlobalInjector
from .spark_log_provider import PythonLoggerProvider


@GlobalInjector.singleton_autobind()
class CurrentViewEntityProvider(BaseEntityProvider):
    """Read-only provider that filters an SCD2 base entity to current rows."""

    @inject
    def __init__(self, logger_provider: PythonLoggerProvider):
        self.logger = logger_provider.get_logger("CurrentViewEntityProvider")

    def _resolve_base(
        self, entity_metadata: EntityMetadata
    ) -> Tuple[EntityMetadata, BaseEntityProvider]:
        base_entity_id = (entity_metadata.tags or {}).get("scd.companion_of")
        if not base_entity_id:
            raise ValueError(
                f"Entity '{entity_metadata.entityid}' is missing required "
                "scd.companion_of tag for current_view provider"
            )

        from .data_entities import DataEntityManager
        from .entity_provider_registry import EntityProviderRegistry

        entity_manager = GlobalInjector.get(DataEntityManager)
        provider_registry = GlobalInjector.get(EntityProviderRegistry)
        base_entity = entity_manager.get_entity_definition(base_entity_id)
        if base_entity is None:
            raise ValueError(
                f"Current view entity '{entity_metadata.entityid}' references "
                f"unknown base entity '{base_entity_id}'"
            )

        return base_entity, provider_registry.get_provider_for_entity(base_entity)

    # [implementer] add SCD2 current companion provider — TASK-20260429-001
    def read_entity(self, entity_metadata: EntityMetadata) -> DataFrame:
        """Read the base SCD2 entity filtered to current rows."""
        base_entity, base_provider = self._resolve_base(entity_metadata)
        cfg = scd_config_from_tags(base_entity)
        return base_provider.read_entity(base_entity).filter(
            col(cfg.is_current_column) == lit(True)
        )

    def check_entity_exists(self, entity_metadata: EntityMetadata) -> bool:
        """Delegate existence checks to the base entity provider."""
        base_entity, base_provider = self._resolve_base(entity_metadata)
        return base_provider.check_entity_exists(base_entity)

    def merge_to_entity(self, df: DataFrame, entity_metadata: EntityMetadata) -> None:
        """Reject writes to read-only current-view companion entities."""
        raise NotImplementedError(
            "current_view entities are read-only; write to the base SCD2 entity"
        )

    def write_to_entity(self, df: DataFrame, entity_metadata: EntityMetadata) -> None:
        """Reject writes to read-only current-view companion entities."""
        raise NotImplementedError(
            "current_view entities are read-only; write to the base SCD2 entity"
        )

    def append_to_entity(self, df: DataFrame, entity_metadata: EntityMetadata) -> None:
        """Reject writes to read-only current-view companion entities."""
        raise NotImplementedError(
            "current_view entities are read-only; write to the base SCD2 entity"
        )
