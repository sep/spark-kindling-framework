from dataclasses import dataclass
from enum import Enum
from typing import Mapping, Optional

GLOBAL_NAMING_KEY = "kindling.storage.table_naming"
ENTITY_NAMING_TAG = "provider.table_naming"


class TableNamingMode(str, Enum):
    """Declarative strategies for deriving physical table-name leaves."""

    LEGACY = "legacy"
    NORMALIZED = "normalized"
    LEAF = "leaf"


_ALLOWED_MODES = (
    TableNamingMode.LEGACY.value,
    TableNamingMode.NORMALIZED.value,
    TableNamingMode.LEAF.value,
)


def normalize_table_leaf(name: str) -> str:
    """Normalize a logical table identifier into a Spark-compatible leaf."""
    return str(name).replace(".", "_").replace("-", "_")


def _allowed_modes_text() -> str:
    return "'legacy', 'normalized', or 'leaf'"


def parse_table_naming_mode(
    value: object, *, key: str, entity_id: Optional[str] = None
) -> Optional[TableNamingMode]:
    """Parse a configured table naming mode.

    ``None`` and blank strings mean the key is unset. Invalid non-blank
    values raise with the config key, rejected value, allowed vocabulary, and
    entity id when the value came from entity tags.
    """
    if value is None:
        return None

    raw_value = str(value).strip()
    if not raw_value:
        return None

    lowered = raw_value.lower()
    if lowered in _ALLOWED_MODES:
        return TableNamingMode(lowered)

    entity_clause = f" for entity '{entity_id}'" if entity_id is not None else ""
    raise ValueError(
        f"Invalid {key} value {raw_value!r}{entity_clause}; " f"expected {_allowed_modes_text()}."
    )


def derive_table_component(entity_id: str, mode: TableNamingMode) -> str:
    """Derive a physical table-name component from an entity id."""
    if mode == TableNamingMode.LEAF:
        return normalize_table_leaf(str(entity_id).rsplit(".", 1)[-1])
    if mode == TableNamingMode.NORMALIZED:
        return normalize_table_leaf(str(entity_id))
    raise ValueError("legacy table naming mode does not derive a table component")


def sdp_mode_for(mode: Optional[TableNamingMode]) -> str:
    """Project table naming vocabulary onto the SDP dataset naming vocabulary."""
    if mode == TableNamingMode.LEAF:
        return TableNamingMode.LEAF.value
    return TableNamingMode.NORMALIZED.value


@dataclass(frozen=True)
class TableNamingPolicy:
    """Resolve entity-level and global table naming configuration."""

    _global_value: object = None

    @property
    def global_mode(self) -> Optional[TableNamingMode]:
        return parse_table_naming_mode(self._global_value, key=GLOBAL_NAMING_KEY)

    @classmethod
    def from_config_value(cls, value: object) -> "TableNamingPolicy":
        return cls(value)

    def mode_for(
        self, entity_id: str, entity_tags: Optional[Mapping[str, object]]
    ) -> Optional[TableNamingMode]:
        tags = entity_tags or {}
        entity_mode = parse_table_naming_mode(
            tags.get(ENTITY_NAMING_TAG), key=ENTITY_NAMING_TAG, entity_id=entity_id
        )
        if entity_mode is not None:
            return entity_mode
        return self.global_mode

    def component_for(
        self, entity_id: str, entity_tags: Optional[Mapping[str, object]]
    ) -> Optional[str]:
        mode = self.mode_for(entity_id, entity_tags)
        if mode is None or mode == TableNamingMode.LEGACY:
            return None
        return derive_table_component(entity_id, mode)
