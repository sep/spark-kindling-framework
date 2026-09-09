from dataclasses import dataclass
from enum import Enum
from typing import Dict, Iterable, List, Mapping, Optional, Set, Tuple

GLOBAL_NAMING_KEY = "kindling.storage.table_naming"
ENTITY_NAMING_TAG = "provider.table_naming"
STORAGE_COLLISION_CHECK_KEY = "kindling.storage.collision_check"
ENTITY_ALIAS_OF_TAG = "provider.table_alias_of"


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
_ALLOWED_COLLISION_CHECK_MODES = ("off", "pipeline", "registry")


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


def parse_collision_check_mode(value: object) -> str:
    """Parse the external-address collision-check scope."""
    if value is None:
        return "off"

    raw_value = str(value).strip()
    if not raw_value:
        return "off"

    lowered = raw_value.lower()
    if lowered in _ALLOWED_COLLISION_CHECK_MODES:
        return lowered

    allowed = "'off', 'pipeline', or 'registry'"
    raise ValueError(
        f"Invalid {STORAGE_COLLISION_CHECK_KEY} value {raw_value!r}; expected {allowed}."
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


@dataclass(frozen=True)
class Collision:
    """A set of distinct entities resolving to one external table address."""

    address: str
    entity_ids: Tuple[str, ...]


def _add_collision_candidate(
    groups: Dict[str, Tuple[str, List[str], Set[str]]],
    entity_id: str,
    resolved_name: str,
) -> None:
    entity_key = str(entity_id)
    address = str(resolved_name).strip()
    if not entity_key or not address:
        return

    folded_address = address.casefold()
    if folded_address not in groups:
        groups[folded_address] = (address, [], set())
    _address, entity_ids, seen = groups[folded_address]
    if entity_key in seen:
        return
    seen.add(entity_key)
    entity_ids.append(entity_key)


def _is_intentional_alias(
    entity_id: str,
    member_set: Set[str],
    tags_by_entity: Mapping[str, Mapping[str, object]],
) -> bool:
    tags = tags_by_entity.get(entity_id) or {}
    alias_of = str(tags.get(ENTITY_ALIAS_OF_TAG, "") or "").strip()
    return bool(alias_of and alias_of != entity_id and alias_of in member_set)


def _non_alias_members(
    entity_ids: List[str],
    tags_by_entity: Mapping[str, Mapping[str, object]],
) -> Tuple[str, ...]:
    member_set = set(entity_ids)
    return tuple(
        sorted(
            {
                entity_id
                for entity_id in entity_ids
                if not _is_intentional_alias(entity_id, member_set, tags_by_entity)
            }
        )
    )


def find_external_collisions(
    resolved_entities: Iterable[Tuple[str, str]],
    entity_tags: Optional[Mapping[str, Mapping[str, object]]] = None,
) -> Tuple[Collision, ...]:
    """Find distinct logical entities that resolve to the same external address.

    The function is pure metadata logic: callers provide already-resolved
    external names and entity tags. Intentional aliases are excluded only when
    ``provider.table_alias_of`` points to another entity in the same address
    group.
    """
    tags_by_entity = entity_tags or {}
    groups: Dict[str, Tuple[str, List[str], Set[str]]] = {}
    for entity_id, resolved_name in resolved_entities:
        _add_collision_candidate(groups, entity_id, resolved_name)

    collisions: List[Collision] = []
    for address, entity_ids, _seen in groups.values():
        member_ids = _non_alias_members(entity_ids, tags_by_entity)
        if len(member_ids) > 1:
            collisions.append(Collision(address, member_ids))
    return tuple(collisions)
