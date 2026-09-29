"""Declaration derivations: clone and extend for entities and pipes.

A *derivation* takes a source declaration and applies additive changes.
``clone_of`` produces a new declaration under a new id from another's raw
registration (used as a template); an extension applies to a declaration's
own id. Both are resolved by the registries when the declaration they
depend on is available, so packages can derive from one another without
import-order coupling, and config overlays still apply on top of the result.

Stacking is additive and last-in-wins, mirroring config-overlay semantics:

- tags: later value wins for the same key;
- columns / inputs: sets that accumulate; a repeated column with the same
  type is a no-op, with a different type an error (a schema must stay
  unambiguous);
- pipe transforms: each wraps the previous result, so the last one
  registered runs outermost and receives the earlier output.

See docs/proposals/declaration_derivations.md.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

DERIVATION_KEYS = ("clone_of", "add_columns", "add_inputs")
"""Config-overlay keys that describe a derivation rather than an override."""


class DerivationError(ValueError):
    """A derivation cannot be applied to its source declaration."""


# --------------------------------------------------------------------------- #
# Column additions
# --------------------------------------------------------------------------- #


def _column_spec(column: Any) -> Tuple[str, Any, bool]:
    """Normalize an ``add_columns`` entry to ``(name, dataType, nullable)``.

    Accepts a ``StructField`` or a mapping ``{name, type, nullable?}`` where
    ``type`` is a Spark DDL type string (``string``, ``decimal(10,2)`` ...),
    which is how the YAML form spells it.
    """
    try:
        from pyspark.sql.types import StructField
    except ImportError:  # pragma: no cover - pyspark is a hard dependency
        StructField = None  # type: ignore[assignment]

    if StructField is not None and isinstance(column, StructField):
        return column.name, column.dataType, bool(column.nullable)
    if isinstance(column, Mapping):
        name = column.get("name")
        type_name = column.get("type")
        if not name or not type_name:
            raise DerivationError(
                f"add_columns entries need 'name' and 'type' (got {dict(column)!r})."
            )
        data_type = parse_column_type(str(type_name), str(name))
        return str(name), data_type, bool(column.get("nullable", True))
    raise DerivationError(
        f"add_columns entries must be StructField or {{name, type}} mappings, got "
        f"{type(column).__name__}."
    )


_TYPE_ALIASES = {
    "int": "integer",
    "bigint": "long",
    "smallint": "short",
    "tinyint": "byte",
    "bool": "boolean",
    "str": "string",
}


def parse_column_type(type_name: str, column: str) -> Any:
    """Parse a column type from the YAML form without a Spark session.

    Supports the atomic Spark SQL type names (``string``, ``integer``/``int``,
    ``long``/``bigint``, ``double``, ``boolean``, ``date``, ``timestamp`` ...),
    ``decimal(p,s)``, and ``array<...>`` of those. Anything richer (structs,
    maps) is declared in code with ``StructField``.
    """
    import re

    from pyspark.sql.types import ArrayType, DecimalType, _all_atomic_types

    text = type_name.strip().lower()
    array_match = re.fullmatch(r"array\s*<\s*(.+)\s*>", text)
    if array_match:
        return ArrayType(parse_column_type(array_match.group(1), column))
    decimal_match = re.fullmatch(r"decimal\s*(?:\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\))?", text)
    if decimal_match:
        precision = int(decimal_match.group(1) or 10)
        scale = int(decimal_match.group(2) or 0)
        return DecimalType(precision, scale)
    name = _TYPE_ALIASES.get(text, text)
    atomic = _all_atomic_types.get(name)
    if atomic is None:
        raise DerivationError(
            f"add_columns: column {column!r} has an invalid type {type_name!r}; use an atomic "
            "Spark SQL type name, decimal(p,s) or array<type>, or declare it in code."
        )
    return atomic()


def add_columns_to_schema(schema: Any, columns: Sequence[Any], owner: str) -> Any:
    """Return ``schema`` with ``columns`` appended.

    Distinct names accumulate; a repeated name with the same type is a
    no-op; a repeated name with a different type is an error. A declaration
    without a schema (``None``, inferred at write time) has nothing to add
    to, so extending it with columns is an error rather than a partial
    schema.
    """
    from pyspark.sql.types import StructField, StructType

    if not columns:
        return schema
    if schema is None:
        raise DerivationError(
            f"{owner}: cannot add columns to a declaration without a schema; declare the "
            "source schema first."
        )
    if not isinstance(schema, StructType):
        raise DerivationError(
            f"{owner}: add_columns requires a StructType schema, got {type(schema).__name__}."
        )
    fields_by_name = {existing.name: existing for existing in schema.fields}
    result = list(schema.fields)
    for column in columns:
        name, data_type, nullable = _column_spec(column)
        existing = fields_by_name.get(name)
        if existing is None:
            new_field = StructField(name, data_type, nullable)
            result.append(new_field)
            fields_by_name[name] = new_field
        elif existing.dataType != data_type:
            raise DerivationError(
                f"{owner}: column {name!r} is already declared as "
                f"{existing.dataType.simpleString()} "
                f"and cannot be re-added as {data_type.simpleString()}; changing a type is a "
                "migration, not an extension."
            )
    return StructType(result)


def merge_tags(
    base: Optional[Mapping[str, Any]], additions: Optional[Mapping[str, Any]]
) -> Dict[str, Any]:
    merged = dict(base or {})
    merged.update(additions or {})
    return merged


def append_unique(base: Optional[Sequence[str]], additions: Optional[Sequence[str]]) -> List[str]:
    result = list(base or [])
    for item in additions or []:
        if item not in result:
            result.append(item)
    return result


# --------------------------------------------------------------------------- #
# Entity derivations
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class EntityDerivation:
    """One additive change set for an entity declaration.

    ``clone_of`` names the template when the target is a new id. Fields
    that are ``None`` leave the source value untouched. ``overrides`` are
    plain replacements for clone-only fields (``name``, ``merge_columns``,
    ``partition_columns``, ``cluster_columns``), which an extension of an
    existing entity may not change.
    """

    clone_of: Optional[str] = None
    tags: Optional[Mapping[str, Any]] = None
    add_columns: Tuple[Any, ...] = ()
    add_partition_columns: Tuple[str, ...] = ()
    add_cluster_columns: Tuple[str, ...] = ()
    overrides: Mapping[str, Any] = field(default_factory=dict)
    source: str = "code"

    @property
    def is_clone(self) -> bool:
        return self.clone_of is not None


_ENTITY_CLONE_ONLY_OVERRIDES = ("name", "merge_columns", "partition_columns", "cluster_columns")


def apply_entity_derivation(
    raw_params: Mapping[str, Any], derivation: EntityDerivation, owner: str
) -> Dict[str, Any]:
    """Apply one derivation to raw registration params and return new params."""
    params = dict(raw_params)
    for key in derivation.overrides:
        if key not in _ENTITY_CLONE_ONLY_OVERRIDES:
            raise DerivationError(
                f"{owner}: {key!r} cannot be overridden by a derivation; supported: "
                f"{', '.join(_ENTITY_CLONE_ONLY_OVERRIDES)}."
            )
        if not derivation.is_clone:
            raise DerivationError(
                f"{owner}: an extension may only add (tags, add_columns, "
                f"add_partition_columns, add_cluster_columns); {key!r} would replace the "
                "source's value. Clone it instead."
            )
    params.update(derivation.overrides)
    if derivation.tags:
        params["tags"] = merge_tags(params.get("tags"), derivation.tags)
    if derivation.add_columns:
        params["schema"] = add_columns_to_schema(
            params.get("schema"), derivation.add_columns, owner
        )
    if derivation.add_partition_columns:
        params["partition_columns"] = append_unique(
            params.get("partition_columns"), derivation.add_partition_columns
        )
    if derivation.add_cluster_columns:
        params["cluster_columns"] = append_unique(
            params.get("cluster_columns"), derivation.add_cluster_columns
        )
    return params


# --------------------------------------------------------------------------- #
# Pipe derivations
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class PipeDerivation:
    """One additive change set for a pipe declaration.

    ``transform`` wraps the pipe's ``execute``: it receives the previous
    output DataFrame first and the DataFrames of this derivation's
    ``add_inputs`` as keyword arguments (entity id with dots replaced by
    underscores, the same convention execute functions use). ``overrides``
    are clone-only replacements (``name``, ``output_entity_id``,
    ``output_type``, ``use_watermark``, ``driving_entity_ids``).
    """

    clone_of: Optional[str] = None
    tags: Optional[Mapping[str, Any]] = None
    add_inputs: Tuple[str, ...] = ()
    transform: Optional[Callable[..., Any]] = None
    overrides: Mapping[str, Any] = field(default_factory=dict)
    source: str = "code"

    @property
    def is_clone(self) -> bool:
        return self.clone_of is not None


_PIPE_CLONE_ONLY_OVERRIDES = (
    "name",
    "output_entity_id",
    "output_type",
    "use_watermark",
    "driving_entity_ids",
)


def input_kwarg(entity_id: str) -> str:
    return entity_id.replace(".", "_")


def wrap_execute(
    original: Callable[..., Any],
    original_inputs: Sequence[str],
    transform: Callable[..., Any],
    added_inputs: Sequence[str],
) -> Callable[..., Any]:
    """Compose ``transform`` over ``original``.

    The original execute receives exactly the keyword arguments for its own
    inputs (it may not accept extras); the transform receives the original's
    output plus the added inputs' DataFrames by keyword. Positional calls
    (the streaming single-input fallback) pass through to the original.
    """
    original_keys = {input_kwarg(entity_id) for entity_id in original_inputs}
    added_keys = [input_kwarg(entity_id) for entity_id in added_inputs]

    def wrapped(*args: Any, **kwargs: Any) -> Any:
        base_kwargs = {key: value for key, value in kwargs.items() if key in original_keys}
        extra_kwargs = {key: kwargs[key] for key in added_keys if key in kwargs}
        try:
            result = original(*args, **base_kwargs)
        except TypeError:
            # Streaming's compatibility path calls a single-input execute
            # positionally when the keyword form does not fit its signature
            # (``def transform(df)``); it can no longer do so through this
            # wrapper once inputs were added, so mirror that retry here.
            if args or len(original_keys) != 1 or len(base_kwargs) != 1:
                raise
            result = original(next(iter(base_kwargs.values())))
        return transform(result, **extra_kwargs)

    wrapped.__name__ = getattr(original, "__name__", "execute")
    wrapped.__qualname__ = f"{wrapped.__name__}+{getattr(transform, '__name__', 'transform')}"
    wrapped.__kindling_wrapped__ = original  # type: ignore[attr-defined]
    return wrapped


def apply_pipe_derivation(
    raw_params: Mapping[str, Any], derivation: PipeDerivation, owner: str
) -> Dict[str, Any]:
    """Apply one derivation to raw pipe params and return new params."""
    params = dict(raw_params)
    for key in derivation.overrides:
        if key not in _PIPE_CLONE_ONLY_OVERRIDES:
            raise DerivationError(
                f"{owner}: {key!r} cannot be overridden by a derivation; supported: "
                f"{', '.join(_PIPE_CLONE_ONLY_OVERRIDES)}."
            )
        if not derivation.is_clone:
            raise DerivationError(
                f"{owner}: an extension may only add (tags, add_inputs, transform); {key!r} "
                "would replace the source's value. Clone it instead."
            )
    params.update(derivation.overrides)
    if derivation.tags:
        params["tags"] = merge_tags(params.get("tags"), derivation.tags)
    original_inputs = list(params.get("input_entity_ids") or [])
    if derivation.add_inputs:
        params["input_entity_ids"] = append_unique(original_inputs, derivation.add_inputs)
    if derivation.transform is not None or derivation.add_inputs:
        original = params.get("execute")
        if original is None:
            raise DerivationError(f"{owner}: the source pipe has no execute to wrap.")
        # Without a transform (the YAML form), added inputs are still read and
        # ordered as dependencies but the original execute must not receive
        # keyword arguments it never declared, so an identity wrapper drops them.
        transform = derivation.transform or (lambda previous, **_added: previous)
        params["execute"] = wrap_execute(
            original, original_inputs, transform, derivation.add_inputs
        )
    return params


# --------------------------------------------------------------------------- #
# Config (YAML) form
# --------------------------------------------------------------------------- #


def derivation_entries(section: Optional[Mapping[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Split a ``dataentities:``/``datapipes:`` config section into the
    entries that carry derivation keys (``clone_of``, ``add_columns``,
    ``add_inputs``), keyed by exact id. Glob patterns cannot derive."""
    found: Dict[str, Dict[str, Any]] = {}
    for key, value in (section or {}).items():
        if not isinstance(value, Mapping):
            continue
        if not any(derivation_key in value for derivation_key in DERIVATION_KEYS):
            continue
        item_id = str(key)
        if any(char in item_id for char in "*?"):
            raise DerivationError(
                f"Config entry {item_id!r} combines a wildcard pattern with a derivation "
                f"({', '.join(k for k in DERIVATION_KEYS if k in value)}); derivations need an "
                "exact id."
            )
        found[item_id] = {k: v for k, v in value.items() if k in DERIVATION_KEYS}
    return found


def strip_derivation_keys(section: Optional[Mapping[str, Any]]) -> Optional[Dict[str, Any]]:
    """The same section without derivation keys, for the override matchers."""
    if section is None:
        return None
    stripped: Dict[str, Any] = {}
    for key, value in section.items():
        if isinstance(value, Mapping):
            remaining = {k: v for k, v in value.items() if k not in DERIVATION_KEYS}
            if remaining or not any(k in value for k in DERIVATION_KEYS):
                stripped[key] = remaining
        else:
            stripped[key] = value
    return stripped
