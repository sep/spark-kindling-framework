"""Surrogate key synthesis.

Kindling entities declare surrogate key columns with ``surrogate.*`` tags
(parsed and validated in ``kindling.data_entities``) and the framework
synthesizes them. This module is the one place that builds key
expressions; every placement calls it:

- SCD2 version and durable keys are minted inside the SCD2 merges of the
  Delta and memory providers, on the inserted rows only, from the staged
  effective_from value (``with_scd2_surrogate_keys``);
- non-SCD2 keys are a DataFrame column added before persist
  (``SurrogateKeyAspect`` on ``persist.before_persist``) or before a
  streaming sink starts (``apply_surrogate_keys``).

All deterministic strategies hash one canonical string, the ``v1``
encoding (``canonical_v1``): the input values in declared order, an
optional namespace first, each value as ``N`` (NULL) or
``S<char_length>:<canonical value>``, concatenated with no separator.
Column names are not part of the input. Only built-in Spark functions are
used: no UDFs and no JVM access, so the same expressions run on classic
Spark 3.4+, Spark 4 and Spark Connect.

Key columns are insert-only: set once when a row is inserted, never
rewritten by any merge clause, and never matched on.
"""

import re
import sys
import uuid as _uuid
from contextlib import contextmanager
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple, Union

from injector import inject
from kindling.data_entities import (
    DataEntityRegistry,
    EntityMetadata,
    SurrogateKeyConfig,
    quote_sql_identifier,
    scd_config_from_tags,
    spark_case_sensitive,
    surrogate_config_from_tags,
)
from kindling.injection import GlobalInjector
from kindling.signaling import SignalProvider
from kindling.spark_log_provider import PythonLoggerProvider
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BinaryType,
    BooleanType,
    ByteType,
    DataType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

try:  # pyspark >= 3.4 has both; guarded so older stubs still import.
    from pyspark.sql.types import CharType, VarcharType

    _STRING_TYPES: Tuple[type, ...] = (StringType, CharType, VarcharType)
except ImportError:  # pragma: no cover
    _STRING_TYPES = (StringType,)

_INTEGRAL_TYPES = (ByteType, ShortType, IntegerType, LongType)

# The encoding this module implements. Recorded on the entity
# (surrogate.encoding tag) and on Delta tables (kindling.surrogate.encoding).
ENCODING = "v1"

# frac + 1 (see _plain_decimal) needs precision scale + 2 <= 38.
_MAX_DECIMAL_SCALE = 36

# Columns a key frame may carry that are not data (SCD2 merge staging).
_STAGING_PREFIX = "__scd2_"
_MERGE_KEY = "__merge_key"

KEY_KINDS = ("version", "durable", "key")


# ── v1 canonical encoding ────────────────────────────────────────────────────


def v1_type_problem(data_type: DataType) -> Optional[str]:
    """Why ``data_type`` cannot be encoded by ``v1``, or None when it can."""
    if isinstance(data_type, _INTEGRAL_TYPES + _STRING_TYPES):
        return None
    if isinstance(data_type, (BooleanType, DateType, TimestampType, BinaryType)):
        return None
    if isinstance(data_type, DecimalType):
        if data_type.scale > _MAX_DECIMAL_SCALE:
            return (
                f"{data_type.simpleString()} has scale above {_MAX_DECIMAL_SCALE}, "
                "which the v1 encoding does not support"
            )
        return None
    name = data_type.simpleString()
    if name in ("float", "double"):
        return f"{name} has no single canonical representation in the v1 encoding"
    if name == "timestamp_ntz":
        return "timestamp_ntz is not an instant; use a timestamp column"
    if name.startswith(("array", "map", "struct")):
        return f"complex type {name} is not encodable in the v1 encoding"
    return f"type {name} is not encodable in the v1 encoding"


@dataclass(frozen=True)
class _Typed:
    """A key input: a Column, its data type, and a SQL reference to it."""

    column: Column
    data_type: DataType
    sql: Optional[str]
    label: str


def _unix_micros(value: _Typed) -> Column:
    if hasattr(F, "unix_micros"):  # pyspark >= 3.5 (and Spark Connect)
        return F.unix_micros(value.column)
    if value.sql is None:  # pragma: no cover - pyspark 3.4 with an untyped expression
        raise ValueError(f"timestamp input {value.label} needs a column reference on pyspark < 3.5")
    return F.expr(f"unix_micros({value.sql})")


def _plain_decimal(column: Column, data_type: DecimalType) -> Column:
    """Plain notation at the declared scale, never scientific.

    Spark's decimal-to-string cast gives scientific notation for small
    magnitudes (``0E-18``, ``1.00000000000E-7``) and ``format_number`` adds
    thousands separators, so the digits are built explicitly: a sign, the
    integer digits (``floor``/``ceil`` toward zero, scale 0, which always
    renders plainly), a point, then exactly ``scale`` fraction digits. The
    fraction ``f = value % 1`` is shifted to ``f + 1`` (positive values) or
    ``f - 1`` (negative values), whose magnitude is in [1, 2) and so also
    always renders plainly; the digits after its point are those of ``|f|``.
    Together these are the digits of the unscaled value. Scale 0 has no
    point.

    No negation or ``abs`` is used: Spark 4.0 widens a negated decimal by
    one digit, which overflows or rounds at precision 37-38.
    """
    scale = data_type.scale
    negative = column < F.lit(0)
    sign = F.when(negative, F.lit("-")).otherwise(F.lit(""))
    toward_zero = F.when(negative, F.ceil(column)).otherwise(F.floor(column))
    integer_digits = F.regexp_replace(toward_zero.cast("string"), "^-", "")
    if scale == 0:
        return F.concat(sign, integer_digits)
    one = F.lit(1).cast(DecimalType(1, 0))
    fraction = column % one
    shifted = F.when(negative, fraction - one).otherwise(fraction + one).cast("string")
    fraction_digits = F.substring(F.substring_index(shifted, ".", -1), 1, scale)
    return F.concat(sign, integer_digits, F.lit("."), fraction_digits)


def _canonical_text(value: _Typed) -> Column:
    """The canonical (non-NULL) text of one input value."""
    column, data_type = value.column, value.data_type
    problem = v1_type_problem(data_type)
    if problem:
        raise ValueError(f"Surrogate key input {value.label} cannot be encoded: {problem}")
    if isinstance(data_type, _INTEGRAL_TYPES):
        return column.cast("string")
    if isinstance(data_type, _STRING_TYPES):
        return column.cast("string")
    if isinstance(data_type, BooleanType):
        return F.when(column, F.lit("T")).otherwise(F.lit("F"))
    if isinstance(data_type, DateType):
        return column.cast("string")
    if isinstance(data_type, TimestampType):
        return _unix_micros(value).cast("string")
    if isinstance(data_type, BinaryType):
        return F.lower(F.hex(column))
    return _plain_decimal(column, data_type)


def _encode(value: _Typed) -> Column:
    text = _canonical_text(value)
    return F.when(value.column.isNull(), F.lit("N")).otherwise(
        F.concat(F.lit("S"), F.length(text).cast("string"), F.lit(":"), text)
    )


def encode_literal_v1(text: str) -> str:
    """The ``v1`` encoding of one literal string value (e.g. a namespace)."""
    return f"S{len(text)}:{text}"


def canonical_v1(values: Sequence[_Typed], namespace: Optional[str] = None) -> Column:
    """The ``v1`` canonical string over typed inputs, namespace first."""
    parts = [F.lit(encode_literal_v1(namespace))] if namespace else []
    parts.extend(_encode(value) for value in values)
    if not parts:
        return F.lit("")
    if len(parts) == 1:
        return parts[0]
    return F.concat(*parts)


def hash_v1(canonical: Column, algo: str = "sha256") -> Column:
    """Hash a canonical string: ``sha256`` gives 64-char lowercase hex."""
    algo = (algo or "sha256").lower()
    if algo in ("md5", "xxhash64"):
        raise ValueError(f"hash algorithm '{algo}' is not yet supported (only 'sha256')")
    if algo != "sha256":
        raise ValueError(f"unknown hash algorithm '{algo}' (expected 'sha256')")
    return F.sha2(canonical, 256)


def uuid5_v1(canonical: Column, namespace: str) -> Column:
    """RFC 4122 name-based (version 5) UUID of a canonical string.

    ``sha1(namespace bytes || utf-8 name)``, first 32 hex digits, version
    nibble 5, variant ``10xx``, formatted 8-4-4-4-12. Matches Python's
    ``uuid.uuid5(UUID(namespace), name)``.
    """
    namespace_hex = _uuid.UUID(str(namespace)).hex
    digest = F.sha1(F.concat(F.unhex(F.lit(namespace_hex)), F.encode(canonical, "UTF-8")))
    variant_nibble = (
        F.conv(F.substring(digest, 17, 1), 16, 10).cast("int").bitwiseAND(3).bitwiseOR(8)
    )
    variant = F.lower(F.conv(variant_nibble.cast("string"), 10, 16))
    return F.concat_ws(
        "-",
        F.substring(digest, 1, 8),
        F.substring(digest, 9, 4),
        F.concat(F.lit("5"), F.substring(digest, 14, 3)),
        F.concat(variant, F.substring(digest, 18, 3)),
        F.substring(digest, 21, 12),
    )


# ── Custom generators ────────────────────────────────────────────────────────

_OUTPUT_TYPE_NAMES: Dict[str, DataType] = {
    "string": StringType(),
    "binary": BinaryType(),
    "bigint": LongType(),
    "long": LongType(),
    "int": IntegerType(),
    "integer": IntegerType(),
}
_DECIMAL_TYPE_PATTERN = re.compile(r"^decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)$")


def _parse_output_type(output_type: Union[str, DataType]) -> DataType:
    if isinstance(output_type, DataType):
        return output_type
    text = str(output_type).strip().lower()
    if text in _OUTPUT_TYPE_NAMES:
        return _OUTPUT_TYPE_NAMES[text]
    match = _DECIMAL_TYPE_PATTERN.match(text)
    if match:
        return DecimalType(int(match.group(1)), int(match.group(2)))
    raise ValueError(
        f"Unsupported surrogate key generator output_type '{output_type}' (expected a "
        f"DataType or one of {sorted(_OUTPUT_TYPE_NAMES)} or 'decimal(p,s)')"
    )


@dataclass(frozen=True)
class SurrogateKeyGenerator:
    """A registered generator: a function ``KeyContext -> Column``."""

    name: str
    func: Callable[["KeyContext"], Column]
    deterministic: bool
    output_type: DataType


class SurrogateKeyGenerators:
    """Class-level registry of custom surrogate key generators.

    Modelled on ``DeltaMergeStrategies``: a plain registry, not the
    injector. A generator builds a Spark Column from a ``KeyContext``; it is
    not a row-level UDF, so the same generator runs as a DataFrame column
    (non-SCD2) and inside the SCD2 merges. Names resolve at
    ``ensure_destination``; the resolved *function object* is what travels
    into a streaming merge's ``foreachBatch`` callback, so generators must be
    picklable (module-level functions, no live SparkSession or DI objects in
    their closure).

    Example::

        @SurrogateKeyGenerators.register(
            "acme_customer_key", deterministic=True, output_type="string"
        )
        def acme_customer_key(ctx: KeyContext) -> Column:
            return F.concat_ws("-", ctx.col("source_system"), ctx.hash(*ctx.key_columns))
    """

    _registry: Dict[str, SurrogateKeyGenerator] = {}

    @classmethod
    def register(
        cls, name: str, *, deterministic: bool, output_type: Union[str, DataType] = "string"
    ):
        """Register a generator under ``name``.

        Re-registering the same function (same module and qualified name,
        e.g. a re-run notebook cell or a re-import) replaces the entry; a
        different function under a taken name is an error.
        """
        if not isinstance(name, str) or not name.strip():
            raise ValueError("Surrogate key generator name must be a non-empty string")
        if not isinstance(deterministic, bool):
            raise ValueError("SurrogateKeyGenerators.register: deterministic must be True/False")
        parsed_type = _parse_output_type(output_type)
        name = name.strip()

        def decorator(func: Callable[["KeyContext"], Column]):
            if not callable(func):
                raise TypeError(f"Surrogate key generator '{name}' must be callable")
            existing = cls._registry.get(name)
            if existing is not None and _qualified_name(existing.func) != _qualified_name(func):
                raise ValueError(f"Surrogate key generator '{name}' is already registered.")
            cls._registry[name] = SurrogateKeyGenerator(
                name=name, func=func, deterministic=deterministic, output_type=parsed_type
            )
            return func

        return decorator

    @classmethod
    def get(cls, name: str) -> SurrogateKeyGenerator:
        """Return the registered generator, or fail listing the registered names."""
        if name not in cls._registry:
            raise ValueError(
                f"Unknown surrogate key generator '{name}'. "
                f"Registered generators: {sorted(cls._registry)}"
            )
        return cls._registry[name]

    @classmethod
    def names(cls) -> List[str]:
        return sorted(cls._registry)

    @classmethod
    def unregister(cls, name: str) -> None:
        cls._registry.pop(name, None)


def _qualified_name(func: Callable) -> Tuple[str, str]:
    return (getattr(func, "__module__", ""), getattr(func, "__qualname__", repr(func)))


@dataclass(frozen=True)
class EntityView:
    """Read-only view of the entity a key is built for (picklable)."""

    entityid: str
    name: str
    merge_columns: Tuple[str, ...]
    tag_items: Tuple[Tuple[str, str], ...]

    @property
    def tags(self) -> Mapping[str, str]:
        return MappingProxyType(dict(self.tag_items))

    @classmethod
    def of(cls, entity: EntityMetadata) -> "EntityView":
        return cls(
            entityid=entity.entityid,
            name=entity.name,
            merge_columns=tuple(entity.merge_columns or ()),
            tag_items=tuple(sorted((str(k), str(v)) for k, v in (entity.tags or {}).items())),
        )


class KeyContext:
    """What a custom generator sees while building one key column.

    - ``col(name)``: a column of the rows being keyed, bound by the
      framework (a plain DataFrame column, or ``alias``-qualified, e.g.
      ``staged.<name>`` inside a merge).
    - ``columns``: every data column, after transforms (staging helpers,
      SCD2 temporal columns and key columns excluded).
    - ``key_columns``: the resolved ``surrogate.from`` columns.
    - ``effective_from``: the final, stored effective_from of the version
      being inserted; ``None`` for non-SCD2 entities and for durable keys.
    - ``effective_to`` / ``is_current``: their values at insert time (NULL,
      true) on SCD2 entities, ``None`` otherwise.
    - ``entity``: read-only view of the entity (id, name, tags, merge columns).
    - ``kind``: ``"version"`` (SCD2 version key), ``"durable"`` (SCD2 durable
      key) or ``"key"`` (non-SCD2).
    - ``options``: the entity's ``surrogate.generator.<option>`` tags.
    - ``canonical(*cols)`` / ``hash(*cols, algo=)``: the ``v1`` helpers. Pass
      column names or columns from ``col()`` / ``effective_from`` to get
      their typed canonical forms; any other Column is encoded as its
      string cast.

    There are deliberately no batch or run ids: they would make every key
    non-reproducible.
    """

    def __init__(
        self,
        *,
        schema: StructType,
        entity: EntityView,
        kind: str,
        key_columns: Sequence[str],
        options: Mapping[str, str],
        scd2: bool,
        effective_from_column: Optional[str] = None,
        namespace: Optional[str] = None,
        alias: Optional[str] = None,
        case_sensitive: bool = False,
        excluded_columns: Sequence[str] = (),
    ):
        if kind not in KEY_KINDS:
            raise ValueError(f"KeyContext kind must be one of {KEY_KINDS}, got '{kind}'")
        self._schema = schema
        self._entity = entity
        self._kind = kind
        self._key_columns = tuple(key_columns)
        self._options = MappingProxyType(dict(options))
        self._alias = alias
        self._case_sensitive = case_sensitive
        self._namespace = namespace
        self._typed: Dict[int, _Typed] = {}
        excluded = {self._norm(name) for name in excluded_columns}
        if effective_from_column:
            excluded.add(self._norm(effective_from_column))
        self._columns = tuple(
            field.name
            for field in schema.fields
            if self._norm(field.name) not in excluded
            and field.name != _MERGE_KEY
            and not field.name.startswith(_STAGING_PREFIX)
        )
        self._effective_from = None
        if effective_from_column and kind == "version":
            typed = _typed_input(
                schema, effective_from_column, alias, case_sensitive, label="effective_from"
            )
            if not isinstance(typed.data_type, TimestampType):
                typed = _Typed(
                    column=typed.column.cast(TimestampType()),
                    data_type=TimestampType(),
                    sql=f"CAST({typed.sql} AS TIMESTAMP)",
                    label=typed.label,
                )
            self._effective_from = typed.column
            self._typed[id(typed.column)] = typed
        self._scd2 = scd2

    def _norm(self, name: str) -> str:
        return name if self._case_sensitive else name.lower()

    @property
    def kind(self) -> str:
        return self._kind

    @property
    def entity(self) -> EntityView:
        return self._entity

    @property
    def options(self) -> Mapping[str, str]:
        return self._options

    @property
    def columns(self) -> Tuple[str, ...]:
        return self._columns

    @property
    def key_columns(self) -> Tuple[str, ...]:
        return self._key_columns

    @property
    def effective_from(self) -> Optional[Column]:
        return self._effective_from

    @property
    def effective_to(self) -> Optional[Column]:
        return F.lit(None).cast(TimestampType()) if self._scd2 else None

    @property
    def is_current(self) -> Optional[Column]:
        return F.lit(True) if self._scd2 else None

    def col(self, name: str) -> Column:
        """The bound column ``name`` (a data column of the rows being keyed)."""
        if self._norm(name) not in {self._norm(column) for column in self._columns}:
            raise ValueError(
                f"KeyContext.col: '{name}' is not a data column of entity "
                f"'{self._entity.entityid}' (columns: {list(self._columns)})"
            )
        typed = _typed_input(self._schema, name, self._alias, self._case_sensitive, label=name)
        self._typed[id(typed.column)] = typed
        return typed.column

    def _resolve(self, value: Union[str, Column]) -> _Typed:
        if isinstance(value, str):
            self.col(value)
            return _typed_input(self._schema, value, self._alias, self._case_sensitive, value)
        typed = self._typed.get(id(value))
        if typed is not None and typed.column is value:
            return typed
        if isinstance(value, Column):
            return _Typed(column=value.cast("string"), data_type=StringType(), sql=None, label="")
        raise TypeError(
            f"KeyContext.canonical expects column names or Columns, got {type(value).__name__}"
        )

    def canonical(self, *cols: Union[str, Column]) -> Column:
        """The ``v1`` canonical string of ``cols`` (with the entity's namespace)."""
        return canonical_v1([self._resolve(value) for value in cols], self._namespace)

    def hash(self, *cols: Union[str, Column], algo: str = "sha256") -> Column:
        """``hash_v1(canonical(*cols))``: 64-char lowercase sha256 hex by default."""
        return hash_v1(self.canonical(*cols), algo)


# ── Resolution ───────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class SurrogateKeyPlan:
    """An entity's surrogate keys, fully resolved on the driver.

    Plain, picklable data: the parsed config, a read-only entity view, and
    (for ``custom``) the generator *function object*. A streaming merge
    captures the plan in its ``foreachBatch`` callback, so the generator name
    is never looked up in the (possibly un-bootstrapped) worker.
    """

    config: SurrogateKeyConfig
    entity: EntityView
    scd2: bool
    generator: Optional[SurrogateKeyGenerator] = None
    # SCD2 effective_from/effective_to/is_current names: never data columns.
    temporal_columns: Tuple[str, ...] = ()

    @property
    def key_columns(self) -> List[str]:
        return self.config.key_columns

    @property
    def output_type(self) -> DataType:
        if self.generator is not None:
            return self.generator.output_type
        return StringType()

    def keys(self) -> List[Tuple[str, str]]:
        """``[(column, kind)]`` for every declared key."""
        cfg = self.config
        result = []
        if cfg.column:
            result.append((cfg.column, "version" if self.scd2 else "key"))
        if cfg.durable_column:
            result.append((cfg.durable_column, "durable"))
        return result


@contextmanager
def generator_pickled_by_value(plan: Optional[SurrogateKeyPlan]):
    """Pickle a plan's generator *by value* inside this context.

    cloudpickle pickles an importable module-level function by reference
    (module + name). Spark Connect's ``foreachBatch`` worker unpickles the
    callback in a fresh process, where the generator's module (a notebook
    module, a test module, a package not on the worker's path) may not be
    importable, so the callback would fail to load. Registering the module
    for by-value pickling while the callback is serialized makes the
    function itself travel. A no-op without a custom generator, for
    ``__main__`` (already by value) and for Kindling's own modules.
    """
    func = plan.generator.func if plan is not None and plan.generator is not None else None
    module_name = getattr(func, "__module__", None) or ""
    module = sys.modules.get(module_name)
    if module is None or module_name == "__main__" or module_name.split(".")[0] == "kindling":
        yield
        return
    try:
        from pyspark import cloudpickle

        registered = module_name in cloudpickle.list_registry_pickle_by_value()
        if not registered:
            cloudpickle.register_pickle_by_value(module)
    except Exception:  # pragma: no cover - cloudpickle without by-value support
        yield
        return
    try:
        yield
    finally:
        if not registered:
            cloudpickle.unregister_pickle_by_value(module)


def resolve_surrogate_keys(entity: EntityMetadata) -> Optional[SurrogateKeyPlan]:
    """Resolve an entity's surrogate keys, or None when it declares none.

    Custom generator names resolve here (the error lists the registered
    names). Phase 1 places deterministic keys only: a ``deterministic=False``
    generator is rejected until random keys (``uuid4``) arrive.
    """
    cfg = surrogate_config_from_tags(entity)
    if not cfg.enabled:
        return None
    if cfg.encoding != ENCODING:
        raise ValueError(
            f"Entity '{entity.entityid}': unsupported surrogate.encoding '{cfg.encoding}' "
            f"(this framework writes '{ENCODING}')"
        )
    generator = None
    if cfg.strategy == "custom":
        try:
            generator = SurrogateKeyGenerators.get(cfg.generator or "")
        except ValueError as error:
            raise ValueError(f"Entity '{entity.entityid}': {error}") from error
        if not generator.deterministic:
            raise ValueError(
                f"Entity '{entity.entityid}': surrogate key generator '{generator.name}' is "
                "declared deterministic=False; non-deterministic generators are not yet "
                "supported (they arrive with the 'uuid4' strategy in a later release)"
            )
        if entity.schema is not None:
            fields_by_name = {field.name.lower(): field for field in entity.schema.fields}
            for column in cfg.key_columns:
                declared = fields_by_name.get(column.lower())
                if declared is not None and declared.dataType != generator.output_type:
                    raise ValueError(
                        f"Entity '{entity.entityid}': surrogate key column '{column}' is "
                        f"declared as {declared.dataType.simpleString()}, but generator "
                        f"'{generator.name}' outputs {generator.output_type.simpleString()}"
                    )
    elif cfg.strategy not in ("hash", "uuid5"):
        raise ValueError(
            f"Entity '{entity.entityid}': surrogate.strategy '{cfg.strategy}' is not supported"
        )
    scd = scd_config_from_tags(entity)
    return SurrogateKeyPlan(
        config=cfg,
        entity=EntityView.of(entity),
        scd2=scd.enabled,
        generator=generator,
        temporal_columns=(
            (scd.effective_from_column, scd.effective_to_column, scd.is_current_column)
            if scd.enabled
            else ()
        ),
    )


def augment_schema_for_surrogate_keys(schema: StructType, entity: EntityMetadata) -> StructType:
    """Add declared key columns missing from ``schema``.

    Like ``augment_schema_for_scd2``: each added column is
    ``nullable=False`` and carries field metadata
    ``kindling.surrogate=<strategy>``; STRING for ``hash`` and ``uuid5``, the
    generator's ``output_type`` for ``custom``.
    """
    schema_struct = schema if isinstance(schema, StructType) else StructType(schema)
    plan = resolve_surrogate_keys(entity)
    if plan is None:
        return schema_struct
    existing = {field.name.lower() for field in schema_struct.fields}
    extra = [
        StructField(
            column,
            plan.output_type,
            False,
            metadata={"kindling.surrogate": plan.config.strategy},
        )
        for column, _ in plan.keys()
        if column.lower() not in existing
    ]
    if not extra:
        return schema_struct
    return StructType(schema_struct.fields + extra)


# ── Key expressions ──────────────────────────────────────────────────────────


def _find_field(schema: StructType, name: str, case_sensitive: bool) -> Optional[StructField]:
    for field in schema.fields:
        if field.name == name or (not case_sensitive and field.name.lower() == name.lower()):
            return field
    return None


def _typed_input(
    schema: StructType, name: str, alias: Optional[str], case_sensitive: bool, label: str
) -> _Typed:
    field = _find_field(schema, name, case_sensitive)
    if field is None:
        raise ValueError(
            f"Surrogate key input column '{name}' is missing from the rows being keyed "
            f"(columns: {[f.name for f in schema.fields]})"
        )
    sql = quote_sql_identifier(field.name, alias)
    return _Typed(column=F.col(sql), data_type=field.dataType, sql=sql, label=f"'{label}'")


def _key_expression(
    plan: SurrogateKeyPlan,
    schema: StructType,
    kind: str,
    effective_from_column: Optional[str] = None,
    alias: Optional[str] = None,
    case_sensitive: bool = False,
) -> Column:
    cfg = plan.config
    ef_column = effective_from_column if kind == "version" else None
    if plan.generator is not None:
        scd = plan.scd2
        ctx = KeyContext(
            schema=schema,
            entity=plan.entity,
            kind=kind,
            key_columns=cfg.from_columns,
            options=dict(cfg.generator_options),
            scd2=scd,
            effective_from_column=ef_column,
            namespace=cfg.namespace,
            alias=alias,
            case_sensitive=case_sensitive,
            excluded_columns=tuple(plan.key_columns) + tuple(plan.temporal_columns),
        )
        built = plan.generator.func(ctx)
        if not isinstance(built, Column):
            raise TypeError(
                f"Surrogate key generator '{plan.generator.name}' must return a Spark "
                f"Column, got {type(built).__name__}"
            )
        return built.cast(plan.generator.output_type)

    inputs = [
        _typed_input(schema, name, alias, case_sensitive, label=name) for name in cfg.from_columns
    ]
    if ef_column and cfg.include_effective_from:
        typed = _typed_input(schema, ef_column, alias, case_sensitive, label="effective_from")
        if not isinstance(typed.data_type, TimestampType):
            typed = _Typed(
                column=typed.column.cast(TimestampType()),
                data_type=TimestampType(),
                sql=f"CAST({typed.sql} AS TIMESTAMP)",
                label=typed.label,
            )
        inputs.append(typed)
    canonical = canonical_v1(inputs, cfg.namespace)
    if cfg.strategy == "uuid5":
        return uuid5_v1(canonical, cfg.uuid_namespace)
    return hash_v1(canonical, cfg.hash_algo)


def _key_column_names(frame: DataFrame, plan: SurrogateKeyPlan, case_sensitive: bool) -> List[str]:
    """Names in ``frame`` that are declared key columns (per case sensitivity)."""
    keys = {name if case_sensitive else name.lower() for name in plan.key_columns}
    return [
        column for column in frame.columns if (column if case_sensitive else column.lower()) in keys
    ]


def drop_surrogate_key_columns(
    frame: DataFrame, plan: Optional[SurrogateKeyPlan], case_sensitive: bool = False
) -> DataFrame:
    """Drop incoming key columns: keys are computed by the framework, never taken."""
    if plan is None:
        return frame
    present = _key_column_names(frame, plan, case_sensitive)
    return frame.drop(*present) if present else frame


def with_scd2_surrogate_keys(
    frame: DataFrame,
    plan: Optional[SurrogateKeyPlan],
    effective_from_column: str,
    case_sensitive: bool = False,
) -> DataFrame:
    """Add SCD2 version/durable keys to rows that are about to be inserted.

    ``effective_from_column`` names the column of ``frame`` carrying the
    final, stored effective_from of each new version; the version key reads
    it, the durable key does not.
    """
    if plan is None:
        return frame
    schema = frame.schema
    for column, kind in plan.keys():
        frame = frame.withColumn(
            column,
            _key_expression(
                plan,
                schema,
                kind,
                effective_from_column=effective_from_column,
                case_sensitive=case_sensitive,
            ),
        )
    return frame


def apply_surrogate_keys(
    df: DataFrame, entity: EntityMetadata, plan: Optional[SurrogateKeyPlan] = None
) -> DataFrame:
    """Add a non-SCD2 entity's key column(s) to ``df`` (a pure column expression).

    Returns ``df`` unchanged for entities without keys and for SCD2
    entities, whose keys are minted inside the merge. Works on batch and
    streaming DataFrames alike. A key column already present is replaced:
    keys are computed, never taken from the input.
    """
    plan = plan if plan is not None else resolve_surrogate_keys(entity)
    if plan is None or plan.scd2:
        return df
    case_sensitive = spark_case_sensitive(df.sparkSession)
    df = drop_surrogate_key_columns(df, plan, case_sensitive)
    schema = df.schema
    for column, kind in plan.keys():
        df = df.withColumn(
            column, _key_expression(plan, schema, kind, case_sensitive=case_sensitive)
        )
    return df


# ── Test helper ──────────────────────────────────────────────────────────────


def assert_generator_deterministic(
    generator: Union[str, SurrogateKeyGenerator, Callable[[KeyContext], Column]],
    sample_df: DataFrame,
    *,
    kind: str = "key",
    key_columns: Optional[Sequence[str]] = None,
    effective_from: Optional[str] = None,
    options: Optional[Mapping[str, str]] = None,
    entity: Optional[EntityMetadata] = None,
    partitions: int = 7,
) -> None:
    """Assert that a generator gives the same key for the same row twice.

    Evaluates the generator over ``sample_df``, repartitions, evaluates it
    again and compares row by row. Catches ``rand()``/``uuid()``-style and
    partition-dependent expressions. Determinism is declared, not inspected
    (inspecting the plan would need the JVM); this is how to check it.
    """
    if isinstance(generator, str):
        func = SurrogateKeyGenerators.get(generator).func
    elif isinstance(generator, SurrogateKeyGenerator):
        func = generator.func
    else:
        func = generator
    view = (
        EntityView.of(entity)
        if entity is not None
        else EntityView(entityid="<sample>", name="<sample>", merge_columns=(), tag_items=())
    )

    def build(frame: DataFrame) -> Column:
        ctx = KeyContext(
            schema=frame.schema,
            entity=view,
            kind=kind,
            key_columns=tuple(key_columns or ()),
            options=dict(options or {}),
            scd2=kind in ("version", "durable"),
            effective_from_column=effective_from,
        )
        built = func(ctx)
        if not isinstance(built, Column):
            raise TypeError(f"generator must return a Spark Column, got {type(built).__name__}")
        return built

    first_col, second_col = "__sk_first", "__sk_second"
    first = sample_df.withColumn(first_col, build(sample_df))
    shuffled = first.repartition(partitions)
    both = shuffled.withColumn(second_col, build(sample_df))
    differing = both.where(~F.col(first_col).eqNullSafe(F.col(second_col)))
    sample = differing.limit(5).collect()
    if sample:
        raise AssertionError(
            "Surrogate key generator is not deterministic: evaluating it twice gave "
            f"different keys for the same rows, e.g. {[row.asDict() for row in sample]}"
        )


# ── Batch aspect ─────────────────────────────────────────────────────────────


@GlobalInjector.singleton_autobind()
class SurrogateKeyAspect:
    """Adds non-SCD2 surrogate keys to pipe output before it is persisted.

    Listens to ``persist.before_persist`` (receivers chain, so a user
    transform and this aspect both apply, in either order) and returns the
    DataFrame with the key column(s), or None for outputs without keys and
    for SCD2 outputs, whose keys are minted inside the merge. Registered at
    bootstrap on every platform. The streaming start path never emits the
    signal; ``SimplePipeStreamStarter`` calls ``apply_surrogate_keys``
    itself.
    """

    @inject
    def __init__(self, lp: PythonLoggerProvider, signal_provider: SignalProvider):
        # The entity registry is resolved when a persist happens, not at
        # registration (bootstrap registers the aspect early).
        self.der = None
        self.logger = lp.get_logger("SurrogateKeyAspect")
        self._signals = signal_provider
        self._registered = False

    def register(self) -> None:
        """Connect the aspect's handler. Idempotent."""
        if self._registered:
            return
        signal = self._signals.get_signal("persist.before_persist") or self._signals.create_signal(
            "persist.before_persist"
        )
        signal.connect(self._on_before_persist, weak=False)
        self._registered = True
        self.logger.debug("SurrogateKeyAspect registered")

    def _on_before_persist(self, sender, df=None, output_entity_id=None, **kwargs):
        if df is None or not output_entity_id:
            return None
        if self.der is None:
            self.der = GlobalInjector.get(DataEntityRegistry)
        entity = self.der.get_entity_definition(output_entity_id)
        if entity is None:
            return None
        plan = resolve_surrogate_keys(entity)
        if plan is None or plan.scd2:
            return None
        return apply_surrogate_keys(df, entity, plan)
