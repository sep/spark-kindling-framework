"""Surrogate keys: tags, sugar, registration validation, generator registry,
resolution and schema augmentation. No Spark session needed (pure metadata);
the encoder and the merges are covered in tests/integration."""

import logging
from unittest.mock import Mock

import pytest
from kindling.data_entities import (
    DataEntities,
    DataEntityManager,
    SCDConfig,
    scd_change_columns,
    surrogate_config_from_tags,
    surrogate_key_columns,
)
from kindling.surrogate_keys import (
    EntityView,
    SurrogateKeyGenerators,
    augment_schema_for_surrogate_keys,
    resolve_surrogate_keys,
    v1_type_problem,
)
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
    TimestampType,
)

SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("source_system", StringType(), True),
        StructField("status", StringType(), True),
        StructField("updated_at", TimestampType(), True),
    ]
)


def _register(tags, *, merge_columns=("customer_id",), schema=SCHEMA, entityid="silver.dim"):
    manager = DataEntityManager()
    manager.register_entity(
        entityid,
        name="dim",
        merge_columns=list(merge_columns),
        tags=dict(tags),
        schema=schema,
    )
    return manager.registry[entityid]


SCD2 = {"scd.type": "2", "scd.sequence_by": "updated_at"}


@pytest.fixture(autouse=True)
def _clean_generators():
    before = dict(SurrogateKeyGenerators._registry)
    yield
    SurrogateKeyGenerators._registry.clear()
    SurrogateKeyGenerators._registry.update(before)


# ── parsing ──────────────────────────────────────────────────────────────────


def test_no_surrogate_tags_is_disabled():
    cfg = surrogate_config_from_tags(_register({}))
    assert not cfg.enabled
    assert cfg.key_columns == []


def test_defaults_hash_over_merge_columns_and_records_encoding():
    entity = _register({"surrogate.column": "product_sk"})
    cfg = surrogate_config_from_tags(entity)
    assert cfg.strategy == "hash"
    assert cfg.from_columns == ("customer_id",)
    assert cfg.hash_algo == "sha256" and cfg.hash_format == "hex"
    assert cfg.namespace is None
    # Framework-written encoding tag.
    assert entity.tags["surrogate.encoding"] == "v1"


def test_declared_from_order_is_kept_and_options_parsed():
    entity = _register(
        {
            "surrogate.column": "sk",
            "surrogate.from": "source_system, customer_id",
            "surrogate.strategy": "custom",
            "surrogate.generator": "acme",
            "surrogate.generator.prefix": "CUST",
            "surrogate.namespace": "crm",
        }
    )
    cfg = surrogate_config_from_tags(entity)
    assert cfg.from_columns == ("source_system", "customer_id")
    assert dict(cfg.generator_options) == {"prefix": "CUST"}
    assert cfg.namespace == "crm"


def test_surrogate_key_columns_lists_version_then_durable():
    entity = _register(
        {**SCD2, "surrogate.column": "customer_version_sk", "surrogate.durable_column": "dk"}
    )
    assert surrogate_key_columns(entity) == ["customer_version_sk", "dk"]


def test_entity_without_surrogate_tags_parses_without_reading_merge_columns():
    # MagicMock-like entities (other providers' tests) must not light up keys.
    entity = Mock(tags={"provider_type": "delta"}, merge_columns=Mock())
    assert not surrogate_config_from_tags(entity).enabled


# ── sugar ────────────────────────────────────────────────────────────────────


class TestSugar:
    @pytest.fixture(autouse=True)
    def fake_registry(self):
        registry = Mock()
        DataEntities.deregistry = registry
        yield registry
        DataEntities.reset()

    def _tags(self, registry):
        return registry.register_entity.call_args.kwargs["tags"]

    def test_fills_column_tags(self, fake_registry):
        DataEntities.entity(
            entityid="silver.dim",
            name="dim",
            merge_columns=["id"],
            tags={"scd.type": "2"},
            schema=None,
            surrogate_key="version_sk",
            durable_key="dk",
        )
        tags = self._tags(fake_registry)
        assert tags["surrogate.column"] == "version_sk"
        assert tags["surrogate.durable_column"] == "dk"
        assert tags["scd.type"] == "2"

    def test_explicit_tags_win(self, fake_registry):
        DataEntities.entity(
            entityid="silver.dim",
            name="dim",
            merge_columns=["id"],
            tags={"surrogate.column": "declared_sk"},
            schema=None,
            surrogate_key="sugar_sk",
        )
        assert self._tags(fake_registry)["surrogate.column"] == "declared_sk"

    def test_no_sugar_leaves_tags_untouched(self, fake_registry):
        DataEntities.entity(
            entityid="silver.dim", name="dim", merge_columns=["id"], tags={"a": "b"}, schema=None
        )
        assert self._tags(fake_registry) == {"a": "b"}

    def test_rejects_non_string(self, fake_registry):
        with pytest.raises(ValueError, match="column name"):
            DataEntities.entity(
                entityid="silver.dim",
                name="dim",
                merge_columns=["id"],
                tags={},
                schema=None,
                surrogate_key=["a"],
            )


# ── validation ───────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "tags, message",
    [
        ({"surrogate.column": "sk", "surrogate.strategy": "uuid4"}, "'uuid4' is not yet supported"),
        ({"surrogate.column": "sk", "surrogate.strategy": "identity"}, "not yet supported"),
        ({"surrogate.column": "sk", "surrogate.strategy": "nope"}, "invalid surrogate.strategy"),
        ({"surrogate.column": "sk", "surrogate.hash.algo": "md5"}, "'md5' is not yet supported"),
        ({"surrogate.column": "sk", "surrogate.hash.algo": "xxhash64"}, "not yet supported"),
        ({"surrogate.column": "sk", "surrogate.hash.algo": "sha1"}, "invalid surrogate.hash.algo"),
        ({"surrogate.column": "sk", "surrogate.hash.format": "binary"}, "not yet supported"),
        ({"surrogate.column": "sk", "surrogate.normalize": "trim"}, "not yet supported"),
        ({"surrogate.column": "sk", "surrogate.unknown_member": "true"}, "not yet supported"),
        ({"surrogate.column": "sk", "surrogate.encoding": "v2"}, "unsupported surrogate.encoding"),
        ({"surrogate.column": "sk", "surrogate.colum": "x"}, "unknown surrogate tag"),
        ({"surrogate.strategy": "hash"}, "neither surrogate.column"),
        ({"surrogate.column": "sk", "surrogate.strategy": "uuid5"}, "requires surrogate.uuid"),
        (
            {
                "surrogate.column": "sk",
                "surrogate.strategy": "uuid5",
                "surrogate.uuid.namespace": "not-a-uuid",
            },
            "not a valid UUID",
        ),
        (
            {
                "surrogate.column": "sk",
                "surrogate.uuid.namespace": "6ba7b811-9dad-11d1-80b4-00c04fd430c8",
            },
            "'uuid5' only",
        ),
        (
            {"surrogate.column": "sk", "surrogate.strategy": "custom"},
            "requires surrogate.generator",
        ),
        ({"surrogate.column": "sk", "surrogate.generator": "g"}, "apply to surrogate.strategy"),
        ({"surrogate.column": "sk", "surrogate.generator.prefix": "X"}, "apply to surrogate"),
        ({"surrogate.column": "sk", "surrogate.from": " , "}, "no usable column"),
        ({"surrogate.column": "sk", "surrogate.from": "missing"}, "not in the entity schema"),
        ({"surrogate.column": "sk", "surrogate.from": "SK"}, "must not appear in surrogate.from"),
        ({"surrogate.column": "Customer_ID"}, "must not appear in merge_columns"),
        ({"surrogate.durable_column": "dk"}, "requires an SCD2 entity"),
        ({"surrogate.column": "sk", "surrogate.include_effective_from": "true"}, "SCD2 version"),
        ({"surrogate.column": "sk", "surrogate.require_reproducible": "yes"}, "'true' or 'false'"),
        ({"surrogate.column": "status"}, None),  # a declared STRING column is fine
    ],
)
def test_validation(tags, message):
    if message is None:
        _register(tags)
        return
    with pytest.raises(ValueError, match=message):
        _register(tags)


def test_key_columns_must_differ_case_insensitively():
    with pytest.raises(ValueError, match="must be different columns"):
        _register({**SCD2, "surrogate.column": "sk", "surrogate.durable_column": "SK"})


@pytest.mark.parametrize(
    "tags, message",
    [
        ({"scd.tracked": "status,Version_SK"}, "must not appear in scd.tracked"),
        ({"scd.untracked": "version_sk"}, "must not appear in scd.untracked"),
        ({"surrogate.from": "customer_id,__effective_to"}, "effective_to or is_current"),
        ({"surrogate.from": "customer_id,__is_current"}, "effective_to or is_current"),
        ({"surrogate.from": "customer_id,__effective_from"}, "include_effective_from"),
    ],
)
def test_scd2_key_validation(tags, message):
    schema = StructType(SCHEMA.fields + [StructField("version_sk", StringType(), False)])
    with pytest.raises(ValueError, match=message):
        _register({**SCD2, "surrogate.column": "version_sk", **tags}, schema=schema)


def test_key_column_must_not_be_temporal():
    with pytest.raises(ValueError, match="temporal"):
        _register({**SCD2, "surrogate.column": "__effective_from"}, schema=None)


def test_key_column_must_not_use_reserved_staging_names():
    with pytest.raises(ValueError, match="reserved"):
        _register({**SCD2, "surrogate.column": "__scd2_sk"}, schema=None)


def test_hash_key_declared_with_conflicting_type_fails():
    schema = StructType(SCHEMA.fields + [StructField("sk", LongType(), False)])
    with pytest.raises(ValueError, match="is a STRING"):
        _register({"surrogate.column": "sk"}, schema=schema)


@pytest.mark.parametrize("data_type", [FloatType(), DoubleType(), TimestampNTZType()])
def test_unencodable_from_column_type_fails(data_type):
    schema = StructType([StructField("id", data_type, False)])
    with pytest.raises(ValueError, match="cannot be encoded"):
        _register({"surrogate.column": "sk"}, merge_columns=["id"], schema=schema)


def test_hash_without_inputs_fails():
    with pytest.raises(ValueError, match="need input columns"):
        _register({"surrogate.column": "sk"}, merge_columns=[])


def test_custom_without_inputs_is_allowed():
    _register(
        {"surrogate.column": "sk", "surrogate.strategy": "custom", "surrogate.generator": "g"},
        merge_columns=[],
    )


def test_scd2_version_key_without_sequence_by_warns(caplog):
    with caplog.at_level(logging.WARNING, logger="kindling.data_entities"):
        _register({"scd.type": "2", "surrogate.column": "version_sk"})
    assert any("not reproducible" in record.message for record in caplog.records)


def test_require_reproducible_turns_the_warning_into_an_error():
    with pytest.raises(ValueError, match="require_reproducible=true requires scd.sequence_by"):
        _register(
            {
                "scd.type": "2",
                "surrogate.column": "version_sk",
                "surrogate.require_reproducible": "true",
            }
        )


def test_durable_only_and_sequence_by_keys_do_not_warn(caplog):
    with caplog.at_level(logging.WARNING, logger="kindling.data_entities"):
        _register({"scd.type": "2", "surrogate.durable_column": "dk"})
        _register({**SCD2, "surrogate.column": "version_sk"}, entityid="silver.dim2")
        _register(
            {
                "scd.type": "2",
                "surrogate.column": "version_sk",
                "surrogate.include_effective_from": "false",
            },
            entityid="silver.dim3",
        )
    assert not [r for r in caplog.records if "not reproducible" in r.message]


def test_surrogate_keys_on_a_sql_entity_fail():
    manager = DataEntityManager()
    with pytest.raises(ValueError, match="SQL entity"):
        manager.register_entity(
            "gold.v",
            name="v",
            merge_columns=[],
            tags={"provider_type": "view", "surrogate.column": "sk", "surrogate.from": "a"},
            schema=None,
            sql="SELECT 1 AS a",
        )


def test_type_problems():
    for ok in [
        IntegerType(),
        LongType(),
        StringType(),
        BooleanType(),
        DateType(),
        TimestampType(),
        BinaryType(),
        DecimalType(38, 18),
        DecimalType(38, 36),
    ]:
        assert v1_type_problem(ok) is None, ok
    for bad in [
        FloatType(),
        DoubleType(),
        TimestampNTZType(),
        ArrayType(StringType()),
        MapType(StringType(), StringType()),
        StructType([]),
        DecimalType(38, 37),
    ]:
        assert v1_type_problem(bad), bad


# ── change-column classification ─────────────────────────────────────────────


def _scd2_cfg(**changes):
    base = dict(
        enabled=True,
        tracked_columns=None,
        effective_from_column="__effective_from",
        effective_to_column="__effective_to",
        is_current_column="__is_current",
        current_entity_id="x.current",
        routing_key_method="hash",
    )
    base.update(changes)
    return SCDConfig(**base)


def test_key_columns_are_neither_tracked_nor_untracked():
    columns = ["id", "status", "load_id", "SK"]
    tracked, untracked = scd_change_columns(
        _scd2_cfg(untracked_columns=["load_id"]),
        columns,
        ["id"],
        case_sensitive=False,
        insert_only_columns=["sk"],
    )
    assert tracked == ["status"] and untracked == ["load_id"]


def test_key_columns_stay_out_of_the_touch_set_under_scd_tracked():
    """Under scd.tracked every column outside the allow-list is untracked (and
    updated in place); a key column there would be overwritten."""
    tracked, untracked = scd_change_columns(
        _scd2_cfg(tracked_columns=["status"]),
        ["id", "status", "load_id", "sk"],
        ["id"],
        case_sensitive=False,
        insert_only_columns=["sk"],
    )
    assert tracked == ["status"] and untracked == ["load_id"]


def test_case_sensitive_classification_keeps_differently_cased_columns():
    tracked, _ = scd_change_columns(
        _scd2_cfg(), ["id", "status", "SK"], ["id"], case_sensitive=True, insert_only_columns=["sk"]
    )
    assert tracked == ["status", "SK"]


# ── generator registry and resolution ────────────────────────────────────────


def _gen(ctx):
    return ctx.hash(*ctx.key_columns)


def _other_gen(ctx):
    return ctx.hash(*ctx.key_columns)


def test_register_and_get():
    SurrogateKeyGenerators.register("acme", deterministic=True, output_type="string")(_gen)
    spec = SurrogateKeyGenerators.get("acme")
    assert spec.func is _gen and spec.deterministic and spec.output_type == StringType()
    assert "acme" in SurrogateKeyGenerators.names()


def test_reregistering_the_same_function_replaces_it():
    SurrogateKeyGenerators.register("acme", deterministic=True)(_gen)
    SurrogateKeyGenerators.register("acme", deterministic=True, output_type="bigint")(_gen)
    assert SurrogateKeyGenerators.get("acme").output_type == LongType()


def test_a_different_function_under_a_taken_name_fails():
    SurrogateKeyGenerators.register("acme", deterministic=True)(_gen)
    with pytest.raises(ValueError, match="already registered"):
        SurrogateKeyGenerators.register("acme", deterministic=True)(_other_gen)


def test_register_requires_explicit_determinism_and_known_type():
    with pytest.raises(TypeError):
        SurrogateKeyGenerators.register("x")  # deterministic is keyword-only and required
    with pytest.raises(ValueError, match="output_type"):
        SurrogateKeyGenerators.register("x", deterministic=True, output_type="uuid")
    assert SurrogateKeyGenerators.register("x", deterministic=True, output_type="decimal(20,0)")


def _custom_entity(generator="acme", schema=SCHEMA, **tags):
    return _register(
        {
            "surrogate.column": "sk",
            "surrogate.strategy": "custom",
            "surrogate.generator": generator,
            **tags,
        },
        schema=schema,
    )


def test_generator_names_are_not_resolved_at_registration():
    # The generator module may be imported after the entity module.
    _custom_entity(generator="registered_later")


def test_resolution_of_a_missing_generator_lists_registered_names():
    SurrogateKeyGenerators.register("acme", deterministic=True)(_gen)
    entity = _custom_entity(generator="acme_typo")
    with pytest.raises(ValueError, match=r"Unknown surrogate key generator 'acme_typo'.*'acme'"):
        resolve_surrogate_keys(entity)


def test_non_deterministic_generators_are_rejected_at_resolution():
    SurrogateKeyGenerators.register("rnd", deterministic=False)(_gen)
    with pytest.raises(ValueError, match="deterministic=False.*uuid4"):
        resolve_surrogate_keys(_custom_entity(generator="rnd"))


def test_resolution_checks_the_declared_type_against_the_generator():
    SurrogateKeyGenerators.register("acme", deterministic=True, output_type="bigint")(_gen)
    schema = StructType(SCHEMA.fields + [StructField("sk", StringType(), False)])
    with pytest.raises(ValueError, match="outputs bigint"):
        resolve_surrogate_keys(_custom_entity(schema=schema))


def test_resolved_plan_captures_the_function_and_a_read_only_entity_view():
    SurrogateKeyGenerators.register("acme", deterministic=True)(_gen)
    plan = resolve_surrogate_keys(_custom_entity(**{"surrogate.generator.prefix": "C"}))
    assert plan.generator.func is _gen
    assert plan.keys() == [("sk", "key")]
    assert isinstance(plan.entity, EntityView)
    with pytest.raises(TypeError):
        plan.entity.tags["surrogate.column"] = "x"
    assert dict(plan.config.generator_options) == {"prefix": "C"}


def test_plan_kinds_for_scd2():
    plan = resolve_surrogate_keys(
        _register({**SCD2, "surrogate.column": "vsk", "surrogate.durable_column": "dk"})
    )
    assert plan.scd2
    assert plan.keys() == [("vsk", "version"), ("dk", "durable")]


def test_no_keys_resolves_to_none():
    assert resolve_surrogate_keys(_register({})) is None


# ── schema augmentation ──────────────────────────────────────────────────────


def test_augmentation_adds_non_nullable_key_columns_with_metadata():
    entity = _register({**SCD2, "surrogate.column": "vsk", "surrogate.durable_column": "dk"})
    augmented = augment_schema_for_surrogate_keys(SCHEMA, entity)
    added = {field.name: field for field in augmented.fields[len(SCHEMA.fields) :]}
    assert list(added) == ["vsk", "dk"]
    for field in added.values():
        assert field.dataType == StringType()
        assert field.nullable is False
        assert field.metadata == {"kindling.surrogate": "hash"}


def test_augmentation_keeps_a_declared_key_column():
    schema = StructType(SCHEMA.fields + [StructField("SK", StringType(), True)])
    entity = _register({"surrogate.column": "sk"}, schema=schema)
    assert augment_schema_for_surrogate_keys(schema, entity) == schema


def test_augmentation_uses_the_generator_output_type():
    SurrogateKeyGenerators.register("acme", deterministic=True, output_type="bigint")(_gen)
    augmented = augment_schema_for_surrogate_keys(SCHEMA, _custom_entity())
    assert augmented["sk"].dataType == LongType()
    assert augmented["sk"].metadata == {"kindling.surrogate": "custom"}


def test_augmentation_without_keys_is_identity():
    assert augment_schema_for_surrogate_keys(SCHEMA, _register({})) == SCHEMA


# ── project-wide override through by-tag overlays ────────────────────────────


def test_by_tag_overlay_applies_a_custom_generator_to_tagged_entities():
    """There is no global default generator: a dataentities-bytag: rule sets
    the strategy, generator and options on every entity with a given tag."""
    from unittest.mock import MagicMock

    sections = {
        "dataentities-bytag": {
            "keyed": {
                "acme": {
                    "tags": {
                        "surrogate.strategy": "custom",
                        "surrogate.generator": "acme_key",
                        "surrogate.generator.prefix": "ACME",
                    }
                }
            }
        }
    }
    config_service = MagicMock()
    config_service.get.side_effect = lambda key, default=None: sections.get(key, default)
    manager = DataEntityManager(None, config_service)
    for entityid in ("silver.a", "silver.b"):
        manager.register_entity(
            entityid,
            name=entityid,
            merge_columns=["customer_id"],
            tags={"keyed": "acme", "surrogate.column": "sk"},
            schema=SCHEMA,
        )
    manager.register_entity(
        "silver.c",
        name="c",
        merge_columns=["customer_id"],
        tags={"surrogate.column": "sk"},
        schema=SCHEMA,
    )
    manager.apply_config_overrides(config_service)

    for entityid in ("silver.a", "silver.b"):
        cfg = surrogate_config_from_tags(manager.get_entity_definition(entityid))
        assert cfg.strategy == "custom" and cfg.generator == "acme_key"
        assert dict(cfg.generator_options) == {"prefix": "ACME"}
    assert surrogate_config_from_tags(manager.get_entity_definition("silver.c")).strategy == "hash"


def test_generator_module_is_pickled_by_value_only_inside_the_context(tmp_path):
    """Spark Connect's foreachBatch worker may not be able to import the
    generator's module; while the callback is serialized the function
    travels by value, so a process without the module can still load it."""
    import subprocess
    import sys

    from kindling.surrogate_keys import generator_pickled_by_value
    from pyspark import cloudpickle

    SurrogateKeyGenerators.register("acme", deterministic=True)(_gen)
    plan = resolve_surrogate_keys(_custom_entity())
    module = _gen.__module__
    by_reference = cloudpickle.dumps(_gen)
    with generator_pickled_by_value(plan):
        assert module in cloudpickle.list_registry_pickle_by_value()
        by_value = cloudpickle.dumps(_gen)
    assert module not in cloudpickle.list_registry_pickle_by_value()
    with generator_pickled_by_value(None):
        pass

    def load_elsewhere(payload):
        blob = tmp_path / "payload.pkl"
        blob.write_bytes(payload)
        return subprocess.run(
            [
                sys.executable,
                "-I",
                "-c",
                "import pickle, sys; f = pickle.loads(open(sys.argv[1], 'rb').read()); "
                "print(f.__name__)",
                str(blob),
            ],
            capture_output=True,
            text=True,
            cwd=str(tmp_path),
        )

    assert load_elsewhere(by_reference).returncode != 0, "by reference needs the module"
    loaded = load_elsewhere(by_value)
    assert loaded.returncode == 0, loaded.stderr
    assert loaded.stdout.strip() == "_gen"


# ── bootstrap registration ───────────────────────────────────────────────────


def test_bootstrap_registration_fails_loudly(monkeypatch):
    """Like the watermark aspect: a run that silently skipped keys would write
    rows without them, so a registration failure is an error."""
    from unittest.mock import patch

    from kindling import bootstrap

    with patch("kindling.injection.GlobalInjector.get", side_effect=RuntimeError("unwired")):
        with pytest.raises(RuntimeError, match="unwired"):
            bootstrap._register_surrogate_key_aspect()


def test_bootstrap_registration_connects_the_aspect():
    from unittest.mock import MagicMock, patch

    from kindling import bootstrap
    from kindling.signaling import BlinkerSignalProvider
    from kindling.surrogate_keys import SurrogateKeyAspect

    signals = BlinkerSignalProvider()
    lp = MagicMock()
    aspect = SurrogateKeyAspect(lp=lp, signal_provider=signals)
    with patch("kindling.injection.GlobalInjector.get", return_value=aspect):
        bootstrap._register_surrogate_key_aspect()
    assert signals.get_signal("persist.before_persist").has_receivers_for(object())
