"""Clone and extend for entity and pipe declarations.

See docs/proposals/declaration_derivations.md: one additive, stackable,
last-in-wins mechanism resolved when the source is available, with config
overlays applied on top.
"""

from unittest.mock import MagicMock

import pytest
from kindling.data_entities import DataEntityManager
from kindling.data_pipes import DataPipesManager
from kindling.declaration_derivations import (
    DerivationError,
    EntityDerivation,
    PipeDerivation,
    add_columns_to_schema,
    derivation_entries,
    strip_derivation_keys,
    wrap_execute,
)
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

ORDERS_SCHEMA = StructType(
    [StructField("order_id", StringType(), False), StructField("amount", IntegerType(), True)]
)


def _entities() -> DataEntityManager:
    return DataEntityManager(signal_provider=MagicMock(), config_service=MagicMock())


def _pipes() -> DataPipesManager:
    provider = MagicMock()
    provider.get_logger.return_value = MagicMock()
    return DataPipesManager(provider)


def _config(values):
    service = MagicMock()
    service.get.side_effect = lambda key, default=None: values.get(key, default)
    return service


def _register_orders(manager: DataEntityManager, **overrides):
    params = {
        "name": "orders",
        "merge_columns": ["order_id"],
        "tags": {"tier": "silver", "owner": "core"},
        "schema": ORDERS_SCHEMA,
    }
    params.update(overrides)
    manager.register_entity("silver.orders", **params)


# --------------------------------------------------------------------------- #
# Pure helpers
# --------------------------------------------------------------------------- #


def test_add_columns_accumulates_and_rejects_type_conflicts():
    once = add_columns_to_schema(ORDERS_SCHEMA, [StructField("region", StringType())], "e")
    assert [f.name for f in once.fields] == ["order_id", "amount", "region"]

    again = add_columns_to_schema(once, [{"name": "region", "type": "string"}], "e")
    assert [f.name for f in again.fields] == ["order_id", "amount", "region"]  # no-op

    with pytest.raises(DerivationError, match="cannot be re-added as int"):
        add_columns_to_schema(once, [{"name": "region", "type": "int"}], "e")
    with pytest.raises(DerivationError, match="without a schema"):
        add_columns_to_schema(None, [{"name": "x", "type": "string"}], "e")
    with pytest.raises(DerivationError, match="invalid type"):
        add_columns_to_schema(ORDERS_SCHEMA, [{"name": "x", "type": "nope"}], "e")


def test_wrap_execute_routes_inputs_and_composes():
    calls = []

    def original(silver_orders):
        calls.append(("original", sorted(["silver_orders"])))
        return f"out({silver_orders})"

    def enrich(previous, ref_regions):
        calls.append(("enrich", previous, ref_regions))
        return f"enriched({previous},{ref_regions})"

    wrapped = wrap_execute(original, ["silver.orders"], enrich, ["ref.regions"])
    result = wrapped(silver_orders="O", ref_regions="R")

    assert result == "enriched(out(O),R)"
    assert calls == [("original", ["silver_orders"]), ("enrich", "out(O)", "R")]
    # Positional single-input calls (the streaming fallback) still reach the original;
    # a transform that added no inputs receives just the previous output.
    passthrough = wrap_execute(original, ["silver.orders"], lambda previous: f"p({previous})", [])
    assert passthrough("O") == "p(out(O))"


def test_config_section_split():
    section = {
        "silver.orders": {"tags": {"a": "b"}},
        "silver.copy": {"clone_of": "silver.orders", "tags": {"c": "d"}},
        "silver.*": {"tags": {"tier": "silver"}},
    }
    assert derivation_entries(section) == {"silver.copy": {"clone_of": "silver.orders"}}
    assert strip_derivation_keys(section) == {
        "silver.orders": {"tags": {"a": "b"}},
        "silver.copy": {"tags": {"c": "d"}},
        "silver.*": {"tags": {"tier": "silver"}},
    }
    with pytest.raises(DerivationError, match="exact id"):
        derivation_entries({"silver.*": {"clone_of": "x"}})


# --------------------------------------------------------------------------- #
# Entities
# --------------------------------------------------------------------------- #


def test_clone_uses_the_source_as_a_template_in_any_order():
    manager = _entities()
    # Clone declared before its source: pending, then resolved on registration.
    manager.derive_entity(
        "silver.orders_eu",
        EntityDerivation(
            clone_of="silver.orders",
            tags={"region": "eu"},
            add_columns=(StructField("region", StringType()),),
            overrides={"name": "orders_eu"},
        ),
    )
    assert manager.pending_derivations() == {
        "silver.orders_eu": "waiting for 'silver.orders' to be registered"
    }
    with pytest.raises(DerivationError, match="waiting for 'silver.orders'"):
        manager.get_entity_definition("silver.orders_eu")

    _register_orders(manager)

    assert manager.pending_derivations() == {}
    clone = manager.get_entity_definition("silver.orders_eu")
    assert clone.name == "orders_eu"
    assert clone.merge_columns == ["order_id"]
    assert clone.tags == {"tier": "silver", "owner": "core", "region": "eu"}
    assert [f.name for f in clone.schema.fields] == ["order_id", "amount", "region"]
    # The source is untouched.
    source = manager.get_entity_definition("silver.orders")
    assert [f.name for f in source.schema.fields] == ["order_id", "amount"]
    assert set(manager.get_entity_ids()) == {"silver.orders", "silver.orders_eu"}


def test_extensions_stack_additively_last_in_wins():
    manager = _entities()
    _register_orders(manager)
    manager.derive_entity(
        "silver.orders",
        EntityDerivation(
            tags={"owner": "sales"}, add_columns=(StructField("region", StringType()),)
        ),
    )
    manager.derive_entity(
        "silver.orders",
        EntityDerivation(
            tags={"owner": "finance", "sla": "gold"},
            add_columns=(StructField("region", StringType()), StructField("zone", StringType())),
            add_partition_columns=("region",),
        ),
    )

    entity = manager.get_entity_definition("silver.orders")
    assert entity.tags == {"tier": "silver", "owner": "finance", "sla": "gold"}
    assert [f.name for f in entity.schema.fields] == ["order_id", "amount", "region", "zone"]
    assert entity.partition_columns == ["region"]


def test_extension_before_registration_is_pending_then_applied():
    manager = _entities()
    manager.derive_entity("silver.orders", EntityDerivation(tags={"owner": "sales"}))
    assert "silver.orders" in manager.pending_derivations()

    _register_orders(manager)

    assert manager.get_entity_definition("silver.orders").tags["owner"] == "sales"
    assert manager.pending_derivations() == {}


def test_clone_copies_source_extensions_but_not_config_overlays():
    manager = _entities()
    _register_orders(manager)
    manager.derive_entity("silver.orders", EntityDerivation(tags={"ext": "yes"}))
    manager.derive_entity("silver.copy", EntityDerivation(clone_of="silver.orders"))
    manager.apply_config_overrides(
        _config({"dataentities": {"silver.orders": {"tags": {"overlay": "source-only"}}}})
    )

    copy = manager.get_entity_definition("silver.copy")
    assert copy.tags["ext"] == "yes"
    assert "overlay" not in copy.tags
    assert manager.get_entity_definition("silver.orders").tags["overlay"] == "source-only"


def test_config_overlays_target_the_clone_by_its_own_id_and_config_can_clone():
    manager = _entities()
    _register_orders(manager)
    manager.apply_config_overrides(
        _config(
            {
                "dataentities": {
                    "silver.orders_dev": {
                        "clone_of": "silver.orders",
                        "add_columns": [{"name": "debug", "type": "string"}],
                        "tags": {"provider.table_name": "dev.orders"},
                    }
                }
            }
        )
    )

    clone = manager.get_entity_definition("silver.orders_dev")
    assert clone.tags["provider.table_name"] == "dev.orders"
    assert [f.name for f in clone.schema.fields] == ["order_id", "amount", "debug"]
    # Re-applying is idempotent: config-sourced derivations are replaced, not stacked.
    manager.apply_config_overrides(
        _config({"dataentities": {"silver.orders_dev": {"clone_of": "silver.orders"}}})
    )
    clone = manager.get_entity_definition("silver.orders_dev")
    assert [f.name for f in clone.schema.fields] == ["order_id", "amount"]


def test_clone_conflicts_and_cycles_are_errors():
    manager = _entities()
    _register_orders(manager)
    with pytest.raises(DerivationError, match="cannot be cloned from itself"):
        manager.derive_entity("silver.orders", EntityDerivation(clone_of="silver.orders"))
    with pytest.raises(DerivationError, match="registered directly"):
        manager.derive_entity("silver.orders", EntityDerivation(clone_of="bronze.x"))
    manager.derive_entity("a", EntityDerivation(clone_of="b"))
    with pytest.raises(DerivationError, match="cycle"):
        manager.derive_entity("b", EntityDerivation(clone_of="a"))
    manager.derive_entity("silver.copy", EntityDerivation(clone_of="silver.orders"))
    with pytest.raises(DerivationError, match="declared as a clone"):
        manager.register_entity("silver.copy", name="x", merge_columns=[], tags={}, schema=None)


def test_extension_may_not_replace_clone_only_fields():
    manager = _entities()
    _register_orders(manager)
    with pytest.raises(DerivationError, match="Clone it instead"):
        manager.derive_entity("silver.orders", EntityDerivation(overrides={"name": "renamed"}))
    with pytest.raises(DerivationError, match="cannot be overridden"):
        manager.derive_entity(
            "silver.copy", EntityDerivation(clone_of="silver.orders", overrides={"schema": None})
        )


def test_scd2_companion_is_derived_for_a_clone():
    manager = _entities()
    _register_orders(manager)
    manager.derive_entity(
        "silver.orders_hist",
        EntityDerivation(clone_of="silver.orders", tags={"scd.type": "2"}),
    )
    assert "silver.orders_hist.current" in manager.get_entity_ids()
    assert "silver.orders.current" not in manager.get_entity_ids()


# --------------------------------------------------------------------------- #
# Pipes
# --------------------------------------------------------------------------- #


def _register_build(manager: DataPipesManager):
    def build(silver_orders):
        return f"built({silver_orders})"

    manager.register_pipe(
        "silver.build_orders",
        name="Build Orders",
        execute=build,
        tags={"tier": "silver"},
        input_entity_ids=["silver.orders"],
        output_entity_id="gold.orders",
        output_type="delta",
    )


def test_pipe_clone_retargets_output_and_wraps_transform():
    manager = _pipes()
    manager.derive_pipe(
        "silver.enrich_orders",
        PipeDerivation(
            clone_of="silver.build_orders",
            add_inputs=("ref.regions",),
            transform=lambda previous, ref_regions: f"enriched({previous},{ref_regions})",
            overrides={"output_entity_id": "gold.orders_enriched", "name": "Enrich"},
        ),
    )
    _register_build(manager)

    clone = manager.get_pipe_definition("silver.enrich_orders")
    assert clone.output_entity_id == "gold.orders_enriched"
    assert clone.input_entity_ids == ["silver.orders", "ref.regions"]
    assert clone.execute(silver_orders="O", ref_regions="R") == "enriched(built(O),R)"
    original = manager.get_pipe_definition("silver.build_orders")
    assert original.output_entity_id == "gold.orders"
    assert original.execute(silver_orders="O") == "built(O)"


def test_pipe_extensions_stack_with_the_last_transform_outermost():
    manager = _pipes()
    _register_build(manager)
    manager.derive_pipe(
        "silver.build_orders",
        PipeDerivation(tags={"sla": "silver"}, transform=lambda previous: f"a({previous})"),
    )
    manager.derive_pipe(
        "silver.build_orders",
        PipeDerivation(
            tags={"sla": "gold"},
            add_inputs=("ref.fx",),
            transform=lambda previous, ref_fx: f"b({previous},{ref_fx})",
        ),
    )

    pipe = manager.get_pipe_definition("silver.build_orders")
    assert pipe.tags == {"tier": "silver", "sla": "gold"}
    assert pipe.input_entity_ids == ["silver.orders", "ref.fx"]
    assert pipe.execute(silver_orders="O", ref_fx="F") == "b(a(built(O)),F)"


def test_pipe_extension_may_not_redirect_output_and_config_can_clone_pipes():
    manager = _pipes()
    _register_build(manager)
    with pytest.raises(DerivationError, match="Clone it instead"):
        manager.derive_pipe(
            "silver.build_orders", PipeDerivation(overrides={"output_entity_id": "x"})
        )

    manager.apply_config_overrides(
        _config(
            {
                "datapipes": {
                    "silver.build_orders_canary": {
                        "clone_of": "silver.build_orders",
                        "output_entity_id": "gold.orders_canary",
                        "add_inputs": ["ref.regions"],
                    }
                }
            }
        )
    )
    canary = manager.get_pipe_definition("silver.build_orders_canary")
    assert canary.output_entity_id == "gold.orders_canary"
    assert canary.input_entity_ids == ["silver.orders", "ref.regions"]
    assert canary.execute(silver_orders="O", ref_regions="R") == "built(O)"
    with pytest.raises(DerivationError, match="applies to entities"):
        manager.apply_config_overrides(
            _config({"datapipes": {"p": {"clone_of": "silver.build_orders", "add_columns": []}}})
        )


# --------------------------------------------------------------------------- #
# Atomicity, config/code conflicts, hot reload, secrets
# --------------------------------------------------------------------------- #


def test_registering_a_source_that_breaks_a_pending_clone_commits_nothing():
    manager = _entities()
    manager.derive_entity(
        "silver.copy",
        EntityDerivation(
            clone_of="silver.orders", add_columns=({"name": "amount", "type": "string"},)
        ),
    )

    with pytest.raises(DerivationError, match="cannot be re-added as string"):
        _register_orders(manager)  # amount is an int in the source schema

    assert "silver.orders" not in manager.get_entity_ids()
    assert "silver.orders" not in manager._raw_params
    assert "silver.copy" in manager.pending_derivations()
    # Dropping the bad clone lets the source register normally.
    manager._derivations.pop("silver.copy")
    manager._pending.pop("silver.copy")
    _register_orders(manager)
    assert "silver.orders" in manager.get_entity_ids()


def test_config_clone_may_not_compete_with_a_code_clone():
    manager = _entities()
    _register_orders(manager)
    manager.register_entity("bronze.other", name="o", merge_columns=[], tags={}, schema=None)
    manager.derive_entity("silver.copy", EntityDerivation(clone_of="silver.orders"))

    with pytest.raises(DerivationError, match="code already clones it"):
        manager.apply_config_overrides(
            _config({"dataentities": {"silver.copy": {"clone_of": "bronze.other"}}})
        )


def test_config_only_clone_disappears_on_hot_reload():
    manager = _entities()
    _register_orders(manager)
    manager.apply_config_overrides(
        _config({"dataentities": {"silver.copy": {"clone_of": "silver.orders"}}})
    )
    assert "silver.copy" in manager.get_entity_ids()

    manager.apply_config_overrides(_config({"dataentities": {}}))

    assert "silver.copy" not in manager.get_entity_ids()
    assert manager.pending_derivations() == {}


def test_secret_references_in_derivation_tags_are_resolved(monkeypatch):
    monkeypatch.setattr(
        "kindling.config_loaders.resolve_secret_value", lambda value, provider: "RESOLVED"
    )
    manager = _entities()
    _register_orders(manager)
    manager.derive_entity(
        "silver.orders", EntityDerivation(tags={"provider.token": "@secret:scope:key"})
    )
    manager.derive_entity(
        "silver.copy",
        EntityDerivation(clone_of="silver.orders", tags={"provider.other": "@secret:scope:k2"}),
    )

    failures = manager.resolve_secret_tags(secret_provider=object())

    assert failures == []
    assert manager.get_entity_definition("silver.orders").tags["provider.token"] == "RESOLVED"
    copy = manager.get_entity_definition("silver.copy")
    assert copy.tags["provider.token"] == "RESOLVED"
    assert copy.tags["provider.other"] == "RESOLVED"


def test_pipe_cascade_is_atomic_and_config_clone_removal_cleans_up():
    manager = _pipes()
    manager.derive_pipe(
        "silver.bad_clone",
        PipeDerivation(clone_of="silver.build_orders", overrides={"schema": "nope"}),
    )
    with pytest.raises(DerivationError, match="cannot be overridden"):
        _register_build(manager)
    assert manager.get_pipe_ids() == []
    assert "silver.build_orders" not in manager._raw_params

    manager._derivations.pop("silver.bad_clone")
    manager._pending.pop("silver.bad_clone")
    _register_build(manager)
    manager.apply_config_overrides(
        _config({"datapipes": {"silver.canary": {"clone_of": "silver.build_orders"}}})
    )
    assert "silver.canary" in manager.get_pipe_ids()
    manager.apply_config_overrides(_config({"datapipes": {}}))
    assert "silver.canary" not in manager.get_pipe_ids()


def test_pipe_derivation_tag_secrets_are_resolved(monkeypatch):
    monkeypatch.setattr(
        "kindling.config_loaders.resolve_secret_value", lambda value, provider: "RESOLVED"
    )
    manager = _pipes()
    _register_build(manager)
    manager.derive_pipe("silver.build_orders", PipeDerivation(tags={"token": "@secret:s:k"}))

    assert manager.resolve_secret_tags(secret_provider=object()) == []
    assert manager.get_pipe_definition("silver.build_orders").tags["token"] == "RESOLVED"


def test_wrap_execute_retries_single_input_originals_positionally():
    def transform(df):  # single positional input, the streaming-compatible shape
        return f"t({df})"

    wrapped = wrap_execute(
        transform, ["silver.orders"], lambda previous, ref_fx: f"{previous}+{ref_fx}", ["ref.fx"]
    )
    assert wrapped(silver_orders="O", ref_fx="F") == "t(O)+F"

    def two_inputs(a, b):
        return a + b

    strict = wrap_execute(two_inputs, ["x.a", "x.b"], lambda previous: previous, [])
    with pytest.raises(TypeError):
        strict(x_a="1", nope="2")


def test_extending_an_scd2_entity_refreshes_its_companion():
    manager = _entities()
    _register_orders(manager, tags={"tier": "silver", "scd.type": "2"})
    assert "silver.orders.current" in manager.get_entity_ids()

    manager.derive_entity(
        "silver.orders", EntityDerivation(add_columns=(StructField("region", StringType()),))
    )

    companion = manager.get_entity_definition("silver.orders.current")
    assert [f.name for f in companion.schema.fields] == ["order_id", "amount", "region"]

    manager.derive_entity(
        "silver.orders", EntityDerivation(tags={"scd.current_entity_id": "silver.orders.latest"})
    )
    assert "silver.orders.latest" in manager.get_entity_ids()
    assert "silver.orders.current" not in manager.get_entity_ids()


def test_unregistering_a_source_pipe_invalidates_its_clones():
    manager = _pipes()
    _register_build(manager)
    manager.derive_pipe("silver.copy", PipeDerivation(clone_of="silver.build_orders"))
    manager.derive_pipe("silver.copy2", PipeDerivation(clone_of="silver.copy"))
    assert set(manager.get_pipe_ids()) == {"silver.build_orders", "silver.copy", "silver.copy2"}

    manager.unregister_pipe("silver.build_orders")

    assert manager.get_pipe_ids() == []
    assert set(manager.pending_derivations()) == {"silver.copy", "silver.copy2"}
    with pytest.raises(DerivationError, match="waiting for 'silver.build_orders'"):
        manager.get_pipe_definition("silver.copy")
    _register_build(manager)  # re-registering the template resolves the chain again
    assert set(manager.get_pipe_ids()) == {"silver.build_orders", "silver.copy", "silver.copy2"}


def test_failed_reregistration_keeps_the_previous_declaration():
    manager = _entities()
    _register_orders(manager)
    manager.derive_entity(
        "silver.copy",
        EntityDerivation(
            clone_of="silver.orders", add_columns=({"name": "amount", "type": "int"},)
        ),
    )
    incompatible = StructType(
        [StructField("order_id", StringType()), StructField("amount", StringType())]
    )

    with pytest.raises(DerivationError, match="cannot be re-added as int"):
        _register_orders(manager, schema=incompatible)

    # The prior valid declaration survives for the source and its clone alike.
    assert manager._raw_params["silver.orders"]["schema"] == ORDERS_SCHEMA
    assert [f.name for f in manager.get_entity_definition("silver.copy").schema.fields] == [
        "order_id",
        "amount",
    ]


def test_wrap_execute_never_reruns_a_body_that_raised_type_error():
    calls = []

    def flaky(silver_orders):
        calls.append(silver_orders)
        raise TypeError("inside the pipe body")

    wrapped = wrap_execute(flaky, ["silver.orders"], lambda previous: previous, [])
    with pytest.raises(TypeError, match="inside the pipe body"):
        wrapped(silver_orders="O")
    assert calls == ["O"]  # executed once, not retried positionally


def test_clone_pending_again_after_reload_is_removed_from_the_registry():
    manager = _entities()
    _register_orders(manager)
    manager.register_entity("bronze.other", name="o", merge_columns=[], tags={}, schema=None)
    manager.apply_config_overrides(
        _config({"dataentities": {"silver.copy": {"clone_of": "bronze.missing"}}})
    )
    assert "silver.copy" in manager.pending_derivations()
    assert "silver.copy" not in manager.get_entity_ids()

    pipes = _pipes()
    _register_build(pipes)
    pipes.derive_pipe("silver.copy", PipeDerivation(clone_of="silver.build_orders"))
    pipes.unregister_pipe("silver.build_orders")
    assert "silver.copy" not in pipes.get_pipe_ids()
