"""Unit tests for the datapipes:/dataentities: config override overlay (gh#30).

Covers DataPipesManager.apply_config_overrides,
DataEntityManager.apply_config_overrides (including SCD2 companion
convergence), overlay-at-registration through the persisted matcher, and
the bootstrap.apply_config_overrides() helper.
"""

from unittest.mock import MagicMock, Mock

import pytest

from kindling.data_entities import DataEntityManager, EntityMetadata
from kindling.data_pipes import DataPipesManager, PipeMetadata


def make_pipes_manager():
    logger_provider = Mock()
    logger_provider.get_logger.return_value = Mock()
    return DataPipesManager(logger_provider)


def make_config_service(sections):
    """Stub ConfigService returning top-level sections by key."""
    config_service = MagicMock()
    config_service.get.side_effect = lambda key, default=None: sections.get(key, default)
    return config_service


def sample_execute(df):
    return df


def register_sample_pipe(manager, pipeid="bronze.ingest_orders", **overrides):
    params = {
        "name": "Ingest Orders",
        "execute": sample_execute,
        "tags": {"domain": "sales"},
        "input_entity_ids": ["raw.orders"],
        "output_entity_id": "bronze.orders",
        "output_type": "delta",
    }
    params.update(overrides)
    manager.register_pipe(pipeid, **params)
    return params


class TestPipeConfigOverlay:
    """DataPipesManager.apply_config_overrides"""

    def test_patterns_merge_tags_and_replace_scalars(self):
        manager = make_pipes_manager()
        register_sample_pipe(manager)
        config_service = make_config_service(
            {
                "datapipes": {
                    "bronze.*": {"tags": {"layer": "bronze"}},
                    "bronze.ingest_orders": {"output_type": "parquet"},
                }
            }
        )

        manager.apply_config_overrides(config_service)

        pipe = manager.get_pipe_definition("bronze.ingest_orders")
        assert pipe.tags == {"domain": "sales", "layer": "bronze"}
        assert pipe.output_type == "parquet"

    def test_execute_callable_preserved_identically(self):
        manager = make_pipes_manager()
        register_sample_pipe(manager)
        config_service = make_config_service(
            {"datapipes": {"**": {"tags": {"managed": "true"}, "execute": "clobber"}}}
        )

        manager.apply_config_overrides(config_service)

        assert manager.get_pipe_definition("bronze.ingest_orders").execute is sample_execute

    def test_name_output_type_use_watermark_overridable(self):
        manager = make_pipes_manager()
        register_sample_pipe(manager)
        config_service = make_config_service(
            {
                "datapipes": {
                    "bronze.ingest_orders": {
                        "name": "Overridden Name",
                        "output_type": "memory",
                        "use_watermark": True,
                        "input_entity_ids": ["raw.orders_v2"],
                    }
                }
            }
        )

        manager.apply_config_overrides(config_service)

        pipe = manager.get_pipe_definition("bronze.ingest_orders")
        assert pipe.name == "Overridden Name"
        assert pipe.output_type == "memory"
        assert pipe.use_watermark is True
        assert pipe.input_entity_ids == ["raw.orders_v2"]

    def test_unknown_and_underscore_keys_are_inert(self):
        baseline_manager = make_pipes_manager()
        register_sample_pipe(baseline_manager)
        baseline_manager.apply_config_overrides(make_config_service({}))
        baseline = baseline_manager.get_pipe_definition("bronze.ingest_orders")

        manager = make_pipes_manager()
        register_sample_pipe(manager)
        config_service = make_config_service(
            {
                "datapipes": {
                    "bronze.ingest_orders": {
                        "_enabled": False,
                        "_remove_tags": ["debug"],
                        "not_a_metadata_field": 42,
                    }
                }
            }
        )

        manager.apply_config_overrides(config_service)

        assert manager.get_pipe_definition("bronze.ingest_orders") == baseline
        debug_calls = [
            str(call) for call in manager.logger.debug.call_args_list if "not applied" in str(call)
        ]
        assert debug_calls, "dropped config keys should be debug-logged"
        assert "_enabled" in debug_calls[0]
        assert "not_a_metadata_field" in debug_calls[0]

    def test_reapply_is_idempotent(self):
        manager = make_pipes_manager()
        register_sample_pipe(manager)
        config_service = make_config_service(
            {"datapipes": {"bronze.*": {"tags": {"layer": "bronze"}, "name": "Renamed"}}}
        )

        manager.apply_config_overrides(config_service)
        first = manager.get_pipe_definition("bronze.ingest_orders")
        manager.apply_config_overrides(config_service)
        second = manager.get_pipe_definition("bronze.ingest_orders")

        assert first == second

    def test_config_change_reresolves_from_raw_without_accumulation(self):
        manager = make_pipes_manager()
        register_sample_pipe(manager)

        manager.apply_config_overrides(
            make_config_service({"datapipes": {"bronze.*": {"tags": {"layer": "bronze"}}}})
        )
        manager.apply_config_overrides(
            make_config_service({"datapipes": {"bronze.*": {"tags": {"sla": "4h"}}}})
        )

        pipe = manager.get_pipe_definition("bronze.ingest_orders")
        # 'layer' came from the first overlay only; re-resolution starts
        # from the raw decorator params, so it must not survive.
        assert pipe.tags == {"domain": "sales", "sla": "4h"}

    def test_registration_after_apply_is_overlaid(self):
        manager = make_pipes_manager()
        manager.apply_config_overrides(
            make_config_service({"datapipes": {"bronze.*": {"tags": {"layer": "bronze"}}}})
        )

        register_sample_pipe(manager, pipeid="bronze.ingest_items")

        pipe = manager.get_pipe_definition("bronze.ingest_items")
        assert pipe.tags == {"domain": "sales", "layer": "bronze"}

    def test_missing_or_empty_section_is_noop(self):
        for section in ({}, {"datapipes": {}}):
            manager = make_pipes_manager()
            params = register_sample_pipe(manager)
            before = manager.get_pipe_definition("bronze.ingest_orders")

            manager.apply_config_overrides(make_config_service(section))

            pipe = manager.get_pipe_definition("bronze.ingest_orders")
            assert pipe == before
            assert pipe.execute is params["execute"]

    def test_registration_without_matcher_matches_legacy_behavior(self):
        manager = make_pipes_manager()
        register_sample_pipe(manager)

        pipe = manager.get_pipe_definition("bronze.ingest_orders")
        assert pipe == PipeMetadata(
            pipeid="bronze.ingest_orders",
            name="Ingest Orders",
            execute=sample_execute,
            tags={"domain": "sales"},
            input_entity_ids=["raw.orders"],
            output_entity_id="bronze.orders",
            output_type="delta",
        )


class TestPipeTagBasedConfigOverlay:
    """DataPipesManager.apply_config_overrides via datapipes-bytag:

    General-purpose counterpart to datapipes:, matching by the pipe's own
    declared tag VALUE instead of its id.
    """

    def test_tag_rule_merges_tags_for_matching_pipe(self):
        manager = make_pipes_manager()
        register_sample_pipe(manager, tags={"domain": "sales", "criticality": "high"})
        config_service = make_config_service(
            {"datapipes-bytag": {"criticality": {"high": {"tags": {"pager": "on-call"}}}}}
        )

        manager.apply_config_overrides(config_service)

        pipe = manager.get_pipe_definition("bronze.ingest_orders")
        assert pipe.tags == {"domain": "sales", "criticality": "high", "pager": "on-call"}

    def test_tag_rule_does_not_apply_to_non_matching_pipe(self):
        manager = make_pipes_manager()
        register_sample_pipe(manager, tags={"domain": "sales", "criticality": "low"})
        config_service = make_config_service(
            {"datapipes-bytag": {"criticality": {"high": {"tags": {"pager": "on-call"}}}}}
        )

        manager.apply_config_overrides(config_service)

        pipe = manager.get_pipe_definition("bronze.ingest_orders")
        assert pipe.tags == {"domain": "sales", "criticality": "low"}

    def test_tag_rule_can_set_any_overridable_field_not_just_tags(self):
        manager = make_pipes_manager()
        register_sample_pipe(manager, tags={"criticality": "high"})
        config_service = make_config_service(
            {"datapipes-bytag": {"criticality": {"high": {"output_type": "memory"}}}}
        )

        manager.apply_config_overrides(config_service)

        assert manager.get_pipe_definition("bronze.ingest_orders").output_type == "memory"

    def test_id_glob_pattern_overrides_broader_tag_based_default(self):
        manager = make_pipes_manager()
        register_sample_pipe(manager, tags={"criticality": "high"})
        config_service = make_config_service(
            {
                "datapipes-bytag": {"criticality": {"high": {"output_type": "memory"}}},
                "datapipes": {"bronze.ingest_orders": {"output_type": "parquet"}},
            }
        )

        manager.apply_config_overrides(config_service)

        assert manager.get_pipe_definition("bronze.ingest_orders").output_type == "parquet"

    def test_wildcard_tag_value_matches(self):
        manager = make_pipes_manager()
        register_sample_pipe(manager, tags={"criticality": "high-p1"})
        config_service = make_config_service(
            {"datapipes-bytag": {"criticality": {"high*": {"tags": {"pager": "on-call"}}}}}
        )

        manager.apply_config_overrides(config_service)

        pipe = manager.get_pipe_definition("bronze.ingest_orders")
        assert pipe.tags["pager"] == "on-call"

    def test_execute_never_overridable_via_tag_rule(self):
        manager = make_pipes_manager()
        register_sample_pipe(manager, tags={"criticality": "high"})
        config_service = make_config_service(
            {"datapipes-bytag": {"criticality": {"high": {"execute": "clobber"}}}}
        )

        manager.apply_config_overrides(config_service)

        assert manager.get_pipe_definition("bronze.ingest_orders").execute is sample_execute

    def test_no_tag_rule_section_is_a_no_op(self):
        manager = make_pipes_manager()
        register_sample_pipe(manager, tags={"criticality": "high"})
        config_service = make_config_service({})

        manager.apply_config_overrides(config_service)

        assert manager.get_pipe_definition("bronze.ingest_orders").tags == {"criticality": "high"}


def make_entity_manager(config_service=None):
    signal_provider = MagicMock()
    signal_provider.create_signal.return_value = MagicMock()
    return DataEntityManager(signal_provider, config_service)


def register_sample_entity(manager, entityid="bronze.orders", **overrides):
    params = {
        "name": "orders",
        "merge_columns": ["order_id"],
        "tags": {"layer": "bronze"},
        "schema": None,
    }
    params.update(overrides)
    manager.register_entity(entityid, **params)
    return params


def emitted_signals(manager):
    return [call.args[0] for call in manager.emit.call_args_list]


class TestEntityConfigOverlay:
    """DataEntityManager.apply_config_overrides"""

    def test_patterns_merge_tags_and_replace_lists(self):
        manager = make_entity_manager()
        register_sample_entity(manager)
        config_service = make_config_service(
            {
                "dataentities": {
                    "bronze.*": {"tags": {"team": "core"}},
                    "bronze.orders": {
                        "merge_columns": ["order_id", "region"],
                        "partition_columns": ["date"],
                        "cluster_columns": ["region"],
                        "name": "orders_renamed",
                    },
                }
            }
        )

        manager.apply_config_overrides(config_service)

        entity = manager.get_entity_definition("bronze.orders")
        assert entity.tags == {"layer": "bronze", "team": "core"}
        assert entity.merge_columns == ["order_id", "region"]
        assert entity.partition_columns == ["date"]
        assert entity.cluster_columns == ["region"]
        assert entity.name == "orders_renamed"

    def test_schema_and_sql_never_overridable(self):
        manager = make_entity_manager()
        schema_sentinel = object()
        register_sample_entity(manager, schema=schema_sentinel)
        config_service = make_config_service(
            {"dataentities": {"bronze.orders": {"schema": "clobber", "sql": "SELECT 1"}}}
        )

        manager.apply_config_overrides(config_service)

        entity = manager.get_entity_definition("bronze.orders")
        assert entity.schema is schema_sentinel
        assert entity.sql is None

    def test_unknown_and_underscore_keys_are_inert(self):
        manager = make_entity_manager()
        register_sample_entity(manager)
        baseline = manager.get_entity_definition("bronze.orders")
        config_service = make_config_service(
            {"dataentities": {"bronze.orders": {"_enabled": False, "mystery": "x"}}}
        )

        manager.apply_config_overrides(config_service)

        assert manager.get_entity_definition("bronze.orders") == baseline

    def test_reapply_reresolves_from_raw_without_accumulation(self):
        manager = make_entity_manager()
        register_sample_entity(manager)

        manager.apply_config_overrides(
            make_config_service({"dataentities": {"bronze.*": {"tags": {"team": "core"}}}})
        )
        manager.apply_config_overrides(
            make_config_service({"dataentities": {"bronze.*": {"tags": {"sla": "4h"}}}})
        )

        entity = manager.get_entity_definition("bronze.orders")
        assert entity.tags == {"layer": "bronze", "sla": "4h"}

    def test_registration_after_apply_is_overlaid(self):
        manager = make_entity_manager()
        manager.apply_config_overrides(
            make_config_service({"dataentities": {"bronze.*": {"tags": {"team": "core"}}}})
        )

        register_sample_entity(manager, entityid="bronze.items")

        entity = manager.get_entity_definition("bronze.items")
        assert entity.tags == {"layer": "bronze", "team": "core"}

    def test_missing_section_is_noop(self):
        manager = make_entity_manager()
        register_sample_entity(manager)
        before = manager.get_entity_definition("bronze.orders")

        manager.apply_config_overrides(make_config_service({}))

        assert manager.get_entity_definition("bronze.orders") == before

    def test_invalid_overlay_raises_at_apply_with_entity_id(self):
        manager = make_entity_manager()
        register_sample_entity(manager, merge_columns=[])
        config_service = make_config_service(
            {"dataentities": {"bronze.orders": {"tags": {"write.mode": "insert"}}}}
        )

        with pytest.raises(ValueError) as exc_info:
            manager.apply_config_overrides(config_service)

        message = str(exc_info.value)
        assert "bronze.orders" in message
        assert "Config overrides" in message
        assert "merge_columns" in str(exc_info.value.__cause__)

    def test_invalid_overlay_raises_at_registration_too(self):
        manager = make_entity_manager()
        manager.apply_config_overrides(
            make_config_service(
                {"dataentities": {"bronze.orders": {"tags": {"write.mode": "insert"}}}}
            )
        )

        with pytest.raises(ValueError, match="Config overrides"):
            register_sample_entity(manager, merge_columns=[])

    def test_invalid_raw_params_not_blamed_on_config(self):
        manager = make_entity_manager()
        manager.apply_config_overrides(
            make_config_service({"dataentities": {"other.*": {"tags": {"team": "core"}}}})
        )

        with pytest.raises(ValueError) as exc_info:
            register_sample_entity(manager, merge_columns=[], tags={"write.mode": "insert"})

        assert "Config overrides" not in str(exc_info.value)


class TestEntityTagBasedConfigOverlay:
    """DataEntityManager.apply_config_overrides via dataentities-bytag:

    General-purpose counterpart to dataentities:, matching by the entity's
    own declared tag VALUE instead of its id.
    """

    def test_tag_rule_merges_tags_for_matching_entity(self):
        manager = make_entity_manager()
        register_sample_entity(
            manager, entityid="staging.device_telemetry", tags={"tier": "bronze"}
        )
        config_service = make_config_service(
            {
                "dataentities-bytag": {
                    "tier": {"bronze": {"tags": {"provider.table_catalog": "dev_bronze"}}}
                }
            }
        )

        manager.apply_config_overrides(config_service)

        entity = manager.get_entity_definition("staging.device_telemetry")
        assert entity.tags == {"tier": "bronze", "provider.table_catalog": "dev_bronze"}

    def test_tag_rule_does_not_apply_to_non_matching_entity(self):
        manager = make_entity_manager()
        register_sample_entity(manager, entityid="staging.device_telemetry", tags={"tier": "gold"})
        config_service = make_config_service(
            {
                "dataentities-bytag": {
                    "tier": {"bronze": {"tags": {"provider.table_catalog": "dev_bronze"}}}
                }
            }
        )

        manager.apply_config_overrides(config_service)

        entity = manager.get_entity_definition("staging.device_telemetry")
        assert entity.tags == {"tier": "gold"}

    def test_tag_rule_can_set_any_overridable_field_not_just_tags(self):
        manager = make_entity_manager()
        register_sample_entity(manager, entityid="staging.device_telemetry", tags={"tier": "gold"})
        config_service = make_config_service(
            {
                "dataentities-bytag": {
                    "tier": {"gold": {"partition_columns": ["date"], "name": "renamed_by_tag"}}
                }
            }
        )

        manager.apply_config_overrides(config_service)

        entity = manager.get_entity_definition("staging.device_telemetry")
        assert entity.partition_columns == ["date"]
        assert entity.name == "renamed_by_tag"

    def test_id_glob_pattern_overrides_broader_tag_based_default(self):
        """dataentities-bytag: applies first (broad default); dataentities:
        applies on top (specific override) for the same field."""
        manager = make_entity_manager()
        register_sample_entity(
            manager, entityid="staging.device_telemetry", tags={"tier": "bronze"}
        )
        config_service = make_config_service(
            {
                "dataentities-bytag": {
                    "tier": {"bronze": {"tags": {"provider.table_catalog": "dev_bronze"}}}
                },
                "dataentities": {
                    "staging.device_telemetry": {"tags": {"provider.table_catalog": "special_cat"}}
                },
            }
        )

        manager.apply_config_overrides(config_service)

        entity = manager.get_entity_definition("staging.device_telemetry")
        assert entity.tags["provider.table_catalog"] == "special_cat"

    def test_schema_and_sql_never_overridable_via_tag_rule(self):
        manager = make_entity_manager()
        schema_sentinel = object()
        register_sample_entity(
            manager,
            entityid="staging.device_telemetry",
            tags={"tier": "gold"},
            schema=schema_sentinel,
        )
        config_service = make_config_service(
            {"dataentities-bytag": {"tier": {"gold": {"schema": "clobber", "sql": "SELECT 1"}}}}
        )

        manager.apply_config_overrides(config_service)

        entity = manager.get_entity_definition("staging.device_telemetry")
        assert entity.schema is schema_sentinel
        assert entity.sql is None

    def test_invalid_metadata_from_tag_rule_is_blamed_on_config(self):
        manager = make_entity_manager()
        manager.apply_config_overrides(
            make_config_service(
                {"dataentities-bytag": {"tier": {"bronze": {"tags": {"write.mode": "insert"}}}}}
            )
        )

        with pytest.raises(ValueError, match="Config overrides"):
            register_sample_entity(
                manager,
                entityid="staging.device_telemetry",
                merge_columns=[],
                tags={"tier": "bronze"},
            )

    def test_no_tag_rule_section_is_a_no_op(self):
        manager = make_entity_manager()
        register_sample_entity(
            manager, entityid="staging.device_telemetry", tags={"tier": "bronze"}
        )
        config_service = make_config_service({})

        manager.apply_config_overrides(config_service)

        entity = manager.get_entity_definition("staging.device_telemetry")
        assert entity.tags == {"tier": "bronze"}


class TestScd2CompanionConvergence:
    """SCD2 current-row companions re-derive from overlaid bases."""

    SCD_TAGS = {"layer": "silver", "scd.type": "2"}

    def test_tag_added_to_base_reflects_in_companion(self):
        manager = make_entity_manager()
        register_sample_entity(manager, entityid="silver.customers", tags=dict(self.SCD_TAGS))

        manager.apply_config_overrides(
            make_config_service({"dataentities": {"silver.*": {"tags": {"team": "core"}}}})
        )

        companion = manager.registry["silver.customers.current"]
        assert companion.tags["team"] == "core"
        assert companion.tags["scd.companion_of"] == "silver.customers"
        assert companion.tags["provider_type"] == "current_view"

    def test_scd_enabled_by_config_creates_companion_with_signals(self):
        manager = make_entity_manager()
        register_sample_entity(manager, entityid="silver.customers")
        assert "silver.customers.current" not in manager.registry
        manager.emit = MagicMock()

        manager.apply_config_overrides(
            make_config_service({"dataentities": {"silver.customers": {"tags": {"scd.type": "2"}}}})
        )

        assert "silver.customers.current" in manager.registry
        assert "entity.registered" in emitted_signals(manager)
        assert "entity.scd2_companion_registered" in emitted_signals(manager)

    def test_scd_disabled_by_config_removes_companion(self):
        manager = make_entity_manager()
        register_sample_entity(manager, entityid="silver.customers", tags=dict(self.SCD_TAGS))
        assert "silver.customers.current" in manager.registry

        manager.apply_config_overrides(
            make_config_service({"dataentities": {"silver.customers": {"tags": {"scd.type": ""}}}})
        )

        assert "silver.customers.current" not in manager.registry

    def test_changed_current_entity_id_moves_companion(self):
        manager = make_entity_manager()
        register_sample_entity(manager, entityid="silver.customers", tags=dict(self.SCD_TAGS))

        manager.apply_config_overrides(
            make_config_service(
                {
                    "dataentities": {
                        "silver.customers": {
                            "tags": {"scd.current_entity_id": "silver.customers.latest"}
                        }
                    }
                }
            )
        )

        assert "silver.customers.current" not in manager.registry
        assert "silver.customers.latest" in manager.registry

    def test_unchanged_companion_replaced_without_reemission(self):
        manager = make_entity_manager()
        register_sample_entity(manager, entityid="silver.customers", tags=dict(self.SCD_TAGS))
        manager.emit = MagicMock()

        manager.apply_config_overrides(make_config_service({}))

        assert "silver.customers.current" in manager.registry
        assert manager.emit.call_count == 0

    def test_companion_id_itself_resolves_patterns(self):
        manager = make_entity_manager()
        register_sample_entity(manager, entityid="silver.customers", tags=dict(self.SCD_TAGS))

        manager.apply_config_overrides(
            make_config_service(
                {"dataentities": {"silver.customers.current": {"tags": {"read_scope": "current"}}}}
            )
        )

        companion = manager.registry["silver.customers.current"]
        assert companion.tags["read_scope"] == "current"
        base = manager.get_entity_definition("silver.customers")
        assert "read_scope" not in base.tags

    def test_user_registered_entity_with_companion_id_is_never_clobbered(self):
        manager = make_entity_manager()
        register_sample_entity(
            manager, entityid="silver.customers.current", tags={"layer": "silver"}
        )
        register_sample_entity(manager, entityid="silver.customers", tags=dict(self.SCD_TAGS))
        user_owned = manager.registry["silver.customers.current"]

        manager.apply_config_overrides(make_config_service({}))

        assert manager.registry["silver.customers.current"] == user_owned


class TestRuntimeChannelsStillWinPerRead:
    """set_entity_tags and tag_overrides stay per-read above baked patterns."""

    def test_set_entity_tags_wins_over_baked_patterns(self):
        config_service = MagicMock()
        config_service.get.side_effect = lambda key, default=None: {
            "dataentities": {"bronze.orders": {"tags": {"provider.start": "baked"}}}
        }.get(key, default)
        config_service.get_entity_tags.return_value = {"provider.start": "per-read"}
        manager = make_entity_manager(config_service)
        register_sample_entity(manager)

        manager.apply_config_overrides(config_service)

        entity = manager.get_entity_definition("bronze.orders")
        assert entity.tags["provider.start"] == "per-read"

    def test_tag_overrides_context_wins_and_restores(self):
        config_service = MagicMock()
        config_service.get.side_effect = lambda key, default=None: {
            "dataentities": {"bronze.orders": {"tags": {"provider.start": "baked"}}}
        }.get(key, default)
        config_service.get_entity_tags.return_value = {"provider.start": "per-read"}
        manager = make_entity_manager(config_service)
        register_sample_entity(manager)
        manager.apply_config_overrides(config_service)

        with manager.tag_overrides({"bronze.orders": {"provider.start": "jit"}}):
            assert manager.get_entity_definition("bronze.orders").tags["provider.start"] == "jit"

        assert manager.get_entity_definition("bronze.orders").tags["provider.start"] == "per-read"


class TestBootstrapApplyConfigOverrides:
    """bootstrap.apply_config_overrides() helper"""

    def _patch_services(self, monkeypatch, config_service, pipes_registry, entity_registry):
        import kindling.bootstrap as bootstrap
        from kindling.data_entities import DataEntityRegistry
        from kindling.data_pipes import DataPipesRegistry
        from kindling.spark_config import ConfigService

        services = {
            ConfigService: config_service,
            DataPipesRegistry: pipes_registry,
            DataEntityRegistry: entity_registry,
        }
        monkeypatch.setattr(bootstrap, "get_kindling_service", lambda iface: services[iface])
        return bootstrap

    def test_applies_to_both_managers_via_injector(self, monkeypatch):
        config_service = make_config_service(
            {
                "datapipes": {"bronze.*": {"tags": {"layer": "bronze"}}},
                "dataentities": {"bronze.*": {"tags": {"team": "core"}}},
            }
        )
        pipes_manager = make_pipes_manager()
        register_sample_pipe(pipes_manager)
        entity_manager = make_entity_manager()
        register_sample_entity(entity_manager)
        bootstrap = self._patch_services(monkeypatch, config_service, pipes_manager, entity_manager)

        bootstrap.apply_config_overrides()

        assert pipes_manager.get_pipe_definition("bronze.ingest_orders").tags == {
            "domain": "sales",
            "layer": "bronze",
        }
        assert entity_manager.get_entity_definition("bronze.orders").tags == {
            "layer": "bronze",
            "team": "core",
        }

    def test_tolerates_absent_sections(self, monkeypatch):
        config_service = make_config_service({})
        pipes_manager = make_pipes_manager()
        register_sample_pipe(pipes_manager)
        entity_manager = make_entity_manager()
        register_sample_entity(entity_manager)
        bootstrap = self._patch_services(monkeypatch, config_service, pipes_manager, entity_manager)

        bootstrap.apply_config_overrides()

        assert pipes_manager.get_pipe_definition("bronze.ingest_orders").tags == {"domain": "sales"}
        assert entity_manager.get_entity_definition("bronze.orders").tags == {"layer": "bronze"}

    def test_skips_registry_without_overlay_support(self, monkeypatch):
        config_service = make_config_service({})
        pipes_manager = make_pipes_manager()
        legacy_registry = MagicMock(spec=["register_entity", "get_entity_ids"])
        legacy_registry.get_entity_ids.return_value = []
        bootstrap = self._patch_services(
            monkeypatch, config_service, pipes_manager, legacy_registry
        )

        bootstrap.apply_config_overrides()
