"""Unit tests for provider-op tracing (kindling.trace_ops)."""

import inspect
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from kindling.data_entities import EntityMetadata
from kindling.entity_provider import BaseEntityProvider, WritableEntityProvider
from kindling.entity_provider_registry import EntityProviderRegistry
from kindling.injection import GlobalInjector
from kindling.test_framework import RecordingTraceProvider
from kindling.trace_ops import (
    LEVEL_MINIMAL,
    LEVEL_STANDARD,
    LEVEL_VERBOSE,
    TRACED_PROVIDER_OPS,
    UNTRACED_PROVIDER_OPS,
    _entity_id_from_args,
    configure_op_tracing,
    level_at_least,
    read_tracing_settings,
    whitelist_details,
    wrap_provider_ops,
)


class _Config:
    def __init__(self, values=None):
        self.values = values or {}

    def get(self, key, default=None):
        return self.values.get(key, default)


class FakeProvider(BaseEntityProvider, WritableEntityProvider):
    """Provider double implementing two capability ABCs plus a de-facto op."""

    def __init__(self, *args, **kwargs):
        self.read_calls = 0
        self.merge_calls = 0

    def read_entity(self, entity_metadata):
        self.read_calls += 1
        return "df"

    def check_entity_exists(self, entity_metadata):
        return True

    def write_to_entity(self, df, entity_metadata):
        return None

    def append_to_entity(self, df, entity_metadata):
        return None

    def merge_to_entity(self, df, entity):
        self.merge_calls += 1
        return None


def _entity(entityid="silver.orders"):
    return EntityMetadata(
        entityid=entityid,
        name=entityid.split(".")[-1],
        partition_columns=[],
        merge_columns=[],
        tags={},
        schema=None,
    )


def _registry():
    logger_provider = MagicMock()
    logger_provider.get_logger.return_value = MagicMock()
    with patch("kindling.entity_provider_registry.GlobalInjector"):
        return EntityProviderRegistry(logger_provider)


class TestWrapProviderOps:
    def test_wrapped_op_emits_span_with_whitelisted_details(self):
        provider = FakeProvider()
        tp = RecordingTraceProvider()

        wrap_provider_ops(provider, tp, provider_type="fake")
        result = provider.read_entity(_entity())

        assert result == "df", "Return value must pass through the wrapper"
        spans = tp.find(component="kindling.entity.fake", operation="read_entity")
        assert len(spans) == 1
        assert spans[0].details["entity_id"] == "silver.orders"
        assert spans[0].details["provider_type"] == "fake"
        assert spans[0].closed

    def test_entity_id_found_in_keyword_arguments(self):
        provider = FakeProvider()
        tp = RecordingTraceProvider()

        wrap_provider_ops(provider, tp, provider_type="fake")
        provider.write_to_entity(df="rows", entity_metadata=_entity("gold.facts"))

        spans = tp.find(operation="write_to_entity")
        assert spans[0].details["entity_id"] == "gold.facts"

    def test_identity_isinstance_hasattr_preserved(self):
        provider = FakeProvider()
        tp = RecordingTraceProvider()

        wrapped = wrap_provider_ops(provider, tp, provider_type="fake")

        assert wrapped is provider, "Wrapping must not replace the instance"
        assert isinstance(provider, BaseEntityProvider)
        assert isinstance(provider, WritableEntityProvider)
        assert hasattr(provider, "merge_to_entity"), "hasattr capability probes must still hold"
        assert not hasattr(provider, "replace_entity"), "Absent ops must stay absent"

    def test_rewrap_is_idempotent(self):
        provider = FakeProvider()
        tp = RecordingTraceProvider()

        wrap_provider_ops(provider, tp, provider_type="fake")
        wrap_provider_ops(provider, tp, provider_type="fake")
        provider.read_entity(_entity())

        assert len(tp.find(operation="read_entity")) == 1, "Re-wrap must not double-span"

    def test_exceptions_propagate_and_span_records_error(self):
        class FailingProvider(FakeProvider):
            def read_entity(self, entity_metadata):
                raise ValueError("storage offline")

        provider = FailingProvider()
        tp = RecordingTraceProvider()
        wrap_provider_ops(provider, tp, provider_type="fake")

        with pytest.raises(ValueError, match="storage offline"):
            provider.read_entity(_entity())

        span = tp.find(operation="read_entity")[0]
        assert span.closed
        assert "storage offline" in span.error

    def test_class_attribute_call_bypasses_wrapper(self):
        """The delta _merge_batch pattern: type(self).op(self, ...) skips tracing."""
        provider = FakeProvider()
        tp = RecordingTraceProvider()
        wrap_provider_ops(provider, tp, provider_type="fake")

        type(provider).merge_to_entity(provider, "batch_df", _entity())

        assert provider.merge_calls == 1
        assert tp.find(operation="merge_to_entity") == [], "Class-attr call must not span"

        provider.merge_to_entity("df", _entity())
        assert len(tp.find(operation="merge_to_entity")) == 1, "Instance call must span"

    def test_delta_merge_batch_uses_class_attribute_bypass(self):
        """Source guard: the foreachBatch closure must keep the wrapper bypass."""
        from kindling.entity_provider_delta import DeltaEntityProvider

        source = inspect.getsource(DeltaEntityProvider.merge_as_stream)
        assert "type(self).merge_to_entity(self" in source


class _SparkConnectLikeDataFrame:
    """Simulates a Spark Connect DataFrame's dangerous __getattr__.

    Spark Connect resolves any unrecognized attribute name as a possible
    column reference and issues a schema-analysis RPC; outside an active
    session/API-URL context that RPC fails with something other than a
    plain AttributeError. This double reproduces just that hazard: any
    attribute access not present in __dict__ blows up loudly instead of
    raising AttributeError, so a test using it fails immediately if
    production code ever falls back to getattr()-style introspection.
    """

    def __getattr__(self, name):
        raise RuntimeError(f"No api url found in local command context (attempted attr={name!r})")


class TestEntityIdFromArgsSafety:
    """Regression coverage: tracing must never getattr() a provider-op argument."""

    def test_dangerous_dataframe_argument_is_not_introspected(self):
        entity_id = _entity_id_from_args(
            (_SparkConnectLikeDataFrame(), _entity("silver.telemetry")), {}
        )
        assert entity_id == "silver.telemetry"

    def test_dangerous_dataframe_as_only_argument_returns_none(self):
        entity_id = _entity_id_from_args((_SparkConnectLikeDataFrame(),), {})
        assert entity_id is None

    def test_merge_to_entity_spans_without_touching_dangerous_df_attrs(self):
        """End-to-end through the real wrapper: merge_to_entity(df, entity)."""
        provider = FakeProvider()
        tp = RecordingTraceProvider()
        wrap_provider_ops(provider, tp, provider_type="fake")

        dangerous_df = _SparkConnectLikeDataFrame()
        provider.merge_to_entity(dangerous_df, _entity("silver.telemetry"))

        assert provider.merge_calls == 1
        spans = tp.find(operation="merge_to_entity")
        assert len(spans) == 1
        assert spans[0].details["entity_id"] == "silver.telemetry"
        assert spans[0].closed


class TestRegistryOpTracing:
    def test_registry_resolved_provider_emits_op_spans(self):
        registry = _registry()
        registry.register_provider("fake", FakeProvider)
        tp = RecordingTraceProvider()
        registry.enable_op_tracing(tp, LEVEL_STANDARD)

        instance = FakeProvider()
        with patch.object(GlobalInjector, "get", return_value=instance):
            provider = registry.get_provider("fake")

        assert provider is instance, "Registry must serve the same (wrapped) instance"
        provider.read_entity(_entity())
        spans = tp.find(component="kindling.entity.fake", operation="read_entity")
        assert len(spans) == 1

    def test_enable_after_resolution_wraps_cached_instances(self):
        registry = _registry()
        registry.register_provider("fake", FakeProvider)
        instance = FakeProvider()
        with patch.object(GlobalInjector, "get", return_value=instance):
            provider = registry.get_provider("fake")

        tp = RecordingTraceProvider()
        registry.enable_op_tracing(tp, LEVEL_STANDARD)

        provider.read_entity(_entity())
        assert len(tp.find(operation="read_entity")) == 1

    def test_without_enable_no_wrapping_happens(self):
        registry = _registry()
        registry.register_provider("fake", FakeProvider)
        instance = FakeProvider()
        with patch.object(GlobalInjector, "get", return_value=instance):
            provider = registry.get_provider("fake")

        provider.read_entity(_entity())
        assert not hasattr(provider, "_kindling_op_tracing_wrapped")


class TestConfigureOpTracing:
    def test_disabled_config_does_not_enable(self):
        registry = MagicMock()
        enabled = configure_op_tracing(
            _Config({"kindling.telemetry.tracing.enabled": "false"}),
            registry=registry,
            trace_provider=RecordingTraceProvider(),
            legacy_provider=FakeProvider(),
        )

        assert enabled is False
        registry.enable_op_tracing.assert_not_called()

    def test_minimal_level_does_not_enable_op_tracing(self):
        registry = MagicMock()
        enabled = configure_op_tracing(
            _Config({"kindling.telemetry.tracing.level": LEVEL_MINIMAL}),
            registry=registry,
            trace_provider=RecordingTraceProvider(),
            legacy_provider=FakeProvider(),
        )

        assert enabled is False
        registry.enable_op_tracing.assert_not_called()

    def test_default_config_enables_and_wraps_legacy_singleton(self):
        registry = MagicMock()
        tp = RecordingTraceProvider()
        legacy = FakeProvider()

        enabled = configure_op_tracing(
            _Config(), registry=registry, trace_provider=tp, legacy_provider=legacy
        )

        assert enabled is True
        registry.enable_op_tracing.assert_called_once_with(tp, LEVEL_STANDARD)
        assert getattr(legacy, "_kindling_op_tracing_wrapped", False) is True

        legacy.read_entity(_entity())
        assert tp.find(component="kindling.entity.delta", operation="read_entity")


class TestTracingSettings:
    def test_defaults_are_enabled_standard(self):
        assert read_tracing_settings(_Config()) == (True, LEVEL_STANDARD)

    def test_string_false_coerces(self):
        enabled, _ = read_tracing_settings(_Config({"kindling.telemetry.tracing.enabled": "false"}))
        assert enabled is False

    def test_level_is_normalized_and_validated(self):
        _, level = read_tracing_settings(_Config({"kindling.telemetry.tracing.level": "VERBOSE"}))
        assert level == LEVEL_VERBOSE

        _, level = read_tracing_settings(_Config({"kindling.telemetry.tracing.level": "bogus"}))
        assert level == LEVEL_STANDARD

    def test_level_at_least_ordering(self):
        assert level_at_least(LEVEL_VERBOSE, LEVEL_STANDARD)
        assert level_at_least(LEVEL_STANDARD, LEVEL_STANDARD)
        assert not level_at_least(LEVEL_MINIMAL, LEVEL_STANDARD)
        assert not level_at_least(LEVEL_STANDARD, LEVEL_VERBOSE)

    def test_whitelist_details_filters_keys(self):
        details = whitelist_details({"pipe_id": "p1", "secret_ref": "nope"}, ("pipe_id",))
        assert details == {"pipe_id": "p1"}
        assert whitelist_details(None, ("pipe_id",)) == {}


class TestOpListDriftGuard:
    def test_op_list_covers_capability_abc_methods(self):
        """Every capability-ABC method must be classified traced or untraced."""
        import kindling.entity_provider as entity_provider_module

        abc_ops = set()
        for name in dir(entity_provider_module):
            obj = getattr(entity_provider_module, name)
            if inspect.isclass(obj) and getattr(obj, "__abstractmethods__", None):
                abc_ops |= set(obj.__abstractmethods__)

        classified = set(TRACED_PROVIDER_OPS) | set(UNTRACED_PROVIDER_OPS)
        missing = {op for op in abc_ops if not op.startswith("_")} - classified
        assert not missing, (
            f"Capability-ABC methods not classified in trace_ops: {sorted(missing)}. "
            "Add them to TRACED_PROVIDER_OPS or UNTRACED_PROVIDER_OPS."
        )

    def test_traced_and_untraced_do_not_overlap(self):
        assert not set(TRACED_PROVIDER_OPS) & set(UNTRACED_PROVIDER_OPS)
