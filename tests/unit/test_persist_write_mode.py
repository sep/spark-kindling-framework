"""
Unit tests for the `write.mode` entity tag on the batch persist path.

The tag is shared with the streaming path (SimplePipeStreamStarter):
- "append" skips the merge even when the provider supports it (e.g.
  append-only fact tables);
- "merge" makes the merge requirement explicit — no silent append fallback;
- unset keeps the default: merge when the provider can, append otherwise.
"""

from unittest.mock import MagicMock, Mock

import pytest

from kindling.data_entities import DataEntityManager
from kindling.entity_provider import BaseEntityProvider, WritableEntityProvider
from kindling.simple_read_persist_strategy import SimpleReadPersistStrategy


class _MergeWritableProvider(BaseEntityProvider, WritableEntityProvider):
    """Spec class: batch-writable provider that also supports merge."""

    def merge_to_entity(self, df, entity): ...


def _make_strategy(dst_entity, out_provider):
    der = Mock()
    src_entity = Mock(entityid="entity.src", tags={})
    der.get_entity_definition.side_effect = lambda eid: {
        "entity.src": src_entity,
        "entity.dst": dst_entity,
    }[eid]

    provider_registry = Mock()
    provider_registry.get_provider_for_entity.return_value = out_provider

    lp = Mock()
    lp.get_logger.return_value = Mock()

    strategy = SimpleReadPersistStrategy(
        ep=Mock(),
        der=der,
        tp=MagicMock(),
        lp=lp,
        provider_registry=provider_registry,
        signal_provider=None,
    )
    pipe = Mock(
        pipeid="pipe1",
        input_entity_ids=["entity.src"],
        output_entity_id="entity.dst",
    )
    return strategy.create_pipe_persist_activator(pipe), strategy


def _capture_emitted_signals(strategy):
    """Replace strategy.emit with a spy that records signal names."""
    emitted = []

    def _spy(signal_name, **kwargs):
        emitted.append(signal_name)
        return []

    strategy.emit = _spy
    return emitted


def test_default_merges_when_provider_supports_merge():
    dst_entity = Mock(entityid="entity.dst", tags={})
    out_provider = Mock(spec=_MergeWritableProvider)
    out_provider.check_entity_exists.return_value = True

    persist, _ = _make_strategy(dst_entity, out_provider)
    persist(Mock(name="df"))

    out_provider.merge_to_entity.assert_called_once()
    out_provider.append_to_entity.assert_not_called()


def test_write_mode_append_skips_merge():
    dst_entity = Mock(entityid="entity.dst", tags={"write.mode": "append"})
    out_provider = Mock(spec=_MergeWritableProvider)
    out_provider.check_entity_exists.return_value = True

    persist, _ = _make_strategy(dst_entity, out_provider)
    persist(Mock(name="df"))

    out_provider.append_to_entity.assert_called_once()
    out_provider.merge_to_entity.assert_not_called()


def test_write_mode_merge_requires_merge_capable_provider():
    dst_entity = Mock(
        entityid="entity.dst",
        tags={"write.mode": "merge", "provider_type": "memory"},
    )
    out_provider = Mock(spec=WritableEntityProvider)

    persist, _ = _make_strategy(dst_entity, out_provider)
    with pytest.raises(ValueError, match="does not support merge"):
        persist(Mock(name="df"))


def test_invalid_write_mode_raises():
    dst_entity = Mock(entityid="entity.dst", tags={"write.mode": "overwrite"})
    out_provider = Mock(spec=_MergeWritableProvider)

    persist, _ = _make_strategy(dst_entity, out_provider)
    with pytest.raises(ValueError, match="write.mode"):
        persist(Mock(name="df"))


def test_write_mode_append_still_writes_when_entity_missing():
    """First write creates the table regardless of write.mode."""
    dst_entity = Mock(entityid="entity.dst", tags={"write.mode": "append"})
    out_provider = Mock(spec=_MergeWritableProvider)
    out_provider.check_entity_exists.return_value = False

    persist, _ = _make_strategy(dst_entity, out_provider)
    persist(Mock(name="df"))

    out_provider.write_to_entity.assert_called_once()
    out_provider.append_to_entity.assert_not_called()
    out_provider.merge_to_entity.assert_not_called()


def test_write_mode_failure_pairs_before_persist_with_persist_failed():
    """A write.mode failure raised during persist must emit
    persist.persist_failed, so observers never see an unpaired
    persist.before_persist."""
    dst_entity = Mock(
        entityid="entity.dst",
        tags={"write.mode": "merge", "provider_type": "memory"},
    )
    out_provider = Mock(spec=WritableEntityProvider)  # no merge_to_entity

    persist, strategy = _make_strategy(dst_entity, out_provider)
    emitted = _capture_emitted_signals(strategy)

    with pytest.raises(ValueError, match="does not support merge"):
        persist(Mock(name="df"))

    assert "persist.before_persist" in emitted
    assert "persist.persist_failed" in emitted


def test_before_persist_gate_failure_blocks_write_and_emits_persist_failed():
    """A raising persist.before_persist handler is the supported write gate
    (e.g. a validation gate): the write must not happen, the exception must
    propagate, and persist.persist_failed must still be emitted so observers
    (WatermarkAspect) treat it as a failed persist."""
    dst_entity = Mock(entityid="entity.dst", tags={})
    out_provider = Mock(spec=_MergeWritableProvider)
    out_provider.check_entity_exists.return_value = True

    persist, strategy = _make_strategy(dst_entity, out_provider)

    emitted = []

    def _gate(signal_name, **kwargs):
        emitted.append(signal_name)
        if signal_name == "persist.before_persist":
            raise ValueError("validation gate rejected batch")
        return []

    strategy.emit = _gate

    with pytest.raises(ValueError, match="validation gate"):
        persist(Mock(name="df"))

    assert "persist.persist_failed" in emitted
    out_provider.merge_to_entity.assert_not_called()
    out_provider.append_to_entity.assert_not_called()
    out_provider.write_to_entity.assert_not_called()


def test_before_persist_gate_failure_reaches_persist_failed_subscribers():
    """Same contract through the real blinker signal provider: a raising
    before_persist subscriber propagates, and persist_failed subscribers
    still receive the failure (with the gate's error in the payload)."""
    from kindling.signaling import BlinkerSignalProvider

    dst_entity = Mock(entityid="entity.dst", tags={})
    out_provider = Mock(spec=_MergeWritableProvider)
    out_provider.check_entity_exists.return_value = True

    der = Mock()
    src_entity = Mock(entityid="entity.src", tags={})
    der.get_entity_definition.side_effect = lambda eid: {
        "entity.src": src_entity,
        "entity.dst": dst_entity,
    }[eid]
    provider_registry = Mock()
    provider_registry.get_provider_for_entity.return_value = out_provider
    lp = Mock()
    lp.get_logger.return_value = Mock()

    signal_provider = BlinkerSignalProvider()
    strategy = SimpleReadPersistStrategy(
        ep=Mock(),
        der=der,
        tp=MagicMock(),
        lp=lp,
        provider_registry=provider_registry,
        signal_provider=signal_provider,
    )
    pipe = Mock(pipeid="pipe1", input_entity_ids=["entity.src"], output_entity_id="entity.dst")
    persist = strategy.create_pipe_persist_activator(pipe)

    def _gate(sender, **kwargs):
        raise ValueError("validation gate rejected batch")

    failures = []

    def _on_failed(sender, **kwargs):
        failures.append(kwargs)

    signal_provider.create_signal("persist.before_persist").connect(_gate)
    signal_provider.create_signal("persist.persist_failed").connect(_on_failed)

    with pytest.raises(ValueError, match="validation gate"):
        persist(Mock(name="df"))

    assert len(failures) == 1
    assert failures[0]["pipe_id"] == "pipe1"
    assert failures[0]["error_type"] == "ValueError"
    assert "validation gate" in failures[0]["error"]
    out_provider.merge_to_entity.assert_not_called()
    out_provider.write_to_entity.assert_not_called()


def test_invalid_write_mode_rejected_at_registration():
    """The static write.mode tag value is validated when the entity is
    registered — typos surface at import time, not mid-persist."""
    manager = DataEntityManager()

    with pytest.raises(ValueError, match="write.mode"):
        manager.register_entity(
            "entity.bad",
            name="bad",
            merge_columns=[],
            tags={"write.mode": "overwrite"},
            schema=None,
        )


@pytest.mark.parametrize("mode", ["append", "merge", "Append", " MERGE "])
def test_valid_write_mode_accepted_at_registration(mode):
    manager = DataEntityManager()

    manager.register_entity(
        "entity.ok",
        name="ok",
        merge_columns=["id"],
        tags={"write.mode": mode},
        schema=None,
    )

    assert "entity.ok" in manager.registry


class _ReplaceCapableProvider(BaseEntityProvider, WritableEntityProvider):
    """Spec class: batch-writable provider that also supports replace."""

    def replace_entity(self, df, entity): ...


def test_derived_dataset_routes_to_replace():
    dst_entity = Mock(entityid="entity.dst", tags={"dataset.kind": "derived"})
    out_provider = Mock(spec=_ReplaceCapableProvider)

    persist, _ = _make_strategy(dst_entity, out_provider)
    persist(Mock(name="df"))

    out_provider.replace_entity.assert_called_once()
    out_provider.write_to_entity.assert_not_called()
    out_provider.append_to_entity.assert_not_called()


def test_derived_dataset_emits_after_persist():
    dst_entity = Mock(entityid="entity.dst", tags={"dataset.kind": "derived"})
    out_provider = Mock(spec=_ReplaceCapableProvider)

    persist, strategy = _make_strategy(dst_entity, out_provider)
    emitted = _capture_emitted_signals(strategy)
    persist(Mock(name="df"))

    assert "persist.before_persist" in emitted
    assert "persist.after_persist" in emitted


def test_derived_dataset_requires_replace_capable_provider():
    dst_entity = Mock(
        entityid="entity.dst",
        tags={"dataset.kind": "derived", "provider_type": "memory"},
    )
    out_provider = Mock(spec=_MergeWritableProvider)  # no replace_entity

    persist, strategy = _make_strategy(dst_entity, out_provider)
    emitted = _capture_emitted_signals(strategy)

    with pytest.raises(ValueError, match="does not support\\s+replace"):
        persist(Mock(name="df"))

    assert "persist.persist_failed" in emitted


def test_write_mode_insert_routes_to_merge_provider():
    """insert rides the merge path; the provider picks the insert-only
    strategy from the entity's write.mode tag."""
    dst_entity = Mock(entityid="entity.dst", tags={"write.mode": "insert"})
    out_provider = Mock(spec=_MergeWritableProvider)
    out_provider.check_entity_exists.return_value = True

    persist, _ = _make_strategy(dst_entity, out_provider)
    persist(Mock(name="df"))

    out_provider.merge_to_entity.assert_called_once()
    out_provider.append_to_entity.assert_not_called()


def test_write_mode_insert_requires_merge_capable_provider():
    dst_entity = Mock(
        entityid="entity.dst",
        tags={"write.mode": "insert", "provider_type": "memory"},
    )
    out_provider = Mock(spec=WritableEntityProvider)

    persist, _ = _make_strategy(dst_entity, out_provider)
    with pytest.raises(ValueError, match="does not support merge"):
        persist(Mock(name="df"))
