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
    return strategy.create_pipe_persist_activator(pipe)


def test_default_merges_when_provider_supports_merge():
    dst_entity = Mock(entityid="entity.dst", tags={})
    out_provider = Mock(spec=_MergeWritableProvider)
    out_provider.check_entity_exists.return_value = True

    persist = _make_strategy(dst_entity, out_provider)
    persist(Mock(name="df"))

    out_provider.merge_to_entity.assert_called_once()
    out_provider.append_to_entity.assert_not_called()


def test_write_mode_append_skips_merge():
    dst_entity = Mock(entityid="entity.dst", tags={"write.mode": "append"})
    out_provider = Mock(spec=_MergeWritableProvider)
    out_provider.check_entity_exists.return_value = True

    persist = _make_strategy(dst_entity, out_provider)
    persist(Mock(name="df"))

    out_provider.append_to_entity.assert_called_once()
    out_provider.merge_to_entity.assert_not_called()


def test_write_mode_merge_requires_merge_capable_provider():
    dst_entity = Mock(
        entityid="entity.dst",
        tags={"write.mode": "merge", "provider_type": "memory"},
    )
    out_provider = Mock(spec=WritableEntityProvider)

    persist = _make_strategy(dst_entity, out_provider)
    with pytest.raises(ValueError, match="does not support merge"):
        persist(Mock(name="df"))


def test_invalid_write_mode_raises():
    dst_entity = Mock(entityid="entity.dst", tags={"write.mode": "overwrite"})
    out_provider = Mock(spec=_MergeWritableProvider)

    persist = _make_strategy(dst_entity, out_provider)
    with pytest.raises(ValueError, match="write.mode"):
        persist(Mock(name="df"))


def test_write_mode_append_still_writes_when_entity_missing():
    """First write creates the table regardless of write.mode."""
    dst_entity = Mock(entityid="entity.dst", tags={"write.mode": "append"})
    out_provider = Mock(spec=_MergeWritableProvider)
    out_provider.check_entity_exists.return_value = False

    persist = _make_strategy(dst_entity, out_provider)
    persist(Mock(name="df"))

    out_provider.write_to_entity.assert_called_once()
    out_provider.append_to_entity.assert_not_called()
    out_provider.merge_to_entity.assert_not_called()
