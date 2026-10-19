"""DataFrame-transforming signals chain: each receiver sees the previous
receiver's DataFrame (persist.before_persist and read.after_read).

Before the fix every receiver got the original DataFrame and the last
non-None return won, so one transform (e.g. a user's before_persist handler,
or a DataSignals dispatcher returning its unchanged input) silently
discarded another's (e.g. the surrogate key aspect's key column). The
end-to-end check with real DataFrames and the key aspect is in
tests/integration/test_surrogate_keys_merge.py.
"""

from unittest.mock import MagicMock, Mock

import pytest
from kindling.entity_provider import BaseEntityProvider, WritableEntityProvider
from kindling.signaling import BlinkerSignalProvider, DataSignals, SignalEmitter
from kindling.simple_read_persist_strategy import SimpleReadPersistStrategy


class FakeFrame:
    """Stands in for a DataFrame: has a ``schema`` and records transforms."""

    def __init__(self, steps=()):
        self.steps = tuple(steps)
        self.schema = object()

    def with_step(self, step):
        return FakeFrame(self.steps + (step,))


def _is_frame(value):
    return value is not None and hasattr(value, "schema")


class _Emitter(SignalEmitter):
    def __init__(self, provider):
        self._init_signal_emitter(provider)


def test_send_chained_threads_each_return_into_the_next_receiver():
    """Blinker does not promise a receiver order; whatever the order, each
    receiver sees the frame accumulated by the receivers called before it,
    and None / non-frame returns leave the value unchanged."""
    provider = BlinkerSignalProvider()
    calls = []

    def make(name, step):
        def receiver(sender, df, **kwargs):
            calls.append((name, df.steps))
            if step is None:
                return None
            if step == "other":
                return "not a frame"
            return df.with_step(step)

        return receiver

    signal = provider.create_signal("persist.before_persist")
    for name, step in (("a", "a"), ("none", None), ("other", "other"), ("b", "b")):
        signal.connect(make(name, step), weak=False)

    value, results = _Emitter(provider).emit_chained(
        "persist.before_persist", "df", _is_frame, df=FakeFrame(), pipe_id="p"
    )

    expected_steps = ()
    for name, seen in calls:
        assert seen == expected_steps, (name, seen, expected_steps)
        if name in ("a", "b"):
            expected_steps += (name,)
    assert value.steps == expected_steps
    assert sorted(value.steps) == ["a", "b"]
    assert len(results) == 4


def test_send_chained_respects_muting():
    provider = BlinkerSignalProvider()
    signal = provider.create_signal("persist.before_persist")
    signal.connect(lambda sender, df, **kw: df.with_step("x"), weak=False)
    start = FakeFrame()
    with signal.muted():
        value, results = _Emitter(provider).emit_chained(
            "persist.before_persist", "df", _is_frame, df=start
        )
    assert value is start and results == []


def test_emit_chained_without_a_provider_returns_the_input():
    start = FakeFrame()
    assert _Emitter(None).emit_chained("x", "df", _is_frame, df=start) == (start, [])


def test_emit_chained_falls_back_to_emit_for_mock_providers():
    emitter = _Emitter(MagicMock())
    emitter.emit = Mock(return_value=[(None, FakeFrame(["a"]))])
    value, _ = emitter.emit_chained("x", "df", _is_frame, df=FakeFrame())
    assert value.steps == ("a",)
    emitter.emit.assert_called_once()


def test_signals_without_chaining_support_fall_back_to_last_wins():
    signal = Mock(spec=["send"])
    signal.send.return_value = [(None, FakeFrame(["a"])), (None, None)]
    provider = Mock()
    provider.get_signal.return_value = signal
    value, _ = _Emitter(provider).emit_chained("x", "df", _is_frame, df=FakeFrame())
    assert value.steps == ("a",)


# ── through the persist strategy ─────────────────────────────────────────────


class _MergeWritableProvider(BaseEntityProvider, WritableEntityProvider):
    def merge_to_entity(self, df, entity): ...


def _persist(signal_provider):
    der = Mock()
    entities = {
        "entity.src": Mock(entityid="entity.src", tags={}),
        "entity.dst": Mock(entityid="entity.dst", tags={}, merge_columns=["id"]),
    }
    der.get_entity_definition.side_effect = entities.__getitem__
    out_provider = Mock(spec=_MergeWritableProvider)
    out_provider.check_entity_exists.return_value = True
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
        signal_provider=signal_provider,
    )
    pipe = Mock(pipeid="pipe1", input_entity_ids=["entity.src"], output_entity_id="entity.dst")
    return strategy.create_pipe_persist_activator(pipe), out_provider


def _key_aspect(sender, df, **kwargs):
    return df.with_step("key")


def _user_transform(sender, df, **kwargs):
    return df.with_step("user")


@pytest.mark.parametrize(
    "receivers", [(_key_aspect, _user_transform), (_user_transform, _key_aspect)]
)
def test_both_before_persist_transforms_apply_in_either_order(receivers):
    provider = BlinkerSignalProvider()
    signal = provider.create_signal("persist.before_persist")
    for receiver in receivers:
        signal.connect(receiver, weak=False)

    persist, out_provider = _persist(provider)
    persist(FakeFrame())

    written = out_provider.merge_to_entity.call_args.args[0]
    assert sorted(written.steps) == ["key", "user"]


def test_a_datasignals_dispatcher_no_longer_discards_an_earlier_transform():
    """DataSignals' dispatcher returns its (possibly unchanged) input
    DataFrame; under last-wins that overwrote an earlier receiver's output."""
    provider = BlinkerSignalProvider()
    DataSignals.reset()
    DataSignals._signal_provider = provider
    try:
        provider.create_signal("persist.before_persist").connect(_key_aspect, weak=False)

        @DataSignals.handler("persist.before_persist")
        def observe_only(sender, df, **kwargs):
            return None

        persist, out_provider = _persist(provider)
        persist(FakeFrame())
        assert out_provider.merge_to_entity.call_args.args[0].steps == ("key",)
    finally:
        DataSignals.reset()
