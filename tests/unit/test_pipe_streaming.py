from dataclasses import dataclass
from typing import Optional, Tuple
from unittest.mock import Mock, patch

import pytest
from kindling.data_pipes import PipeMetadata
from kindling.entity_provider import (
    StreamableEntityProvider,
    StreamMergeableEntityProvider,
    StreamWritableEntityProvider,
)
from kindling.pipe_streaming import SimplePipeStreamStarter


class _MergeCapableProvider(StreamWritableEntityProvider, StreamMergeableEntityProvider):
    """Spec class for mocking a sink that supports both append and merge."""


def _make_pipe(pipe_id="pipe1", output_entity_id="entity.dst"):
    return PipeMetadata(
        pipeid=pipe_id,
        name=pipe_id,
        execute=Mock(return_value=Mock(name="transformed_stream")),
        tags={},
        input_entity_ids=["entity.src"],
        output_entity_id=output_entity_id,
        output_type="delta",
    )


def test_start_pipe_streaming_for_name_uses_to_table():
    cs = Mock()
    cs.get.side_effect = lambda key: (
        "/checkpoints" if key == "kindling.storage.checkpoint_root" else None
    )
    dpr = Mock()
    der = Mock()
    provider_registry = Mock()
    epl = Mock()
    plp = Mock()
    plp.get_logger.return_value = Mock()

    pipe = _make_pipe()
    dpr.get_pipe_definition.return_value = pipe

    src_entity = Mock(entityid="entity.src", tags={"provider_type": "delta"})
    dst_entity = Mock(
        entityid="entity.dst",
        tags={
            "provider_type": "delta",
            "provider.access_mode": "catalog",
            "provider.table_name": "main.analytics.entity_dst",
        },
    )
    der.get_entity_definition.side_effect = lambda eid: {
        "entity.src": src_entity,
        "entity.dst": dst_entity,
    }[eid]

    src_provider = Mock(spec=StreamableEntityProvider)
    src_provider.read_entity_as_stream.return_value = Mock(name="stream_df")

    out_provider = Mock(spec=StreamWritableEntityProvider)
    writer = Mock()
    query = Mock(id="q-1")
    writer.toTable.return_value = query
    writer.start.return_value = Mock(id="q-start")
    out_provider.append_as_stream.return_value = writer

    provider_registry.get_provider_for_entity.side_effect = lambda entity: {
        "entity.src": src_provider,
        "entity.dst": out_provider,
    }[entity.entityid]

    starter = SimplePipeStreamStarter(cs, dpr, provider_registry, der, epl, plp)
    result = starter.start_pipe_stream("pipe1")

    assert result is query
    writer.toTable.assert_called_once_with("main.analytics.entity_dst")
    writer.start.assert_not_called()
    epl.get_table_path.assert_not_called()


def test_start_pipe_streaming_for_path_uses_start_with_path():
    cs = Mock()
    cs.get.side_effect = lambda key: (
        "/checkpoints" if key == "kindling.storage.checkpoint_root" else None
    )
    dpr = Mock()
    der = Mock()
    provider_registry = Mock()
    epl = Mock()
    epl.get_table_path.return_value = "/tables/entity_dst"
    plp = Mock()
    plp.get_logger.return_value = Mock()

    pipe = _make_pipe()
    dpr.get_pipe_definition.return_value = pipe

    src_entity = Mock(entityid="entity.src", tags={"provider_type": "delta"})
    dst_entity = Mock(
        entityid="entity.dst",
        tags={"provider_type": "delta", "provider.access_mode": "storage"},
    )
    der.get_entity_definition.side_effect = lambda eid: {
        "entity.src": src_entity,
        "entity.dst": dst_entity,
    }[eid]

    src_provider = Mock(spec=StreamableEntityProvider)
    src_provider.read_entity_as_stream.return_value = Mock(name="stream_df")

    out_provider = Mock(spec=StreamWritableEntityProvider)
    writer = Mock()
    query = Mock(id="q-2")
    writer.start.return_value = query
    out_provider.append_as_stream.return_value = writer

    provider_registry.get_provider_for_entity.side_effect = lambda entity: {
        "entity.src": src_provider,
        "entity.dst": out_provider,
    }[entity.entityid]

    starter = SimplePipeStreamStarter(cs, dpr, provider_registry, der, epl, plp)
    result = starter.start_pipe_stream("pipe1")

    assert result is query
    writer.start.assert_called_once_with("/tables/entity_dst")
    epl.get_table_path.assert_called_once_with(dst_entity)


def test_start_pipe_streaming_for_name_resolves_table_from_mapper_when_missing_tag():
    cs = Mock()
    cs.get.side_effect = lambda key: (
        "/checkpoints" if key == "kindling.storage.checkpoint_root" else None
    )
    dpr = Mock()
    der = Mock()
    provider_registry = Mock()
    epl = Mock()
    plp = Mock()
    plp.get_logger.return_value = Mock()

    pipe = _make_pipe()
    dpr.get_pipe_definition.return_value = pipe

    src_entity = Mock(entityid="entity.src", tags={"provider_type": "delta"})
    dst_entity = Mock(
        entityid="entity.dst",
        tags={"provider_type": "delta", "provider.access_mode": "catalog"},
    )
    der.get_entity_definition.side_effect = lambda eid: {
        "entity.src": src_entity,
        "entity.dst": dst_entity,
    }[eid]

    src_provider = Mock(spec=StreamableEntityProvider)
    src_provider.read_entity_as_stream.return_value = Mock(name="stream_df")

    out_provider = Mock(spec=StreamWritableEntityProvider)
    writer = Mock()
    query = Mock(id="q-3")
    writer.toTable.return_value = query
    out_provider.append_as_stream.return_value = writer

    provider_registry.get_provider_for_entity.side_effect = lambda entity: {
        "entity.src": src_provider,
        "entity.dst": out_provider,
    }[entity.entityid]

    mapper = Mock()
    mapper.get_table_name.return_value = "main.analytics.entity_dst"

    starter = SimplePipeStreamStarter(cs, dpr, provider_registry, der, epl, plp)
    with patch("kindling.pipe_streaming.GlobalInjector.get", return_value=mapper):
        result = starter.start_pipe_stream("pipe1")

    assert result is query
    mapper.get_table_name.assert_called_once_with(dst_entity)
    writer.toTable.assert_called_once_with("main.analytics.entity_dst")


def _make_starter(dst_entity, out_provider):
    cs = Mock()
    cs.get.side_effect = lambda key: (
        "/checkpoints" if key == "kindling.storage.checkpoint_root" else None
    )
    dpr = Mock()
    der = Mock()
    provider_registry = Mock()
    epl = Mock()
    plp = Mock()
    plp.get_logger.return_value = Mock()

    pipe = _make_pipe()
    dpr.get_pipe_definition.return_value = pipe

    src_entity = Mock(entityid="entity.src", tags={"provider_type": "delta"})
    der.get_entity_definition.side_effect = lambda eid: {
        "entity.src": src_entity,
        "entity.dst": dst_entity,
    }[eid]

    src_provider = Mock(spec=StreamableEntityProvider)
    src_provider.read_entity_as_stream.return_value = Mock(name="stream_df")

    provider_registry.get_provider_for_entity.side_effect = lambda entity: {
        "entity.src": src_provider,
        "entity.dst": out_provider,
    }[entity.entityid]

    return SimplePipeStreamStarter(cs, dpr, provider_registry, der, epl, plp), pipe


def _make_selection_starter(
    input_entity_ids,
    driving_entity_ids=None,
    non_streamable_entity_ids=(),
    configure_input_providers=None,
):
    """Build a starter that records each input's stream-vs-batch read path."""
    cs = Mock()
    cs.get.side_effect = lambda key: (
        "/checkpoints" if key == "kindling.storage.checkpoint_root" else None
    )
    dpr = Mock()
    der = Mock()
    provider_registry = Mock()
    epl = Mock()
    plp = Mock()
    plp.get_logger.return_value = Mock()

    frames = {
        entity_id: Mock(name=f"{entity_id.replace('.', '_')}_frame")
        for entity_id in input_entity_ids
    }
    reads = []
    non_streamable = set(non_streamable_entity_ids)

    pipe = PipeMetadata(
        pipeid="pipe1",
        name="pipe1",
        execute=Mock(return_value=Mock(name="transformed_stream")),
        tags={},
        input_entity_ids=list(input_entity_ids),
        output_entity_id="entity.dst",
        output_type="delta",
        driving_entity_ids=(list(driving_entity_ids) if driving_entity_ids is not None else None),
    )
    dpr.get_pipe_definition.return_value = pipe

    input_entities = {
        entity_id: Mock(entityid=entity_id, tags={"provider_type": "delta"})
        for entity_id in input_entity_ids
    }
    dst_entity = Mock(
        entityid="entity.dst",
        tags={
            "provider_type": "delta",
            "provider.access_mode": "catalog",
            "provider.table_name": "main.analytics.entity_dst",
        },
        merge_columns=[],
    )
    entities = {**input_entities, "entity.dst": dst_entity}
    der.get_entity_definition.side_effect = lambda eid: entities[eid]

    def read_stream(entity):
        reads.append((entity.entityid, "stream"))
        return frames[entity.entityid]

    def read_batch(entity):
        reads.append((entity.entityid, "batch"))
        return frames[entity.entityid]

    input_providers = {}
    for entity_id in input_entity_ids:
        if entity_id in non_streamable:
            provider = Mock()
        else:
            provider = Mock(spec=StreamableEntityProvider)
            provider.read_entity_as_stream.side_effect = read_stream
        provider.read_entity = Mock(side_effect=read_batch)
        input_providers[entity_id] = provider

    if configure_input_providers is not None:
        configure_input_providers(input_providers)

    out_provider = Mock(spec=StreamWritableEntityProvider)
    writer = Mock()
    writer.toTable.return_value = Mock(id="q-selection")

    def append_as_stream(df, entity, checkpoint_path):
        return writer

    out_provider.append_as_stream.side_effect = append_as_stream

    providers = {**input_providers, "entity.dst": out_provider}
    provider_registry.get_provider_for_entity.side_effect = lambda entity: providers[
        entity.entityid
    ]

    starter = SimplePipeStreamStarter(cs, dpr, provider_registry, der, epl, plp)
    return starter, pipe, reads, frames, out_provider


@dataclass(frozen=True)
class _StreamingSelectionCase:
    case_id: str
    input_entity_ids: Tuple[str, ...]
    driving_entity_ids: Optional[Tuple[str, ...]]
    expected_reads: Tuple[Tuple[str, str], ...]


_STREAMING_SELECTION_CASES = (
    _StreamingSelectionCase(
        case_id="default_first_input",
        input_entity_ids=("source.orders", "reference.customers"),
        driving_entity_ids=None,
        expected_reads=(
            ("source.orders", "stream"),
            ("reference.customers", "batch"),
        ),
    ),
    _StreamingSelectionCase(
        case_id="two_declared_driving_inputs_plus_reference",
        input_entity_ids=("source.orders", "source.lines", "reference.customers"),
        driving_entity_ids=("source.orders", "source.lines"),
        expected_reads=(
            ("source.orders", "stream"),
            ("source.lines", "stream"),
            ("reference.customers", "batch"),
        ),
    ),
    _StreamingSelectionCase(
        case_id="declared_driving_input_not_first",
        input_entity_ids=("reference.calendar", "source.orders"),
        driving_entity_ids=("source.orders",),
        expected_reads=(
            ("reference.calendar", "batch"),
            ("source.orders", "stream"),
        ),
    ),
)


@pytest.mark.parametrize(
    "case",
    _STREAMING_SELECTION_CASES,
    ids=lambda case: case.case_id,
)
def test_streaming_driving_entity_selection_matches_declared_inputs(case):
    starter, pipe, reads, frames, out_provider = _make_selection_starter(
        case.input_entity_ids, driving_entity_ids=case.driving_entity_ids
    )

    starter.start_pipe_stream("pipe1")

    assert tuple(reads) == case.expected_reads

    assert pipe.execute.call_args.args == ()
    expected_kwargs = tuple(eid.replace(".", "_") for eid in case.input_entity_ids)
    assert tuple(pipe.execute.call_args.kwargs) == expected_kwargs
    for entity_id in case.input_entity_ids:
        assert pipe.execute.call_args.kwargs[entity_id.replace(".", "_")] is frames[entity_id]

    out_provider.append_as_stream.assert_called_once()
    assert out_provider.append_as_stream.call_args.args[2] == "/checkpoints/pipe1"


def test_declared_driving_input_not_first_is_streamed(recwarn):
    starter, pipe, reads, frames, _ = _make_selection_starter(
        ("reference.calendar", "source.orders"),
        driving_entity_ids=("source.orders",),
    )

    starter.start_pipe_stream("pipe1")

    assert tuple(reads) == (
        ("reference.calendar", "batch"),
        ("source.orders", "stream"),
    )
    assert pipe.execute.call_args.args == ()
    assert tuple(pipe.execute.call_args.kwargs) == (
        "reference_calendar",
        "source_orders",
    )
    assert pipe.execute.call_args.kwargs["reference_calendar"] is frames["reference.calendar"]
    assert pipe.execute.call_args.kwargs["source_orders"] is frames["source.orders"]
    assert list(recwarn) == []


def test_selection_is_independent_of_use_watermark():
    starter, pipe, reads, frames, _ = _make_selection_starter(
        ("source.orders", "source.lines", "reference.customers"),
        driving_entity_ids=("source.orders", "source.lines"),
    )
    pipe.use_watermark = False

    starter.start_pipe_stream("pipe1")

    assert pipe.use_watermark is False
    assert tuple(reads) == (
        ("source.orders", "stream"),
        ("source.lines", "stream"),
        ("reference.customers", "batch"),
    )
    assert pipe.execute.call_args.args == ()
    assert pipe.execute.call_args.kwargs["source_orders"] is frames["source.orders"]
    assert pipe.execute.call_args.kwargs["source_lines"] is frames["source.lines"]
    assert pipe.execute.call_args.kwargs["reference_customers"] is frames["reference.customers"]


def test_non_streamable_driving_provider_raises_before_any_read():
    input_providers = {}

    starter, _, reads, _, _ = _make_selection_starter(
        ("source.orders", "source.lines", "reference.customers"),
        driving_entity_ids=("source.orders", "source.lines"),
        non_streamable_entity_ids=("source.lines",),
        configure_input_providers=input_providers.update,
    )

    with pytest.raises(TypeError) as exc_info:
        starter.start_pipe_stream("pipe1")

    message = str(exc_info.value)
    assert "source.lines" in message
    assert "delta" in message
    assert reads == []
    for provider in input_providers.values():
        provider.read_entity.assert_not_called()
        provider.read_entity_as_stream.assert_not_called()


def test_non_streamable_non_driving_provider_is_batch_read():
    input_providers = {}

    starter, _, reads, _, _ = _make_selection_starter(
        ("source.orders", "reference.customers"),
        driving_entity_ids=("source.orders",),
        non_streamable_entity_ids=("reference.customers",),
        configure_input_providers=input_providers.update,
    )

    starter.start_pipe_stream("pipe1")

    assert tuple(reads) == (
        ("source.orders", "stream"),
        ("reference.customers", "batch"),
    )
    input_providers["reference.customers"].read_entity.assert_called_once()
    input_providers["reference.customers"].read_entity_as_stream.assert_not_called()


def test_single_input_positional_fallback_passes_its_own_stream():
    starter, pipe, _, frames, out_provider = _make_selection_starter(("source.orders",))
    transformed_stream = Mock(name="legacy_transformed_stream")

    def execute(*args, **kwargs):
        if kwargs:
            raise TypeError("legacy pipe does not accept kwargs")
        assert args[0] is frames["source.orders"]
        return transformed_stream

    pipe.execute.side_effect = execute

    starter.start_pipe_stream("pipe1")

    assert pipe.execute.call_count == 2
    first_call, second_call = pipe.execute.call_args_list
    assert first_call.args == ()
    assert first_call.kwargs["source_orders"] is frames["source.orders"]
    assert second_call.args == (frames["source.orders"],)
    assert second_call.kwargs == {}
    out_provider.append_as_stream.assert_called_once()
    assert out_provider.append_as_stream.call_args.args[0] is transformed_stream


def test_multi_input_kwargs_type_error_is_not_retried_positionally():
    starter, pipe, _, frames, _ = _make_selection_starter(
        ("source.orders", "source.lines", "reference.customers"),
        driving_entity_ids=("source.orders", "source.lines"),
    )
    pipe.execute.side_effect = TypeError("kwargs failure")

    with pytest.raises(TypeError, match="kwargs failure"):
        starter.start_pipe_stream("pipe1")

    pipe.execute.assert_called_once()
    assert pipe.execute.call_args.args == ()
    assert pipe.execute.call_args.kwargs["source_orders"] is frames["source.orders"]
    assert pipe.execute.call_args.kwargs["source_lines"] is frames["source.lines"]
    assert pipe.execute.call_args.kwargs["reference_customers"] is frames["reference.customers"]


def test_streaming_selection_does_not_touch_watermark_state():
    input_providers = {}

    def configure(providers):
        input_providers.update(providers)
        for provider in providers.values():
            provider.get_cursor = Mock()
            provider.save_cursor = Mock()

    starter, pipe, reads, _, _ = _make_selection_starter(
        ("source.orders", "source.lines", "reference.customers"),
        driving_entity_ids=("source.orders", "source.lines"),
        configure_input_providers=configure,
    )
    pipe.use_watermark = True

    with patch("kindling.signaling.SignalEmitter.emit") as emit:
        starter.start_pipe_stream("pipe1")

    assert tuple(reads) == (
        ("source.orders", "stream"),
        ("source.lines", "stream"),
        ("reference.customers", "batch"),
    )
    for provider in input_providers.values():
        provider.get_cursor.assert_not_called()
        provider.save_cursor.assert_not_called()
    assert [call for call in emit.call_args_list if "persist.watermark_saved" in str(call)] == []


def test_zero_input_streaming_pipe_still_raises_value_error():
    starter, _, reads, _, _ = _make_selection_starter(())

    with pytest.raises(ValueError, match=r"Streaming pipe 'pipe1' has no input entities"):
        starter.start_pipe_stream("pipe1")

    assert reads == []


def test_merge_columns_route_to_merge_as_stream_when_provider_supports_merge():
    dst_entity = Mock(
        entityid="entity.dst",
        tags={"provider_type": "delta", "provider.access_mode": "catalog"},
        merge_columns=["order_id"],
    )
    out_provider = Mock(spec=_MergeCapableProvider)
    query = Mock(id="q-merge")
    out_provider.merge_as_stream.return_value = query

    starter, pipe = _make_starter(dst_entity, out_provider)
    result = starter.start_pipe_stream("pipe1")

    assert result is query
    out_provider.merge_as_stream.assert_called_once_with(
        pipe.execute.return_value, dst_entity, "/checkpoints/pipe1", options={}
    )
    out_provider.append_as_stream.assert_not_called()


def test_streaming_options_plumbed_through_to_merge_as_stream():
    """trigger/query_name passed to start_pipe_stream reach merge_as_stream;
    starter-only options (base_checkpoint_path) are not forwarded."""
    dst_entity = Mock(
        entityid="entity.dst",
        tags={"provider_type": "delta", "provider.access_mode": "catalog"},
        merge_columns=["order_id"],
    )
    out_provider = Mock(spec=_MergeCapableProvider)

    starter, pipe = _make_starter(dst_entity, out_provider)
    starter.start_pipe_stream(
        "pipe1",
        options={
            "base_checkpoint_path": "/custom-chk",
            "trigger": {"availableNow": True},
            "query_name": "orders-merge",
        },
    )

    out_provider.merge_as_stream.assert_called_once_with(
        pipe.execute.return_value,
        dst_entity,
        "/custom-chk/pipe1",
        options={"trigger": {"availableNow": True}, "query_name": "orders-merge"},
    )


def test_write_mode_append_tag_forces_append_despite_merge_columns():
    dst_entity = Mock(
        entityid="entity.dst",
        tags={
            "provider_type": "delta",
            "provider.access_mode": "catalog",
            "provider.table_name": "main.analytics.entity_dst",
            "write.mode": "append",
        },
        merge_columns=["order_id"],
    )
    out_provider = Mock(spec=_MergeCapableProvider)
    writer = Mock()
    query = Mock(id="q-append")
    writer.toTable.return_value = query
    out_provider.append_as_stream.return_value = writer

    starter, _ = _make_starter(dst_entity, out_provider)
    result = starter.start_pipe_stream("pipe1")

    assert result is query
    out_provider.merge_as_stream.assert_not_called()
    writer.toTable.assert_called_once_with("main.analytics.entity_dst")


def test_no_merge_columns_defaults_to_append():
    dst_entity = Mock(
        entityid="entity.dst",
        tags={
            "provider_type": "delta",
            "provider.access_mode": "catalog",
            "provider.table_name": "main.analytics.entity_dst",
        },
        merge_columns=[],
    )
    out_provider = Mock(spec=_MergeCapableProvider)
    writer = Mock()
    query = Mock(id="q-append-2")
    writer.toTable.return_value = query
    out_provider.append_as_stream.return_value = writer

    starter, _ = _make_starter(dst_entity, out_provider)
    result = starter.start_pipe_stream("pipe1")

    assert result is query
    out_provider.merge_as_stream.assert_not_called()


def test_write_mode_merge_tag_requires_merge_capable_provider():
    dst_entity = Mock(
        entityid="entity.dst",
        tags={"provider_type": "delta", "write.mode": "merge"},
        merge_columns=["order_id"],
    )
    out_provider = Mock(spec=StreamWritableEntityProvider)

    starter, _ = _make_starter(dst_entity, out_provider)
    with pytest.raises(TypeError, match="streaming merges"):
        starter.start_pipe_stream("pipe1")


def test_invalid_write_mode_tag_raises():
    dst_entity = Mock(
        entityid="entity.dst",
        tags={"provider_type": "delta", "write.mode": "upsert"},
        merge_columns=["order_id"],
    )
    out_provider = Mock(spec=_MergeCapableProvider)

    starter, _ = _make_starter(dst_entity, out_provider)
    with pytest.raises(ValueError, match="write.mode"):
        starter.start_pipe_stream("pipe1")


def test_derived_dataset_rejected_as_streaming_sink():
    dst_entity = Mock(
        entityid="entity.dst",
        tags={"provider_type": "delta", "dataset.kind": "derived"},
        merge_columns=None,
    )
    out_provider = Mock(spec=_MergeCapableProvider)

    starter, _ = _make_starter(dst_entity, out_provider)
    with pytest.raises(TypeError, match="derived dataset"):
        starter.start_pipe_stream("pipe1")

    out_provider.merge_as_stream.assert_not_called()
    out_provider.append_as_stream.assert_not_called()


def test_write_mode_insert_routes_to_merge_as_stream():
    dst_entity = Mock(
        entityid="entity.dst",
        tags={
            "provider_type": "delta",
            "provider.access_mode": "catalog",
            "write.mode": "insert",
        },
        merge_columns=["event_id"],
    )
    out_provider = Mock(spec=_MergeCapableProvider)
    query = Mock(id="q-insert")
    out_provider.merge_as_stream.return_value = query

    starter, pipe = _make_starter(dst_entity, out_provider)
    result = starter.start_pipe_stream("pipe1")

    assert result is query
    out_provider.merge_as_stream.assert_called_once_with(
        pipe.execute.return_value, dst_entity, "/checkpoints/pipe1", options={}
    )
    out_provider.append_as_stream.assert_not_called()


def test_write_mode_insert_requires_merge_capable_provider():
    dst_entity = Mock(
        entityid="entity.dst",
        tags={"provider_type": "delta", "write.mode": "insert"},
        merge_columns=["event_id"],
    )
    out_provider = Mock(spec=StreamWritableEntityProvider)

    starter, _ = _make_starter(dst_entity, out_provider)
    with pytest.raises(TypeError, match="streaming merges"):
        starter.start_pipe_stream("pipe1")
