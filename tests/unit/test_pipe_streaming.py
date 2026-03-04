from unittest.mock import Mock, patch

from kindling.data_pipes import PipeMetadata
from kindling.entity_provider import StreamableEntityProvider, StreamWritableEntityProvider
from kindling.pipe_streaming import SimplePipeStreamStarter


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
    cs.get.side_effect = (
        lambda key: "/checkpoints" if key == "kindling.storage.checkpoint_root" else None
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
            "provider.access_mode": "forName",
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
    cs.get.side_effect = (
        lambda key: "/checkpoints" if key == "kindling.storage.checkpoint_root" else None
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
    dst_entity = Mock(entityid="entity.dst", tags={"provider_type": "delta"})
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
    cs.get.side_effect = (
        lambda key: "/checkpoints" if key == "kindling.storage.checkpoint_root" else None
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
        tags={"provider_type": "delta", "provider.access_mode": "forName"},
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
