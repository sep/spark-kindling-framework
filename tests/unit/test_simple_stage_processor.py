from unittest.mock import MagicMock

from kindling.data_pipes import PipeMetadata
from kindling.simple_stage_processor import StageProcessor


def _pipe(pipeid: str, use_watermark: bool) -> PipeMetadata:
    return PipeMetadata(
        pipeid=pipeid,
        name=pipeid,
        execute=MagicMock(),
        tags={},
        input_entity_ids=["source.entity"],
        output_entity_id="target.entity",
        output_type="delta",
        use_watermark=use_watermark,
    )


def _make_processor(pipes):
    dpr = MagicMock()
    dpr.get_pipe_ids.return_value = pipes.keys()
    dpr.get_pipe_definition.side_effect = lambda pipeid: pipes[pipeid]
    ep = MagicMock()
    dep = MagicMock()
    wef = MagicMock()
    wef.get_watermark_entity_for_layer.return_value = MagicMock()
    trace_provider = MagicMock()
    trace_provider.span.return_value.__enter__.return_value = None
    trace_provider.span.return_value.__exit__.return_value = False

    return StageProcessor(dpr, ep, dep, wef, trace_provider), ep, dep, wef


def test_stage_processor_does_not_ensure_watermark_table_without_watermark_enabled_pipe():
    processor, ep, dep, wef = _make_processor(
        {
            "bronze.load_orders": _pipe("bronze.load_orders", use_watermark=False),
            "bronze.load_customers": _pipe("bronze.load_customers", use_watermark=False),
        }
    )

    processor.execute("bronze", "Bronze", {}, "bronze")

    wef.get_watermark_entity_for_layer.assert_not_called()
    ep.ensure_entity_table.assert_not_called()
    dep.run_datapipes.assert_called_once_with(["bronze.load_orders", "bronze.load_customers"])


def test_stage_processor_ensures_watermark_table_for_watermark_enabled_pipe():
    processor, ep, dep, wef = _make_processor(
        {
            "bronze.load_orders": _pipe("bronze.load_orders", use_watermark=True),
        }
    )
    watermark_entity = wef.get_watermark_entity_for_layer.return_value

    processor.execute("bronze", "Bronze", {}, "bronze")

    wef.get_watermark_entity_for_layer.assert_called_once_with("bronze")
    ep.ensure_entity_table.assert_called_once_with(watermark_entity)
    dep.run_datapipes.assert_called_once_with(["bronze.load_orders"])
