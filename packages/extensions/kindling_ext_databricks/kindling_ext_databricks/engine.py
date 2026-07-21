"""Databricks Lakeflow SDP engine: the OSS core plus adapter-tier features.

The bridge is augmentation, not translation (see the proposal's gap
analysis): Lakeflow is an interoperable superset of OSS ``pyspark.
pipelines``, so this engine reuses the OSS emission wholesale and layers
Databricks-only capabilities on top. Phase 3 layers **expectations**;
AUTO CDC (SCD mapping) is Phase 5.

Expectations come from the pipe's engine config (adapter-tier keys are
capability-gated by the core — declaring them against ``OSS_SDP`` already
fails fast at validation):

.. code-block:: yaml

    datapipes:
      silver.orders:
        engine:
          databricks_sdp:
            expectations:            # violations counted, rows kept (warn)
              valid_order_id: "order_id IS NOT NULL"
            expectations_drop:       # violating rows dropped
              positive_qty: "quantity > 0"
            expectations_fail:       # violation fails the update
              no_future_dates: "order_date <= current_date()"

``refresh_policy: incremental`` is accepted (gated as adapter-tier) but
currently emits nothing: incremental materialized-view refresh on
Databricks (Enzyme) is engine behavior, not a declaration keyword.
Documented as a hint pending verification against a live workspace.
"""

from typing import Any, Callable, Dict, List, Optional

from kindling_ext_databricks.auto_cdc import (
    SCD_SOURCE_SUFFIX,
    ScdSpec,
    scd_spec_from_tags,
    validate_scd_spec,
)
from kindling_ext_sdp.capabilities import DATABRICKS_SDP, CapabilitySet
from kindling_ext_sdp.declaration_plan import (
    DatasetDeclaration,
    DatasetType,
    DeclarationIssue,
    pipeline_dataset_name,
)
from kindling_ext_sdp.oss_engine import OssSdpEngine

#: Engine-config key -> Lakeflow expectation decorator (warn/drop/fail).
EXPECTATION_DECORATORS = {
    "expectations": "expect_all",
    "expectations_drop": "expect_all_or_drop",
    "expectations_fail": "expect_all_or_fail",
}


class DatabricksSdpEngine(OssSdpEngine):
    """OSS emission with Databricks expectations layered on."""

    def __init__(
        self,
        entity_registry,
        pipe_registry,
        capabilities: CapabilitySet = DATABRICKS_SDP,
        **kwargs: Any,
    ):
        super().__init__(entity_registry, pipe_registry, capabilities=capabilities, **kwargs)

    def validate(self, pipe_ids: Optional[List[str]] = None) -> List[DeclarationIssue]:
        """Core validation plus the AUTO CDC mapping requirements for
        SCD-tagged targets (Phase 5)."""
        issues = super().validate(pipe_ids)
        seen_scd_issues = set()
        for pipe_id in self._select_pipe_ids(pipe_ids):
            pipe = self.pipe_registry.get_pipe_definition(pipe_id)
            if pipe is None or not pipe.output_entity_id:
                continue
            entity = self.entity_registry.get_entity_definition(pipe.output_entity_id)
            if entity is None:
                continue
            spec = scd_spec_from_tags(entity.tags)
            if spec is None:
                continue
            for code, reason in validate_scd_spec(spec, list(entity.merge_columns or ())):
                issue_key = (pipe.output_entity_id, code)
                if issue_key in seen_scd_issues:
                    continue
                seen_scd_issues.add(issue_key)
                issues.append(DeclarationIssue(pipe_id=pipe_id, code=code, reason=reason))
        return issues

    def _declare_dataset(self, dp, dataset: DatasetDeclaration) -> None:
        scd_spec = scd_spec_from_tags(dataset.tags)
        if scd_spec is not None:
            self._declare_scd_dataset(dp, dataset, scd_spec)
            return
        if dataset.dataset_type is not DatasetType.MATERIALIZED_VIEW:
            raise NotImplementedError(
                f"Dataset '{dataset.name}': dataset_type "
                f"'{dataset.dataset_type.value}' is not supported yet — "
                "streaming tables and append flows are Phase 4."
            )
        query_function = self._build_dataset_function(dataset)
        query_function = self._apply_expectations(dp, dataset, query_function)
        dp.materialized_view(**self._declaration_kwargs(dataset))(query_function)

    # ------------------------------------------------------------------ #
    # AUTO CDC (Phase 5): SCD declared flows                               #
    # ------------------------------------------------------------------ #

    def _declare_scd_dataset(self, dp, dataset: DatasetDeclaration, spec: ScdSpec) -> None:
        """Declare an SCD target as streaming table + AUTO CDC flow.

        Three declarations per the Lakeflow CDC pattern:

        1. A pipeline-scoped view holding the pipe's change/snapshot
           source (expectations, if any, attach here — data quality is
           checked on the incoming feed). For a CHANGE FEED the driving
           input (first, per the runner's driving-source convention) is
           read with ``spark.readStream.table()`` so the flow consumes it
           incrementally — no hand-rolled foreachBatch; remaining inputs
           stay batch reads (stream-static joins). A SNAPSHOT source keeps
           batch reads: the API diffs whole snapshots per update.
        2. ``create_streaming_table`` for the target. The entity's schema
           is deliberately NOT passed: AUTO CDC emits ``__START_AT``/
           ``__END_AT``, not the runner engine's effective-date columns.
        3. ``create_auto_cdc_flow`` (change feed) or
           ``create_auto_cdc_from_snapshot_flow`` (snapshot — sequencing
           comes from ingestion order, not scd.sequence_by).
        """
        entity = self.entity_registry.get_entity_definition(dataset.name)
        if entity is None:
            raise RuntimeError(
                f"Dataset '{dataset.name}': output entity could not be resolved "
                "while declaring its AUTO CDC flow."
            )
        target_name = pipeline_dataset_name(dataset.name)
        source_name = f"{target_name}{SCD_SOURCE_SUFFIX}"

        view_decorator = getattr(dp, "temporary_view", None) or getattr(dp, "view", None)
        if view_decorator is None:
            raise RuntimeError(
                f"Dataset '{dataset.name}': this pipelines runtime exposes "
                "no view decorator (temporary_view/view) to declare the "
                "AUTO CDC source with."
            )
        query_function = self._build_dataset_function(
            dataset, stream_first_input=not spec.is_snapshot
        )
        query_function = self._apply_expectations(dp, dataset, query_function)
        view_decorator(name=source_name)(query_function)

        target_kwargs = self._declaration_kwargs(dataset)
        target_kwargs.pop("schema", None)  # runner-shape schema; see docstring
        dp.create_streaming_table(**target_kwargs)

        keys = list(entity.merge_columns or ())
        if spec.is_snapshot:
            dp.create_auto_cdc_from_snapshot_flow(
                target=target_name,
                source=source_name,
                keys=keys,
                stored_as_scd_type=int(spec.scd_type),
            )
            return

        flow_kwargs: Dict[str, Any] = dict(
            target=target_name,
            source=source_name,
            keys=keys,
            sequence_by=spec.sequence_by,
            stored_as_scd_type=int(spec.scd_type),
        )
        if spec.delete_when:
            from pyspark.sql.functions import expr

            flow_kwargs["apply_as_deletes"] = expr(spec.delete_when)
        # Parity with #159: the sequence column is ordering authority, not
        # content — a row whose only difference is a newer sequence value
        # must not create a new history version.
        if spec.tracked_columns:
            flow_kwargs["track_history_column_list"] = list(spec.tracked_columns)
        elif spec.sequence_by:
            flow_kwargs["track_history_except_column_list"] = [spec.sequence_by]
        dp.create_auto_cdc_flow(**flow_kwargs)

    def _apply_expectations(self, dp, dataset: DatasetDeclaration, query_function: Callable):
        """Wrap the dataset function in Lakeflow expectation decorators."""
        for config_key, decorator_name in EXPECTATION_DECORATORS.items():
            expectations = self._resolve_expectations(dataset.pipe_id, config_key)
            if not expectations:
                continue
            decorator = getattr(dp, decorator_name, None)
            if decorator is None:
                raise RuntimeError(
                    f"Dataset '{dataset.name}' declares '{config_key}' but "
                    f"this pipelines runtime has no '{decorator_name}' — "
                    "expectations require the Databricks Lakeflow runtime, "
                    "not OSS pyspark.pipelines."
                )
            query_function = decorator(expectations)(query_function)
        return query_function

    def _resolve_expectations(self, pipe_id: str, config_key: str) -> Dict[str, str]:
        """Merge one expectation block across the engine-config precedence
        chain (common ``sdp`` block first, ``databricks_sdp`` on top)."""
        merged: Dict[str, str] = {}
        for engine_name in reversed(self._engine_block_precedence()):
            block = self._pipe_engine_block(pipe_id, engine_name).get(config_key)
            if isinstance(block, dict):
                merged.update({str(k).strip(): str(v).strip() for k, v in block.items()})
        return merged
