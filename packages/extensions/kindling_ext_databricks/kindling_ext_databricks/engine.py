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

from typing import Any, Callable, Dict, List, Optional, Tuple

from kindling.data_pipes import PipeMetadata
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
        # Chain pipes are custom-lowered (stratified graph): the conditions
        # current view is read through the entity name mapper by the
        # lowering itself, so the generic external-read check's
        # current_view rejection does not apply to them.
        chain_pipe_ids = {
            pipe_id
            for pipe_id in self._select_pipe_ids(pipe_ids)
            if (pipe := self.pipe_registry.get_pipe_definition(pipe_id)) is not None
            and str((pipe.tags or {}).get("temporal.kind", "")).startswith("chain_")
        }
        issues = [
            issue
            for issue in issues
            if not (
                issue.pipe_id in chain_pipe_ids and issue.code == "external_input_not_declarable"
            )
        ]
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

    def _emitted_dataset_names(self, pipe) -> List[Tuple[str, str]]:
        """Reserve outputs plus the adapter's generated dataset namespace."""
        names = super()._emitted_dataset_names(pipe)
        if not names:
            return names
        kind = str((pipe.tags or {}).get("temporal.kind", ""))
        if kind == "chain_episodes":
            # Share the reservation identity with the events sibling that
            # owns emission, but still validate explicitly selected outputs.
            owner, target = names[0]
            return [(f"{owner} (chain episodes)", target)]
        entity = self.entity_registry.get_entity_definition(pipe.output_entity_id)
        tags = (entity.tags if entity else None) or {}
        owner, target = names[0]
        if kind == "chain_events":
            from kindling_ext_databricks.temporal_lowering import (
                DETERMINATIONS_SUFFIX,
                SNAPSHOT_SUFFIX,
                STRATUM_SUFFIX,
            )

            episodes_pipe, max_generations = self._temporal_chain_settings(pipe.tags or {})
            names.extend(
                (
                    f"{owner} (temporal stratum {generation})",
                    f"{target}{STRATUM_SUFFIX}{generation}",
                )
                for generation in range(max_generations + 1)
            )
            if episodes_pipe:
                episodes_id = episodes_pipe.output_entity_id
                episodes = self.dataset_name(episodes_id)
                names.extend(
                    [
                        (f"{episodes_id} (chain episodes)", episodes),
                        (f"{episodes_id} (episode snapshot)", f"{episodes}{SNAPSHOT_SUFFIX}"),
                        (f"{owner} (determinations)", f"{target}{DETERMINATIONS_SUFFIX}"),
                        (f"{owner} (higher stratum)", f"{target}{STRATUM_SUFFIX}hi"),
                    ]
                )
            return names
        if scd_spec_from_tags(tags) is not None:
            names.append((f"{owner} (AUTO CDC source)", f"{target}{SCD_SOURCE_SUFFIX}"))
        return names

    def _temporal_chain_settings(
        self, pipe_tags: Dict[str, str]
    ) -> Tuple[Optional[PipeMetadata], int]:
        """Resolve the sibling and topology shared by validation and emission.

        Takes the chain pipe's OWN tags, not its output entity's:
        ``collapse_temporal_chain`` stamps ``temporal.chain_id`` onto the two
        composite chain pipes only. Passing entity tags here silently reads
        the ``"default"`` fallback, which finds no sibling for any other
        chain id and drops the whole episode branch from the graph.
        """
        from kindling_ext_temporal.chain import (
            DEFAULT_MAX_GENERATIONS,
            MAX_GENERATIONS_CONFIG_KEY,
            chain_episodes_pipe_id,
        )

        chain_id = (pipe_tags or {}).get("temporal.chain_id", "default")
        episodes_pipe = self.pipe_registry.get_pipe_definition(chain_episodes_pipe_id(chain_id))
        if episodes_pipe is not None and not episodes_pipe.output_entity_id:
            episodes_pipe = None
        try:
            from kindling.injection import GlobalInjector
            from kindling.spark_config import ConfigService

            value = GlobalInjector.get(ConfigService).get(MAX_GENERATIONS_CONFIG_KEY, None)
        except Exception:  # noqa: BLE001 - config service unavailable in bare tests
            value = None
        max_generations = DEFAULT_MAX_GENERATIONS if value is None else int(value)
        return episodes_pipe, max_generations

    def _declare_dataset(self, dp, dataset: DatasetDeclaration) -> None:
        # Chain markers live on the PIPE's tags; dataset.tags carries the
        # output entity's tags (temporal.kind=events/episodes there).
        pipe = self.pipe_registry.get_pipe_definition(dataset.pipe_id)
        temporal_kind = str(((pipe.tags if pipe else None) or {}).get("temporal.kind", ""))
        if temporal_kind == "chain_events":
            self._declare_temporal_chain(dp, dataset, pipe)
            return
        if temporal_kind == "chain_episodes":
            # Emitted together with its chain_events sibling.
            return
        scd_spec = scd_spec_from_tags(dataset.tags)
        if scd_spec is not None:
            self._declare_scd_dataset(dp, dataset, scd_spec)
            return
        if dataset.streaming_source_inputs:
            self._declare_streaming_source_dataset(dp, dataset)
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

    def _declare_streaming_source_dataset(self, dp, dataset: DatasetDeclaration) -> None:
        """Lower a provider-owned streaming source to a Lakeflow append flow.

        The output entity's runner-shape ``schema`` is not forwarded to
        ``create_streaming_table`` until Lakeflow platform evidence proves
        the exact schema contract; the target schema is inferred from the
        append-flow DataFrame.
        """
        target_name = self.dataset_name(dataset.name)
        flow_name = f"{target_name}_flow"
        query_function = self._build_dataset_function(dataset, stream_driving_inputs=True)
        query_function.__name__ = flow_name
        query_function.__qualname__ = flow_name
        query_function = self._apply_expectations(dp, dataset, query_function)
        query_function.__name__ = flow_name
        query_function.__qualname__ = flow_name

        target_kwargs = self._declaration_kwargs(dataset)
        target_kwargs.pop("schema", None)  # runner-shape schema; see docstring
        dp.create_streaming_table(**target_kwargs)
        dp.append_flow(target=target_name, name=flow_name)(query_function)

    def _declare_temporal_chain(self, dp, dataset: DatasetDeclaration, pipe) -> None:
        """Lower a temporal chain-events pipe as the stratified dataset graph.

        Requires kindling-ext-temporal (soft dependency: only apps that
        registered chain pipes reach this branch). The chain_episodes
        sibling — found by chain id through the pipe registry — is emitted
        here too, so both halves share one wiring computation. ``pipe`` is
        the chain_events pipe itself: the chain id is a pipe tag, absent
        from ``dataset.tags`` (the output entity's).
        """
        try:
            from kindling_ext_databricks.temporal_lowering import (
                declare_stratified_temporal,
            )
        except ImportError as exc:
            raise RuntimeError(
                f"Dataset '{dataset.name}' is a temporal chain pipe but "
                "kindling-ext-temporal is not installed in this pipeline "
                "environment."
            ) from exc

        episodes_pipe, max_generations = self._temporal_chain_settings(pipe.tags if pipe else {})
        episodes_name = self.dataset_name(episodes_pipe.output_entity_id) if episodes_pipe else None

        declare_stratified_temporal(
            dp,
            events_name=self.dataset_name(dataset.name),
            episodes_name=episodes_name,
            max_generations=max_generations,
            mode=self._temporal_execution_mode(),
            strata_materialization=self._temporal_strata_materialization(),
        )

    def _temporal_strata_materialization(self) -> str:
        """Resolve how the numbered event strata are persisted.

        Same read-here-not-in-validation reasoning as
        ``_temporal_execution_mode``; the dataset NAMES are identical either
        way, so validation is unaffected by this setting — only whether a
        table is created behind each name.
        """
        from kindling.injection import GlobalInjector
        from kindling.spark_config import ConfigService

        return str(
            GlobalInjector.get(ConfigService).get(
                "kindling.lakeflow.temporal_strata_materialization", "table"
            )
        )

    def _temporal_execution_mode(self) -> str:
        """Resolve the temporal-chain execution mode for this declaration.

        Read here rather than in ``_temporal_chain_settings`` because that
        helper is also reached from ``_emitted_dataset_names`` inside
        ``validate()``, which must return every issue instead of raising.
        This runs before any Lakeflow decorator or target-creation call, so
        an invalid value still fails fast without a partial graph. The
        generated dataset names are mode-independent, so validation does not
        need the value.
        """
        from kindling.injection import GlobalInjector
        from kindling.spark_config import ConfigService

        return str(
            GlobalInjector.get(ConfigService).get("kindling.lakeflow.temporal_mode", "streaming")
        )

    # ------------------------------------------------------------------ #
    # AUTO CDC (Phase 5): SCD declared flows                               #
    # ------------------------------------------------------------------ #

    def _declare_scd_dataset(self, dp, dataset: DatasetDeclaration, spec: ScdSpec) -> None:
        """Declare an SCD target as streaming table + AUTO CDC flow.

        Three declarations per the Lakeflow CDC pattern:

        1. A pipeline-scoped view holding the pipe's change/snapshot
           source (expectations, if any, attach here — data quality is
           checked on the incoming feed). For a CHANGE FEED the inputs
           selected by ``driving_entity_ids`` are read with
           ``spark.readStream.table()`` so the flow consumes them
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
        target_name = self.dataset_name(dataset.name)
        source_name = f"{target_name}{SCD_SOURCE_SUFFIX}"

        view_decorator = getattr(dp, "temporary_view", None) or getattr(dp, "view", None)
        if view_decorator is None:
            raise RuntimeError(
                f"Dataset '{dataset.name}': this pipelines runtime exposes "
                "no view decorator (temporary_view/view) to declare the "
                "AUTO CDC source with."
            )
        query_function = self._build_dataset_function(
            dataset, stream_driving_inputs=not spec.is_snapshot
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
