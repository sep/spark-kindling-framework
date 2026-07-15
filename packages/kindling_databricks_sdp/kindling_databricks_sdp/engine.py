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

from typing import Any, Callable, Dict

from kindling_sdp.capabilities import DATABRICKS_SDP, CapabilitySet
from kindling_sdp.declaration_plan import DatasetDeclaration, DatasetType
from kindling_sdp.oss_engine import OssSdpEngine

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

    def _declare_dataset(self, dp, dataset: DatasetDeclaration) -> None:
        if dataset.dataset_type is not DatasetType.MATERIALIZED_VIEW:
            raise NotImplementedError(
                f"Dataset '{dataset.name}': dataset_type "
                f"'{dataset.dataset_type.value}' is not supported yet — "
                "streaming tables and append flows are Phase 4."
            )
        query_function = self._build_dataset_function(dataset)
        query_function = self._apply_expectations(dp, dataset, query_function)
        dp.materialized_view(**self._declaration_kwargs(dataset))(query_function)

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
