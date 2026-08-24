"""Kindling engine-extension entry point for ``engine="databricks_sdp"``.

Resolved by kindling core's engine-module mapping (import
``kindling_ext_databricks``, call ``engine_extension()``). The engine selector
remains ``databricks_sdp`` while the package is the Databricks extension
umbrella.
"""

from typing import Any, List, Optional


class DatabricksSdpEngineExtension:
    """The ``engine="databricks_sdp"`` execution-engine extension.

    Same execution-mode posture as the OSS extension: Lakeflow owns
    incrementality and persistence, so the watermark aspect is never
    registered and the write-inert provider guard is installed.
    """

    name = "databricks_sdp"
    owns_incrementality = True
    # temporal_lowering.declare_stratified_temporal lowers each
    # DataEvents.base_event declaration to its own native Lakeflow
    # append_flow into a shared stratum-0 streaming table -- it re-derives
    # base-event wiring directly from the registry and never runs
    # kindling_ext_temporal.chain's composite pipe body, so that module's
    # single-driving-entity requirement doesn't apply here. The plain OSS
    # pyspark.pipelines engine has no equivalent native lowering yet, so it
    # does NOT declare this.
    supports_multi_source_temporal_chain = True

    def activate(self) -> None:
        from kindling_ext_sdp.bootstrap import activate_sdp_mode

        activate_sdp_mode()

    def declare_pipeline(self, pipe_ids: Optional[List[str]] = None) -> Any:
        from kindling_ext_databricks.engine import DatabricksSdpEngine
        from kindling_ext_sdp.bootstrap import declare_pipeline

        return declare_pipeline(pipe_ids=pipe_ids, engine_factory=DatabricksSdpEngine)


def engine_extension() -> DatabricksSdpEngineExtension:
    """Factory resolved by kindling core for ``engine="databricks_sdp"``."""
    return DatabricksSdpEngineExtension()
