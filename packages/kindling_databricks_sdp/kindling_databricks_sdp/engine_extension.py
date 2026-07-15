"""Kindling engine-extension entry point for ``engine="databricks_sdp"``.

Resolved by kindling core's naming convention (import
``kindling_databricks_sdp``, call ``engine_extension()``) — no core
changes were needed to add this engine, by design.
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

    def activate(self) -> None:
        from kindling_sdp.bootstrap import activate_sdp_mode

        activate_sdp_mode()

    def declare_pipeline(self, pipe_ids: Optional[List[str]] = None) -> Any:
        from kindling_databricks_sdp.engine import DatabricksSdpEngine
        from kindling_sdp.bootstrap import declare_pipeline

        return declare_pipeline(pipe_ids=pipe_ids, engine_factory=DatabricksSdpEngine)


def engine_extension() -> DatabricksSdpEngineExtension:
    """Factory resolved by kindling core for ``engine="databricks_sdp"``."""
    return DatabricksSdpEngineExtension()
