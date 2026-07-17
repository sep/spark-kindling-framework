"""Kindling engine-extension entry point for the SDP execution engine.

Kindling core resolves ``initialize(engine="sdp")`` by importing
``kindling_ext_sdp`` and calling its ``engine_extension()`` factory — core
knows only that contract, never SDP itself. This module is that contract's
implementation:

- ``owns_incrementality = True`` — SDP manages incremental reads
  (streaming checkpoints, MV refresh), so core never registers the
  watermark aspect.
- ``activate()`` — installs the write-inert provider guard ("SDP owns
  persistence"), called once after framework initialization.
- ``declare_pipeline()`` — backs ``kindling.declare_pipeline()``, the
  entry point's mandatory last step.
"""

from typing import Any, List, Optional


class SdpEngineExtension:
    """The ``engine="sdp"`` execution-engine extension."""

    name = "sdp"
    owns_incrementality = True

    def activate(self) -> None:
        from kindling_ext_sdp.bootstrap import activate_sdp_mode

        activate_sdp_mode()

    def declare_pipeline(self, pipe_ids: Optional[List[str]] = None) -> Any:
        from kindling_ext_sdp.bootstrap import declare_pipeline

        return declare_pipeline(pipe_ids=pipe_ids)


def engine_extension() -> SdpEngineExtension:
    """Factory resolved by kindling core for ``engine="sdp"``."""
    return SdpEngineExtension()
