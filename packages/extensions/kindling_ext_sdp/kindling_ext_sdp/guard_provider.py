"""Write-inert entity-provider guard for SDP mode.

"SDP owns persistence" (``declarative_pipelines_engine.md``, Provider
Behavior in SDP Mode): while the SDP engine executes the pipeline, every
imperative write path through Kindling's entity providers must fail
immediately — reads stay live. This module implements the mechanism the
Phase-1 notes recommended: a distinct engine-mode binding that wraps each
provider instance at the registry seam, rather than a per-entity tag or
``if sdp_mode:`` branches inside every provider.

The guard is fail-fast by construction: it also covers write paths that
declaration-time static validation cannot see (side-effecting pipe bodies,
extensions calling providers directly), which is what makes deferring
pipe-body introspection safe.

Raising style follows the precedent of ``entity_provider_delta.py``'s
``ReadOnlyEntityError`` — raised from the write path, naming the entity
and the reason.
"""

from typing import Any

#: Every provider method that mutates entity storage, across the write
#: ABCs (``WritableEntityProvider``, ``StreamWritableEntityProvider``,
#: ``DestinationEnsuringProvider``) and the Delta provider's own surface.
WRITE_PATH_METHODS = (
    "write_to_entity",
    "append_to_entity",
    "merge_to_entity",
    "append_as_stream",
    "ensure_destination",
    "ensure_entity_table",
)


class SdpModeWriteError(Exception):
    """An imperative write was attempted while SDP owns persistence."""


def _entity_id_from_args(args: tuple, kwargs: dict) -> str:
    for value in list(args) + list(kwargs.values()):
        entity_id = getattr(value, "entityid", None)
        if entity_id:
            return str(entity_id)
    return "<unknown>"


class SdpWriteGuardProvider:
    """Wraps a provider: reads delegate unchanged, writes raise.

    Not a subclass of the provider ABCs — it mirrors whatever surface the
    wrapped provider actually has via delegation, so a guarded provider
    never claims a capability its inner provider lacks. Write-path methods
    are shadowed explicitly and raise :class:`SdpModeWriteError` whether or
    not the inner provider implements them.
    """

    def __init__(self, inner: Any):
        self._inner = inner

    def __getattr__(self, name: str) -> Any:
        # Only reached for names not defined on this class: read paths and
        # provider-specific helpers delegate; write paths never get here.
        return getattr(self._inner, name)

    def _refuse(self, method: str, args: tuple, kwargs: dict) -> None:
        entity_id = _entity_id_from_args(args, kwargs)
        raise SdpModeWriteError(
            f"SDP owns persistence — '{method}' attempted an imperative "
            f"write to entity '{entity_id}' in SDP mode. Pipes must return "
            "DataFrames and let the declared pipeline persist them; remove "
            "the manual write, or run under the runner engine instead."
        )

    def write_to_entity(self, *args: Any, **kwargs: Any) -> None:
        self._refuse("write_to_entity", args, kwargs)

    def append_to_entity(self, *args: Any, **kwargs: Any) -> None:
        self._refuse("append_to_entity", args, kwargs)

    def merge_to_entity(self, *args: Any, **kwargs: Any) -> None:
        self._refuse("merge_to_entity", args, kwargs)

    def append_as_stream(self, *args: Any, **kwargs: Any) -> None:
        self._refuse("append_as_stream", args, kwargs)

    def ensure_destination(self, *args: Any, **kwargs: Any) -> None:
        self._refuse("ensure_destination", args, kwargs)

    def ensure_entity_table(self, *args: Any, **kwargs: Any) -> None:
        self._refuse("ensure_entity_table", args, kwargs)
