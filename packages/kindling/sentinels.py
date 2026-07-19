"""Shared sentinel values.

Kept in a leaf module (no kindling imports) so both low-level executors and
user-facing facades can share sentinels without circular imports.
"""

from typing import Any


class _Unset:
    """Sentinel type for 'not passed — resolve from configuration'."""

    def __repr__(self):
        return "UNSET"


#: Default for execution options: fall through to `kindling.execution.*`
#: config, then to the built-in default. Pass a real value to override
#: config just-in-time (spot-testing); config is the primary interface.
#: Distinct from ``None``, which is itself a meaningful value for some
#: options (``pipe_timeout=None`` disables the timeout).
UNSET: Any = _Unset()
