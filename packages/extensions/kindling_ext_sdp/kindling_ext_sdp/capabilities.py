"""Capability model for SDP declaration targets.

The OSS `pyspark.pipelines` API (Spark 4.1+) and Databricks Lakeflow share
one declaration surface; Databricks layers extra capabilities on top
(expectations, AUTO CDC, incremental materialized-view refresh — see the
"Gap Between OSS SDP and Databricks Lakeflow" section of
``docs/proposals/declarative_pipelines_engine.md``).

The declaration engine validates registrations against a target
``CapabilitySet``: config or tags that request an adapter-tier feature while
declaring against a target that lacks it fail fast at declaration time with
an actionable diagnostic, instead of producing declarations the runtime
cannot execute.

Style note: this module follows the small-ABC-plus-``is_*``-helper pattern
of ``kindling.entity_provider`` — capability checks are tiny, composable
predicates over a frozen capability set.
"""

from dataclasses import dataclass
from enum import Enum
from typing import FrozenSet


class SdpFeature(str, Enum):
    """Features that exist above the common OSS declaration surface."""

    # Declarative data-quality constraints (@dp.expect*) — Databricks only.
    EXPECTATIONS = "expectations"
    # AUTO CDC flows (create_auto_cdc_flow / ..._from_snapshot_flow) with
    # built-in SCD Type 1/2 — Databricks only. Kindling `scd.*` entity tags
    # route here (Phase 5 of the proposal).
    AUTO_CDC = "auto_cdc"
    # Incremental materialized-view refresh (Enzyme). `refresh_policy:
    # incremental` is a Databricks hint, not a portable declaration.
    INCREMENTAL_MV_REFRESH = "incremental_mv_refresh"


@dataclass(frozen=True)
class CapabilitySet:
    """A named declaration target and the adapter-tier features it supports.

    ``engine_name`` doubles as the key under a pipe's ``engine:`` config
    block (see the proposal's example config: ``engine.sdp`` vs
    ``engine.databricks_sdp``).
    """

    engine_name: str
    features: FrozenSet[SdpFeature]

    def supports(self, feature: SdpFeature) -> bool:
        return feature in self.features


#: Vanilla Spark 4.1+ `pyspark.pipelines`: the common Layer-1 surface only.
OSS_SDP = CapabilitySet(engine_name="sdp", features=frozenset())

#: Databricks Lakeflow: interoperable superset of the OSS surface.
DATABRICKS_SDP = CapabilitySet(
    engine_name="databricks_sdp",
    features=frozenset(
        {
            SdpFeature.EXPECTATIONS,
            SdpFeature.AUTO_CDC,
            SdpFeature.INCREMENTAL_MV_REFRESH,
        }
    ),
)


def supports_expectations(capabilities: CapabilitySet) -> bool:
    """Check if the target supports declarative expectations."""
    return capabilities.supports(SdpFeature.EXPECTATIONS)


def supports_auto_cdc(capabilities: CapabilitySet) -> bool:
    """Check if the target supports AUTO CDC flows (SCD tag routing)."""
    return capabilities.supports(SdpFeature.AUTO_CDC)


def supports_incremental_mv_refresh(capabilities: CapabilitySet) -> bool:
    """Check if the target supports incremental MV refresh hints."""
    return capabilities.supports(SdpFeature.INCREMENTAL_MV_REFRESH)


#: Adapter-tier keys that may appear in a pipe's engine config block, mapped
#: to the feature the target must support for the key to be declarable.
ADAPTER_TIER_CONFIG_KEYS = {
    "expectations": SdpFeature.EXPECTATIONS,
    "refresh_policy": SdpFeature.INCREMENTAL_MV_REFRESH,
}
