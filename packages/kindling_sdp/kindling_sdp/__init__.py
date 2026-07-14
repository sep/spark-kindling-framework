"""Spark Declarative Pipelines (SDP) declaration engine for Kindling.

Phase 1: abstract declaration-engine interface, internal/external input
classification, capability gating, and the pure-metadata DeclarationPlan.
No ``pyspark.pipelines`` dependency — declaration emission is Phase 2.

See ``docs/proposals/declarative_pipelines_engine.md`` and
``docs/proposals/sdp_engine_phase1_notes.md``.
"""

from kindling_sdp.capabilities import (
    ADAPTER_TIER_CONFIG_KEYS,
    DATABRICKS_SDP,
    OSS_SDP,
    CapabilitySet,
    SdpFeature,
    supports_auto_cdc,
    supports_expectations,
    supports_incremental_mv_refresh,
)
from kindling_sdp.declaration_engine import (
    DATASET_TYPE_CONFIG_KEY,
    DATASET_TYPE_TAG,
    DeclarationEngine,
)
from kindling_sdp.declaration_plan import (
    ClassifiedInput,
    DatasetDeclaration,
    DatasetType,
    DeclarationIssue,
    DeclarationPlan,
    DeclarationValidationError,
    InputClassification,
)

__version__ = "0.1.0"

__all__ = [
    "ADAPTER_TIER_CONFIG_KEYS",
    "CapabilitySet",
    "ClassifiedInput",
    "DATABRICKS_SDP",
    "DATASET_TYPE_CONFIG_KEY",
    "DATASET_TYPE_TAG",
    "DatasetDeclaration",
    "DatasetType",
    "DeclarationEngine",
    "DeclarationIssue",
    "DeclarationPlan",
    "DeclarationValidationError",
    "InputClassification",
    "OSS_SDP",
    "SdpFeature",
    "supports_auto_cdc",
    "supports_expectations",
    "supports_incremental_mv_refresh",
]
