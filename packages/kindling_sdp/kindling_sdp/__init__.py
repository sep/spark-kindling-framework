"""Spark Declarative Pipelines (SDP) declaration engine for Kindling.

Phase 1: abstract declaration-engine interface, internal/external input
classification, capability gating, and the pure-metadata DeclarationPlan.

Phase 2: the OSS emission engine (``OssSdpEngine``), the write-inert
provider guard, the SDP bootstrap surface (``kindling.initialize(
engine="sdp")`` / ``kindling.declare_pipeline()``), and the local
``spark-pipelines dry-run`` validation harness.

Importing this package never imports ``pyspark.pipelines``; the ``dp``
module is resolved lazily at declaration time (Spark 4.1+) or injected.

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
from kindling_sdp.dry_run import (
    DryRunResult,
    SparkPipelinesCliNotFoundError,
    dry_run,
    write_pipeline_spec,
)
from kindling_sdp.engine_extension import SdpEngineExtension, engine_extension
from kindling_sdp.guard_provider import SdpModeWriteError, SdpWriteGuardProvider
from kindling_sdp.oss_engine import OssSdpEngine, SdpRuntimeUnavailableError

__version__ = "0.2.0"

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
    "DryRunResult",
    "InputClassification",
    "OSS_SDP",
    "OssSdpEngine",
    "SdpEngineExtension",
    "SdpFeature",
    "SdpModeWriteError",
    "SdpRuntimeUnavailableError",
    "SdpWriteGuardProvider",
    "SparkPipelinesCliNotFoundError",
    "dry_run",
    "engine_extension",
    "write_pipeline_spec",
    "supports_auto_cdc",
    "supports_expectations",
    "supports_incremental_mv_refresh",
]
