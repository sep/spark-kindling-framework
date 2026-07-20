"""Databricks Lakeflow adapter for Kindling's SDP declaration engine.

Layers Databricks-only capabilities on the OSS ``kindling_ext_sdp`` core:
Phase 3 adds expectations; AUTO CDC (SCD2 declared flows) is Phase 5.
Selected via ``kindling.initialize(engine="databricks_sdp")`` — resolved
through core's generic engine-extension convention, zero core changes.
"""

from kindling_ext_databricks.auto_cdc import (
    SCD_SOURCE_SUFFIX,
    ScdSpec,
    scd_spec_from_tags,
    validate_scd_spec,
)
from kindling_ext_databricks.engine import EXPECTATION_DECORATORS, DatabricksSdpEngine
from kindling_ext_databricks.engine_extension import (
    DatabricksSdpEngineExtension,
    engine_extension,
)
from kindling_ext_databricks.lakeflow_app_selector import (
    ALLOWED_APPS_CONFIG_KEY,
    APP_ENTRY_POINT_GROUP,
    DATA_APP_CONFIG_KEY,
    LakeflowAppConflictError,
    LakeflowAppDeclarationError,
    LakeflowAppNotAuthorizedError,
    LakeflowAppNotFoundError,
    LakeflowAppSelectionError,
    declare_from_pipeline_config,
)

__version__ = "0.2.0"

__all__ = [
    "DatabricksSdpEngine",
    "DatabricksSdpEngineExtension",
    "ALLOWED_APPS_CONFIG_KEY",
    "APP_ENTRY_POINT_GROUP",
    "DATA_APP_CONFIG_KEY",
    "EXPECTATION_DECORATORS",
    "LakeflowAppConflictError",
    "LakeflowAppDeclarationError",
    "LakeflowAppNotAuthorizedError",
    "LakeflowAppNotFoundError",
    "LakeflowAppSelectionError",
    "SCD_SOURCE_SUFFIX",
    "ScdSpec",
    "declare_from_pipeline_config",
    "engine_extension",
    "scd_spec_from_tags",
    "validate_scd_spec",
]
