"""Pure-metadata declaration plan produced by the SDP declaration engine.

Everything in this module is a plain dataclass/enum: no Spark session, no
``pyspark.pipelines`` import, no I/O. A concrete engine (Phase 2) walks a
``DeclarationPlan`` and emits the actual ``pyspark.pipelines`` (or
Databricks Lakeflow) declarations; the plan itself is inert and fully
inspectable, which is what makes declaration-time validation and unit
testing possible without a cluster.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple


def pipeline_dataset_name(entity_id: str) -> str:
    """The emitted (physical) dataset name for a Kindling entity id.

    Datasets are emitted with single-part names inside the pipeline's
    target catalog/schema, dots normalized to underscores — the same leaf
    normalization the runner engine's EntityNameMapper applies. Unity
    Catalog pipelines interpret a dotted dataset name as schema-qualified
    (``silver.customers`` becomes ``<catalog>.silver.customers``), which
    bypasses the pipeline target schema and requires CREATE SCHEMA on the
    catalog; pipeline-scoped views reject dotted names outright ("View
    with multipart name ... is not supported"). Plan-level
    ``DatasetDeclaration.name`` stays the logical entity id.
    """
    return entity_id.replace(".", "_").replace("-", "_")


class DatasetType(str, Enum):
    """The SDP dataset kind a pipe's output is declared as.

    Selection precedence (see ``DeclarationEngine._select_dataset_type``):
    entity tag ``sdp.dataset_type`` first, engine config
    (``datapipes.<pipeid>.engine.sdp.dataset_type``) second, default
    ``materialized_view``.

    NOTE — open question in the proposal ("`dataset_type` placement"):
    materialized-view-vs-streaming-table is a property of the output
    *dataset* (entity-level metadata), but engine config naturally hangs off
    the pipe. Phase 1 resolves the precedence (entity tag wins over pipe
    engine config) but the ownership question stays open until Phase 4
    (streaming tables) forces it.
    """

    MATERIALIZED_VIEW = "materialized_view"
    STREAMING_TABLE = "streaming_table"


class InputClassification(str, Enum):
    """How a pipe input is read in the declared pipeline.

    INTERNAL — some other registered pipe produces the entity, so it is an
    in-pipeline dependency: the dataset function reads it by dataset name
    and SDP infers the graph edge.

    EXTERNAL — nothing in the pipeline produces it: it is a read from
    storage (an external table read).

    Derived purely from the registries: "does any registered pipe output
    this entity id?" (the engine's core Phase-1 logic per the proposal's
    Model Mapping section).
    """

    INTERNAL = "internal"
    EXTERNAL = "external"


@dataclass(frozen=True)
class ClassifiedInput:
    """One pipe input with its internal/external classification."""

    entity_id: str
    classification: InputClassification
    #: For INTERNAL inputs: the pipe id that produces this entity.
    produced_by: Optional[str] = None


@dataclass(frozen=True)
class DatasetDeclaration:
    """One declared dataset (one pipe output) in the plan.

    ``name`` is the Kindling entity id (e.g. ``silver.orders``). Mapping to
    catalog/schema/table names under Unity Catalog and OSS catalogs is an
    explicit open question in the proposal ("Catalog naming") and is
    deferred; consumers must treat ``name`` as a logical identifier.
    """

    name: str
    dataset_type: DatasetType
    #: The registered pipe's execute callable — the DataFrame-returning
    #: query body, passed through unchanged. Never invoked at plan time.
    execute: Callable
    pipe_id: str
    inputs: Tuple[ClassifiedInput, ...]
    # --- entity metadata passthrough ---
    partition_columns: Tuple[str, ...] = ()
    cluster_columns: Tuple[str, ...] = ()
    tags: Dict[str, str] = field(default_factory=dict)
    comment: Optional[str] = None
    #: The entity's declared schema, passed through opaquely (a pyspark
    #: StructType or DDL string — never inspected at plan time).
    schema: Any = None
    #: Resolved table properties: entity tags ``sdp.table_properties.<key>``
    #: merged over the active engine-config ``table_properties`` block
    #: (entity tag wins per key — same precedence rule as dataset_type).
    #: NOTE (dual-engine divergence, documented per the parity criterion):
    #: unlike the runner engine's Delta provider, SDP does NOT force
    #: ``delta.enableChangeDataFeed=true`` — CDF feeds the runner's
    #: watermark machinery, which SDP mode never registers. Declare the
    #: tag explicitly if CDF is wanted for external consumers.
    table_properties: Dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class DeclarationPlan:
    """The validated, ordered set of dataset declarations for one pipeline."""

    engine_name: str
    datasets: Tuple[DatasetDeclaration, ...]

    def get_dataset(self, name: str) -> Optional[DatasetDeclaration]:
        for dataset in self.datasets:
            if dataset.name == name:
                return dataset
        return None

    @property
    def internal_entity_ids(self) -> "frozenset[str]":
        return frozenset(dataset.name for dataset in self.datasets)


@dataclass(frozen=True)
class DeclarationIssue:
    """One actionable validation failure, attributed to a pipe."""

    pipe_id: str
    code: str
    reason: str

    def __str__(self) -> str:
        return f"[{self.code}] pipe '{self.pipe_id}': {self.reason}"


class DeclarationValidationError(Exception):
    """Raised by ``build_plan()`` when validation fails.

    Carries ALL accumulated issues (never first-error-only) so one
    declaration attempt surfaces every unsupported pipe at once.
    """

    def __init__(self, issues: List[DeclarationIssue]):
        self.issues = list(issues)
        lines = "\n".join(f"  - {issue}" for issue in self.issues)
        super().__init__(f"Pipeline declaration failed with {len(self.issues)} error(s):\n{lines}")
