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

from kindling.entity_provider import StreamingSourceSpec


@dataclass(frozen=True)
class DatasetNameMapper:
    """Map logical entity IDs to pipeline-local, single-part dataset names.

    This is independent of EntityNameMapper, which resolves external tables.
    Both modes preserve the historical hyphen-to-underscore normalization.
    """

    mode: str = "normalized"

    def __post_init__(self) -> None:
        if self.mode not in ("normalized", "leaf"):
            raise ValueError(
                "Invalid kindling.sdp.dataset_naming value "
                f"{self.mode!r}; expected 'normalized' or 'leaf'."
            )

    def __call__(self, entity_id: str) -> str:
        """Return the emitted name within the pipeline's catalog/schema.

        Unity Catalog interprets dotted names as schema-qualified, bypassing
        the pipeline target schema. Pipeline-scoped views reject multipart
        names outright, so both naming modes emit single-part identifiers.
        """
        name = entity_id.rsplit(".", 1)[-1] if self.mode == "leaf" else entity_id
        return name.replace(".", "_").replace("-", "_")


def pipeline_dataset_name(entity_id: str) -> str:
    """Return the historical default name (compatibility helper).

    Engines use their configured DatasetNameMapper instead. Logical entity
    IDs in DatasetDeclaration.name remain unchanged.
    """
    return DatasetNameMapper()(entity_id)


class DatasetType(str, Enum):
    """The SDP dataset kind a pipe's output is declared as.

    Selection precedence (see ``DeclarationEngine._select_dataset_type``):
    entity tag ``sdp.dataset_type`` first, engine config
    (``datapipes.<pipeid>.engine.sdp.dataset_type``) second, default
    ``materialized_view``.

    NOTE — streaming-source lowering settles the Phase-4 placement question:
    the output entity tag still wins over pipe engine config, but a
    provider-owned streaming driving input infers ``streaming_table`` and
    explicit materialized-view requests fail with
    ``streaming_dataset_type_conflict`` instead of being silently overridden.
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

    EXTERNAL_STREAMING_SOURCE — nothing in the pipeline produces it, and
    the provider can declare an external streaming source that the target
    engine may lower to native streaming declarations.

    Derived purely from the registries: "does any registered pipe output
    this entity id?" (the engine's core Phase-1 logic per the proposal's
    Model Mapping section).
    """

    INTERNAL = "internal"
    EXTERNAL = "external"
    EXTERNAL_STREAMING_SOURCE = "external_streaming_source"


@dataclass(frozen=True)
class ClassifiedInput:
    """One pipe input with its internal/external classification."""

    entity_id: str
    classification: InputClassification
    #: For INTERNAL inputs: the pipe id that produces this entity.
    produced_by: Optional[str] = None
    #: Whether this entity id is selected by resolve_driving_entity_ids(pipe).
    driving: bool = False
    #: For EXTERNAL_STREAMING_SOURCE inputs: provider-owned source metadata.
    streaming_source: Optional[StreamingSourceSpec] = None


@dataclass(frozen=True)
class DatasetDeclaration:
    """One declared dataset (one pipe output) in the plan.

    ``name`` is the Kindling entity id (e.g. ``silver.orders``). Mapping to
    emitted pipeline-local names uses the engine's DatasetNameMapper;
    consumers must treat ``name`` as a logical identifier.
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

    #: Entity ids of EXTERNAL inputs an app explicitly opted into being read
    #: incrementally (``engine.<engine>.streaming_inputs``). Distinct from
    #: ``streaming_source_inputs``: those are provider-owned streams whose
    #: provider builds the source itself, while these are ordinary Delta
    #: tables read with ``spark.readStream.table``. Either one lowers the
    #: output to a streaming table plus an append flow.
    streamed_external_inputs: Tuple[str, ...] = ()

    @property
    def streaming_source_inputs(self) -> Tuple[ClassifiedInput, ...]:
        """Inputs declared as provider-owned streaming sources."""
        return tuple(
            pipe_input
            for pipe_input in self.inputs
            if pipe_input.classification is InputClassification.EXTERNAL_STREAMING_SOURCE
        )

    @property
    def has_streaming_driving_input(self) -> bool:
        """Whether this dataset lowers to a streaming table + append flow."""
        return bool(self.streaming_source_inputs or self.streamed_external_inputs)


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
