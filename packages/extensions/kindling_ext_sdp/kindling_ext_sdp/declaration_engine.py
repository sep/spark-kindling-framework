"""Abstract SDP declaration engine: registries in, validated plan out.

The engine consumes the populated Kindling registries
(``DataEntityRegistry`` / ``DataPipesRegistry``) and produces a validated
:class:`~kindling_ext_sdp.declaration_plan.DeclarationPlan`. Emitting actual
``pyspark.pipelines`` declarations from the plan is Phase 2 — this module
must never import ``pyspark.pipelines``.

Bootstrap ordering (Phase-1 decision, see
``docs/proposals/sdp_engine_phase1_notes.md``): ``build_plan()`` /
``declare_pipeline()`` must run **after** the post-registration config
overlay (``docs/proposals/package_config_architecture.md``), or
config-resolved engine keys like ``dataset_type`` won't be final when
datasets are declared. The engine reads entities through
``DataEntityRegistry.get_entity_definition()``, which applies config tag
overrides at retrieval time, so entity-tag inputs pick up the overlay
automatically — but the ``engine_config`` mapping handed to the constructor
must itself be resolved from the fully-overlaid config.

What is deliberately NOT validated here (runtime / Phase-2 concerns):

- Side-effecting pipe bodies (``collect``/``count``/``write``/
  ``saveAsTable``/``start`` inside dataset functions) and explicit
  watermark-API calls (``get_watermark``). Those require pipe-body
  introspection or runtime guards, not registry metadata; Phase 1 validates
  only what is statically checkable from ``PipeMetadata``/``EntityMetadata``.
- ``use_watermark=True`` pipes are declarable as-is: watermarking is a
  signal aspect (PR #158) that SDP mode simply never registers, so nothing
  in the pipe knows the difference.
"""

from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional, Tuple

from kindling.data_pipes import resolve_driving_entity_ids
from kindling.entity_provider import (
    DeclarableStreamingSource,
    StreamableEntityProvider,
    StreamingSourceSpec,
    is_declarable_streaming_source,
)
from kindling_ext_sdp.capabilities import (
    ADAPTER_TIER_CONFIG_KEYS,
    CapabilitySet,
    supports_auto_cdc,
    supports_streaming_source_lowering,
)
from kindling_ext_sdp.declaration_plan import (
    ClassifiedInput,
    DatasetDeclaration,
    DatasetNameMapper,
    DatasetType,
    DeclarationIssue,
    DeclarationPlan,
    DeclarationValidationError,
    InputClassification,
)

#: Entity tag that selects the SDP dataset type (highest precedence).
DATASET_TYPE_TAG = "sdp.dataset_type"

#: Engine-config key that selects the SDP dataset type (second precedence).
DATASET_TYPE_CONFIG_KEY = "dataset_type"

#: Entity-tag prefix mapping tags to declared table properties, e.g.
#: ``sdp.table_properties.delta.enableChangeDataFeed: "true"``.
TABLE_PROPERTIES_TAG_PREFIX = "sdp.table_properties."

#: Provider types the MVP can declare as a pipeline-managed output dataset.
#: An absent ``provider_type`` tag means delta (the framework default).
DECLARABLE_OUTPUT_PROVIDER_TYPES = frozenset({"delta"})

#: Provider types the MVP can represent as an external (storage) read.
#: Local-only providers (``memory``) and framework-internal view providers
#: cannot be represented as an SDP read — fail fast per the proposal's
#: Unsupported list.
DECLARABLE_EXTERNAL_READ_PROVIDER_TYPES = frozenset({"delta"})


class _DatasetTypeConflict(ValueError):
    """Raised when metadata explicitly conflicts with inferred streaming type."""


def _provider_type(entity) -> str:
    """Resolve an entity's provider type; absent tag means delta."""
    return str((entity.tags or {}).get("provider_type", "delta")).strip().lower()


def _is_scd_tagged(entity) -> bool:
    """True when the entity carries SCD intent tags (`scd.type`)."""
    return bool(str((entity.tags or {}).get("scd.type", "")).strip())


def _default_provider_resolver(entity_metadata):
    """Resolve an entity provider through Kindling DI when available."""
    try:
        import __main__

        if not hasattr(__main__, "_global_injector_instance"):
            return None
        from kindling.entity_provider_registry import EntityProviderRegistry
        from kindling.injection import GlobalInjector

        registry = GlobalInjector.get(EntityProviderRegistry)
        provider_type = _provider_type(entity_metadata)
        provider_class = registry.get_provider_class(provider_type)
        if provider_class is not None and not (
            issubclass(provider_class, DeclarableStreamingSource)
            and issubclass(provider_class, StreamableEntityProvider)
        ):
            return None
        return registry.get_provider_for_entity(entity_metadata)
    except Exception:  # noqa: BLE001 - bare declaration tests may not initialize DI
        return None


class DeclarationEngine(ABC):
    """Turns registered entities and pipes into a validated DeclarationPlan.

    Shared, registry-derived logic (classification, gating, dataset-type
    selection, plan assembly) is concrete on this base; only the actual
    emission of declarations — :meth:`declare_pipeline` — is abstract and
    lands in Phase 2 (`kindling_ext_sdp` emits the OSS ``pyspark.pipelines``
    surface; `kindling_ext_databricks` augments it).

    Args:
        entity_registry: A ``kindling.data_entities.DataEntityRegistry``
            (or anything exposing ``get_entity_ids()`` /
            ``get_entity_definition(entity_id)``).
        pipe_registry: A ``kindling.data_pipes.DataPipesRegistry`` (or
            anything exposing ``get_pipe_ids()`` /
            ``get_pipe_definition(pipe_id)``).
        capabilities: The target capability set (``OSS_SDP`` or
            ``DATABRICKS_SDP``).
        engine_config: Per-pipe engine config blocks keyed by pipe id — the
            resolved value of ``datapipes.<pipeid>.engine`` (e.g.
            ``{"silver.orders": {"sdp": {"dataset_type": ...},
            "databricks_sdp": {"expectations": ...}}}``). Must be resolved
            from config **after** the post-registration overlay.
        dataset_naming: Pipeline-local output naming mode: "normalized"
            (default) or "leaf". Independent of external EntityNameMapper.
    """

    def __init__(
        self,
        entity_registry,
        pipe_registry,
        capabilities: CapabilitySet,
        engine_config: Optional[Dict[str, Dict[str, Any]]] = None,
        dataset_naming: str = "normalized",
        provider_resolver: Optional[Callable[[Any], Any]] = None,
    ):
        self.entity_registry = entity_registry
        self.pipe_registry = pipe_registry
        self.capabilities = capabilities
        self.engine_config = dict(engine_config or {})
        self._provider_resolver = provider_resolver or _default_provider_resolver
        self._streaming_source_specs: Dict[str, Optional[StreamingSourceSpec]] = {}
        self._dataset_naming_issue: Optional[DeclarationIssue] = None
        try:
            self.dataset_name = DatasetNameMapper(dataset_naming)
        except ValueError as exc:
            self._dataset_naming_issue = DeclarationIssue(
                pipe_id="<pipeline>", code="invalid_dataset_naming", reason=str(exc)
            )
            # Continue collecting unrelated validation issues; an invalid
            # configuration can never reach emission through build_plan().
            self.dataset_name = DatasetNameMapper()

    # ------------------------------------------------------------------ #
    # Phase 2 surface                                                     #
    # ------------------------------------------------------------------ #

    @abstractmethod
    def declare_pipeline(self, plan: DeclarationPlan) -> None:
        """Emit the plan as engine-native declarations.

        Phase-2 implementations translate each ``DatasetDeclaration`` into
        ``pyspark.pipelines`` (OSS) or Lakeflow (Databricks adapter)
        declarations. Must not be given an unvalidated plan — always the
        output of :meth:`build_plan`.
        """

    # ------------------------------------------------------------------ #
    # Plan building                                                       #
    # ------------------------------------------------------------------ #

    def build_plan(self, pipe_ids: Optional[List[str]] = None) -> DeclarationPlan:
        """Validate the selected pipes and assemble the DeclarationPlan.

        Raises:
            DeclarationValidationError: carrying ALL accumulated issues when
                any selected pipe cannot be declared safely.
        """
        selected = self._select_pipe_ids(pipe_ids)
        issues = self.validate(selected)
        if issues:
            raise DeclarationValidationError(issues)

        producers = self._producers_by_entity(selected)
        datasets = tuple(self._build_dataset(pipe_id, producers) for pipe_id in selected)
        return DeclarationPlan(engine_name=self.capabilities.engine_name, datasets=datasets)

    def validate(self, pipe_ids: Optional[List[str]] = None) -> List[DeclarationIssue]:
        """Validate the selected pipes, returning ALL issues at once.

        Never stops at the first error: one declaration attempt surfaces
        every unsupported pipe with an actionable pipe id + reason.
        """
        selected = self._select_pipe_ids(pipe_ids)
        producers = self._producers_by_entity(selected)

        issues: List[DeclarationIssue] = []
        if self._dataset_naming_issue is not None:
            issues.append(self._dataset_naming_issue)
        emitted_outputs: Dict[str, Tuple[str, str]] = {}
        for pipe_id in selected:
            pipe = self.pipe_registry.get_pipe_definition(pipe_id)
            if pipe is None:
                issues.append(
                    DeclarationIssue(
                        pipe_id=pipe_id,
                        code="unknown_pipe",
                        reason="pipe id is not registered in the DataPipes registry",
                    )
                )
                continue
            for owner, emitted in self._emitted_dataset_names(pipe):
                # Spark/Lakeflow dataset identifiers are case-insensitive.
                previous = emitted_outputs.setdefault(emitted.casefold(), (pipe_id, owner))
                if previous[1] != owner:
                    issues.append(
                        DeclarationIssue(
                            pipe_id=pipe_id,
                            code="duplicate_dataset_name",
                            reason=(
                                f"outputs '{previous[1]}' (pipe '{previous[0]}') and "
                                f"'{owner}' resolve to the same emitted "
                                f"dataset name '{emitted}' with kindling.sdp.dataset_naming="
                                f"'{self.dataset_name.mode}'. Select these outputs in separate "
                                "pipelines or choose distinct dataset names."
                            ),
                        )
                    )
            issues.extend(self._validate_output(pipe, producers))
            issues.extend(self._validate_inputs(pipe, producers))
            issues.extend(self._validate_capabilities(pipe))
            issues.extend(self._validate_streaming_inputs(pipe, producers))
            issues.extend(self._validate_dataset_type(pipe, producers))
        return issues

    def _emitted_dataset_names(self, pipe) -> List[Tuple[str, str]]:
        """Return (owner description, local name) reservations for validation.

        Adapters include generated datasets and implicitly emitted siblings.
        """
        if not pipe.output_entity_id:
            return []
        return [(pipe.output_entity_id, self.dataset_name(pipe.output_entity_id))]

    def classify_inputs(
        self, pipe_id: str, pipe_ids: Optional[List[str]] = None
    ) -> Tuple[ClassifiedInput, ...]:
        """Classify one pipe's inputs against the selected pipeline scope."""
        selected = self._select_pipe_ids(pipe_ids)
        producers = self._producers_by_entity(selected)
        pipe = self.pipe_registry.get_pipe_definition(pipe_id)
        if pipe is None:
            raise KeyError(f"Pipe '{pipe_id}' is not registered in the DataPipes registry")
        return self._classify(pipe, producers)

    # ------------------------------------------------------------------ #
    # Internals                                                           #
    # ------------------------------------------------------------------ #

    def _select_pipe_ids(self, pipe_ids: Optional[List[str]]) -> List[str]:
        if pipe_ids is None:
            return list(self.pipe_registry.get_pipe_ids())
        return list(pipe_ids)

    def _producers_by_entity(self, pipe_ids: List[str]) -> Dict[str, str]:
        """Map output entity id -> producing pipe id, within the selection.

        This is the source of internal/external classification: an input is
        INTERNAL exactly when some registered pipe in the same pipeline
        produces it (an in-pipeline dependency SDP infers a graph edge
        from); anything else is EXTERNAL (a read from storage).
        """
        producers: Dict[str, str] = {}
        for pipe_id in pipe_ids:
            pipe = self.pipe_registry.get_pipe_definition(pipe_id)
            if pipe is not None and pipe.output_entity_id:
                # Duplicate producers are reported by _validate_output; the
                # first producer wins here only for classification purposes.
                producers.setdefault(pipe.output_entity_id, pipe_id)
        return producers

    def _classify(self, pipe, producers: Dict[str, str]) -> Tuple[ClassifiedInput, ...]:
        classified = []
        driving = set(resolve_driving_entity_ids(pipe))
        for entity_id in pipe.input_entity_ids:
            is_driving = entity_id in driving
            producer = producers.get(entity_id)
            if producer is not None:
                classified.append(
                    ClassifiedInput(
                        entity_id=entity_id,
                        classification=InputClassification.INTERNAL,
                        produced_by=producer,
                        driving=is_driving,
                    )
                )
            else:
                entity = self.entity_registry.get_entity_definition(entity_id)
                spec = self._streaming_source_spec(entity) if entity is not None else None
                if spec is not None:
                    classified.append(
                        ClassifiedInput(
                            entity_id=entity_id,
                            classification=InputClassification.EXTERNAL_STREAMING_SOURCE,
                            driving=is_driving,
                            streaming_source=spec,
                        )
                    )
                    continue
                classified.append(
                    ClassifiedInput(
                        entity_id=entity_id,
                        classification=InputClassification.EXTERNAL,
                        driving=is_driving,
                    )
                )
        return tuple(classified)

    def _streamed_external_input_ids(self, pipe) -> List[str]:
        """Entity ids this pipe opted into reading incrementally.

        Read from the pipe's engine block, active engine over the common
        ``sdp`` block (the same precedence as ``dataset_type`` and
        ``table_properties``)::

            datapipes:
              silver.device_telemetry:
                engine:
                  databricks_sdp:
                    streaming_inputs: ["bronze.device_telemetry"]

        Opting in is never inferred: an external Delta input is a batch read
        unless an app names it here, because incremental reads change a
        pipe's semantics (append-only, no reprocessing of revised rows) and
        that is the app's decision, not the adapter's.
        """
        for engine_name in self._engine_block_precedence():
            declared = self._pipe_engine_block(pipe.pipeid, engine_name).get("streaming_inputs")
            if declared is None:
                continue
            if isinstance(declared, str):
                declared = [declared]
            if not isinstance(declared, (list, tuple)):
                return []
            return [str(entity_id).strip() for entity_id in declared if str(entity_id).strip()]
        return []

    def _streaming_source_spec(self, entity) -> Optional[StreamingSourceSpec]:
        """Return a provider-owned streaming-source spec for an entity, if any."""
        entity_id = entity.entityid
        if entity_id not in self._streaming_source_specs:
            provider = self._provider_resolver(entity)
            if is_declarable_streaming_source(provider):
                self._streaming_source_specs[entity_id] = provider.streaming_source_spec(entity)
            else:
                self._streaming_source_specs[entity_id] = None
        return self._streaming_source_specs[entity_id]

    # --- validation pieces --------------------------------------------- #

    def _validate_output(self, pipe, producers: Dict[str, str]) -> List[DeclarationIssue]:
        issues: List[DeclarationIssue] = []

        if not (pipe.output_entity_id or "").strip():
            issues.append(
                DeclarationIssue(
                    pipe_id=pipe.pipeid,
                    code="missing_output_entity",
                    reason=(
                        "pipe has no output_entity_id; every declared dataset "
                        "needs a target entity"
                    ),
                )
            )
            return issues

        # One pipe per output entity: SDP's append_flow allows several flows
        # per target, but Kindling's model assumes one pipe per output (the
        # proposal's "Multiple flows into one target" open question). Fail
        # fast rather than silently declare a conflicting graph.
        producer = producers.get(pipe.output_entity_id)
        if producer is not None and producer != pipe.pipeid:
            issues.append(
                DeclarationIssue(
                    pipe_id=pipe.pipeid,
                    code="duplicate_output_entity",
                    reason=(
                        f"output entity '{pipe.output_entity_id}' is also produced by "
                        f"pipe '{producer}'; multiple flows into one target are not "
                        "supported (append_flow mapping is a deferred open question)"
                    ),
                )
            )

        if str((pipe.tags or {}).get("pipe_type", "")).strip().lower() == "view":
            issues.append(
                DeclarationIssue(
                    pipe_id=pipe.pipeid,
                    code="sql_view_pipe",
                    reason=(
                        "SQL-view pipes (DataPipes.view, memory-backed) cannot be "
                        "declared as SDP datasets in Phase 1; a temporary-view "
                        "mapping is deferred"
                    ),
                )
            )
            return issues

        entity = self.entity_registry.get_entity_definition(pipe.output_entity_id)
        if entity is None:
            issues.append(
                DeclarationIssue(
                    pipe_id=pipe.pipeid,
                    code="output_entity_not_registered",
                    reason=(
                        f"output entity '{pipe.output_entity_id}' is not registered "
                        "in the DataEntities registry"
                    ),
                )
            )
            return issues

        if entity.is_sql_entity:
            issues.append(
                DeclarationIssue(
                    pipe_id=pipe.pipeid,
                    code="output_entity_not_table_backed",
                    reason=(
                        f"output entity '{pipe.output_entity_id}' is a SQL-defined "
                        "(view) entity; only Delta/table-backed outputs are "
                        "declarable in the MVP"
                    ),
                )
            )
        elif _provider_type(entity) not in DECLARABLE_OUTPUT_PROVIDER_TYPES:
            issues.append(
                DeclarationIssue(
                    pipe_id=pipe.pipeid,
                    code="output_entity_not_table_backed",
                    reason=(
                        f"output entity '{pipe.output_entity_id}' has provider type "
                        f"'{_provider_type(entity)}'; only Delta/table-backed "
                        "outputs are declarable in the MVP"
                    ),
                )
            )
        return issues

    def _validate_inputs(self, pipe, producers: Dict[str, str]) -> List[DeclarationIssue]:
        issues: List[DeclarationIssue] = []
        classified_inputs = self._classify(pipe, producers)
        streaming_inputs = [
            classified
            for classified in classified_inputs
            if classified.classification is InputClassification.EXTERNAL_STREAMING_SOURCE
        ]
        driving_inputs = [classified for classified in classified_inputs if classified.driving]

        for classified in classified_inputs:
            entity_id = classified.entity_id

            if entity_id == pipe.output_entity_id:
                issues.append(
                    DeclarationIssue(
                        pipe_id=pipe.pipeid,
                        code="self_referencing_pipe",
                        reason=(
                            f"pipe reads its own output entity '{entity_id}'; "
                            "self-referencing (merge-into-self) pipes cannot be "
                            "declared — SDP owns persistence"
                        ),
                    )
                )
                continue

            if classified.classification is InputClassification.INTERNAL:
                # The producing pipe's own output validation covers it.
                continue

            if classified.classification is InputClassification.EXTERNAL_STREAMING_SOURCE:
                spec = classified.streaming_source
                provider = spec.provider_type if spec is not None else "<unknown>"
                if spec is not None:
                    for source_issue in spec.validation_issues:
                        issues.append(
                            DeclarationIssue(
                                pipe_id=pipe.pipeid,
                                code="streaming_source_invalid",
                                reason=(
                                    f"external streaming input '{entity_id}' "
                                    f"(provider '{provider}') violates {source_issue}"
                                ),
                            )
                        )
                if not classified.driving:
                    issues.append(
                        DeclarationIssue(
                            pipe_id=pipe.pipeid,
                            code="streaming_source_not_driving_input",
                            reason=(
                                f"external streaming input '{entity_id}' must be selected by "
                                "driving_entity_ids for declarative streaming-source lowering"
                            ),
                        )
                    )
                continue

            entity = self.entity_registry.get_entity_definition(entity_id)
            if entity is None:
                issues.append(
                    DeclarationIssue(
                        pipe_id=pipe.pipeid,
                        code="input_entity_not_registered",
                        reason=(
                            f"input entity '{entity_id}' is not registered in the "
                            "DataEntities registry and no pipe produces it"
                        ),
                    )
                )
                continue

            provider = _provider_type(entity)
            if entity.is_sql_entity or provider not in DECLARABLE_EXTERNAL_READ_PROVIDER_TYPES:
                issues.append(
                    DeclarationIssue(
                        pipe_id=pipe.pipeid,
                        code="external_input_not_declarable",
                        reason=(
                            f"external input '{entity_id}' has provider type "
                            f"'{'view' if entity.is_sql_entity else provider}', which "
                            "cannot be represented as an SDP storage read "
                            "(local-only/non-table providers are unsupported)"
                        ),
                    )
                )
        if streaming_inputs:
            if len(streaming_inputs) > 1 or len(driving_inputs) > 1:
                issues.append(
                    DeclarationIssue(
                        pipe_id=pipe.pipeid,
                        code="multiple_streaming_sources_not_supported",
                        reason=(
                            "exactly one declarable streaming source may drive a pipe; "
                            "additional driving inputs would require another streaming read"
                        ),
                    )
                )

            pipe_temporal_kind = str((pipe.tags or {}).get("temporal.kind", ""))
            output_entity = (
                self.entity_registry.get_entity_definition(pipe.output_entity_id)
                if pipe.output_entity_id
                else None
            )
            if pipe_temporal_kind.startswith("chain_") or (
                output_entity is not None and _is_scd_tagged(output_entity)
            ):
                issues.append(
                    DeclarationIssue(
                        pipe_id=pipe.pipeid,
                        code="streaming_source_lowering_not_supported",
                        reason=(
                            "declarable provider streams cannot yet compose with temporal "
                            "chain or AUTO CDC lowering; split the pipeline or remove the "
                            "specialized target tags"
                        ),
                    )
                )
            if not supports_streaming_source_lowering(self.capabilities):
                issues.append(
                    DeclarationIssue(
                        pipe_id=pipe.pipeid,
                        code="streaming_source_lowering_not_supported",
                        reason=(
                            f"target '{self.capabilities.engine_name}' does not support "
                            "provider-owned streaming-source lowering"
                        ),
                    )
                )
        return issues

    def _validate_capabilities(self, pipe) -> List[DeclarationIssue]:
        """Gate adapter-tier features against the target capability set.

        Only the active engine's config block (``engine.<engine_name>``) is
        gated; blocks addressed to other engines are declared intent for a
        different target and are ignored here.
        """
        issues: List[DeclarationIssue] = []

        engine_block = self._pipe_engine_block(pipe.pipeid, self.capabilities.engine_name)
        for key, feature in ADAPTER_TIER_CONFIG_KEYS.items():
            if key in engine_block and not self.capabilities.supports(feature):
                issues.append(
                    DeclarationIssue(
                        pipe_id=pipe.pipeid,
                        code="capability_not_supported",
                        reason=(
                            f"engine config requests adapter-tier feature '{key}' but "
                            f"target '{self.capabilities.engine_name}' does not "
                            f"support it (requires {feature.value}); move it under "
                            "the databricks_sdp engine block or drop it"
                        ),
                    )
                )

        # SCD tags route to AUTO CDC (proposal Phase 5). On a target without
        # AUTO CDC the intent is undeclarable — fail rather than silently
        # dropping SCD semantics.
        entity = (
            self.entity_registry.get_entity_definition(pipe.output_entity_id)
            if pipe.output_entity_id
            else None
        )
        if (
            entity is not None
            and _is_scd_tagged(entity)
            and not supports_auto_cdc(self.capabilities)
        ):
            issues.append(
                DeclarationIssue(
                    pipe_id=pipe.pipeid,
                    code="capability_not_supported",
                    reason=(
                        f"output entity '{pipe.output_entity_id}' carries scd.* tags, "
                        "which route to AUTO CDC — not available on target "
                        f"'{self.capabilities.engine_name}' (OSS SDP has no AUTO CDC)"
                    ),
                )
            )
        return issues

    def _validate_streaming_inputs(self, pipe, producers: Dict[str, str]) -> List[DeclarationIssue]:
        """Validate an explicit ``streaming_inputs`` opt-in.

        The opt-in must name the pipe's driving inputs exactly. The append
        flow streams whatever ``resolve_driving_entity_ids`` selects, so a
        partial opt-in would silently stream an input the app did not ask to
        stream; requiring the sets to match keeps the declaration honest
        instead of quietly widening it.
        """
        declared = self._streamed_external_input_ids(pipe)
        if not declared:
            return []

        issues: List[DeclarationIssue] = []

        def issue(code: str, reason: str) -> None:
            issues.append(DeclarationIssue(pipe_id=pipe.pipeid, code=code, reason=reason))

        classified_inputs = self._classify(pipe, producers)
        by_id = {classified.entity_id: classified for classified in classified_inputs}
        driving_ids = {
            classified.entity_id for classified in classified_inputs if classified.driving
        }

        if len(declared) > 1:
            issue(
                "multiple_streaming_inputs",
                f"streaming_inputs names {len(declared)} inputs "
                f"({', '.join(sorted(declared))}); a flow may stream only one "
                "input — the remaining inputs stay batch reads (stream-static joins)",
            )

        for entity_id in declared:
            classified = by_id.get(entity_id)
            if classified is None:
                issue(
                    "streaming_input_not_an_input",
                    f"streaming_inputs names '{entity_id}', which is not one of this "
                    f"pipe's input_entity_ids ({', '.join(pipe.input_entity_ids or []) or 'none'})",
                )
                continue
            if classified.classification is InputClassification.INTERNAL:
                issue(
                    "streaming_input_internal",
                    f"streaming_inputs names '{entity_id}', which is produced inside this "
                    "pipeline; in-pipeline inputs are already streamed when driving — "
                    "the opt-in is for external tables only",
                )
                continue
            if classified.classification is InputClassification.EXTERNAL_STREAMING_SOURCE:
                issue(
                    "streaming_input_provider_owned",
                    f"streaming_inputs names '{entity_id}', whose provider already declares "
                    "a streaming source; it is streamed without the opt-in",
                )
                continue
            entity = self.entity_registry.get_entity_definition(entity_id)
            provider_type = str(((entity.tags if entity else None) or {}).get("provider_type", ""))
            if provider_type.strip().lower() != "delta":
                issue(
                    "streaming_input_not_delta",
                    f"streaming_inputs names '{entity_id}' with provider_type "
                    f"'{provider_type or 'unset'}'; only Delta entities can be read with "
                    "spark.readStream.table — a non-Delta source needs a provider that "
                    "declares a streaming source",
                )
                continue
            if entity_id not in driving_ids:
                issue(
                    "streaming_input_not_driving",
                    f"streaming_inputs names '{entity_id}', which is not a driving input of "
                    "this pipe; only driving inputs are read incrementally (declare it in "
                    "driving_entity_ids, or drop it from streaming_inputs)",
                )

        unnamed_driving = sorted(driving_ids - set(declared))
        if not issues and unnamed_driving:
            issue(
                "streaming_inputs_partial",
                f"streaming_inputs must name every driving input; "
                f"{', '.join(unnamed_driving)} would be streamed by the append flow "
                "without being opted in",
            )
        return issues

    def _validate_dataset_type(self, pipe, producers: Dict[str, str]) -> List[DeclarationIssue]:
        streaming_driving = self._streams_a_driving_input(pipe, producers)
        try:
            self._select_dataset_type(pipe, streaming_driving=streaming_driving)
        except _DatasetTypeConflict as error:
            return [
                DeclarationIssue(
                    pipe_id=pipe.pipeid,
                    code="streaming_dataset_type_conflict",
                    reason=str(error),
                )
            ]
        except ValueError as error:
            return [
                DeclarationIssue(
                    pipe_id=pipe.pipeid,
                    code="invalid_dataset_type",
                    reason=str(error),
                )
            ]
        return []

    def _streams_a_driving_input(self, pipe, producers: Dict[str, str]) -> bool:
        """Whether this pipe's output lowers to a streaming table + flow.

        True for a provider-owned streaming source on a driving input, and
        for an explicit ``streaming_inputs`` opt-in. Both reach the same
        emission path, so both must reach the same dataset-type decision.
        """
        if self._streamed_external_input_ids(pipe):
            return True
        return any(
            classified.classification is InputClassification.EXTERNAL_STREAMING_SOURCE
            and classified.driving
            for classified in self._classify(pipe, producers)
        )

    # --- dataset assembly ----------------------------------------------- #

    def _build_dataset(self, pipe_id: str, producers: Dict[str, str]) -> DatasetDeclaration:
        pipe = self.pipe_registry.get_pipe_definition(pipe_id)
        entity = self.entity_registry.get_entity_definition(pipe.output_entity_id)
        tags = dict(entity.tags or {})
        inputs = self._classify(pipe, producers)
        streamed_external = tuple(self._streamed_external_input_ids(pipe))
        streaming_driving = self._streams_a_driving_input(pipe, producers)
        return DatasetDeclaration(
            name=pipe.output_entity_id,
            dataset_type=self._select_dataset_type(pipe, streaming_driving=streaming_driving),
            execute=pipe.execute,
            pipe_id=pipe.pipeid,
            inputs=inputs,
            partition_columns=tuple(entity.partition_columns or ()),
            cluster_columns=tuple(entity.cluster_columns or ()),
            tags=tags,
            comment=tags.get("comment"),
            schema=getattr(entity, "schema", None),
            table_properties=self._resolve_table_properties(pipe, tags),
            streamed_external_inputs=streamed_external,
        )

    def _resolve_table_properties(self, pipe, tags: Dict[str, str]) -> Dict[str, str]:
        """Merge table properties: engine config first, entity tags on top.

        Sources, lowest to highest precedence (per key — the same rule as
        dataset_type: the dataset's own metadata wins over pipe config):
        1. ``datapipes.<pipeid>.engine.<engine>.table_properties`` blocks,
           walked in the engine's fallback order (common ``sdp`` block
           first, active engine's block over it).
        2. Entity tags prefixed ``sdp.table_properties.`` — e.g.
           ``sdp.table_properties.delta.enableChangeDataFeed: "true"``.
        """
        properties: Dict[str, str] = {}
        for engine_name in reversed(self._engine_block_precedence()):
            block = self._pipe_engine_block(pipe.pipeid, engine_name).get("table_properties")
            if isinstance(block, dict):
                properties.update({str(k).strip(): str(v).strip() for k, v in block.items()})
        for tag_key, value in tags.items():
            if tag_key.startswith(TABLE_PROPERTIES_TAG_PREFIX):
                properties[tag_key[len(TABLE_PROPERTIES_TAG_PREFIX) :].strip()] = str(value).strip()
        return properties

    def _select_dataset_type(self, pipe, streaming_driving: bool = False) -> DatasetType:
        """Select the dataset type: entity tag, then engine config, default MV.

        Precedence (see also the placement note on ``DatasetType``):
        1. Output entity tag ``sdp.dataset_type`` — the dataset kind is a
           property of the output dataset, so entity metadata wins.
        2. Engine config ``datapipes.<pipeid>.engine.<engine_name>.
           dataset_type`` (the active engine's block, falling back to the
           common ``sdp`` block for the Databricks adapter).
        3. Default: ``materialized_view``.
        """
        raw: Optional[str] = None
        source = "default"

        entity = (
            self.entity_registry.get_entity_definition(pipe.output_entity_id)
            if pipe.output_entity_id
            else None
        )
        if entity is not None:
            tag_value = str((entity.tags or {}).get(DATASET_TYPE_TAG, "")).strip()
            # A derived dataset (core tag dataset.kind='derived') IS a
            # materialized view — that is precisely what the declaration
            # means on a declarative engine. An explicit conflicting
            # sdp.dataset_type is a contradiction, not an override.
            if str((entity.tags or {}).get("dataset.kind", "")).strip().lower() == "derived":
                if streaming_driving:
                    raise _DatasetTypeConflict(
                        f"Entity '{entity.entityid}': dataset.kind='derived' conflicts "
                        "with a provider-owned streaming driving input, which lowers "
                        "to a streaming table"
                    )
                if tag_value and tag_value.lower() != DatasetType.MATERIALIZED_VIEW.value:
                    raise ValueError(
                        f"Entity '{entity.entityid}': dataset.kind='derived' "
                        f"lowers to a materialized view; conflicting "
                        f"{DATASET_TYPE_TAG}='{tag_value}'"
                    )
                return DatasetType.MATERIALIZED_VIEW
            if tag_value:
                raw, source = tag_value, f"entity tag '{DATASET_TYPE_TAG}'"

        if raw is None:
            for engine_name in self._engine_block_precedence():
                block = self._pipe_engine_block(pipe.pipeid, engine_name)
                config_value = str(block.get(DATASET_TYPE_CONFIG_KEY, "")).strip()
                if config_value:
                    raw, source = config_value, f"engine config '{engine_name}'"
                    break

        if streaming_driving:
            if raw is None:
                return DatasetType.STREAMING_TABLE
            selected = self._parse_dataset_type(raw, source)
            if selected is not DatasetType.STREAMING_TABLE:
                raise _DatasetTypeConflict(
                    f"provider-owned streaming driving input lowers to "
                    f"'{DatasetType.STREAMING_TABLE.value}', but {source} requests "
                    f"'{selected.value}'"
                )
            return selected

        if raw is None:
            return DatasetType.MATERIALIZED_VIEW

        return self._parse_dataset_type(raw, source)

    def _parse_dataset_type(self, raw: str, source: str) -> DatasetType:
        try:
            return DatasetType(raw.lower())
        except ValueError as exc:
            valid = ", ".join(dt.value for dt in DatasetType)
            raise ValueError(
                f"invalid dataset_type '{raw}' from {source}; expected one of: {valid}"
            ) from exc

    def _engine_block_precedence(self) -> List[str]:
        engine_name = self.capabilities.engine_name
        return [engine_name] if engine_name == "sdp" else [engine_name, "sdp"]

    def _pipe_engine_block(self, pipe_id: str, engine_name: str) -> Dict[str, Any]:
        pipe_config = self.engine_config.get(pipe_id) or {}
        block = pipe_config.get(engine_name) or {}
        return block if isinstance(block, dict) else {}
