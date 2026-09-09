"""Concrete OSS SDP engine: DeclarationPlan in, ``pyspark.pipelines`` out.

This is the Phase-2 emission engine from
``docs/proposals/declarative_pipelines_engine.md``: it walks a validated
:class:`~kindling_ext_sdp.declaration_plan.DeclarationPlan` and declares each
dataset through the OSS ``pyspark.pipelines`` (``dp``) decorator API —
Layer-1 common surface only, so the same output runs unmodified on vanilla
Spark 4.1+ and on Databricks.

The ``dp`` module is an injected dependency, not a top-level import:
Kindling core supports Spark runtimes older than 4.1, so ``pyspark.
pipelines`` is resolved lazily at declaration time (with an actionable
error when absent) and tests substitute a recording fake. Nothing in this
module imports ``pyspark.pipelines`` at import time — the Phase-1
invariant holds until the moment a pipeline is actually declared.

Deferred to Phase 3 (per the proposal's phase list): table properties,
partitioning/clustering, schema mapping, comments. Deferred to Phase 4:
streaming tables and append flows — a ``streaming_table`` dataset type
fails fast here rather than being silently declared as something else.
"""

from typing import Any, Callable, Dict, Optional

from kindling.entity_provider import DECLARATIVE_SOURCE_OPTION
from kindling_ext_sdp.capabilities import OSS_SDP, CapabilitySet
from kindling_ext_sdp.declaration_engine import DeclarationEngine
from kindling_ext_sdp.declaration_plan import (
    DatasetDeclaration,
    DatasetType,
    DeclarationPlan,
    InputClassification,
)


class SdpRuntimeUnavailableError(ImportError):
    """``pyspark.pipelines`` is not importable on this runtime."""


def _import_dp_module():
    try:
        from pyspark import pipelines
    except ImportError as exc:
        import pyspark

        raise SdpRuntimeUnavailableError(
            "pyspark.pipelines is not available on this runtime "
            f"(pyspark {pyspark.__version__} found; Spark 4.1+ is required). "
            "Install with: pip install 'pyspark[pipelines]>=4.1'"
        ) from exc
    return pipelines


def _default_session_provider():
    from pyspark.sql import SparkSession

    session = SparkSession.getActiveSession()
    if session is None:
        raise RuntimeError(
            "No active Spark session. SDP dataset functions run inside a "
            "spark-pipelines execution context, which provides the session."
        )
    return session


def _default_provider_stream_resolver(_spark, entity_id: str):
    """Resolve a provider-owned stream at dataset-function evaluation time."""
    from kindling.data_entities import DataEntityRegistry
    from kindling.entity_provider_registry import EntityProviderRegistry
    from kindling.injection import GlobalInjector

    entity = GlobalInjector.get(DataEntityRegistry).get_entity_definition(entity_id)
    if entity is None:
        raise RuntimeError(
            f"External streaming source entity '{entity_id}' is not registered "
            "when evaluating the SDP dataset function."
        )
    provider = GlobalInjector.get(EntityProviderRegistry).get_provider_for_entity(entity)
    return provider.read_entity_as_stream(entity, options={DECLARATIVE_SOURCE_OPTION: True})


class OssSdpEngine(DeclarationEngine):
    """Declares the plan through the OSS ``pyspark.pipelines`` API.

    Args:
        entity_registry / pipe_registry / engine_config: see
            :class:`~kindling_ext_sdp.declaration_engine.DeclarationEngine`.
        dp_module: The ``pyspark.pipelines`` module (or a test double).
            When ``None``, resolved lazily at :meth:`declare_pipeline` time.
        session_provider: Zero-arg callable returning the active Spark
            session, called inside each dataset function at evaluation
            time. Defaults to ``SparkSession.getActiveSession()``.
        external_read_resolver: ``(spark, entity_id) -> DataFrame`` for
            EXTERNAL inputs. Defaults to ``spark.table(entity_id)`` — the
            entity id as a catalog table name, pending the proposal's open
            "Catalog naming" question.
        external_stream_read_resolver: ``(spark, entity_id) -> DataFrame``
            for streamed EXTERNAL inputs. Defaults to
            ``spark.readStream.table(entity_id)``.
        provider_stream_resolver: ``(spark, entity_id) -> DataFrame`` for
            EXTERNAL_STREAMING_SOURCE inputs. The default resolves the entity
            provider at evaluation time and calls ``read_entity_as_stream``.
    """

    def __init__(
        self,
        entity_registry,
        pipe_registry,
        capabilities: CapabilitySet = OSS_SDP,
        engine_config: Optional[Dict[str, Dict[str, Any]]] = None,
        dp_module: Any = None,
        session_provider: Optional[Callable[[], Any]] = None,
        external_read_resolver: Optional[Callable[[Any, str], Any]] = None,
        external_stream_read_resolver: Optional[Callable[[Any, str], Any]] = None,
        provider_resolver: Optional[Callable[[Any], Any]] = None,
        provider_stream_resolver: Optional[Callable[[Any, str], Any]] = None,
        dataset_naming: str = "normalized",
    ):
        super().__init__(
            entity_registry,
            pipe_registry,
            capabilities,
            engine_config,
            dataset_naming,
            provider_resolver=provider_resolver,
        )
        self._dp_module = dp_module
        self._session_provider = session_provider or _default_session_provider
        self._external_read_resolver = external_read_resolver
        self._external_stream_read_resolver = external_stream_read_resolver or (
            lambda spark, entity_id: spark.readStream.table(entity_id)
        )
        self._provider_stream_resolver = (
            provider_stream_resolver or _default_provider_stream_resolver
        )

    def declare_pipeline(self, plan: DeclarationPlan) -> None:
        """Declare every dataset in the (already validated) plan."""
        dp = self._dp_module if self._dp_module is not None else _import_dp_module()
        for dataset in plan.datasets:
            self._declare_dataset(dp, dataset)

    # ------------------------------------------------------------------ #
    # Internals                                                           #
    # ------------------------------------------------------------------ #

    def _declare_dataset(self, dp, dataset: DatasetDeclaration) -> None:
        if dataset.dataset_type is not DatasetType.MATERIALIZED_VIEW:
            raise NotImplementedError(
                f"Dataset '{dataset.name}': dataset_type "
                f"'{dataset.dataset_type.value}' is not supported by the OSS "
                "engine yet — streaming tables and append flows are Phase 4."
            )
        decorator = dp.materialized_view(**self._declaration_kwargs(dataset))
        decorator(self._build_dataset_function(dataset))

    def _declaration_kwargs(self, dataset: DatasetDeclaration) -> dict:
        """Entity metadata → the OSS decorator surface (Spark 4.1 keywords:
        name, comment, table_properties, partition_cols, cluster_by,
        schema). Empty values are omitted rather than passed as empties."""
        kwargs: dict = {"name": self.dataset_name(dataset.name)}
        if dataset.comment:
            kwargs["comment"] = dataset.comment
        if dataset.table_properties:
            kwargs["table_properties"] = dict(dataset.table_properties)
        if dataset.partition_columns:
            kwargs["partition_cols"] = list(dataset.partition_columns)
        if dataset.cluster_columns:
            kwargs["cluster_by"] = list(dataset.cluster_columns)
        if dataset.schema is not None:
            kwargs["schema"] = dataset.schema
        return kwargs

    def _build_dataset_function(
        self, dataset: DatasetDeclaration, stream_driving_inputs: bool = False
    ) -> Callable[[], Any]:
        """The DataFrame-returning query body SDP evaluates.

        Reproduces the runner engine's input contract exactly
        (``generation_executor``): the pipe's execute callable receives one
        kwarg per input entity, keyed by the entity id with dots replaced
        by underscores, in ``input_entity_ids`` order (``DataPipes.view``
        bodies bind positionally over ``entity_dfs.values()``).

        INTERNAL inputs are read with ``spark.table(<dataset name>)`` so
        SDP infers the pipeline graph edge; EXTERNAL inputs go through the
        resolver (default: also a catalog-table read).

        ``stream_driving_inputs`` reads inputs selected by
        ``resolve_driving_entity_ids(pipe)`` incrementally so SDP lowering
        honors the same driving-source contract as the runner. Provider-owned
        streaming sources always use the provider stream resolver; remaining
        inputs stay batch reads (stream-static joins).
        """
        session_provider = self._session_provider
        resolver = self._external_read_resolver
        stream_resolver = self._external_stream_read_resolver
        provider_stream_resolver = self._provider_stream_resolver
        dataset_name = self.dataset_name

        def dataset_function():
            spark = session_provider()
            input_dfs = {}
            for pipe_input in dataset.inputs:
                if pipe_input.classification is InputClassification.INTERNAL:
                    # In-pipeline references use the emitted (single-part)
                    # dataset name so SDP infers the graph edge.
                    table_name = dataset_name(pipe_input.entity_id)
                else:
                    table_name = pipe_input.entity_id
                if pipe_input.classification is InputClassification.EXTERNAL_STREAMING_SOURCE:
                    df = provider_stream_resolver(spark, pipe_input.entity_id)
                elif stream_driving_inputs and pipe_input.driving:
                    if pipe_input.classification is InputClassification.INTERNAL:
                        df = spark.readStream.table(table_name)
                    else:
                        df = stream_resolver(spark, pipe_input.entity_id)
                elif pipe_input.classification is InputClassification.INTERNAL or resolver is None:
                    df = spark.table(table_name)
                else:
                    df = resolver(spark, pipe_input.entity_id)
                input_dfs[pipe_input.entity_id.replace(".", "_")] = df
            return dataset.execute(**input_dfs)

        dataset_function.__name__ = dataset_name(dataset.name)
        dataset_function.__qualname__ = dataset_function.__name__
        return dataset_function
