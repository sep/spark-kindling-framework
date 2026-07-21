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

from kindling_ext_sdp.capabilities import OSS_SDP, CapabilitySet
from kindling_ext_sdp.declaration_engine import DeclarationEngine
from kindling_ext_sdp.declaration_plan import (
    DatasetDeclaration,
    DatasetType,
    DeclarationPlan,
    InputClassification,
    pipeline_dataset_name,
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
    ):
        super().__init__(entity_registry, pipe_registry, capabilities, engine_config)
        self._dp_module = dp_module
        self._session_provider = session_provider or _default_session_provider
        self._external_read_resolver = external_read_resolver
        self._external_stream_read_resolver = external_stream_read_resolver or (
            lambda spark, entity_id: spark.readStream.table(entity_id)
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
        kwargs: dict = {"name": pipeline_dataset_name(dataset.name)}
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
        self, dataset: DatasetDeclaration, stream_first_input: bool = False
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

        ``stream_first_input`` reads the FIRST input — the driving source,
        per the runner engine's driving-source convention — with
        ``spark.readStream.table()`` so a consuming flow processes it
        incrementally (AUTO CDC change feeds); remaining inputs stay batch
        reads (stream-static joins).
        """
        session_provider = self._session_provider
        resolver = self._external_read_resolver
        stream_resolver = self._external_stream_read_resolver

        def dataset_function():
            spark = session_provider()
            input_dfs = {}
            for position, pipe_input in enumerate(dataset.inputs):
                if pipe_input.classification is InputClassification.INTERNAL:
                    # In-pipeline references use the emitted (single-part)
                    # dataset name so SDP infers the graph edge.
                    table_name = pipeline_dataset_name(pipe_input.entity_id)
                else:
                    table_name = pipe_input.entity_id
                if stream_first_input and position == 0:
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

        dataset_function.__name__ = dataset.name.replace(".", "_")
        dataset_function.__qualname__ = dataset_function.__name__
        return dataset_function
