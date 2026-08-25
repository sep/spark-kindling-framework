"""Reusable entrypoint helpers for a thin app.py's ``if __name__ ==
'__main__':`` block.

The CLI/runner never special-case an app's execution pattern (batch,
streaming, file-ingestion, ...) -- that dispatch lives in an app's own
``__main__`` block, which calls one of these helpers explicitly. Framework
bootstrap and entity/pipe registration happen before app.py is ever loaded
(see ``kindling_cli.cli._bootstrap_app``), so these helpers assume the
framework is already initialized and only need to drive execution.
"""

import os
from typing import Any, Dict, List, Optional


def run_batch_app(pipe_ids: Optional[List[str]] = None, *, use_dag: bool = True) -> None:
    """Run registered pipes to completion. Mirrors what the scaffolded
    batch app template used to inline directly."""
    from kindling.data_pipes import DataPipesExecution, DataPipesRegistry
    from kindling.injection import get_kindling_service

    registry = get_kindling_service(DataPipesRegistry)
    execution = get_kindling_service(DataPipesExecution)
    execution.run_datapipes(pipe_ids or sorted(registry.get_pipe_ids()), use_dag=use_dag)


def run_streaming_app(
    pipe_ids: Optional[List[str]] = None,
    *,
    streaming_options: Optional[Dict[str, Any]] = None,
) -> None:
    """Start registered streaming pipes. Mirrors what the scaffolded
    streaming app template used to inline directly."""
    from kindling.data_pipes import DataPipesRegistry
    from kindling.execution_orchestrator import ExecutionOrchestrator
    from kindling.injection import get_kindling_service

    registry = get_kindling_service(DataPipesRegistry)
    orchestrator = get_kindling_service(ExecutionOrchestrator)
    if streaming_options is None:
        checkpoint_path = os.environ.get("KINDLING_CHECKPOINT_PATH")
        streaming_options = {"base_checkpoint_path": checkpoint_path} if checkpoint_path else None
    orchestrator.execute_streaming(
        pipe_ids or sorted(registry.get_pipe_ids()), streaming_options=streaming_options
    )


def run_file_ingestion_app(source_path: Optional[str] = None) -> None:
    """Ingest a file path (KINDLING_INGESTION_PATH by default). Mirrors
    what the scaffolded file-ingestion app template used to inline
    directly."""
    from kindling.file_ingestion import FileIngestionProcessor
    from kindling.injection import get_kindling_service

    source_path = source_path or os.environ.get("KINDLING_INGESTION_PATH")
    if not source_path:
        raise RuntimeError("Set KINDLING_INGESTION_PATH to the source path to ingest.")
    processor = get_kindling_service(FileIngestionProcessor)
    processor.process_path(source_path)
