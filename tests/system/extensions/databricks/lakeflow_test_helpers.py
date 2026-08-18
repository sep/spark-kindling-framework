"""Shared helpers for Databricks Lakeflow platform system tests.

Extracted from ``test_lakeflow_scd_platform.py`` and
``test_temporal_lakeflow.py``, which had near-identical copies of these —
wheel-version resolution, pipeline notebook generation, update polling,
error-event reporting, SQL execution, and warehouse selection are the same
across every Lakeflow platform test regardless of which app it deploys.
"""

import sys
import time
from pathlib import Path
from typing import Dict, Iterable, Optional

WORKSPACE_ROOT = Path(__file__).parent.parent.parent.parent
PACKAGES_VOLUME = "/Volumes/{catalog}/{schema}/artifacts/packages"


def wheel_version(pyproject: Path) -> str:
    """Read the ``version = "..."`` line from a pyproject.toml."""
    import re

    match = re.search(r'^version = "([^"]+)"', pyproject.read_text(), re.MULTILINE)
    assert match, f"no version in {pyproject}"
    return match.group(1)


def pipeline_notebook(wheel_paths: Iterable[str]) -> str:
    """Databricks notebook source that installs the given wheels (UC
    volume paths) and declares the Lakeflow pipeline via the app selector."""
    wheels = " ".join(wheel_paths)
    return f"""# Databricks notebook source
# MAGIC %pip install {wheels}

# COMMAND ----------

from kindling_ext_databricks.lakeflow_app_selector import declare_from_pipeline_config

declare_from_pipeline_config()
"""


def wait_for_update(w, pipeline_id: str, update_id: str, max_wait: float = 1800.0) -> str:
    """Poll a Lakeflow pipeline update until it reaches a terminal state."""
    deadline = time.time() + max_wait
    state = None
    while time.time() < deadline:
        info = w.pipelines.get_update(pipeline_id, update_id)
        state = info.update.state.value if info.update and info.update.state else None
        if state in {"COMPLETED", "FAILED", "CANCELED"}:
            return state
        time.sleep(15)
    return state or "TIMEOUT"


def print_error_events(w, pipeline_id: str) -> None:
    """Print ERROR-level Lakeflow pipeline events (and their nested
    exceptions) so a failed update's cause shows up in the test log."""
    for event in list(w.pipelines.list_pipeline_events(pipeline_id, max_results=50)):
        if event.level and event.level.value == "ERROR":
            print(f"[ERROR] {event.event_type}: {(event.message or '').strip()[:2000]}")
            error = getattr(event, "error", None)
            if error and getattr(error, "exceptions", None):
                for exc in error.exceptions:
                    print("  EXC:", (exc.message or "").strip()[:2000])
    sys.stdout.flush()


def select_warehouse_id(w, env_override: Optional[str] = None) -> Optional[str]:
    """Pick a SQL warehouse to seed/verify pipeline data against.

    Prefers a RUNNING warehouse over a STOPPED one -- a stopped serverless
    warehouse auto-starts on statement execution but adds startup latency.
    Returns None (caller should skip) if no warehouse is available.
    """
    if env_override:
        return env_override
    warehouses = sorted(
        (wh for wh in w.warehouses.list() if wh.state and wh.state.value in ("RUNNING", "STOPPED")),
        key=lambda wh: wh.state.value != "RUNNING",
    )
    return warehouses[0].id if warehouses else None


def execute_statement(
    w, warehouse_id: str, statement: str, parameters: Optional[Dict[str, str]] = None
):
    """Run a SQL statement via the Statement Execution API with typed
    parameters (never inline rule/expression values into SQL literals --
    quote mangling). Returns the result rows; asserts on failure."""
    params = None
    if parameters:
        from databricks.sdk.service.sql import StatementParameterListItem

        params = [
            StatementParameterListItem(name=key, value=value, type="STRING")
            for key, value in parameters.items()
        ]
    result = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        wait_timeout="50s",
        parameters=params,
    )
    state = result.status.state.value if result.status else None
    assert state == "SUCCEEDED", (
        f"SQL failed: "
        f"{result.status.error.message if result.status and result.status.error else state}"
    )
    return result.result.data_array if result.result else []
