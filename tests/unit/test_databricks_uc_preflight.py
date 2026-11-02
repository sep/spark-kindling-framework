"""Tests for scripts/databricks_uc_preflight.py (loaded by path)."""

import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

_SCRIPT_PATH = Path(__file__).parent.parent.parent / "scripts" / "databricks_uc_preflight.py"
_spec = importlib.util.spec_from_file_location("databricks_uc_preflight", _SCRIPT_PATH)
preflight = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = preflight
_spec.loader.exec_module(preflight)


def _client(state):
    client = MagicMock()
    client.clusters.get.return_value = SimpleNamespace(state=SimpleNamespace(value=state))
    return client


def test_start_cluster_starts_a_terminated_cluster_without_waiting():
    client = _client("TERMINATED")

    preflight.start_cluster(client, "c1")

    client.clusters.start.assert_called_once_with("c1")
    # start() returns a waiter; calling result() would block on the cold start.
    client.clusters.start.return_value.result.assert_not_called()


def test_start_cluster_leaves_a_running_or_starting_cluster_alone():
    for state in ("RUNNING", "PENDING", "RESTARTING"):
        client = _client(state)

        preflight.start_cluster(client, "c1")

        client.clusters.start.assert_not_called()


def test_start_cluster_never_raises(capsys):
    client = MagicMock()
    client.clusters.get.side_effect = RuntimeError("403 Forbidden")

    preflight.start_cluster(client, "c1")

    assert "Could not start Databricks cluster c1: 403 Forbidden" in capsys.readouterr().out


def test_cluster_warm_up_job_cannot_fail_ci():
    import yaml

    workflow = yaml.safe_load(
        (_SCRIPT_PATH.parents[1] / ".github" / "workflows" / "ci.yml").read_text()
    )

    assert workflow["jobs"]["start-databricks-cluster"]["continue-on-error"] is True
