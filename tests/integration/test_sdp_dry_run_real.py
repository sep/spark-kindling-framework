"""Real ``spark-pipelines dry-run`` against an OssSdpEngine-declared graph.

This is the proposal's Phase-2 acceptance criterion executed for real: the
prototype graph (one bronze source, one silver materialized view) is
declared by ``OssSdpEngine`` inside a definitions file, and the actual
Spark 4.1 CLI validates the resulting dataflow graph — no Databricks, no
cluster.

The CLI lives in an isolated venv (``poe sdp-runtime`` provisions
``.venv-sdp41/``) because the repo's main environment pins pyspark <4.0;
the test is skipped when no CLI is available (as in CI today). The
definitions file runs in the CLI's own Python, where only ``kindling_ext_sdp``
(duck-typed registries, no kindling-core import) is on PYTHONPATH — which
is itself a regression test for the package's standalone-import invariant.

Budget note: a dry-run boots a local Spark JVM (~30-60s).
"""

import os
import shutil
from pathlib import Path

import pytest
from kindling_ext_sdp import dry_run, write_pipeline_spec

REPO_ROOT = Path(__file__).resolve().parents[2]
KINDLING_SDP_PACKAGE = REPO_ROOT / "packages" / "extensions" / "kindling_ext_sdp"


def _find_cli():
    explicit = os.environ.get("SPARK_PIPELINES_BIN")
    if explicit and Path(explicit).exists():
        return explicit
    default = REPO_ROOT / ".venv-sdp41" / "bin" / "spark-pipelines"
    if default.exists():
        return str(default)
    return shutil.which("spark-pipelines")


CLI = _find_cli()

pytestmark = pytest.mark.skipif(
    CLI is None,
    reason="spark-pipelines CLI not available — provision with: poe sdp-runtime",
)

# Self-contained on purpose: this file is executed by the CLI's Python,
# which has kindling_ext_sdp but not kindling core (nor delta-spark) installed.
DEFINITIONS = '''
"""Prototype graph: bronze source -> silver materialized view."""
from types import SimpleNamespace

from kindling_ext_sdp import OssSdpEngine


def make_entity(eid, tags=None, partition_columns=(), schema=None):
    return SimpleNamespace(
        entityid=eid, name=eid.split(".")[-1],
        partition_columns=list(partition_columns),
        cluster_columns=None, tags=tags or {}, schema=schema, merge_columns=[],
        is_sql_entity=False,
    )


def bronze_execute(**dfs):
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import lit
    spark = SparkSession.getActiveSession()
    return spark.range(3).withColumnRenamed("id", "order_id").withColumn("status", lit("open"))


def silver_execute(**dfs):
    return dfs["bronze_orders"].select("order_id", "status")


class Reg(dict):
    def get_entity_ids(self):
        return self.keys()
    get_entity_definition = dict.get
    def get_pipe_ids(self):
        return self.keys()
    get_pipe_definition = dict.get


# silver carries the full Phase-3 metadata surface so the REAL CLI
# validates the comment/table_properties/partition_cols/schema emission.
entities = Reg({e.entityid: e for e in [
    make_entity("bronze.orders"),
    make_entity(
        "silver.orders",
        tags={
            "comment": "Cleaned orders",
            "sdp.table_properties.kindling.quality": "silver",
        },
        partition_columns=["status"],
        schema="order_id LONG, status STRING",
    ),
]})
pipes = Reg({
    "ingest.orders": SimpleNamespace(
        pipeid="ingest.orders", input_entity_ids=[], output_entity_id="bronze.orders",
        execute=bronze_execute, tags={},
    ),
    "bronze_to_silver.orders": SimpleNamespace(
        pipeid="bronze_to_silver.orders", input_entity_ids=["bronze.orders"],
        output_entity_id="silver.orders", execute=silver_execute, tags={},
    ),
})

engine = OssSdpEngine(entities, pipes)
engine.declare_pipeline(engine.build_plan())
'''


def test_declared_prototype_graph_passes_real_dry_run(tmp_path):
    definitions_dir = tmp_path / "definitions"
    definitions_dir.mkdir()
    (definitions_dir / "pipeline_defs.py").write_text(DEFINITIONS)

    spec = write_pipeline_spec(tmp_path, "kindling_prototype", ["definitions/**"])

    result = dry_run(
        spec,
        executable=CLI,
        extra_env={"PYTHONPATH": str(KINDLING_SDP_PACKAGE)},
    )

    assert result.ok, (
        f"dry-run failed (rc={result.returncode})\n"
        f"--- stdout ---\n{result.stdout}\n--- stderr ---\n{result.stderr}"
    )
    assert "Run is COMPLETED" in result.stdout + result.stderr
