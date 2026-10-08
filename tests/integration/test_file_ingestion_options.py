"""End-to-end batch file ingestion through the real standalone DI wiring.

One run covers the three behaviors that used to disagree with the documented
``FileIngestionEntries.entry()`` API:

- every pattern in ``patterns`` is tried in order (the file matches only the
  second one);
- ``infer_schema=True`` reaches the Spark reader (the ``qty`` column lands as
  an integer, not a string);
- matched files are appended through the destination entity's own provider
  (``provider_type: parquet``), not the default Delta provider.

Runs in a fresh interpreter, like test_di_wiring_standalone, so the framework
bootstrap and its global registries never leak into other tests.
"""

import subprocess
import sys
import textwrap

import pytest

pytestmark = [pytest.mark.integration]


@pytest.mark.requires_spark
def test_batch_ingestion_honors_patterns_infer_schema_and_entity_provider(tmp_path) -> None:
    landing = tmp_path / "landing"
    landing.mkdir()
    (landing / "orders_20260101.csv").write_text("order_id,qty\nA,3\nB,5\n")
    (landing / "ignored.txt").write_text("not a match\n")
    out = tmp_path / "out" / "orders"

    code = textwrap.dedent(f"""
        from kindling.bootstrap import initialize_framework

        initialize_framework({{"platform": "standalone", "environment": "local"}})

        from pyspark.sql.types import IntegerType
        from kindling.data_entities import DataEntities
        from kindling.file_ingestion import FileIngestionEntries, FileIngestionProcessor
        from kindling.injection import get_kindling_service
        from kindling.spark_session import get_or_create_spark_session

        DataEntities.entity(
            entityid="bronze.orders",
            name="bronze_orders",
            partition_columns=[],
            merge_columns=[],
            tags={{"provider_type": "parquet", "provider.path": {str(out)!r}}},
            schema=None,
        )
        FileIngestionEntries.entry(
            entry_id="orders",
            name="orders",
            patterns=[r"sales_(?P<region>\\w+)\\.csv", r"orders_(?P<day>\\d{{8}})\\.csv"],
            dest_entity_id="bronze.orders",
            tags={{}},
            filetype="csv",
            infer_schema=True,
        )

        get_kindling_service(FileIngestionProcessor).process_path({str(landing)!r})

        df = get_or_create_spark_session().read.parquet({str(out)!r})
        rows = sorted(df.collect(), key=lambda r: r["order_id"])
        assert [(r["order_id"], r["qty"], r["day"]) for r in rows] == [
            ("A", 3, "20260101"),
            ("B", 5, "20260101"),
        ], rows
        assert isinstance(df.schema["qty"].dataType, IntegerType), df.schema
        print("FILE_INGESTION_OPTIONS_OK")
        """)
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=180,
        cwd=tmp_path,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert "FILE_INGESTION_OPTIONS_OK" in result.stdout
    # Written by the parquet provider, not as a Delta table.
    assert not (out / "_delta_log").exists()
