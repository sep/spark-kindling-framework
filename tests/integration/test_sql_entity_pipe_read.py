"""Integration tests: SQL entities (``@DataEntities.sql_entity``) in the core runner.

A SQL entity is tagged ``provider_type: "view"``. The core runner resolves
every pipe input through the ``EntityProviderRegistry``, so the ``view``
provider must be registered there or a SQL-entity input fails with
``Unknown provider type: 'view'``.

The read evaluates the entity's declared SQL. It does not need
``kindling migrate apply`` to have created the catalog view first. That
matters standalone, where the catalog is per-session and a separately run
``migrate apply`` leaves nothing behind for the next process.

Boots the full framework standalone in a fresh subprocess, the same way
tests/integration/test_config_override_overlay_integration.py does.
"""

import subprocess
import sys
import textwrap

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.requires_spark]

_PRELUDE = """
    from kindling.bootstrap import initialize_framework

    initialize_framework({"platform": "standalone", "environment": "local"})

    from kindling.data_entities import DataEntities
    from kindling.data_pipes import DataPipes, DataPipesExecution
    from kindling.injection import GlobalInjector
    from kindling.spark_session import get_or_create_spark_session

    spark = get_or_create_spark_session()

    DataEntities.entity(
        entityid="it.orders",
        name="orders",
        merge_columns=[],
        tags={"provider_type": "memory"},
        schema=None,
    )
    # Memory entities register a session temp view named after the entity
    # id with dots changed to underscores, so the SQL can query it.
    spark.createDataFrame(
        [(1, 5), (2, 50), (3, 500)], ["id", "amount"]
    ).createOrReplaceTempView("it_orders")

    DataEntities.sql_entity(
        entityid="it.big_orders",
        name="it_big_orders_view",
        sql="SELECT id, amount FROM it_orders WHERE amount > 10",
    )
"""


def _run_fresh_python(code: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(_PRELUDE) + textwrap.dedent(code)],
        capture_output=True,
        text=True,
        timeout=300,
    )


def test_sql_entity_is_readable_as_pipe_input_in_core_runner():
    result = _run_fresh_python("""
        DataEntities.entity(
            entityid="it.big_orders_copy",
            name="big_orders_copy",
            merge_columns=[],
            tags={"provider_type": "memory"},
            schema=None,
        )

        @DataPipes.pipe(
            pipeid="it.copy_big_orders",
            name="Copy big orders",
            tags={},
            input_entity_ids=["it.big_orders"],
            output_entity_id="it.big_orders_copy",
            output_type="memory",
        )
        def copy_big_orders(it_big_orders):
            return it_big_orders

        GlobalInjector.get(DataPipesExecution).run_datapipes(["it.copy_big_orders"])

        rows = sorted(r.id for r in spark.table("it_big_orders_copy").collect())
        assert rows == [2, 3], rows

        # Reading evaluates the SQL; it never creates the catalog view
        # (that stays the job of `kindling migrate apply`).
        assert not spark.catalog.tableExists("it_big_orders_view")
        print("SQL_ENTITY_READ_OK")
        """)

    assert result.returncode == 0, result.stdout + result.stderr
    assert "SQL_ENTITY_READ_OK" in result.stdout


def test_writing_to_sql_entity_fails_clearly():
    result = _run_fresh_python("""
        @DataPipes.pipe(
            pipeid="it.write_into_view",
            name="Write into a SQL entity",
            tags={},
            input_entity_ids=["it.orders"],
            output_entity_id="it.big_orders",
            output_type="view",
        )
        def write_into_view(it_orders):
            return it_orders

        try:
            GlobalInjector.get(DataPipesExecution).run_datapipes(["it.write_into_view"])
        except Exception as exc:
            message = str(exc)
            assert "it.big_orders" in message, message
            assert "read-only" in message, message
            assert "SQL entity" in message, message
        else:
            raise AssertionError("writing to a SQL entity should fail")

        # No DDL side effect: the failed run must not have created the view.
        assert not spark.catalog.tableExists("it_big_orders_view")
        print("SQL_ENTITY_WRITE_REJECTED")
        """)

    assert result.returncode == 0, result.stdout + result.stderr
    assert "SQL_ENTITY_WRITE_REJECTED" in result.stdout
