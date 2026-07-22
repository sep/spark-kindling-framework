"""
Unit tests for the ``schema.drift`` policy: ``evolve`` (default, no
preflight), ``warn`` (log and proceed), ``fail`` (SchemaDriftError before
the write starts).

Drift = columns the table lacks, or same-named columns with different
types. Columns the table has but the DataFrame lacks are NOT drift —
Delta null-fills them and SCD2 strategies add bookkeeping columns after
the check.
"""

from unittest.mock import MagicMock

import pytest
from kindling.data_entities import (
    DataEntityManager,
    EntityMetadata,
    EntityNameMapper,
    EntityPathLocator,
)
from kindling.entity_provider_delta import DeltaEntityProvider, SchemaDriftError
from kindling.signaling import SignalProvider
from kindling.spark_config import ConfigService
from kindling.spark_log_provider import PythonLoggerProvider
from pyspark.sql.types import LongType, StringType, StructField, StructType


@pytest.fixture(autouse=True)
def mock_spark_session(monkeypatch):
    spark = MagicMock()
    monkeypatch.setattr("kindling.entity_provider_delta.get_or_create_spark_session", lambda: spark)
    return spark


@pytest.fixture
def provider():
    config = MagicMock(spec=ConfigService)
    config.get.side_effect = lambda key, default=None: (
        "catalog" if key == "kindling.delta.access_mode" else default
    )
    logger_provider = MagicMock(spec=PythonLoggerProvider)
    logger_provider.get_logger.return_value = MagicMock()
    return DeltaEntityProvider(
        config=config,
        entity_name_mapper=MagicMock(spec=EntityNameMapper),
        path_locator=MagicMock(spec=EntityPathLocator),
        tp=logger_provider,
        signal_provider=MagicMock(spec=SignalProvider),
    )


def _entity(policy=None):
    tags = {"schema.drift": policy} if policy else {}
    return EntityMetadata(
        entityid="silver.orders",
        name="orders",
        merge_columns=["order_id"],
        tags=tags,
        schema=None,
    )


def _table_ref(fields):
    table_ref = MagicMock()
    table_ref.get_delta_table.return_value.toDF.return_value.schema = StructType(fields)
    return table_ref


def _df(fields):
    df = MagicMock()
    df.schema = StructType(fields)
    return df


_TABLE = [StructField("order_id", StringType()), StructField("amount", LongType())]


class TestDriftPolicyEnforcement:
    def test_whitespace_policy_defaults_to_evolve(self, provider):
        table_ref = MagicMock()
        provider._enforce_schema_drift_policy(_df(_TABLE), _entity(" "), table_ref)
        table_ref.get_delta_table.assert_not_called()

    def test_evolve_default_skips_preflight_entirely(self, provider):
        table_ref = MagicMock()
        provider._enforce_schema_drift_policy(_df(_TABLE), _entity(), table_ref)
        table_ref.get_delta_table.assert_not_called()

    def test_fail_raises_on_new_column(self, provider):
        df = _df(_TABLE + [StructField("discount", LongType())])
        with pytest.raises(SchemaDriftError, match="discount"):
            provider._enforce_schema_drift_policy(df, _entity("fail"), _table_ref(_TABLE))

    def test_fail_raises_on_type_conflict(self, provider):
        df = _df([StructField("order_id", StringType()), StructField("amount", StringType())])
        with pytest.raises(SchemaDriftError, match="amount: bigint != string"):
            provider._enforce_schema_drift_policy(df, _entity("fail"), _table_ref(_TABLE))

    def test_missing_columns_are_not_drift(self, provider):
        # Table has 'amount', df doesn't: Delta null-fills, SCD strategies
        # add their own columns later — must pass under 'fail'.
        df = _df([StructField("order_id", StringType())])
        provider._enforce_schema_drift_policy(df, _entity("fail"), _table_ref(_TABLE))

    def test_matching_schema_passes_fail_policy(self, provider):
        provider._enforce_schema_drift_policy(_df(_TABLE), _entity("fail"), _table_ref(_TABLE))

    def test_warn_logs_and_proceeds(self, provider):
        df = _df(_TABLE + [StructField("discount", LongType())])
        provider._enforce_schema_drift_policy(df, _entity("warn"), _table_ref(_TABLE))
        provider.logger.warning.assert_called_once()
        assert "discount" in provider.logger.warning.call_args.args[0]

    def test_merge_path_runs_preflight(self, provider):
        entity = _entity("fail")
        df = _df(_TABLE + [StructField("discount", LongType())])
        table_ref = _table_ref(_TABLE)
        with pytest.raises(SchemaDriftError):
            provider._merge_to_delta_table(df, entity, table_ref)

    def test_append_path_runs_preflight(self, provider):
        entity = _entity("fail")
        df = _df(_TABLE + [StructField("discount", LongType())])
        table_ref = _table_ref(_TABLE)
        table_ref.access_mode = "catalog"
        with pytest.raises(SchemaDriftError):
            provider._append_to_delta_table(df, entity, table_ref)


class TestDriftTagRegistration:
    @pytest.mark.parametrize("policy", ["evolve", "warn", "fail", "FAIL "])
    def test_valid_policies_register(self, policy):
        manager = DataEntityManager()
        manager.register_entity(
            "silver.orders",
            name="orders",
            merge_columns=["order_id"],
            tags={"schema.drift": policy},
            schema=None,
        )
        assert "silver.orders" in manager.registry

    def test_invalid_policy_rejected(self):
        with pytest.raises(ValueError, match="invalid schema.drift"):
            DataEntityManager().register_entity(
                "silver.orders",
                name="orders",
                merge_columns=["order_id"],
                tags={"schema.drift": "strict"},
                schema=None,
            )
