"""Integration tests for data_pipes module.

These tests verify end-to-end workflows with real Spark DataFrames and
actual implementations of strategies, registries, and executors.
"""

import shutil
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest
from kindling.data_entities import (
    DataEntities,
    DataEntityManager,
    EntityMetadata,
    EntityProvider,
)
from kindling.data_pipes import (
    DataPipes,
    DataPipesExecuter,
    DataPipesManager,
    EntityReadPersistStrategy,
    PipeMetadata,
)
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


@pytest.fixture(scope="module")
def spark():
    """Create a SparkSession for integration tests"""
    spark = (
        SparkSession.builder.appName("DataPipesIntegrationTest")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .getOrCreate()
    )

    yield spark

    spark.stop()


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test data"""
    temp_path = tempfile.mkdtemp()
    yield Path(temp_path)
    shutil.rmtree(temp_path)


@pytest.fixture
def mock_logger_provider():
    """Create a mock logger provider"""
    mock_provider = Mock()
    mock_logger = Mock()
    mock_provider.get_logger.return_value = mock_logger
    return mock_provider


@pytest.fixture
def entity_registry(mock_logger_provider):
    """Create an entity registry with sample entities"""
    registry = DataEntityManager()

    # Register some test entities
    registry.register_entity(
        "bronze.sales",
        name="Bronze Sales",
        partition_columns=["date"],
        merge_columns=["id"],
        tags={"layer": "bronze"},
        schema=StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("product", StringType(), True),
                StructField("amount", IntegerType(), True),
                StructField("date", StringType(), True),
            ]
        ),
    )

    registry.register_entity(
        "bronze.customers",
        name="Bronze Customers",
        partition_columns=[],
        merge_columns=["customer_id"],
        tags={"layer": "bronze"},
        schema=StructType(
            [
                StructField("customer_id", IntegerType(), False),
                StructField("name", StringType(), True),
                StructField("region", StringType(), True),
            ]
        ),
    )

    registry.register_entity(
        "silver.enriched_sales",
        name="Silver Enriched Sales",
        partition_columns=["date"],
        merge_columns=["id"],
        tags={"layer": "silver"},
        schema=StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("product", StringType(), True),
                StructField("amount", IntegerType(), True),
                StructField("customer_name", StringType(), True),
                StructField("region", StringType(), True),
                StructField("date", StringType(), True),
            ]
        ),
    )

    return registry


@pytest.fixture
def pipes_registry(mock_logger_provider):
    """Create a pipes registry"""
    return DataPipesManager(mock_logger_provider)


class MockEntityReadPersistStrategy(EntityReadPersistStrategy):
    """Mock implementation of EntityReadPersistStrategy that uses in-memory DataFrames"""

    def __init__(self, spark: SparkSession, data_store: dict):
        self.spark = spark
        self.data_store = data_store  # entity_id -> DataFrame
        self.written_data = {}  # entity_id -> DataFrame

    def create_pipe_entity_reader(self, pipe: str):
        """Create an entity reader function"""

        def reader(entity: EntityMetadata, is_first: bool):
            entity_id = entity.entityid
            # Return data from the data store, or None if not available
            return self.data_store.get(entity_id)

        return reader

    def create_pipe_persist_activator(self, pipe: PipeMetadata):
        """Create a persist activator function"""

        def activator(df: DataFrame):
            # Store the result in memory for verification
            output_entity_id = pipe.output_entity_id
            self.written_data[output_entity_id] = df
            # Trigger action to execute the pipeline
            df.count()

        return activator


class TestDataPipesRegistrationAndExecution:
    """Test complete data pipe registration and execution workflow"""

    def test_register_and_execute_simple_pipe(
        self, spark, entity_registry, pipes_registry, mock_logger_provider, temp_dir
    ):
        """Test registering a pipe and executing it with real data"""
        # Create test data
        sales_data = spark.createDataFrame(
            [
                (1, "Widget", 100, "2024-01-01"),
                (2, "Gadget", 200, "2024-01-01"),
                (3, "Widget", 150, "2024-01-02"),
            ],
            ["id", "product", "amount", "date"],
        )

        # Create data store
        data_store = {"bronze.sales": sales_data}

        # Create strategy
        strategy = MockEntityReadPersistStrategy(spark, data_store)

        # Define a transformation pipe
        def aggregate_by_product(bronze_sales):
            """Aggregate sales by product"""
            from pyspark.sql.functions import count
            from pyspark.sql.functions import sum as spark_sum

            return bronze_sales.groupBy("product").agg(
                spark_sum("amount").alias("total_amount"), count("*").alias("count")
            )

        # Register the pipe
        pipes_registry.register_pipe(
            "aggregate_sales",
            name="Aggregate Sales by Product",
            execute=aggregate_by_product,
            tags={"type": "aggregation"},
            input_entity_ids=["bronze.sales"],
            output_entity_id="silver.aggregated_sales",
            output_type="delta",
        )

        # Create trace provider mock
        mock_trace_provider = Mock()
        mock_trace_provider.span.return_value.__enter__ = Mock()
        mock_trace_provider.span.return_value.__exit__ = Mock(return_value=False)

        # Create executor
        executor = DataPipesExecuter(
            mock_logger_provider, entity_registry, pipes_registry, strategy, mock_trace_provider
        )

        # Execute the pipe
        executor.run_datapipes(["aggregate_sales"])

        # Verify the output
        result_df = strategy.written_data["silver.aggregated_sales"]
        assert result_df is not None, "Output DataFrame should be written"

        result_data = result_df.collect()
        assert len(result_data) == 2, "Should have 2 products"

        # Verify aggregation results
        results_dict = {row["product"]: row["total_amount"] for row in result_data}
        assert results_dict["Widget"] == 250, "Widget total should be 250"
        assert results_dict["Gadget"] == 200, "Gadget total should be 200"

    def test_pipe_with_multiple_inputs(
        self, spark, entity_registry, pipes_registry, mock_logger_provider
    ):
        """Test executing a pipe with multiple input entities"""
        # Create test data
        sales_data = spark.createDataFrame(
            [
                (1, "Widget", 100, 101, "2024-01-01"),
                (2, "Gadget", 200, 102, "2024-01-01"),
                (3, "Widget", 150, 101, "2024-01-02"),
            ],
            ["id", "product", "amount", "customer_id", "date"],
        )

        customers_data = spark.createDataFrame(
            [(101, "Alice", "North"), (102, "Bob", "South")], ["customer_id", "name", "region"]
        )

        # Create data store
        data_store = {"bronze.sales": sales_data, "bronze.customers": customers_data}

        # Create strategy
        strategy = MockEntityReadPersistStrategy(spark, data_store)

        # Define a join pipe
        def enrich_sales_with_customers(bronze_sales, bronze_customers):
            """Join sales with customer information"""
            return bronze_sales.join(bronze_customers, on="customer_id", how="left").select(
                "id", "product", "amount", "name", "region", "date"
            )

        # Register the pipe
        pipes_registry.register_pipe(
            "enrich_sales",
            name="Enrich Sales with Customer Data",
            execute=enrich_sales_with_customers,
            tags={"type": "enrichment"},
            input_entity_ids=["bronze.sales", "bronze.customers"],
            output_entity_id="silver.enriched_sales",
            output_type="delta",
        )

        # Create trace provider mock
        mock_trace_provider = Mock()
        mock_trace_provider.span.return_value.__enter__ = Mock()
        mock_trace_provider.span.return_value.__exit__ = Mock(return_value=False)

        # Create executor
        executor = DataPipesExecuter(
            mock_logger_provider, entity_registry, pipes_registry, strategy, mock_trace_provider
        )

        # Execute the pipe
        executor.run_datapipes(["enrich_sales"])

        # Verify the output
        result_df = strategy.written_data["silver.enriched_sales"]
        assert result_df is not None

        result_data = result_df.collect()
        assert len(result_data) == 3, "Should have 3 enriched sales records"

        # Verify customer names are joined
        names = {row["name"] for row in result_data}
        assert "Alice" in names
        assert "Bob" in names

    def test_pipe_chain_execution(
        self, spark, entity_registry, pipes_registry, mock_logger_provider
    ):
        """Test executing a chain of pipes where output of one feeds into another"""
        # Create initial test data
        raw_data = spark.createDataFrame(
            [
                (1, "WIDGET", 100, "2024-01-01"),
                (2, "gadget", 200, "2024-01-01"),
                (3, "Widget", 150, "2024-01-02"),
            ],
            ["id", "product", "amount", "date"],
        )

        # Create data store - starts with raw data
        data_store = {"bronze.sales": raw_data}

        # Create strategy
        strategy = MockEntityReadPersistStrategy(spark, data_store)

        # Register the intermediate entity (silver.clean_sales)
        entity_registry.register_entity(
            "silver.clean_sales",
            name="Clean Sales",
            partition_columns=["date"],
            merge_columns=["id"],
            tags={"layer": "silver"},
            schema=StructType(
                [
                    StructField("id", IntegerType(), False),
                    StructField("product", StringType(), True),
                    StructField("amount", IntegerType(), True),
                    StructField("date", StringType(), True),
                ]
            ),
        )

        # Define first pipe: cleansing
        def cleanse_data(bronze_sales):
            """Clean and standardize product names"""
            from pyspark.sql.functions import initcap, lower

            return bronze_sales.withColumn("product", initcap(lower("product")))

        # Define second pipe: aggregation
        def aggregate_by_product(silver_clean_sales):
            """Aggregate cleaned sales by product"""
            from pyspark.sql.functions import sum as spark_sum

            return silver_clean_sales.groupBy("product").agg(
                spark_sum("amount").alias("total_amount")
            )

        # Register both pipes
        pipes_registry.register_pipe(
            "cleanse_sales",
            name="Cleanse Sales Data",
            execute=cleanse_data,
            tags={"layer": "silver", "type": "cleansing"},
            input_entity_ids=["bronze.sales"],
            output_entity_id="silver.clean_sales",
            output_type="delta",
        )

        pipes_registry.register_pipe(
            "aggregate_clean_sales",
            name="Aggregate Clean Sales",
            execute=aggregate_by_product,
            tags={"layer": "gold", "type": "aggregation"},
            input_entity_ids=["silver.clean_sales"],
            output_entity_id="gold.sales_summary",
            output_type="delta",
        )

        # Create trace provider mock
        mock_trace_provider = Mock()
        mock_trace_provider.span.return_value.__enter__ = Mock()
        mock_trace_provider.span.return_value.__exit__ = Mock(return_value=False)

        # Create executor
        executor = DataPipesExecuter(
            mock_logger_provider, entity_registry, pipes_registry, strategy, mock_trace_provider
        )

        # Execute first pipe
        executor.run_datapipes(["cleanse_sales"])

        # Add the output to data store for next pipe
        data_store["silver.clean_sales"] = strategy.written_data["silver.clean_sales"]

        # Execute second pipe
        executor.run_datapipes(["aggregate_clean_sales"])

        # Verify final output
        result_df = strategy.written_data["gold.sales_summary"]
        assert result_df is not None

        result_data = result_df.collect()
        assert len(result_data) == 2, "Should have 2 unique products after cleansing"

        # All "widget" variants should be consolidated
        results_dict = {row["product"]: row["total_amount"] for row in result_data}
        assert results_dict["Widget"] == 250, "All widget variants should sum to 250"
        assert results_dict["Gadget"] == 200

    def test_pipe_execution_with_decorator(
        self, spark, entity_registry, pipes_registry, mock_logger_provider
    ):
        """Test using the @DataPipes.pipe decorator in integration scenario"""
        # Set up the registry
        DataPipes.dpregistry = pipes_registry

        # Create test data
        sales_data = spark.createDataFrame(
            [(1, "Widget", 100, "2024-01-01"), (2, "Gadget", 200, "2024-01-01")],
            ["id", "product", "amount", "date"],
        )

        data_store = {"bronze.sales": sales_data}
        strategy = MockEntityReadPersistStrategy(spark, data_store)

        # Define pipe using decorator
        @DataPipes.pipe(
            pipeid="filter_high_value_sales",
            name="Filter High Value Sales",
            tags={"type": "filter"},
            input_entity_ids=["bronze.sales"],
            output_entity_id="silver.high_value_sales",
            output_type="delta",
        )
        def filter_high_value(bronze_sales):
            """Filter sales above threshold"""
            return bronze_sales.filter("amount >= 150")

        # Verify pipe was registered
        pipe_def = pipes_registry.get_pipe_definition("filter_high_value_sales")
        assert pipe_def is not None
        assert pipe_def.name == "Filter High Value Sales"

        # Create trace provider mock
        mock_trace_provider = Mock()
        mock_trace_provider.span.return_value.__enter__ = Mock()
        mock_trace_provider.span.return_value.__exit__ = Mock(return_value=False)

        # Execute the pipe
        executor = DataPipesExecuter(
            mock_logger_provider, entity_registry, pipes_registry, strategy, mock_trace_provider
        )

        executor.run_datapipes(["filter_high_value_sales"])

        # Verify output
        result_df = strategy.written_data["silver.high_value_sales"]
        assert result_df is not None

        result_data = result_df.collect()
        assert len(result_data) == 1, "Should have 1 high value sale"
        assert result_data[0]["product"] == "Gadget"
        assert result_data[0]["amount"] == 200


class TestPipeExecutionEdgeCases:
    """Test edge cases and error scenarios in pipe execution"""

    def test_pipe_execution_with_missing_input_entity(
        self, spark, entity_registry, pipes_registry, mock_logger_provider
    ):
        """Test that pipe is skipped when input entity data is not available"""
        # Create empty data store
        data_store = {}
        strategy = MockEntityReadPersistStrategy(spark, data_store)

        # Define a pipe
        def transform_sales(bronze_sales):
            return bronze_sales.select("*")

        pipes_registry.register_pipe(
            "transform_missing",
            name="Transform Missing Data",
            execute=transform_sales,
            tags={},
            input_entity_ids=["bronze.sales"],
            output_entity_id="silver.sales",
            output_type="delta",
        )

        # Create trace provider mock
        mock_trace_provider = Mock()
        mock_trace_provider.span.return_value.__enter__ = Mock()
        mock_trace_provider.span.return_value.__exit__ = Mock(return_value=False)

        # Create executor
        executor = DataPipesExecuter(
            mock_logger_provider, entity_registry, pipes_registry, strategy, mock_trace_provider
        )

        # Execute - should skip due to missing data
        executor.run_datapipes(["transform_missing"])

        # Verify no output was written
        assert "silver.sales" not in strategy.written_data

        # Verify logger was called with skip message
        calls = [str(call) for call in mock_logger_provider.get_logger().debug.call_args_list]
        skip_messages = [c for c in calls if "Skipping" in c]
        assert len(skip_messages) > 0, "Should log skipping message"

    def test_pipe_execution_with_empty_dataframe(
        self, spark, entity_registry, pipes_registry, mock_logger_provider
    ):
        """Test pipe execution when input DataFrame is empty"""
        # Create empty DataFrame
        empty_df = spark.createDataFrame([], "id INT, product STRING, amount INT, date STRING")

        data_store = {"bronze.sales": empty_df}
        strategy = MockEntityReadPersistStrategy(spark, data_store)

        # Define a pipe
        def transform_sales(bronze_sales):
            from pyspark.sql.functions import upper

            return bronze_sales.withColumn("product", upper("product"))

        pipes_registry.register_pipe(
            "transform_empty",
            name="Transform Empty Data",
            execute=transform_sales,
            tags={},
            input_entity_ids=["bronze.sales"],
            output_entity_id="silver.sales",
            output_type="delta",
        )

        # Create trace provider mock
        mock_trace_provider = Mock()
        mock_trace_provider.span.return_value.__enter__ = Mock()
        mock_trace_provider.span.return_value.__exit__ = Mock(return_value=False)

        # Create executor
        executor = DataPipesExecuter(
            mock_logger_provider, entity_registry, pipes_registry, strategy, mock_trace_provider
        )

        # Execute - should handle empty DataFrame
        executor.run_datapipes(["transform_empty"])

        # Verify output exists but is empty
        result_df = strategy.written_data["silver.sales"]
        assert result_df is not None
        assert result_df.count() == 0, "Output should be empty"

    def test_multiple_pipes_execution_in_sequence(
        self, spark, entity_registry, pipes_registry, mock_logger_provider
    ):
        """Test executing multiple independent pipes in one call"""
        # Create test data
        sales_data = spark.createDataFrame(
            [(1, "Widget", 100, "2024-01-01"), (2, "Gadget", 200, "2024-01-01")],
            ["id", "product", "amount", "date"],
        )

        data_store = {"bronze.sales": sales_data}
        strategy = MockEntityReadPersistStrategy(spark, data_store)

        # Define two independent pipes
        def filter_widgets(bronze_sales):
            return bronze_sales.filter("product = 'Widget'")

        def filter_gadgets(bronze_sales):
            return bronze_sales.filter("product = 'Gadget'")

        pipes_registry.register_pipe(
            "filter_widgets",
            name="Filter Widgets",
            execute=filter_widgets,
            tags={"product": "widget"},
            input_entity_ids=["bronze.sales"],
            output_entity_id="silver.widgets",
            output_type="delta",
        )

        pipes_registry.register_pipe(
            "filter_gadgets",
            name="Filter Gadgets",
            execute=filter_gadgets,
            tags={"product": "gadget"},
            input_entity_ids=["bronze.sales"],
            output_entity_id="silver.gadgets",
            output_type="delta",
        )

        # Create trace provider mock
        mock_trace_provider = Mock()
        mock_trace_provider.span.return_value.__enter__ = Mock()
        mock_trace_provider.span.return_value.__exit__ = Mock(return_value=False)

        # Create executor
        executor = DataPipesExecuter(
            mock_logger_provider, entity_registry, pipes_registry, strategy, mock_trace_provider
        )

        # Execute both pipes
        executor.run_datapipes(["filter_widgets", "filter_gadgets"])

        # Verify both outputs
        widgets_df = strategy.written_data["silver.widgets"]
        gadgets_df = strategy.written_data["silver.gadgets"]

        assert widgets_df.count() == 1
        assert gadgets_df.count() == 1

        assert widgets_df.collect()[0]["product"] == "Widget"
        assert gadgets_df.collect()[0]["product"] == "Gadget"


class TestTraceIntegration:
    """Test integration with tracing/observability"""

    def test_pipe_execution_creates_trace_spans(
        self, spark, entity_registry, pipes_registry, mock_logger_provider
    ):
        """Test that pipe execution creates appropriate trace spans"""
        # Create test data
        sales_data = spark.createDataFrame(
            [(1, "Widget", 100, "2024-01-01")], ["id", "product", "amount", "date"]
        )

        data_store = {"bronze.sales": sales_data}
        strategy = MockEntityReadPersistStrategy(spark, data_store)

        # Define a pipe
        def transform_sales(bronze_sales):
            return bronze_sales.select("*")

        pipes_registry.register_pipe(
            "test_pipe",
            name="Test Pipe",
            execute=transform_sales,
            tags={"env": "test"},
            input_entity_ids=["bronze.sales"],
            output_entity_id="silver.sales",
            output_type="delta",
        )

        # Create trace provider mock
        mock_trace_provider = Mock()
        mock_span_context = MagicMock()
        mock_trace_provider.span.return_value = mock_span_context

        # Create executor
        executor = DataPipesExecuter(
            mock_logger_provider, entity_registry, pipes_registry, strategy, mock_trace_provider
        )

        # Execute
        executor.run_datapipes(["test_pipe"])

        # Verify trace spans were created
        assert mock_trace_provider.span.call_count >= 2, "Should create at least 2 spans"

        # Verify outer span for overall execution
        first_call = mock_trace_provider.span.call_args_list[0]
        assert first_call[1]["component"] == "data_pipes_executer"
        assert first_call[1]["operation"] == "execute_datapipes"

        # Verify inner span for specific pipe
        second_call = mock_trace_provider.span.call_args_list[1]
        assert second_call[1]["operation"] == "execute_datapipe"
        assert second_call[1]["component"] == "pipe-test_pipe"
        assert second_call[1]["details"] == {"env": "test"}


class TestComplexTransformations:
    """Test complex transformation scenarios"""

    def test_pipe_with_window_functions(
        self, spark, entity_registry, pipes_registry, mock_logger_provider
    ):
        """Test pipe with window functions and complex aggregations"""
        from pyspark.sql.functions import desc, row_number
        from pyspark.sql.window import Window

        # Create test data with timestamps
        sales_data = spark.createDataFrame(
            [
                (1, "Widget", 100, "2024-01-01", "North"),
                (2, "Widget", 150, "2024-01-02", "North"),
                (3, "Widget", 120, "2024-01-03", "North"),
                (4, "Gadget", 200, "2024-01-01", "South"),
                (5, "Gadget", 250, "2024-01-02", "South"),
            ],
            ["id", "product", "amount", "date", "region"],
        )

        data_store = {"bronze.sales": sales_data}
        strategy = MockEntityReadPersistStrategy(spark, data_store)

        # Define pipe with window function
        def get_top_sales_per_region(bronze_sales):
            """Get top 2 sales per region"""
            window_spec = Window.partitionBy("region").orderBy(desc("amount"))

            return bronze_sales.withColumn("rank", row_number().over(window_spec)).filter(
                "rank <= 2"
            )

        pipes_registry.register_pipe(
            "top_sales",
            name="Top Sales Per Region",
            execute=get_top_sales_per_region,
            tags={"type": "ranking"},
            input_entity_ids=["bronze.sales"],
            output_entity_id="silver.top_sales",
            output_type="delta",
        )

        # Create trace provider mock
        mock_trace_provider = Mock()
        mock_trace_provider.span.return_value.__enter__ = Mock()
        mock_trace_provider.span.return_value.__exit__ = Mock(return_value=False)

        # Create executor
        executor = DataPipesExecuter(
            mock_logger_provider, entity_registry, pipes_registry, strategy, mock_trace_provider
        )

        # Execute
        executor.run_datapipes(["top_sales"])

        # Verify output
        result_df = strategy.written_data["silver.top_sales"]
        result_data = result_df.collect()

        # Should have top 2 from each region = 4 rows total
        assert len(result_data) == 4

        # Verify North region top sales
        north_sales = [row for row in result_data if row["region"] == "North"]
        north_amounts = sorted([row["amount"] for row in north_sales], reverse=True)
        assert north_amounts == [150, 120], "Should have top 2 North sales"

        # Verify South region top sales
        south_sales = [row for row in result_data if row["region"] == "South"]
        south_amounts = sorted([row["amount"] for row in south_sales], reverse=True)
        assert south_amounts == [250, 200], "Should have top 2 South sales"


class TestEntityKeyTransformation:
    """Test entity key transformation (dot to underscore)"""

    def test_entity_keys_with_dots_become_underscores(
        self, spark, entity_registry, pipes_registry, mock_logger_provider
    ):
        """Test that entity IDs with dots are converted to underscores in function parameters"""
        # Register entities with dot notation
        entity_registry.register_entity(
            "bronze.raw.sales",
            name="Raw Sales",
            partition_columns=[],
            merge_columns=["id"],
            tags={},
            schema=StructType(
                [StructField("id", IntegerType()), StructField("value", IntegerType())]
            ),
        )

        entity_registry.register_entity(
            "bronze.raw.metadata",
            name="Raw Metadata",
            partition_columns=[],
            merge_columns=["id"],
            tags={},
            schema=StructType(
                [StructField("id", IntegerType()), StructField("info", StringType())]
            ),
        )

        # Create test data
        sales_data = spark.createDataFrame([(1, 100), (2, 200)], ["id", "value"])
        metadata_data = spark.createDataFrame([(1, "info1"), (2, "info2")], ["id", "info"])

        data_store = {"bronze.raw.sales": sales_data, "bronze.raw.metadata": metadata_data}
        strategy = MockEntityReadPersistStrategy(spark, data_store)

        # Track function parameters
        captured_params = {}

        def join_with_metadata(**kwargs):
            """Join sales with metadata - capture parameter names"""
            captured_params.update(kwargs)
            # Parameters should be bronze_raw_sales and bronze_raw_metadata
            return kwargs["bronze_raw_sales"].join(kwargs["bronze_raw_metadata"], on="id")

        pipes_registry.register_pipe(
            "join_sales_metadata",
            name="Join Sales with Metadata",
            execute=join_with_metadata,
            tags={},
            input_entity_ids=["bronze.raw.sales", "bronze.raw.metadata"],
            output_entity_id="silver.joined",
            output_type="delta",
        )

        # Create trace provider mock
        mock_trace_provider = Mock()
        mock_trace_provider.span.return_value.__enter__ = Mock()
        mock_trace_provider.span.return_value.__exit__ = Mock(return_value=False)

        # Create executor and execute
        executor = DataPipesExecuter(
            mock_logger_provider, entity_registry, pipes_registry, strategy, mock_trace_provider
        )

        executor.run_datapipes(["join_sales_metadata"])

        # Verify parameter keys were transformed
        assert "bronze_raw_sales" in captured_params, "Dots should be converted to underscores"
        assert "bronze_raw_metadata" in captured_params
        assert "bronze.raw.sales" not in captured_params, "Original dot notation should not be used"

        # Verify output
        result_df = strategy.written_data["silver.joined"]
        assert result_df.count() == 2
