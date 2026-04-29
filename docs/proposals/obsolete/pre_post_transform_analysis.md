# Analysis: Pre-Transform & Post-Transform Activities in Kindling

**Date:** February 2, 2026
**Status:** Analysis
**Related Proposal:** [signal_dag_streaming_proposal.md](signal_dag_streaming_proposal.md)

---

## Executive Summary

This document analyzes the need for pre-transform and post-transform hook activities within the datapipe execution lifecycle. The analysis evaluates congruence with the existing signal_dag_streaming proposal and recommends integration patterns.

**Conclusion:** ✅ **FULLY CONGRUENT** - The signal-based architecture already provides the foundation for pre/post-transform activities. This analysis proposes specific implementation patterns that complement and extend the existing proposal.

---

## 1. Use Cases Analysis

### Pre-Transform Activities

Activities that must occur **before** a pipe's transform function executes:

| Use Case | Description | Data Flow Impact |
|----------|-------------|------------------|
| **Data Validation** | Validate input schema, null checks, business rules | Can reject/fail transform |
| **Schema Verification** | Ensure input entities match expected schema | Can fail transform |
| **Row Count Thresholds** | Verify minimum/maximum rows before processing | Can skip transform |
| **Data Freshness Checks** | Verify input data is not stale | Can delay/skip transform |
| **Dependency Verification** | Check all input entities are populated | Can block transform |
| **Pre-aggregation Sampling** | Sample data for quality profiling | Informational only |
| **Resource Pre-allocation** | Cache broadcast variables, temp views | Performance optimization |

### Post-Transform Activities

Activities that must occur **after** a pipe's transform function completes:

| Use Case | Description | Data Flow Impact |
|----------|-------------|------------------|
| **Audit Column Injection** | Add transform version, timestamp metadata | Modifies output |
| **Data Quality Scoring** | Calculate quality metrics on output | Informational/metadata |
| **Lineage Recording** | Record which transform version produced data | Modifies output |
| **Output Validation** | Validate output schema, null constraints | Can reject persist |
| **Row Count Verification** | Verify output row count expectations | Can alert/fail |
| **Metric Emission** | Emit row counts, duration, quality scores | Informational |
| **Checkpoint Coordination** | Coordinate watermarks, versions | State management |
| **Derived Entity Triggers** | Signal downstream pipes to execute | Orchestration |

---

## 2. Congruence with Current Proposal

### 2.1 Signal-Based Architecture Alignment

The signal_dag_streaming proposal provides `before_pipe` and `after_pipe` signals that align directly with pre/post-transform needs:

**Current Signals (from proposal):**
```
datapipes.before_pipe  → Pre-transform hook point
datapipes.after_pipe   → Post-transform hook point
datapipes.pipe_failed  → Error handling hook point
```

**Key Insight:** The signal architecture provides the **observation points** for pre/post activities. What's needed is:
1. **Interceptor capability** - ability to modify execution flow (block, modify, etc.)
2. **Transform context** - access to input/output DataFrames within handlers
3. **Priority ordering** - ensure activities execute in correct order

### 2.2 Gap Analysis

| Capability | Proposal Status | Gap |
|------------|-----------------|-----|
| Pre-transform notification | ✅ `before_pipe` signal | None |
| Post-transform notification | ✅ `after_pipe` signal | None |
| Error notification | ✅ `pipe_failed` signal | None |
| Transform blocking | ⚠️ Signals are observational | Need interceptor pattern |
| DataFrame access in hooks | ⚠️ Not in payload | Need context injection |
| Priority ordering | ⚠️ Blinker receiver order | Need explicit ordering |
| Output modification | ⚠️ Post-transform only observes | Need interceptor pattern |

### 2.3 Lineage Tracking Synergy

The proposal already includes **Part 4: Transform Version Lineage & Tracking** which directly addresses the post-transform audit column use case:

```python
# From proposal - VersionedExecutionContext
def add_lineage_column(self, df, pipe_id, layer):
    """Add version lineage metadata to DataFrame."""
    col_name = f"_{layer}_lineage"
    lineage_struct = struct(
        lit(pipe_id).alias("pipe"),
        lit(self.package_name).alias("package"),
        lit(self.version).alias("version"),
        current_timestamp().alias("timestamp")
    )
    return df.withColumn(col_name, lineage_struct)
```

This is **exactly** the post-transform activity pattern requested.

---

## 3. Proposed Integration Pattern

### 3.1 Transform Interceptor Chain

Extend the signal system with an **interceptor pattern** that allows handlers to modify execution flow:

```python
from dataclasses import dataclass, field
from typing import List, Callable, Optional, Any
from enum import Enum
from pyspark.sql import DataFrame

class InterceptAction(Enum):
    """Actions an interceptor can return."""
    CONTINUE = "continue"      # Proceed with transform
    SKIP = "skip"              # Skip this pipe (no error)
    FAIL = "fail"              # Fail the transform
    RETRY = "retry"            # Retry with modified input

@dataclass
class TransformContext:
    """Context passed to pre/post transform interceptors."""
    pipe_id: str
    pipe_name: str
    pipe_metadata: 'PipeMetadata'
    run_id: str

    # Input data (available in pre-transform)
    input_dataframes: dict[str, DataFrame] = field(default_factory=dict)

    # Output data (available in post-transform)
    output_dataframe: Optional[DataFrame] = None

    # Execution metadata
    execution_start_time: Optional[float] = None
    execution_duration: Optional[float] = None

    # For interceptor communication
    metadata: dict[str, Any] = field(default_factory=dict)

    # Interceptor control
    action: InterceptAction = InterceptAction.CONTINUE
    skip_reason: Optional[str] = None
    fail_reason: Optional[str] = None

@dataclass
class InterceptorResult:
    """Result from an interceptor execution."""
    action: InterceptAction = InterceptAction.CONTINUE
    modified_input: Optional[dict[str, DataFrame]] = None  # Pre-transform only
    modified_output: Optional[DataFrame] = None             # Post-transform only
    message: Optional[str] = None

class TransformInterceptor:
    """Base class for transform interceptors."""

    # Priority: lower = runs first
    priority: int = 100

    def pre_transform(self, context: TransformContext) -> InterceptorResult:
        """Called before transform execution. Override to implement."""
        return InterceptorResult(action=InterceptAction.CONTINUE)

    def post_transform(self, context: TransformContext) -> InterceptorResult:
        """Called after transform execution. Override to implement."""
        return InterceptorResult(action=InterceptAction.CONTINUE)

    def on_transform_error(self, context: TransformContext, error: Exception) -> InterceptorResult:
        """Called when transform fails. Override to implement."""
        return InterceptorResult(action=InterceptAction.FAIL, message=str(error))
```

### 3.2 Concrete Interceptor Implementations

#### Pre-Transform: Data Validation Interceptor

**Option A: Custom Validation Rules** (Simple, no dependencies)

```python
from pyspark.sql.functions import col, count, when

class DataValidationInterceptor(TransformInterceptor):
    """Validates input data before transform execution."""

    priority = 10  # Run early

    def __init__(self, validation_rules: dict[str, 'ValidationRule'] = None):
        self.rules = validation_rules or {}

    def pre_transform(self, context: TransformContext) -> InterceptorResult:
        """Validate all input DataFrames."""
        validation_errors = []

        for entity_key, df in context.input_dataframes.items():
            if df is None:
                continue

            rules = self.rules.get(entity_key, [])
            for rule in rules:
                if not rule.validate(df):
                    validation_errors.append(
                        f"{entity_key}: {rule.error_message}"
                    )

        if validation_errors:
            return InterceptorResult(
                action=InterceptAction.FAIL,
                message=f"Validation failed: {'; '.join(validation_errors)}"
            )

        return InterceptorResult(action=InterceptAction.CONTINUE)

# Example validation rules
@dataclass
class ValidationRule:
    """Base validation rule."""
    error_message: str

    def validate(self, df: DataFrame) -> bool:
        raise NotImplementedError

class NotNullRule(ValidationRule):
    """Ensure column has no nulls."""
    column: str

    def validate(self, df: DataFrame) -> bool:
        null_count = df.filter(col(self.column).isNull()).count()
        return null_count == 0

class MinRowCountRule(ValidationRule):
    """Ensure minimum row count."""
    min_rows: int

    def validate(self, df: DataFrame) -> bool:
        return df.count() >= self.min_rows

class SchemaMatchRule(ValidationRule):
    """Ensure schema matches expected."""
    expected_columns: List[str]

    def validate(self, df: DataFrame) -> bool:
        actual = set(df.columns)
        expected = set(self.expected_columns)
        return expected.issubset(actual)
```

**Option B: Community DataFrame Validation Libraries** (Production-grade)

See [Section 3.6: DataFrame Validation Library Integration](#36-dataframe-validation-library-integration) below for integration with:
- **Great Expectations** (most popular)
- **Pydantic-Spark** (type-safe schemas)
- **pandera** (Pandas-style for Spark)
- **soda-spark** (SQL-based validation)
- **deequ** (Amazon's data quality library)

#### Post-Transform: Audit/Lineage Interceptor

```python
from importlib.metadata import version as pkg_version
from pyspark.sql.functions import lit, struct, current_timestamp

class AuditLineageInterceptor(TransformInterceptor):
    """Adds audit columns and lineage metadata to transform output."""

    priority = 90  # Run late (after other post-transforms)

    def __init__(
        self,
        package_name: str = "kindling",
        include_lineage: bool = True,
        include_audit: bool = True
    ):
        self.package_name = package_name
        self.include_lineage = include_lineage
        self.include_audit = include_audit

        try:
            self.package_version = pkg_version(package_name)
        except:
            self.package_version = "dev"

    def post_transform(self, context: TransformContext) -> InterceptorResult:
        """Add lineage and audit columns to output DataFrame."""
        df = context.output_dataframe

        if df is None:
            return InterceptorResult(action=InterceptAction.CONTINUE)

        # Detect layer from pipe_id
        layer = self._detect_layer(context.pipe_id)

        # Add lineage column
        if self.include_lineage:
            lineage_col = f"_{layer}_lineage"
            df = df.withColumn(
                lineage_col,
                struct(
                    lit(context.pipe_id).alias("pipe"),
                    lit(self.package_name).alias("package"),
                    lit(self.package_version).alias("version"),
                    current_timestamp().alias("timestamp"),
                    lit(context.run_id).alias("run_id")
                )
            )

        # Add audit columns
        if self.include_audit:
            df = df.withColumn("_transform_version", lit(self.package_version))
            df = df.withColumn("_transform_timestamp", current_timestamp())
            df = df.withColumn("_transform_pipe", lit(context.pipe_id))

        return InterceptorResult(
            action=InterceptAction.CONTINUE,
            modified_output=df
        )

    def _detect_layer(self, pipe_id: str) -> str:
        if pipe_id.startswith("bronze."):
            return "bronze"
        elif pipe_id.startswith("silver."):
            return "silver"
        elif pipe_id.startswith("gold."):
            return "gold"
        return "unknown"
```

#### Post-Transform: Data Quality Scoring

```python
from pyspark.sql.functions import col, count, when, sum as spark_sum

class DataQualityInterceptor(TransformInterceptor):
    """Calculates and emits data quality metrics after transform."""

    priority = 50  # Run mid-order

    def __init__(self, signal_provider: 'SignalProvider'):
        self.signals = signal_provider

    def post_transform(self, context: TransformContext) -> InterceptorResult:
        """Calculate quality metrics and emit signal."""
        df = context.output_dataframe

        if df is None:
            return InterceptorResult(action=InterceptAction.CONTINUE)

        # Calculate quality metrics
        total_rows = df.count()

        # Null percentage per column
        null_metrics = {}
        for col_name in df.columns:
            if not col_name.startswith("_"):  # Skip metadata columns
                null_count = df.filter(col(col_name).isNull()).count()
                null_metrics[col_name] = {
                    "null_count": null_count,
                    "null_percentage": (null_count / total_rows * 100) if total_rows > 0 else 0
                }

        # Emit quality signal
        self.signals.emit(
            "datapipes.quality_metrics",
            pipe_id=context.pipe_id,
            run_id=context.run_id,
            total_rows=total_rows,
            null_metrics=null_metrics,
            quality_score=self._calculate_score(null_metrics)
        )

        return InterceptorResult(action=InterceptAction.CONTINUE)

    def _calculate_score(self, null_metrics: dict) -> float:
        """Calculate overall quality score (0-100)."""
        if not null_metrics:
            return 100.0

        avg_null_pct = sum(
            m["null_percentage"] for m in null_metrics.values()
        ) / len(null_metrics)

        return max(0.0, 100.0 - avg_null_pct)
```

---

## 3.6 DataFrame Validation Library Integration

### ⚠️ CRITICAL: Lazy vs Eager Evaluation

**The Problem:** Most DataFrame validation libraries trigger Spark **actions** (like `.collect()`, `.count()`, `.take()`), which force immediate execution of the DataFrame. This means:

1. **DataFrame computed TWICE**: Once for validation, once for actual transform
2. **Performance penalty**: 2x computation time for every validated DataFrame
3. **Wasted resources**: Double the Spark cluster utilization

**Eager Validation (Most Libraries):**
```python
# ⚠️ BAD: Triggers action, computes DataFrame
@validate.great_expectations(suite)
def my_pipe(bronze_events: DataFrame):
    # DataFrame already computed during validation!
    # Computing AGAIN for transform = 2x cost
    return bronze_events.filter(...)
```

**Lazy Validation (Ideal):**
```python
# ✅ GOOD: Adds constraints to plan, validates during write
@validate.delta_constraints(constraints)
def my_pipe(bronze_events: DataFrame):
    # Constraints enforced on write, not on read
    # Single-pass computation
    return bronze_events.filter(...)
```

### Lazy Validation Options

#### Option 1: Delta Lake Constraints (Built-in, Zero Dependencies)

Delta Lake's CHECK constraints are **enforced at write time**, not validated at read time:

```python
class DeltaConstraintInterceptor(TransformInterceptor):
    """Add Delta Lake CHECK constraints - enforced on write, no eager evaluation."""

    priority = 10

    def __init__(self, constraints: dict[str, list[str]]):
        """
        constraints: entity_key -> list of CHECK expressions

        Example:
        {
            "bronze_events": [
                "event_id IS NOT NULL",
                "event_type IN ('click', 'view', 'purchase')",
                "amount >= 0"
            ]
        }
        """
        self.constraints = constraints

    def post_transform(self, context: TransformContext) -> InterceptorResult:
        """Add constraints to output DataFrame - enforced on write."""
        if context.output_dataframe is None:
            return InterceptorResult(action=InterceptAction.CONTINUE)

        # Get entity key for this pipe
        entity_key = context.metadata.get("output_entity_key")
        if entity_key not in self.constraints:
            return InterceptorResult(action=InterceptAction.CONTINUE)

        # Add metadata for Delta writer to create constraints
        # Constraints enforced when DataFrame is written, not now
        constraint_metadata = {
            "delta.constraints": self.constraints[entity_key]
        }
        context.metadata["write_options"] = constraint_metadata

        app_logger.info(
            f"Added {len(self.constraints[entity_key])} Delta constraints "
            f"to {entity_key} (enforced on write)"
        )

        return InterceptorResult(action=InterceptAction.CONTINUE)

# Usage - NO eager evaluation!
@DataPipes.pipe(...)
@validate.delta_constraints([
    "event_id IS NOT NULL",
    "event_type IN ('click', 'view', 'purchase')",
    "amount >= 0"
], entity="silver_events")
def my_pipe(bronze_events: DataFrame):
    # No validation happens here - just returns DataFrame plan
    return bronze_events.filter(...)

# Constraints enforced when DataPipesExecuter writes to Delta table
# Single-pass: validation happens during write, not before
```

**Pros:**
- ✅ **Zero eager evaluation** - constraints enforced on write
- ✅ **Single-pass computation** - no double execution
- ✅ **Built-in to Delta** - no external dependencies
- ✅ **Permanent** - constraints persist with table
- ✅ **Fast** - Delta optimizes constraint checking

**Cons:**
- ⚠️ Only works with Delta Lake tables (not Parquet, CSV, etc.)
- ⚠️ Violations caught on write, not before transform starts
- ⚠️ Limited to SQL expressions (no custom Python logic)

---

#### Option 2: Schema Validation (Lazy, Catalyst-Optimized)

Validate schema without triggering actions - Spark's Catalyst optimizer handles this lazily:

```python
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

class SchemaValidationInterceptor(TransformInterceptor):
    """Validate DataFrame schema - NO action triggered."""

    priority = 10

    def __init__(self, schemas: dict[str, StructType]):
        self.schemas = schemas

    def pre_transform(self, context: TransformContext) -> InterceptorResult:
        """Validate schema - uses df.schema (lazy, no action)."""
        validation_errors = []

        for entity_key, df in context.input_dataframes.items():
            if df is None or entity_key not in self.schemas:
                continue

            expected_schema = self.schemas[entity_key]
            actual_schema = df.schema  # ✅ LAZY - no action triggered

            # Compare field names and types
            expected_fields = {f.name: f.dataType for f in expected_schema.fields}
            actual_fields = {f.name: f.dataType for f in actual_schema.fields}

            # Check for missing required fields
            missing = set(expected_fields.keys()) - set(actual_fields.keys())
            if missing:
                validation_errors.append(
                    f"{entity_key}: Missing required columns: {', '.join(missing)}"
                )

            # Check for type mismatches
            for field_name in expected_fields:
                if field_name in actual_fields:
                    if expected_fields[field_name] != actual_fields[field_name]:
                        validation_errors.append(
                            f"{entity_key}.{field_name}: Expected {expected_fields[field_name]}, "
                            f"got {actual_fields[field_name]}"
                        )

        if validation_errors:
            return InterceptorResult(
                action=InterceptAction.FAIL,
                message=f"Schema validation failed: {'; '.join(validation_errors)}"
            )

        return InterceptorResult(action=InterceptAction.CONTINUE)

# Usage - NO eager evaluation!
expected_schema = StructType([
    StructField("event_id", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=False),
    StructField("amount", DoubleType(), nullable=False)
])

@DataPipes.pipe(...)
@validate.schema(expected_schema, entity="bronze_events")
def my_pipe(bronze_events: DataFrame):
    # Schema validated without triggering action!
    return bronze_events.filter(...)
```

**Pros:**
- ✅ **Zero eager evaluation** - `df.schema` is lazy
- ✅ **Works with any format** - not Delta-specific
- ✅ **Fast** - just metadata access
- ✅ **Type safety** - catches schema drift early

**Cons:**
- ⚠️ Only validates schema, not data values
- ⚠️ Can't check "amount >= 0" or "event_type IN (...)"

---

### Eager Validation Libraries (Trigger Actions)

The following libraries **ALL trigger Spark actions** and compute DataFrames eagerly. Use them only when:
- You need complex validation logic not expressible in SQL
- You're willing to accept 2x computation cost
- The DataFrame is small (caching mitigates cost)

Community libraries can power the validation interceptors, providing production-grade data quality checks:

### 1. **Great Expectations** (Most Popular)
- **PyPI**: `great-expectations`
- **Maturity**: ✅✅✅ Very mature, industry standard
- **PySpark Support**: ✅ Full support via SparkDFExecutionEngine
- **⚠️ Evaluation**: **EAGER** - Triggers DataFrame actions, computes data twice

```python
from great_expectations.dataset import SparkDFDataset
from great_expectations.core import ExpectationSuite

class GreatExpectationsInterceptor(TransformInterceptor):
    """Validate DataFrames using Great Expectations.

    ⚠️ WARNING: This triggers Spark actions and computes DataFrames eagerly.
    Use Delta constraints or schema validation for better performance.
    """

    priority = 10

    def __init__(self, expectation_suites: dict[str, ExpectationSuite]):
        self.suites = expectation_suites

    def pre_transform(self, context: TransformContext) -> InterceptorResult:
        """Run GE validations on input DataFrames."""
        validation_errors = []

        for entity_key, df in context.input_dataframes.items():
            if df is None or entity_key not in self.suites:
                continue

            # Wrap Spark DataFrame
            ge_df = SparkDFDataset(df)

            # Run expectations
            suite = self.suites[entity_key]
            results = ge_df.validate(expectation_suite=suite)

            if not results.success:
                failed = [r.expectation_config.expectation_type
                         for r in results.results if not r.success]
                validation_errors.append(
                    f"{entity_key}: Failed expectations: {', '.join(failed)}"
                )

        if validation_errors:
            return InterceptorResult(
                action=InterceptAction.FAIL,
                message=f"GE validation failed: {'; '.join(validation_errors)}"
            )

        return InterceptorResult(action=InterceptAction.CONTINUE)

# Usage with decorator
from great_expectations.core.expectation_suite import ExpectationSuite

bronze_suite = ExpectationSuite(expectation_suite_name="bronze_events")
bronze_suite.add_expectation({
    "expectation_type": "expect_column_values_to_not_be_null",
    "kwargs": {"column": "event_id"}
})
bronze_suite.add_expectation({
    "expectation_type": "expect_table_row_count_to_be_between",
    "kwargs": {"min_value": 1, "max_value": None}
})

@DataPipes.pipe(...)
@validate.great_expectations(suite=bronze_suite, entity="bronze_events")
def my_pipe(bronze_events: DataFrame): ...
```

**Pros:**
- ✅ Industry standard, well-documented
- ✅ Rich validation library (100+ expectations)
- ✅ Built-in profiling and documentation
- ✅ Data quality dashboard/reports
- ✅ Works with Spark, Pandas, SQL

**Cons:**
- ⚠️ Heavy dependency (~50+ packages)
- ⚠️ Steep learning curve
- ⚠️ Can be slow on large DataFrames

---

### 2. **Pydantic-Spark** (Type-Safe Schemas)
- **PyPI**: `pydantic-spark`
- **Maturity**: ✅✅ Mature
- **PySpark Support**: ✅ Native Spark integration
- **⚠️ Evaluation**: **EAGER** - Triggers DataFrame actions, computes data twice

```python
from pydantic import BaseModel, Field, validator
from pydantic_spark import SparkDataFrameValidator

class CustomerEventSchema(BaseModel):
    """Type-safe schema for customer events."""
    event_id: str = Field(..., min_length=1)
    customer_id: str = Field(..., min_length=1)
    event_type: str = Field(..., regex="^(click|view|purchase)$")
    event_timestamp: datetime
    amount: float = Field(ge=0.0)

    @validator('event_timestamp')
    def timestamp_not_future(cls, v):
        if v > datetime.now():
            raise ValueError("Timestamp cannot be in future")
        return v

class PydanticSparkInterceptor(TransformInterceptor):
    """Validate DataFrames using Pydantic schemas.

    ⚠️ WARNING: This triggers Spark actions and computes DataFrames eagerly.
    Use schema validation interceptor for lazy type checking.
    """

    priority = 10

    def __init__(self, schemas: dict[str, type[BaseModel]]):
        self.schemas = schemas
        self.validators = {
            key: SparkDataFrameValidator(schema)
            for key, schema in schemas.items()
        }

    def pre_transform(self, context: TransformContext) -> InterceptorResult:
        """Validate DataFrames against Pydantic schemas."""
        validation_errors = []

        for entity_key, df in context.input_dataframes.items():
            if df is None or entity_key not in self.validators:
                continue

            validator = self.validators[entity_key]

            try:
                # Validates entire DataFrame
                validator.validate(df)
            except ValidationError as e:
                validation_errors.append(f"{entity_key}: {str(e)}")

        if validation_errors:
            return InterceptorResult(
                action=InterceptAction.FAIL,
                message=f"Schema validation failed: {'; '.join(validation_errors)}"
            )

        return InterceptorResult(action=InterceptAction.CONTINUE)

# Usage with decorator
@DataPipes.pipe(...)
@validate.pydantic_schema(CustomerEventSchema, entity="bronze_events")
def my_pipe(bronze_events: DataFrame): ...
```

**Pros:**
- ✅ Type-safe Python schemas
- ✅ IDE autocomplete for schemas
- ✅ Familiar Pydantic API
- ✅ Lightweight compared to GE
- ✅ Fast validation

**Cons:**
- ⚠️ Less feature-rich than GE
- ⚠️ Requires Pydantic knowledge

---

### 3. **pandera** (Pandas-style for Spark)
- **PyPI**: `pandera[pyspark]`
- **Maturity**: ✅✅ Mature
- **PySpark Support**: ✅ Experimental PySpark support
- **⚠️ Evaluation**: **EAGER** - Triggers DataFrame actions, computes data twice

```python
import pandera.pyspark as pa
from pandera.pyspark import DataFrameModel

class CustomerEventSchema(DataFrameModel):
    """Pandera schema for customer events."""
    event_id: str = pa.Field(str_length={"min_value": 1})
    customer_id: str = pa.Field(str_length={"min_value": 1})
    event_type: str = pa.Field(isin=["click", "view", "purchase"])
    event_timestamp: datetime = pa.Field()
    amount: float = pa.Field(ge=0.0)

    class Config:
        strict = True  # Fail on unexpected columns

class PanderaInterceptor(TransformInterceptor):
    """Validate DataFrames using Pandera schemas.

    ⚠️ WARNING: This triggers Spark actions and computes DataFrames eagerly.
    """

    priority = 10

    def __init__(self, schemas: dict[str, type[DataFrameModel]]):
        self.schemas = schemas

    def pre_transform(self, context: TransformContext) -> InterceptorResult:
        """Validate DataFrames against Pandera schemas."""
        validation_errors = []

        for entity_key, df in context.input_dataframes.items():
            if df is None or entity_key not in self.schemas:
                continue

            schema = self.schemas[entity_key]

            try:
                # Validate DataFrame
                schema.validate(df, lazy=True)
            except pa.errors.SchemaError as e:
                validation_errors.append(f"{entity_key}: {str(e)}")

        if validation_errors:
            return InterceptorResult(
                action=InterceptAction.FAIL,
                message=f"Pandera validation failed: {'; '.join(validation_errors)}"
            )

        return InterceptorResult(action=InterceptAction.CONTINUE)

# Usage with decorator
@DataPipes.pipe(...)
@validate.pandera_schema(CustomerEventSchema, entity="bronze_events")
def my_pipe(bronze_events: DataFrame): ...
```

**Pros:**
- ✅ Pandas-like API (familiar)
- ✅ Statistical validation support
- ✅ Lazy validation (collects all errors)
- ✅ Hypothesis testing capabilities

**Cons:**
- ⚠️ PySpark support is experimental
- ⚠️ Less battle-tested for Spark than GE

---

### 4. **soda-spark** (SQL-Based Validation)
- **PyPI**: `soda-spark`
- **Maturity**: ✅✅ Mature (from Soda.io)
- **PySpark Support**: ✅ Native Spark support
- **⚠️ Evaluation**: **EAGER** - Triggers DataFrame actions, computes data twice

```python
from soda.scan import Scan

class SodaInterceptor(TransformInterceptor):
    """Validate DataFrames using Soda SQL checks.

    ⚠️ WARNING: This triggers Spark actions and computes DataFrames eagerly.
    Use Delta constraints for SQL-based validation with lazy evaluation.
    """

    priority = 10

    def __init__(self, checks_yaml: dict[str, str]):
        """
        checks_yaml: entity_key -> YAML check definitions

        Example YAML:
        ```yaml
        checks for bronze_events:
          - row_count > 0
          - missing_count(event_id) = 0
          - invalid_count(event_type) = 0:
              valid values: ['click', 'view', 'purchase']
          - duplicate_count(event_id) = 0
        ```
        """
        self.checks = checks_yaml

    def pre_transform(self, context: TransformContext) -> InterceptorResult:
        """Run Soda checks on input DataFrames."""
        validation_errors = []

        for entity_key, df in context.input_dataframes.items():
            if df is None or entity_key not in self.checks:
                continue

            # Create temp view for Soda
            df.createOrReplaceTempView(f"_soda_temp_{entity_key}")

            # Run Soda scan
            scan = Scan()
            scan.add_spark_session(df.sparkSession)
            scan.add_sodacl_yaml_str(self.checks[entity_key])

            result = scan.execute()

            if result.has_check_fails():
                failures = [f.check.name for f in result.get_checks_fail()]
                validation_errors.append(
                    f"{entity_key}: Failed checks: {', '.join(failures)}"
                )

        if validation_errors:
            return InterceptorResult(
                action=InterceptAction.FAIL,
                message=f"Soda validation failed: {'; '.join(validation_errors)}"
            )

        return InterceptorResult(action=InterceptAction.CONTINUE)

# Usage with YAML-based checks
bronze_checks = """
checks for bronze_events:
  - row_count > 0
  - missing_count(event_id) = 0
  - missing_count(customer_id) = 0
  - invalid_count(event_type) = 0:
      valid values: ['click', 'view', 'purchase']
"""

@DataPipes.pipe(...)
@validate.soda_checks(bronze_checks, entity="bronze_events")
def my_pipe(bronze_events: DataFrame): ...
```

**Pros:**
- ✅ SQL-like syntax (easy for analysts)
- ✅ YAML-based configuration
- ✅ Rich check library
- ✅ Integration with Soda Cloud

**Cons:**
- ⚠️ Requires Soda account for full features
- ⚠️ YAML can be verbose

---

### 5. **deequ** (Amazon's Data Quality)
- **PyPI**: `pydeequ`
- **Maturity**: ✅✅✅ Very mature (from Amazon)
- **PySpark Support**: ✅ Built for Spark
- **⚠️ Evaluation**: **EAGER** - Triggers DataFrame actions, computes data twice

```python
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite, VerificationResult

class DeequInterceptor(TransformInterceptor):
    """Validate DataFrames using Amazon Deequ.

    ⚠️ WARNING: This triggers Spark actions and computes DataFrames eagerly.
    Best used for anomaly detection on sampled data, not full validation.
    """

    priority = 10

    def __init__(self, checks: dict[str, list[Check]]):
        self.checks = checks

    def pre_transform(self, context: TransformContext) -> InterceptorResult:
        """Run Deequ checks on input DataFrames."""
        validation_errors = []

        for entity_key, df in context.input_dataframes.items():
            if df is None or entity_key not in self.checks:
                continue

            # Build verification suite
            suite = VerificationSuite(df.sparkSession) \
                .onData(df)

            for check in self.checks[entity_key]:
                suite = suite.addCheck(check)

            # Run verification
            result: VerificationResult = suite.run()

            if result.status != "Success":
                failures = [
                    f"{c.constraint}: {c.message}"
                    for c in result.checkResults.values()
                    if c.status != "Success"
                ]
                validation_errors.append(
                    f"{entity_key}: {'; '.join(failures)}"
                )

        if validation_errors:
            return InterceptorResult(
                action=InterceptAction.FAIL,
                message=f"Deequ validation failed: {'; '.join(validation_errors)}"
            )

        return InterceptorResult(action=InterceptAction.CONTINUE)

# Usage with Deequ checks
from pydeequ.checks import Check, CheckLevel

bronze_checks = [
    Check(df.sparkSession, CheckLevel.Error, "Bronze Events Quality")
        .hasSize(lambda x: x > 0)
        .isComplete("event_id")
        .isComplete("customer_id")
        .isContainedIn("event_type", ["click", "view", "purchase"])
        .isNonNegative("amount")
]

@DataPipes.pipe(...)
@validate.deequ_checks(bronze_checks, entity="bronze_events")
def my_pipe(bronze_events: DataFrame): ...
```

**Pros:**
- ✅ Built by Amazon for production Spark
- ✅ Scala-based (very fast)
- ✅ Anomaly detection built-in
- ✅ Metrics computation
- ✅ Profile/suggest checks

**Cons:**
- ⚠️ Requires JVM (Scala dependencies)
- ⚠️ Steeper learning curve
- ⚠️ Python wrapper less intuitive

---

### Comparison Matrix

| Library | Maturity | Learning Curve | Performance | **Evaluation** | Best For |
|---------|----------|----------------|-------------|----------------|----------|
| **Delta Constraints** | ✅✅✅ | Low | ✅✅✅ Very High | **✅ LAZY** | SQL-expressible constraints, single-pass |
| **Schema Validation** | ✅✅✅ | Low | ✅✅✅ Very High | **✅ LAZY** | Type checking, schema drift detection |
| **Great Expectations** | ✅✅✅ | Medium-High | ⚠️ Medium | **❌ EAGER** | Comprehensive validation + reports |
| **Pydantic-Spark** | ✅✅ | Low | ⚠️ Medium | **❌ EAGER** | Type-safe schemas, lightweight |
| **pandera** | ✅✅ | Low-Medium | ⚠️ Medium | **❌ EAGER** | Pandas users, statistical checks |
| **soda-spark** | ✅✅ | Low | ⚠️ Medium | **❌ EAGER** | SQL analysts, YAML config |
| **deequ** | ✅✅✅ | Medium-High | ⚠️ High | **❌ EAGER** | Production Spark, anomaly detection |

### Critical Performance Note

**Eager validation libraries compute DataFrames twice:**
1. Once during validation (triggered by `.count()`, `.collect()`, etc.)
2. Once during transform execution

**Example cost:**
```python
# ❌ BAD: Double computation
@validate.great_expectations(suite)  # Computes df here (Action #1)
def my_pipe(bronze_events: DataFrame):
    result = bronze_events.filter(...)  # Computes again (Action #2)
    return result  # 2x cost!

# ✅ GOOD: Single computation
@validate.delta_constraints(constraints)  # Just adds metadata
def my_pipe(bronze_events: DataFrame):
    result = bronze_events.filter(...)
    return result  # Constraints checked on write - 1x cost!
```

### Recommendation by Use Case

| Use Case | Recommended Approach | Why |
|----------|---------------------|-----|
| **SQL-expressible validations** | **Delta Constraints** | ✅ Lazy, built-in, single-pass, zero dependencies |
| **Schema/type validation** | **Schema Validation** | ✅ Lazy, fast, works with any format |
| **Complex business rules** | **Great Expectations** | Industry standard, rich features (accept eager cost) |
| **Type-safe Python schemas** | **Pydantic-Spark** | Pythonic, IDE support (accept eager cost) |
| **High-performance production** | **Delta Constraints + deequ** | Best hybrid: lazy constraints + eager anomaly detection |
| **SQL-familiar teams** | **Delta Constraints** | SQL syntax, no library learning curve |

### Production-Ready Hybrid Approach (Recommended)

Use **Delta Lake Constraints** for common validations + **deequ** only for advanced anomaly detection:

```python
# ✅ Common validations: Lazy (single-pass)
@validate.delta_constraints([
    "event_id IS NOT NULL",
    "customer_id IS NOT NULL",
    "event_type IN ('click', 'view', 'purchase')",
    "amount >= 0"
], entity="silver_events")
# ✅ Advanced checks: Eager (double-pass, but only on small sample)
@validate.deequ_checks(anomaly_detection_checks, sample_fraction=0.1)
@DataPipes.pipe(...)
def my_pipe(bronze_events: DataFrame): ...
```

This gives you:
- ✅ **90% of validations lazy** (Delta constraints)
- ✅ **10% of validations eager** (deequ anomaly detection on sample)
- ✅ **Minimal performance impact** (most work is single-pass)
- ✅ **Production-grade quality** (constraints + anomaly detection)
- ✅ **Reasonable dependency footprint**

### Alternative: Schema Validation + Great Expectations

For non-Delta environments:

```python
# ✅ Schema validation: Lazy (zero cost)
@validate.schema(expected_schema, entity="bronze_events")
# ⚠️ Value validation: Eager (double-pass cost)
@validate.great_expectations(suite, entity="bronze_events")
@DataPipes.pipe(...)
def my_pipe(bronze_events: DataFrame): ...
```

**Key Insight:** Use lazy validation (Delta constraints, schema checks) for the majority of cases. Only use eager validation libraries (GE, deequ, etc.) when you need validations that can't be expressed in SQL constraints.

---

### Eager Validation as Data Pipes (Recommended Pattern)

**Key Insight:** If you're doing eager validation (which computes the DataFrame), treat it as a **data pipe** that produces output, not as a pre-check interceptor.

#### Pattern 1: Quarantine/Dead Letter Queue

Invalid rows written to separate table for investigation:

```python
# Validation as a data pipe - produces TWO outputs
@DataPipes.pipe(
    pipe_id="validate_customers",
    input_entities=["bronze_customers"],
    output_entity="silver_customers_validated"
)
def validate_customers(bronze_customers: DataFrame) -> DataFrame:
    """Validate and separate valid/invalid rows."""
    from great_expectations.dataset import SparkDFDataset

    # Wrap DataFrame for validation
    ge_df = SparkDFDataset(bronze_customers)

    # Run validation
    results = ge_df.validate(expectation_suite=customer_suite)

    # Add validation results as column
    validation_results_df = bronze_customers.withColumn(
        "_ge_validation_passed",
        F.lit(results.success)  # Or per-row validation
    )

    # Split into valid/invalid
    valid_df = validation_results_df.filter(F.col("_ge_validation_passed"))
    invalid_df = validation_results_df.filter(~F.col("_ge_validation_passed"))

    # Write invalid rows to quarantine table
    invalid_df.write \
        .mode("append") \
        .format("delta") \
        .saveAsTable("silver.customers_quarantine")

    # Return only valid rows for downstream processing
    return valid_df.drop("_ge_validation_passed")

# Downstream pipes only process validated data
@DataPipes.pipe(
    pipe_id="enrich_customers",
    input_entities=["silver_customers_validated"],
    output_entity="gold_customers"
)
def enrich_customers(silver_customers_validated: DataFrame) -> DataFrame:
    # Only valid data here - safe to process
    return silver_customers_validated.join(...)
```

**Medallion Architecture:**
```
bronze.customers → [validate_customers] → silver.customers_validated
                                        ↓
                                   silver.customers_quarantine
```

#### Pattern 2: Tagging Pattern (Keep All Data)

Add validation columns, let downstream pipes decide what to do:

```python
@DataPipes.pipe(
    pipe_id="tag_with_validation",
    input_entities=["bronze_events"],
    output_entity="bronze_events_tagged"
)
def tag_with_validation(bronze_events: DataFrame) -> DataFrame:
    """Add validation columns - downstream decides filtering."""

    # Add validation result columns
    df_with_validation = bronze_events \
        .withColumn("_valid_amount", F.col("amount") >= 0) \
        .withColumn("_valid_email", F.col("email").rlike(email_regex)) \
        .withColumn("_valid_event_type", F.col("event_type").isin(valid_types)) \
        .withColumn(
            "_is_valid",
            F.col("_valid_amount") & F.col("_valid_email") & F.col("_valid_event_type")
        )

    return df_with_validation

# Downstream pipe filters based on use case
@DataPipes.pipe(
    pipe_id="process_for_analytics",
    input_entities=["bronze_events_tagged"],
    output_entity="silver_events_analytics"
)
def process_for_analytics(bronze_events_tagged: DataFrame) -> DataFrame:
    # Analytics requires perfect data - filter to valid only
    return bronze_events_tagged.filter(F.col("_is_valid"))

@DataPipes.pipe(
    pipe_id="process_for_auditing",
    input_entities=["bronze_events_tagged"],
    output_entity="silver_events_audit"
)
def process_for_auditing(bronze_events_tagged: DataFrame) -> DataFrame:
    # Audit needs ALL data - keep validation flags for reporting
    return bronze_events_tagged
```

**Benefit:** Single validation pass, multiple consumers with different tolerances

#### Pattern 3: Quality Metrics as Side Output

Compute and persist data quality metrics (deequ pattern):

```python
@DataPipes.pipe(
    pipe_id="compute_quality_metrics",
    input_entities=["bronze_events"],
    output_entity="bronze_events"  # Pass-through
)
def compute_quality_metrics(bronze_events: DataFrame) -> DataFrame:
    """Compute quality metrics and persist to metrics table."""
    from pydeequ.checks import Check, CheckLevel
    from pydeequ.verification import VerificationSuite

    # Run quality checks
    check = Check(spark, CheckLevel.Warning, "Event Quality") \
        .hasSize(lambda x: x > 0) \
        .isComplete("event_id") \
        .isComplete("customer_id") \
        .isContainedIn("event_type", ["click", "view", "purchase"])

    result = VerificationSuite(spark) \
        .onData(bronze_events) \
        .addCheck(check) \
        .run()

    # Extract metrics
    metrics_df = result.checkResults \
        .select("check", "constraint", "constraint_status", "constraint_message")

    # Add metadata
    metrics_with_context = metrics_df \
        .withColumn("timestamp", F.current_timestamp()) \
        .withColumn("source_table", F.lit("bronze.events")) \
        .withColumn("record_count", F.lit(bronze_events.count()))

    # Persist metrics to monitoring table
    metrics_with_context.write \
        .mode("append") \
        .format("delta") \
        .saveAsTable("monitoring.data_quality_metrics")

    # Pass through original data unchanged
    return bronze_events
```

**Architecture:**
```
bronze.events → [compute_quality_metrics] → bronze.events (unchanged)
                         ↓
                    monitoring.data_quality_metrics
```

#### Pattern 4: Row-Level Validation Results

Add detailed validation results per row (Great Expectations pattern):

```python
@DataPipes.pipe(
    pipe_id="validate_with_details",
    input_entities=["bronze_customers"],
    output_entity="silver_customers_validated"
)
def validate_with_details(bronze_customers: DataFrame) -> DataFrame:
    """Add per-row validation results."""

    # Create validation columns for each rule
    validated_df = bronze_customers \
        .withColumn(
            "validation_results",
            F.struct(
                F.when(F.col("email").rlike(email_regex), F.lit(True))
                    .otherwise(F.lit(False)).alias("email_valid"),
                F.when(F.col("age") >= 18, F.lit(True))
                    .otherwise(F.lit(False)).alias("age_valid"),
                F.when(F.col("customer_id").isNotNull(), F.lit(True))
                    .otherwise(F.lit(False)).alias("id_valid")
            )
        ) \
        .withColumn(
            "_is_valid",
            F.col("validation_results.email_valid") &
            F.col("validation_results.age_valid") &
            F.col("validation_results.id_valid")
        )

    return validated_df

# Query validation results
spark.sql("""
    SELECT
        customer_id,
        validation_results.email_valid,
        validation_results.age_valid,
        validation_results.id_valid,
        _is_valid
    FROM silver.customers_validated
    WHERE NOT _is_valid
""")
```

#### Library-Specific Output Patterns

**Great Expectations:**
```python
# GE returns validation results, doesn't modify DataFrame
results = ge_df.validate(suite)

# You decide what to do:
# 1. Raise error if validation fails
if not results.success:
    raise ValueError(f"Validation failed: {results}")

# 2. Add validation column and split
df_with_flag = df.withColumn("_ge_passed", F.lit(results.success))
valid = df_with_flag.filter(F.col("_ge_passed"))
invalid = df_with_flag.filter(~F.col("_ge_passed"))
```

**deequ:**
```python
# deequ returns metrics, doesn't modify DataFrame
result = VerificationSuite(spark).onData(df).addCheck(check).run()

# You decide what to do with metrics:
# 1. Store metrics in monitoring table
result.checkResults.write.saveAsTable("monitoring.quality_metrics")

# 2. Fail pipeline if checks fail
if result.status != "Success":
    raise ValueError(f"Quality checks failed")

# 3. Pass through original DataFrame unchanged
return df
```

**Pydantic-Spark:**
```python
# Pydantic-Spark validates or raises error
try:
    validator.validate(df)
    # Validation passed - return original df
    return df
except ValidationError as e:
    # You decide error handling:
    # 1. Fail pipeline
    raise

    # 2. Filter to schema-compliant rows only
    return df.filter(schema_filter_expr)
```

### Recommendations by Use Case

| Use Case | Pattern | Output Strategy |
|----------|---------|-----------------|
| **Production ETL with strict quality** | Quarantine | Valid → downstream, Invalid → quarantine table |
| **Data lake with mixed quality** | Tagging | Add validation columns, downstream filters as needed |
| **Monitoring/Observability** | Metrics | Pass-through data, persist metrics to monitoring table |
| **Detailed investigation** | Row-level results | Persist validation details per row for debugging |
| **Fail-fast pipelines** | Interceptor with raise | Use pre-transform interceptor, fail on first error |

### Best Practice: Validation Layers

```python
# Layer 1: Lazy validation (schema, basic constraints) - Interceptor
@validate.schema(schema)
@validate.delta_constraints(basic_constraints)
# Layer 2: Complex eager validation - Data Pipe
@DataPipes.pipe(...)
def validate_business_rules(df: DataFrame) -> DataFrame:
    # Compute validation, produce output
    return validated_df
# Layer 3: Quality metrics - Separate Data Pipe
@DataPipes.pipe(...)
def compute_quality_metrics(df: DataFrame) -> DataFrame:
    # Compute metrics as side effect, pass through data
    return df
```

This gives you:
- ✅ Fast lazy validation for common cases (interceptor)
- ✅ Detailed validation with output for complex cases (data pipe)
- ✅ Quality monitoring without blocking pipeline (metrics pipe)

---

### Streaming Validation Patterns

**Critical Difference:** Streaming DataFrames execute **per micro-batch**, not once. Validation happens continuously.

#### When Validation Executes in Streaming

```python
# Batch processing
df = spark.read.format("delta").load("bronze.events")
validated_df = validate(df)  # ← Executes once when .write() is called
validated_df.write.format("delta").save("silver.events")

# Streaming processing
df = spark.readStream.format("delta").load("bronze.events")
validated_df = validate(df)  # ← Executes per micro-batch, continuously!
validated_df.writeStream.format("delta").start("silver.events")
```

**Key Insight:** Validation logic is applied to **each micro-batch** as it arrives.

---

#### Pattern 1: Lazy Validation in Streaming (Delta Constraints)

**Delta constraints work perfectly with streaming** - enforced per micro-batch on write:

```python
from pyspark.sql.streaming import StreamingQuery

def streaming_with_constraints(spark):
    """Lazy validation with Delta constraints - works in streaming!"""

    # Read stream
    bronze_stream = spark.readStream \
        .format("delta") \
        .load("bronze.events")

    # Add validation columns (lazy - part of DataFrame plan)
    validated_stream = bronze_stream \
        .withColumn("_event_id_valid", F.col("event_id").isNotNull()) \
        .withColumn("_amount_valid", F.col("amount") >= 0) \
        .withColumn(
            "_is_valid",
            F.col("_event_id_valid") & F.col("_amount_valid")
        )

    # Write with Delta constraints
    # Constraints enforced per micro-batch automatically!
    query = validated_stream.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "/checkpoints/silver_events") \
        .option("delta.constraints.eventIdNotNull", "event_id IS NOT NULL") \
        .option("delta.constraints.amountNonNegative", "amount >= 0") \
        .start("silver.events")

    return query
```

**✅ This works because:**
- Delta constraints are enforced on write (per micro-batch)
- No eager evaluation - constraints checked as data is written
- Failed rows cause micro-batch to fail (exactly once semantics preserved)

---

#### Pattern 2: Streaming Schema Validation (Lazy)

**Schema validation executes once when stream starts** (not per micro-batch):

```python
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

def streaming_with_schema_validation(spark):
    """Schema validation - checked when stream starts."""

    # Define expected schema
    expected_schema = StructType([
        StructField("event_id", StringType(), nullable=False),
        StructField("customer_id", StringType(), nullable=False),
        StructField("amount", DoubleType(), nullable=False)
    ])

    # Read stream with schema enforcement
    bronze_stream = spark.readStream \
        .format("delta") \
        .schema(expected_schema)  # ✅ Enforced at stream start
        .load("bronze.events")

    # Any schema drift causes stream to fail immediately
    query = bronze_stream.writeStream \
        .format("delta") \
        .start("silver.events")

    return query
```

**⚠️ Note:** Schema validation happens **once** when stream starts, not per micro-batch.

---

#### Pattern 3: Streaming Quarantine with foreachBatch

**Eager validation requires foreachBatch** - executes per micro-batch:

```python
def quarantine_invalid_rows(micro_batch_df: DataFrame, batch_id: int):
    """Executed per micro-batch - can do eager validation here."""

    # Add validation columns
    validated_df = micro_batch_df \
        .withColumn("_event_id_valid", F.col("event_id").isNotNull()) \
        .withColumn("_amount_valid", F.col("amount") >= 0) \
        .withColumn("_email_valid", F.col("email").rlike(email_regex)) \
        .withColumn(
            "_is_valid",
            F.col("_event_id_valid") & F.col("_amount_valid") & F.col("_email_valid")
        ) \
        .withColumn("_batch_id", F.lit(batch_id)) \
        .withColumn("_processing_time", F.current_timestamp())

    # Split valid/invalid
    valid_df = validated_df.filter(F.col("_is_valid")).drop("_is_valid", "_event_id_valid", "_amount_valid", "_email_valid")
    invalid_df = validated_df.filter(~F.col("_is_valid"))

    # Write valid rows to silver
    valid_df.write \
        .format("delta") \
        .mode("append") \
        .save("silver.events_validated")

    # Write invalid rows to quarantine
    invalid_df.write \
        .format("delta") \
        .mode("append") \
        .save("silver.events_quarantine")

def streaming_with_quarantine(spark):
    """Streaming with quarantine pattern using foreachBatch."""

    bronze_stream = spark.readStream \
        .format("delta") \
        .load("bronze.events")

    # Use foreachBatch to split valid/invalid per micro-batch
    query = bronze_stream.writeStream \
        .foreachBatch(quarantine_invalid_rows) \
        .option("checkpointLocation", "/checkpoints/quarantine") \
        .start()

    return query
```

**✅ This works because:**
- `foreachBatch` gives you the micro-batch DataFrame
- You can perform any batch-like operations (including eager validation)
- Executes per micro-batch automatically
- Can write to multiple sinks (valid + quarantine)

---

#### Pattern 4: Lazy Column-Based Filtering (Recommended for Streaming)

**Best performance** - no foreachBatch needed:

```python
def streaming_lazy_filtering(spark):
    """Filter invalid rows lazily - no foreachBatch needed."""

    bronze_stream = spark.readStream \
        .format("delta") \
        .load("bronze.events")

    # Add validation columns (lazy!)
    validated_stream = bronze_stream \
        .withColumn("_is_valid",
            F.col("event_id").isNotNull() &
            (F.col("amount") >= 0) &
            F.col("email").rlike(email_regex)
        )

    # Split into two streams

    # Valid stream → silver
    valid_query = validated_stream \
        .filter(F.col("_is_valid")) \
        .drop("_is_valid") \
        .writeStream \
        .format("delta") \
        .option("checkpointLocation", "/checkpoints/valid") \
        .start("silver.events_validated")

    # Invalid stream → quarantine
    invalid_query = validated_stream \
        .filter(~F.col("_is_valid")) \
        .writeStream \
        .format("delta") \
        .option("checkpointLocation", "/checkpoints/invalid") \
        .start("silver.events_quarantine")

    return [valid_query, invalid_query]
```

**✅ Best approach because:**
- Completely lazy - Spark optimizer handles everything
- No foreachBatch overhead
- Multiple output streams from single source
- Automatic checkpointing per stream

---

#### Streaming Validation Decision Matrix

| Validation Type | Lazy/Eager | When Executes | Pattern | Performance |
|----------------|-----------|---------------|---------|-------------|
| **Schema validation** | Lazy | Stream start (once) | `.schema(expected_schema)` | ✅✅✅ Excellent |
| **Delta constraints** | Lazy | Per micro-batch (write) | Delta write options | ✅✅✅ Excellent |
| **Column expression filters** | Lazy | Per micro-batch | `.filter(validation_expr)` | ✅✅✅ Excellent |
| **Quarantine split** | Lazy | Per micro-batch | Multiple writeStream | ✅✅ Very Good |
| **Great Expectations** | Eager | Per micro-batch | foreachBatch + GE | ⚠️ Medium |
| **deequ metrics** | Eager | Per micro-batch | foreachBatch + deequ | ⚠️ Medium |

---

#### Best Practices for Streaming Validation

**✅ DO:**
- Use lazy validation (Delta constraints, Column expressions)
- Add validation columns to stream plan
- Use multiple output streams for quarantine
- Keep validation logic simple and fast
- Use schema enforcement at stream start

**❌ DON'T:**
- Use eager validation libraries without foreachBatch
- Call `.count()` or `.collect()` in stream logic
- Do complex aggregations in validation
- Block micro-batch processing with slow validation
- Fail entire stream on single bad record (use quarantine instead)

---

#### Example: Complete Streaming Data Pipe with Validation

```python
@DataPipes.pipe(
    pipe_id="streaming_validate_events",
    input_entities=["bronze_events_stream"],
    output_entity="silver_events_validated",
    streaming=True  # Mark as streaming pipe
)
def streaming_validate_events(bronze_events_stream: DataFrame) -> DataFrame:
    """Streaming validation pipe - executes per micro-batch."""

    # Add validation columns (lazy - part of stream plan)
    validated_stream = bronze_events_stream \
        .withColumn("_event_id_valid", F.col("event_id").isNotNull()) \
        .withColumn("_amount_valid", F.col("amount") >= 0) \
        .withColumn("_email_valid", F.col("email").rlike(email_regex)) \
        .withColumn(
            "_is_valid",
            F.col("_event_id_valid") & F.col("_amount_valid") & F.col("_email_valid")
        )

    # Return validated stream (filtering happens in DataPipesExecuter)
    return validated_stream

# DataPipesExecuter handles streaming write
executer.execute_streaming_pipe(
    pipe_id="streaming_validate_events",
    checkpoint_location="/checkpoints/validate_events",
    output_mode="append"
)
```

---

#### Summary: Streaming vs Batch Validation

| Aspect | Batch | Streaming |
|--------|-------|-----------|
| **Execution** | Once per `.write()` | Per micro-batch |
| **Lazy validation** | Works identically | Works identically |
| **Eager validation** | Direct library calls | Requires foreachBatch |
| **Quarantine** | Single write split | Multiple writeStream or foreachBatch |
| **Metrics** | Single computation | Per micro-batch computation |
| **Interceptors** | Execute on transform | Execute when stream defined (logic runs per batch) |
| **Performance** | One-time cost | Continuous overhead |

**Key Takeaway:** For streaming, **prefer lazy validation patterns** (Delta constraints, Column expressions) over eager libraries to minimize per-micro-batch overhead.

---

## 3.6.1 Building Lazy Validation from Python Libraries

**Can we adapt Python validation libraries for lazy evaluation?**

Short answer: **Not directly**, but we can create **Spark-native lazy wrappers** inspired by Python validation patterns.

### The Problem with Python Validation Libraries

Traditional Python validation libraries (Pydantic, Cerberus, Voluptuous, etc.) are designed for in-memory data:
- They iterate over data immediately (eager)
- They work on Python dicts/objects, not distributed DataFrames
- No concept of lazy execution plans

### The Solution: Spark Column Expression Builder Pattern

**Key insight:** We can create a Python DSL that **compiles to Spark Column expressions**, which are inherently lazy.

```python
from pyspark.sql import Column
import pyspark.sql.functions as F
from typing import Callable

class LazyValidationRule:
    """Base class for validation rules that compile to Spark Column expressions."""

    def to_column_expr(self, col_name: str) -> Column:
        """Convert validation rule to Spark Column expression (lazy!)."""
        raise NotImplementedError

class NotNull(LazyValidationRule):
    """Validates that a column is not null."""

    def to_column_expr(self, col_name: str) -> Column:
        # Returns Column expression, doesn't trigger action!
        return F.col(col_name).isNotNull()

class InSet(LazyValidationRule):
    """Validates that a column value is in a set of allowed values."""

    def __init__(self, allowed_values: list):
        self.allowed_values = allowed_values

    def to_column_expr(self, col_name: str) -> Column:
        # Returns Column expression, doesn't trigger action!
        return F.col(col_name).isin(self.allowed_values)

class MinValue(LazyValidationRule):
    """Validates that a column value >= min_val."""

    def __init__(self, min_val: float):
        self.min_val = min_val

    def to_column_expr(self, col_name: str) -> Column:
        # Returns Column expression, doesn't trigger action!
        return F.col(col_name) >= self.min_val

class Regex(LazyValidationRule):
    """Validates that a column matches a regex pattern."""

    def __init__(self, pattern: str):
        self.pattern = pattern

    def to_column_expr(self, col_name: str) -> Column:
        # Returns Column expression, doesn't trigger action!
        return F.col(col_name).rlike(self.pattern)

class StringLength(LazyValidationRule):
    """Validates string length constraints."""

    def __init__(self, min_length: int = None, max_length: int = None):
        self.min_length = min_length
        self.max_length = max_length

    def to_column_expr(self, col_name: str) -> Column:
        col_len = F.length(F.col(col_name))

        conditions = []
        if self.min_length is not None:
            conditions.append(col_len >= self.min_length)
        if self.max_length is not None:
            conditions.append(col_len <= self.max_length)

        # Combine conditions with AND
        if len(conditions) == 1:
            return conditions[0]
        return conditions[0] & conditions[1]
```

### Declarative Schema Definition (Pydantic-inspired, but Lazy)

```python
from dataclasses import dataclass, field
from typing import Dict, List

@dataclass
class ColumnValidation:
    """Defines validation rules for a single column."""
    name: str
    rules: List[LazyValidationRule] = field(default_factory=list)

    def get_combined_expr(self) -> Column:
        """Combine all rules into single Column expression (lazy!)."""
        if not self.rules:
            return F.lit(True)

        # Combine with AND
        expr = self.rules[0].to_column_expr(self.name)
        for rule in self.rules[1:]:
            expr = expr & rule.to_column_expr(self.name)
        return expr

@dataclass
class DataFrameSchema:
    """Defines validation schema for a DataFrame (Pydantic-inspired)."""
    columns: List[ColumnValidation] = field(default_factory=list)

    def add_column(self, name: str, *rules: LazyValidationRule) -> 'DataFrameSchema':
        """Fluent API for adding column validations."""
        self.columns.append(ColumnValidation(name, list(rules)))
        return self

    def get_validation_expr(self) -> Column:
        """Get single Column expression that validates entire row (lazy!)."""
        if not self.columns:
            return F.lit(True)

        # Combine all column validations with AND
        expr = self.columns[0].get_combined_expr()
        for col_validation in self.columns[1:]:
            expr = expr & col_validation.get_combined_expr()
        return expr

    def add_validation_column(self, df: DataFrame) -> DataFrame:
        """Add validation result column to DataFrame (lazy!)."""
        return df.withColumn("_validation_passed", self.get_validation_expr())

    def filter_valid_rows(self, df: DataFrame) -> DataFrame:
        """Return only valid rows (lazy!)."""
        return df.filter(self.get_validation_expr())

    def filter_invalid_rows(self, df: DataFrame) -> DataFrame:
        """Return only invalid rows for error reporting (lazy!)."""
        return df.filter(~self.get_validation_expr())
```

### Usage Example (Pydantic-style, but Lazy!)

```python
# Define schema (similar to Pydantic)
customer_schema = DataFrameSchema() \
    .add_column("event_id", NotNull(), StringLength(min_length=1)) \
    .add_column("customer_id", NotNull(), StringLength(min_length=1)) \
    .add_column("event_type", NotNull(), InSet(["click", "view", "purchase"])) \
    .add_column("amount", NotNull(), MinValue(0.0)) \
    .add_column("email", Regex(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"))

# Use in interceptor (LAZY - no action triggered!)
class LazyValidationInterceptor(TransformInterceptor):
    """Validates using Spark Column expressions - completely lazy."""

    priority = 10

    def __init__(self, schemas: dict[str, DataFrameSchema]):
        self.schemas = schemas

    def pre_transform(self, context: TransformContext) -> InterceptorResult:
        """Add validation columns to DataFrames - NO action triggered."""

        for entity_key, df in context.input_dataframes.items():
            if df is None or entity_key not in self.schemas:
                continue

            schema = self.schemas[entity_key]

            # Add validation column (lazy operation!)
            validated_df = schema.add_validation_column(df)

            # Replace in context (still lazy!)
            context.input_dataframes[entity_key] = validated_df

            # Add metadata for post-transform error handling
            context.metadata[f"{entity_key}_schema"] = schema

        return InterceptorResult(action=InterceptAction.CONTINUE)

    def post_transform(self, context: TransformContext) -> InterceptorResult:
        """Check if any invalid rows exist - triggers SINGLE action."""

        for entity_key, schema in context.metadata.items():
            if not entity_key.endswith("_schema"):
                continue

            entity_name = entity_key.replace("_schema", "")
            df = context.input_dataframes.get(entity_name)

            if df is None:
                continue

            # Check if validation column exists
            if "_validation_passed" not in df.columns:
                continue

            # Single action: count invalid rows
            invalid_count = df.filter(~F.col("_validation_passed")).count()

            if invalid_count > 0:
                # Get sample of invalid rows for error message
                invalid_sample = df.filter(~F.col("_validation_passed")).limit(5).collect()

                return InterceptorResult(
                    action=InterceptAction.FAIL,
                    message=f"{entity_name}: {invalid_count} rows failed validation. "
                            f"Sample: {invalid_sample}"
                )

        return InterceptorResult(action=InterceptAction.CONTINUE)

# Usage with decorator
@DataPipes.pipe(...)
@validate.lazy_schema(customer_schema, entity="bronze_events")
def my_pipe(bronze_events: DataFrame):
    # Validation column added to DataFrame plan - NO action triggered!
    # DataFrame still lazy until final write
    return bronze_events.filter(...)
```

### Advanced: Conditional Validation Rules

```python
class ConditionalRule(LazyValidationRule):
    """Apply validation only when condition is met."""

    def __init__(self, condition: LazyValidationRule, then_rule: LazyValidationRule):
        self.condition = condition
        self.then_rule = then_rule

    def to_column_expr(self, col_name: str) -> Column:
        # when(condition).then(validate).otherwise(True)
        return F.when(
            self.condition.to_column_expr(col_name),
            self.then_rule.to_column_expr(col_name)
        ).otherwise(F.lit(True))

class CustomRule(LazyValidationRule):
    """Custom validation using any Spark Column expression builder."""

    def __init__(self, expr_builder: Callable[[str], Column]):
        self.expr_builder = expr_builder

    def to_column_expr(self, col_name: str) -> Column:
        return self.expr_builder(col_name)

# Example usage
schema = DataFrameSchema() \
    .add_column(
        "discount",
        ConditionalRule(
            condition=CustomRule(lambda col: F.col("event_type") == "purchase"),
            then_rule=MinValue(0.0)  # Only validate discount >= 0 for purchases
        )
    )
```

### Benefits of This Approach

✅ **Completely Lazy** - No actions triggered, pure Column expressions
✅ **Pythonic API** - Familiar Pydantic-style declarative syntax
✅ **Composable** - Rules can be combined, nested, reused
✅ **Single-Pass Validation** - Validation happens during DataFrame processing
✅ **Zero Dependencies** - Pure PySpark, no external libraries
✅ **Extensible** - Easy to add custom rules
✅ **IDE Support** - Type hints, autocomplete work perfectly

### Integration with Existing Python Libraries

You could even create **adapters** that convert existing Python validation libraries to lazy Spark rules:

```python
from pydantic import BaseModel, Field

def pydantic_to_spark_rules(pydantic_model: type[BaseModel]) -> DataFrameSchema:
    """Convert Pydantic model to Spark lazy validation schema."""
    schema = DataFrameSchema()

    for field_name, field_info in pydantic_model.model_fields.items():
        rules = []

        # Not nullable -> NotNull
        if field_info.is_required():
            rules.append(NotNull())

        # Regex pattern -> Regex
        if hasattr(field_info, 'pattern') and field_info.pattern:
            rules.append(Regex(field_info.pattern))

        # Min/max constraints
        if hasattr(field_info, 'ge'):  # greater than or equal
            rules.append(MinValue(field_info.ge))

        if field_name and rules:
            schema.add_column(field_name, *rules)

    return schema

# Use Pydantic model definition
class CustomerEvent(BaseModel):
    event_id: str = Field(min_length=1)
    amount: float = Field(ge=0.0)
    event_type: str = Field(pattern="^(click|view|purchase)$")

# Convert to lazy Spark schema
spark_schema = pydantic_to_spark_rules(CustomerEvent)

# Use in interceptor (completely lazy!)
@validate.lazy_schema(spark_schema, entity="bronze_events")
def my_pipe(bronze_events: DataFrame): ...
```

### Comparison: Eager vs Lazy Validation

**Eager (Traditional Libraries):**
```python
# ❌ Triggers action immediately
validator.validate(df)  # Calls .collect() or .count() internally
# DataFrame computed here = 100% cost
result = df.filter(...)  # DataFrame computed AGAIN = 200% total cost
```

**Lazy (Column Expression Builder):**
```python
# ✅ Adds validation to DataFrame plan
validated_df = schema.add_validation_column(df)  # No action!
# ✅ Single computation during write
result = validated_df.filter(...)  # DataFrame + validation computed together = 100% total cost
```

### Conclusion

While you can't directly use Python validation libraries in a lazy way, you can:
1. **Build Spark-native lazy validation** using Column expression builders
2. **Mimic the API** of Python libraries (Pydantic-style)
3. **Create adapters** to convert Python library schemas to lazy Spark rules
4. **Get the best of both worlds**: Pythonic syntax + Spark lazy evaluation

This approach gives you the ergonomics of Python validation libraries with the performance of Spark's lazy execution model.

```python
class DataPipesExecuter(DataPipesExecution, SignalEmitter):
    """Enhanced with interceptor chain support."""

    @inject
    def __init__(
        self,
        lp: PythonLoggerProvider,
        dpe: DataEntityRegistry,
        dpr: DataPipesRegistry,
        erps: EntityReadPersistStrategy,
        tp: SparkTraceProvider,
        signal_provider: SignalProvider = None,
    ):
        self._init_signal_emitter(signal_provider)
        self.erps = erps
        self.dpr = dpr
        self.dpe = dpe
        self.logger = lp.get_logger("data_pipes_executer")
        self.tp = tp

        # NEW: Interceptor registry
        self._interceptors: List[TransformInterceptor] = []

    def register_interceptor(self, interceptor: TransformInterceptor):
        """Register a transform interceptor."""
        self._interceptors.append(interceptor)
        # Sort by priority
        self._interceptors.sort(key=lambda x: x.priority)
        self.logger.debug(f"Registered interceptor: {type(interceptor).__name__}")

    def _execute_datapipe(
        self,
        entity_reader: Callable[[str], DataFrame],
        activator: Callable[[DataFrame], None],
        pipe: PipeMetadata,
        run_id: str,
    ) -> bool:
        """Execute a single data pipe with interceptor chain."""

        # Build transform context
        context = TransformContext(
            pipe_id=pipe.pipeid,
            pipe_name=pipe.name,
            pipe_metadata=pipe,
            run_id=run_id,
            execution_start_time=time.time()
        )

        # Populate input DataFrames
        context.input_dataframes = self._populate_source_dict(entity_reader, pipe)

        # ========== PRE-TRANSFORM INTERCEPTORS ==========
        for interceptor in self._interceptors:
            try:
                result = interceptor.pre_transform(context)

                if result.action == InterceptAction.FAIL:
                    raise ValueError(f"Pre-transform failed: {result.message}")

                if result.action == InterceptAction.SKIP:
                    self.logger.info(f"Skipping pipe {pipe.pipeid}: {result.message}")
                    return True  # Skipped

                # Apply modified input if provided
                if result.modified_input:
                    context.input_dataframes = result.modified_input

            except Exception as e:
                self.logger.error(f"Interceptor {type(interceptor).__name__} failed: {e}")
                raise

        # ========== EXECUTE TRANSFORM ==========
        first_source = list(context.input_dataframes.values())[0]
        if first_source is None:
            return True  # Skipped - no data

        self.logger.debug(f"Executing data pipe: {pipe.pipeid}")
        context.output_dataframe = pipe.execute(**context.input_dataframes)
        context.execution_duration = time.time() - context.execution_start_time

        # ========== POST-TRANSFORM INTERCEPTORS ==========
        for interceptor in self._interceptors:
            try:
                result = interceptor.post_transform(context)

                if result.action == InterceptAction.FAIL:
                    raise ValueError(f"Post-transform failed: {result.message}")

                # Apply modified output if provided
                if result.modified_output is not None:
                    context.output_dataframe = result.modified_output

            except Exception as e:
                self.logger.error(f"Interceptor {type(interceptor).__name__} failed: {e}")
                raise

        # ========== PERSIST ==========
        activator(context.output_dataframe)

        return False  # Not skipped
```

---

## 4. Signal Catalog Extension

### New Signals for Pre/Post-Transform

| Signal | When | Key Payload Fields |
|--------|------|-------------------|
| `datapipes.pre_transform` | After pre-interceptors, before transform | pipe_id, input_row_counts, interceptors_passed |
| `datapipes.post_transform` | After transform, before post-interceptors | pipe_id, output_row_count, transform_duration |
| `datapipes.validation_passed` | All validations pass | pipe_id, rules_checked, duration |
| `datapipes.validation_failed` | Validation fails | pipe_id, failed_rules, error_details |
| `datapipes.quality_metrics` | Quality metrics calculated | pipe_id, quality_score, null_metrics |
| `datapipes.lineage_applied` | Lineage metadata added | pipe_id, package, version, layer |
| `datapipes.interceptor_error` | Interceptor fails | pipe_id, interceptor_name, error |

---

## 5. Configuration

```yaml
# settings.yaml
kindling:
  datapipes:
    interceptors:
      # Pre-transform interceptors
      validation:
        enabled: true
        fail_on_error: true
        rules:
          - type: not_null
            columns: ["id", "timestamp"]
          - type: min_rows
            min: 1

      # Post-transform interceptors
      lineage:
        enabled: true
        package_name: "my-transforms"  # Auto-detect if not specified
        include_layers:
          - bronze
          - silver
          - gold

      audit:
        enabled: true
        columns:
          - _transform_version
          - _transform_timestamp
          - _transform_pipe

      quality:
        enabled: true
        emit_signals: true
        fail_threshold: 50  # Fail if quality score < 50
```

---

## 6. Code-First Declarative API (Recommended)

### 6.1 Design Philosophy

Following Kindling's existing decorator patterns (`@DataPipes.pipe()`, `@DataEntities.entity()`), interceptors should support **code-first declaration** alongside YAML configuration.

**Benefits:**
- ✅ Consistent with existing Kindling API patterns
- ✅ Type-safe with IDE autocomplete
- ✅ Co-located with pipe definitions
- ✅ Compile-time validation via Python type hints
- ✅ Easy to test and refactor

### 6.2 Decorator-Based Interceptor Registration

```python
from kindling.interceptors import pre_transform, post_transform, validate
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, current_timestamp

# ========== Pattern 1: Decorator on Functions ==========

@DataPipes.pipe(
    pipeid="silver.cleanse_customers",
    name="Cleanse Customer Data",
    input_entity_ids=["bronze.customers"],
    output_entity_id="silver.customers",
    output_type="delta",
    tags={"layer": "silver", "domain": "customer"}
)
@validate.not_null(columns=["customer_id", "email"])  # Pre-transform validation
@validate.min_rows(min=1)  # Pre-transform validation
@post_transform.add_lineage(layer="silver")  # Post-transform
@post_transform.add_quality_metrics()  # Post-transform
def cleanse_customers(bronze_customers: DataFrame) -> DataFrame:
    """Cleanse customer data with validation and audit."""
    return (bronze_customers
        .dropDuplicates(["customer_id"])
        .filter(col("email").isNotNull())
        .withColumn("cleansed_at", current_timestamp())
    )


# ========== Pattern 2: Inline Interceptor Definitions ==========

@DataPipes.pipe(
    pipeid="silver.enrich_orders",
    name="Enrich Order Data",
    input_entity_ids=["bronze.orders", "silver.customers"],
    output_entity_id="silver.orders",
    output_type="delta",
    tags={"layer": "silver", "domain": "order"}
)
def enrich_orders(bronze_orders: DataFrame, silver_customers: DataFrame) -> DataFrame:
    """Enrich orders with customer data."""
    return bronze_orders.join(silver_customers, "customer_id", "left")

# Register custom pre-transform interceptor
@pre_transform("silver.enrich_orders", priority=10)
def validate_customer_lookup(context: TransformContext) -> InterceptorResult:
    """Ensure customer dimension is not empty before enrichment."""
    customers_df = context.input_dataframes.get("silver_customers")

    if customers_df is None or customers_df.count() == 0:
        return InterceptorResult(
            action=InterceptAction.FAIL,
            message="Customer dimension is empty - cannot enrich orders"
        )

    # Check for customer data freshness
    if "last_updated" in customers_df.columns:
        from datetime import datetime, timedelta
        latest = customers_df.agg({"last_updated": "max"}).collect()[0][0]
        if datetime.now() - latest > timedelta(hours=24):
            return InterceptorResult(
                action=InterceptAction.SKIP,
                message="Customer data is stale (>24h old)"
            )

    return InterceptorResult(action=InterceptAction.CONTINUE)

# Register custom post-transform interceptor
@post_transform("silver.enrich_orders", priority=90)
def add_enrichment_audit(context: TransformContext) -> InterceptorResult:
    """Add enrichment metadata to output."""
    df = context.output_dataframe

    # Add enrichment indicator
    df = df.withColumn("enriched_from_customer",
        col("customer_name").isNotNull()
    )

    # Add enrichment timestamp
    df = df.withColumn("enrichment_timestamp", current_timestamp())

    return InterceptorResult(
        action=InterceptAction.CONTINUE,
        modified_output=df
    )


# ========== Pattern 3: Validation Rule Classes ==========

from kindling.interceptors import ValidationRule

class BusinessHoursRule(ValidationRule):
    """Custom validation: ensure data is from business hours."""

    timestamp_column: str = "event_timestamp"

    def validate(self, df: DataFrame) -> bool:
        from pyspark.sql.functions import hour

        # Check if any records fall outside 9am-5pm
        off_hours = df.filter(
            (hour(col(self.timestamp_column)) < 9) |
            (hour(col(self.timestamp_column)) > 17)
        ).count()

        return off_hours == 0

@DataPipes.pipe(
    pipeid="gold.daily_metrics",
    name="Calculate Daily Metrics",
    input_entity_ids=["silver.transactions"],
    output_entity_id="gold.metrics",
    output_type="delta",
    tags={"layer": "gold", "domain": "analytics"}
)
@validate.custom_rule(BusinessHoursRule(
    timestamp_column="transaction_time",
    error_message="Found transactions outside business hours (9am-5pm)"
))
def calculate_daily_metrics(silver_transactions: DataFrame) -> DataFrame:
    """Calculate daily business metrics."""
    return silver_transactions.groupBy("date").agg(...)
```

### 6.3 Decorator Implementation

```python
from functools import wraps
from typing import Callable, List, Optional, Union
from dataclasses import dataclass

class InterceptorRegistry:
    """Global registry for code-first interceptor declarations."""

    _instance = None

    def __init__(self):
        # pipe_id -> List[interceptor]
        self._pre_interceptors: dict[str, List[Callable]] = {}
        self._post_interceptors: dict[str, List[Callable]] = {}
        self._pipe_validation_rules: dict[str, List[ValidationRule]] = {}

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def register_pre(self, pipe_id: str, func: Callable, priority: int):
        """Register pre-transform interceptor for a pipe."""
        if pipe_id not in self._pre_interceptors:
            self._pre_interceptors[pipe_id] = []
        self._pre_interceptors[pipe_id].append((priority, func))
        self._pre_interceptors[pipe_id].sort(key=lambda x: x[0])

    def register_post(self, pipe_id: str, func: Callable, priority: int):
        """Register post-transform interceptor for a pipe."""
        if pipe_id not in self._post_interceptors:
            self._post_interceptors[pipe_id] = []
        self._post_interceptors[pipe_id].append((priority, func))
        self._post_interceptors[pipe_id].sort(key=lambda x: x[0])

    def add_validation_rule(self, pipe_id: str, rule: ValidationRule):
        """Register validation rule for a pipe."""
        if pipe_id not in self._pipe_validation_rules:
            self._pipe_validation_rules[pipe_id] = []
        self._pipe_validation_rules[pipe_id].append(rule)

    def get_interceptors_for_pipe(self, pipe_id: str) -> tuple[List, List]:
        """Get all interceptors for a pipe."""
        pre = [func for _, func in self._pre_interceptors.get(pipe_id, [])]
        post = [func for _, func in self._post_interceptors.get(pipe_id, [])]
        return pre, post


# Decorator functions
registry = InterceptorRegistry.get_instance()

def pre_transform(pipe_id: str, priority: int = 50):
    """Decorator to register a pre-transform interceptor.

    Args:
        pipe_id: Pipe ID to attach interceptor to
        priority: Execution priority (lower = earlier)

    Example:
        @pre_transform("silver.my_pipe", priority=10)
        def validate_input(context: TransformContext) -> InterceptorResult:
            # validation logic
            return InterceptorResult(action=InterceptAction.CONTINUE)
    """
    def decorator(func: Callable):
        registry.register_pre(pipe_id, func, priority)
        return func
    return decorator

def post_transform(pipe_id: str, priority: int = 50):
    """Decorator to register a post-transform interceptor.

    Args:
        pipe_id: Pipe ID to attach interceptor to
        priority: Execution priority (lower = earlier)

    Example:
        @post_transform("silver.my_pipe", priority=90)
        def add_metadata(context: TransformContext) -> InterceptorResult:
            df = context.output_dataframe.withColumn("processed", lit(True))
            return InterceptorResult(action=InterceptAction.CONTINUE, modified_output=df)
    """
    def decorator(func: Callable):
        registry.register_post(pipe_id, func, priority)
        return func
    return decorator


# Validation decorator namespace
class validate:
    """Namespace for validation decorators."""

    @staticmethod
    def not_null(columns: Union[str, List[str]]):
        """Validate columns are not null.

        Example:
            @validate.not_null(columns=["customer_id", "email"])
            @DataPipes.pipe(...)
            def my_pipe(df): ...
        """
        def decorator(func: Callable):
            # Extract pipe_id from function's @DataPipes.pipe decorator
            pipe_id = _extract_pipe_id(func)

            cols = [columns] if isinstance(columns, str) else columns
            for col in cols:
                registry.add_validation_rule(
                    pipe_id,
                    NotNullRule(column=col, error_message=f"{col} cannot be null")
                )
            return func
        return decorator

    @staticmethod
    def min_rows(min: int):
        """Validate minimum row count.

        Example:
            @validate.min_rows(min=1)
            @DataPipes.pipe(...)
            def my_pipe(df): ...
        """
        def decorator(func: Callable):
            pipe_id = _extract_pipe_id(func)
            registry.add_validation_rule(
                pipe_id,
                MinRowCountRule(min_rows=min, error_message=f"Expected at least {min} rows")
            )
            return func
        return decorator

    @staticmethod
    def schema_matches(expected_columns: List[str]):
        """Validate schema contains expected columns.

        Example:
            @validate.schema_matches(["id", "name", "timestamp"])
            @DataPipes.pipe(...)
            def my_pipe(df): ...
        """
        def decorator(func: Callable):
            pipe_id = _extract_pipe_id(func)
            registry.add_validation_rule(
                pipe_id,
                SchemaMatchRule(
                    expected_columns=expected_columns,
                    error_message=f"Schema missing columns: {expected_columns}"
                )
            )
            return func
        return decorator

    @staticmethod
    def custom_rule(rule: ValidationRule):
        """Register custom validation rule.

        Example:
            @validate.custom_rule(MyCustomRule(...))
            @DataPipes.pipe(...)
            def my_pipe(df): ...
        """
        def decorator(func: Callable):
            pipe_id = _extract_pipe_id(func)
            registry.add_validation_rule(pipe_id, rule)
            return func
        return decorator


# Post-transform decorator namespace
class post_transform:
    """Namespace for common post-transform decorators."""

    @staticmethod
    def add_lineage(layer: str = None, package_name: str = None):
        """Add lineage metadata to output DataFrame.

        Example:
            @post_transform.add_lineage(layer="silver")
            @DataPipes.pipe(...)
            def my_pipe(df): ...
        """
        def decorator(func: Callable):
            pipe_id = _extract_pipe_id(func)

            @post_transform(pipe_id, priority=90)
            def _add_lineage(context: TransformContext) -> InterceptorResult:
                pkg_name = package_name or context.metadata.get("package_name", "kindling")
                detected_layer = layer or _detect_layer(context.pipe_id)

                interceptor = AuditLineageInterceptor(
                    package_name=pkg_name,
                    include_lineage=True,
                    include_audit=True
                )
                return interceptor.post_transform(context)

            return func
        return decorator

    @staticmethod
    def add_quality_metrics():
        """Calculate and emit quality metrics.

        Example:
            @post_transform.add_quality_metrics()
            @DataPipes.pipe(...)
            def my_pipe(df): ...
        """
        def decorator(func: Callable):
            pipe_id = _extract_pipe_id(func)

            @post_transform(pipe_id, priority=50)
            def _add_quality(context: TransformContext) -> InterceptorResult:
                interceptor = DataQualityInterceptor(GlobalInjector.get(SignalProvider))
                return interceptor.post_transform(context)

            return func
        return decorator

    @staticmethod
    def add_columns(**column_exprs: str):
        """Add columns to output DataFrame.

        Example:
            @post_transform.add_columns(
                processed_at="current_timestamp()",
                version="lit('v1.0')"
            )
            @DataPipes.pipe(...)
            def my_pipe(df): ...
        """
        def decorator(func: Callable):
            pipe_id = _extract_pipe_id(func)

            @post_transform(pipe_id, priority=80)
            def _add_columns(context: TransformContext) -> InterceptorResult:
                df = context.output_dataframe

                for col_name, expr in column_exprs.items():
                    df = df.withColumn(col_name, eval(expr))

                return InterceptorResult(
                    action=InterceptAction.CONTINUE,
                    modified_output=df
                )

            return func
        return decorator


def _extract_pipe_id(func: Callable) -> str:
    """Extract pipe_id from function decorated with @DataPipes.pipe()."""
    # Check if function has _pipe_metadata attribute set by @DataPipes.pipe()
    if hasattr(func, '_pipe_metadata'):
        return func._pipe_metadata.pipeid

    # Fallback: use function name
    return func.__name__


def _detect_layer(pipe_id: str) -> str:
    """Detect medallion layer from pipe_id."""
    if pipe_id.startswith("bronze."):
        return "bronze"
    elif pipe_id.startswith("silver."):
        return "silver"
    elif pipe_id.startswith("gold."):
        return "gold"
    return "unknown"
```

### 6.4 DataPipes Enhancement for Metadata

Update `@DataPipes.pipe()` decorator to store metadata on the function:

```python
class DataPipes:
    @classmethod
    def pipe(cls, **decorator_params):
        def decorator(func):
            if cls.dpregistry is None:
                cls.dpregistry = GlobalInjector.get(DataPipesRegistry)

            decorator_params["execute"] = func
            pipeid = decorator_params["pipeid"]

            # NEW: Store metadata on function for interceptor extraction
            func._pipe_metadata = PipeMetadata(pipeid, **decorator_params)

            cls.dpregistry.register_pipe(pipeid, **decorator_params)
            return func
        return decorator
```

### 6.5 Complete Working Example

```python
from kindling import DataPipes, DataEntities
from kindling.interceptors import (
    pre_transform, post_transform, validate,
    TransformContext, InterceptorResult, InterceptAction
)
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, current_timestamp, count, when

# ========== Define Entities ==========

@DataEntities.entity(
    entityid="bronze.customer_events",
    name="Customer Events Bronze",
    partition_columns=["date"],
    merge_columns=["event_id"],
    tags={"layer": "bronze", "domain": "customer"},
    schema=None
)

@DataEntities.entity(
    entityid="silver.customer_events",
    name="Customer Events Silver",
    partition_columns=["date"],
    merge_columns=["event_id"],
    tags={"layer": "silver", "domain": "customer"},
    schema=None
)

# ========== Bronze Pipe: Ingestion with Validation ==========

@DataPipes.pipe(
    pipeid="bronze.ingest_events",
    name="Ingest Customer Events",
    input_entity_ids=[],  # External source
    output_entity_id="bronze.customer_events",
    output_type="delta",
    tags={"layer": "bronze"}
)
@validate.not_null(columns=["event_id", "customer_id", "event_timestamp"])
@validate.min_rows(min=1)
@post_transform.add_lineage(layer="bronze")
def ingest_events() -> DataFrame:
    """Ingest raw events from external source."""
    # Read from external source
    return spark.read.format("json").load("/data/raw/events/*.json")


# ========== Silver Pipe: Cleansing with Custom Validation ==========

@DataPipes.pipe(
    pipeid="silver.cleanse_events",
    name="Cleanse Customer Events",
    input_entity_ids=["bronze.customer_events"],
    output_entity_id="silver.customer_events",
    output_type="delta",
    tags={"layer": "silver", "domain": "customer"}
)
@validate.schema_matches(["event_id", "customer_id", "event_type", "event_timestamp"])
@post_transform.add_lineage(layer="silver", package_name="customer-analytics")
@post_transform.add_quality_metrics()
def cleanse_events(bronze_customer_events: DataFrame) -> DataFrame:
    """Cleanse and standardize customer events."""
    return (bronze_customer_events
        .dropDuplicates(["event_id"])
        .filter(col("event_timestamp").isNotNull())
        .filter(col("event_type").isin(["click", "view", "purchase"]))
        .withColumn("normalized_timestamp",
            col("event_timestamp").cast("timestamp")
        )
    )

# Custom pre-transform validation
@pre_transform("silver.cleanse_events", priority=20)
def validate_time_range(context: TransformContext) -> InterceptorResult:
    """Ensure events are within valid time range (not in future)."""
    from datetime import datetime

    df = context.input_dataframes["bronze_customer_events"]

    future_events = df.filter(col("event_timestamp") > lit(datetime.now())).count()

    if future_events > 0:
        return InterceptorResult(
            action=InterceptAction.FAIL,
            message=f"Found {future_events} events with future timestamps"
        )

    return InterceptorResult(action=InterceptAction.CONTINUE)

# Custom post-transform enrichment
@post_transform("silver.cleanse_events", priority=85)
def add_data_quality_flags(context: TransformContext) -> InterceptorResult:
    """Add quality flags to cleansed data."""
    df = context.output_dataframe

    # Flag suspicious events
    df = df.withColumn(
        "is_suspicious",
        when(
            (col("event_timestamp").isNull()) |
            (col("customer_id").isNull()) |
            (col("event_type").isNull()),
            lit(True)
        ).otherwise(lit(False))
    )

    # Add quality score
    df = df.withColumn(
        "quality_score",
        when(col("is_suspicious"), lit(0.0))
        .otherwise(lit(1.0))
    )

    return InterceptorResult(
        action=InterceptAction.CONTINUE,
        modified_output=df
    )


# ========== Execute Pipeline ==========

from kindling.data_pipes import DataPipesExecuter

executer = GlobalInjector.get(DataPipesExecuter)

# All interceptors automatically execute based on decorators
executer.run_datapipes([
    "bronze.ingest_events",
    "silver.cleanse_events"
])

# Output will have:
# - Validation performed automatically
# - _bronze_lineage and _silver_lineage columns
# - _transform_version, _transform_timestamp columns
# - is_suspicious and quality_score columns
# - Quality metrics emitted via signals
```

---

## 7. Hybrid Approach: Code + YAML

Support both patterns for maximum flexibility:

```python
# Code-first (co-located with pipe)
@validate.not_null(columns=["id"])
@DataPipes.pipe(pipeid="silver.my_pipe", ...)
def my_pipe(df): ...

# OR YAML-based (centralized configuration)
# settings.yaml:
# kindling:
#   interceptors:
#     silver.my_pipe:
#       pre:
#         - type: validate.not_null
#           columns: ["id"]
```

**Precedence:** Code-first decorators > YAML configuration (decorators override YAML)

---

## 8. Usage Example

```python

---

## 9. API Comparison Summary

### Three Ways to Use Interceptors

| Approach | Use Case | Example |
|----------|----------|---------|
| **1. Decorators (Recommended)** | Per-pipe interceptors, co-located with code | `@validate.not_null(columns=["id"])` |
| **2. YAML Config** | Global/cross-cutting interceptors, environment-specific | `interceptors: {validation: {...}}` |
| **3. Programmatic** | Dynamic/runtime interceptors, testing | `executer.register_interceptor(...)` |

**Decorator Pattern Advantages:**
- ✅ Type-safe with IDE support
- ✅ Co-located with pipe definition (easy to find/maintain)
- ✅ Consistent with `@DataPipes.pipe()` / `@DataEntities.entity()` patterns
- ✅ Self-documenting code
- ✅ Compile-time validation

**YAML Pattern Advantages:**
- ✅ Centralized configuration
- ✅ Environment-specific rules (dev vs prod)
- ✅ Non-code changes don't require redeployment
- ✅ Cross-cutting concerns (apply same rule to many pipes)

**Programmatic Pattern Advantages:**
- ✅ Dynamic registration at runtime
- ✅ Conditional interceptors based on runtime state
- ✅ Testing scenarios with mock interceptors

---

## 10. Recommendation

### Implementation Phases

**Phase 1: Signal-Only Observation** (Already in proposal)
- Use existing `before_pipe` / `after_pipe` signals
- Signal handlers observe but don't modify execution
- Suitable for monitoring, logging, metrics

**Phase 2: Core Interceptor Pattern**
- Add `TransformInterceptor` base class
- Add `TransformContext` for rich data passing
- Add `InterceptorRegistry` for decorator support
- Enable pre-transform validation blocking
- Enable post-transform output modification
- **Support code-first decorators** (`@validate`, `@pre_transform`, `@post_transform`)

**Phase 3: Built-in Interceptors**
- Implement core interceptors (validation, lineage, quality)
- Add `@post_transform.add_lineage()` decorator
- Add `@validate.not_null()`, `@validate.min_rows()` decorators
- Add YAML configuration support

**Phase 4: Advanced Features** (Future)
- Async interceptors for streaming
- Interceptor composition and chaining
- Per-environment interceptor profiles
- Custom validation DSL

### Recommended Primary API

**Use decorators as the primary API:**

```python
# ✅ RECOMMENDED: Code-first decorator approach
@DataPipes.pipe(...)
@validate.not_null(columns=["id", "email"])
@post_transform.add_lineage(layer="silver")
def my_pipe(df): ...

# ✅ ALSO GOOD: Custom interceptors for complex logic
@pre_transform("my_pipe", priority=10)
def custom_validation(context):
    # complex validation logic
    pass

# ⚠️  ACCEPTABLE: YAML for cross-cutting concerns
# settings.yaml - applies to ALL silver pipes
kindling:
  interceptors:
    silver.*:
      post:
        - type: add_lineage
          layer: silver

# ⚠️  USE SPARINGLY: Programmatic registration
executer.register_interceptor(...)  # Only for testing/dynamic scenarios
```

---

## 11. Compatibility Notes

1. **Non-breaking:** Interceptors are opt-in; existing code works unchanged
2. **Signal Integration:** Interceptors can emit their own signals
3. **Testability:** Interceptors are unit-testable in isolation
4. **Composability:** Chain multiple interceptors with priority ordering
5. **Lineage Alignment:** Directly implements Part 4 of signal_dag_streaming proposal
6. **API Consistency:** Decorators match existing `@DataPipes.pipe()` pattern

---

## 12. Community Libraries for Typed Signal Systems

### Overview of Options

If you prefer **Option #2 (Registry-Style Typed Signals)** and want community library support for **Option #3 (Hybrid Approach)**, here are the leading alternatives:

### 1. **PySignal** (Typed Signals)
- **Repository**: `dgovil/PySignal`
- **Type Safety**: ✅ Full type-hint support
- **Approach**: Decorator-based, typed signal definitions
- **Status**: Active, Python 3.7+

```python
from pysignals import Signal

# Define typed signals
class PipeSignals:
    before_run = Signal(pipe_ids=list, pipe_count=int, run_id=str)
    after_run = Signal(pipe_ids=list, success_count=int, duration=float)

# Connect handlers
@PipeSignals.before_run.connect
def on_before_run(pipe_ids, pipe_count, run_id):
    print(f"Starting {pipe_count} pipes")

# Emit
PipeSignals.before_run.emit(
    pipe_ids=["pipe1", "pipe2"],
    pipe_count=2,
    run_id="abc123"
)
```

**Pros:**
- ✅ Type-safe signal definitions
- ✅ Keyword-argument validation
- ✅ Clean API similar to Option #2

**Cons:**
- ⚠️ Less mature than Blinker
- ⚠️ Smaller community

---

### 2. **Pydantic Events** (Pydantic v2+)
- **Package**: Built into Pydantic v2
- **Type Safety**: ✅✅✅ Full Pydantic validation
- **Approach**: Pydantic models as event payloads
- **Status**: Very active, part of Pydantic ecosystem

```python
from pydantic import BaseModel, Field
from pydantic.events import EventBus

# Define typed event payloads
class PipeRunPayload(BaseModel):
    pipe_ids: list[str]
    pipe_count: int = Field(gt=0)
    run_id: str

class PipeRunResultPayload(PipeRunPayload):
    success_count: int = Field(ge=0)
    duration_seconds: float = Field(gt=0.0)

# Create event bus
bus = EventBus()

# Subscribe with validation
@bus.on("datapipes.before_run")
def on_before_run(payload: PipeRunPayload):
    print(f"Starting {payload.pipe_count} pipes")

# Emit with automatic validation
bus.emit("datapipes.before_run", PipeRunPayload(
    pipe_ids=["pipe1", "pipe2"],
    pipe_count=2,
    run_id="abc123"
))
```

**Pros:**
- ✅✅✅ Full Pydantic validation (runtime + IDE)
- ✅ JSON serialization built-in
- ✅ Mature, well-maintained (Pydantic team)
- ✅ Already widely used in data engineering
- ✅ Excellent for hybrid approach (typed + string-based)

**Cons:**
- ⚠️ Requires Pydantic v2+ dependency
- ⚠️ Slightly heavier than pure signal libraries

---

### 3. **python-dispatch** (Type-hint aware)
- **Repository**: `oliverbestmann/python-dispatch`
- **Type Safety**: ✅ Type hints supported
- **Approach**: Decorator-based dispatching
- **Status**: Maintained, Python 3.6+

```python
from dispatch import Signal, receiver

# Define signal
pipe_run_signal = Signal(providing_args=["pipe_ids", "pipe_count", "run_id"])

# Connect with type hints
@receiver(pipe_run_signal)
def on_pipe_run(pipe_ids: list[str], pipe_count: int, run_id: str, **kwargs):
    print(f"Running {pipe_count} pipes")

# Emit
pipe_run_signal.send(
    sender=None,
    pipe_ids=["pipe1"],
    pipe_count=1,
    run_id="xyz"
)
```

**Pros:**
- ✅ Django-like signal pattern
- ✅ Type hints for IDE support
- ✅ Simple, lightweight

**Cons:**
- ⚠️ No runtime validation
- ⚠️ Less type-safe than Pydantic approach

---

### 4. **EventBus (typed-event-bus)**
- **Repository**: `geowurster/typed-event-bus`
- **Type Safety**: ✅✅ Full generic type support
- **Approach**: Generic-based typing
- **Status**: Active maintenance

```python
from typed_event_bus import EventBus, Event
from dataclasses import dataclass

# Define typed events
@dataclass
class PipeRunEvent(Event):
    pipe_ids: list[str]
    pipe_count: int
    run_id: str

# Create typed bus
bus = EventBus()

# Subscribe with type safety
@bus.on(PipeRunEvent)
def on_pipe_run(event: PipeRunEvent):
    print(f"Running {event.pipe_count} pipes")

# Emit with type checking
bus.emit(PipeRunEvent(
    pipe_ids=["pipe1", "pipe2"],
    pipe_count=2,
    run_id="abc"
))
```

**Pros:**
- ✅ Clean dataclass-based API
- ✅ Good IDE support
- ✅ Type-safe dispatch

**Cons:**
- ⚠️ Smaller ecosystem
- ⚠️ Less feature-rich than Pydantic

---

### 5. **Blinker + Pydantic** (Hybrid Enhancement)
- **Approach**: Wrap Blinker with Pydantic validation
- **Type Safety**: ✅✅✅ Best of both worlds
- **Status**: DIY pattern (combine two mature libraries)

```python
from blinker import Namespace
from pydantic import BaseModel, ValidationError
from typing import Type, Optional

signals = Namespace()

class TypedSignalWrapper:
    """Wrap Blinker signal with Pydantic validation."""

    def __init__(self, signal_name: str, payload_type: Type[BaseModel]):
        self.signal = signals.signal(signal_name)
        self.payload_type = payload_type

    def emit(self, sender, **kwargs):
        """Emit with validation."""
        try:
            validated = self.payload_type(**kwargs)
            return self.signal.send(sender, **validated.model_dump())
        except ValidationError as e:
            raise ValueError(f"Invalid signal payload: {e}")

    def connect(self, func):
        """Connect handler (receives validated dict)."""
        return self.signal.connect(func)

# Define typed signals
class PipeRunPayload(BaseModel):
    pipe_ids: list[str]
    pipe_count: int
    run_id: str

before_run = TypedSignalWrapper("datapipes.before_run", PipeRunPayload)

# Use it
@before_run.connect
def handler(sender, **kwargs):
    print(kwargs)  # Already validated

before_run.emit(None, pipe_ids=["p1"], pipe_count=1, run_id="xyz")
```

**Pros:**
- ✅✅✅ Keeps existing Blinker investment
- ✅✅✅ Adds Pydantic validation
- ✅ Hybrid: typed where needed, strings elsewhere
- ✅ Gradual migration path

**Cons:**
- ⚠️ Custom wrapper code to maintain
- ⚠️ Two dependencies (Blinker + Pydantic)

---

### Recommendation: **Pydantic Events** or **Blinker + Pydantic Hybrid**

For Kindling's use case, I recommend:

#### **Option A: Pydantic Events (Full Migration)**

**When to use:**
- You want full type safety throughout
- Already using Pydantic elsewhere in the stack
- Want runtime validation for all signals
- Can justify the Pydantic dependency

**Implementation effort:** Medium (requires migration from Blinker)

#### **Option B: Blinker + Pydantic Hybrid (Recommended)**

**When to use:**
- Want to keep existing Blinker code working
- Want type safety for critical signals only
- Need gradual migration path
- Want Option #3 (Hybrid) from the proposal

**Implementation effort:** Low (wrapper around existing code)

```python
# Hybrid approach - both work simultaneously
from kindling.signaling import BlinkerSignalProvider, TypedSignalWrapper

# Core framework signals: typed with Pydantic
class PipeRunPayload(BaseModel):
    pipe_ids: list[str]
    pipe_count: int
    run_id: str

before_run = TypedSignalWrapper("datapipes.before_run", PipeRunPayload)

# Extension/custom signals: flexible strings
signals = BlinkerSignalProvider()
signals.emit("custom.my_extension.event", data="flexible")
```

This gives you:
- ✅ Type safety for core framework signals
- ✅ Flexibility for extensions/plugins
- ✅ Backward compatibility
- ✅ Gradual migration path

---

### Dependency Impact

| Library | Size | Already in Kindling? | Breaking Change? |
|---------|------|---------------------|------------------|
| Blinker | ~50KB | ✅ Yes | No |
| Pydantic v2 | ~2MB | ❌ No | No (opt-in) |
| PySignal | ~30KB | ❌ No | Yes (replace Blinker) |
| python-dispatch | ~20KB | ❌ No | Yes (replace Blinker) |
| typed-event-bus | ~40KB | ❌ No | Yes (replace Blinker) |

**Conclusion:** The **Blinker + Pydantic hybrid** approach offers the best of both worlds:
- Minimal disruption (keeps Blinker)
- Type safety where you want it (add Pydantic wrapper)
- Supports Option #3 from the proposal perfectly

---

## 13. Conclusion

The pre-transform and post-transform requirements are **fully congruent** with the signal_dag_streaming proposal:

| Requirement | Proposal Support | Gap/Extension |
|-------------|------------------|---------------|
| Pre-transform data validation | `before_pipe` signal | Add interceptor pattern for blocking |
| Post-transform audit columns | Part 4: Lineage Tracking | Already designed |
| Post-transform version upsert | Part 4: `add_lineage_column()` | Already designed |
| Pre-transform schema checks | `before_pipe` signal | Add validation rules |
| Post-transform quality scoring | `after_pipe` signal | Add quality interceptor |
| Execution flow control | Not in proposal | Add `InterceptAction` enum |
| DataFrame access in hooks | Not in proposal | Add `TransformContext` |

The interceptor pattern proposed here is a **natural extension** of the signal architecture, providing:
- **Observation:** Via signals (already proposed)
- **Interception:** Via priority-ordered interceptor chain (new)
- **Modification:** Via `InterceptorResult` with modified DataFrames (new)

This creates a powerful, composable system for pre/post-transform activities without disrupting the core signal-based design.

---

*Document Version: 1.0*
*Date: February 2, 2026*
*Author: Kindling Framework Team*
