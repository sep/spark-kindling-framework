"""Unit tests for the Phase-2 OSS emission engine, guard provider, and
dry-run harness.

``pyspark.pipelines`` requires Spark 4.1+ and this repo pins pyspark <4.0,
so emission is tested against a recording fake of the ``dp`` module — which
is also the designed seam (``dp_module`` is an injected dependency of
``OssSdpEngine``). The lazy-import error path is REAL here, not simulated:
on this runtime the import genuinely fails, which is exactly the diagnostic
being asserted.
"""

import os
import stat
import sys
from types import SimpleNamespace

import pytest
from kindling.data_entities import EntityMetadata
from kindling.data_pipes import PipeMetadata
from kindling_sdp import (
    DatasetType,
    OssSdpEngine,
    SdpModeWriteError,
    SdpRuntimeUnavailableError,
    SdpWriteGuardProvider,
    SparkPipelinesCliNotFoundError,
    dry_run,
    write_pipeline_spec,
)

# --------------------------------------------------------------------- #
# Fixtures: fake registries (same graph as test_sdp_declaration_engine) #
# --------------------------------------------------------------------- #


class FakeEntityRegistry:
    def __init__(self, entities):
        self.registry = {entity.entityid: entity for entity in entities}

    def get_entity_ids(self):
        return self.registry.keys()

    def get_entity_definition(self, name):
        return self.registry.get(name)


class FakePipeRegistry:
    def __init__(self, pipes):
        self.registry = {pipe.pipeid: pipe for pipe in pipes}

    def get_pipe_ids(self):
        return self.registry.keys()

    def get_pipe_definition(self, name):
        return self.registry.get(name)


def make_entity(entityid, tags=None, **overrides):
    params = dict(
        entityid=entityid,
        name=entityid.split(".")[-1],
        merge_columns=["id"],
        tags=tags or {},
        schema=None,
    )
    params.update(overrides)
    return EntityMetadata(**params)


def make_pipe(pipeid, input_entity_ids, output_entity_id, execute=None, **overrides):
    params = dict(
        pipeid=pipeid,
        name=pipeid,
        execute=execute or (lambda **dfs: None),
        tags={},
        input_entity_ids=input_entity_ids,
        output_entity_id=output_entity_id,
        output_type="delta",
    )
    params.update(overrides)
    return PipeMetadata(**params)


# --------------------------------------------------------------------- #
# Recording fakes for the dp module and the Spark session                #
# --------------------------------------------------------------------- #


class FakeDpModule:
    """Records materialized_view declarations the way pyspark.pipelines
    would receive them."""

    def __init__(self):
        self.declared = {}  # name -> dataset function

    def materialized_view(self, name=None):
        def decorator(fn):
            self.declared[name or fn.__name__] = fn
            return fn

        return decorator


class FakeSession:
    """Records spark.table() reads; returns a distinct token per table."""

    def __init__(self):
        self.reads = []

    def table(self, name):
        self.reads.append(name)
        return f"df:{name}"


@pytest.fixture
def graph():
    entities = [
        make_entity("landing.orders"),
        make_entity("ref.customers"),
        make_entity("bronze.orders"),
        make_entity("silver.orders"),
    ]
    captured = {}

    def silver_execute(**dfs):
        captured.update(dfs)
        return "df:silver.result"

    pipes = [
        make_pipe("ingest.orders", ["landing.orders"], "bronze.orders"),
        make_pipe(
            "bronze_to_silver.orders",
            ["bronze.orders", "ref.customers"],
            "silver.orders",
            execute=silver_execute,
        ),
    ]
    return SimpleNamespace(
        entity_registry=FakeEntityRegistry(entities),
        pipe_registry=FakePipeRegistry(pipes),
        captured=captured,
    )


def make_engine(graph, **kwargs):
    kwargs.setdefault("dp_module", FakeDpModule())
    kwargs.setdefault("session_provider", FakeSession)
    return OssSdpEngine(graph.entity_registry, graph.pipe_registry, **kwargs)


# --------------------------------------------------------------------- #
# Emission                                                               #
# --------------------------------------------------------------------- #


class TestEmission:
    def test_declares_one_materialized_view_per_dataset(self, graph):
        dp = FakeDpModule()
        engine = make_engine(graph, dp_module=dp)

        engine.declare_pipeline(engine.build_plan())

        assert set(dp.declared) == {"bronze.orders", "silver.orders"}

    def test_dataset_function_rebuilds_the_runner_kwarg_contract(self, graph):
        """Inputs arrive as entity ids with dots -> underscores, in
        input_entity_ids order — identical to generation_executor."""
        dp = FakeDpModule()
        session = FakeSession()
        engine = make_engine(graph, dp_module=dp, session_provider=lambda: session)
        engine.declare_pipeline(engine.build_plan())

        result = dp.declared["silver.orders"]()

        assert result == "df:silver.result"
        assert list(graph.captured) == ["bronze_orders", "ref_customers"]
        assert graph.captured["bronze_orders"] == "df:bronze.orders"

    def test_internal_and_external_inputs_read_by_table_name(self, graph):
        """spark.table(<name>) for both: internal so SDP infers the edge,
        external as the default catalog-table read."""
        dp = FakeDpModule()
        session = FakeSession()
        engine = make_engine(graph, dp_module=dp, session_provider=lambda: session)
        engine.declare_pipeline(engine.build_plan())

        dp.declared["silver.orders"]()

        assert session.reads == ["bronze.orders", "ref.customers"]

    def test_external_read_resolver_overrides_external_reads_only(self, graph):
        dp = FakeDpModule()
        session = FakeSession()
        resolved = []

        def resolver(spark, entity_id):
            resolved.append(entity_id)
            return f"resolved:{entity_id}"

        engine = make_engine(
            graph,
            dp_module=dp,
            session_provider=lambda: session,
            external_read_resolver=resolver,
        )
        engine.declare_pipeline(engine.build_plan())

        dp.declared["silver.orders"]()

        assert resolved == ["ref.customers"], "internal inputs must not hit the resolver"
        assert session.reads == ["bronze.orders"]
        assert graph.captured["ref_customers"] == "resolved:ref.customers"

    def test_streaming_table_fails_fast_as_phase4(self, graph):
        graph.entity_registry.registry["silver.orders"] = make_entity(
            "silver.orders", tags={"sdp.dataset_type": "streaming_table"}
        )
        engine = make_engine(graph)

        with pytest.raises(NotImplementedError, match="Phase 4"):
            engine.declare_pipeline(engine.build_plan())

    def test_missing_pyspark_pipelines_raises_actionable_error(self, graph):
        """Real on this runtime: pyspark <4.0 has no pipelines module."""
        engine = OssSdpEngine(graph.entity_registry, graph.pipe_registry)

        with pytest.raises(SdpRuntimeUnavailableError, match="Spark 4.1"):
            engine.declare_pipeline(engine.build_plan())


# --------------------------------------------------------------------- #
# Write-guard provider                                                   #
# --------------------------------------------------------------------- #


class FakeProvider:
    def read_entity(self, entity):
        return f"read:{entity.entityid}"

    def check_entity_exists(self, entity):
        return True

    def write_to_entity(self, df, entity):  # pragma: no cover - must never run
        raise AssertionError("guard failed to intercept")


class TestWriteGuard:
    def setup_method(self):
        self.entity = make_entity("silver.orders")
        self.guard = SdpWriteGuardProvider(FakeProvider())

    def test_reads_delegate_unchanged(self):
        assert self.guard.read_entity(self.entity) == "read:silver.orders"
        assert self.guard.check_entity_exists(self.entity) is True

    @pytest.mark.parametrize(
        "method",
        [
            "write_to_entity",
            "append_to_entity",
            "merge_to_entity",
            "append_as_stream",
            "ensure_destination",
            "ensure_entity_table",
        ],
    )
    def test_every_write_path_raises_naming_the_entity(self, method):
        with pytest.raises(SdpModeWriteError, match="silver.orders"):
            getattr(self.guard, method)("df", self.entity)

    def test_write_raises_even_when_inner_lacks_the_method(self):
        """Fail-fast is a property of the mode, not of the wrapped class."""
        with pytest.raises(SdpModeWriteError):
            SdpWriteGuardProvider(object()).merge_to_entity("df", self.entity)

    def test_registry_decorator_hook_wraps_instances(self):
        from unittest.mock import MagicMock

        from kindling.entity_provider_registry import EntityProviderRegistry

        registry = EntityProviderRegistry.__new__(EntityProviderRegistry)
        registry.logger = MagicMock()
        registry._provider_classes = {}
        registry._provider_instances = {"delta": FakeProvider()}
        registry._provider_decorator = None

        registry.set_provider_decorator(SdpWriteGuardProvider)

        guarded = registry._provider_instances["delta"]
        assert isinstance(guarded, SdpWriteGuardProvider)
        with pytest.raises(SdpModeWriteError):
            guarded.write_to_entity("df", self.entity)


# --------------------------------------------------------------------- #
# Dry-run harness                                                        #
# --------------------------------------------------------------------- #


class TestDryRunHarness:
    def test_spec_contains_required_fields(self, tmp_path):
        spec_path = write_pipeline_spec(
            tmp_path,
            name="orders_pipeline",
            definitions_globs=["definitions/**"],
            database="silver",
        )

        text = spec_path.read_text()
        assert spec_path.name == "spark-pipeline.yml"
        assert "name: orders_pipeline" in text
        assert "include: definitions/**" in text
        assert "storage: " in text, "storage is required by the spec schema"
        assert "database: silver" in text
        assert "catalog:" not in text

    def _stub_cli(self, tmp_path, exit_code):
        stub = tmp_path / "spark-pipelines"
        stub.write_text(
            "#!/bin/sh\n" 'echo "args: $@"\n' f'echo "diagnostic" >&2\nexit {exit_code}\n'
        )
        stub.chmod(stub.stat().st_mode | stat.S_IEXEC)
        return str(stub)

    def test_dry_run_invokes_cli_with_spec(self, tmp_path):
        spec = write_pipeline_spec(tmp_path, "p", ["defs/**"])

        result = dry_run(spec, executable=self._stub_cli(tmp_path, exit_code=0))

        assert result.ok is True
        assert f"args: dry-run --spec {spec}" in result.stdout

    def test_failed_validation_is_a_result_not_an_exception(self, tmp_path):
        spec = write_pipeline_spec(tmp_path, "p", ["defs/**"])

        result = dry_run(spec, executable=self._stub_cli(tmp_path, exit_code=1))

        assert result.ok is False
        assert result.returncode == 1
        assert "diagnostic" in result.stderr

    def test_missing_cli_raises_with_install_guidance(self, tmp_path):
        spec = write_pipeline_spec(tmp_path, "p", ["defs/**"])

        with pytest.raises(SparkPipelinesCliNotFoundError, match="pyspark\\[pipelines\\]"):
            dry_run(spec, executable="definitely-not-a-real-cli")

    def test_wildcard_glob_not_ending_in_folder_star_star_is_rejected(self, tmp_path):
        """The real CLI rejects *.py patterns (PIPELINE_SPEC_INVALID_GLOB
        _PATTERN); the harness fails earlier with the same rule."""
        with pytest.raises(ValueError, match="definitions/\\*\\*"):
            write_pipeline_spec(tmp_path, "p", ["definitions/*.py"])

    def test_literal_file_path_glob_is_allowed(self, tmp_path):
        spec = write_pipeline_spec(tmp_path, "p", ["definitions/pipeline_defs.py"])

        assert "include: definitions/pipeline_defs.py" in spec.read_text()

    def test_extra_env_reaches_the_cli(self, tmp_path):
        stub = tmp_path / "spark-pipelines"
        stub.write_text('#!/bin/sh\necho "marker=$KINDLING_TEST_MARKER"\nexit 0\n')
        stub.chmod(stub.stat().st_mode | stat.S_IEXEC)
        spec = write_pipeline_spec(tmp_path, "p", ["defs/**"])

        result = dry_run(spec, executable=str(stub), extra_env={"KINDLING_TEST_MARKER": "on"})

        assert "marker=on" in result.stdout

    def test_spark_env_derived_from_venv_installed_cli(self, tmp_path):
        """A CLI inside a venv with pyspark gets SPARK_HOME and
        PYSPARK_PYTHON pointed at that venv — without this, the launcher
        picks up an ambient SPARK_HOME or the system python3 and fails."""
        from kindling_sdp.dry_run import spark_env_for_executable

        venv_root = tmp_path / "venv"
        (venv_root / "bin").mkdir(parents=True)
        (venv_root / "bin" / "python").touch()
        cli = venv_root / "bin" / "spark-pipelines"
        cli.touch()
        pyspark_dir = venv_root / "lib" / "python3.11" / "site-packages" / "pyspark"
        pyspark_dir.mkdir(parents=True)

        env = spark_env_for_executable(cli)

        assert env["SPARK_HOME"] == str(pyspark_dir)
        assert env["PYSPARK_PYTHON"] == str(venv_root / "bin" / "python")
        assert env["PYSPARK_DRIVER_PYTHON"] == str(venv_root / "bin" / "python")

    def test_no_spark_env_derived_for_bare_executable(self, tmp_path):
        """A stub or PATH-wide install (no sibling python/pyspark) needs no
        overrides."""
        from kindling_sdp.dry_run import spark_env_for_executable

        stub = tmp_path / "spark-pipelines"
        stub.touch()

        assert spark_env_for_executable(stub) == {}
