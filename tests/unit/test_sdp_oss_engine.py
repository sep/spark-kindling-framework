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
from kindling_ext_sdp import (
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

        assert set(dp.declared) == {"bronze_orders", "silver_orders"}

    def test_dataset_function_rebuilds_the_runner_kwarg_contract(self, graph):
        """Inputs arrive as entity ids with dots -> underscores, in
        input_entity_ids order — identical to generation_executor."""
        dp = FakeDpModule()
        session = FakeSession()
        engine = make_engine(graph, dp_module=dp, session_provider=lambda: session)
        engine.declare_pipeline(engine.build_plan())

        result = dp.declared["silver_orders"]()

        assert result == "df:silver.result"
        assert list(graph.captured) == ["bronze_orders", "ref_customers"]
        assert graph.captured["bronze_orders"] == "df:bronze_orders"

    def test_internal_and_external_inputs_read_by_table_name(self, graph):
        """spark.table(<name>) for both: internal so SDP infers the edge,
        external as the default catalog-table read."""
        dp = FakeDpModule()
        session = FakeSession()
        engine = make_engine(graph, dp_module=dp, session_provider=lambda: session)
        engine.declare_pipeline(engine.build_plan())

        dp.declared["silver_orders"]()

        # Internal inputs read the emitted single-part dataset name;
        # external inputs keep the entity id (resolver contract).
        assert session.reads == ["bronze_orders", "ref.customers"]

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

        dp.declared["silver_orders"]()

        assert resolved == ["ref.customers"], "internal inputs must not hit the resolver"
        assert session.reads == ["bronze_orders"]
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

    def _make_registry(self):
        from unittest.mock import MagicMock

        from kindling.entity_provider_registry import EntityProviderRegistry

        registry = EntityProviderRegistry.__new__(EntityProviderRegistry)
        registry.logger = MagicMock()
        registry._provider_classes = {}
        registry._provider_instances = {"delta": FakeProvider()}
        registry._provider_decorator = None
        return registry

    def test_reinstalling_same_decorator_never_double_wraps(self):
        registry = self._make_registry()
        registry.set_provider_decorator(SdpWriteGuardProvider)

        registry.set_provider_decorator(SdpWriteGuardProvider)

        guarded = registry._provider_instances["delta"]
        assert isinstance(guarded, SdpWriteGuardProvider)
        assert isinstance(guarded._inner, FakeProvider), "must not nest guards"

    def test_installing_a_different_decorator_is_a_mode_conflict(self):
        registry = self._make_registry()
        registry.set_provider_decorator(SdpWriteGuardProvider)

        with pytest.raises(ValueError, match="already installed"):
            registry.set_provider_decorator(lambda p: p)


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
        from kindling_ext_sdp.dry_run import spark_env_for_executable

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
        from kindling_ext_sdp.dry_run import spark_env_for_executable

        stub = tmp_path / "spark-pipelines"
        stub.touch()

        assert spark_env_for_executable(stub) == {}


@pytest.mark.parametrize("engine_name", ["sdp", "databricks_sdp"])
@pytest.mark.parametrize(
    "mode, expected", [("normalized", "silver_device_telemetry"), ("leaf", "device_telemetry")]
)
def test_configured_dataset_names_and_internal_reads(engine_name, mode, expected):
    from kindling_ext_databricks import DatabricksSdpEngine

    entities = FakeEntityRegistry(
        [make_entity("silver.device_telemetry"), make_entity("gold.shower_sessions")]
    )
    captured = {}
    pipes = FakePipeRegistry(
        [
            make_pipe("telemetry", [], "silver.device_telemetry"),
            make_pipe(
                "sessions",
                ["silver.device_telemetry"],
                "gold.shower_sessions",
                execute=lambda **dfs: captured.update(dfs),
            ),
        ]
    )
    dp = FakeDpModule()
    spark = FakeSession()
    engine_class = OssSdpEngine if engine_name == "sdp" else DatabricksSdpEngine
    engine = engine_class(
        entities, pipes, dp_module=dp, session_provider=lambda: spark, dataset_naming=mode
    )
    plan = engine.build_plan()
    engine.declare_pipeline(plan)
    assert plan.datasets[0].name == "silver.device_telemetry"
    assert expected in dp.declared
    assert dp.declared[expected].__name__ == expected
    sessions = "shower_sessions" if mode == "leaf" else "gold_shower_sessions"
    dp.declared[sessions]()
    assert spark.reads == [expected]
    assert captured == {"silver_device_telemetry": f"df:{expected}"}


def test_leaf_names_are_scoped_to_selected_pipeline_resource(graph):
    from kindling_ext_databricks import DatabricksSdpEngine

    # Both resources share the registries; selection defines the naming scope.
    for pipe_id in graph.pipe_registry.get_pipe_ids():
        dp = FakeDpModule()
        external_reads = []
        engine = DatabricksSdpEngine(
            graph.entity_registry,
            graph.pipe_registry,
            dp_module=dp,
            dataset_naming="leaf",
            session_provider=FakeSession,
            external_read_resolver=lambda spark, entity_id: external_reads.append(entity_id),
        )
        plan = engine.build_plan([pipe_id])
        engine.declare_pipeline(plan)
        assert list(dp.declared) == ["orders"]
        dp.declared["orders"]()
        assert external_reads == list(
            graph.pipe_registry.get_pipe_definition(pipe_id).input_entity_ids
        )


def test_duplicate_leaf_names_fail_before_emission(graph):
    from kindling_ext_sdp import DeclarationValidationError

    engine = make_engine(graph, dataset_naming="leaf")
    with pytest.raises(DeclarationValidationError) as exc:
        engine.build_plan()
    message = str(exc.value)
    assert "duplicate_dataset_name" in message
    assert "bronze.orders" in message
    assert "silver.orders" in message
    assert "emitted dataset name 'orders'" in message
    assert "separate pipelines" in message
    assert engine._dp_module.declared == {}


def test_default_name_helper_is_backward_compatible():
    from kindling_ext_sdp.declaration_plan import (
        DatasetNameMapper,
        pipeline_dataset_name,
    )

    assert pipeline_dataset_name("silver.device_telemetry") == "silver_device_telemetry"
    assert pipeline_dataset_name("silver.device-telemetry") == "silver_device_telemetry"
    assert DatasetNameMapper("legacy").mode == "normalized"
    assert DatasetNameMapper("legacy")("silver.device-telemetry") == "silver_device_telemetry"


def test_invalid_dataset_naming_fails_clearly(graph):
    from kindling_ext_sdp import DeclarationValidationError

    engine = make_engine(graph, dataset_naming="typo")
    with pytest.raises(DeclarationValidationError, match="kindling.sdp.dataset_naming.*'typo'"):
        engine.build_plan()

    codes = {issue.code for issue in engine.validate(["missing"])}
    assert codes == {"invalid_dataset_naming", "unknown_pipe"}


def test_default_normalization_collisions_are_rejected():
    from kindling_ext_sdp import DeclarationValidationError

    entities = FakeEntityRegistry([make_entity("a.b"), make_entity("a_b")])
    pipes = FakePipeRegistry([make_pipe("one", [], "a.b"), make_pipe("two", [], "a_b")])
    with pytest.raises(DeclarationValidationError, match="duplicate_dataset_name"):
        OssSdpEngine(entities, pipes).build_plan()


def test_pipeline_local_name_stays_single_part_with_multipart_external_override(graph):
    graph.entity_registry.registry["silver.orders"] = make_entity(
        "silver.orders",
        tags={"provider.table_name": "dev_silver.cwmdp.orders"},
    )
    dp = FakeDpModule()
    engine = make_engine(graph, dp_module=dp, dataset_naming="leaf")

    dataset = engine.build_plan(["bronze_to_silver.orders"]).get_dataset("silver.orders")

    assert engine._declaration_kwargs(dataset)["name"] == "orders"
    assert "." not in engine._declaration_kwargs(dataset)["name"]


@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.parametrize("explicit_name", [None, "dev_bronze.cwmdp.device_telemetry"])
def test_leaf_naming_preserves_external_entity_name_mapper_catalog(streaming, explicit_name):
    from unittest.mock import MagicMock

    from kindling.entity_resolution import ConfigDrivenEntityNameMapper

    tags = {"provider.table_catalog": "dev_bronze"}
    if explicit_name:
        tags["provider.table_name"] = explicit_name
    external = make_entity("bronze.device_telemetry", tags=tags)
    entities = FakeEntityRegistry([external, make_entity("silver.device_telemetry")])
    pipes = FakePipeRegistry(
        [make_pipe("clean", ["bronze.device_telemetry"], "silver.device_telemetry")]
    )
    config = MagicMock()
    config.get.return_value = None
    mapper = ConfigDrivenEntityNameMapper(config, MagicMock())
    spark = MagicMock()

    def external_read(session, entity_id):
        return session.table(mapper.get_table_name(entities.get_entity_definition(entity_id)))

    def external_stream_read(session, entity_id):
        return session.readStream.table(
            mapper.get_table_name(entities.get_entity_definition(entity_id))
        )

    engine = OssSdpEngine(
        entities,
        pipes,
        dataset_naming="leaf",
        session_provider=lambda: spark,
        external_read_resolver=external_read,
        external_stream_read_resolver=external_stream_read,
    )
    dataset = engine.build_plan().datasets[0]
    assert engine._declaration_kwargs(dataset)["name"] == "device_telemetry"
    engine._build_dataset_function(dataset, stream_first_input=streaming)()
    reader = spark.readStream.table if streaming else spark.table
    reader.assert_called_once_with(explicit_name or "dev_bronze.bronze.device_telemetry")


@pytest.mark.parametrize("mode", ["normalized", "leaf"])
def test_dataset_collision_names_are_case_insensitive(mode):
    from kindling_ext_sdp import DeclarationValidationError

    entities = FakeEntityRegistry([make_entity("silver.Orders"), make_entity("silver.orders")])
    pipes = FakePipeRegistry(
        [make_pipe("one", [], "silver.Orders"), make_pipe("two", [], "silver.orders")]
    )
    with pytest.raises(DeclarationValidationError, match="duplicate_dataset_name"):
        OssSdpEngine(entities, pipes, dataset_naming=mode).build_plan()
