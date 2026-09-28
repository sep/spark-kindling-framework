"""Tests for ``kindling bundle build`` (kindling_cli.bundle).

The generator is Spark-free and never imports app code; these tests build
bundles from the checked-in ``examples/lakeflow-telemetry`` project and from
small synthetic projects, then prove the inline configuration resolves to
exactly what the hand-written, file-based example resolves to.
"""

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest
import yaml
from click.testing import CliRunner
from kindling_cli import bundle
from kindling_cli.cli import cli
from kindling_ext_databricks import lakeflow_app_selector as selector


class FakeConf:
    def __init__(self, values=None):
        self.values = dict(values or {})

    def get(self, key, default=None):
        return self.values.get(key, default)

    def getAll(self):
        return dict(self.values)


class FakeSpark:
    def __init__(self, values=None):
        self.conf = FakeConf(values)


class PointLookupConf:
    """Serverless surface: get() only, no enumeration."""

    def __init__(self, values):
        self.values = dict(values)

    def get(self, key, default=None):
        return self.values.get(key, default)


class PointLookupSpark:
    def __init__(self, values):
        self.conf = PointLookupConf(values)


def _repo_root() -> Path:
    return Path(__file__).parents[2]


def _example_root() -> Path:
    return _repo_root() / "examples" / "lakeflow-telemetry"


EXAMPLE_HOST = "https://adb-lakeflow-telemetry.azuredatabricks.net"
EXAMPLE_APP_OPTIONS = {
    "telemetry": {
        "pipelines": {
            "bronze": {
                "catalog": "dev_bronze",
                "schema": "cwmdp",
                "pipes": ["bronze.ingest_telemetry"],
            },
            "silver": {
                "catalog": "dev_silver",
                "schema": "cwmdp",
                "pipes": [
                    "silver.build_telemetry",
                    "silver.derive_events",
                    "silver.derive_episodes",
                ],
            },
        }
    }
}


def _example_inputs(**overrides) -> bundle.BundleInputs:
    values = {
        "name": "lakeflow-telemetry",
        "target": "dev",
        "apps": ("telemetry",),
        "workspace_host": EXAMPLE_HOST,
        "workspace_root": "/Workspace/Shared/kindling/lakeflow-telemetry/dev",
        "runtime_env": "dev",
        "workspace_id": "adb-lakeflow-telemetry",
        "app_options": bundle._validate_app_options(EXAMPLE_APP_OPTIONS, ["telemetry"]),
        "dependencies": ("spark-kindling-ext-databricks==0.2.0",),
    }
    values.update(overrides)
    return bundle.BundleInputs(**values)


def _build_example(tmp_path: Path, **overrides) -> bundle.BuildResult:
    return bundle.build_bundle(
        _example_inputs(**overrides), project_root=_example_root(), output_dir=tmp_path / "out"
    )


def _resource(result: bundle.BuildResult, key: str) -> dict:
    path = result.output_dir / "resources" / f"{key}.pipeline.yml"
    return yaml.safe_load(path.read_text(encoding="utf-8"))["resources"]["pipelines"][key]


def _resolve_through_runtime(configuration: dict, spark_factory=FakeSpark) -> dict:
    """Bridge a pipeline configuration exactly as the selector does, then load
    it through the shared DynaconfConfig path and read back resolved values."""
    from kindling.spark_config import DynaconfConfig

    initial = selector._pipeline_config_for_kindling(spark_factory(configuration), "telemetry")
    service = DynaconfConfig()
    with patch(
        "kindling.spark_config.get_or_create_spark_session",
        return_value=SimpleNamespace(conf=FakeConf()),
    ):
        service.initialize(
            config_files=initial.get("config_files"),
            initial_config=initial,
            environment=initial.get("environment", "development"),
        )
    dataentities = service.get("dataentities")
    bytag = service.get("dataentities-bytag")
    return {
        "platform": service.get("kindling.platform.environment"),
        "dataset_naming": service.get("kindling.sdp.dataset_naming"),
        "logging_level": service.get("kindling.telemetry.logging.level"),
        "table_schema": service.get("kindling.storage.table_schema"),
        "events_table": dataentities["silver.events"]["tags"]["provider.table_name"],
        "bronze_retention": bytag["tier"]["bronze"]["tags"]["retention.days"],
        "pipes": service.get("kindling.lakeflow.pipes"),
        "data_app": service.get("kindling.data_app"),
    }


def _example_source_files() -> list:
    """The example's settings files in the runtime's overlay order."""
    root = _example_root()
    files = [
        root / "config" / "settings.yaml",
        root / "config" / "settings.databricks.yaml",
        root / "config" / "workspace_adb-lakeflow-telemetry.yaml",
        root / "config" / "settings.dev.yaml",
        root / "data-apps" / "telemetry" / "settings.yaml",
    ]
    return [str(path) for path in files]


def _resolve_files_through_dynaconf(files: list) -> dict:
    """Layer the same source files with Dynaconf itself (the runtime's loader)."""
    from kindling.spark_config import DynaconfConfig

    service = DynaconfConfig()
    with patch(
        "kindling.spark_config.get_or_create_spark_session",
        return_value=SimpleNamespace(conf=FakeConf()),
    ):
        service.initialize(config_files=files, initial_config={}, environment="dev")
    dataentities = service.get("dataentities")
    bytag = service.get("dataentities-bytag")
    return {
        "platform": service.get("kindling.platform.environment"),
        "dataset_naming": service.get("kindling.sdp.dataset_naming"),
        "logging_level": service.get("kindling.telemetry.logging.level"),
        "table_schema": service.get("kindling.storage.table_schema"),
        "events_table": dataentities["silver.events"]["tags"]["provider.table_name"],
        "bronze_retention": bytag["tier"]["bronze"]["tags"]["retention.days"],
    }


# --------------------------------------------------------------------------- #
# Generation from the example project
# --------------------------------------------------------------------------- #


def test_example_project_builds_expected_files(tmp_path):
    result = _build_example(tmp_path)

    assert result.files == [
        "databricks.yml",
        "manifest.json",
        "resources/telemetry_bronze.pipeline.yml",
        "resources/telemetry_silver.pipeline.yml",
        "src/kindling_lakeflow.py",
    ]
    root = yaml.safe_load((result.output_dir / "databricks.yml").read_text(encoding="utf-8"))
    assert root["bundle"] == {"name": "lakeflow-telemetry"}
    assert root["include"] == ["resources/*.pipeline.yml"]
    assert root["sync"] == {"include": ["src/**"]}
    assert root["targets"]["dev"]["workspace"] == {
        "host": EXAMPLE_HOST,
        "root_path": "/Workspace/Shared/kindling/lakeflow-telemetry/dev",
    }
    assert "run_as" not in root["targets"]["dev"]

    source = (result.output_dir / "src" / "kindling_lakeflow.py").read_text(encoding="utf-8")
    assert "declare_from_pipeline_config()" in source
    assert result.warnings == []


def test_example_pipeline_resource_shape(tmp_path):
    result = _build_example(tmp_path)
    silver = _resource(result, "telemetry_silver")

    assert silver["name"] == "telemetry-silver"
    assert silver["serverless"] is True
    assert silver["catalog"] == "dev_silver"
    assert silver["target"] == "cwmdp"
    assert silver["continuous"] is False
    assert silver["libraries"] == [{"file": {"path": "../src/kindling_lakeflow.py"}}]
    assert silver["environment"] == {"dependencies": ["spark-kindling-ext-databricks==0.2.0"]}
    configuration = silver["configuration"]
    assert configuration["kindling.data_app"] == "telemetry"
    assert configuration["kindling.lakeflow.pipes"] == (
        "silver.build_telemetry,silver.derive_events,silver.derive_episodes"
    )
    assert configuration["spark.kindling.bootstrap.environment"] == "dev"
    assert configuration["spark.kindling.bootstrap.workspace_id"] == "adb-lakeflow-telemetry"
    assert "spark.kindling.bootstrap.config_files" not in configuration
    assert "kindling.lakeflow.allowed_apps" not in configuration
    assert all(isinstance(value, str) for value in configuration.values())
    # Every emitted key the selector does not point-look-up by default is named.
    assert configuration["kindling.lakeflow.config_keys"] == (
        "spark.kindling.bootstrap.environment,spark.kindling.bootstrap.workspace_id"
    )


def test_inline_settings_preserve_structured_ids_and_overlay_order(tmp_path):
    result = _build_example(tmp_path)
    settings = json.loads(
        _resource(result, "telemetry_bronze")["configuration"][selector.SETTINGS_JSON_CONFIG_KEY]
    )

    # App overlay wins over environment, workspace, platform and base.
    assert settings["kindling"]["storage"]["table_schema"] == "cwmdp"
    assert settings["kindling"]["telemetry"]["logging"]["level"] == "CRITICAL"
    assert settings["kindling"]["sdp"]["dataset_naming"] == "leaf"
    # Workspace overlay wins over base for keys the app does not touch.
    assert settings["dataentities-bytag"]["tier"]["bronze"]["tags"]["retention.days"] == "5"
    # Dotted entity ids and tag keys are literal JSON keys, never split.
    assert settings["dataentities"]["silver.events"]["tags"]["provider.table_name"] == (
        "dev_silver.cwmdp.events"
    )
    assert settings["datapipes"]["silver.derive_episodes"]["output_type"] == "delta"


def test_inline_settings_match_dynaconf_layering_of_the_source_files(tmp_path):
    """The build-time merge reproduces what Dynaconf produces when it layers
    the same files itself, so inlining loses nothing versus file loading."""
    result = _build_example(tmp_path)

    inline = _resolve_through_runtime(_resource(result, "telemetry_silver")["configuration"])
    files = _resolve_files_through_dynaconf(_example_source_files())

    assert {key: inline[key] for key in files} == files
    assert inline["platform"] == "databricks"
    assert inline["dataset_naming"] == "leaf"
    assert inline["logging_level"] == "CRITICAL"
    assert inline["table_schema"] == "cwmdp"
    assert inline["events_table"] == "dev_silver.cwmdp.events"
    assert inline["bronze_retention"] == "5"


def test_generated_configuration_is_fully_readable_on_a_restricted_runtime(tmp_path):
    """Serverless exposes point lookups only; every key the generator writes
    must still reach Kindling, including the inline settings sections."""
    result = _build_example(tmp_path)
    configuration = _resource(result, "telemetry_bronze")["configuration"]

    resolved = _resolve_through_runtime(configuration, spark_factory=PointLookupSpark)

    assert resolved["data_app"] == "telemetry"
    assert resolved["pipes"] == "bronze.ingest_telemetry"
    assert resolved["events_table"] == "dev_silver.cwmdp.events"
    bridged = selector._pipeline_config_for_kindling(PointLookupSpark(configuration), "telemetry")
    assert bridged["environment"] == "dev"
    assert bridged["workspace_id"] == "adb-lakeflow-telemetry"
    assert selector.SETTINGS_JSON_CONFIG_KEY not in bridged


def test_generated_configuration_is_bridged_from_flat_configuration_keys_by_default():
    """Keys the generator relies on being point-looked-up must be in the
    selector's default list; otherwise config_keys would have to name them."""
    values = {key: "x" for key in bundle.SELECTOR_DEFAULT_LOOKUP_KEYS}
    values[selector.SETTINGS_JSON_CONFIG_KEY] = "{}"
    values["kindling.lakeflow.config_keys"] = ""

    bridged = selector._pipeline_config_for_kindling(PointLookupSpark(values), "telemetry")

    for key in bundle.SELECTOR_DEFAULT_LOOKUP_KEYS - {
        selector.SETTINGS_JSON_CONFIG_KEY,
        "kindling.lakeflow.config_keys",
    }:
        assert key in bridged, key


def test_output_is_deterministic(tmp_path):
    first = _build_example(tmp_path / "a")
    second = _build_example(tmp_path / "b")

    assert first.files == second.files
    for relative in first.files:
        assert (first.output_dir / relative).read_bytes() == (
            second.output_dir / relative
        ).read_bytes(), relative
    manifest = first.manifest
    assert manifest["generator"]["name"] == bundle.GENERATOR_NAME
    assert "transport" not in json.dumps(manifest)
    assert [pipeline["key"] for pipeline in manifest["pipelines"]] == [
        "telemetry_bronze",
        "telemetry_silver",
    ]
    bronze = manifest["pipelines"][0]
    assert [source["path"] for source in bronze["config_sources"]] == [
        "config/settings.yaml",
        "config/settings.databricks.yaml",
        "config/workspace_adb-lakeflow-telemetry.yaml",
        "config/settings.dev.yaml",
        "data-apps/telemetry/settings.yaml",
    ]
    assert all(len(source["sha256"]) == 64 for source in bronze["config_sources"])
    assert bronze["settings_json_bytes"] > 0
    assert "timestamp" not in json.dumps(manifest)


def test_run_as_permissions_and_wheels_are_rendered(tmp_path):
    wheel = tmp_path / "telemetry_app-1.2.3-py3-none-any.whl"
    wheel.write_bytes(b"not really a wheel")
    result = _build_example(
        tmp_path,
        run_as_service_principal="sp-telemetry",
        permissions=({"level": "CAN_MANAGE", "group_name": "data-eng"},),
        wheels=(wheel,),
    )

    root = yaml.safe_load((result.output_dir / "databricks.yml").read_text(encoding="utf-8"))
    assert root["targets"]["dev"]["run_as"] == {"service_principal_name": "sp-telemetry"}
    bronze = _resource(result, "telemetry_bronze")
    assert bronze["permissions"] == [{"level": "CAN_MANAGE", "group_name": "data-eng"}]
    assert bronze["environment"]["dependencies"] == [
        "spark-kindling-ext-databricks==0.2.0",
        "../wheels/telemetry_app-1.2.3-py3-none-any.whl",
    ]
    assert (result.output_dir / "wheels" / wheel.name).read_bytes() == wheel.read_bytes()
    assert result.manifest["wheels"][0]["file"] == "wheels/telemetry_app-1.2.3-py3-none-any.whl"


def test_unpinned_default_dependency_warns(tmp_path):
    result = _build_example(
        tmp_path, dependencies=bundle.DEFAULT_DEPENDENCIES, dependencies_defaulted=True
    )

    assert any("unpinned" in warning for warning in result.warnings)
    assert result.manifest["warnings"] == result.warnings


def test_wheels_alone_form_the_dependency_set(tmp_path):
    extension = tmp_path / "spark_kindling_ext_databricks-0.2.0-py3-none-any.whl"
    extension.write_bytes(b"x")
    app = tmp_path / "orders_app-1.0.0-py3-none-any.whl"
    app.write_bytes(b"y")

    inputs = bundle.resolve_bundle_inputs(
        {"wheels": (extension, app)}, {**_REQUIRED_ENV, "KINDLING_BUNDLE_APPS": '["telemetry"]'}
    )
    assert inputs.dependencies == ()
    assert inputs.dependencies_defaulted is True

    result = _build_example(
        tmp_path, dependencies=(), dependencies_defaulted=True, wheels=(extension, app)
    )
    assert result.warnings == []
    assert _resource(result, "telemetry_bronze")["environment"]["dependencies"] == [
        "../wheels/spark_kindling_ext_databricks-0.2.0-py3-none-any.whl",
        "../wheels/orders_app-1.0.0-py3-none-any.whl",
    ]


def test_missing_databricks_extension_warns(tmp_path):
    app = tmp_path / "orders_app-1.0.0-py3-none-any.whl"
    app.write_bytes(b"y")

    result = _build_example(tmp_path, dependencies=(), dependencies_defaulted=True, wheels=(app,))

    assert any("spark-kindling-ext-databricks" in warning for warning in result.warnings)
    pinned = _build_example(tmp_path / "b", dependencies=("Spark-Kindling-Ext-Databricks>=0.2.0",))
    assert pinned.warnings == []
    volume = _build_example(
        tmp_path / "c",
        dependencies=(
            "/Volumes/cat/schema/artifacts/packages/spark_kindling-0.12.48-py3-none-any.whl",
            "/Volumes/cat/schema/artifacts/packages/"
            "spark_kindling_ext_databricks-0.2.0-py3-none-any.whl",
        ),
    )
    assert volume.warnings == []


# --------------------------------------------------------------------------- #
# Synthetic projects: layout resolution and safety
# --------------------------------------------------------------------------- #


def _write_project(root: Path, apps_dir: str = "apps") -> Path:
    (root / "config").mkdir(parents=True)
    (root / "config" / "settings.yaml").write_text(
        "kindling:\n  storage:\n    table_schema: base\n", encoding="utf-8"
    )
    (root / "config" / "settings.local.yaml").write_text(
        "kindling:\n  storage:\n    table_schema: local-only\n", encoding="utf-8"
    )
    (root / "config" / "settings.prod.yaml").write_text(
        "kindling:\n  storage:\n    table_schema: prod\n", encoding="utf-8"
    )
    app = root / apps_dir / "sales_ops"
    app.mkdir(parents=True)
    (app / "settings.yaml").write_text(
        "datapipes:\n  sales.load:\n    output_type: delta\n", encoding="utf-8"
    )
    return root


def _inputs(**overrides) -> bundle.BundleInputs:
    values = {
        "name": "sales",
        "target": "prod",
        "apps": ("sales-ops",),
        "workspace_host": "https://adb-1.azuredatabricks.net",
        "workspace_root": "/Workspace/Shared/kindling/sales/prod",
        "runtime_env": "prod",
        "catalog": "prod_sales",
        "schema": "sales",
        "dependencies": ("spark-kindling-ext-databricks==0.2.0",),
    }
    values.update(overrides)
    return bundle.BundleInputs(**values)


def test_kebab_app_resolves_snake_directory_and_local_settings_are_excluded(tmp_path):
    root = _write_project(tmp_path / "project")

    result = bundle.build_bundle(_inputs(), project_root=root)

    assert result.output_dir == root / "dist" / "bundles" / "databricks"
    pipeline = _resource(result, "sales_ops")
    assert pipeline["name"] == "sales-ops"
    settings = json.loads(pipeline["configuration"][selector.SETTINGS_JSON_CONFIG_KEY])
    assert settings["kindling"]["storage"]["table_schema"] == "prod"
    assert settings["datapipes"]["sales.load"]["output_type"] == "delta"
    assert [source["path"] for source in result.manifest["pipelines"][0]["config_sources"]] == [
        "config/settings.yaml",
        "config/settings.prod.yaml",
        "apps/sales_ops/settings.yaml",
    ]


def test_runtime_environment_may_differ_from_target(tmp_path):
    root = _write_project(tmp_path / "project")

    result = bundle.build_bundle(_inputs(target="uat", runtime_env="prod"), project_root=root)

    configuration = _resource(result, "sales_ops")["configuration"]
    assert configuration["spark.kindling.bootstrap.environment"] == "prod"
    root_yaml = yaml.safe_load((result.output_dir / "databricks.yml").read_text(encoding="utf-8"))
    assert list(root_yaml["targets"]) == ["uat"]


def test_missing_app_directory_is_reported(tmp_path):
    root = _write_project(tmp_path / "project")

    with pytest.raises(bundle.BundleProjectError, match="apps/orders"):
        bundle.build_bundle(_inputs(apps=("sales-ops", "orders")), project_root=root)


def test_missing_apps_directory_names_candidates(tmp_path):
    root = tmp_path / "project"
    (root / "config").mkdir(parents=True)

    with pytest.raises(bundle.BundleProjectError, match="data-apps, apps"):
        bundle.build_bundle(_inputs(), project_root=root)


def test_app_without_any_settings_is_rejected(tmp_path):
    root = tmp_path / "project"
    (root / "apps" / "sales_ops").mkdir(parents=True)

    with pytest.raises(bundle.BundleProjectError, match="No settings files found"):
        bundle.build_bundle(_inputs(), project_root=root)


def test_dynaconf_merge_directives_warn_in_inline_mode(tmp_path):
    root = _write_project(tmp_path / "project")
    (root / "apps" / "sales_ops" / "settings.yaml").write_text(
        "kindling:\n  bootstrap:\n    required_packages: '@merge [extra]'\n", encoding="utf-8"
    )

    result = bundle.build_bundle(_inputs(), project_root=root)

    assert any("kindling.bootstrap.required_packages" in warning for warning in result.warnings)


def test_output_directory_safety(tmp_path):
    root = _write_project(tmp_path / "project")
    foreign = tmp_path / "foreign"
    foreign.mkdir()
    (foreign / "keep.txt").write_text("user file", encoding="utf-8")

    with pytest.raises(bundle.BundleProjectError, match="--force"):
        bundle.build_bundle(_inputs(), project_root=root, output_dir=foreign)
    assert (foreign / "keep.txt").exists()

    bundle.build_bundle(_inputs(), project_root=root, output_dir=foreign, force=True)
    assert not (foreign / "keep.txt").exists()
    assert (foreign / "manifest.json").exists()

    # A directory this tool wrote is replaced without --force, and stale
    # output from a previous app set does not survive.
    (foreign / "resources" / "stale.pipeline.yml").write_text("x", encoding="utf-8")
    bundle.build_bundle(_inputs(), project_root=root, output_dir=foreign)
    assert not (foreign / "resources" / "stale.pipeline.yml").exists()

    with pytest.raises(bundle.BundleProjectError, match="contains the project root"):
        bundle.build_bundle(_inputs(), project_root=root, output_dir=root)


# --------------------------------------------------------------------------- #
# Deployment input resolution
# --------------------------------------------------------------------------- #

_REQUIRED_ENV = {
    "KINDLING_BUNDLE_NAME": "sales",
    "KINDLING_BUNDLE_TARGET": "dev",
    "KINDLING_BUNDLE_APPS": '["orders", "customers"]',
    "KINDLING_BUNDLE_WORKSPACE_HOST": "https://adb-1.azuredatabricks.net/",
}


def test_inputs_come_from_environment_with_documented_defaults():
    inputs = bundle.resolve_bundle_inputs({}, _REQUIRED_ENV)

    assert inputs.name == "sales"
    assert inputs.apps == ("orders", "customers")
    assert inputs.workspace_host == "https://adb-1.azuredatabricks.net"
    assert inputs.workspace_root == "/Workspace/Shared/kindling/sales/dev"
    assert inputs.runtime_env == "dev"
    assert inputs.continuous is False
    assert inputs.dependencies == bundle.DEFAULT_DEPENDENCIES
    assert inputs.dependencies_defaulted is True


def test_cli_options_override_environment_and_collections_replace():
    environ = {
        **_REQUIRED_ENV,
        "KINDLING_BUNDLE_RUNTIME_ENV": "dev",
        "KINDLING_BUNDLE_CONTINUOUS": "true",
        "KINDLING_BUNDLE_DEPENDENCIES": '["a==1"]',
    }
    inputs = bundle.resolve_bundle_inputs(
        {
            "target": "prod",
            "apps": ("orders",),
            "env": "production",
            "continuous": False,
            "dependencies": ("b==2", "c==3"),
        },
        environ,
    )

    assert inputs.target == "prod"
    assert inputs.apps == ("orders",)
    assert inputs.runtime_env == "production"
    assert inputs.continuous is False
    assert inputs.dependencies == ("b==2", "c==3")
    assert inputs.dependencies_defaulted is False


@pytest.mark.parametrize(
    ("environ", "message"),
    [
        ({}, "--name or set KINDLING_BUNDLE_NAME"),
        ({"KINDLING_BUNDLE_NAME": "sales"}, "--target or set KINDLING_BUNDLE_TARGET"),
        (
            {"KINDLING_BUNDLE_NAME": "sales", "KINDLING_BUNDLE_TARGET": "dev"},
            "--app .* or set KINDLING_BUNDLE_APPS",
        ),
        (
            {**_REQUIRED_ENV, "KINDLING_BUNDLE_APPS": '"orders"'},
            "KINDLING_BUNDLE_APPS must be a JSON array",
        ),
        ({**_REQUIRED_ENV, "KINDLING_BUNDLE_CONTINUOUS": "yes"}, "exactly 'true' or 'false'"),
        ({**_REQUIRED_ENV, "KINDLING_BUNDLE_WORKSPACE_HOST": "adb-1"}, "https://"),
        ({**_REQUIRED_ENV, "KINDLING_BUNDLE_APP_OPTIONS": "{not json"}, "not valid JSON"),
        (
            {**_REQUIRED_ENV, "KINDLING_BUNDLE_APP_OPTIONS": '{"payments": {}}'},
            "not in the managed app set: payments",
        ),
        (
            {**_REQUIRED_ENV, "KINDLING_BUNDLE_APP_OPTIONS": '{"orders": {"cluster": "x"}}'},
            "unsupported keys for app 'orders': cluster",
        ),
        (
            {**_REQUIRED_ENV, "KINDLING_BUNDLE_APP_OPTIONS": '{"orders": {"pipes": "a,b"}}'},
            "'pipes' for app 'orders' must be a JSON array",
        ),
        (
            {
                **_REQUIRED_ENV,
                "KINDLING_BUNDLE_PERMISSIONS": '[{"level": "ADMIN", "group_name": "g"}]',
            },
            "'level' must be one of",
        ),
        (
            {**_REQUIRED_ENV, "KINDLING_BUNDLE_PERMISSIONS": '[{"level": "CAN_VIEW"}]'},
            "exactly one of user_name",
        ),
    ],
)
def test_invalid_inputs_name_the_option_and_variable(environ, message):
    with pytest.raises(bundle.BundleInputError, match=message):
        bundle.resolve_bundle_inputs({}, environ)


def test_pipelines_require_catalog_and_schema_per_pipeline():
    inputs = bundle.resolve_bundle_inputs(
        {"app_options_json": json.dumps({"orders": {"pipelines": {"bronze": {}}}})},
        {**_REQUIRED_ENV, "KINDLING_BUNDLE_SCHEMA": "orders"},
    )
    with pytest.raises(bundle.BundleInputError, match="Pipeline 'orders/bronze' has no catalog"):
        inputs.pipelines()

    inputs = bundle.resolve_bundle_inputs(
        {
            "catalog": "dev",
            "schema": "orders",
            "app_options_json": json.dumps(
                {
                    "orders": {
                        "continuous": True,
                        "pipelines": {"bronze": {"pipes": ["b.load"]}, "silver": {"schema": "s"}},
                    }
                }
            ),
        },
        _REQUIRED_ENV,
    )
    specs = inputs.pipelines()
    assert [
        (spec.key, spec.name, spec.catalog, spec.schema, spec.continuous, spec.pipes)
        for spec in specs
    ] == [
        ("orders_bronze", "orders-bronze", "dev", "orders", True, ("b.load",)),
        ("orders_silver", "orders-silver", "dev", "s", True, ()),
        ("customers", "customers", "dev", "orders", False, ()),
    ]


def test_duplicate_resource_keys_are_rejected():
    inputs = bundle.resolve_bundle_inputs(
        {"catalog": "dev", "schema": "s", "apps": ("sales-ops", "sales_ops")},
        {**_REQUIRED_ENV},
    )
    with pytest.raises(bundle.BundleInputError, match="duplicates: sales_ops"):
        inputs.pipelines()


# --------------------------------------------------------------------------- #
# CLI surface
# --------------------------------------------------------------------------- #


def test_bundle_build_command_emits_json_summary(tmp_path):
    output = tmp_path / "bundle"
    result = CliRunner().invoke(
        cli,
        [
            "bundle",
            "build",
            "--project-root",
            str(_example_root()),
            "--output",
            str(output),
            "--name",
            "lakeflow-telemetry",
            "--target",
            "dev",
            "--app",
            "telemetry",
            "--workspace-host",
            EXAMPLE_HOST,
            "--catalog",
            "dev_bronze",
            "--schema",
            "cwmdp",
            "--dependency",
            "spark-kindling-ext-databricks==0.2.0",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    summary = json.loads(result.output)
    assert summary["output_dir"] == str(output)
    assert summary["pipelines"] == ["telemetry"]
    assert "resources/telemetry.pipeline.yml" in summary["files"]
    assert (output / "manifest.json").exists()


def test_bundle_build_command_reports_input_errors(tmp_path):
    result = CliRunner().invoke(
        cli,
        [
            "bundle",
            "build",
            "--project-root",
            str(_example_root()),
            "--output",
            str(tmp_path / "o"),
        ],
    )

    assert result.exit_code != 0
    assert "KINDLING_BUNDLE_NAME" in result.output


def test_bundle_build_command_prints_next_steps(tmp_path):
    result = CliRunner().invoke(
        cli,
        [
            "bundle",
            "build",
            "--project-root",
            str(_example_root()),
            "--output",
            str(tmp_path / "o"),
            "--name",
            "lakeflow-telemetry",
            "--target",
            "dev",
            "--app",
            "telemetry",
            "--workspace-host",
            EXAMPLE_HOST,
            "--catalog",
            "dev_bronze",
            "--schema",
            "cwmdp",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "databricks bundle validate -t dev" in result.output
    assert "databricks bundle run -t dev telemetry" in result.output
    assert "unpinned" in result.output
