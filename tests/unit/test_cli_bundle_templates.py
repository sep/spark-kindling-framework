"""Template-driven bundle generation.

The project owns ordinary Databricks bundle YAML with Jinja placeholders; the
generator supplies the merged settings and the pieces that must stay
consistent with them through ``kindling.configuration()``. The built-in
template is the default and what ``kindling bundle template init`` copies.
"""

import json
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner
from kindling_cli import bundle
from kindling_cli.cli import cli


def _write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def _project(tmp_path: Path) -> Path:
    root = tmp_path / "project"
    _write(root / "config" / "settings.yaml", "kindling:\n  storage:\n    table_schema: base\n")
    _write(root / "config" / "settings.dev.yaml", "kindling:\n  storage:\n    table_schema: dev\n")
    _write(
        root / "data-apps" / "orders" / "settings.yaml",
        "datapipes:\n  orders.load:\n    output_type: delta\n",
    )
    _write(root / "data-apps" / "customers" / "settings.yaml", "kindling:\n  a: customers\n")
    return root


def _inputs(**overrides) -> bundle.BundleInputs:
    values = {
        "name": "sales",
        "target": "dev",
        "apps": (),
        "workspace_host": "https://adb-1.azuredatabricks.net",
        "workspace_root": "/Workspace/Users/me/sales",
        "runtime_env": "dev",
        "dependencies": ("spark-kindling-ext-databricks==0.2.0",),
    }
    values.update(overrides)
    return bundle.BundleInputs(**values)


def _resource(output_dir: Path, relative: str) -> dict:
    return yaml.safe_load((output_dir / relative).read_text(encoding="utf-8"))


CUSTOM_TEMPLATE = {
    "databricks.yml.j2": """\
bundle:
  name: {{ bundle.name }}
include:
- resources/*.yml
targets:
  {{ bundle.target }}:
    default: true
    workspace:
      host: {{ bundle.workspace_host }}
      root_path: {{ bundle.workspace_root }}
""",
    "resources/pipelines.yml.j2": """\
resources:
  pipelines:
    legacy_orders_ingest:            # identity owned by the project
      name: Orders Ingest (legacy name)
      catalog: prod_sales
      target: orders
      tags:
        team: sales
      permissions:
      - level: CAN_VIEW
        group_name: analysts
      libraries:
      - file:
          path: ../src/pipeline.py
      environment:
        dependencies:
        {{ dependencies | to_yaml | indent(8) }}
      configuration:
        {{ kindling.configuration("orders", pipes=["orders.load"],
             extra={"kindling.lakeflow.temporal_mode": "chain", "kindling.debug": true})
           | to_yaml | indent(8) }}
    customers_all:
      name: customers
      catalog: prod_sales
      target: customers
      libraries:
      - file:
          path: ../src/pipeline.py
      configuration:
        {{ kindling.configuration("customers") | to_yaml | indent(8) }}
""",
    "src/pipeline.py": (
        "from kindling_ext_databricks.lakeflow_app_selector import "
        "declare_from_pipeline_config\n\ndeclare_from_pipeline_config()\n"
    ),
}


def _custom_template(root: Path) -> Path:
    template_dir = root / "bundle-template"
    for relative, text in CUSTOM_TEMPLATE.items():
        _write(template_dir / relative, text)
    return template_dir


# --------------------------------------------------------------------------- #
# Custom templates
# --------------------------------------------------------------------------- #


def test_custom_template_owns_identity_and_any_dab_field(tmp_path):
    root = _project(tmp_path)
    template_dir = _custom_template(root)

    result = bundle.build_bundle(
        _inputs(), project_root=root, output_dir=tmp_path / "out", template_dir=template_dir
    )

    assert result.files == [
        "databricks.yml",
        "manifest.json",
        "resources/pipelines.yml",
        "src/pipeline.py",
    ]
    pipelines = _resource(result.output_dir, "resources/pipelines.yml")["resources"]["pipelines"]
    legacy = pipelines["legacy_orders_ingest"]
    assert legacy["name"] == "Orders Ingest (legacy name)"
    assert legacy["tags"] == {"team": "sales"}
    assert legacy["permissions"] == [{"level": "CAN_VIEW", "group_name": "analysts"}]
    assert legacy["environment"]["dependencies"] == ["spark-kindling-ext-databricks==0.2.0"]
    assert [p["key"] for p in result.manifest["pipelines"]] == [
        "legacy_orders_ingest",
        "customers_all",
    ]
    assert result.manifest["template"]["source"] == "bundle-template"
    assert {f["path"] for f in result.manifest["template"]["files"]} == set(CUSTOM_TEMPLATE)
    assert result.warnings == []


def test_configuration_helper_completes_config_keys_and_encodes_extras(tmp_path):
    root = _project(tmp_path)
    template_dir = _custom_template(root)

    result = bundle.build_bundle(
        _inputs(workspace_id="ws1"),
        project_root=root,
        output_dir=tmp_path / "out",
        template_dir=template_dir,
    )

    pipelines = _resource(result.output_dir, "resources/pipelines.yml")["resources"]["pipelines"]
    configuration = pipelines["legacy_orders_ingest"]["configuration"]
    assert configuration["kindling.data_app"] == "orders"
    assert configuration["kindling.lakeflow.pipes"] == "orders.load"
    assert configuration["kindling.lakeflow.temporal_mode"] == "chain"
    assert configuration["kindling.debug"] == "true"
    assert configuration["spark.kindling.bootstrap.workspace_id"] == "ws1"
    settings = json.loads(configuration["kindling.lakeflow.settings_json"])
    assert settings["kindling"]["storage"]["table_schema"] == "dev"
    assert settings["datapipes"]["orders.load"]["output_type"] == "delta"
    # Every emitted key the selector does not point-look-up by default is named.
    assert configuration["kindling.lakeflow.config_keys"].split(",") == [
        "spark.kindling.bootstrap.environment",
        "spark.kindling.bootstrap.workspace_id",
        "kindling.lakeflow.temporal_mode",
        "kindling.debug",
    ]
    assert all(isinstance(value, str) for value in configuration.values())
    # The second pipeline selects a different app without a pipe subset.
    customers = pipelines["customers_all"]["configuration"]
    assert customers["kindling.data_app"] == "customers"
    assert "kindling.lakeflow.pipes" not in customers
    assert json.loads(customers["kindling.lakeflow.settings_json"])["kindling"]["a"] == "customers"


def test_apps_are_discovered_when_not_named(tmp_path):
    root = _project(tmp_path)
    template_dir = _custom_template(root)

    result = bundle.build_bundle(
        _inputs(apps=()), project_root=root, output_dir=tmp_path / "out", template_dir=template_dir
    )

    assert [app["name"] for app in result.manifest["apps"]] == ["customers", "orders"]


@pytest.mark.parametrize(
    ("configuration_expr", "message"),
    [
        ("{{ kindling.configuration('nope') | to_yaml | indent(8) }}", "unknown app"),
        (
            "{{ kindling.configuration('orders', extra={'kindling.lakeflow.config_keys': 'x'}) "
            "| to_yaml | indent(8) }}",
            "computed by the helper",
        ),
        ("kindling.data_app: orders", "has no kindling.lakeflow.settings_json"),
        (
            "kindling.data_app: orders\n        kindling.lakeflow.settings_json: '{}'\n"
            "        kindling.custom: x",
            "config_keys must name every non-default key",
        ),
    ],
)
def test_template_pipelines_must_use_the_helper_consistently(tmp_path, configuration_expr, message):
    root = _project(tmp_path)
    template_dir = root / "bundle-template"
    _write(template_dir / "databricks.yml.j2", "bundle:\n  name: {{ bundle.name }}\n")
    _write(
        template_dir / "resources" / "p.yml.j2",
        "resources:\n  pipelines:\n    p:\n      name: p\n      configuration:\n        "
        + configuration_expr
        + "\n",
    )

    with pytest.raises(bundle.BundleProjectError, match=message):
        bundle.build_bundle(
            _inputs(), project_root=root, output_dir=tmp_path / "out", template_dir=template_dir
        )


def test_undefined_template_variable_is_reported(tmp_path):
    root = _project(tmp_path)
    template_dir = root / "bundle-template"
    _write(template_dir / "databricks.yml.j2", "bundle:\n  name: {{ bundel.name }}\n")

    with pytest.raises(bundle.BundleProjectError, match="failed to render"):
        bundle.build_bundle(
            _inputs(), project_root=root, output_dir=tmp_path / "out", template_dir=template_dir
        )


def test_template_without_pipelines_is_rejected(tmp_path):
    root = _project(tmp_path)
    template_dir = root / "bundle-template"
    _write(template_dir / "databricks.yml.j2", "bundle:\n  name: {{ bundle.name }}\n")

    with pytest.raises(bundle.BundleProjectError, match="rendered no pipeline resources"):
        bundle.build_bundle(
            _inputs(), project_root=root, output_dir=tmp_path / "out", template_dir=template_dir
        )


def test_pipeline_token_renders_once_per_pipeline(tmp_path):
    root = _project(tmp_path)
    template_dir = root / "bundle-template"
    _write(template_dir / "databricks.yml.j2", "bundle:\n  name: {{ bundle.name }}\n")
    _write(
        template_dir / "resources" / "__pipeline__.yml.j2",
        "resources:\n  pipelines:\n    {{ pipeline.key }}:\n      name: {{ pipeline.name }}\n"
        "      configuration:\n        "
        "{{ kindling.configuration(pipeline.app, pipes=pipeline.pipes)"
        " | to_yaml | indent(8) }}\n",
    )
    inputs = _inputs(
        apps=("orders",),
        catalog="c",
        schema="s",
        app_options=bundle._validate_app_options(
            {"orders": {"pipelines": {"a": {"pipes": ["orders.load"]}, "b": {}}}}, ["orders"]
        ),
    )

    result = bundle.build_bundle(
        inputs, project_root=root, output_dir=tmp_path / "out", template_dir=template_dir
    )

    assert [f for f in result.files if f.startswith("resources/")] == [
        "resources/orders_a.yml",
        "resources/orders_b.yml",
    ]
    assert (
        _resource(result.output_dir, "resources/orders_a.yml")["resources"]["pipelines"][
            "orders_a"
        ]["configuration"]["kindling.lakeflow.pipes"]
        == "orders.load"
    )


def test_custom_template_is_protected_from_the_output_directory(tmp_path):
    root = _project(tmp_path)
    template_dir = _custom_template(root)

    with pytest.raises(bundle.BundleProjectError, match="overlaps the project input"):
        bundle.build_bundle(
            _inputs(),
            project_root=root,
            output_dir=template_dir,
            template_dir=template_dir,
            force=True,
        )
    assert (template_dir / "databricks.yml.j2").exists()


# --------------------------------------------------------------------------- #
# Built-in template and template init
# --------------------------------------------------------------------------- #


def test_builtin_template_is_the_default_and_hashed_in_the_manifest(tmp_path):
    root = _project(tmp_path)

    result = bundle.build_bundle(
        _inputs(apps=("orders",), catalog="c", schema="s"),
        project_root=root,
        output_dir=tmp_path / "out",
    )

    assert result.manifest["template"]["source"] == "builtin"
    assert {f["path"] for f in result.manifest["template"]["files"]} == {
        "README.md",
        "databricks.yml.j2",
        "resources/__pipeline__.pipeline.yml.j2",
        "src/kindling_lakeflow.py",
    }
    assert "README.md" not in result.files


def test_template_init_copies_the_builtin_template_and_renders_identically(tmp_path):
    root = _project(tmp_path)
    inputs = _inputs(apps=("orders",), catalog="c", schema="s")
    builtin = bundle.build_bundle(inputs, project_root=root, output_dir=tmp_path / "a")

    copied = bundle.init_template(root)
    assert copied == root / "bundle-template"
    assert (copied / "resources" / "__pipeline__.pipeline.yml.j2").exists()
    with pytest.raises(bundle.BundleProjectError, match="--force"):
        bundle.init_template(root)

    from_copy = bundle.build_bundle(
        inputs, project_root=root, output_dir=tmp_path / "b", template_dir=copied
    )
    assert from_copy.files == builtin.files
    for relative in builtin.files:
        if relative == bundle.MANIFEST_FILE:
            continue
        assert (from_copy.output_dir / relative).read_bytes() == (
            builtin.output_dir / relative
        ).read_bytes(), relative
    assert from_copy.manifest["template"]["source"] == "bundle-template"


def test_template_init_command(tmp_path):
    root = _project(tmp_path)

    result = CliRunner().invoke(
        cli, ["bundle", "template", "init", "--project-root", str(root), "--dir", "tpl"]
    )

    assert result.exit_code == 0, result.output
    assert (root / "tpl" / "databricks.yml.j2").exists()
    assert "--template-dir" in result.output


def test_build_command_accepts_template_dir(tmp_path):
    root = _project(tmp_path)
    template_dir = _custom_template(root)

    result = CliRunner().invoke(
        cli,
        [
            "bundle",
            "build",
            "--project-root",
            str(root),
            "--output",
            str(tmp_path / "out"),
            "--template-dir",
            str(template_dir),
            "--name",
            "sales",
            "--target",
            "dev",
            "--workspace-host",
            "https://adb-1.azuredatabricks.net",
            "--dependency",
            "spark-kindling-ext-databricks==0.2.0",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    summary = json.loads(result.output)
    assert summary["pipelines"] == ["legacy_orders_ingest", "customers_all"]
