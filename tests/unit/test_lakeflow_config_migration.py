import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import yaml
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


def _repo_root() -> Path:
    return Path(__file__).parents[2]


def _example_root() -> Path:
    return _repo_root() / "examples" / "lakeflow-telemetry"


def _load_example_pipeline_config(pipeline_name: str = "telemetry_bronze") -> dict:
    with (_example_root() / "databricks.yml").open(encoding="utf-8") as config_file:
        bundle = yaml.safe_load(config_file)
    return bundle["resources"]["pipelines"][pipeline_name]["configuration"]


def _localize_workspace_config_files(configuration: dict) -> dict:
    localized = dict(configuration)
    workspace_prefix = "/Workspace/Shared/kindling/lakeflow-telemetry/dev/"
    configured_files = json.loads(localized[selector.CANONICAL_CONFIG_FILES_CONFIG_KEY])
    localized_files = []
    for configured_file in configured_files:
        assert configured_file.startswith(workspace_prefix)
        localized_files.append(str(_example_root() / configured_file[len(workspace_prefix) :]))
    localized[selector.CANONICAL_CONFIG_FILES_CONFIG_KEY] = json.dumps(localized_files)
    return localized


def _load_shared_config(initial_config: dict):
    from kindling.spark_config import DynaconfConfig

    config_service = DynaconfConfig()
    empty_spark = SimpleNamespace(conf=FakeConf())
    with patch("kindling.spark_config.get_or_create_spark_session", return_value=empty_spark):
        config_service.initialize(
            config_files=initial_config["config_files"],
            initial_config=initial_config,
            environment=initial_config["environment"],
        )
    return config_service


def test_deprecated_lakeflow_config_files_alias_warns_and_delegates(caplog):
    with caplog.at_level("WARNING", logger=selector.__name__):
        config = selector._pipeline_config_for_kindling(
            FakeSpark(
                {
                    "kindling.data_app": "telemetry",
                    "spark.kindling.bootstrap.config_files": '["canonical.yaml"]',
                    selector.CONFIG_FILES_CONFIG_KEY: "legacy.yaml",
                }
            ),
            "telemetry",
        )

    assert config["config_files"] == ["canonical.yaml", "legacy.yaml"]
    assert selector.CONFIG_FILES_CONFIG_KEY in caplog.text
    assert selector.CANONICAL_CONFIG_FILES_CONFIG_KEY in caplog.text


def test_example_bundle_uses_canonical_config_files_and_loads_as_written():
    bundle_config = _load_example_pipeline_config()

    assert selector.CANONICAL_CONFIG_FILES_CONFIG_KEY in bundle_config
    assert selector.CONFIG_FILES_CONFIG_KEY not in bundle_config

    lakeflow_config = selector._pipeline_config_for_kindling(
        FakeSpark(_localize_workspace_config_files(bundle_config)),
        "telemetry",
    )
    config_service = _load_shared_config(lakeflow_config)

    assert config_service.get("kindling.platform.environment") == "databricks"
    assert config_service.get("kindling.sdp.dataset_naming") == "leaf"
    assert config_service.get("kindling.telemetry.logging.level") == "CRITICAL"
    assert config_service.get("dataentities")["silver.events"]["tags"]["provider.table_name"] == (
        "dev_silver.cwmdp.events"
    )
