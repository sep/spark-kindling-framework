"""Runtime manifest: lifecycle, change capture, fingerprints, the bootstrap
hooks, the app-install hooks and the provenance run_id. No Spark session."""

import json
import types
from unittest.mock import MagicMock, patch

import kindling.bootstrap as bootstrap_module
import pytest
from kindling import runtime_manifest as rm
from kindling.injection import GlobalInjector
from kindling.signaling import BlinkerSignalProvider
from kindling.test_framework import RecordingTraceProvider

KEYS = {"pyspark", "pandas", "py4j"}
INSTALLED = {
    "pyspark": "3.5.1",
    "pandas": "2.2.2",
    "py4j": "0.10.9.7",
    "delta-spark": "3.3.2",
    "spark-kindling": "0.14.1",
    "spark-kindling-ext-sdp": "0.14.1",
    "requests": "2.32.3",
}


class _Config:
    def __init__(self, values=None):
        self.values = values or {}

    def get(self, key, default=None):
        return self.values.get(key, default)


@pytest.fixture(autouse=True)
def _no_manifest():
    rm.reset_runtime_manifest()
    yield
    rm.reset_runtime_manifest()


def _spark(version="3.5.1", app_id="local-123", connect=False):
    module = "pyspark.sql.connect.session" if connect else "pyspark.sql.session"
    cls = type("SparkSession", (), {"__module__": module})
    spark = cls()
    spark.version = version
    spark.conf = types.SimpleNamespace(
        get=lambda key, default=None: {"spark.app.id": app_id}.get(key, default)
    )
    return spark


def _completed(**overrides):
    rm.begin_runtime_manifest(platform="databricks", kindling_version_requested="latest")
    params = dict(
        installed=INSTALLED,
        key_distributions=KEYS,
        spark=_spark(),
        config_service=_Config({"kindling.runtime.features.databricks.runtime_version": "15.4"}),
    )
    params.update(overrides)
    return rm.complete_runtime_manifest(**params)


class TestLifecycle:
    def test_nothing_before_begin(self):
        assert rm.get_runtime_manifest() is None
        assert rm.current_run_id() is None
        assert rm.complete_runtime_manifest(installed={}, key_distributions=()) is None

    def test_each_initialization_gets_a_new_run_id(self):
        first = rm.begin_runtime_manifest().run_id
        second = rm.begin_runtime_manifest().run_id
        assert first != second
        assert rm.current_run_id() == second

    def test_complete_fills_versions(self):
        manifest = _completed()
        assert manifest.platform == "databricks"
        assert manifest.platform_runtime_version == "15.4"
        assert manifest.spark_version == "3.5.1"
        assert manifest.spark_application_id == "local-123"
        assert manifest.spark_connect is False
        assert manifest.pyspark_version == "3.5.1"
        assert manifest.delta_version == "3.3.2"
        assert manifest.kindling_version == "0.14.1"
        assert manifest.kindling_version_requested == "latest"
        assert manifest.extensions == {"spark-kindling-ext-sdp": "0.14.1"}
        assert manifest.key_packages == {"pandas": "2.2.2", "py4j": "0.10.9.7", "pyspark": "3.5.1"}

    def test_discovered_spark_version_wins_and_connect_is_detected(self):
        manifest = _completed(
            spark=_spark(version="9.9", connect=True),
            config_service=_Config({"kindling.runtime.features.spark.version": "3.5.0"}),
        )
        assert manifest.spark_version == "3.5.0"
        assert manifest.spark_connect is True
        assert manifest.platform_runtime_version is None

    def test_missing_context_is_null_never_an_error(self):
        broken = MagicMock()
        broken.get.side_effect = RuntimeError("no config")
        spark = types.SimpleNamespace()  # no version, no conf
        manifest = _completed(spark=spark, config_service=broken, installed={})
        assert manifest.spark_version is None
        assert manifest.spark_application_id is None
        assert manifest.kindling_version is None
        assert manifest.extensions == {}

    def test_to_dict_is_json_serializable(self):
        manifest = _completed()
        rm.record_package_changes({"pandas": "2.2.2"}, {"pandas": "2.3.0"}, "ext")
        json.dumps(manifest.to_dict())


class TestFingerprints:
    def test_stable_for_the_same_runtime(self):
        first = _completed()
        fingerprints = (first.runtime_fingerprint, first.package_fingerprint)
        assert all(len(value) == 64 for value in fingerprints)
        assert (_completed().runtime_fingerprint, _completed().package_fingerprint) == fingerprints

    def test_runtime_fingerprint_follows_key_packages_only(self):
        base = _completed()
        other_package = _completed(installed={**INSTALLED, "requests": "2.33.0"})
        key_package = _completed(installed={**INSTALLED, "pandas": "2.3.0"})
        assert other_package.runtime_fingerprint == base.runtime_fingerprint
        assert other_package.package_fingerprint != base.package_fingerprint
        assert key_package.runtime_fingerprint != base.runtime_fingerprint

    def test_runtime_fingerprint_follows_platform_runtime(self):
        base = _completed()
        newer = _completed(
            config_service=_Config({"kindling.runtime.features.databricks.runtime_version": "16.4"})
        )
        assert newer.runtime_fingerprint != base.runtime_fingerprint


class TestPackageChanges:
    def test_additions_and_replacements_are_recorded(self):
        rm.begin_runtime_manifest()
        changes = rm.record_package_changes(
            {"pandas": "2.2.2", "py4j": "0.10.9.7"},
            {"pandas": "2.3.0", "py4j": "0.10.9.7", "azure-core": "1.30.0"},
            "spark-kindling-ext-cosmos",
        )
        assert changes == [
            rm.PackageChange("azure-core", None, "1.30.0", "spark-kindling-ext-cosmos"),
            rm.PackageChange("pandas", "2.2.2", "2.3.0", "spark-kindling-ext-cosmos"),
        ]
        assert rm.get_runtime_manifest().package_changes == changes

    def test_a_later_change_refreshes_both_fingerprints(self):
        manifest = _completed()
        before = (manifest.runtime_fingerprint, manifest.package_fingerprint)
        rm.record_package_changes(INSTALLED, {**INSTALLED, "pandas": "2.3.0"}, "app x dependencies")
        assert manifest.key_packages["pandas"] == "2.3.0"
        assert manifest.runtime_fingerprint != before[0]
        assert manifest.package_fingerprint != before[1]

    def test_a_later_install_refreshes_every_package_derived_field(self):
        """An app install that adds a key package the runtime lacked and
        upgrades PySpark, Delta and an extension: the manifest reports the
        environment after it, not the one at bootstrap."""
        manifest = _completed(key_distributions=KEYS | {"pyarrow"})
        before = (manifest.runtime_fingerprint, manifest.package_fingerprint)
        assert "pyarrow" not in manifest.key_packages
        after = {
            **INSTALLED,
            "pyarrow": "17.0.0",
            "pyspark": "3.5.3",
            "delta-spark": "3.3.3",
            "spark-kindling-ext-sdp": "0.14.2",
        }
        rm.record_package_changes(INSTALLED, after, "app x dependencies")
        assert manifest.key_packages["pyarrow"] == "17.0.0"
        assert manifest.key_packages["pyspark"] == "3.5.3"
        assert manifest.pyspark_version == "3.5.3"
        assert manifest.delta_version == "3.3.3"
        assert manifest.extensions == {"spark-kindling-ext-sdp": "0.14.2"}
        assert manifest.runtime_fingerprint != before[0]
        assert manifest.package_fingerprint != before[1]

    def test_changes_without_a_manifest_are_still_returned(self):
        assert rm.record_package_changes({}, {"a": "1"}, "x") == [
            rm.PackageChange("a", None, "1", "x")
        ]


class TestSummaryAndSpan:
    def test_summary_names_the_main_versions(self):
        manifest = _completed()
        manifest.app_name, manifest.app_version = "orders", "1.2.0"
        line = manifest.summary()
        for expected in (
            f"run_id={manifest.run_id}",
            "platform=databricks",
            "runtime=15.4",
            "spark=3.5.1",
            "delta=3.3.2",
            "kindling=0.14.1 (requested latest)",
            "extensions=[sdp=0.14.1]",
            "app=orders@1.2.0",
            f"runtime_fingerprint={manifest.runtime_fingerprint[:12]}",
        ):
            assert expected in line, expected

    def test_span_details_are_scalar_strings(self):
        details = _completed().span_details()
        assert details["platform"] == "databricks"
        assert details["extension_count"] == "1"
        assert all(isinstance(value, str) for value in details.values())


class TestExecutionIds:
    @pytest.mark.parametrize(
        "signal",
        [
            "orchestrator.execution_started",
            "datapipes.before_run",
            "streaming.orchestrator_started",
        ],
    )
    def test_executor_runs_are_listed(self, signal):
        rm.begin_runtime_manifest()
        signals = BlinkerSignalProvider()
        recorder = rm.RuntimeManifestExecutionRecorder(signals)
        recorder.register()
        recorder.register()  # idempotent
        signals.get_signal(signal).send(None, run_id="exec-1")
        signals.get_signal(signal).send(None, run_id="exec-1")
        signals.get_signal(signal).send(None, run_id="exec-2")
        assert rm.get_runtime_manifest().execution_ids == ["exec-1", "exec-2"]

    def test_a_dag_run_is_listed_once(self):
        """run_datapipes_dag emits before_run with its own id, then the
        orchestrator emits execution_started with the run's executor id:
        only that one is listed."""
        rm.begin_runtime_manifest()
        signals = BlinkerSignalProvider()
        rm.RuntimeManifestExecutionRecorder(signals).register()
        signals.get_signal("datapipes.before_run").send(
            None, run_id="facade-id", delegated_to_orchestrator=True
        )
        signals.get_signal("orchestrator.execution_started").send(None, run_id="exec-1")
        assert rm.get_runtime_manifest().execution_ids == ["exec-1"]


class TestBootstrapHooks:
    def test_replaced_distributions_are_recorded(self, monkeypatch):
        rm.begin_runtime_manifest()
        monkeypatch.setattr(
            bootstrap_module,
            "_installed_distribution_versions",
            lambda: {"pandas": "2.3.0", "new-dep": "1.0"},
        )
        logger = MagicMock()
        bootstrap_module._report_replaced_distributions(
            logger, {"pandas": ("2.2.2", frozenset())}, "spark-kindling-ext-x"
        )
        logger.warning.assert_called_once()
        assert "replaced installed pandas 2.2.2 with 2.3.0" in logger.warning.call_args.args[0]
        (added,), _ = logger.info.call_args
        assert added == "Installing spark-kindling-ext-x added new-dep 1.0"
        assert [c.name for c in rm.get_runtime_manifest().package_changes] == ["new-dep", "pandas"]

    def test_complete_logs_the_manifest_line(self, monkeypatch):
        rm.begin_runtime_manifest()
        monkeypatch.setattr(bootstrap_module, "_installed_distribution_versions", lambda: INSTALLED)
        logger = MagicMock()
        bootstrap_module._complete_runtime_manifest(_Config(), "standalone", logger)
        (line,), _ = logger.info.call_args
        assert line.startswith("Runtime manifest: run_id=")
        assert "platform=standalone" in line

    def test_complete_never_breaks_bootstrap(self, monkeypatch):
        rm.begin_runtime_manifest()

        def boom():
            raise RuntimeError("metadata unavailable")

        monkeypatch.setattr(bootstrap_module, "_installed_distribution_versions", boom)
        logger = MagicMock()
        bootstrap_module._complete_runtime_manifest(_Config(), "standalone", logger)
        logger.warning.assert_called_once()

    def test_span_is_emitted_with_scalar_details(self):
        _completed()
        tp = RecordingTraceProvider()
        with patch.object(GlobalInjector, "get", return_value=tp):
            bootstrap_module._emit_runtime_manifest_span(_Config())
        (span,) = [s for s in tp.spans if s.operation == "runtime_manifest"]
        assert span.details["run_id"] == rm.current_run_id()
        assert span.details["platform"] == "databricks"

    def test_span_respects_disabled_tracing(self):
        _completed()
        tp = RecordingTraceProvider()
        with patch.object(GlobalInjector, "get", return_value=tp):
            bootstrap_module._emit_runtime_manifest_span(
                _Config({"kindling.telemetry.tracing.enabled": False})
            )
        assert tp.spans == []


class TestAppInstalls:
    def _manager(self, config):
        from kindling.data_apps import DataAppManager

        manager = object.__new__(DataAppManager)
        manager.config = _Config(config)
        manager.logger = MagicMock()
        return manager

    def test_pins_are_opt_in(self, tmp_path):
        assert self._manager({})._runtime_pin_args(str(tmp_path)) == []

    def test_pins_constrain_the_key_packages(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            bootstrap_module,
            "_installed_distribution_versions",
            lambda: {"pandas": "2.2.2", "requests": "2.32.3"},
        )
        args = self._manager({"kindling.apps.pin_runtime_packages": True})._runtime_pin_args(
            str(tmp_path)
        )
        assert args[0] == "-c"
        assert open(args[1]).read() == "pandas==2.2.2\n"

    def test_pypi_install_reports_changes_and_records_the_app(self, monkeypatch):
        from kindling import data_apps

        rm.begin_runtime_manifest()
        versions = iter([{"pandas": "2.2.2"}, {"pandas": "2.3.0"}])
        monkeypatch.setattr(
            bootstrap_module, "_installed_distribution_versions", lambda: next(versions)
        )
        run = MagicMock(return_value=types.SimpleNamespace(returncode=0, stderr=""))
        monkeypatch.setattr(data_apps.subprocess, "run", run)
        manager = self._manager({})
        manager._install_app_dependencies("orders", ["pandas==2.3.0"], [])

        (change,) = rm.get_runtime_manifest().package_changes
        assert change == rm.PackageChange("pandas", "2.2.2", "2.3.0", "app orders dependencies")
        assert "-c" not in run.call_args.args[0]
        data_apps._record_app_in_manifest("orders", "1.2.0")
        manifest = rm.get_runtime_manifest()
        assert (manifest.app_name, manifest.app_version) == ("orders", "1.2.0")


def test_a_failed_app_install_still_records_what_changed(monkeypatch):
    """Lake wheels install, then the PyPI install fails: the wheels' changes
    are still reported and recorded."""
    from kindling import data_apps
    from kindling.data_apps import DataAppManager

    rm.begin_runtime_manifest()
    versions = iter([{"pandas": "2.2.2"}, {"pandas": "2.2.2", "lake-pkg": "1.0"}])
    monkeypatch.setattr(
        bootstrap_module, "_installed_distribution_versions", lambda: next(versions)
    )
    manager = object.__new__(DataAppManager)
    manager.config = _Config({})
    manager.logger = MagicMock()
    monkeypatch.setattr(
        manager, "_download_lake_wheels", lambda *args: "/tmp/wheels", raising=False
    )
    monkeypatch.setattr(manager, "_install_lake_wheels", lambda *args: True, raising=False)

    def failing_pypi(*args):
        raise Exception("PyPI dependency installation failed")

    monkeypatch.setattr(manager, "_install_pypi_dependencies", failing_pypi, raising=False)
    monkeypatch.setattr(data_apps.tempfile, "mkdtemp", lambda prefix="": "/tmp/app")

    with pytest.raises(Exception, match="PyPI dependency installation failed"):
        manager._install_app_dependencies("orders", ["requests"], ["lake-pkg"])

    (change,) = rm.get_runtime_manifest().package_changes
    assert change == rm.PackageChange("lake-pkg", None, "1.0", "app orders dependencies")


class TestProvenanceRunId:
    def _stamp(self, mode):
        from kindling.data_entities import EntityMetadata
        from kindling.provenance import build_provenance_stamp

        entity = EntityMetadata("silver.x", "x", ["id"], {"provenance.enabled": "true"}, None)
        pipe = types.SimpleNamespace(pipeid="p", name="p", tags={}, input_entity_ids=["a"])
        return build_provenance_stamp(pipe, entity, mode=mode, config_service=_Config())

    def test_batch_and_streaming_stamps_carry_the_run_id(self):
        run_id = rm.begin_runtime_manifest().run_id
        assert self._stamp("batch").run_id == run_id
        assert self._stamp("streaming").run_id == run_id

    def test_declared_sdp_stamps_have_no_run_id(self):
        rm.begin_runtime_manifest()
        assert self._stamp("sdp").run_id is None

    def test_no_manifest_no_run_id(self):
        assert self._stamp("batch").run_id is None
