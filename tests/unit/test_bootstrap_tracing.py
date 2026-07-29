"""Bootstrap span-tree tests for gh#210 Phase 3.

The bootstrap phases predate a usable trace provider, so timings are
recorded by _BootstrapPhaseRecorder and retro-flushed as one span tree
(root kindling.bootstrap/initialize + one record_span child per phase).
"""

import time
from unittest.mock import MagicMock, patch

import kindling.bootstrap as bootstrap_module
import pytest
from kindling.bootstrap import _bootstrap_phase, _flush_bootstrap_trace
from kindling.injection import GlobalInjector
from kindling.test_framework import RecordingTraceProvider


class _Config:
    def __init__(self, values=None):
        self.values = values or {}

    def get(self, key, default=None):
        return self.values.get(key, default)


@pytest.fixture(autouse=True)
def _fresh_recorder():
    from datetime import datetime

    bootstrap_module._PHASE_RECORDER.reset(start_time=datetime.now())
    yield
    bootstrap_module._PHASE_RECORDER.phases = []
    bootstrap_module._PHASE_RECORDER.start_time = None


def _flush_into(config=None, error=None):
    tp = RecordingTraceProvider()
    with patch.object(GlobalInjector, "get", return_value=tp):
        _flush_bootstrap_trace(config or _Config(), error=error)
    return tp


class TestBootstrapPhaseRecorder:
    def test_phases_record_name_and_window(self):
        with _bootstrap_phase("config_download"):
            time.sleep(0.01)
        with _bootstrap_phase("platform_init"):
            pass

        phases = bootstrap_module._PHASE_RECORDER.phases
        assert [p["name"] for p in phases] == ["config_download", "platform_init"]
        assert all(p["start"] is not None and p["end"] is not None for p in phases)
        assert phases[0]["end"] >= phases[0]["start"]
        assert all(p["error"] is None for p in phases)

    def test_phase_error_recorded_and_reraised(self):
        with pytest.raises(RuntimeError, match="download exploded"):
            with _bootstrap_phase("config_download"):
                raise RuntimeError("download exploded")

        entry = bootstrap_module._PHASE_RECORDER.phases[0]
        assert entry["error"] == "download exploded"
        assert entry["end"] is not None


class TestFlushBootstrapTrace:
    def test_flush_yields_root_plus_phase_children(self):
        with _bootstrap_phase("config_download"):
            pass
        with _bootstrap_phase("platform_init"):
            pass

        tp = _flush_into()

        roots = tp.tree()
        assert len(roots) == 1
        root = roots[0]
        assert root.component == "kindling.bootstrap"
        assert root.operation == "initialize"
        assert root.details["phase_count"] == 2
        assert "error" not in root.details
        assert [child.operation for child in root.children] == [
            "config_download",
            "platform_init",
        ]
        assert all(child.component == "kindling.bootstrap" for child in root.children)

    def test_flush_honors_recorded_timestamps(self):
        with _bootstrap_phase("dependency_install"):
            time.sleep(0.01)
        entry = bootstrap_module._PHASE_RECORDER.phases[0]

        tp = _flush_into()

        child = tp.find(operation="dependency_install")[0]
        assert child.start_time == entry["start"]
        assert child.end_time == entry["end"]

    def test_flush_error_path_marks_failing_phase_and_root(self):
        with _bootstrap_phase("config_overlay"):
            pass
        with pytest.raises(RuntimeError):
            with _bootstrap_phase("workspace_packages"):
                raise RuntimeError("load failed")

        tp = _flush_into(error="load failed")

        root = tp.tree()[0]
        assert root.details["error"] == "load failed"
        failing = tp.find(operation="workspace_packages")[0]
        assert failing.error == "load failed"
        healthy = tp.find(operation="config_overlay")[0]
        assert healthy.error is None

    def test_flush_disabled_by_config(self):
        with _bootstrap_phase("config_download"):
            pass

        tp = _flush_into(config=_Config({"kindling.telemetry.tracing.enabled": "false"}))

        assert tp.spans == []

    def test_flush_is_one_shot(self):
        with _bootstrap_phase("config_download"):
            pass

        first = _flush_into()
        second = _flush_into()

        assert len(first.spans) == 2  # root + one phase
        assert second.spans == [], "A second flush must not re-emit the tree"

    def test_flush_with_no_recorded_phases_is_noop(self):
        tp = _flush_into()
        assert tp.spans == []

    def test_flush_never_breaks_bootstrap(self):
        with _bootstrap_phase("config_download"):
            pass

        with patch.object(GlobalInjector, "get", side_effect=RuntimeError("no provider")):
            _flush_bootstrap_trace(_Config())  # must not raise


class TestShortCircuitedReinit:
    def test_short_circuited_reinit_records_nothing(self):
        bootstrap_module._PHASE_RECORDER.phases = []
        bootstrap_module._PHASE_RECORDER.start_time = None

        with patch.object(bootstrap_module, "is_framework_initialized", return_value=True):
            with patch.object(bootstrap_module, "get_kindling_service", MagicMock()):
                with patch.object(bootstrap_module, "_import_local_package_registrations"):
                    with patch.object(bootstrap_module, "_flush_bootstrap_trace") as flush:
                        bootstrap_module.initialize_framework({})

        assert bootstrap_module._PHASE_RECORDER.phases == []
        assert bootstrap_module._PHASE_RECORDER.start_time is None
        flush.assert_not_called()
