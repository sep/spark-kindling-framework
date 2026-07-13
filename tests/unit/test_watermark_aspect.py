"""Unit tests for WatermarkAspect pending-state lifecycle.

The aspect captures (source_entity_id, version) per pipe at read time and
advances the watermark only after a successful persist. These tests pin
the lifecycle guarantees around failure and full-refresh paths — in
particular that a version captured by a FAILED execution can never be
saved by a later execution of the same pipe.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from kindling.signaling import BlinkerSignalProvider
from kindling.spark_log_provider import PythonLoggerProvider
from kindling.watermarking import ResolvedRead, WatermarkAspect, WatermarkService


@pytest.fixture
def signal_provider():
    return BlinkerSignalProvider()


@pytest.fixture
def wms():
    wms = MagicMock(spec=WatermarkService)
    wms.read_changes.return_value = (MagicMock(name="df"), "7")
    return wms


@pytest.fixture
def aspect_logger():
    return MagicMock()


@pytest.fixture
def aspect(wms, signal_provider, aspect_logger):
    logger_provider = MagicMock(spec=PythonLoggerProvider)
    logger_provider.get_logger.return_value = aspect_logger
    aspect = WatermarkAspect(wms=wms, lp=logger_provider, signal_provider=signal_provider)
    aspect.register()
    return aspect


def _emit(signal_provider, name, **kwargs):
    signal = signal_provider.get_signal(name) or signal_provider.create_signal(name)
    return signal.send(None, **kwargs)


def _pipe(pipeid="pipe.p1", inputs=("bronze.src", "bronze.ref")):
    return SimpleNamespace(pipeid=pipeid, name=pipeid, input_entity_ids=list(inputs))


def _entity(entityid="bronze.src"):
    return SimpleNamespace(entityid=entityid, name=entityid)


class TestHappyPath:
    def test_watermarked_read_then_persist_saves_captured_version(
        self, aspect, wms, signal_provider
    ):
        pipe = _pipe()
        results = _emit(
            signal_provider,
            "read.resolve_read",
            entity=_entity("bronze.src"),
            pipe=pipe,
            pipe_id=pipe.pipeid,
            use_watermark=True,
        )
        resolved = [r for _, r in results if isinstance(r, ResolvedRead)]
        assert len(resolved) == 1

        _emit(signal_provider, "persist.after_persist", pipe_id=pipe.pipeid, persist_id="x")

        wms.save_cursor.assert_called_once()
        args = wms.save_cursor.call_args[0]
        assert args[0] == "bronze.src"
        assert args[1] == pipe.pipeid
        assert args[2] == "7"  # the cursor captured at read time

    def test_non_integer_cursor_round_trips_opaquely(self, aspect, wms, signal_provider):
        """A REST-style timestamp cursor is stored verbatim — the aspect
        never interprets cursor contents."""
        pipe = _pipe()
        wms.read_changes.return_value = (
            MagicMock(name="df"),
            "2026-07-13T10:00:00Z",
        )
        _emit(
            signal_provider,
            "read.resolve_read",
            entity=_entity("bronze.src"),
            pipe=pipe,
            pipe_id=pipe.pipeid,
            use_watermark=True,
        )
        _emit(signal_provider, "persist.after_persist", pipe_id=pipe.pipeid, persist_id="x")

        wms.save_cursor.assert_called_once()
        assert wms.save_cursor.call_args[0][2] == "2026-07-13T10:00:00Z"

    def test_reference_input_read_does_not_clear_driving_capture(
        self, aspect, wms, signal_provider
    ):
        pipe = _pipe()
        _emit(
            signal_provider,
            "read.resolve_read",
            entity=_entity("bronze.src"),
            pipe=pipe,
            pipe_id=pipe.pipeid,
            use_watermark=True,
        )
        # Reference input is read in full (use_watermark=False) AFTER the
        # driving input — it must not disturb the driving capture.
        _emit(
            signal_provider,
            "read.resolve_read",
            entity=_entity("bronze.ref"),
            pipe=pipe,
            pipe_id=pipe.pipeid,
            use_watermark=False,
        )
        _emit(signal_provider, "persist.after_persist", pipe_id=pipe.pipeid, persist_id="x")

        wms.save_cursor.assert_called_once()


class TestStalePendingLifecycle:
    """A version captured by a failed execution must never be saved later."""

    def _capture_then_fail_before_persist(self, signal_provider, pipe):
        """Simulate: watermarked driving read succeeds (capture recorded),
        then the pipe dies before persist — no persist.* signal fires."""
        _emit(
            signal_provider,
            "read.resolve_read",
            entity=_entity("bronze.src"),
            pipe=pipe,
            pipe_id=pipe.pipeid,
            use_watermark=True,
        )

    def test_full_refresh_after_failed_run_does_not_save_stale_watermark(
        self, aspect, wms, signal_provider
    ):
        pipe = _pipe()
        self._capture_then_fail_before_persist(signal_provider, pipe)

        # Later: a full-refresh run of the same pipe (use_watermark=False
        # for the DRIVING input) reads everything and persists.
        _emit(
            signal_provider,
            "read.resolve_read",
            entity=_entity("bronze.src"),
            pipe=pipe,
            pipe_id=pipe.pipeid,
            use_watermark=False,
        )
        _emit(signal_provider, "persist.after_persist", pipe_id=pipe.pipeid, persist_id="y")

        # Full-refresh runs never save watermarks — and in particular must
        # not save the version captured by the earlier failed execution.
        wms.save_cursor.assert_not_called()

    @pytest.mark.parametrize(
        "failure_signal", ["datapipes.pipe_failed", "orchestrator.pipe_failed"]
    )
    def test_pipe_failure_clears_pending(self, aspect, wms, signal_provider, failure_signal):
        pipe = _pipe()
        self._capture_then_fail_before_persist(signal_provider, pipe)
        _emit(signal_provider, failure_signal, pipe_id=pipe.pipeid, error="boom")

        # Even a bare after_persist for this pipe (no fresh resolve_read)
        # must now find nothing to save.
        _emit(signal_provider, "persist.after_persist", pipe_id=pipe.pipeid, persist_id="z")
        wms.save_cursor.assert_not_called()

    def test_persist_failed_clears_pending(self, aspect, wms, signal_provider):
        pipe = _pipe()
        self._capture_then_fail_before_persist(signal_provider, pipe)
        _emit(signal_provider, "persist.persist_failed", pipe_id=pipe.pipeid, error="boom")

        _emit(signal_provider, "persist.after_persist", pipe_id=pipe.pipeid, persist_id="z")
        wms.save_cursor.assert_not_called()

    def test_no_new_data_read_clears_prior_pending(self, aspect, wms, signal_provider):
        pipe = _pipe()
        self._capture_then_fail_before_persist(signal_provider, pipe)

        # Next watermarked run finds no new data — capture must be cleared,
        # not left pointing at the failed run's version.
        wms.read_changes.return_value = (None, None)
        _emit(
            signal_provider,
            "read.resolve_read",
            entity=_entity("bronze.src"),
            pipe=pipe,
            pipe_id=pipe.pipeid,
            use_watermark=True,
        )
        _emit(signal_provider, "persist.after_persist", pipe_id=pipe.pipeid, persist_id="z")
        wms.save_cursor.assert_not_called()

    @pytest.mark.parametrize("skip_signal", ["datapipes.pipe_skipped", "orchestrator.pipe_skipped"])
    def test_pipe_skip_clears_pending(self, aspect, wms, signal_provider, skip_signal):
        """Driving read had data, but the pipe was skipped (e.g. a
        reference input was unavailable) — nothing persisted, so the
        capture must not survive to a later after_persist."""
        pipe = _pipe()
        self._capture_then_fail_before_persist(signal_provider, pipe)
        _emit(signal_provider, skip_signal, pipe_id=pipe.pipeid, skip_reason="no_data")

        _emit(signal_provider, "persist.after_persist", pipe_id=pipe.pipeid, persist_id="z")
        wms.save_cursor.assert_not_called()

    def test_overlapping_same_pipe_captures_warn_and_last_wins(
        self, aspect, wms, signal_provider, aspect_logger
    ):
        """Concurrent same-pipe execution is unsupported (one cursor per
        source/reader). The aspect's documented behavior when captures
        overlap anyway: warn, last capture wins, single save."""
        pipe = _pipe()
        wms.read_changes.return_value = (MagicMock(name="df"), "10")
        _emit(
            signal_provider,
            "read.resolve_read",
            entity=_entity("bronze.src"),
            pipe=pipe,
            pipe_id=pipe.pipeid,
            use_watermark=True,
        )
        aspect_logger.warning.assert_not_called()

        # A second capture for the same pipe before the first persists.
        wms.read_changes.return_value = (MagicMock(name="df"), "12")
        _emit(
            signal_provider,
            "read.resolve_read",
            entity=_entity("bronze.src"),
            pipe=pipe,
            pipe_id=pipe.pipeid,
            use_watermark=True,
        )
        aspect_logger.warning.assert_called_once()

        _emit(signal_provider, "persist.after_persist", pipe_id=pipe.pipeid, persist_id="a")
        _emit(signal_provider, "persist.after_persist", pipe_id=pipe.pipeid, persist_id="b")

        wms.save_cursor.assert_called_once()
        assert wms.save_cursor.call_args[0][2] == "12"

    def test_failure_in_one_pipe_does_not_affect_another(self, aspect, wms, signal_provider):
        pipe_a = _pipe("pipe.a")
        pipe_b = _pipe("pipe.b")
        self._capture_then_fail_before_persist(signal_provider, pipe_a)
        _emit(signal_provider, "datapipes.pipe_failed", pipe_id=pipe_a.pipeid, error="boom")

        _emit(
            signal_provider,
            "read.resolve_read",
            entity=_entity("bronze.src"),
            pipe=pipe_b,
            pipe_id=pipe_b.pipeid,
            use_watermark=True,
        )
        _emit(signal_provider, "persist.after_persist", pipe_id=pipe_b.pipeid, persist_id="w")

        wms.save_cursor.assert_called_once()
        assert wms.save_cursor.call_args[0][1] == pipe_b.pipeid
