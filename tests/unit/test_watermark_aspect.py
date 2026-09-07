"""Unit tests for WatermarkAspect pending-state lifecycle.

The aspect captures source cursor state per pipe at read time and advances
watermarks only after a successful persist. These tests pin the lifecycle
guarantees around failure and full-refresh paths — in particular that a cursor
captured by a FAILED execution can never be saved by a later execution of the
same pipe.
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


def _pipe(
    pipeid="pipe.p1",
    inputs=("bronze.src", "bronze.ref"),
    driving_entity_ids=None,
):
    pipe = SimpleNamespace(pipeid=pipeid, name=pipeid, input_entity_ids=list(inputs))
    if driving_entity_ids is not None:
        pipe.driving_entity_ids = list(driving_entity_ids)
    return pipe


def _entity(entityid="bronze.src"):
    return SimpleNamespace(entityid=entityid, name=entityid)


def _record_saved_events(signal_provider):
    events = []

    def _record(sender, **kwargs):
        events.append(dict(kwargs))

    signal = signal_provider.get_signal("persist.watermark_saved")
    signal = signal or signal_provider.create_signal("persist.watermark_saved")
    signal.connect(_record, weak=False)
    return events


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

    def test_multi_driving_reads_persist_every_captured_source(self, aspect, wms, signal_provider):
        pipe = _pipe(
            inputs=("bronze.src_a", "bronze.src_b", "bronze.ref"),
            driving_entity_ids=("bronze.src_a", "bronze.src_b"),
        )
        saved_events = _record_saved_events(signal_provider)
        wms.read_changes.side_effect = [
            (MagicMock(name="df-a"), "7"),
            (MagicMock(name="df-b"), "8"),
        ]

        _emit(
            signal_provider,
            "read.resolve_read",
            entity=_entity("bronze.src_a"),
            pipe=pipe,
            pipe_id=pipe.pipeid,
            use_watermark=True,
        )
        _emit(
            signal_provider,
            "read.resolve_read",
            entity=_entity("bronze.src_b"),
            pipe=pipe,
            pipe_id=pipe.pipeid,
            use_watermark=True,
        )
        assert aspect._pending == {
            pipe.pipeid: {
                "bronze.src_a": "7",
                "bronze.src_b": "8",
            }
        }
        _emit(
            signal_provider,
            "read.resolve_read",
            entity=_entity("bronze.ref"),
            pipe=pipe,
            pipe_id=pipe.pipeid,
            use_watermark=False,
        )

        _emit(signal_provider, "persist.after_persist", pipe_id=pipe.pipeid, persist_id="x")

        saved = [
            (call.args[0], call.args[1], call.args[2]) for call in wms.save_cursor.call_args_list
        ]
        assert saved == [
            ("bronze.src_a", pipe.pipeid, "7"),
            ("bronze.src_b", pipe.pipeid, "8"),
        ]
        assert [
            (event["source_entity_id"], event["cursor"], event["version"], event["persist_id"])
            for event in saved_events
        ] == [
            ("bronze.src_a", "7", 7, "x"),
            ("bronze.src_b", "8", 8, "x"),
        ]

    def test_second_source_save_failure_leaves_only_unsaved_source_pending(
        self, aspect, wms, signal_provider, aspect_logger
    ):
        pipe = _pipe(
            inputs=("bronze.src_a", "bronze.src_b"),
            driving_entity_ids=("bronze.src_a", "bronze.src_b"),
        )
        saved_events = _record_saved_events(signal_provider)
        wms.read_changes.side_effect = [
            (MagicMock(name="df-a"), "7"),
            (MagicMock(name="df-b"), "8"),
        ]
        wms.save_cursor.side_effect = [None, RuntimeError("boom")]

        for source in ("bronze.src_a", "bronze.src_b"):
            _emit(
                signal_provider,
                "read.resolve_read",
                entity=_entity(source),
                pipe=pipe,
                pipe_id=pipe.pipeid,
                use_watermark=True,
            )

        _emit(signal_provider, "persist.after_persist", pipe_id=pipe.pipeid, persist_id="x")

        saved = [
            (call.args[0], call.args[1], call.args[2]) for call in wms.save_cursor.call_args_list
        ]
        assert saved == [
            ("bronze.src_a", pipe.pipeid, "7"),
            ("bronze.src_b", pipe.pipeid, "8"),
        ]
        assert [
            (event["source_entity_id"], event["cursor"], event["version"], event["persist_id"])
            for event in saved_events
        ] == [("bronze.src_a", "7", 7, "x")]
        assert aspect._pending == {pipe.pipeid: {"bronze.src_b": "8"}}
        aspect_logger.exception.assert_called_once()

        wms.save_cursor.reset_mock()
        wms.save_cursor.side_effect = None
        _emit(signal_provider, "persist.after_persist", pipe_id=pipe.pipeid, persist_id="y")

        wms.save_cursor.assert_called_once()
        assert wms.save_cursor.call_args[0][0] == "bronze.src_b"
        assert wms.save_cursor.call_args[0][2] == "8"
        assert [
            (event["source_entity_id"], event["cursor"], event["version"], event["persist_id"])
            for event in saved_events
        ] == [
            ("bronze.src_a", "7", 7, "x"),
            ("bronze.src_b", "8", 8, "y"),
        ]
        assert aspect._pending == {}


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

    def test_no_new_data_read_clears_only_that_source(self, aspect, wms, signal_provider):
        pipe = _pipe(
            inputs=("bronze.src_a", "bronze.src_b"),
            driving_entity_ids=("bronze.src_a", "bronze.src_b"),
        )
        wms.read_changes.side_effect = [
            (MagicMock(name="df-a"), "10"),
            (MagicMock(name="df-b"), "20"),
        ]
        for source in ("bronze.src_a", "bronze.src_b"):
            _emit(
                signal_provider,
                "read.resolve_read",
                entity=_entity(source),
                pipe=pipe,
                pipe_id=pipe.pipeid,
                use_watermark=True,
            )

        wms.read_changes.side_effect = None
        wms.read_changes.return_value = (None, None)
        _emit(
            signal_provider,
            "read.resolve_read",
            entity=_entity("bronze.src_a"),
            pipe=pipe,
            pipe_id=pipe.pipeid,
            use_watermark=True,
        )
        _emit(signal_provider, "persist.after_persist", pipe_id=pipe.pipeid, persist_id="z")

        wms.save_cursor.assert_called_once()
        assert wms.save_cursor.call_args[0][0] == "bronze.src_b"
        assert wms.save_cursor.call_args[0][2] == "20"

    def test_non_watermarked_driving_read_clears_only_that_source(
        self, aspect, wms, signal_provider
    ):
        pipe = _pipe(
            inputs=("bronze.src_a", "bronze.src_b", "bronze.ref"),
            driving_entity_ids=("bronze.src_a", "bronze.src_b"),
        )
        wms.read_changes.side_effect = [
            (MagicMock(name="df-a"), "10"),
            (MagicMock(name="df-b"), "20"),
        ]
        for source in ("bronze.src_a", "bronze.src_b"):
            _emit(
                signal_provider,
                "read.resolve_read",
                entity=_entity(source),
                pipe=pipe,
                pipe_id=pipe.pipeid,
                use_watermark=True,
            )

        _emit(
            signal_provider,
            "read.resolve_read",
            entity=_entity("bronze.src_a"),
            pipe=pipe,
            pipe_id=pipe.pipeid,
            use_watermark=False,
        )
        _emit(
            signal_provider,
            "read.resolve_read",
            entity=_entity("bronze.ref"),
            pipe=pipe,
            pipe_id=pipe.pipeid,
            use_watermark=False,
        )
        _emit(signal_provider, "persist.after_persist", pipe_id=pipe.pipeid, persist_id="z")

        wms.save_cursor.assert_called_once()
        assert wms.save_cursor.call_args[0][0] == "bronze.src_b"
        assert wms.save_cursor.call_args[0][2] == "20"

    def test_no_watermark_driving_reads_clear_all_pending_sources(
        self, aspect, wms, signal_provider
    ):
        pipe = _pipe(
            inputs=("bronze.src_a", "bronze.src_b"),
            driving_entity_ids=("bronze.src_a", "bronze.src_b"),
        )
        wms.read_changes.side_effect = [
            (MagicMock(name="df-a"), "10"),
            (MagicMock(name="df-b"), "20"),
        ]
        for source in ("bronze.src_a", "bronze.src_b"):
            _emit(
                signal_provider,
                "read.resolve_read",
                entity=_entity(source),
                pipe=pipe,
                pipe_id=pipe.pipeid,
                use_watermark=True,
            )

        for source in ("bronze.src_a", "bronze.src_b"):
            _emit(
                signal_provider,
                "read.resolve_read",
                entity=_entity(source),
                pipe=pipe,
                pipe_id=pipe.pipeid,
                use_watermark=False,
            )

        assert aspect._pending == {}
        _emit(signal_provider, "persist.after_persist", pipe_id=pipe.pipeid, persist_id="z")
        wms.save_cursor.assert_not_called()

    def test_pipe_failure_after_first_driving_read_clears_partial_capture(
        self, aspect, wms, signal_provider
    ):
        pipe = _pipe(
            inputs=("bronze.src_a", "bronze.src_b"),
            driving_entity_ids=("bronze.src_a", "bronze.src_b"),
        )
        wms.read_changes.return_value = (MagicMock(name="df-a"), "10")

        _emit(
            signal_provider,
            "read.resolve_read",
            entity=_entity("bronze.src_a"),
            pipe=pipe,
            pipe_id=pipe.pipeid,
            use_watermark=True,
        )
        _emit(signal_provider, "datapipes.pipe_failed", pipe_id=pipe.pipeid, error="boom")

        assert aspect._pending == {}
        _emit(signal_provider, "persist.after_persist", pipe_id=pipe.pipeid, persist_id="z")
        wms.save_cursor.assert_not_called()

    @pytest.mark.parametrize("skip_signal", ["datapipes.pipe_skipped", "orchestrator.pipe_skipped"])
    def test_pipe_skip_clears_pending(self, aspect, wms, signal_provider, skip_signal):
        """All driving reads were empty, so nothing persisted and the capture
        must not survive to a later after_persist."""
        pipe = _pipe()
        self._capture_then_fail_before_persist(signal_provider, pipe)
        _emit(signal_provider, skip_signal, pipe_id=pipe.pipeid, skip_reason="no_data")

        _emit(signal_provider, "persist.after_persist", pipe_id=pipe.pipeid, persist_id="z")
        wms.save_cursor.assert_not_called()

    @pytest.mark.parametrize(
        "discard_signal",
        [
            "persist.persist_failed",
            "datapipes.pipe_failed",
            "orchestrator.pipe_failed",
            "datapipes.pipe_skipped",
            "orchestrator.pipe_skipped",
        ],
    )
    def test_discard_signals_clear_all_pending_sources(
        self, aspect, wms, signal_provider, discard_signal
    ):
        pipe = _pipe(
            inputs=("bronze.src_a", "bronze.src_b"),
            driving_entity_ids=("bronze.src_a", "bronze.src_b"),
        )
        wms.read_changes.side_effect = [
            (MagicMock(name="df-a"), "10"),
            (MagicMock(name="df-b"), "20"),
        ]
        for source in ("bronze.src_a", "bronze.src_b"):
            _emit(
                signal_provider,
                "read.resolve_read",
                entity=_entity(source),
                pipe=pipe,
                pipe_id=pipe.pipeid,
                use_watermark=True,
            )

        _emit(
            signal_provider,
            discard_signal,
            pipe_id=pipe.pipeid,
            error="boom",
            skip_reason="no_data",
        )

        assert aspect._pending == {}
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

    def test_duplicate_capture_warning_is_per_source(
        self, aspect, wms, signal_provider, aspect_logger
    ):
        pipe = _pipe(
            inputs=("bronze.src_a", "bronze.src_b"),
            driving_entity_ids=("bronze.src_a", "bronze.src_b"),
        )
        wms.read_changes.side_effect = [
            (MagicMock(name="df-a-1"), "10"),
            (MagicMock(name="df-b"), "20"),
            (MagicMock(name="df-a-2"), "12"),
        ]

        _emit(
            signal_provider,
            "read.resolve_read",
            entity=_entity("bronze.src_a"),
            pipe=pipe,
            pipe_id=pipe.pipeid,
            use_watermark=True,
        )
        _emit(
            signal_provider,
            "read.resolve_read",
            entity=_entity("bronze.src_b"),
            pipe=pipe,
            pipe_id=pipe.pipeid,
            use_watermark=True,
        )
        aspect_logger.warning.assert_not_called()

        _emit(
            signal_provider,
            "read.resolve_read",
            entity=_entity("bronze.src_a"),
            pipe=pipe,
            pipe_id=pipe.pipeid,
            use_watermark=True,
        )

        aspect_logger.warning.assert_called_once()
        message = aspect_logger.warning.call_args[0][0]
        assert pipe.pipeid in message
        assert "bronze.src_a" in message

        _emit(signal_provider, "persist.after_persist", pipe_id=pipe.pipeid, persist_id="a")

        saved = [(call.args[0], call.args[2]) for call in wms.save_cursor.call_args_list]
        assert saved == [("bronze.src_a", "12"), ("bronze.src_b", "20")]

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
