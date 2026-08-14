"""Tests for the public Kindling in-process reset API."""

import __main__

import kindling
from kindling.data_entities import DataEntities
from kindling.data_pipes import DataPipes
from kindling.injection import GlobalInjector
from kindling.signaling import DataSignals


def test_reset_clears_framework_state_without_stopping_spark():
    # Start from a known-clean DataSignals baseline: it's a process-wide
    # singleton (class attributes), so under parallel test execution it can
    # carry state from whatever else has run earlier in the same worker
    # process. Reset first, then assert on the specific sentinel this test
    # adds -- a claim the reset contract actually makes -- rather than on
    # global list emptiness, which assumes this test has the process to
    # itself.
    DataSignals.reset()

    DataEntities.deregistry = object()
    DataPipes.dpregistry = object()
    sentinel_handler = object()
    DataSignals._handlers = DataSignals._handlers + [sentinel_handler]
    GlobalInjector.get_injector()
    kindling._active_engine_extension = object()

    kindling.reset()

    assert DataEntities.deregistry is None
    assert DataPipes.dpregistry is None
    assert sentinel_handler not in DataSignals._handlers
    assert kindling._active_engine_extension is None
    assert not hasattr(__main__, "_global_injector_instance")
