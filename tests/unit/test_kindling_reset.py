"""Tests for the public Kindling in-process reset API."""

import __main__

import kindling
from kindling.data_entities import DataEntities
from kindling.data_pipes import DataPipes
from kindling.injection import GlobalInjector
from kindling.signaling import DataSignals


def test_reset_clears_framework_state_without_stopping_spark():
    DataEntities.deregistry = object()
    DataPipes.dpregistry = object()
    DataSignals._handlers = [object()]
    GlobalInjector.get_injector()
    kindling._active_engine_extension = object()

    kindling.reset()

    assert DataEntities.deregistry is None
    assert DataPipes.dpregistry is None
    assert DataSignals._handlers == []
    assert kindling._active_engine_extension is None
    assert not hasattr(__main__, "_global_injector_instance")
