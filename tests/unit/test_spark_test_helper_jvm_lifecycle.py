"""Focused tests for the process-global JVM lifecycle in tests/spark_test_helper.py.

No JVM is started here. The gateway, its launcher process and the active
session are fakes; ``SparkContext``/``SparkSession`` class-level state is
patched through ``monkeypatch`` so it is restored after every test even when
the code under test clears it.

What is pinned down, because each was a real regression on the way to #297:
- a jar-less JVM this process launched is torn down and its process reaped;
- a Delta-launched JVM is never torn down, active session or not;
- a dead gateway is relaunched, a dead EXTERNAL gateway is reported, never
  shut down;
- an external gateway proven jar-less is reported, not handed back;
- rebuilding an active session requires BOTH Delta static confs and clears
  the process-level references to the stopped session;
- reaping closes stdin, waits, and kills only when the wait times out.
"""

from __future__ import annotations

import subprocess
import sys
from types import SimpleNamespace

import pytest
from pyspark import SparkContext
from pyspark.sql import SparkSession

import tests.spark_test_helper as helper

DELTA_ARGS = [
    "spark-submit",
    "--conf",
    "spark.jars.packages=io.delta:delta-spark_2.12:3.3.2",
    "pyspark-shell",
]
PLAIN_ARGS = ["spark-submit", "pyspark-shell"]


class FakeProc:
    def __init__(self, args, *, alive=True, hangs=False):
        self.args = args
        self._alive = alive
        self._hangs = hangs
        self.stdin = SimpleNamespace(closed=False)
        self.stdin.close = self._close_stdin
        self.killed = False
        self.waited = []

    def _close_stdin(self):
        self.stdin.closed = True

    def poll(self):
        return None if self._alive else 0

    def wait(self, timeout=None):
        self.waited.append(timeout)
        if self._hangs and not self.killed:
            raise subprocess.TimeoutExpired("java", timeout)
        self._alive = False
        return 0

    def kill(self):
        self.killed = True
        self._hangs = False


class FakeGateway:
    def __init__(self, proc):
        self.proc = proc
        self.shutdowns = 0

    def shutdown(self):
        self.shutdowns += 1


class FakeSession:
    def __init__(self, confs=None, jars=""):
        self._confs = confs or {}
        self._jars = jars
        self.stopped = False
        self.conf = SimpleNamespace(get=lambda key, default=None: self._confs.get(key, default))
        self.sparkContext = SimpleNamespace(
            _jsc=SimpleNamespace(
                sc=lambda: SimpleNamespace(
                    listJars=lambda: SimpleNamespace(mkString=lambda sep: self._jars)
                )
            )
        )

    def stop(self):
        self.stopped = True


DELTA_CONFS = {
    "spark.sql.extensions": helper._DELTA_EXTENSION,
    "spark.sql.catalog.spark_catalog": helper._DELTA_CATALOG,
}


@pytest.fixture
def state(monkeypatch):
    """Patch class-level Spark state with restore-on-exit; return knobs."""
    main = sys.modules["__main__"]
    monkeypatch.setattr(SparkContext, "_gateway", None)
    monkeypatch.setattr(SparkContext, "_jvm", None)
    monkeypatch.setattr(SparkContext, "_active_spark_context", None)
    monkeypatch.setattr(SparkSession, "_instantiatedSession", None)
    monkeypatch.setattr(SparkSession, "_activeSession", None)
    # __main__.spark is managed by hand: the code under test delattr()s it, and
    # monkeypatch's undo would then fail trying to delete it a second time.
    had_main_spark = hasattr(main, "spark")
    saved_main_spark = getattr(main, "spark", None)
    if had_main_spark:
        delattr(main, "spark")

    def set_gateway(gateway):
        monkeypatch.setattr(SparkContext, "_gateway", gateway)
        monkeypatch.setattr(SparkContext, "_jvm", object() if gateway else None)

    def set_active(session_or_exc):
        if isinstance(session_or_exc, Exception):

            def raiser():
                raise session_or_exc

            monkeypatch.setattr(SparkSession, "getActiveSession", staticmethod(raiser))
        else:
            monkeypatch.setattr(
                SparkSession, "getActiveSession", staticmethod(lambda: session_or_exc)
            )

    teardowns = []
    real_teardown = helper._teardown_existing_spark_jvm

    def record_teardown():
        teardowns.append(True)

    yield SimpleNamespace(
        set_gateway=set_gateway,
        set_active=set_active,
        teardowns=teardowns,
        spy_teardown=lambda: monkeypatch.setattr(
            helper, "_teardown_existing_spark_jvm", record_teardown
        ),
        real_teardown=real_teardown,
        main=main,
    )
    if hasattr(main, "spark"):
        delattr(main, "spark")
    if had_main_spark:
        setattr(main, "spark", saved_main_spark)


# ----------------------------------------------------------------- probe


def test_launch_probe_reads_the_gateway_command_line(state):
    state.set_gateway(FakeGateway(FakeProc(DELTA_ARGS)))
    assert helper._gateway_launched_with_delta() is True
    state.set_gateway(FakeGateway(FakeProc(PLAIN_ARGS)))
    assert helper._gateway_launched_with_delta() is False


def test_launch_probe_is_unknown_without_gateway_or_visible_launch(state):
    assert helper._gateway_launched_with_delta() is None
    state.set_gateway(FakeGateway(proc=None))  # PYSPARK_GATEWAY_PORT style
    assert helper._gateway_launched_with_delta() is None


def test_session_probe_uses_jar_list_not_confs(state):
    assert helper._session_can_load_delta(FakeSession(DELTA_CONFS, jars="")) is False
    assert (
        helper._session_can_load_delta(FakeSession({}, jars="a.jar,delta-spark_2.12.jar")) is True
    )


# ----------------------------------------------------------------- preflight


def test_preflight_no_gateway_is_a_noop(state):
    state.spy_teardown()
    assert helper._teardown_non_delta_jvm() is False
    assert state.teardowns == []


def test_preflight_tears_down_a_jar_less_jvm_we_launched(state):
    state.set_gateway(FakeGateway(FakeProc(PLAIN_ARGS)))
    state.set_active(FakeSession({}))
    state.spy_teardown()
    assert helper._teardown_non_delta_jvm() is True
    assert state.teardowns == [True]


@pytest.mark.parametrize("active", [None, "delta"])
def test_preflight_never_tears_down_a_delta_launched_jvm(state, active):
    """Active session or not: a stop() leaves the gateway alive and the next
    getOrCreate() makes a new SparkContext in the same, Delta-capable JVM."""
    state.set_gateway(FakeGateway(FakeProc(DELTA_ARGS)))
    session = FakeSession(DELTA_CONFS) if active else None
    state.set_active(session)
    state.spy_teardown()
    assert helper._teardown_non_delta_jvm() is False
    assert state.teardowns == []
    if session is not None:
        assert session.stopped is False


def test_preflight_rebuilds_a_plain_session_on_a_delta_jvm_without_killing_it(state):
    state.set_gateway(FakeGateway(FakeProc(DELTA_ARGS)))
    plain = FakeSession({"spark.sql.extensions": helper._DELTA_EXTENSION})  # catalog missing
    state.set_active(plain)
    state.main.spark = plain
    state.spy_teardown()
    assert helper._teardown_non_delta_jvm() is False
    assert plain.stopped is True
    assert state.teardowns == []
    assert getattr(state.main, "spark", None) is None


def test_preflight_relaunches_a_dead_gateway_we_launched(state):
    state.set_gateway(FakeGateway(FakeProc(PLAIN_ARGS)))
    state.set_active(RuntimeError("Answer from Java side is empty"))
    state.spy_teardown()
    assert helper._teardown_non_delta_jvm() is True
    assert state.teardowns == [True]


def test_preflight_relaunches_when_our_jvm_exited_without_raising(state):
    """After the last session was stopped, an exited JVM makes getActiveSession()
    return None rather than raise; the stale gateway must still be cleared."""
    state.set_gateway(FakeGateway(FakeProc(DELTA_ARGS, alive=False)))
    state.set_active(None)
    state.spy_teardown()
    assert helper._teardown_non_delta_jvm() is True
    assert state.teardowns == [True]


def test_preflight_reports_a_dead_external_gateway_instead_of_shutting_it_down(state):
    gateway = FakeGateway(proc=None)
    state.set_gateway(gateway)
    state.set_active(RuntimeError("Answer from Java side is empty"))
    state.spy_teardown()
    with pytest.raises(helper.ExternalGatewayUnusable):
        helper._teardown_non_delta_jvm()
    assert state.teardowns == []
    assert gateway.shutdowns == 0


def test_preflight_reports_an_external_gateway_proven_jar_less(state):
    gateway = FakeGateway(proc=None)
    state.set_gateway(gateway)
    state.set_active(FakeSession(DELTA_CONFS, jars="plain.jar"))
    state.spy_teardown()
    with pytest.raises(helper.ExternalGatewayUnusable):
        helper._teardown_non_delta_jvm()
    assert state.teardowns == []
    assert gateway.shutdowns == 0


@pytest.mark.parametrize("active", [None, "delta"])
def test_preflight_keeps_an_external_gateway_that_is_delta_capable_or_unproven(state, active):
    gateway = FakeGateway(proc=None)
    state.set_gateway(gateway)
    session = FakeSession(DELTA_CONFS, jars="delta-spark_2.12.jar") if active else None
    state.set_active(session)
    state.spy_teardown()
    assert helper._teardown_non_delta_jvm() is False
    assert state.teardowns == []
    assert gateway.shutdowns == 0


# ----------------------------------------------------------------- rebuild


def test_rebuild_requires_both_delta_static_confs(state):
    assert helper._rebuild_plain_active_session(FakeSession(DELTA_CONFS)) is False
    only_extension = FakeSession({"spark.sql.extensions": helper._DELTA_EXTENSION})
    assert helper._rebuild_plain_active_session(only_extension) is True
    assert only_extension.stopped is True
    only_catalog = FakeSession({"spark.sql.catalog.spark_catalog": helper._DELTA_CATALOG})
    assert helper._rebuild_plain_active_session(only_catalog) is True


# ----------------------------------------------------------------- teardown + reap


def test_teardown_stops_shuts_down_reaps_and_clears_globals(state):
    proc = FakeProc(PLAIN_ARGS)
    gateway = FakeGateway(proc)
    state.set_gateway(gateway)
    session = FakeSession({})
    state.set_active(session)
    state.main.spark = session
    SparkSession._instantiatedSession = session
    SparkSession._activeSession = session

    state.real_teardown()

    assert session.stopped is True
    assert gateway.shutdowns == 1
    assert SparkContext._gateway is None and SparkContext._jvm is None
    assert proc.stdin.closed is True and proc.poll() is not None and proc.killed is False
    assert SparkSession._instantiatedSession is None and SparkSession._activeSession is None
    assert getattr(state.main, "spark", None) is None


def test_teardown_survives_a_dead_session_lookup(state):
    proc = FakeProc(PLAIN_ARGS)
    state.set_gateway(FakeGateway(proc))
    state.set_active(RuntimeError("Error while sending or receiving"))
    state.real_teardown()
    assert SparkContext._gateway is None
    assert proc.poll() is not None


def test_reap_kills_only_when_the_wait_times_out():
    polite = FakeProc(PLAIN_ARGS)
    helper._reap_gateway_process(polite, timeout=1)
    assert polite.stdin.closed and polite.killed is False and polite.waited == [1]

    stubborn = FakeProc(PLAIN_ARGS, hangs=True)
    helper._reap_gateway_process(stubborn, timeout=1)
    assert stubborn.killed is True and stubborn.poll() is not None

    already_gone = FakeProc(PLAIN_ARGS, alive=False)
    helper._reap_gateway_process(already_gone)
    assert already_gone.waited == [] and already_gone.stdin.closed is False

    helper._reap_gateway_process(None)  # external gateway: nothing to reap
