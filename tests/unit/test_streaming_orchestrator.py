from types import SimpleNamespace

from kindling.execution_strategy import ExecutionPlan, Generation
from kindling.generation_executor import ErrorStrategy
from kindling.signaling import BlinkerSignalProvider
from kindling.streaming_orchestrator import StreamingOrchestrator
from kindling.streaming_query_manager import StreamingQueryInfo, StreamingQueryState


class FakeLogger:
    def info(self, *args, **kwargs):
        return None

    def warning(self, *args, **kwargs):
        return None

    def error(self, *args, **kwargs):
        return None

    def debug(self, *args, **kwargs):
        return None


class FakeLoggerProvider:
    def get_logger(self, _name):
        return FakeLogger()


class FakeStreams:
    def __init__(self):
        self.listeners = []

    def addListener(self, listener):
        self.listeners.append(listener)

    def removeListener(self, listener):
        if listener in self.listeners:
            self.listeners.remove(listener)


class FakeSpark:
    def __init__(self):
        self.streams = FakeStreams()


class FakeStreamingQuery:
    def __init__(self, query_id, run_id):
        self.id = query_id
        self.runId = run_id
        self.isActive = True
        self._exception = None
        self.await_calls = 0
        self.stop_calls = 0

    def stop(self):
        self.stop_calls += 1
        self.isActive = False

    def awaitTermination(self):
        self.await_calls += 1
        self.isActive = False

    def exception(self):
        return self._exception


class FakePipeStreamStarter:
    def __init__(self):
        self.calls = []

    def start_pipe_stream(self, pipeid, options=None):
        self.calls.append((pipeid, dict(options or {})))
        return FakeStreamingQuery(query_id=f"query-{pipeid}", run_id=f"run-{pipeid}")


class FakeStreamingQueryManager:
    def __init__(self):
        self.spark = FakeSpark()
        self._builders = {}
        self._queries = {}

    def register_query(self, query_id, builder_fn, config=None):
        if query_id in self._queries:
            raise ValueError(f"Query '{query_id}' already registered")
        self._builders[query_id] = (builder_fn, config or {})
        self._queries[query_id] = StreamingQueryInfo(
            query_id=query_id,
            query_name=(config or {}).get("query_name", query_id),
            state=StreamingQueryState.REGISTERED,
            config=config or {},
        )
        return self._queries[query_id]

    def start_query(self, query_id):
        builder_fn, config = self._builders[query_id]
        query = builder_fn(self.spark, config)
        info = self._queries[query_id]
        info.spark_query = query
        info.state = StreamingQueryState.ACTIVE
        return info

    def stop_query(self, query_id, await_termination=True):
        info = self._queries[query_id]
        info.spark_query.stop()
        if await_termination:
            info.spark_query.awaitTermination()
        info.state = StreamingQueryState.STOPPED
        return info

    def get_query_status(self, query_id):
        return self._queries[query_id]

    def resolve_registered_query_id(self, query_id_or_spark_id):
        if query_id_or_spark_id in self._queries:
            return query_id_or_spark_id
        for query_id, info in self._queries.items():
            if info.spark_query_id == query_id_or_spark_id:
                return query_id
        return None


class FakeManagedService:
    def __init__(self):
        self.started = 0
        self.stopped = 0

    def start(self):
        self.started += 1

    def stop(self, *args, **kwargs):
        self.stopped += 1
        return True


def make_plan():
    graph = SimpleNamespace(get_dependencies=lambda _pipe_id: [])
    return ExecutionPlan(
        pipe_ids=["pipe_a", "pipe_b"],
        generations=[
            Generation(number=0, pipe_ids=["pipe_a"]),
            Generation(number=1, pipe_ids=["pipe_b"]),
        ],
        graph=graph,
        strategy="streaming",
        metadata={},
    )


def make_orchestrator():
    listener = FakeManagedService()
    health = FakeManagedService()
    recovery = FakeManagedService()
    return StreamingOrchestrator(
        pipe_stream_starter=FakePipeStreamStarter(),
        query_manager=FakeStreamingQueryManager(),
        streaming_listener=listener,
        health_monitor=health,
        recovery_manager=SimpleNamespace(
            start=recovery.start,
            stop=recovery.stop,
            get_all_recovery_states=lambda: {},
        ),
        logger_provider=FakeLoggerProvider(),
        signal_provider=BlinkerSignalProvider(),
    )


def test_start_runtime_registers_listener_and_starts_services():
    orchestrator = make_orchestrator()

    orchestrator.start_runtime()

    assert len(orchestrator.query_manager.spark.streams.listeners) == 1
    assert orchestrator.get_status().listener_registered is True


def test_start_starts_runtime_and_queries_in_plan_order():
    orchestrator = make_orchestrator()
    plan = make_plan()

    result = orchestrator.start(
        plan,
        streaming_options={"pipe_a": {"alpha": 1}, "pipe_b": {"beta": 2}},
        error_strategy=ErrorStrategy.FAIL_FAST,
    )

    assert list(result.streaming_queries.keys()) == ["pipe_a", "pipe_b"]
    assert orchestrator.pipe_stream_starter.calls == [
        ("pipe_a", {"alpha": 1}),
        ("pipe_b", {"beta": 2}),
    ]
    assert len(orchestrator.query_manager.spark.streams.listeners) == 1
    assert orchestrator.get_status().active_query_count == 2


def test_await_termination_marks_queries_stopped():
    orchestrator = make_orchestrator()
    result = orchestrator.start(make_plan())

    for query in result.streaming_queries.values():
        query.isActive = False

    completed = orchestrator.await_termination(timeout=1.0, poll_interval=0.01)

    assert completed is result
    assert orchestrator.get_status().query_states == {
        "pipe_a": StreamingQueryState.STOPPED.value,
        "pipe_b": StreamingQueryState.STOPPED.value,
    }


def test_stop_stops_queries_and_runtime_components():
    orchestrator = make_orchestrator()
    result = orchestrator.start(make_plan())

    queries = list(result.streaming_queries.values())
    orchestrator.stop(await_termination=True)

    assert all(query.stop_calls == 1 for query in queries)
    assert all(query.await_calls == 1 for query in queries)
    assert orchestrator.get_status().active_query_count == 0
