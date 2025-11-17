"""Unit tests for signaling module."""

# Import to ensure decorator runs
import kindling.signaling  # noqa: F401
import pytest
from kindling.injection import GlobalInjector
from kindling.signaling import (
    BlinkerSignalProvider,
    BlinkerSignalWrapper,
    Signal,
    SignalProvider,
)


@pytest.fixture(autouse=True)
def reset_injector():
    """Reset injector before each test."""
    GlobalInjector.reset()
    # Re-import to re-register after reset
    import importlib

    importlib.reload(kindling.signaling)
    yield
    GlobalInjector.reset()


class TestBlinkerSignalProvider:
    """Test BlinkerSignalProvider implementation."""

    def test_initialization(self):
        """BlinkerSignalProvider should initialize successfully."""
        provider = BlinkerSignalProvider()
        assert provider is not None
        assert hasattr(provider, "_namespace")
        assert hasattr(provider, "_signals")

    def test_create_signal(self):
        """create_signal should create a new signal."""
        provider = BlinkerSignalProvider()
        signal = provider.create_signal("test.signal", doc="Test signal")

        assert signal is not None
        # Check the type name since isinstance has module loading issues in tests
        assert type(signal).__name__ == "BlinkerSignalWrapper"

    def test_create_signal_returns_same_instance(self):
        """create_signal should return same instance for same name."""
        provider = BlinkerSignalProvider()
        signal1 = provider.create_signal("test.signal")
        signal2 = provider.create_signal("test.signal")

        assert signal1 is signal2

    def test_get_signal_returns_existing(self):
        """get_signal should return existing signal."""
        provider = BlinkerSignalProvider()
        signal = provider.create_signal("test.signal")

        retrieved = provider.get_signal("test.signal")
        assert retrieved is signal

    def test_get_signal_returns_none_if_not_exists(self):
        """get_signal should return None if signal doesn't exist."""
        provider = BlinkerSignalProvider()

        retrieved = provider.get_signal("nonexistent.signal")
        assert retrieved is None

    def test_singleton_autobind(self):
        """BlinkerSignalProvider should be registered as singleton."""
        # Use concrete class since ABC can't be instantiated
        provider1 = GlobalInjector.get(BlinkerSignalProvider)
        provider2 = GlobalInjector.get(BlinkerSignalProvider)

        assert provider1 is provider2
        assert isinstance(provider1, BlinkerSignalProvider)


class TestBlinkerSignalWrapper:
    """Test BlinkerSignalWrapper implementation."""

    @pytest.fixture
    def provider(self):
        """Provide a signal provider."""
        return BlinkerSignalProvider()

    @pytest.fixture
    def signal(self, provider):
        """Provide a test signal."""
        return provider.create_signal("test.signal")

    def test_connect_and_send(self, signal):
        """Signal should connect receiver and send to it."""
        received = []

        def receiver(sender, **kwargs):
            received.append((sender, kwargs))

        signal.connect(receiver)
        signal.send("test_sender", data="test_data")

        assert len(received) == 1
        assert received[0][0] == "test_sender"
        assert received[0][1] == {"data": "test_data"}

    def test_send_returns_results(self, signal):
        """Signal.send should return list of (receiver, result) tuples."""

        def receiver1(sender, **kwargs):
            return "result1"

        def receiver2(sender, **kwargs):
            return "result2"

        signal.connect(receiver1)
        signal.connect(receiver2)

        results = signal.send("sender")

        assert len(results) == 2
        assert "result1" in [r[1] for r in results]
        assert "result2" in [r[1] for r in results]

    def test_disconnect(self, signal):
        """Signal should disconnect receiver."""
        received = []

        def receiver(sender, **kwargs):
            received.append(kwargs)

        signal.connect(receiver)
        signal.send("sender", data="first")

        signal.disconnect(receiver)
        signal.send("sender", data="second")

        assert len(received) == 1
        assert received[0] == {"data": "first"}

    def test_sender_filtering(self, signal):
        """Signal should only call receiver for specific sender."""
        received = []

        def receiver(sender, **kwargs):
            received.append(sender)

        signal.connect(receiver, sender="specific_sender")

        signal.send("other_sender", data="test")
        assert len(received) == 0

        signal.send("specific_sender", data="test")
        assert len(received) == 1
        assert received[0] == "specific_sender"

    def test_has_receivers_for(self, signal):
        """Signal should check if receivers exist for sender."""

        def receiver(sender, **kwargs):
            pass

        assert not signal.has_receivers_for("any_sender")

        signal.connect(receiver)
        assert signal.has_receivers_for("any_sender")

    def test_muted_context(self, signal):
        """Signal should not emit when muted."""
        received = []

        def receiver(sender, **kwargs):
            received.append(kwargs)

        signal.connect(receiver)

        with signal.muted():
            signal.send("sender", data="muted")

        assert len(received) == 0

        signal.send("sender", data="unmuted")
        assert len(received) == 1
        assert received[0] == {"data": "unmuted"}

    def test_connected_to_context(self, signal):
        """Signal should temporarily connect in context."""
        received = []

        def receiver(sender, **kwargs):
            received.append(kwargs)

        signal.send("sender", data="before")
        assert len(received) == 0

        with signal.connected_to(receiver):
            signal.send("sender", data="during")

        signal.send("sender", data="after")

        assert len(received) == 1
        assert received[0] == {"data": "during"}

    def test_multiple_receivers(self, signal):
        """Signal should call all connected receivers."""
        results = []

        def receiver1(sender, **kwargs):
            results.append("receiver1")

        def receiver2(sender, **kwargs):
            results.append("receiver2")

        def receiver3(sender, **kwargs):
            results.append("receiver3")

        signal.connect(receiver1)
        signal.connect(receiver2)
        signal.connect(receiver3)

        signal.send("sender")

        assert len(results) == 3
        assert "receiver1" in results
        assert "receiver2" in results
        assert "receiver3" in results


class TestSignalIntegration:
    """Integration tests for signal usage patterns."""

    def test_service_based_signal_pattern(self):
        """Test service owning and emitting signals."""
        # Simulate a service with signals
        provider = BlinkerSignalProvider()

        # Service creates its signals
        before_operation = provider.create_signal(
            "service.before_operation", doc="Emitted before operation"
        )
        after_operation = provider.create_signal(
            "service.after_operation", doc="Emitted after operation"
        )

        # Plugin connects to signals
        events = []

        def on_before(sender, **kwargs):
            events.append(("before", kwargs))

        def on_after(sender, **kwargs):
            events.append(("after", kwargs))

        before_operation.connect(on_before)
        after_operation.connect(on_after)

        # Service emits signals
        before_operation.send("service", operation="test")
        # ... do operation ...
        after_operation.send("service", operation="test", result="success")

        assert len(events) == 2
        assert events[0][0] == "before"
        assert events[0][1]["operation"] == "test"
        assert events[1][0] == "after"
        assert events[1][1]["result"] == "success"

    def test_data_modification_pattern(self):
        """Test modifying data via signal."""
        provider = BlinkerSignalProvider()
        signal = provider.create_signal("data.transforming")

        # Plugin that modifies data
        def add_timestamp(sender, context, **kwargs):
            context["timestamp"] = "2025-01-01"

        def add_user(sender, context, **kwargs):
            context["user"] = "test_user"

        signal.connect(add_timestamp)
        signal.connect(add_user)

        # Emit with mutable context
        context = {"original": "data"}
        signal.send("service", context=context)

        assert context["timestamp"] == "2025-01-01"
        assert context["user"] == "test_user"
        assert context["original"] == "data"
