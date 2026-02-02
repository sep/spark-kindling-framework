"""Signal-based pub/sub mechanism for Kindling framework.

This module provides an abstraction layer for in-process pub/sub eventing,
allowing components to emit and listen to signals without tight coupling.

The abstraction allows swapping underlying implementations (currently Blinker)
while maintaining a consistent API throughout the framework.

Key components:
- Signal: Abstract signal interface
- SignalProvider: Creates and manages signals
- SignalEmitter: Mixin for classes that emit signals
- BlinkerSignalProvider: Default implementation using Blinker library
"""

import uuid
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple, Type

from injector import inject
from kindling.injection import GlobalInjector

# =============================================================================
# Signal Payload Base Classes
# =============================================================================


@dataclass
class SignalPayload:
    """Base class for typed signal payloads.

    Provides common fields for all signal payloads including
    operation tracking and timestamps.

    Example:
        @dataclass
        class PipeRunPayload(SignalPayload):
            pipe_ids: List[str]
            pipe_count: int
    """

    operation_id: str = None
    timestamp: str = None

    def __post_init__(self):
        if self.operation_id is None:
            self.operation_id = str(uuid.uuid4())
        if self.timestamp is None:
            self.timestamp = datetime.utcnow().isoformat()

    def to_dict(self) -> Dict[str, Any]:
        """Convert payload to dictionary for signal emission."""
        return {k: v for k, v in vars(self).items() if v is not None}


# =============================================================================
# Signal Emitter Mixin
# =============================================================================


class SignalEmitter:
    """Mixin class that provides emit() helper for services.

    Services inherit from this to gain easy signal emission capabilities.
    The EMITS class attribute documents which signals the class can emit.

    Example:
        class MyService(SignalEmitter):
            EMITS = [
                "myservice.before_operation",
                "myservice.after_operation",
                "myservice.operation_failed",
            ]

            def do_operation(self):
                self.emit("myservice.before_operation", item_count=10)
                try:
                    # ... do work ...
                    self.emit("myservice.after_operation", duration=1.5)
                except Exception as e:
                    self.emit("myservice.operation_failed", error=str(e))
                    raise
    """

    # Declare signals this class emits (for documentation/introspection)
    EMITS: List[str] = []

    def _init_signal_emitter(self, signal_provider: Optional["SignalProvider"] = None):
        """Initialize signal emitter capabilities.

        Call this in __init__ of classes that use SignalEmitter mixin.

        Args:
            signal_provider: Optional SignalProvider instance
        """
        self._signal_provider: Optional["SignalProvider"] = signal_provider

    def set_signal_provider(self, provider: "SignalProvider"):
        """Set the signal provider for this emitter.

        Args:
            provider: SignalProvider instance to use for emissions
        """
        self._signal_provider = provider

    def emit(self, signal_name: str, **kwargs) -> List[Tuple[Callable, Any]]:
        """Emit a signal with the given payload.

        Signals are created lazily on first use. If no signal provider
        is configured, emissions are silently ignored.

        Args:
            signal_name: Name of the signal (e.g., 'datapipes.before_run')
            **kwargs: Signal payload fields

        Returns:
            List of (receiver, return_value) tuples from handlers,
            or empty list if no provider configured
        """
        if not hasattr(self, "_signal_provider") or self._signal_provider is None:
            return []

        signal = self._signal_provider.get_signal(signal_name)
        if signal is None:
            # Create signal on first use (lazy creation)
            signal = self._signal_provider.create_signal(signal_name)

        return signal.send(self, **kwargs)

    def get_emitted_signals(self) -> List[str]:
        """Return list of signals this emitter can emit.

        Returns:
            Copy of the EMITS class attribute
        """
        return self.EMITS.copy()


class Signal(ABC):
    """Abstract signal interface - implementation agnostic.

    Signals allow components to emit events that other components can listen to,
    enabling loose coupling and extensibility through the observer pattern.
    """

    @abstractmethod
    def connect(self, receiver: Callable, sender: Any = None, weak: bool = True) -> Any:
        """Connect a receiver function to this signal.

        Args:
            receiver: Callable to invoke when signal is emitted.
                     Signature: receiver(sender, **kwargs) -> Any
            sender: Only call receiver for this specific sender (None = any sender)
            weak: Use weak reference (auto-disconnect when receiver is garbage collected)

        Returns:
            Connection handle (implementation-specific)
        """
        pass

    @abstractmethod
    def disconnect(self, receiver: Callable, sender: Any = None) -> None:
        """Disconnect a receiver from this signal.

        Args:
            receiver: The receiver function to disconnect
            sender: Only disconnect for this specific sender (None = any sender)
        """
        pass

    @abstractmethod
    def send(self, sender: Any, **kwargs) -> List[Tuple[Callable, Any]]:
        """Emit the signal to all connected receivers.

        Args:
            sender: Object emitting the signal
            **kwargs: Signal payload passed to receivers

        Returns:
            List of (receiver, return_value) tuples
        """
        pass

    @abstractmethod
    def has_receivers_for(self, sender: Any) -> bool:
        """Check if signal has receivers for given sender.

        Args:
            sender: Sender to check for receivers

        Returns:
            True if at least one receiver is connected for this sender
        """
        pass

    @abstractmethod
    @contextmanager
    def muted(self):
        """Context manager to temporarily disable signal emission.

        Example:
            with signal.muted():
                # Signal emissions are suppressed
                service.do_something()  # Won't emit signals
        """
        pass

    @abstractmethod
    @contextmanager
    def connected_to(self, receiver: Callable, sender: Any = None):
        """Context manager for temporary connection.

        Useful for testing or temporary event handling.

        Example:
            with signal.connected_to(my_handler):
                # Handler is connected only within this block
                service.do_something()  # Will call my_handler
        """
        pass


class SignalProvider(ABC):
    """Abstract signal provider - creates and manages signals.

    This abstraction allows the underlying signal implementation to be
    swapped (e.g., Blinker, custom, different library) without affecting
    consuming code.
    """

    @abstractmethod
    def create_signal(self, name: str, doc: Optional[str] = None) -> Signal:
        """Create or retrieve a named signal.

        Args:
            name: Signal name (for debugging/introspection)
            doc: Documentation string describing the signal

        Returns:
            Signal instance
        """
        pass

    @abstractmethod
    def get_signal(self, name: str) -> Optional[Signal]:
        """Get existing signal by name.

        Args:
            name: Signal name

        Returns:
            Signal instance if exists, None otherwise
        """
        pass


class BlinkerSignalWrapper(Signal):
    """Wrapper around Blinker's Signal implementation.

    Adapts Blinker's signal API to match our abstract Signal interface.
    """

    def __init__(self, blinker_signal):
        """Initialize wrapper with a Blinker signal.

        Args:
            blinker_signal: blinker.Signal instance
        """
        self._signal = blinker_signal

    def connect(self, receiver: Callable, sender: Any = None, weak: bool = True) -> Any:
        """Connect receiver to Blinker signal."""
        try:
            from blinker import ANY

            actual_sender = sender if sender is not None else ANY
            return self._signal.connect(receiver, sender=actual_sender, weak=weak)
        except ImportError:
            raise RuntimeError("Blinker library not installed. Install with: pip install blinker")

    def disconnect(self, receiver: Callable, sender: Any = None) -> None:
        """Disconnect receiver from Blinker signal."""
        try:
            from blinker import ANY

            actual_sender = sender if sender is not None else ANY
            self._signal.disconnect(receiver, sender=actual_sender)
        except ImportError:
            raise RuntimeError("Blinker library not installed. Install with: pip install blinker")

    def send(self, sender: Any, **kwargs) -> List[Tuple[Callable, Any]]:
        """Send signal via Blinker."""
        return self._signal.send(sender, **kwargs)

    def has_receivers_for(self, sender: Any) -> bool:
        """Check if Blinker signal has receivers."""
        return self._signal.has_receivers_for(sender)

    @contextmanager
    def muted(self):
        """Temporarily mute Blinker signal."""
        with self._signal.muted():
            yield

    @contextmanager
    def connected_to(self, receiver: Callable, sender: Any = None):
        """Temporarily connect to Blinker signal."""
        try:
            from blinker import ANY

            actual_sender = sender if sender is not None else ANY
            with self._signal.connected_to(receiver, sender=actual_sender):
                yield
        except ImportError:
            raise RuntimeError("Blinker library not installed. Install with: pip install blinker")


@GlobalInjector.singleton_autobind()
class BlinkerSignalProvider(SignalProvider):
    """Blinker-based implementation of SignalProvider.

    Uses Blinker's Namespace to manage named signals.
    This is the default signal provider for the Kindling framework.
    """

    def __init__(self):
        """Initialize Blinker signal provider."""
        try:
            from blinker import Namespace

            self._namespace = Namespace()
            self._signals: Dict[str, BlinkerSignalWrapper] = {}
        except ImportError:
            raise RuntimeError(
                "Blinker library not installed. "
                "Install with: pip install blinker\n"
                "Or add 'blinker' to your requirements.txt"
            )

    def create_signal(self, name: str, doc: Optional[str] = None) -> Signal:
        """Create or retrieve a named Blinker signal.

        If signal with given name already exists, returns the existing signal.

        Args:
            name: Signal name (e.g., 'file_ingestion.before_file_read')
            doc: Documentation string

        Returns:
            BlinkerSignalWrapper instance
        """
        if name in self._signals:
            return self._signals[name]

        # Create Blinker signal with documentation
        blinker_sig = self._namespace.signal(name, doc=doc)
        wrapped = BlinkerSignalWrapper(blinker_sig)
        self._signals[name] = wrapped
        return wrapped

    def get_signal(self, name: str) -> Optional[Signal]:
        """Get existing signal by name.

        Args:
            name: Signal name

        Returns:
            Signal if exists, None otherwise
        """
        return self._signals.get(name)
