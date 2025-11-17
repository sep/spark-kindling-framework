"""Signal-based pub/sub mechanism for Kindling framework.

This module provides an abstraction layer for in-process pub/sub eventing,
allowing components to emit and listen to signals without tight coupling.

The abstraction allows swapping underlying implementations (currently Blinker)
while maintaining a consistent API throughout the framework.
"""

from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Any, Callable, Dict, List, Optional, Tuple

from injector import inject
from kindling.injection import GlobalInjector


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
