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

import logging
import threading
import uuid
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple, Type

from injector import inject

from kindling.injection import GlobalInjector

_signals_logger = logging.getLogger("kindling.signals")

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


# =============================================================================
# DataSignals — registry-based signal handler declarations
# =============================================================================


@dataclass
class _HandlerEntry:
    func: Callable
    signal_name: str
    mode: Literal["sync", "async"] = "sync"
    priority: int = 50
    on_error: Literal["raise", "log"] = "raise"
    pipe_id: Optional[str] = None


class DataSignals:
    """Registry for declaring signal handlers with decorator syntax.

    Analogous to ``DataEntities`` and ``DataPipes`` — handlers are declared
    once with a decorator and the framework wires them into the signal system.

    Two execution modes:

    - ``sync`` (default): runs before pipeline continues, can transform the
      DataFrame by returning a new one, can abort by raising.
    - ``async``: fire-and-forget via thread pool; pipeline does not wait,
      exceptions are logged, return values are ignored.

    Example::

        @DataSignals.handler("persist.before_persist", priority=10)
        def validate_amounts(sender, df, pipe_id, **kwargs):
            if df.filter(col("amount") < 0).count() > 0:
                raise ValueError("negative amounts")

        @DataSignals.handler("persist.before_persist", priority=20)
        def add_audit_column(sender, df, **kwargs):
            return df.withColumn("_loaded_at", current_timestamp())

        @DataSignals.handler("persist.after_persist",
                             mode="async", on_error="log")
        def log_row_count(sender, df, pipe_id, **kwargs):
            metrics.record(pipe_id, df.count())
    """

    _signal_provider: Optional[SignalProvider] = None
    _handlers: List[_HandlerEntry] = []
    _dispatcher_funcs: Dict[str, Callable] = {}
    _executor: Optional[ThreadPoolExecutor] = None
    _lock: threading.Lock = threading.Lock()

    @classmethod
    def reset(cls) -> None:
        """Disconnect all handlers and clear the registry. Use between tests."""
        with cls._lock:
            if cls._signal_provider is not None:
                for signal_name, dispatcher in cls._dispatcher_funcs.items():
                    sig = cls._signal_provider.get_signal(signal_name)
                    if sig is not None:
                        try:
                            sig.disconnect(dispatcher)
                        except Exception:
                            pass
            cls._handlers = []
            cls._dispatcher_funcs = {}
            cls._signal_provider = None
            if cls._executor is not None:
                cls._executor.shutdown(wait=False)
                cls._executor = None

    @classmethod
    def _get_provider(cls) -> SignalProvider:
        if cls._signal_provider is None:
            try:
                cls._signal_provider = GlobalInjector.get(SignalProvider)
            except Exception as exc:
                raise RuntimeError(
                    "DataSignals.handler decorator fired before initialize() was called. "
                    "Import handler modules after initialize()."
                ) from exc
        return cls._signal_provider

    @classmethod
    def _get_executor(cls) -> ThreadPoolExecutor:
        if cls._executor is None:
            cls._executor = ThreadPoolExecutor(
                max_workers=4, thread_name_prefix="kindling-signal-async"
            )
        return cls._executor

    @classmethod
    def _ensure_dispatcher(cls, signal_name: str) -> None:
        if signal_name in cls._dispatcher_funcs:
            return
        provider = cls._get_provider()
        sig = provider.get_signal(signal_name) or provider.create_signal(signal_name)

        def dispatcher(sender, **kwargs):
            return cls._run_handlers(signal_name, sender, **kwargs)

        cls._dispatcher_funcs[signal_name] = dispatcher
        sig.connect(dispatcher, weak=False)

    @classmethod
    def _run_handlers(cls, signal_name: str, sender, **kwargs) -> Optional[Any]:
        pipe_id = kwargs.get("pipe_id")

        entries = [
            e
            for e in cls._handlers
            if e.signal_name == signal_name and (e.pipe_id is None or e.pipe_id == pipe_id)
        ]

        async_entries = [e for e in entries if e.mode == "async"]
        sync_entries = sorted([e for e in entries if e.mode == "sync"], key=lambda e: e.priority)

        for entry in async_entries:
            cls._get_executor().submit(cls._run_async, entry, sender, kwargs.copy())

        df = kwargs.get("df")
        for entry in sync_entries:
            try:
                result = entry.func(sender, **kwargs)
                if result is not None and hasattr(result, "schema"):
                    df = result
                    kwargs = {**kwargs, "df": df}
            except Exception as exc:
                if entry.on_error == "log":
                    _signals_logger.warning(
                        "Signal handler %r for %r failed: %s",
                        entry.func.__name__,
                        signal_name,
                        exc,
                    )
                else:
                    raise

        return df

    @classmethod
    def _run_async(cls, entry: _HandlerEntry, sender, kwargs: dict) -> None:
        try:
            entry.func(sender, **kwargs)
        except Exception as exc:
            _signals_logger.warning(
                "Async signal handler %r for %r failed: %s",
                entry.func.__name__,
                entry.signal_name,
                exc,
            )

    @classmethod
    def handler(
        cls,
        signal_name: str,
        *,
        mode: Literal["sync", "async"] = "sync",
        priority: int = 50,
        on_error: Literal["raise", "log"] = "raise",
        pipe_id: Optional[str] = None,
    ) -> Callable:
        """Register a signal handler.

        Args:
            signal_name: Signal to subscribe to (e.g. ``"persist.before_persist"``).
            mode: ``"sync"`` blocks the pipeline and can transform/abort;
                  ``"async"`` is fire-and-forget via thread pool.
            priority: Execution order among sync handlers (lower = earlier).
                      Ignored for async handlers.
            on_error: ``"raise"`` propagates exceptions (default);
                      ``"log"`` logs and continues. Ignored for async handlers
                      (always logs).
            pipe_id: Scope handler to a specific pipe ID. ``None`` matches all pipes.
        """

        def decorator(func: Callable) -> Callable:
            entry = _HandlerEntry(
                func=func,
                signal_name=signal_name,
                mode=mode,
                priority=priority,
                on_error=on_error,
                pipe_id=pipe_id,
            )
            with cls._lock:
                cls._handlers.append(entry)
                cls._ensure_dispatcher(signal_name)
            return func

        return decorator
