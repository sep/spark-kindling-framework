import inspect
from abc import ABC

from injector import Injector, SingletonScope, singleton


class GlobalInjector:
    _instance = None

    @classmethod
    def get_instance_id(cls):
        return id(cls.get_injector())

    @classmethod
    def get_injector(cls):
        import __main__

        if not hasattr(__main__, "_global_injector_instance"):
            __main__._global_injector_instance = Injector(auto_bind=True)
        return __main__._global_injector_instance

    @classmethod
    def reset(cls):
        """Reset the global injector instance. Use this in tests or when you need to reinitialize."""
        import __main__

        if hasattr(__main__, "_global_injector_instance"):
            delattr(__main__, "_global_injector_instance")

    @classmethod
    def add_global(cls, key, value):
        globals()[key] = value

    @classmethod
    def get(cls, interface):
        # print(f"Injector ID: {id(cls.get_injector())}")
        return cls.get_injector().get(interface)

    @classmethod
    def bind(cls, interface, implementation):
        # print("Calling bind on GI ...")
        cls.get_injector().binder.bind(interface, to=implementation)

    @classmethod
    def rebind_singleton(cls, interface, implementation):
        """Bind ``interface`` to ``implementation`` in singleton scope, replacing
        both the earlier binding and any instance already resolved for it.

        ``binder.bind`` alone only replaces the binding: injector's
        ``SingletonScope`` caches each resolved instance by interface key, so an
        interface resolved before the rebind keeps returning the old instance.
        This also evicts that cached instance, so the next resolution uses
        ``implementation``. Objects that already hold the old instance (for
        example through constructor injection, or a logger obtained from it)
        keep it; only later resolutions see the new binding.
        """
        injector = cls.get_injector()
        injector.binder.bind(interface, to=implementation, scope=singleton)
        try:
            scope_binding, _ = injector.binder.get_binding(SingletonScope)
            cached = getattr(scope_binding.provider.get(injector), "_context", None)
        except Exception:  # noqa: BLE001 - eviction is best effort across injector versions
            return
        if isinstance(cached, dict):
            cached.pop(interface, None)

    @classmethod
    def autobind(cls, *interfaces):
        def decorator(cls_to_bind):
            # If no explicit interfaces provided, find parent interfaces
            if not interfaces:
                # Get all direct parent classes
                bases = cls_to_bind.__bases__
                # Filter for those that are abstract
                for base in bases:
                    if inspect.isabstract(base):
                        cls.bind(base, cls_to_bind)
            else:
                # Bind to explicitly provided interfaces
                for interface in interfaces:
                    cls.bind(interface, cls_to_bind)

            return cls_to_bind

        return decorator

    @classmethod
    def singleton_autobind(cls, *interfaces):
        # print("singleton_autobind invoked ...")
        def decorator(cls_to_bind):
            # First apply singleton to create a singleton-wrapped class
            singleton_cls = singleton(cls_to_bind)

            # Then bind the singleton-wrapped class to each interface with explicit scope
            for interface in interfaces or [
                base for base in cls_to_bind.__bases__ if inspect.isabstract(base)
            ]:
                cls.get_injector().binder.bind(
                    interface,
                    to=singleton_cls,
                    scope=singleton,  # Explicitly setting singleton scope
                )

            # Also bind the class to itself with explicit scope
            cls.get_injector().binder.bind(cls_to_bind, to=singleton_cls, scope=singleton)

            return singleton_cls

        return decorator


def get_kindling_service(iface):
    return GlobalInjector.get(iface)
