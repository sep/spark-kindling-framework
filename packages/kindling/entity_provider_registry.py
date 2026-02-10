"""
Entity Provider Registry

Central registry for managing entity provider types using dependency injection.
"""

from typing import Dict, Type

from injector import inject

from .data_entities import EntityMetadata
from .entity_provider import BaseEntityProvider
from .injection import GlobalInjector
from .spark_log_provider import PythonLoggerProvider


@GlobalInjector.singleton_autobind()
class EntityProviderRegistry:
    """
    Registry for entity provider types.

    Manages the mapping from provider_type strings to provider implementations.
    Providers are instantiated via dependency injection to support proper dependency management.
    """

    @inject
    def __init__(self, logger_provider: PythonLoggerProvider):
        self.logger = logger_provider.get_logger("EntityProviderRegistry")
        self._provider_classes: Dict[str, Type[BaseEntityProvider]] = {}
        self._provider_instances: Dict[str, BaseEntityProvider] = {}
        self._register_builtin_providers()

    def register_provider(
        self, provider_type: str, provider_class: Type[BaseEntityProvider]
    ) -> None:
        """
        Register a provider type with its implementation class.

        Args:
            provider_type: Unique identifier for this provider type (e.g., "delta", "csv")
            provider_class: Provider class that implements BaseEntityProvider
        """
        if provider_type in self._provider_classes:
            self.logger.warning(f"Provider type '{provider_type}' already registered, overwriting")

        self._provider_classes[provider_type] = provider_class
        self.logger.info(f"Registered provider type: {provider_type} -> {provider_class.__name__}")

    def get_provider(self, provider_type: str) -> BaseEntityProvider:
        """
        Get provider instance for the given type.

        Uses dependency injection to create provider instances with proper dependency management.
        Providers are cached as singletons.

        Args:
            provider_type: Provider type identifier

        Returns:
            Provider instance

        Raises:
            ValueError: If provider type is not registered
        """
        # Return cached instance if available
        if provider_type in self._provider_instances:
            return self._provider_instances[provider_type]

        # Check if provider type is registered
        if provider_type not in self._provider_classes:
            available = ", ".join(self._provider_classes.keys())
            raise ValueError(
                f"Unknown provider type: '{provider_type}'. "
                f"Available providers: {available}. "
                f"Register custom providers via register_provider()."
            )

        # Get provider class and instantiate via DI
        provider_class = self._provider_classes[provider_type]
        self.logger.debug(f"Instantiating provider: {provider_type} ({provider_class.__name__})")

        try:
            provider_instance = GlobalInjector.get(provider_class)
            self._provider_instances[provider_type] = provider_instance
            self.logger.info(f"Created provider instance: {provider_type}")
            return provider_instance

        except Exception as e:
            self.logger.error(f"Failed to instantiate provider '{provider_type}': {e}")
            raise ValueError(
                f"Failed to create provider instance for type '{provider_type}': {e}"
            ) from e

    def get_provider_for_entity(self, entity_metadata: EntityMetadata) -> BaseEntityProvider:
        """
        Get appropriate provider for an entity based on its metadata.

        Reads provider_type from entity tags. Falls back to 'delta' provider if not specified
        (backward compatibility).

        Args:
            entity_metadata: Entity metadata

        Returns:
            Provider instance

        Raises:
            ValueError: If specified provider type is not registered
        """
        provider_type = entity_metadata.tags.get("provider_type", "delta")

        self.logger.debug(
            f"Resolving provider for entity '{entity_metadata.entityid}': type={provider_type}"
        )

        return self.get_provider(provider_type)

    def list_registered_providers(self) -> list[str]:
        """
        List all registered provider types.

        Returns:
            List of provider type identifiers
        """
        return list(self._provider_classes.keys())

    def _register_builtin_providers(self) -> None:
        """
        Register built-in provider types.

        This is called during initialization to make core providers available.
        """
        # Import here to avoid circular dependencies
        from .entity_provider_delta import DeltaEntityProvider

        # Register Delta provider (always available)
        self.register_provider("delta", DeltaEntityProvider)

        # Register other built-in providers
        # Note: These will be registered after they're implemented
        try:
            from .entity_provider_csv import CSVEntityProvider

            self.register_provider("csv", CSVEntityProvider)
        except ImportError:
            self.logger.debug("CSV provider not available")

        try:
            from .entity_provider_eventhub import EventHubEntityProvider

            self.register_provider("eventhub", EventHubEntityProvider)
        except ImportError:
            self.logger.debug("EventHub provider not available")

        try:
            from .entity_provider_memory import MemoryEntityProvider

            self.register_provider("memory", MemoryEntityProvider)
        except ImportError:
            self.logger.debug("Memory provider not available")

        self.logger.info(f"Registered {len(self._provider_classes)} built-in provider(s)")
