"""
Integration tests for ConfigService with real Spark sessions.

These tests verify singleton behavior, module reloading, and end-to-end
configuration lifecycle with actual Spark initialization.
"""

import pytest
from kindling.injection import GlobalInjector, get_kindling_service
from kindling.spark_config import (
    ConfigService,
    DynaconfConfig,
    configure_injector_with_config,
)

# Mark all tests in this module as integration tests
pytestmark = [pytest.mark.integration]


class TestConfigureInjectorFunction:
    """Integration tests for configure_injector_with_config with real Spark"""

    def setup_method(self):
        """Clean up before each test"""
        GlobalInjector.reset()

    def teardown_method(self):
        """Clean up after each test"""
        GlobalInjector.reset()

    def test_configure_injector_creates_singleton(self):
        """Test that configure_injector_with_config sets up singleton with real Spark"""
        # Re-import to re-register decorators after clearing injector
        import importlib

        import kindling.spark_config

        importlib.reload(kindling.spark_config)
        from kindling.spark_config import (
            ConfigService,
            DynaconfConfig,
            configure_injector_with_config,
        )

        configure_injector_with_config(
            config_files=None,
            initial_config={"log_level": "INFO"},
            environment="test",
        )

        # Verify singleton was created and initialized
        config_service = get_kindling_service(ConfigService)

        assert config_service is not None, "Should create ConfigService singleton"
        assert isinstance(config_service, DynaconfConfig), "Should be DynaconfConfig instance"
        assert config_service.spark is not None, "Should have real Spark session"

    def test_configure_injector_with_all_parameters(self):
        """Test configure_injector_with_config with all parameters and real Spark"""
        # Re-import to re-register decorators after clearing injector
        import importlib

        import kindling.spark_config

        importlib.reload(kindling.spark_config)
        from kindling.spark_config import (
            ConfigService,
            DynaconfConfig,
            configure_injector_with_config,
        )

        initial_config = {"log_level": "DEBUG", "print_logging": True}

        configure_injector_with_config(
            config_files=None,  # No config files for integration test
            initial_config=initial_config,
            environment="test",
            artifacts_storage_path=None,
        )

        # Verify configuration was applied
        config_service = get_kindling_service(ConfigService)

        assert config_service.initial_config == initial_config, "Should apply initial config"
        assert config_service.spark is not None, "Should have real Spark session"
        assert config_service.dynaconf is not None, "Should have Dynaconf instance"


class TestConfigIntegration:
    """Integration tests for complete config lifecycle with real Spark"""

    def setup_method(self):
        """Clean up before each test"""
        GlobalInjector.reset()

    def teardown_method(self):
        """Clean up after each test"""
        GlobalInjector.reset()

    def test_config_lifecycle(self):
        """Test complete config initialization and usage lifecycle with real Spark"""
        # Re-import to re-register decorators after clearing injector
        import importlib

        import kindling.spark_config

        importlib.reload(kindling.spark_config)
        from kindling.spark_config import (
            ConfigService,
            DynaconfConfig,
            configure_injector_with_config,
        )

        # Initialize config with real Spark
        configure_injector_with_config(initial_config={"log_level": "INFO"})

        # Get config service
        config = get_kindling_service(ConfigService)

        assert config is not None, "Should get ConfigService instance"
        assert config.spark is not None, "Should have real Spark session"

        # Test set
        config.set("test_key", "test_value")

        # Test get
        value = config.get("test_key", "default")
        assert value == "test_value", "Should retrieve set values"

        # Test get_all
        all_config = config.get_all()
        assert isinstance(all_config, dict), "Should return dict from get_all"
        # Dynaconf stores keys uppercase
        assert (
            "TEST_KEY" in all_config or "test_key" in all_config
        ), "Should include all config in get_all"

    def test_config_service_injection_pattern(self):
        """Test that ConfigService follows proper injection pattern with real Spark"""
        # Re-import to re-register decorators after clearing injector
        import importlib

        import kindling.spark_config

        importlib.reload(kindling.spark_config)
        from kindling.spark_config import ConfigService, DynaconfConfig

        # Initialize once to trigger singleton creation
        configure_injector_with_config(initial_config={})

        # Get config service via interface
        config1 = get_kindling_service(ConfigService)
        config2 = get_kindling_service(ConfigService)

        # Should be same singleton
        assert config1 is config2, "Should return same singleton instance"

        # Should be concrete implementation
        assert isinstance(config1, DynaconfConfig), "Should inject DynaconfConfig implementation"

        # Should have real Spark
        assert config1.spark is not None, "Should have real Spark session"


if __name__ == "__main__":
    # Allow running tests directly
    pytest.main([__file__, "-v"])
