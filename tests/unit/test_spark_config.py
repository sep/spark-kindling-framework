"""
Unit tests for kindling.spark_config module.

Tests the ConfigService interface and DynaconfConfig implementation,
including YAML configuration loading, bootstrap config translation,
and Spark configuration integration.
"""
import pytest
from unittest.mock import MagicMock, patch, PropertyMock
from typing import Dict, Any
import tempfile
from pathlib import Path

from kindling.spark_config import (
    ConfigService,
    DynaconfConfig,
    configure_injector_with_config
)
from kindling.injection import GlobalInjector


class TestConfigServiceInterface:
    """Tests for ConfigService abstract interface"""

    def test_config_service_is_abstract(self):
        """Test that ConfigService is an abstract base class"""
        from abc import ABC

        assert issubclass(ConfigService, ABC), "ConfigService should be an ABC"

    def test_config_service_has_required_methods(self):
        """Test that ConfigService defines all required abstract methods"""
        required_methods = ['get', 'set', 'get_all', 'using_env', 'initialize']

        for method_name in required_methods:
            assert hasattr(
                ConfigService, method_name), f"ConfigService should define {method_name}"

    def test_config_service_cannot_be_instantiated(self):
        """Test that ConfigService cannot be instantiated directly"""
        with pytest.raises(TypeError) as exc_info:
            ConfigService()

        assert "abstract" in str(exc_info.value).lower(
        ), "Should raise error about abstract class"


class TestDynaconfConfigInitialization:
    """Tests for DynaconfConfig initialization"""

    def setup_method(self):
        """Clean up before each test"""
        GlobalInjector._instance = None

    def teardown_method(self):
        """Clean up after each test"""
        GlobalInjector._instance = None

    def test_dynaconf_config_instantiation(self):
        """Test that DynaconfConfig can be instantiated"""
        config = DynaconfConfig()

        assert config.spark is None, "Spark should be None before initialization"
        assert config.initial_config == {}, "Initial config should be empty dict"
        assert config.dynaconf is None, "Dynaconf should be None before initialization"

    def test_dynaconf_config_implements_interface(self):
        """Test that DynaconfConfig implements ConfigService interface"""
        assert issubclass(
            DynaconfConfig, ConfigService), "DynaconfConfig should implement ConfigService"

    def test_dynaconf_config_has_singleton_decorator(self):
        """Test that DynaconfConfig is decorated with singleton_autobind"""
        # Re-import to re-register decorators after clearing injector
        import importlib
        import kindling.spark_config
        importlib.reload(kindling.spark_config)
        from kindling.spark_config import ConfigService, DynaconfConfig

        # Get two instances via GlobalInjector
        config1 = GlobalInjector.get(ConfigService)
        config2 = GlobalInjector.get(ConfigService)

        assert config1 is config2, "Should return same singleton instance"
        assert isinstance(
            config1, DynaconfConfig), "Should be DynaconfConfig instance"

    @patch('kindling.spark_config.get_or_create_spark_session')
    @patch('kindling.spark_config.Dynaconf')
    def test_initialize_with_defaults(self, mock_dynaconf_class, mock_spark_fn):
        """Test initialize method with default parameters"""
        mock_spark = MagicMock()
        mock_spark_fn.return_value = mock_spark
        mock_dynaconf = MagicMock()
        mock_dynaconf_class.return_value = mock_dynaconf

        config = DynaconfConfig()
        config.initialize()

        assert config.spark == mock_spark, "Spark should be set"
        assert config.initial_config == {}, "Initial config should be empty"
        assert config.dynaconf == mock_dynaconf, "Dynaconf should be set"

        mock_spark_fn.assert_called_once(), "Should create Spark session"
        mock_dynaconf_class.assert_called_once(), "Should create Dynaconf instance"

    @patch('kindling.spark_config.get_or_create_spark_session')
    @patch('kindling.spark_config.Dynaconf')
    def test_initialize_with_config_files(self, mock_dynaconf_class, mock_spark_fn):
        """Test initialize with configuration files"""
        mock_spark = MagicMock()
        mock_spark_fn.return_value = mock_spark
        mock_dynaconf = MagicMock()
        mock_dynaconf_class.return_value = mock_dynaconf

        config_files = ['/path/to/config1.yaml', '/path/to/config2.yaml']

        config = DynaconfConfig()
        config.initialize(config_files=config_files, environment='production')

        # Verify Dynaconf was initialized with correct parameters
        call_kwargs = mock_dynaconf_class.call_args[1]
        assert call_kwargs['settings_files'] == config_files, "Should pass config files"
        assert call_kwargs['env'] == 'production', "Should use production environment"
        assert call_kwargs['environments'] is True, "Should enable environments"
        assert call_kwargs['MERGE_ENABLED_FOR_DYNACONF'] is True, "Should enable merging"
        assert call_kwargs['envvar_prefix'] == 'KINDLING', "Should use KINDLING prefix"

    @patch('kindling.spark_config.get_or_create_spark_session')
    @patch('kindling.spark_config.Dynaconf')
    def test_initialize_with_initial_config(self, mock_dynaconf_class, mock_spark_fn):
        """Test initialize with bootstrap initial config"""
        mock_spark = MagicMock()
        mock_spark_fn.return_value = mock_spark
        mock_dynaconf = MagicMock()
        mock_dynaconf_class.return_value = mock_dynaconf

        initial_config = {
            'log_level': 'DEBUG',
            'print_logging': True,
            'load_local_packages': True
        }

        config = DynaconfConfig()
        config.initialize(initial_config=initial_config)

        assert config.initial_config == initial_config, "Should store initial config"

        # Verify translation methods were called (they set values on dynaconf)
        assert mock_dynaconf.set.called, "Should translate and set config values"


class TestDynaconfConfigGetMethod:
    """Tests for DynaconfConfig get() method"""

    def test_get_with_no_spark_session(self):
        """Test get method when Spark session is not available"""
        config = DynaconfConfig()
        config.spark = None
        config.dynaconf = MagicMock()
        config.dynaconf.get.return_value = 'dynaconf_value'

        result = config.get('test_key', 'default_value')

        assert result == 'dynaconf_value', "Should return value from Dynaconf"
        config.dynaconf.get.assert_called_once_with(
            'test_key', 'default_value')

    def test_get_prefers_spark_config(self):
        """Test that Spark configuration has priority over Dynaconf"""
        config = DynaconfConfig()
        config.spark = MagicMock()
        config.spark.conf.get.return_value = 'spark_value'
        config.dynaconf = MagicMock()
        config.dynaconf.get.return_value = 'dynaconf_value'

        result = config.get('TEST_KEY', 'default')

        assert result == 'spark_value', "Should prefer Spark value over Dynaconf"
        config.spark.conf.get.assert_called_once_with('TEST_KEY')

    def test_get_falls_back_to_dynaconf(self):
        """Test fallback to Dynaconf when Spark config not found"""
        config = DynaconfConfig()
        config.spark = MagicMock()
        config.spark.conf.get.return_value = None
        config.dynaconf = MagicMock()
        config.dynaconf.get.return_value = 'dynaconf_value'

        result = config.get('test_key', 'default')

        assert result == 'dynaconf_value', "Should fall back to Dynaconf when Spark returns None"

    def test_get_handles_spark_exceptions(self):
        """Test that Spark exceptions are handled gracefully"""
        config = DynaconfConfig()
        config.spark = MagicMock()
        config.spark.conf.get.side_effect = Exception("Spark error")
        config.dynaconf = MagicMock()
        config.dynaconf.get.return_value = 'dynaconf_fallback'

        result = config.get('test_key', 'default')

        assert result == 'dynaconf_fallback', "Should fall back to Dynaconf on Spark exception"

    def test_get_returns_default_when_not_found(self):
        """Test that default value is returned when key not found"""
        config = DynaconfConfig()
        config.spark = None
        config.dynaconf = MagicMock()
        config.dynaconf.get.return_value = 'default_value'

        result = config.get('missing_key', 'default_value')

        assert result == 'default_value', "Should return default when key not found"


class TestDynaconfConfigSetMethod:
    """Tests for DynaconfConfig set() method"""

    def test_set_updates_dynaconf(self):
        """Test that set method updates Dynaconf configuration"""
        config = DynaconfConfig()
        config.dynaconf = MagicMock()

        config.set('test_key', 'test_value')

        config.dynaconf.set.assert_called_once_with('test_key', 'test_value')


class TestDynaconfConfigGetAllMethod:
    """Tests for DynaconfConfig get_all() method"""

    def test_get_all_without_spark(self):
        """Test get_all when Spark session is not available"""
        config = DynaconfConfig()
        config.spark = None
        config.dynaconf = MagicMock()
        config.dynaconf.to_dict.return_value = {
            'key1': 'value1', 'key2': 'value2'}
        config.dynaconf.get.side_effect = lambda k: {
            'key1': 'value1', 'key2': 'value2'}[k]

        result = config.get_all()

        assert 'key1' in result, "Should include Dynaconf keys"
        assert 'key2' in result, "Should include Dynaconf keys"

    def test_get_all_merges_spark_and_dynaconf(self):
        """Test that get_all merges Spark and Dynaconf configurations"""
        config = DynaconfConfig()
        config.spark = MagicMock()
        config.spark.conf.getAll.return_value = [
            ('spark.key1', 'spark_value1'),
            ('spark.key2', 'spark_value2')
        ]
        config.dynaconf = MagicMock()
        config.dynaconf.to_dict.return_value = {
            'dynaconf_key': 'dynaconf_value'}
        config.dynaconf.get.return_value = 'dynaconf_value'

        result = config.get_all()

        assert 'dynaconf_key' in result, "Should include Dynaconf config"
        assert 'spark.key1' in result, "Should include Spark config"
        assert 'spark.key2' in result, "Should include Spark config"
        assert result['spark.key1'] == 'spark_value1', "Should have correct Spark values"

    def test_get_all_handles_spark_exceptions(self):
        """Test that Spark exceptions don't break get_all"""
        config = DynaconfConfig()
        config.spark = MagicMock()
        config.spark.conf.getAll.side_effect = Exception("Spark error")
        config.dynaconf = MagicMock()
        config.dynaconf.to_dict.return_value = {
            'dynaconf_key': 'dynaconf_value'}
        config.dynaconf.get.return_value = 'dynaconf_value'

        result = config.get_all()

        assert 'dynaconf_key' in result, "Should still return Dynaconf config on Spark error"
        assert len(
            result) == 1, "Should only have Dynaconf config when Spark fails"


class TestDynaconfConfigHelperMethods:
    """Tests for DynaconfConfig helper and utility methods"""

    def test_using_env_delegates_to_dynaconf(self):
        """Test that using_env delegates to Dynaconf"""
        config = DynaconfConfig()
        config.dynaconf = MagicMock()
        mock_env_config = MagicMock()
        config.dynaconf.using_env.return_value = mock_env_config

        result = config.using_env('production')

        assert result == mock_env_config, "Should return Dynaconf environment context"
        config.dynaconf.using_env.assert_called_once_with('production')

    def test_reload_delegates_to_dynaconf(self):
        """Test that reload delegates to Dynaconf"""
        config = DynaconfConfig()
        config.dynaconf = MagicMock()

        config.reload()

        config.dynaconf.reload.assert_called_once(), "Should call Dynaconf reload"

    def test_get_fresh_prefers_spark(self):
        """Test that get_fresh prefers Spark configuration"""
        config = DynaconfConfig()
        config.spark = MagicMock()
        config.spark.conf.get.return_value = 'spark_fresh'
        config.dynaconf = MagicMock()

        result = config.get_fresh('test_key', 'default')

        assert result == 'spark_fresh', "Should prefer Spark value"
        config.spark.conf.get.assert_called_once_with('test_key')

    def test_get_fresh_falls_back_to_dynaconf(self):
        """Test that get_fresh falls back to Dynaconf when Spark unavailable"""
        config = DynaconfConfig()
        config.spark = MagicMock()
        config.spark.conf.get.return_value = None
        config.dynaconf = MagicMock()
        config.dynaconf.get_fresh.return_value = 'dynaconf_fresh'

        result = config.get_fresh('test_key', 'default')

        assert result == 'dynaconf_fresh', "Should fall back to Dynaconf"
        config.dynaconf.get_fresh.assert_called_once_with(
            'test_key', default='default')

    def test_get_fresh_handles_spark_exceptions(self):
        """Test that get_fresh handles Spark exceptions"""
        config = DynaconfConfig()
        config.spark = MagicMock()
        config.spark.conf.get.side_effect = Exception("Spark error")
        config.dynaconf = MagicMock()
        config.dynaconf.get_fresh.return_value = 'dynaconf_fallback'

        result = config.get_fresh('test_key', 'default')

        assert result == 'dynaconf_fallback', "Should handle Spark exception gracefully"


class TestDynaconfConfigAttributeAccess:
    """Tests for DynaconfConfig __getattr__ magic method"""

    def test_getattr_returns_config_value(self):
        """Test that attribute access returns config values"""
        config = DynaconfConfig()
        config.dynaconf = MagicMock()
        config.dynaconf.get.return_value = 'database_url_value'
        config.spark = None

        result = config.database_url

        assert result == 'database_url_value', "Should return config value via attribute access"

    def test_getattr_raises_for_missing_config(self):
        """Test that AttributeError is raised for missing config"""
        config = DynaconfConfig()
        config.dynaconf = MagicMock()
        config.dynaconf.get.return_value = None
        config.spark = None

        with pytest.raises(AttributeError) as exc_info:
            _ = config.nonexistent_setting

        assert "No configuration found for 'nonexistent_setting'" in str(exc_info.value), \
            "Should raise AttributeError with clear message"

    def test_getattr_allows_private_attributes(self):
        """Test that private attributes are handled normally"""
        config = DynaconfConfig()
        config.dynaconf = MagicMock()

        # This should raise AttributeError for the actual missing attribute
        with pytest.raises(AttributeError) as exc_info:
            _ = config._missing_private

        assert "object has no attribute" in str(exc_info.value), \
            "Should raise normal AttributeError for private attributes"


class TestConfigTranslation:
    """Tests for YAML-to-bootstrap and bootstrap-to-YAML translation"""

    @patch('kindling.spark_config.get_or_create_spark_session')
    @patch('kindling.spark_config.Dynaconf')
    def test_translate_yaml_to_flat(self, mock_dynaconf_class, mock_spark_fn):
        """Test YAML nested keys are translated to flat bootstrap keys"""
        mock_spark = MagicMock()
        mock_spark_fn.return_value = mock_spark
        mock_dynaconf = MagicMock()
        mock_dynaconf_class.return_value = mock_dynaconf

        # Simulate YAML config structure
        mock_dynaconf.get.side_effect = lambda k: {
            'TELEMETRY.logging.level': 'INFO',
            'TELEMETRY.logging.print': True,
            'DELTA.tablerefmode': 'name'
        }.get(k)

        config = DynaconfConfig()
        config.initialize()

        # Verify translations were set
        set_calls = [call[0] for call in mock_dynaconf.set.call_args_list]

        # Should have translated nested to flat
        assert any('log_level' in str(call) for call in set_calls), \
            "Should translate TELEMETRY.logging.level to log_level"

    @patch('kindling.spark_config.get_or_create_spark_session')
    @patch('kindling.spark_config.Dynaconf')
    def test_translate_bootstrap_to_nested(self, mock_dynaconf_class, mock_spark_fn):
        """Test bootstrap flat keys are translated to nested structure"""
        mock_spark = MagicMock()
        mock_spark_fn.return_value = mock_spark
        mock_dynaconf = MagicMock()
        mock_dynaconf_class.return_value = mock_dynaconf
        mock_dynaconf.get.return_value = None

        initial_config = {
            'log_level': 'DEBUG',
            'print_logging': False,
            'DELTA_TABLE_ACCESS_MODE': 'path'
        }

        config = DynaconfConfig()
        config.initialize(initial_config=initial_config)

        # Verify nested translations were set
        set_calls = [call[0] for call in mock_dynaconf.set.call_args_list]

        # Should have translated flat to nested
        assert any('TELEMETRY' in str(call) for call in set_calls), \
            "Should translate log_level to TELEMETRY.logging.level"

    @patch('kindling.spark_config.get_or_create_spark_session')
    @patch('kindling.spark_config.Dynaconf')
    def test_apply_bootstrap_overrides_spark_configs(self, mock_dynaconf_class, mock_spark_fn):
        """Test that spark_configs from bootstrap are properly translated"""
        mock_spark = MagicMock()
        mock_spark_fn.return_value = mock_spark
        mock_dynaconf = MagicMock()
        mock_dynaconf_class.return_value = mock_dynaconf
        mock_dynaconf.get.return_value = None

        initial_config = {
            'spark_configs': {
                'spark.sql.adaptive.enabled': 'true',
                'spark.databricks.delta.schema.autoMerge.enabled': 'true'
            }
        }

        config = DynaconfConfig()
        config.initialize(initial_config=initial_config)

        # Verify spark_configs was translated to SPARK_CONFIGS
        set_calls = mock_dynaconf.set.call_args_list
        set_dict = {str(call[0][0]): call[0][1]
                    for call in set_calls if len(call[0]) == 2}

        # Should have SPARK_CONFIGS key
        assert any('SPARK_CONFIGS' in key for key in set_dict.keys()), \
            "Should translate spark_configs to SPARK_CONFIGS"


class TestConfigureInjectorFunction:
    """Tests for configure_injector_with_config helper function"""

    def setup_method(self):
        """Clean up before each test"""
        GlobalInjector._instance = None

    def teardown_method(self):
        """Clean up after each test"""
        GlobalInjector._instance = None

    @pytest.mark.skip(reason="Requires full Spark JVM which has compatibility issues in dev container")
    @patch('kindling.spark_config.get_or_create_spark_session')
    @patch('kindling.spark_config.Dynaconf')
    def test_configure_injector_creates_singleton(self, mock_dynaconf_class, mock_spark_fn):
        """Test that configure_injector_with_config sets up singleton"""
        # Re-import to re-register decorators after clearing injector
        import importlib
        import kindling.spark_config
        importlib.reload(kindling.spark_config)
        from kindling.spark_config import ConfigService, DynaconfConfig, configure_injector_with_config

        mock_spark = MagicMock()
        mock_spark_fn.return_value = mock_spark
        mock_dynaconf = MagicMock()
        mock_dynaconf_class.return_value = mock_dynaconf
        mock_dynaconf.get.return_value = None

        configure_injector_with_config(
            config_files=['/path/to/config.yaml'],
            initial_config={'log_level': 'INFO'},
            environment='production'
        )

        # Verify singleton was created and initialized
        config_service = GlobalInjector.get(ConfigService)

        assert config_service is not None, "Should create ConfigService singleton"
        assert isinstance(
            config_service, DynaconfConfig), "Should be DynaconfConfig instance"
        assert config_service.spark == mock_spark, "Should have Spark session"

    @pytest.mark.skip(reason="Requires full Spark JVM which has compatibility issues in dev container")
    @patch('kindling.spark_config.get_or_create_spark_session')
    @patch('kindling.spark_config.Dynaconf')
    def test_configure_injector_with_all_parameters(self, mock_dynaconf_class, mock_spark_fn):
        """Test configure_injector_with_config with all parameters"""
        # Re-import to re-register decorators after clearing injector
        import importlib
        import kindling.spark_config
        importlib.reload(kindling.spark_config)
        from kindling.spark_config import ConfigService, DynaconfConfig, configure_injector_with_config

        mock_spark = MagicMock()
        mock_spark_fn.return_value = mock_spark
        mock_dynaconf = MagicMock()
        mock_dynaconf_class.return_value = mock_dynaconf
        mock_dynaconf.get.return_value = None

        config_files = ['/path/to/config1.yaml', '/path/to/config2.yaml']
        initial_config = {'log_level': 'DEBUG', 'print_logging': True}

        configure_injector_with_config(
            config_files=config_files,
            initial_config=initial_config,
            environment='staging',
            artifacts_storage_path='/path/to/artifacts'
        )

        # Verify configuration was applied
        config_service = GlobalInjector.get(ConfigService)

        assert config_service.initial_config == initial_config, \
            "Should apply initial config"

        # Verify Dynaconf was initialized with correct parameters
        call_kwargs = mock_dynaconf_class.call_args[1]
        assert call_kwargs['settings_files'] == config_files, \
            "Should pass config files to Dynaconf"
        assert call_kwargs['env'] == 'staging', \
            "Should use staging environment"


class TestConfigIntegration:
    """Integration tests for config system"""

    def setup_method(self):
        """Clean up before each test"""
        GlobalInjector._instance = None

    def teardown_method(self):
        """Clean up after each test"""
        GlobalInjector._instance = None

    @pytest.mark.skip(reason="Requires full Spark JVM which has compatibility issues in dev container")
    @patch('kindling.spark_config.get_or_create_spark_session')
    @patch('kindling.spark_config.Dynaconf')
    def test_config_lifecycle(self, mock_dynaconf_class, mock_spark_fn):
        """Test complete config initialization and usage lifecycle"""
        # Re-import to re-register decorators after clearing injector
        import importlib
        import kindling.spark_config
        importlib.reload(kindling.spark_config)
        from kindling.spark_config import ConfigService, DynaconfConfig, configure_injector_with_config

        mock_spark = MagicMock()
        mock_spark_fn.return_value = mock_spark
        mock_dynaconf = MagicMock()
        mock_dynaconf_class.return_value = mock_dynaconf

        # Setup mock returns
        mock_dynaconf.get.side_effect = lambda k, default=None: {
            'database_url': 'postgresql://localhost/test',
            'log_level': 'INFO'
        }.get(k, default)

        mock_dynaconf.to_dict.return_value = {
            'database_url': 'postgresql://localhost/test',
            'log_level': 'INFO'
        }

        # Initialize config
        configure_injector_with_config(
            initial_config={'log_level': 'INFO'}
        )

        # Get config service
        config = GlobalInjector.get(ConfigService)

        # Test get
        assert config.get('database_url') == 'postgresql://localhost/test', \
            "Should retrieve config values"

        # Test set
        config.set('new_key', 'new_value')
        mock_dynaconf.set.assert_called_with('new_key', 'new_value')

        # Test get_all
        all_config = config.get_all()
        assert 'database_url' in all_config, "Should include all config in get_all"

    def test_config_service_injection_pattern(self):
        """Test that ConfigService follows proper injection pattern"""
        # Re-import to re-register decorators after clearing injector
        import importlib
        import kindling.spark_config
        importlib.reload(kindling.spark_config)
        from kindling.spark_config import ConfigService, DynaconfConfig

        # Get config service via interface
        config1 = GlobalInjector.get(ConfigService)
        config2 = GlobalInjector.get(ConfigService)

        # Should be same singleton
        assert config1 is config2, "Should return same singleton instance"

        # Should be concrete implementation
        assert isinstance(config1, DynaconfConfig), \
            "Should inject DynaconfConfig implementation"


if __name__ == "__main__":
    # Allow running tests directly
    pytest.main([__file__, "-v"])
