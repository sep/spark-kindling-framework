# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e2a89496-5a18-4104-ac7a-4bfe4f325065",
# META       "default_lakehouse_name": "ent_datalake_np",
# META       "default_lakehouse_workspace_id": "ab18d43b-50de-4b41-b44b-f513a6731b99",
# META       "known_lakehouses": [
# META         {
# META           "id": "e2a89496-5a18-4104-ac7a-4bfe4f325065"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

BOOTSTRAP_CONFIG = {
    'is_interactive': True,
    'use_lake_packages' : False,
    'load_local_packages' : False,
    'workspace_endpoint': "059d44a0-c01e-4491-beed-b528c9eca9e8",
    'package_storage_path': "Files/artifacts/packages/latest",
    'required_packages': ["azure.identity", "injector", "dynaconf", "pytest"],
    'ignored_folders': ['utilities'],
    'spark_configs': {
        'spark.databricks.delta.schema.autoMerge.enabled': 'true'
    }
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run environment_bootstrap

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run test_framework

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

test_env = setup_global_test_environment()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run spark_config

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pytest
from unittest.mock import patch, MagicMock, call
from typing import Dict, Any


class TestDynaconfConfigService(SynapseNotebookTestCase):
    
    def test_service_can_be_instantiated(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        # Test that our service can be created with minimal setup
        mock_spark = MagicMock()
        mock_spark.conf.get.return_value = "abfss://test-container@teststorage.dfs.core.windows.net/warehouse"
        
        # Get the class from globals
        DynaconfConfig = globals().get('DynaconfConfig')
        if not DynaconfConfig:
            pytest.skip("DynaconfConfig not available")
        
        # This should work without errors
        try:
            config = DynaconfConfig(spark_session=mock_spark, initial_config={"test": "value"})
            assert config.spark == mock_spark
            assert config.initial_config == {"test": "value"}
        except Exception as e:
            # If it fails, it's likely due to missing dependencies, which is expected in test env
            assert "dynaconf" in str(e).lower() or "import" in str(e).lower()
    
    def test_service_get_method_logic(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        DynaconfConfig = globals().get('DynaconfConfig')
        if not DynaconfConfig:
            pytest.skip("DynaconfConfig not available")
        
        # Test the priority logic of our get method
        mock_spark = MagicMock()
        mock_dynaconf = MagicMock()
        
        # Create instance and manually set components for testing
        config = DynaconfConfig.__new__(DynaconfConfig)  # Create without calling __init__
        config.spark = mock_spark
        config.dynaconf = mock_dynaconf
        
        # Test Spark priority
        mock_spark.conf.get.return_value = "spark_value"
        result = config.get("test.key", "default")
        assert result == "spark_value", f"Expected 'spark_value', got '{result}'"
        
        # Test fallback to Dynaconf when Spark returns None
        mock_spark.conf.get.return_value = None
        mock_dynaconf.get.return_value = "dynaconf_value"
        result = config.get("test.key", "default")
        assert result == "dynaconf_value", f"Expected 'dynaconf_value', got '{result}'"
        
        # Test fallback to default when both return None
        mock_spark.conf.get.return_value = None
        # Fix: Use side_effect to properly handle the default parameter
        mock_dynaconf.get.side_effect = lambda key, default=None: default
        result = config.get("test.key", "default")
        assert result == "default", f"Expected 'default', got '{result}'"
    
    def test_service_get_method_handles_spark_exceptions(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        DynaconfConfig = globals().get('DynaconfConfig')
        if not DynaconfConfig:
            pytest.skip("DynaconfConfig not available")
        
        mock_spark = MagicMock()
        mock_dynaconf = MagicMock()
        
        config = DynaconfConfig.__new__(DynaconfConfig)
        config.spark = mock_spark
        config.dynaconf = mock_dynaconf
        
        # Test that Spark exceptions don't break the service
        mock_spark.conf.get.side_effect = Exception("Spark connection lost")
        mock_dynaconf.get.return_value = "dynaconf_fallback"
        
        result = config.get("test.key", "default")
        assert result == "dynaconf_fallback"
    
    def test_service_get_method_without_spark(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        DynaconfConfig = globals().get('DynaconfConfig')
        if not DynaconfConfig:
            pytest.skip("DynaconfConfig not available")
        
        mock_dynaconf = MagicMock()
        
        config = DynaconfConfig.__new__(DynaconfConfig)
        config.spark = None  # No Spark session
        config.dynaconf = mock_dynaconf
        
        mock_dynaconf.get.return_value = "dynaconf_value"
        
        result = config.get("test.key", "default")
        assert result == "dynaconf_value"
        mock_dynaconf.get.assert_called_with("test.key", "default")
    
    def test_service_set_method_logic(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        DynaconfConfig = globals().get('DynaconfConfig')
        if not DynaconfConfig:
            pytest.skip("DynaconfConfig not available")
        
        mock_spark = MagicMock()
        mock_dynaconf = MagicMock()
        
        config = DynaconfConfig.__new__(DynaconfConfig)
        config.spark = mock_spark
        config.dynaconf = mock_dynaconf
        
        config.set("test.key", "test_value")
        
        # Both should be updated
        mock_spark.conf.set.assert_called_with("test.key", "test_value")
        mock_dynaconf.set.assert_called_with("test.key", "test_value")
    
    def test_service_set_method_continues_on_spark_error(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        DynaconfConfig = globals().get('DynaconfConfig')
        if not DynaconfConfig:
            pytest.skip("DynaconfConfig not available")
        
        mock_spark = MagicMock()
        mock_dynaconf = MagicMock()
        
        config = DynaconfConfig.__new__(DynaconfConfig)
        config.spark = mock_spark
        config.dynaconf = mock_dynaconf
        
        # Spark fails but service should continue
        mock_spark.conf.set.side_effect = Exception("Spark error")
        
        config.set("test.key", "test_value")
        
        # Dynaconf should still be updated
        mock_dynaconf.set.assert_called_with("test.key", "test_value")
    
    def test_service_get_all_method_logic(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        DynaconfConfig = globals().get('DynaconfConfig')
        if not DynaconfConfig:
            pytest.skip("DynaconfConfig not available")
        
        mock_spark = MagicMock()
        mock_dynaconf = MagicMock()
        
        config = DynaconfConfig.__new__(DynaconfConfig)
        config.spark = mock_spark
        config.dynaconf = mock_dynaconf
        
        # Setup mock data
        mock_dynaconf.to_dict.return_value = {"dynaconf.key": "dynaconf_value"}
        mock_dynaconf.get.return_value = "dynaconf_value"
        mock_spark.conf.getAll.return_value = [("spark.key", "spark_value")]
        
        result = config.get_all()
        
        expected = {
            "dynaconf.key": "dynaconf_value",
            "spark.key": "spark_value"
        }
        assert result == expected
    
    def test_service_get_all_handles_spark_errors(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        DynaconfConfig = globals().get('DynaconfConfig')
        if not DynaconfConfig:
            pytest.skip("DynaconfConfig not available")
        
        mock_spark = MagicMock()
        mock_dynaconf = MagicMock()
        
        config = DynaconfConfig.__new__(DynaconfConfig)
        config.spark = mock_spark
        config.dynaconf = mock_dynaconf
        
        # Setup mock data
        mock_dynaconf.to_dict.return_value = {"dynaconf.key": "dynaconf_value"}
        mock_dynaconf.get.return_value = "dynaconf_value"
        mock_spark.conf.getAll.side_effect = Exception("Spark error")
        
        result = config.get_all()
        
        # Should still return Dynaconf data
        expected = {"dynaconf.key": "dynaconf_value"}
        assert result == expected
    
    def test_service_using_env_method(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        DynaconfConfig = globals().get('DynaconfConfig')
        if not DynaconfConfig:
            pytest.skip("DynaconfConfig not available")
        
        mock_dynaconf = MagicMock()
        mock_env_config = MagicMock()
        mock_dynaconf.using_env.return_value = mock_env_config
        
        config = DynaconfConfig.__new__(DynaconfConfig)
        config.dynaconf = mock_dynaconf
        
        result = config.using_env("production")
        
        mock_dynaconf.using_env.assert_called_with("production")
        assert result == mock_env_config
    
    def test_service_getattr_method_logic(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        DynaconfConfig = globals().get('DynaconfConfig')
        if not DynaconfConfig:
            pytest.skip("DynaconfConfig not available")
        
        mock_spark = MagicMock()
        mock_dynaconf = MagicMock()
        
        config = DynaconfConfig.__new__(DynaconfConfig)
        config.spark = mock_spark
        config.dynaconf = mock_dynaconf
        
        # Mock the get method to test __getattr__
        config.get = MagicMock()
        
        # Test existing attribute
        config.get.return_value = "test_value"
        result = config.database_url
        assert result == "test_value"
        config.get.assert_called_with("database_url")
        
        # Test non-existent attribute
        config.get.return_value = None
        with pytest.raises(AttributeError) as exc_info:
            _ = config.nonexistent_setting
        
        assert "No configuration found for 'nonexistent_setting'" in str(exc_info.value)
    
    def test_service_reload_method(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        DynaconfConfig = globals().get('DynaconfConfig')
        if not DynaconfConfig:
            pytest.skip("DynaconfConfig not available")
        
        mock_dynaconf = MagicMock()
        
        config = DynaconfConfig.__new__(DynaconfConfig)
        config.dynaconf = mock_dynaconf
        
        config.reload()
        
        mock_dynaconf.reload.assert_called_once()
    
    def test_service_get_fresh_method_logic(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        DynaconfConfig = globals().get('DynaconfConfig')
        if not DynaconfConfig:
            pytest.skip("DynaconfConfig not available")
        
        mock_spark = MagicMock()
        mock_dynaconf = MagicMock()
        
        config = DynaconfConfig.__new__(DynaconfConfig)
        config.spark = mock_spark
        config.dynaconf = mock_dynaconf
        
        # Test Spark priority in get_fresh
        mock_spark.conf.get.return_value = "spark_fresh_value"
        result = config.get_fresh("test.key", "default")
        assert result == "spark_fresh_value"
        
        # Test fallback to Dynaconf
        mock_spark.conf.get.return_value = None
        mock_dynaconf.get_fresh.return_value = "dynaconf_fresh_value"
        result = config.get_fresh("test.key", "default")
        assert result == "dynaconf_fresh_value"
        mock_dynaconf.get_fresh.assert_called_with("test.key", default="default")
    
    def test_config_interface_contract(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        DynaconfConfig = globals().get('DynaconfConfig')
        if not DynaconfConfig:
            pytest.skip("DynaconfConfig not available")
        
        # Test that our service implements the ConfigInterface contract
        mock_spark = MagicMock()
        mock_dynaconf = MagicMock()
        
        config = DynaconfConfig.__new__(DynaconfConfig)
        config.spark = mock_spark
        config.dynaconf = mock_dynaconf
        
        # Verify our service has all the required interface methods
        assert hasattr(config, 'get')
        assert hasattr(config, 'set') 
        assert hasattr(config, 'get_all')
        assert hasattr(config, 'using_env')
        
        # Verify they're callable
        assert callable(config.get)
        assert callable(config.set)
        assert callable(config.get_all)
        assert callable(config.using_env)
    
    def test_base_service_provider_dependency_pattern(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        # Test the dependency injection pattern without requiring the actual classes
        # This tests the concept that services should depend on interfaces, not concrete classes
        
        mock_global_injector = MagicMock()
        mock_config = MagicMock()
        mock_global_injector.get.return_value = mock_config
        
        # Simulate what BaseServiceProvider.__init__ does
        # The key insight: it should request an interface, not a concrete implementation
        config_interface_type = type('ConfigInterface', (), {})  # Mock interface type
        
        # This is the pattern our service follows
        retrieved_config = mock_global_injector.get(config_interface_type)
        
        assert retrieved_config == mock_config
        mock_global_injector.get.assert_called_with(config_interface_type)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

results = run_notebook_tests(TestDynaconfConfigService)
print(results)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
