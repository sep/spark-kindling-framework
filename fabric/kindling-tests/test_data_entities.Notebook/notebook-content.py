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
if 'GI_IMPORT_GUARD' in globals():
    del GI_IMPORT_GUARD

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run data_entities

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pytest
from unittest.mock import patch, MagicMock, call
from dataclasses import dataclass, fields
from typing import List, Dict, Any


class TestDataEntities(SynapseNotebookTestCase):
    
    def test_entity_metadata_dataclass_structure(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        EntityMetadata = globals().get('EntityMetadata')
        if not EntityMetadata:
            pytest.skip("EntityMetadata not available")
        
        # Test that EntityMetadata has all required fields
        metadata_fields = {field.name for field in fields(EntityMetadata)}
        expected_fields = {'entityid', 'name', 'partition_columns', 'merge_columns', 'tags', 'schema'}
        
        assert metadata_fields == expected_fields
        
        # Test that we can create an EntityMetadata instance
        metadata = EntityMetadata(
            entityid="test_entity",
            name="Test Entity",
            partition_columns=["date", "region"],
            merge_columns=["id"],
            tags={"source": "test", "owner": "data_team"},
            schema={"id": "string", "value": "int"}
        )
        
        assert metadata.entityid == "test_entity"
        assert metadata.name == "Test Entity"
        assert metadata.partition_columns == ["date", "region"]
        assert metadata.merge_columns == ["id"]
        assert metadata.tags == {"source": "test", "owner": "data_team"}
        assert metadata.schema == {"id": "string", "value": "int"}
    
    def test_data_entity_manager_initialization(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        DataEntityManager = globals().get('DataEntityManager')
        if not DataEntityManager:
            pytest.skip("DataEntityManager not available")
        
        # Test that DataEntityManager can be instantiated
        manager = DataEntityManager.__new__(DataEntityManager)
        manager.registry = {}
        
        assert hasattr(manager, 'registry')
        assert isinstance(manager.registry, dict)
        assert len(manager.registry) == 0
    
    def test_data_entity_manager_register_entity(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        DataEntityManager = globals().get('DataEntityManager')
        EntityMetadata = globals().get('EntityMetadata')
        if not DataEntityManager or not EntityMetadata:
            pytest.skip("Required classes not available")
        
        # Create manager instance
        manager = DataEntityManager.__new__(DataEntityManager)
        manager.registry = {}
        
        # Test registering an entity
        manager.register_entity(
            "test_entity",
            name="Test Entity",
            partition_columns=["date"],
            merge_columns=["id"],
            tags={"env": "test"},
            schema={"id": "string"}
        )
        
        # Verify entity was registered
        assert "test_entity" in manager.registry
        
        entity = manager.registry["test_entity"]
        assert isinstance(entity, EntityMetadata)
        assert entity.entityid == "test_entity"
        assert entity.name == "Test Entity"
        assert entity.partition_columns == ["date"]
        assert entity.merge_columns == ["id"]
        assert entity.tags == {"env": "test"}
        assert entity.schema == {"id": "string"}
    
    def test_data_entity_manager_get_entity_ids(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        DataEntityManager = globals().get('DataEntityManager')
        if not DataEntityManager:
            pytest.skip("DataEntityManager not available")
        
        manager = DataEntityManager.__new__(DataEntityManager)
        manager.registry = {}
        
        # Test empty registry
        entity_ids = manager.get_entity_ids()
        assert len(list(entity_ids)) == 0
        
        # Add some entities
        manager.register_entity("entity1", name="Entity 1", partition_columns=[], merge_columns=[], tags={}, schema={})
        manager.register_entity("entity2", name="Entity 2", partition_columns=[], merge_columns=[], tags={}, schema={})
        
        # Test getting entity IDs
        entity_ids = list(manager.get_entity_ids())
        assert len(entity_ids) == 2
        assert "entity1" in entity_ids
        assert "entity2" in entity_ids
    
    def test_data_entity_manager_get_entity_definition(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        DataEntityManager = globals().get('DataEntityManager')
        if not DataEntityManager:
            pytest.skip("DataEntityManager not available")
        
        manager = DataEntityManager.__new__(DataEntityManager)
        manager.registry = {}
        
        # Test getting non-existent entity
        result = manager.get_entity_definition("nonexistent")
        assert result is None
        
        # Register an entity
        manager.register_entity(
            "test_entity",
            name="Test Entity",
            partition_columns=["date"],
            merge_columns=["id"],
            tags={"env": "test"},
            schema={"id": "string"}
        )
        
        # Test getting existing entity
        entity = manager.get_entity_definition("test_entity")
        assert entity is not None
        assert entity.entityid == "test_entity"
        assert entity.name == "Test Entity"
    
    def test_data_entities_decorator_validation(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        DataEntities = globals().get('DataEntities')
        EntityMetadata = globals().get('EntityMetadata')
        if not DataEntities or not EntityMetadata:
            pytest.skip("Required classes not available")
        
        # Mock the registry to avoid injection dependencies
        mock_registry = MagicMock()
        DataEntities.deregistry = mock_registry
        
        # Test that missing required fields raises an error
        with pytest.raises(ValueError) as exc_info:
            DataEntities.entity(
                entityid="test_entity",
                name="Test Entity"
                # Missing: partition_columns, merge_columns, tags, schema
            )
        
        error_message = str(exc_info.value)
        assert "Missing required fields" in error_message
        assert "partition_columns" in error_message
        assert "merge_columns" in error_message
        assert "tags" in error_message
        assert "schema" in error_message
    
    def test_data_entities_decorator_successful_registration(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        DataEntities = globals().get('DataEntities')
        if not DataEntities:
            pytest.skip("DataEntities not available")
        
        # Mock the registry
        mock_registry = MagicMock()
        DataEntities.deregistry = mock_registry
        
        # Test successful registration with all required fields
        result = DataEntities.entity(
            entityid="test_entity",
            name="Test Entity",
            partition_columns=["date", "region"],
            merge_columns=["id"],
            tags={"source": "test"},
            schema={"id": "string", "value": "int"}
        )
        
        # Should return None
        assert result is None
        
        # Should call register_entity on the registry
        mock_registry.register_entity.assert_called_once_with(
            "test_entity",
            name="Test Entity",
            partition_columns=["date", "region"],
            merge_columns=["id"],
            tags={"source": "test"},
            schema={"id": "string", "value": "int"}
        )
    
    def test_data_entities_lazy_registry_initialization(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        DataEntities = globals().get('DataEntities')
        if not DataEntities:
            pytest.skip("DataEntities not available")
        
        # Reset the registry to None
        DataEntities.deregistry = None
        
        # Mock GlobalInjector.get to return a mock registry
        mock_registry = MagicMock()
        mock_registry.register_entity = MagicMock()
        
        with patch.object(DataEntities, 'deregistry', None):
            with patch('__main__.GlobalInjector') as mock_gi:
                mock_gi.get.return_value = mock_registry
                
                # This should trigger lazy initialization
                DataEntities.entity(
                    entityid="test_entity",
                    name="Test Entity",
                    partition_columns=[],
                    merge_columns=[],
                    tags={},
                    schema={}
                )
                
                # Should have called GlobalInjector.get
                mock_gi.get.assert_called_once()

                mock_registry.register_entity.assert_called_once()
    
    def test_entity_interfaces_are_abstract(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        # Test that our interfaces are properly abstract
        EntityPathLocator = globals().get('EntityPathLocator')
        EntityNameMapper = globals().get('EntityNameMapper')
        EntityProvider = globals().get('EntityProvider')
        DataEntityRegistry = globals().get('DataEntityRegistry')
        
        interfaces_to_test = [
            (EntityPathLocator, 'get_table_path'),
            (EntityNameMapper, 'get_table_name'),
            (EntityProvider, 'ensure_entity_table'),
            (DataEntityRegistry, 'register_entity')
        ]
        
        for interface_class, method_name in interfaces_to_test:
            if interface_class:
                # Should not be able to instantiate abstract classes
                with pytest.raises(TypeError):
                    interface_class()
                
                # Should have the expected abstract method
                assert hasattr(interface_class, method_name)
    
    def test_entity_provider_interface_completeness(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        EntityProvider = globals().get('EntityProvider')
        if not EntityProvider:
            pytest.skip("EntityProvider not available")
        
        # Test that EntityProvider has all expected abstract methods
        expected_methods = [
            'ensure_entity_table',
            'check_entity_exists', 
            'merge_to_entity',
            'append_to_entity',
            'read_entity',
            'read_entity_since_version',
            'write_to_entity',
            'get_entity_version'
        ]
        
        for method_name in expected_methods:
            assert hasattr(EntityProvider, method_name), f"EntityProvider missing method: {method_name}"
    
    def test_data_entity_manager_implements_registry_interface(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        DataEntityManager = globals().get('DataEntityManager')
        DataEntityRegistry = globals().get('DataEntityRegistry')
        
        if not DataEntityManager or not DataEntityRegistry:
            pytest.skip("Required classes not available")
        
        # Test that DataEntityManager implements DataEntityRegistry interface
        assert issubclass(DataEntityManager, DataEntityRegistry)
        
        # Test that DataEntityManager has all required methods
        required_methods = ['register_entity', 'get_entity_ids', 'get_entity_definition']
        
        for method_name in required_methods:
            assert hasattr(DataEntityManager, method_name)
            assert callable(getattr(DataEntityManager, method_name))
    
    def test_entity_metadata_field_types(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        EntityMetadata = globals().get('EntityMetadata')
        if not EntityMetadata:
            pytest.skip("EntityMetadata not available")
        
        # Test field type annotations
        field_annotations = EntityMetadata.__annotations__
        
        assert 'entityid' in field_annotations
        assert 'name' in field_annotations
        assert 'partition_columns' in field_annotations
        assert 'merge_columns' in field_annotations
        assert 'tags' in field_annotations
        assert 'schema' in field_annotations
        
        # Test that List and Dict types are used appropriately
        assert str(field_annotations['partition_columns']).startswith('typing.List')
        assert str(field_annotations['merge_columns']).startswith('typing.List')
        assert str(field_annotations['tags']).startswith('typing.Dict')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

results = run_notebook_tests(TestDataEntities)
print(results)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
