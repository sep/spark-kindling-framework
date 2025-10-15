"""
Unit tests for kindling.data_entities module.

Tests the entity registration system, metadata handling, and the DataEntityManager.
"""
import pytest
from unittest.mock import MagicMock, patch
from dataclasses import fields
from typing import List, Dict, Any

from kindling.data_entities import (
    EntityMetadata,
    DataEntities,
    DataEntityManager,
    DataEntityRegistry,
    EntityProvider,
    EntityPathLocator,
    EntityNameMapper
)
from kindling.injection import GlobalInjector


class TestEntityMetadata:
    """Tests for EntityMetadata dataclass"""

    def test_entity_metadata_has_required_fields(self):
        """Test that EntityMetadata has all required fields"""
        metadata_fields = {field.name for field in fields(EntityMetadata)}
        expected_fields = {'entityid', 'name',
                           'partition_columns', 'merge_columns', 'tags', 'schema'}

        assert metadata_fields == expected_fields, f"EntityMetadata fields mismatch. Expected: {expected_fields}, Got: {metadata_fields}"

    def test_entity_metadata_field_types(self):
        """Test that EntityMetadata fields have correct type annotations"""
        field_annotations = EntityMetadata.__annotations__

        assert 'entityid' in field_annotations, "EntityMetadata should have 'entityid' field"
        assert 'name' in field_annotations, "EntityMetadata should have 'name' field"
        assert 'partition_columns' in field_annotations, "EntityMetadata should have 'partition_columns' field"
        assert 'merge_columns' in field_annotations, "EntityMetadata should have 'merge_columns' field"
        assert 'tags' in field_annotations, "EntityMetadata should have 'tags' field"
        assert 'schema' in field_annotations, "EntityMetadata should have 'schema' field"

        # Check that collection types are correct
        assert field_annotations['partition_columns'] == List[str], "partition_columns should be List[str]"
        assert field_annotations['merge_columns'] == List[str], "merge_columns should be List[str]"
        assert field_annotations['tags'] == Dict[str,
                                                 str], "tags should be Dict[str, str]"
        assert field_annotations['schema'] == Any, "schema should be Any type"

    def test_entity_metadata_creation(self):
        """Test creating an EntityMetadata instance with all fields"""
        metadata = EntityMetadata(
            entityid="test_entity",
            name="Test Entity",
            partition_columns=["date", "region"],
            merge_columns=["id"],
            tags={"source": "test", "owner": "data_team"},
            schema={"id": "string", "value": "int"}
        )

        assert metadata.entityid == "test_entity", "EntityMetadata.entityid should be 'test_entity'"
        assert metadata.name == "Test Entity", "EntityMetadata.name should be 'Test Entity'"
        assert metadata.partition_columns == [
            "date", "region"], "EntityMetadata.partition_columns should be ['date', 'region']"
        assert metadata.merge_columns == [
            "id"], "EntityMetadata.merge_columns should be ['id']"
        assert metadata.tags == {
            "source": "test", "owner": "data_team"}, "EntityMetadata.tags should match expected dict"
        assert metadata.schema == {
            "id": "string", "value": "int"}, "EntityMetadata.schema should match expected dict"

    def test_entity_metadata_empty_collections(self):
        """Test EntityMetadata with empty lists and dicts"""
        metadata = EntityMetadata(
            entityid="simple_entity",
            name="Simple Entity",
            partition_columns=[],
            merge_columns=[],
            tags={},
            schema=None
        )

        assert metadata.partition_columns == [], "partition_columns should be empty list"
        assert metadata.merge_columns == [], "merge_columns should be empty list"
        assert metadata.tags == {}, "tags should be empty dict"
        assert metadata.schema is None, "schema should be None"

    def test_entity_metadata_complex_schema(self):
        """Test EntityMetadata with complex schema object"""
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType

        complex_schema = StructType([
            StructField("id", StringType(), False),
            StructField("value", IntegerType(), True),
        ])

        metadata = EntityMetadata(
            entityid="complex_entity",
            name="Complex Entity",
            partition_columns=["partition_key"],
            merge_columns=["id"],
            tags={"type": "complex"},
            schema=complex_schema
        )

        assert metadata.schema == complex_schema, "schema should equal the complex_schema StructType"
        assert isinstance(
            metadata.schema, StructType), "schema should be instance of StructType"


class TestDataEntityManager:
    """Tests for DataEntityManager class"""

    def test_initialization(self):
        """Test DataEntityManager initialization"""
        manager = DataEntityManager()

        assert hasattr(
            manager, 'registry'), "DataEntityManager should have 'registry' attribute"
        assert isinstance(manager.registry, dict), "registry should be a dict"
        assert len(
            manager.registry) == 0, "registry should be empty after initialization"

    def test_register_entity(self):
        """Test registering an entity"""
        manager = DataEntityManager()

        manager.register_entity(
            "test_entity",
            name="Test Entity",
            partition_columns=["date"],
            merge_columns=["id"],
            tags={"env": "test"},
            schema={"id": "string"}
        )

        # Verify entity was registered
        assert "test_entity" in manager.registry, "Entity 'test_entity' should be in registry"

        entity = manager.registry["test_entity"]
        assert isinstance(
            entity, EntityMetadata), "Registered entity should be EntityMetadata instance"
        assert entity.entityid == "test_entity", "Entity entityid should be 'test_entity'"
        assert entity.name == "Test Entity", "Entity name should be 'Test Entity'"
        assert entity.partition_columns == [
            "date"], "Entity partition_columns should be ['date']"
        assert entity.merge_columns == [
            "id"], "Entity merge_columns should be ['id']"
        assert entity.tags == {
            "env": "test"}, "Entity tags should be {'env': 'test'}"
        assert entity.schema == {
            "id": "string"}, "Entity schema should be {'id': 'string'}"

    def test_register_multiple_entities(self):
        """Test registering multiple entities"""
        manager = DataEntityManager()

        manager.register_entity(
            "entity1",
            name="Entity 1",
            partition_columns=["date"],
            merge_columns=["id"],
            tags={},
            schema={}
        )

        manager.register_entity(
            "entity2",
            name="Entity 2",
            partition_columns=["region"],
            merge_columns=["key"],
            tags={"type": "dimension"},
            schema={}
        )

        assert len(manager.registry) == 2, "Registry should contain 2 entities"
        assert "entity1" in manager.registry, "Registry should contain 'entity1'"
        assert "entity2" in manager.registry, "Registry should contain 'entity2'"

    def test_register_entity_overwrites_existing(self):
        """Test that registering an entity with same ID overwrites the previous one"""
        manager = DataEntityManager()

        # Register first version
        manager.register_entity(
            "test_entity",
            name="First Version",
            partition_columns=["v1"],
            merge_columns=["id"],
            tags={"version": "1"},
            schema={}
        )

        # Register second version with same ID
        manager.register_entity(
            "test_entity",
            name="Second Version",
            partition_columns=["v2"],
            merge_columns=["id"],
            tags={"version": "2"},
            schema={}
        )

        # Should have only one entity
        assert len(
            manager.registry) == 1, "Registry should only contain 1 entity after overwrite"

        # Should be the second version
        entity = manager.registry["test_entity"]
        assert entity.name == "Second Version", "Entity name should be updated to 'Second Version'"
        assert entity.partition_columns == [
            "v2"], "Entity partition_columns should be updated to ['v2']"
        assert entity.tags == {
            "version": "2"}, "Entity tags should be updated to {'version': '2'}"

    def test_get_entity_ids_empty(self):
        """Test getting entity IDs from empty registry"""
        manager = DataEntityManager()

        entity_ids = manager.get_entity_ids()
        assert len(list(entity_ids)
                   ) == 0, "Empty registry should return no entity IDs"

    def test_get_entity_ids(self):
        """Test getting entity IDs"""
        manager = DataEntityManager()

        manager.register_entity("entity1", name="E1", partition_columns=[
        ], merge_columns=[], tags={}, schema={})
        manager.register_entity("entity2", name="E2", partition_columns=[
        ], merge_columns=[], tags={}, schema={})
        manager.register_entity("entity3", name="E3", partition_columns=[
        ], merge_columns=[], tags={}, schema={})

        entity_ids = list(manager.get_entity_ids())
        assert len(entity_ids) == 3, "Should return 3 entity IDs"
        assert "entity1" in entity_ids, "Should contain 'entity1'"
        assert "entity2" in entity_ids, "Should contain 'entity2'"
        assert "entity3" in entity_ids, "Should contain 'entity3'"

    def test_get_entity_definition_nonexistent(self):
        """Test getting definition for non-existent entity"""
        manager = DataEntityManager()

        result = manager.get_entity_definition("nonexistent")
        assert result is None, "Getting non-existent entity should return None"

    def test_get_entity_definition_existing(self):
        """Test getting definition for existing entity"""
        manager = DataEntityManager()

        manager.register_entity(
            "test_entity",
            name="Test Entity",
            partition_columns=["date"],
            merge_columns=["id"],
            tags={"env": "test"},
            schema={"id": "string"}
        )

        entity = manager.get_entity_definition("test_entity")
        assert entity is not None, "Getting existing entity should not return None"
        assert isinstance(
            entity, EntityMetadata), "Returned entity should be EntityMetadata instance"
        assert entity.entityid == "test_entity", "Entity entityid should match 'test_entity'"
        assert entity.name == "Test Entity", "Entity name should match 'Test Entity'"

    def test_implements_data_entity_registry_interface(self):
        """Test that DataEntityManager implements DataEntityRegistry interface"""
        assert issubclass(
            DataEntityManager, DataEntityRegistry), "DataEntityManager should be subclass of DataEntityRegistry"

        # Check all abstract methods are implemented
        manager = DataEntityManager()
        assert callable(
            manager.register_entity), "register_entity should be callable"
        assert callable(
            manager.get_entity_ids), "get_entity_ids should be callable"
        assert callable(
            manager.get_entity_definition), "get_entity_definition should be callable"


class TestDataEntitiesDecorator:
    """Tests for DataEntities.entity decorator"""

    def setup_method(self):
        """Reset DataEntities.deregistry before each test"""
        DataEntities.deregistry = None

    def test_decorator_validates_required_fields(self):
        """Test that decorator validates all required fields are present"""
        mock_registry = MagicMock()
        DataEntities.deregistry = mock_registry

        # Test missing all fields except entityid
        with pytest.raises(ValueError) as exc_info:
            DataEntities.entity(entityid="test_entity")

        error_message = str(exc_info.value)
        assert "Missing required fields" in error_message, "Error should mention 'Missing required fields'"
        assert "name" in error_message, "Error should mention missing 'name' field"
        assert "partition_columns" in error_message, "Error should mention missing 'partition_columns' field"
        assert "merge_columns" in error_message, "Error should mention missing 'merge_columns' field"
        assert "tags" in error_message, "Error should mention missing 'tags' field"
        assert "schema" in error_message, "Error should mention missing 'schema' field"

    def test_decorator_validates_partial_missing_fields(self):
        """Test that decorator validates when some fields are missing"""
        mock_registry = MagicMock()
        DataEntities.deregistry = mock_registry

        with pytest.raises(ValueError) as exc_info:
            DataEntities.entity(
                entityid="test_entity",
                name="Test Entity",
                partition_columns=["date"]
                # Missing: merge_columns, tags, schema
            )

        error_message = str(exc_info.value)
        assert "Missing required fields" in error_message, "Error should mention 'Missing required fields'"
        assert "merge_columns" in error_message, "Error should mention missing 'merge_columns'"
        assert "tags" in error_message, "Error should mention missing 'tags'"
        assert "schema" in error_message, "Error should mention missing 'schema'"
        # Should not complain about fields that were provided
        assert "name" not in error_message or "Missing" not in error_message, "Error should not mention provided 'name' field"

    def test_decorator_successful_registration(self):
        """Test successful entity registration with all required fields"""
        mock_registry = MagicMock()
        DataEntities.deregistry = mock_registry

        result = DataEntities.entity(
            entityid="test_entity",
            name="Test Entity",
            partition_columns=["date", "region"],
            merge_columns=["id"],
            tags={"source": "test"},
            schema={"id": "string", "value": "int"}
        )

        # Decorator should return None
        assert result is None, "Decorator should return None"

        # Should call register_entity on the registry
        mock_registry.register_entity.assert_called_once_with(
            "test_entity",
            name="Test Entity",
            partition_columns=["date", "region"],
            merge_columns=["id"],
            tags={"source": "test"},
            schema={"id": "string", "value": "int"}
        ), "Should call register_entity with correct parameters"

    def test_decorator_removes_entityid_from_params(self):
        """Test that decorator removes entityid before passing to register_entity"""
        mock_registry = MagicMock()
        DataEntities.deregistry = mock_registry

        DataEntities.entity(
            entityid="test_entity",
            name="Test Entity",
            partition_columns=[],
            merge_columns=[],
            tags={},
            schema={}
        )

        # Check that entityid was not passed to register_entity (first arg only)
        call_args = mock_registry.register_entity.call_args
        assert call_args[0][0] == "test_entity", "First positional arg should be entityid value 'test_entity'"
        assert 'entityid' not in call_args[1], "entityid should not be in kwargs passed to register_entity"

    def test_decorator_lazy_initializes_registry(self):
        """Test that decorator lazily initializes registry from GlobalInjector"""
        # Ensure deregistry is None
        DataEntities.deregistry = None

        # Mock GlobalInjector.get
        mock_registry = MagicMock()
        with patch.object(GlobalInjector, 'get', return_value=mock_registry) as mock_get:
            DataEntities.entity(
                entityid="test_entity",
                name="Test Entity",
                partition_columns=[],
                merge_columns=[],
                tags={},
                schema={}
            )

            # Should have called GlobalInjector.get to retrieve registry
            mock_get.assert_called_once(
            ), "Should call GlobalInjector.get exactly once to initialize registry"

            # Registry should be set
            assert DataEntities.deregistry == mock_registry, "deregistry should be set to the registry from GlobalInjector"

    def test_decorator_reuses_existing_registry(self):
        """Test that decorator reuses registry if already initialized"""
        mock_registry = MagicMock()
        DataEntities.deregistry = mock_registry

        with patch.object(GlobalInjector, 'get') as mock_get:
            DataEntities.entity(
                entityid="test_entity",
                name="Test Entity",
                partition_columns=[],
                merge_columns=[],
                tags={},
                schema={}
            )

            # Should NOT call GlobalInjector.get since registry already exists
            mock_get.assert_not_called(
            ), "Should not call GlobalInjector.get when registry already exists"

            # Should use the existing registry
            mock_registry.register_entity.assert_called_once(
            ), "Should call register_entity on existing registry"


class TestAbstractBaseClasses:
    """Tests for abstract base classes to ensure they define expected interfaces"""

    def test_entity_path_locator_interface(self):
        """Test EntityPathLocator has required abstract methods"""
        from abc import ABC

        assert issubclass(EntityPathLocator,
                          ABC), "EntityPathLocator should be subclass of ABC"

        # Cannot instantiate abstract class
        with pytest.raises(TypeError, match=".*abstract.*"):
            EntityPathLocator()

    def test_entity_name_mapper_interface(self):
        """Test EntityNameMapper has required abstract methods"""
        from abc import ABC

        assert issubclass(EntityNameMapper,
                          ABC), "EntityNameMapper should be subclass of ABC"

        # Cannot instantiate abstract class
        with pytest.raises(TypeError, match=".*abstract.*"):
            EntityNameMapper()

    def test_entity_provider_interface(self):
        """Test EntityProvider has required abstract methods"""
        from abc import ABC

        assert issubclass(
            EntityProvider, ABC), "EntityProvider should be subclass of ABC"

        # Cannot instantiate abstract class
        with pytest.raises(TypeError, match=".*abstract.*"):
            EntityProvider()

    def test_entity_provider_has_all_required_methods(self):
        """Test EntityProvider defines all required methods"""
        required_methods = [
            'ensure_entity_table',
            'check_entity_exists',
            'merge_to_entity',
            'append_to_entity',
            'read_entity',
            'read_entity_as_stream',
            'read_entity_since_version',
            'write_to_entity',
            'get_entity_version',
            'append_as_stream'
        ]

        for method_name in required_methods:
            assert hasattr(
                EntityProvider, method_name), f"EntityProvider should have abstract method '{method_name}'"

    def test_data_entity_registry_interface(self):
        """Test DataEntityRegistry has required abstract methods"""
        from abc import ABC

        assert issubclass(DataEntityRegistry,
                          ABC), "DataEntityRegistry should be subclass of ABC"

        # Cannot instantiate abstract class
        with pytest.raises(TypeError, match=".*abstract.*"):
            DataEntityRegistry()

        # Check required methods exist
        assert hasattr(
            DataEntityRegistry, 'register_entity'), "DataEntityRegistry should have 'register_entity' method"
        assert hasattr(
            DataEntityRegistry, 'get_entity_ids'), "DataEntityRegistry should have 'get_entity_ids' method"
        assert hasattr(
            DataEntityRegistry, 'get_entity_definition'), "DataEntityRegistry should have 'get_entity_definition' method"


class TestDataEntityManagerInjection:
    """Tests for DataEntityManager dependency injection integration"""

    def test_has_global_injector_singleton_decorator(self):
        """Test that DataEntityManager is decorated with singleton_autobind"""
        # The decorator should have been applied, but we can't easily test that
        # without accessing the GlobalInjector state. Instead, we verify the manager
        # can be instantiated and has the inject decorator
        manager = DataEntityManager()
        assert manager is not None, "DataEntityManager should be instantiable"

    def test_manager_is_injectable(self):
        """Test that DataEntityManager can be injected"""
        # This tests that the __init__ method has the @inject decorator
        import inspect

        # Check __init__ signature accepts no arguments beyond self
        sig = inspect.signature(DataEntityManager.__init__)
        params = list(sig.parameters.keys())
        assert params == [
            'self'], "DataEntityManager.__init__ should only accept 'self' parameter (no other dependencies)"


class TestEntityMetadataIntegration:
    """Integration tests for EntityMetadata with the registration system"""

    def test_full_registration_workflow(self):
        """Test complete workflow of registering and retrieving entities"""
        manager = DataEntityManager()

        # Register multiple entities with different configurations
        entities_to_register = [
            {
                "entityid": "customers",
                "name": "Customer Dimension",
                "partition_columns": ["country", "state"],
                "merge_columns": ["customer_id"],
                "tags": {"type": "dimension", "domain": "sales"},
                "schema": {"customer_id": "string", "name": "string", "email": "string"}
            },
            {
                "entityid": "orders",
                "name": "Order Facts",
                "partition_columns": ["order_date"],
                "merge_columns": ["order_id"],
                "tags": {"type": "fact", "domain": "sales"},
                "schema": {"order_id": "string", "customer_id": "string", "amount": "decimal"}
            },
            {
                "entityid": "products",
                "name": "Product Dimension",
                "partition_columns": [],
                "merge_columns": ["product_id"],
                "tags": {"type": "dimension", "domain": "catalog"},
                "schema": {"product_id": "string", "name": "string", "price": "decimal"}
            }
        ]

        # Register all entities
        for entity_data in entities_to_register:
            manager.register_entity(**entity_data)

        # Verify all were registered
        assert len(list(manager.get_entity_ids())
                   ) == 3, "Should have 3 registered entities"

        # Verify each entity can be retrieved correctly
        for entity_data in entities_to_register:
            entity = manager.get_entity_definition(entity_data["entityid"])
            assert entity is not None, f"Entity '{entity_data['entityid']}' should be retrievable"
            assert entity.entityid == entity_data[
                "entityid"], f"Entity entityid should match '{entity_data['entityid']}'"
            assert entity.name == entity_data[
                "name"], f"Entity name should match '{entity_data['name']}'"
            assert entity.partition_columns == entity_data[
                "partition_columns"], f"Entity partition_columns should match for '{entity_data['entityid']}'"
            assert entity.merge_columns == entity_data[
                "merge_columns"], f"Entity merge_columns should match for '{entity_data['entityid']}'"
            assert entity.tags == entity_data[
                "tags"], f"Entity tags should match for '{entity_data['entityid']}'"
            assert entity.schema == entity_data[
                "schema"], f"Entity schema should match for '{entity_data['entityid']}'"


class TestEntityProviderAbstractMethods:
    """Tests for EntityProvider abstract methods to ensure proper interface definition"""

    def test_entity_provider_requires_all_methods(self):
        """Test that EntityProvider requires implementation of all abstract methods"""
        # Try to create a class that doesn't implement all methods
        class IncompleteProvider(EntityProvider):
            # Only implement one method
            def ensure_entity_table(self, entity):
                return None

        # Should raise TypeError when trying to instantiate
        with pytest.raises(TypeError) as exc_info:
            IncompleteProvider()

        error_msg = str(exc_info.value)
        assert "abstract" in error_msg.lower(), "Error should mention abstract methods"

    def test_entity_provider_complete_implementation(self):
        """Test that a complete implementation of EntityProvider can be instantiated"""
        class CompleteProvider(EntityProvider):
            def ensure_entity_table(self, entity):
                pass

            def check_entity_exists(self, entity):
                pass

            def merge_to_entity(self, df, entity):
                pass

            def append_to_entity(self, df, entity):
                pass

            def read_entity(self, entity):
                pass

            def read_entity_as_stream(self, entity):
                pass

            def read_entity_since_version(self, entity, since_version):
                pass

            def write_to_entity(self, df, entity):
                pass

            def get_entity_version(self, entity):
                pass

            def append_as_stream(self, entity, df, checkpointLocation, format=None, options=None):
                pass

        # Should be able to instantiate
        provider = CompleteProvider()
        assert provider is not None, "Complete EntityProvider implementation should be instantiable"

        # Verify all methods are callable
        assert callable(provider.ensure_entity_table)
        assert callable(provider.check_entity_exists)
        assert callable(provider.merge_to_entity)
        assert callable(provider.append_to_entity)
        assert callable(provider.read_entity)
        assert callable(provider.read_entity_as_stream)
        assert callable(provider.read_entity_since_version)
        assert callable(provider.write_to_entity)
        assert callable(provider.get_entity_version)
        assert callable(provider.append_as_stream)


class TestEntityPathLocatorAbstractMethods:
    """Tests for EntityPathLocator abstract methods"""

    def test_entity_path_locator_requires_get_table_path(self):
        """Test that EntityPathLocator requires get_table_path implementation"""
        class IncompleteLocator(EntityPathLocator):
            pass

        with pytest.raises(TypeError) as exc_info:
            IncompleteLocator()

        assert "abstract" in str(exc_info.value).lower()

    def test_entity_path_locator_complete_implementation(self):
        """Test that a complete EntityPathLocator implementation can be instantiated"""
        class CompleteLocator(EntityPathLocator):
            def get_table_path(self, entity):
                return f"/path/to/{entity}"

        locator = CompleteLocator()
        assert locator is not None, "Complete EntityPathLocator should be instantiable"
        assert callable(locator.get_table_path)

        # Test method works
        result = locator.get_table_path("test_entity")
        assert result == "/path/to/test_entity"


class TestEntityNameMapperAbstractMethods:
    """Tests for EntityNameMapper abstract methods"""

    def test_entity_name_mapper_requires_get_table_name(self):
        """Test that EntityNameMapper requires get_table_name implementation"""
        class IncompleteMapper(EntityNameMapper):
            pass

        with pytest.raises(TypeError) as exc_info:
            IncompleteMapper()

        assert "abstract" in str(exc_info.value).lower()

    def test_entity_name_mapper_complete_implementation(self):
        """Test that a complete EntityNameMapper implementation can be instantiated"""
        class CompleteMapper(EntityNameMapper):
            def get_table_name(self, entity):
                return f"tbl_{entity}"

        mapper = CompleteMapper()
        assert mapper is not None, "Complete EntityNameMapper should be instantiable"
        assert callable(mapper.get_table_name)

        # Test method works
        result = mapper.get_table_name("test_entity")
        assert result == "tbl_test_entity"


class TestDataEntityRegistryAbstractMethods:
    """Tests for DataEntityRegistry abstract methods"""

    def test_data_entity_registry_requires_all_methods(self):
        """Test that DataEntityRegistry requires all abstract methods"""
        class IncompleteRegistry(DataEntityRegistry):
            def register_entity(self, entityid, **decorator_params):
                pass
            # Missing: get_entity_ids, get_entity_definition

        with pytest.raises(TypeError) as exc_info:
            IncompleteRegistry()

        assert "abstract" in str(exc_info.value).lower()

    def test_data_entity_registry_complete_implementation(self):
        """Test that a complete DataEntityRegistry implementation can be instantiated"""
        class CompleteRegistry(DataEntityRegistry):
            def register_entity(self, entityid, **decorator_params):
                pass

            def get_entity_ids(self):
                return []

            def get_entity_definition(self, name):
                return None

        registry = CompleteRegistry()
        assert registry is not None, "Complete DataEntityRegistry should be instantiable"
        assert callable(registry.register_entity)
        assert callable(registry.get_entity_ids)
        assert callable(registry.get_entity_definition)


class TestDataEntitiesEdgeCases:
    """Test edge cases and error conditions for DataEntities"""

    def setup_method(self):
        """Reset DataEntities.deregistry before each test"""
        DataEntities.deregistry = None

    def test_decorator_with_extra_fields(self):
        """Test that decorator accepts extra fields beyond required ones"""
        mock_registry = MagicMock()
        DataEntities.deregistry = mock_registry

        # Include an extra field not in EntityMetadata
        result = DataEntities.entity(
            entityid="test_entity",
            name="Test Entity",
            partition_columns=[],
            merge_columns=[],
            tags={},
            schema={},
            extra_field="extra_value"  # Extra field
        )

        # Should still work and pass extra field to register_entity
        mock_registry.register_entity.assert_called_once()
        call_kwargs = mock_registry.register_entity.call_args[1]
        assert 'extra_field' in call_kwargs
        assert call_kwargs['extra_field'] == "extra_value"

    def test_decorator_entityid_only_missing_field_error(self):
        """Test error message when only entityid is provided"""
        mock_registry = MagicMock()
        DataEntities.deregistry = mock_registry

        with pytest.raises(ValueError) as exc_info:
            DataEntities.entity(entityid="test_only")

        # All fields except entityid should be listed as missing
        error_msg = str(exc_info.value)
        required_fields = ['name', 'partition_columns',
                           'merge_columns', 'tags', 'schema']
        for field in required_fields:
            assert field in error_msg, f"Error message should mention missing field '{field}'"

    def test_entity_metadata_is_dataclass(self):
        """Test that EntityMetadata is properly defined as a dataclass"""
        from dataclasses import is_dataclass

        assert is_dataclass(
            EntityMetadata), "EntityMetadata should be a dataclass"

    def test_entity_metadata_equality(self):
        """Test that EntityMetadata instances can be compared for equality"""
        metadata1 = EntityMetadata(
            entityid="test",
            name="Test",
            partition_columns=["col1"],
            merge_columns=["id"],
            tags={"key": "value"},
            schema={"id": "string"}
        )

        metadata2 = EntityMetadata(
            entityid="test",
            name="Test",
            partition_columns=["col1"],
            merge_columns=["id"],
            tags={"key": "value"},
            schema={"id": "string"}
        )

        metadata3 = EntityMetadata(
            entityid="different",
            name="Different",
            partition_columns=["col2"],
            merge_columns=["key"],
            tags={},
            schema={}
        )

        assert metadata1 == metadata2, "Identical EntityMetadata instances should be equal"
        assert metadata1 != metadata3, "Different EntityMetadata instances should not be equal"

    def test_entity_metadata_repr(self):
        """Test that EntityMetadata has a useful string representation"""
        metadata = EntityMetadata(
            entityid="test_entity",
            name="Test Entity",
            partition_columns=["date"],
            merge_columns=["id"],
            tags={"env": "test"},
            schema={}
        )

        repr_str = repr(metadata)
        assert "EntityMetadata" in repr_str, "repr should contain class name"
        assert "test_entity" in repr_str, "repr should contain entityid value"

    def test_data_entity_manager_registry_is_mutable(self):
        """Test that the registry dict can be modified after initialization"""
        manager = DataEntityManager()

        # Directly modify registry
        manager.registry["manual_entry"] = EntityMetadata(
            entityid="manual_entry",
            name="Manual Entry",
            partition_columns=[],
            merge_columns=[],
            tags={},
            schema={}
        )

        # Should be retrievable
        entity = manager.get_entity_definition("manual_entry")
        assert entity is not None
        assert entity.entityid == "manual_entry"

    def test_data_entity_manager_get_entity_ids_returns_dict_keys(self):
        """Test that get_entity_ids returns a dict_keys view"""
        manager = DataEntityManager()
        manager.register_entity("entity1", name="E1", partition_columns=[],
                                merge_columns=[], tags={}, schema={})

        entity_ids = manager.get_entity_ids()

        # Should be dict_keys type
        assert hasattr(
            entity_ids, '__iter__'), "get_entity_ids should return iterable"

        # Should be dynamically updated when registry changes
        manager.register_entity("entity2", name="E2", partition_columns=[],
                                merge_columns=[], tags={}, schema={})

        # The dict_keys view should reflect the change
        assert len(list(entity_ids)
                   ) == 2, "dict_keys view should reflect registry updates"
