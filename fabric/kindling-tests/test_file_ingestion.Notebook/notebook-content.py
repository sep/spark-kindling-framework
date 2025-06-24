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

%run spark_config

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run file_ingestion

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

test_config = {
    'use_real_spark': False,
    'test_data': {
        'test_path': '/test/data/path',
        'sample_files': ['sales_west_20240315.csv', 'sales_east_20240316.json']
    }
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pytest
import uuid
from unittest.mock import MagicMock, patch
from dataclasses import dataclass, fields
import re

class TestFileIngestionMetadata(SynapseNotebookTestCase):
    """Tests for FileIngestionMetadata dataclass"""
    
    def test_file_ingestion_metadata_creation(self, notebook_runner, basic_test_config):
        """Test FileIngestionMetadata can be created with all required fields"""
        notebook_runner.prepare_test_environment(basic_test_config)
        
        # After %run of your notebook, test dataclass creation
        metadata = FileIngestionMetadata(
            entry_id="test_entry_1",
            name="Test File Ingestion",
            patterns=["sales_(?P<region>\\w+)_(?P<date>\\d{8})\\.(?P<filetype>csv|json)"],
            dest_entity_id="sales_{region}_{date}",
            tags={"source": "external", "category": "sales"},
            infer_schema=True
        )
        
        assert metadata.entry_id == "test_entry_1"
        assert metadata.name == "Test File Ingestion"
        assert len(metadata.patterns) == 1
        assert metadata.dest_entity_id == "sales_{region}_{date}"
        assert metadata.tags["source"] == "external"
        assert metadata.infer_schema is True
        
    def test_file_ingestion_metadata_defaults(self, notebook_runner, basic_test_config):
        """Test FileIngestionMetadata uses default values correctly"""
        notebook_runner.prepare_test_environment(basic_test_config)
        
        # Test with default infer_schema value
        metadata = FileIngestionMetadata(
            entry_id="test_entry_2",
            name="Test File",
            patterns=["test_pattern"],
            dest_entity_id="test_entity",
            tags={}
        )
        
        assert metadata.infer_schema is True  # Default value


class TestFileIngestionEntries(SynapseNotebookTestCase):
    """Tests for FileIngestionEntries decorator functionality"""
    
    def test_entry_decorator_registers_entry(self, notebook_runner, basic_test_config):
        """Test that @FileIngestionEntries.entry decorator registers entries correctly"""
        notebook_runner.prepare_test_environment(basic_test_config)
        
        # Mock the registry that gets injected
        mock_registry = MagicMock()
        
        # Set up GlobalInjector to return our mock
        globals()['GlobalInjector'].get = MagicMock(return_value=mock_registry)
        
        # After %run, test the decorator
        FileIngestionEntries.entry(
            entry_id="sales_files",
            name="Sales File Ingestion",
            patterns=["sales_(?P<region>\\w+)_(?P<date>\\d{8})\\.(?P<filetype>csv)"],
            dest_entity_id="sales_{region}",
            tags={"category": "sales"}
        )
        
        # Verify registry was called correctly
        mock_registry.register_entry.assert_called_once()
        call_args = mock_registry.register_entry.call_args
        
        assert call_args[0][0] == "sales_files"  # entryId
        assert call_args[1]['name'] == "Sales File Ingestion"
        assert call_args[1]['patterns'] == ["sales_(?P<region>\\w+)_(?P<date>\\d{8})\\.(?P<filetype>csv)"]
        assert call_args[1]['dest_entity_id'] == "sales_{region}"
        assert call_args[1]['infer_schema'] is True  # Default value
        
    def test_entry_decorator_missing_required_fields(self, notebook_runner, basic_test_config):
        """Test that decorator raises error for missing required fields"""
        notebook_runner.prepare_test_environment(basic_test_config)
        
        mock_registry = MagicMock()
        globals()['GlobalInjector'].get = MagicMock(return_value=mock_registry)

        # After %run, test validation
        with pytest.raises(ValueError, match="Missing required fields"):
            FileIngestionEntries.entry(
                entry_id="incomplete_entry",
                name="Incomplete Entry"
                # Missing patterns, dest_entity_id, tags
            )
            
    def test_entry_decorator_with_explicit_infer_schema(self, notebook_runner, basic_test_config):
        """Test decorator respects explicit infer_schema setting"""
        notebook_runner.prepare_test_environment(basic_test_config)
        
        mock_registry = MagicMock()
        globals()['GlobalInjector'].get = MagicMock(return_value=mock_registry)
        
        # Test with infer_schema=False
        FileIngestionEntries.entry(
            entry_id="test_entry",
            name="Test Entry",
            patterns=["test_pattern"],
            dest_entity_id="test_entity",
            tags={},
            infer_schema=False
        )
        
        call_args = mock_registry.register_entry.call_args
        assert call_args[1]['infer_schema'] is False


class TestFileIngestionManager(SynapseNotebookTestCase):
    """Tests for FileIngestionManager registry operations"""
    
    def test_register_and_retrieve_entry(self, notebook_runner, basic_test_config):
        """Test FileIngestionManager can register and retrieve entries"""
        notebook_runner.prepare_test_environment(basic_test_config)
        
        # After %run, create manager instance
        manager = FileIngestionManager()
        
        # Register an entry
        manager.register_entry(
            "test_entry",
            name="Test Entry",
            patterns=["test_pattern"],
            dest_entity_id="test_entity",
            tags={"category": "test"}
        )
        
        # Verify entry was registered
        entry_ids = manager.get_entry_ids()
        assert "test_entry" in entry_ids
        
        # Verify entry definition
        definition = manager.get_entry_definition("test_entry")
        assert definition.entry_id == "test_entry"
        assert definition.name == "Test Entry"
        assert definition.patterns == ["test_pattern"]
        assert definition.dest_entity_id == "test_entity"
        assert definition.tags["category"] == "test"
        
    def test_get_nonexistent_entry(self, notebook_runner, basic_test_config):
        """Test getting definition for non-existent entry returns None"""
        notebook_runner.prepare_test_environment(basic_test_config)
        
        manager = FileIngestionManager()
        definition = manager.get_entry_definition("nonexistent")
        assert definition is None
        
    def test_multiple_entries_registration(self, notebook_runner, basic_test_config):
        """Test multiple entries can be registered and retrieved"""
        notebook_runner.prepare_test_environment(basic_test_config)
        
        manager = FileIngestionManager()
        
        # Register multiple entries
        manager.register_entry("entry1", name="Entry 1", patterns=["pattern1"], dest_entity_id="entity1", tags={})
        manager.register_entry("entry2", name="Entry 2", patterns=["pattern2"], dest_entity_id="entity2", tags={})
        
        entry_ids = list(manager.get_entry_ids())
        assert len(entry_ids) == 2
        assert "entry1" in entry_ids
        assert "entry2" in entry_ids


class TestSimpleFileIngestionProcessor(SynapseNotebookTestCase):
    """Tests for SimpleFileIngestionProcessor file processing logic"""
    
    def setup_file_processor_mocks(self, notebook_runner):
        """Setup mocks specific to file processor testing"""
        # Mock notebookutils.fs.ls to return test file list
        mock_file1 = MagicMock()
        mock_file1.name = "sales_west_20240315.csv"
        mock_file1.isFile = True
        
        mock_file2 = MagicMock()
        mock_file2.name = "sales_east_20240316.json" 
        mock_file2.isFile = True
        
        mock_dir = MagicMock()
        mock_dir.name = "archive"
        mock_dir.isFile = False  # Directory
        
        # Configure notebookutils mock
        globals()['notebookutils'].fs.ls = MagicMock(return_value=[mock_file1, mock_file2, mock_dir])
        
        # Setup file ingestion registry mock
        mock_registry = MagicMock()
        mock_entry = MagicMock()
        mock_entry.patterns = ["sales_(?P<region>\\w+)_(?P<date>\\d{8})\\.(?P<filetype>csv|json)"]
        mock_entry.dest_entity_id = "sales_{region}_{date}"
        mock_entry.infer_schema = True
        
        mock_registry.get_entry_ids.return_value = ["sales_pattern"]
        mock_registry.get_entry_definition.return_value = mock_entry
        
        # Setup other required mocks
        mock_entity_provider = MagicMock()
        mock_entity_registry = MagicMock()
        mock_entity_def = MagicMock()
        mock_entity_registry.get_entity_definition.return_value = mock_entity_def
        
        mock_trace_provider = MagicMock()
        mock_span = MagicMock()
        mock_span.__enter__ = MagicMock(return_value=mock_span)
        mock_span.__exit__ = MagicMock(return_value=None)
        mock_trace_provider.span.return_value = mock_span
        
        mock_logger_provider = MagicMock()
        mock_logger = MagicMock()
        mock_logger_provider.get_logger.return_value = mock_logger

        mock_config = MagicMock()
        
        return {
            'c': mock_config,
            'fir': mock_registry,
            'ep': mock_entity_provider, 
            'der': mock_entity_registry,
            'tp': mock_trace_provider,
            'lp': mock_logger_provider
        }
    
    def test_process_path_with_matching_files(self, notebook_runner, basic_test_config):
        """Test processing path with files that match ingestion patterns"""
        notebook_runner.prepare_test_environment(basic_test_config)
        
        mocks = self.setup_file_processor_mocks(notebook_runner)
        
        # Create processor with mocked dependencies
        processor = SimpleFileIngestionProcessor(
            config=mocks['c'],
            fir=mocks['fir'],
            ep=mocks['ep'],
            der=mocks['der'], 
            tp=mocks['tp'],
            lp=mocks['lp']
        )
        
        # Mock spark read operations
        mock_df = MagicMock()
        spark_read_mock = MagicMock()
        spark_read_mock.format.return_value = spark_read_mock
        spark_read_mock.option.return_value = spark_read_mock
        spark_read_mock.load.return_value = mock_df
        
        spark = notebook_runner.test_env.spark_session
        spark.read = spark_read_mock
        
        # Process the path
        processor.process_path("/test/path")
        
        # Verify notebookutils.fs.ls was called
        globals()['notebookutils'].fs.ls.assert_called_once_with("/test/path")
        
        # Verify file ingestion registry was queried
        mocks['fir'].get_entry_ids.assert_called_once()
        mocks['fir'].get_entry_definition.assert_called()
        
        # Verify spark read was called for matching files
        assert spark_read_mock.format.call_count == 2  # 2 matching files
        assert spark_read_mock.load.call_count == 2
        
        # Verify entity merge was called
        assert mocks['ep'].merge_to_entity.call_count == 2
        
    def test_process_path_with_no_matching_files(self, notebook_runner, basic_test_config):
        """Test processing path with no files matching patterns"""
        notebook_runner.prepare_test_environment(basic_test_config)
        
        # Setup non-matching files
        mock_file = MagicMock()
        mock_file.name = "random_file.txt"
        mock_file.isFile = True
        
        globals()['notebookutils'].fs.ls = MagicMock(return_value=[mock_file])
        
        mocks = self.setup_file_processor_mocks(notebook_runner)
        
        processor = SimpleFileIngestionProcessor(
            config=mocks['c'],
            fir=mocks['fir'],
            ep=mocks['ep'],
            der=mocks['der'],
            tp=mocks['tp'], 
            lp=mocks['lp']
        )
        
        # Process the path
        processor.process_path("/test/path")
        
        # Verify no merges occurred
        mocks['ep'].merge_to_entity.assert_not_called()
        
    def test_process_path_pattern_matching_details(self, notebook_runner, basic_test_config):
        """Test detailed pattern matching and entity ID formatting"""
        notebook_runner.prepare_test_environment(basic_test_config)
        
        # Setup specific test file
        mock_file = MagicMock()
        mock_file.name = "sales_northeast_20240401.csv"
        mock_file.isFile = True
        
        globals()['notebookutils'].fs.ls = MagicMock(return_value=[mock_file])
        
        mocks = self.setup_file_processor_mocks(notebook_runner)
        
        processor = SimpleFileIngestionProcessor(
            config=mocks['c'],
            fir=mocks['fir'],
            ep=mocks['ep'],
            der=mocks['der'],
            tp=mocks['tp'],
            lp=mocks['lp']
        )
        
        # Mock spark read chain
        mock_df = MagicMock()
        spark_read_mock = MagicMock()
        spark_read_mock.format.return_value = spark_read_mock
        spark_read_mock.option.return_value = spark_read_mock
        spark_read_mock.load.return_value = mock_df
        
        spark = notebook_runner.test_env.spark_session
        spark.read = spark_read_mock
        
        # Process the path
        processor.process_path("/test/path")
        
        # Verify entity registry was called with formatted entity ID
        # Expected: "sales_northeast_20240401" based on pattern and filename
        mocks['der'].get_entity_definition.assert_called_with("sales_northeast_20240401")
        
        # Verify spark read was configured correctly
        spark_read_mock.format.assert_called_with("csv")  # Extracted from filename
        
        # Verify options were set correctly
        option_calls = spark_read_mock.option.call_args_list
        assert any(call[0] == ("header", "true") for call in option_calls)
        assert any(call[0] == ("inferSchema", "true") for call in option_calls)
        
    def test_process_path_with_infer_schema_false(self, notebook_runner, basic_test_config):
        """Test processing with infer_schema=False"""
        notebook_runner.prepare_test_environment(basic_test_config)
        
        mock_file = MagicMock()
        mock_file.name = "sales_west_20240315.csv"
        mock_file.isFile = True
        
        globals()['notebookutils'].fs.ls = MagicMock(return_value=[mock_file])
        
        mocks = self.setup_file_processor_mocks(notebook_runner)
        
        # Override entry to have infer_schema=False
        mock_entry = MagicMock()
        mock_entry.patterns = ["sales_(?P<region>\\w+)_(?P<date>\\d{8})\\.(?P<filetype>csv)"]
        mock_entry.dest_entity_id = "sales_{region}_{date}"
        mock_entry.infer_schema = False
        mocks['fir'].get_entry_definition.return_value = mock_entry
        
        processor = SimpleFileIngestionProcessor(
            config=mocks['c'],
            fir=mocks['fir'],
            ep=mocks['ep'],
            der=mocks['der'],
            tp=mocks['tp'],
            lp=mocks['lp']
        )
        
        # Mock spark read
        spark_read_mock = MagicMock()
        spark_read_mock.format.return_value = spark_read_mock
        spark_read_mock.option.return_value = spark_read_mock
        spark_read_mock.load.return_value = MagicMock()
        
        spark = notebook_runner.test_env.spark_session
        spark.read = spark_read_mock
        
        # Process the path
        processor.process_path("/test/path")
        
        # Verify inferSchema was set to "false"
        option_calls = spark_read_mock.option.call_args_list
        assert any(call[0] == ("inferSchema", "false") for call in option_calls)
        
    def test_process_path_trace_spans_created(self, notebook_runner, basic_test_config):
        """Test that trace spans are created during processing"""
        notebook_runner.prepare_test_environment(basic_test_config)
        
        mock_file = MagicMock()
        mock_file.name = "sales_west_20240315.csv"
        mock_file.isFile = True
        
        globals()['notebookutils'].fs.ls = MagicMock(return_value=[mock_file])
        
        mocks = self.setup_file_processor_mocks(notebook_runner)
        
        processor = SimpleFileIngestionProcessor(
            config=mocks['c'],
            fir=mocks['fir'],
            ep=mocks['ep'],
            der=mocks['der'],
            tp=mocks['tp'],
            lp=mocks['lp']
        )
        
        # Mock spark read
        spark_read_mock = MagicMock()
        spark_read_mock.format.return_value = spark_read_mock  
        spark_read_mock.option.return_value = spark_read_mock
        spark_read_mock.load.return_value = MagicMock()
        
        spark = notebook_runner.test_env.spark_session
        spark.read = spark_read_mock
        
        # Process the path
        processor.process_path("/test/path")
        
        # Verify trace spans were created
        span_calls = mocks['tp'].span.call_args_list
        
        # Should have main span + spans for matching and merging
        assert len(span_calls) >= 3
        
        # Verify main span
        main_span_call = span_calls[0]
        assert main_span_call[1]['component'] == "SimpleFileIngestionProcessor"
        assert main_span_call[1]['operation'] == "process_path"
        
        # Verify operation-specific spans
        operation_spans = [call[1]['operation'] for call in span_calls if 'operation' in call[1]]
        assert "ingest_on_match" in operation_spans
        assert "merge_to_entity" in operation_spans


# Integration test for the complete file ingestion flow
class TestFileIngestionIntegration(SynapseNotebookTestCase):
    """Integration tests for complete file ingestion workflow"""
    
    def test_end_to_end_file_ingestion(self, notebook_runner, basic_test_config):
        """Test complete file ingestion from registration to processing"""
        notebook_runner.prepare_test_environment(basic_test_config)
        
        # Step 1: Register file ingestion entry
        manager = FileIngestionManager()
        manager.register_entry(
            "sales_files",
            name="Sales File Ingestion",
            patterns=["sales_(?P<region>\\w+)_(?P<date>\\d{8})\\.(?P<filetype>csv)"],
            dest_entity_id="sales_{region}_{date}",
            tags={"category": "sales"},
            infer_schema=True
        )
        
        # Step 2: Setup file system mock
        mock_file = MagicMock()
        mock_file.name = "sales_west_20240315.csv"
        mock_file.isFile = True
        
        globals()['notebookutils'].fs.ls = MagicMock(return_value=[mock_file])
        
        # Step 3: Setup other mocks
        mock_entity_provider = MagicMock()
        mock_entity_registry = MagicMock()
        mock_entity_def = MagicMock()
        mock_entity_registry.get_entity_definition.return_value = mock_entity_def
        
        mock_trace_provider = MagicMock()
        mock_span = MagicMock()
        mock_span.__enter__ = MagicMock(return_value=mock_span)
        mock_span.__exit__ = MagicMock(return_value=None)
        mock_trace_provider.span.return_value = mock_span
        
        mock_logger_provider = MagicMock()
        mock_logger = MagicMock()
        mock_logger_provider.get_logger.return_value = mock_logger
        
        mock_config = MagicMock()

        # Step 4: Create processor and process files
        processor = SimpleFileIngestionProcessor(
            config=mock_config,
            fir=manager,  # Use real manager
            ep=mock_entity_provider,
            der=mock_entity_registry,
            tp=mock_trace_provider,
            lp=mock_logger_provider
        )
        
        # Mock spark read
        mock_df = MagicMock()
        spark_read_mock = MagicMock()
        spark_read_mock.format.return_value = spark_read_mock
        spark_read_mock.option.return_value = spark_read_mock
        spark_read_mock.load.return_value = mock_df
        
        spark = notebook_runner.test_env.spark_session
        spark.read = spark_read_mock
        
        # Process the path
        processor.process_path("/test/path")
        
        # Step 5: Verify end-to-end flow
        # File was read with correct format
        spark_read_mock.format.assert_called_with("csv")
        
        # Entity definition was retrieved with correct formatted ID
        mock_entity_registry.get_entity_definition.assert_called_with("sales_west_20240315")
        
        # Data was merged to entity
        mock_entity_provider.merge_to_entity.assert_called_once_with(mock_df, mock_entity_def)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

results = run_notebook_tests(
    TestFileIngestionMetadata,
    TestFileIngestionEntries, 
    TestFileIngestionManager,
    TestSimpleFileIngestionProcessor,
    TestFileIngestionIntegration,
    test_config=test_config
)
print(results)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
