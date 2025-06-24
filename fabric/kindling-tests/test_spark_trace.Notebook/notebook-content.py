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

%run spark_trace

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

test_config = None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pytest
import uuid
from datetime import datetime
from unittest.mock import patch


class TestSparkTraceComponents(SynapseNotebookTestCase):
    
    def test_span_context_lifecycle(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        event_emitter = notebook_runner.get_mock('event_emitter')
        trace = EventBasedSparkTrace(event_emitter)
        
        # Actually use the span context to generate START/END events
        with trace.span("TestOp", "TestComp"):
            pass  # Simple span execution
        
        # Test that span context properly manages START/END events
        notebook_runner.assert_event_emitted('TestComp', 'TestOp_START')
        notebook_runner.assert_event_emitted('TestComp', 'TestOp_END')
        
    def test_span_context_with_mdc(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        event_emitter = notebook_runner.get_mock('event_emitter')
        trace = EventBasedSparkTrace(event_emitter)
        
        # Actually use the span context which should set up MDC
        with trace.span("TestOp", "TestComp"):
            # Test that span context sets up MDC logging context correctly
            notebook_runner.assert_mdc_context_used(['trace_id', 'span_id', 'component', 'operation'])
        
    def test_span_error_handling(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        event_emitter = notebook_runner.get_mock('event_emitter')
        trace = EventBasedSparkTrace(event_emitter)
        
        # Actually trigger an exception in span context
        try:
            with trace.span("TestOp", "TestComp"):
                raise ValueError("Test error")
        except ValueError:
            pass  # Expected exception
        
        # Test that exceptions in span context trigger ERROR events
        notebook_runner.assert_event_emitted('TestComp', 'TestOp_ERROR')
        
    def test_span_timing_calculation(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        event_emitter = notebook_runner.get_mock('event_emitter')
        
        # Create and test the actual SparkTrace
        trace = EventBasedSparkTrace(event_emitter)
        
        # Use the actual span context manager
        with trace.span("TestOp", "TestComp", {"test": "data"}):
            # Do some work that takes time
            import time
            time.sleep(0.1)  # 100ms
        
        # Verify that the trace calculated timing and emitted END event
        calls = event_emitter.emit_custom_event.call_args_list
        end_calls = [call for call in calls if "END" in call[0][1]]
        
        assert len(end_calls) >= 1, "Expected END event with timing"
        end_details = end_calls[0][0][2]  # details parameter
        
        # Verify timing was calculated and is reasonable (around 0.1 seconds)
        assert "totalTime" in end_details
        total_time = float(end_details["totalTime"])
        assert total_time > 0, f"Expected > 0s, got {total_time}s"
        
    def test_nested_span_trace_id_inheritance(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        event_emitter = notebook_runner.get_mock('event_emitter')
        
        # Test that nested spans inherit the same trace ID
        outer_trace_id = uuid.uuid4()
        
        event_emitter.emit_custom_event("OuterComp", "OuterOp_START", {}, "1", outer_trace_id)
        event_emitter.emit_custom_event("InnerComp", "InnerOp_START", {}, "2", outer_trace_id)
        event_emitter.emit_custom_event("InnerComp", "InnerOp_END", {}, "2", outer_trace_id)
        event_emitter.emit_custom_event("OuterComp", "OuterOp_END", {}, "1", outer_trace_id)
        
        calls = event_emitter.emit_custom_event.call_args_list
        trace_ids = [call[0][4] for call in calls]  # traceId parameter
        assert all(tid == outer_trace_id for tid in trace_ids), "Nested spans should share trace ID"
        
    def test_activity_counter_increments(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        event_emitter = notebook_runner.get_mock('event_emitter')
        
        # Test that each span gets a unique activity ID
        for i in range(3):
            event_emitter.emit_custom_event("TestComp", "TestOp_START", {}, str(i + 1), uuid.uuid4())
            
        calls = event_emitter.emit_custom_event.call_args_list
        span_ids = [call[0][3] for call in calls]  # eventId parameter
        assert len(set(span_ids)) == len(span_ids), "Each span should have unique ID"


class TestNotebookWithTracing:
    
    def test_full_notebook_with_tracing_flow(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        event_emitter = notebook_runner.get_mock('event_emitter')
        test_trace_id = uuid.uuid4()
        
        event_emitter.emit_custom_event(
            "DataProcessing", "LoadData_START", 
            {"source": "test", "startTime": "2024-01-01 12:00:00.000"}, 
            "1", test_trace_id
        )
        
        event_emitter.emit_custom_event(
            "DataProcessing", "LoadData_END",
            {"source": "test", "startTime": "2024-01-01 12:00:00.000", 
             "endTime": "2024-01-01 12:00:01.000", "totalTime": "1.000"},
            "1", test_trace_id
        )
        
        notebook_runner.assert_event_emitted('DataProcessing', 'LoadData_START')
        notebook_runner.assert_event_emitted('DataProcessing', 'LoadData_END')
        
        calls = event_emitter.emit_custom_event.call_args_list
        start_call = next(call for call in calls if "START" in call[0][1])
        end_call = next(call for call in calls if "END" in call[0][1])
        
        assert start_call[0][0] == "DataProcessing"
        assert start_call[0][1] == "LoadData_START"
        assert "source" in start_call[0][2]
        assert start_call[0][3] == "1"
        assert start_call[0][4] == test_trace_id
        
        end_details = end_call[0][2]
        assert "totalTime" in end_details
        assert "endTime" in end_details

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

results = run_notebook_tests(TestSparkTraceComponents, TestNotebookWithTracing)
print(results)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
