"""
Streaming KDA Runner for Delta Live Tables

Specialized runner for streaming data applications using DLT.
"""

from typing import Dict, Any
from .dlt_pipeline import DLTPipelineManager


class StreamingKDARunner:
    """Runs streaming KDAs using Delta Live Tables"""

    def __init__(self, dlt_manager: DLTPipelineManager):
        self.dlt_manager = dlt_manager

    def run_streaming_app(self, app_name: str, config: Dict[str, Any]) -> str:
        """Run a KDA as a streaming DLT pipeline"""

        streaming_config = {
            'name': f"{app_name}-streaming",
            'mode': 'continuous',
            'source': config.get('source', 'delta-table'),
            'target': config.get('target', 'delta-table'),
            **config
        }

        return self.dlt_manager.deploy_streaming_kda(
            kda_path=f"apps/{app_name}/",
            pipeline_config=streaming_config
        )
