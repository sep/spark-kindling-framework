# [implementer] cover real standalone DI construction boundary — TASK-20260430-002
import subprocess
import sys
import textwrap

import pytest


def _run_fresh_python(code: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        capture_output=True,
        text=True,
        timeout=90,
    )


@pytest.mark.requires_spark
def test_standalone_di_constructs_data_pipe_execution_with_null_watermark_finder() -> None:
    result = _run_fresh_python(
        """
        from kindling.bootstrap import initialize_framework
        from kindling.data_entities import DataEntityRegistry
        from kindling.data_pipes import DataPipesExecution, DataPipesRegistry
        from kindling.injection import GlobalInjector
        from kindling.watermarking import NullWatermarkEntityFinder, WatermarkEntityFinder

        initialize_framework({"platform": "standalone", "environment": "local"})

        executor = GlobalInjector.get(DataPipesExecution)
        GlobalInjector.get(DataEntityRegistry)
        GlobalInjector.get(DataPipesRegistry)
        executor.run_datapipes([])

        finder = GlobalInjector.get(WatermarkEntityFinder)
        assert isinstance(finder, NullWatermarkEntityFinder)

        try:
            finder.get_watermark_entity_for_entity("bronze.orders")
        except NotImplementedError:
            pass
        else:
            raise AssertionError("NullWatermarkEntityFinder should fail when used")
        """
    )

    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.requires_spark
def test_standalone_di_preserves_custom_watermark_finder_binding() -> None:
    result = _run_fresh_python(
        """
        from kindling.bootstrap import initialize_framework
        from kindling.injection import GlobalInjector
        from kindling.watermarking import WatermarkEntityFinder

        class CustomWatermarkEntityFinder(WatermarkEntityFinder):
            def get_watermark_entity_for_entity(self, context: str) -> str:
                return f"entity:{context}"

            def get_watermark_entity_for_layer(self, layer: str) -> str:
                return f"layer:{layer}"

        GlobalInjector.bind(WatermarkEntityFinder, CustomWatermarkEntityFinder)
        initialize_framework({"platform": "standalone", "environment": "local"})

        finder = GlobalInjector.get(WatermarkEntityFinder)
        assert isinstance(finder, CustomWatermarkEntityFinder)
        assert finder.get_watermark_entity_for_entity("bronze.orders") == "entity:bronze.orders"
        """
    )

    assert result.returncode == 0, result.stdout + result.stderr
