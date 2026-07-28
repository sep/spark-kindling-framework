"""Integration tests for the datapipes:/dataentities: config overlay (gh#30).

Boots the full framework standalone (fresh subprocess, like
tests/unit/test_di_wiring_standalone.py) with settings YAML carrying
datapipes/dataentities pattern sections, then verifies:

- effective pipe/entity metadata post-bootstrap (register-time overlay
  through the persisted matcher — registrations happen after bootstrap,
  the app-code lifecycle),
- an executed pipe proves overridden tags flow into execution (the
  output entity's declared provider_type would fail; the config override
  routes persistence to the memory provider),
- hot reload: mutate config, re-call bootstrap.apply_config_overrides(),
  metadata re-resolves from raw params with no accumulation.
"""

import subprocess
import sys
import textwrap

import pytest

pytestmark = [pytest.mark.integration]

SETTINGS_YAML = textwrap.dedent("""\
    datapipes:
      "it.**":
        tags:
          overlaid: "yes"
      "it.orders_to_summary":
        name: "Overridden Orders Summary"

    dataentities:
      "it.*":
        tags:
          layer: "test"
      "it.summary":
        tags:
          provider_type: "memory"
    """)


def _run_fresh_python(code: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        capture_output=True,
        text=True,
        timeout=300,
    )


def test_standalone_bootstrap_applies_overlay_and_hot_reloads(tmp_path):
    settings_path = tmp_path / "settings.yaml"
    settings_path.write_text(SETTINGS_YAML, encoding="utf-8")

    result = _run_fresh_python(f"""
        from kindling.bootstrap import apply_config_overrides, initialize_framework

        initialize_framework(
            {{
                "platform": "standalone",
                "environment": "local",
                "config_files": [{str(settings_path)!r}],
            }}
        )

        from kindling.data_entities import DataEntities, DataEntityRegistry
        from kindling.data_pipes import DataPipes, DataPipesExecution, DataPipesRegistry
        from kindling.injection import GlobalInjector, get_kindling_service
        from kindling.spark_config import ConfigService, get_or_create_spark_session

        # Registrations happen AFTER bootstrap's overlay pass (the app-code
        # lifecycle): the persisted matcher must overlay them at registration.
        DataEntities.entity(
            entityid="it.orders",
            name="orders",
            merge_columns=[],
            tags={{"provider_type": "memory"}},
            schema=None,
        )
        # Declared provider_type would fail at persist time if the config
        # override did not rewrite it to the memory provider.
        DataEntities.entity(
            entityid="it.summary",
            name="summary",
            merge_columns=[],
            tags={{"provider_type": "unregistered_provider"}},
            schema=None,
        )

        @DataPipes.pipe(
            pipeid="it.orders_to_summary",
            name="Orders To Summary",
            tags={{}},
            input_entity_ids=["it.orders"],
            output_entity_id="it.summary",
            output_type="memory",
        )
        def orders_to_summary(it_orders):
            return it_orders

        pipes_registry = GlobalInjector.get(DataPipesRegistry)
        entity_registry = GlobalInjector.get(DataEntityRegistry)

        pipe = pipes_registry.get_pipe_definition("it.orders_to_summary")
        assert pipe.tags.get("overlaid") == "yes", pipe.tags
        assert pipe.name == "Overridden Orders Summary", pipe.name
        assert pipe.execute is orders_to_summary

        summary = entity_registry.get_entity_definition("it.summary")
        assert summary.tags.get("provider_type") == "memory", summary.tags
        assert summary.tags.get("layer") == "test", summary.tags

        # Execute the pipe: overridden provider_type must drive persistence.
        spark = get_or_create_spark_session()
        spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"]).createOrReplaceTempView(
            "it_orders"
        )
        executor = GlobalInjector.get(DataPipesExecution)
        executor.run_datapipes(["it.orders_to_summary"])
        assert spark.table("it_summary").count() == 2

        # Hot reload: rewrite the settings file (drop the it.** pattern and
        # the whole dataentities section), reload config, re-call the
        # bootstrap helper. config_service.set() is unsuitable here — with
        # MERGE_ENABLED_FOR_DYNACONF it merges into the existing section
        # rather than replacing it.
        import pathlib

        pathlib.Path({str(settings_path)!r}).write_text(
            'datapipes:\\n  "it.orders_to_summary":\\n    name: "Second Name"\\n',
            encoding="utf-8",
        )
        config_service = get_kindling_service(ConfigService)
        reload_result = config_service.reload()
        assert reload_result["status"] == "success", reload_result
        apply_config_overrides()

        pipe = pipes_registry.get_pipe_definition("it.orders_to_summary")
        assert pipe.name == "Second Name", pipe.name
        # Re-resolution starts from raw decorator params: the first
        # overlay's tag must not survive (no accumulation).
        assert "overlaid" not in pipe.tags, pipe.tags

        summary = entity_registry.get_entity_definition("it.summary")
        assert summary.tags.get("provider_type") == "unregistered_provider", summary.tags
        assert "layer" not in summary.tags, summary.tags

        print("OVERLAY_INTEGRATION_OK")
        """)

    assert result.returncode == 0, result.stdout + result.stderr
    assert "OVERLAY_INTEGRATION_OK" in result.stdout
