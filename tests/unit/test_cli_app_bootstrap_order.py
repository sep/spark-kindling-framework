"""Regression tests for the `_load_app_module` fallback that lets `entity
tags` and `app validate` initialize the framework before a decorator-bearing
import fires, even when app.py imports its entity module above its own
`initialize()` definition.

These exercise real `@DataEntities.entity` registration (the bug only shows
up once decorator registration and framework init actually race), but stop
short of a real `initialize_framework()` call -- like
tests/unit/test_bootstrap_standalone.py, driving that for real inside
pytest fights the injector's auto_bind resolution once GlobalInjector.reset()
starts a process from a cold, purely-abstract-bindings state. Instead,
`_preinitialize_for_inspection` is replaced with a minimal stand-in that
answers just the two lookups the decorator path needs (a live
PlatformServiceProvider and the DataEntityRegistry) and otherwise defers to
the real injector.
"""

from pathlib import Path
from types import SimpleNamespace

import pytest
from kindling.data_entities import KindlingNotInitializedError
from kindling.injection import GlobalInjector
from kindling_cli.cli import _load_app_module


def _write_badly_ordered_app(app_dir: Path) -> Path:
    """An app.py that imports its entity module above its own `initialize()`
    definition -- decorator-bearing imports aren't deferred, the documented
    anti-pattern that used to make `entity tags`/`app validate` fail with
    KindlingNotInitializedError (see docs/guide/local_python_first.md)."""
    (app_dir / "entities.py").write_text(
        "from pyspark.sql.types import StringType, StructField, StructType\n"
        "from kindling.data_entities import DataEntities\n"
        "\n"
        "DataEntities.entity(\n"
        "    entityid='bronze.widgets',\n"
        "    name='widgets',\n"
        "    merge_columns=['id'],\n"
        "    tags={'provider_type': 'delta', 'layer': 'bronze'},\n"
        "    schema=StructType([StructField('id', StringType(), False)]),\n"
        ")\n",
        encoding="utf-8",
    )
    app_path = app_dir / "app.py"
    app_path.write_text(
        "import sys\n"
        "from pathlib import Path\n"
        "\n"
        "sys.path.insert(0, str(Path(__file__).resolve().parent))\n"
        "\n"
        "import entities  # noqa: F401 -- top-level, above initialize()\n"
        "\n"
        "\n"
        "def initialize(env=None):\n"
        "    pass\n",
        encoding="utf-8",
    )
    return app_path


@pytest.fixture(autouse=True)
def _reset_global_injector():
    GlobalInjector.reset()
    yield
    GlobalInjector.reset()


def test_load_app_module_without_preinitialize_still_raises(tmp_path):
    """Locks in that the fallback is opt-in: callers that don't pass
    `allow_preinitialize` (e.g. `pipeline run`) see the original failure
    unchanged."""
    app_path = _write_badly_ordered_app(tmp_path)

    with pytest.raises(KindlingNotInitializedError):
        _load_app_module(app_path, env="local")


def test_load_app_module_retries_after_preinitializing(tmp_path, monkeypatch):
    """The first load attempt fails exactly as above; `allow_preinitialize`
    catches that, pre-initializes, and retries -- so the entity ends up
    registered instead of the CLI command failing."""
    from kindling.data_entities import DataEntityManager, DataEntityRegistry
    from kindling.platform_provider import PlatformServiceProvider

    app_path = _write_badly_ordered_app(tmp_path)
    preinit_calls = []
    fake_registry = DataEntityManager()
    fake_platform_service_provider = SimpleNamespace(get_service=lambda: object())
    real_injector = GlobalInjector.get_injector()

    class _FakeInjector:
        """Answers the two lookups `_raise_if_not_initialized` and
        `DataEntities.entity` need with pre-built fakes; everything else
        falls through to the real (auto_bind) injector."""

        def get(self, iface):
            if iface is PlatformServiceProvider:
                return fake_platform_service_provider
            if iface is DataEntityRegistry:
                return fake_registry
            return real_injector.get(iface)

    def _fake_preinitialize(app_dir, env):
        preinit_calls.append((app_dir, env))
        monkeypatch.setattr(
            GlobalInjector, "get_injector", classmethod(lambda cls: _FakeInjector())
        )

    monkeypatch.setattr("kindling_cli.cli._preinitialize_for_inspection", _fake_preinitialize)

    _load_app_module(app_path, env="local", allow_preinitialize=True)

    assert len(preinit_calls) == 1
    assert "bronze.widgets" in fake_registry.get_entity_ids()
