"""Regression tests for a config-layering leak across repeated
`initialize_framework()` calls in the same process.

`initialize_framework` short-circuits to "already initialized, skip
re-init" twice: once at its own top (via `is_framework_initialized()`) and
once inside its "config_init" phase (whenever `ConfigService.dynaconf` is
already set). Neither check used to compare the *requested*
`environment`/`config_files` against what was already loaded, so a second
call in the same process that explicitly asked for a different environment
silently kept the *first* call's Dynaconf state -- including its
`entity_tags:` overlays -- rather than loading what it was just told to
load. `kindling config show` (a pure YAML merge, no framework/Dynaconf
involved) never exhibited this, which is how the discrepancy between it and
`kindling entity tags` surfaced.

These exercise a real `initialize_framework()`/Dynaconf/entity-registration
path (not mocks) -- the bug is specifically about what real Dynaconf state
survives a second call, so mocking `configure_injector_with_config` away
(as tests/unit/test_bootstrap_standalone.py does for its unrelated
call-argument assertions) would hide it.
"""

from pathlib import Path

import pytest
from kindling.data_entities import DataEntities
from kindling.injection import GlobalInjector


def _write_settings(app_dir: Path) -> None:
    (app_dir / "settings.yaml").write_text(
        "entity_tags:\n" "  bronze.widgets:\n" "    write.mode: append\n",
        encoding="utf-8",
    )
    # The "other" environment's overlay carries a conflicting exact tag that
    # must never apply to a *different* environment's explicit config_files.
    (app_dir / "settings.other.yaml").write_text(
        "entity_tags:\n" "  bronze.widgets:\n" "    provider_type: memory\n",
        encoding="utf-8",
    )
    (app_dir / "settings.dev.yaml").write_text(
        "kindling:\n" "  telemetry:\n" "    logging:\n" "      level: DEBUG\n",
        encoding="utf-8",
    )


def _register_widgets_entity():
    from pyspark.sql.types import StringType, StructField, StructType

    DataEntities.entity(
        entityid="bronze.widgets",
        name="widgets",
        merge_columns=["id"],
        tags={},
        schema=StructType([StructField("id", StringType(), False)]),
    )


@pytest.fixture(autouse=True)
def _reset_entity_registry():
    """DataEntities.reset() only, not GlobalInjector.reset(): the module-
    level @singleton_autobind() registrations (DynaconfConfig,
    SparkPlatformServiceProvider, ...) run once at first import of
    `kindling`, binding into whichever Injector exists at that moment.
    GlobalInjector.reset() discards that Injector for good -- the next
    access builds a fresh auto_bind one with none of those bindings, so a
    real initialize_framework() call after a reset fails outright
    (`Can't instantiate abstract class ConfigService`). The bug under test
    is specifically about repeated initialize_framework() calls *without*
    a reset in between (e.g. two tests, or two notebook cells, in one
    process), so not resetting is also the faithful reproduction.

    A real `initialize_framework()` call also registers Kindling's custom
    Dynaconf secret loader via a process-wide `LOADERS_FOR_DYNACONF` env
    var (`register_kindling_loaders()`), which otherwise leaks into any
    later test in the same process that builds its own bare `Dynaconf(...)`
    expecting `@secret:` references to stay literal until it resolves them
    itself. Unregister it afterward so this file doesn't destabilize
    unrelated secret-resolution tests that happen to run later.
    """
    DataEntities.reset()
    yield
    DataEntities.reset()
    from kindling.config_loaders import unregister_kindling_loaders

    unregister_kindling_loaders()


def test_reinit_with_different_environment_does_not_leak_prior_overlay(tmp_path):
    """A second initialize_framework() call in the same process, with an
    explicit different environment/config_files, must load *that* config --
    not silently keep the first call's."""
    from kindling.bootstrap import initialize_framework
    from kindling.data_entities import DataEntityRegistry

    _write_settings(tmp_path)

    initialize_framework(
        {
            "platform": "standalone",
            "environment": "other",
            "config_files": [
                str(tmp_path / "settings.yaml"),
                str(tmp_path / "settings.other.yaml"),
            ],
            "install_bootstrap_dependencies": False,
        }
    )

    initialize_framework(
        {
            "platform": "standalone",
            "environment": "dev",
            "config_files": [
                str(tmp_path / "settings.yaml"),
                str(tmp_path / "settings.dev.yaml"),
            ],
            "install_bootstrap_dependencies": False,
        }
    )

    _register_widgets_entity()

    registry = GlobalInjector.get(DataEntityRegistry)
    entity = registry.get_entity_definition("bronze.widgets")

    assert entity.tags == {"write.mode": "append"}


def test_reinit_with_same_config_still_short_circuits(tmp_path):
    """Calling initialize_framework() again with the *same*
    environment/config_files (e.g. a notebook cell re-run) must remain a
    cheap no-op -- this guards against overcorrecting the leak fix into
    always reloading."""
    from kindling.bootstrap import initialize_framework

    _write_settings(tmp_path)
    cfg = {
        "platform": "standalone",
        "environment": "dev",
        "config_files": [
            str(tmp_path / "settings.yaml"),
            str(tmp_path / "settings.dev.yaml"),
        ],
        "install_bootstrap_dependencies": False,
    }

    first = initialize_framework(dict(cfg))
    second = initialize_framework(dict(cfg))

    assert first is second
