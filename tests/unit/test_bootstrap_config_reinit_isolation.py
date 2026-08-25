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

Each test's actual repro runs in a subprocess, not in-process. In-process,
this test is at the mercy of whatever else already ran in the same pytest
worker: `GlobalInjector.reset()` (used elsewhere, e.g.
test_bootstrap_standalone.py) discards the module-level
`@singleton_autobind()` bindings (`DynaconfConfig`, `SparkPlatformServiceProvider`,
...) for good -- the next `initialize_framework()` call in that same
process fails outright (`Can't instantiate abstract class ConfigService`),
regardless of anything this file does. Under xdist's parallel test
distribution, which test lands in which worker (and in what order) isn't
under this file's control, so that failure showed up in CI even though a
plain sequential local run never hit it. A subprocess is immune to any of
that by construction -- exactly how this bug was originally isolated and
verified by hand.
"""

import json
import subprocess
import sys
import textwrap
from pathlib import Path


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


def _run_repro(script: str, tmp_path: Path) -> dict:
    """Run `script` in a fresh subprocess with tmp_path and a result-file
    path as argv, returning the JSON the script wrote to that file. Never
    parse subprocess stdout for the result -- Spark/Ivy/logging noise on
    stdout would make that fragile."""
    result_path = tmp_path / "result.json"
    proc = subprocess.run(
        [sys.executable, "-c", script, str(tmp_path), str(result_path)],
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert proc.returncode == 0, (
        f"repro subprocess failed (exit {proc.returncode})\n"
        f"--- stdout ---\n{proc.stdout}\n--- stderr ---\n{proc.stderr}"
    )
    return json.loads(result_path.read_text(encoding="utf-8"))


_DIFFERENT_ENV_REPRO = textwrap.dedent("""
    import json
    import sys

    from kindling.bootstrap import initialize_framework
    from kindling.data_entities import DataEntities, DataEntityRegistry
    from kindling.injection import GlobalInjector
    from pyspark.sql.types import StringType, StructField, StructType

    tmp_path, result_path = sys.argv[1], sys.argv[2]

    initialize_framework({
        "platform": "standalone",
        "environment": "other",
        "config_files": [f"{tmp_path}/settings.yaml", f"{tmp_path}/settings.other.yaml"],
        "install_bootstrap_dependencies": False,
    })
    initialize_framework({
        "platform": "standalone",
        "environment": "dev",
        "config_files": [f"{tmp_path}/settings.yaml", f"{tmp_path}/settings.dev.yaml"],
        "install_bootstrap_dependencies": False,
    })

    DataEntities.entity(
        entityid="bronze.widgets",
        name="widgets",
        merge_columns=["id"],
        tags={},
        schema=StructType([StructField("id", StringType(), False)]),
    )

    registry = GlobalInjector.get(DataEntityRegistry)
    entity = registry.get_entity_definition("bronze.widgets")
    with open(result_path, "w", encoding="utf-8") as f:
        json.dump({"tags": entity.tags}, f)
    """)


def test_reinit_with_different_environment_does_not_leak_prior_overlay(tmp_path):
    """A second initialize_framework() call in the same process, with an
    explicit different environment/config_files, must load *that* config --
    not silently keep the first call's."""
    _write_settings(tmp_path)

    result = _run_repro(_DIFFERENT_ENV_REPRO, tmp_path)

    assert result["tags"] == {"write.mode": "append"}


_SAME_CONFIG_REPRO = textwrap.dedent("""
    import json
    import sys

    from kindling.bootstrap import initialize_framework

    tmp_path, result_path = sys.argv[1], sys.argv[2]
    cfg = {
        "platform": "standalone",
        "environment": "dev",
        "config_files": [f"{tmp_path}/settings.yaml", f"{tmp_path}/settings.dev.yaml"],
        "install_bootstrap_dependencies": False,
    }

    first = initialize_framework(dict(cfg))
    second = initialize_framework(dict(cfg))
    with open(result_path, "w", encoding="utf-8") as f:
        json.dump({"same_instance": first is second}, f)
    """)


def test_reinit_with_same_config_still_short_circuits(tmp_path):
    """Calling initialize_framework() again with the *same*
    environment/config_files (e.g. a notebook cell re-run) must remain a
    cheap no-op -- this guards against overcorrecting the leak fix into
    always reloading."""
    _write_settings(tmp_path)

    result = _run_repro(_SAME_CONFIG_REPRO, tmp_path)

    assert result["same_instance"] is True
