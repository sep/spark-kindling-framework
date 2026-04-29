"""Unit tests for dynamic platform service discovery.

These tests codify the contract between distribution metadata, the
``spark_kindling.platforms`` entry-point group, and
``initialize_platform_services``. They're designed to catch regressions that
would silently ship a broken wheel:

- An entry point that doesn't resolve to a real module.
- A platform the framework expects to exist but that isn't advertised in
  distribution metadata (typo in ``pyproject.toml``, missing install step).
- The missing-extras error path losing its actionable message.
"""

import logging
from importlib.metadata import EntryPoint
from unittest.mock import MagicMock, patch

import pytest

EXPECTED_PLATFORMS = {"synapse", "databricks", "fabric", "standalone"}


class TestRegisteredPlatformEntryPoints:
    def test_all_expected_platforms_are_advertised(self):
        """The installed distribution must register every platform the
        framework ships with. Catches missing/typo'd entries in the
        ``[tool.poetry.plugins."spark_kindling.platforms"]`` section."""
        from kindling.bootstrap import _registered_platform_entry_points

        eps = _registered_platform_entry_points()

        missing = EXPECTED_PLATFORMS - set(eps)
        assert not missing, (
            f"Platforms expected but not advertised via entry points: {missing}. "
            f'Check [tool.poetry.plugins."spark_kindling.platforms"] in the '
            f"root pyproject.toml."
        )

    def test_every_entry_point_resolves_to_importable_module(self):
        """Each entry point must load without ImportError on the dev venv
        (which has every extra installed). Catches dead references."""
        from kindling.bootstrap import _registered_platform_entry_points

        eps = _registered_platform_entry_points()

        for name, ep in eps.items():
            # .load() imports the target; raises ImportError if the module
            # isn't importable with currently-installed packages.
            ep.load()


class TestInitializePlatformServicesErrors:
    """Contract: initialize_platform_services raises errors that tell the
    user exactly which ``pip install`` command fixes the problem."""

    def _fake_entry_point(self, name, value):
        return EntryPoint(name=name, value=value, group="spark_kindling.platforms")

    def test_missing_module_import_raises_with_extras_hint(self):
        from kindling import bootstrap

        broken_ep = self._fake_entry_point("synapse", "does_not_exist:Service")
        eps = {"synapse": broken_ep}

        with patch.object(bootstrap, "_registered_platform_entry_points", return_value=eps):
            with pytest.raises(RuntimeError, match=r"pip install 'spark-kindling\[synapse\]'"):
                bootstrap.initialize_platform_services(
                    "synapse", config={}, logger=logging.getLogger("test")
                )

    def test_unknown_platform_falls_back_to_module_convention(self):
        """If no entry point is registered, the loader falls back to the
        ``kindling.platform_<name>`` naming convention. That's the path
        exercised when in-tree code runs before the distribution is installed,
        and for third-party platform modules that don't add entry points."""
        from kindling import bootstrap

        with patch.object(bootstrap, "_registered_platform_entry_points", return_value={}):
            with pytest.raises(RuntimeError, match=r"pip install 'spark-kindling\[unknown\]'"):
                bootstrap.initialize_platform_services(
                    "unknown", config={}, logger=logging.getLogger("test")
                )

    def test_module_loads_but_registers_no_factory_raises_clear_error(self):
        """If an entry point resolves to a module that forgets the
        @PlatformServices.register decorator, fail loudly — don't NoneType."""
        from kindling import bootstrap
        from kindling.notebook_framework import PlatformServices

        # Point at a real module that does *not* register a "ghost" platform.
        ghost_ep = self._fake_entry_point("ghost", "kindling.injection")
        eps = {"ghost": ghost_ep}

        with patch.object(bootstrap, "_registered_platform_entry_points", return_value=eps):
            with patch.object(PlatformServices, "get_service_definition", return_value=None):
                with pytest.raises(RuntimeError, match=r"did not register a service"):
                    bootstrap.initialize_platform_services(
                        "ghost", config={}, logger=logging.getLogger("test")
                    )

    def test_factory_import_error_surfaces_extras_hint(self):
        """A factory whose body does ``from azure.synapse.artifacts import ...``
        lazily will ImportError at construction time if the [synapse] extra
        wasn't installed. That error must be converted to the actionable
        'install with ...' message, not left as a raw ImportError."""
        from kindling import bootstrap
        from kindling.notebook_framework import PlatformServices

        ep = self._fake_entry_point("synapse", "kindling.platform_synapse")
        eps = {"synapse": ep}

        broken_factory = MagicMock(
            side_effect=ImportError("No module named 'azure.synapse.artifacts'")
        )
        definition = MagicMock()
        definition.factory = broken_factory

        with patch.object(bootstrap, "_registered_platform_entry_points", return_value=eps):
            with patch.object(PlatformServices, "get_service_definition", return_value=definition):
                with pytest.raises(RuntimeError, match=r"pip install 'spark-kindling\[synapse\]'"):
                    bootstrap.initialize_platform_services(
                        "synapse", config={}, logger=logging.getLogger("test")
                    )


class TestDetectPlatformBranches:
    """Covers the detect_platform branches not already hit by
    test_bootstrap_standalone.py — in particular, explicit names and the
    config-level override path."""

    @pytest.mark.parametrize("platform", sorted(EXPECTED_PLATFORMS))
    def test_detect_platform_honors_explicit_platform_service_config(self, platform):
        from kindling.bootstrap import detect_platform

        assert detect_platform({"platform_service": platform}) == platform

    @pytest.mark.parametrize("platform", sorted(EXPECTED_PLATFORMS))
    def test_detect_platform_honors_explicit_platform_alias_config(self, platform):
        from kindling.bootstrap import detect_platform

        assert detect_platform({"platform": platform}) == platform

    def test_detect_platform_ignores_unknown_explicit_value(self):
        """Garbage in the config doesn't short-circuit detection."""
        from kindling.bootstrap import detect_platform

        mock_spark = MagicMock()
        mock_spark.conf.get.return_value = None

        with (
            patch("kindling.bootstrap.detect_platform_from_utils", return_value=None),
            patch("kindling.bootstrap.get_or_create_spark_session", return_value=mock_spark),
        ):
            # Unknown explicit value is ignored; with allow_standalone_fallback
            # we reach the final fallback.
            platform = detect_platform(
                {"platform": "not-a-real-platform", "allow_standalone_fallback": True}
            )

        assert platform == "standalone"
