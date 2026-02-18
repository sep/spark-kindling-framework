"""
Unit tests for secret provider wiring and @secret resolution.

These tests codify the expected runtime behavior:
1. A SecretProvider contract exists in runtime code.
2. Platform services expose get_secret() for runtime retrieval.
3. The @secret loader resolves references using the SecretProvider.
4. Kindling registers its custom secret loader during config setup.
"""

import os
import tempfile
import importlib
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from dynaconf import Dynaconf

from kindling.config_loaders import load_secrets_from_provider, register_kindling_loaders
from kindling.injection import GlobalInjector


class TestSecretProviderContract:
    def teardown_method(self):
        GlobalInjector.reset()

    def test_secret_provider_interface_exposes_get_secret(self):
        from kindling.platform_provider import SecretProvider

        assert hasattr(SecretProvider, "get_secret"), "SecretProvider should define get_secret()"

    def test_platform_services_expose_get_secret_method(self):
        from kindling.platform_databricks import DatabricksService
        from kindling.platform_fabric import FabricService
        from kindling.platform_standalone import StandaloneService
        from kindling.platform_synapse import SynapseService

        for svc_class in (DatabricksService, FabricService, SynapseService, StandaloneService):
            assert hasattr(
                svc_class, "get_secret"
            ), f"{svc_class.__name__} should expose get_secret()"


class TestSecretLoaderResolution:
    def setup_method(self):
        GlobalInjector.reset()

    def teardown_method(self):
        GlobalInjector.reset()

    def test_loader_resolves_secret_reference_from_provider(self):
        from kindling.platform_provider import SecretProvider

        class FakeSecretProvider(SecretProvider):
            def get_secret(self, secret_name: str, default=None) -> str:
                if secret_name == "api-token":
                    return "resolved-token-value"
                if default is not None:
                    return default
                raise KeyError(secret_name)

        GlobalInjector.bind(SecretProvider, FakeSecretProvider())

        with tempfile.TemporaryDirectory() as td:
            settings_path = Path(td) / "settings.yaml"
            settings_path.write_text(
                "kindling:\n  external_api:\n    token: '@secret api-token'\n",
                encoding="utf-8",
            )

            settings = Dynaconf(
                settings_files=[str(settings_path)],
                environments=False,
                envvar_prefix="KINDLING",
            )

            load_secrets_from_provider(settings, silent=False)

            assert (
                settings.get("kindling.external_api.token") == "resolved-token-value"
            ), "Expected @secret value to be resolved via SecretProvider"

    @pytest.mark.parametrize(
        "secret_ref",
        [
            "@secret api-token",
            "@secret:api-token",
        ],
    )
    def test_loader_resolves_secret_reference_both_supported_formats(self, secret_ref):
        from kindling.platform_provider import SecretProvider

        class FakeSecretProvider(SecretProvider):
            def get_secret(self, secret_name: str, default=None) -> str:
                if secret_name == "api-token":
                    return "resolved-token-value"
                if default is not None:
                    return default
                raise KeyError(secret_name)

        GlobalInjector.bind(SecretProvider, FakeSecretProvider())

        with tempfile.TemporaryDirectory() as td:
            settings_path = Path(td) / "settings.yaml"
            settings_path.write_text(
                f"kindling:\n  external_api:\n    token: '{secret_ref}'\n",
                encoding="utf-8",
            )

            settings = Dynaconf(
                settings_files=[str(settings_path)],
                environments=False,
                envvar_prefix="KINDLING",
            )

            load_secrets_from_provider(settings, silent=False)

            assert (
                settings.get("kindling.external_api.token") == "resolved-token-value"
            ), "Expected @secret value to be resolved via SecretProvider"

    def test_loader_does_not_break_lazy_format_interpolation_paths(self):
        from kindling.platform_provider import SecretProvider

        class FakeSecretProvider(SecretProvider):
            def get_secret(self, secret_name: str, default=None) -> str:
                if secret_name == "svc-token":
                    return "resolved-token-value"
                if default is not None:
                    return default
                raise KeyError(secret_name)

        GlobalInjector.bind(SecretProvider, FakeSecretProvider())

        with tempfile.TemporaryDirectory() as td:
            settings_path = Path(td) / "settings.yaml"
            settings_path.write_text(
                (
                    "kindling:\n"
                    "  secrets:\n"
                    "    service:\n"
                    "      api_token: '@secret:svc-token'\n"
                    "  secret_templates:\n"
                    "    auth_header: '@format Bearer {this.secrets.service.api_token}'\n"
                ),
                encoding="utf-8",
            )

            settings = Dynaconf(
                settings_files=[str(settings_path)],
                environments=False,
                envvar_prefix="KINDLING",
            )

            load_secrets_from_provider(settings, silent=False)

            assert settings.get("kindling.secrets.service.api_token") == "resolved-token-value"


class TestSecretLoaderRegistration:
    def setup_method(self):
        GlobalInjector.reset()

    def teardown_method(self):
        GlobalInjector.reset()

    def test_config_setup_registers_secret_loader(self):
        os.environ.pop("LOADERS_FOR_DYNACONF", None)

        import kindling.spark_config as spark_config_module

        importlib.reload(spark_config_module)
        with (
            patch.object(
                spark_config_module, "get_or_create_spark_session", return_value=MagicMock()
            ),
            patch.object(spark_config_module, "Dynaconf", return_value=MagicMock()),
        ):
            spark_config_module.configure_injector_with_config(
                config_files=[],
                initial_config={},
                environment="development",
            )

        loaders = os.environ.get("LOADERS_FOR_DYNACONF", "")
        assert (
            "kindling.config_loaders" in loaders
        ), "Expected config setup to register Kindling secret loader"

    def test_registered_loaders_env_var_is_parseable_by_dynaconf(self):
        os.environ.pop("LOADERS_FOR_DYNACONF", None)
        register_kindling_loaders()

        settings = Dynaconf(settings_files=[], environments=False, envvar_prefix="KINDLING")
        # Trigger setup to ensure Dynaconf can import configured loaders.
        assert settings.get("nonexistent_key") is None
