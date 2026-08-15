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
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from dynaconf import Dynaconf

from kindling.config_loaders import (
    find_unresolved_secret_references,
    load_secrets_from_provider,
    register_kindling_loaders,
)
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


class TestSecretProviderContractMethods:
    """Tests for secret_exists() and list_secrets() contract methods."""

    def teardown_method(self):
        GlobalInjector.reset()

    def test_secret_provider_interface_exposes_secret_exists(self):
        from kindling.platform_provider import SecretProvider

        assert hasattr(
            SecretProvider, "secret_exists"
        ), "SecretProvider should define secret_exists()"

    def test_secret_provider_interface_exposes_list_secrets(self):
        from kindling.platform_provider import SecretProvider

        assert hasattr(
            SecretProvider, "list_secrets"
        ), "SecretProvider should define list_secrets()"

    def test_platform_services_expose_secret_exists(self):
        from kindling.platform_databricks import DatabricksService
        from kindling.platform_fabric import FabricService
        from kindling.platform_standalone import StandaloneService
        from kindling.platform_synapse import SynapseService

        for svc_class in (DatabricksService, FabricService, SynapseService, StandaloneService):
            assert hasattr(
                svc_class, "secret_exists"
            ), f"{svc_class.__name__} should expose secret_exists()"

    def test_platform_services_expose_list_secrets(self):
        from kindling.platform_databricks import DatabricksService
        from kindling.platform_fabric import FabricService
        from kindling.platform_standalone import StandaloneService
        from kindling.platform_synapse import SynapseService

        for svc_class in (DatabricksService, FabricService, SynapseService, StandaloneService):
            assert hasattr(
                svc_class, "list_secrets"
            ), f"{svc_class.__name__} should expose list_secrets()"

    def test_secret_exists_returns_true_when_secret_found(self):
        from kindling.platform_provider import SecretProvider

        class FakeProvider(SecretProvider):
            def get_secret(self, secret_name: str, default=None) -> str:
                if secret_name == "existing-secret":
                    return "value"
                raise KeyError(secret_name)

        provider = FakeProvider()
        assert provider.secret_exists("existing-secret") is True

    def test_secret_exists_returns_false_when_secret_missing(self):
        from kindling.platform_provider import SecretProvider

        class FakeProvider(SecretProvider):
            def get_secret(self, secret_name: str, default=None) -> str:
                raise KeyError(secret_name)

        provider = FakeProvider()
        assert provider.secret_exists("missing-secret") is False

    def test_list_secrets_default_returns_empty_list(self):
        from kindling.platform_provider import SecretProvider

        class MinimalProvider(SecretProvider):
            def get_secret(self, secret_name: str, default=None) -> str:
                raise KeyError(secret_name)

        provider = MinimalProvider()
        assert provider.list_secrets() == []

    def test_standalone_secret_exists_with_env_var(self, monkeypatch):
        from unittest.mock import MagicMock

        from kindling.platform_standalone import StandaloneService

        monkeypatch.setenv("KINDLING_SECRET_MY_TOKEN", "secret-value")

        svc = StandaloneService.__new__(StandaloneService)
        svc.logger = MagicMock()
        svc._config_get = MagicMock(return_value=None)

        assert svc.secret_exists("my-token") is True
        assert svc.secret_exists("nonexistent-secret-xyz") is False

    def test_standalone_list_secrets_returns_prefixed_env_vars(self, monkeypatch):
        from unittest.mock import MagicMock

        from kindling.platform_standalone import StandaloneService

        for key in ["KINDLING_SECRET_DB_PASS", "KINDLING_SECRET_API_KEY"]:
            monkeypatch.setenv(key, "value")

        svc = StandaloneService.__new__(StandaloneService)
        svc.logger = MagicMock()

        secrets = svc.list_secrets()
        assert "db-pass" in secrets
        assert "api-key" in secrets

    def test_platform_service_secret_provider_delegates_secret_exists(self):
        from kindling.platform_provider import (
            PlatformServiceProvider,
            PlatformServiceSecretProvider,
            SecretProvider,
        )

        class FakePlatformService:
            def get_secret(self, secret_name, default=None):
                return "value"

            def secret_exists(self, secret_name):
                return secret_name == "known-secret"

        class FakeProvider(PlatformServiceProvider):
            def set_service(self, svc):
                pass

            def get_service(self):
                return FakePlatformService()

        GlobalInjector.bind(PlatformServiceProvider, FakeProvider())

        wrapper = PlatformServiceSecretProvider()
        assert wrapper.secret_exists("known-secret") is True
        assert wrapper.secret_exists("unknown-secret") is False

    def test_platform_service_secret_provider_delegates_list_secrets(self):
        from kindling.platform_provider import (
            PlatformServiceProvider,
            PlatformServiceSecretProvider,
        )

        class FakePlatformService:
            def get_secret(self, secret_name, default=None):
                raise KeyError(secret_name)

            def list_secrets(self):
                return ["token-a", "token-b"]

        class FakeProvider(PlatformServiceProvider):
            def set_service(self, svc):
                pass

            def get_service(self):
                return FakePlatformService()

        GlobalInjector.bind(PlatformServiceProvider, FakeProvider())

        wrapper = PlatformServiceSecretProvider()
        assert wrapper.list_secrets() == ["token-a", "token-b"]


class TestBootstrapSecretResolutionOrdering:
    """Regression coverage for the exact bug: an app.py entity_tags @secret
    reference (e.g. an EventHub connectionString) surviving unresolved past
    bootstrap because resolution ran once, too early, before platform
    services existed -- through the REAL DI wiring
    (PlatformServiceSecretProvider -> SparkPlatformServiceProvider), not a
    hand-bound fake SecretProvider like the tests above use.
    """

    def setup_method(self):
        GlobalInjector.reset()

    def teardown_method(self):
        GlobalInjector.reset()

    def test_early_resolution_before_platform_init_leaves_reference_unresolved(self):
        from kindling.platform_provider import (
            PlatformServiceProvider,
            PlatformServiceSecretProvider,
            SecretProvider,
            SparkPlatformServiceProvider,
        )

        # Mirrors real bootstrap: SparkPlatformServiceProvider.svc is None
        # until initialize_platform_services() calls set_service() during
        # the platform_init phase -- config_init (and its secret-loader pass)
        # runs before that, every time.
        platform_provider = SparkPlatformServiceProvider()
        GlobalInjector.bind(PlatformServiceProvider, platform_provider)
        GlobalInjector.bind(SecretProvider, PlatformServiceSecretProvider())

        with tempfile.TemporaryDirectory() as td:
            settings_path = Path(td) / "settings.yaml"
            settings_path.write_text(
                "entity_tags:\n"
                "  incoming.device_telemetry:\n"
                "    provider.eventhub.connectionString: '@secret:telemetry_eh_conn_string'\n",
                encoding="utf-8",
            )

            settings = Dynaconf(
                settings_files=[str(settings_path)], environments=False, envvar_prefix="KINDLING"
            )

            # Phase 1: config_init runs before platform_init -- svc is still None.
            load_secrets_from_provider(settings, silent=True)

            entity_tags = settings.get("entity_tags")
            assert (
                entity_tags["incoming.device_telemetry"]["provider.eventhub.connectionString"]
                == "@secret:telemetry_eh_conn_string"
            ), "Unresolved before platform init -- expected, not yet a failure"
            assert find_unresolved_secret_references(settings) == [
                "ENTITY_TAGS.incoming.device_telemetry.provider.eventhub.connectionString"
            ]

            # Phase 2: platform_init runs, wiring a real platform service that
            # can resolve secrets (e.g. Databricks dbutils-backed).
            class FakePlatformService:
                def get_secret(self, secret_name, default=None):
                    if secret_name == "telemetry_eh_conn_string":
                        return "Endpoint=sb://real-namespace.servicebus.windows.net/"
                    if default is not None:
                        return default
                    raise KeyError(secret_name)

            platform_provider.set_service(FakePlatformService())

            # Phase 3: bootstrap's post-platform_init secret_resolution pass.
            load_secrets_from_provider(settings, silent=True)

            entity_tags = settings.get("entity_tags")
            assert (
                entity_tags["incoming.device_telemetry"]["provider.eventhub.connectionString"]
                == "Endpoint=sb://real-namespace.servicebus.windows.net/"
            )
            assert find_unresolved_secret_references(settings) == []

    def test_unresolved_secret_reports_path_never_value(self):
        """Requirement: a genuine resolution failure must be reported by
        config path only -- never by attempting to surface the (nonexistent)
        resolved value, which doesn't exist to leak, but the raw @secret:
        reference itself (the secret's *name*, not a value) is fine to
        report since it's not the credential."""
        from kindling.platform_provider import SecretProvider

        class AlwaysFailsProvider(SecretProvider):
            def get_secret(self, secret_name: str, default=None) -> str:
                raise KeyError(secret_name)

        GlobalInjector.bind(SecretProvider, AlwaysFailsProvider())

        with tempfile.TemporaryDirectory() as td:
            settings_path = Path(td) / "settings.yaml"
            settings_path.write_text(
                "entity_tags:\n"
                "  incoming.device_telemetry:\n"
                "    provider.eventhub.connectionString: '@secret:telemetry_eh_conn_string'\n",
                encoding="utf-8",
            )
            settings = Dynaconf(
                settings_files=[str(settings_path)], environments=False, envvar_prefix="KINDLING"
            )

            load_secrets_from_provider(settings, silent=True)

            unresolved = find_unresolved_secret_references(settings)
            assert unresolved == [
                "ENTITY_TAGS.incoming.device_telemetry.provider.eventhub.connectionString"
            ]
            # The path list must never contain anything that looks like a
            # resolved credential value -- only dotted config paths.
            assert all(not p.startswith("Endpoint=") for p in unresolved)


class TestSecretLoaderRegistration:
    def setup_method(self):
        GlobalInjector.reset()

    def teardown_method(self):
        GlobalInjector.reset()

    def test_config_setup_registers_secret_loader(self):
        os.environ.pop("LOADERS_FOR_DYNACONF", None)

        import kindling.spark_config as spark_config_module
        from kindling.spark_config import DynaconfConfig

        # Re-apply the decorator directly to re-register DynaconfConfig
        # against the fresh injector (setup_method calls GlobalInjector.reset()
        # above, which drops the binding singleton_autobind made at
        # kindling.spark_config's original import time). Avoid
        # importlib.reload(spark_config_module) here: reloading replaces
        # ConfigService/DynaconfConfig with brand-new class objects for the
        # rest of the process, silently breaking identity checks in any test
        # that runs afterward in the same worker/process.
        GlobalInjector.singleton_autobind()(DynaconfConfig)
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
