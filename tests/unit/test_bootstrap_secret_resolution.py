"""
Unit tests for bootstrap._resolve_and_validate_secrets.

Regression coverage for the bootstrap-ordering bug: @secret references are
resolved once early (inside ConfigService.initialize(), before platform
services exist -- always a no-op there) and once again after platform_init,
via this helper. Before this fix, the second pass logged a warning and let
bootstrap complete anyway when resolution still failed, silently handing a
literal "@secret:..." string to whatever read that config next.
"""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from dynaconf import Dynaconf

from kindling.bootstrap import _resolve_and_validate_secrets
from kindling.injection import GlobalInjector


def _dynaconf_with_entity_tags_secret(tmp_path: Path) -> Dynaconf:
    settings_path = tmp_path / "settings.yaml"
    settings_path.write_text(
        "entity_tags:\n"
        "  incoming.device_telemetry:\n"
        "    provider.eventhub.connectionString: '@secret:telemetry_eh_conn_string'\n",
        encoding="utf-8",
    )
    return Dynaconf(
        settings_files=[str(settings_path)], environments=False, envvar_prefix="KINDLING"
    )


class TestResolveAndValidateSecrets:
    def setup_method(self):
        GlobalInjector.reset()

    def teardown_method(self):
        GlobalInjector.reset()

    def test_succeeds_when_platform_secret_provider_resolves_everything(self, tmp_path):
        from kindling.platform_provider import PlatformServiceProvider, SecretProvider

        class WorkingSecretProvider(SecretProvider):
            def get_secret(self, secret_name, default=None):
                if secret_name == "telemetry_eh_conn_string":
                    return "Endpoint=sb://real/;SharedAccessKey=abc"
                raise KeyError(secret_name)

        GlobalInjector.bind(SecretProvider, WorkingSecretProvider())

        config_service = MagicMock()
        config_service.dynaconf = _dynaconf_with_entity_tags_secret(tmp_path)
        logger = MagicMock()

        _resolve_and_validate_secrets(config_service, logger)

        entity_tags = config_service.dynaconf.get("entity_tags")
        assert (
            entity_tags["incoming.device_telemetry"]["provider.eventhub.connectionString"]
            == "Endpoint=sb://real/;SharedAccessKey=abc"
        )
        logger.debug.assert_called_once()

    def test_raises_actionable_error_when_still_unresolved(self, tmp_path):
        """This is the exact bug: no working SecretProvider yet (or a
        genuinely broken one) -- bootstrap must fail loudly here, not
        complete and defer to a confusing downstream KeyError."""
        from kindling.platform_provider import SecretProvider

        class AlwaysFailsProvider(SecretProvider):
            def get_secret(self, secret_name, default=None):
                raise KeyError(secret_name)

        GlobalInjector.bind(SecretProvider, AlwaysFailsProvider())

        config_service = MagicMock()
        config_service.dynaconf = _dynaconf_with_entity_tags_secret(tmp_path)
        logger = MagicMock()

        with pytest.raises(RuntimeError) as exc_info:
            _resolve_and_validate_secrets(config_service, logger)

        message = str(exc_info.value)
        assert "ENTITY_TAGS.incoming.device_telemetry.provider.eventhub.connectionString" in message
        # The failure must name the config path, never a secret value --
        # there is no resolved value in this scenario to leak, but guard
        # against ever interpolating one into the message.
        assert "Endpoint=" not in message
        logger.debug.assert_not_called()

    def test_noop_when_dynaconf_not_yet_initialized(self):
        """config_service.dynaconf is None before ConfigService.initialize()
        has run -- must not raise, just skip (bootstrap calls this only
        after config_init, but stay defensive)."""
        config_service = MagicMock()
        config_service.dynaconf = None
        logger = MagicMock()

        _resolve_and_validate_secrets(config_service, logger)

        logger.debug.assert_not_called()
