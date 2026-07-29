"""Design-time CLI tracing tests (gh#210 Phase 4)."""

from contextlib import nullcontext
from unittest.mock import patch

import kindling_cli._tracing as cli_tracing
import pytest
from kindling_cli._tracing import cli_span, cli_tracing_enabled, traced_command


@pytest.fixture(autouse=True)
def _reset_provider(monkeypatch):
    monkeypatch.delenv("KINDLING_TRACE", raising=False)
    monkeypatch.delenv("KINDLING_KINDLING__TELEMETRY__TRACING__PRINT", raising=False)
    cli_tracing._provider = None
    yield
    cli_tracing._provider = None


class TestCliSpanGating:
    def test_disabled_without_env(self):
        assert cli_tracing_enabled() is False
        assert isinstance(cli_span("app.deploy"), nullcontext)

    def test_enabled_by_env_flag(self, monkeypatch):
        monkeypatch.setenv("KINDLING_TRACE", "1")
        assert cli_tracing_enabled() is True

    def test_span_emits_through_plain_provider(self, monkeypatch):
        monkeypatch.setenv("KINDLING_TRACE", "true")

        with cli_span("app.deploy", {"app_name": "demo"}):
            provider = cli_tracing._provider
            assert provider is not None
            assert provider.current_span.component == "kindling.cli"
            assert provider.current_span.operation == "app.deploy"
            assert provider.current_span.attributes["app_name"] == "demo"

        assert provider.current_span is None

    def test_print_env_is_honored(self, monkeypatch):
        monkeypatch.setenv("KINDLING_TRACE", "1")
        monkeypatch.setenv("KINDLING_KINDLING__TELEMETRY__TRACING__PRINT", "true")

        with patch("builtins.print") as mock_print:
            with cli_span("package.deploy"):
                pass

        printed = [c[0][0] for c in mock_print.call_args_list]
        assert any("package.deploy_START" in line for line in printed)
        assert any("package.deploy_END" in line for line in printed)

    def test_provider_failure_degrades_to_noop(self, monkeypatch):
        monkeypatch.setenv("KINDLING_TRACE", "1")

        with patch.object(cli_tracing, "_get_provider", side_effect=ImportError("no core")):
            cm = cli_span("app.deploy")

        assert isinstance(cm, nullcontext)


class TestTracedCommand:
    def test_wraps_callback_and_whitelists_kwargs(self, monkeypatch):
        monkeypatch.setenv("KINDLING_TRACE", "1")
        seen = {}

        @traced_command("app.run")
        def command(app_name, secret_token, wait):
            provider = cli_tracing._provider
            seen["operation"] = provider.current_span.operation
            seen["attrs"] = dict(provider.current_span.attributes)
            return "ran"

        result = command(app_name="demo", secret_token="hunter2", wait=True)

        assert result == "ran"
        assert seen["operation"] == "app.run"
        assert seen["attrs"] == {"app_name": "demo"}, "Only whitelisted kwargs become attrs"

    def test_untraced_invocation_passes_through(self):
        @traced_command("app.run")
        def command(app_name):
            return f"ran {app_name}"

        assert command(app_name="demo") == "ran demo"
        assert cli_tracing._provider is None, "Provider must stay unbuilt when disabled"

    def test_errors_propagate_when_traced(self, monkeypatch):
        monkeypatch.setenv("KINDLING_TRACE", "1")

        @traced_command("app.deploy")
        def command():
            raise ValueError("deploy failed")

        with pytest.raises(ValueError, match="deploy failed"):
            command()


class TestCliCommandsAreDecorated:
    def test_deploy_and_run_commands_carry_span_wrappers(self):
        from kindling_cli import cli as cli_module

        for group, name in [
            ("workspace", "init"),
            ("workspace", "deploy"),
            ("app", "deploy"),
            ("app", "run"),
            ("package", "deploy"),
            ("runtime", "deploy"),
        ]:
            command = cli_module.cli.commands[group].commands[name]
            assert command.callback.__wrapped__ is not None, f"{group} {name} must be traced"
