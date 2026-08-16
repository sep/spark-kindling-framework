"""Unit tests for `kindling config show` and `kindling config diff`.

These commands must never start Dynaconf or Spark -- they replay the
base -> platform -> env settings.yaml merge directly, so tests write plain
YAML fixtures and invoke the CLI with CliRunner, no mocking of the kindling
runtime package required.
"""

import json
from pathlib import Path

from click.testing import CliRunner
from kindling_cli.cli import (
    _MISSING_CONFIG_KEY,
    _diff_flat_config,
    _flatten_config_dict,
    _get_nested_key,
    _is_secret_reference,
    _load_effective_raw_config,
    _redact_config_tree,
    _secret_display_name,
    cli,
)


def _write_app(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "def initialize(env=None, config_dir=None):\n    return None\n", encoding="utf-8"
    )
    return path


# ---------------------------------------------------------------------------
# Helper unit tests
# ---------------------------------------------------------------------------


class TestGetNestedKey:
    def test_returns_value_for_present_key(self):
        assert _get_nested_key({"a": {"b": 1}}, "a.b") == 1

    def test_returns_sentinel_for_missing_key(self):
        assert _get_nested_key({"a": {"b": 1}}, "a.c") is _MISSING_CONFIG_KEY

    def test_returns_sentinel_when_traversal_hits_scalar(self):
        assert _get_nested_key({"a": 1}, "a.b") is _MISSING_CONFIG_KEY

    def test_none_value_is_not_missing(self):
        assert _get_nested_key({"a": None}, "a") is None


class TestSecretReference:
    def test_detects_colon_form(self):
        assert _is_secret_reference("@secret:scope:key")

    def test_detects_space_form(self):
        assert _is_secret_reference("@secret my-secret")

    def test_non_secret_string_is_false(self):
        assert not _is_secret_reference("plain-value")

    def test_non_string_is_false(self):
        assert not _is_secret_reference(123)

    def test_display_name_strips_colon_prefix(self):
        assert _secret_display_name("@secret:kv/orders-table") == "kv/orders-table"

    def test_display_name_strips_space_prefix(self):
        assert _secret_display_name("@secret my-secret") == "my-secret"


class TestRedactConfigTree:
    def test_redacts_nested_secret_by_default(self):
        tree = {"a": {"b": "@secret:scope:key"}}
        redacted = _redact_config_tree(tree, reveal=False)
        assert redacted == {"a": {"b": "<secret: scope:key>"}}

    def test_reveal_secrets_prints_plaintext(self):
        tree = {"a": "@secret:scope:key"}
        assert _redact_config_tree(tree, reveal=True) == {"a": "@secret:scope:key"}

    def test_redacts_within_lists(self):
        tree = {"a": ["@secret:scope:key", "plain"]}
        redacted = _redact_config_tree(tree, reveal=False)
        assert redacted == {"a": ["<secret: scope:key>", "plain"]}

    def test_non_secret_values_pass_through(self):
        tree = {"a": 1, "b": True, "c": None}
        assert _redact_config_tree(tree, reveal=False) == tree


class TestFlattenAndDiff:
    def test_flatten_nested_dict(self):
        flat = _flatten_config_dict({"a": {"b": {"c": 1}}, "d": 2})
        assert flat == {"a.b.c": 1, "d": 2}

    def test_diff_reports_only_changed_keys(self):
        a = {"kindling": {"level": "standard"}, "same": 1}
        b = {"kindling": {"level": "verbose"}, "same": 1}
        diffs = _diff_flat_config(a, b)
        assert diffs == [("kindling.level", "standard", "verbose")]

    def test_diff_reports_keys_only_in_one_side(self):
        diffs = _diff_flat_config({"a": 1}, {"b": 2})
        assert ("a", 1, _MISSING_CONFIG_KEY) in diffs
        assert ("b", _MISSING_CONFIG_KEY, 2) in diffs


class TestLoadEffectiveRawConfig:
    def test_merges_base_platform_env_in_order(self, tmp_path):
        (tmp_path / "settings.yaml").write_text(
            "kindling:\n  level: base\n  only_base: 1\n", encoding="utf-8"
        )
        (tmp_path / "settings.databricks.yaml").write_text(
            "kindling:\n  level: platform\n  only_platform: 1\n", encoding="utf-8"
        )
        (tmp_path / "settings.dev.yaml").write_text("kindling:\n  level: env\n", encoding="utf-8")

        merged, used = _load_effective_raw_config(tmp_path, "dev", "databricks")

        assert merged["kindling"]["level"] == "env"  # env wins over platform
        assert merged["kindling"]["only_base"] == 1
        assert merged["kindling"]["only_platform"] == 1
        assert len(used) == 3

    def test_missing_files_are_skipped_silently(self, tmp_path):
        (tmp_path / "settings.yaml").write_text("name: solo\n", encoding="utf-8")

        merged, used = _load_effective_raw_config(tmp_path, "dev", "databricks")

        assert merged == {"name": "solo"}
        assert used == [tmp_path / "settings.yaml"]

    def test_no_settings_file_returns_empty(self, tmp_path):
        merged, used = _load_effective_raw_config(tmp_path, "dev", None)
        assert merged == {}
        assert used == []


# ---------------------------------------------------------------------------
# kindling config show
# ---------------------------------------------------------------------------


class TestConfigShow:
    def test_prints_merged_config_with_env_overlay(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            app_path = _write_app(Path("app.py"))
            Path("settings.yaml").write_text(
                "name: demo\nkindling:\n  telemetry:\n    tracing:\n      level: standard\n",
                encoding="utf-8",
            )
            Path("settings.dev.yaml").write_text(
                "kindling:\n  telemetry:\n    tracing:\n      level: verbose\n",
                encoding="utf-8",
            )

            result = runner.invoke(cli, ["config", "show", "--app", str(app_path), "--env", "dev"])

        assert result.exit_code == 0, result.output
        assert "env: dev" in result.output
        assert "level: verbose" in result.output
        assert "settings.dev.yaml" in result.output

    def test_key_flag_prints_single_value(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            app_path = _write_app(Path("app.py"))
            Path("settings.yaml").write_text(
                "kindling:\n  telemetry:\n    tracing:\n      level: standard\n",
                encoding="utf-8",
            )

            result = runner.invoke(
                cli,
                [
                    "config",
                    "show",
                    "--app",
                    str(app_path),
                    "--key",
                    "kindling.telemetry.tracing.level",
                ],
            )

        assert result.exit_code == 0, result.output
        assert "kindling.telemetry.tracing.level = 'standard'" in result.output

    def test_key_flag_missing_key_fails(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            app_path = _write_app(Path("app.py"))
            Path("settings.yaml").write_text("name: demo\n", encoding="utf-8")

            result = runner.invoke(
                cli, ["config", "show", "--app", str(app_path), "--key", "does.not.exist"]
            )

        assert result.exit_code != 0
        assert "not found in effective config" in result.output

    def test_no_settings_file_fails_with_hint(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            app_path = _write_app(Path("app.py"))

            result = runner.invoke(cli, ["config", "show", "--app", str(app_path)])

        assert result.exit_code != 0
        assert "config init" in result.output

    def test_secrets_redacted_by_default(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            app_path = _write_app(Path("app.py"))
            Path("settings.yaml").write_text(
                "kindling:\n  secrets:\n    key_vault_url: '@secret:kv/orders'\n",
                encoding="utf-8",
            )

            result = runner.invoke(cli, ["config", "show", "--app", str(app_path)])

        assert result.exit_code == 0, result.output
        assert "<secret: kv/orders>" in result.output
        assert "@secret:kv/orders" not in result.output

    def test_reveal_secrets_prints_plaintext_with_warning(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            app_path = _write_app(Path("app.py"))
            Path("settings.yaml").write_text(
                "kindling:\n  secrets:\n    key_vault_url: '@secret:kv/orders'\n",
                encoding="utf-8",
            )

            result = runner.invoke(
                cli, ["config", "show", "--app", str(app_path), "--reveal-secrets"]
            )

        assert result.exit_code == 0, result.output
        assert "WARNING" in result.output
        assert "@secret:kv/orders" in result.output

    def test_json_output_is_valid_and_redacts_secrets(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            app_path = _write_app(Path("app.py"))
            Path("settings.yaml").write_text(
                "name: demo\nkindling:\n  secrets:\n    key_vault_url: '@secret:kv/orders'\n",
                encoding="utf-8",
            )

            result = runner.invoke(cli, ["config", "show", "--app", str(app_path), "--json"])

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["config"]["name"] == "demo"
        assert payload["config"]["kindling"]["secrets"]["key_vault_url"] == "<secret: kv/orders>"
        assert payload["env"] == "local"

    def test_platform_overlay_layers_in(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            app_path = _write_app(Path("app.py"))
            Path("settings.yaml").write_text("kindling:\n  target: base\n", encoding="utf-8")
            Path("settings.databricks.yaml").write_text(
                "kindling:\n  target: databricks\n", encoding="utf-8"
            )

            result = runner.invoke(
                cli,
                ["config", "show", "--app", str(app_path), "--platform", "databricks", "--json"],
            )

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["config"]["kindling"]["target"] == "databricks"

    def test_env_falls_back_to_kindling_env_variable(self, monkeypatch):
        monkeypatch.setenv("KINDLING_ENV", "staging")
        runner = CliRunner()
        with runner.isolated_filesystem():
            app_path = _write_app(Path("app.py"))
            Path("settings.yaml").write_text("kindling:\n  level: base\n", encoding="utf-8")
            Path("settings.staging.yaml").write_text(
                "kindling:\n  level: from-staging\n", encoding="utf-8"
            )

            result = runner.invoke(cli, ["config", "show", "--app", str(app_path), "--json"])

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["env"] == "staging"
        assert payload["config"]["kindling"]["level"] == "from-staging"


# ---------------------------------------------------------------------------
# kindling config diff
# ---------------------------------------------------------------------------


class TestConfigDiff:
    def test_requires_diff_env_or_diff_platform(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            app_path = _write_app(Path("app.py"))
            Path("settings.yaml").write_text("name: demo\n", encoding="utf-8")

            result = runner.invoke(cli, ["config", "diff", "--app", str(app_path)])

        assert result.exit_code != 0
        assert "Provide --diff-env" in result.output

    def test_reports_only_differing_keys(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            app_path = _write_app(Path("app.py"))
            Path("settings.yaml").write_text(
                "kindling:\n  level: standard\n  unchanged: yes\n", encoding="utf-8"
            )
            Path("settings.prod.yaml").write_text("kindling:\n  level: verbose\n", encoding="utf-8")

            result = runner.invoke(
                cli,
                [
                    "config",
                    "diff",
                    "--app",
                    str(app_path),
                    "--env",
                    "local",
                    "--diff-env",
                    "prod",
                ],
            )

        assert result.exit_code == 0, result.output
        assert "kindling.level" in result.output
        assert "standard" in result.output
        assert "verbose" in result.output
        assert "unchanged" not in result.output

    def test_no_differences_message(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            app_path = _write_app(Path("app.py"))
            Path("settings.yaml").write_text("name: demo\n", encoding="utf-8")

            result = runner.invoke(
                cli,
                [
                    "config",
                    "diff",
                    "--app",
                    str(app_path),
                    "--env",
                    "local",
                    "--diff-env",
                    "local",
                ],
            )

        assert result.exit_code == 0, result.output
        assert "No differences." in result.output

    def test_json_output_redacts_secrets_by_default(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            app_path = _write_app(Path("app.py"))
            Path("settings.yaml").write_text(
                "kindling:\n  secrets:\n    key: '@secret:scope:one'\n", encoding="utf-8"
            )
            Path("settings.prod.yaml").write_text(
                "kindling:\n  secrets:\n    key: '@secret:scope:two'\n", encoding="utf-8"
            )

            result = runner.invoke(
                cli,
                [
                    "config",
                    "diff",
                    "--app",
                    str(app_path),
                    "--env",
                    "local",
                    "--diff-env",
                    "prod",
                    "--json",
                ],
            )

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        diffs = {d["key"]: d for d in payload["diffs"]}
        assert diffs["kindling.secrets.key"]["a"] == "<secret: scope:one>"
        assert diffs["kindling.secrets.key"]["b"] == "<secret: scope:two>"

    def test_diff_platform_defaults_to_platform_when_omitted(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            app_path = _write_app(Path("app.py"))
            Path("settings.yaml").write_text("kindling:\n  level: base\n", encoding="utf-8")
            Path("settings.databricks.yaml").write_text(
                "kindling:\n  level: databricks\n", encoding="utf-8"
            )
            Path("settings.dev.yaml").write_text("kindling:\n  level: dev\n", encoding="utf-8")
            Path("settings.prod.yaml").write_text("kindling:\n  level: prod\n", encoding="utf-8")

            result = runner.invoke(
                cli,
                [
                    "config",
                    "diff",
                    "--app",
                    str(app_path),
                    "--platform",
                    "databricks",
                    "--env",
                    "dev",
                    "--diff-env",
                    "prod",
                    "--json",
                ],
            )

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        # both sides should have used the databricks platform overlay
        assert payload["side_a"]["platform"] == "databricks"
        assert payload["side_b"]["platform"] == "databricks"
