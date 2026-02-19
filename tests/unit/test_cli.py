from pathlib import Path

from click.testing import CliRunner

from kindling_cli.cli import cli


def test_config_init_writes_settings_file():
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(cli, ["config", "init", "--name", "demo-app"])

        assert result.exit_code == 0
        settings_path = Path("settings.yaml")
        assert settings_path.exists()
        content = settings_path.read_text(encoding="utf-8")
        assert "name: demo-app" in content
        assert "kindling:" in content


def test_config_init_refuses_to_overwrite_without_force():
    runner = CliRunner()
    with runner.isolated_filesystem():
        settings_path = Path("settings.yaml")
        settings_path.write_text("original", encoding="utf-8")

        result = runner.invoke(cli, ["config", "init"])

        assert result.exit_code != 0
        assert "Refusing to overwrite existing file" in result.output
        assert settings_path.read_text(encoding="utf-8") == "original"


def test_config_init_overwrites_with_force():
    runner = CliRunner()
    with runner.isolated_filesystem():
        settings_path = Path("settings.yaml")
        settings_path.write_text("original", encoding="utf-8")

        result = runner.invoke(cli, ["config", "init", "--force", "--name", "forced-app"])

        assert result.exit_code == 0
        content = settings_path.read_text(encoding="utf-8")
        assert "name: forced-app" in content
        assert "kindling:" in content


def test_env_check_passes_for_generated_settings_file():
    runner = CliRunner()
    with runner.isolated_filesystem():
        init_result = runner.invoke(cli, ["config", "init"])
        assert init_result.exit_code == 0

        result = runner.invoke(cli, ["env", "check"])

        assert result.exit_code == 0
        assert "Environment check passed." in result.output
