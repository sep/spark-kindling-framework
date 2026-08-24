"""Unit tests for --trace/--trace-level on `kindling app run` and
`kindling pipeline run` (Phase 4 of the CLI devex-gaps proposal).

Both flags are documented sugar for existing mechanisms:
  --trace                      == --param print_trace=true
                                   (app run) / KINDLING_KINDLING__TELEMETRY__
                                   TRACING__PRINT=true (pipeline run)
  --trace-level <level>        == --param kindling.telemetry.tracing.level=<level>
                                   (app run) / KINDLING_KINDLING__TELEMETRY__
                                   TRACING__LEVEL=<level> (pipeline run)
"""

import json
import subprocess
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

from click.testing import CliRunner
from kindling.data_pipes import DataPipesExecution, DataPipesRegistry
from kindling_cli.cli import (
    _apply_trace_env_vars,
    _prepend_trace_param_overrides,
    cli,
)


def _write_app(path: Path, body: str = None) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        body or "def initialize(env=None, config_dir=None):\n    return None\n",
        encoding="utf-8",
    )
    return path


# ---------------------------------------------------------------------------
# Pure helper unit tests
# ---------------------------------------------------------------------------


class TestPrependTraceParamOverrides:
    def test_no_flags_returns_original_overrides_unchanged(self):
        assert _prepend_trace_param_overrides(False, None, ("a=b",)) == ("a=b",)

    def test_trace_flag_prepends_print_trace_param(self):
        assert _prepend_trace_param_overrides(True, None, ()) == ("print_trace=true",)

    def test_trace_level_prepends_dotted_param(self):
        result = _prepend_trace_param_overrides(False, "verbose", ())
        assert result == ("kindling.telemetry.tracing.level=verbose",)

    def test_both_flags_prepend_both_params_in_order(self):
        result = _prepend_trace_param_overrides(True, "minimal", ())
        assert result == (
            "print_trace=true",
            "kindling.telemetry.tracing.level=minimal",
        )

    def test_explicit_param_appears_after_synthetic_ones_so_it_wins(self):
        result = _prepend_trace_param_overrides(True, None, ("print_trace=false",))
        assert result == ("print_trace=true", "print_trace=false")
        # _resolve_runtime_parameters applies overrides in order, last wins --
        # the explicit override is last, so it wins.
        assert result[-1] == "print_trace=false"


class TestApplyTraceEnvVars:
    def test_no_flags_leaves_env_untouched(self, monkeypatch):
        monkeypatch.delenv("KINDLING_KINDLING__TELEMETRY__TRACING__PRINT", raising=False)
        monkeypatch.delenv("KINDLING_KINDLING__TELEMETRY__TRACING__LEVEL", raising=False)

        _apply_trace_env_vars(False, None)

        import os

        assert "KINDLING_KINDLING__TELEMETRY__TRACING__PRINT" not in os.environ
        assert "KINDLING_KINDLING__TELEMETRY__TRACING__LEVEL" not in os.environ

    def test_trace_sets_print_env_var(self, monkeypatch):
        monkeypatch.delenv("KINDLING_KINDLING__TELEMETRY__TRACING__PRINT", raising=False)

        _apply_trace_env_vars(True, None)

        import os

        assert os.environ["KINDLING_KINDLING__TELEMETRY__TRACING__PRINT"] == "true"

    def test_trace_level_sets_level_env_var(self, monkeypatch):
        monkeypatch.delenv("KINDLING_KINDLING__TELEMETRY__TRACING__LEVEL", raising=False)

        _apply_trace_env_vars(False, "verbose")

        import os

        assert os.environ["KINDLING_KINDLING__TELEMETRY__TRACING__LEVEL"] == "verbose"


# ---------------------------------------------------------------------------
# kindling pipeline run --trace/--trace-level
# ---------------------------------------------------------------------------


class TestPipelineRunTraceFlags:
    def test_trace_flag_sets_env_var_equivalent_to_manual_export(self, monkeypatch):
        pipe_registry = Mock()
        pipe_registry.get_pipe_definition.return_value = SimpleNamespace(pipeid="bronze_to_silver")
        executor = Mock()

        def fake_get(service_type):
            if service_type is DataPipesRegistry:
                return pipe_registry
            if service_type is DataPipesExecution:
                return executor
            raise AssertionError(f"unexpected service: {service_type!r}")

        monkeypatch.setattr("kindling.injection.GlobalInjector.get", fake_get)
        monkeypatch.delenv("KINDLING_KINDLING__TELEMETRY__TRACING__PRINT", raising=False)
        monkeypatch.delenv("KINDLING_KINDLING__TELEMETRY__TRACING__LEVEL", raising=False)

        seen_env = {}

        runner = CliRunner()
        with runner.isolated_filesystem():
            app_path = _write_app(
                Path("app.py"),
                "import os\n"
                "def initialize(env=None, config_dir=None):\n"
                "    seen = {\n"
                "        'print': os.environ.get('KINDLING_KINDLING__TELEMETRY__TRACING__PRINT'),\n"
                "        'level': os.environ.get('KINDLING_KINDLING__TELEMETRY__TRACING__LEVEL'),\n"
                "    }\n"
                "    import json\n"
                "    from pathlib import Path\n"
                "    Path('seen_env.json').write_text(json.dumps(seen))\n",
            )

            result = runner.invoke(
                cli,
                [
                    "pipeline",
                    "run",
                    "bronze_to_silver",
                    "--app",
                    str(app_path),
                    "--trace",
                    "--trace-level",
                    "verbose",
                ],
            )

            seen_env = json.loads(Path("seen_env.json").read_text())

        assert result.exit_code == 0, result.output
        assert seen_env == {"print": "true", "level": "verbose"}

    def test_help_documents_trace_flags(self):
        result = CliRunner().invoke(cli, ["pipeline", "run", "--help"])
        assert result.exit_code == 0
        assert "--trace" in result.output
        assert "--trace-level" in result.output


# ---------------------------------------------------------------------------
# kindling app run --trace/--trace-level
# ---------------------------------------------------------------------------


class TestAppRunTraceFlags:
    def test_trace_flag_is_equivalent_to_param_print_trace_true(self, tmp_path, monkeypatch):
        app_dir = tmp_path / "myapp"
        app_dir.mkdir()
        (app_dir / "app.py").write_text("# stub\n", encoding="utf-8")

        captured_env = {}

        def fake_run(cmd, env=None, **kwargs):
            captured_env.update(env or {})
            return subprocess.CompletedProcess(cmd, returncode=0)

        monkeypatch.setattr(subprocess, "run", fake_run)

        result_via_flag = CliRunner().invoke(
            cli, ["app", "run", "myapp", "--local-folder", str(app_dir), "--trace"]
        )
        params_via_flag = json.loads(captured_env["KINDLING_RUN_PARAMETERS"])

        captured_env.clear()
        result_via_param = CliRunner().invoke(
            cli,
            [
                "app",
                "run",
                "myapp",
                "--local-folder",
                str(app_dir),
                "--param",
                "print_trace=true",
            ],
        )
        params_via_param = json.loads(captured_env["KINDLING_RUN_PARAMETERS"])

        assert result_via_flag.exit_code == 0, result_via_flag.output
        assert result_via_param.exit_code == 0, result_via_param.output
        assert params_via_flag == params_via_param == {"print_trace": True}

    def test_trace_level_flag_is_equivalent_to_dotted_param(self, tmp_path, monkeypatch):
        app_dir = tmp_path / "myapp"
        app_dir.mkdir()
        (app_dir / "app.py").write_text("# stub\n", encoding="utf-8")

        captured_env = {}

        def fake_run(cmd, env=None, **kwargs):
            captured_env.update(env or {})
            return subprocess.CompletedProcess(cmd, returncode=0)

        monkeypatch.setattr(subprocess, "run", fake_run)

        result = CliRunner().invoke(
            cli,
            [
                "app",
                "run",
                "myapp",
                "--local-folder",
                str(app_dir),
                "--trace-level",
                "minimal",
            ],
        )
        params = json.loads(captured_env["KINDLING_RUN_PARAMETERS"])

        assert result.exit_code == 0, result.output
        assert params == {"kindling": {"telemetry": {"tracing": {"level": "minimal"}}}}

    def test_explicit_param_overrides_trace_flag(self, tmp_path, monkeypatch):
        app_dir = tmp_path / "myapp"
        app_dir.mkdir()
        (app_dir / "app.py").write_text("# stub\n", encoding="utf-8")

        captured_env = {}

        def fake_run(cmd, env=None, **kwargs):
            captured_env.update(env or {})
            return subprocess.CompletedProcess(cmd, returncode=0)

        monkeypatch.setattr(subprocess, "run", fake_run)

        result = CliRunner().invoke(
            cli,
            [
                "app",
                "run",
                "myapp",
                "--local-folder",
                str(app_dir),
                "--trace",
                "--param",
                "print_trace=false",
            ],
        )
        params = json.loads(captured_env["KINDLING_RUN_PARAMETERS"])

        assert result.exit_code == 0, result.output
        assert params == {"print_trace": False}

    def test_help_documents_trace_flags(self):
        result = CliRunner().invoke(cli, ["app", "run", "--help"])
        assert result.exit_code == 0
        assert "--trace" in result.output
        assert "--trace-level" in result.output
