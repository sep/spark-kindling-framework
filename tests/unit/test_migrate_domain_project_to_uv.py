"""Regression tests for scripts/migrate_domain_project_to_uv.py.

The script is a standalone, throwaway migration tool (not part of
kindling_cli's shipped surface, not on sys.path by default) -- loaded here
by file path rather than as an installed package.
"""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

_SCRIPT_PATH = Path(__file__).parent.parent.parent / "scripts" / "migrate_domain_project_to_uv.py"
_spec = importlib.util.spec_from_file_location("migrate_domain_project_to_uv", _SCRIPT_PATH)
migrate = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = migrate
_spec.loader.exec_module(migrate)


class TestConvertCaretSpecifier:
    @pytest.mark.parametrize(
        "specifier, expected",
        [
            ("^1.2.3", ">=1.2.3,<2.0.0"),
            ("^0.2.3", ">=0.2.3,<0.3.0"),
            ("^0.0.3", ">=0.0.3,<0.0.4"),
            ("^0.0", ">=0.0,<0.1.0"),
            ("^0", ">=0,<1.0.0"),
            ("^3.10", ">=3.10,<4.0.0"),
        ],
    )
    def test_caret_conversion_matches_poetry_semantics(self, specifier, expected):
        assert migrate._convert_caret_specifier(specifier) == expected


class TestNoSyncFlag:
    def test_no_sync_skips_uv_sync_entirely(self, tmp_path, monkeypatch, capsys):
        project = tmp_path / "proj"
        project.mkdir()
        (project / "pyproject.toml").write_text(
            '[tool.poetry]\nname = "proj"\nversion = "0.1.0"\n'
            '[tool.poetry.dependencies]\npython = "^3.10"\n'
        )
        monkeypatch.setattr(sys, "argv", ["migrate", "--project", str(project), "--no-sync"])

        with patch.object(migrate.subprocess, "run") as mock_run:
            migrate.main()

        mock_run.assert_not_called()
        assert "Skipping `uv sync`" in capsys.readouterr().out
