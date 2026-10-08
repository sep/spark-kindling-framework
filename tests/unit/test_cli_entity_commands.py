# [implementer] unit tests for entity show/validate and app inspect --entities — ki-9bw
"""Unit tests for kindling entity show, entity validate, and app inspect --entities."""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from click.testing import CliRunner
from kindling.data_entities import DataEntityRegistry
from kindling_cli.cli import (
    _fixture_csv_path,
    _format_table,
    _read_csv_rows,
    _resolve_entity_info,
    cli,
)


@pytest.fixture(autouse=True)
def _mock_bootstrap_app(monkeypatch):
    """The CLI now always calls a real `initialize_framework()` via
    `_bootstrap_app` before loading app.py; these tests mock
    `GlobalInjector.get` narrowly and don't want a real bootstrap call."""
    monkeypatch.setattr("kindling_cli.cli._bootstrap_app", lambda *a, **kw: None)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_app(path: Path, body: str = None) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        body or "def initialize(env=None, config_dir=None):\n    return None\n",
        encoding="utf-8",
    )
    return path


def _make_entity(entity_id: str, tags: dict = None, merge_columns=None, schema=None):
    return SimpleNamespace(
        entityid=entity_id,
        tags=tags or {},
        merge_columns=merge_columns or [],
        schema=schema,
    )


def _write_fixture_csv(base: Path, entity_id: str, content: str) -> Path:
    """Write a tests/entities/ CSV fixture for the given entity_id."""
    parts = [p for p in entity_id.split(".") if p]
    if len(parts) > 1:
        rel = Path(*parts[:-1]) / f"{parts[-1]}.csv"
    else:
        rel = Path(f"{parts[0]}.csv")
    fixture_path = base / "tests" / "entities" / rel
    fixture_path.parent.mkdir(parents=True, exist_ok=True)
    fixture_path.write_text(content, encoding="utf-8")
    return fixture_path


# ---------------------------------------------------------------------------
# _fixture_csv_path
# ---------------------------------------------------------------------------


class TestFixtureCsvPath:
    """Test _fixture_csv_path helper."""

    def test_returns_none_when_no_fixture_dir(self, tmp_path):
        result = _fixture_csv_path("bronze.orders", tmp_path)
        assert result is None

    def test_returns_path_for_nested_entity(self, tmp_path):
        _write_fixture_csv(tmp_path, "bronze.orders", "id,name\n1,foo\n")
        result = _fixture_csv_path("bronze.orders", tmp_path)
        assert result is not None
        assert result.is_file()

    def test_returns_none_when_file_absent(self, tmp_path):
        (tmp_path / "tests" / "entities" / "bronze").mkdir(parents=True)
        result = _fixture_csv_path("bronze.orders", tmp_path)
        assert result is None

    def test_returns_path_for_simple_entity(self, tmp_path):
        _write_fixture_csv(tmp_path, "orders", "id\n1\n")
        result = _fixture_csv_path("orders", tmp_path)
        assert result is not None
        assert result.name == "orders.csv"


# ---------------------------------------------------------------------------
# _read_csv_rows
# ---------------------------------------------------------------------------


class TestReadCsvRows:
    """Test _read_csv_rows returns headers and rows correctly."""

    def test_reads_headers_and_rows(self, tmp_path):
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("id,name\n1,Alice\n2,Bob\n")
        headers, rows = _read_csv_rows(csv_file)
        assert headers == ["id", "name"]
        assert rows == [["1", "Alice"], ["2", "Bob"]]

    def test_empty_file_returns_empty(self, tmp_path):
        csv_file = tmp_path / "empty.csv"
        csv_file.write_text("")
        headers, rows = _read_csv_rows(csv_file)
        assert headers == []
        assert rows == []

    def test_headers_only_returns_empty_rows(self, tmp_path):
        csv_file = tmp_path / "headers_only.csv"
        csv_file.write_text("id,name\n")
        headers, rows = _read_csv_rows(csv_file)
        assert headers == ["id", "name"]
        assert rows == []


# ---------------------------------------------------------------------------
# _resolve_entity_info
# ---------------------------------------------------------------------------


class TestResolveEntityInfo:
    """Test _resolve_entity_info extracts provider type and path from tags."""

    def test_defaults_to_delta_and_dash(self):
        entity = _make_entity("silver.records")
        provider_type, path = _resolve_entity_info("silver.records", entity)
        assert provider_type == "delta"
        assert path == "-"

    def test_reads_provider_type_from_tags(self):
        entity = _make_entity(
            "bronze.raw", tags={"provider_type": "csv", "provider.path": "/data/raw.csv"}
        )
        provider_type, path = _resolve_entity_info("bronze.raw", entity)
        assert provider_type == "csv"
        assert path == "/data/raw.csv"


# ---------------------------------------------------------------------------
# _format_table
# ---------------------------------------------------------------------------


class TestFormatTable:
    """Test _format_table produces readable output."""

    def test_renders_headers_and_rows(self):
        output = _format_table(["id", "name"], [["1", "Alice"], ["2", "Bob"]])
        assert "id" in output
        assert "name" in output
        assert "Alice" in output
        assert "Bob" in output

    def test_empty_headers_returns_message(self):
        output = _format_table([], [])
        assert "no columns" in output


# ---------------------------------------------------------------------------
# kindling entity show — fixture CSV path
# ---------------------------------------------------------------------------


class TestEntityShow:
    """Test entity show command reads from tests/entities/ fixture CSV."""

    def test_entity_show_with_fixture_csv_prints_table(self, monkeypatch):
        runner = CliRunner()
        entity_registry = Mock(spec=DataEntityRegistry)
        entity_registry.get_entity_definition.return_value = _make_entity("bronze.orders")
        entity_registry.get_entity_ids.return_value = ["bronze.orders"]

        def fake_get(service_type):
            if service_type is DataEntityRegistry:
                return entity_registry
            raise AssertionError(f"unexpected service: {service_type!r}")

        monkeypatch.setattr("kindling.injection.GlobalInjector.get", fake_get)

        with runner.isolated_filesystem():
            app_path = _write_app(Path("app.py"))
            _write_fixture_csv(Path("."), "bronze.orders", "id,value\n1,10\n2,20\n")

            result = runner.invoke(
                cli,
                ["entity", "show", "bronze.orders", "--app", str(app_path)],
            )

        assert result.exit_code == 0, result.output
        assert "bronze.orders" in result.output
        assert "id" in result.output
        assert "value" in result.output
        assert "Rows: 2" in result.output

    def test_entity_show_count_only_flag(self, monkeypatch):
        runner = CliRunner()
        entity_registry = Mock(spec=DataEntityRegistry)
        entity_registry.get_entity_definition.return_value = _make_entity("bronze.orders")
        entity_registry.get_entity_ids.return_value = ["bronze.orders"]

        def fake_get(service_type):
            if service_type is DataEntityRegistry:
                return entity_registry
            raise AssertionError(f"unexpected service: {service_type!r}")

        monkeypatch.setattr("kindling.injection.GlobalInjector.get", fake_get)

        with runner.isolated_filesystem():
            app_path = _write_app(Path("app.py"))
            _write_fixture_csv(Path("."), "bronze.orders", "id,value\n1,10\n2,20\n")

            result = runner.invoke(
                cli,
                ["entity", "show", "bronze.orders", "--count", "--app", str(app_path)],
            )

        assert result.exit_code == 0, result.output
        assert "Rows: 2" in result.output
        # Should not print the data table (no separator line from _format_table)
        assert "-+-" not in result.output

    def test_entity_show_unknown_entity_fails(self, monkeypatch):
        runner = CliRunner()
        entity_registry = Mock(spec=DataEntityRegistry)
        entity_registry.get_entity_definition.return_value = None
        entity_registry.get_entity_ids.return_value = []

        def fake_get(service_type):
            if service_type is DataEntityRegistry:
                return entity_registry
            raise AssertionError(f"unexpected service: {service_type!r}")

        monkeypatch.setattr("kindling.injection.GlobalInjector.get", fake_get)

        with runner.isolated_filesystem():
            app_path = _write_app(Path("app.py"))

            result = runner.invoke(
                cli,
                ["entity", "show", "missing.entity", "--app", str(app_path)],
            )

        assert result.exit_code != 0
        assert "not registered" in result.output

    def test_entity_show_no_fixture_falls_back_gracefully(self, monkeypatch):
        runner = CliRunner()
        entity_registry = Mock(spec=DataEntityRegistry)
        entity_registry.get_entity_definition.return_value = _make_entity(
            "silver.records", tags={"provider_type": "delta"}
        )
        entity_registry.get_entity_ids.return_value = ["silver.records"]

        def fake_get(service_type):
            if service_type is DataEntityRegistry:
                return entity_registry
            raise AssertionError(f"unexpected service: {service_type!r}")

        monkeypatch.setattr("kindling.injection.GlobalInjector.get", fake_get)

        with runner.isolated_filesystem():
            app_path = _write_app(Path("app.py"))
            # Intentionally do NOT write a fixture CSV

            result = runner.invoke(
                cli,
                ["entity", "show", "silver.records", "--app", str(app_path)],
            )

        assert result.exit_code == 0, result.output
        assert "silver.records" in result.output
        assert "No tests/entities/ fixture found" in result.output


# ---------------------------------------------------------------------------
# kindling entity validate — zero-row error and null warning
# ---------------------------------------------------------------------------


class TestEntityValidate:
    """Test entity validate command performs data quality checks."""

    def test_entity_validate_zero_rows_is_error(self, monkeypatch):
        runner = CliRunner()
        entity_registry = Mock(spec=DataEntityRegistry)
        entity_registry.get_entity_definition.return_value = _make_entity("bronze.orders")
        entity_registry.get_entity_ids.return_value = ["bronze.orders"]

        def fake_get(service_type):
            if service_type is DataEntityRegistry:
                return entity_registry
            raise AssertionError(f"unexpected service: {service_type!r}")

        monkeypatch.setattr("kindling.injection.GlobalInjector.get", fake_get)

        with runner.isolated_filesystem():
            app_path = _write_app(Path("app.py"))
            # Headers only — no data rows
            _write_fixture_csv(Path("."), "bronze.orders", "id,value\n")

            result = runner.invoke(
                cli,
                ["entity", "validate", "bronze.orders", "--app", str(app_path)],
            )

        assert result.exit_code == 1
        assert "ERROR" in result.output

    def test_entity_validate_null_in_key_column_is_warning(self, monkeypatch):
        runner = CliRunner()
        entity = _make_entity("bronze.orders", merge_columns=["id"])
        entity_registry = Mock(spec=DataEntityRegistry)
        entity_registry.get_entity_definition.return_value = entity
        entity_registry.get_entity_ids.return_value = ["bronze.orders"]

        def fake_get(service_type):
            if service_type is DataEntityRegistry:
                return entity_registry
            raise AssertionError(f"unexpected service: {service_type!r}")

        monkeypatch.setattr("kindling.injection.GlobalInjector.get", fake_get)

        with runner.isolated_filesystem():
            app_path = _write_app(Path("app.py"))
            # One row has empty id (null)
            _write_fixture_csv(Path("."), "bronze.orders", "id,value\n1,10\n,20\n")

            result = runner.invoke(
                cli,
                ["entity", "validate", "bronze.orders", "--app", str(app_path)],
            )

        # Should exit 0 (warnings only)
        assert result.exit_code == 0, result.output
        assert "WARN" in result.output
        assert "null_check" in result.output
        assert "warning" in result.output.lower()

    def test_entity_validate_all_pass(self, monkeypatch):
        runner = CliRunner()
        entity = _make_entity("bronze.orders", merge_columns=["id"])
        entity_registry = Mock(spec=DataEntityRegistry)
        entity_registry.get_entity_definition.return_value = entity
        entity_registry.get_entity_ids.return_value = ["bronze.orders"]

        def fake_get(service_type):
            if service_type is DataEntityRegistry:
                return entity_registry
            raise AssertionError(f"unexpected service: {service_type!r}")

        monkeypatch.setattr("kindling.injection.GlobalInjector.get", fake_get)

        with runner.isolated_filesystem():
            app_path = _write_app(Path("app.py"))
            _write_fixture_csv(Path("."), "bronze.orders", "id,value\n1,10\n2,20\n")

            result = runner.invoke(
                cli,
                ["entity", "validate", "bronze.orders", "--app", str(app_path)],
            )

        assert result.exit_code == 0, result.output
        assert "All checks passed" in result.output

    def test_entity_validate_missing_fixture_fails(self, monkeypatch):
        runner = CliRunner()
        entity_registry = Mock(spec=DataEntityRegistry)
        entity_registry.get_entity_definition.return_value = _make_entity("bronze.orders")
        entity_registry.get_entity_ids.return_value = ["bronze.orders"]

        def fake_get(service_type):
            if service_type is DataEntityRegistry:
                return entity_registry
            raise AssertionError(f"unexpected service: {service_type!r}")

        monkeypatch.setattr("kindling.injection.GlobalInjector.get", fake_get)

        with runner.isolated_filesystem():
            app_path = _write_app(Path("app.py"))

            result = runner.invoke(
                cli,
                ["entity", "validate", "bronze.orders", "--app", str(app_path)],
            )

        assert result.exit_code != 0
        assert "No tests/entities/ fixture found" in result.output


# ---------------------------------------------------------------------------
# kindling app inspect --entities — entity table output
# ---------------------------------------------------------------------------


class TestAppInspectEntities:
    """Test app inspect --entities shows entity resolution table."""

    def test_app_inspect_entities_table_output(self, monkeypatch):
        runner = CliRunner()
        entity_registry = Mock(spec=DataEntityRegistry)
        entity_registry.get_entity_ids.return_value = [
            "bronze.orders",
            "silver.orders",
        ]
        entity_registry.get_entity_definition.side_effect = lambda eid: {
            "bronze.orders": _make_entity(
                "bronze.orders",
                tags={"provider_type": "csv", "provider.path": "Files/raw/orders.csv"},
            ),
            "silver.orders": _make_entity(
                "silver.orders",
                tags={
                    "provider_type": "delta",
                    "provider.path": "abfss://silver@acct.dfs.core.windows.net/orders",
                },
            ),
        }[eid]

        def fake_get(service_type):
            if service_type is DataEntityRegistry:
                return entity_registry
            raise AssertionError(f"unexpected service: {service_type!r}")

        monkeypatch.setattr("kindling.injection.GlobalInjector.get", fake_get)

        with runner.isolated_filesystem():
            app_path = _write_app(Path("app.py"))
            # Write a fixture for the bronze entity
            _write_fixture_csv(Path("."), "bronze.orders", "id,value\n1,10\n")

            result = runner.invoke(
                cli,
                ["app", "inspect", "myapp", "--entities", "--app", str(app_path)],
            )

        assert result.exit_code == 0, result.output
        assert "App: myapp" in result.output
        assert "bronze.orders" in result.output
        assert "silver.orders" in result.output
        assert "csv" in result.output
        assert "delta" in result.output
        # fixture column shows the path for bronze, dash for silver
        assert "tests/entities" in result.output
        assert "-" in result.output

    def test_app_inspect_marks_fixture_without_rows_as_ignored(self, monkeypatch):
        runner = CliRunner()
        entity_registry = Mock(spec=DataEntityRegistry)
        entity_registry.get_entity_ids.return_value = ["bronze.orders"]
        entity_registry.get_entity_definition.return_value = _make_entity(
            "bronze.orders", tags={"provider_type": "delta"}
        )
        monkeypatch.setattr("kindling.injection.GlobalInjector.get", lambda _t: entity_registry)

        with runner.isolated_filesystem():
            app_path = _write_app(Path("app.py"))
            _write_fixture_csv(Path("."), "bronze.orders", "id\n")  # header-only stub

            result = runner.invoke(
                cli,
                ["app", "inspect", "myapp", "--entities", "--app", str(app_path)],
            )

        assert result.exit_code == 0, result.output
        assert "tests/entities/bronze/orders.csv (no data rows; ignored)" in result.output

    def test_app_inspect_entities_no_entities(self, monkeypatch):
        runner = CliRunner()
        entity_registry = Mock(spec=DataEntityRegistry)
        entity_registry.get_entity_ids.return_value = []

        def fake_get(service_type):
            if service_type is DataEntityRegistry:
                return entity_registry
            raise AssertionError(f"unexpected service: {service_type!r}")

        monkeypatch.setattr("kindling.injection.GlobalInjector.get", fake_get)

        with runner.isolated_filesystem():
            app_path = _write_app(Path("app.py"))

            result = runner.invoke(
                cli,
                ["app", "inspect", "myapp", "--entities", "--app", str(app_path)],
            )

        assert result.exit_code == 0, result.output
        assert "no entities registered" in result.output

    def test_app_inspect_without_entities_flag(self, monkeypatch):
        runner = CliRunner()
        entity_registry = Mock(spec=DataEntityRegistry)
        entity_registry.get_entity_ids.return_value = ["bronze.orders"]

        def fake_get(service_type):
            if service_type is DataEntityRegistry:
                return entity_registry
            raise AssertionError(f"unexpected service: {service_type!r}")

        monkeypatch.setattr("kindling.injection.GlobalInjector.get", fake_get)

        with runner.isolated_filesystem():
            app_path = _write_app(Path("app.py"))

            result = runner.invoke(
                cli,
                ["app", "inspect", "myapp", "--app", str(app_path)],
            )

        assert result.exit_code == 0, result.output
        assert "App: myapp" in result.output
        assert "--entities" in result.output

    def test_app_inspect_has_help_text(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["app", "inspect", "--help"])
        assert result.exit_code == 0
        assert "entity" in result.output.lower() or "entities" in result.output.lower()

    def test_entity_show_has_help_text(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["entity", "show", "--help"])
        assert result.exit_code == 0
        assert "limit" in result.output.lower() or "count" in result.output.lower()

    def test_entity_validate_has_help_text(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["entity", "validate", "--help"])
        assert result.exit_code == 0
        assert "env" in result.output.lower() or "quality" in result.output.lower()


# ---------------------------------------------------------------------------
# _warn_missing_entity_fixtures
# ---------------------------------------------------------------------------


class TestWarnMissingEntityFixtures:
    """Delta source entities without a usable fixture get a [WARN] line."""

    @staticmethod
    def _registries():
        entity_registry = Mock()
        entity_registry.get_entity_ids.return_value = ["bronze.orders"]
        entity_registry.get_entity_definition.return_value = _make_entity(
            "bronze.orders", tags={"provider_type": "delta"}
        )
        pipe_registry = Mock()
        pipe_registry.get_pipe_ids.return_value = ["silver.orders"]
        pipe_registry.get_pipe_definition.return_value = SimpleNamespace(
            input_entity_ids=["bronze.orders"]
        )
        return entity_registry, pipe_registry

    @pytest.mark.parametrize(
        "content, expected",
        [
            (None, "no fixture CSV at tests/entities/bronze/orders.csv"),
            ("id\n", "has no data rows and is ignored"),
            ("# stub\n", "has no data rows and is ignored"),
            ("id\n1\n", None),
        ],
    )
    def test_warning_reflects_whether_fixture_is_used(
        self, tmp_path, monkeypatch, capsys, content, expected
    ):
        from kindling_cli.cli import _warn_missing_entity_fixtures

        monkeypatch.chdir(tmp_path)
        if content is not None:
            _write_fixture_csv(tmp_path, "bronze.orders", content)

        _warn_missing_entity_fixtures(*self._registries())

        out = capsys.readouterr().out
        if expected is None:
            assert out == ""
        else:
            assert "[WARN] entity.bronze.orders.fixture" in out
            assert expected in out


def test_read_csv_rows_skips_comments_and_blank_records(tmp_path):
    """Matches the runtime fixture reader: a leading comment is not the header."""
    from kindling_cli.cli import _read_csv_rows

    fixture = tmp_path / "orders.csv"
    fixture.write_text("# sample orders\nid,name\n\n1,foo\n# trailing note\n2,bar\n")

    assert _read_csv_rows(fixture) == (["id", "name"], [["1", "foo"], ["2", "bar"]])
    (tmp_path / "empty.csv").write_text("# only a comment\n")
    assert _read_csv_rows(tmp_path / "empty.csv") == ([], [])


def test_read_csv_rows_keeps_delimiter_only_null_rows(tmp_path):
    """`,` is a data row of nulls (entity validate must see the null key)."""
    from kindling_cli.cli import _read_csv_rows

    fixture = tmp_path / "orders.csv"
    fixture.write_text("id,name\n,\n1,foo\n")
    assert _read_csv_rows(fixture) == (["id", "name"], [["", ""], ["1", "foo"]])
