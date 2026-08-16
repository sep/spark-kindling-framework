"""Unit tests for `kindling app check` and `kindling package check`.

These replace the top-level `kindling doctor` command originally proposed
in docs/proposals/kindling_cli_devex_gaps.md -- the project owner's design
correction folds the composite health-check idea into the existing `app`
and `package` groups using the `check` verb (consistent with `env check`)
instead of adding a new top-level command.
"""

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

from click.testing import CliRunner
from kindling.data_entities import DataEntityRegistry
from kindling.data_pipes import DataPipesRegistry
from kindling_cli.cli import _detect_runtime_version_skew, cli


def _write_app(path: Path, body: str = None) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        body or "def initialize(env=None, config_dir=None):\n    return None\n",
        encoding="utf-8",
    )
    return path


def _patch_registries(monkeypatch, entity_registry, pipe_registry):
    def fake_get(service_type):
        if service_type is DataEntityRegistry:
            return entity_registry
        if service_type is DataPipesRegistry:
            return pipe_registry
        raise AssertionError(f"unexpected service: {service_type!r}")

    monkeypatch.setattr("kindling.injection.GlobalInjector.get", fake_get)


def _good_registries():
    entity_registry = Mock()
    entity_registry.get_entity_ids.return_value = ["bronze.records", "silver.records"]
    entity_registry.get_entity_definition.side_effect = lambda entity_id: {
        "bronze.records": SimpleNamespace(
            entityid="bronze.records", tags={"provider_type": "memory"}, merge_columns=[]
        ),
        "silver.records": SimpleNamespace(
            entityid="silver.records", tags={"provider_type": "delta"}, merge_columns=["id"]
        ),
    }[entity_id]
    pipe_registry = Mock()
    pipe_registry.get_pipe_ids.return_value = ["bronze_to_silver"]
    pipe_registry.get_pipe_definition.return_value = SimpleNamespace(
        pipeid="bronze_to_silver",
        input_entity_ids=["bronze.records"],
        output_entity_id="silver.records",
    )
    return entity_registry, pipe_registry


# ---------------------------------------------------------------------------
# _detect_runtime_version_skew (pure logic)
# ---------------------------------------------------------------------------


class TestDetectRuntimeVersionSkew:
    def test_no_deployed_wheel_returns_none(self):
        store = Mock()
        store.list_files.return_value = []
        assert _detect_runtime_version_skew(store) is None

    def test_store_error_returns_none(self):
        store = Mock()
        store.list_files.side_effect = RuntimeError("boom")
        assert _detect_runtime_version_skew(store) is None

    def test_outdated_deployed_wheel_flagged(self, monkeypatch):
        store = Mock()
        store.list_files.return_value = ["packages/spark_kindling-0.1.0-py3-none-any.whl"]
        monkeypatch.setattr(
            "kindling_cli.cli._get_version", lambda pkg: "9.9.9" if "cli" in pkg else "unknown"
        )

        result = _detect_runtime_version_skew(store)

        assert result == ("0.1.0", "9.9.9", True)

    def test_up_to_date_deployed_wheel_not_flagged(self, monkeypatch):
        store = Mock()
        store.list_files.return_value = ["packages/spark_kindling-9.9.9-py3-none-any.whl"]
        monkeypatch.setattr(
            "kindling_cli.cli._get_version", lambda pkg: "1.0.0" if "cli" in pkg else "unknown"
        )

        result = _detect_runtime_version_skew(store)

        assert result == ("9.9.9", "1.0.0", False)


# ---------------------------------------------------------------------------
# kindling app check
# ---------------------------------------------------------------------------


class TestAppCheck:
    def test_passes_with_healthy_app_and_notes_skipped_version_skew(self, monkeypatch):
        entity_registry, pipe_registry = _good_registries()
        _patch_registries(monkeypatch, entity_registry, pipe_registry)

        runner = CliRunner()
        with runner.isolated_filesystem():
            app_path = _write_app(Path("app.py"))
            result = runner.invoke(cli, ["app", "check", "--app", str(app_path)])

        assert result.exit_code == 0, result.output
        assert "[PASS] app_import" in result.output
        assert "[PASS] entities_registered" in result.output
        assert "[PASS] pipe.bronze_to_silver.input_entities: OK" in result.output
        assert "[SKIP] runtime_version_skew: skipped (pass --platform" in result.output
        assert "Entities: 2  Pipes: 1" in result.output
        assert "App check passed." in result.output

    def test_import_failure_reports_app_import_check_without_aborting(self, monkeypatch):
        runner = CliRunner()
        with runner.isolated_filesystem():
            app_path = _write_app(Path("app.py"), "raise ImportError('missing dependency')\n")
            result = runner.invoke(cli, ["app", "check", "--app", str(app_path)])

        assert result.exit_code == 1
        assert "[FAIL] app_import" in result.output
        assert "missing dependency" in result.output
        assert "Entities: 0  Pipes: 0" in result.output
        assert "App check failed" in result.output

    def test_graph_failure_fails_check_and_exit_code(self, monkeypatch):
        entity_registry = Mock()
        entity_registry.get_entity_ids.return_value = []
        pipe_registry = Mock()
        pipe_registry.get_pipe_ids.return_value = ["orphan_pipe"]
        pipe_registry.get_pipe_definition.return_value = SimpleNamespace(
            pipeid="orphan_pipe",
            input_entity_ids=["missing.entity"],
            output_entity_id="missing.output",
        )
        _patch_registries(monkeypatch, entity_registry, pipe_registry)

        runner = CliRunner()
        with runner.isolated_filesystem():
            app_path = _write_app(Path("app.py"))
            result = runner.invoke(cli, ["app", "check", "--app", str(app_path)])

        assert result.exit_code == 1
        assert "[FAIL] pipe.orphan_pipe.input_entities: missing: missing.entity" in result.output
        assert "App check failed" in result.output

    def test_json_output_on_success(self, monkeypatch):
        entity_registry, pipe_registry = _good_registries()
        _patch_registries(monkeypatch, entity_registry, pipe_registry)

        runner = CliRunner()
        with runner.isolated_filesystem():
            app_path = _write_app(Path("app.py"))
            result = runner.invoke(cli, ["app", "check", "--app", str(app_path), "--json"])

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["passed"] is True
        assert payload["entity_count"] == 2
        assert payload["pipe_count"] == 1
        names = {c["name"] for c in payload["checks"]}
        assert "app_import" in names
        assert any("runtime_version_skew" in note for note in payload["notes"])

    def test_json_output_on_import_failure(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            app_path = _write_app(Path("app.py"), "raise ImportError('bang')\n")
            result = runner.invoke(cli, ["app", "check", "--app", str(app_path), "--json"])

        assert result.exit_code == 1
        payload = json.loads(result.output)
        assert payload["passed"] is False
        assert payload["checks"][0]["name"] == "app_import"
        assert payload["checks"][0]["passed"] is False

    def test_platform_flag_runs_version_skew_check(self, monkeypatch):
        entity_registry, pipe_registry = _good_registries()
        _patch_registries(monkeypatch, entity_registry, pipe_registry)
        monkeypatch.setattr(
            "kindling_cli.cli.resolve_artifacts_path", lambda *a, **k: "/tmp/fake-artifacts"
        )
        fake_store = Mock()
        monkeypatch.setattr("kindling_cli.cli._open_store", lambda dest: fake_store)
        monkeypatch.setattr(
            "kindling_cli.cli._detect_runtime_version_skew",
            lambda store: ("0.1.0", "9.9.9", True),
        )

        runner = CliRunner()
        with runner.isolated_filesystem():
            app_path = _write_app(Path("app.py"))
            result = runner.invoke(
                cli, ["app", "check", "--app", str(app_path), "--platform", "databricks"]
            )

        assert result.exit_code == 1
        assert "[FAIL] runtime_version_skew" in result.output
        assert "deployed v0.1.0 vs CLI v9.9.9" in result.output

    def test_platform_flag_without_artifacts_path_skips_gracefully(self, monkeypatch):
        entity_registry, pipe_registry = _good_registries()
        _patch_registries(monkeypatch, entity_registry, pipe_registry)

        def fake_resolve(*args, **kwargs):
            raise ValueError("no artifacts destination configured")

        monkeypatch.setattr("kindling_cli.cli.resolve_artifacts_path", fake_resolve)

        runner = CliRunner()
        with runner.isolated_filesystem():
            app_path = _write_app(Path("app.py"))
            result = runner.invoke(
                cli, ["app", "check", "--app", str(app_path), "--platform", "databricks"]
            )

        assert result.exit_code == 0, result.output
        assert "[SKIP] runtime_version_skew: skipped (no artifacts storage configured" in (
            result.output
        )


# ---------------------------------------------------------------------------
# kindling package check
# ---------------------------------------------------------------------------


class TestPackageCheck:
    def _write_package(self, root: Path, with_src: bool = True) -> Path:
        root.mkdir(parents=True, exist_ok=True)
        (root / "pyproject.toml").write_text(
            '[tool.poetry]\nname = "domain-records"\nversion = "1.2.3"\n',
            encoding="utf-8",
        )
        if with_src:
            pkg_dir = root / "src" / "domain_records"
            pkg_dir.mkdir(parents=True)
            (pkg_dir / "__init__.py").write_text("", encoding="utf-8")
        return root

    def test_skip_build_checks_metadata_and_layout_only(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            package_dir = Path("packages/domain_records")
            self._write_package(package_dir)

            result = runner.invoke(
                cli,
                [
                    "package",
                    "check",
                    "domain-records",
                    "--local-folder",
                    str(package_dir),
                    "--skip-build",
                ],
            )

        assert result.exit_code == 0, result.output
        assert "[PASS] pyproject: domain-records 1.2.3" in result.output
        assert "[PASS] src_layout" in result.output
        assert "[SKIP] wheel_build: skipped (--skip-build)" in result.output
        assert "Package check passed." in result.output

    def test_missing_pyproject_fails(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            package_dir = Path("packages/domain_records")
            package_dir.mkdir(parents=True)

            result = runner.invoke(
                cli,
                [
                    "package",
                    "check",
                    "domain-records",
                    "--local-folder",
                    str(package_dir),
                    "--skip-build",
                ],
            )

        assert result.exit_code == 1
        assert "[FAIL] pyproject" in result.output
        assert "Package check failed" in result.output

    def test_missing_src_layout_fails(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            package_dir = Path("packages/domain_records")
            self._write_package(package_dir, with_src=False)

            result = runner.invoke(
                cli,
                [
                    "package",
                    "check",
                    "domain-records",
                    "--local-folder",
                    str(package_dir),
                    "--skip-build",
                ],
            )

        assert result.exit_code == 1
        assert "[FAIL] src_layout" in result.output

    def test_wheel_build_check_runs_poetry_build(self, monkeypatch):
        calls = {}

        def fake_run(cmd, cwd=None, capture_output=False, text=False):
            calls["cmd"] = cmd
            calls["cwd"] = cwd
            dist = Path(cwd) / "dist"
            dist.mkdir(exist_ok=True)
            (dist / "domain_records-1.2.3-py3-none-any.whl").write_bytes(b"wheel")
            return SimpleNamespace(returncode=0, stdout="", stderr="")

        monkeypatch.setattr("kindling_cli.cli.subprocess.run", fake_run)

        runner = CliRunner()
        with runner.isolated_filesystem():
            package_dir = Path("packages/domain_records")
            self._write_package(package_dir)

            result = runner.invoke(
                cli,
                ["package", "check", "domain-records", "--local-folder", str(package_dir)],
            )

        assert result.exit_code == 0, result.output
        assert calls["cmd"][:2] == ["poetry", "build"]
        assert "[PASS] wheel_build: built domain_records-1.2.3-py3-none-any.whl" in result.output

    def test_wheel_build_failure_reports_check_failure(self, monkeypatch):
        def fake_run(cmd, cwd=None, capture_output=False, text=False):
            return SimpleNamespace(returncode=1, stdout="", stderr="dependency resolution failed")

        monkeypatch.setattr("kindling_cli.cli.subprocess.run", fake_run)

        runner = CliRunner()
        with runner.isolated_filesystem():
            package_dir = Path("packages/domain_records")
            self._write_package(package_dir)

            result = runner.invoke(
                cli,
                ["package", "check", "domain-records", "--local-folder", str(package_dir)],
            )

        assert result.exit_code == 1
        assert "[FAIL] wheel_build" in result.output
        assert "dependency resolution failed" in result.output

    def test_json_output(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            package_dir = Path("packages/domain_records")
            self._write_package(package_dir)
            expected_path = str(package_dir.resolve())

            result = runner.invoke(
                cli,
                [
                    "package",
                    "check",
                    "domain-records",
                    "--local-folder",
                    str(package_dir),
                    "--skip-build",
                    "--json",
                ],
            )

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["passed"] is True
        assert payload["package_path"] == expected_path
        names = {c["name"] for c in payload["checks"]}
        assert names == {"pyproject", "src_layout"}
