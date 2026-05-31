import importlib.metadata
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

from kindling_cli import _runner


def test_install_local_package_version_shim_returns_local_version(monkeypatch):
    monkeypatch.setenv("KINDLING_LOCAL_PACKAGE_MODULES", '["sample_engine"]')
    monkeypatch.delenv("KINDLING_LOCAL_PACKAGE_VERSION", raising=False)

    restore = _runner._install_local_package_version_shim()
    assert restore is not None

    try:
        assert importlib.metadata.version("sample_engine") == "0.0.0+local"
        assert importlib.metadata.version("sample-engine") == "0.0.0+local"
    finally:
        restore()


def test_install_local_package_version_shim_uses_configured_local_version(monkeypatch):
    monkeypatch.setenv("KINDLING_LOCAL_PACKAGE_MODULES", '["sample_engine"]')
    monkeypatch.setenv("KINDLING_LOCAL_PACKAGE_VERSION", "dev")

    restore = _runner._install_local_package_version_shim()
    assert restore is not None

    try:
        assert importlib.metadata.version("sample_engine") == "dev"
    finally:
        restore()


def test_install_local_package_version_shim_delegates_non_local_packages(monkeypatch):
    monkeypatch.setenv("KINDLING_LOCAL_PACKAGE_MODULES", '["sample_engine"]')

    restore = _runner._install_local_package_version_shim()
    assert restore is not None

    try:
        assert importlib.metadata.version("pytest")
    finally:
        restore()


def test_install_local_package_version_shim_ignores_invalid_env(monkeypatch):
    monkeypatch.setenv("KINDLING_LOCAL_PACKAGE_MODULES", "not-json")
    monkeypatch.delenv("PYTHONPATH", raising=False)

    restore = _runner._install_local_package_version_shim()
    assert restore is None


def test_load_local_package_modules_discovers_from_pythonpath(monkeypatch, tmp_path):
    package_root = tmp_path / "local_src"
    package_root.mkdir()
    package_dir = package_root / "sample_engine"
    package_dir.mkdir()
    (package_dir / "__init__.py").write_text("", encoding="utf-8")

    monkeypatch.delenv("KINDLING_LOCAL_PACKAGE_MODULES", raising=False)
    monkeypatch.setenv("PYTHONPATH", str(package_root))

    modules = _runner._load_local_package_modules()
    assert "sample_engine" in modules


def test_load_local_package_modules_discovers_namespace_packages(monkeypatch, tmp_path):
    package_root = tmp_path / "local_src"
    package_root.mkdir()
    (package_root / "sample_engine").mkdir()  # namespace package — no __init__.py

    monkeypatch.delenv("KINDLING_LOCAL_PACKAGE_MODULES", raising=False)
    monkeypatch.setenv("PYTHONPATH", str(package_root))

    modules = _runner._load_local_package_modules()
    assert "sample_engine" in modules


def test_install_local_package_version_shim_discovers_from_pythonpath(monkeypatch, tmp_path):
    package_root = tmp_path / "local_src"
    package_root.mkdir()
    package_dir = package_root / "sample_engine"
    package_dir.mkdir()
    (package_dir / "__init__.py").write_text("", encoding="utf-8")

    monkeypatch.delenv("KINDLING_LOCAL_PACKAGE_MODULES", raising=False)
    monkeypatch.setenv("PYTHONPATH", str(package_root))
    monkeypatch.delenv("KINDLING_LOCAL_PACKAGE_VERSION", raising=False)

    restore = _runner._install_local_package_version_shim()
    assert restore is not None

    try:
        assert importlib.metadata.version("sample_engine") == "0.0.0+local"
    finally:
        restore()


# --- lake-reqs.txt tests ---


def test_read_lake_requirements_returns_packages(tmp_path):
    (tmp_path / "lake-reqs.txt").write_text(
        "my-pkg==1.0.0\nother-pkg\n# comment\n", encoding="utf-8"
    )

    result = _runner._read_lake_requirements(tmp_path)

    assert result == ["my-pkg==1.0.0", "other-pkg"]


def test_read_lake_requirements_missing_file_returns_empty(tmp_path):
    result = _runner._read_lake_requirements(tmp_path)

    assert result == []


def test_read_lake_requirements_skips_blank_and_comment_lines(tmp_path):
    (tmp_path / "lake-reqs.txt").write_text(
        "\n# header\nfoo\n  \n# another comment\nbar\n", encoding="utf-8"
    )

    result = _runner._read_lake_requirements(tmp_path)

    assert result == ["foo", "bar"]


def test_is_editable_install_returns_true_for_editable(tmp_path):
    direct_url = tmp_path / "direct_url.json"
    direct_url.write_text(
        '{"url": "file:///some/path", "dir_info": {"editable": true}}', encoding="utf-8"
    )

    mock_dist = MagicMock()
    mock_dist.read_text.return_value = direct_url.read_text(encoding="utf-8")

    with patch("importlib.metadata.distribution", return_value=mock_dist):
        assert _runner._is_editable_install("spark-kindling") is True


def test_is_editable_install_returns_false_for_wheel():
    mock_dist = MagicMock()
    mock_dist.read_text.return_value = None  # no direct_url.json

    with patch("importlib.metadata.distribution", return_value=mock_dist):
        assert _runner._is_editable_install("some-pkg") is False


def test_is_editable_install_returns_false_when_not_installed():
    import importlib.metadata as im

    with patch("importlib.metadata.distribution", side_effect=im.PackageNotFoundError("nope")):
        assert _runner._is_editable_install("missing-pkg") is False


def test_filter_editable_packages_removes_editable_entries():
    def fake_is_editable(name):
        return name == "spark-kindling"

    with patch.object(_runner, "_is_editable_install", side_effect=fake_is_editable):
        result = _runner._filter_editable_packages(["spark-kindling==0.10.0", "custom-pkg==1.0.0"])

    assert result == ["custom-pkg==1.0.0"]


def test_filter_editable_packages_passes_non_editable_through():
    with patch.object(_runner, "_is_editable_install", return_value=False):
        result = _runner._filter_editable_packages(["custom-pkg==1.0.0", "other-pkg"])

    assert result == ["custom-pkg==1.0.0", "other-pkg"]


def test_install_lake_requirements_warns_when_no_artifacts_path():
    mock_manager = MagicMock()
    mock_manager.artifacts_path = None

    with (
        patch("kindling.data_apps.DataAppManager") as _mock_cls,
        patch("kindling.injection.get_kindling_service", return_value=mock_manager),
    ):
        _runner._install_lake_requirements(["my-pkg==1.0.0"], "myapp")

    mock_manager._install_app_dependencies.assert_not_called()


def test_install_lake_requirements_calls_manager_when_artifacts_configured():
    mock_manager = MagicMock()
    mock_manager.artifacts_path = "abfss://artifacts@storage.dfs.core.windows.net"

    with patch("kindling.injection.get_kindling_service", return_value=mock_manager):
        _runner._install_lake_requirements(["my-pkg==1.0.0", "other-pkg"], "myapp")

    mock_manager._install_app_dependencies.assert_called_once_with(
        "myapp", [], ["my-pkg==1.0.0", "other-pkg"]
    )


def test_install_lake_requirements_skips_empty_list():
    with patch("kindling.injection.get_kindling_service") as mock_get:
        _runner._install_lake_requirements([], "myapp")

    mock_get.assert_not_called()


def test_install_lake_requirements_warns_on_exception():
    with patch(
        "kindling.injection.get_kindling_service", side_effect=RuntimeError("injector not ready")
    ):
        # should not raise
        _runner._install_lake_requirements(["my-pkg==1.0.0"], "myapp")
