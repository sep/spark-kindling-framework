import importlib.metadata
import os

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
