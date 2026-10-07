"""A deployed app's lake-reqs packages register their entities and pipes.

Remotely, DataAppManager installs the packages listed in lake-reqs.txt and
imports them. A scaffolded package keeps declarations in its entities /
pipes / ingestion subpackages with an empty __init__.py, so importing the
package alone registered nothing; the walk the local runner does must happen
here too.
"""

import sys
from unittest.mock import MagicMock

import pytest
from kindling.data_apps import DataAppManager


@pytest.fixture
def scaffolded_package(tmp_path, monkeypatch):
    """A package laid out like `kindling package init`: empty __init__.py,
    declarations in subpackages that record their import in a shared list."""
    name = "remote_reg_domain"
    root = tmp_path / name
    for sub in ("entities", "pipes"):
        (root / sub).mkdir(parents=True)
        (root / sub / "__init__.py").write_text("")
    (root / "__init__.py").write_text("")
    (root / "entities" / "bronze.py").write_text(
        "import builtins\nbuiltins._remote_reg.append('entities.bronze')\n"
    )
    (root / "pipes" / "silver_orders.py").write_text(
        "import builtins\nbuiltins._remote_reg.append('pipes.silver_orders')\n"
    )
    (root / "transforms.py").write_text(
        "import builtins\nbuiltins._remote_reg.append('transforms')\n"
    )

    import builtins

    builtins._remote_reg = []
    monkeypatch.syspath_prepend(str(tmp_path))
    yield name
    for module in [m for m in sys.modules if m == name or m.startswith(name + ".")]:
        del sys.modules[module]
    del builtins._remote_reg


def _manager():
    manager = DataAppManager.__new__(DataAppManager)
    manager.logger = MagicMock()
    return manager


def test_lake_package_declarations_register(scaffolded_package):
    import builtins

    _manager()._import_installed_packages([f"{scaffolded_package.replace('_', '-')}==0.1.0"])

    assert sorted(builtins._remote_reg) == ["entities.bronze", "pipes.silver_orders"]


def test_package_without_declaration_namespaces_is_fine(tmp_path, monkeypatch):
    (tmp_path / "plain_lib").mkdir()
    (tmp_path / "plain_lib" / "__init__.py").write_text("")
    monkeypatch.syspath_prepend(str(tmp_path))

    _manager()._import_installed_packages(["plain-lib"])
    sys.modules.pop("plain_lib", None)


def test_error_inside_a_declaration_module_is_raised(scaffolded_package, tmp_path):
    (tmp_path / scaffolded_package / "pipes" / "broken.py").write_text(
        "import not_a_real_module_xyz\n"
    )

    with pytest.raises(ModuleNotFoundError, match="not_a_real_module_xyz"):
        _manager()._import_installed_packages([scaffolded_package])


def test_registration_runs_after_all_dependencies_are_installed(monkeypatch):
    """A declaration module may import a library the app's requirements.txt
    provides, so lake packages are imported only after PyPI installs too."""
    order = []
    manager = _manager()
    monkeypatch.setattr(manager, "_download_lake_wheels", lambda app, reqs, tmp: "/cache")
    monkeypatch.setattr(
        manager, "_install_lake_wheels", lambda cache, reqs: order.append("lake") or True
    )
    monkeypatch.setattr(
        manager, "_install_pypi_dependencies", lambda deps, cache: order.append("pypi")
    )
    monkeypatch.setattr(
        manager, "_import_installed_packages", lambda reqs: order.append("register")
    )

    manager._install_app_dependencies("app", ["requests"], ["sales-domain"])

    assert order == ["lake", "pypi", "register"]


def test_no_registration_when_no_lake_wheels_installed(monkeypatch):
    calls = []
    manager = _manager()
    monkeypatch.setattr(manager, "_download_lake_wheels", lambda app, reqs, tmp: "/cache")
    monkeypatch.setattr(manager, "_install_lake_wheels", lambda cache, reqs: False)
    monkeypatch.setattr(manager, "_import_installed_packages", lambda reqs: calls.append(reqs))

    manager._install_app_dependencies("app", [], ["sales-domain"])

    assert calls == []
