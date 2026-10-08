"""Import a domain package's declaration modules so they register.

Entities, pipes and file-ingestion entries register as a side effect of
importing their modules. By convention a package keeps them in its
``entities``, ``pipes`` and ``ingestion`` subpackages; importing the package
itself does not import those (a scaffolded ``__init__.py`` is empty). Both the
local runner and a deployed app's ``lake-reqs.txt`` packages go through
``import_package_registrations`` so a package registers the same way
everywhere.
"""

import importlib
import pkgutil
from typing import Iterable

REGISTRATION_NAMESPACES = ("entities", "pipes", "ingestion")


def import_registration_namespace(module_name: str) -> int:
    """Import a namespace package and every module under it; return the count."""
    imported_count = 0
    package = importlib.import_module(module_name)
    imported_count += 1

    package_path = getattr(package, "__path__", None)
    if package_path is None:
        return imported_count

    for module_info in pkgutil.walk_packages(package_path, prefix=f"{module_name}."):
        importlib.import_module(module_info.name)
        imported_count += 1
    return imported_count


def import_package_registrations(logger, module_roots: Iterable[str]) -> int:
    """Import ``<root>.entities`` / ``.pipes`` / ``.ingestion`` (and everything
    under them) for each module root. A root without one of those namespaces
    is fine; an import error inside a declaration module propagates."""
    roots = [root for root in module_roots if root]
    total_imported = 0
    for module_root in roots:
        imported_for_root = 0
        for namespace in REGISTRATION_NAMESPACES:
            module_name = f"{module_root}.{namespace}"
            try:
                imported_for_root += import_registration_namespace(module_name)
            except ModuleNotFoundError as error:
                if error.name in {module_root, module_name}:
                    continue
                raise
        total_imported += imported_for_root
        if imported_for_root == 0:
            logger.debug(f"No package registration modules found under {module_root}")

    if roots:
        logger.info(
            f"Imported {total_imported} package registration "
            f"module{'' if total_imported == 1 else 's'} from {', '.join(roots)}"
        )
    return total_imported
