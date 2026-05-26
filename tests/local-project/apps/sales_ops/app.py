"""Sales Ops application entrypoint.

Responsibilities:
  - Locate app-owned config files relative to this app directory
  - Import entity/pipe modules to trigger registration side-effects
  - Expose initialize() for use by tests, notebooks, and deployed jobs

Local usage:
    from app import initialize
    svc = initialize()   # reads settings.local.yaml by default

Notebook/job usage (after installing the wheel):
    from app import initialize
    svc = initialize(env="prod")
"""

import os
import sys
from pathlib import Path

_APP_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _APP_DIR.parent.parent
_PACKAGE_SRC = _REPO_ROOT / "src"
if _PACKAGE_SRC.exists():
    sys.path.insert(0, str(_PACKAGE_SRC))


def _config_files(env: str) -> list[str]:
    files = [str(_APP_DIR / "settings.yaml")]
    env_file = _APP_DIR / f"settings.{env}.yaml"
    if env_file.exists():
        files.append(str(env_file))
    return files


def register_all() -> None:
    """Import all entity and pipe modules to trigger @DataEntities / @DataPipes registration."""
    import sales_ops.entities.orders  # noqa: F401
    import sales_ops.pipes.bronze_to_silver  # noqa: F401


def initialize(env: str | None = None):
    """Initialize the Kindling framework for standalone (local) execution.

    Args:
        env: Environment overlay name. Defaults to the KINDLING_ENV env var,
             falling back to "local".

    Returns:
        The initialized StandaloneService instance.
    """
    from kindling.bootstrap import initialize_framework

    if env is None:
        env = os.environ.get("KINDLING_ENV", "local")

    # initialize_framework must come first: it wires ConfigService into the DI
    # container. The @DataPipes.pipe decorator (fired on module import inside
    # register_all) resolves DataPipesManager from the injector, which
    # transitively needs ConfigService to be live.
    svc = initialize_framework(
        {
            "platform": "standalone",
            "environment": env,
            "config_files": _config_files(env),
        }
    )

    register_all()

    return svc
