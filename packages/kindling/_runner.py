"""Standalone app runner.

Invoked by `kindling app run` for local/standalone execution:
    python -m kindling._runner --env local --config settings.yaml [--config ...] app.py

Initializes the Kindling framework then execs the app entrypoint in-process,
mirroring how DataAppManager._execute_app() works for remote runs.
"""

import argparse
import builtins
import sys
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(prog="kindling._runner", add_help=False)
    parser.add_argument("app_path")
    parser.add_argument("--env", default="local")
    parser.add_argument("--config", action="append", dest="config_files", default=[])
    args = parser.parse_args()

    app_path = Path(args.app_path).resolve()
    config_files = [f for f in args.config_files if Path(f).exists()]

    from kindling.bootstrap import initialize_framework

    initialize_framework(
        {
            "platform": "standalone",
            "environment": args.env,
            "config_files": config_files,
        }
    )

    code = app_path.read_text(encoding="utf-8")
    exec_globals = {
        "__name__": "__main__",
        "__file__": str(app_path),
        "__spec__": None,
        "__builtins__": builtins,
    }
    # Ensure imports inside the app resolve relative to its directory
    app_dir = str(app_path.parent)
    if app_dir not in sys.path:
        sys.path.insert(0, app_dir)

    exec(compile(code, str(app_path), "exec"), exec_globals)  # noqa: S102


if __name__ == "__main__":
    main()
