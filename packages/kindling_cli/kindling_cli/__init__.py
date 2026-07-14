"""Command-line tooling for Kindling.

INVARIANT: this module must stay import-light (no click, no third-party
imports). ``python -m kindling_cli.test_runner`` is the frontend Poe tasks
use to run tests from any interpreter — importing the package must never
require CLI dependencies. Pinned by
``tests/unit/test_test_runner.py::test_runner_module_imports_without_click``.
"""
