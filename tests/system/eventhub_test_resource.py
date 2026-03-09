#!/usr/bin/env python3
"""
Event Hub test resource resolution for EventHubEntityProvider system tests.

Event Hub infrastructure is managed declaratively via Terraform in:
  iac/azure/eventhub

This helper resolves Event Hub test configuration from either:
1. Existing environment variables (EVENTHUB_TEST_*)
2. Terraform outputs from the IaC stack
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional


class EventHubResourceResolutionError(RuntimeError):
    """Raised when Event Hub test resource values cannot be resolved."""


@dataclass(frozen=True)
class EventHubTestResource:
    """Resolved Event Hub test resource details."""

    resource_group: str
    namespace_name: str
    eventhub_name: str
    authorization_rule: str
    consumer_group: str
    connection_string: str

    def to_env_vars(self) -> dict[str, str]:
        """Render resource values as environment variables."""
        return {
            "EVENTHUB_TEST_RESOURCE_GROUP": self.resource_group,
            "EVENTHUB_TEST_NAMESPACE": self.namespace_name,
            "EVENTHUB_TEST_NAME": self.eventhub_name,
            "EVENTHUB_TEST_AUTH_RULE": self.authorization_rule,
            "EVENTHUB_TEST_CONSUMER_GROUP": self.consumer_group,
            "EVENTHUB_TEST_CONNECTION_STRING": self.connection_string,
        }


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Resolve Event Hub test resource values from env vars or Terraform outputs "
            "(iac/azure/eventhub)."
        )
    )
    parser.add_argument(
        "--iac-dir",
        default="iac/azure/eventhub",
        help="Terraform directory containing Event Hub IaC stack.",
    )
    parser.add_argument(
        "--env-file",
        default="",
        help="Optional file path to write environment variables (KEY=VALUE lines).",
    )
    return parser.parse_args(argv)


def _ensure_terraform_cli() -> None:
    if shutil.which("terraform") is None:
        raise EventHubResourceResolutionError(
            "Terraform is not installed or not in PATH. " "Install Terraform to read IaC outputs."
        )


def _run_terraform_output(iac_dir: str) -> dict[str, Any]:
    _ensure_terraform_cli()

    cmd = ["terraform", f"-chdir={iac_dir}", "output", "-json"]
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)

    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        raise EventHubResourceResolutionError(
            f"Failed to read Terraform outputs from {iac_dir}. "
            f"Run 'terraform init && terraform apply' first.\n{stderr}"
        )

    stdout = (result.stdout or "").strip()
    if not stdout:
        raise EventHubResourceResolutionError(
            f"Terraform output is empty in {iac_dir}. " "Apply the Event Hub IaC stack first."
        )

    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise EventHubResourceResolutionError(
            f"Terraform output was not valid JSON: {exc}"
        ) from exc

    if not isinstance(payload, dict):
        raise EventHubResourceResolutionError("Unexpected Terraform output payload format.")

    return payload


def _terraform_output_value(outputs: dict[str, Any], key: str) -> str:
    entry = outputs.get(key)
    if not isinstance(entry, dict) or "value" not in entry:
        raise EventHubResourceResolutionError(
            f"Missing Terraform output '{key}'. Ensure iac/azure/eventhub/outputs.tf defines it."
        )

    value = entry["value"]
    if value is None:
        raise EventHubResourceResolutionError(f"Terraform output '{key}' is null.")

    text = str(value).strip()
    if not text:
        raise EventHubResourceResolutionError(f"Terraform output '{key}' is empty.")

    return text


def _env_resource() -> Optional[EventHubTestResource]:
    required = {
        "resource_group": os.getenv("EVENTHUB_TEST_RESOURCE_GROUP", "").strip(),
        "namespace_name": os.getenv("EVENTHUB_TEST_NAMESPACE", "").strip(),
        "eventhub_name": os.getenv("EVENTHUB_TEST_NAME", "").strip(),
        "authorization_rule": os.getenv("EVENTHUB_TEST_AUTH_RULE", "").strip(),
        "consumer_group": os.getenv("EVENTHUB_TEST_CONSUMER_GROUP", "").strip(),
        "connection_string": os.getenv("EVENTHUB_TEST_CONNECTION_STRING", "").strip(),
    }

    if all(required.values()):
        return EventHubTestResource(**required)

    return None


def load_eventhub_test_resource_from_iac(iac_dir: str) -> EventHubTestResource:
    """Load Event Hub test resource details from Terraform outputs."""
    outputs = _run_terraform_output(iac_dir)

    return EventHubTestResource(
        resource_group=_terraform_output_value(outputs, "eventhub_test_resource_group"),
        namespace_name=_terraform_output_value(outputs, "eventhub_test_namespace"),
        eventhub_name=_terraform_output_value(outputs, "eventhub_test_name"),
        authorization_rule=_terraform_output_value(outputs, "eventhub_test_auth_rule"),
        consumer_group=_terraform_output_value(outputs, "eventhub_test_consumer_group"),
        connection_string=_terraform_output_value(outputs, "eventhub_test_connection_string"),
    )


def resolve_eventhub_test_resource(iac_dir: str) -> EventHubTestResource:
    """Resolve Event Hub test resource from env vars or IaC outputs."""
    env_resource = _env_resource()
    if env_resource is not None:
        return env_resource

    return load_eventhub_test_resource_from_iac(iac_dir)


def _write_env_file(path: str, env_vars: dict[str, str]) -> None:
    lines = [f"{key}={value}" for key, value in env_vars.items()]
    output = "\n".join(lines) + "\n"
    Path(path).write_text(output, encoding="utf-8")


def _print_exports(env_vars: dict[str, str]) -> None:
    print("\n# Export commands")
    for key, value in env_vars.items():
        print(f"export {key}={shlex.quote(value)}")


def main(argv: Optional[list[str]] = None) -> int:
    args = _parse_args(argv)

    try:
        resource = resolve_eventhub_test_resource(iac_dir=args.iac_dir)
    except EventHubResourceResolutionError as exc:
        print(f"[ERROR] {exc}")
        return 1

    env_vars = resource.to_env_vars()
    print("[OK] Event Hub test resource resolved.")

    if args.env_file:
        _write_env_file(args.env_file, env_vars)
        print(f"[WRITE] Wrote env file: {args.env_file}")

    _print_exports(env_vars)
    return 0


if __name__ == "__main__":
    sys.exit(main())
