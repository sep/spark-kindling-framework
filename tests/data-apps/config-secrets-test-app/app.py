#!/usr/bin/env python3
"""
Config Secrets System Test App.

Validates that secret-like values injected via config overrides are available
at runtime and that YAML references to those values are resolved by Dynaconf.
"""

import sys

from kindling.injection import get_kindling_service
from kindling.spark_config import ConfigService
from kindling.spark_log_provider import SparkLoggerProvider


def get_logger():
    logger_provider = get_kindling_service(SparkLoggerProvider)
    return logger_provider.get_logger("config-secrets-test-app")


def get_test_id(config_service: ConfigService) -> str:
    test_id = config_service.get("test_id")
    return str(test_id) if test_id else "unknown"


def build_expected(test_id: str):
    return {
        "service_token": f"svc-{test_id}-A1",
        "database_password": f"db-{test_id}-P9",
        "integration_key": f"wh-{test_id}-K7",
    }


def emit_result(logger, test_id: str, test_name: str, passed: bool, message: str = ""):
    status = "PASSED" if passed else "FAILED"
    suffix = f" {message}" if message else ""
    line = f"TEST_ID={test_id} test={test_name} status={status}{suffix}"
    logger.info(line)
    print(line)


def main():
    logger = get_logger()
    config_service = get_kindling_service(ConfigService)
    test_id = get_test_id(config_service)
    expected = build_expected(test_id)

    start_line = f"TEST_ID={test_id} status=STARTED component=config_secrets"
    logger.info(start_line)
    print(start_line)

    checks = {
        "config_secret_raw_service_token": (
            config_service.get("kindling.secrets.service.api_token"),
            expected["service_token"],
        ),
        "config_secret_raw_database_password": (
            config_service.get("kindling.secrets.database.password"),
            expected["database_password"],
        ),
        "config_secret_raw_integration_key": (
            config_service.get("kindling.secrets.integration.webhook_key"),
            expected["integration_key"],
        ),
        "config_secret_yaml_ref_service_token": (
            config_service.get("kindling.secret_refs.service.api_token"),
            expected["service_token"],
        ),
        "config_secret_yaml_ref_database_password": (
            config_service.get("kindling.secret_refs.database.password"),
            expected["database_password"],
        ),
        "config_secret_yaml_ref_integration_key": (
            config_service.get("kindling.secret_refs.integration.webhook_key"),
            expected["integration_key"],
        ),
        "config_secret_yaml_template_auth_header": (
            config_service.get("kindling.secret_templates.auth_header"),
            f"Bearer {expected['service_token']}",
        ),
    }

    all_passed = True
    for test_name, (actual, expected_value) in checks.items():
        passed = actual == expected_value
        all_passed = all_passed and passed
        detail = ""
        if not passed:
            detail = f"expected_len={len(str(expected_value))} actual_len={len(str(actual))}"
        emit_result(logger, test_id, test_name, passed, detail)

    print("\nTEST SUMMARY - " + ("PASSED" if all_passed else "FAILED"))
    for test_name, (actual, expected_value) in checks.items():
        status = "PASSED" if actual == expected_value else "FAILED"
        print(f"  {test_name}: {status}")

    final = "PASSED" if all_passed else "FAILED"
    complete_line = f"TEST_ID={test_id} status=COMPLETED result={final}"
    logger.info(complete_line)
    print(complete_line)

    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
