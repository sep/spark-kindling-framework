import json

import pytest

from tests.system import eventhub_test_resource as eh_setup


def test_env_resource_returns_none_when_partial(monkeypatch):
    monkeypatch.setenv("EVENTHUB_TEST_RESOURCE_GROUP", "rg-kindling-test")
    monkeypatch.delenv("EVENTHUB_TEST_CONNECTION_STRING", raising=False)

    resource = eh_setup._env_resource()
    assert resource is None


def test_env_resource_returns_resource_when_complete(monkeypatch):
    monkeypatch.setenv("EVENTHUB_TEST_RESOURCE_GROUP", "rg-kindling-test")
    monkeypatch.setenv("EVENTHUB_TEST_NAMESPACE", "kindling-test-ns")
    monkeypatch.setenv("EVENTHUB_TEST_NAME", "kindling-provider-test-hub")
    monkeypatch.setenv("EVENTHUB_TEST_AUTH_RULE", "kindling-system-tests")
    monkeypatch.setenv("EVENTHUB_TEST_CONSUMER_GROUP", "$Default")
    monkeypatch.setenv(
        "EVENTHUB_TEST_CONNECTION_STRING",
        "Endpoint=sb://kindling-test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=abc;EntityPath=kindling-provider-test-hub",
    )

    resource = eh_setup._env_resource()
    assert resource is not None
    assert resource.namespace_name == "kindling-test-ns"


def test_load_eventhub_test_resource_from_iac(monkeypatch):
    payload = {
        "eventhub_test_resource_group": {"value": "rg-kindling-test"},
        "eventhub_test_namespace": {"value": "kindling-test-ns"},
        "eventhub_test_name": {"value": "kindling-provider-test-hub"},
        "eventhub_test_auth_rule": {"value": "kindling-system-tests"},
        "eventhub_test_consumer_group": {"value": "$Default"},
        "eventhub_test_connection_string": {
            "value": "Endpoint=sb://kindling-test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=abc;EntityPath=kindling-provider-test-hub"
        },
    }

    monkeypatch.setattr(eh_setup, "_run_terraform_output", lambda _: payload)

    resource = eh_setup.load_eventhub_test_resource_from_iac("iac/azure/eventhub")
    assert resource.resource_group == "rg-kindling-test"
    assert resource.authorization_rule == "kindling-system-tests"


def test_resolve_prefers_env_when_present(monkeypatch):
    env_resource = eh_setup.EventHubTestResource(
        resource_group="env-rg",
        namespace_name="env-ns",
        eventhub_name="env-eh",
        authorization_rule="env-rule",
        consumer_group="$Default",
        connection_string="Endpoint=sb://env/",
    )

    monkeypatch.setattr(eh_setup, "_env_resource", lambda: env_resource)
    monkeypatch.setattr(
        eh_setup,
        "load_eventhub_test_resource_from_iac",
        lambda _: pytest.fail("IaC should not be called when env is complete"),
    )

    resolved = eh_setup.resolve_eventhub_test_resource("iac/azure/eventhub")
    assert resolved.resource_group == "env-rg"


def test_run_terraform_output_parses_json(monkeypatch):
    class Result:
        returncode = 0
        stdout = json.dumps({"eventhub_test_name": {"value": "kindling-provider-test-hub"}})
        stderr = ""

    monkeypatch.setattr(eh_setup.shutil, "which", lambda _: "/usr/bin/terraform")
    monkeypatch.setattr(eh_setup.subprocess, "run", lambda *args, **kwargs: Result())

    output = eh_setup._run_terraform_output("iac/azure/eventhub")
    assert output["eventhub_test_name"]["value"] == "kindling-provider-test-hub"


def test_main_returns_error_on_unresolved_resource(monkeypatch, capsys):
    def fail_resolve(*args, **kwargs):
        raise eh_setup.EventHubResourceResolutionError("no iac output")

    monkeypatch.setattr(eh_setup, "resolve_eventhub_test_resource", fail_resolve)

    exit_code = eh_setup.main([])
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "[ERROR]" in captured.out
