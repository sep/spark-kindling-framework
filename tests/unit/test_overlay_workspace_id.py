"""On Databricks the workspace_id setting is also the REST API host, so the
SDK passes the workspace URL. The workspace_<id>.yaml overlay must not be
looked up by that URL."""

from unittest.mock import patch

from kindling.bootstrap import _overlay_workspace_id


def test_plain_ids_are_unchanged():
    assert _overlay_workspace_id("1234567890", "databricks") == "1234567890"
    assert _overlay_workspace_id(None, "databricks") is None
    assert _overlay_workspace_id("my-synapse-ws", "synapse") == "my-synapse-ws"


def test_url_uses_detected_workspace_id():
    with patch("kindling.bootstrap._get_workspace_id_for_platform", return_value="1234567890"):
        assert (
            _overlay_workspace_id("https://adb-1234567890.4.azuredatabricks.net/", "databricks")
            == "1234567890"
        )


def test_url_falls_back_to_host_form_detection_uses():
    with patch("kindling.bootstrap._get_workspace_id_for_platform", return_value=None):
        assert (
            _overlay_workspace_id("https://adb-1234567890.4.azuredatabricks.net", "databricks")
            == "adb-1234567890_4_azuredatabricks_net"
        )
