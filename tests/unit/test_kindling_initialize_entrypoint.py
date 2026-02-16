from unittest.mock import patch


def test_initialize_passes_config_through():
    import kindling

    with patch.object(kindling, "initialize_framework", return_value="ok") as mock_init:
        result = kindling.initialize({"environment": "dev"})

    assert result == "ok"
    mock_init.assert_called_once_with({"environment": "dev"})


def test_initialize_app_name_argument_overrides_config():
    import kindling

    with patch.object(kindling, "initialize_framework", return_value="ok") as mock_init:
        kindling.initialize({"app_name": "from-config"}, app_name="from-arg")

    mock_init.assert_called_once_with({"app_name": "from-arg"})
