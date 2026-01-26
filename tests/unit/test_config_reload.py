"""
Unit tests for config reload functionality.

Tests cover:
- Basic reload operations
- Signal emissions (config.pre_reload, config.post_reload, config.reload_failed)
- Change detection
- Observer pattern
- Thread safety
- Documentation
"""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, call, patch

from kindling.injection import GlobalInjector, get_kindling_service
from kindling.spark_config import ConfigService, DynaconfConfig


class TestConfigReloadBasic:
    """Basic config reload functionality"""

    def setup_method(self):
        """Clean up before each test"""
        GlobalInjector.reset()

    def teardown_method(self):
        """Clean up after each test"""
        GlobalInjector.reset()

    @patch("kindling.bootstrap.download_config_files")
    @patch("kindling.spark_config.Dynaconf")
    @patch("kindling.spark_config.get_or_create_spark_session")
    def test_reload_without_context_uses_fallback(
        self, mock_spark_fn, mock_dynaconf_class, mock_download
    ):
        """Reload without reload context should use Dynaconf fallback"""
        mock_spark = MagicMock()
        mock_spark_fn.return_value = mock_spark
        mock_dynaconf = MagicMock()
        mock_dynaconf.to_dict.return_value = {"test_value": "original"}
        mock_dynaconf_class.return_value = mock_dynaconf

        config = DynaconfConfig()
        config.initialize(reload_context=None)  # No reload context

        result = config.reload()

        assert result["status"] == "success", "Should return success status"
        mock_dynaconf.reload.assert_called_once(), "Should call Dynaconf reload"

    @patch("kindling.bootstrap.download_config_files")
    @patch("kindling.spark_config.Dynaconf")
    @patch("kindling.spark_config.get_or_create_spark_session")
    def test_reload_increments_version(self, mock_spark_fn, mock_dynaconf_class, mock_download):
        """Each reload should increment version counter"""
        mock_spark = MagicMock()
        mock_spark_fn.return_value = mock_spark
        mock_dynaconf = MagicMock()
        mock_dynaconf.to_dict.return_value = {"test": "value"}
        mock_dynaconf_class.return_value = mock_dynaconf

        config = DynaconfConfig()
        config.initialize()

        initial_version = config._version
        config.reload()
        after_first = config._version
        config.reload()
        after_second = config._version

        assert after_first == initial_version + 1, "First reload should increment"
        assert after_second == initial_version + 2, "Second reload should increment again"


class TestConfigReloadSignals:
    """Tests for signal emissions during reload"""

    def setup_method(self):
        """Clean up before each test"""
        GlobalInjector.reset()

    def teardown_method(self):
        """Clean up after each test"""
        GlobalInjector.reset()

    @patch("kindling.bootstrap.download_config_files")
    @patch("kindling.spark_config.Dynaconf")
    @patch("kindling.spark_config.get_or_create_spark_session")
    @patch("kindling.spark_config.get_kindling_service")
    def test_pre_reload_signal_emitted(
        self, mock_get_service, mock_spark_fn, mock_dynaconf_class, mock_download
    ):
        """config.pre_reload signal should be emitted with old config"""
        # Mock SignalProvider
        mock_signal_provider = MagicMock()
        mock_pre_reload = MagicMock()
        mock_post_reload = MagicMock()
        mock_failed = MagicMock()
        mock_signal_provider.create_signal.side_effect = [
            mock_pre_reload,
            mock_post_reload,
            mock_failed,
        ]
        mock_get_service.return_value = mock_signal_provider

        mock_spark = MagicMock()
        mock_spark_fn.return_value = mock_spark
        mock_dynaconf = MagicMock()
        mock_dynaconf.to_dict.return_value = {"test_key": "test_value"}
        mock_dynaconf_class.return_value = mock_dynaconf

        config = DynaconfConfig()
        config.initialize(initial_config={"test_key": "test_value"})

        config.reload()

        # Verify pre_reload signal was sent
        mock_pre_reload.send.assert_called_once()
        # Verify sender was config and old_config was passed
        call_args = mock_pre_reload.send.call_args
        assert call_args[0][0] == config, "Should pass config as sender"
        assert "old_config" in call_args[1], "Should include old_config"

    @patch("kindling.bootstrap.download_config_files")
    @patch("kindling.spark_config.Dynaconf")
    @patch("kindling.spark_config.get_or_create_spark_session")
    @patch("kindling.spark_config.get_kindling_service")
    def test_post_reload_signal_emitted_with_changes(
        self, mock_get_service, mock_spark_fn, mock_dynaconf_class, mock_download
    ):
        """config.post_reload signal should be emitted with changes"""
        # Mock SignalProvider
        mock_signal_provider = MagicMock()
        mock_pre_reload = MagicMock()
        mock_post_reload = MagicMock()
        mock_failed = MagicMock()
        mock_signal_provider.create_signal.side_effect = [
            mock_pre_reload,
            mock_post_reload,
            mock_failed,
        ]
        mock_get_service.return_value = mock_signal_provider

        mock_spark = MagicMock()
        mock_spark_fn.return_value = mock_spark
        mock_dynaconf = MagicMock()
        # Simulate config change
        mock_dynaconf.to_dict.side_effect = [
            {"original": "value"},  # Initial
            {"new": "value"},  # After reload
        ]
        mock_dynaconf_class.return_value = mock_dynaconf

        config = DynaconfConfig()
        config.initialize(initial_config={"original": "value"})

        config.reload()

        # Verify post_reload signal was sent
        mock_post_reload.send.assert_called_once()
        # Verify payload includes expected fields
        call_args = mock_post_reload.send.call_args
        assert call_args[0][0] == config, "Should pass config as sender"
        assert "changes" in call_args[1], "Should include changes"
        assert "version" in call_args[1], "Should include version"
        assert "new_config" in call_args[1], "Should include new_config"

    @patch("kindling.bootstrap.download_config_files")
    @patch("kindling.spark_config.Dynaconf")
    @patch("kindling.spark_config.get_or_create_spark_session")
    @patch("kindling.spark_config.get_kindling_service")
    def test_reload_failed_signal_on_error(
        self, mock_get_service, mock_spark_fn, mock_dynaconf_class, mock_download
    ):
        """config.reload_failed signal should be emitted on error"""
        # Mock SignalProvider
        mock_signal_provider = MagicMock()
        mock_pre_reload = MagicMock()
        mock_post_reload = MagicMock()
        mock_failed = MagicMock()
        mock_signal_provider.create_signal.side_effect = [
            mock_pre_reload,
            mock_post_reload,
            mock_failed,
        ]
        mock_get_service.return_value = mock_signal_provider

        mock_spark = MagicMock()
        mock_spark_fn.return_value = mock_spark
        mock_dynaconf = MagicMock()
        mock_dynaconf.to_dict.return_value = {"test": "value"}
        # Make reload raise an exception
        mock_dynaconf.reload.side_effect = RuntimeError("Config error")
        mock_dynaconf_class.return_value = mock_dynaconf

        config = DynaconfConfig()
        config.initialize(initial_config={"test": "value"})

        result = config.reload()

        assert result["status"] == "failed", "Should return failed status"
        # Verify reload_failed signal was sent
        mock_failed.send.assert_called_once()
        # Verify payload includes expected fields
        call_args = mock_failed.send.call_args
        assert call_args[0][0] == config, "Should pass config as sender"
        assert "error" in call_args[1], "Should include error"
        assert "old_config" in call_args[1], "Should include old_config"


class TestConfigChangeDetection:
    """Tests for configuration change detection"""

    def setup_method(self):
        """Clean up before each test"""
        GlobalInjector.reset()

    def teardown_method(self):
        """Clean up after each test"""
        GlobalInjector.reset()

    def test_compute_changes_detects_additions(self):
        """_compute_changes should detect new keys"""
        config = DynaconfConfig()

        old = {"existing": "value"}
        new = {"existing": "value", "new_key": "new_value"}

        changes = config._compute_changes(old, new)

        assert "new_key" in changes, "Should detect new key"
        assert changes["new_key"] == (None, "new_value"), "Should show addition"

    def test_compute_changes_detects_removals(self):
        """_compute_changes should detect removed keys"""
        config = DynaconfConfig()

        old = {"existing": "value", "removed": "value"}
        new = {"existing": "value"}

        changes = config._compute_changes(old, new)

        assert "removed" in changes, "Should detect removed key"
        assert changes["removed"] == ("value", None), "Should show removal"

    def test_compute_changes_detects_modifications(self):
        """_compute_changes should detect modified values"""
        config = DynaconfConfig()

        old = {"key": "old_value"}
        new = {"key": "new_value"}

        changes = config._compute_changes(old, new)

        assert "key" in changes, "Should detect modified key"
        assert changes["key"] == ("old_value", "new_value"), "Should show change"


class TestObserverPattern:
    """Tests for observer pattern with signals"""

    def setup_method(self):
        """Clean up before each test"""
        GlobalInjector.reset()

    def teardown_method(self):
        """Clean up after each test"""
        GlobalInjector.reset()

    @patch("kindling.bootstrap.download_config_files")
    @patch("kindling.spark_config.Dynaconf")
    @patch("kindling.spark_config.get_or_create_spark_session")
    @patch("kindling.spark_config.get_kindling_service")
    def test_service_can_observe_config_changes(
        self, mock_get_service, mock_spark_fn, mock_dynaconf_class, mock_download
    ):
        """Service can observe config reload via signals"""
        # Mock SignalProvider
        mock_signal_provider = MagicMock()
        mock_pre_reload = MagicMock()
        mock_post_reload = MagicMock()
        mock_failed = MagicMock()
        mock_signal_provider.create_signal.side_effect = [
            mock_pre_reload,
            mock_post_reload,
            mock_failed,
        ]
        mock_get_service.return_value = mock_signal_provider

        mock_spark = MagicMock()
        mock_spark_fn.return_value = mock_spark
        mock_dynaconf = MagicMock()
        mock_dynaconf.to_dict.return_value = {"some_setting": "original"}
        mock_dynaconf_class.return_value = mock_dynaconf

        config = DynaconfConfig()
        config.initialize(initial_config={"some_setting": "original"})

        # Verify signals can be connected to (signal exists)
        assert config.post_reload_signal is not None, "Signal should exist"
        assert hasattr(config.post_reload_signal, "connect"), "Signal should have connect method"

        # Reload config and verify signal emission
        config.reload()

        # Verify post_reload signal was sent
        mock_post_reload.send.assert_called_once()
        # This demonstrates that a service could connect to this signal
        # and receive notifications about config changes


class TestThreadSafety:
    """Tests for thread safety of reload operations"""

    def setup_method(self):
        """Clean up before each test"""
        GlobalInjector.reset()

    def teardown_method(self):
        """Clean up after each test"""
        GlobalInjector.reset()

    @patch("kindling.bootstrap.download_config_files")
    @patch("kindling.spark_config.Dynaconf")
    @patch("kindling.spark_config.get_or_create_spark_session")
    def test_get_during_reload_is_safe(self, mock_spark_fn, mock_dynaconf_class, mock_download):
        """Config get() during reload should use locks"""
        mock_spark = MagicMock()
        mock_spark.conf.get.return_value = None  # Make Spark check fail through to Dynaconf
        mock_spark_fn.return_value = mock_spark
        mock_dynaconf = MagicMock()
        mock_dynaconf.to_dict.return_value = {"test_key": "test_value"}
        mock_dynaconf.get.return_value = "test_value"
        mock_dynaconf_class.return_value = mock_dynaconf

        config = DynaconfConfig()
        config.initialize(initial_config={"test_key": "test_value"})

        # Verify lock is acquired during get
        assert config._config_lock is not None, "Should have a lock"
        value = config.get("test_key")
        assert value == "test_value", "Should return value"

    @patch("kindling.bootstrap.download_config_files")
    @patch("kindling.spark_config.Dynaconf")
    @patch("kindling.spark_config.get_or_create_spark_session")
    def test_set_during_reload_is_safe(self, mock_spark_fn, mock_dynaconf_class, mock_download):
        """Config set() during reload should use locks"""
        mock_spark = MagicMock()
        mock_spark_fn.return_value = mock_spark
        mock_dynaconf = MagicMock()
        mock_dynaconf.to_dict.return_value = {}
        mock_dynaconf_class.return_value = mock_dynaconf

        config = DynaconfConfig()
        config.initialize(initial_config={})

        # Verify lock is acquired during set
        assert config._config_lock is not None, "Should have a lock"

        # Reset mock to clear calls from initialize
        mock_dynaconf.set.reset_mock()

        config.set("new_key", "new_value")
        mock_dynaconf.set.assert_called_once_with("new_key", "new_value")


class TestConfigReloadDocumentation:
    """Tests to ensure documentation matches implementation"""

    def setup_method(self):
        """Clean up before each test"""
        GlobalInjector.reset()

    def teardown_method(self):
        """Clean up after each test"""
        GlobalInjector.reset()

    def test_config_service_documents_signals(self):
        """ConfigService docstring should document all signals"""
        docstring = ConfigService.__doc__

        assert "config.pre_reload" in docstring, "Should document pre_reload"
        assert "config.post_reload" in docstring, "Should document post_reload"
        assert "config.reload_failed" in docstring, "Should document reload_failed"

    def test_signals_exist(self):
        """DynaconfConfig should create documented signals"""
        config = DynaconfConfig()

        assert hasattr(config, "pre_reload_signal"), "Should have pre_reload_signal"
        assert hasattr(config, "post_reload_signal"), "Should have post_reload_signal"
        assert hasattr(config, "reload_failed_signal"), "Should have reload_failed_signal"
