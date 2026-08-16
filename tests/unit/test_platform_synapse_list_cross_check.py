"""Unit tests for SynapseService.list() ADLS SDK cross-check.

Regression: the Synapse system test test_lake_wheel_bfs failed because
_download_lake_wheels' BFS walk (kindling/data_apps.py) trusted
SynapseService.list()'s mssparkutils.fs.ls() result as the authoritative
listing of available lake wheels. mssparkutils.fs.ls() can return a stale
listing for a just-uploaded blob (ABFS driver caching / Blob -> ADLS
propagation lag), so a transitive dependency wheel uploaded moments earlier
was silently missing from the listing and the BFS walk treated it as "not
in the lake" and skipped it.

DatabricksService.list() was already hardened against exactly this failure
mode (commit e73eea8, "add BFS listing cross-check") by cross-checking a
non-empty ABFS listing against the ADLS Gen2 SDK and preferring the SDK
result when the counts disagree. That fix was never ported to
SynapseService.list(), which is why Databricks-UC and Fabric passed this
system test while Synapse did not. These tests cover the ported fix.
"""

from unittest.mock import MagicMock

from kindling.platform_synapse import SynapseService


def _service():
    svc = SynapseService.__new__(SynapseService)
    svc.logger = MagicMock()
    return svc


def _fake_file(name):
    f = MagicMock()
    f.name = name
    return f


def _fake_path_item(name, is_directory=False):
    item = MagicMock()
    item.name = name
    item.is_directory = is_directory
    return item


class TestSynapseListAdlsCrossCheck:
    def test_stale_mssparkutils_listing_is_replaced_by_sdk_result(self, monkeypatch):
        """mssparkutils.fs.ls() misses a recently-uploaded wheel; SDK sees it."""
        svc = _service()

        mssparkutils = MagicMock()
        mssparkutils.fs.ls.return_value = [_fake_file("test_lake_dep_a-1.0.0-py3-none-any.whl")]
        monkeypatch.setattr("kindling.platform_synapse._get_mssparkutils", lambda: mssparkutils)

        fs_client = MagicMock()
        fs_client.get_paths.return_value = [
            _fake_path_item("packages/test_lake_dep_a-1.0.0-py3-none-any.whl"),
            _fake_path_item("packages/test_lake_dep_b-1.0.0-py3-none-any.whl"),
        ]
        svc._adls_fs_client = MagicMock(return_value=fs_client)

        result = svc.list("abfss://artifacts@acct.dfs.core.windows.net/packages/")

        assert set(result) == {
            "test_lake_dep_a-1.0.0-py3-none-any.whl",
            "test_lake_dep_b-1.0.0-py3-none-any.whl",
        }
        svc.logger.warning.assert_called_once()

    def test_agreeing_listing_is_returned_without_sdk_override(self, monkeypatch):
        """When mssparkutils and the SDK agree, no override and no warning."""
        svc = _service()

        mssparkutils = MagicMock()
        mssparkutils.fs.ls.return_value = [_fake_file("only_pkg-1.0.0-py3-none-any.whl")]
        monkeypatch.setattr("kindling.platform_synapse._get_mssparkutils", lambda: mssparkutils)

        fs_client = MagicMock()
        fs_client.get_paths.return_value = [
            _fake_path_item("packages/only_pkg-1.0.0-py3-none-any.whl"),
        ]
        svc._adls_fs_client = MagicMock(return_value=fs_client)

        result = svc.list("abfss://artifacts@acct.dfs.core.windows.net/packages/")

        assert result == ["only_pkg-1.0.0-py3-none-any.whl"]
        svc.logger.warning.assert_not_called()

    def test_non_abfss_path_skips_cross_check(self, monkeypatch):
        """Cross-check only applies to abfss:// paths — local/dbfs-style paths pass through."""
        svc = _service()

        mssparkutils = MagicMock()
        mssparkutils.fs.ls.return_value = [_fake_file("local_pkg.whl")]
        monkeypatch.setattr("kindling.platform_synapse._get_mssparkutils", lambda: mssparkutils)
        svc._adls_fs_client = MagicMock(
            side_effect=AssertionError("SDK cross-check must not run for non-abfss paths")
        )

        result = svc.list("/local/packages/")

        assert result == ["local_pkg.whl"]

    def test_sdk_cross_check_failure_is_non_fatal(self, monkeypatch):
        """If the SDK cross-check itself errors, fall back to the mssparkutils result."""
        svc = _service()

        mssparkutils = MagicMock()
        mssparkutils.fs.ls.return_value = [_fake_file("pkg-1.0.0-py3-none-any.whl")]
        monkeypatch.setattr("kindling.platform_synapse._get_mssparkutils", lambda: mssparkutils)
        svc._adls_fs_client = MagicMock(side_effect=Exception("auth failed"))

        result = svc.list("abfss://artifacts@acct.dfs.core.windows.net/packages/")

        assert result == ["pkg-1.0.0-py3-none-any.whl"]
