"""PyPI publishing guards: the published-package list and the check that an
existing version on the index is never left with different contents."""

import hashlib
import importlib.util
import io
import json
import re
import urllib.error
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location(
    "check_pypi_artifacts", _REPO_ROOT / "scripts" / "check_pypi_artifacts.py"
)
check = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(check)


def test_cli_published_set_matches_publish_script():
    from kindling_cli.cli import _PYPI_PUBLISHED_DISTRIBUTIONS

    script = (_REPO_ROOT / "scripts" / "select_pypi_dists.sh").read_text()
    block = re.search(r"PUBLISHED=\((.*?)\)", script, re.S).group(1)
    names = {name.replace("_", "-") for name in block.split()}
    assert names == set(_PYPI_PUBLISHED_DISTRIBUTIONS)


def _fake_index(monkeypatch, files_by_project):
    def fake_urlopen(url, timeout):
        for (name, version), files in files_by_project.items():
            if url.endswith(f"/pypi/{name}/{version}/json"):
                body = {
                    "urls": [
                        {"filename": fname, "digests": {"sha256": digest}}
                        for fname, digest in files.items()
                    ]
                }
                return io.BytesIO(json.dumps(body).encode())
        raise urllib.error.HTTPError(url, 404, "Not Found", None, None)

    monkeypatch.setattr(check.urllib.request, "urlopen", fake_urlopen)


def test_identical_existing_files_and_new_versions_pass(monkeypatch, tmp_path):
    wheel = tmp_path / "spark_kindling_ext_sdp-0.3.4-py3-none-any.whl"
    wheel.write_bytes(b"same")
    (tmp_path / "spark_kindling-0.14.0.tar.gz").write_bytes(b"new")
    _fake_index(
        monkeypatch,
        {("spark-kindling-ext-sdp", "0.3.4"): {wheel.name: hashlib.sha256(b"same").hexdigest()}},
    )

    assert check.find_mismatches(tmp_path, "https://pypi.org") == []


def test_changed_file_under_existing_version_is_reported(monkeypatch, tmp_path):
    wheel = tmp_path / "spark_kindling_ext_sdp-0.3.4-py3-none-any.whl"
    wheel.write_bytes(b"changed code")
    _fake_index(
        monkeypatch,
        {("spark-kindling-ext-sdp", "0.3.4"): {wheel.name: hashlib.sha256(b"old").hexdigest()}},
    )

    [problem] = check.find_mismatches(tmp_path, "https://pypi.org")
    assert "spark-kindling-ext-sdp 0.3.4 changed without a version bump" in problem


def test_unofficial_name_is_never_pinned_from_pypi(monkeypatch, tmp_path):
    """A GitHub-only package name someone else claimed on PyPI must not be
    installed from PyPI, even with --source pypi."""
    from kindling_cli import cli as cli_module

    (tmp_path / "pyproject.toml").write_text(
        "[project]\nname = 'demo'\nversion = '0.1.0'\ndependencies = []\n", encoding="utf-8"
    )
    commands = []
    monkeypatch.setattr(cli_module, "_published_on_pypi", lambda dist, version: True)
    monkeypatch.setattr(cli_module, "_run_checked", lambda cmd, cwd=None: commands.append(cmd))
    url = "https://example.com/spark_kindling_ext_adx-0.1.0-py3-none-any.whl"

    for source in ("auto", "pypi"):
        commands.clear()
        pinned = cli_module._uv_pin_kindling(
            tmp_path,
            {"distribution": "spark-kindling-ext-adx", "version": "0.1.0", "url": url},
            source=source,
        )
        assert pinned == "github"
        assert commands == [["uv", "add", url]]
