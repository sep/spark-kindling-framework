"""Shared app artifact selection rules."""

from pathlib import PurePosixPath
from typing import Optional

APP_TEXT_FILE_NAMES = {"requirements.txt", "lake-reqs.txt"}
APP_TEXT_FILE_SUFFIXES = {".py", ".yaml", ".yml"}
APP_MANIFEST_FILE = "app.yaml"
APP_SETTINGS_FILE = "settings.yaml"
LOCAL_SETTINGS_FILE = "settings.local.yaml"
LOCAL_SETTINGS_FILES = {LOCAL_SETTINGS_FILE, "settings.local.yml"}


def is_settings_overlay(filename: str) -> bool:
    """Return True for app-local dot-style settings overlays."""
    return (
        filename not in {APP_SETTINGS_FILE, *LOCAL_SETTINGS_FILES}
        and filename.startswith("settings.")
        and filename.endswith((".yaml", ".yml"))
    )


def is_selected_settings_overlay(
    filename: str,
    *,
    platform: Optional[str] = None,
    environment: Optional[str] = None,
) -> bool:
    """Return True when a settings overlay matches the selected runtime target."""
    return filename in {
        f"settings.{platform}.yaml" if platform else "",
        f"settings.{environment}.yaml" if environment else "",
    }


def is_deployable_app_file(
    path: str,
    *,
    platform: Optional[str] = None,
    environment: Optional[str] = None,
) -> bool:
    """Return True when an app artifact should be deployed as text."""
    file_path = PurePosixPath(path.replace("\\", "/"))
    if file_path.name in LOCAL_SETTINGS_FILES:
        return False
    if (
        file_path.name.startswith("app.")
        and file_path.name != APP_MANIFEST_FILE
        and file_path.suffix.lower() in {".yaml", ".yml"}
    ):
        return False
    if is_settings_overlay(file_path.name):
        return is_selected_settings_overlay(
            file_path.name,
            platform=platform,
            environment=environment,
        )
    return (
        file_path.name in APP_TEXT_FILE_NAMES or file_path.suffix.lower() in APP_TEXT_FILE_SUFFIXES
    )
