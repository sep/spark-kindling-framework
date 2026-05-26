"""Shared app artifact selection rules."""

from pathlib import PurePosixPath

APP_TEXT_FILE_NAMES = {"requirements.txt", "lake-reqs.txt"}
APP_TEXT_FILE_SUFFIXES = {".py", ".yaml", ".yml"}


def is_deployable_app_file(path: str) -> bool:
    """Return True when an app artifact should be deployed as text."""
    file_path = PurePosixPath(path.replace("\\", "/"))
    return (
        file_path.name in APP_TEXT_FILE_NAMES or file_path.suffix.lower() in APP_TEXT_FILE_SUFFIXES
    )
