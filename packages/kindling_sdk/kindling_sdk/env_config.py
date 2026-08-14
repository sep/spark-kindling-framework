"""Shared CONFIG__ environment-variable-to-nested-config parsing.

Used by both `kindling app run` (so a shell/.env-sourced CONFIG__ variable
can override app config without needing --param on every invocation) and
the system-test harness (tests/system/test_helpers.py), which was the
original home of this convention before it was extracted here for reuse.
"""

from typing import Any, Dict, List

CONFIG_ENV_PREFIX = "CONFIG__"


def coerce_env_config_value(raw_value: str) -> Any:
    """Coerce CONFIG__ env values to primitive Python types when obvious."""
    lowered = raw_value.strip().lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    return raw_value


def get_env_config_overrides(env: Dict[str, str], platform_name: str = "") -> Dict[str, Any]:
    """
    Parse CONFIG__ environment variables into nested config overrides.

    Supports:
    - CONFIG__kindling__temp_path=/...                      (global)
    - CONFIG__platform_databricks__kindling__temp_path=/... (platform-specific,
      only applied when platform_name matches)

    Args:
        env: Environment mapping to read from (typically os.environ)
        platform_name: Current platform, for platform_<name>-scoped entries.
            Platform-scoped entries are dropped entirely if this is empty.

    Returns:
        Nested dict of overrides, "__"-separated key segments as nesting levels.
    """
    platform_prefix = f"platform_{platform_name}" if platform_name else None
    overrides: Dict[str, Any] = {}
    global_entries: List[tuple] = []
    platform_entries: List[tuple] = []

    for env_key, raw_value in env.items():
        if not env_key.startswith(CONFIG_ENV_PREFIX):
            continue

        path = env_key[len(CONFIG_ENV_PREFIX) :]
        parts = [part for part in path.split("__") if part]
        if not parts:
            continue

        value = coerce_env_config_value(raw_value)
        first = parts[0]
        if first.startswith("platform_"):
            if not platform_prefix or first != platform_prefix:
                continue
            scoped_parts = parts[1:]
            if scoped_parts:
                platform_entries.append((scoped_parts, value))
            continue

        global_entries.append((parts, value))

    # Apply global entries first, then matching platform-specific entries
    # so platform overrides are deterministic regardless of env iteration order.
    for parts, value in global_entries + platform_entries:
        current = overrides
        for part in parts[:-1]:
            existing = current.get(part)
            if not isinstance(existing, dict):
                current[part] = {}
            current = current[part]

        current[parts[-1]] = value

    return overrides
