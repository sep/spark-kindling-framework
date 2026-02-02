"""
Custom Dynaconf loaders for Kindling Framework.

This module provides custom loaders that integrate Dynaconf's configuration
system with Kindling's platform-specific services, particularly for secret
management and dynamic value resolution.

Key Features:
- Secret resolution via SecretProvider abstraction (@secret syntax)
- Platform-agnostic secret management (Key Vault, Databricks Secrets, Env Vars)
- JIT (Just-In-Time) secret retrieval respecting config hierarchy
- Integration with Dynaconf's @format interpolation
- Support for nested secrets in complex structures
"""

import logging
from typing import Any, Dict, List, Optional

# Dynaconf imports
from dynaconf.utils.parse_conf import parse_conf_data

logger = logging.getLogger(__name__)


def load_secrets_from_provider(obj, env: str = None, silent: bool = True, key: str = None):
    """
    Custom Dynaconf loader that resolves @secret references via SecretProvider.

    This loader integrates Dynaconf with Kindling's platform-specific SecretProvider,
    enabling unified secret management across:
    - Fabric/Synapse: Azure Key Vault (via mssparkutils)
    - Databricks: Databricks Secrets (via dbutils)
    - Standalone/Local: Environment Variables

    Args:
        obj: Dynaconf settings object
        env: Environment name (e.g., 'development', 'production')
        silent: If True, suppress errors and return original value
        key: Specific key to resolve (None = resolve all)

    Syntax:
        # Simple secret reference
        password: "@secret my-password"

        # Secret in interpolated string (works with @format)
        auth_header: "@format Bearer {@secret api-token}"

        # Secret in nested structure
        database:
          password: "@secret db-password"

    Integration with @format:
        Dynaconf's @format interpolation works seamlessly with @secret.
        The loader resolves secrets recursively, so you can nest them:

        connection_string: "@format {this.host}:{this.port}?password={@secret db-pass}"

    Notes:
        - This loader must run AFTER file loaders but BEFORE env loader
        - Secrets are cached by Dynaconf; use get_fresh() to bypass cache
        - Failed secret resolution returns original value if silent=True
        - SecretProvider is obtained via dependency injection
    """
    try:
        from kindling.injection import get_kindling_service
        from kindling.platform_provider import SecretProvider

        # Get SecretProvider from DI container
        # This will be platform-specific (KeyVault/DbSecrets/EnvVars)
        try:
            secret_provider = get_kindling_service(SecretProvider)
        except Exception as e:
            # SecretProvider not initialized yet (early loading phase)
            # Secrets will be resolved on first access via get_fresh()
            if not silent:
                logger.warning(
                    f"SecretProvider not available during config load: {e}. "
                    "Secrets will be resolved on first access."
                )
            return

    except ImportError as e:
        # Kindling framework not fully loaded
        if not silent:
            logger.error(f"Failed to import Kindling dependencies: {e}")
        return

    def resolve_secret(value: Any, path: str = "") -> Any:
        """
        Recursively resolve @secret references in config values.

        Args:
            value: Config value to process (str, dict, list, or other)
            path: Config path for error messages

        Returns:
            Resolved value with secrets replaced
        """
        if isinstance(value, str):
            # Check for secret reference syntax
            if value.startswith("@secret "):
                # Extract secret name: "@secret my-secret-name"
                secret_name = value.replace("@secret ", "").strip()

                try:
                    # Fetch from platform-specific secret provider
                    resolved = secret_provider.get_secret(secret_name)

                    # Log success (without revealing value)
                    logger.debug(f"Resolved secret '{secret_name}' at {path}")

                    return resolved

                except Exception as e:
                    error_msg = f"Failed to resolve secret '{secret_name}' at {path}: {e}"

                    if not silent:
                        logger.error(error_msg)
                        raise RuntimeError(error_msg)

                    # Return original value if silent mode
                    logger.warning(f"{error_msg}. Returning original value.")
                    return value

            # Not a secret reference - return as-is
            # Note: @format interpolation is handled by Dynaconf's formatter
            return value

        elif isinstance(value, dict):
            # Recursively resolve secrets in nested dicts
            return {
                k: resolve_secret(v, path=f"{path}.{k}" if path else k) for k, v in value.items()
            }

        elif isinstance(value, list):
            # Recursively resolve secrets in lists
            return [resolve_secret(item, path=f"{path}[{i}]") for i, item in enumerate(value)]

        # Other types (int, bool, None, etc.) - return as-is
        return value

    # Resolve secrets in settings
    if key:
        # Specific key requested - resolve just that key
        if key in obj.store:
            original_value = obj.get(key)
            resolved_value = resolve_secret(original_value, path=key)

            # Update settings object with resolved value
            if resolved_value != original_value:
                obj.set(key, resolved_value)
                logger.debug(f"Updated '{key}' with resolved secrets")

    else:
        # Resolve all keys in current environment
        # Note: obj.store contains all settings for current environment
        for setting_key in list(obj.store.keys()):
            original_value = obj.get(setting_key)
            resolved_value = resolve_secret(original_value, path=setting_key)

            # Update only if value changed (optimization)
            if resolved_value != original_value:
                obj.set(setting_key, resolved_value)
                logger.debug(f"Updated '{setting_key}' with resolved secrets")


def register_kindling_loaders():
    """
    Register Kindling-specific Dynaconf loaders.

    This function sets up the loader chain to include our custom secret
    resolver. It must be called during bootstrap BEFORE creating the
    Dynaconf instance.

    Loader Chain (in order):
        1. File loaders (YAML, TOML, JSON) - load base config
        2. Secret loader (ours) - resolve @secret references
        3. Env loader - override with environment variables

    Usage:
        # In bootstrap.py
        from kindling.config_loaders import register_kindling_loaders

        register_kindling_loaders()
        config = Dynaconf(...)  # Will use registered loaders

    Environment Variable:
        Sets LOADERS_FOR_DYNACONF to control loader chain.
        Can be overridden by setting this variable before calling.
    """
    import os

    # Check if loaders already configured
    existing_loaders = os.environ.get("LOADERS_FOR_DYNACONF", "")

    if not existing_loaders:
        # Default loader chain with our secret resolver
        loaders = [
            "dynaconf.loaders.yaml_loader",  # Load .yaml files
            "dynaconf.loaders.toml_loader",  # Load .toml files
            "dynaconf.loaders.json_loader",  # Load .json files
            "kindling.config_loaders.load_secrets_from_provider",  # ← Our custom loader
            "dynaconf.loaders.env_loader",  # Override with env vars (highest priority)
        ]

        os.environ["LOADERS_FOR_DYNACONF"] = ",".join(loaders)
        logger.info("Registered Kindling custom loaders for Dynaconf")

    else:
        # Loaders already configured - check if ours is included
        loaders = existing_loaders.split(",")

        if "kindling.config_loaders.load_secrets_from_provider" not in loaders:
            # Insert our loader before env_loader (if it exists)
            try:
                env_loader_idx = loaders.index("dynaconf.loaders.env_loader")
                loaders.insert(env_loader_idx, "kindling.config_loaders.load_secrets_from_provider")

                os.environ["LOADERS_FOR_DYNACONF"] = ",".join(loaders)
                logger.info("Added Kindling secret loader to existing loader chain")

            except ValueError:
                # env_loader not in chain - append our loader at the end
                loaders.append("kindling.config_loaders.load_secrets_from_provider")
                os.environ["LOADERS_FOR_DYNACONF"] = ",".join(loaders)
                logger.info("Appended Kindling secret loader to loader chain")

        else:
            logger.debug("Kindling secret loader already registered")


def unregister_kindling_loaders():
    """
    Remove Kindling custom loaders from Dynaconf loader chain.

    This is primarily for testing scenarios where you want to reset
    the loader configuration to defaults.

    Usage:
        # In test teardown
        from kindling.config_loaders import unregister_kindling_loaders
        unregister_kindling_loaders()
    """
    import os

    existing_loaders = os.environ.get("LOADERS_FOR_DYNACONF", "")

    if existing_loaders:
        loaders = existing_loaders.split(",")
        loaders = [loader for loader in loaders if "kindling.config_loaders" not in loader]

        if loaders:
            os.environ["LOADERS_FOR_DYNACONF"] = ",".join(loaders)
        else:
            # No loaders left - remove env var
            os.environ.pop("LOADERS_FOR_DYNACONF", None)

        logger.info("Unregistered Kindling custom loaders")


# Module-level exports
__all__ = [
    "load_secrets_from_provider",
    "register_kindling_loaders",
    "unregister_kindling_loaders",
]
