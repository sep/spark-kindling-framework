# Secret Provider Service - Platform-Specific Secret Management

**Status:** Proposal
**Created:** 2026-02-02
**Related:** config_based_entity_providers.md, platform_api_architecture.md

## Executive Summary

This proposal introduces a **platform-specific secret provider service** that abstracts secret/credential management across Fabric, Synapse, and Databricks platforms.

**Key Requirements:**
1. **Platform-specific implementations** - Each platform has different secret storage (Key Vault, Databricks Secrets, etc.)
2. **Unified interface** - Framework code uses consistent API regardless of platform
3. **Configuration-driven** - Secret scope/vault configured via settings, not hardcoded
4. **Entity provider integration** - Connection strings, API keys, database passwords for external providers

**Platform Secret APIs:**

| Platform | Secret Store | API Pattern |
|----------|-------------|-------------|
| **Synapse** | Azure Key Vault | `mssparkutils.credentials.getSecret(vault_url, secret_name)` |
| **Fabric** | Azure Key Vault | `mssparkutils.credentials.getSecret(vault_url, secret_name)` |
| **Databricks** | Databricks Secrets | `dbutils.secrets.get(scope, key)` |
| **Standalone/Local** | Environment Variables | `os.environ.get(key)` |

## Problem Statement

### Use Cases Requiring Secrets

1. **External Entity Providers:**
   ```python
   # Snowflake provider needs connection credentials
   class SnowflakeEntityProvider(EntityProvider):
       def __init__(self, entity, config, secret_provider):
           # Need: username, password, account, database
           password = secret_provider.get_secret("snowflake-password")
   ```

2. **Storage Account Keys:**
   ```python
   # Accessing external storage accounts
   storage_key = secret_provider.get_secret("external-storage-account-key")
   ```

3. **API Tokens:**
   ```python
   # REST API provider needs authentication
   api_token = secret_provider.get_secret("external-api-token")
   ```

4. **Database Connections:**
   ```python
   # JDBC connections need credentials
   db_password = secret_provider.get_secret("postgres-password")
   ```

### Current State - No Abstraction

Currently, framework code would need platform checks:
```python
# ❌ BAD: Platform-specific code everywhere
if platform == "databricks":
    password = dbutils.secrets.get("my-scope", "db-password")
elif platform == "fabric" or platform == "synapse":
    password = mssparkutils.credentials.getSecret(
        "https://my-vault.vault.azure.net",
        "db-password"
    )
else:
    password = os.environ.get("DB_PASSWORD")
```

## Proposed Solution

### SecretProvider Interface

```python
from abc import ABC, abstractmethod
from typing import Optional

class SecretProvider(ABC):
    """
    Abstract interface for platform-specific secret management.

    Implementations handle retrieving secrets from:
    - Azure Key Vault (Fabric/Synapse)
    - Databricks Secrets (Databricks)
    - Environment variables (Standalone/Local)
    """

    @abstractmethod
    def get_secret(self, secret_name: str, default: Optional[str] = None) -> str:
        """
        Retrieve a secret by name.

        Args:
            secret_name: Name/key of the secret to retrieve
            default: Default value if secret not found (optional)

        Returns:
            Secret value as string

        Raises:
            KeyError: If secret not found and no default provided
            ValueError: If secret name is invalid
        """
        pass

    @abstractmethod
    def list_secrets(self) -> list[str]:
        """
        List available secret names (metadata only, not values).

        Returns:
            List of secret names/keys

        Note: Some platforms may not support listing secrets
        """
        pass

    @abstractmethod
    def secret_exists(self, secret_name: str) -> bool:
        """
        Check if a secret exists without retrieving its value.

        Args:
            secret_name: Name/key of the secret to check

        Returns:
            True if secret exists, False otherwise
        """
        pass
```

### Platform Implementations

#### Synapse/Fabric (Azure Key Vault)

```python
from kindling.platform_provider import SecretProvider
from kindling.spark_config import ConfigService
from kindling.spark_log_provider import PythonLoggerProvider

class AzureKeyVaultSecretProvider(SecretProvider):
    """
    Secret provider for Synapse/Fabric using Azure Key Vault.

    Uses mssparkutils.credentials.getSecret() API.
    """

    def __init__(
        self,
        config: ConfigService,
        logger_provider: PythonLoggerProvider
    ):
        self.config = config
        self.logger = logger_provider.get_logger("AzureKeyVaultSecretProvider")

        # Get Key Vault URL from configuration
        self.vault_url = self.config.get(
            "KEY_VAULT_URL",
            required=True,
            error_msg="KEY_VAULT_URL must be configured for secret access"
        )

        # Validate vault URL format
        if not self.vault_url.startswith("https://"):
            raise ValueError(f"Invalid Key Vault URL: {self.vault_url}")

        if not self.vault_url.endswith(".vault.azure.net"):
            self.logger.warning(
                f"Key Vault URL may be invalid: {self.vault_url}. "
                "Expected format: https://<vault-name>.vault.azure.net"
            )

        # Get mssparkutils
        try:
            from __main__ import mssparkutils
            self.mssparkutils = mssparkutils
        except ImportError:
            try:
                from notebookutils import mssparkutils
                self.mssparkutils = mssparkutils
            except ImportError:
                raise RuntimeError(
                    "mssparkutils not available. "
                    "This provider requires Fabric or Synapse runtime."
                )

    def get_secret(self, secret_name: str, default: Optional[str] = None) -> str:
        """Retrieve secret from Azure Key Vault"""
        try:
            self.logger.debug(f"Retrieving secret '{secret_name}' from Key Vault")

            # Call Key Vault API
            secret_value = self.mssparkutils.credentials.getSecret(
                self.vault_url,
                secret_name
            )

            if not secret_value:
                if default is not None:
                    self.logger.warning(
                        f"Secret '{secret_name}' is empty, using default"
                    )
                    return default
                raise KeyError(f"Secret '{secret_name}' is empty")

            self.logger.debug(f"Successfully retrieved secret '{secret_name}'")
            return secret_value

        except Exception as e:
            if default is not None:
                self.logger.warning(
                    f"Failed to retrieve secret '{secret_name}': {e}. "
                    "Using default value."
                )
                return default

            self.logger.error(
                f"Failed to retrieve secret '{secret_name}' from Key Vault: {e}"
            )
            raise KeyError(f"Secret '{secret_name}' not found: {e}")

    def secret_exists(self, secret_name: str) -> bool:
        """Check if secret exists (try to retrieve it)"""
        try:
            self.get_secret(secret_name)
            return True
        except KeyError:
            return False

    def list_secrets(self) -> list[str]:
        """
        List secrets in Key Vault.

        Note: mssparkutils.credentials does not provide a list API.
        Would need to use Azure SDK directly for this functionality.
        """
        self.logger.warning(
            "list_secrets() not supported by mssparkutils.credentials API. "
            "Returning empty list."
        )
        return []
```

#### Databricks (Databricks Secrets)

```python
class DatabricksSecretProvider(SecretProvider):
    """
    Secret provider for Databricks using Databricks Secrets.

    Uses dbutils.secrets.get() API.
    """

    def __init__(
        self,
        config: ConfigService,
        logger_provider: PythonLoggerProvider
    ):
        self.config = config
        self.logger = logger_provider.get_logger("DatabricksSecretProvider")

        # Get secrets scope from configuration
        self.scope = self.config.get(
            "DATABRICKS_SECRET_SCOPE",
            default="kindling-secrets"  # Default scope name
        )

        self.logger.info(f"Using Databricks secret scope: {self.scope}")

        # Get dbutils (injected into global namespace)
        import __main__
        self.dbutils = getattr(__main__, "dbutils", None)

        if self.dbutils is None:
            raise RuntimeError(
                "dbutils not available. "
                "This provider requires Databricks runtime."
            )

    def get_secret(self, secret_name: str, default: Optional[str] = None) -> str:
        """Retrieve secret from Databricks Secrets"""
        try:
            self.logger.debug(
                f"Retrieving secret '{secret_name}' from scope '{self.scope}'"
            )

            # Call Databricks Secrets API
            secret_value = self.dbutils.secrets.get(
                scope=self.scope,
                key=secret_name
            )

            # Note: Databricks returns "[REDACTED]" in notebook output for security
            # But the actual value is available in code

            if not secret_value:
                if default is not None:
                    self.logger.warning(
                        f"Secret '{secret_name}' is empty, using default"
                    )
                    return default
                raise KeyError(f"Secret '{secret_name}' is empty")

            self.logger.debug(f"Successfully retrieved secret '{secret_name}'")
            return secret_value

        except Exception as e:
            if default is not None:
                self.logger.warning(
                    f"Failed to retrieve secret '{secret_name}': {e}. "
                    "Using default value."
                )
                return default

            self.logger.error(
                f"Failed to retrieve secret '{secret_name}' "
                f"from scope '{self.scope}': {e}"
            )
            raise KeyError(
                f"Secret '{secret_name}' not found in scope '{self.scope}': {e}"
            )

    def secret_exists(self, secret_name: str) -> bool:
        """Check if secret exists"""
        try:
            self.get_secret(secret_name)
            return True
        except KeyError:
            return False

    def list_secrets(self) -> list[str]:
        """List secrets in the configured scope"""
        try:
            # Databricks provides a list API
            secrets_list = self.dbutils.secrets.list(scope=self.scope)
            return [secret.key for secret in secrets_list]
        except Exception as e:
            self.logger.error(f"Failed to list secrets in scope '{self.scope}': {e}")
            return []
```

#### Standalone/Local (Environment Variables)

```python
import os

class EnvironmentVariableSecretProvider(SecretProvider):
    """
    Secret provider for standalone/local development using environment variables.

    Supports:
    - Direct environment variables (SECRET_NAME)
    - Prefixed environment variables (KINDLING_SECRET_NAME)
    - .env file loading
    """

    def __init__(
        self,
        config: ConfigService,
        logger_provider: PythonLoggerProvider
    ):
        self.config = config
        self.logger = logger_provider.get_logger("EnvironmentVariableSecretProvider")

        # Get optional environment variable prefix
        self.prefix = self.config.get("SECRET_PREFIX", default="KINDLING_")

        self.logger.info(
            f"Using environment variables with prefix '{self.prefix}' for secrets"
        )

        # Optionally load .env file
        env_file = self.config.get("SECRET_ENV_FILE", default=None)
        if env_file:
            self._load_env_file(env_file)

    def _load_env_file(self, env_file: str):
        """Load secrets from .env file"""
        try:
            from dotenv import load_dotenv
            load_dotenv(env_file)
            self.logger.info(f"Loaded secrets from {env_file}")
        except ImportError:
            self.logger.warning(
                "python-dotenv not installed. "
                "Install with: pip install python-dotenv"
            )
        except Exception as e:
            self.logger.error(f"Failed to load .env file {env_file}: {e}")

    def _get_env_key(self, secret_name: str) -> str:
        """Get environment variable key with prefix"""
        # Try prefixed version first
        prefixed_key = f"{self.prefix}{secret_name.upper().replace('-', '_')}"

        if prefixed_key in os.environ:
            return prefixed_key

        # Fall back to unprefixed version
        direct_key = secret_name.upper().replace('-', '_')
        return direct_key

    def get_secret(self, secret_name: str, default: Optional[str] = None) -> str:
        """Retrieve secret from environment variables"""
        env_key = self._get_env_key(secret_name)

        secret_value = os.environ.get(env_key)

        if secret_value is None:
            if default is not None:
                self.logger.debug(
                    f"Secret '{secret_name}' (env: {env_key}) not found, "
                    "using default"
                )
                return default

            self.logger.error(
                f"Secret '{secret_name}' not found in environment. "
                f"Expected environment variable: {env_key}"
            )
            raise KeyError(
                f"Secret '{secret_name}' not found. "
                f"Set environment variable: {env_key}"
            )

        self.logger.debug(f"Retrieved secret '{secret_name}' from environment")
        return secret_value

    def secret_exists(self, secret_name: str) -> bool:
        """Check if secret exists in environment"""
        env_key = self._get_env_key(secret_name)
        return env_key in os.environ

    def list_secrets(self) -> list[str]:
        """List all secrets (prefixed environment variables)"""
        if not self.prefix:
            # Can't list all env vars - too many non-secrets
            return []

        # List only prefixed variables
        secrets = []
        for key in os.environ:
            if key.startswith(self.prefix):
                # Remove prefix and convert back to secret name format
                secret_name = key[len(self.prefix):].lower().replace('_', '-')
                secrets.append(secret_name)

        return secrets
```

### Dependency Injection Integration

```python
from kindling.injection import GlobalInjector
from kindling.platform_provider import SecretProvider

# Register in platform service initialization
def initialize_platform_services(platform_name: str):
    """Initialize platform-specific services including secrets"""

    if platform_name == "fabric" or platform_name == "synapse":
        # Azure Key Vault for Fabric/Synapse
        secret_provider = AzureKeyVaultSecretProvider(
            config=GlobalInjector.get(ConfigService),
            logger_provider=GlobalInjector.get(PythonLoggerProvider)
        )
    elif platform_name == "databricks":
        # Databricks Secrets
        secret_provider = DatabricksSecretProvider(
            config=GlobalInjector.get(ConfigService),
            logger_provider=GlobalInjector.get(PythonLoggerProvider)
        )
    else:
        # Standalone/Local - environment variables
        secret_provider = EnvironmentVariableSecretProvider(
            config=GlobalInjector.get(ConfigService),
            logger_provider=GlobalInjector.get(PythonLoggerProvider)
        )

    # Register as singleton
    GlobalInjector.register(SecretProvider, secret_provider)
```

### Configuration

```yaml
# Fabric/Synapse configuration
kindling:
  secrets:
    key_vault_url: https://my-vault.vault.azure.net
    # Optional: Key Vault linked via Fabric/Synapse workspace

# Databricks configuration
kindling:
  secrets:
    databricks_secret_scope: production-secrets
    # Scope must be created in Databricks workspace

# Standalone/Local configuration
kindling:
  secrets:
    secret_prefix: KINDLING_  # Optional prefix for env vars
    secret_env_file: .env      # Optional .env file path
```

### Usage in Entity Providers

```python
from kindling.platform_provider import SecretProvider

class SnowflakeEntityProvider(EntityProvider):
    """External entity provider requiring secrets"""

    @inject
    def __init__(
        self,
        entity: DataEntity,
        config: ConfigService,
        secret_provider: SecretProvider,  # ← Injected secret provider
        logger_provider: PythonLoggerProvider
    ):
        super().__init__(entity, config)
        self.secret_provider = secret_provider
        self.logger = logger_provider.get_logger("SnowflakeEntityProvider")

        # Get connection details from configuration
        self.account = self.config.get("SNOWFLAKE_ACCOUNT", required=True)
        self.database = self.config.get("SNOWFLAKE_DATABASE", required=True)
        self.warehouse = self.config.get("SNOWFLAKE_WAREHOUSE", required=True)

        # Get credentials from secrets
        self.username = self.secret_provider.get_secret("snowflake-username")
        self.password = self.secret_provider.get_secret("snowflake-password")

        self._connection = None

    def _get_connection(self):
        """Create Snowflake connection with secrets"""
        if self._connection is None:
            import snowflake.connector

            self._connection = snowflake.connector.connect(
                account=self.account,
                user=self.username,
                password=self.password,  # From secret provider
                database=self.database,
                warehouse=self.warehouse
            )

        return self._connection

    def read(self) -> DataFrame:
        """Read from Snowflake using authenticated connection"""
        query = self.tags.get("query", f"SELECT * FROM {self.entity.name}")

        conn = self._get_connection()
        # Use Spark-Snowflake connector with credentials
        # ...
```

### Usage in Configuration

```python
# Entity configuration with external provider
@data_entity(
    name="external.customer_master",
    tags={
        "provider": "snowflake",
        "table": "CUSTOMERS",
        # Secrets referenced by name (not values!)
        "username_secret": "snowflake-username",
        "password_secret": "snowflake-password"
    }
)
class ExternalCustomers:
    pass
```

## Security Considerations

### Secret Values Never in Logs

```python
class SecretProvider(ABC):
    """Base class ensures safe logging"""

    def get_secret(self, secret_name: str, default: Optional[str] = None) -> str:
        secret_value = self._get_secret_impl(secret_name, default)

        # ❌ NEVER log the actual secret value
        # ✅ Only log that retrieval succeeded
        self.logger.debug(f"Retrieved secret '{secret_name}' (value redacted)")

        return secret_value
```

### Secret Values Never in Configuration Files

```yaml
# ❌ BAD: Secret in config file
kindling:
  snowflake:
    password: "my-secret-password"  # NEVER DO THIS

# ✅ GOOD: Secret name/reference in config
kindling:
  snowflake:
    password_secret: "snowflake-password"  # Reference to secret name
```

### Least Privilege Access

- **Key Vault**: Grant workspace/service principal only READ access to specific secrets
- **Databricks Secrets**: Use scopes to limit access per user/job
- **Environment Variables**: Restrict access to deployment environments

## Implementation Phases

### Phase 1: Core Interface (1 day)
- [ ] Define `SecretProvider` ABC in `platform_provider.py`
- [ ] Add to PlatformAPI interface (optional methods)
- [ ] Unit tests for interface contract

### Phase 2: Platform Implementations (3 days)
- [ ] Implement `AzureKeyVaultSecretProvider` (Fabric/Synapse)
- [ ] Implement `DatabricksSecretProvider` (Databricks)
- [ ] Implement `EnvironmentVariableSecretProvider` (Standalone/Local)
- [ ] Unit tests with mocked platform APIs
- [ ] Integration tests with actual Key Vault/Secrets

### Phase 3: DI Integration (1 day)
- [ ] Add `SecretProvider` registration to platform initialization
- [ ] Update bootstrap to configure secret providers
- [ ] Configuration validation for secret settings

### Phase 4: Documentation & Examples (1 day)
- [ ] Usage guide for each platform
- [ ] External entity provider example (Snowflake)
- [ ] Security best practices documentation
- [ ] Configuration examples

**Total Estimated Time:** 1 week

## Benefits

✅ **Platform abstraction** - Framework code works on any platform
✅ **Secure by default** - Secrets never in logs or config files
✅ **Configuration-driven** - Secret scope/vault via settings, not code
✅ **Enables external providers** - Snowflake, APIs, databases can use secrets
✅ **Consistent API** - Same code works with Key Vault or Databricks Secrets
✅ **Local development** - Environment variables for dev/test
✅ **Testable** - Mock SecretProvider in unit tests

---

## Integration with Dynaconf - Secret References & Interpolation

### Overview

Dynaconf (Kindling's config library) has **built-in support** for:
1. **Secret references** - `@secret` syntax with custom loaders
2. **Variable interpolation** - `@format {this.path.to.key}` for config references
3. **JIT resolution** - Secrets fetched when accessed, not at load time
4. **Custom loaders** - Easy to add platform-specific secret backends

**This means we can provide a seamless config-driven secret experience!**

### Secret References in Configuration

Instead of calling `secret_provider.get_secret()` in code, reference secrets directly in YAML:

```yaml
# settings.yaml
snowflake:
  account: "myaccount"
  database: "mydb"
  warehouse: "compute_wh"
  username: "svc_user"
  # Secret reference - resolved JIT via SecretProvider
  password: "@secret snowflake-password"

external_api:
  base_url: "https://api.example.com"
  # Secret in interpolated string
  auth_header: "@format Bearer {@secret api-token}"

storage_account:
  name: "mystorageaccount"
  # Platform-specific secret resolution
  access_key: "@secret storage-account-key"
```

**Usage in code:**
```python
# Secrets are resolved automatically when accessed
config = get_kindling_service(ConfigService)

# This triggers JIT secret retrieval via custom loader
password = config.get("snowflake.password")  # Returns actual password from vault

# Interpolation works seamlessly
auth_header = config.get("external_api.auth_header")  # Returns "Bearer <token>"
```

### Custom Dynaconf Loaders for Platform-Specific Secrets

Create loaders that integrate with our `SecretProvider` abstraction:

```python
# kindling/config_loaders.py

from typing import Any, Optional
from dynaconf.utils.parse_conf import parse_conf_data

def load_secrets_from_provider(obj, env, silent, key):
    """
    Custom Dynaconf loader that resolves @secret references via SecretProvider.

    Syntax: @secret secret-name

    This loader integrates Dynaconf with Kindling's platform-specific SecretProvider,
    enabling unified secret management across Fabric (Key Vault), Databricks (Secrets),
    and Standalone (Environment Variables).
    """
    from kindling.platform_provider import SecretProvider
    from kindling.injection import get_kindling_service

    try:
        secret_provider = get_kindling_service(SecretProvider)
    except Exception:
        # SecretProvider not initialized yet (early loading)
        # Secrets will be resolved on first access
        return

    def resolve_secret(value: Any) -> Any:
        """Recursively resolve @secret references in config values"""
        if isinstance(value, str):
            if value.startswith("@secret "):
                # Extract secret name: "@secret my-secret-name"
                secret_name = value.replace("@secret ", "").strip()

                try:
                    # Fetch from platform-specific secret provider
                    return secret_provider.get_secret(secret_name)
                except Exception as e:
                    if not silent:
                        raise RuntimeError(
                            f"Failed to resolve secret '{secret_name}': {e}"
                        )
                    return value

            # Handle interpolated secrets: "@format Bearer {@secret token}"
            # Dynaconf's @format will call this recursively for nested @secret
            return value

        elif isinstance(value, dict):
            # Recursively resolve secrets in nested dicts
            return {k: resolve_secret(v) for k, v in value.items()}

        elif isinstance(value, list):
            # Recursively resolve secrets in lists
            return [resolve_secret(item) for item in value]

        return value

    # If specific key requested, resolve just that key
    if key:
        if key in obj.store:
            obj.set(key, resolve_secret(obj.get(key)))
    else:
        # Resolve all keys in current environment
        for setting_key in list(obj.store.keys()):
            value = obj.get(setting_key)
            resolved_value = resolve_secret(value)
            obj.set(setting_key, resolved_value)


def register_kindling_loaders():
    """
    Register Kindling-specific Dynaconf loaders.

    Call this during bootstrap to enable @secret resolution in config files.
    """
    import os

    # Add our secret loader to the loader chain
    # It must run AFTER file loaders but BEFORE env loader
    loaders = os.environ.get('LOADERS_FOR_DYNACONF', '')

    if not loaders:
        # Default loader chain with our secret resolver
        loaders = [
            'dynaconf.loaders.yaml_loader',
            'dynaconf.loaders.toml_loader',
            'dynaconf.loaders.json_loader',
            'kindling.config_loaders.load_secrets_from_provider',  # ← Our loader
            'dynaconf.loaders.env_loader',  # Env vars override secrets
        ]
    else:
        # Insert our loader before env_loader
        loaders = loaders.split(',')
        env_loader_idx = loaders.index('dynaconf.loaders.env_loader')
        loaders.insert(env_loader_idx, 'kindling.config_loaders.load_secrets_from_provider')

    os.environ['LOADERS_FOR_DYNACONF'] = ','.join(loaders)
```

### Variable Interpolation for Config References

Dynaconf's `@format` syntax allows referencing other config values:

```yaml
# settings.yaml - Base configuration
kindling:
  storage:
    base_path: "/mnt/data"
    bronze_base: "@format {this.kindling.storage.base_path}/bronze"
    silver_base: "@format {this.kindling.storage.base_path}/silver"
    gold_base: "@format {this.kindling.storage.base_path}/gold"

  # Entity provider path patterns using interpolation
  entity_providers:
    delta:
      # {layer} resolved at runtime based on entity name
      path_pattern: "@format {this.kindling.storage.{layer}_base}/{entity_name}"

    csv:
      path_pattern: "@format {this.kindling.storage.bronze_base}/csv/{entity_name}.csv"

# platform_fabric.yaml - Override for Fabric
kindling:
  storage:
    base_path: "Files"  # Override base path
    # bronze_base auto-resolves to "Files/bronze" via interpolation

# platform_databricks.yaml - Override for Databricks
kindling:
  storage:
    base_path: "/dbfs/mnt/datalake"
    # bronze_base auto-resolves to "/dbfs/mnt/datalake/bronze"
```

**Result:** Platform-specific paths automatically resolved via hierarchical config!

### Complete Configuration Example

```yaml
# settings.yaml - Base configuration with secrets and interpolation
kindling:
  # Storage configuration with interpolation
  storage:
    account: "mystorageaccount"
    container: "data"
    base_path: "@format abfss://{this.kindling.storage.container}@{this.kindling.storage.account}.dfs.core.windows.net"
    bronze_path: "@format {this.kindling.storage.base_path}/bronze"
    silver_path: "@format {this.kindling.storage.base_path}/silver"
    gold_path: "@format {this.kindling.storage.base_path}/gold"

    # Secret reference for storage key
    access_key: "@secret storage-account-key"

  # Snowflake external provider with secrets
  snowflake:
    account: "mycompany"
    region: "us-east-1"
    database: "analytics"
    warehouse: "compute_wh"
    schema: "public"

    # Interpolated connection string
    connection_string: "@format {this.kindling.snowflake.account}.{this.kindling.snowflake.region}.snowflakecomputing.com"

    # Secrets for credentials
    username: "@secret snowflake-username"
    password: "@secret snowflake-password"

  # REST API integration
  external_api:
    base_url: "https://api.example.com"
    version: "v2"

    # Interpolated endpoint
    endpoint: "@format {this.kindling.external_api.base_url}/{this.kindling.external_api.version}"

    # Secret in Authorization header
    auth_header: "@format Bearer {@secret api-token}"

    # Timeout configuration (no secrets)
    timeout_seconds: 30
    retry_count: 3

# platform_fabric.yaml - Fabric-specific overrides
kindling:
  storage:
    # Override to use Fabric OneLake paths
    base_path: "Files"
    bronze_path: "Files/bronze"
    silver_path: "Files/silver"
    gold_path: "Files/gold"

  secrets:
    # Fabric uses Azure Key Vault
    key_vault_url: "https://my-fabric-vault.vault.azure.net"

# platform_databricks.yaml - Databricks-specific overrides
kindling:
  storage:
    # Override to use DBFS paths
    base_path: "/mnt/datalake"
    bronze_path: "/mnt/datalake/bronze"
    silver_path: "/mnt/datalake/silver"
    gold_path: "/mnt/datalake/gold"

  secrets:
    # Databricks uses Databricks Secrets
    databricks_secret_scope: "production-secrets"

# env_dev.yaml - Development environment overrides
kindling:
  storage:
    # Dev uses local paths
    base_path: "./local_data"
    bronze_path: "./local_data/bronze"
    silver_path: "./local_data/silver"
    gold_path: "./local_data/gold"

  # Dev secrets from environment variables
  secrets:
    secret_prefix: "DEV_"
```

### Usage in Entity Providers

```python
class SnowflakeEntityProvider(EntityProvider):
    """External entity provider using config with secrets"""

    @inject
    def __init__(
        self,
        entity: DataEntity,
        config: ConfigService,
        logger_provider: PythonLoggerProvider
    ):
        super().__init__(entity, config)
        self.logger = logger_provider.get_logger("SnowflakeEntityProvider")

        # Get connection details from config
        # Secrets are automatically resolved by Dynaconf loader!
        self.connection_string = self.config.get("kindling.snowflake.connection_string")
        self.database = self.config.get("kindling.snowflake.database")
        self.warehouse = self.config.get("kindling.snowflake.warehouse")

        # These are actual secret values (not "@secret ..." references)
        # because the custom loader already resolved them
        self.username = self.config.get("kindling.snowflake.username")
        self.password = self.config.get("kindling.snowflake.password")

        self.logger.info(
            f"Connecting to Snowflake: {self.connection_string}"
            # Password is NOT logged (logger filters sensitive keys)
        )

    def _get_connection(self):
        """Create Snowflake connection with config-resolved secrets"""
        import snowflake.connector

        return snowflake.connector.connect(
            account=self.connection_string.split('.')[0],
            user=self.username,
            password=self.password,  # ← Already resolved from secret store
            database=self.database,
            warehouse=self.warehouse
        )
```

### Bootstrap Integration

Update bootstrap to register custom loaders:

```python
# kindling/bootstrap.py

def initialize_config_with_secrets(
    config_files: List[str],
    initial_config: Dict[str, Any]
) -> ConfigService:
    """Initialize config service with secret resolution support"""

    # Register Kindling's custom loaders BEFORE creating Dynaconf instance
    from kindling.config_loaders import register_kindling_loaders
    register_kindling_loaders()

    # Create config service (uses registered loaders)
    config_service = DynaconfConfig()
    config_service.initialize(
        config_files=config_files,
        initial_config=initial_config,
        environment=initial_config.get("environment", "development")
    )

    return config_service
```

### Advanced Features

#### Fresh Secret Retrieval

Force re-fetch of secrets (bypass cache):

```python
# Get fresh secret value from Key Vault/Secrets store
password = config.get_fresh("snowflake.password")

# Or use fresh context
with config.fresh():
    # All secrets re-fetched within this context
    password = config.get("snowflake.password")
```

#### Secret Validation

Validate that required secrets are present:

```python
from dynaconf import Validator

settings = Dynaconf(
    validators=[
        # Ensure required secrets are configured
        Validator("kindling.snowflake.username", must_exist=True),
        Validator("kindling.snowflake.password", must_exist=True),
        Validator("kindling.storage.access_key", must_exist=True),

        # Validate secret format
        Validator("kindling.external_api.auth_header", must_exist=True,
                  startswith="Bearer "),
    ]
)

# Validate on startup
settings.validators.validate()
```

#### Conditional Secret Loading

Use Dynaconf hooks for conditional logic:

```python
# dynaconf_hooks.py

def post(settings):
    """Post-load hook for conditional secret resolution"""

    # Only load expensive secrets in production
    if settings.get("ENV_FOR_DYNACONF") == "production":
        # Trigger secret resolution for production secrets
        _ = settings.get("kindling.snowflake.password")
        _ = settings.get("kindling.storage.access_key")

    return {}
```

## Benefits of Dynaconf Integration

✅ **Declarative Secrets** - Define secret references in YAML, not code
✅ **JIT Resolution** - Secrets fetched when accessed, respects config hierarchy
✅ **Config Interpolation** - Reference other config values with `@format`
✅ **Platform Abstraction** - Same config syntax across Fabric/Databricks/Local
✅ **Hierarchical Overrides** - Platform/workspace/environment override secrets
✅ **Type Safety** - Dynaconf handles type casting automatically
✅ **Fresh Values** - `get_fresh()` for cache-busting secret retrieval
✅ **Validation** - Built-in validators for required secrets
✅ **Custom Loaders** - Easy to extend for new secret backends
✅ **Security** - Secrets never written to logs or exported configs

## Implementation Phases

### Phase 1: Custom Loader (1 day)
- [ ] Implement `load_secrets_from_provider()` loader
- [ ] Register loader in bootstrap
- [ ] Unit tests with mock SecretProvider

### Phase 2: Config Examples (1 day)
- [ ] Update example configs to use `@secret` syntax
- [ ] Add interpolation examples for path patterns
- [ ] Document secret reference patterns

### Phase 3: Entity Provider Updates (2 days)
- [ ] Update Snowflake example to use config secrets
- [ ] Add REST API provider example
- [ ] Update documentation

### Phase 4: Validation & Testing (1 day)
- [ ] Add secret validators
- [ ] Integration tests with real Key Vault/Secrets
- [ ] Security audit (ensure no secrets in logs)

**Total Estimated Time:** 1 week

---

## Related Patterns

This follows the same platform service pattern as:
- **Storage Operations**: `platform.copy()`, `platform.exists()`
- **Job Deployment**: `platform.deploy_spark_job()`
- **Token Authentication**: `platform.get_token()`

Secret management is another cross-cutting platform concern that needs proper abstraction.

**Integration with Dynaconf** provides:
- Configuration-driven secret references (`@secret`)
- Variable interpolation (`@format {this.path.to.key}`)
- Platform-specific secret backends via custom loaders
- JIT secret resolution with caching and refresh
