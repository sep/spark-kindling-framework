"""
Kindling Framework - Secret Validation Examples

This module demonstrates how to validate configuration with secret references
using Dynaconf's built-in validator system integrated with Kindling's
platform-specific SecretProvider.

Key Features Demonstrated:
1. Required secret validation (must_exist)
2. Secret format validation (startswith, endswith, condition)
3. Conditional secret validation (when)
4. Environment-specific secret validation
5. Combined validators with operators (|, &)
6. Custom error messages for secret validation failures
7. Fresh secret retrieval (cache-busting)
8. Secret validation only in specific environments
"""

from dynaconf import Dynaconf, Validator
from kindling.config_loaders import register_kindling_loaders


def create_settings_with_secret_validation():
    """
    Example: Create Dynaconf settings with comprehensive secret validation.

    This shows how to ensure all required secrets are present and properly
    formatted before the application starts.
    """

    # Register Kindling's custom loaders BEFORE creating Dynaconf instance
    # This enables @secret resolution via platform-specific SecretProvider
    register_kindling_loaders()

    settings = Dynaconf(
        settings_files=["settings.yaml", ".secrets.yaml"],
        environments=True,
        envvar_prefix="KINDLING",
        # Validate secrets on instantiation
        validators=[
            # ================================================================
            # Storage Secrets - Required for all environments
            # ================================================================
            Validator(
                "kindling.storage.access_key",
                must_exist=True,
                messages={
                    "must_exist_true": "Storage access key is required. Set @secret storage-account-key in config."
                },
            ),
            # ================================================================
            # Snowflake Secrets - Required when Snowflake provider is enabled
            # ================================================================
            Validator(
                "kindling.snowflake.username",
                "kindling.snowflake.password",
                must_exist=True,
                when=Validator("kindling.entity_providers.snowflake.enabled", eq=True),
                messages={
                    "must_exist_true": "Snowflake credentials required when provider is enabled. Set @secret snowflake-username and @secret snowflake-password."
                },
            ),
            # Validate Snowflake connection string format
            Validator(
                "kindling.snowflake.connection_string",
                must_exist=True,
                condition=lambda v: ".snowflakecomputing.com" in v,
                when=Validator("kindling.entity_providers.snowflake.enabled", eq=True),
                messages={
                    "condition": "Invalid Snowflake connection string: {value}. Must contain .snowflakecomputing.com"
                },
            ),
            # ================================================================
            # API Secrets - Format validation for auth headers
            # ================================================================
            Validator(
                "kindling.external_api.auth_header",
                must_exist=True,
                startswith="Bearer ",
                messages={
                    "must_exist_true": "API authentication required. Set @secret api-token in config.",
                    "operations": "API auth header must start with 'Bearer ' but got: {value}",
                },
            ),
            # ================================================================
            # Database Connection Secrets
            # ================================================================
            Validator(
                "kindling.postgres.username",
                "kindling.postgres.password",
                must_exist=True,
                messages={
                    "must_exist_true": "PostgreSQL credentials required. Set @secret postgres-username and @secret postgres-password."
                },
            ),
            # Validate connection URL contains credentials
            Validator(
                "kindling.postgres.connection_url",
                must_exist=True,
                condition=lambda v: v.startswith("postgresql://") and "@" in v,
                messages={"condition": "Invalid PostgreSQL connection URL format: {value}"},
            ),
            # ================================================================
            # Telemetry Secrets - Required only in production
            # ================================================================
            Validator(
                "kindling.telemetry.app_insights.instrumentation_key",
                must_exist=True,
                env="production",
                messages={
                    "must_exist_true": "Application Insights key required in production. Set @secret app-insights-key."
                },
            ),
            Validator(
                "kindling.telemetry.metrics_endpoint.api_key",
                must_exist=True,
                env="production",
                messages={
                    "must_exist_true": "Metrics API key required in production. Set @secret metrics-api-key."
                },
            ),
            # ================================================================
            # Platform-Specific Secret Provider Configuration
            # ================================================================
            # Fabric/Synapse must have Key Vault URL configured
            Validator(
                "kindling.secrets.key_vault_url",
                must_exist=True,
                startswith="https://",
                endswith=".vault.azure.net",
                when=Validator("kindling.secrets.provider_type", is_in=["azure_keyvault"]),
                messages={
                    "must_exist_true": "Azure Key Vault URL required for Fabric/Synapse platforms.",
                    "operations": "Key Vault URL must be valid Azure URL: https://<vault>.vault.azure.net",
                },
            ),
            # Databricks must have secret scope configured
            Validator(
                "kindling.secrets.secret_scope",
                must_exist=True,
                len_min=1,
                when=Validator("kindling.secrets.provider_type", eq="databricks_secrets"),
                messages={
                    "must_exist_true": "Databricks secret scope required for Databricks platform."
                },
            ),
            # ================================================================
            # Combined Validators - Flexible secret configuration
            # ================================================================
            # Either storage key OR managed identity must be configured
            (
                Validator("kindling.storage.access_key", must_exist=True)
                | Validator("kindling.storage.use_managed_identity", eq=True)
            ),
            # If using external auth, must have BOTH username AND password
            (
                Validator("kindling.external_api.username", must_exist=True)
                & Validator("kindling.external_api.password", must_exist=True)
            ),
        ],
        # Validate only settings in current environment
        validate_only_current_env=True,
        # Trigger validation on config updates
        validate_on_update=True,
    )

    return settings


def validate_secrets_manually():
    """
    Example: Manual secret validation after settings creation.

    This pattern is useful when you want to defer validation until
    after the application is initialized, or when you want to validate
    only specific sections of configuration.
    """

    # Register custom loaders
    register_kindling_loaders()

    # Create settings without automatic validation
    settings = Dynaconf(
        settings_files=["settings.yaml"],
        environments=True,
    )

    # Register validators manually
    settings.validators.register(
        # Storage secrets
        Validator("kindling.storage.access_key", must_exist=True),
        # External provider secrets
        Validator(
            "kindling.snowflake.username",
            "kindling.snowflake.password",
            must_exist=True,
            when=Validator("kindling.entity_providers.snowflake.enabled", eq=True),
        ),
    )

    # Trigger validation manually
    try:
        # Raises ValidationError on first error found
        settings.validators.validate()
        print("✅ All secret validations passed!")

    except Exception as e:
        print(f"❌ Secret validation failed: {e}")
        return False

    return True


def validate_all_secrets_with_accumulation():
    """
    Example: Validate all secrets and collect all errors.

    This pattern is useful during configuration audits where you want
    to see ALL validation failures at once, not just the first one.
    """

    register_kindling_loaders()

    settings = Dynaconf(
        settings_files=["settings.yaml"],
        environments=True,
    )

    # Register validators
    settings.validators.register(
        Validator("kindling.storage.access_key", must_exist=True),
        Validator("kindling.snowflake.username", must_exist=True),
        Validator("kindling.snowflake.password", must_exist=True),
        Validator("kindling.external_api.auth_header", must_exist=True, startswith="Bearer "),
    )

    # Validate all and accumulate errors
    try:
        # Raises after evaluating ALL validators
        settings.validators.validate_all()
        print("✅ All secret validations passed!")

    except Exception as e:
        # Get accumulated errors
        print(f"❌ Secret validation failed with {len(e.details)} errors:")
        for error in e.details:
            print(f"  - {error}")

        return False

    return True


def validate_secrets_selectively():
    """
    Example: Validate only specific sections of configuration.

    This is useful for incremental validation when different parts
    of your application initialize at different times.
    """

    register_kindling_loaders()

    settings = Dynaconf(
        settings_files=["settings.yaml"],
        environments=True,
    )

    # Register all validators
    settings.validators.register(
        # Storage validators
        Validator("kindling.storage.access_key", must_exist=True),
        # Snowflake validators
        Validator("kindling.snowflake.username", must_exist=True),
        Validator("kindling.snowflake.password", must_exist=True),
        # API validators
        Validator("kindling.external_api.auth_header", must_exist=True),
    )

    # Validate only storage secrets first
    print("Validating storage secrets...")
    settings.validators.validate(only=["kindling.storage"])
    print("✅ Storage secrets valid")

    # Later, validate Snowflake secrets when that provider initializes
    print("Validating Snowflake secrets...")
    settings.validators.validate(only=["kindling.snowflake"])
    print("✅ Snowflake secrets valid")

    # Skip API validation if not using external API
    print("Validating API secrets (excluding external_api)...")
    settings.validators.validate(exclude=["kindling.external_api"])
    print("✅ Selected secrets valid")


def get_fresh_secrets():
    """
    Example: Force fresh secret retrieval (bypass cache).

    Use this when you need to ensure you have the latest secret value
    from the secret store (Key Vault, Databricks Secrets, etc.)
    """

    register_kindling_loaders()

    settings = Dynaconf(
        settings_files=["settings.yaml"],
        environments=True,
    )

    # Normal access - may return cached secret
    password = settings.kindling.snowflake.password
    print(f"Cached password length: {len(password)}")

    # Force fresh retrieval - bypasses Dynaconf cache
    fresh_password = settings.get_fresh("kindling.snowflake.password")
    print(f"Fresh password length: {len(fresh_password)}")

    # Use fresh context for multiple secrets
    with settings.fresh():
        # All secrets re-fetched within this context
        storage_key = settings.kindling.storage.access_key
        api_token = settings.kindling.external_api.auth_header
        db_password = settings.kindling.postgres.password

        print("✅ Retrieved fresh values for all secrets")


def validate_secret_defaults():
    """
    Example: Provide default values for optional secrets.

    This pattern allows secrets to be optional in development
    while required in production.
    """

    register_kindling_loaders()

    settings = Dynaconf(
        settings_files=["settings.yaml"],
        environments=True,
        validators=[
            # Required in production, default in development
            Validator(
                "kindling.telemetry.app_insights.instrumentation_key",
                must_exist=True,
                env="production",
            ),
            Validator(
                "kindling.telemetry.app_insights.instrumentation_key",
                default="dev-mock-key-12345",
                env="development",
            ),
            # Computed default based on other config
            Validator(
                "kindling.storage.connection_string",
                default=lambda settings, validator: (
                    f"DefaultEndpointsProtocol=https;"
                    f"AccountName={settings.kindling.storage.account};"
                    f"AccountKey={settings.kindling.storage.access_key};"
                    f"EndpointSuffix=core.windows.net"
                ),
            ),
        ],
    )

    return settings


def custom_secret_validation():
    """
    Example: Custom validation logic for secrets.

    This shows how to implement complex validation rules
    that go beyond Dynaconf's built-in validators.
    """

    def validate_jwt_token(value):
        """Ensure value looks like a JWT token"""
        parts = value.split(".")
        return len(parts) == 3 and all(part for part in parts)

    def validate_connection_string(value):
        """Ensure connection string has required components"""
        required_parts = ["AccountName=", "AccountKey=", "EndpointSuffix="]
        return all(part in value for part in required_parts)

    def validate_snowflake_password_strength(value):
        """Ensure password meets Snowflake requirements"""
        # At least 8 chars, contains uppercase, lowercase, and digit
        return (
            len(value) >= 8
            and any(c.isupper() for c in value)
            and any(c.islower() for c in value)
            and any(c.isdigit() for c in value)
        )

    register_kindling_loaders()

    settings = Dynaconf(
        settings_files=["settings.yaml"],
        environments=True,
        validators=[
            # JWT token validation
            Validator(
                "kindling.external_api.auth_header",
                must_exist=True,
                condition=validate_jwt_token,
                messages={"condition": "API token must be a valid JWT (3 parts separated by dots)"},
            ),
            # Storage connection string validation
            Validator(
                "kindling.storage.connection_string",
                condition=validate_connection_string,
                messages={"condition": "Storage connection string missing required components"},
            ),
            # Password strength validation (production only)
            Validator(
                "kindling.snowflake.password",
                condition=validate_snowflake_password_strength,
                env="production",
                messages={
                    "condition": "Snowflake password does not meet strength requirements (8+ chars, uppercase, lowercase, digit)"
                },
            ),
        ],
    )

    return settings


if __name__ == "__main__":
    """
    Run all validation examples to demonstrate patterns.
    """

    print("=" * 70)
    print("Kindling Framework - Secret Validation Examples")
    print("=" * 70)

    print("\n1. Creating settings with automatic validation...")
    try:
        settings = create_settings_with_secret_validation()
        print("✅ Settings created and validated")
    except Exception as e:
        print(f"❌ Validation failed: {e}")

    print("\n2. Manual validation...")
    success = validate_secrets_manually()
    print(f"Result: {'✅ Success' if success else '❌ Failed'}")

    print("\n3. Validate all with error accumulation...")
    success = validate_all_secrets_with_accumulation()
    print(f"Result: {'✅ Success' if success else '❌ Failed'}")

    print("\n4. Selective validation...")
    try:
        validate_secrets_selectively()
        print("✅ Selective validation passed")
    except Exception as e:
        print(f"❌ Selective validation failed: {e}")

    print("\n5. Fresh secret retrieval...")
    try:
        get_fresh_secrets()
        print("✅ Fresh secrets retrieved")
    except Exception as e:
        print(f"❌ Fresh retrieval failed: {e}")

    print("\n6. Secret validation with defaults...")
    try:
        settings = validate_secret_defaults()
        print("✅ Defaults applied")
    except Exception as e:
        print(f"❌ Default validation failed: {e}")

    print("\n7. Custom secret validation...")
    try:
        settings = custom_secret_validation()
        print("✅ Custom validation passed")
    except Exception as e:
        print(f"❌ Custom validation failed: {e}")

    print("\n" + "=" * 70)
    print("All examples completed!")
    print("=" * 70)
