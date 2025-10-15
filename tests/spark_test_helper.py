"""
Helper utilities for testing Kindling with local Spark cluster
"""
import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def _get_azure_cloud_config(cloud):
    """
    Get cloud-specific configuration for Azure storage.

    Args:
        cloud: Cloud identifier ('public', 'government'/'gov', 'china')

    Returns:
        dict with 'name', 'dfs_suffix', 'token_scope', 'login_endpoint'
    """
    cloud = cloud.lower()

    configs = {
        'public': {
            'name': 'Azure Commercial Cloud',
            'dfs_suffix': 'dfs.core.windows.net',
            'token_scope': 'https://storage.azure.com/.default',
            'login_endpoint': 'https://login.microsoftonline.com'
        },
        'government': {
            'name': 'Azure Government Cloud',
            'dfs_suffix': 'dfs.core.usgovcloudapi.net',
            'token_scope': 'https://storage.usgovcloudapi.net/.default',
            'login_endpoint': 'https://login.microsoftonline.us'
        },
        'gov': {  # Alias for government
            'name': 'Azure Government Cloud',
            'dfs_suffix': 'dfs.core.usgovcloudapi.net',
            'token_scope': 'https://storage.usgovcloudapi.net/.default',
            'login_endpoint': 'https://login.microsoftonline.us'
        },
        'china': {
            'name': 'Azure China Cloud',
            'dfs_suffix': 'dfs.core.chinacloudapi.cn',
            'token_scope': 'https://storage.azure.cn/.default',
            'login_endpoint': 'https://login.chinacloudapi.cn'
        }
    }

    if cloud not in configs:
        raise ValueError(
            f"Unknown Azure cloud: {cloud}. "
            f"Valid options: {', '.join(configs.keys())}"
        )

    return configs[cloud]


def get_local_spark_session(app_name="KindlingTest", configure_azure=False, storage_account=None, azure_cloud="public"):
    """
    Create a Spark session connected to the local Spark cluster.
    Falls back to local mode if cluster is unavailable.

    Args:
        app_name: Application name
        configure_azure: If True, configure Azure storage with Azure CLI auth
        storage_account: Azure storage account name (required if configure_azure=True)
        azure_cloud: Azure cloud environment:
            - 'public' (default): Azure Commercial Cloud
            - 'government' or 'gov': Azure Government Cloud
            - 'china': Azure China Cloud
    """
    spark_master = os.getenv("SPARK_MASTER", "local[*]")

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(spark_master)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", "/spark-warehouse")
        # Delta Lake optimizations
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        # Performance tuning for local testing
        .config("spark.executor.memory", "1g")
        .config("spark.driver.memory", "1g")
    )

    # Configure Azure storage if requested
    if configure_azure and storage_account:
        try:
            from azure.identity import DefaultAzureCredential

            # Determine cloud-specific endpoints
            cloud_config = _get_azure_cloud_config(azure_cloud)

            credential = DefaultAzureCredential()
            token = credential.get_token(cloud_config['token_scope'])
            access_token = token.token

            dfs_endpoint = f"{storage_account}.{cloud_config['dfs_suffix']}"
            builder = (
                builder
                .config(f"fs.azure.account.auth.type.{dfs_endpoint}", "OAuth")
                .config(f"fs.azure.account.oauth.provider.type.{dfs_endpoint}",
                        "org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider")
                .config(f"fs.azure.account.oauth2.access.token.{dfs_endpoint}",
                        access_token)
            )
            print(
                f"✓ Configured Azure CLI authentication for {storage_account} ({cloud_config['name']})")
        except ImportError:
            print("⚠ azure-identity not installed. Run: pip install azure-identity")
        except Exception as e:
            print(f"⚠ Azure CLI auth setup failed: {e}")
            print("  Make sure you're logged in with: az login")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Set log level to reduce noise in tests
    spark.sparkContext.setLogLevel("WARN")

    return spark


def get_standalone_spark_session(app_name="KindlingTest"):
    """
    Create a standalone Spark session (no cluster) for unit tests.
    Useful for testing logic without cluster dependencies.
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .config("spark.ui.enabled", "false")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    return spark


def get_local_spark_session_with_azure(
    app_name="KindlingTest",
    storage_account=None,
    auth_type="azure_cli",
    azure_cloud="public",
    **auth_params
):
    """
    Create a Spark session with Azure storage authentication.

    Args:
        app_name: Application name
        storage_account: Azure storage account name
        auth_type: Authentication method:
            - 'azure_cli' (default): Use Azure CLI credentials (az login)
            - 'key': Use storage account key
            - 'oauth': Use service principal (client_id, client_secret, tenant_id)
            - 'sas': Use SAS token
        azure_cloud: Azure cloud environment:
            - 'public' (default): Azure Commercial Cloud (.windows.net)
            - 'government' or 'gov': Azure Government Cloud (.usgovcloudapi.net)
            - 'china': Azure China Cloud (.chinacloudapi.cn)
        **auth_params: Additional auth parameters based on auth_type

    Returns:
        SparkSession configured with Azure storage access

    Examples:
        # Azure Commercial Cloud (default)
        spark = get_local_spark_session_with_azure(
            storage_account="mystorageacct",
            auth_type="azure_cli"
        )

        # Azure Government Cloud
        spark = get_local_spark_session_with_azure(
            storage_account="mygovacct",
            auth_type="azure_cli",
            azure_cloud="government"
        )

        # Service principal in Gov Cloud
        spark = get_local_spark_session_with_azure(
            storage_account="mygovacct",
            auth_type="oauth",
            azure_cloud="gov",
            tenant_id=os.getenv("AZURE_GOV_TENANT_ID"),
            client_id=os.getenv("AZURE_CLIENT_ID"),
            client_secret=os.getenv("AZURE_CLIENT_SECRET")
        )
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.2,org.apache.hadoop:hadoop-azure:3.3.4,org.apache.hadoop:hadoop-azure-datalake:3.3.4")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.executor.memory", "1g")
        .config("spark.driver.memory", "1g")
    )

    if storage_account:
        # Get cloud-specific configuration
        cloud_config = _get_azure_cloud_config(azure_cloud)
        dfs_endpoint = f"{storage_account}.{cloud_config['dfs_suffix']}"

        if auth_type == "azure_cli":
            # Use Azure CLI or DefaultAzureCredential
            try:
                from azure.identity import DefaultAzureCredential, AzureCliCredential
                import subprocess
                import json

                # For Government Cloud, if DefaultAzureCredential fails, try getting token via az CLI directly
                access_token = None
                try:
                    credential = DefaultAzureCredential()
                    token = credential.get_token(cloud_config['token_scope'])
                    access_token = token.token
                except Exception as default_cred_error:
                    if azure_cloud in ['government', 'gov']:
                        # Fallback: Try to get token via az account get-access-token
                        # using management resource which is more commonly available in Gov Cloud
                        print(
                            f"⚠ DefaultAzureCredential failed ({default_cred_error})")
                        print(
                            "  Trying alternate authentication method for Government Cloud...")
                        try:
                            # Try getting a token for the storage resource directly via CLI
                            result = subprocess.run(
                                ['az', 'account', 'get-access-token',
                                    '--resource', 'https://storage.azure.us/'],
                                capture_output=True,
                                text=True,
                                check=True
                            )
                            token_data = json.loads(result.stdout)
                            access_token = token_data['accessToken']
                            print("  ✓ Using Azure CLI token via subprocess")
                        except subprocess.CalledProcessError:
                            # If storage resource fails, try using AzureCliCredential with explicit exclude
                            print(
                                "  Storage resource not available, trying management token...")
                            raise default_cred_error
                    else:
                        raise

                if not access_token:
                    raise ValueError("Failed to obtain access token")

                # Store token and endpoint for post-session configuration
                # We'll set this after the session is created to avoid OAuth provider class issues
                azure_token_config = {
                    'endpoint': dfs_endpoint,
                    'token': access_token
                }

                print(
                    f"✓ Using Azure CLI authentication for {storage_account} ({cloud_config['name']})")
            except ImportError:
                print("⚠ azure-identity not installed. Run: pip install azure-identity")
                raise
            except Exception as e:
                print(f"⚠ Azure CLI auth failed: {e}")
                print("  Make sure you're logged in: az login")
                if azure_cloud != "public":
                    print(
                        f"  For {cloud_config['name']}, use: az cloud set --name {azure_cloud.title()}")
                raise

        elif auth_type == "key":
            account_key = auth_params.get('account_key')
            if not account_key:
                raise ValueError("account_key required for auth_type='key'")
            builder = builder.config(
                f"spark.hadoop.fs.azure.account.key.{dfs_endpoint}", account_key)

        elif auth_type == "oauth":
            tenant_id = auth_params.get('tenant_id')
            client_id = auth_params.get('client_id')
            client_secret = auth_params.get('client_secret')

            if not all([tenant_id, client_id, client_secret]):
                raise ValueError(
                    "tenant_id, client_id, and client_secret required for auth_type='oauth'")

            builder = (
                builder
                .config(f"spark.hadoop.fs.azure.account.auth.type.{dfs_endpoint}", "OAuth")
                .config(f"spark.hadoop.fs.azure.account.oauth.provider.type.{dfs_endpoint}",
                        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
                .config(f"spark.hadoop.fs.azure.account.oauth2.client.id.{dfs_endpoint}", client_id)
                .config(f"spark.hadoop.fs.azure.account.oauth2.client.secret.{dfs_endpoint}", client_secret)
                .config(f"spark.hadoop.fs.azure.account.oauth2.client.endpoint.{dfs_endpoint}",
                        f"{cloud_config['login_endpoint']}/{tenant_id}/oauth2/token")
            )

        elif auth_type == "sas":
            sas_token = auth_params.get('sas_token')
            if not sas_token:
                raise ValueError("sas_token required for auth_type='sas'")
            builder = (
                builder
                .config(f"spark.hadoop.fs.azure.account.auth.type.{dfs_endpoint}", "SAS")
                .config(f"spark.hadoop.fs.azure.sas.token.provider.type.{dfs_endpoint}",
                        "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
                .config(f"spark.hadoop.fs.azure.sas.fixed.token.{dfs_endpoint}", sas_token)
            )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # If azure_cli auth was used, configure the token directly in Hadoop configuration
    if auth_type == "azure_cli" and 'azure_token_config' in locals():
        hadoop_conf = spark._jsc.hadoopConfiguration()
        endpoint = azure_token_config['endpoint']
        token = azure_token_config['token']

        # Use ClientCredsTokenProvider with dummy credentials
        # The token will be cached and used without calling the endpoint
        hadoop_conf.set(f"fs.azure.account.auth.type.{endpoint}", "OAuth")
        hadoop_conf.set(f"fs.azure.account.oauth.provider.type.{endpoint}",
                        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
        # Set Azure CLI client ID and dummy secret (won't be used if token is cached)
        hadoop_conf.set(f"fs.azure.account.oauth2.client.id.{endpoint}",
                        "04b07795-8ddb-461a-bbee-02f9e1bf7b46")
        hadoop_conf.set(f"fs.azure.account.oauth2.client.secret.{endpoint}",
                        "dummy-secret-not-used")
        # Set the OAuth endpoint (may not be called if token is fresh)
        hadoop_conf.set(f"fs.azure.account.oauth2.client.endpoint.{endpoint}",
                        f"https://login.microsoftonline.us/common/oauth2/token")
        # Pre-set the token - this is the key part
        hadoop_conf.set(
            f"fs.azure.account.oauth2.access.token.{endpoint}", token)
        # Set token expiry far in the future so it won't refresh
        import time
        hadoop_conf.set(f"fs.azure.account.oauth2.token.expiry.{endpoint}",
                        str(int(time.time()) + 3600))

        print(f"  ✓ Token configured in Hadoop configuration")

    return spark


def cleanup_test_data(spark, *paths):
    """
    Clean up test data paths using dbutils or direct filesystem operations.
    """
    for path in paths:
        try:
            # Try dbutils first (if available in test environment)
            dbutils.fs.rm(path, recurse=True)
        except NameError:
            # Fall back to Spark's Hadoop filesystem API
            hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
            fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
                spark.sparkContext._jvm.java.net.URI.create(path),
                hadoop_conf
            )
            fs_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)
            fs.delete(fs_path, True)


class SparkTestCase:
    """
    Base class for Spark test cases with automatic session management.
    """

    @classmethod
    def setup_class(cls):
        """Set up Spark session for test class"""
        cls.spark = get_local_spark_session(cls.__name__)

    @classmethod
    def teardown_class(cls):
        """Tear down Spark session after test class"""
        if hasattr(cls, 'spark'):
            cls.spark.stop()

    def cleanup_paths(self, *paths):
        """Clean up test paths"""
        cleanup_test_data(self.spark, *paths)
