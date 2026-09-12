"""
Helper utilities for testing Kindling with local Spark cluster

This module provides testing utilities that assume environment variables
are properly configured by the test runner or development environment.

For interactive environment setup, use: python scripts/init_azure_dev.sh
"""

import json
import os
import subprocess
import sys
from typing import Optional

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def check_required_env_vars(required_vars=None, check_env=True):
    """
    Check for required environment variables without running setup wizards.
    This is for testing utilities that assume proper environment configuration.

    Args:
        required_vars: List of required environment variable names
        check_env: Whether to actually check (for backward compatibility)

    Returns:
        bool: True if all required variables are set, False otherwise
    """
    if not check_env:
        return True

    if required_vars is None:
        required_vars = [
            "AZURE_STORAGE_ACCOUNT",
            "AZURE_CONTAINER",
            "AZURE_BASE_PATH",
            "AZURE_CLOUD",
        ]

    missing_vars = [var for var in required_vars if not os.getenv(var)]

    if missing_vars:
        print(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
        print("   Please configure these variables in your environment")
        print("   For development setup, run: python scripts/init_azure_dev.sh")
        return False

    return True


def _worker_scoped_ivy_dir():
    """Give each pytest-xdist worker process its own Ivy cache directory.

    configure_spark_with_delta_pip only sets spark.jars.packages and relies on
    Spark's built-in Ivy resolver, which otherwise defaults to one shared
    cache (~/.ivy2.5.2/cache) for every process. Concurrent xdist worker
    processes resolving/writing to that same cache at session-startup time
    can race, leaving a corrupt/incomplete delta-spark jar that then fails to
    load DeltaCatalog (ClassNotFoundException) for whichever test happens to
    hit that worker's Spark session next.
    """
    worker = os.getenv("PYTEST_XDIST_WORKER", "master")
    ivy_dir = os.path.join(os.path.expanduser("~"), ".ivy2-test-workers", worker)
    os.makedirs(ivy_dir, exist_ok=True)
    return ivy_dir


def _get_azure_cloud_config(cloud):
    """
    Get cloud-specific configuration for Azure storage.

    Args:
        cloud: Cloud identifier (AZURE_PUBLIC_CLOUD, AZURE_US_GOVERNMENT, AZURE_CHINA_CLOUD, or legacy format)

    Returns:
        dict with 'name', 'dfs_suffix', 'token_scope', 'login_endpoint'
    """
    # Handle both new Azure CLI format and legacy format
    cloud_normalized = cloud.upper() if cloud else "AZURE_PUBLIC_CLOUD"

    # Map legacy format to Azure CLI format
    legacy_mapping = {
        "PUBLIC": "AZURE_PUBLIC_CLOUD",
        "GOVERNMENT": "AZURE_US_GOVERNMENT",
        "GOV": "AZURE_US_GOVERNMENT",
        "CHINA": "AZURE_CHINA_CLOUD",
    }

    if cloud_normalized in legacy_mapping:
        cloud_normalized = legacy_mapping[cloud_normalized]

    configs = {
        "AZURE_PUBLIC_CLOUD": {
            "name": "Azure Commercial Cloud",
            "dfs_suffix": "dfs.core.windows.net",
            "token_scope": "https://storage.azure.com/.default",
            "login_endpoint": "https://login.microsoftonline.com",
        },
        "AZURE_US_GOVERNMENT": {
            "name": "Azure Government Cloud",
            "dfs_suffix": "dfs.core.usgovcloudapi.net",
            "token_scope": "https://storage.usgovcloudapi.net/.default",
            "login_endpoint": "https://login.microsoftonline.us",
        },
        "AZURE_CHINA_CLOUD": {
            "name": "Azure China Cloud",
            "dfs_suffix": "dfs.core.chinacloudapi.cn",
            "token_scope": "https://storage.azure.cn/.default",
            "login_endpoint": "https://login.chinacloudapi.cn",
        },
    }

    if cloud_normalized not in configs:
        raise ValueError(
            f"Unknown Azure cloud: {cloud}. " f"Valid options: {', '.join(configs.keys())}"
        )

    return configs[cloud_normalized]


def get_local_spark_session(
    app_name="KindlingTest",
    configure_azure=False,
    storage_account=None,
    azure_cloud="public",
    check_env=True,
):
    """
    Create a Spark session connected to the local Spark cluster.
    Falls back to local mode if cluster is unavailable.

    Args:
        app_name: Application name
        configure_azure: If True, configure Azure storage with Azure CLI auth
        storage_account: Azure storage account name (will use AZURE_STORAGE_ACCOUNT env var if not provided)
        azure_cloud: Azure cloud environment (will use AZURE_CLOUD env var if not provided):
            - 'public' (default): Azure Commercial Cloud
            - 'government' or 'gov': Azure Government Cloud
            - 'china': Azure China Cloud
        check_env: Whether to check for required environment variables when configure_azure=True
    """
    # Check environment if Azure is being configured
    if configure_azure and check_env:
        required_vars = ["AZURE_STORAGE_ACCOUNT"] if not storage_account else []
        if not check_required_env_vars(required_vars, check_env=True):
            print("\n⚠️  Required environment variables not set for Azure configuration.")
            print("   Tests expect environment to be properly configured by test runner.")
            raise EnvironmentError("Required Azure environment variables not configured")

    # Use environment variables as defaults
    if configure_azure:
        storage_account = storage_account or os.getenv("AZURE_STORAGE_ACCOUNT")
        azure_cloud = (
            azure_cloud
            if azure_cloud != "public"
            else os.getenv("AZURE_CLOUD", "AZURE_PUBLIC_CLOUD")
        )

    spark_master = os.getenv("SPARK_MASTER", "local[*]")

    builder = (
        SparkSession.builder.appName(app_name)
        .master(spark_master)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .config("spark.jars.ivy", _worker_scoped_ivy_dir())
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
            token = credential.get_token(cloud_config["token_scope"])
            access_token = token.token

            dfs_endpoint = f"{storage_account}.{cloud_config['dfs_suffix']}"
            builder = (
                builder.config(f"fs.azure.account.auth.type.{dfs_endpoint}", "OAuth")
                .config(
                    f"fs.azure.account.oauth.provider.type.{dfs_endpoint}",
                    "org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider",
                )
                .config(f"fs.azure.account.oauth2.access.token.{dfs_endpoint}", access_token)
            )
            print(
                f"✓ Configured Azure CLI authentication for {storage_account} ({cloud_config['name']})"
            )
        except ImportError:
            print("⚠ azure-identity not installed. Run: pip install azure-identity")
        except Exception as e:
            print(f"⚠ Azure CLI auth setup failed: {e}")
            print("  Make sure you're logged in with: az login")

    # Same preflight as get_standalone_spark_session: this is the path behind
    # the shared conftest ``spark_session`` fixture, so a plain JVM launched by
    # an earlier module must be relaunched here too or Delta fails at first use.
    _teardown_non_delta_jvm()
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Set log level to reduce noise in tests
    spark.sparkContext.setLogLevel("WARN")

    return spark


_DELTA_EXTENSION = "io.delta.sql.DeltaSparkSessionExtension"
_DELTA_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog"


class ExternalGatewayUnusable(RuntimeError):
    """Raised when the py4j gateway was started outside this process
    (``PYSPARK_GATEWAY_PORT``) and is proven unable to serve Delta, or is dead.
    We do not own such a gateway, so the helper can neither relaunch it nor
    hand back a session that would fail at first Delta use."""


def _active_session_or_none():
    """``SparkSession.getActiveSession()`` that treats a failing lookup as
    "no usable session". The lookup itself is a JVM call and raises a Py4J
    error when ``_active_spark_context`` still points at a dead gateway; that
    state must flow into teardown, not abort before it."""
    try:
        return SparkSession.getActiveSession()
    except Exception:  # noqa: BLE001
        return None


def _clear_stale_session_globals() -> None:
    """Drop every process-level reference that would hand back a stopped or
    dead session.

    - ``SparkContext._active_spark_context`` and ``SparkSession``'s
      ``_instantiatedSession`` / ``_activeSession``: ``getOrCreate()`` returns
      the cached session while ``_sc._jsc is not None``, which a dead gateway
      never clears because ``stop()`` could not run.
    - ``__main__.spark``: ``kindling.spark_session.get_or_create_spark_session``
      returns that global before consulting Spark at all.
    """
    import __main__
    from pyspark import SparkContext

    try:
        with SparkContext._lock:
            SparkContext._active_spark_context = None
    except Exception:  # noqa: BLE001
        pass
    for attr in ("_instantiatedSession", "_activeSession"):
        try:
            setattr(SparkSession, attr, None)
        except Exception:  # noqa: BLE001
            pass
    if getattr(__main__, "spark", None) is not None:
        try:
            delattr(__main__, "spark")
        except Exception:  # noqa: BLE001
            pass


def _teardown_existing_spark_jvm() -> None:
    """Stop the active session (if any), shut down the py4j gateway and END the
    JVM, so the next ``getOrCreate()`` launches a NEW one.

    Spark's JVM is a process-wide singleton and ``spark.jars.packages`` (what
    ``configure_spark_with_delta_pip`` adds) is honoured only at gateway
    launch; this is the only way to get Delta onto the classpath once some
    earlier module has launched a plain JVM. Callers go through
    :func:`_teardown_non_delta_jvm`, which only invokes this for a jar-less or
    dead gateway that this process launched.

    ``gateway.shutdown()`` alone does NOT end the JVM: PySpark launches it with
    a stdin pipe and the Java side exits on EOF of that pipe, which only
    happens when the Python process dies. Without closing it every relaunch
    leaves a ~1 GB orphan behind, and with four xdist workers each relaunching
    several times the CI runner is OOM-killed (exit 137). So the launcher
    process is reaped here: stdin closed, then waited on, killed if it lingers.
    """
    from pyspark import SparkContext

    active = _active_session_or_none()
    if active is not None:
        try:
            active.stop()
        except Exception:  # noqa: BLE001
            pass
    _clear_stale_session_globals()
    gateway = SparkContext._gateway
    if gateway is not None:
        proc = getattr(gateway, "proc", None)
        try:
            gateway.shutdown()
        except Exception:  # noqa: BLE001
            pass
        SparkContext._gateway = None
        SparkContext._jvm = None
        _reap_gateway_process(proc)


def _reap_gateway_process(proc, timeout: float = 30.0) -> None:
    """End the JVM launched by ``launch_gateway`` (see teardown docstring)."""
    if proc is None or proc.poll() is not None:
        return
    try:
        if proc.stdin is not None:
            proc.stdin.close()
    except Exception:  # noqa: BLE001
        pass
    try:
        proc.wait(timeout=timeout)
    except Exception:  # noqa: BLE001
        try:
            proc.kill()
            proc.wait(timeout=10)
        except Exception:  # noqa: BLE001
            pass


def _gateway_launched_with_delta() -> Optional[bool]:
    """Whether the live py4j gateway's JVM was launched with the Delta jars.

    Session-independent and launch-truthful: PySpark starts the JVM through
    ``spark-submit`` with ``spark.jars.packages`` on the command line, and
    ``--packages`` jars are on that JVM's classpath for its whole life -- a
    SparkContext created later in the same JVM (after a ``stop()``) still has
    them, even though its own ``listJars()`` may not say so. Returns None when
    there is no gateway, or an externally started one whose command line is
    not visible (``PYSPARK_GATEWAY_PORT``).
    """
    from pyspark import SparkContext

    gateway = SparkContext._gateway
    if gateway is None:
        return None
    proc = getattr(gateway, "proc", None)
    args = getattr(proc, "args", None)
    if not args:
        return None
    return any("delta-spark" in str(a) for a in args)


def _session_can_load_delta(spark) -> bool:
    """Fallback evidence for a gateway whose launch we cannot see: the Delta
    jars in the SparkContext's jar list. Not a SQL conf value -- a session can
    carry ``spark.sql.catalog.spark_catalog=...DeltaCatalog`` on a jar-less
    gateway, and ``getOrCreate()`` copies ``spark.jars.packages`` onto an
    existing session too -- and not a py4j ``Class.forName`` probe, which
    resolves through the gateway's classloader rather than Spark's."""
    try:
        return "delta-spark" in spark.sparkContext._jsc.sc().listJars().mkString(",")
    except Exception:  # noqa: BLE001 - a half-dead session counts as unusable
        return False


def _jvm_can_load_delta(spark) -> bool:
    """Whether the JVM behind ``spark`` can serve Delta: how it was launched
    when that is visible, else :func:`_session_can_load_delta`."""
    launched = _gateway_launched_with_delta()
    if launched is not None:
        return launched
    return _session_can_load_delta(spark)


def _session_has_delta_confs(active) -> bool:
    """Both static Delta settings are present on the session: the SQL
    extension (MERGE/DDL) and the session catalog (``DeltaCatalog``). They are
    separate settings and ``getOrCreate()`` retrofits neither."""
    try:
        extensions = active.conf.get("spark.sql.extensions", "") or ""
        catalog = active.conf.get("spark.sql.catalog.spark_catalog", "") or ""
    except Exception:  # noqa: BLE001 - unusable session
        return False
    return _DELTA_EXTENSION in extensions and _DELTA_CATALOG in catalog


def _rebuild_plain_active_session(active) -> bool:
    """Stop an active session built WITHOUT both Delta static confs so the
    caller's ``getOrCreate()`` creates one with them -- on the same JVM.

    ``getOrCreate()`` hands back the active session and cannot install
    ``spark.sql.extensions`` or the session catalog retroactively. Only the
    session is stopped; the JVM, and everything bound to the JVM, survives.
    The process-level references to the stopped session are cleared as well,
    or ``kindling.spark_session.get_or_create_spark_session()`` would keep
    returning it. Returns True when it rebuilt.

    A JVM launched with the Delta confs carries them as system properties, so
    new sessions in it normally already have both; this is the safety net for
    a session created some other way.
    """
    if active is None or _session_has_delta_confs(active):
        return False
    try:
        active.stop()
    except Exception:  # noqa: BLE001
        pass
    _clear_stale_session_globals()
    return True


def _teardown_non_delta_jvm() -> bool:
    """Make the next Delta-configured ``getOrCreate()`` succeed, touching as
    little as possible. Returns True when a JVM relaunch was forced.

    Why this exists: if an earlier test module built a plain session first --
    ``SparkSession.builder...getOrCreate()`` with no Delta packages -- every
    later ``getOrCreate()`` silently reuses that jar-less JVM and Delta fails
    with "Cannot find catalog plugin class ... DeltaCatalog". That is masked on
    machines whose Spark install ships Delta on the default classpath (the
    devcontainer) and bites in the CI image, so it surfaced as an
    order-dependent Integration Tests failure once Delta-backed modules were
    moved from tests/unit into tests/integration.

    The decision is about the JVM, not the session:

    - No gateway: nothing to do.
    - Gateway this process launched WITH Delta: reuse it, active session or
      not (a module's ``spark.stop()`` leaves the gateway alive and the next
      ``getOrCreate()`` makes a new SparkContext in it). Killing such a JVM
      would invalidate everything else bound to it -- wider-scoped fixtures,
      PySpark UDF objects that cache their Java counterpart, kindling's global
      session -- which is exactly what happened when this preflight first tore
      down on "no active session". At most the active *session* is rebuilt
      when it lacks the Delta static confs.
    - Gateway this process launched WITHOUT Delta, or dead: tear down and
      reap via :func:`_teardown_existing_spark_jvm`.
    - Externally started gateway (launch not visible): never torn down. If an
      active session proves it jar-less, or it is dead, raise
      :class:`ExternalGatewayUnusable` rather than hand back a session that
      fails at first Delta use; with no session to inspect, proceed.
    """
    from pyspark import SparkContext

    if SparkContext._gateway is None:
        return False
    launched = _gateway_launched_with_delta()
    try:
        active = SparkSession.getActiveSession()
    except Exception as exc:  # noqa: BLE001 - dead gateway
        if launched is None:
            raise ExternalGatewayUnusable(
                "the externally started py4j gateway (PYSPARK_GATEWAY_PORT) is not "
                "responding and cannot be relaunched from here"
            ) from exc
        _teardown_existing_spark_jvm()
        return True
    if launched is None:
        if active is not None and not _session_can_load_delta(active):
            raise ExternalGatewayUnusable(
                "the externally started py4j gateway (PYSPARK_GATEWAY_PORT) was "
                "launched without the Delta jars; spark.jars.packages cannot add them "
                "after launch and the gateway is not ours to relaunch"
            )
        _rebuild_plain_active_session(active)
        return False
    if launched:
        _rebuild_plain_active_session(active)
        return False
    _teardown_existing_spark_jvm()
    return True


def get_standalone_spark_session(app_name="KindlingTest"):
    """
    Create a standalone, Delta-configured Spark session (no cluster) for tests.

    Order-independent by construction: if an earlier module left a JVM
    without the Delta catalog, it is torn down first (see
    ``_teardown_non_delta_jvm``) so the session returned here really does have
    Delta, regardless of which test module launched Spark first in this
    worker/process.
    """
    _teardown_non_delta_jvm()
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .config("spark.ui.enabled", "false")
        .config("spark.jars.ivy", _worker_scoped_ivy_dir())
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    return spark


def get_local_spark_session_with_azure(
    app_name="KindlingTest",
    storage_account=None,
    auth_type="azure_cli",
    azure_cloud="public",
    check_env=True,
    **auth_params,
):
    """
    Create a Spark session with Azure storage authentication.

    Args:
        app_name: Application name
        storage_account: Azure storage account name (will use AZURE_STORAGE_ACCOUNT env var if not provided)
        auth_type: Authentication method:
            - 'azure_cli' (default): Use Azure CLI credentials (az login)
            - 'key': Use storage account key
            - 'oauth': Use service principal (client_id, client_secret, tenant_id)
            - 'sas': Use SAS token
        azure_cloud: Azure cloud environment (will use AZURE_CLOUD env var if not provided):
            - 'public' (default): Azure Commercial Cloud (.windows.net)
            - 'government' or 'gov': Azure Government Cloud (.usgovcloudapi.net)
            - 'china': Azure China Cloud (.chinacloudapi.cn)
        check_env: Whether to check for required environment variables
        **auth_params: Additional auth parameters based on auth_type

    Returns:
        SparkSession configured with Azure storage access
    """

    # Check environment variables without running setup wizard
    if check_env:
        required_vars = ["AZURE_STORAGE_ACCOUNT"] if not storage_account else []
        if not check_required_env_vars(required_vars, check_env=True):
            print("\n⚠️  Required environment variables not set.")
            print("   Tests expect environment to be properly configured by test runner.")
            print("   For development setup, run: python scripts/init_azure_dev.sh")
            raise EnvironmentError("Required Azure environment variables not configured")

    # Use environment variables as defaults
    storage_account = storage_account or os.getenv("AZURE_STORAGE_ACCOUNT")
    azure_cloud = (
        azure_cloud if azure_cloud != "public" else os.getenv("AZURE_CLOUD", "AZURE_PUBLIC_CLOUD")
    )

    if not storage_account:
        raise ValueError(
            "storage_account must be provided or set AZURE_STORAGE_ACCOUNT environment variable"
        )

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.3.2,org.apache.hadoop:hadoop-azure:3.3.4,org.apache.hadoop:hadoop-azure-datalake:3.3.4",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.executor.memory", "1g")
        .config("spark.driver.memory", "1g")
        .config("spark.jars.ivy", _worker_scoped_ivy_dir())
    )

    if storage_account:
        # Get cloud-specific configuration
        cloud_config = _get_azure_cloud_config(azure_cloud)
        dfs_endpoint = f"{storage_account}.{cloud_config['dfs_suffix']}"

        if auth_type == "azure_cli":
            # Use Azure CLI or DefaultAzureCredential
            try:
                import json
                import subprocess

                from azure.identity import AzureCliCredential, DefaultAzureCredential

                # For Government Cloud, if DefaultAzureCredential fails, try getting token via az CLI directly
                access_token = None
                try:
                    credential = DefaultAzureCredential()
                    token = credential.get_token(cloud_config["token_scope"])
                    access_token = token.token
                except Exception as default_cred_error:
                    if azure_cloud == "AZURE_US_GOVERNMENT":
                        # Fallback: Try to get token via az account get-access-token
                        # using management resource which is more commonly available in Gov Cloud
                        print(f"⚠ DefaultAzureCredential failed ({default_cred_error})")
                        print("  Trying alternate authentication method for Government Cloud...")
                        try:
                            # Try getting a token for the storage resource directly via CLI
                            result = subprocess.run(
                                [
                                    "az",
                                    "account",
                                    "get-access-token",
                                    "--resource",
                                    "https://storage.azure.us/",
                                ],
                                capture_output=True,
                                text=True,
                                check=True,
                            )
                            token_data = json.loads(result.stdout)
                            access_token = token_data["accessToken"]
                            print("  ✓ Using Azure CLI token via subprocess")
                        except subprocess.CalledProcessError:
                            # If storage resource fails, try using AzureCliCredential with explicit exclude
                            print("  Storage resource not available, trying management token...")
                            raise default_cred_error
                    else:
                        raise

                if not access_token:
                    raise ValueError("Failed to obtain access token")

                # Store token and endpoint for post-session configuration
                # We'll set this after the session is created to avoid OAuth provider class issues
                azure_token_config = {"endpoint": dfs_endpoint, "token": access_token}

                print(
                    f"✓ Using Azure CLI authentication for {storage_account} ({cloud_config['name']})"
                )
            except ImportError:
                print("⚠ azure-identity not installed. Run: pip install azure-identity")
                raise
            except Exception as e:
                print(f"⚠ Azure CLI auth failed: {e}")
                print("  Make sure you're logged in: az login")
                if azure_cloud != "AZURE_PUBLIC_CLOUD":
                    print(f"  For {cloud_config['name']}, use: az cloud set --name {azure_cloud}")
                raise

        elif auth_type == "key":
            account_key = auth_params.get("account_key") or os.getenv("AZURE_STORAGE_KEY")
            if not account_key:
                raise ValueError(
                    "account_key required for auth_type='key' or set AZURE_STORAGE_KEY environment variable"
                )
            builder = builder.config(
                f"spark.hadoop.fs.azure.account.key.{dfs_endpoint}", account_key
            )

        elif auth_type == "oauth":
            tenant_id = auth_params.get("tenant_id") or os.getenv("AZURE_TENANT_ID")
            client_id = auth_params.get("client_id") or os.getenv("AZURE_CLIENT_ID")
            client_secret = auth_params.get("client_secret") or os.getenv("AZURE_CLIENT_SECRET")

            if not all([tenant_id, client_id, client_secret]):
                raise ValueError(
                    "tenant_id, client_id, and client_secret required for auth_type='oauth' "
                    "or set AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET environment variables"
                )

            builder = (
                builder.config(f"spark.hadoop.fs.azure.account.auth.type.{dfs_endpoint}", "OAuth")
                .config(
                    f"spark.hadoop.fs.azure.account.oauth.provider.type.{dfs_endpoint}",
                    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                )
                .config(f"spark.hadoop.fs.azure.account.oauth2.client.id.{dfs_endpoint}", client_id)
                .config(
                    f"spark.hadoop.fs.azure.account.oauth2.client.secret.{dfs_endpoint}",
                    client_secret,
                )
                .config(
                    f"spark.hadoop.fs.azure.account.oauth2.client.endpoint.{dfs_endpoint}",
                    f"{cloud_config['login_endpoint']}/{tenant_id}/oauth2/token",
                )
            )

        elif auth_type == "sas":
            sas_token = auth_params.get("sas_token")
            if not sas_token:
                raise ValueError("sas_token required for auth_type='sas'")
            builder = (
                builder.config(f"spark.hadoop.fs.azure.account.auth.type.{dfs_endpoint}", "SAS")
                .config(
                    f"spark.hadoop.fs.azure.sas.token.provider.type.{dfs_endpoint}",
                    "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider",
                )
                .config(f"spark.hadoop.fs.azure.sas.fixed.token.{dfs_endpoint}", sas_token)
            )

    # Delta-configured builder: same preflight as the other constructors, or a
    # plain JVM launched earlier stays jar-less for the Azure path too.
    _teardown_non_delta_jvm()
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # If azure_cli auth was used, configure the token directly in Hadoop configuration
    if auth_type == "azure_cli" and "azure_token_config" in locals():
        hadoop_conf = spark._jsc.hadoopConfiguration()
        endpoint = azure_token_config["endpoint"]
        token = azure_token_config["token"]

        # Use ClientCredsTokenProvider with dummy credentials
        # The token will be cached and used without calling the endpoint
        hadoop_conf.set(f"fs.azure.account.auth.type.{endpoint}", "OAuth")
        hadoop_conf.set(
            f"fs.azure.account.oauth.provider.type.{endpoint}",
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        )
        # Set Azure CLI client ID and dummy secret (won't be used if token is cached)
        # Use environment variable or fallback to Azure CLI well-known client ID
        azure_cli_client_id = os.getenv(
            "AZURE_CLI_CLIENT_ID", "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
        )
        hadoop_conf.set(f"fs.azure.account.oauth2.client.id.{endpoint}", azure_cli_client_id)
        hadoop_conf.set(
            f"fs.azure.account.oauth2.client.secret.{endpoint}", "dummy-secret-not-used"
        )
        # Set the OAuth endpoint (may not be called if token is fresh)
        hadoop_conf.set(
            f"fs.azure.account.oauth2.client.endpoint.{endpoint}",
            f"{cloud_config['login_endpoint']}/common/oauth2/token",
        )
        # Pre-set the token - this is the key part
        hadoop_conf.set(f"fs.azure.account.oauth2.access.token.{endpoint}", token)
        # Set token expiry far in the future so it won't refresh
        import time

        hadoop_conf.set(
            f"fs.azure.account.oauth2.token.expiry.{endpoint}", str(int(time.time()) + 3600)
        )

        print(f"  ✓ Token configured in Hadoop configuration")

    return spark


def cleanup_test_data(spark, *paths):
    """
    Clean up test data paths using dbutils or direct filesystem operations.
    """
    for path in paths:
        try:
            # Try dbutils first (if available in test environment)
            try:
                import dbutils

                dbutils.fs.rm(path, recurse=True)
            except (ImportError, NameError):
                # Fall back to Spark's Hadoop filesystem API
                hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
                fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
                    spark.sparkContext._jvm.java.net.URI.create(path), hadoop_conf
                )
                fs_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)
                fs.delete(fs_path, True)
        except Exception as e:
            print(f"Warning: Could not clean up {path}: {e}")


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
        if hasattr(cls, "spark"):
            cls.spark.stop()

    def cleanup_paths(self, *paths):
        """Clean up test paths"""
        cleanup_test_data(self.spark, *paths)


# DEPRECATED: Environment setup functions have been moved to separate module
# For interactive environment setup, use: python scripts/init_azure_dev.sh


def check_and_setup_env():
    """
    DEPRECATED: Use environment setup scripts instead.

    This function has been moved to a separate environment setup module.
    For interactive environment setup, run: python scripts/init_azure_dev.sh

    Returns:
        bool: False (setup wizard no longer available in test utilities)
    """
    print("❌ Environment setup wizard has been moved out of test utilities.")
    print("   Tests expect environment variables to be configured by test runner.")
    print("   For development setup, run: python scripts/init_azure_dev.sh")
    return False


def run_env_setup_wizard():
    """
    DEPRECATED: Use environment setup scripts instead.

    This function has been moved to a separate environment setup module.
    For interactive environment setup, run: python scripts/init_azure_dev.sh

    Returns:
        bool: False (setup wizard no longer available in test utilities)
    """
    print("❌ Environment setup wizard has been moved out of test utilities.")
    print("   Tests expect environment variables to be configured by test runner.")
    print("   For development setup, run: python scripts/init_azure_dev.sh")
    return False


def setup_kindling_env():
    """
    DEPRECATED: Use environment setup scripts instead.

    For interactive environment setup, run: python scripts/init_azure_dev.sh

    Returns:
        bool: False (setup wizard no longer available in test utilities)
    """
    print("❌ Environment setup wizard has been moved out of test utilities.")
    print("   Tests expect environment variables to be configured by test runner.")
    print("   For development setup, run: python scripts/init_azure_dev.sh")
    return False


if __name__ == "__main__":
    # If this script is run directly, show deprecated message
    print("❌ Environment setup wizard has been moved out of test utilities.")
    print("   For interactive environment setup, run: python scripts/init_azure_dev.sh")
