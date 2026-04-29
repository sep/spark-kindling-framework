from kindling.spark_session import get_or_create_spark_session


def _normalize_server_host(server: str) -> str:
    server_host = server.strip().lower()

    if server_host.startswith("tcp:"):
        server_host = server_host[4:]

    if "," in server_host:
        server_host = server_host.split(",", 1)[0]

    if ":" in server_host:
        server_host = server_host.split(":", 1)[0]

    return server_host


def _normalize_server_for_jdbc(server: str) -> str:
    normalized_server = server.strip()

    if normalized_server.lower().startswith("tcp:"):
        normalized_server = normalized_server[4:]

    if "," in normalized_server:
        server_host, server_port = normalized_server.split(",", 1)
        normalized_server = f"{server_host}:{server_port}"

    if ":" in normalized_server:
        server_host, server_port = normalized_server.rsplit(":", 1)
        normalized_host = _normalize_server_host(server_host)

        if server_port == "1443" and (
            normalized_host.endswith(".sql.azuresynapse.net")
            or normalized_host.endswith(".sql.azuresynapse.usgovcloudapi.net")
            or normalized_host.endswith(".database.windows.net")
            or normalized_host.endswith(".database.usgovcloudapi.net")
        ):
            normalized_server = f"{server_host}:1433"

    return normalized_server


def _get_host_name_in_certificate(server: str) -> str:
    normalized_host = _normalize_server_host(server)

    wildcard_certificate_suffixes = (
        ".sql.azuresynapse.net",
        ".sql.azuresynapse.usgovcloudapi.net",
        ".database.windows.net",
        ".database.usgovcloudapi.net",
    )

    for certificate_suffix in wildcard_certificate_suffixes:
        if normalized_host.endswith(certificate_suffix):
            return f"*{certificate_suffix}"

    return normalized_host


def createJdbcUrl(server: str, database: str, trust_server_certificate: bool = False) -> str:
    normalized_server = _normalize_server_for_jdbc(server)
    trust_server_certificate_value = str(trust_server_certificate).lower()
    jdbc_url = (
        f"jdbc:sqlserver://{normalized_server};databaseName={database};"
        f"encrypt=true;trustServerCertificate={trust_server_certificate_value};"
        "loginTimeout=10"
    )

    if not trust_server_certificate:
        host_name_in_certificate = _get_host_name_in_certificate(normalized_server)
        jdbc_url = f"{jdbc_url};hostNameInCertificate={host_name_in_certificate}"

    return jdbc_url


def createJdbcConnection(server: str, database: str, trust_server_certificate: bool = False):
    spark_session = get_or_create_spark_session()
    url = createJdbcUrl(server, database, trust_server_certificate=trust_server_certificate)
    driver_manager = spark_session._sc._gateway.jvm.java.sql.DriverManager
    properties = spark_session._sc._gateway.jvm.java.util.Properties()
    token = mssparkutils.credentials.getToken("DW")
    properties.setProperty("accessToken", token)
    properties.setProperty("trustServerCertificate", str(trust_server_certificate).lower())

    return driver_manager.getConnection(url, properties)


def _extract_server_from_jdbc_uri(jdbcUri: str) -> str:
    jdbc_prefix = "jdbc:sqlserver://"

    if jdbcUri.startswith(jdbc_prefix):
        server_definition = jdbcUri[len(jdbc_prefix) :].split(";", 1)[0]
        return _normalize_server_for_jdbc(server_definition)

    return jdbcUri


def _extract_trust_server_certificate_from_jdbc_uri(jdbcUri: str):
    trust_setting = "trustServerCertificate="

    for jdbc_property in jdbcUri.split(";"):
        if jdbc_property.startswith(trust_setting):
            return jdbc_property[len(trust_setting) :].strip().lower() == "true"

    return None


def createConnectionProperties(server: str = None, trust_server_certificate: bool = False):
    token = mssparkutils.credentials.getToken("DW")
    properties = {
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "accessToken": token,
        "encrypt": "true",
        "trustServerCertificate": str(trust_server_certificate).lower(),
    }

    if server and not trust_server_certificate:
        properties["hostNameInCertificate"] = _get_host_name_in_certificate(server)

    return properties


def spark_jdbc_read(jdbcUri, table, trust_server_certificate=None):
    spark_session = get_or_create_spark_session()
    server = _extract_server_from_jdbc_uri(jdbcUri)
    if trust_server_certificate is None:
        trust_server_certificate = _extract_trust_server_certificate_from_jdbc_uri(jdbcUri)

    if trust_server_certificate is None:
        trust_server_certificate = False

    return spark_session.read.jdbc(
        jdbcUri,
        table=table,
        properties=createConnectionProperties(
            server,
            trust_server_certificate=trust_server_certificate,
        ),
    )


def execute_ddl(connection, ddl_statement: str):
    exec_statement = connection.prepareCall(ddl_statement)
    exec_statement.execute()


def create_external_table(connection, location, schema, table):
    create_table_statement = f"""
    CREATE EXTERNAL TABLE [{schema}].[{table}]
    WITH
    (
        LOCATION = '{location}',
        DATA_SOURCE = [DatalakeCurated],
        FILE_FORMAT = [ParquetFormat]
    )
    """

    execute_ddl(connection, create_table_statement)


def create_sql_database(connection, dbname):

    create_schema_statement = f"""
    IF (NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{dbname}'))
    BEGIN
        EXEC ('CREATE SCHEMA [{dbname}]')
    END
    """

    execute_ddl(connection, create_schema_statement)


def create_view(connection, schema, view_name, view_statement: str):
    create_view_statement = f"""
    CREATE OR ALTER VIEW [{schema}].{view_name}
    AS
    {view_statement}
    """
    execute_ddl(connection, create_view_statement)
