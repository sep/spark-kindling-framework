from unittest.mock import Mock

from kindling import spark_jdbc


def test_create_jdbc_url_uses_azure_sql_certificate_wildcard():
    url = spark_jdbc.createJdbcUrl("server.database.windows.net", "warehouse")

    assert url == (
        "jdbc:sqlserver://server.database.windows.net;databaseName=warehouse;"
        "encrypt=true;trustServerCertificate=false;loginTimeout=10;"
        "hostNameInCertificate=*.database.windows.net"
    )


def test_create_jdbc_url_uses_synapse_gov_certificate_wildcard():
    url = spark_jdbc.createJdbcUrl("pool.sql.azuresynapse.usgovcloudapi.net", "warehouse")

    assert "hostNameInCertificate=*.sql.azuresynapse.usgovcloudapi.net" in url


def test_create_jdbc_url_normalizes_tcp_prefix_and_comma_port():
    url = spark_jdbc.createJdbcUrl("tcp:server.database.windows.net,1433", "warehouse")

    assert url.startswith("jdbc:sqlserver://server.database.windows.net:1433;")


def test_create_jdbc_url_corrects_azure_1443_port_to_1433():
    url = spark_jdbc.createJdbcUrl("server.database.windows.net:1443", "warehouse")

    assert url.startswith("jdbc:sqlserver://server.database.windows.net:1433;")


def test_create_jdbc_url_trust_server_certificate_suppresses_host_name_in_certificate():
    url = spark_jdbc.createJdbcUrl(
        "server.database.windows.net",
        "warehouse",
        trust_server_certificate=True,
    )

    assert "trustServerCertificate=true" in url
    assert "hostNameInCertificate" not in url


def test_create_connection_properties_uses_token_and_certificate_host(monkeypatch):
    mssparkutils = Mock()
    mssparkutils.credentials.getToken.return_value = "token"
    monkeypatch.setattr(spark_jdbc, "mssparkutils", mssparkutils, raising=False)

    properties = spark_jdbc.createConnectionProperties("server.database.windows.net")

    assert properties == {
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "accessToken": "token",
        "encrypt": "true",
        "trustServerCertificate": "false",
        "hostNameInCertificate": "*.database.windows.net",
    }


def test_spark_jdbc_read_gets_or_creates_spark_session(monkeypatch):
    mssparkutils = Mock()
    mssparkutils.credentials.getToken.return_value = "token"
    spark = Mock()
    monkeypatch.setattr(spark_jdbc, "mssparkutils", mssparkutils, raising=False)
    monkeypatch.setattr(spark_jdbc, "get_or_create_spark_session", lambda: spark)

    spark_jdbc.spark_jdbc_read(
        "jdbc:sqlserver://server.database.windows.net;databaseName=warehouse",
        "dbo.orders",
    )

    spark.read.jdbc.assert_called_once_with(
        "jdbc:sqlserver://server.database.windows.net;databaseName=warehouse",
        table="dbo.orders",
        properties={
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "accessToken": "token",
            "encrypt": "true",
            "trustServerCertificate": "false",
            "hostNameInCertificate": "*.database.windows.net",
        },
    )


def test_create_jdbc_connection_gets_or_creates_spark_session(monkeypatch):
    mssparkutils = Mock()
    mssparkutils.credentials.getToken.return_value = "token"
    properties = Mock()
    driver_manager = Mock()
    driver_manager.getConnection.return_value = "connection"
    spark = Mock()
    spark._sc._gateway.jvm.java.util.Properties.return_value = properties
    spark._sc._gateway.jvm.java.sql.DriverManager = driver_manager
    monkeypatch.setattr(spark_jdbc, "mssparkutils", mssparkutils, raising=False)
    monkeypatch.setattr(spark_jdbc, "get_or_create_spark_session", lambda: spark)

    connection = spark_jdbc.createJdbcConnection("server.database.windows.net", "warehouse")

    assert connection == "connection"
    properties.setProperty.assert_any_call("accessToken", "token")
    properties.setProperty.assert_any_call("trustServerCertificate", "false")
    driver_manager.getConnection.assert_called_once_with(
        (
            "jdbc:sqlserver://server.database.windows.net;databaseName=warehouse;"
            "encrypt=true;trustServerCertificate=false;loginTimeout=10;"
            "hostNameInCertificate=*.database.windows.net"
        ),
        properties,
    )


def test_create_external_table_uses_supplied_arguments():
    connection = Mock()

    spark_jdbc.create_external_table(
        connection,
        "curated/orders",
        "sales",
        "orders",
    )

    statement = connection.prepareCall.call_args.args[0]
    assert "CREATE EXTERNAL TABLE [sales].[orders]" in statement
    assert "LOCATION = 'curated/orders'" in statement
    assert "DATA_SOURCE = [DatalakeCurated]" in statement
    assert "FILE_FORMAT = [ParquetFormat]" in statement
    connection.prepareCall.return_value.execute.assert_called_once_with()


def test_create_view_uses_execute_ddl():
    connection = Mock()

    spark_jdbc.create_view(connection, "sales", "orders_v", "SELECT * FROM sales.orders")

    statement = connection.prepareCall.call_args.args[0]
    assert "CREATE OR ALTER VIEW [sales].orders_v" in statement
    assert "SELECT * FROM sales.orders" in statement
    connection.prepareCall.return_value.execute.assert_called_once_with()
