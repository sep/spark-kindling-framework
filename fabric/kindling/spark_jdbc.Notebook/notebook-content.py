# Fabric notebook source


# CELL ********************

def createJdbcUrl( server: str, database: str ) -> str:
    return f'jdbc:sqlserver://{server};databaseName={database};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=10'
 
def createJdbcConnection( server: str, database: str ):
    url = createJdbcUrl(server, database)
    driver_manager = spark._sc._gateway.jvm.java.sql.DriverManager
    properties = spark._sc._gateway.jvm.java.util.Properties()
    token = mssparkutils.credentials.getToken('DW')
    properties.setProperty('accessToken', token)
    
    return driver_manager.getConnection(url, properties)
     
def createConnectionProperties():
    token = mssparkutils.credentials.getToken('DW')
    return {
        'driver': 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
        'accessToken': token,
        'encrypt': 'true',
        'trustServerCertificate': 'false'
    }

def spark_jdbc_read( jdbcUri, table ):
    return spark.read.jdbc(jdbcUri, table=table, properties=createConnectionProperties())

def execute_ddl(
    connection,
    ddl_statement: str
):
    exec_statement = connection.prepareCall(ddl_statement)
    exec_statement.execute()
def create_external_table(
    connection,
    location,
    schema,
    table

):
    create_table_statement = f"""
    CREATE EXTERNAL TABLE [{dest_schema}].[{view_name}{slot_suffix}]
    WITH
    (
        LOCATION = '/{dest_schema}/{view_name}{slot_suffix}',
        DATA_SOURCE = [DatalakeCurated],
        FILE_FORMAT = [ParquetFormat]
    )
    AS
    select * from [{src_db}].[{src_schema}].{view_name}
    """

    exec_statement = connection.prepareCall(create_table_statement)
    exec_statement.execute()

def create_sql_database(connection, dbname):
    
    create_schema_statement = f"""
    IF (NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{dbname}')) 
    BEGIN
        EXEC ('CREATE SCHEMA [{dbname}]')
    END
    """

    execute_ddl( connection, create_schema_statement )
def create_view(
    connection,
    schema,
    view_name,
    view_statement: str
):
    create_view_statement = f"""
    CREATE OR ALTER VIEW [{schema}].{view_name}
    AS
    {view_statement}
    """
    execute_ddl( connection, create_view_statement )
