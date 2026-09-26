import os
from urllib.parse import quote

from airflow.exceptions import AirflowException
from airflow.providers.mysql.hooks.mysql import MySqlHook


def get_mysql_jdbc_config(mysql_conn_id):
    """Build a JDBC configuration from an Airflow connection without logging secrets."""
    mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)
    airflow_connection = mysql_hook.get_connection(mysql_conn_id)

    required_fields = {
        "host": airflow_connection.host,
        "schema": airflow_connection.schema,
        "login": airflow_connection.login,
        "password": airflow_connection.password,
    }
    missing_fields = [name for name, value in required_fields.items() if not value]
    if missing_fields:
        missing = ", ".join(missing_fields)
        raise AirflowException(
            f"Airflow Connection '{mysql_conn_id}' belum lengkap. Field wajib: {missing}."
        )

    host = airflow_connection.host
    port = airflow_connection.port or 3306
    schema = quote(airflow_connection.schema, safe="")
    jdbc_url = f"jdbc:mysql://{host}:{port}/{schema}"

    jdbc_options = os.getenv("MYSQL_JDBC_OPTIONS", "").strip().lstrip("?")
    if jdbc_options:
        jdbc_url = f"{jdbc_url}?{jdbc_options}"

    jdbc_properties = {
        "user": airflow_connection.login,
        "password": airflow_connection.password,
        "driver": "com.mysql.cj.jdbc.Driver",
    }
    return mysql_hook, jdbc_url, jdbc_properties


def create_spark_session(app_name):
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER_URL", "local[*]"))
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.sql.caseSensitive", "false")
    )

    driver_host = os.getenv("SPARK_DRIVER_HOST")
    if driver_host:
        builder = builder.config("spark.driver.host", driver_host)

    jdbc_jar = os.getenv("SPARK_JDBC_JAR")
    if jdbc_jar:
        builder = builder.config("spark.jars", jdbc_jar)

    return builder.getOrCreate()


def uppercase_columns(dataframe):
    """Normalize JDBC column names so transformations work across MySQL settings."""
    from pyspark.sql import functions as F

    return dataframe.select(
        *[F.col(f"`{column}`").alias(column.upper()) for column in dataframe.columns]
    )


def prepare_target_table(mysql_hook, create_table_sql, table_name):
    connection = None
    cursor = None
    try:
        connection = mysql_hook.get_conn()
        connection.ping()
        cursor = connection.cursor()
        cursor.execute(create_table_sql)
        cursor.execute(f"TRUNCATE TABLE `{table_name}`")
        connection.commit()
    except Exception:
        if connection is not None:
            connection.rollback()
        raise
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()
