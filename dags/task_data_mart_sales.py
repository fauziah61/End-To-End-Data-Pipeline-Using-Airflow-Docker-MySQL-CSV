import os
from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta

from spark_mysql_utils import (
    create_spark_session,
    get_mysql_jdbc_config,
    prepare_target_table,
    uppercase_columns,
)

MYSQL_CONN_ID = os.getenv('MYSQL_CONN_ID', 'mysql-localhost')
TARGET_TABLE = 'MART_SALES_DATAMART'

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@task
def create_sales_datamart():
    from pyspark.sql import functions as F
    from pyspark.sql.types import DecimalType

    mysql_hook, jdbc_url, jdbc_properties = get_mysql_jdbc_config(MYSQL_CONN_ID)
    spark = create_spark_session('sales-datamart')

    try:
        sales_df = uppercase_columns(
            spark.read.jdbc(
                url=jdbc_url,
                table='TXF_SALES',
                properties=jdbc_properties,
            )
        )

        price = F.col('PRICE')
        sales_datamart_df = (
            sales_df
            .withColumn('PERIODE', F.date_format(F.col('INVOICE_DATE'), 'yyyy-MM'))
            .withColumn(
                'CLASS',
                F.when(price.between(100000000, 250000000), F.lit('LOW'))
                .when(price.between(250000001, 400000000), F.lit('MEDIUM'))
                .when(price > 400000000, F.lit('HIGH')),
            )
            .groupBy('PERIODE', 'CLASS', 'MODEL')
            .agg(
                F.sum('PRICE')
                .cast(DecimalType(38, 5))
                .alias('TOTAL')
            )
            .select('PERIODE', 'CLASS', 'MODEL', 'TOTAL')
        )

        prepare_target_table(mysql_hook, """
        CREATE TABLE IF NOT EXISTS `MART_SALES_DATAMART` (
            PERIODE VARCHAR(7) NOT NULL,
            CLASS VARCHAR(10),
            MODEL VARCHAR(50),
            TOTAL DECIMAL(38,5),
            LOAD_TIMESTAMP DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
            PRIMARY KEY (PERIODE, CLASS, MODEL)
        );
        """, TARGET_TABLE)

        (
            sales_datamart_df.write
            .mode('append')
            .option('batchsize', os.getenv('SPARK_JDBC_BATCH_SIZE', '1000'))
            .jdbc(
                url=jdbc_url,
                table=TARGET_TABLE,
                properties=jdbc_properties,
            )
        )
        print(f"{TARGET_TABLE} successfully created and loaded with PySpark")

    except Exception as e:
        raise RuntimeError(f"Gagal membuat datamart: {e}")

    finally:
        spark.stop()


# DAG
with DAG(
    dag_id='task_ingest_sales_datamart',
    default_args=default_args,
    # schedule='@daily',
    schedule='0 10 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['dwh'],
) as dag:
    create_sales_datamart()
