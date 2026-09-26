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
TARGET_TABLE = 'MART_CUST_SERVICE_PRIO_DATAMART'

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@task
def create_cust_service_prio_datamart():
    from pyspark.sql import functions as F

    mysql_hook, jdbc_url, jdbc_properties = get_mysql_jdbc_config(MYSQL_CONN_ID)
    spark = create_spark_session('customer-service-priority-datamart')

    try:
        after_sales_df = uppercase_columns(
            spark.read.jdbc(
                url=jdbc_url,
                table='TXF_AFTER_SALES',
                properties=jdbc_properties,
            )
        )
        customers_df = uppercase_columns(
            spark.read.jdbc(
                url=jdbc_url,
                table='TXF_CUSTOMERS',
                properties=jdbc_properties,
            )
        )
        customer_address_df = uppercase_columns(
            spark.read.jdbc(
                url=jdbc_url,
                table='TXF_CUSTOMER_ADDRESS',
                properties=jdbc_properties,
            )
        )

        service_count_df = (
            after_sales_df
            .withColumn('PERIODE', F.year('SERVICE_DATE'))
            .groupBy('PERIODE', 'VIN', 'CUSTOMER_ID')
            .agg(F.count(F.lit(1)).cast('int').alias('COUNT_SERVICE'))
        )

        customer_lookup_df = customers_df.select(
            F.col('ID').alias('CUSTOMER_ID'),
            F.col('NAME').alias('CUSTOMER_NAME'),
        )
        address_lookup_df = customer_address_df.select('CUSTOMER_ID', 'ADDRESS')

        customer_service_datamart_df = (
            service_count_df
            .join(customer_lookup_df, on='CUSTOMER_ID', how='inner')
            .join(address_lookup_df, on='CUSTOMER_ID', how='left')
            .withColumn(
                'PRIORITY',
                F.when(F.col('COUNT_SERVICE') > 10, F.lit('HIGH'))
                .when(F.col('COUNT_SERVICE').between(5, 10), F.lit('MED'))
                .otherwise(F.lit('LOW')),
            )
            .select(
                'PERIODE',
                'VIN',
                'CUSTOMER_NAME',
                'ADDRESS',
                'COUNT_SERVICE',
                'PRIORITY',
            )
        )

        prepare_target_table(mysql_hook, """
        CREATE TABLE IF NOT EXISTS `MART_CUST_SERVICE_PRIO_DATAMART` (
            periode INT,
            vin VARCHAR(50),
            customer_name VARCHAR(255),
            address VARCHAR(255),
            count_service INT,
            priority VARCHAR(10),
            LOAD_TIMESTAMP DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6)
        );
        """, TARGET_TABLE)

        (
            customer_service_datamart_df.write
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
    dag_id='task_ingest_cust_service_prio_datamart',
    default_args=default_args,
    schedule='0 10 * * *',
    # schedule='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['dwh'],
) as dag:
    create_cust_service_prio_datamart()
