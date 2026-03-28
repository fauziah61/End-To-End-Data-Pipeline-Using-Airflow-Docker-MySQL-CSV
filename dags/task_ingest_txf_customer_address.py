from time import time
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
# from airflow.sensors.external_task import ExternalTaskSensor
# from airflow.datasets import Dataset
from datetime import datetime, timedelta

MYSQL_CONN_ID = 'mysql-localhost'

# # SAME DATASET
# customer_address_dataset = Dataset("stg_customer_address")

default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

@task
def ingest_txf_customer_address(**context):
    # Ambil tanggal Airflow (manual / scheduler)
    input_date = context.get("params", {}).get("input_date")
    # ds = input_date if input_date else context['logical_date'].strftime('%Y-%m-%d')
    ds = input_date if input_date else context['ds']  # YYYY-MM-DD
    try:
        mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        conn = mysql_hook.get_conn()
        # conn.autocommit = True 
        conn.ping()
        cursor = conn.cursor()
    except Exception as e:
        raise ConnectionError(f"Gagal konek ke MySQL: {str(e)}")

    # CREATE TABLE
    try:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS `TXF_CUSTOMER_ADDRESS` (
            id VARCHAR(255),
            customer_id VARCHAR(255) not null,
            address VARCHAR(255),
            city VARCHAR(50),
            province VARCHAR(25),
            created_at DATETIME(3),
            LOAD_TIMESTAMP DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
            PRIMARY KEY (customer_id)
        );
        """)
        
        # Duplicate handling
        cursor.execute("""
        DELETE FROM TXF_CUSTOMER_ADDRESS
        WHERE DATE(LOAD_TIMESTAMP) = %s
        """, (ds,))

        cursor.execute("""
                INSERT INTO `TXF_CUSTOMER_ADDRESS` (
                    ID, CUSTOMER_ID, ADDRESS, CITY, PROVINCE, CREATED_AT
                )
                SELECT
                    ID,
                    CUSTOMER_ID,
                    ADDRESS,
                    CITY,
                    PROVINCE,
                    CAST(CREATED_AT AS DATETIME(3)) AS CREATED_AT
                FROM stg_customer_address
                ON DUPLICATE KEY UPDATE
                address = VALUES(address),
                city = VALUES(city),
                province = VALUES(province),
                created_at = VALUES(created_at),
                load_timestamp = CURRENT_TIMESTAMP(6);
                """)
        conn.commit()

        # time.sleep(2)

    except Exception as e:
        conn.rollback()
        raise RuntimeError(f"Gagal insert/update data: {e}")

    finally:
        cursor.close()
        conn.close()

    print(f"TXF_CUSTOMER_ADDRESS loaded for date {ds}")

# DAG
with DAG(
    dag_id='task_ingest_txf_customer_address',
    default_args=default_args,
    schedule='0 8 * * *',  # jam 08:00
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['dwh'],
) as dag:
    ingest_txf_customer_address()

# with DAG(
#     dag_id='task_ingest_txf_customer_address',
#     default_args=default_args,
#     schedule=[customer_address_dataset],  # EVENT-BASED TRIGGER
#     start_date=datetime(2026, 1, 1),
#     catchup=False,
#     tags=['dwh'],
# ) as dag:
#     ingest_txf_customer_address()

# with DAG(
#     dag_id='task_ingest_txf_customer_address',
#     default_args=default_args,
#     schedule='@daily',
#     start_date=datetime(2026, 1, 1),
#     catchup=False,
#     tags=['dwh'],
# ) as dag:
#     wait_stg = ExternalTaskSensor(
#         task_id='wait_for_ingest_stg_customer_address',
#         external_dag_id='ingest_stgcustomer_address_csv',
#         external_task_id='ingest_csv_task',
#         poke_interval=30,
#         timeout=60 * 60,
#         mode='reschedule',
#         allowed_states=['success'],              # hanya lanjut kalau sukses
#         failed_states=['failed', 'skipped'],     # langsung fail kalau upstream gagal
#         execution_delta=timedelta(0),            # sinkronkan execution_date
#     )

#     task_txf = ingest_txf_customer_address()

#     wait_stg >> task_txf