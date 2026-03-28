from airflow import DAG
# from airflow.operators.python import PythonOperator -> muncul warning karena sudah deprecated, diganti dengan import dari providers
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta
# from airflow.datasets import Dataset
import os
import pandas as pd
import time

# CONFIG
BASE_PATH = "/opt/airflow/data"
TABLE_NAME = "stg_customer_address"
MYSQL_CONN_ID = 'mysql-localhost'

# # DATASET (event)
# customer_address_dataset = Dataset("stg_customer_address")

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_file_path(ds):
    file_date = ds.replace('-', '')
    return os.path.join(BASE_PATH, f"customer_address_{file_date}.csv")

def wait_for_stable_file(file_path, wait_time=5, check_interval=1): # WFF: Wait For File untuk menghindari file yang belum selesai dikirim
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File tidak ditemukan: {file_path}")

    last_size = -1
    stable_time = 0

    while True:
        current_size = os.path.getsize(file_path)

        if current_size == last_size:
            stable_time += check_interval
        else:
            stable_time = 0
            last_size = current_size

        if stable_time >= wait_time:
            break

        time.sleep(check_interval)


def ingest_csv(**context):
    # ds = context['ds']
    # file_path = get_file_path(ds)
    input_date = context.get("params", {}).get("input_date")
    # Cara menjalankan input data manual -> di kolom json 
    # {
    # "input_date": "2026-03-26"
    # }

    ds = input_date if input_date else context['ds'] # Run by scheduler
    
    file_path = get_file_path(ds)

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File tidak ditemukan: {file_path}")

    # Tunggu file selesai ditulis (size stabil)
    wait_for_stable_file(file_path, wait_time=5)

    df = pd.read_csv(file_path, sep='\\|@~', engine='python')

    # Raise error jika koneksi gagal
    try:
        mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        conn = mysql_hook.get_conn()
        conn.ping()
        cursor = conn.cursor()
    except Exception as e:
        raise ConnectionError(f"Gagal konek ke MySQL: {str(e)}")

    # DDL
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS `{TABLE_NAME}` (
        id VARCHAR(255),
        customer_id VARCHAR(255) NOT NULL,
        address VARCHAR(255),
        city VARCHAR(50),
        province VARCHAR(25),
        created_at VARCHAR(50),
        load_timestamp TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6),
        PRIMARY KEY (customer_id)
    );
    """
    cursor.execute(create_table_sql)
    conn.commit()
    # UPSERT SCD Type 1 Karena tidak ada requirement untuk menyimpan history, 
    # jadi cukup update data lama dengan data baru jika terjadi duplikasi customer_id
    insert_sql = f"""
    INSERT INTO `{TABLE_NAME}` (
        id, customer_id, address, city, province, created_at
    ) VALUES (%s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        address = VALUES(address),
        city = VALUES(city),
        province = VALUES(province),
        created_at = VALUES(created_at),
        load_timestamp = CURRENT_TIMESTAMP(6);
    """

    data = list(df.itertuples(index=False, name=None))

    try:
        for row in data:
            cursor.execute(insert_sql, row)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise RuntimeError(f"Gagal insert/update data: {e}")
    finally:
        cursor.close()
        conn.close()

    print(f"{len(df)} rows processed (insert/update)")

with DAG(
    dag_id='ingest_stg_customer_address_csv',
    default_args=default_args,
    schedule='0 5 * * *',  # jam 05:00
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['dwh'],
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_csv_task',
        python_callable=ingest_csv
    )

# with DAG(
#     dag_id='ingest_stg_customer_address_csv',
#     default_args=default_args,
#     schedule='@daily',
#     start_date=datetime(2026, 1, 1),
#     catchup=False,
#     tags=['dwh'],
# ) as dag:

    # ingest_task = PythonOperator(
    #     task_id='ingest_csv_task',
    #     python_callable=ingest_csv,
    #     outlets=[customer_address_dataset]  # publish event
    # )

    # ingest_task = PythonOperator(
    #     task_id='ingest_csv_task',
    #     python_callable=ingest_csv
    # )