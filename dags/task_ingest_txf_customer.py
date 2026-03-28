from airflow import DAG
from airflow.decorators import task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta

MYSQL_CONN_ID = 'mysql-localhost'

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@task
def ingest_txf_customers(**context):
    # Ambil tanggal Airflow (manual / scheduler)
    input_date = context.get("params", {}).get("input_date")
    ds = input_date if input_date else context['ds']  # YYYY-MM-DD

    try:
        mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        conn = mysql_hook.get_conn()
        conn.ping()
        cursor = conn.cursor()
    except Exception as e:
        raise ConnectionError(f"Gagal konek ke MySQL: {str(e)}")
    
    try:
        # CREATE TABLE
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS `TXF_CUSTOMERS` (
            ID VARCHAR(255) NOT NULL,
            NAME VARCHAR(255),
            DOB DATE,
            CREATED_AT DATETIME(6),
            LOAD_TIMESTAMP DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
            PRIMARY KEY (ID)
        );
        """)

        # Duplicate handling
        cursor.execute("""
        DELETE FROM TXF_CUSTOMERS
        WHERE DATE(LOAD_TIMESTAMP) = %s
        """, (ds,))

        # SELECT dulu dari staging
        cursor.execute("""
            INSERT INTO `TXF_CUSTOMERS` (id, name, dob, created_at)
            SELECT   
                ID,
                NAME,
                CAST(
                    CASE
                        WHEN dob REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN dob
                        WHEN dob REGEXP '^[0-9]{4}/[0-9]{2}/[0-9]{2}$' THEN STR_TO_DATE(dob, '%Y/%m/%d')
                        WHEN dob REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN STR_TO_DATE(dob, '%d/%m/%Y')
                        ELSE NULL
                    END AS DATE
                ),
                CAST(created_at AS DATETIME(3))
            FROM stg_customers_raw
            ON DUPLICATE KEY UPDATE
            NAME = VALUES(NAME),
            DOB = VALUES(DOB),
            CREATED_AT = VALUES(CREATED_AT),
            LOAD_TIMESTAMP = CURRENT_TIMESTAMP(6)                       ;
            """)

        conn.commit()

    except Exception as e:
        conn.rollback()
        raise RuntimeError(f"Gagal insert/update data: {e}")

    finally:
        cursor.close()
        conn.close()

    print(f"TXF_CUSTOMERS loaded for date {ds}")

    # DAG
with DAG(
    dag_id='task_ingest_txf_customer',
    default_args=default_args,
    # schedule='@daily',
    schedule='0 8 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['dwh'],
) as dag:
    ingest_txf_customers()