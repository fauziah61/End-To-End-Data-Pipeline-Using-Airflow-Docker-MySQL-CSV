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
def ingest_txf_sales(**context):
    # Ambil tanggal Airflow (manual / scheduler)
    input_date = context.get("params", {}).get("input_date")
    is_initial_load = context.get("params", {}).get("is_initial_load", False)
    ds = input_date if input_date else context['ds']  # YYYY-MM-DD
    ds_str = datetime.strptime(ds[:10], "%Y-%m-%d").strftime("%Y-%m-%d")

    # ds_str = datetime.strptime(ds, "%Y-%m-%d").strftime("%Y-%m-%d")

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
        CREATE TABLE IF NOT EXISTS `TXF_SALES` (
            VIN VARCHAR(25) NOT NULL,
            CUSTOMER_ID VARCHAR(255),
            MODEL VARCHAR(50),
            INVOICE_DATE DATE NOT NULL,
            PRICE DECIMAL(38,5),
            CREATED_AT DATETIME(3),
            LOAD_TIMESTAMP DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
            PRIMARY KEY (VIN, INVOICE_DATE),
            INDEX idx_invdate_customer (INVOICE_DATE, CUSTOMER_ID),
            INDEX idx_customer_id (CUSTOMER_ID),
            INDEX idx_invdate (INVOICE_DATE)
        )
        PARTITION BY RANGE (TO_DAYS(INVOICE_DATE)) (
            PARTITION p2022 VALUES LESS THAN (TO_DAYS('2023-01-01')),
            PARTITION p2023 VALUES LESS THAN (TO_DAYS('2024-01-01')),
            PARTITION p2024 VALUES LESS THAN (TO_DAYS('2025-01-01')),
            PARTITION p2025 VALUES LESS THAN (TO_DAYS('2026-01-01')),
            PARTITION pmax VALUES LESS THAN MAXVALUE
        );
        """)

        # Duplicate handling
        if not is_initial_load:
            cursor.execute("""
            DELETE FROM TXF_SALES
            WHERE DATE(LOAD_TIMESTAMP) = %s
            """, (ds_str,))

        insert_sql_txf_sales = f"""
        INSERT INTO `TXF_SALES` (
            VIN, CUSTOMER_ID, MODEL, INVOICE_DATE, PRICE, CREATED_AT
        )
        SELECT
            VIN,
            CUSTOMER_ID,
            MODEL,
            CAST(INVOICE_DATE AS DATE) AS INVOICE_DATE,
            CAST(REPLACE(PRICE, '.', '') AS DECIMAL(38,5)) AS PRICE,
            CAST(CREATED_AT AS DATETIME(3)) AS CREATED_AT
        FROM stg_sales_raw
        {"WHERE DATE(CREATED_AT) = %s" if not is_initial_load else ""};
        """

        # Jika initial load di kolom json masukan contoh:
        #{
        # "input_date": "2026-03-27",
        # "is_initial_load": true
        # }
        
        params = (ds_str,) if not is_initial_load else None
        cursor.execute(insert_sql_txf_sales, params)

        conn.commit()

    except Exception as e:
        conn.rollback()
        raise RuntimeError(f"Gagal insert/update data: {e}")

    finally:
        cursor.close()
        conn.close()

    print(f"TXF_SALES loaded for date {ds}")

    # DAG
with DAG(
    dag_id='task_ingest_txf_sales',
    default_args=default_args,
    # schedule='@daily',
    schedule='0 8 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['dwh'],
) as dag:
    ingest_txf_sales()

