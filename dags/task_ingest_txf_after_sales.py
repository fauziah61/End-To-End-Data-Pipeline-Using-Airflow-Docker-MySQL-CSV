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
def ingest_txf_after_sales(**context):
    # Ambil tanggal Airflow (manual / scheduler)
    input_date = context.get("params", {}).get("input_date")
    is_initial_load = context.get("params", {}).get("is_initial_load", False)
    ds = input_date if input_date else context['ds']  # YYYY-MM-DD
    ds_str = datetime.strptime(ds[:10], "%Y-%m-%d").strftime("%Y-%m-%d")

    try:
        mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        conn = mysql_hook.get_conn()
        conn.ping()
        cursor = conn.cursor()
    except Exception as e:
        raise ConnectionError(f"Gagal konek ke MySQL: {str(e)}")

    # CREATE TABLE
    try:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS `TXF_AFTER_SALES` (
            service_ticket VARCHAR(10),
            vin VARCHAR(50),
            customer_id VARCHAR(255),
            model VARCHAR(50),
            service_date DATE,
            service_type VARCHAR(5),
            created_at DATETIME(3),
            LOAD_TIMESTAMP DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
            PRIMARY KEY (SERVICE_TICKET)
        );
        """)

        # Duplicate handling
        if not is_initial_load:
            cursor.execute("""
            DELETE FROM TXF_SALES
            WHERE DATE(LOAD_TIMESTAMP) = %s
            """, (ds_str,))

        insert_sql_txf_sales = f"""
        INSERT INTO `TXF_AFTER_SALES` (
            SERVICE_TICKET, VIN, CUSTOMER_ID, MODEL, SERVICE_DATE, SERVICE_TYPE, CREATED_AT
        )
        SELECT
            SERVICE_TICKET,
            VIN,
            CUSTOMER_ID,
            MODEL,
            CAST(SERVICE_DATE AS DATE) AS SERVICE_DATE,
            SERVICE_TYPE,
            CAST(CREATED_AT AS DATETIME(3)) AS CREATED_AT
        FROM stg_after_sales_raw
        {"WHERE DATE(CREATED_AT) = %s" if not is_initial_load else ""};
        """
        params = (ds_str,) if not is_initial_load else None
        cursor.execute(insert_sql_txf_sales, params)

        conn.commit()

    except Exception as e:
        conn.rollback()
        raise RuntimeError(f"Gagal insert/update data: {e}")

    finally:
        cursor.close()
        conn.close()

    print(f"TXF_AFTER_SALES loaded for date {ds}")

# DAG
with DAG(
    dag_id='task_ingest_txf_after_sales',
    default_args=default_args,
    # schedule='@daily',
    schedule='0 8 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['dwh'],
) as dag:
    ingest_txf_after_sales()
