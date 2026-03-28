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
def create_sales_datamart():
    try:
        mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        conn = mysql_hook.get_conn()
        conn.ping()
        cursor = conn.cursor()
    except Exception as e:
        raise ConnectionError(f"Gagal konek ke MySQL: {str(e)}")

    try:
        # DDL
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS `MART_SALES_DATAMART` (
            PERIODE VARCHAR(7) NOT NULL,
            CLASS VARCHAR(10),
            MODEL VARCHAR(50),
            TOTAL DECIMAL(38,5),
            LOAD_TIMESTAMP DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
            PRIMARY KEY (PERIODE, CLASS, MODEL)
        );
        """)

        # 2. Truncate table (hapus semua data sebelumnya) -> TYPE LOAD TRUNCATE INSERT
        cursor.execute("TRUNCATE TABLE MART_SALES_DATAMART;")

        # 3. Insert hasil agregasi
        cursor.execute("""
        INSERT INTO MART_SALES_DATAMART (PERIODE, CLASS, MODEL, TOTAL)
        SELECT
            DATE_FORMAT(INVOICE_DATE, '%Y-%m') AS PERIODE,
            CASE
                WHEN PRICE BETWEEN 100000000 AND 250000000 THEN 'LOW'
                WHEN PRICE BETWEEN 250000001 AND 400000000 THEN 'MEDIUM'
                WHEN PRICE > 400000000 THEN 'HIGH'
            END AS CLASS,
            MODEL,
            SUM(PRICE) AS TOTAL
        FROM `mysql-dwh-dev`.TXF_SALES
        GROUP BY DATE_FORMAT(INVOICE_DATE, '%Y-%m'), CLASS, MODEL
        ORDER BY PERIODE, CLASS;
        """)

        conn.commit()
        print("MART_SALES_DATAMART successfully created and loaded")

    except Exception as e:
        conn.rollback()
        raise RuntimeError(f"Gagal membuat datamart: {e}")

    finally:
        cursor.close()
        conn.close()


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