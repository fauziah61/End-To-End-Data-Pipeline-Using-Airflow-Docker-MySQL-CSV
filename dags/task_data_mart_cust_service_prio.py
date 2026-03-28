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
def create_cust_service_prio_datamart():
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
        CREATE TABLE IF NOT EXISTS `MART_CUST_SERVICE_PRIO_DATAMART` (
            periode INT,
            vin VARCHAR(50),
            customer_name VARCHAR(255),
            address VARCHAR(255),
            count_service INT,
            priority VARCHAR(10),
            LOAD_TIMESTAMP DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6)
        );
        """)

        # 2. Truncate table (hapus semua data sebelumnya) -> TYPE LOAD TRUNCATE INSERT
        cursor.execute("TRUNCATE TABLE MART_CUST_SERVICE_PRIO_DATAMART;")

        # 3. Insert hasil agregasi
        cursor.execute("""
        INSERT INTO MART_CUST_SERVICE_PRIO_DATAMART (PERIODE, VIN, CUSTOMER_NAME, ADDRESS, COUNT_SERVICE, PRIORITY)
        select
            sc.periode,
            sc.vin,
            c.name as customer_name,
            ca.address,
            sc.count_service,
            case
                when sc.count_service > 10 then 'HIGH'
                when sc.count_service between 5 and 10 then 'MED'
                else 'LOW'
            end as priority
        from
            (
            select
                year(service_date) as periode,
                vin,
                customer_id,
                COUNT(*) as count_service
            from
                `mysql-dwh-dev`.txf_after_sales
            group by
                year(service_date),
                vin,
                customer_id
        ) sc
        join `mysql-dwh-dev`.txf_customers c
            on
            sc.customer_id = c.id
        left join `mysql-dwh-dev`.txf_customer_address ca
            on
            sc.customer_id = ca.customer_id;
        """)

        conn.commit()
        print("MART_CUST_SERVICE_PRIO_DATAMART successfully created and loaded")

    except Exception as e:
        conn.rollback()
        raise RuntimeError(f"Gagal membuat datamart: {e}")

    finally:
        cursor.close()
        conn.close()


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