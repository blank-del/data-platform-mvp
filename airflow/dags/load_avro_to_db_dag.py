from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import duckdb
from pathlib import Path
import logging

# Set up logging
logger = logging.getLogger(__name__)

# Path to Avro files
ORDER_CREATED_AVRO = "/opt/airflow/data_avro/orders_created_*.avro"
ORDER_COMPLETED_AVRO = "/opt/airflow/data_avro/orders_completed_*.avro"
DUCK_DB_PATH = "/opt/airflow/dbt/mvp.duckdb"
# Function to load order_created table
def load_order_created():
    try:
        con = duckdb.connect(DUCK_DB_PATH)
        con.sql("CREATE SCHEMA IF NOT EXISTS raw")
        
        tables = [t[0] for t in con.sql("SHOW TABLES FROM raw").fetchall()]
        if "order_created" not in tables:
            logger.info(f"Creating table order_created from {ORDER_CREATED_AVRO}")
            con.sql(f"CREATE TABLE raw.order_created AS SELECT * FROM read_avro('{ORDER_CREATED_AVRO}')")
        else:
            logger.info(f"Appending into order_created from {ORDER_CREATED_AVRO}")
            con.sql(f"INSERT INTO raw.order_created SELECT * FROM read_avro('{ORDER_CREATED_AVRO}')")
        con.close()
        logger.info("Loaded order_created successfully")
    except Exception as e:
        logger.error(f"Error loading order_created: {e}")
        raise

# Function to load order_completed table
def load_order_completed():
    try:
        con = duckdb.connect(DUCK_DB_PATH)
        con.sql("CREATE SCHEMA IF NOT EXISTS raw")
        
        tables = [t[0] for t in con.sql("SHOW TABLES FROM raw").fetchall()]
        if "order_completed" not in tables:
            logger.info(f"Creating table order_completed from {ORDER_COMPLETED_AVRO}")
            con.sql(f"CREATE TABLE raw.order_completed AS SELECT * FROM read_avro('{ORDER_COMPLETED_AVRO}')")
        else:
            logger.info(f"Appending into order_completed from {ORDER_COMPLETED_AVRO}")
            con.sql(f"INSERT INTO raw.order_completed SELECT * FROM read_avro('{ORDER_COMPLETED_AVRO}')")
        
        con.close()
        logger.info("Loaded order_completed successfully")
    except Exception as e:
        logger.error(f"Error loading order_completed: {e}")
        raise

# Optional: Verification task (runs after both loads)
def verify_load():
    try:
        con = duckdb.connect(DUCK_DB_PATH)
        
        logger.info("Tables in DuckDB:")
        logger.info(con.sql("SHOW TABLES").fetchall())
        
        logger.info("Schema for raw.order_created:")
        logger.info(con.sql("DESCRIBE raw.order_created").fetchall())
        
        logger.info("Schema for raw.order_completed:")
        logger.info(con.sql("DESCRIBE raw.order_completed").fetchall())
        
        created_count = con.sql("SELECT count(*) FROM raw.order_created").fetchone()[0]
        completed_count = con.sql("SELECT count(*) FROM raw.order_completed").fetchone()[0]
        
        logger.info(f"Row count → raw.order_created: {created_count}")
        logger.info(f"Row count → raw.order_completed: {completed_count}")
        
        logger.info("Latest 5 created orders:")
        logger.info(con.sql("SELECT * FROM raw.order_created ORDER BY created_at DESC LIMIT 5").fetchall())
        
        logger.info("Latest 5 completed orders:")
        logger.info(con.sql("SELECT * FROM raw.order_completed ORDER BY completed_at DESC LIMIT 5").fetchall())
        
        con.close()
        logger.info("Verification complete")
    except Exception as e:
        logger.error(f"Error during verification: {e}")
        raise


# Define the DAG
with DAG(
    'load_avro_to_duckdb',

    # Default arguments for the DAG
    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2026, 1, 1),  # Adjust as needed
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=2),
    },
    description='Load Avro files into DuckDB tables',
    schedule='@once',  # Manual trigger; change to '@daily' for daily runs
    catchup=False,
) as dag:
    # Define tasks
    task_load_created = PythonOperator(
        task_id='load_order_created',
        python_callable=load_order_created,
        dag=dag,
    )

    task_load_completed = PythonOperator(
        task_id='load_order_completed',
        python_callable=load_order_completed,
        dag=dag,
    )

    task_verify = PythonOperator(
        task_id='verify_load',
        python_callable=verify_load,
        dag=dag,
    )
# Set dependencies (parallel loads, then verify)
# since duck db allows only one connection at a time, we run sequentially
task_load_created >> task_load_completed >> task_verify