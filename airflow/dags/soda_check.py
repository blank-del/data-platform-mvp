from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
from soda_core.contracts import verify_contract_locally
from soda_core import configure_logging
from pathlib import Path
import logging

# Set up logging
logger = logging.getLogger(__name__)

# Function to run Soda checks
def run_soda_checks(table_config: dict):
    try:
        configure_logging(verbose=True)
        contract_name = table_config["table"]
        
        # Use absolute paths for container
        soda_dir = Path("/opt/airflow/soda")
        contract_path = soda_dir / f"{contract_name}_contract.yml"

        logger.info(f"Soda directory: {soda_dir}")
        logger.info("Running Soda contract checks...")
        
        result = verify_contract_locally(
            data_source_file_path=str(soda_dir / "ds.yml"),
            contract_file_path=str(contract_path),
        )
        
        # Check if result is successful
        logger.info(f"Soda check has errors: {result.is_failed}")
        if result is None or result.is_failed:
            raise Exception(f"Soda checks for {contract_name} failed!")
        
        logger.info("Soda checks passed successfully!")
        
    except Exception as e:
        logger.error(f"Error running Soda checks: {e}")
        raise

# Define the DAG
with DAG(
    'soda_data_quality_checks',
    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2026, 1, 1),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    },
    description='Run Soda data quality checks',
    schedule='@daily',  # Adjust schedule as needed
    catchup=False,
) as dag:
    
    check_configs = [
        {"table": "order_created"},
        {"table": "order_completed"},
    ]
    task_soda_checks = PythonOperator.partial(
        task_id='run_soda_checks',
        python_callable=run_soda_checks,
        map_index_template="{{ task.op_kwargs['table_config']['table'] }}"
    ).expand(
        op_kwargs=[{"table_config": c} for c in check_configs]
    )