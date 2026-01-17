from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta


# define the DAG
with DAG(
    'run_dbt_models',

    # Default arguments for the DAG
    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2026, 1, 1),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    },
    description='Run dbt models to transform raw data',
    schedule='@once',  # Run after load_avro completes
    catchup=False,
) as dag:
    
    # Run dbt
    run_dbt = BashOperator(
        task_id='run_dbt_models',
        bash_command='cd /opt/airflow/dbt && dbt run',
    )
    
    # Dependencies
    run_dbt