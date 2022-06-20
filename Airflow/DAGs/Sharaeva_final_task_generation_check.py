from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from sharaeva_functions.fetch_api_functions import _check_generations_count
from sharaeva_functions.utils import _process_success, _process_failure

with DAG(
        dag_id='Sharaeva_final_task_generation_check',
        schedule_interval="@daily",
        start_date=days_ago(2),
        catchup=False,
        max_active_runs=1,
        tags=['de_school', 'sharaeva', 'final_task']
) as dag:
    check_generations_count = PythonOperator(
        task_id='check_generations_count',
        python_callable=_check_generations_count,
    )

    success = PythonOperator(
        task_id='success',
        python_callable=_process_success,
    )

    failure = PythonOperator(
        task_id='failure',
        python_callable=_process_failure,
        trigger_rule='one_failed'
    )

    check_generations_count >> [success, failure]


