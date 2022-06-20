from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from sharaeva_functions.utils import _cleanup, _process_success, _process_failure

with DAG(
        dag_id='Sharaeva_final_task_cleanup',
        schedule_interval=None,
        start_date=days_ago(2),
        catchup=False,
        max_active_runs=1,
        tags=['de_school', 'sharaeva', 'final_task']
) as dag:
    cleanup = PythonOperator(
        task_id='cleanup',
        python_callable=_cleanup,
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

    cleanup >> [success, failure]

