from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum


def execute_sql_functions():
    """Executes a given SQL function in PostgreSQL."""
    sync_functions = (
        'golden.refresh_types_statistics',
        'golden.refresh_moves_statistics',
        'golden.refresh_stats_statistics',
        'golden.refresh_types_generations_statistics',
    )
    for sync_function in sync_functions:
        hook = PostgresHook(postgres_conn_id='warehouse')
        hook.run(f"SELECT {sync_function}();")



with DAG(
    dag_id='sync_golden_layer_dag',
    schedule_interval=None,
    start_date=pendulum.now(),
    catchup=False,
    max_active_runs=1,
    concurrency=2,
    tags=['pokemons', 'gold']
) as dag:
    start_op = EmptyOperator(
        task_id='start'
    )

    sync_golden_layer_op = PythonOperator(
        task_id='sync_golden_layer',
        python_callable=execute_sql_functions,
    )

    finish_op = EmptyOperator(
        task_id='finish'
    )

    start_op >> sync_golden_layer_op >> finish_op
