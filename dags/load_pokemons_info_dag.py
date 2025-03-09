from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum

from lib.api_functions import load_data


def get_engine():
    """Получаем SQLAlchemy engine через PostgresHook"""
    hook = PostgresHook(postgres_conn_id='warehouse')
    return hook.get_sqlalchemy_engine()


with DAG(
    dag_id='load_pokemons_info_dag',
    schedule_interval=None,
    start_date=pendulum.now(),
    catchup=False,
    max_active_runs=1,
    concurrency=2,
    tags=['pokemons', 'bronze']
) as dag:
    start_op = EmptyOperator(
        task_id='start'
    )

    load_types_op = PythonOperator(
        task_id='load_types',
        python_callable=load_data,
        op_kwargs={
            'engine': get_engine(),
            'file_name': 'types.csv',
            'table_name': 'types',
        }
    )

    load_pokemon_types_op = PythonOperator(
        task_id='load_pokemon_types',
        python_callable=load_data,
        op_kwargs={
            'engine': get_engine(),
            'file_name': 'pokemon_types.csv',
            'table_name': 'types',
        }
    )

    load_moves_op = PythonOperator(
        task_id='load_moves',
        python_callable=load_data,
        op_kwargs={
            'engine': get_engine(),
            'file_name': 'moves.csv',
            'table_name': 'moves',
        }
    )

    load_pokemon_moves_op = PythonOperator(
        task_id='load_pokemon_moves',
        python_callable=load_data,
        op_kwargs={
            'engine': get_engine(),
            'file_name': 'pokemon_moves.csv',
            'table_name': 'pokemon_moves',
        }
    )

    load_generations_op = PythonOperator(
        task_id='load_generations',
        python_callable=load_data,
        op_kwargs={
            'engine': get_engine(),
            'file_name': 'generations.csv',
            'table_name': 'generations',
        }
    )

    load_generations_species_op = PythonOperator(
        task_id='load_generations_species',
        python_callable=load_data,
        op_kwargs={
            'engine': get_engine(),
            'file_name': 'generation_species.csv',
            'table_name': 'generation_species',
        }
    )

    load_pokemons_species_op = PythonOperator(
        task_id='load_pokemons_species',
        python_callable=load_data,
        op_kwargs={
            'engine': get_engine(),
            'file_name': 'pokemon_species.csv',
            'table_name': 'pokemon_species',
        }
    )

    load_pokemons_op = PythonOperator(
        task_id='load_pokemons',
        python_callable=load_data,
        op_kwargs={
            'engine': get_engine(),
            'file_name': 'pokemons.csv',
            'table_name': 'pokemons',
        }
    )

    load_pokemons_stats_op = PythonOperator(
        task_id='load_pokemons_stats',
        python_callable=load_data,
        op_kwargs={
            'engine': get_engine(),
            'file_name': 'pokemon_stats.csv',
            'table_name': 'pokemon_stats',
        }
    )

    finish_op = EmptyOperator(
        task_id='finish'
    )

    trigger_child_dag_op = TriggerDagRunOperator(
        task_id='trigger_sync_golden_layer_dag',
        trigger_dag_id='sync_golden_layer_dag',
        wait_for_completion=False
    )

    start_op >> [load_types_op, load_moves_op, load_generations_op, load_pokemons_op]

    load_types_op >> load_pokemon_types_op
    load_moves_op >> load_pokemon_moves_op
    load_generations_op >> load_generations_species_op >> load_pokemons_species_op
    load_pokemons_op >> load_pokemons_stats_op

    [load_pokemon_types_op, load_pokemon_moves_op, load_pokemons_species_op, load_pokemons_stats_op] >> finish_op

    finish_op >> trigger_child_dag_op
