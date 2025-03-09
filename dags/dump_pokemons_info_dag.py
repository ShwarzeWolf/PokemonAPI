from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum

from lib.api_functions import (
    dump_types,
    dump_pokemon_types,
    dump_pokemon_moves,
    dump_moves,
    dump_generations,
    dump_generation_species,
    dump_pokemon_species,
    dump_pokemons,
    dump_pokemon_stats,
)


def get_engine():
    """Получаем SQLAlchemy engine через PostgresHook"""
    hook = PostgresHook(postgres_conn_id='warehouse')
    return hook.get_sqlalchemy_engine()


with DAG(
    dag_id='dump_pokemons_info_dag',
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

    dump_types_op = PythonOperator(
        task_id='dump_types',
        python_callable=dump_types,
        op_kwargs={"engine": get_engine()}
    )

    dump_pokemon_types_op = PythonOperator(
        task_id='dump_pokemon_types',
        python_callable=dump_pokemon_types,
        op_kwargs={"engine": get_engine()},
    )

    dump_moves_op = PythonOperator(
        task_id='dump_moves',
        python_callable=dump_moves,
        op_kwargs={"engine": get_engine()},
    )

    dump_pokemon_moves_op = PythonOperator(
        task_id='dump_pokemon_moves',
        python_callable=dump_pokemon_moves,
        op_kwargs={"engine": get_engine()},
    )

    dump_generations_op = PythonOperator(
        task_id='dump_generations',
        python_callable=dump_generations,
        op_kwargs={"engine": get_engine()},
    )

    dump_generations_species_op = PythonOperator(
        task_id='dump_generations_species',
        python_callable=dump_generation_species,
        op_kwargs={"engine": get_engine()},
    )

    dump_pokemons_species_op = PythonOperator(
        task_id='dump_pokemons_species',
        python_callable=dump_pokemon_species,
        op_kwargs={"engine": get_engine()},
    )

    dump_pokemons_op = PythonOperator(
        task_id='dump_pokemons',
        python_callable=dump_pokemons,
        op_kwargs={"engine": get_engine()},
    )

    dump_pokemons_stats_op = PythonOperator(
        task_id='dump_pokemons_stats',
        python_callable=dump_pokemon_stats,
        op_kwargs={"engine": get_engine()},
    )

    finish_op = EmptyOperator(
        task_id='finish'
    )

    start_op >> [dump_types_op, dump_moves_op, dump_generations_op, dump_pokemons_op]

    dump_types_op >> dump_pokemon_types_op
    dump_moves_op >> dump_pokemon_moves_op
    dump_generations_op >> dump_generations_species_op >> dump_pokemons_species_op
    dump_pokemons_op >> dump_pokemons_stats_op

    [dump_pokemon_types_op, dump_pokemon_moves_op, dump_pokemons_species_op, dump_pokemons_stats_op] >> finish_op
