from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import pendulum
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

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
    )

    dump_pokemon_types_op = PythonOperator(
        task_id='dump_pokemon_types',
        python_callable=dump_pokemon_types,
    )

    dump_moves_op = PythonOperator(
        task_id='dump_moves',
        python_callable=dump_moves,
    )

    dump_pokemon_moves_op = PythonOperator(
        task_id='dump_pokemon_moves',
        python_callable=dump_pokemon_moves,
    )

    dump_generations_op = PythonOperator(
        task_id='dump_generations',
        python_callable=dump_generations,
    )

    dump_generations_species_op = PythonOperator(
        task_id='dump_generations_species',
        python_callable=dump_generation_species,
    )

    dump_pokemons_species_op = PythonOperator(
        task_id='dump_pokemons_species',
        python_callable=dump_pokemon_species,
    )

    dump_pokemons_op = PythonOperator(
        task_id='dump_pokemons',
        python_callable=dump_pokemons,
    )

    dump_pokemons_stats_op = PythonOperator(
        task_id='dump_pokemons_stats',
        python_callable=dump_pokemon_stats,
    )

    finish_op = EmptyOperator(
        task_id='finish'
    )

    trigger_child_dag_op = TriggerDagRunOperator(
        task_id='trigger_load_pokemons_info_dag',
        trigger_dag_id='load_pokemons_info_dag',
        wait_for_completion=False
    )

    start_op >> [dump_types_op, dump_moves_op, dump_generations_op, dump_pokemons_op]

    dump_types_op >> dump_pokemon_types_op
    dump_moves_op >> dump_pokemon_moves_op
    dump_generations_op >> dump_generations_species_op >> dump_pokemons_species_op
    dump_pokemons_op >> dump_pokemons_stats_op

    [dump_pokemon_types_op, dump_pokemon_moves_op, dump_pokemons_species_op, dump_pokemons_stats_op] >> finish_op

    finish_op >> trigger_child_dag_op
