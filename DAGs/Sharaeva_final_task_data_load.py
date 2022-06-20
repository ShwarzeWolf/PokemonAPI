from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from sharaeva_functions.fetch_api_functions import _get_types, _get_pokemon_types, _get_pokemon_moves, _get_moves, \
    _get_generations, _get_generations_species, _get_pokemons_species, _get_pokemons, _get_pokemons_stats
from sharaeva_functions.utils import _process_success

with DAG(
        dag_id='Sharaeva_final_task_data_load',
        schedule_interval=None,
        start_date=days_ago(2),
        catchup=False,
        max_active_runs=1,
        concurrency=2,
        tags=['de_school', 'sharaeva', 'final_task']
) as dag:
    getting_types = PythonOperator(
        task_id='getting_types',
        python_callable=_get_types,
    )

    getting_pokemon_types = PythonOperator(
        task_id='getting_pokemon_types',
        python_callable=_get_pokemon_types,
    )

    getting_moves = PythonOperator(
        task_id='getting_moves',
        python_callable=_get_moves
    )

    getting_pokemon_moves = PythonOperator(
        task_id='getting_pokemon_moves',
        python_callable=_get_pokemon_moves,
    )

    getting_generations = PythonOperator(
        task_id='getting_generations',
        python_callable=_get_generations,
    )

    getting_generations_species = PythonOperator(
        task_id='getting_generations_species',
        python_callable=_get_generations_species
    )

    getting_pokemons_species = PythonOperator(
        task_id='getting_pokemons_species',
        python_callable=_get_pokemons_species
    )

    getting_pokemons = PythonOperator(
        task_id='getting_pokemons',
        python_callable=_get_pokemons
    )

    getting_pokemons_stats = PythonOperator(
        task_id='getting_pokemons_stats',
        python_callable=_get_pokemons_stats
    )

    success = PythonOperator(
        task_id='success',
        python_callable=_process_success,
    )

    getting_types >> getting_pokemon_types
    getting_moves >> getting_pokemon_moves
    getting_generations >> getting_generations_species >> getting_pokemons_species
    getting_pokemons >> getting_pokemons_stats

    [getting_pokemon_types, getting_pokemon_moves, getting_pokemons_species, getting_pokemons_stats] >> success
