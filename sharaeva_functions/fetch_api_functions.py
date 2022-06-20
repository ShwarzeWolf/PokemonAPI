import logging

import pandas as pd

from sharaeva_functions.utils import get_data_by_url, save_file_into_S3, read_file_from_S3


def _get_pokemons():
    """Gets list of pokemons and loads it into separate file"""
    url = 'https://pokeapi.co/api/v2/pokemon'
    pokemons_count = get_data_by_url(url)['count']

    url = f'https://pokeapi.co/api/v2/pokemon?offset=0&limit={pokemons_count}'
    pokemons_raw = get_data_by_url(url)['results']

    pokemons = pd.json_normalize(pokemons_raw)
    save_file_into_S3('pokemons.csv', pokemons)


def _get_pokemons_stats():
    """Gets list of pokemons from API and loads the into pokemon_stats file
    Will move to _get_pokemons dag in Airflow"""
    pokemons = read_file_from_S3('pokemons.csv')

    pokemon_stats_chunks = []

    for index, pokemon in pokemons.iterrows():
        url = pokemon['url']
        raw_pokemon_stats = get_data_by_url(url)['stats']

        pokemon_stats_chunk = pd.json_normalize(raw_pokemon_stats) \
                                .rename(columns={'base_stat': 'value'})
        pokemon_stats_chunk['pokemon'] = pokemon['name']

        pokemon_stats_chunks.append(pokemon_stats_chunk)

    pokemons_stats = pd.concat(pokemon_stats_chunks)
    save_file_into_S3(filename='pokemons_stats.csv', data=pokemons_stats)


def _get_types():
    """Gets list of types and loads it into separate file"""
    url = 'https://pokeapi.co/api/v2/type'
    types_count = get_data_by_url(url)['count']

    url = f'https://pokeapi.co/api/v2/type?offset=0&limit={types_count}'
    types_raw = get_data_by_url(url)['results']

    types = pd.json_normalize(types_raw)
    save_file_into_S3('types.csv', types)


def _get_pokemon_types():
    """Loads all types and pokemons from Pokemon API into pokemon_types CSV file.
       Will migrate to get_types task in DAG"""
    types = read_file_from_S3('types.csv')

    pokemons_types_chunks = []

    for index, _type in types.iterrows():
        url = _type['url']

        response = get_data_by_url(url)['pokemon']
        pokemons_type = pd.json_normalize(response)

        if not pokemons_type.empty:
            pokemons_type['type'] = _type['name']

            # TODO на подумать - так ли нам нужен id или name в этой таблице?
            #  Там же функциональная зависимость между ними
            pokemons_type['id'] = pokemons_type.apply(lambda row: row['pokemon.url'].split('/')[-2], axis=1)
            pokemons_type.rename(columns={'pokemon.name': 'name'}, inplace=True)
            pokemons_type.drop(columns=['slot', 'pokemon.url'], inplace=True)

            pokemons_types_chunks.append(pokemons_type)

    pokemons_types = pd.concat(pokemons_types_chunks)
    save_file_into_S3('pokemons_types.csv', pokemons_types)



def _get_generations():
    """Gets list of generations and loads it into separate file"""
    url = 'https://pokeapi.co/api/v2/generation'
    generations_count = get_data_by_url(url)['count']

    url = f'https://pokeapi.co/api/v2/generation?offset=0&limit={generations_count}'
    generations_raw = get_data_by_url(url)['results']

    generations = pd.json_normalize(generations_raw)
    save_file_into_S3('generations.csv', generations)


def _get_generations_species():
    """Gets list of generation from pokemon API and logs info about generation count
       Will migrate _to get_generations task in DAG"""
    generations = read_file_from_S3('generations.csv')

    pokemon_generations_chunks = []

    for index, generation in generations.iterrows():
        url = generation['url']
        response = get_data_by_url(url)['pokemon_species']
        pokemon_species = pd.json_normalize(response)

        pokemon_species['generation'] = generation['name']
        pokemon_species['id'] = pokemon_species.apply(lambda row: row['url'].split('/')[-2], axis=1)
        pokemon_generations_chunks.append(pokemon_species)

    generations_species = pd.concat(pokemon_generations_chunks)
    save_file_into_S3('generations_species.csv', generations_species)


def _get_pokemons_species():
    """Gets all pokemons from pokemon species"""
    pokemon_species = read_file_from_S3('generations_species.csv')['url'].unique()

    species_chunks = []

    for specie in pokemon_species:
        response = get_data_by_url(specie)['varieties']
        pokemon_species = pd.json_normalize(response)
        pokemon_species['specie_id'] = specie.split('/')[-2]
        species_chunks.append(pokemon_species)

    pokemon_species = pd.concat(species_chunks)
    save_file_into_S3('pokemons_species.csv', pokemon_species)


def _get_moves():
    """Gets list of moves and loads it into separate file"""
    url = 'https://pokeapi.co/api/v2/move'
    moves_count = get_data_by_url(url)['count']

    url = f'https://pokeapi.co/api/v2/move?offset=0&limit={moves_count}'
    moves_raw = get_data_by_url(url)['results']

    moves = pd.json_normalize(moves_raw)
    save_file_into_S3('moves.csv', moves)


def _get_pokemon_moves():
    """Gets all moves and pokemons from pokemon API"""
    moves = read_file_from_S3('moves.csv')

    pokemon_moves_chunks = []

    for index, move in moves.iterrows():
        url = move['url']
        response = get_data_by_url(url)['learned_by_pokemon']

        pokemon_moves_chunk = pd.json_normalize(response)

        if not pokemon_moves_chunk.empty:
            pokemon_moves_chunk['move'] = move['name']
            pokemon_moves_chunk['pokemon_id'] = pokemon_moves_chunk.apply(lambda row: row['url'].split('/')[-2], axis=1)
            pokemon_moves_chunk.rename(columns={'name': 'pokemon_name'}, inplace=True)
            pokemon_moves_chunks.append(pokemon_moves_chunk)

    pokemon_moves = pd.concat(pokemon_moves_chunks)
    save_file_into_S3('pokemons_moves.csv', pokemon_moves)


def _check_generations_count():
    """Gets list of generation from pokemon API and logs info about generation count
       Will migrate to check_generations_count task in DAG"""
    url = 'https://pokeapi.co/api/v2/generation'
    generations = get_data_by_url(url)

    logging.info(f'Today exist {generations["count"]} generations')
