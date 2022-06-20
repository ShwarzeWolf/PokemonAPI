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

    save_file_into_S3(filename='pokemons.csv', data=pokemons)


def _get_pokemons_stats():
    """Gets list of pokemons from API and loads the into pokemon_stats file
    Will move to _get_pokemons dag in Airflow"""
    pokemons = read_file_from_S3(filename='pokemons.csv')

    pokemons_stats_chunks = []

    for index, pokemon in pokemons.iterrows():
        url = pokemon['url']
        raw_pokemon_stats = get_data_by_url(url)['stats']

        pokemons_stats_chunk = pd.json_normalize(raw_pokemon_stats)

        if not pokemons_stats_chunk.empty:
            pokemons_stats_chunk['pokemon'] = pokemon['name']
            pokemons_stats_chunk.rename(columns={'base_stat': 'power', 'stat.name': 'stat'})
            pokemons_stats_chunk.drop(columns=['effort', 'stat.url'], inplace=True)

            pokemons_stats_chunks.append(pokemons_stats_chunk)

    pokemons_stats = pd.concat(pokemons_stats_chunks)
    save_file_into_S3(filename='pokemons_stats.csv', data=pokemons_stats)


def _get_types():
    """Gets list of types and loads it into separate file"""
    url = 'https://pokeapi.co/api/v2/type'
    types_count = get_data_by_url(url)['count']

    url = f'https://pokeapi.co/api/v2/type?offset=0&limit={types_count}'
    types_raw = get_data_by_url(url)['results']
    types = pd.json_normalize(types_raw)

    save_file_into_S3(filename='types.csv', data=types)


def _get_pokemon_types():
    """Loads all types and pokemons from Pokemon API into pokemon_types CSV file.
       Will migrate to get_types task in DAG"""
    types = read_file_from_S3(filename='types.csv')

    pokemons_types_chunks = []

    for index, _type in types.iterrows():
        url = _type['url']

        response = get_data_by_url(url)['pokemon']
        pokemons_types_chunk = pd.json_normalize(response)

        if not pokemons_types_chunk.empty:
            pokemons_types_chunk['type'] = _type['name']

            pokemons_types_chunk.rename(columns={'pokemon.name': 'pokemon'}, inplace=True)
            pokemons_types_chunk.drop(columns=['slot', 'pokemon.url'], inplace=True)

            pokemons_types_chunks.append(pokemons_types_chunk)

    pokemons_types = pd.concat(pokemons_types_chunks)
    save_file_into_S3(filename='pokemons_types.csv', data=pokemons_types)


def _get_generations():
    """Gets list of generations and loads it into separate file"""
    url = 'https://pokeapi.co/api/v2/generation'
    generations_count = get_data_by_url(url)['count']

    url = f'https://pokeapi.co/api/v2/generation?offset=0&limit={generations_count}'
    generations_raw = get_data_by_url(url)['results']
    generations = pd.json_normalize(generations_raw)

    save_file_into_S3(filename='generations.csv', data=generations)


def _get_generations_species():
    """Gets list of generation from pokemon API and logs info about generation count
       Will migrate _to get_generations task in DAG"""
    generations = read_file_from_S3(filename='generations.csv')

    pokemons_generations_chunks = []

    for index, generation in generations.iterrows():
        url = generation['url']
        response = get_data_by_url(url)['pokemon_species']
        pokemons_species_chunk = pd.json_normalize(response)

        if not pokemons_species_chunk.empty:
            pokemons_species_chunk['generation'] = generation['name']
            pokemons_species_chunk['specie_id'] = pokemons_species_chunk.apply(lambda row: row['url'].split('/')[-2], axis=1)
            pokemons_species_chunk.rename(columns={'name': 'specie_name', 'url': 'specie_url'}, inplace=True)

            pokemons_generations_chunks.append(pokemons_species_chunk)

    generations_species = pd.concat(pokemons_generations_chunks)
    save_file_into_S3(filename='generations_species.csv', data=generations_species)


def _get_pokemons_species():
    """Gets all pokemons from pokemon species"""
    pokemons_species_chunk = read_file_from_S3(filename='generations_species.csv')['specie_url'].unique()

    pokemons_species_chunks = []

    for specie in pokemons_species_chunk:
        response = get_data_by_url(specie)['varieties']
        pokemons_species_chunk = pd.json_normalize(response)

        pokemons_species_chunk['specie_id'] = specie.split('/')[-2]
        pokemons_species_chunk.rename(columns={'pokemon.name': 'pokemon'}, inplace=True)
        pokemons_species_chunk.drop(columns=['is_default', 'pokemon.url'], inplace=True)

        pokemons_species_chunks.append(pokemons_species_chunk)

    pokemons_species = pd.concat(pokemons_species_chunks)
    save_file_into_S3(filename='pokemons_species.csv', data=pokemons_species)


def _get_moves():
    """Gets list of moves and loads it into separate file"""
    url = 'https://pokeapi.co/api/v2/move'
    moves_count = get_data_by_url(url)['count']

    url = f'https://pokeapi.co/api/v2/move?offset=0&limit={moves_count}'
    moves_raw = get_data_by_url(url)['results']
    moves = pd.json_normalize(moves_raw)

    save_file_into_S3(filename='moves.csv', data=moves)


def _get_pokemon_moves():
    """Gets all moves and pokemons from pokemon API"""
    moves = read_file_from_S3(filename='moves.csv')

    pokemons_moves_chunks = []

    for index, move in moves.iterrows():
        url = move['url']
        response = get_data_by_url(url)['learned_by_pokemon']

        pokemons_moves_chunk = pd.json_normalize(response)

        if not pokemons_moves_chunk.empty:
            pokemons_moves_chunk['move'] = move['name']
            pokemons_moves_chunk.rename(columns={'name': 'pokemon'}, inplace=True)
            pokemons_moves_chunk.drop(columns=['url'], inplace=True)

            pokemons_moves_chunks.append(pokemons_moves_chunk)

    pokemon_moves = pd.concat(pokemons_moves_chunks)
    save_file_into_S3(filename='pokemons_moves.csv', data=pokemon_moves)


def _check_generations_count():
    """Gets list of generation from pokemon API and logs info about generation count
       Will migrate to check_generations_count task in DAG"""
    url = 'https://pokeapi.co/api/v2/generation'
    generations = get_data_by_url(url)

    logging.info(f'Today exist {generations["count"]} generations')
