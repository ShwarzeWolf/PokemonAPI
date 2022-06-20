import logging

import pandas as pd

from utils.utils import get_data_by_url


def _get_pokemons():
    """Gets list of pokemons and loads it into separate file"""
    url = 'https://pokeapi.co/api/v2/pokemon'
    pokemons_count = get_data_by_url(url)['count']

    url = f'https://pokeapi.co/api/v2/pokemon?offset=0&limit={pokemons_count}'
    pokemons_raw = get_data_by_url(url)['results']

    pokemons = pd.json_normalize(pokemons_raw)
    pokemons.to_csv('./pokemons.csv', index=False, header=True)


def _get_pokemons_stats():
    """Gets list of pokemons from API and loads the into pokemon_stats file
    Will move to _get_pokemons dag in Airflow"""
    url = 'pokemons.csv'
    pokemons = pd.read_csv(url)

    pokemon_stats_chunks = []

    for index, pokemon in pokemons.iterrows():
        url = pokemon['url']
        raw_pokemon_stats = get_data_by_url(url)['stats']

        pokemon_stats_chunk = pd.json_normalize(raw_pokemon_stats) \
                                .rename(columns={'base_stat': 'value'})
        pokemon_stats_chunk['pokemon'] = pokemon['name']

        pokemon_stats_chunks.append(pokemon_stats_chunk)

    pokemon_stats = pd.concat(pokemon_stats_chunks)
    pokemon_stats.to_csv('./pokemon_stats.csv', index=False, header=True)


def _get_types():
    """Gets list of types and loads it into separate file"""
    url = 'https://pokeapi.co/api/v2/type'
    types_count = get_data_by_url(url)['count']

    url = f'https://pokeapi.co/api/v2/type?offset=0&limit={types_count}'
    types_raw = get_data_by_url(url)['results']

    types = pd.json_normalize(types_raw)
    types.to_csv('./types.csv', index=False, header=True)


def _get_pokemon_types():
    """Loads all types and pokemons from Pokemon API into pokemon_types CSV file.
       Will migrate to get_types task in DAG"""
    url = 'types.csv'
    types = pd.read_csv(url)

    pokemon_types_chunks = []

    for index, _type in types.iterrows():
        url = _type['url']

        response = get_data_by_url(url)['pokemon']
        pokemon_type = pd.json_normalize(response)

        if not pokemon_type.empty:
            pokemon_type['type'] = _type['name']

            # TODO на подумать - так ли нам нужен id или name в этой таблице?
            #  Там же функциональная зависимость между ними
            pokemon_type['id'] = pokemon_type.apply(lambda row: row['pokemon.url'].split('/')[-2], axis=1)
            pokemon_type.rename(columns={'pokemon.name': 'name'}, inplace=True)
            pokemon_type.drop(columns=['slot', 'pokemon.url'], inplace=True)

            pokemon_types_chunks.append(pokemon_type)

    pokemon_types = pd.concat(pokemon_types_chunks)
    pokemon_types.to_csv('./pokemon_types.csv', index=False, header=True)


def _get_generations():
    """Gets list of generations and loads it into separate file"""
    url = 'https://pokeapi.co/api/v2/generation'
    generations_count = get_data_by_url(url)['count']

    url = f'https://pokeapi.co/api/v2/generation?offset=0&limit={generations_count}'
    generations_raw = get_data_by_url(url)['results']

    generations = pd.json_normalize(generations_raw)
    generations.to_csv('./generations.csv', index=False, header=True)


def _get_generations_species():
    """Gets list of generation from pokemon API and logs info about generation count
       Will migrate _to get_generations task in DAG"""
    url = 'generations.csv'
    generations = pd.read_csv(url)

    pokemon_generations_chunks = []

    for index, generation in generations.iterrows():
        url = generation['url']
        response = get_data_by_url(url)['pokemon_species']
        pokemon_species = pd.json_normalize(response)

        pokemon_species['generation'] = generation['name']
        pokemon_species['id'] = pokemon_species.apply(lambda row: row['url'].split('/')[-2], axis=1)
        pokemon_generations_chunks.append(pokemon_species)

    pokemon_generations = pd.concat(pokemon_generations_chunks)
    pokemon_generations.to_csv('./generations_species.csv', index=False, header=True)


def _get_pokemons_species():
    """Gets all pokemons from pokemon species"""
    url = 'generations_species.csv'
    pokemon_species = pd.read_csv(url)['url'].unique()

    species_chunks = []

    for specie in pokemon_species:
        response = get_data_by_url(specie)['varieties']
        pokemon_species = pd.json_normalize(response)
        pokemon_species['specie_id'] = specie.split('/')[-2]
        species_chunks.append(pokemon_species)

    pokemon_species = pd.concat(species_chunks)
    pokemon_species.to_csv('./pokemons_species.csv', index=False, header=True)


def _get_moves():
    """Gets list of moves and loads it into separate file"""
    url = 'https://pokeapi.co/api/v2/move'
    moves_count = get_data_by_url(url)['count']

    url = f'https://pokeapi.co/api/v2/move?offset=0&limit={moves_count}'
    moves_raw = get_data_by_url(url)['results']

    types = pd.json_normalize(moves_raw)
    types.to_csv('./moves.csv', index=False, header=True)


def _get_pokemon_moves():
    """Gets all moves and pokemons from pokemon API"""
    url = 'moves.csv'
    moves = pd.read_csv(url)

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
    pokemon_moves.to_csv('./pokemon_moves.csv', index=False, header=True)


def _check_generations_count():
    """Gets list of generation from pokemon API and logs info about generation count
       Will migrate to check_generations_count task in DAG"""
    url = 'https://pokeapi.co/api/v2/generation'
    generations = get_data_by_url(url)

    logging.info(f'Today exist {generations["count"]} generations')
