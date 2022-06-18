import logging
import requests
import pandas as pd

logging.basicConfig(
    format="%(asctime)s => %(filename)s => %(levelname)s => %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO
)


def _get_types():
    """Loads all types from Pokemon API into pokemon_types CSV file.
       Will migrate to get_types task in DAG"""
    url = 'https://pokeapi.co/api/v2/type'
    types = requests.get(url).json()['results']

    pokemon_types_chunks = []

    for _type in types:
        url = _type['url']

        response = requests.get(url).json()['pokemon']
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


def _check_generations_count():
    """Gets list of generation from pokemon API and logs info about generation count
       Will migrate to check_generations_count task in DAG"""
    url = 'https://pokeapi.co/api/v2/generation'
    generations = requests.get(url).json()

    logging.info(f'Today exist {generations["count"]} generations')


def _get_generations():
    """Gets list of generation from pokemon API and logs info about generation count
       Will migrate _to get_generations task in DAG"""
    url = 'https://pokeapi.co/api/v2/generation'
    generations = requests.get(url).json()['results']

    pokemon_generations_chunks = []

    for generation in generations:
        url = generation['url']
        response = requests.get(url).json()['pokemon_species']
        pokemon_species = pd.json_normalize(response)

        pokemon_species['generation'] = generation['name']
        pokemon_species['id'] = pokemon_species.apply(lambda row: row['url'].split('/')[-2], axis=1)
        pokemon_generations_chunks.append(pokemon_species)

    pokemon_generations = pd.concat(pokemon_generations_chunks)
    pokemon_generations.to_csv('./pokemon_generations.csv', index=False, header=True)


def _get_generations_species():
    """Gets all pokemons from pokemon species"""
    url = 'pokemon_generations.csv'
    pokemon_species = pd.read_csv(url)['url'].unique()

    species_chunks = []
    for specie in pokemon_species:
        response = requests.get(specie).json()['varieties']
        pokemon_species = pd.json_normalize(response)
        pokemon_species['specie_id'] = specie.split('/')[-2]
        species_chunks.append(pokemon_species)

    pokemon_species = pd.concat(species_chunks)
    pokemon_species.to_csv('./species_pokemons.csv', index=False, header=True)


def _get_moves():
    """Gets all moves from pokemon API"""
    url = 'https://pokeapi.co/api/v2/move'
    moves_count = requests.get(url).json()['count']

    url = f'https://pokeapi.co/api/v2/move?offset=0&limit={moves_count}'
    moves = requests.get(url).json()['results']

    pokemon_moves_chunks = []

    for move in moves:
        url = move['url']
        response = requests.get(url).json()['learned_by_pokemon']

        pokemon_moves_chunk = pd.json_normalize(response)

        if not pokemon_moves_chunk.empty:
            pokemon_moves_chunk['move'] = move['name']
            pokemon_moves_chunk['pokemon_id'] = pokemon_moves_chunk.apply(lambda row: row['url'].split('/')[-2], axis=1)
            pokemon_moves_chunk.rename(columns={'name': 'pokemon_name'}, inplace=True)
            pokemon_moves_chunks.append(pokemon_moves_chunk)

    pokemon_moves = pd.concat(pokemon_moves_chunks)
    pokemon_moves.to_csv('./pokemon_moves.csv', index=False, header=True)


def _get_pokemon_stats():
    """Gets list of pokemons from API and loads the into pokemon_stats file
    Will move to _get_pokemons_stats dag in Airflow"""
    url = 'https://pokeapi.co/api/v2/pokemon'
    pokemons_count = requests.get(url).json()['count']

    url = f'https://pokeapi.co/api/v2/pokemon?offset=0&limit={pokemons_count}'
    pokemons = requests.get(url).json()['results']

    pokemon_stats_chunks = []
    for pokemon in pokemons:
        url = pokemon['url']
        raw_pokemon_stats = requests.get(url).json()['stats']

        pokemon_stats_chunk = pd.json_normalize(raw_pokemon_stats) \
                                .rename(columns={'base_stat': 'value'})

        pokemon_stats_chunk['pokemon'] = pokemon['name']

        pokemon_stats_chunks.append(pokemon_stats_chunk)

    pokemon_stats = pd.concat(pokemon_stats_chunks)
    pokemon_stats.to_csv('./pokemon_stats.csv', index=False, header=True)


_get_pokemon_stats()

