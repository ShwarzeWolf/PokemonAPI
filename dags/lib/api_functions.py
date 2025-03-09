import logging

import pandas as pd
import requests


def get_data_by_url(url):
    """Returns results by provided url"""
    data = requests.get(url).json()
    return data


def dump_pokemons(engine):
    """Gets list of pokemons and loads it into the database"""
    url = 'https://pokeapi.co/api/v2/pokemon'
    pokemons_count = get_data_by_url(url)['count']

    url = f'https://pokeapi.co/api/v2/pokemon?offset=0&limit={pokemons_count}'
    pokemons_raw = get_data_by_url(url)['results']
    pokemons_df = pd.json_normalize(pokemons_raw)

    pokemons_df.to_sql('pokemons', engine, if_exists='replace', index=False, schema='silver')
    logging.info('Pokemons successfully ingested into the database')


def dump_pokemon_stats(engine):
    """Gets list of pokemons and loads their stats into the database"""
    query = "SELECT name, url FROM pokemons"
    pokemons = pd.read_sql(query, engine)

    pokemons_stats_chunks = []

    for _, pokemon in pokemons.iterrows():
        url = pokemon['url']
        raw_pokemon_stats = get_data_by_url(url)['stats']
        pokemons_stats_chunk = pd.json_normalize(raw_pokemon_stats)

        if not pokemons_stats_chunk.empty:
            pokemons_stats_chunk['pokemon'] = pokemon['name']
            pokemons_stats_chunk.rename(columns={'base_stat': 'power', 'stat.name': 'stat'}, inplace=True)
            pokemons_stats_chunk.drop(columns=['effort', 'stat.url'], inplace=True)

            pokemons_stats_chunks.append(pokemons_stats_chunk)

    pokemons_stats = pd.concat(pokemons_stats_chunks)
    pokemons_stats.to_sql('pokemon_stats', engine, if_exists='replace', index=False, schema='silver')
    logging.info('Pokemon stats successfully ingested into the database')


def dump_types(engine):
    """Gets list of types and loads them into the database"""
    url = 'https://pokeapi.co/api/v2/type'
    types_count = get_data_by_url(url)['count']

    url = f'https://pokeapi.co/api/v2/type?offset=0&limit={types_count}'
    types_raw = get_data_by_url(url)['results']
    types = pd.json_normalize(types_raw)

    types.to_sql('types', engine, if_exists='replace', index=False, schema='silver')
    logging.info('Types successfully ingested into the database')


def dump_pokemon_types(engine):
    """Loads all types and pokemons into the database"""
    query = "SELECT name, url FROM types"
    types = pd.read_sql(query, engine)

    pokemons_types_chunks = []

    for _, _type in types.iterrows():
        url = _type['url']
        response = get_data_by_url(url)['pokemon']
        pokemons_types_chunk = pd.json_normalize(response)

        if not pokemons_types_chunk.empty:
            pokemons_types_chunk['type'] = _type['name']
            pokemons_types_chunk.rename(columns={'pokemon.name': 'pokemon'}, inplace=True)
            pokemons_types_chunk.drop(columns=['slot', 'pokemon.url'], inplace=True)
            pokemons_types_chunks.append(pokemons_types_chunk)

    pokemons_types = pd.concat(pokemons_types_chunks)
    pokemons_types.to_sql('pokemon_types', engine, if_exists='replace', index=False, schema='silver')
    logging.info('Pokemon types successfully ingested into the database')


def dump_moves(engine):
    """Gets list of moves and loads them into the database"""
    url = 'https://pokeapi.co/api/v2/move'
    moves_count = get_data_by_url(url)['count']

    url = f'https://pokeapi.co/api/v2/move?offset=0&limit={moves_count}'
    moves_raw = get_data_by_url(url)['results']
    moves = pd.json_normalize(moves_raw)

    moves.to_sql('moves', engine, if_exists='replace', index=False, schema='silver')
    logging.info('Moves successfully ingested into the database')


def dump_pokemon_moves(engine):
    """Gets all moves and pokemons from API and loads into the database"""
    query = "SELECT name, url FROM moves"
    moves = pd.read_sql(query, engine)

    pokemons_moves_chunks = []

    for _, move in moves.iterrows():
        url = move['url']
        response = get_data_by_url(url)['learned_by_pokemon']
        pokemons_moves_chunk = pd.json_normalize(response)

        if not pokemons_moves_chunk.empty:
            pokemons_moves_chunk['move'] = move['name']
            pokemons_moves_chunk.rename(columns={'name': 'pokemon'}, inplace=True)
            pokemons_moves_chunk.drop(columns=['url'], inplace=True)
            pokemons_moves_chunks.append(pokemons_moves_chunk)

    pokemon_moves = pd.concat(pokemons_moves_chunks)
    pokemon_moves.to_sql('pokemon_moves', engine, if_exists='replace', index=False, schema='silver')
    logging.info('Pokemon moves successfully ingested into the database')

def dump_generations(engine):
    """Gets list of generations and loads them into the database"""
    url = 'https://pokeapi.co/api/v2/generation'
    generations_count = get_data_by_url(url)['count']

    url = f'https://pokeapi.co/api/v2/generation?offset=0&limit={generations_count}'
    generations_raw = get_data_by_url(url)['results']
    generations = pd.json_normalize(generations_raw)

    generations.to_sql('generations', engine, if_exists='replace', index=False, schema='silver')
    logging.info('Generations successfully ingested into the database')

def dump_generation_species(engine):
    """Gets list of generation species and loads them into the database"""
    query = "SELECT name, url FROM generations"
    generations = pd.read_sql(query, engine)

    pokemons_generations_chunks = []

    for _, generation in generations.iterrows():
        url = generation['url']
        response = get_data_by_url(url)['pokemon_species']
        pokemons_species_chunk = pd.json_normalize(response)

        if not pokemons_species_chunk.empty:
            pokemons_species_chunk['generation'] = generation['name']
            pokemons_species_chunk['specie_id'] = pokemons_species_chunk['url'].apply(lambda x: x.split('/')[-2])
            pokemons_species_chunk.rename(columns={'name': 'specie_name', 'url': 'specie_url'}, inplace=True)
            pokemons_generations_chunks.append(pokemons_species_chunk)

    generations_species = pd.concat(pokemons_generations_chunks)
    generations_species.to_sql('generations_species', engine, if_exists='replace', index=False, schema='silver')
    logging.info('Generation species successfully ingested into the database')

def dump_pokemon_species(engine):
    """Gets all pokemons from pokemon species and loads them into the database"""
    query = "SELECT specie_url FROM generations_species"
    species = pd.read_sql(query, engine)['specie_url'].unique()

    pokemons_species_chunks = []

    for specie in species:
        response = get_data_by_url(specie)['varieties']
        pokemons_species_chunk = pd.json_normalize(response)

        pokemons_species_chunk['specie_id'] = specie.split('/')[-2]
        pokemons_species_chunk.rename(columns={'pokemon.name': 'pokemon'}, inplace=True)
        pokemons_species_chunk.drop(columns=['is_default', 'pokemon.url'], inplace=True)
        pokemons_species_chunks.append(pokemons_species_chunk)

    pokemons_species = pd.concat(pokemons_species_chunks)
    pokemons_species.to_sql('pokemon_species', engine, if_exists='replace', index=False, schema='silver')
    logging.info('Pokemon species successfully ingested into the database')


def _check_generations_count():
    """Logs the count of generations from the API"""
    url = 'https://pokeapi.co/api/v2/generation'
    generations = get_data_by_url(url)
    logging.info(f'Today exist {generations["count"]} generations')