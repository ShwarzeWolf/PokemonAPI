import logging
import pandas as pd
import requests
import boto3
from io import BytesIO


def get_data_by_url(url):
    """Returns results by provided url"""
    data = requests.get(url).json()
    return data


def upload_to_minio(bucket_name, file_name, data):
    """Uploads DataFrame to MinIO as a CSV file"""
    s3 = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='admin',
        aws_secret_access_key='adminadmin'
    )

    csv_buffer = BytesIO()
    data.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    s3.put_object(Bucket=bucket_name, Key=file_name, Body=csv_buffer.getvalue())
    logging.info(f'{file_name} successfully uploaded to MinIO')


def dump_pokemons():
    """Gets list of pokemons and uploads to MinIO"""
    url = 'https://pokeapi.co/api/v2/pokemon'
    pokemons_count = get_data_by_url(url)['count']

    url = f'https://pokeapi.co/api/v2/pokemon?offset=0&limit={pokemons_count}'
    pokemons_raw = get_data_by_url(url)['results']
    pokemons_df = pd.json_normalize(pokemons_raw)

    upload_to_minio('bronze', 'pokemons.csv', pokemons_df)


def dump_pokemon_stats():
    """Gets list of pokemons and uploads their stats to MinIO"""
    url = 'https://pokeapi.co/api/v2/pokemon'
    pokemons_count = get_data_by_url(url)['count']

    url = f'https://pokeapi.co/api/v2/pokemon?offset=0&limit={pokemons_count}'
    pokemons_raw = get_data_by_url(url)['results']
    pokemons = pd.json_normalize(pokemons_raw)

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

    if pokemons_stats_chunks:
        pokemons_stats = pd.concat(pokemons_stats_chunks)
        upload_to_minio('bronze', 'pokemon_stats.csv', pokemons_stats)


def dump_types():
    """Gets list of types and uploads to MinIO"""
    url = 'https://pokeapi.co/api/v2/type'
    types_count = get_data_by_url(url)['count']

    url = f'https://pokeapi.co/api/v2/type?offset=0&limit={types_count}'
    types_raw = get_data_by_url(url)['results']
    types = pd.json_normalize(types_raw)

    upload_to_minio('bronze', 'types.csv', types)


def dump_pokemon_types():
    """Gets list of pokemon types and uploads to MinIO"""
    url = 'https://pokeapi.co/api/v2/type'
    types_count = get_data_by_url(url)['count']

    url = f'https://pokeapi.co/api/v2/type?offset=0&limit={types_count}'
    types_raw = get_data_by_url(url)['results']
    types = pd.json_normalize(types_raw)

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

    types_df = pd.concat(pokemons_types_chunks)
    upload_to_minio('bronze', 'pokemon_types.csv', types_df)


def dump_moves():
    """Gets list of moves and uploads to MinIO"""
    url = 'https://pokeapi.co/api/v2/move'
    moves_count = get_data_by_url(url)['count']

    url = f'https://pokeapi.co/api/v2/move?offset=0&limit={moves_count}'
    moves_raw = get_data_by_url(url)['results']
    moves = pd.json_normalize(moves_raw)

    upload_to_minio('bronze', 'moves.csv', moves)


def dump_pokemon_moves():
    """Gets all moves and pokemons from API and loads into the database"""
    url = 'https://pokeapi.co/api/v2/move'
    moves_count = get_data_by_url(url)['count']

    url = f'https://pokeapi.co/api/v2/move?offset=0&limit={moves_count}'
    moves_raw = get_data_by_url(url)['results']
    moves = pd.json_normalize(moves_raw)

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

    pokemon_moves_df = pd.concat(pokemons_moves_chunks)
    upload_to_minio('bronze', 'pokemon_moves.csv', pokemon_moves_df)
    logging.info('Pokemon moves successfully ingested into the database')


def dump_generation_species():
    """Gets list of generation species and loads them into the database"""
    url = 'https://pokeapi.co/api/v2/generation'
    generations_count = get_data_by_url(url)['count']

    url = f'https://pokeapi.co/api/v2/generation?offset=0&limit={generations_count}'
    generations_raw = get_data_by_url(url)['results']
    generations = pd.json_normalize(generations_raw)

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

    generations_species_df = pd.concat(pokemons_generations_chunks)
    upload_to_minio('bronze', 'generation_species.csv', generations_species_df)
    logging.info('Generation species successfully ingested into the database')


def dump_generations():
    """Gets list of generations and uploads to MinIO"""
    url = 'https://pokeapi.co/api/v2/generation'
    generations_count = get_data_by_url(url)['count']

    url = f'https://pokeapi.co/api/v2/generation?offset=0&limit={generations_count}'
    generations_raw = get_data_by_url(url)['results']
    generations = pd.json_normalize(generations_raw)

    upload_to_minio('bronze', 'generations.csv', generations)


def dump_pokemon_species():
    """Gets all pokemons from pokemon species and loads them into the database"""
    species_df = download_from_minio('bronze', 'generation_species.csv')
    species = species_df['specie_url'].unique()

    pokemons_species_chunks = []

    for specie in species:
        response = get_data_by_url(specie)['varieties']
        pokemons_species_chunk = pd.json_normalize(response)

        pokemons_species_chunk['specie_id'] = specie.split('/')[-2]
        pokemons_species_chunk.rename(columns={'pokemon.name': 'pokemon'}, inplace=True)
        pokemons_species_chunk.drop(columns=['is_default', 'pokemon.url'], inplace=True)
        pokemons_species_chunks.append(pokemons_species_chunk)

    pokemons_species_df = pd.concat(pokemons_species_chunks)
    upload_to_minio('bronze', 'pokemon_species.csv', pokemons_species_df)
    logging.info('Pokemon species successfully ingested into the database')


def download_from_minio(bucket_name, file_name):
    """Downloads CSV file from MinIO and returns it as a DataFrame"""
    s3 = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='admin',
        aws_secret_access_key='adminadmin'
    )

    obj = s3.get_object(Bucket=bucket_name, Key=file_name)
    return pd.read_csv(obj['Body'])


def load_data(engine, file_name, table_name):
    """Loads data from MinIO into the database"""
    data_df = download_from_minio('bronze', file_name)
    data_df.to_sql(table_name, engine, if_exists='replace', index=False, schema='silver')
    logging.info(f'{table_name} successfully ingested into the database')
