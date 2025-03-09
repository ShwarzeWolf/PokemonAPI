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
    url = 'https://pokeapi.co/api/v2/pokemon?offset=0&limit=100'
    pokemons = get_data_by_url(url)['results']

    pokemons_stats_chunks = []

    for pokemon in pokemons:
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
    types = get_data_by_url(url)['results']
    types_df = pd.json_normalize(types)
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
    """Gets all moves and pokemons from API and uploads to MinIO"""
    url = 'https://pokeapi.co/api/v2/move'
    moves = get_data_by_url(url)['results']
    moves_df = pd.json_normalize(moves)
    upload_to_minio('bronze', 'pokemon_moves.csv', moves_df)


def dump_generations():
    """Gets list of generations and uploads to MinIO"""
    url = 'https://pokeapi.co/api/v2/generation'
    generations_count = get_data_by_url(url)['count']

    url = f'https://pokeapi.co/api/v2/generation?offset=0&limit={generations_count}'
    generations_raw = get_data_by_url(url)['results']
    generations = pd.json_normalize(generations_raw)

    upload_to_minio('bronze', 'generations.csv', generations)


def dump_generation_species():
    """Gets list of generation species and uploads to MinIO"""
    url = 'https://pokeapi.co/api/v2/generation'
    generations = get_data_by_url(url)['results']
    generations_df = pd.json_normalize(generations)
    upload_to_minio('bronze', 'generation_species.csv', generations_df)


def dump_pokemon_species():
    """Gets all pokemons from pokemon species and uploads to MinIO"""
    url = 'https://pokeapi.co/api/v2/pokemon-species'
    species = get_data_by_url(url)['results']
    species_df = pd.json_normalize(species)
    upload_to_minio('bronze', 'pokemon_species.csv', species_df)


def _check_generations_count():
    """Logs the count of generations from the API"""
    url = 'https://pokeapi.co/api/v2/generation'
    generations = get_data_by_url(url)
    logging.info(f'Today exist {generations["count"]} generations')
