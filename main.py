import logging
import requests
import pandas as pd

logging.basicConfig(
    format="%(asctime)s => %(filename)s => %(levelname)s => %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO
)

# getting all types -> will migrate to _get_types task in DAG
url = f'https://pokeapi.co/api/v2/type'
types = requests.get(url).json()['results']

pokemon_types_chunks = []

for _type in types:
    url = _type['url']

    response = requests.get(url).json()['pokemon']
    pokemon_type = pd.json_normalize(response)

    if not pokemon_type.empty:
        pokemon_type['type'] = _type['name']
        # на подумать - так ли нам нужен id или name в этой таблице? Там же функциональная зависимость между ними
        pokemon_type['id'] = pokemon_type.apply(lambda row: row['pokemon.url'].split('/')[-2], axis=1)
        pokemon_type.rename(columns={'pokemon.name': 'name'}, inplace=True)
        pokemon_type.drop(columns=['slot', 'pokemon.url'], inplace=True)

        pokemon_types_chunks.append(pokemon_type)

pokemon_types = pd.concat(pokemon_types_chunks)
pokemon_types.to_csv('./pokemon_types.csv', index=False, header=True)


# getting all generations -> will migrate to _check_generations_count task in DAG
def _check_generations_count():
    url = 'https://pokeapi.co/api/v2/generation'
    generations = requests.get(url).json()

    logging.info(f'Today exist {generations["count"]} generations')


_check_generations_count()
