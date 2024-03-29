import logging
from functools import wraps
from io import StringIO
from time import sleep

import pandas as pd
import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable

logging.basicConfig(
    format="%(asctime)s => %(filename)s => %(levelname)s => %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO
)


def api_handler(api_call):
    """Proceeds api calls"""
    SLEEP_TIME = 0.2
    """Amount of time to sleep after API call"""
    ATTEMPTS = 3
    """NUmber of times to retry API call in case of failure"""

    @wraps(api_call)
    def inner_executor(*args, **kwargs):
        """Executes api call with time delay """
        for i in range(ATTEMPTS):
            try:
                result = api_call(*args, **kwargs)
                sleep(SLEEP_TIME)
                return result
            except Exception:
                logging.warning(f'Something went wrong, attempt {i + 1}/{ATTEMPTS}, trying again')

    return inner_executor


@api_handler
def get_data_by_url(url):
    """Returns results by provided url"""
    data = requests.get(url).json()
    return data


def _process_success() -> None:
    """Prints success message"""
    logging.info('Process executed successfully')


def _process_failure() -> None:
    """Prints message on failure"""
    logging.info('Something went wrong')


def load_string_on_s3(data: str, key: str) -> None:
    """Loads data into S3 bucket"""
    s3hook = S3Hook()
    s3hook.load_string(string_data=data, key=key, replace=True)


def save_file_into_S3(filename, data):
    """Saves files from data frames into buckets"""
    data_stream = StringIO()

    data.to_csv(data_stream, index=False, header=True)

    s3_key = f'{Variable.get("snowpipe_files")}Sharaeva/{filename}'
    data = str(data_stream.getvalue())

    load_string_on_s3(data, s3_key)


def read_file_from_S3(filename):
    """Reads saved files from buckets"""
    s3_hook = S3Hook()

    s3_source = f'{Variable.get("snowpipe_files")}Sharaeva/{filename}'

    data_string = s3_hook.read_key(s3_source)
    data_IO = StringIO(data_string)

    return pd.read_csv(data_IO)


def _cleanup() -> None:
    """Deleting files from snowpipe/Sharaeva folder"""
    unprocessed_files_folder_url = Variable.get('snowpipe_files')

    _, _, bucket_name, unprocessed_files_folder, _ = unprocessed_files_folder_url.split('/')

    s3_hook = S3Hook()

    all_files = s3_hook.list_keys(
        bucket_name=bucket_name,
        prefix=f'{unprocessed_files_folder}/Sharaeva/',
        delimiter='/')

    files_to_delete = [file for file in all_files if file.endswith('.csv')]
    s3_hook.delete_objects(bucket=bucket_name, keys=files_to_delete)

    logging.info('Data cleaned')