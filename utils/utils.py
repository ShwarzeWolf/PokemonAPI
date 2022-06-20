import logging
from functools import wraps
from time import sleep

import requests


logging.basicConfig(
    format="%(asctime)s => %(filename)s => %(levelname)s => %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO
)


def api_handler(api_call):
    """Proceeds api calls"""
    SLEEP_TIME = 5
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
    """Returns data by provided url"""
    data = requests.get(url).json()
    return data


def _process_success() -> None:
    """Prints success message"""
    logging.info('Process executed successfully')


def _process_failure() -> None:
    """Prints message on failure"""
    logging.info('Something went wrong')