import logging
import os

from dotenv import find_dotenv, load_dotenv


logger = logging.getLogger("")


class Config:
    def __init__(self):
        dotenv_path = find_dotenv()
        if dotenv_path == "":
            logger.info("ENV file not found.")
        else:
            load_dotenv(dotenv_path)

    @staticmethod
    def get_env(env: str):
        value = os.getenv(env)
        if value is None:
            logger.error("Missing environment variable: %s", env)
        return value
