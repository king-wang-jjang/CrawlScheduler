import os
from dotenv import load_dotenv,find_dotenv
import logging
# from utils.loghandler import setup_logger
# *** 해당 코드에 로깅코드 작성시 애러발생 ***
logger = logging.getLogger("")

class Config:
    def __init__(self):
        if find_dotenv() == "":
            logger.info("ENV File Not Found.")
            # if os.getenv("DB_HOST") == None:
            #     logger.error("ENV에 항목이 존재하지 않음.")
        else:
            load_dotenv(find_dotenv())

    @staticmethod
    def get_env(env: str):
        if os.getenv(env) == None:
            logger.error(f"ENV에 항목이 존재하지 않음. {env}")
            return None
        else:
            return os.getenv(env)
