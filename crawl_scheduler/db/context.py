from pymongo import MongoClient
from urllib.parse import quote_plus
from config import Config


class Database:
    db_host = Config().get_env("DB_HOST")
    db_name = Config().get_env("DB_NAME")
    db_user = Config().get_env("DB_USER")
    db_password = Config().get_env("DB_PASSWORD")
    escaped_user = quote_plus(db_user)
    escaped_password = quote_plus(db_password)
    client = MongoClient(
        f'mongodb://{escaped_user}:{escaped_password}@{db_host}/{db_name}?authSource=admin&authMechanism=SCRAM-SHA-256'
    )
    # client = MongoClient(f'mongodb://{escaped_user}:{escaped_password}@{db_host}/{db_name}?authMechanism=SCRAM-SHA-256')
    # client = MongoClient(
    #     f'mongodb://{escaped_user}:{escaped_password}@{db_host}:27017/{db_name}?authSource=kingwangjjang&authMechanism=SCRAM-SHA-256'
    # )
    db = client.get_default_database()

    @staticmethod
    def get_collection(name):
        return Database.db[name]