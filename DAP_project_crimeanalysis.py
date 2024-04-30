import json
import traceback
import urllib
import io
import luigi
import os
import datetime
import pandas as pd
import numpy as np
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from sqlalchemy import MetaData, Table, create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import seaborn as sns
import matplotlib.pyplot as plt

class Database:
    __engine = None
    __Session = None
    __client = None

    def __init__(self):
        pass

    # This method initializes Global Session, Engine
    @classmethod
    def get_config(cls):
        config = {
            'postgres': {
                "type": "postgres",
                "driver": "postgresql",
                "username": "dap",
                "password": "dap",
                "host": "localhost",
                "port": "5432",
                "database": "dap",
                "schema": "public"
            },
            'mongodb': {
                "username": "dap",
                "password": "dap",
                "database": "dap",
                "port": 27017
            }
        }
        return config

    @classmethod
    def initialize_postgres_database(cls, db_config=None):
        config = cls.get_config()
        if db_config is None:
            db_config = config['postgres']
        db_uri = Database.get_db_uri(db_config)
        Database.__engine = create_engine(db_uri)
        Database.__Session = sessionmaker(bind=Database.__engine)

    @classmethod
    def initialize_mongodb(cls):
        config = Database.get_config()['mongodb']
        username = config['username']
        password = config['password']
        database = config['database']
        port = config['port']
        uri = f'mongodb://{username}:{password}@localhost:{port}'
        Database.__client = MongoClient(uri, server_api=ServerApi('1'))

    # This method will return global object of session maker, engine
    @staticmethod
    def get_postgres_session():
        if Database.__Session:
            return Database.__Session()
        else:
            Database.initialize_postgres_database()
            return Database.__Session()

    @staticmethod
    def get_postgres_engine():
        if Database.__engine:
            return Database.__engine
        else:
            Database.initialize_postgres_database()
            return Database.__engine

    @staticmethod
    def close_postgres_session(session):
        if session:
            session.close()

    @staticmethod
    def get_mongo_client():
        if Database.__client:
            return Database.__client
        else:
            Database.initialize_mongodb()
            return Database.__client

    @classmethod
    def get_db_uri(cls, db_config, mongo=False):
        driver = db_config['driver']
        username = urllib.parse.quote_plus(db_config['username'])
        password = urllib.parse.quote_plus(db_config['password'])
        host = db_config['host']
        port = db_config['port']
        database = db_config['database']
        schema = db_config['schema'] if 'schema' in db_config else 'public'

        mongo_uri = f"{driver}:///?Server={host}&Port={port}&Database={database}&User={username}&Password={password}"
        uri = f"{driver}://{username}:{password}@{host}:{port}/{database}?options=--search_path%3D{schema}"

        if "msdriver" in db_config:
            uri = "{}?driver={}".format(uri, db_config['msdriver'])
        return mongo_uri if mongo else uri
