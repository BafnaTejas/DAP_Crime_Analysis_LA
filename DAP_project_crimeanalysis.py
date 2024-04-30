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
        
    @staticmethod
    def save_to_postgres(df, table_name, schema='public', session=None, append=False):
        session_f = Database.get_postgres_session() if session is None else session
        cur = session_f.connection().connection.cursor()
        metadata = MetaData(schema=schema)
        table = Table(table_name, metadata, autoload_with=Database.__engine)
        output = io.StringIO()
        success = True
        common_cols = {i.name for i in table.columns}.intersection(set(df.columns))
        cols = [col for col in df.columns if col in common_cols]
        try:
            if not append:
                Database.delete_table_data(table_name=table_name, schema_name=schema, session=session)
            columns_with_quotes = [f"{col}" for col in cols]
            df[cols].to_csv(output, sep='\t', header=False, index=False)
            output.seek(0)
            cur.copy_from(output, table_name, sep='\t', columns=columns_with_quotes, null="")  # null values become
            if session is None:
                session_f.commit()
        except Exception as e:
            print(traceback.format_exc())
            session_f.rollback()
            raise Exception(e)
        finally:
            # connection.close()
            if session is None:
                session_f.close()
    @staticmethod
    def delete_table_data(table_name, schema_name='public', filter_column=None, filter_value=None,
                          session=None):
        # connection = DatabaseUtil.__engine.connect()
        session_f = Database.get_postgres_session() if session is None else session
        try:
            metadata = MetaData(schema=schema_name)
            table = Table(table_name, metadata, autoload_with=Database.__engine)
            if filter_column is not None:
                # query = delete(table).where((table.c[filter_column] == filter_value))
                session_f.query(table).filter((table.c[filter_column] == filter_value)).delete(
                    synchronize_session=False)
            else:
                # query = delete(table)
                session_f.query(table).delete(synchronize_session=False)
            # connection.execute(query)
            if session is None:
                session_f.commit()
        except Exception as e:
            print(traceback.format_exc())
            session_f.rollback()
            raise Exception(e)
        finally:
            # connection.close()
            if session is None:
                session_f.close()

class save_final(luigi.Task):
    collection_name = luigi.Parameter()

    def requires(self):
        return Save_to_mongo(collection_name=self.collection_name)

    def filter_df(self, df, cols_to_remove=None):
        df = df.drop(cols_to_remove, axis=1) if cols_to_remove not in [None, []] else df
        column_names = df.columns.tolist()
        column_mapping = {name: name.lower().replace(' ', '_') for name in column_names}
        df.rename(columns=column_mapping, inplace=True)
        return df

    def run(self):
        client = Database.get_mongo_client()
        db = client.dap
        # fetch all collections
        collection = db[self.collection_name]
        df = pd.DataFrame(list(collection.find()))

        # data cleaning/processing
        df.replace({np.nan: None}, inplace=True)

        if self.collection_name == 'crime_la':
            cols_to_remove = ['_id', 'sid', 'position', 'created_meta', 'updated_meta', 'meta', 'created_at', 'updated_at']
            df = self.filter_df(df, cols_to_remove)
            df = df.loc[~df['time_occ'].isin([1, 0, None, np.nan])]
            df = df.loc[~df['date_occ'].isin([1, 0, None, np.nan])]
            df['time_occ'] = pd.to_datetime(df['time_occ'], format='mixed')
            df['date_occ'] = df['date_occ'].astype('datetime64[ns]').dt.date
        elif self.collection_name == 'arrest_la':
            cols_to_remove = ['_id', 'sid', 'position', 'created_meta', 'updated_meta', 'meta', 'created_at', 'updated_at']
            df = self.filter_df(df, cols_to_remove)
            df.rename(columns={'neighborhood_councils_(certified)': 'neighborhood_councils_certified'}, inplace=True)
            df = df.loc[~df['time'].isin([1, 0, None, np.nan])]
            df = df.loc[~df['arrest_date'].isin([1, 0, None, np.nan])]
            df = df.loc[~df['booking_time'].isin([1, 0, None, np.nan])]
            df['time'] = pd.to_datetime(df['time'], format='mixed')
            df['booking_time'] = pd.to_datetime(df['booking_time'], format='mixed')
            df['arrest_date'] = df['arrest_date'].astype('datetime64[ns]').dt.date
        elif self.collection_name == 'calls_la':
            cols_to_remove = ['_id', 'sid', 'position', 'created_meta', 'updated_meta', 'meta', 'created_at', 'updated_at']
            df = self.filter_df(df, cols_to_remove)

        # Load your table model
        table_name = self.collection_name
        meta = MetaData()
        table = Table(table_name, meta, auto_load=True, autoload_with=Database.get_postgres_engine())
        # Get column names and data types
        column_data_types = {column.name: str(column.type) for column in table.columns}
        int_cols = []
        for key, value in column_data_types.items():
            if column_data_types[key] == 'INTEGER':
                int_cols.append(key)

        df[int_cols] = df[int_cols].replace({None: 0}).astype(int)
        p_key = [str(x).split('.')[1] for x in list(table.primary_key)]
        for col in p_key:
            df = df[df[col].notna()]
            df = df[~df[col].isnull()]
        df = df.drop_duplicates()
        try:
            session = Database.get_postgres_session()
            Database.save_to_postgres(df, self.collection_name, append=False, session=session)  # truncate and insert
            session.commit()
        except Exception as err:
            session.rollback()
            raise err
        finally:
            Database.close_postgres_session(session)
            # Save DataFrame as CSV
            cwd = os.getcwd()
            csv_path = os.path.join(cwd, 'Data', f'{self.collection_name}_postgres_data.csv').replace("\\", '/')
            df.to_csv(csv_path, index=False)
            print("fetch ran")
