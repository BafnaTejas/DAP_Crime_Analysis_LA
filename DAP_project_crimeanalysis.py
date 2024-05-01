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

class Extract_data(luigi.Task):
    collection_name = luigi.Parameter()

    def output(self):
        cwd = os.getcwd()
        csv_path = os.path.join(cwd, 'Data', f'{self.collection_name}.csv').replace("\\", '/')
        return luigi.LocalTarget(csv_path)

    def run(self):
        # read csv
        cwd = os.getcwd()
        path = os.path.join(cwd, 'Data', f'{self.collection_name}.json').replace("\\", '/')
        file_path = path
        # changes for loading json
        data = json.load(open(file_path))
        df = pd.DataFrame(data["data"])

        new_names = pd.DataFrame(data['meta']['view']['columns'])['name'].tolist()
        old_names = df.keys().tolist()
        col_rename = list(zip(old_names, new_names))
        col_rename = {key: value for key, value in col_rename}
        df.rename(columns=col_rename, inplace=True)
        # Save DataFrame as CSV
        cwd = os.getcwd()
        csv_path = os.path.join(cwd, 'Data', f'{self.collection_name}.csv').replace("\\", '/')
        df.to_csv(csv_path, index=False)
        print("extract ran")
        
class Save_to_mongo(luigi.Task):
    collection_name = luigi.Parameter()

    def requires(self):
        return Extract_data(collection_name=self.collection_name)

    def output(self):
        cwd = os.getcwd()
        csv_path = os.path.join(cwd, 'Data', f'{self.collection_name}.csv').replace("\\", '/')
        return luigi.LocalTarget(csv_path)

    def insert_mongo_chunk_records(self, db, collection_name, records):
        db[collection_name].delete_many({})
        collection = db[collection_name]
        if len(records) > 99999:
            records = [records[i: i + 50000] for i in range(0, len(records), 50000)]
            for record in list(records):
                collection.insert_many(list(record), ordered=False)
                print(datetime.now())
        else:
            collection.insert_many(records)



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

from sqlalchemy import Column, Integer, String, Text, Date, Time, Float, CHAR
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class CrimeLA(Base):
    __tablename__ = 'crime_la'

    id = Column(String, primary_key=True)
    dr_no = Column(Integer)
    date_rptd = Column(Date)
    date_occ = Column(Date)
    time_occ = Column(Time)
    area = Column(Integer)
    area_name = Column(String(255))
    rpt_dist_no = Column(Integer)
    part_1_2 = Column(String(255))
    crm_cd = Column(Integer)
    crm_cd_desc = Column(String(255))
    mocodes = Column(Text)
    vict_age = Column(Integer)
    vict_sex = Column(CHAR(1))
    vict_descent = Column(String(255))
    premis_cd = Column(Integer)
    premis_desc = Column(String(255))
    weapon_used_cd = Column(Integer)
    weapon_desc = Column(String(255))
    status = Column(String(255))
    status_desc = Column(String(255))
    crm_cd_1 = Column(Integer)
    crm_cd_2 = Column(Integer)
    crm_cd_3 = Column(Integer)
    crm_cd_4 = Column(Integer)
    location = Column(String(255))
    cross_street = Column(String(255))
    lat = Column(Float)
    lon = Column(Float)

class ArrestLA(Base):
    __tablename__ = 'arrest_la'

    id = Column(String, primary_key=True)
    report_id = Column(Integer)
    report_type = Column(String(255))
    arrest_date = Column(Date)
    time = Column(Time)
    area_id = Column(Integer)
    area_name = Column(String(255))
    reporting_district = Column(Integer)
    age = Column(Integer)
    sex_code = Column(CHAR(1))
    descent_code = Column(String(255))
    charge_group_code = Column(String(255))
    charge_group_description = Column(String(255))
    arrest_type_code = Column(String(255))
    charge = Column(String(255))
    charge_description = Column(String(255))
    disposition_description = Column(String(255))
    address = Column(String(255))
    cross_street = Column(String(255))
    lat = Column(Float)
    lon = Column(Float)
    location = Column(String(255))
    booking_date = Column(Date)
    booking_time = Column(Time)
    booking_location = Column(String(255))
    booking_location_code = Column(String(255))
    zip_codes = Column(String(255))
    census_tracts = Column(String(255))
    precinct_boundaries = Column(String(255))
    la_specific_plans = Column(String(255))
    council_districts = Column(String(255))
    neighborhood_councils_certified = Column(String(255))

class CallsLA(Base):
    __tablename__ = 'calls_la'

    id = Column(String, primary_key=True)
    meta = Column(Text)
    incident_number = Column(String)
    area_occ = Column(String)
    rpt_dist = Column(Integer)
    dispatch_date = Column(Date)
    dispatch_time = Column(Time)
    call_type_code = Column(String(255))
    call_type_text = Column(String(255))

if __name__ == '__main__':
    luigi.build([save_final(collection_name='crime_la'),
                 save_final(collection_name='arrest_la'),
                 save_final(collection_name='calls_la')], local_scheduler=True)
    
    # fetch data from postgres using sqlalchemy orm
    session = Database.get_postgres_session()
    sq = session.query(CrimeLA)
    crime = pd.read_sql(sq.statement, session.bind)
    sq1 = session.query(ArrestLA)
    arrest = pd.read_sql(sq1.statement, session.bind)
    sq2 = session.query(CallsLA)
    calls = pd.read_sql(sq2.statement, session.bind)
    Database.close_postgres_session(session)

#************************** Visualization Process Started ********************************************************** 

# Crime rate based on area_name and area
crime_rate = crime.groupby(['area_name', 'area'])['crm_cd'].count().reset_index()
crime_rate.columns = ['area_name', 'area', 'crime_count'] 
crime_rate['crime_rate'] = crime_rate['crime_count'] / crime['area'].nunique()
crime_rate = crime_rate.sort_values('crime_rate', ascending=False)

# Crime type based on area_name 
crime_type = crime.groupby(['area_name', 'crm_cd_desc'])['crm_cd'].count().reset_index()
crime_type = crime_type.sort_values('crm_cd', ascending=False)

# Crime based on victim (male or female)
crime_victim = crime.groupby(['vict_sex'])['crm_cd'].count().reset_index()

# Plot a chart based on number of crime instances occurred as per area
crime_counts = crime_data.groupby('area_name')['id'].count().reset_index()

# Sort the data by the number of crimes in descending order
crime_counts = crime_counts.sort_values(by='id', ascending=False)

crime_counts.plot(x='area_name', y='id', kind='bar')
plt.xlabel('Area')
plt.ylabel('Number of crimes')
plt.title('Number of crimes by area')
plt.show()

# Plot a chart identifying crime (crime_cd_desc) which occurred most in based on each area
crime_counts_by_crime = crime_data.groupby(['area_name', 'crm_cd_desc'])['id'].count().reset_index()
crime_counts_by_crime = crime_counts_by_crime.sort_values(['area_name', 'id'], ascending=False)
crime_counts_by_crime.plot(x='crm_cd_desc', y='id', kind='bar')
plt.xlabel('Crime description')
plt.ylabel('Number of crimes')
plt.title('Number of crimes by crime description')
plt.show()

from sklearn.cluster import KMeans
import numpy as np

# Define the crimes and their counts
crime_names = [
    "VEHICLE - STOLEN", "BATTERY - SIMPLE ASSAULT", "BURGLARY FROM VEHICLE", "BURGLARY", 
    "VANDALISM - FELONY ($400 & OVER, ALL CHURCH VANDALISMS)", "ASSAULT WITH DEADLY WEAPON, AGGRAVATED ASSAULT",
    "THEFT OF IDENTITY", "INTIMATE PARTNER - SIMPLE ASSAULT", "THEFT PLAIN - PETTY ($950 & UNDER)",
    "THEFT FROM MOTOR VEHICLE - PETTY ($950 & UNDER)", "ROBBERY", "VANDALISM - MISDEAMEANOR ($399 OR UNDER)",
    "THEFT-GRAND ($950.01 & OVER)EXCPT,GUNS,FOWL,LIVESTK,PROD", 
    "THEFT FROM MOTOR VEHICLE - GRAND ($950.01 AND OVER)", "CRIMINAL THREATS - NO WEAPON DISPLAYED",
    "SHOPLIFTING - PETTY THEFT ($950 & UNDER)", "BRANDISH WEAPON", "INTIMATE PARTNER - AGGRAVATED ASSAULT",
    "TRESPASSING", "VIOLATION OF RESTRAINING ORDER", "BIKE - STOLEN", "OTHER MISCELLANEOUS CRIME",
    "LETTERS, LEWD  -  TELEPHONE CALLS, LEWD", "VIOLATION OF COURT ORDER", "BUNCO, GRAND THEFT",
    "ATTEMPTED ROBBERY", "OTHER ASSAULT", "THEFT, PERSON", "BATTERY WITH SEXUAL CONTACT", "RAPE, FORCIBLE",
    "EMBEZZLEMENT, GRAND THEFT ($950.01 & OVER)", "CHILD ABUSE (PHYSICAL) - SIMPLE ASSAULT", "BURGLARY, ATTEMPTED",
    "ARSON", "DOCUMENT FORGERY / STOLEN FELONY", "BUNCO, PETTY THEFT", "BATTERY POLICE (SIMPLE)",
    "DISCHARGE FIREARMS/SHOTS FIRED", "VEHICLE - ATTEMPT STOLEN", 
    "CRM AGNST CHLD (13 OR UNDER) (14-15 & SUSP 10 YRS OLDER)", "CONTEMPT OF COURT", 
    "SHOPLIFTING-GRAND THEFT ($950.01 & OVER)", "SHOTS FIRED AT INHABITED DWELLING", 
    "SEXUAL PENETRATION W/FOREIGN OBJECT", "EXTORTION", "ASSAULT WITH DEADLY WEAPON ON POLICE OFFICER",
    "SEX,UNLAWFUL(INC MUTUAL CONSENT, PENETRATION W/ FRGN OBJ", "CRIMINAL HOMICIDE",
    "CHILD NEGLECT (SEE 300 W.I.C.)", "CHILD ANNOYING (17YRS & UNDER)", "INDECENT EXPOSURE", 
    "DISTURBING THE PEACE", "SEX OFFENDER REGISTRANT OUT OF COMPLIANCE", "ORAL COPULATION",
    "FAILURE TO YIELD", "THEFT FROM MOTOR VEHICLE - ATTEMPT", "CHILD ABUSE (PHYSICAL) - AGGRAVATED ASSAULT",
    "BURGLARY FROM VEHICLE, ATTEMPTED", "VEHICLE, STOLEN - OTHER (MOTORIZED SCOOTERS, BIKES, ETC)",
    "RESISTING ARREST", "KIDNAPPING", "VIOLATION OF TEMPORARY RESTRAINING ORDER", "LEWD CONDUCT",
    "THEFT PLAIN - ATTEMPT", "UNAUTHORIZED COMPUTER ACCESS", "THROWING OBJECT AT MOVING VEHICLE", "STALKING",
    "CHILD STEALING", "HUMAN TRAFFICKING - COMMERCIAL SEX ACTS",
    "SODOMY/SEXUAL CONTACT B/W PENIS OF ONE PERS TO ANUS OTH", "THREATENING PHONE CALLS/LETTERS",
    "BUNCO, ATTEMPT", "DEFRAUDING INNKEEPER/THEFT OF SERVICES, $950 & UNDER",
    "SHOTS FIRED AT MOVING VEHICLE, TRAIN OR AIRCRAFT", "RAPE, ATTEMPTED", "PICKPOCKET", 
    "FALSE IMPRISONMENT", "DRIVING WITHOUT OWNER CONSENT (DWOC)", "PEEPING TOM", "BOMB SCARE",
    "CHILD PORNOGRAPHY", "RECKLESS DRIVING", "PIMPING", "KIDNAPPING - GRAND ATTEMPT", "PROWLER",
    "SHOPLIFTING - ATTEMPT", "COUNTERFEIT", "DEFRAUDING INNKEEPER/THEFT OF SERVICES, OVER $950.01",
    "CRUELTY TO ANIMALS", "FALSE POLICE REPORT", "BATTERY ON A FIREFIGHTER", 
    "EMBEZZLEMENT, PETTY THEFT ($950 & UNDER)", "DOCUMENT WORTHLESS ($200 & UNDER)",
    "LEWD/LASCIVIOUS ACTS WITH CHILD", "THEFT FROM PERSON - ATTEMPT", "CREDIT CARDS, FRAUD USE ($950 & UNDER",
    "ILLEGAL DUMPING", "DOCUMENT WORTHLESS ($200.01 & OVER)", "HUMAN TRAFFICKING - INVOLUNTARY SERVITUDE",
    "BOAT - STOLEN", "DISHONEST EMPLOYEE - GRAND THEFT", "PURSE SNATCHING", "PANDERING", 
    "WEAPONS POSSESSION/BOMBING", "THEFT, COIN MACHINE - PETTY ($950 & UNDER)", 
    "CREDIT CARDS, FRAUD USE ($950.01 & OVER)", "MANSLAUGHTER, NEGLIGENT", "CONTRIBUTING",
    "PETTY THEFT - AUTO REPAIR", "DISHONEST EMPLOYEE - PETTY THEFT", "LYNCHING", "BIKE - ATTEMPTED STOLEN",
    "LYNCHING - ATTEMPTED", "PURSE SNATCHING - ATTEMPT", "THEFT, COIN MACHINE - ATTEMPT",
    "FIREARMS RESTRAINING ORDER (FIREARMS RO)", "DRUGS, TO A MINOR", 
    "REPLICA FIREARMS(SALE,DISPLAY,MANUFACTURE OR DISTRIBUTE)", "TILL TAP - GRAND THEFT ($950.01 & OVER)",
    "TILL TAP - PETTY ($950 & UNDER)"
]

crime_counts = [
    5063, 3971, 3074, 3053, 3042, 2737, 2666, 2658, 2641, 2380, 1703, 1656, 1376, 1111, 1049, 832, 763, 702, 680,
    610, 519, 419, 410, 327, 322, 290, 267, 240, 238, 236, 218, 209, 204, 188, 187, 153, 151, 148, 138, 131, 130,
    127, 118, 100, 99, 77, 77, 71, 70, 69, 68, 56, 52, 51, 48, 44, 44, 43, 43, 41, 40, 38, 37, 37, 35, 35, 34, 30,
    26, 26, 24, 22, 20, 19, 18, 17, 17, 17, 16, 13, 12, 11, 10, 9, 9, 8, 8, 7, 7, 7, 6, 6, 5, 5, 5, 5, 4, 4, 4, 3,
    3, 3, 3, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1
]

# Reshape the crime counts for clustering
crime_counts_reshaped = np.array(crime_counts).reshape(-1, 1)

# Use KMeans clustering to categorize crimes into 10 clusters
kmeans = KMeans(n_clusters=10, random_state=0)
kmeans.fit(crime_counts_reshaped)

# Get the labels of each crime
crime_labels = kmeans.labels_

# Create a dictionary to store counts of crimes in each category
category_counts = {f'Category {i+1}': 0 for i in range(10)}

# Increment counts for each category
for label, count in zip(crime_labels, crime_counts):
    category_counts[f'Category {label+1}'] += count

# Print counts for each category
for category, count in category_counts.items():
    print(f'{category}: {count}')
    
# Define names for each category based on characteristics of crimes
category_names = {
    'Category 1': 'Vehicle-Related Crimes',
    'Category 2': 'Assault and Battery',
    'Category 3': 'Property Theft and Vandalism',
    'Category 4': 'Financial Crimes',
    'Category 5': 'Domestic Violence',
    'Category 6': 'Robbery and Burglary',
    'Category 7': 'Sexual Crimes',
    'Category 8': 'Drug-Related Crimes',
    'Category 9': 'Public Disorder',
    'Category 10': 'Miscellaneous Crimes'
}

# Print names and counts for each category
for category, count in category_counts.items():
    print(f'{category}: {category_names[category]} - {count}')

#************************************************************************************************************
# Define the crimes and their counts
crime_names = [
    "VEHICLE - STOLEN", "BATTERY - SIMPLE ASSAULT", "BURGLARY FROM VEHICLE", "BURGLARY", 
    "VANDALISM - FELONY ($400 & OVER, ALL CHURCH VANDALISMS)", "ASSAULT WITH DEADLY WEAPON, AGGRAVATED ASSAULT",
    "THEFT OF IDENTITY", "INTIMATE PARTNER - SIMPLE ASSAULT", "THEFT PLAIN - PETTY ($950 & UNDER)",
    "THEFT FROM MOTOR VEHICLE - PETTY ($950 & UNDER)", "ROBBERY", "VANDALISM - MISDEAMEANOR ($399 OR UNDER)",
    "THEFT-GRAND ($950.01 & OVER)EXCPT,GUNS,FOWL,LIVESTK,PROD", 
    "THEFT FROM MOTOR VEHICLE - GRAND ($950.01 AND OVER)", "CRIMINAL THREATS - NO WEAPON DISPLAYED",
    "SHOPLIFTING - PETTY THEFT ($950 & UNDER)", "BRANDISH WEAPON", "INTIMATE PARTNER - AGGRAVATED ASSAULT",
    "TRESPASSING", "VIOLATION OF RESTRAINING ORDER", "BIKE - STOLEN", "OTHER MISCELLANEOUS CRIME",
    "LETTERS, LEWD  -  TELEPHONE CALLS, LEWD", "VIOLATION OF COURT ORDER", "BUNCO, GRAND THEFT",
    "ATTEMPTED ROBBERY", "OTHER ASSAULT", "THEFT, PERSON", "BATTERY WITH SEXUAL CONTACT", "RAPE, FORCIBLE",
    "EMBEZZLEMENT, GRAND THEFT ($950.01 & OVER)", "CHILD ABUSE (PHYSICAL) - SIMPLE ASSAULT", "BURGLARY, ATTEMPTED",
    "ARSON", "DOCUMENT FORGERY / STOLEN FELONY", "BUNCO, PETTY THEFT", "BATTERY POLICE (SIMPLE)",
    "DISCHARGE FIREARMS/SHOTS FIRED", "VEHICLE - ATTEMPT STOLEN", 
    "CRM AGNST CHLD (13 OR UNDER) (14-15 & SUSP 10 YRS OLDER)", "CONTEMPT OF COURT", 
    "SHOPLIFTING-GRAND THEFT ($950.01 & OVER)", "SHOTS FIRED AT INHABITED DWELLING", 
    "SEXUAL PENETRATION W/FOREIGN OBJECT", "EXTORTION", "ASSAULT WITH DEADLY WEAPON ON POLICE OFFICER",
    "SEX,UNLAWFUL(INC MUTUAL CONSENT, PENETRATION W/ FRGN OBJ", "CRIMINAL HOMICIDE",
    "CHILD NEGLECT (SEE 300 W.I.C.)", "CHILD ANNOYING (17YRS & UNDER)", "INDECENT EXPOSURE", 
    "DISTURBING THE PEACE", "SEX OFFENDER REGISTRANT OUT OF COMPLIANCE", "ORAL COPULATION",
    "FAILURE TO YIELD", "THEFT FROM MOTOR VEHICLE - ATTEMPT", "CHILD ABUSE (PHYSICAL) - AGGRAVATED ASSAULT",
    "BURGLARY FROM VEHICLE, ATTEMPTED", "VEHICLE, STOLEN - OTHER (MOTORIZED SCOOTERS, BIKES, ETC)",
    "RESISTING ARREST", "KIDNAPPING", "VIOLATION OF TEMPORARY RESTRAINING ORDER", "LEWD CONDUCT",
    "THEFT PLAIN - ATTEMPT", "UNAUTHORIZED COMPUTER ACCESS", "THROWING OBJECT AT MOVING VEHICLE", "STALKING",
    "CHILD STEALING", "HUMAN TRAFFICKING - COMMERCIAL SEX ACTS",
    "SODOMY/SEXUAL CONTACT B/W PENIS OF ONE PERS TO ANUS OTH", "THREATENING PHONE CALLS/LETTERS",
    "BUNCO, ATTEMPT", "DEFRAUDING INNKEEPER/THEFT OF SERVICES, $950 & UNDER",
    "SHOTS FIRED AT MOVING VEHICLE, TRAIN OR AIRCRAFT", "RAPE, ATTEMPTED", "PICKPOCKET", 
    "FALSE IMPRISONMENT", "DRIVING WITHOUT OWNER CONSENT (DWOC)", "PEEPING TOM", "BOMB SCARE",
    "CHILD PORNOGRAPHY", "RECKLESS DRIVING", "PIMPING", "KIDNAPPING - GRAND ATTEMPT", "PROWLER",
    "SHOPLIFTING - ATTEMPT", "COUNTERFEIT", "DEFRAUDING INNKEEPER/THEFT OF SERVICES, OVER $950.01",
    "CRUELTY TO ANIMALS", "FALSE POLICE REPORT", "BATTERY ON A FIREFIGHTER", 
    "EMBEZZLEMENT, PETTY THEFT ($950 & UNDER)", "DOCUMENT WORTHLESS ($200 & UNDER)",
    "LEWD/LASCIVIOUS ACTS WITH CHILD", "THEFT FROM PERSON - ATTEMPT", "CREDIT CARDS, FRAUD USE ($950 & UNDER",
    "ILLEGAL DUMPING", "DOCUMENT WORTHLESS ($200.01 & OVER)", "HUMAN TRAFFICKING - INVOLUNTARY SERVITUDE",
    "BOAT - STOLEN", "DISHONEST EMPLOYEE - GRAND THEFT", "PURSE SNATCHING", "PANDERING", 
    "WEAPONS POSSESSION/BOMBING", "THEFT, COIN MACHINE - PETTY ($950 & UNDER)", 
    "CREDIT CARDS, FRAUD USE ($950.01 & OVER)", "MANSLAUGHTER, NEGLIGENT", "CONTRIBUTING",
    "PETTY THEFT - AUTO REPAIR", "DISHONEST EMPLOYEE - PETTY THEFT", "LYNCHING", "BIKE - ATTEMPTED STOLEN",
    "LYNCHING - ATTEMPTED", "PURSE SNATCHING - ATTEMPT", "THEFT, COIN MACHINE - ATTEMPT",
    "FIREARMS RESTRAINING ORDER (FIREARMS RO)", "DRUGS, TO A MINOR", 
    "REPLICA FIREARMS(SALE,DISPLAY,MANUFACTURE OR DISTRIBUTE)", "TILL TAP - GRAND THEFT ($950.01 & OVER)",
    "TILL TAP - PETTY ($950 & UNDER)"
]

crime_counts = [
    5063, 3971, 3074, 3053, 3042, 2737, 2666, 2658, 2641, 2380, 1703, 1656, 1376, 1111, 1049, 832, 763, 702, 680,
    610, 519, 419, 410, 327, 322, 290, 267, 240, 238, 236, 218, 209, 204, 188, 187, 153, 151, 148, 138, 131, 130,
    127, 118, 100, 99, 77, 77, 71, 70, 69, 68, 56, 52, 51, 48, 44, 44, 43, 43, 41, 40, 38, 37, 37, 35, 35, 34, 30,
    26, 26, 24, 22, 20, 19, 18, 17, 17, 17, 16, 13, 12, 11, 10, 9, 9, 8, 8, 7, 7, 7, 6, 6, 5, 5, 5, 5, 4, 4, 4, 3,
    3, 3, 3, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1
]

# Reshape the crime counts for clustering
crime_counts_reshaped = np.array(crime_counts).reshape(-1, 1)

# Use KMeans clustering to categorize crimes into 10 clusters
kmeans = KMeans(n_clusters=10, random_state=0)
kmeans.fit(crime_counts_reshaped)

# Get the labels of each crime
crime_labels = kmeans.labels_

# Create a dictionary to store counts of crimes in each category
category_counts = {f'Category {i+1}': 0 for i in range(10)}

# Increment counts for each category
for label, count in zip(crime_labels, crime_counts):
    category_counts[f'Category {label+1}'] += count

# Print counts for each category
for category, count in category_counts.items():
    print(f'{category}: {count}')
    
# Define names for each category based on characteristics of crimes
category_names = {
    'Category 1': 'Vehicle-Related Crimes',
    'Category 2': 'Assault and Battery',
    'Category 3': 'Property Theft and Vandalism',
    'Category 4': 'Financial Crimes',
    'Category 5': 'Domestic Violence',
    'Category 6': 'Robbery and Burglary',
    'Category 7': 'Sexual Crimes',
    'Category 8': 'Drug-Related Crimes',
    'Category 9': 'Public Disorder',
    'Category 10': 'Miscellaneous Crimes'
}

# Print names and counts for each category
for category, count in category_counts.items():
    print(f'{category}: {category_names[category]} - {count}')

#***************************************************************************************************

# Assign each crime to its corresponding category
crime_categories = {
    "VEHICLE - STOLEN": 'Vehicle-Related Crimes',
    "BATTERY - SIMPLE ASSAULT": 'Assault and Battery',
    "BURGLARY FROM VEHICLE": 'Property Theft and Vandalism',
    "BURGLARY": 'Property Theft and Vandalism',
    "VANDALISM - FELONY ($400 & OVER, ALL CHURCH VANDALISMS)": 'Property Theft and Vandalism',
    "ASSAULT WITH DEADLY WEAPON, AGGRAVATED ASSAULT": 'Assault and Battery',
    "THEFT OF IDENTITY": 'Financial Crimes',
    "INTIMATE PARTNER - SIMPLE ASSAULT": 'Domestic Violence',
    "THEFT PLAIN - PETTY ($950 & UNDER)": 'Property Theft and Vandalism',
    "THEFT FROM MOTOR VEHICLE - PETTY ($950 & UNDER)": 'Property Theft and Vandalism',
    "ROBBERY": 'Robbery and Burglary',
    "VANDALISM - MISDEAMEANOR ($399 OR UNDER)": 'Property Theft and Vandalism',
    "THEFT-GRAND ($950.01 & OVER)EXCPT,GUNS,FOWL,LIVESTK,PROD": 'Property Theft and Vandalism',
    "THEFT FROM MOTOR VEHICLE - GRAND ($950.01 AND OVER)": 'Property Theft and Vandalism',
    "CRIMINAL THREATS - NO WEAPON DISPLAYED": 'Assault and Battery',
    "SHOPLIFTING - PETTY THEFT ($950 & UNDER)": 'Property Theft and Vandalism',
    "BRANDISH WEAPON": 'Assault and Battery',
    "INTIMATE PARTNER - AGGRAVATED ASSAULT": 'Assault and Battery',
    "TRESPASSING": 'Public Disorder',
    "VIOLATION OF RESTRAINING ORDER": 'Domestic Violence',
    "BIKE - STOLEN": 'Vehicle-Related Crimes',
    "OTHER MISCELLANEOUS CRIME": 'Miscellaneous Crimes',
    "LETTERS, LEWD  -  TELEPHONE CALLS, LEWD": 'Miscellaneous Crimes',
    "VIOLATION OF COURT ORDER": 'Domestic Violence',
    "BUNCO, GRAND THEFT": 'Financial Crimes',
    "ATTEMPTED ROBBERY": 'Robbery and Burglary',
    "OTHER ASSAULT": 'Assault and Battery',
    "THEFT, PERSON": 'Property Theft and Vandalism',
    "BATTERY WITH SEXUAL CONTACT": 'Sexual Crimes',
    "RAPE, FORCIBLE": 'Sexual Crimes',
    "EMBEZZLEMENT, GRAND THEFT ($950.01 & OVER)": 'Financial Crimes',
    "CHILD ABUSE (PHYSICAL) - SIMPLE ASSAULT": 'Domestic Violence',
    "BURGLARY, ATTEMPTED": 'Property Theft and Vandalism',
    "ARSON": 'Property Theft and Vandalism',
    "DOCUMENT FORGERY / STOLEN FELONY": 'Financial Crimes',
    "BUNCO, PETTY THEFT": 'Financial Crimes',
    "BATTERY POLICE (SIMPLE)": 'Assault and Battery',
    "DISCHARGE FIREARMS/SHOTS FIRED": 'Drug-Related Crimes',
    "VEHICLE - ATTEMPT STOLEN": 'Vehicle-Related Crimes',
    "CRM AGNST CHLD (13 OR UNDER) (14-15 & SUSP 10 YRS OLDER)": 'Domestic Violence',
    "CONTEMPT OF COURT": 'Miscellaneous Crimes',
    "SHOPLIFTING-GRAND THEFT ($950.01 & OVER)": 'Property Theft and Vandalism',
    "SHOTS FIRED AT INHABITED DWELLING": 'Drug-Related Crimes',
    "SEXUAL PENETRATION W/FOREIGN OBJECT": 'Sexual Crimes',
    "EXTORTION": 'Financial Crimes',
    "ASSAULT WITH DEADLY WEAPON ON POLICE OFFICER": 'Assault and Battery',
    "SEX,UNLAWFUL(INC MUTUAL CONSENT, PENETRATION W/ FRGN OBJ": 'Sexual Crimes',
    "CRIMINAL HOMICIDE": 'Sexual Crimes',
    "CHILD NEGLECT (SEE 300 W.I.C.)": 'Domestic Violence',
    "CHILD ANNOYING (17YRS & UNDER)": 'Domestic Violence',
    "INDECENT EXPOSURE": 'Sexual Crimes',
    "DISTURBING THE PEACE": 'Public Disorder',
    "SEX OFFENDER REGISTRANT OUT OF COMPLIANCE": 'Domestic Violence',
    "ORAL COPULATION": 'Sexual Crimes',
    "FAILURE TO YIELD": 'Public Disorder',
    "THEFT FROM MOTOR VEHICLE - ATTEMPT": 'Property Theft and Vandalism',
    "CHILD ABUSE (PHYSICAL) - AGGRAVATED ASSAULT": 'Domestic Violence',
    "BURGLARY FROM VEHICLE, ATTEMPTED": 'Property Theft and Vandalism',
    "VEHICLE, STOLEN - OTHER (MOTORIZED SCOOTERS, BIKES, ETC)": 'Vehicle-Related Crimes',
    "RESISTING ARREST": 'Public Disorder',
    "KIDNAPPING": 'Sexual Crimes',
    "VIOLATION OF TEMPORARY RESTRAINING ORDER": 'Domestic Violence',
    "LEWD CONDUCT": 'Sexual Crimes',
    "THEFT PLAIN - ATTEMPT": 'Property Theft and Vandalism',
    "UNAUTHORIZED COMPUTER ACCESS": 'Drug-Related Crimes',
    "THROWING OBJECT AT MOVING VEHICLE": 'Public Disorder',
    "STALKING": 'Domestic Violence',
    "CHILD STEALING": 'Domestic Violence',
    "HUMAN TRAFFICKING - COMMERCIAL SEX ACTS": 'Sexual Crimes',
    "SODOMY/SEXUAL CONTACT B/W PENIS OF ONE PERS TO ANUS OTH": 'Sexual Crimes',
    "THREATENING PHONE CALLS/LETTERS": 'Miscellaneous Crimes',
    "BUNCO, ATTEMPT": 'Financial Crimes',
    "DEFRAUDING INNKEEPER/THEFT OF SERVICES, $950 & UNDER": 'Financial Crimes',
    "SHOTS FIRED AT MOVING VEHICLE, TRAIN OR AIRCRAFT": 'Drug-Related Crimes',
    "RAPE, ATTEMPTED": 'Sexual Crimes',
    "PICKPOCKET": 'Property Theft and Vandalism',
    "FALSE IMPRISONMENT": 'Domestic Violence',
    "DRIVING WITHOUT OWNER CONSENT (DWOC)": 'Vehicle-Related Crimes',
    "PEEPING TOM": 'Sexual Crimes',
    "BOMB SCARE": 'Miscellaneous Crimes',
    "CHILD PORNOGRAPHY": 'Sexual Crimes',
    "RECKLESS DRIVING": 'Public Disorder',
    "PIMPING": 'Sexual Crimes',
    "KIDNAPPING - GRAND ATTEMPT": 'Sexual Crimes',
    "PROWLER": 'Domestic Violence',
    "SHOPLIFTING - ATTEMPT": 'Property Theft and Vandalism',
    "COUNTERFEIT": 'Financial Crimes',
    "DEFRAUDING INNKEEPER/THEFT OF SERVICES, OVER $950.01": 'Financial Crimes',
    "CRUELTY TO ANIMALS": 'Miscellaneous Crimes',
    "FALSE POLICE REPORT": 'Miscellaneous Crimes',
    "BATTERY ON A FIREFIGHTER": 'Assault and Battery',
    "EMBEZZLEMENT, PETTY THEFT ($950 & UNDER)": 'Financial Crimes',
    "DOCUMENT WORTHLESS ($200 & UNDER)": 'Miscellaneous Crimes',
    "LEWD/LASCIVIOUS ACTS WITH CHILD": 'Sexual Crimes',
    "THEFT FROM PERSON - ATTEMPT": 'Property Theft and Vandalism',
    "CREDIT CARDS, FRAUD USE ($950 & UNDER": 'Financial Crimes',
    "ILLEGAL DUMPING": 'Miscellaneous Crimes',
    "DOCUMENT WORTHLESS ($200.01 & OVER)": 'Miscellaneous Crimes',
    "HUMAN TRAFFICKING - INVOLUNTARY SERVITUDE": 'Sexual Crimes',
    "BOAT - STOLEN": 'Vehicle-Related Crimes',
    "DISHONEST EMPLOYEE - GRAND THEFT": 'Financial Crimes',
    "PURSE SNATCHING": 'Robbery and Burglary',
    "PANDERING": 'Sexual Crimes',
    "WEAPONS POSSESSION/BOMBING": 'Drug-Related Crimes',
    "THEFT, COIN MACHINE - PETTY ($950 & UNDER)": 'Property Theft and Vandalism',
    "CREDIT CARDS, FRAUD USE ($950.01 & OVER)": 'Financial Crimes',
    "MANSLAUGHTER, NEGLIGENT": 'Sexual Crimes',
    "CONTRIBUTING": 'Miscellaneous Crimes',
    "PETTY THEFT - AUTO REPAIR": 'Financial Crimes',
    "DISHONEST EMPLOYEE - PETTY THEFT": 'Financial Crimes',
    "LYNCHING": 'Miscellaneous Crimes',
    "BIKE - ATTEMPTED STOLEN": 'Vehicle-Related Crimes',
    "LYNCHING - ATTEMPTED": 'Miscellaneous Crimes',
    "PURSE SNATCHING - ATTEMPT": 'Robbery and Burglary',
    "THEFT, COIN MACHINE - ATTEMPT": 'Property Theft and Vandalism',
    "FIREARMS RESTRAINING ORDER (FIREARMS RO)": 'Drug-Related Crimes',
    "DRUGS, TO A MINOR": 'Drug-Related Crimes',
    "REPLICA FIREARMS(SALE,DISPLAY,MANUFACTURE OR DISTRIBUTE)": 'Drug-Related Crimes',
    "TILL TAP - GRAND THEFT ($950.01 & OVER)": 'Financial Crimes',
    "TILL TAP - PETTY ($950 & UNDER)": 'Financial Crimes'
}

# Add category column to the data
crime_data['Category'] = crime_data['crm_cd_desc'].map(crime_categories)

# Group data by area and category, then count the crimes
crime_counts_by_area_category = crime_data.groupby(['area_name', 'Category']).size().reset_index(name='count')

# Sort the data by area and count of crimes
crime_counts_by_area_category = crime_counts_by_area_category.sort_values(['area_name', 'count'], ascending=[True, False])

# Get the top five areas for each category
top_areas_by_category = crime_counts_by_area_category.groupby('Category').head(5)

# Plotting the top five areas for each category
for category in top_areas_by_category['Category'].unique():
    category_data = top_areas_by_category[top_areas_by_category['Category'] == category]
    plt.figure(figsize=(7.5, 4.5))  # Reduced size by 35%
    plt.barh(category_data['area_name'], category_data['count'])
    plt.xlabel('Number of crimes')
    plt.ylabel('Area Name')
    plt.title(f'Top 5 areas with most {category} crimes')
    plt.gca().invert_yaxis()  # Invert y-axis to show the top areas on top
    plt.tight_layout()
    plt.show()

#***********************************************************************************

# Group data by victim sex and count the number of occurrences
victim_sex_counts = crime_data.groupby('vict_sex')['id'].count()

# Plotting the bar graph
plt.figure(figsize=(10, 6))
victim_sex_counts.plot(kind='bar', color=['blue', 'orange', 'green', 'red', 'purple'])  # You can add more colors as needed
plt.xlabel('Victim Sex')
plt.ylabel('Number of Victims')
plt.title('Count of Victims by Sex')
plt.xticks(rotation=45)  # Rotate x-axis labels for better readability
plt.show()


#*************************************************************************************************************

# Count of crimes for each area and victim sex
crime_counts_by_victim = crime_data.groupby(['area_name', 'vict_sex'])['id'].nunique().reset_index()

# Sort by area_name and count of crimes
crime_counts_by_victim = crime_counts_by_victim.sort_values(['area_name', 'id'], ascending=False)

# Get the top 10 areas based on the count of crimes
top_areas = crime_counts_by_victim.groupby('area_name').size().nlargest(10).index

# Filter the data for top 10 areas
top_areas_data = crime_counts_by_victim[crime_counts_by_victim['area_name'].isin(top_areas)]

# Plotting the graph
plt.figure(figsize=(6.75, 4.5))  # Reduced size by 55%

# Define colors for different genders
colors = {'M': 'blue', 'F': 'orange', 'X': 'green', 'H': 'red', 'N': 'purple'}

# Iterate through each gender and plot bar
for gender, data in top_areas_data.groupby('vict_sex'):
    plt.bar(data['area_name'], data['id'], color=colors[gender], label=gender)

plt.xlabel('Area')
plt.ylabel('Number of crimes')
plt.title('Number of crimes by area and victim gender')
plt.xticks(rotation=45)  # Rotate x-axis labels for better readability
plt.legend(title='Victim Gender')
plt.tight_layout()  # Adjust layout to prevent clipping of labels
plt.show()

#*************************************************************************************

# Load the calls data from a CSV file
calls_data = calls

# Plot a chart identifying areas where maximum number of calls are reported
calls_counts = calls_data.groupby('area_occ')['id'].count().reset_index()
calls_counts = calls_counts.sort_values('id', ascending=False)

# Get data for the donut chart
labels = calls_counts['area_occ']
sizes = calls_counts['id']

# Plotting the donut chart
plt.figure(figsize=(7, 7))  # Reduced size by 44%
plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90, wedgeprops=dict(width=0.3, edgecolor='w'))
plt.title('Number of calls by area')
plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
plt.show()


# Define the categories
categories = {
    "NARCOTICS": ["110 NARCOTIC ACTIVIT", "110 NARCOTIC SUSP", "110 POSS NARCOTIC AC", "110 POSS NARCOTIC SU"],
    "SUSPICION": ["146 POSS SUSP", "146 POSS SUSP J/L", "146 SUSP", "146 SUSP NOW"],
    "ASSAULT": ["187 AMB", "187 POSS", "187 POSS AMB", "187 POSS INVEST", "187 POSS SUSPECT", "187 SUSP NOW"],
    "ATTACK": ["207 AMB", "207 AMB J/O", "207 ATT", "207 ATT INVEST", "207 ATT J/O", "207 INVEST", 
               "207 I/P", "207 J/O", "207 POSS", "207 POSS ATT SUSP", "207 POSS DOM VIOL SU", 
               "207 POSS I/P", "207 POSS J/O", "207 POSS SUSP", "207 POSS SUSP J/L", "207 POSS SUSP NOW", 
               "207 SUSP", "207 SUSP J/L", "207 SUSP NOW"],
    "ROBBERY": ["211 AMB", "211 AMB ATT J/O", "211 AMB GTA", "211 AMB GTA J/O", "211 AMB J/O", 
                "211 ATT", "211 ATT J/O", "211 ATT SUSP J/L", "211 ATT SUSP NOW", "211 GTA", 
                "211 GTA INVEST", "211 GTA I/P", "211 GTA J/O", "211 GTA SUSP", "211 GTA SUSP NOW", 
                "211 INVEST", "211 I/P", "211 J/O", "211 OFCR HLDG", "211 OFCR HLDG ATT", "211 POSS", 
                "211 POSS ATT", "211 POSS ATT P/S", "211 POSS GTA", "211 POSS GTA SUSP", "211 POSS I/P", 
                "211 POSS J/O", "211 POSS SUSP", "211 POSS SUSP J/L", "211 POSS SUSP NOW", "211 P/S", 
                "211 P/S SHOTS FIRED", "211 P/S SUSP", "211 ROBBERY", "211 SUSP", "211 SUSP J/L", 
                "211 SUSP NOW"],
    "BATTERY": ["242 AMB", "242 AMB DOM VIOL", "242 AMB DOM VIOL SUS", "242 AMB INVEST", 
                "242 AMB J/O", "242 AMB SUSP J/L", "242 AMB SUSP NOW", "242 BATTERY", 
                "242 BATTERY DOMESTIC", "242 BATTERY INVEST", "242 DOM VIOL", "242 DOM VIOL INVESTI", 
                "242 DOM VIOL J/O", "242 DOM VIOL R/O VIO", "242 DOM VIOL SUSP", "242 I/P", 
                "242 J/O", "242 J/O  HATE CRIME", "242 OFCR HLDG", "242 POSS AMB", 
                "242 POSS AMB SUSP", "242 POSS DOM VIOL", "242 POSS I/P", "242 POSS J/O", 
                "242 POSS SUSP", "242 POSS SUSP HATE C", "242 POSS SUSP J/L", "242 POSS SUSP NOW", 
                "242 SUSP", "242 SUSP J/L", "242 SUSP J/L HATE CR", "242 SUSP NOW"],
    "ADW": ["245 ADW", "245 ADW POSS DOM VIO", "245 AMB", "245 AMB CUTTING", "245 AMB DOM VIOL", 
            "245 AMB I/P", "245 AMB J/O", "245 AMB SHOTS FIRED", "245 AMB SUSP", "245 AMB SUSP J/L", 
            "245 AMB SUSP NOW", "245 ATTEMPT", "245 ATTEMPT W/VEH", "245 CUTTING INV", "245 DOM VIOL", 
            "245 DOM VIOL IN PROG", "245 DOM VIOL INVEST", "245 DOM VIOL R/O", "245 DOM VIOL SUSP", 
            "245 DOM VIOL SUSP J/", "245 DOM VIOL SUSP NO", "245 INVEST", "245 I/P", "245 I/P W/ VEH", 
            "245 J/O", "245 J/O SHOTS FIRED", "245 OFCR HLDG", "245 POSS", "245 POSS AMB SUSP", 
            "245 POSS DOM VIOL I/", "245 POSS HATE CRIME", "245 POSS I/P", "245 POSS SHOTS FIRED", 
            "245 POSS SUSP", "245 POSS SUSP NOW", "245 SHOTS FIRED", "245 SHOTS FIRED I/P", 
            "245 SHOTS FIRED J/O", "245 SUSP", "245 SUSP J/L", "245 SUSP NOW", "245 W/VEH", 
            "245 W/VEH AMB E/R", "245 W/VEH INVEST", "245 W/VEH INVEST HAT", "245 W/VEH J/O", 
            "245 W/VEH JUST LEFT", "245 W/VEH SUSP", "245 W/VEH SUSP J/L", "245 W/VEH SUSP NOW"],
    "BURGLARY": ["246 AMB", "246 HEARD ONLY", "246 INHAB DWELLING", "246 INHAB DWELL INVE", 
                 "246 INVEST", "246 I/P", "246 POSS HEARD ONLY", "246 POSS INHAB DWELL", 
                 "246 SHOTS FIRED"],
    "HOMICIDE": ["261 AMB", "261 AMB ATT SUSP", "261 AMB INVEST", "261 AMB J/O", "261 ATT", 
                 "261 ATTACK", "261 ATT INVEST", "261 ATT J/O", "261 INVEST", "261 I/P", "261 J/O", 
                 "261 POSS", "261 POSS I/P", "261 POSS SUSP", "261 SUSP", "261 SUSP J/L", 
                 "261 SUSP THERE NOW"],
    "ABUSE": ["288 ABUSE", "288 ABUSE INVEST", "288 ABUSE I/P", "288 ABUSE J/O", "288 ABUSE SUSP", 
              "288 ABUSE SUSP J/L", "288 ABUSE SUSP NOW", "288 ABUSE SUSP/VICT", "288 ALONE", 
              "288 ALONE INVEST", "288 AMB ABUSE", "288 AMB ABUSE INVEST", "288 AMB ABUSE J/O", 
              "288 AMB I/P", "288 ASSAULTING", "288 ASSAULT INVEST", "288 CHILD", "288 MOLEST", 
              "288 MOLEST INVEST", "288 MOLEST SUSP", "288 MOLEST SUSP J/L", "288 MOLEST SUSP NOW", 
              "288 POSS ABUSE", "288 POSS ABUSE INVES", "288 POSS ABUSE J/O", "288 POSS ABUSE SUSP", 
              "288 POSS ALONE", "288 POSS MOLEST", "288 POSS MOLEST INVE", "288 POSS MOLEST SUSP", 
              "288 POSS NEGLECT"],
    "DISPUTE": ["620 BUSN", "620 BUSN R/O", "620 DISPUTE", "620 DISPUTE OTHER", "620 DOM VIOL", 
                "620 DOM VIOL R/O", "620 FAMILY", "620 FAMILY R/O", "620 LANDLORD TENANT", 
                "620 LANDLORD/TENANT", "620 MAN/WMN", "620 NEIGHBOR", "620 NEIGHBOR R/O", 
                "620 ROOMMATE", "620 ROOMMATE R/O"]
}


# Create a new column 'category' based on the values in 'call_type_text'
def categorize_call_type(call_type):
    for category, values in categories.items():
        if call_type in values:
            return category

calls_data['category'] = calls_data['call_type_text'].apply(categorize_call_type)

# Group by 'category' and count occurrences
category_counts = calls_data['category'].value_counts()

# Plotting the bar graph with reduced size
plt.figure(figsize=(6.8, 4.08))  # Reduced size by 32%
category_counts.plot(kind='bar', color='skyblue')
plt.title('Number of Calls by Category')
plt.xlabel('Category')
plt.ylabel('Number of Calls')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.show()

#*****************************************************************************************
# Convert 'dispatch_time' column to datetime format
calls_data['dispatch_time'] = pd.to_datetime(calls_data['dispatch_time'])

# Extract the hour component
calls_data['hour'] = calls_data['dispatch_time'].dt.hour

# Define time windows
time_windows = [(0, 3), (3, 6), (6, 9), (9, 12), (12, 15), (15, 18), (18, 21), (21, 24)]

# Function to categorize the calls based on time windows
def categorize_time_window(hour):
    for start, end in time_windows:
        if start <= hour < end:
            return f"{start:02d}-{end:02d}"
    return "Other"

# Apply the categorization function to create a new column 'time_window'
calls_data['time_window'] = calls_data['hour'].apply(categorize_time_window)

# Group by 'time_window' and count occurrences
time_window_counts = calls_data['time_window'].value_counts()

# Plotting the donut chart
plt.figure(figsize=(7, 7))
plt.pie(time_window_counts, labels=time_window_counts.index, autopct='%1.1f%%', startangle=90, wedgeprops=dict(width=0.3, edgecolor='w'))
plt.title('Number of Calls by Time Window')
plt.axis('equal')
plt.show()

#**********************************************************************************************************

# Group by 'area_name' and count the number of arrests
arrest_counts = arrest.groupby('area_name')['id'].count()

# Plotting the bar graph
plt.figure(figsize=(10, 6))  # Adjust the figure size as needed
arrest_counts.plot(kind='bar', color='skyblue')
plt.title('Number of Arrests by Area')
plt.xlabel('Area Name')
plt.ylabel('Number of Arrests')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.show()

#************************************************************************************************************

# Define categories
categories = {
    "Drug Related": ["Narcotic Drug Laws"],
    "Traffic Violations": ["Moving Traffic Violations"],
    "Property Crime": ["Receive Stolen Property", "Larceny", "Vehicle Theft", "Burglary"],
    "Violent Crime": ["Aggravated Assault", "Robbery", "Homicide"],
    "Public Order": ["Disturbing the Peace", "Disorderly Conduct"],
    "Alcohol Related": ["Liquor Laws", "Drunkeness", "Driving Under Influence"],
    "Sexual Offenses": ["Rape", "Sex (except rape/prst)"],
    "Other": ["Federal Offenses", "Gambling", "Miscellaneous Other Violations", "Non-Criminal Detention",
              "Pre-Delinquency", "Weapon (carry/poss)", "Prostitution/Allied", "Fraud/Embezzlement",
              "Forgery/Counterfeit", "Other Assaults", "Against Family/Child"]
}

# Create a new column 'crime_category' based on the values in 'charge_group_description'
def categorize_charge(charge):
    for category, values in categories.items():
        if charge in values:
            return category
    return "Other"

arrest['crime_category'] = arrest['charge_group_description'].apply(categorize_charge)

# Group by 'crime_category' and count occurrences
crime_category_counts = arrest['crime_category'].value_counts()

# Plotting the pie chart with reduced size
plt.figure(figsize=(6, 6))  # Reduced size
crime_category_counts.plot(kind='pie', autopct='%1.1f%%', startangle=140, colors=plt.cm.tab20.colors)
plt.title('Distribution of Crime Categories')
plt.ylabel('')
plt.axis('equal')
plt.show()

#************************************************************************************************************


# Count of crimes for each area and victim sex
crime_counts_by_victim = arrest.groupby(['area_name', 'sex_code'])['id'].nunique().reset_index()

# Pivot the data to have areas as rows and victim genders as columns
pivot_table = crime_counts_by_victim.pivot(index='area_name', columns='sex_code', values='id').fillna(0)

# Plotting the graph
plt.figure(figsize=(10, 6))

# Define bar width
bar_width = 0.35

# Get the x-axis positions
x = np.arange(len(pivot_table))

# Plot bars for each gender
plt.bar(x - bar_width/2, pivot_table['M'], bar_width, color='blue', label='Male')
plt.bar(x + bar_width/2, pivot_table['F'], bar_width, color='red', label='Female')

plt.xlabel('Area')
plt.ylabel('Number of crimes')
plt.title('Number of crimes by area and victim gender')
plt.xticks(x, pivot_table.index, rotation=45)  # Set x-axis labels to area names
plt.legend(title='Victim Gender')
plt.tight_layout()
plt.show()

#************************************************************************************************************


# Define age categories
age_categories = {
    'Children': (0, 12),
    'Teenagers': (13, 19),
    'Young Adults': (20, 29),
    'Adults (30-39)': (30, 39),
    'Adults (40-49)': (40, 49),
    'Adults (50-59)': (50, 59),
    'Seniors': (60, 120)  # Assuming maximum age as 120
}

# Function to assign age category
def get_age_category(age):
    for category, (min_age, max_age) in age_categories.items():
        if min_age <= age <= max_age:
            return category
    return 'Unknown'

# Apply age category function to create a new column
arrest['age_category'] = arrest['age'].apply(get_age_category)

# Count the number of arrests in each age category
arrests_by_age_category = arrest.groupby('age_category')['id'].count()

# Plotting the donut chart
plt.figure(figsize=(7, 7))

# Colors for the sections
colors = ['#ff9999','#66b3ff','#99ff99','#ffcc99', '#c2c2f0', '#ffb3e6', '#c2f0c2']

# Create pie chart
plt.pie(arrests_by_age_category, labels=arrests_by_age_category.index, autopct='%1.1f%%', startangle=90, colors=colors)

# Draw a circle in the middle to create a donut chart
centre_circle = plt.Circle((0,0),0.70,fc='white')
fig = plt.gcf()
fig.gca().add_artist(centre_circle)

plt.title('Arrests by Age Category')
plt.axis('equal')  
plt.tight_layout()
plt.show()

#**********************************************************************************************************



# Define disposition descriptions and their counts
disposition_counts = {
    "Legal Proceedings": [
        "PRE-FILING INVESTIGATION",
        "MISDEMEANOR COMPLAINT FILED",
        "FELONY COMPLAINT FILED",
        "PROVED ADULT",
        "DISTRICT ATTORNEY REJECT",
        "CITY ATTORNEY REJECT"
    ],
    "Social Services": [
        "DEPARTMENT OF SOCIAL SERVICES",
        "DEPARTMENT OF MENTAL HEALTH",
        "CALIFORNIA YOUTH AUTHORITY",
        "COMMUNITY SERVICE"
    ],
    "Release": [
        "RELEASED/INSUFFICENT EVIDENCE",
        "COUNSELED/RELEASED",
        "RELEASED TO MILITARY AUTHORITY",
        "RELEASED PETITION"
    ],
    "Technical Arrest": [
        "849(B)(1) PC/TECHNICAL ARREST"
    ],
    "Other Law Enforcement": [
        "OTHER LAW ENFORCEMENT AGENCY",
        "ACTION SUSP"
    ],
    "Miscellaneous": [
        "OTHER (REQUIRES ADDITION ACTION)",
        "OTHER DISPOSITION"
    ],
    "Probation": [
        "PROBATION"
    ],
    "Court Ordered": [
        "PROBATION",
        "COMMUNITY SERVICE"
    ]
}

# Aggregate counts for each category
category_counts = {category: 0 for category in disposition_counts}
for category, dispositions in disposition_counts.items():
    category_counts[category] = sum(disposition_counts[category].count(disposition) for disposition in arrest['disposition_description'])

# Plotting the bar graph
plt.figure(figsize=(8, 6))

# Extracting category names and counts
categories = list(category_counts.keys())
counts = list(category_counts.values())

# Plotting the bar graph
plt.barh(categories, counts, color='#86bf91')

# Adding labels and title
plt.xlabel('Count')
plt.ylabel('Disposition Categories')
plt.title('Distribution of Disposition Categories')

# Display the plot
plt.tight_layout()
plt.show()

#*********************************************************************************************************************

# Arrest based on area and sex 
arrest_sex = arrest.groupby(['area_name', 'sex_code'])['charge_group_code'].count().reset_index() 

# Arrest based on age and area 
arrest_age = arrest.groupby(['area_name', 'age'])['charge_group_code'].count().reset_index() 

# Arrest based on charge_group_description and count of arrest 
arrest_charge = arrest.groupby('charge_group_description')['charge_group_code'].count().reset_index() 

# Arrest based on charge_group_description and area_name 
arrest_area = arrest.groupby(['area_name', 'charge_group_description'])['charge_group_code'].count().reset_index()
