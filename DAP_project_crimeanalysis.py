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

#**************************************************************************************
