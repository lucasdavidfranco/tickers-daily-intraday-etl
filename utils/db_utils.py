
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import hashlib

def import_db_variables():
    
    '''
    
    Import redshift database variables
    
    We get from an .env file all configurations of the database
    
    '''
    
    load_dotenv()
    
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    host = os.getenv('DB_HOST')
    port = os.getenv('DB_PORT')
    database = os.getenv('DB_NAME')
    redshift_schema = os.getenv('REDSHIFT_SCHEMA')
    
    return {
        'user': user,
        'password': password,
        'host': host,
        'port': port,
        'database': database,
        'redshift_schema': redshift_schema
    }

def connect_to_redshift():
    
    '''
    
    Connection to redshift
    
    We get a sql alchemy engine to perform all type of .sql scripts on our database 
    
    '''
    
    db_variables = import_db_variables()
    user = db_variables['user']
    password = db_variables['password']
    host = db_variables['host']
    port = db_variables['port']
    database = db_variables['database']
    connection_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(connection_string)
    connection = engine.connect()
    connection_query = """SELECT schema_name FROM information_schema.schemata;"""
    connection.execute(connection_query)
    return connection

def subrogate_key(*cols):
    
    '''
    
    Subrogate key
    
    Hashes a record based on input columns to obtain a unique identifiers
    
    '''
    
    combined_str = ''.join(cols)
    return hashlib.sha1(combined_str.encode()).hexdigest()
