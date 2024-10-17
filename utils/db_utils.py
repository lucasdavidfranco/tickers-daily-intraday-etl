
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import hashlib
import sys

def import_db_variables():
    
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

def import_api_variables():
    
    load_dotenv()
    
    alpha_url = "https://www.alphavantage.co/query"
    alpha_key = os.getenv('ALPHA_KEY')
    twelve_url = "https://api.twelvedata.com/time_series"
    twelve_key = os.getenv('TWELVE_KEY')
    tickers = ['XOM','CVX'] #['XOM', 'CVX', 'SHEL', 'JPM', 'BAC', 'WFC']

    return {
        'alpha_url': alpha_url,
        'alpha_key': alpha_key,
        'twelve_url': twelve_url,
        'twelve_key': twelve_key,
        'tickers': tickers
    }

def connect_to_redshift():
    
    db_variables = import_db_variables()
    user = db_variables['user']
    password = db_variables['password']
    host = db_variables['host']
    port = db_variables['port']
    database = db_variables['database']
    connection_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(connection_string)
    
    try:
        
        connection = engine.connect()
        connection_query = """SELECT schema_name FROM information_schema.schemata;"""
        connection.execute(connection_query)
        return connection  # Retorna la conexión para poder usarla

    except Exception as e:
        
        print(f"Unable to connect to Redshift database\nError: {e}")
        sys.exit("End of process")

def subrogate_key(*cols):
    
    combined_str = ''.join(cols)
    return hashlib.sha1(combined_str.encode()).hexdigest()
