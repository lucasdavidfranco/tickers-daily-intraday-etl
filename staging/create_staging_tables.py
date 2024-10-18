
# IMPORTACION BIBLIOTECAS REQUERIDAS # 

import os
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.join(current_dir, '..')
sys.path.append(project_root)
import utils.db_utils as db_utils

def create_staging_tables():
    
    ''' Checks if all staging tables exists and creates them if they are not '''

    redshift_schema = db_utils.import_db_variables()['redshift_schema']
    connection = db_utils.connect_to_redshift()

    tables = {
        "staging_daily_tickers": f"""
            CREATE TABLE IF NOT EXISTS "{redshift_schema}".staging_daily_tickers (
                event_date DATE,
                open_value DOUBLE PRECISION,
                high_value DOUBLE PRECISION,
                low_value DOUBLE PRECISION,
                close_value DOUBLE PRECISION,
                volume_amount DOUBLE PRECISION,
                ticker VARCHAR(10),
                audit_datetime TIMESTAMP
            );
        """,
        "staging_intraday_tickers": f"""
            CREATE TABLE IF NOT EXISTS "{redshift_schema}".staging_intraday_tickers (
                event_datetime TIMESTAMP,
                open_value DOUBLE PRECISION,
                high_value DOUBLE PRECISION,
                low_value DOUBLE PRECISION,
                close_value DOUBLE PRECISION,
                volume_amount DOUBLE PRECISION,
                ticker VARCHAR(10),
                audit_datetime TIMESTAMP
            );
        """
    }
    
    for table_name, create_query in tables.items():
        
        check_table_query = f"""select 1 from information_schema.tables where table_schema = '{redshift_schema}' and table_name = '{table_name}'"""
        exists_table = connection.execute(check_table_query).fetchone()
        
        if exists_table is not None:
        
            print(f"Table {table_name} is already created\n")
    
        else:
            
            connection.execute(create_query)
            print(f"Table {table_name} was created correctly\n")

    connection.close()
