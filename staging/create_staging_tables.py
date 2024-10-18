
# IMPORTACION BIBLIOTECAS REQUERIDAS # 

import os
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.join(current_dir, '..')
sys.path.append(project_root)
import utils.db_utils as db_utils

def create_staging_tables():

    redshift_schema = db_utils.import_db_variables()['redshift_schema']
    connection = db_utils.connect_to_redshift()

    try:
        
        table_existence = f"""select 1 from "{redshift_schema}".staging_daily_tickers"""
        connection.execute(table_existence)
        print("Table staging_daily_tickers is already created\n")
        
    except Exception as e:
        
        create_table_query = f"""
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
        """
        
        connection.execute(create_table_query)
        print("Table staging_daily_tickers was created correctly\n")
        
    try:
        
        table_existence = f"""select 1 from "{redshift_schema}".staging_intraday_tickers"""
        connection.execute(table_existence)
        print("Table staging_intraday_tickers is already created\n")
        
    except Exception as e:
        
        create_table_query = f"""
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
        
        connection.execute(create_table_query)
        print("Table staging_intraday_tickers was created correctly\n")
    
    connection.close
