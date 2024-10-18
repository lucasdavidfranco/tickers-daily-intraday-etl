
# IMPORTACION BIBLIOTECAS REQUERIDAS # 

import sys
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.join(current_dir, '..')
sys.path.append(project_root)
import utils.db_utils as db_utils

def create_analytics_tables():

    redshift_schema = db_utils.import_db_variables()['redshift_schema']
    connection = db_utils.connect_to_redshift()

    tables = {
        "analytics_fact_daily_detail_tickers": f"""
            CREATE TABLE IF NOT EXISTS "{redshift_schema}".analytics_fact_daily_detail_tickers (
                event_datetime TIMESTAMP,
                ticker VARCHAR(10),
                open_value DOUBLE PRECISION,
                high_value DOUBLE PRECISION,
                low_value DOUBLE PRECISION,
                close_value DOUBLE PRECISION,
                volume_amount DOUBLE PRECISION,
                close_value_sma DOUBLE PRECISION,
                volume_sma DOUBLE PRECISION,
                previous_volume_amount DOUBLE PRECISION,
                minute_volume_amount_variation DOUBLE PRECISION,
                previous_close_value DOUBLE PRECISION,
                minute_close_value_variation DOUBLE PRECISION,
                audit_datetime TIMESTAMP
        );
        """,
        "analytics_fact_daily_summary_tickers": f"""
            CREATE TABLE IF NOT EXISTS "{redshift_schema}".analytics_fact_daily_summary_tickers (
                event_date DATE,
                ticker VARCHAR(10),
                open_value DOUBLE PRECISION,
                high_value DOUBLE PRECISION,
                low_value DOUBLE PRECISION,
                close_value DOUBLE PRECISION,
                volume_amount DOUBLE PRECISION,
                close_value_sma DOUBLE PRECISION,
                volume_sma DOUBLE PRECISION,
                previous_volume_amount DOUBLE PRECISION,
                daily_volume_amount_variation DOUBLE PRECISION,
                previous_close_value DOUBLE PRECISION,
                daily_close_value_variation DOUBLE PRECISION,
                audit_datetime TIMESTAMP
        );
        """, 
        "analytics_dim_tickers": f"""
            CREATE TABLE IF NOT EXISTS "{redshift_schema}".analytics_dim_tickers (
                ticker VARCHAR(10), 
                asset_type VARCHAR(50), 
                name VARCHAR(50),
                country VARCHAR(10),
                sector VARCHAR(50),
                industry VARCHAR(50),
                address VARCHAR(50),
                official_site VARCHAR(50),
                analyst_rating VARCHAR(30),
                subrogate_key VARCHAR(50),
                date_from DATE,
                date_to DATE,
                is_current DOUBLE PRECISION,
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
