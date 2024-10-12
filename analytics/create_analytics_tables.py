
# IMPORTACION BIBLIOTECAS REQUERIDAS # 

import db_utils

# DEFINICION DE LA CONEXION A REDSHIFT #

redshift_schema = db_utils.import_db_variables()['redshift_schema']
connection = db_utils.connect_to_redshift()
 
# VERIFICACION EXISTENCIA DE NUESTRA TABLA , SI ES PRIMERA VEZ Y NO EXISTE LA CREA # 

try:
    
    table_existence = f"""select 1 from "{redshift_schema}".analytics_fact_daily_detail_tickers"""
    
    connection.execute(table_existence)
    
    print("Table analytics_fact_daily_detail_tickers is already created\n")
    
except Exception as e:
    
    create_table_query = f"""
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
    """

    connection.execute(create_table_query)
    
    print("Table analytics_fact_daily_detail_tickers was created correctly\n")
    
try:
    
    table_existence = f"""select 1 from "{redshift_schema}".analytics_fact_daily_summary_tickers"""
    
    connection.execute(table_existence)
    
    print("Table analytics_fact_daily_summary_tickers is already created\n")
    
except Exception as e:
    
    create_table_query = f"""
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
    """
    
    connection.execute(create_table_query)
    
    print("Table analytics_fact_daily_summary_tickers was created correctly\n")
    
try:
    
    table_existence = f"""select 1 from "{redshift_schema}".analytics_dim_tickers"""
    
    connection.execute(table_existence)
    
    print("Table analytics_dim_tickers is already created\n")
    
except Exception as e:
    
    create_table_query = f"""
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

    connection.execute(create_table_query)
    
    print("Table analytics_dim_tickers was created correctly\n")
