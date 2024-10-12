
# IMPORTACION BIBLIOTECAS REQUERIDAS # 

import pandas as pd
import sys
import hashlib
import db_utils

# DEFINICION DE LA CONEXION A REDSHIFT #

def upload_dimension_data():

    redshift_schema = db_utils.import_db_variables()['redshift_schema']
    connection = db_utils.connect_to_redshift()
    alpha_url = db_utils.import_api_variables()['alpha_url']
    alpha_key = db_utils.import_api_variables()['alpha_key']
    tickers = db_utils.import_api_variables()['tickers']


        
    # TRANSFORMACIONES A LA RAW DATA PARA OBTENER UN DATAFRAME CON COLUMNAS DESEADAS Y FORMATO DESEADO # 
    
    ticker_dataframe = pd.DataFrame.from_dict(raw_json, orient='index').T
    
    ticker_dataframe_filter = ticker_dataframe[['Symbol', 'AssetType', 'Name', 'Country', 'Sector', 'Industry', 'Address', 'OfficialSite', 'AnalystRatingStrongBuy', 'AnalystRatingBuy', 'AnalystRatingHold', 'AnalystRatingSell', 'AnalystRatingStrongSell']].copy()
    
    ticker_dataframe_filter.columns = ['ticker', 'asset_type', 'name', 'country', 'sector', 'industry', 'address', 'official_site', 'strong_buy_rating', 'buy_rating', 'hold_rating', 'sell_rating', 'strong_sell_rating']
    
    string_columns = ['asset_type', 'name', 'country', 'sector', 'industry', 'address', 'official_site']
    
    numeric_columns = ['strong_buy_rating', 'buy_rating', 'hold_rating', 'sell_rating', 'strong_sell_rating']
    
    ticker_dataframe_filter.loc[:, string_columns] = ticker_dataframe_filter[string_columns].apply(lambda col: col.str.slice(0, 50))
    
    ticker_dataframe_filter.loc[:, numeric_columns] = ticker_dataframe_filter[numeric_columns].apply(pd.to_numeric, errors = 'coerce')
    
    dataframe_append.append(ticker_dataframe_filter)

ticker_dataframe_final = pd.concat(dataframe_append, ignore_index = True)

rating_map = {
    'strong_buy_rating': 'Strong Buy',
    'buy_rating': 'Buy',
    'hold_rating': 'Hold',
    'sell_rating': 'Sell',
    'strong_sell_rating': 'Strong Sell'
}

ticker_dataframe_final['analyst_rating'] = ticker_dataframe_final[['strong_buy_rating', 'buy_rating', 'hold_rating', 'sell_rating', 'strong_sell_rating']].idxmax(axis=1).map(rating_map)

ticker_dataframe_final.drop(['strong_buy_rating', 'buy_rating', 'hold_rating', 'sell_rating', 'strong_sell_rating'], axis=1, inplace=True)

def subrogate_key(*cols):
    combined_str = ''.join(cols)
    return hashlib.sha1(combined_str.encode()).hexdigest()

ticker_dataframe_final.loc[:, 'subrogate_key'] = ticker_dataframe_final[['ticker', 'asset_type', 'name', 'country', 'sector', 'industry', 'address', 'official_site', 'analyst_rating']].astype(str).apply(lambda row: subrogate_key(*row), axis = 1)

ticker_dataframe_final = ticker_dataframe_final.assign(
    is_current = 1,
    audit_datetime = pd.Timestamp.now()
)
   
create_temp_table_query = """
    CREATE TEMPORARY TABLE temp_dim_tickers (
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
            is_current DOUBLE PRECISION,
            audit_datetime TIMESTAMP
    );
"""

try:
    
    connection.execute(create_temp_table_query)
        
    print("Temporary table created\n")
        
except Exception as e:
        
    print(f"Error creating temporary table {e}\n")
    
    connection.close()
    
    sys.exit("End of process")


try:
        
    ticker_dataframe_final.to_sql(name = 'temp_dim_tickers', con = connection, schema = None, index=False, if_exists='append')

    print("Data uploaded to temporary table\n")

except Exception as e:
        
    print(f"Could not upload data to temporary table: {e}\n")
    
    connection.close()
    
    sys.exit("End of process")

update_query = f"""

    UPDATE "{redshift_schema}".analytics_dim_tickers
    SET
        asset_type = temp_dim_tickers.asset_type,
        name = temp_dim_tickers.name,
        country = temp_dim_tickers.country,
        sector = temp_dim_tickers.sector,
        industry = temp_dim_tickers.industry,
        address = temp_dim_tickers.address,
        official_site = temp_dim_tickers.official_site,
        analyst_rating = temp_dim_tickers.analyst_rating,
        date_to = dateadd('day', -1, current_date),
        is_current = 0,
        audit_datetime = temp_dim_tickers.audit_datetime
    FROM temp_dim_tickers
    WHERE "{redshift_schema}".analytics_dim_tickers.ticker = temp_dim_tickers.ticker 
    AND "{redshift_schema}".analytics_dim_tickers.subrogate_key != temp_dim_tickers.subrogate_key;

    INSERT INTO "{redshift_schema}".analytics_dim_tickers (ticker, asset_type, name, country, sector, industry,
        address, official_site, weight_rank, subrogate_key, date_from, date_to, is_current, audit_datetime)
    SELECT t.ticker, t.asset_type, t.name, t.country, t.sector, t.industry, t.address, t.official_site,
        t.analyst_rating, t.subrogate_key, current_date as date_from, date'2099-12-31' as date_to, t.is_current, t.audit_datetime
    FROM temp_dim_tickers as t
    WHERE NOT EXISTS (SELECT 1 FROM "{redshift_schema}".analytics_dim_tickers d WHERE d.subrogate_key = t.subrogate_key);

"""

try:
    
    connection.execute(update_query)

    print("Data refreshed on incremental table\n")
    
    connection.close()

except Exception as e:
        
    print(f"Could not refresh data {e} \n")
    
    connection.close()
    
    sys.exit("End of process")
