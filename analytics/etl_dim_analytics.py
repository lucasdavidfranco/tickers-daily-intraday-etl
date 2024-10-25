
import requests
import pandas as pd
import sys
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.join(current_dir, '..')
sys.path.append(project_root)
import utils.db_utils as db_utils
import utils.api_utils as api_utils

def extract_dimension_data(parquet_path=None):
    
    ''' Gets data from alphavantage API
    
    This function is to retrieve dimensional data of tickers.
    
    Due to the fact that only one ticker can be requested in every api call, this process is iterated for every ticker using a for loop
    
    Once every ticker has been requested to the API, all data is concatenated on a final dataframe used in next step transform_daily_data
    
    If request can not be done, process ends with its error code 
    
    '''
    
    alpha_url = api_utils.import_api_variables()['alpha_url']
    alpha_key = api_utils.import_api_variables()['alpha_key']
    tickers = api_utils.import_api_variables()['tickers']
    
    ticker_params = {
        'function': 'OVERVIEW',
        'apikey':  alpha_key,
    }
    
    dataframe_append = []

    for ticker in tickers:
        
        ticker_params['symbol'] = ticker
        ticker_response = requests.get(alpha_url, params = ticker_params)
                
        if ticker_response.status_code == 200:

            raw_json = ticker_response.json()
            ticker_dataframe = pd.DataFrame.from_dict(raw_json, orient='index').T
            dataframe_append.append(ticker_dataframe)

        else:
            
            print(f"Error: Could not retrieve data (State code: {ticker_response.status_code})\n")
            ticker_response.raise_for_status()
            
    ticker_dataframe_final = pd.concat(dataframe_append, ignore_index = True)
    if not parquet_path:
        parquet_path = os.path.join(current_dir, 'data/extract_dimension_data.parquet')
    ticker_dataframe_final.to_parquet(parquet_path)


def transform_dimension_data(read_dir=None, write_dir=None):
    
    ''' Tranforms data retrieved from API
    
    Once we get the data, this is transformed using pandas
    
    We convert columns to numeric using pandas and drop columns that are not necessary
    
    We create new columns such as is_current, audit_datetime, analyst_rating and subrogate_key
    
    Analyst rating is a column based on four numeric columns which after applying a map we can set which is analyst most popular recommendation
    
    Subrogate key is based on a function. We create this field to get a unique identificator for the row based on attributes 
    
    This identificator will help us on upload_dimension_data to determinate if a record is new and should be inserted or not
    
    '''
    
    current_dir = os.path.dirname(os.path.abspath(__file__))
    write_dir = write_dir or os.path.join(current_dir, "data")
    read_dir = read_dir or os.path.join(current_dir, "data")
    
    os.makedirs(write_dir, exist_ok=True)
    
    parquet_read = os.path.join(read_dir, 'extract_dimension_data.parquet')
    ticker_dataframe = pd.read_parquet(parquet_read)
    ticker_dataframe_filter = ticker_dataframe[['Symbol', 'AssetType', 'Name', 'Country', 'Sector', 'Industry', 'Address', 'OfficialSite', 'AnalystRatingStrongBuy', 'AnalystRatingBuy', 'AnalystRatingHold', 'AnalystRatingSell', 'AnalystRatingStrongSell']].copy()
    ticker_dataframe_filter.columns = ['ticker', 'asset_type', 'name', 'country', 'sector', 'industry', 'address', 'official_site', 'strong_buy_rating', 'buy_rating', 'hold_rating', 'sell_rating', 'strong_sell_rating']
    string_columns = ['asset_type', 'name', 'country', 'sector', 'industry', 'address', 'official_site']
    numeric_columns = ['strong_buy_rating', 'buy_rating', 'hold_rating', 'sell_rating', 'strong_sell_rating']
    ticker_dataframe_filter.loc[:, string_columns] = ticker_dataframe_filter[string_columns].apply(lambda col: col.str.slice(0, 50))
    ticker_dataframe_filter.loc[:, numeric_columns] = ticker_dataframe_filter[numeric_columns].apply(pd.to_numeric, errors = 'coerce')
    ticker_dataframe_filter[numeric_columns] = ticker_dataframe_filter[numeric_columns].fillna(-1)
    rating_map = {
        'strong_buy_rating': 'Strong Buy',
        'buy_rating': 'Buy',
        'hold_rating': 'Hold',
        'sell_rating': 'Sell',
        'strong_sell_rating': 'Strong Sell'
    }
    ticker_dataframe_final = ticker_dataframe_filter.copy()
    ticker_dataframe_final['analyst_rating'] = ticker_dataframe_filter[numeric_columns].idxmax(axis=1).map(rating_map)
    ticker_dataframe_final.drop(numeric_columns, axis=1, inplace=True)
    ticker_dataframe_final.loc[:, 'subrogate_key'] = ticker_dataframe_final[['ticker', 'asset_type', 'name', 'country', 'sector', 'industry', 'address', 'official_site', 'analyst_rating']].astype(str).apply(lambda row: db_utils.subrogate_key(*row), axis = 1)
    ticker_dataframe_final = ticker_dataframe_final.assign(
        is_current = 1,
        audit_datetime = pd.Timestamp.now()
    )
    parquet_transform = os.path.join(write_dir, f'transform_dimension_data.parquet')
    ticker_dataframe_final.to_parquet(parquet_transform)


def load_dimension_data(read_dir=None):
    
    ''' Load data retrieved from API
    
    Once we get the data in proper data types and transformed we upload it to redshift using SQL Alchemy engine
    
    For the upload 4 SQL scripts are run: 
    
    create_temp_table_query: This scripts creates a temporary table which is automatically deleted when session is over.
    Data retrieved from API is uploaded to this temporary table.
    
    replace_old_dimensions: In case we retrieve a ticker that has changes and exists on table, subrogate key will change, so data that 
    is on this table for this ticker is no longer current data. Is_current flg is set to 0 and date_to is set to previous day. 
    Also is updated audit_datetime to keep a track when this record was set to not current

    update_last_refresh: In case we retrieve a ticker that does not have changes, subrogate key will be the same. Data on the table
    for this ticker is current data. We update audit_datetime to keep a track when this record was last retrieved
    
    insert_new_dimensions: In case we retrieve a new ticker or a ticker that has changes, subrogate key will change. 
    This data is new current data. We upload it to dimension data
    
    In case there is any error on this, a log will be printed with its error on airflow log   
    
    '''

    redshift_schema = db_utils.import_db_variables()['redshift_schema']
    connection = db_utils.connect_to_redshift()  
    read_dir = read_dir or os.path.join(current_dir, "data")
    parquet_read = os.path.join(read_dir, 'transform_dimension_data.parquet')
    ticker_dataframe_final = pd.read_parquet(parquet_read)
   
    queries = {
        "create_temp_table_query": """
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
        """,
        "replace_old_dimensions": f"""
            UPDATE "{redshift_schema}".analytics_dim_tickers
            SET
                date_to = dateadd('day', -1, current_date),
                is_current = 0,
                audit_datetime = temp_dim_tickers.audit_datetime
            FROM temp_dim_tickers
            WHERE "{redshift_schema}".analytics_dim_tickers.ticker = temp_dim_tickers.ticker 
            AND "{redshift_schema}".analytics_dim_tickers.subrogate_key != temp_dim_tickers.subrogate_key;
        """,
        "update_last_refresh": f"""
            UPDATE "{redshift_schema}".analytics_dim_tickers
            SET
                audit_datetime = temp_dim_tickers.audit_datetime
            FROM temp_dim_tickers
            WHERE "{redshift_schema}".analytics_dim_tickers.ticker = temp_dim_tickers.ticker 
            AND "{redshift_schema}".analytics_dim_tickers.subrogate_key = temp_dim_tickers.subrogate_key;
        """,
        "insert_new_dimensions": f"""
            INSERT INTO "{redshift_schema}".analytics_dim_tickers (
                ticker, 
                asset_type, 
                name, country, 
                sector,
                industry,
                address, 
                official_site,
                analyst_rating,
                subrogate_key, 
                date_from, 
                date_to, 
                is_current, 
                audit_datetime
            )
            SELECT 
                t.ticker, 
                t.asset_type, 
                t.name, 
                t.country, 
                t.sector,
                t.industry,
                t.address,
                t.official_site,
                t.analyst_rating, 
                t.subrogate_key, 
                current_date as date_from, 
                date'2099-12-31' as date_to, 
                t.is_current, 
                t.audit_datetime
            FROM temp_dim_tickers as t
            WHERE NOT EXISTS (SELECT 1 FROM "{redshift_schema}".analytics_dim_tickers d WHERE d.subrogate_key = t.subrogate_key);
        """
    }
   
    for query_name, query_definition in queries.items():
        
        if query_name == 'create_temp_table_query':

            connection.execute(query_definition)
            ticker_dataframe_final.to_sql(name = 'temp_dim_tickers', con = connection, schema = None, index=False, if_exists='append')
            
        else:
                
            connection.execute(query_definition)
        
    print("Table analytics_dim_tickers up to date")
    connection.close()
