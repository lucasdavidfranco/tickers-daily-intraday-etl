
import os
from dotenv import load_dotenv

def import_api_variables():
    
    '''
    
    Import api variables
    
    We get from an .env file API keys to retrieve data. 
    
    Also on this function is set all tickers that will be processed on full ETL process
    
    '''
    
    load_dotenv()
    
    alpha_url = "https://www.alphavantage.co/query"
    alpha_key = os.getenv('ALPHA_KEY')
    twelve_url = "https://api.twelvedata.com/time_series"
    twelve_key = os.getenv('TWELVE_KEY')
    tickers = ['XOM', 'CVX', 'JPM']

    return {
        'alpha_url': alpha_url,
        'alpha_key': alpha_key,
        'twelve_url': twelve_url,
        'twelve_key': twelve_key,
        'tickers': tickers
    }
