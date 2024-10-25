
import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
import sys
import os
import tempfile

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.join(current_dir, '../..')
sys.path.append(project_root)

from staging.extract_staging_data import extract_intraday_data
from staging.extract_staging_data import extract_daily_data

@patch('utils.api_utils.import_api_variables')
@patch('staging.extract_staging_data.requests.get')
def test_extract_intraday_data(mock_get, mock_import_api_variables):
    
    '''
    
    Test extract intraday data
    
    We use unitttest and run a pytest to mock a fake api call
    
    Then we check if our fake api is processed as how we expected with assert validations
    
    '''
    
    with tempfile.TemporaryDirectory() as temp_dir:
        parquet_path = os.path.join(temp_dir, 'extract_intraday_data.parquet')
    
        mock_import_api_variables.return_value = {
            'twelve_url': 'twelve_url',
            'twelve_key': 'twelve_key',
            'tickers': ['AAPL']
        }
        
        mock_get.return_value.status_code = 200
        
        mock_get.return_value.json.return_value = {
            "values": [
                {
                    "datetime": "2020-02-26 15:59:00",
                    "open": "292.89001",
                    "high": "293.10001",
                    "low": "292.50000",
                    "close": "292.64999",
                    "volume": "385977"
                },
            ],
            "status": "ok"
        }
        
        extract_intraday_data(parquet_path=parquet_path)
        
        assert os.path.exists(parquet_path)
        
        result = pd.read_parquet(parquet_path)
        assert not result.empty
        assert 'ticker' in result.columns
        assert 'event_datetime' in result.columns
        assert 'open_value' in result.columns
        assert 'high_value' in result.columns
        assert 'low_value' in result.columns
        assert 'close_value' in result.columns
        assert 'volume_amount' in result.columns
        assert len(result['ticker'].unique()) == 1
        assert result['ticker'].unique()[0] == 'AAPL'
        assert pd.to_datetime(result['event_datetime'], errors='coerce').notnull().all()


@patch('utils.db_utils.connect_to_redshift')
@patch('utils.api_utils.import_api_variables')
@patch('utils.db_utils.import_db_variables')
@patch('staging.extract_staging_data.requests.get')
def test_extract_daily_data(mock_get, mock_import_db_variables, mock_import_api_variables, mock_connect_to_redshift):
    
    '''
    
    Test extract daily data
    
    We use unitttest and run a pytest to mock a fake api call
    
    Then we check if our fake api is processed as how we expected with assert validations
    
    '''
    
    with tempfile.TemporaryDirectory() as temp_dir:
        parquet_path = os.path.join(temp_dir, 'extract_daily_data.parquet')
        
        mock_import_api_variables.return_value = {
            'alpha_url': 'alpha_url',
            'alpha_key': 'alpha_key',
            'tickers': ['AAPL']
        }
        
        mock_import_db_variables.return_value = {'redshift_schema': 'fake_schema'}
        mock_connection = MagicMock()
        mock_connect_to_redshift.return_value = mock_connection
        mock_connection.execute.return_value.fetchone.return_value = [None]
        mock_get.return_value.status_code = 200
        
        mock_get.return_value.json.return_value = {
            "Time Series (Daily)": {
                "2024-10-16": {
                    "1. open": "232.1100",
                    "2. high": "233.8800",
                    "3. low": "231.1200",
                    "4. close": "233.6700",
                    "5. volume": "2846669"
                },
            }
        }
        
        extract_daily_data(parquet_path=parquet_path)
        
        result = pd.read_parquet(parquet_path)
        assert not result.empty
        assert 'ticker' in result.columns
        assert 'event_date' in result.columns
        assert 'open_value' in result.columns
        assert 'high_value' in result.columns
        assert 'low_value' in result.columns
        assert 'close_value' in result.columns
        assert 'volume_amount' in result.columns
        assert len(result['ticker'].unique()) == 1
        assert result['ticker'].unique()[0] == 'AAPL'
        assert pd.to_datetime(result['event_date'], errors='coerce').notnull().all()

if __name__ == "__main__":
    pytest.main()
