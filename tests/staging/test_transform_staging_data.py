
import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
import sys
import os
import tempfile

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.join(current_dir, '../..')
sys.path.append(project_root)

from staging.transform_staging_data import transform_staging_data

@patch('utils.db_utils.connect_to_redshift')
@patch('utils.db_utils.import_db_variables')
@patch('pandas.read_sql')
def test_transform_intraday_data(mock_read_sql, mock_import_db_variables, mock_connect_to_redshift):
    
    '''
    
    Test transform intraday data
    
    We use unitttest and run a pytest to mock a fake redshift connection, read_sql and fake data retrieved from API
    
    Then we check if our fake data is processed as how we expected with assert validations
    
    '''
    
    mock_import_db_variables.return_value = {
        'redshift_schema': 'fake_schema'
    }
    
    mock_connection = MagicMock()
    mock_connect_to_redshift.return_value = mock_connection
    
    mock_read_sql.side_effect = [
        pd.DataFrame({
            'ticker': ['AAPL'],
            'last_event_datetime': [pd.Timestamp('2020-02-26 15:58:00')]
        }),
        pd.DataFrame({
            'ticker': ['AAPL'],
            'last_event_date': [pd.Timestamp('2020-02-25').date()]
        })
    ]

    with tempfile.TemporaryDirectory() as temp_dir:
        
        intraday_data = pd.DataFrame({
            'event_datetime': ['2020-02-26 15:59:00'],
            'open_value': ['292.89001'],
            'high_value': ['293.10001'],
            'low_value': ['292.50000'], 
            'close_value': ['292.64999'],
            'volume_amount': ['385977'],
            'ticker': ['AAPL']
        })
        
        daily_data = pd.DataFrame({
            'event_date': ['2024-10-16'],
            'open_value': ['232.1100'],
            'high_value': ['233.8800'],
            'low_value': ['231.1200'], 
            'close_value': ['233.6700'],
            'volume_amount': ['2846669'],
            'ticker': ['AAPL']
        })
        
        parquet_extract_intraday_path = os.path.join(temp_dir, f'extract_intraday_data.parquet')
        parquet_extract_daily_path = os.path.join(temp_dir, f'extract_daily_data.parquet')
        intraday_data.to_parquet(parquet_extract_intraday_path)
        daily_data.to_parquet(parquet_extract_daily_path)

        transform_staging_data(read_dir=temp_dir,write_dir=temp_dir)
        
        for transformation in ['daily', 'intraday']:

            parquet_read = os.path.join(temp_dir, f'transform_{transformation}_data.parquet')
            result = pd.read_parquet(parquet_read)
            print(result)
            assert not result.empty
            assert 'ticker' in result.columns
            assert 'open_value' in result.columns
            assert 'high_value' in result.columns
            assert 'low_value' in result.columns
            assert 'close_value' in result.columns
            assert 'volume_amount' in result.columns
            
            if transformation == 'intraday':
                assert 'event_datetime' in result.columns
                assert pd.to_datetime(result['event_datetime']).max() > pd.Timestamp('2020-02-26 15:58:00')
            
            else:
                assert 'event_date' in result.columns
                assert pd.to_datetime(result['event_date']).max() > pd.Timestamp('2020-02-25')

if __name__ == "__main__":
    pytest.main()