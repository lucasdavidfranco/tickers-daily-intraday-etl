
import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.join(current_dir, '../..')
sys.path.append(project_root)

from staging.etl_staging_daily import load_daily_data
from staging.etl_staging_intradiary import load_intradiary_data

@patch('utils.db_utils.connect_to_redshift')
@patch('utils.db_utils.import_db_variables')
@patch('staging.etl_staging_daily.transform_daily_data')
@patch('pandas.DataFrame.to_sql')
def test_load_daily_data(mock_to_sql, mock_transform_daily_data, mock_import_db_variables, mock_connect_to_redshift):
    
    '''
    
    Test load daily data
    
    We use unitttest and run a pytest to mock a fake redshift connection, mock upload to sql and fake data retrieved from API
    
    Then we check if our fake connection runs a fake execute to upload our fake data to fake database
    
    '''
    
    mock_import_db_variables.return_value = {
        'redshift_schema': 'fake_schema'
    }
    
    mock_connection = MagicMock()
    mock_connect_to_redshift.return_value = mock_connection

    mock_transform_daily_data.return_value = pd.DataFrame({
        'event_date': ['2024-10-16'],
        'open_value': [232.11],
        'high_value': [233.88],
        'low_value': [231.12],
        'close_value': [233.67],
        'volume_amount': [2846669],
        'ticker': ['AAPL'],
        'last_event_date': [pd.Timestamp('2024-10-15')]
    })
    
    load_daily_data()
    
    try:

        mock_to_sql.assert_called_once_with(
            'staging_daily_tickers',
            con=mock_connection,
            index=False,
            if_exists='append',
            method='multi',
            schema='fake_schema'
        )
        
    except Exception as e:
        pytest.fail(f"Error when executing test load_daily_data: {e}")
    

@patch('utils.db_utils.connect_to_redshift')
@patch('utils.db_utils.import_db_variables')
@patch('staging.etl_staging_intradiary.transform_intradiary_data')
@patch('pandas.DataFrame.to_sql')
def test_load_intradiary_data(mock_to_sql, mock_transform_intradiary_data, mock_import_db_variables, mock_connect_to_redshift):
    
    '''
    
    Test load intraday data
    
    We use unitttest and run a pytest to mock a fake redshift connection, mock upload to sql and fake data retrieved from API
    
    Then we check if our fake connection runs a fake execute to upload our fake data to fake database
    
    '''
    
    mock_import_db_variables.return_value = {
        'redshift_schema': 'fake_schema'
    }
    
    mock_connection = MagicMock()
    mock_connect_to_redshift.return_value = mock_connection

    mock_transform_intradiary_data.return_value = pd.DataFrame({
        'event_datetime': ['2020-02-26 15:59:00'],
        'open_value': [292.89001],
        'high_value': [293.10001],
        'low_value': [292.50000],
        'close_value': [292.64999],
        'volume_amount': [385977],
        'ticker': ['AAPL'],
        'last_event_datetime': [pd.Timestamp('2020-02-26 15:58:00')]
    })
    
    load_intradiary_data()
    
    try:

        mock_to_sql.assert_called_once_with(
            'staging_intraday_tickers',
            con=mock_connection,
            index=False,
            if_exists='append',
            method='multi',
            schema='fake_schema'
        )
    
    except Exception as e:
        pytest.fail(f"Error when executing test load_intraday_data: {e}")
    
    
@patch('utils.db_utils.connect_to_redshift')
@patch('utils.db_utils.import_db_variables')
@patch('staging.etl_staging_daily.transform_daily_data')
def test_load_daily_data_no_new_data(mock_transform_daily_data, mock_import_db_variables, mock_connect_to_redshift):
    
    '''
    
    Test no load daily data
    
    We use unitttest and run a pytest to mock a fake redshift connection and fake data retrieved from API
    
    Then we check if our fake retrieved data is empty if connection is closed
    
    '''
    
    mock_import_db_variables.return_value = {
        'redshift_schema': 'fake_schema'
    }
    
    mock_connection = MagicMock()
    mock_connect_to_redshift.return_value = mock_connection
    mock_transform_daily_data.return_value = pd.DataFrame()
    
    load_daily_data()
    
    try:
        
        assert mock_connection.close.call_count == 1
    
    except Exception as e:
        pytest.fail(f"Error when executing test load_daily_data_no_new_data: {e}")
        

@patch('utils.db_utils.connect_to_redshift')
@patch('utils.db_utils.import_db_variables')
@patch('staging.etl_staging_intradiary.transform_intradiary_data')
def test_load_intradiary_data_no_new_data(mock_transform_intradiary_data, mock_import_db_variables, mock_connect_to_redshift):
    
    '''
    
    Test no load intraday data
    
    We use unitttest and run a pytest to mock a fake redshift connection and fake data retrieved from API
    
    Then we check if our fake retrieved data is empty if connection is closed
    
    '''
    
    mock_import_db_variables.return_value = {
        'redshift_schema': 'fake_schema'
    }
    
    mock_connection = MagicMock()
    mock_connect_to_redshift.return_value = mock_connection
    mock_transform_intradiary_data.return_value = pd.DataFrame()
    
    load_intradiary_data()

    try:
        assert mock_connection.close.call_count == 1

    except Exception as e:
        pytest.fail(f"Error when executing test load_intradiary_data_no_new_data: {e}")

if __name__ == "__main__":
    pytest.main()
