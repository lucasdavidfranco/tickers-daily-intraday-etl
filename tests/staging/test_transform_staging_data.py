
import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.join(current_dir, '../..')
sys.path.append(project_root)

from staging.etl_staging_intradiary import transform_intradiary_data
from staging.etl_staging_daily import transform_daily_data

@patch('utils.db_utils.connect_to_redshift')
@patch('utils.db_utils.import_db_variables')
@patch('staging.etl_staging_intradiary.extract_intraday_data')
@patch('pandas.read_sql')
def test_transform_intraday_data(mock_read_sql, mock_extract_intraday_data, mock_import_db_variables, mock_connect_to_redshift):
    
    mock_import_db_variables.return_value = {
        'redshift_schema': 'fake_schema'
    }
    
    mock_connection = MagicMock()
    mock_connect_to_redshift.return_value = mock_connection
    
    mock_read_sql.return_value = pd.DataFrame({
        'ticker': ['AAPL'],
        'last_event_datetime': [pd.Timestamp('2020-02-26 15:58:00')]
    })
    
    mock_extract_intraday_data.return_value = pd.DataFrame({
        'event_datetime': ['2020-02-26 15:59:00'],
        'open_value': ['292.89001'],
        'high_value': ['293.10001'],
        'low_value': ['292.50000'], 
        'close_value': ['292.64999'],
        'volume_amount': ['385977'],
        'ticker': ['AAPL']
    })

    try:
        result = transform_intradiary_data()
        print(result)

        assert not result.empty
        assert 'ticker' in result.columns
        assert 'event_datetime' in result.columns
        assert 'open_value' in result.columns
        assert 'high_value' in result.columns
        assert 'low_value' in result.columns
        assert 'close_value' in result.columns
        assert 'volume_amount' in result.columns
        assert pd.to_datetime(result['event_datetime']).max() > pd.Timestamp('2020-02-26 15:58:00')
    
    except Exception as e:
        pytest.fail(f"Error al ejecutar extract_intradiary_data: {e}")


@patch('utils.db_utils.connect_to_redshift')
@patch('utils.db_utils.import_db_variables')
@patch('staging.etl_staging_daily.extract_daily_data')
@patch('pandas.read_sql')
def test_transform_daily_data(mock_read_sql, mock_extract_daily_data, mock_import_db_variables, mock_connect_to_redshift):
    
    mock_import_db_variables.return_value = {
        'redshift_schema': 'fake_schema'
    }
    
    mock_connection = MagicMock()
    mock_connect_to_redshift.return_value = mock_connection
    
    mock_read_sql.return_value = pd.DataFrame({
        'ticker': ['AAPL'],
        'last_event_date': [pd.Timestamp('2024-10-15')]
    })
    
    mock_extract_daily_data.return_value = pd.DataFrame({
        'event_date': ['2024-10-16'],
        'open_value': ['232.1100'],
        'high_value': ['233.8800'],
        'low_value': ['231.1200'], 
        'close_value': ['233.6700'],
        'volume_amount': ['2846669'],
        'ticker': ['AAPL']
    })

    try:
        result = transform_daily_data()
        print(result)

        assert not result.empty
        assert 'ticker' in result.columns
        assert 'event_date' in result.columns
        assert 'open_value' in result.columns
        assert 'high_value' in result.columns
        assert 'low_value' in result.columns
        assert 'close_value' in result.columns
        assert 'volume_amount' in result.columns
        assert pd.to_datetime(result['event_date']).max() > pd.Timestamp('2024-10-15')
    
    except Exception as e:
        pytest.fail(f"Error al ejecutar extract_daily_data: {e}")

if __name__ == "__main__":
    pytest.main()
