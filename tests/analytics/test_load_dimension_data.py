
import pytest
from unittest import mock
import pandas as pd
import sys
import os
import tempfile

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.join(current_dir, '../..')
sys.path.append(project_root)

from analytics.etl_dim_analytics import load_dimension_data

@mock.patch('utils.db_utils.import_db_variables')
@mock.patch('utils.db_utils.connect_to_redshift')
@mock.patch('analytics.etl_dim_analytics.transform_dimension_data')
@mock.patch('pandas.DataFrame.to_sql')
def test_load_dimension_data(mock_to_sql, mock_transform_dimension_data, mock_connect_to_redshift, mock_import_db_variables):
    
    '''
    
    Test load dimension data
    
    We use unitttest and run a pytest to mock a fake redshift connection, mock upload to sql and fake data retrieved from API
    
    Then we check if our fake connection runs a fake execute to upload our fake data to fake database
    
    Also we check if our fake connection runs a fake upload to_sql our fake data to fake database temp table
    
    In the end we check if our fake connection runs a fake execute insert into script to upload our fake data to fake database
    
    '''
    
    mock_import_db_variables.return_value = {
        'redshift_schema': 'fake_schema'
    }
    
    mock_connection = mock.Mock()
    mock_connect_to_redshift.return_value = mock_connection
    
    dimension_data = pd.DataFrame({
    'ticker': ['AAPL', 'MSFT'],
    'asset_type': ['Stock', 'Stock'],
    'name': ['Apple Inc.', 'Microsoft Corp.'],
    'country': ['USA', 'USA'],
    'sector': ['Technology', 'Technology'],
    'industry': ['Consumer Electronics', 'Software'],
    'address': ['1 Apple Park Way', '1 Microsoft Way'],
    'official_site': ['www.apple.com', 'www.microsoft.com'],
    'analyst_rating': ['Buy', 'Hold'],
    'subrogate_key': ['key1', 'key2'],
    'is_current': [1, 1],
    'audit_datetime': ['2024-10-17', '2024-10-17']
    })

    with tempfile.TemporaryDirectory() as temp_dir:
        
        parquet_transform_dimension_path = os.path.join(temp_dir, f'transform_dimension_data.parquet')
        dimension_data.to_parquet(parquet_transform_dimension_path)

        load_dimension_data(read_dir=temp_dir)
    
        mock_connect_to_redshift.assert_called_once()
        mock_to_sql.assert_called_once()
        mock_connection.execute.assert_called()
        assert "INSERT INTO" in str(mock_connection.execute.call_args[0][0])

if __name__ == "__main__":
    pytest.main()