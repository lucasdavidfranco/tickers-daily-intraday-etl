
import pytest
from unittest import mock
import pandas as pd
import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.join(current_dir, '../..')
sys.path.append(project_root)

from analytics.etl_dim_analytics import load_dimension_data

@mock.patch('utils.db_utils.import_db_variables')
@mock.patch('utils.db_utils.connect_to_redshift')
@mock.patch('analytics.etl_dim_analytics.transform_dimension_data')
@mock.patch('pandas.DataFrame.to_sql')
def test_load_dimension_data(mock_to_sql, mock_transform_dimension_data, mock_connect_to_redshift, mock_import_db_variables):
    
    mock_import_db_variables.return_value = {
        'redshift_schema': 'fake_schema'
    }
    
    mock_connection = mock.Mock()
    mock_connect_to_redshift.return_value = mock_connection
    
    mock_transform_dimension_data.return_value = pd.DataFrame({
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

    load_dimension_data()
    
    mock_connect_to_redshift.assert_called_once()
    mock_to_sql.assert_called_once()
    mock_connection.execute.assert_called()
    assert "INSERT INTO" in str(mock_connection.execute.call_args[0][0])


if __name__ == "__main__":
    pytest.main()