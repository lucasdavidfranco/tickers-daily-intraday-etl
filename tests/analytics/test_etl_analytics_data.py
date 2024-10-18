
import pytest
from unittest import mock
import sys
import os 

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.join(current_dir, '../..')
sys.path.append(project_root)

from analytics.etl_fact_analytics import etl_intradiary_analytics
from analytics.etl_fact_analytics import etl_daily_analytics

@mock.patch('utils.db_utils.import_db_variables')
@mock.patch('utils.db_utils.connect_to_redshift')
def test_etl_intradiary_analytics(mock_connect_to_redshift, mock_import_db_variables):
    
    mock_connection = mock.Mock()
    mock_connect_to_redshift.return_value = mock_connection
    
    mock_import_db_variables.return_value = {
        'redshift_schema': 'fake_schema'
    }

    etl_intradiary_analytics()

    mock_connect_to_redshift.assert_called_once()

    mock_connection.execute.assert_called_once()

    assert "INSERT INTO" in str(mock_connection.execute.call_args[0][0])
    
@mock.patch('utils.db_utils.import_db_variables')
@mock.patch('utils.db_utils.connect_to_redshift')
def test_etl_daily_analytics(mock_connect_to_redshift, mock_import_db_variables):
    
    mock_connection = mock.Mock()
    mock_connect_to_redshift.return_value = mock_connection
    
    mock_import_db_variables.return_value = {
        'redshift_schema': 'fake_schema'
    }

    etl_daily_analytics()

    mock_connect_to_redshift.assert_called_once()

    mock_connection.execute.assert_called_once()

    assert "INSERT INTO" in str(mock_connection.execute.call_args[0][0])

if __name__ == "__main__":
    pytest.main()