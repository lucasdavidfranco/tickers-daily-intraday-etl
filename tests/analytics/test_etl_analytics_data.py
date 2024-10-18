
import pytest
from unittest import mock
import sys
import os 

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.join(current_dir, '../..')
sys.path.append(project_root)

from analytics.etl_fact_analytics import etl_fact_analytics

@mock.patch('utils.db_utils.import_db_variables')
@mock.patch('utils.db_utils.connect_to_redshift')
def test_etl_fact_analytics(mock_connect_to_redshift, mock_import_db_variables):
    
    mock_connection = mock.Mock()
    mock_connect_to_redshift.return_value = mock_connection
    mock_import_db_variables.return_value = {'redshift_schema': 'fake_schema'}

    etl_fact_analytics()
    
    try:
        mock_connect_to_redshift.assert_called_once()
        assert mock_connection.execute.call_count == 2
        insert_queries = [str(call[0][0]) for call in mock_connection.execute.call_args_list]
        assert all("INSERT INTO" in query for query in insert_queries)
        
    except Exception as e:
        pytest.fail(f"Error al ejecutar test etl_fact_analytics: {e}")

if __name__ == "__main__":
    pytest.main()
