
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
    
    '''
    
    Test etl fact analytics
    
    We use unitttest and run a pytest to mock a fake redshift connection
    
    Then we check if our fake connection connects and runs 2 fake execute to upload our fake data to fake database
    
    Also we check if those execute are two insert data scripts 
    
    '''
    
    mock_connection = mock.Mock()
    mock_connect_to_redshift.return_value = mock_connection
    mock_import_db_variables.return_value = {'redshift_schema': 'fake_schema'}

    etl_fact_analytics()
    
    mock_connect_to_redshift.assert_called_once()
    assert mock_connection.execute.call_count == 2
    insert_queries = [str(call[0][0]) for call in mock_connection.execute.call_args_list]
    assert all("INSERT INTO" in query for query in insert_queries)

if __name__ == "__main__":
    pytest.main()
