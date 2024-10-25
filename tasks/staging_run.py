
import os
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.join(current_dir, '..')
sys.path.append(project_root)
from staging import (create_staging_tables , extract_staging_data, transform_staging_data, load_staging_data)

def staging_run():
    
    '''
    
    Staging task set to run on airflow dag. 
    
    It is the first tast to run. It runs staging ETL in following order 
    
    First checks if tables are created and creates them if not
    
    Then it runs intradiary ETL from getting API data to update it on staging table
    
    Same process is run for daily ETL
    
    '''
    
    create_staging_tables.create_staging_tables()
    
    extract_staging_data.extract_daily_data()
    
    extract_staging_data.extract_intraday_data()
    
    transform_staging_data.transform_staging_data()
    
    load_staging_data.load_staging_data()
    
if __name__ == "__main__":
    staging_run()
