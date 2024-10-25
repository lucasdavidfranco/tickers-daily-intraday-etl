
import os
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.join(current_dir, '..')
sys.path.append(project_root)
from analytics import (create_analytics_tables , etl_fact_analytics, etl_dim_analytics)

def analytics_run():
    
    '''
    
    Analytics task set to run on airflow dag. 
    
    It runs after staging tasks and runs analytics ETL in following order 
    
    First checks if tables are created and creates them if not
    
    Then it runs analytics process to update fact tables 
    
    In the end runs dimension process from getting API data to update it on dimension table
    
    '''
    
    create_analytics_tables.create_analytics_tables()
    
    etl_fact_analytics.etl_fact_analytics()
    
    etl_dim_analytics.extract_dimension_data()
    
    etl_dim_analytics.transform_dimension_data()
    
    etl_dim_analytics.load_dimension_data()
    
if __name__ == "__main__":
    analytics_run()
