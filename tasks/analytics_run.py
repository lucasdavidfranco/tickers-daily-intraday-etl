
import os
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.join(current_dir, '..')
sys.path.append(project_root)
from analytics import (create_analytics_tables , etl_fact_analytics, etl_dim_analytics)

def analytics_run():
    
    create_analytics_tables.create_analytics_tables()
    
    etl_fact_analytics.etl_intradiary_analytics()
    
    etl_fact_analytics.etl_daily_analytics()
    
    etl_dim_analytics.load_dimension_data()
    
if __name__ == "__main__":
    analytics_run()
